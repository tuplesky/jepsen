(ns jepsen.tuplesky.wan
  "A simulated wide-area network between the nodes, and packet faults on
  top of it, both with tc netem on each node's egress.

  A WAN profile gives every ordered pair of nodes a one-way delay: either
  one delay for every pair, or three regions (see `regions`) with nodes
  placed round-robin in the order of :nodes. The delays hold for the whole
  test, from the nemesis's setup to its teardown, through the final reads:
  they are the network the test runs on, not a fault.

  The clients run on the control node. By default (:clients :first) they
  sit beside the first node, as a Jepsen control node on real hosts sits in
  one region: each node's traffic to the control node is delayed by its
  whole round trip to the first node, since only the nodes' egress is
  shaped. A request then reaches a node at once and its answer takes the
  round trip, which is the same round trip a client in that region sees.
  With :clients :local the control node's traffic is not shaped, as if
  every client sat beside the node it talks to. That flatters a protocol
  whose clients send to every replica (SwiftPaxos's fast path takes one
  client round trip to a quorum, which :local makes free) against one whose
  clients talk to one node.

  The packet fault (:start-packet and :stop-packet, as Jepsen's own packet
  nemesis) adds one of `packet-behaviors` to the traffic between nodes,
  to and from some of them, for a while: loss, jitter (which reorders),
  reordering, corruption or a bandwidth cap, on top of the profile's delay.
  Stopping it goes back to the profile, not to an unshaped network. A fault
  whose tc commands fail on a node puts that node back on the profile
  before the failure is reported.

  Jepsen's own packet nemesis cannot do this: jepsen.net/shape! gives each
  node one netem queue, for its traffic to the targets, and clears the rest,
  so a fault would erase the WAN delays. Here each node gets a prio qdisc
  with one netem band per distinct behaviour among its peers, and a u32
  filter per peer's address.

  Depends on Jepsen alone, like jepsen.tuplesky.nemesis, so the etcd
  baseline can load it too."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [nemesis :as n]
                    [util :as util]]
            [jepsen.control.net :as cn]
            [jepsen.nemesis.combined :as nc]))

(def regions
  "Three regions and the one-way delays between them, in milliseconds:
  about half the round trips between AWS us-east-1, us-west-2 and
  eu-west-1 (roughly 66, 74 and 130 ms). Nodes in one region are a
  millisecond apart, as across availability zones."
  {:names ["us-east" "us-west" "eu-west"]
   :local 1
   :delay {#{0 1} 33
           #{0 2} 37
           #{1 2} 65}})

(def packet-behaviors
  "What a packet fault adds to the profile on the traffic to and from its
  targets. :delay-ms adds to the profile's delay; jitter reorders packets,
  as netem does whenever it has jitter and no rate. Not duplication: the
  kernel refuses a duplicating netem in a tree with other netems (\"netem:
  cannot mix duplicating netems with other netems in tree\"), and a node
  under a profile has one netem per delay."
  [{:loss "1%"}
   {:loss "5%" :loss-correlation "25%"}
   {:delay-ms 50 :jitter-ms 25}
   {:delay-ms 5 :reorder "5%"}
   {:corrupt "1%"}
   {:rate "10mbit"}])

(defn parse-spec
  "Parses --wan: `none`, `regions`, or a one-way delay in milliseconds
  between every two nodes. The clients sit beside the first node."
  [spec]
  (cond
    (or (nil? spec) (= "none" spec)) nil
    (= "regions" spec)               {:kind :regions, :clients :first}
    (re-matches #"\d+(\.\d+)?" spec) {:kind     :uniform
                                      :delay-ms (Double/parseDouble spec)
                                      :clients  :first}
    :else (throw (IllegalArgumentException.
                   (str "--wan must be none, regions or milliseconds, not "
                        (pr-str spec))))))

(defn parse-clients
  "Parses --wan-clients: `first` (beside the first node) or `local` (beside
  each node they talk to)."
  [spec]
  (case spec
    "first" :first
    "local" :local
    (throw (IllegalArgumentException.
             (str "--wan-clients must be first or local, not " (pr-str spec))))))

(defn with-clients
  "The profile with its clients placed, when there is a profile and a
  placement."
  [wan clients]
  (cond-> wan (and wan clients) (assoc :clients clients)))

(defn region
  "The region index of a node: round-robin over :nodes, in their order."
  [nodes node]
  (mod (.indexOf ^java.util.List (vec nodes) node) (count (:names regions))))

(defn region-name
  [nodes node]
  (get (:names regions) (region nodes node)))

(defn base-delay-ms
  "The profile's one-way delay from src to dst, in milliseconds."
  [wan nodes src dst]
  (cond
    (or (nil? wan) (= src dst)) 0
    (= :uniform (:kind wan))    (:delay-ms wan)
    :else (let [a (region nodes src)
                b (region nodes dst)]
            (if (= a b)
              (:local regions)
              (get (:delay regions) #{a b})))))

(defn client-delay-ms
  "The delay on a node's traffic to the clients: its round trip to the
  first node, with the clients beside it, and none with :local clients."
  [wan nodes node]
  (if (= :first (:clients wan))
    (* 2 (base-delay-ms wan nodes node (first nodes)))
    0))

(defn behavior
  "What src's traffic to dst gets: the profile's delay, plus the fault's
  behaviour when either end is one of the fault's targets."
  [wan nodes fault src dst]
  (let [base {:delay-ms (base-delay-ms wan nodes src dst)}]
    (if (and fault (or (contains? (:targets fault) src)
                       (contains? (:targets fault) dst)))
      (merge-with (fn [a b] (if (number? a) (+ a b) b))
                  base (:behavior fault))
      base)))

(defn- ms [x]
  (str (if (== x (Math/rint x)) (long x) x) "ms"))

(defn netem-args
  "The netem arguments for a behaviour, or nil when it changes nothing."
  [{:keys [delay-ms jitter-ms loss loss-correlation reorder corrupt rate]}]
  (let [delay-ms  (or delay-ms 0)
        jitter-ms (or jitter-ms 0)
        args (cond-> []
               (or (pos? delay-ms) (pos? jitter-ms))
               (into (if (pos? jitter-ms)
                       [:delay (ms delay-ms) (ms jitter-ms) :distribution :normal]
                       [:delay (ms delay-ms)]))
               loss      (into (cond-> [:loss loss]
                                 loss-correlation (conj loss-correlation)))
               reorder   (into [:reorder reorder])
               corrupt   (into [:corrupt corrupt])
               rate      (into [:rate rate]))]
    (when (seq args) args)))

(defn bands
  "src's netem bands: one per distinct set of netem arguments among its
  peers, as [args peers], in a stable order. The clients are the peer
  :control."
  [wan nodes fault src]
  (->> nodes
       (remove #{src})
       (keep (fn [dst]
               (when-let [args (netem-args (behavior wan nodes fault src dst))]
                 [args dst])))
       (concat (when-let [args (netem-args
                                 {:delay-ms (client-delay-ms wan nodes src)})]
                 [[args :control]]))
       (group-by first)
       (map (fn [[args pairs]] [args (mapv second pairs)]))
       (sort-by (comp str first))
       vec))

(def priomap
  "The prio qdisc's default map onto its first three bands, as
  jepsen.net's shaping uses: bands from the fourth on take only what a
  filter sends there."
  [1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1])

(defn tc-commands
  "The tc commands that give a node's device its bands; each is a vector of
  arguments to tc. Removing the root qdisc comes first and is left to the
  caller, since it fails when there is none."
  [dev ips node-bands]
  (when (seq node-bands)
    (assert (<= (+ 3 (count node-bands)) 16) "a prio qdisc has 16 bands")
    (into [(into [:qdisc :add :dev dev :root :handle "1:"
                  :prio :bands (+ 3 (count node-bands)) :priomap]
                 priomap)]
          cat
          (map-indexed
            (fn [i [args peers]]
              (let [band (+ 4 i)]
                (into [(into [:qdisc :add :dev dev :parent (str "1:" band)
                              :handle (str (+ 10 band) ":") :netem]
                             args)]
                      (for [peer peers]
                        [:filter :add :dev dev :parent "1:0" :protocol :ip
                         :prio 3 :u32 :match :ip :dst (str (ips peer) "/32")
                         :flowid (str "1:" band)]))))
            node-bands))))

(defn- apply-bands!
  "Replaces the current node's root qdisc with its bands."
  [dev ips bs]
  (util/meh (c/exec :tc :qdisc :del :dev dev :root))
  (doseq [cmd (tc-commands dev ips bs)]
    (apply c/exec :tc cmd)))

(defn shape!
  "Shapes every node's egress for the profile and the fault (nil for
  none). A node whose commands fail for a fault goes back to the profile
  before the failure is thrown. Returns {node bands}."
  [test {:keys [ips devs control-ips]} wan fault]
  (let [nodes (:nodes test)]
    (c/on-nodes test
                (fn [_ node]
                  (let [dev  (devs node)
                        ips' (assoc ips :control (control-ips node))
                        bs   (bands wan nodes fault node)]
                    (c/su
                      (try (apply-bands! dev ips' bs)
                           (catch Exception e
                             (when fault
                               (util/meh (apply-bands! dev ips' (bands wan nodes nil node))))
                             (throw e))))
                    (mapv (fn [[args peers]] [(str/join " " (map name args)) peers])
                          bs))))))

(defn clear!
  "Removes every node's shaping."
  [test {:keys [devs]}]
  (c/on-nodes test
              (fn [_ node]
                (c/su (util/meh (c/exec :tc :qdisc :del :dev (devs node) :root))))))

(defn learn
  "Each node's address, the device it reaches its peers through, and the
  control node's address as the node sees it (its SSH client)."
  [test]
  (let [ips  (into {} (c/on-nodes test (fn [_ _] (cn/local-ip))))
        control-ips (into {} (c/on-nodes
                               test
                               (fn [_ _]
                                 (first (str/split (str/trim (c/exec :printenv :SSH_CLIENT))
                                                   #"\s+")))))
        devs (into {} (c/on-nodes
                        test
                        (fn [_ node]
                          (let [peer (some (fn [[n ip]] (when (not= n node) ip))
                                           ips)
                                route (if peer
                                        (c/exec :ip :-o :route :get peer)
                                        (c/exec :ip :-o :route :show :default))]
                            (or (second (re-find #"\bdev (\S+)" route))
                                (throw (IllegalStateException.
                                         (str node " has no route device in: "
                                              route))))))))]
    {:ips ips, :devs devs, :control-ips control-ips}))

(defn ping-ms
  "The mean round trip of three pings from the current node, in
  milliseconds, or nil when none came back."
  [ip]
  (let [out (try (c/exec :ping :-c 3 :-i "0.2" :-q ip)
                 (catch RuntimeException e (str e)))]
    (some-> (re-find #"= [\d.]+/([\d.]+)/" out)
            second
            (Double/parseDouble))))

(defn measure
  "Round trips in milliseconds: from the first node to each other node,
  and from each node to the control node (where only the node's side is
  shaped, so it is the clients' round trip); logged, so a run shows that
  the shaping took. As [[from to measured profile] ...]."
  [test wan {:keys [ips control-ips]}]
  (let [nodes        (:nodes test)
        [a & others] nodes
        peers   (when (seq others)
                  (get (c/on-nodes test [a]
                                   (fn [_ _]
                                     (mapv (fn [b] [b (ping-ms (ips b))]) others)))
                       a))
        clients (c/on-nodes test (fn [_ node] (ping-ms (control-ips node))))]
    (concat
      (for [[b rtt] peers]
        [a b rtt (* 2 (base-delay-ms wan nodes a b))])
      (for [node nodes]
        [node "control" (get clients node) (client-delay-ms wan nodes node)]))))

(defrecord Nemesis [wan db state]
  n/Reflection
  (fs [_] #{:start-packet :stop-packet})

  n/Nemesis
  (setup! [this test]
    (let [s (learn test)]
      (reset! state s)
      (let [shaped (shape! test s wan nil)]
        (info "WAN" (pr-str wan) "shaped:" (pr-str (into (sorted-map) shaped))))
      (when wan
        (let [nodes (:nodes test)]
          (info "WAN regions:" (pr-str (into (sorted-map)
                                             (map (juxt identity
                                                        (partial region-name nodes))
                                                  nodes))))
          (info "WAN clients:" (name (:clients wan :local)))
          (doseq [[from to rtt expected] (measure test wan s)]
            (info "WAN round trip" from "->" to ":" rtt "ms, profile" expected "ms")
            (when (and (pos? expected) (or (nil? rtt) (< rtt (* 0.8 expected))))
              (warn "WAN round trip" from "->" to "is" rtt
                    "ms, below the profile's" expected "ms: is netem shaping?")))))
      this))

  (invoke! [_ test {:keys [f value] :as op}]
    (case f
      :start-packet
      (let [[spec b] value
            targets (set (nc/db-nodes test db spec))
            fault   {:targets targets, :behavior b}]
        (shape! test @state wan fault)
        (assoc op :value [(vec (sort targets)) b]))

      :stop-packet
      (do (shape! test @state wan nil)
          (assoc op :value (if wan :wan :unshaped)))))

  (teardown! [_ test]
    (when-let [s @state]
      (clear! test s))))

(defn nemesis
  "The WAN nemesis: shapes the profile (nil for none) at setup, and takes
  :start-packet [node-spec behaviour] and :stop-packet."
  [wan db]
  (Nemesis. wan db (atom nil)))
