(ns jepsen.tuplesky.wan
  "A simulated wide-area network between the nodes, and packet faults on
  top of it, both with tc netem on each node's egress.

  A WAN profile gives every ordered pair of nodes a one-way delay: either
  one delay for every pair, or three regions (see `regions`) with nodes
  placed round-robin in the order of :nodes. The delays hold for the whole
  test, from the nemesis's setup to its teardown, through the final reads:
  they are the network the test runs on, not a fault.

  The packet fault (:start-packet and :stop-packet, as Jepsen's own packet
  nemesis) adds one of `packet-behaviors` to the traffic to and from some
  nodes for a while: loss, jitter (which reorders), duplication,
  corruption or a bandwidth cap, on top of the profile's delay. Stopping it
  goes back to the profile, not to an unshaped network.

  Jepsen's own packet nemesis cannot do this: jepsen.net/shape! gives each
  node one netem queue, for its traffic to the targets, and clears the rest,
  so a fault would erase the WAN delays. Here each node gets a prio qdisc
  with one netem band per distinct behaviour among its peers, and a u32
  filter per peer's address. Traffic to anything else, the control node and
  so the clients included, stays unshaped: the clients sit next to the node
  they talk to.

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
  as netem does whenever it has jitter and no rate."
  [{:loss "1%"}
   {:loss "5%" :loss-correlation "25%"}
   {:delay-ms 50 :jitter-ms 25}
   {:delay-ms 5 :reorder "5%"}
   {:duplicate "2%"}
   {:corrupt "1%"}
   {:rate "10mbit"}])

(defn parse-spec
  "Parses --wan: `none`, `regions`, or a one-way delay in milliseconds
  between every two nodes."
  [spec]
  (cond
    (or (nil? spec) (= "none" spec)) nil
    (= "regions" spec)               {:kind :regions}
    (re-matches #"\d+(\.\d+)?" spec) {:kind :uniform
                                      :delay-ms (Double/parseDouble spec)}
    :else (throw (IllegalArgumentException.
                   (str "--wan must be none, regions or milliseconds, not "
                        (pr-str spec))))))

(defn valid-spec?
  "Whether --wan parses."
  [spec]
  (try (parse-spec spec) true
       (catch IllegalArgumentException _ false)))

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
  [{:keys [delay-ms jitter-ms loss loss-correlation reorder duplicate corrupt
           rate]}]
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
               duplicate (into [:duplicate duplicate])
               corrupt   (into [:corrupt corrupt])
               rate      (into [:rate rate]))]
    (when (seq args) args)))

(defn bands
  "src's netem bands: one per distinct set of netem arguments among its
  peers, as [args peers], in a stable order."
  [wan nodes fault src]
  (->> nodes
       (remove #{src})
       (keep (fn [dst]
               (when-let [args (netem-args (behavior wan nodes fault src dst))]
                 [args dst])))
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

(defn shape!
  "Shapes every node's egress for the profile and the fault (nil for
  none). Returns {node bands}."
  [test {:keys [ips devs]} wan fault]
  (let [nodes (:nodes test)]
    (c/on-nodes test
                (fn [_ node]
                  (let [dev (devs node)
                        bs  (bands wan nodes fault node)]
                    (c/su
                      (util/meh (c/exec :tc :qdisc :del :dev dev :root))
                      (doseq [cmd (tc-commands dev ips bs)]
                        (apply c/exec :tc cmd)))
                    (mapv (fn [[args peers]] [(str/join " " (map name args)) peers])
                          bs))))))

(defn clear!
  "Removes every node's shaping."
  [test {:keys [devs]}]
  (c/on-nodes test
              (fn [_ node]
                (c/su (util/meh (c/exec :tc :qdisc :del :dev (devs node) :root))))))

(defn learn
  "Each node's address, and the device it reaches its peers through."
  [test]
  (let [ips  (into {} (c/on-nodes test (fn [_ _] (cn/local-ip))))
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
    {:ips ips, :devs devs}))

(defn measure
  "The round trip in milliseconds from the first node to each other node,
  from three pings; logged, so a run shows that the shaping took."
  [test {:keys [ips]}]
  (let [[a & others] (:nodes test)]
    (when (seq others)
      (c/on-nodes test [a]
                  (fn [_ _]
                    (into (sorted-map)
                          (for [b others]
                            (let [out (try (c/exec :ping :-c 3 :-i "0.2" :-q (ips b))
                                           (catch RuntimeException e (str e)))]
                              [b (some-> (re-find #"= [\d.]+/([\d.]+)/" out)
                                         second
                                         (Double/parseDouble))]))))))))

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
        (let [nodes (:nodes test)
              a     (first nodes)]
          (info "WAN regions:" (pr-str (into (sorted-map)
                                             (map (juxt identity
                                                        (partial region-name nodes))
                                                  nodes))))
          (doseq [[b rtt] (get (measure test s) a)]
            (let [expected (* 2 (base-delay-ms wan nodes a b))]
              (info "WAN round trip" a "->" b ":" rtt "ms, profile" expected "ms")
              (when (or (nil? rtt) (< rtt (* 0.8 expected)))
                (warn "WAN round trip" a "->" b "is" rtt
                      "ms, below the profile's" expected "ms: is netem shaping?"))))))
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
