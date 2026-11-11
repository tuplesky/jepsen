(ns jepsen.tuplesky.nemesis
  "Fault schedules for the combined nemesis.

  jepsen.nemesis.combined's DB package draws kill/start and pause/resume
  from one staggered mix: each operation picks one of the two flip-flops at
  random. After a kill, the start waits until the mix draws kill/start
  again, so a run of pauses and resumes can leave every node down for
  minutes (four TupleSky runs and two etcd runs in a day lost two to four
  minutes this way). Here the two flip-flops are staggered on their own, so
  a start follows each kill within one interval, at most twice :interval
  seconds.

  The network a test runs on, and its packet faults, come from
  jepsen.tuplesky.wan instead of Jepsen's packet package (see wan-package).

  Depends on Jepsen alone, so the etcd baseline can load it too and both
  tests keep the same fault schedule."
  (:require [jepsen [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [nemesis :as n]
                    [random :as rand]]
            [jepsen.nemesis.combined :as nc]
            [jepsen.tuplesky.wan :as wan]))

(defn db-package?
  "Whether a package from nc/nemesis-packages is the DB package, the one
  that kills, starts, pauses and resumes processes."
  [pkg]
  (boolean (some #(= "kill" (:name %)) (:perf pkg))))

(defn stagger
  "Like gen/stagger up to io.jepsen/generator 0.1.3: delays between g's
  operations are uniform in [0, 2 * interval) seconds. From 0.1.4 on,
  gen/stagger draws them from an exponential distribution capped at 100
  seconds, so a start could follow a kill by over three intervals. This takes
  uniform delays through stagger-nanos where the generator library has it
  (0.1.4 on), and from gen/stagger where that is still uniform (the etcd
  test's Jepsen 0.3.11 uses 0.1.1)."
  [interval g]
  (if-let [stagger-nanos (resolve 'jepsen.generator/stagger-nanos)]
    (let [bound (gen/secs->nanos (* 2 interval))]
      (stagger-nanos (fn [] (rand/long bound)) g))
    (gen/stagger interval g)))

(defn db-generator
  "A generator of kills and pauses, like nc/db-generators', with kill/start
  and pause/resume each staggered by :interval on their own. Nil when opts
  enable neither fault, or the DB supports neither."
  [opts]
  (let [db       (:db opts)
        faults   (set (:faults opts))
        interval (:interval opts nc/default-interval)
        kill?    (and (satisfies? db/Kill db) (contains? faults :kill))
        pause?   (and (satisfies? db/Pause db) (contains? faults :pause))
        kill-targets  (:targets (:kill opts) (nc/node-specs db))
        pause-targets (:targets (:pause opts) (nc/node-specs db))
        kill   (fn [_ _] {:type :info, :f :kill, :value (rand/nth kill-targets)})
        pause  (fn [_ _] {:type :info, :f :pause, :value (rand/nth pause-targets)})
        start  {:type :info, :f :start, :value :all}
        resume {:type :info, :f :resume, :value :all}
        gens   (cond-> []
                 kill?  (conj (stagger interval
                                       (gen/flip-flop kill (gen/repeat start))))
                 pause? (conj (stagger interval
                                       (gen/flip-flop pause (gen/repeat resume)))))]
    (when (seq gens)
      (apply gen/any gens))))

(defn separate-db-faults
  "Takes nemesis-package opts and the packages nc/nemesis-packages made from
  them, and gives the DB package the generator of db-generator. The DB
  package's nemesis, final generator and perf are kept."
  [opts packages]
  (let [g (db-generator opts)]
    (map (fn [pkg]
           (if (and g (db-package? pkg) (:generator pkg))
             (assoc pkg :generator g)
             pkg))
         packages)))

(defn partition-package?
  "Whether a package from nc/nemesis-packages is the partition package."
  [pkg]
  (boolean (some #(= "partition" (:name %)) (:perf pkg))))

(defn follower-grudge
  "A grudge that cuts one voter off from every other node: a random node
  outside db/primaries, so not the voter leading the highest ballot. Every
  node when the DB names no primaries, or names them all."
  [test db]
  (let [nodes     (vec (:nodes test))
        primaries (set (when (satisfies? db/Primary db) (db/primaries db test)))
        others    (vec (remove primaries nodes))
        node      (rand/nth (if (seq others) others nodes))]
    (n/complete-grudge [[node] (vec (remove #{node} nodes))])))

(defn follower-partition-nemesis
  "Wraps nc/partition-nemesis `p` so a :start-partition whose value is
  :follower cuts off one voter that does not lead (follower-grudge); every
  other operation goes to `p` as it is."
  [db p]
  (reify
    n/Reflection
    (fs [_] [:start-partition :stop-partition])

    n/Nemesis
    (setup! [_ test]
      (follower-partition-nemesis db (n/setup! p test)))

    (invoke! [_ test op]
      (if (and (= :start-partition (:f op)) (= :follower (:value op)))
        (n/invoke! p test (assoc op :value (follower-grudge test db)))
        (n/invoke! p test op)))

    (teardown! [_ test]
      (n/teardown! p test))))

(defrecord Partitions [next start? targets hold-nanos gap-nanos]
  ; Alternates a :start-partition on one of targets and a :stop-partition.
  ; Each is due at `next`, absolute in the test's time, so asking again
  ; before it is due gives the same time; the one after is due a hold (a
  ; start's) or a gap (a stop's) later, drawn when this one is taken.
  gen/Generator
  (op [this test ctx]
    (let [op (if start?
               {:type :info, :f :start-partition, :value (rand/nth targets)}
               {:type :info, :f :stop-partition, :value nil})
          o  (gen/fill-in-op op ctx)]
      (if (= :pending o)
        [:pending this]
        (let [t (max (:time o) next)]
          [(assoc o :time t)
           (Partitions. (+ t (if start? (hold-nanos) (gap-nanos)))
                        (not start?) targets hold-nanos gap-nanos)]))))

  (update [this _ _ _] this))

(defn partition-generator
  "Partitions on :partition's :targets that each heal after a uniform
  [lo, hi] seconds, :partition's :seconds, the next starting a uniform
  [0, 2 * every) seconds after the heal, :partition's :every or else
  :interval; the first one that far into the test. Nil without :seconds."
  [opts]
  (when-let [[lo hi] (:seconds (:partition opts))]
    (let [every   (:every (:partition opts) (:interval opts nc/default-interval))
          targets (vec (:targets (:partition opts) [:one]))
          nanos   (fn [secs] (long (gen/secs->nanos secs)))
          span    (nanos (- hi lo))
          hold    (fn [] (+ (nanos lo) (if (pos? span) (rand/long span) 0)))
          gap     (fn [] (rand/long (nanos (* 2 every))))]
      (Partitions. (gap) true targets hold gap))))

(defn separate-partition-faults
  "Takes nemesis-package opts and nc/nemesis-packages' packages, and gives
  the partition package partition-generator's schedule when :partition has
  :seconds, and follower-partition-nemesis when its :targets include
  :follower. Other packages, and a partition package that needs neither,
  are kept."
  [opts packages]
  (let [g         (partition-generator opts)
        follower? (some #{:follower} (:targets (:partition opts)))]
    (map (fn [pkg]
           (if (partition-package? pkg)
             (cond-> pkg
               (and g (:generator pkg)) (assoc :generator g)
               follower? (update :nemesis #(follower-partition-nemesis
                                              (:db opts) %)))
             pkg))
         packages)))

(def packet-targets
  "Whose traffic a packet fault disrupts: to and from one node, a
  minority, or every node."
  [:one :minority :all])

(defn wan-package
  "The WAN nemesis (jepsen.tuplesky.wan) as a package: the network profile
  :wan (parsed by wan/parse-spec; nil for none), and, when :faults has
  :packet, packet faults on top of it, each from :packet's :behaviors
  (wan/packet-behaviors) on :packet's :targets (packet-targets), staggered
  by :interval. Nil when there is neither."
  [opts]
  (let [profile  (:wan opts)
        packet?  (contains? (set (:faults opts)) :packet)
        interval (:interval opts nc/default-interval)
        targets  (:targets (:packet opts) packet-targets)
        behaviors (:behaviors (:packet opts) wan/packet-behaviors)
        start    (fn [_ _] {:type  :info
                            :f     :start-packet
                            :value [(rand/nth targets) (rand/nth behaviors)]})
        stop     {:type :info, :f :stop-packet, :value nil}]
    (when (or profile packet?)
      {:nemesis         (wan/nemesis profile (:db opts))
       :generator       (when packet?
                          (stagger interval
                                   (gen/flip-flop start (gen/repeat stop))))
       :final-generator (when packet? stop)
       :perf            #{{:name  "packet"
                           :start #{:start-packet}
                           :stop  #{:stop-packet}
                           :color "#D1E8A0"}}})))

(defn jepsen-packet-package?
  "Whether a package from nc/nemesis-packages is Jepsen's packet package.
  Its nemesis is there even without the fault, and clears every node's
  shaping at setup."
  [pkg]
  (boolean (some #(= "packet" (:name %)) (:perf pkg))))

(defn with-wan
  "Takes nemesis-package opts and the packages nc/nemesis-packages made from
  them, and puts wan-package in place of Jepsen's packet package when
  there is a profile or the packet fault. Unchanged otherwise."
  [opts packages]
  (if-let [pkg (wan-package opts)]
    (conj (vec (remove jepsen-packet-package? packages)) pkg)
    packages))

(defn- release-peers!
  "Takes every node's hold on its inbound peer-plane traffic away. A node
  that holds none is left as it is."
  [test port]
  (c/on-nodes test
              (fn [_ _]
                (try (c/su (c/exec :iptables :-D :INPUT :-p :udp
                                   :--dport port :-j :DROP))
                     (catch Exception _ nil)))))

(defrecord HoldPeers [port]
  n/Reflection
  (fs [_] #{:hold-peers :release-peers})

  n/Nemesis
  (setup! [this _] this)

  (invoke! [_ test op]
    (case (:f op)
      :hold-peers
      (do (c/on-nodes test
                      (fn [_ _]
                        (c/su (c/exec :iptables :-I :INPUT :-p :udp
                                      :--dport port :-j :DROP))))
          (assoc op :value [:held port]))

      :release-peers
      (do (release-peers! test port)
          (assoc op :value [:released port]))))

  (teardown! [_ test]
    (release-peers! test port)))

(defn hold-peers-package
  "Every node's inbound UDP on the peer plane's port dropped at the start
  of the workload, for :seconds, while the clients run (task-d67's
  start-up row): a leader's proposals and the voters' acknowledgements to
  each other wait, and the collectors, on the API plane, still reach every
  voter. A hold shorter than QUIC's idle timeout (30 s) keeps the
  connections. The hold itself is hold-peers-generator's, from the
  workload's start; this package carries the nemesis, and the release in
  case the workload ends first. Nil for no hold."
  [{:keys [seconds port]}]
  (when (and seconds (pos? seconds))
    {:nemesis         (HoldPeers. port)
     :final-generator {:type :info, :f :release-peers, :value nil}
     :perf            #{{:name  "hold-peers"
                         :start #{:hold-peers}
                         :stop  #{:release-peers}
                         :color "#E8C8A0"}}}))

(defn hold-peers-generator
  "The hold on the peer plane for its first `seconds`, from the workload's
  start; nil for none."
  [seconds]
  (when (and seconds (pos? seconds))
    (gen/phases {:type :info, :f :hold-peers, :value nil}
                (gen/sleep seconds)
                {:type :info, :f :release-peers, :value nil})))

(defrecord At [nanos op]
  gen/Generator
  (op [this test ctx]
    (when op
      (let [o (gen/fill-in-op op ctx)]
        (if (= :pending o)
          [:pending this]
          [(assoc o :time (max (:time o) nanos)) (At. nanos nil)]))))

  (update [this _ _ _] this))

(defn at
  "A generator of one operation, `op`, at `secs` seconds into the
  generator's run, and nothing after it. Unlike a gen/sleep before it, it
  holds no worker while it waits, so under gen/any the other generator's
  operations go on in the meantime."
  [secs op]
  (At. (gen/secs->nanos secs) op))

(defn nemesis-package
  "nc/nemesis-package, with the DB package's faults separated, the
  partition package's own schedule and follower targets when :partition
  asks for them (separate-partition-faults), the network from
  jepsen.tuplesky.wan, the peer plane's hold at the
  start when :hold-peers asks for one (hold-peers-package), and any
  packages in :extra (nils skipped)."
  [opts]
  (let [opts (update opts :faults set)
        hold (hold-peers-package (:hold-peers opts))]
    (nc/compose-packages
      (cond-> (vec (with-wan opts (->> (nc/nemesis-packages opts)
                                       (separate-db-faults opts)
                                       (separate-partition-faults opts))))
        hold (conj hold)
        true (into (remove nil? (:extra opts)))))))
