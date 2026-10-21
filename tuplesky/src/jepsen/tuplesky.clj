(ns jepsen.tuplesky
  "Jepsen tests for TupleSky.

  Every node runs one voter of a TupleSky domain (see jepsen.tuplesky.db),
  and every client drives the domain through its native API with the
  `coord-jepsen` shim (see jepsen.tuplesky.client). The etcd test cannot be
  used instead: TupleSky's etcd face is Kine, which serves only the
  transaction shapes Kubernetes sends."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [random :as rand]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.os.debian :as debian]
            [jepsen.tests.cycle [append :as append]
                                [wr :as wr]]
            [jepsen.tests.linearizable-register :as lr]
            [jepsen.tuplesky [client :as client]
                             [db :as db]
                             [nemesis :as tn]
                             [wan :as wan]]
            [knossos.model :as model]))

(defn append-workload
  "Elle list-append: transactions of appends and reads over a few lists,
  checked for strict serializability."
  [opts]
  (assoc (append/test {:key-count          (:key-count opts 3)
                       :key-dist           (:key-dist opts :exponential)
                       :max-txn-length     4
                       :consistency-models [:strict-serializable]})
         :client (client/client)))

(defn read-only
  "A wr transaction with its writes turned into reads of the same keys."
  [txn]
  (mapv (fn [[f k _ :as mop]] (if (= :w f) [:r k nil] mop)) txn))

(defn with-read-ratio
  "Makes about `ratio` of a wr workload's transactions read-only. Elle's
  wr-txns picks a read or a write for each operation of a transaction,
  half each, so the reads are then ratio + (1 - ratio) / 2 of the
  operations: 0.6 makes about 80%. Writes stay unique per key, since a
  value a read-only transaction would have written is simply never
  written. A ratio of 0 leaves the workload as it is."
  [ratio workload]
  (if (pos? ratio)
    (update workload :generator
            (fn [g]
              (gen/map (fn [op]
                         (if (< (rand/double) ratio)
                           (update op :value read-only)
                           op))
                       g)))
    workload))

(defn wr-workload
  "Elle rw-register: transactions of writes and reads over a few registers,
  checked for strict serializability; --read-ratio makes some of them
  read-only."
  [opts]
  (->> (assoc (wr/test {:key-count          (:key-count opts 3)
                        :key-dist           (:key-dist opts :exponential)
                        :max-txn-length     4
                        :consistency-models [:strict-serializable]})
              :client (client/client))
       (with-read-ratio (:read-ratio opts 0))))

(defn register-workload
  "Linearizable reads, writes and compare-and-set on independent
  registers, checked by Knossos."
  [opts]
  (assoc (lr/test {:nodes         (:nodes opts)
                   :model         (model/cas-register)
                   :per-key-limit (:per-key-limit opts)})
         :client (client/client)))

(def workloads
  {:append   append-workload
   :wr       wr-workload
   :register register-workload})

(def nemeses
  "Faults we know how to inject. :packet disrupts packets on top of the
  --wan profile (see jepsen.tuplesky.wan)."
  #{:kill :pause :partition :clock :packet})

(def all-nemeses
  "Combinations of faults test-all runs."
  [[]
   [:kill]
   [:pause]
   [:partition]
   [:clock]
   [:packet]
   [:kill :partition]
   [:pause :kill :partition :clock :packet]])

(defn parse-nemesis-spec
  "Parses a comma-separated list of faults; `none` is no faults."
  [spec]
  (if (= "none" spec)
    []
    (mapv keyword (str/split spec #","))))

(defn throttle
  "Staggers a generator to about rate operations a second; a rate of 0
  leaves it unthrottled, so each client issues its next operation as soon
  as the last completes, and the throughput is what the system sustains at
  that concurrency."
  [rate gen]
  (if (pos? rate)
    (gen/stagger (/ rate) gen)
    gen))

(defn test-name
  "The test's name, which names its store directory: the system, workload,
  faults, and the WAN profile when there is one."
  [system opts]
  (str system " " (name (:workload opts)) " "
       (if (seq (:nemesis opts))
         (str/join "," (map name (:nemesis opts)))
         "none")
       (when-let [w (:wan opts)]
         (str " wan-" (if (= :uniform (:kind w))
                        (str (:delay-ms w) "ms")
                        (name (:kind w)))
              (when (= :local (:clients w)) "-local-clients")))))

(defn run-dir
  "Where the domain is provisioned on the control node. Absolute, and new
  for every test: each test provisions its own domain."
  []
  (.getCanonicalPath
    (io/file "store" "tuplesky-runs"
             (str (System/currentTimeMillis) "-" (rand-int 100000)))))

(defn tuplesky-test
  "Constructs a test from parsed CLI options."
  [opts]
  (let [opts          (update opts :wan wan/with-clients (:wan-clients opts))
        workload-name (:workload opts)
        workload      ((workloads workload-name) opts)
        db            (db/db)
        ; Kill/start and pause/resume on their own schedules, so a start
        ; follows every kill within one interval (see jepsen.tuplesky.nemesis).
        nemesis       (tn/nemesis-package
                        {:db        db
                         :nodes     (:nodes opts)
                         :faults    (:nemesis opts)
                         :partition {:targets [:one :majority :majorities-ring]}
                         :pause     {:targets [:one :majority]}
                         :kill      {:targets (case (:kill-targets opts)
                                                    :leader [:primaries]
                                                    [:one :majority :all])}
                         :wan       (:wan opts)
                         :hold-peers {:seconds (:hold-peer-links opts)
                                      :port    (:peer-port opts)}
                         :interval  (:nemesis-interval opts)})
        wrap          (:wrap-generator workload identity)
        gen           (->> (:generator workload)
                           (throttle (:rate opts))
                           ; The peer plane's hold, if any, from the
                           ; workload's start; the faults after it.
                           (gen/nemesis (gen/phases (tn/hold-peers-generator
                                                      (:hold-peer-links opts))
                                                    (gen/sleep 5)
                                                    (:generator nemesis)))
                           (gen/time-limit (:time-limit opts)))
        gen           (gen/phases
                        gen
                        (gen/log "Healing cluster")
                        (gen/nemesis (:final-generator nemesis))
                        ; A killed peer is noticed only at the transport's
                        ; idle timeout (30 s), and a restarted one may
                        ; rejoin that late again.
                        (gen/log "Waiting for recovery")
                        (gen/sleep (:recovery-time opts))
                        (gen/clients (:final-generator workload)))]
    (merge tests/noop-test
           opts
           {:name            (test-name "tuplesky" opts)
            :pure-generators true
            :os              debian/os
            :db              db
            :run-dir         (run-dir)
            :key-prefix      (str "jepsen/" (System/currentTimeMillis) "/")
            :client          (:client workload)
            :nemesis         (:nemesis nemesis)
            :generator       (wrap gen)
            :checker         (checker/compose
                               {:perf       (checker/perf
                                              {:nemeses (:perf nemesis)})
                                :clock      (checker/clock-plot)
                                :stats      (checker/stats)
                                :exceptions (checker/unhandled-exceptions)
                                ; A voter that panics stops serving; that
                                ; is a finding even when the history is
                                ; clean.
                                :crash      (checker/log-file-pattern
                                              #"panicked at" "coordd.log")
                                :workload   (:checker workload)})})))

(def cli-opts
  "Command line options."
  [[nil "--bin-dir DIR" "Directory holding coordd, coord-harness and coord-jepsen built for the nodes' and this machine's platform (for instance a TupleSky checkout's target/release)."
    :default "../../tuplesky/target/release"
    :parse-fn #(.getCanonicalPath (io/file %))]

   [nil "--api-port PORT" "UDP port of each voter's API plane."
    :default 7001
    :parse-fn parse-long]

   [nil "--peer-port PORT" "UDP port of each voter's peer plane."
    :default 7002
    :parse-fn parse-long]

   [nil "--attempt-ms MS" "How long one shim attempt waits for an answer."
    :default 2000
    :parse-fn parse-long]

   [nil "--budget-ms MS" "How long one operation may take, attempts, reconnects and resolution included, before it is :info."
    :default 10000
    :parse-fn parse-long]

   [nil "--connect-ms MS" "How long opening a shim session may take."
    :default 10000
    :parse-fn parse-long]

   [nil "--nemesis FAULTS" "Comma-separated faults (kill, pause, partition, clock, packet), or none."
    :default []
    :parse-fn parse-nemesis-spec
    :validate [(partial every? nemeses) (cli/one-of nemeses)]]

   [nil "--nemesis-interval SECONDS" "Roughly how long between nemesis operations for each class of fault."
    :default 30
    :parse-fn read-string
    :validate [pos? "Must be positive"]]

   [nil "--kill-targets TARGETS" "Which voters a kill takes: any (one, a majority or all, at random) or leader (the voter leading the highest ballot, from the voters' logs)."
    :default :any
    :parse-fn keyword
    :validate [#{:any :leader} "Must be any or leader"]]

   [nil "--recovery-time SECONDS" "How long to wait after healing before the final reads."
    :default 60
    :parse-fn read-string
    :validate [(complement neg?) "Must not be negative"]]

   [nil "--key-count N" "Keys in play at once in the append and wr workloads (Elle's key-count): each key retires after its share of writes and a fresh one takes its place."
    :default 3
    :parse-fn parse-long
    :validate [pos? "Must be positive"]]

   [nil "--key-dist DIST" "How the append and wr workloads pick among the keys in play: exponential (Elle's default: each key twice as likely as the one before, so a few stay hot whatever the key count) or uniform."
    :default :exponential
    :parse-fn keyword
    :validate [#{:exponential :uniform} "Must be exponential or uniform"]]

   [nil "--read-ratio FRACTION" "The share of the wr workload's transactions made read-only; its other transactions mix reads and writes half each, so 0.6 makes about 80% of operations reads. 0 for Elle's mix."
    :default 0
    :parse-fn read-string
    :validate [#(and (number? %) (<= 0 % 1)) "Must be a number from 0 to 1"]]

   [nil "--hold-peer-links SECONDS" "Drop every voter's inbound peer-plane traffic for this long from the workload's start, while the clients run and the collectors still reach every voter: a leader's first proposals and the voters' votes wait (task-d67's start-up row). 0 for none."
    :default 0
    :parse-fn read-string
    :validate [#(and (number? %) (not (neg? %))) "Must be a number, 0 or more"]]

   [nil "--per-key-limit N" "Roughly how many operations each register gets, in the register workload."
    :default 100
    :parse-fn parse-long
    :validate [pos? "Must be positive"]]

   ["-r" "--rate HZ" "Approximate number of requests per second, or 0 for as many as the clients can issue (a throughput test)."
    :default 20
    :parse-fn read-string
    :validate [#(and (number? %) (not (neg? %))) "Must be a number, 0 or more"]]

   [nil "--wan PROFILE" "The network between the nodes: none, regions (three regions, 33 to 65 ms apart one way), or a one-way delay in milliseconds between every two nodes. See jepsen.tuplesky.wan."
    :default nil
    :parse-fn wan/parse-spec]

   [nil "--wan-clients WHERE" "Where the clients sit under --wan: first (beside the first node, as a control node on real hosts sits in one region; the default) or local (beside each node they talk to)."
    :default nil
    :parse-fn wan/parse-clients]

   [nil "--voter-workers N" "Tokio worker threads per voter (TOKIO_WORKER_THREADS); unset for tokio's default of one per host core. Voters that share a host share its cores."
    :default nil
    :parse-fn parse-long
    :validate [pos? "Must be positive"]]

   [nil "--voter-env NAME=VALUE" "An environment variable every voter starts with, such as COORDD_PEER_STREAM_FRAMES=1; repeat for more."
    :default {}
    :parse-fn (fn [s]
                (let [[k v] (str/split s #"=" 2)]
                  [k (or v "")]))
    :validate [(fn [[k _]] (re-matches #"[A-Za-z_][A-Za-z0-9_]*" k)) "Must be NAME=VALUE"]
    :assoc-fn (fn [m k [n v]] (assoc-in m [k n] v))]

   ["-w" "--workload NAME" "What workload to run."
    :default :append
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]])

(defn all-tests
  "Every workload against every fault combination, unless the CLI names
  one."
  [opts]
  (let [ws   (if-let [w (:workload opts)] [w] (keys workloads))
        ; An explicit `none` parses to [], which is a choice: only an
        ; absent option means every combination.
        nems (if (nil? (:nemesis opts)) all-nemeses [(:nemesis opts)])]
    (for [n nems, w ws, _ (range (:test-count opts))]
      (tuplesky-test (assoc opts :nemesis n :workload w)))))

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  tuplesky-test
                                         :opt-spec cli-opts})
                   ; Without these defaults, test-all can tell an
                   ; absent --workload or --nemesis from a chosen one.
                   (cli/test-all-cmd {:tests-fn all-tests
                                      :opt-spec (cli/without-defaults-for
                                                  [:workload :nemesis]
                                                  cli-opts)})
                   (cli/serve-cmd))
            args))
