(ns jepsen.swiftpaxos
  "Jepsen tests for SwiftPaxos (github.com/imdea-software/swiftpaxos), the
  reference implementation of the protocol TupleSky's consensus follows.

  It runs beside the TupleSky test as a baseline: the same cluster, fault
  schedule (jepsen.tuplesky.nemesis), rate and concurrency. SwiftPaxos's
  state machine is a map of registers with reads and writes, one key per
  command and no transactions, so its workload is linearizable reads and
  writes on independent registers, checked by Knossos.

  Every node runs one replica, and the master runs on the control node (see
  jepsen.swiftpaxos.db). Every client drives the cluster through the
  `swiftpaxos-jepsen` shim (see jepsen.swiftpaxos.client)."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [independent :as independent]
                    [random :as rand]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.os.debian :as debian]
            [jepsen.swiftpaxos [client :as client]
                               [db :as db]]
            [jepsen.tuplesky [nemesis :as tn]
                             [wan :as wan]]
            [knossos.model :as model]))

(defn r [_ _] {:type :invoke, :f :read})
(defn w [_ _] {:type :invoke, :f :write, :value (rand/long 5)})

(defn register-workload
  "Linearizable reads and writes on independent registers, as
  jepsen.tests.linearizable-register without compare-and-set, which
  SwiftPaxos does not have."
  [opts]
  (let [n (count (:nodes opts))]
    {:client    (client/client)
     :checker   (independent/checker
                  (checker/compose
                    {:linearizable (checker/linearizable
                                     {:model (model/register)})
                     :timeline     (timeline/html)}))
     :generator (independent/concurrent-generator
                  (* 2 n)
                  (range)
                  (fn [k]
                    (->> (gen/reserve n r w)
                         ; Randomized, so keys fall out of step over time.
                         (gen/limit (* (+ (rand/double 0.1) 0.9)
                                       (:per-key-limit opts)))
                         (gen/process-limit 20))))}))

(def workloads
  {:register register-workload})

(def nemeses
  "Faults we know how to inject. Not clock: the Docker cluster shares the
  control node's clock. :packet disrupts packets on top of the --wan
  profile (see jepsen.tuplesky.wan)."
  #{:kill :pause :partition :packet})

(defn parse-nemesis-spec
  "Parses a comma-separated list of faults; `none` is no faults."
  [spec]
  (if (= "none" spec)
    []
    (mapv keyword (str/split spec #","))))

(def crash-patterns
  "Log lines of a replica that stopped: Go's panics, and the messages of the
  upstream replica's fatal exits."
  ["panic:"
   "fatal error:"
   "Error: received unknown"
   "Got proposal for the delivered command"
   "the number of hashes does not match"
   "Don't know what to do"
   "listen error"])

(defn throttle
  "Staggers a generator to about rate operations a second, or leaves it
  unthrottled at 0, as jepsen.tuplesky/throttle."
  [rate gen]
  (if (pos? rate)
    (gen/stagger (/ rate) gen)
    gen))

(defn test-name
  "The test's name, as jepsen.tuplesky/test-name: workload, faults, and the
  WAN profile when there is one."
  [opts]
  (str "swiftpaxos " (name (:workload opts)) " "
       (if (seq (:nemesis opts))
         (str/join "," (map name (:nemesis opts)))
         "none")
       (when-let [w (:wan opts)]
         (str " wan-" (if (= :uniform (:kind w))
                        (str (:delay-ms w) "ms")
                        (name (:kind w)))
              (when (= :local (:clients w)) "-local-clients")))))

(defn swiftpaxos-test
  "Constructs a test from parsed CLI options."
  [opts]
  (let [opts          (update opts :wan wan/with-clients (:wan-clients opts))
        workload-name (:workload opts)
        workload      ((workloads workload-name) opts)
        db            (db/db client/warm-up!)
        nemesis       (tn/nemesis-package
                        {:db        db
                         :nodes     (:nodes opts)
                         :faults    (:nemesis opts)
                         :partition {:targets [:one :majority :majorities-ring]}
                         :pause     {:targets [:one :majority]}
                         :kill      {:targets [:one :majority :all]}
                         :wan       (:wan opts)
                         :interval  (:nemesis-interval opts)})
        gen           (->> (:generator workload)
                           (throttle (:rate opts))
                           (gen/nemesis (gen/phases (gen/sleep 5)
                                                    (:generator nemesis)))
                           (gen/time-limit (:time-limit opts)))]
    (merge tests/noop-test
           opts
           {:name            (test-name opts)
            :pure-generators true
            :os              debian/os
            :db              db
            :client          (:client workload)
            :nemesis         (:nemesis nemesis)
            :generator       (gen/phases
                               gen
                               (gen/log "Healing cluster")
                               (gen/nemesis (:final-generator nemesis)))
            :checker         (checker/compose
                               {:perf       (checker/perf
                                              {:nemeses (:perf nemesis)})
                                :stats      (checker/stats)
                                :exceptions (checker/unhandled-exceptions)
                                ; A replica that panics, or exits on one of
                                ; its fatal errors (upstream's Fatal calls),
                                ; stops serving; that is a finding even when
                                ; the history is clean.
                                :crash      (checker/log-file-pattern
                                              (re-pattern
                                                (str/join "|" crash-patterns))
                                              "replica.log")
                                :workload   (:checker workload)})})))

(def cli-opts
  "Command line options."
  [[nil "--bin-dir DIR" "Directory holding swiftpaxos and swiftpaxos-jepsen built for the nodes' and this machine's platform (see build.sh)."
    :default "bin"
    :parse-fn #(.getCanonicalPath (io/file %))]

   [nil "--master-host HOST" "Where the replicas reach the master on this machine. Default: the address this machine reaches the first node from."
    :default nil]

   [nil "--master-port PORT" "The master's port on this machine."
    :default 7087
    :parse-fn parse-long]

   [nil "--timeout-ms MS" "How long one operation may take before it is :info (a write) or :fail (a read)."
    :default 5000
    :parse-fn parse-long]

   [nil "--shim-logs" "Keep each shim session's client log in the store (control/shim-*.log). Off by default: a run starts a few hundred sessions."]

   [nil "--connect-ms MS" "How long connecting a shim may take."
    :default 30000
    :parse-fn parse-long]

   [nil "--nemesis FAULTS" "Comma-separated faults (kill, pause, partition, packet), or none. A killed replica does not rejoin (see jepsen.swiftpaxos.db)."
    :default []
    :parse-fn parse-nemesis-spec
    :validate [(partial every? nemeses) (cli/one-of nemeses)]]

   [nil "--nemesis-interval SECONDS" "Roughly how long between nemesis operations for each class of fault."
    :default 30
    :parse-fn read-string
    :validate [pos? "Must be positive"]]

   [nil "--per-key-limit N" "Roughly how many operations each register gets."
    :default 100
    :parse-fn parse-long
    :validate [pos? "Must be positive"]]

   ["-r" "--rate HZ" "Approximate number of requests per second, or 0 for as many as the clients can issue (a throughput test)."
    :default 20
    :parse-fn read-string
    :validate [#(and (number? %) (not (neg? %))) "Must be a number, 0 or more"]]

   [nil "--wan PROFILE" "The network between the replicas: none, regions (three regions, 33 to 65 ms apart one way), or a one-way delay in milliseconds between every two replicas. The master, on this machine, is not shaped. See jepsen.tuplesky.wan."
    :default nil
    :parse-fn wan/parse-spec]

   [nil "--wan-clients WHERE" "Where the clients sit under --wan: first (beside the first node, as a control node on real hosts sits in one region; the default) or local (beside each node they talk to)."
    :default nil
    :parse-fn wan/parse-clients]

   ["-w" "--workload NAME" "What workload to run."
    :default :register
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]])

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  swiftpaxos-test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
