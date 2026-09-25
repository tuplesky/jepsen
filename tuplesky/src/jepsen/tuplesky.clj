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
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.nemesis.combined :as nc]
            [jepsen.os.debian :as debian]
            [jepsen.tests.cycle [append :as append]
                                [wr :as wr]]
            [jepsen.tests.linearizable-register :as lr]
            [jepsen.tuplesky [client :as client]
                             [db :as db]]
            [knossos.model :as model]))

(defn append-workload
  "Elle list-append: transactions of appends and reads over a few lists,
  checked for strict serializability."
  [opts]
  (assoc (append/test {:key-count          3
                       :max-txn-length     4
                       :consistency-models [:strict-serializable]})
         :client (client/client)))

(defn wr-workload
  "Elle rw-register: transactions of writes and reads over a few registers,
  checked for strict serializability."
  [opts]
  (assoc (wr/test {:key-count          3
                   :max-txn-length     4
                   :consistency-models [:strict-serializable]})
         :client (client/client)))

(defn register-workload
  "Linearizable reads, writes and compare-and-set on independent
  registers, checked by Knossos."
  [opts]
  (assoc (lr/test {:nodes (:nodes opts)
                   :model (model/cas-register)})
         :client (client/client)))

(def workloads
  {:append   append-workload
   :wr       wr-workload
   :register register-workload})

(def nemeses
  "Faults we know how to inject."
  #{:kill :pause :partition :clock})

(def all-nemeses
  "Combinations of faults test-all runs."
  [[]
   [:kill]
   [:pause]
   [:partition]
   [:clock]
   [:kill :partition]
   [:pause :kill :partition :clock]])

(defn parse-nemesis-spec
  "Parses a comma-separated list of faults; `none` is no faults."
  [spec]
  (if (= "none" spec)
    []
    (mapv keyword (str/split spec #","))))

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
  (let [workload-name (:workload opts)
        workload      ((workloads workload-name) opts)
        db            (db/db)
        nemesis       (nc/nemesis-package
                        {:db        db
                         :nodes     (:nodes opts)
                         :faults    (:nemesis opts)
                         :partition {:targets [:one :majority :majorities-ring]}
                         :pause     {:targets [:one :majority]}
                         :kill      {:targets [:one :majority :all]}
                         :interval  (:nemesis-interval opts)})
        wrap          (:wrap-generator workload identity)
        gen           (->> (:generator workload)
                           (gen/stagger (/ (:rate opts)))
                           (gen/nemesis (gen/phases (gen/sleep 5)
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
           {:name            (str "tuplesky " (name workload-name) " "
                                  (if (seq (:nemesis opts))
                                    (str/join "," (map name (:nemesis opts)))
                                    "none"))
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

   [nil "--nemesis FAULTS" "Comma-separated faults (kill, pause, partition, clock), or none."
    :default []
    :parse-fn parse-nemesis-spec
    :validate [(partial every? nemeses) (cli/one-of nemeses)]]

   [nil "--nemesis-interval SECONDS" "Roughly how long between nemesis operations for each class of fault."
    :default 30
    :parse-fn read-string
    :validate [pos? "Must be positive"]]

   [nil "--recovery-time SECONDS" "How long to wait after healing before the final reads."
    :default 60
    :parse-fn read-string
    :validate [(complement neg?) "Must not be negative"]]

   ["-r" "--rate HZ" "Approximate number of requests per second."
    :default 20
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]

   ["-w" "--workload NAME" "What workload to run."
    :default :append
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]])

(defn all-tests
  "Every workload against every fault combination, unless the CLI names
  one."
  [opts]
  (let [ws (if-let [w (:workload opts)] [w] (keys workloads))
        nems (if (seq (:nemesis opts)) [(:nemesis opts)] all-nemeses)]
    (for [n nems, w ws, _ (range (:test-count opts))]
      (tuplesky-test (assoc opts :nemesis n :workload w)))))

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  tuplesky-test
                                         :opt-spec cli-opts})
                   (cli/test-all-cmd {:tests-fn all-tests
                                      :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
