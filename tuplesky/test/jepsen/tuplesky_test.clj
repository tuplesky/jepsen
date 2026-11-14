(ns jepsen.tuplesky-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.cli :as cli]
            [jepsen.generator :as gen]
            [jepsen.generator.context :as ctx]
            [jepsen.generator.test :as gen.test]
            [jepsen.independent :as independent]
            [jepsen.tuplesky :as t]))

(def base
  {:nodes            ["n1" "n2" "n3"]
   :rate             20
   :time-limit       60
   :nemesis-interval 30
   :recovery-time    60
   :test-count       1})

(deftest test-all-selection
  (testing "no --workload and no --nemesis: every workload, every fault set"
    (is (= (* (count t/workloads) (count t/all-nemeses))
           (count (t/all-tests base)))))
  (testing "an explicit --nemesis none is one fault-free run per workload"
    (let [tests (t/all-tests (assoc base :nemesis []))]
      (is (= (count t/workloads) (count tests)))
      (is (every? #(re-find #" none$" (:name %)) tests))))
  (testing "a chosen workload and fault set is one run"
    (is (= 1 (count (t/all-tests (assoc base :workload :append
                                              :nemesis [:kill])))))))

(deftest names-and-rates
  (testing "a test is named for its workload, faults and WAN profile"
    (is (= "tuplesky append kill,packet"
           (:name (t/tuplesky-test (assoc base :workload :append
                                               :nemesis [:kill :packet])))))
    (is (= "tuplesky register none wan-regions"
           (:name (t/tuplesky-test (assoc base :workload :register
                                               :nemesis []
                                               :wan {:kind :regions})))))
    (is (= "tuplesky append none wan-regions-local-clients"
           (:name (t/tuplesky-test (assoc base :workload :append
                                               :nemesis []
                                               :wan {:kind :regions :clients :first}
                                               :wan-clients :local)))))
    (is (= "tuplesky append none wan-50.0ms"
           (:name (t/tuplesky-test (assoc base :workload :append
                                               :nemesis []
                                               :wan {:kind :uniform
                                                     :delay-ms 50.0}))))))
  (testing "a rate of 0 leaves the generator unthrottled"
    (let [g (repeat {:f :read})]
      (is (identical? g (t/throttle 0 g)))
      (is (not (identical? g (t/throttle 20 g))))))
  (testing "--rate takes 0, --wan parses its profile"
    (let [parse #(cli/parse-opts % t/cli-opts)]
      (is (= 0 (:rate (:options (parse ["--rate" "0"])))))
      (is (seq (:errors (parse ["--rate" "-1"]))))
      (is (= {:kind :regions :clients :first}
             (:wan (:options (parse ["--wan" "regions"])))))
      (is (= {:kind :uniform :delay-ms 40.0 :clients :first}
             (:wan (:options (parse ["--wan" "40"])))))
      (is (= :local (:wan-clients (:options (parse ["--wan-clients" "local"])))))
      (is (nil? (:wan (:options (parse [])))))
      (is (seq (:errors (parse ["--wan" "mars"])))))))

(deftest read-ratio
  (testing "a read-only transaction reads what the transaction would have written"
    (is (= [[:r 1 nil] [:r 2 nil] [:r 1 nil]]
           (t/read-only [[:w 1 3] [:r 2 nil] [:r 1 nil]]))))
  (testing "a ratio of 0 leaves the workload as it is"
    (let [w {:generator :g}]
      (is (identical? w (t/with-read-ratio 0 w)))))
  (testing "the option parses, and refuses a ratio past 1"
    (is (= 0.6 (get-in (cli/parse-opts ["--read-ratio" "0.6"] t/cli-opts)
                       [:options :read-ratio])))
    (is (seq (:errors (cli/parse-opts ["--read-ratio" "1.5"] t/cli-opts))))))

(def ^:private five-nodes
  {:nodes ["n1" "n2" "n3" "n4" "n5"] :per-key-limit 100})

(def ^:private etcd-register-all-quirks t/etcd-register-quirks)

(defn- etcd-mix-invokes
  "The invocations of 3000 operations of the etcd mix with these quirks
  (nil for the default), on 30 threads that never crash."
  [quirks]
  (->> (t/etcd-register-generator
         (cond-> five-nodes quirks (assoc :etcd-register-quirks quirks)))
       (gen/limit 3000)
       (gen.test/perfect (gen.test/n+nemesis-context 30))
       (filter #(= :invoke (:type %)))))

(deftest etcd-register-mix
  (testing "the default, on-threads: the first n threads read, and only they"
    (let [ops    (etcd-mix-invokes nil)
          reads  (filter #(= :read (:f %)) ops)
          others (remove #(= :read (:f %)) ops)]
      (is (= #{0 1 2 3 4} (set (map :process reads))))
      (is (= #{:write :cas} (set (map :f others))))
      (is (every? #(<= 5 % 29) (map :process others)))
      (is (< 0.12 (/ (count reads) (count ops)) 0.22))))
  (testing "etcd's literal expression: here, n readers in each key's group of 2n"
    (let [ops    (etcd-mix-invokes #{})
          reads  (filter #(= :read (:f %)) ops)
          others (remove #(= :read (:f %)) ops)]
      (is (= (set (concat (range 0 5) (range 10 15) (range 20 25)))
             (set (map :process reads))))
      (is (= #{:write :cas} (set (map :f others))))))
  (testing "the default keeps every key group working to the end"
    (let [late (drop 2000 (etcd-mix-invokes nil))]
      (is (= #{0 1 2} (set (map #(quot (:process %) 10) late))))))
  (testing "all four quirks under the perfect scheduler (printed, not checked: it can't lose the race)"
    (let [late (drop 2000 (etcd-mix-invokes etcd-register-all-quirks))]
      (println "etcd mix, all four quirks, perfect scheduler: key groups invoking after op 2000:"
               (sort (set (map #(quot (:process %) 10) late)))))))

(defn- first-op-after-a-whole-test-update
  "What one fresh key's generator of the etcd mix with these quirks yields
  to the key group of threads 20-29 out of 30, when, before it is asked
  for an operation, it is updated with a completion from thread 25 in the
  whole test's context: what the concurrent generator does when a key
  change loses the tie for the next operation (#98)."
  [quirks]
  (let [test  {}
        all   (gen.test/n+nemesis-context 30)
        group ((ctx/make-thread-filter (set (range 20 30))) all)
        gen   (t/etcd-register-key-generator five-nodes quirks)
        gen   (gen/update gen test all {:type :ok, :process 25, :f :write,
                                        :value 1, :time 0})]
    (first (gen/op gen test group))))

(deftest etcd-register-key-after-a-whole-test-update
  (testing "the default still gives the key's own group an operation"
    (let [op (first-op-after-a-whole-test-update t/etcd-register-default-quirks)]
      (is (map? op))
      (is (<= 20 (:process op) 29))
      (is (#{:write :cas} (:f op)))))
  (testing "so does on-threads with jitter"
    (is (map? (first-op-after-a-whole-test-update #{:on-threads :jitter})))))

(defn- recording
  "A generator of writes that keeps, in the atom seen, the threads of the
  context each update hands it."
  [seen]
  (reify gen/Generator
    (op [this test ctx]
      (let [[op _] (gen/op {:f :write, :value 1} test ctx)]
        [op this]))
    (update [this test ctx event]
      (swap! seen conj (set (gen/all-threads ctx)))
      this)))

(deftest concurrent-generator-updates-in-the-group-context
  (testing "a completion reaches its key's generator with its group's threads alone, as an op does"
    (let [seen  (atom [])
          all   (gen.test/n+nemesis-context 30)
          g     (independent/concurrent-generator 10 (range) (fn [_k] (recording seen)))
          [_ g] (gen/op g {} all)]
      (gen/update g {} all {:type :ok, :process 25, :f :write, :time 0,
                            :value (independent/tuple 2 1)})
      (is (= [(set (range 20 30))] @seen))))
  (testing "whole-test-update hands it every thread's context, as the concurrent generator did before"
    (let [seen  (atom [])
          all   (gen.test/n+nemesis-context 30)
          g     (t/whole-test-concurrent-generator 10 (range) (fn [_k] (recording seen)))
          [op g] (gen/op g {} all)]
      (is (map? op))
      (gen/update g {} all {:type :ok, :process 25, :f :write, :time 0,
                            :value (independent/tuple 2 1)})
      (is (= [(set (range 30))] @seen))))
  (testing "and so process-limit's first 20 of 120 threads (0-4, 32-35, 38, 64, 68-71, 98, 100-103) leave the group of threads 10-19 nothing"
    (let [all    (gen.test/n+nemesis-context 120)
          group  ((ctx/make-thread-filter (set (range 10 20))) all)
          old    (t/etcd-register-key-generator five-nodes etcd-register-all-quirks)
          starve (gen/update old {} all {:type :ok, :process 15, :f :write,
                                         :value 1, :time 0})]
      (is (not (map? (first (gen/op starve {} group))))))))

(deftest etcd-register-quirks-option
  (let [parse #(cli/parse-opts % t/cli-opts)]
    (is (= #{:on-threads} (:etcd-register-quirks (:options (parse [])))))
    (is (= #{} (:etcd-register-quirks (:options (parse ["--etcd-register-quirks" "none"])))))
    (is (= #{:on-threads :jitter}
           (:etcd-register-quirks (:options (parse ["--etcd-register-quirks" "on-threads,jitter"])))))
    (is (= t/etcd-register-quirks
           (:etcd-register-quirks (:options (parse ["--etcd-register-quirks" "on-threads,process-limit,jitter,whole-test-update"])))))
    (is (seq (:errors (parse ["--etcd-register-quirks" "on-threads,other"]))))))

(deftest swiftpaxos-register-mix
  (let [ops    (->> (t/swiftpaxos-register-generator
                      {:nodes ["n1" "n2" "n3" "n4" "n5"] :per-key-limit 100})
                    (gen/limit 3000)
                    (gen.test/perfect (gen.test/n+nemesis-context 30))
                    (filter #(= :invoke (:type %))))
        kinds  (set (map :f ops))]
    (testing "reads and writes alone, as the reference's test draws them"
      (is (= #{:read :write} kinds)))
    (testing "both kinds are drawn"
      (is (some #(= :read (:f %)) ops))
      (is (some #(= :write (:f %)) ops)))))

(deftest register-mix-option
  (testing "the register workload takes the jepsen mix unless told etcd or swiftpaxos"
    (let [parse #(:options (cli/parse-opts % t/cli-opts))]
      (is (= :jepsen (:register-mix (parse []))))
      (is (= :etcd (:register-mix (parse ["--register-mix" "etcd"]))))
      (is (= :swiftpaxos (:register-mix (parse ["--register-mix" "swiftpaxos"]))))
      (is (seq (:errors (cli/parse-opts ["--register-mix" "other"] t/cli-opts)))))))
