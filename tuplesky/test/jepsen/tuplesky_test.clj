(ns jepsen.tuplesky-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.cli :as cli]
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
