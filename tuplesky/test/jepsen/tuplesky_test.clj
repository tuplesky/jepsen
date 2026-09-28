(ns jepsen.tuplesky-test
  (:require [clojure.test :refer [deftest is testing]]
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
