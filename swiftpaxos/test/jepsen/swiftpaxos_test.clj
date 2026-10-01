(ns jepsen.swiftpaxos-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [jepsen.swiftpaxos :as s]))

(def base
  {:nodes            ["n1" "n2" "n3"]
   :workload         :register
   :rate             20
   :time-limit       60
   :nemesis-interval 30
   :per-key-limit    100})

(deftest tests
  (testing "a test is named for its workload and faults"
    (is (= "swiftpaxos register pause,partition"
           (:name (s/swiftpaxos-test (assoc base :nemesis [:pause :partition])))))
    (is (= "swiftpaxos register none"
           (:name (s/swiftpaxos-test (assoc base :nemesis [])))))
    (is (= "swiftpaxos register packet wan-regions"
           (:name (s/swiftpaxos-test (assoc base :nemesis [:packet]
                                                 :wan {:kind :regions}))))))
  (testing "a rate of 0 leaves the generator unthrottled"
    (let [g (repeat {:f :read})]
      (is (identical? g (s/throttle 0 g)))))
  (testing "none parses to no faults"
    (is (= [] (s/parse-nemesis-spec "none")))
    (is (= [:pause :partition] (s/parse-nemesis-spec "pause,partition")))))

(deftest crash-patterns
  (let [crash (re-pattern (str/join "|" s/crash-patterns))]
    (is (re-find crash "2026/09/30 19:39:38 Error: received unknown client message 6"))
    (is (re-find crash "panic: runtime error: invalid memory address"))
    (is (not (re-find crash "2026/09/30 19:39:37 Replica 2: done connecting to peers")))))
