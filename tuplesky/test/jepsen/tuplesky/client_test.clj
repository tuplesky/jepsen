(ns jepsen.tuplesky.client-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.independent :as independent]
            [jepsen.tuplesky.client :as c]))

(deftest requests
  (is (= {:f "txn", :value [["r" 1 nil] ["append" 1 3]]}
         (c/request {:f :txn, :value [[:r 1 nil] [:append 1 3]]})))
  (is (= {:f "read", :key 4}
         (c/request {:f :read, :value (independent/tuple 4 nil)})))
  (is (= {:f "write", :key 4, :value 2}
         (c/request {:f :write, :value (independent/tuple 4 2)})))
  (is (= {:f "cas", :key 4, :value [1 2]}
         (c/request {:f :cas, :value (independent/tuple 4 [1 2])}))))

(deftest completions
  (testing "a transaction's reads come back as Elle reads them"
    (is (= {:f :txn, :type :ok, :value [[:r 1 [3]] [:append 1 4]]}
           (c/completion {:f :txn, :value [[:r 1 nil] [:append 1 4]]}
                         {:type "ok", :value [["r" 1 [3]] ["append" 1 4]]}))))
  (testing "a register read keeps its key"
    (is (= (independent/tuple 4 2)
           (:value (c/completion {:f :read, :value (independent/tuple 4 nil)}
                                 {:type "ok", :value 2})))))
  (testing "fail and info carry the shim's reason"
    (is (= {:f :cas, :type :fail, :error "guard-failed"
            :value (independent/tuple 4 [1 2])}
           (c/completion {:f :cas, :value (independent/tuple 4 [1 2])}
                         {:type "fail", :error "guard-failed"})))
    (is (= :info (:type (c/completion {:f :write
                                       :value (independent/tuple 4 1)}
                                      {:type "info", :error "timeout"}))))))

(deftest read-only
  (is (c/read-only? {:f :read}))
  (is (c/read-only? {:f :txn, :value [[:r 1 nil] [:r 2 nil]]}))
  (is (not (c/read-only? {:f :txn, :value [[:r 1 nil] [:append 2 1]]})))
  (is (not (c/read-only? {:f :cas}))))
