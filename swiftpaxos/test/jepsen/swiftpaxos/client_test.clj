(ns jepsen.swiftpaxos.client-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.independent :as independent]
            [jepsen.swiftpaxos.client :as c]))

(deftest requests
  (is (= {:f "read", :key 4}
         (c/request {:f :read, :value (independent/tuple 4 nil)})))
  (is (= {:f "write", :key 4, :value 2}
         (c/request {:f :write, :value (independent/tuple 4 2)}))))

(deftest completions
  (testing "a read keeps its key"
    (is (= (independent/tuple 4 2)
           (:value (c/completion {:f :read, :value (independent/tuple 4 nil)}
                                 {:type "ok", :value 2}))))
    (is (= (independent/tuple 4 nil)
           (:value (c/completion {:f :read, :value (independent/tuple 4 nil)}
                                 {:type "ok", :value nil})))))
  (testing "a fail ends the process: the shim ended its session"
    (is (= {:f :read, :type :fail, :error "timeout", :end-process? true
            :value (independent/tuple 4 nil)}
           (c/completion {:f :read, :value (independent/tuple 4 nil)}
                         {:type "fail", :error "timeout"}))))
  (testing "info carries the shim's reason"
    (is (= {:f :write, :type :info, :error "timeout"
            :value (independent/tuple 4 1)}
           (c/completion {:f :write, :value (independent/tuple 4 1)}
                         {:type "info", :error "timeout"}))))
  (testing "no answer: a read had no effect, a write may have"
    (is (= [:fail true]
           ((juxt :type :end-process?)
            (c/completion {:f :read, :value (independent/tuple 4 nil)} nil))))
    (is (= [:info true]
           ((juxt :type :end-process?)
            (c/completion {:f :write, :value (independent/tuple 4 1)} nil))))))
