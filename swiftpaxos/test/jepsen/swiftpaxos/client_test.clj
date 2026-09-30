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

(deftest shim-commands
  (let [test {:bin-dir "/b", :nodes ["n1" "n2"], :master-host "10.0.0.1"
              :master-port 7087, :timeout-ms 5000, :connect-ms 30000}]
    (testing "the bare host, and no client log unless one is given"
      (is (= ["/b/swiftpaxos-jepsen" "-server" "n2" "-master" "10.0.0.1"
              "-master-port" "7087" "-replicas" "2" "-timeout-ms" "5000"
              "-connect-ms" "30000"]
             (c/shim-command test "n2" nil))))
    (testing "a client log when --shim-logs asks for one"
      (is (= ["-log" "/s/shim-n2-1.log"]
             (take-last 2 (c/shim-command test "n2" "/s/shim-n2-1.log")))))))
