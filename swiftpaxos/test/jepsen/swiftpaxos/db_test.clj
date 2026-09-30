(ns jepsen.swiftpaxos.db-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [jepsen.swiftpaxos.db :as db]))

(deftest config
  (is (= ["-- Replicas --"
          "n1 n1"
          "n2 n2"
          "n3 n3"
          ""
          "-- Master --"
          "master 10.0.0.1"
          ""
          "masterPort: 7087"
          "protocol: swiftpaxos"
          "noop: false"
          "thrifty: false"
          "optread: false"
          "leaderless: false"
          "fast: true"
          ""
          "-- Proxy --"
          "server_alias n1"
          "server_alias n2"
          "server_alias n3"
          "---"]
         (str/split-lines
           (db/config {:nodes ["n1" "n2" "n3"], :master-port 7087}
                      "10.0.0.1")))))
