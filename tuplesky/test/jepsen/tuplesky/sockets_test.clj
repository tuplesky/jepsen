(ns jepsen.tuplesky.sockets-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.tuplesky.sockets :as s]))

(deftest loop-command
  (is (= (str "nohup sh -c 'while :; do date -u +%FT%TZ; ss -tin"
              " \"( sport = :7070 or dport = :7070 )\"; sleep 10; done'"
              " >> /opt/x/sockets.log 2>&1 < /dev/null & echo $! > /opt/x/ss.pid")
         (s/loop-command {:log "/opt/x/sockets.log", :pidfile "/opt/x/ss.pid"
                          :flags "-tin", :ports [7070]})))
  (is (re-find #"\( sport = :7001 or dport = :7001 or sport = :7002 or dport = :7002 \)"
               (s/loop-command {:log "l", :pidfile "p", :flags "-uanm"
                                :ports [7001 7002], :interval 5}))))
