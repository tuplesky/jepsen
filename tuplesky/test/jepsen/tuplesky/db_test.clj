(ns jepsen.tuplesky.db-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [jepsen.tuplesky.db :as tdb]))

(deftest daemon-env-sets-only-what-the-test-asks
  (is (nil? (tdb/daemon-env {})))
  (is (nil? (tdb/daemon-env {:voter-workers nil})))
  (is (= {:TOKIO_WORKER_THREADS "1"} (tdb/daemon-env {:voter-workers 1})))
  (is (nil? (tdb/daemon-env {:voter-env {}})))
  (is (= {:COORDD_PEER_STREAM_FRAMES "1" :TOKIO_WORKER_THREADS "2"}
         (tdb/daemon-env {:voter-env {"COORDD_PEER_STREAM_FRAMES" "1"}
                          :voter-workers 2}))))

(def leader-status
  (str "epoch 3 leader ab12 (this node)\n"
       "active early-proposal,release-at-execution\n"
       "activating -\n"
       "voter ab12 advertised early-proposal,release-at-execution prepared - held early-proposal,release-at-execution\n"
       "voter cd34 advertised early-proposal,release-at-execution prepared early-proposal held early-proposal,release-at-execution\n"))

(deftest active-set-reads-the-active-line
  (is (= #{"early-proposal" "release-at-execution"} (tdb/active-set leader-status)))
  (is (= #{} (tdb/active-set "epoch 1 leader - (not this node: its view is not the domain's)\nactive -\n")))
  (is (= #{} (tdb/active-set nil))))

(deftest status-holds-only-when-every-voter-holds-at-the-leader
  (let [both ["early-proposal" "release-at-execution"]]
    (is (tdb/status-holds? both leader-status))
    ; One voter the leader has not heard hold the second yet.
    (is (not (tdb/status-holds?
               both (str/replace leader-status
                                 #"held early-proposal,release-at-execution\n$"
                                 "held early-proposal\n"))))
    (is (not (tdb/status-holds? both (str/replace leader-status #"held [^\n]*\n$" "held ?\n"))))
    ; A follower's own view: its active set is what it holds.
    (is (tdb/status-holds?
          both (str "epoch 3 leader ab12 (not this node: its view is not the domain's)\n"
                    "active early-proposal,release-at-execution\nactivating -\n")))
    (is (not (tdb/status-holds?
               both (str "epoch 3 leader ab12 (not this node: its view is not the domain's)\n"
                         "active early-proposal\nactivating -\n"))))
    (is (not (tdb/status-holds? both "refused: this node does not lead")))))
