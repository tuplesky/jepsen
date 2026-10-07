(ns jepsen.tuplesky.db-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.tuplesky.db :as tdb]))

(deftest daemon-env-sets-only-what-the-test-asks
  (is (nil? (tdb/daemon-env {})))
  (is (nil? (tdb/daemon-env {:voter-workers nil})))
  (is (= {:TOKIO_WORKER_THREADS "1"} (tdb/daemon-env {:voter-workers 1})))
  (is (nil? (tdb/daemon-env {:voter-env {}})))
  (is (= {:COORDD_PEER_STREAM_FRAMES "1" :TOKIO_WORKER_THREADS "2"}
         (tdb/daemon-env {:voter-env {"COORDD_PEER_STREAM_FRAMES" "1"}
                          :voter-workers 2}))))
