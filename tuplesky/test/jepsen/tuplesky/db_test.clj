(ns jepsen.tuplesky.db-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.tuplesky.db :as tdb]))

(deftest daemon-env-sets-tokio-workers-only-when-asked
  (is (nil? (tdb/daemon-env {})))
  (is (nil? (tdb/daemon-env {:voter-workers nil})))
  (is (= {:TOKIO_WORKER_THREADS "1"} (tdb/daemon-env {:voter-workers 1}))))
