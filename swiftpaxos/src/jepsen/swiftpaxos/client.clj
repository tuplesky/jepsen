(ns jepsen.swiftpaxos.client
  "A Jepsen client that drives SwiftPaxos through `swiftpaxos-jepsen`, a
  client shim in the tuplesky/swiftpaxos fork (cmd/swiftpaxos-jepsen), built by
  build.sh in this project.

  Each Jepsen process runs one shim on the control node, a session of the
  upstream SwiftPaxos client with the node's replica as its closest. It
  speaks JSON lines, as TupleSky's coord-jepsen does: one request line in,
  one answer line out, the answer already `ok`, `fail` (a read that timed
  out) or `info` (a write that timed out). A shim ends its session after
  any operation that timed out, so every answer that is not `ok` ends the
  Jepsen process too, and the next operation opens a fresh shim."
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]
                    [independent :as independent]]
            [jepsen.swiftpaxos.db :as db]
            [slingshot.slingshot :refer [throw+]])
  (:import (java.io BufferedReader BufferedWriter)
           (java.lang ProcessBuilder ProcessBuilder$Redirect)
           (java.util.concurrent TimeUnit)))

(def instances
  "Distinguishes every shim this JVM starts, for their logs."
  (atom 0))

(defn shim-command
  "The command line that starts a shim closest to `node`'s replica."
  [test node log]
  [(str (:bin-dir test) "/swiftpaxos-jepsen")
   "-server"      (str node ":" db/replica-port)
   "-master"      (or (:master-host test)
                      (db/route-source (first (:nodes test))))
   "-master-port" (str (:master-port test))
   "-replicas"    (str (count (:nodes test)))
   "-timeout-ms"  (str (:timeout-ms test))
   "-connect-ms"  (str (:connect-ms test))
   "-log"         (str log)])

(defn read-line-within
  "Reads one line from `reader` within `ms` milliseconds; nil on timeout or
  end of stream."
  [^BufferedReader reader ms]
  (let [f (future (.readLine reader))]
    (deref f ms nil)))

(defn start-shim!
  "Starts a shim for `node` and waits for its ready line. Throws if it does
  not connect."
  [test node]
  (let [instance (swap! instances inc)
        base     (io/file (db/control-dir test) (str "shim-" node "-" instance))
        pb       (doto (ProcessBuilder. ^java.util.List
                                        (shim-command test node (str base ".log")))
                   (.redirectError (ProcessBuilder$Redirect/appendTo
                                     (io/file (str base ".err")))))
        proc     (.start pb)
        in       (io/reader (.getInputStream proc))
        out      (io/writer (.getOutputStream proc))
        line     (read-line-within in (+ (:connect-ms test) 5000))
        ready    (some-> line (json/parse-string true))]
    (if (:ready ready)
      {:process proc, :in in, :out out, :node node, :instance instance}
      (do (.destroyForcibly proc)
          (throw+ {:type  ::shim-not-ready
                   :node  node
                   :error (or (:error ready) line "no ready line")})))))

(defn stop-shim!
  "Closes a shim's input, which ends it, and makes sure it is gone."
  [{:keys [^Process process ^BufferedWriter out]}]
  (when process
    (try (.close out) (catch Exception _))
    (when-not (.waitFor process 2 TimeUnit/SECONDS)
      (.destroyForcibly process))))

(defn exchange!
  "Sends one request to a shim and returns its answer, or nil when none
  came within the shim's own timeout and some slack."
  [test {:keys [^BufferedWriter out ^BufferedReader in]} req]
  (.write out ^String (json/generate-string req))
  (.newLine out)
  (.flush out)
  (some-> (read-line-within in (+ (:timeout-ms test) 10000))
          (json/parse-string true)))

(defn request
  "The shim request line for an invocation."
  [op]
  (let [[k v] (:value op)]
    (case (:f op)
      :read  {:f "read", :key k}
      :write {:f "write", :key k, :value v})))

(defn completion
  "Applies a shim answer (nil: none came) to an invocation."
  [op answer]
  (let [k (first (:value op))]
    (case (some-> answer :type keyword)
      :ok   (assoc op :type :ok
                   :value (independent/tuple k (:value answer)))
      :fail (assoc op :type :fail, :error (:error answer), :end-process? true)
      :info (assoc op :type :info, :error (:error answer))
      (assoc op
             :type         (if (= :read (:f op)) :fail :info)
             :error        :shim-unresponsive
             :end-process? true))))

(defrecord Client [shim]
  client/Client
  (open! [this test node]
    (assoc this :shim (start-shim! test node)))

  (setup! [this test])

  (invoke! [this test op]
    (let [answer (exchange! test shim (request op))]
      (when-not answer (stop-shim! shim))
      (completion op answer)))

  (teardown! [this test])

  (close! [this test]
    (stop-shim! shim)))

(defn client
  "A fresh client."
  []
  (Client. nil))

(def warm-up-key
  "A key no workload uses."
  -1)

(defn warm-up!
  "Puts one write through the cluster from the first node, retrying for up
  to two minutes; throws if none is ok."
  [test]
  (let [deadline (+ (System/currentTimeMillis) 120000)
        test     (assoc test :timeout-ms (max (:timeout-ms test) 30000))]
    (loop [attempt 1]
      (let [answer (try
                     (let [shim (start-shim! test (first (:nodes test)))]
                       (try (exchange! test shim {:f "write", :key warm-up-key
                                                  :value attempt})
                            (finally (stop-shim! shim))))
                     (catch Exception e
                       (warn e "warm-up attempt" attempt "could not run")
                       nil))]
        (cond
          (= "ok" (:type answer))
          (info "Warm-up write ok after" attempt "attempt(s)")

          (< (System/currentTimeMillis) deadline)
          (do (info "Warm-up write not ok:" answer)
              (recur (inc attempt)))

          :else
          (throw+ {:type ::warm-up-failed, :answer answer}))))))
