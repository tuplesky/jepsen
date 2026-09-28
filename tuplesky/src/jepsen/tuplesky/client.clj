(ns jepsen.tuplesky.client
  "A Jepsen client that drives TupleSky through `coord-jepsen`, the native
  client shim in the TupleSky repository.

  Each Jepsen process runs one shim process on the control node. The shim
  binds one session on the frontend of the node's voter and speaks JSON
  lines: one request line in, one answer line out. It decides what an
  answer means before we see it -- `ok`, `fail` (the operation had no
  effect) or `info` (it may have) -- after resolving an answer that did not
  arrive by asking the domain about the request's identity. So this client
  only translates.

  After an `:info`, Jepsen gives the worker a new process and this client is
  reopened, which starts a fresh shim and a fresh session."
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]
                    [independent :as independent]]
            [slingshot.slingshot :refer [throw+]])
  (:import (java.io BufferedReader BufferedWriter File)
           (java.lang ProcessBuilder ProcessBuilder$Redirect)
           (java.util.concurrent TimeUnit)))

(def instances
  "Distinguishes every shim this JVM starts: each gets its own client
  certificate and client instance."
  (atom 0))

(defn voter
  "The one-based voter number of a node: its position in the test's nodes,
  which is the order the domain was provisioned in."
  [test node]
  (let [i (.indexOf ^java.util.List (vec (:nodes test)) node)]
    (when (neg? i)
      (throw+ {:type ::unknown-node, :node node}))
    (inc i)))

(defn shim-command
  "The command line that starts a shim bound to `node`'s voter."
  [test node instance]
  [(str (:bin-dir test) "/coord-jepsen")
   "--dir"        (:run-dir test)
   "--voter"      (str (voter test node))
   "--instance"   (str instance)
   "--attempt-ms" (str (:attempt-ms test))
   "--budget-ms"  (str (:budget-ms test))
   "--connect-ms" (str (:connect-ms test))
   "--prefix"     (str (:key-prefix test))])

(defn read-line-within
  "Reads one line from `reader` within `ms` milliseconds; nil on timeout or
  end of stream."
  [^BufferedReader reader ms]
  (let [f (future (.readLine reader))]
    (deref f ms nil)))

(defn start-shim!
  "Starts a shim for `node` and waits for its ready line. Throws if it does
  not bind."
  [test node]
  (let [instance (bit-and (swap! instances inc) 0xffff)
        cmd      (shim-command test node instance)
        log      (io/file (:run-dir test) (str "shim-" node "-" instance ".log"))
        pb       (doto (ProcessBuilder. ^java.util.List cmd)
                   (.redirectError (ProcessBuilder$Redirect/appendTo log)))
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
                   :error (or (:error ready) (if line line "no ready line"))})))))

(defn stop-shim!
  "Closes a shim's input, which ends it, and makes sure it is gone."
  [{:keys [^Process process ^BufferedWriter out]}]
  (when process
    (try (.close out) (catch Exception _))
    (when-not (.waitFor process 2 TimeUnit/SECONDS)
      (.destroyForcibly process))))

(defn mop->json
  "An Elle micro-operation, [:append k v], as the shim takes it."
  [[f k v]]
  [(name f) k v])

(defn json->mop
  "A completed micro-operation from the shim, as Elle reads it."
  [[f k v]]
  [(keyword f) k v])

(defn request
  "The shim request line for an invocation."
  [op]
  (case (:f op)
    :txn   {:f "txn", :value (mapv mop->json (:value op))}
    :read  (let [[k _] (:value op)] {:f "read", :key k})
    :write (let [[k v] (:value op)] {:f "write", :key k, :value v})
    :cas   (let [[k v] (:value op)] {:f "cas", :key k, :value v})))

(defn read-only?
  "Whether an operation cannot have an effect, so that not knowing its
  outcome is the same as it failing."
  [op]
  (case (:f op)
    :read true
    :txn  (every? (comp #{:r} first) (:value op))
    false))

(defn completion
  "Applies a shim answer to an invocation."
  [op answer]
  (let [type (keyword (:type answer))]
    (case type
      :ok (assoc op :type :ok
                 :value (case (:f op)
                          :txn  (mapv json->mop (:value answer))
                          (:read :write :cas)
                          (independent/tuple (first (:value op))
                                             (:value answer))))
      :fail (assoc op :type :fail, :error (:error answer))
      :info (assoc op :type :info, :error (:error answer)))))

(defrecord Client [shim]
  client/Client
  (open! [this test node]
    (assoc this :shim (start-shim! test node)))

  (setup! [this test])

  (invoke! [this test op]
    (let [{:keys [^BufferedWriter out ^BufferedReader in]} shim
          line (do (.write out ^String (json/generate-string (request op)))
                   (.newLine out)
                   (.flush out)
                   ; The shim answers within its own budget; beyond that
                   ; and some slack, it is stuck and we stop waiting.
                   (read-line-within in (+ (:budget-ms test) 10000)))]
      (if line
        (completion op (json/parse-string line true))
        ; The shim is stopped, so this process's client is gone: end the
        ; process either way, or a :fail here would leave the next op
        ; writing to a closed stream.
        (do (stop-shim! shim)
            (assoc op
                   :type         (if (read-only? op) :fail :info)
                   :error        :shim-unresponsive
                   :end-process? true)))))

  (teardown! [this test])

  (close! [this test]
    (stop-shim! shim)))

(defn client
  "A fresh client."
  []
  (Client. nil))
