(ns jepsen.swiftpaxos.db
  "Runs SwiftPaxos (github.com/imdea-software/swiftpaxos): its master on the
  control node, one replica on each node.

  SwiftPaxos's master assigns replica ids as replicas register, answers
  clients' questions about the replicas and the leader, and replaces a
  leader it cannot ping. It runs here, outside the nodes, so no fault
  reaches it: the test's faults are the replicas'. A partition cuts replicas
  from each other, not from the master or the clients. Every replica and
  client reads one configuration file, written on the control node for the
  test.

  The replicas keep their state in memory, and SwiftPaxos does not recover a
  replica that stops: kill is accepted, but it is not a fault SwiftPaxos
  claims to survive. A killed replica restarts empty, and it cannot rejoin:
  its peers take connections from peers only while they start, so its new
  connections reach their client listener, which exits on its first peer
  message (\"received unknown client message\"). One kill and restart has
  been seen to end every replica this way.

  The master pings replicas without a timeout, so a paused replica holds its
  pings until it resumes: a paused leader is not replaced."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [store :as store]
                    [util :as util :refer [meh]]]
            [jepsen.control.util :as cu]
            [slingshot.slingshot :refer [throw+]])
  (:import (java.lang ProcessBuilder ProcessBuilder$Redirect)
           (java.net DatagramSocket InetAddress)
           (java.util.concurrent TimeUnit)))

(def dir "/opt/swiftpaxos")
(def binary (str dir "/swiftpaxos"))
(def config-file (str dir "/swiftpaxos.conf"))
(def logfile (str dir "/replica.log"))
(def pidfile (str dir "/replica.pid"))

(def replica-port
  "Every replica's port for peers and clients (SwiftPaxos's default); it
  serves the master on this plus 1000."
  7070)

(defn control-dir
  "Where this test keeps what the control node runs: the configuration, the
  master's log and the clients' logs. Inside the test's store, so they are
  kept with its results."
  [test]
  (let [d (store/path test "control")]
    (.mkdirs d)
    (.getCanonicalPath d)))

(defn config
  "The configuration every participant reads: each node a replica at its
  own name, the master at master-host. The proxy section names no clients;
  replicas need one to take client connections at all."
  [test master-host]
  (str/join
    "\n"
    (concat ["-- Replicas --"]
            (map #(str % " " %) (:nodes test))
            [""
             "-- Master --"
             (str "master " master-host)
             ""
             (str "masterPort: " (:master-port test))
             "protocol: swiftpaxos"
             "noop: false"
             "thrifty: false"
             "optread: false"
             "leaderless: false"
             "fast: true"
             ""
             "-- Proxy --"]
            (map #(str "server_alias " %) (:nodes test))
            ["---" ""])))

(defn route-source
  "The address this host uses to reach `node`: where the replicas reach the
  master. A UDP socket connected to the node has it as its local address;
  connecting one sends nothing."
  [node]
  (with-open [s (DatagramSocket.)]
    (.connect s (InetAddress/getByName node) (int replica-port))
    (let [a (.getLocalAddress s)]
      (when (.isAnyLocalAddress a)
        (throw+ {:type ::no-route, :node node}))
      (.getHostAddress a))))

(defn start-master!
  "Starts the master on this host, writing the configuration first. Returns
  its process."
  [test]
  (let [cdir   (control-dir test)
        host   (or (:master-host test) (route-source (first (:nodes test))))
        conf   (io/file cdir "swiftpaxos.conf")
        log    (io/file cdir "master.log")]
    (spit conf (config test host))
    (info "Starting the SwiftPaxos master at" (str host ":" (:master-port test)))
    (.start (doto (ProcessBuilder.
                    ^java.util.List [(str (:bin-dir test) "/swiftpaxos")
                                     "-run" "master" "-config" (str conf)])
              (.redirectErrorStream true)
              (.redirectOutput (ProcessBuilder$Redirect/appendTo log))))))

(defn stop-master!
  [^Process p]
  (when p
    (.destroy p)
    (when-not (.waitFor p 5 TimeUnit/SECONDS)
      (.destroyForcibly p))))

(defn await-log!
  "Waits until the replica's log has a line matching `pattern`."
  [pattern timeout-ms what]
  (util/await-fn
    (fn []
      (when (str/blank? (c/exec :bash :-c
                                (str "grep -E '" pattern "' " logfile
                                     " | tail -n 1 || true")))
        (throw+ {:type ::not-yet, :what what})))
    {:log-message (str "Waiting for the replica: " what)
     :timeout     timeout-ms}))

(defn start!
  [test node]
  (c/su
    (cu/start-daemon!
      {:logfile logfile
       :pidfile pidfile
       :chdir   dir}
      binary
      :-run    :server
      :-config config-file
      :-alias  node)))

(defn kill!
  [test node]
  (c/su (cu/stop-daemon! "swiftpaxos" pidfile)))

(defrecord DB [master warm-up]
  db/DB
  (setup! [this test node]
    ; One master for the test, whichever node gets here first.
    (locking master
      (when-not @master
        (reset! master (start-master! test))))
    (c/su
      (c/exec :mkdir :-p dir)
      (c/upload [(str (:bin-dir test) "/swiftpaxos")] binary)
      (c/exec :chmod :+x binary)
      (c/upload [(str (control-dir test) "/swiftpaxos.conf")] config-file))
    (start! test node)
    ; A replica connects to its peers once every replica has registered
    ; with the master.
    (await-log! "done connecting to peers" 120000 "peers connected")
    (jepsen/synchronize test)
    ; The cluster's first command has been seen to take six seconds; it is
    ; spent here rather than on the test's first operation.
    (when (= node (jepsen/primary test))
      (warm-up test))
    (jepsen/synchronize test))

  (teardown! [this test node]
    (meh (kill! test node))
    (c/su (c/exec :rm :-rf dir))
    (locking master
      (when-let [p @master]
        (stop-master! p)
        (reset! master nil))))

  db/LogFiles
  (log-files [this test node]
    {logfile "replica.log"})

  db/Process
  (start! [this test node] (start! test node))
  (kill! [this test node] (kill! test node))

  ; By process name, as jepsen.tuplesky.db pauses coordd.
  db/Pause
  (pause!  [this test node] (c/su (cu/signal! "swiftpaxos" "STOP")))
  (resume! [this test node] (c/su (cu/signal! "swiftpaxos" "CONT"))))

(defn db
  "A SwiftPaxos DB. `warm-up` takes the test and puts one command through
  the cluster, throwing if it cannot."
  [warm-up]
  (DB. (atom nil) warm-up))
