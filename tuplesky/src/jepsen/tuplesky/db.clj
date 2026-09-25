(ns jepsen.tuplesky.db
  "Provisions a TupleSky domain across the test's nodes and runs one voter on
  each.

  The domain is provisioned once, on the control node, by the TupleSky test
  harness: `coord-harness provision --hosts n1=HOST:API:PEER,...` writes a
  signed genesis, an endpoint catalog, and one self-contained bundle per
  voter whose certificates carry that voter's host. Each node gets its
  bundle and the `coordd` binary, initializes its store once, and runs
  `coordd` from inside the bundle. The run directory stays on the control
  node, where the clients' shims read it for their credentials.

  These are fixture credentials from the harness's test authority. See
  docs/operations/multi-host-test.md in the TupleSky repository for what the
  harness does and does not promise."
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [store :as store]
                    [util :as util :refer [meh]]]
            [jepsen.control.util :as cu]
            [slingshot.slingshot :refer [throw+]]))

(def dir "/opt/tuplesky")
(def binary (str dir "/coordd"))

(defn bundle
  "The directory a node's bundle is unpacked to."
  [n]
  (str dir "/n" n))

(defn logfile [n] (str (bundle n) "/coordd.log"))
(defn pidfile [n] (str (bundle n) "/coordd.pid"))

(defn voter
  "One-based voter number of a node."
  [test node]
  (inc (.indexOf ^java.util.List (vec (:nodes test)) node)))

(defn hosts-spec
  "The harness's host list: every node a voter, at its own name, on the
  test's fixed ports."
  [test]
  (->> (:nodes test)
       (map-indexed (fn [i node]
                      (str "n" (inc i) "=" node ":" (:api-port test) ":"
                           (:peer-port test))))
       (str/join ",")))

(defn sh!
  "Runs a local command, throwing on a nonzero exit."
  [& args]
  (let [{:keys [exit out err]} (apply shell/sh args)]
    (when-not (zero? exit)
      (throw+ {:type ::local-command-failed, :cmd args, :exit exit
               :out out, :err err}))
    out))

(defn provision!
  "Provisions the domain into the test's run directory on the control node
  and packs each voter's bundle for upload."
  [test]
  (let [run (:run-dir test)]
    (info "Provisioning a TupleSky domain in" run "for" (hosts-spec test))
    (sh! (str (:bin-dir test) "/coord-harness") "provision"
         "--dir" run "--hosts" (hosts-spec test))
    (doseq [i (range 1 (inc (count (:nodes test))))]
      (sh! "tar" "czf" (str run "/n" i ".tgz") "-C" run (str "n" i)))
    run))

(defn await-log!
  "Waits until the last line of `n`'s log matching `pattern` also matches
  `wanted`."
  [n pattern wanted timeout-ms what]
  (util/await-fn
    (fn []
      (let [line (c/exec :bash :-c
                         (str "grep -E '" pattern "' " (logfile n)
                              " | tail -n 1 || true"))]
        (when-not (re-find wanted line)
          (throw+ {:type ::not-yet, :what what, :last line}))
        line))
    {:log-message (str "Waiting for voter " n ": " what)
     :timeout     timeout-ms}))

(defn start!
  "Starts coordd from the node's bundle."
  [test node]
  (let [n (voter test node)]
    (c/su
      (cu/start-daemon!
        {:logfile (logfile n)
         :pidfile (pidfile n)
         :chdir   (bundle n)}
        binary
        :--config "coordd.toml"))))

(defn kill!
  "Kills coordd outright."
  [test node]
  (c/su (cu/stop-daemon! "coordd" (pidfile (voter test node)))))

(defrecord DB [provisioned]
  db/DB
  (setup! [this test node]
    ; One provisioning for the whole test, whichever node gets here first.
    (locking provisioned
      (when-not @provisioned
        (reset! provisioned (provision! test))))
    (let [n    (voter test node)
          peers (dec (count (:nodes test)))]
      (c/su
        (c/exec :mkdir :-p dir)
        (c/upload [(str (:bin-dir test) "/coordd")] binary)
        (c/exec :chmod :+x binary)
        (c/upload [(str (:run-dir test) "/n" n ".tgz")] (str dir "/bundle.tgz"))
        (c/exec :tar :xzf (str dir "/bundle.tgz") :-C dir)
        (c/exec :rm (str dir "/bundle.tgz"))
        ; A store is initialized once; init refuses one that exists.
        (c/cd (bundle n)
              (c/exec binary :--config "coordd.toml" :init)))
      (start! test node)
      (await-log! n "^coordd phase=" #"phase=live" 60000 "live")
      ; Every voter up before anyone waits for the mesh.
      (jepsen/synchronize test)
      (await-log! n "^peers connected=" (re-pattern (str "=" peers " of"))
                  120000 "peer plane meshed")
      (await-log! n "^voters submittable=" (re-pattern (str "=" peers " of"))
                  120000 "collector links up")
      (jepsen/synchronize test)))

  (teardown! [this test node]
    (meh (kill! test node))
    (c/su (c/exec :rm :-rf dir)))

  db/LogFiles
  (log-files [this test node]
    (let [n (voter test node)]
      {(logfile n) "coordd.log"}))

  db/Process
  (start! [this test node]
    (start! test node))

  (kill! [this test node]
    (kill! test node))

  db/Pause
  (pause!  [this test node] (c/su (cu/grepkill! :stop "coordd")))
  (resume! [this test node] (c/su (cu/grepkill! :cont "coordd"))))

(defn db
  "A TupleSky DB."
  []
  (DB. (atom nil)))
