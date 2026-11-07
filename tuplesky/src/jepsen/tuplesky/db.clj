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
  (:require [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [store :as store]
                    [util :as util :refer [meh]]]
            [jepsen.control.util :as cu]
            [jepsen.tuplesky.sockets :as sockets]
            [slingshot.slingshot :refer [throw+]]))

(def dir "/opt/tuplesky")
(def binary (str dir "/coordd"))

(def coordctl
  "Where `coordctl` goes on a node, beside coordd, when the test activates
  replicated features."
  (str dir "/coordctl"))

(defn bundle
  "The directory a node's bundle is unpacked to."
  [n]
  (str dir "/n" n))

(defn logfile [n] (str (bundle n) "/coordd.log"))

(defn sockets
  "The voter's UDP sockets, API and peer plane, every 10 s: a Send-Q that
  stops draining under a partition shows there (see
  jepsen.tuplesky.sockets)."
  [test]
  {:log     (str dir "/sockets.log")
   :pidfile (str dir "/sockets.pid")
   :flags   "-uanm"
   :ports   [(:api-port test) (:peer-port test)]})
(defn pidfile [n] (str (bundle n) "/coordd.pid"))

(defn admin-socket
  "The voter's admin socket (task-d99), beside its state, which the
  bundle's configuration puts in the bundle itself. A build before
  task-d99 opens none."
  [n]
  (str (bundle n) "/admin.sock"))

(defn features-file
  "Where a voter's `coordctl feature status` is kept, at :start (after the
  activation) or :end (before its logs are taken)."
  [n at]
  (str (bundle n) "/features-" (name at) ".txt"))

(defn active-set
  "The features a `coordctl feature status` text says are active, as a set
  of names; empty when none is, or the text has no `active` line."
  [text]
  (or (some (fn [line]
              (when-let [[_ names] (re-find #"^active (\S+)$" line)]
                (if (= "-" names) #{} (set (str/split names #",")))))
            (str/split-lines (or text "")))
      #{}))

(defn status-holds?
  "Whether a `coordctl feature status` text says every one of `features`
  is active at this voter and, where this voter leads, that every voter it
  lists holds them all: the leader's view is the domain's."
  [features text]
  (let [lines  (str/split-lines (or text ""))
        leads? (some #(and (str/starts-with? % "epoch ")
                           (str/includes? % "(this node)"))
                     lines)
        held   (keep #(second (re-find #"^voter \S+ .* held (\S+)$" %)) lines)]
    (boolean
      (and (every? (active-set text) features)
           (or (not leads?)
               (and (seq held)
                    (every? (fn [h] (every? (set (str/split h #",")) features))
                            held)))))))

(defn coordctl!
  "Runs `coordctl feature --socket <voter n's admin socket> <args>` on
  this node as root, and returns [exit text], whatever its exit: 1 is a
  refusal (a follower, or a leader still missing a voter's advertisement),
  2 a node it cannot reach."
  [n & args]
  (let [out (c/su (c/exec :bash :-c
                          (str coordctl " feature --socket " (admin-socket n) " "
                               (str/join " " args) " 2>&1; echo \"exit=$?\"")))
        [_ text code] (re-find #"(?s)^(.*?)\n?exit=(\d+)$" out)]
    [(parse-long code) text]))

(defn save-status!
  "Keeps voter n's `coordctl feature status` in its features file at `at`,
  or says the node has no admin socket."
  [n at]
  (c/su (c/exec :bash :-c
                (str "if [ -S " (admin-socket n) " ]; then "
                     coordctl " feature --socket " (admin-socket n) " status > "
                     (features-file n at) " 2>&1; else echo 'no admin socket' > "
                     (features-file n at) "; fi"))))

(defn activate!
  "Activates the test's :activate features through this node's admin
  socket, in order, and waits until this voter reports them all active
  and, where it leads, every voter holding them, then keeps its status in
  features-start.txt. Every node asks: a follower is refused, and the
  leader is refused until every voter's advertisement has reached it, so
  each asks again until its own view holds. A node without an admin socket
  (a pair's base, built before task-d99) is left as it is, and says so in
  the file. Throws when the features are not held within `timeout-ms`, so
  a run of an inactive build is not measured."
  [test node timeout-ms]
  (let [n        (voter test node)
        features (:activate test)
        socket?  (= "yes" (c/exec :bash :-c (str "test -S " (admin-socket n)
                                                  " && echo yes || echo no")))]
    (if-not socket?
      (do (info "Voter" n "has no admin socket; no feature activated")
          (save-status! n :start))
      (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
        (loop []
          (let [[_ text] (coordctl! n "status")]
            (cond
              (status-holds? features text)
              (do (info "Voter" n "holds" (str/join "," features))
                  (save-status! n :start))

              (< deadline (System/currentTimeMillis))
              (throw+ {:type ::features-not-held, :node node
                       :features features, :status text})

              :else
              (do (when-let [f (first (remove (active-set text) features))]
                    (coordctl! n "activate" f))
                  (Thread/sleep 1000)
                  (recur)))))))))

(defn voter
  "One-based voter number of a node."
  [test node]
  (let [i (.indexOf ^java.util.List (vec (:nodes test)) node)]
    (when (neg? i)
      (throw+ {:type ::unknown-node, :node node}))
    (inc i)))

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

(defn daemon-env
  "The environment coordd starts with: the test's :voter-env (names to
  values), and TOKIO_WORKER_THREADS when it sets :voter-workers, which tokio
  reads for its runtime's worker count, so co-located voters can run fewer
  than one worker per host core. Nil when neither is set, which leaves the
  environment as start-stop-daemon's."
  [test]
  (let [env (cond-> (into {} (map (fn [[k v]] [(keyword k) (str v)]))
                          (:voter-env test))
              (:voter-workers test)
              (assoc :TOKIO_WORKER_THREADS (str (:voter-workers test))))]
    (not-empty env)))

(defn start!
  "Starts coordd from the node's bundle."
  [test node]
  (let [n (voter test node)]
    (c/su
      (cu/start-daemon!
        (cond-> {:logfile (logfile n)
                 :pidfile (pidfile n)
                 :chdir   (bundle n)}
          (daemon-env test) (assoc :env (daemon-env test)))
        binary
        :--config "coordd.toml"))))

(defn store-file
  "The voter's store database, the newest generation's `domain.redb` under
  its bundle, or nil when there is none."
  [n]
  (some-> (meh (c/exec :ls :-1 (c/lit (str (bundle n) "/state/gen-*/domain.redb"))))
          str/split-lines
          last
          str/trim
          not-empty))

(defn led-ballot
  "The highest ballot this voter's log says it led (`this voter leads ballot
  N`, over all its boots), or nil when it led none."
  [n]
  ; grep exits 1 on a log without the line, and `meh` returns what that
  ; threw rather than nil.
  (some->> (meh (c/exec :grep :-oE "^this voter leads ballot [0-9]+" (logfile n)))
           (#(when (string? %) %))
           str/split-lines
           (keep #(some-> (re-find #"[0-9]+$" %) parse-long))
           seq
           (reduce max)))

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
        ; coordctl beside it, where the test activates features and the
        ; build has one.
        (when (and (seq (:activate test))
                   (.exists (io/file (str (:bin-dir test) "/coordctl"))))
          (c/upload [(str (:bin-dir test) "/coordctl")] coordctl)
          (c/exec :chmod :+x coordctl))
        (c/upload [(str (:run-dir test) "/n" n ".tgz")] (str dir "/bundle.tgz"))
        (c/exec :tar :xzf (str dir "/bundle.tgz") :-C dir)
        (c/exec :rm (str dir "/bundle.tgz"))
        ; A store is initialized once; init refuses one that exists.
        (c/cd (bundle n)
              (c/exec binary :--config "coordd.toml" :init)))
      (start! test node)
      (sockets/start! (sockets test))
      (await-log! n "^coordd phase=" #"phase=live" 60000 "live")
      ; Every voter up before anyone waits for the mesh.
      (jepsen/synchronize test)
      (await-log! n "^peers connected=" (re-pattern (str "=" peers " of"))
                  120000 "peer plane meshed")
      (await-log! n "^voters submittable=" (re-pattern (str "=" peers " of"))
                  120000 "collector links up")
      (jepsen/synchronize test)
      ; The replicated features the test asks for (task-d99), active and
      ; held at every voter before the nemesis and the workload.
      (when (seq (:activate test))
        (activate! test node 60000)
        (jepsen/synchronize test 120))))

  (teardown! [this test node]
    (meh (kill! test node))
    (sockets/stop! (sockets test))
    (c/su (c/exec :rm :-rf dir)))

  ; The store comes too, before teardown removes it: a voter that stops on
  ; a release contradicting its own execution leaves the orders to compare
  ; only there. coordd is killed first (the teardown kills it anyway): a
  ; copy taken while it commits can read one commit's header and pages a
  ; later commit reused, while a copy of a killed store is a crash image,
  ; which redb recovers to its last commit.
  db/LogFiles
  (log-files [this test node]
    (let [n (voter test node)]
      ; The features as they stand at the end, before the kill: restarts
      ; in the run kept them or did not.
      (when (seq (:activate test))
        (meh (save-status! n :end)))
      (meh (kill! test node))
      (let [store (store-file n)]
        (cond-> {(logfile n)            "coordd.log"
                 (:log (sockets test)) "sockets.log"}
          store                 (assoc store "domain.redb")
          (seq (:activate test)) (assoc (features-file n :start) "features-start.txt"
                                        (features-file n :end) "features-end.txt")))))

  ; The leader is the voter that led the highest ballot any log names: a
  ; new leader logs its ballot when it takes it, and a deposed one's lines
  ; name only lower ones. Best-effort, as db/Primary allows: between a
  ; leader's death and the next election it is still the one named. With
  ; no ballot led yet, a random voter.
  db/Primary
  (primaries [this test]
    (let [led (->> (c/on-nodes test (fn [test node] (led-ballot (voter test node))))
                   (filter val))]
      (if (seq led)
        [(key (apply max-key val led))]
        [(rand-nth (:nodes test))])))

  (setup-primary! [this test node])

  db/Process
  (start! [this test node]
    (start! test node))

  (kill! [this test node]
    (kill! test node))

  ; By process name (pkill), not cu/grepkill!: its `pgrep -f pattern |
  ; xargs kill` can list its own xargs, still a fork of the `bash -c`
  ; that names the pattern, and xargs then stops itself, so the pause
  ; never returns and holds the nemesis (seen in the etcd baseline).
  db/Pause
  (pause!  [this test node] (c/su (cu/signal! "coordd" "STOP")))
  (resume! [this test node] (c/su (cu/signal! "coordd" "CONT"))))

(defn db
  "A TupleSky DB."
  []
  (DB. (atom nil)))
