(ns jepsen.tuplesky.barrier
  "The quiescent barrier at the end of a run (TupleSky #313 T4, #312 A).

  The history checker judges what the clients saw, so a run can pass with
  a voter that never caught up. The barrier asks the other question.
  After the final heal, the recovery time and the final reads, the
  clients have stopped. Within a bound, every configured voter must then
  reach the highest executed position any voter reports.

  The position is read live, from each voter's `metrics` lines: coordd
  prints one every interval (10 s by default), and its
  `cost.executed` is the commands that replica has applied. Only a line
  printed after the barrier started counts, so a voter that is down or
  hung shows as one with no fresh line, not as its stale position. The
  barrier writes what it saw to `barrier.txt` in the store, one `key=value`
  line for the result and one per voter, for the run's summary and the
  job's gate. It doesn't throw: a voter behind is a finding, not a
  failure of the harness.

  The no-traffic cell (--lag-follower) leaves the barrier one voter that
  only catch-up can bring back: a follower killed early in the workload,
  so its store falls behind everything after, started again once the
  final reads are done but cut off from every other node for a while,
  then healed with no client traffic at all (#312 finding 3: catch-up
  that waits for traffic to probe). The barrier that follows must see it
  reach the others."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [db :as jdb]
                    [nemesis :as n]
                    [net :as net]
                    [random :as rand]
                    [store :as store]
                    [util :refer [meh]]]
            [jepsen.tuplesky.db :as db]))

(def poll-ms
  "How often the barrier reads every voter's log."
  2000)

(def executed-pattern
  "The voter's executed position in a `metrics` line."
  #"\"cost\":\{\"Observed\":\{\"executed\":(\d+)")

(defn parse-sample
  "A node's `<lines> <last metrics line's cost part>` answer as
  {:lines n, :executed e}; :executed is nil where the last line has none."
  [text]
  (let [[lines & more] (str/split (str/trim (or text "")) #"\s+" 2)]
    {:lines    (or (some-> lines (->> (re-matches #"\d+")) parse-long) 0)
     :executed (some->> (first more) (re-find executed-pattern) second parse-long)}))

(defn sample
  "How many `metrics` lines voter n's log holds, and the executed position
  in the last of them, read on the node."
  [test node]
  (let [log (db/logfile (db/voter test node))]
    (parse-sample
      (meh (c/su (c/exec :sh :-c
                         (str "f=" log "; "
                              "printf '%s ' \"$(grep -c '^metrics ' \"$f\" 2>/dev/null)\"; "
                              "grep '^metrics ' \"$f\" 2>/dev/null | tail -n 1 | "
                              "grep -o '\"cost\":{\"Observed\":{\"executed\":[0-9]*' || true")))))))

(defn step
  "Folds one round of samples into the barrier's state: the line counts at
  its start (:base), each voter's latest fresh position (:latest), and
  the highest position any fresh line reported (:target)."
  [state samples]
  (reduce-kv
    (fn [state node {:keys [lines executed]}]
      (if (and executed (> lines (get-in state [:base node] 0)))
        (-> state
            (assoc-in [:latest node] executed)
            (update :target (fnil max 0) executed))
        state))
    state
    samples))

(defn caught-up?
  "Whether every one of `nodes` has a fresh line at the target."
  [{:keys [latest target]} nodes]
  (boolean (and target (every? #(some-> (get latest %) (>= target)) nodes))))

(defn verdict
  "Each voter's row: its latest fresh position and gap to the target, or
  that it printed no line since the barrier started."
  [{:keys [latest target]} nodes running]
  (into (sorted-map)
        (for [node nodes]
          [node (let [e (get latest node)]
                  {:executed e
                   :gap      (when (and e target) (- target e))
                   :running  (contains? running node)})])))

(defn render
  "barrier.txt: the result line, then one line per voter."
  [{:keys [bound-s waited-s result target]} rows]
  (str/join
    (cons (str "result=" (name result) " bound_s=" bound-s
               " waited_s=" (format "%.1f" (double waited-s))
               " target=" (or target "-") "\n")
          (for [[node {:keys [executed gap running]}] rows]
            (str "node=" node " executed=" (or executed "-")
                 " gap=" (or gap "-")
                 " fresh=" (if executed "yes" "no")
                 " running=" (if running "yes" "no") "\n")))))

(defn wait!
  "Waits up to `bound-s` seconds for every voter to reach the target,
  writes barrier.txt, and returns the result and the rows."
  [test bound-s]
  (let [nodes   (vec (:nodes test))
        started (System/nanoTime)
        waited  #(/ (- (System/nanoTime) started) 1e9)
        read!   #(c/on-nodes test nodes sample)
        state0  {:base (into {} (for [[node s] (read!)] [node (:lines s)]))}]
    (info "Barrier: every voter must reach the highest executed position within" bound-s "s")
    (loop [state state0]
      (let [state (step state (read!))
            done? (caught-up? state nodes)]
        (if (or done? (>= (waited) bound-s))
          (let [running (db/running-nodes test nodes)
                rows    (verdict state nodes running)
                result  (if done? :caught-up :behind)
                text    (render {:bound-s bound-s :waited-s (waited)
                                 :result result :target (:target state)}
                                rows)]
            (spit (store/path! test "barrier.txt") text)
            (if done?
              (info "Barrier: every voter at" (:target state) "after" (format "%.1f" (double (waited))) "s")
              (warn "Barrier: not every voter reached the target within" bound-s "s:\n" text))
            {:result result :target (:target state) :voters rows})
          (do (Thread/sleep poll-ms)
              (recur state)))))))

(defrecord Barrier [bound-s]
  n/Reflection
  (fs [_] #{:barrier})

  n/Nemesis
  (setup! [this _] this)
  (invoke! [_ test op]
    (assoc op :value (wait! test bound-s)))
  (teardown! [_ _]))

(defn package
  "The barrier's nemesis, for tuplesky-test's last phase; nil when
  `bound-s` is zero or unset (no barrier)."
  [bound-s]
  (when (and bound-s (pos? bound-s))
    {:nemesis (Barrier. bound-s)
     :perf    #{{:name  "barrier"
                 :fs    #{:barrier}
                 :color "#C8C8C8"}}}))

(defn lag-target
  "The follower the no-traffic cell kills: a running voter that doesn't
  lead the highest ballot, at random; nil when there is none."
  [test]
  (let [nodes     (vec (:nodes test))
        primaries (set (meh (jdb/primaries (:db test) test)))
        running   (db/running-nodes test (remove primaries nodes))]
    (when (seq running)
      (rand/nth (vec (sort running))))))

(defrecord LagFollower [node]
  n/Reflection
  (fs [_] #{:lag-kill :lag-start-cut :lag-heal})

  n/Nemesis
  (setup! [this _] this)

  ; :lag-kill takes the follower down early in the workload; it stays
  ; down until :lag-start-cut, after the final reads, starts it again
  ; behind a grudge that cuts it off from every other node; :lag-heal
  ; ends the grudge, with no client running.
  (invoke! [_ test op]
    (case (:f op)
      :lag-kill
      (if-let [f (lag-target test)]
        (do (reset! node f)
            (c/on-nodes test [f] db/kill!)
            (assoc op :value {:killed f}))
        (assoc op :value :no-follower-running))

      :lag-start-cut
      (if-let [f @node]
        (let [others (vec (remove #{f} (:nodes test)))]
          (net/drop-all! test (n/complete-grudge [[f] others]))
          (c/on-nodes test [f] db/start!)
          (assoc op :value {:started f :cut-from others}))
        (assoc op :value :nothing-killed))

      :lag-heal
      (do (net/heal! (:net test) test)
          (assoc op :value {:healed @node}))))

  (teardown! [_ _]))

(defn lag-package
  "The no-traffic cell's nemesis, for tuplesky-test; nil when `secs`, the
  time the restarted follower is cut off, is zero or unset."
  [secs]
  (when (and secs (pos? secs))
    {:nemesis (LagFollower. (atom nil))
     :perf    #{{:name  "lag"
                 :fs    #{:lag-kill :lag-start-cut :lag-heal}
                 :color "#E8C8A0"}}}))
