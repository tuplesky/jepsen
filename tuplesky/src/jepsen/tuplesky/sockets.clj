(ns jepsen.tuplesky.sockets
  "Socket snapshots on a node: `ss` every few seconds into a log that the
  DB's log-files collects, so a stall under a partition can be read from
  the store. A connection whose sender is blocked shows it there: a Send-Q
  that stops draining and a retransmit timer backing off.

  Depends on Jepsen alone, so the SwiftPaxos test can load it too."
  (:require [clojure.string :as str]
            [jepsen [control :as c]
                    [util :refer [meh]]]))

(defn loop-command
  "The shell command that snapshots sockets into `log` every `interval`
  seconds, in the background, and records its pid in `pidfile`. `flags`
  are ss's (for instance \"-tin\"); `ports` limits it to sockets with one
  of these as their source or destination port."
  [{:keys [log pidfile flags ports interval] :or {interval 10}}]
  (let [expr (->> ports
                    (mapcat (fn [p] [(str "sport = :" p) (str "dport = :" p)]))
                    (str/join " or "))]
    (str "nohup sh -c 'while :; do date -u +%FT%TZ; ss " flags
         " \"( " expr " )\"; sleep " interval "; done'"
         " >> " log " 2>&1 < /dev/null & echo $! > " pidfile)))

(defn start!
  "Starts the snapshot loop (see loop-command) on the current node."
  [opts]
  (c/su (c/exec :bash :-c (loop-command opts))))

(defn stop!
  "Stops the loop start! began on the current node, if it is running."
  [{:keys [pidfile]}]
  (c/su (meh (c/exec :bash :-c (str "[ -f " pidfile " ] && kill $(cat " pidfile
                                    "); rm -f " pidfile)))))
