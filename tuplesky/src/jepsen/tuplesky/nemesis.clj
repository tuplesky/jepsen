(ns jepsen.tuplesky.nemesis
  "Fault schedules for the combined nemesis.

  jepsen.nemesis.combined's DB package draws kill/start and pause/resume
  from one staggered mix: each operation picks one of the two flip-flops at
  random. After a kill, the start waits until the mix draws kill/start
  again, so a run of pauses and resumes can leave every node down for
  minutes (four TupleSky runs and two etcd runs in a day lost two to four
  minutes this way). Here the two flip-flops are staggered on their own, so
  a start follows each kill within one interval, at most twice :interval
  seconds.

  Depends on Jepsen alone, so the etcd baseline can load it too and both
  tests keep the same fault schedule."
  (:require [jepsen [db :as db]
                    [generator :as gen]
                    [random :as rand]]
            [jepsen.nemesis.combined :as nc]))

(defn db-package?
  "Whether a package from nc/nemesis-packages is the DB package, the one
  that kills, starts, pauses and resumes processes."
  [pkg]
  (boolean (some #(= "kill" (:name %)) (:perf pkg))))

(defn stagger
  "Like gen/stagger up to io.jepsen/generator 0.1.3: delays between g's
  operations are uniform in [0, 2 * interval) seconds. From 0.1.4 on,
  gen/stagger draws them from an exponential distribution capped at 100
  seconds, so a start could follow a kill by over three intervals. This takes
  uniform delays through stagger-nanos where the generator library has it
  (0.1.4 on), and from gen/stagger where that is still uniform (the etcd
  test's Jepsen 0.3.11 uses 0.1.1)."
  [interval g]
  (if-let [stagger-nanos (resolve 'jepsen.generator/stagger-nanos)]
    (let [bound (gen/secs->nanos (* 2 interval))]
      (stagger-nanos (fn [] (rand/long bound)) g))
    (gen/stagger interval g)))

(defn db-generator
  "A generator of kills and pauses, like nc/db-generators', with kill/start
  and pause/resume each staggered by :interval on their own. Nil when opts
  enable neither fault, or the DB supports neither."
  [opts]
  (let [db       (:db opts)
        faults   (set (:faults opts))
        interval (:interval opts nc/default-interval)
        kill?    (and (satisfies? db/Kill db) (contains? faults :kill))
        pause?   (and (satisfies? db/Pause db) (contains? faults :pause))
        kill-targets  (:targets (:kill opts) (nc/node-specs db))
        pause-targets (:targets (:pause opts) (nc/node-specs db))
        kill   (fn [_ _] {:type :info, :f :kill, :value (rand/nth kill-targets)})
        pause  (fn [_ _] {:type :info, :f :pause, :value (rand/nth pause-targets)})
        start  {:type :info, :f :start, :value :all}
        resume {:type :info, :f :resume, :value :all}
        gens   (cond-> []
                 kill?  (conj (stagger interval
                                       (gen/flip-flop kill (gen/repeat start))))
                 pause? (conj (stagger interval
                                       (gen/flip-flop pause (gen/repeat resume)))))]
    (when (seq gens)
      (apply gen/any gens))))

(defn separate-db-faults
  "Takes nemesis-package opts and the packages nc/nemesis-packages made from
  them, and gives the DB package the generator of db-generator. The DB
  package's nemesis, final generator and perf are kept."
  [opts packages]
  (let [g (db-generator opts)]
    (map (fn [pkg]
           (if (and g (db-package? pkg) (:generator pkg))
             (assoc pkg :generator g)
             pkg))
         packages)))

(defn nemesis-package
  "nc/nemesis-package, with the DB package's faults separated."
  [opts]
  (let [opts (update opts :faults set)]
    (nc/compose-packages (separate-db-faults opts (nc/nemesis-packages opts)))))
