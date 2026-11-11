(ns jepsen.tuplesky.nemesis-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen [db :as db]
                    [generator :as gen]
                    [util :as util]]
            [jepsen.generator.test :as gen.test]
            [jepsen.nemesis]
            [jepsen.nemesis.combined :as nc]
            [jepsen.tuplesky.nemesis :as tn]))

(def fake-db
  "A DB that can be killed and paused, and does nothing."
  (reify
    db/DB
    (setup! [_ _ _])
    (teardown! [_ _ _])
    db/Kill
    (kill! [_ _ _])
    (start! [_ _ _])
    db/Pause
    (pause! [_ _ _])
    (resume! [_ _ _])))

(def opts
  {:db       fake-db
   :nodes    ["n1" "n2" "n3" "n4" "n5"]
   :faults   #{:kill :pause :partition}
   :kill     {:targets [:one :majority :all]}
   :pause    {:targets [:one :majority]}
   :interval 30})

(defn db-ops
  "The kills, starts, pauses and resumes of the separated DB package, run
  through a simulated test. Nemesis operations are :info, which
  gen.test/perfect leaves out, so this takes every operation from perfect*
  and keeps the :info ones (an invocation and its completion each)."
  [n]
  (let [pkgs (tn/separate-db-faults opts (nc/nemesis-packages opts))
        g    (:generator (first (filter tn/db-package? pkgs)))]
    (->> (gen/limit n g)
         gen/nemesis
         gen.test/perfect*
         (filter (comp #{:info} :type)))))

(deftest separates-only-the-db-package
  (let [pkgs  (nc/nemesis-packages opts)
        pkgs' (tn/separate-db-faults opts pkgs)]
    (testing "exactly one package is the DB package"
      (is (= 1 (count (filter tn/db-package? pkgs)))))
    (testing "the DB package gets a new generator, and keeps the rest"
      (let [[old new] (map #(first (filter tn/db-package? %)) [pkgs pkgs'])]
        (is (not= (:generator old) (:generator new)))
        (is (= (dissoc old :generator) (dissoc new :generator)))))
    (testing "every other package is untouched"
      (is (= (remove tn/db-package? pkgs) (remove tn/db-package? pkgs')))))
  (testing "without kill or pause nothing changes"
    (let [o    (assoc opts :faults #{:partition})
          pkgs (nc/nemesis-packages o)]
      (is (= pkgs (tn/separate-db-faults o pkgs))))))

(deftest a-start-follows-every-kill-within-two-intervals
  (let [ops   (db-ops 400)
        kills (filter (comp #{:kill :start} :f) ops)
        runs  (partition-by :f kills)
        fs    (map (comp :f first) runs)
        bound (+ (util/secs->nanos (* 2 (:interval opts))) 1e6)]
    (is (< 20 (count fs)) "the simulation kills often enough to judge")
    (is (= (take (count fs) (cycle [:kill :start])) fs)
        "kills and starts alternate, beginning with a kill")
    (doseq [[k s] (partition 2 (map first runs))]
      (is (<= (- (:time s) (:time k)) bound)
          (str "start " (:time s) " too long after kill " (:time k))))
    (testing "pauses and resumes still happen, on their own schedule"
      (is (some (comp #{:pause} :f) ops))
      (is (some (comp #{:resume} :f) ops)))))

(deftest the-peer-plane-hold
  (testing "no hold, no package and no generator"
    (is (nil? (tn/hold-peers-package {:seconds 0 :port 7002})))
    (is (nil? (tn/hold-peers-package nil)))
    (is (nil? (tn/hold-peers-generator 0))))
  (testing "a hold's package carries its nemesis and its release"
    (let [pkg (tn/hold-peers-package {:seconds 10 :port 7002})]
      (is (= #{:hold-peers :release-peers}
             (jepsen.nemesis/fs (:nemesis pkg))))
      (is (= :release-peers (:f (:final-generator pkg))))))
  (testing "composed, the hold's two operations reach its nemesis"
    (let [pkg (tn/nemesis-package (assoc opts :hold-peers {:seconds 10 :port 7002}))]
      (is (every? (jepsen.nemesis/fs (:nemesis pkg)) [:hold-peers :release-peers]))))
  ; The simulation gives each nemesis operation one :info entry.
  (testing "the generator holds once, then releases once"
    (is (= [:hold-peers :release-peers]
           (->> (gen/nemesis (tn/hold-peers-generator 10))
                gen.test/perfect*
                (filter (comp #{:info} :type))
                (map :f))))))

(def partition-opts
  (assoc opts :partition {:targets [:follower] :seconds [5 10] :every 60}))

(defn partition-ops
  "The partitions and heals of separate-partition-faults' partition
  package, run through a simulated test, one :info entry each."
  [o n]
  (let [pkgs (tn/separate-partition-faults o (nc/nemesis-packages o))
        g    (:generator (first (filter tn/partition-package? pkgs)))]
    (->> (gen/limit n g)
         gen/nemesis
         gen.test/perfect*
         (filter (comp #{:info} :type))
         ; One entry an operation, whether the simulation gives it one or two.
         (partition-by (juxt :f :time))
         (map first))))

(deftest partitions-heal-after-their-hold-and-wait-out-their-gap
  (let [ops   (partition-ops partition-opts 200)
        fs    (map :f ops)
        times (map :time ops)
        secs  util/secs->nanos]
    (is (= 200 (count ops)))
    (is (= (take 200 (cycle [:start-partition :stop-partition])) fs)
        "partitions and heals alternate, beginning with a partition")
    (is (every? #{:follower} (map :value (filter (comp #{:start-partition} :f) ops))))
    (is (< (first times) (secs 120)) "the first partition within twice :every")
    (doseq [[[a b] i] (map vector (partition 2 1 times) (range))]
      (if (even? i)
        (is (<= (secs 5) (- b a) (+ (secs 10) 1e6))
            (str "partition " i " held " (- b a)))
        (is (<= 0 (- b a) (+ (secs 120) 1e6))
            (str "gap " i " was " (- b a)))))
    (testing "the holds vary over the range"
      (let [holds (map (fn [[a b]] (- b a)) (partition 2 times))]
        (is (< 1 (count (distinct holds))))))))

(deftest separates-only-the-partition-package
  (let [pkgs  (nc/nemesis-packages partition-opts)
        pkgs' (tn/separate-partition-faults partition-opts pkgs)
        [old new] (map #(first (filter tn/partition-package? %)) [pkgs pkgs'])]
    (testing "exactly one package is the partition package"
      (is (= 1 (count (filter tn/partition-package? pkgs)))))
    (testing "it gets the schedule and the follower nemesis, and keeps the rest"
      (is (instance? jepsen.tuplesky.nemesis.Partitions (:generator new)))
      (is (not= (:nemesis old) (:nemesis new)))
      (is (= (dissoc old :generator :nemesis) (dissoc new :generator :nemesis))))
    (testing "every other package is untouched"
      (is (= (remove tn/partition-package? pkgs)
             (remove tn/partition-package? pkgs')))))
  (testing "without :seconds or :follower nothing changes"
    (let [o    (assoc opts :partition {:targets [:one :majority]})
          pkgs (nc/nemesis-packages o)]
      (is (= pkgs (tn/separate-partition-faults o pkgs)))))
  (testing ":follower alone keeps the interval's schedule"
    (let [o    (assoc opts :partition {:targets [:follower]})
          pkgs (nc/nemesis-packages o)
          [old new] (map #(first (filter tn/partition-package? %))
                         [pkgs (tn/separate-partition-faults o pkgs)])]
      (is (= (:generator old) (:generator new)))
      (is (not= (:nemesis old) (:nemesis new))))))

(def led-by-n2
  "A DB whose primary is n2."
  (reify
    db/DB
    (setup! [_ _ _])
    (teardown! [_ _ _])
    db/Primary
    (primaries [_ _] ["n2"])
    (setup-primary! [_ _ _])))

(deftest a-follower-grudge-cuts-one-non-leader-off
  (let [test {:nodes ["n1" "n2" "n3" "n4" "n5"]}]
    (dotimes [_ 50]
      (let [g        (tn/follower-grudge test led-by-n2)
            [node]   (filter #(= 4 (count (g %))) (:nodes test))]
        (is (some? node) (str "one node is cut off from the four others: " g))
        (is (not= "n2" node) "never the leader")
        (doseq [other (remove #{node} (:nodes test))]
          (is (= #{node} (set (g other)))
              (str other " is cut off from " node " alone")))))
    (testing "the follower is drawn among the non-leaders"
      (is (< 1 (count (distinct (repeatedly 50 #(->> (tn/follower-grudge test led-by-n2)
                                                     (filter (fn [[_ v]] (= 4 (count v))))
                                                     ffirst)))))))))

(deftest the-follower-nemesis-passes-a-grudge-to-the-partitioner
  (let [seen (atom [])
        p    (reify
               jepsen.nemesis/Nemesis
               (setup! [this _] this)
               (invoke! [_ _ op] (swap! seen conj op) op)
               (teardown! [_ _]))
        fp   (tn/follower-partition-nemesis led-by-n2 p)
        test {:nodes ["n1" "n2" "n3" "n4" "n5"]}]
    (jepsen.nemesis/invoke! fp test {:type :info :f :start-partition :value :follower})
    (jepsen.nemesis/invoke! fp test {:type :info :f :start-partition :value :one})
    (jepsen.nemesis/invoke! fp test {:type :info :f :stop-partition :value nil})
    (let [[a b c] @seen]
      (is (map? (:value a)) "a follower partition becomes a grudge")
      (is (= :one (:value b)) "any other spec goes through as it is")
      (is (= :stop-partition (:f c))))))

(deftest a-follower-grudge-skips-a-node-that-is-not-running
  (let [test   {:nodes ["n1" "n2" "n3" "n4" "n5"]}
        isolated (fn [g] (ffirst (filter (fn [[_ v]] (= 4 (count v))) g)))
        asked  (atom [])]
    (testing "only a running non-leader is cut off"
      (dotimes [_ 30]
        (is (= "n4" (isolated (tn/follower-grudge test led-by-n2
                                                  (fn [_ nodes] (swap! asked conj (set nodes)) #{"n4" "n2"})))))))
    (testing "it asks only about the non-leaders"
      (is (every? #{#{"n1" "n3" "n4" "n5"}} @asked)))
    (testing "with none running, any non-leader rather than none"
      (dotimes [_ 30]
        (is (not= "n2" (isolated (tn/follower-grudge test led-by-n2 (fn [_ _] #{})))))))
    (testing "the nemesis passes its live function on"
      (let [seen (atom nil)
            p    (reify
                   jepsen.nemesis/Nemesis
                   (setup! [this _] this)
                   (invoke! [_ _ op] (reset! seen op) op)
                   (teardown! [_ _]))
            fp   (tn/follower-partition-nemesis led-by-n2 p (fn [_ _] #{"n5"}))]
        (jepsen.nemesis/invoke! fp test {:type :info :f :start-partition :value :follower})
        (is (= "n5" (isolated (:value @seen))))))))
