(ns jepsen.tuplesky.nemesis-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen [db :as db]
                    [generator :as gen]
                    [util :as util]]
            [jepsen.generator.test :as gen.test]
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
  through a simulated test."
  [n]
  (let [pkgs (tn/separate-db-faults opts (nc/nemesis-packages opts))
        g    (:generator (first (filter tn/db-package? pkgs)))]
    (->> (gen/limit n g)
         gen/nemesis
         (gen.test/perfect (gen.test/n+nemesis-context 2)))))

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
