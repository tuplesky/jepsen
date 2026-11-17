(ns jepsen.tuplesky.barrier-test
  (:require [clojure.test :refer [deftest is]]
            [jepsen.nemesis]
            [jepsen.tuplesky.barrier :as b]))

(deftest parse-sample-reads-the-count-and-the-position
  (is (= {:lines 30 :executed 255688}
         (b/parse-sample "30 \"cost\":{\"Observed\":{\"executed\":255688\n")))
  ; No metrics line yet, or no log.
  (is (= {:lines 0 :executed nil} (b/parse-sample "0 ")))
  (is (= {:lines 0 :executed nil} (b/parse-sample "")))
  (is (= {:lines 0 :executed nil} (b/parse-sample nil))))

(def nodes ["n1" "n2" "n3"])

(defn fold [state & rounds]
  (reduce b/step state rounds))

(deftest only-a-line-after-the-start-counts
  (let [s0 {:base {"n1" 5 "n2" 5 "n3" 5}}
        ; n3's line is the one it printed before the barrier: stale.
        s  (fold s0 {"n1" {:lines 6 :executed 100}
                     "n2" {:lines 6 :executed 100}
                     "n3" {:lines 5 :executed 100}})]
    (is (= 100 (:target s)))
    (is (not (b/caught-up? s nodes)))
    (is (b/caught-up? (fold s {"n3" {:lines 6 :executed 100}}) nodes))))

(deftest a-voter-behind-the-highest-position-holds-the-barrier
  (let [s (fold {:base {}}
                {"n1" {:lines 1 :executed 100}
                 "n2" {:lines 1 :executed 100}
                 "n3" {:lines 1 :executed 40}})]
    (is (not (b/caught-up? s nodes)))
    (is (= {"n1" {:executed 100 :gap 0 :running true}
            "n2" {:executed 100 :gap 0 :running true}
            "n3" {:executed 40 :gap 60 :running false}}
           (b/verdict s nodes #{"n1" "n2"})))
    ; It catches up on a later line.
    (is (b/caught-up? (fold s {"n3" {:lines 2 :executed 100}}) nodes))))

(deftest no-fresh-line-at-all-is-behind
  (let [s (fold {:base {"n1" 3 "n2" 3 "n3" 3}}
                {"n1" {:lines 3 :executed 9} "n2" {:lines 3 :executed 9} "n3" {:lines 3 :executed 9}})]
    (is (nil? (:target s)))
    (is (not (b/caught-up? s nodes)))
    (is (= {:executed nil :gap nil :running false}
           (get (b/verdict s nodes #{}) "n3")))))

(deftest render-writes-a-result-line-and-a-line-a-voter
  (is (= (str "result=behind bound_s=60 waited_s=60.2 target=100\n"
              "node=n1 executed=100 gap=0 fresh=yes running=yes\n"
              "node=n2 executed=- gap=- fresh=no running=no\n")
         (b/render {:bound-s 60 :waited-s 60.21 :result :behind :target 100}
                   (sorted-map "n1" {:executed 100 :gap 0 :running true}
                               "n2" {:executed nil :gap nil :running false})))))

(deftest lag-package-only-when-asked
  (is (nil? (b/lag-package 0)))
  (is (nil? (b/lag-package nil)))
  (is (= #{:lag-kill :lag-start-cut :lag-heal}
         (jepsen.nemesis/fs (:nemesis (b/lag-package 10))))))
