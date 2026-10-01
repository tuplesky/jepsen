(ns jepsen.tuplesky.wan-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.nemesis.combined :as nc]
            [jepsen.tuplesky [nemesis :as tn]
                             [nemesis-test :refer [fake-db]]
                             [wan :as wan]]))

(def nodes ["n1" "n2" "n3" "n4" "n5"])

(deftest parses-the-profile
  (is (nil? (wan/parse-spec "none")))
  (is (nil? (wan/parse-spec nil)))
  (is (= {:kind :regions} (wan/parse-spec "regions")))
  (is (= {:kind :uniform, :delay-ms 50.0} (wan/parse-spec "50")))
  (is (= {:kind :uniform, :delay-ms 2.5} (wan/parse-spec "2.5")))
  (is (thrown? IllegalArgumentException (wan/parse-spec "mars")))
  (is (thrown? IllegalArgumentException (wan/parse-spec "-5"))))

(deftest places-nodes-in-regions-round-robin
  (is (= ["us-east" "us-west" "eu-west" "us-east" "us-west"]
         (map (partial wan/region-name nodes) nodes)))
  (let [w {:kind :regions}]
    (testing "a pair's delay is its regions', the same both ways"
      (is (= 33 (wan/base-delay-ms w nodes "n1" "n2")))
      (is (= 33 (wan/base-delay-ms w nodes "n2" "n1")))
      (is (= 37 (wan/base-delay-ms w nodes "n1" "n3")))
      (is (= 65 (wan/base-delay-ms w nodes "n3" "n5"))))
    (testing "one region's nodes are a millisecond apart"
      (is (= 1 (wan/base-delay-ms w nodes "n1" "n4"))))
    (testing "a node is no distance from itself"
      (is (= 0 (wan/base-delay-ms w nodes "n1" "n1")))))
  (testing "a uniform profile delays every pair the same"
    (is (= 20.0 (wan/base-delay-ms {:kind :uniform, :delay-ms 20.0} nodes "n1" "n5"))))
  (testing "no profile, no delay"
    (is (= 0 (wan/base-delay-ms nil nodes "n1" "n2")))))

(deftest a-fault-adds-to-the-profile-for-its-targets
  (let [w     {:kind :regions}
        fault {:targets #{"n2"}, :behavior {:delay-ms 50 :jitter-ms 25}}]
    (is (= {:delay-ms 83 :jitter-ms 25} (wan/behavior w nodes fault "n1" "n2"))
        "traffic to a target")
    (is (= {:delay-ms 83 :jitter-ms 25} (wan/behavior w nodes fault "n2" "n1"))
        "traffic from a target")
    (is (= {:delay-ms 37} (wan/behavior w nodes fault "n1" "n3"))
        "traffic between two other nodes keeps the profile")
    (is (= {:delay-ms 0 :loss "5%"}
           (wan/behavior nil nodes {:targets #{"n1"} :behavior {:loss "5%"}} "n1" "n2"))
        "without a profile, only the fault")))

(deftest netem-arguments
  (is (nil? (wan/netem-args {:delay-ms 0})))
  (is (= [:delay "33ms"] (wan/netem-args {:delay-ms 33})))
  (is (= [:delay "2.5ms"] (wan/netem-args {:delay-ms 2.5})))
  (is (= [:delay "83ms" "25ms" :distribution :normal]
         (wan/netem-args {:delay-ms 83 :jitter-ms 25})))
  (is (= [:delay "33ms" :loss "5%" "25%"]
         (wan/netem-args {:delay-ms 33 :loss "5%" :loss-correlation "25%"})))
  (is (= [:loss "1%"] (wan/netem-args {:delay-ms 0 :loss "1%"})))
  (is (= [:delay "38ms" :reorder "5%"] (wan/netem-args {:delay-ms 38 :reorder "5%"})))
  (is (= [:rate "10mbit"] (wan/netem-args {:rate "10mbit"})))
  (testing "every packet behaviour makes arguments, alone and on a profile"
    (doseq [b wan/packet-behaviors]
      (is (seq (wan/netem-args b)) (pr-str b))
      (is (seq (wan/netem-args (merge {:delay-ms 33} b))) (pr-str b)))))

(deftest bands-group-peers-by-behaviour
  (let [w {:kind :regions}]
    (testing "n1 (us-east): n4 in its region, n2 and n5 in us-west, n3 in eu-west"
      (is (= [[[:delay "1ms"] ["n4"]]
              [[:delay "33ms"] ["n2" "n5"]]
              [[:delay "37ms"] ["n3"]]]
             (wan/bands w nodes nil "n1"))))
    (testing "a fault on n2 gives n1's traffic to it a band of its own"
      (is (= [[[:delay "1ms"] ["n4"]]
              [[:delay "33ms" :loss "1%"] ["n2"]]
              [[:delay "33ms"] ["n5"]]
              [[:delay "37ms"] ["n3"]]]
             (wan/bands w nodes {:targets #{"n2"} :behavior {:loss "1%"}} "n1"))))
    (testing "a uniform profile is one band"
      (is (= [[[:delay "50ms"] ["n2" "n3" "n4" "n5"]]]
             (wan/bands {:kind :uniform :delay-ms 50} nodes nil "n1")))))
  (testing "no profile and no fault: no bands, an unshaped node"
    (is (= [] (wan/bands nil nodes nil "n1")))))

(deftest tc-commands
  (let [ips {"n2" "10.0.0.2", "n3" "10.0.0.3", "n4" "10.0.0.4"}
        cmds (wan/tc-commands "eth0" ips [[[:delay "33ms"] ["n2" "n4"]]
                                          [[:delay "37ms"] ["n3"]]])]
    (is (= (into [:qdisc :add :dev "eth0" :root :handle "1:" :prio :bands 5 :priomap]
                 wan/priomap)
           (first cmds)))
    (is (= [[:qdisc :add :dev "eth0" :parent "1:4" :handle "14:" :netem :delay "33ms"]
            [:filter :add :dev "eth0" :parent "1:0" :protocol :ip :prio 3 :u32
             :match :ip :dst "10.0.0.2/32" :flowid "1:4"]
            [:filter :add :dev "eth0" :parent "1:0" :protocol :ip :prio 3 :u32
             :match :ip :dst "10.0.0.4/32" :flowid "1:4"]
            [:qdisc :add :dev "eth0" :parent "1:5" :handle "15:" :netem :delay "37ms"]
            [:filter :add :dev "eth0" :parent "1:0" :protocol :ip :prio 3 :u32
             :match :ip :dst "10.0.0.3/32" :flowid "1:5"]]
           (rest cmds))))
  (is (nil? (wan/tc-commands "eth0" {} []))))

(def opts
  {:db       fake-db
   :nodes    nodes
   :faults   #{:partition}
   :interval 30})

(deftest the-wan-package-replaces-jepsens-packet-package
  (testing "without a profile or the packet fault, the packages are Jepsen's"
    (let [pkgs (nc/nemesis-packages opts)]
      (is (nil? (tn/wan-package opts)))
      (is (= pkgs (tn/with-wan opts pkgs)))))
  (testing "a profile alone: the WAN nemesis, no packet operations"
    (let [o    (assoc opts :wan {:kind :regions})
          pkgs (tn/with-wan o (nc/nemesis-packages o))
          pkg  (tn/wan-package o)]
      ; The WAN package plots its packet faults as Jepsen's did, so it is
      ; the one package left with that name.
      (is (= 1 (count (filter tn/jepsen-packet-package? pkgs))))
      (is (instance? jepsen.tuplesky.wan.Nemesis (:nemesis (last pkgs))))
      (is (nil? (:generator pkg)))
      (is (nil? (:final-generator pkg)))))
  (testing "the packet fault: start and stop packet operations, and a final stop"
    (let [o   (assoc opts :faults #{:packet})
          pkg (tn/wan-package o)]
      (is (some? (:generator pkg)))
      (is (= {:type :info, :f :stop-packet, :value nil} (:final-generator pkg)))))
  (testing "the composed nemesis takes each packet operation once"
    (let [o (assoc opts :faults #{:packet :partition} :wan {:kind :regions})]
      (is (some? (:nemesis (tn/nemesis-package o)))))))
