(defproject jepsen.tuplesky "0.1.0-SNAPSHOT"
  :description "Jepsen tests for TupleSky, through its native client"
  :url "https://github.com/tuplesky/jepsen"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  ; Built against the Jepsen in this repository: run `lein install` in
  ; ../jepsen first.
  :dependencies [[org.clojure/clojure "1.12.6"]
                 [jepsen "0.3.15-SNAPSHOT"]
                 [cheshire "5.13.0"]]
  :jvm-opts ["-Djava.awt.headless=true"
             "-server"
             "-Xmx8g"]
  :repl-options {:init-ns jepsen.tuplesky}
  :main jepsen.tuplesky
  :profiles {:uberjar {:target-path "target/uberjar"
                       :aot :all}})
