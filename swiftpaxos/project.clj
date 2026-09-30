(defproject jepsen.swiftpaxos "0.1.0-SNAPSHOT"
  :description "Jepsen tests for the SwiftPaxos reference implementation, a baseline for TupleSky's"
  :url "https://github.com/tuplesky/jepsen"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  ; Built against the Jepsen in this repository: run `lein install` in
  ; ../jepsen first. The TupleSky test's sources come too, for its fault
  ; schedule (jepsen.tuplesky.nemesis), so both tests keep one.
  :dependencies [[org.clojure/clojure "1.12.6"]
                 [jepsen "0.3.15-SNAPSHOT"]
                 [cheshire "5.13.0"]]
  :source-paths ["src" "../tuplesky/src"]
  :jvm-opts ["-Djava.awt.headless=true"
             "-server"
             "-Xmx8g"]
  :repl-options {:init-ns jepsen.swiftpaxos}
  :main jepsen.swiftpaxos)
