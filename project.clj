(defproject lucre "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [gloss "0.2.1"]]
  :repositories {"stuart" "http://stuartsierra.com/maven2"}
  :profiles {:dev {:dependencies [[com.stuartsierra/lazytest "1.2.3"]
                                  [midje "1.4.0"]]
                   :plugins [[lein-midje "2.0.0-SNAPSHOT"]]}})
