(defproject franzy-connect "0.1.0-SNAPSHOT"
  :description "Clojure interface for making Kafka Connectors"
  :url "https://github.com/yanatan16/franzy-connect"
  :license {:name "MIT"
            :url "https://github.com/yanatan16/franzy-connect/blob/master/LICENSE"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.apache.kafka/connect-api "0.10.0.1"]
                 [ymilky/franzy-common "0.0.1"]]

  :profiles {:example {:source-paths ["example"]
                       :aot :all
                       :uberjar-name "franzy-connect-test-standalone.jar"}})
