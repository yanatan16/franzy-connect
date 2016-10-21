(defproject org.clojars.yanatan16/franzy-connect "0.1.1"
  :description "Clojure interface for making Kafka Connectors"
  :url "https://github.com/yanatan16/franzy-connect"
  :license {:name "MIT"
            :url "https://github.com/yanatan16/franzy-connect/blob/master/LICENSE"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.apache.kafka/connect-api "0.10.0.1"]]

  :profiles {:uberjar {:source-paths ["examples"]
                       :aot :all
                       :uberjar-name "franzy-connect-test-standalone.jar"}
             :dev {:source-paths ["example"]
                   :aot [print-sink]}}

  :plugins [[lein-codox "0.9.0"]]
  :deploy-repositories [["releases" :clojars]])
