(ns franzy.connect.sink-test
  (:require [clojure.test :refer :all]
            [franzy.connect.sink :as sink]
            [franzy.connect.config :refer [make-config-def]])
  (:import [org.apache.kafka.connect.sink SinkTask]))

(def puts (atom []))
(sink/make-sink "org.clojars.yanatan16.franzy.connect.test.Test"
                "0.1.0"
                {:put #(swap! puts conj %2)})
