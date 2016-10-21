(ns franzy.connect.sink-test
  (:require [clojure.test :refer :all]
            [franzy.connect.sink :as sink]
            [franzy.connect.config :as config])
  (:import [org.apache.kafka.connect.sink SinkTask]))

(def puts (atom []))
(def cfg (make-config-def))
(sink/make-task "org.clojars.yanatan16.franzy.connect.sink-test."
                {:put-1 #(swap! puts conj %2)})
(sink/make-connector "org.clojars.yanatan16.franzy.connect.sink-test."
                     {:config-def cfg})
