(ns print-sink
  (:require [franzy.connect.sink :as sink]
            [franzy.connect.config :refer [make-config-def]]))

(def cfg (make-config-def
          (:print.prefix :type/string "Kafka Record:" :importance/low "Prefix for line to be printed")))

(sink/make-sink
 franzy.connect.example.PrintSink
 {:put-1 #(println (:print.prefix %1) %2)}
 {:config-def cfg})
