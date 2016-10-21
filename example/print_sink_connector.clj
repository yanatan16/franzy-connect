(ns print-sink-connector
  (:require [franzy.connect.sink :as sink]
            [franzy.connect.config :refer [make-config-def]]
            [print-sink-task]))

(def cfg (make-config-def
          (:print.prefix :type/string "Kafka Record:" :importance/low "Prefix for line to be printed")))

(sink/make-connector
 franzy.connect.example.PrintSink
 {:start #(do %2 (println :print-connector :started %1) %1)
  :stop #(println :print-connector :stopped %)
  :task-configs #(do (println :print-connector :task-configs %1 %2)
                     (repeat %2 %1))
  :config-def cfg})
