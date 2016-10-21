(ns print-sink.print-sink
  (:require [franzy.connect.sink :as sink]
            [franzy.connect.config :refer [make-config-def]]))

(def cfg (make-config-def
          (:print.prefix :type/string "Kafka Record:" :importance/low "Prefix for line to be printed")))

(sink/make-sink
 franzy.connect.example.PrintSink

 {:start #(do (println :print-task :started %) %)
  :stop #(println :print-task :stopped %)
  :put-1 #(println :print-sink :put-1 (:print.prefix %1) %2)}

 {:start #(do %2 (println :print-connector :started %1) %1)
  :stop #(println :print-connector :stopped %)
  :task-configs #(do (println :print-connector :task-configs %1 %2)
                     (repeat %2 %1))
  :config-def cfg})
