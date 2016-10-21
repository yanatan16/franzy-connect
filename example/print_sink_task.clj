(ns print-sink-task
  (:require [franzy.connect.sink :as sink]))

(sink/make-task
 franzy.connect.example.PrintSink
 {:start #(do (println :print-task :started %) %)
  :stop #(println :print-task :stopped %)
  :put-1 #(println :print-sink :put-1 (:print.prefix %1) %2)})
