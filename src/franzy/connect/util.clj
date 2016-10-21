(ns franzy.connect.util
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [franzy.common.configuration.codec :refer [decode]])
  (:import [java.util Map Set List]
           [org.apache.kafka.connect.sink SinkRecord]))

(defn read-version []
  (or (some-> (io/resource "project.clj") slurp edn/read-string (nth 2))
      "unknown"))

(defn record->map [^SinkRecord r]
  {:key (decode (.key r))
   :value (decode (.value r))
   :topic (.topic r)
   :partition (.kafkaPartition r)
   :offset (.kafkaOffset r)})

(defn stateful-call [state method & args]
  (->> (apply method @state args)
       (reset! state)))
