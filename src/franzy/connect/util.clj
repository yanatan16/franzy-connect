(ns franzy.connect.util
  (:import [java.util Map Set List]
           [org.apache.kafka.connect.sink SinkRecord]))

(defn java->clj [^Object o]
  (cond
    (instance? Map o) (zipmap (map keyword (.keySet o)) (map java->clj (.values o)))
    (instance? List o) (vec (map java->clj o))
    (instance? Set o) (set (map java->clj o))
    (string? o) (str o)
    :else o))

(defn record->map [^SinkRecord r]
  {:key (java->clj (.key r))
   :value (java->clj (.value r))
   :topic (.topic r)
   :partition (.kafkaPartition r)
   :offset (.kafkaOffset r)})

(defn stateful-call [state method & args]
  (->> (apply method @state args)
       (reset! state)))
