(ns franzy.connect.sink
  (:require [franzy.connect.util :as u]
            [franzy.connect.config :refer [config->clj clj->config]])
  (:import [org.apache.kafka.connect.sink SinkConnector SinkTask]
           [java.util ArrayList]))

(defn put-1->put [put-1]
  (fn [state records]
    (loop [records records
           state state]
      (if (empty? records)
        state
        (recur (rest records)
               (put-1 state (first records)))))))

(defmacro make-task
  "Make an arbitrarily complex SinkTask

  Class prefix should be a full path. Connector will be appended onto it.

  Task should be a map with one of the two keys:
   :put (fn [state records]) Returns: task state. This function should start the work of
     pushing records to the remote database, but not necessarily block for completion.
   :put-1 (fn [state record]) Returns :task state. This function should start the work of
     pushing a a single record into the remote database, but not necessarily block for
     completion.

  Task may optionally have the following keys:
   :start (fn [config]) Returns: task state. This function should start any
     connections to the remote database for pushing records.
   :stop (fn [state]) This function should stop any connections to remote databases.
   :flush (fn [state offsets]) Returns: task state. This function should
     block for completion of all writes for any offset passed into it.
     offsets is a map of topic/partition to offset that must be flushed.
   :on-parts-assigned (fn [state partitions]) Returns: task state. This function will
     be called when partitions are assigned to it dynamically (only done when the
     connector calls (.requestTaskReconfiguration)). It should do any work necessary
     to be prepared to push from new partitions.
   :on-parts-revoked (fn [state partitions]) Returns: task state. This function will
     be called when partitions are revoked from it dynamically (only done when the
     connector calles (.requestTaskReconfiguration)). It should stop any work being
     done for the topic-partitions being revoked."

  [class-prefix {:keys [start stop put put-1 flush on-parts-revoked on-parts-assigned]}]

  `(let [start# ~(if start start identity)
         stop# ~(if stop stop identity)
         put# ~(cond put put
                     put-1 `(put-1->put ~put-1))
         flush# ~(if flush flush `(fn [state# _#] state#))]

     (gen-class
      :name ~(symbol (str class-prefix "Task"))
      :extends ~'org.apache.kafka.connect.sink.SinkTask
      :state "state"
      :init "init"
      :constructors {[] []}
      :prefix "task-")

     (defn ~'task-init []
       [[] (atom nil)])

     (defn ~'task-start [this# props#]
       (reset! (.state this#) (start# (config->clj props#))))

     (defn ~'task-stop [this#]
       (u/stateful-call (.state this#) stop#))

     (defn ~'task-version [_#] (u/read-version))

     (defn ~'task-put [this# records#]
       (u/stateful-call (.state this#) put# (map u/record->map records#)))

     (defn ~'task-flush [this# offsets#]
       (u/stateful-call (.state this#)
                        flush# offsets#))

     ~@(if (nil? on-parts-assigned) []
           [`(defn ~'task-onPartitionsAssigned [this# partitions#]
               (u/stateful-call (.state this#)
                                ~on-parts-assigned partitions#))])

     ~@(if (nil? on-parts-revoked) []
           [`(defn ~'task-onPartitionsRevoked [this# partitions#]
               (u/stateful-call (.state this#)
                                ~on-parts-revoked partitions#))])
     ))

(defmacro make-connector
  "Make an arbitrarily complext SinkConnector.

  Class prefix should be a full path. Connector will be appended onto it.

  connector should be a map with the following required keys:
   :config-def a configuration definition, see franzy.connect.config

  connector can optionally have the following keys:
   :start (fn [config context]) Returns: connector state. This function should
     monitor the remote database for partition changes and call (.requestTaskReconfiguration context)
     By default, start simply returns the config as state.
   :stop (fn [state]) This function should stop any monitoring of remote databases.
     By default, does nothing.
   :task-configs (fn [state max-tasks]) This function should return configurations
     for each task in a sequence. This can be simply `(repeat max-tasks config)' or
     it can take into account partitions of the remote database. The configurations
     will be encoded to kafka properties for serialization.
     By default, it assumes the state is just the config, and returns (repeat max-tasks config)"
  [class-prefix {:keys [start stop task-configs config-def]}]
  `(let [start# ~(if start start (fn [cfg ctx] cfg))
         stop# ~(if stop stop identity)
         task-configs# ~(if task-configs task-configs (fn [cfg max-tasks] (repeat max-tasks cfg)))
         config-def# ~config-def]

     (gen-class
      :name ~(symbol (str class-prefix "Connector"))
      :extends ~'org.apache.kafka.connect.sink.SinkConnector
      :state ~'state
      :init ~'init
      :exposes {~'context {:get ~'context}}
      :constructors {[] []}
      :prefix "connector-")

     (defn ~'connector-init []
       [[] (atom nil)])

     (defn ~'connector-start [this# props#]
       (reset! (.state this#) (start# (config->clj props#) (.context this#))))

     (defn ~'connector-stop [this#]
       (u/stateful-call (.state this#) stop#))

     (defn ~'connector-version [_#] (u/read-version))

     (defn ~'connector-config [_#] config-def#)

     (import ~(symbol (str class-prefix "Task")))
     (defn ~'connector-taskClass [_#] ~(symbol (str class-prefix "Task")))

     (defn ~'connector-taskConfigs [this# max-tasks#]
       (->> (task-configs# @(.state this#) max-tasks#)
            (map clj->config)
            vec
            ArrayList.))))

(defmacro make-sink
  "Shorthand for (do (make-task ...) (make-connector ...))"
  [class-prefix task connector]
  `(do
     (make-task ~class-prefix ~task)
     (make-connector ~class-prefix ~connector)))
