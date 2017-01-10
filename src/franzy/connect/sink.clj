(ns franzy.connect.sink
  (:require [franzy.connect.util :as u]
            [franzy.connect.base :refer [make-connector make-base-task]])
  (:import [org.apache.kafka.connect.sink SinkConnector SinkTask]))

(defmacro make-task
  "Make an arbitrarily complex SinkTask

  Class prefix should be a full path. Connector will be appended onto it.

  Version should be a string.

  The arguments should be a map of keywords to functions.

  Sink Task-specific keys are:
   :put (fn [state records]) Required. Returns: task state. This function should start the work of
     pushing records to the remote database, but not necessarily block for completion.
   :flush (fn [state offsets]) Optional. Returns: task state. This function should
     block for completion of all writes. Offsets can mostly be ignored, but useful for establishing exactly once semantics.

  Base Tasks may optionally have the following keys:
   :start (fn [config]) Returns: task state. This function should start any
     connections to the remote database for pushing records.
   :stop (fn [state]) This function should stop any connections to remote databases.
   :on-parts-assigned (fn [state partitions]) Returns: task state. This function will
     be called when partitions are assigned to it dynamically (only done when the
     connector calls (.requestTaskReconfiguration)). It should do any work necessary
     to be prepared to push from new partitions.
   :on-parts-revoked (fn [state partitions]) Returns: task state. This function will
     be called when partitions are revoked from it dynamically (only done when the
     connector calles (.requestTaskReconfiguration)). It should stop any work being
     done for the topic-partitions being revoked."

  [class-prefix version {:keys [put flush] :as args}]

  `(let [put# ~put
         flush# ~(if flush flush `(fn [state# _#] state#))]

     (make-base-task
      ~class-prefix
      ~'org.apache.kafka.connect.sink.SinkTask
      ~version
      ~args

      (put [this# records#]
           (u/stateful-call (.state this#) put# (map u/record->map records#)))

      (flush [this# offsets#]
             (u/stateful-call (.state this#)
                              flush# offsets#)))))


(defmacro make-sink
  "Shorthand for (do (make-task ...) (make-connector ...))"
  [class-prefix version task & [connector]]
  `(do
     (make-task ~class-prefix ~version ~task)
     (make-connector ~class-prefix
                     ~'org.apache.kafka.connect.sink.SinkConnector
                     ~connector)))
