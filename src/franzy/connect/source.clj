(ns franzy.connect.source
  (:require [franzy.connect.util :as u]
            [franzy.connect.base :refer [make-connector make-base-task]])
  (:import [org.apache.kafka.connect.source SourceConnector SourceTask]))

(defmacro make-task
  "Make an arbitrarily complex SourceTask

  Class prefix should be a full path. Connector will be appended onto it.

  Version should be a string.

  Source Task-specific keys are:
   :poll (fn [state on-records]) Required. Returns: new-state. Poll the remote database for records. Call on-records with any records found.

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

  [class-prefix version {:keys [poll] :as args}]

  `(let [poll# ~poll]

     (make-base-task
      ~class-prefix
      ~'org.apache.kafka.connect.source.SourceTask
      ~version
      ~args

      (poll [this#]
       (u/stateful-call (.state this#) poll#
                             #(.onRecords (.context this#) %))))))


(defmacro make-source
  "Shorthand for (do (make-task ...) (make-connector ...))"
  [class-prefix version task & [connector]]
  `(do
     (make-task ~class-prefix ~version ~task)
     (make-connector ~class-prefix
                     ~'org.apache.kafka.connect.source.SourceConnector
                     ~connector)))
