(ns franzy.connect.base
  (:require [franzy.connect.util :as u]
            [franzy.connect.config :refer [make-config-def config->clj clj->config]])
  (:import [java.util ArrayList]))


(defn- class-method [prefix [m-name & forms]]
  `(defn ~(symbol (str prefix (name m-name)))
     ~@forms))
(defmacro make-prefixed-class [gen-class-args & methods]
  (let [prefix (str (gensym) "-")]
    `(do
       (gen-class
        :prefix ~prefix
        ~@(apply concat gen-class-args))

       ~@(map #(class-method prefix %) methods))))
(defmacro make-class
  [{:keys [class extends exposes]} & methods]
  `(make-prefixed-class
    {:name ~class
     :extends ~extends
     :exposes ~exposes
     :state ~'state
     :init ~'init
     :constructors {[] []}}

    (~'init [] [[] (atom nil)])
    ~@methods))

(defmacro make-connector
  "Make an arbitrarily complex Connector.

  Class prefix should be a full path. Connector will be appended onto it.

  Extends should either be SinkConnector or SourceConnector.

  connector can optionally have any of the following keys:
   :config-def a ConfigDef configuration definition for filling in Control Center fields. See franzy.connect.config for easy creation. By default, it will be an empty ConfigDef.
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
  [class-prefix extends {:keys [start stop task-configs config-def]}]
  `(let [start# ~(if start start (fn [cfg ctx] cfg))
         stop# ~(if stop stop identity)
         task-configs# ~(if task-configs task-configs (fn [cfg max-tasks] (repeat max-tasks cfg)))
         config-def# ~(if config-def config-def `(make-config-def))]

     (import ~(symbol (str class-prefix "Task")))
     (make-class
      {:name ~(symbol (str class-prefix "Connector"))
       :extends ~extends
       :exposes {~'context {:get ~'context}}}

      (~'start [this# props#]
       (reset! (.state this#)
               (start# (config->clj props#) (.context this#))))
      (~'stop [this#]
        (u/stateful-call (.state this#) stop#))
      (~'version [_#] (u/read-version))
      (~'config [_#] config-def#)
      (~'taskClass [_#] ~(symbol (str class-prefix "Task")))
      (~'taskConfigs [this# max-tasks#]
        (->> (task-configs# @(.state this#) max-tasks#)
             (map clj->config)
             vec
             ArrayList.)))))


(defmacro make-base-task
  [class-prefix extends version {:keys [start stop on-parts-revoked on-parts-assigned]} & methods]

  `(let [start# ~(if start start identity)
         stop# ~(if stop stop identity)]

     (make-class
      {:name ~(symbol (str class-prefix "Task"))
       :extends ~extends
       :exposes {~'context {:get ~'context}}})

     (~'start [this# props#]
       (reset! (.state this#) (start# (config->clj props#))))

     (~'stop [this#]
       (u/stateful-call (.state this#) stop#))

     (~'version [_#] version)

     ~@(if (nil? on-parts-assigned) []
           [`(~'onPartitionsAssigned [this# partitions#]
               (u/stateful-call (.state this#)
                                ~on-parts-assigned partitions#))])

     ~@(if (nil? on-parts-revoked) []
           [`(~'onPartitionsRevoked [this# partitions#]
               (u/stateful-call (.state this#)
                                ~on-parts-revoked partitions#))])

     ~@methods))
