(ns franzy.connect.config
  (:require [clojure.walk :refer [keywordize-keys stringify-keys]])
  (:import [org.apache.kafka.common.config
            ConfigDef ConfigDef$Type ConfigDef$Importance
            ConfigDef$Width ConfigDef$Range
            ConfigDef$Recommender ConfigDef$ValidString
            ConfigDef$Validator ConfigException]
           [java.util HashMap]))

(defn config->clj [c]
  (keywordize-keys (into {} c)))
(defn clj->config [c]
  (HashMap. (stringify-keys c)))

(def kw->enum
  {:type/string ConfigDef$Type/STRING
   :type/boolean ConfigDef$Type/BOOLEAN
   :type/class ConfigDef$Type/CLASS
   :type/double ConfigDef$Type/DOUBLE
   :type/int ConfigDef$Type/INT
   :type/list ConfigDef$Type/LIST
   :type/long ConfigDef$Type/LONG
   :type/password ConfigDef$Type/PASSWORD
   :type/short ConfigDef$Type/SHORT

   :importance/low ConfigDef$Importance/LOW
   :importance/medium ConfigDef$Importance/MEDIUM
   :importance/high ConfigDef$Importance/HIGH

   :width/none ConfigDef$Width/NONE
   :width/short ConfigDef$Width/SHORT
   :width/medium ConfigDef$Width/MEDIUM
   :width/long ConfigDef$Width/LONG

   ::no-default-value ConfigDef/NO_DEFAULT_VALUE})

(defn validator [f msg]
  (reify ConfigDef$Validator
     (ensureValid [_ name value]
       (if-not (f value)
         (throw (ConfigException. name value msg))))))

(defn recommender [valid-values visible]
  (reify ConfigDef$Recommender
     (validValues [_ name parsed-config]
       (valid-values name (keywordize-keys parsed-config)))
     (visible [_ name parsed-config]
       (visible name (keywordize-keys parsed-config)))))

(defn validator-range
  ([n] (ConfigDef$Range/atLeast n))
  ([n m] (ConfigDef$Range/between n m)))

(defn validator-string-enum [& strs]
  (ConfigDef$ValidString/in (into-array strs)))

(defn- define-arg [arg]
  `(if-let [enum# (kw->enum ~arg)]
     enum# ~arg))
(defn- define-form [[key & args]]
  `(.define ~(name key) ~@(map define-arg args)))

(defn- config-def [forms]
  `(doto (ConfigDef.)
     ~@(map define-form forms)))

(defmacro make-config-def
  "Make a kafka Configuration Definition (ConfigDef)

  Each form should be a list starting with the keyword property value
  followed by arguments to ConfigDef.define

  Default values are passed through to .define
  If no default value exists, use :franzy.connect.config/no-default-value

  Enum arguments like Type/STRING, Importance/HIGH, and Width/LONG can be keywords
  such as :type/string, :importance/high, and :width/long

  Validator arguments can be made with range, string-enum, or the validator macros

  Recommender arguments can be made with the recommender macro."
  [& forms]
  (config-def forms))
