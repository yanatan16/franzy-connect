(ns franzy.connect.config-test
  (:require [clojure.test :refer [is deftest]]
            [franzy.connect.config :as cfg]
            [clojure.walk :refer [keywordize-keys stringify-keys]])
  (:import [org.apache.kafka.common.config
            ConfigDef ConfigDef$Type ConfigDef$Importance
            ConfigDef$Width ConfigDef$Range
            ConfigDef$Recommender ConfigDef$ValidString
            ConfigDef$Validator ConfigException]))

(deftest kw->enum-test
  (is (instance? ConfigDef$Type (cfg/kw->enum :type/long)))
  (is (instance? ConfigDef$Width (cfg/kw->enum :width/long)))
  (is (instance? ConfigDef$Importance (cfg/kw->enum :importance/high))))

(deftest validator-test
  (let [v (cfg/validator #(< 0 %) "must be positive")]
    (is (instance? ConfigDef$Validator v))
    (is (nil? (.ensureValid v "key" 5)))
    (is (thrown? ConfigException (.ensureValid v "key" -1)))))

(deftest recommender-test
  "FIXME: This test requires more work"
  (let [r (cfg/recommender (fn [n cfg] []) (fn [n cfg] true))]
    (is (instance? ConfigDef$Recommender r))
    (is (= [] (.validValues r "abc" {})))
    (is (true? (.visible r "abc" {})))))

(deftest validator-range-1
  (let [v (cfg/validator-range 1)]
    (is (instance? ConfigDef$Validator v))
    (is (nil? (.ensureValid v "key" 5)))
    (is (thrown? ConfigException (.ensureValid v "key" 0)))))
(deftest validator-range-2
  (let [v (cfg/validator-range 1 10)]
    (is (instance? ConfigDef$Validator v))
    (is (nil? (.ensureValid v "key" 5)))
    (is (thrown? ConfigException (.ensureValid v "key" 0)))
    (is (thrown? ConfigException (.ensureValid v "key" 11)))))

(deftest validator-string-enum
  (let [v (cfg/validator-string-enum "foo" "bar")]
    (is (instance? ConfigDef$Validator v))
    (is (nil? (.ensureValid v "key" "foo")))
    (is (thrown? ConfigException (.ensureValid v "key" "foobar")))))

(deftest make-config-def-test-basic
  (is (instance? ConfigDef (cfg/make-config-def))))

(deftest make-config-def-test-1-field
  (let [cfgdef (cfg/make-config-def (:test.value :type/string "default" :importance/low "documentation"))]
    (is (instance? ConfigDef cfgdef))
    (println (.parse cfgdef {}))
    (is (= "abcdef" (->> {"test.value" "abcdef"} (.parse cfgdef) cfg/config->clj :test.value)))
    (is (= "default" (->> {} (.parse cfgdef) cfg/config->clj :test.value)))))
