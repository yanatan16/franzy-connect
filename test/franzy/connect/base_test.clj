(ns franzy.connect.base-test
  (:require [clojure.test :refer :all]
            [franzy.connect.base :refer :all]
            [franzy.connect.config :refer [make-config-def]])
  (:import [org.apache.kafka.connect.connector Connector]))

;;NOTE you must `(compile 'franzy.connect.base-test)' to run these tests

(make-class
 {:class org.clojars.yanatan16.franzy.connect.test.HelloWorld
  :methods [["hello" [String] String]]}
 (hello [this world] (str "hello " world)))

(deftest hello-world-class-test
  (let [obj (eval `(do (import 'org.clojars.yanatan16.franzy.connect.test.HelloWorld)
                       (org.clojars.yanatan16.franzy.connect.test.HelloWorld.)))]

    (is (= (.hello obj "java")
           "hello java"))))
