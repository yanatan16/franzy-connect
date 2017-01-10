(ns franzy.connect.sink.sweet
  (:require [clojure.tools.logging :as log]
            [manifold.deferred :as d]
            [franzy.connect.sink :refer [make-task make-connector]]))

(defn deferred-put
  "Handle flushes with deferred-flush.
   Put should return `[new-state d]'.
   d should be a manifold deferrable (such as a promise or future) that will be resolved.
   deferred-flush will handle blocking until all deferrables have been resolved."
  [put]
  (fn [{{d :d} :--dstate :as state} records]
    (let [[state' d'] (put state records)]
      (assoc state' :--dstate
             {:d (d/chain d (constantly d'))}))))

(defn deferred-flush
  "A flush function for sink/make-task. Works with deferred-put."
  ([] (deferred-flush identity))
  ([flush]
   (fn [state]
     (let [{{d :d} :--dstate :as state} (flush state)]
       (if (d/deferred? d) @d)
       state))))

(defmacro retry-catch
  [e n retry]
  `(cond (and (instance? ExceptionInfo ~e) (-> ~e ex-data :no-retry))
         (do (log/error ~e "Caught unretriable exception")
             (throw ~e))

         (<= n 0)
         (do (log/error ~e "Retries exhausted.")
             (throw ~e))

         :else
         (do (log/warnf ~e "Caught retriable exception. Retrying %d more times." n)
             ~retry)))

(defn retrying-blocking-put
  "Retries calls to a blocking put unless an ExeptionInfo with :no-retry is caught.
   Currently only takes a static number of retries"
  [put n-retries]
  (fn [state records]
    (loop [n (inc n-retries) e nil]
      (try (put state records)
           (catch Exception e
             (retry-catch e n (recur (dec n) e)))))))

(defn- retry-dput-f
  [put n state records]
  (let [d (put state records)
        catchf (fn [e] (retry-catch e n (retry-dput-f put (dec n) state records)))]
    (d/catch d catchf)))

(defn retrying-deferred-put
  "Retries calls to a deferred put (one that returns d,
   where d is a manifold deferrable, promise, or future).
   State cannot be updated.
   Currently only takes a static number of retries"
  [put n-retries]
  (deferred-put
    (fn [state records]
      [state (retry-dput-f put n-retries state records)])))

(defn future-put
  "Calls put in a future. Retries with retrying-deferred-put.
   State cannot be updated.
   You must use deferred-flush to handle flushes."
  ([put]
   (deferred-put
     (fn [state records] [state (d/future (put state records))] state)))
  ([put n-retries]
   (retrying-deferred-put
    (fn [state records] [state (d/future (put state records))] state)
    n-retries)))


(defn- batched-put-flush-on-put [put batch-size]
  (fn [state records]
    (loop [batches (partition batch-size batch-size nil records)
           state state]
      (if (empty? batches)
        state
        (recur (rest batches)
               (put state (first batches)))))))

(defn- batched-put-flush-on-flush [put batch-size]
  (fn [{buf :--buffer :as state} records]
    (loop [batches (partition batch-size batch-size nil (concat buf records))
           state state]
      (if (or (empty? batches) (< (count (first batches)) batch-size))
        (assoc state :--buffer (first batches))
        (recur (rest batches)
               (put state (first batches)))))))

(defn batched-flush
  "A flush function to flush any remaining records from
   the batch buffer used by batched-put with :flush-on-flush mode"
  ([put] (batched-flush identity))
  ([put flush]
   (fn [{buf :--buffer :as state} offsets]
     (let [state (if buf (put state buf) state)]
       (flush state offsets)))))

(defn batched-put
  "Batches records in a buffer for pushing in chunks.
   Calls put with chunks of at most batch-size number of records.
   It can work in two ways:
    :flush-on-flush keeping a buffer of leftover records
                    in between calls to put and flushing on flush.
                    You must use batched-flush as your flush function then.
    :flush-on-put never keeping records between calls to put (no state)"
  [put batch-size mode]
  (case mode
    :flush-on-flush (batched-put-flush-on-flush put batch-size)
    :flush-on-put (batched-put-flush-on-put put batch-size)))


(defn single-put
  "Wrap a put that gets called for each record"
  (bached-put #(put %1 (first %2)) 1 :flush-on-put))
