;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.param
  (:require
    [asphalt.internal :as i]
    [asphalt.param.internal :as pi])
  (:import
    [java.util Calendar TimeZone]
    [java.sql Date PreparedStatement Time Timestamp]))


;; ----- utility fns to turn (local) date/time/timestamp into another timezone -----


(defn date->cal
  "Given a (local) date and a timezone/calendar instance, return a calendar suitable to set SQL date param."
  [^Date date tz-or-cal]
  (doto ^Calendar (pi/resolve-cal tz-or-cal)
    (.setTime date)))


(defn time->cal
  "Given a (local) time and a timezone/calendar instance, return a calendar suitable to set SQL time param."
  [^Time time tz-or-cal]
  (doto ^Calendar (pi/resolve-cal tz-or-cal)
    (.setTimeInMillis (.getTime time))))


(defn timestamp->cal
  "Given a (local) timestamp and a timezone/calendar instance, return a calendar suitable to set SQL timestamp param."
  [^Timestamp timestamp tz-or-cal]
  (doto ^Calendar (pi/resolve-cal tz-or-cal)
    (.setTimeInMillis (.getTime timestamp))))


;; ----- lay SQL params for known SQL types -----


(defmacro lay-params*
  "Implementation detail of `lay-params`.
  See: `lay-params`"
  [prepared-stmt param-keys param-types params]
  (i/expected vector? "vector of SQL param keys" param-keys)
  (i/expected vector? "vector of SQL param types" param-types)
  (doseq [each-type param-types]
    (i/expected (partial contains? pi/sql-all-types-map)
      (str "a valid SQL param type - either of " pi/sql-all-types-keys) each-type))
  (when (not= (count param-types) (count param-keys))
    (i/expected (format "param-types (%d) and param-keys (%d) to be of the same length"
                  (count param-types) (count param-keys)) {:param-types param-types
                                                           :param-keys  param-keys}))
  (let [prepared-stmt-sym (with-meta (gensym "prepared-stmt-") {:tag "java.sql.PreparedStatement"})
        params-sym  (gensym "sql-params-")
        param-count (count param-types)
        indices-sym (gensym "param-indices-")
        indices-exp (pi/params-indices param-keys param-types params-sym)
        idx-binding (if (vector? indices-exp)
                      []
                      [indices-sym indices-exp])]
    `(let [~params-sym ~params
           ~@(when-not (vector? indices-exp)  ; indices-exp is a vector for single params, S-expr otherwise
               [indices-sym indices-exp])]
       (if (<= ~param-count (count ~params-sym))
         (let [~prepared-stmt-sym ~prepared-stmt]
           (doseq [each-key# ~param-keys]
             (when-not (contains? ~params-sym each-key#)
               (i/expected (str "key " each-key# " to be present in SQL params") ~params-sym)))
           ~@(->> (map vector param-types param-keys)
               (map-indexed (fn [^long idx [t k]]
                              (pi/lay-param-expr prepared-stmt-sym t
                                (if (vector? indices-exp)  ; indices-exp is vector for single params, S-expr otherwise
                                  (get indices-exp idx)
                                  `(get ~indices-sym ~idx))
                                `(get ~params-sym ~k))))))
         (i/expected ~(str param-count " params") ~params-sym)))))


(defmacro lay-params
  "Given literal SQL param types and keys, return an expression to set vector/map params on a JDBC prepared statement."
  ([prepared-stmt param-keys param-types params]
    (let [params-sym (gensym "sql-params-")]
      `(let [~params-sym ~params]
         (cond
           (vector?
             ~params-sym)     (lay-params* ~prepared-stmt ~(vec (range (count param-types))) ~param-types ~params-sym)
           (map? ~params-sym) (lay-params* ~prepared-stmt ~param-keys ~param-types ~params-sym)
           (nil? ~params-sym) nil
           :otherwise         (i/expected "SQL params as a vector/map/nil" ~params-sym)))))
  ([prepared-stmt param-key-type-pairs params]
    (i/expected vector? "vector of SQL param key/type pairs" param-key-type-pairs)
    (i/expected (comp even? count) "even number of elements in SQL param key/type pairs" param-key-type-pairs)
    `(lay-params ~prepared-stmt ~@(let [pairs (partition 2 param-key-type-pairs)]
                                    [(mapv first pairs) (mapv second pairs)])
       ~params)))


;; ----- set SQL params at runtime -----


(def default-param-types (repeat nil))


(defn param-keys
  "Given a vector/map/nil of SQL params return param keys for use with `set-params`."
  [params]
  (if (vector? params)
    (pi/cached-indices (count params))
    (keys params)))


(defn set-params
  "Given prepared statement and a vector/map/nil of SQL params, set the params at runtime.
  See:
    asphalt.param/default-param-types
    asphalt.param/param-keys"
  ([^PreparedStatement prepared-stmt params]
    (set-params
      prepared-stmt (param-keys params) default-param-types params))
  ([^PreparedStatement prepared-stmt param-key-type-pairs params]
    (let [[param-keys param-types] (let [pairs (partition 2 param-key-type-pairs)]
                                     [(mapv first pairs) (mapv second pairs)])]
      (set-params
        prepared-stmt param-keys param-types params)))
  ([^PreparedStatement prepared-stmt param-keys param-types params]
    (loop [pi 1  ; param index
           ks (seq param-keys)
           ts (seq param-types)]
      (when ks
        (let [k (first ks)]
          (if (contains? params k)
            (let [t (first ts)
                  v (get params k)]
              (if-let [single-type (get pi/sql-multi-single-map t)]  ; multi-value param?
                (let [k (count v)]  ; iterate over all values of multi-value param
                  (loop [i 0]
                    (when (< i k)
                      (pi/set-param-value prepared-stmt single-type (unchecked-add pi i) (get v i))
                      (recur (unchecked-inc i))))
                  (recur (unchecked-add pi k) (next ks) (next ts)))
                (do
                  (pi/set-param-value prepared-stmt t pi v)
                  (recur (unchecked-inc pi) (next ks) (next ts)))))
            (i/illegal-arg "No value found for key:" k "in" (pr-str params))))))))