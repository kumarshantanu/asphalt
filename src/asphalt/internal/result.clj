;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.internal.result
  (:require
    [asphalt.internal :as i]
    [asphalt.type     :as t])
  (:import
    [java.util Calendar Map TimeZone]
    [java.sql Blob Clob ResultSet ResultSetMetaData]))


(defn tz-cal
  ^java.util.Calendar
  [^String x]
  (Calendar/getInstance (TimeZone/getTimeZone x)))


(defn read-column-value
  ([^ResultSet result-set ^long column-index]
    (let [data (.getObject result-set column-index)]
      (cond
        (instance? Clob data) (.getString result-set column-index)
        (instance? Blob data) (.getBytes  result-set column-index)
        (nil? data)           nil
        :otherwise            (let [^String class-name (.getName ^Class (class data))
                                    ^ResultSetMetaData rsmd (.getMetaData result-set)
                                    ^String mdcn (.getColumnClassName rsmd column-index)]
                                (cond
                                  (.startsWith class-name
                                    "oracle.sql.DATE")                (if (#{"java.sql.Timestamp"
                                                                             "oracle.sql.TIMESTAMP"} mdcn)
                                                                        (.getTimestamp result-set column-index)
                                                                        (.getDate      result-set column-index))
                                  (and (instance? java.sql.Date data)
                                    (= "java.sql.Timestamp" mdcn))    (.getTimestamp result-set column-index)
                                  :otherwise                          data)))))
  ([column-type column-arg ^ResultSet result-set ^long column-index]
    (case (get t/single-typemap column-type)
      :nil        (read-column-value result-set column-index)
      :boolean    (.getBoolean   result-set column-index)
      :byte       (.getByte      result-set column-index)
      :byte-array (.getBytes     result-set column-index)
      :date       (cond
                    (nil? column-arg)     (.getDate result-set column-index)
                    (instance? Calendar
                      column-arg)         (.getDate result-set column-index ^Calendar column-arg)
                    (string? column-arg)  (.getDate result-set column-index (tz-cal column-arg))
                    (i/named? column-arg) (.getDate result-set column-index (tz-cal (i/as-str column-arg)))
                    :otherwise            (i/expected "nil, timezone keyword/string or java.util.Calendar" column-arg))
      :double     (.getDouble    result-set column-index)
      :float      (.getFloat     result-set column-index)
      :int        (.getInt       result-set column-index)
      :long       (.getLong      result-set column-index)
      :nstring    (.getNString   result-set column-index)
      :object     (cond
                    (nil? column-arg)     (.getObject result-set column-index)
                    (class? column-arg)   (.getObject result-set column-index ^Class column-arg)
                    (instance? Map
                      column-arg)         (.getObject result-set column-index ^Map column-arg)
                    :otherwise            (i/expected "nil, class or map" column-arg))
      :string     (.getString    result-set column-index)
      :time       (cond
                    (nil? column-arg)     (.getTime result-set column-index)
                    (instance? Calendar
                      column-arg)         (.getTime result-set column-index ^Calendar column-arg)
                    (string? column-arg)  (.getTime result-set column-index (tz-cal column-arg))
                    (i/named? column-arg) (.getTime result-set column-index (tz-cal (i/as-str column-arg)))
                    :otherwise            (i/expected "nil, timezone keyword/string or java.util.Calendar" column-arg))
      :timestamp  (cond
                    (nil? column-arg)     (.getTimestamp result-set column-index)
                    (instance? Calendar
                      column-arg)         (.getTimestamp result-set column-index ^Calendar column-arg)
                    (string? column-arg)  (.getTimestamp result-set column-index (tz-cal column-arg))
                    (i/named? column-arg) (.getTimestamp result-set column-index (tz-cal (i/as-str column-arg)))
                    :otherwise            (i/expected "nil, timezone keyword/string or java.util.Calendar" column-arg))
      (i/expected-result-type column-type))))


;; ----- read ResultSet columns with type information -----


(defn read-column-expr
  "Given column-type, result-set binding symbol and column index/label, return an expression to fetch a JDBC column
  value."
  [column-type result-set-sym col-ref col-arg]
  (i/expected symbol?  "a symbol" result-set-sym)
  (i/expected (some-fn string? integer?) "an integer or string column index/label" col-ref)
  (case (get t/single-typemap column-type)
    :nil        `(read-column-value ~result-set-sym ~col-ref)
    :boolean    `(.getBoolean   ~result-set-sym ~col-ref)
    :byte       `(.getByte      ~result-set-sym ~col-ref)
    :byte-array `(.getBytes     ~result-set-sym ~col-ref)
    :date       (cond
                  (nil? col-arg)     `(.getDate ~result-set-sym ~col-ref)
                  (string? col-arg)  `(.getDate ~result-set-sym ~col-ref (tz-cal ~col-arg))
                  (i/named? col-arg) `(.getDate ~result-set-sym ~col-ref (tz-cal ~(i/as-str col-arg)))
                  :otherwise         `(.getDate ~result-set-sym ~col-ref ~col-arg))
    :double     `(.getDouble    ~result-set-sym ~col-ref)
    :float      `(.getFloat     ~result-set-sym ~col-ref)
    :int        `(.getInt       ~result-set-sym ~col-ref)
    :long       `(.getLong      ~result-set-sym ~col-ref)
    :nstring    `(.getNString   ~result-set-sym ~col-ref)
    :object     (if (nil? col-arg)
                  `(.getObject    ~result-set-sym ~col-ref)
                  `(let [col-arg# ~col-arg]
                     (if (class? col-arg#)
                       (.getObject    ~result-set-sym ~col-ref ^Class col-arg#)
                       (.getObject    ~result-set-sym ~col-ref ^Map col-arg#))))
    :string     `(.getString    ~result-set-sym ~col-ref)
    :time       (cond
                  (nil? col-arg)     `(.getTime ~result-set-sym ~col-ref)
                  (string? col-arg)  `(.getTime ~result-set-sym ~col-ref (tz-cal ~col-arg))
                  (i/named? col-arg) `(.getTime ~result-set-sym ~col-ref (tz-cal ~(i/as-str col-arg)))
                  :otherwise         `(.getTime ~result-set-sym ~col-ref ~col-arg))
    :timestamp  (cond
                  (nil? col-arg)     `(.getTimestamp ~result-set-sym ~col-ref)
                  (string? col-arg)  `(.getTimestamp ~result-set-sym ~col-ref (tz-cal ~col-arg))
                  (i/named? col-arg) `(.getTimestamp ~result-set-sym ~col-ref (tz-cal ~(i/as-str col-arg)))
                  :otherwise         `(.getTimestamp ~result-set-sym ~col-ref ~col-arg))
    (i/expected-result-type column-type)))


(defn read-column-binding
  "Given a value binding symbol/LHS, result-set binding symbol and column index/label, return a vector containing the
  correctly hinted symbol/LHS and the expression to fetch the JDBC column value."
  [sym result-set-sym column-index-or-label]
  (let [[col-sym col-arg] (i/as-vector sym)
        column-type (-> col-sym meta :tag keyword)]
    [(apply vary-meta col-sym
       (case (get t/single-typemap column-type)
         ;; Can't type hint a local with a primitive initializer, so we dissoc the tag for primitive types
         :nil        [assoc  :tag "java.lang.Object"]
         :boolean    [dissoc :tag]
         :byte       [dissoc :tag]
         :byte-array [assoc  :tag "bytes"]
         :date       [assoc  :tag "java.sql.Date"]
         :double     [dissoc :tag]
         :float      [dissoc :tag]
         :int        [dissoc :tag]
         :long       [dissoc :tag]
         :nstring    [assoc  :tag "java.lang.String"]
         :object     [assoc  :tag "lava.lang.Object"]
         :string     [assoc  :tag "java.lang.String"]
         :time       [assoc  :tag "java.sql.Time"]
         :timestamp  [assoc  :tag "java.sql.Timestamp"]
         (i/expected-result-type column-type)))
     (read-column-expr column-type result-set-sym column-index-or-label col-arg)]))
