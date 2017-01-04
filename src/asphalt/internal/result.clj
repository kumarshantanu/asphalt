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


;; ----- read ResultSet column at runtime -----


(defn read-column-value
  [^ResultSet result-set ^long column-index]
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


(defn read-columns
  [^ResultSet result-set ^long column-count]
  (let [^objects row (object-array column-count)]
    (loop [i (int 0)]
      (when (< i column-count)
        (let [j (unchecked-inc i)]
          (aset row i (read-column-value result-set j))
          (recur j))))
    (vec row)))


;; ----- read ResultSet columns with type information -----


(defn tz-cal
  ^java.util.Calendar
  [^String x]
  (Calendar/getInstance (TimeZone/getTimeZone x)))


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
    :integer    `(.getInt       ~result-set-sym ~col-ref)
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
    (i/expected (str "valid SQL type - either of " (vec (keys t/single-typemap))) column-type)))


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
         :integer    [dissoc :tag]
         :long       [dissoc :tag]
         :nstring    [assoc  :tag "java.lang.String"]
         :object     [assoc  :tag "lava.lang.Object"]
         :string     [assoc  :tag "java.lang.String"]
         :time       [assoc  :tag "java.sql.Time"]
         :timestamp  [assoc  :tag "java.sql.Timestamp"]
         (i/expected (str "valid SQL type - either of " (vec (keys t/single-typemap))) column-type)))
     (read-column-expr column-type result-set-sym column-index-or-label col-arg)]))
