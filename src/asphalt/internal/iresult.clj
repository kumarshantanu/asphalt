;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.internal.iresult
  (:require
    [asphalt.internal :as i]
    [asphalt.type     :as t])
  (:import
    [java.io   InputStream Reader]
    [java.math BigDecimal]
    [java.net  URL]
    [java.sql  Array Blob Clob Date NClob Ref ResultSet ResultSetMetaData RowId SQLXML Time Timestamp]
    [java.util Calendar Map TimeZone]))


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
      :nil               (read-column-value      result-set column-index)
      :array             (.getArray              result-set column-index)
      :ascii-stream      (.getAsciiStream        result-set column-index)
      :big-decimal       (.getBigDecimal         result-set column-index)
      :binary-stream     (.getBinaryStream       result-set column-index)
      :blob              (.getBlob               result-set column-index)
      :boolean           (.getBoolean            result-set column-index)
      :byte              (.getByte               result-set column-index)
      :byte-array        (.getBytes              result-set column-index)
      :character-stream  (.getCharacterStream    result-set column-index)
      :clob              (.getClob               result-set column-index)
      :date              (cond
                           (nil? column-arg)     (.getDate result-set column-index)
                           (instance? Calendar
                             column-arg)         (.getDate result-set column-index ^Calendar column-arg)
                           (string? column-arg)  (.getDate result-set column-index (tz-cal column-arg))
                           (i/named? column-arg) (.getDate result-set column-index (tz-cal (i/as-str column-arg)))
                           :otherwise            (i/expected "nil, timezone keyword/string or java.util.Calendar" column-arg))
      :double            (.getDouble             result-set column-index)
      :float             (.getFloat              result-set column-index)
      :int               (.getInt                result-set column-index)
      :long              (.getLong               result-set column-index)
      :ncharacter-stream (.getNCharacterStream   result-set column-index)
      :nclob             (.getNClob              result-set column-index)
      :nstring           (.getNString            result-set column-index)
      :object            (cond
                           (nil? column-arg)     (.getObject result-set column-index)
                           (class? column-arg)   (.getObject result-set column-index ^Class column-arg)
                           (instance? Map
                             column-arg)         (.getObject result-set column-index ^Map column-arg)
                           :otherwise            (i/expected "nil, class or map" column-arg))
      :ref               (.getRef                result-set column-index)
      :row-id            (.getRowId              result-set column-index)
      :string            (.getString             result-set column-index)
      :sql-xml           (.getSQLXML             result-set column-index)
      :time              (cond
                           (nil? column-arg)     (.getTime result-set column-index)
                           (instance? Calendar
                             column-arg)         (.getTime result-set column-index ^Calendar column-arg)
                           (string? column-arg)  (.getTime result-set column-index (tz-cal column-arg))
                           (i/named? column-arg) (.getTime result-set column-index (tz-cal (i/as-str column-arg)))
                           :otherwise            (i/expected "nil, timezone keyword/string or java.util.Calendar" column-arg))
      :timestamp         (cond
                           (nil? column-arg)     (.getTimestamp result-set column-index)
                           (instance? Calendar
                             column-arg)         (.getTimestamp result-set column-index ^Calendar column-arg)
                           (string? column-arg)  (.getTimestamp result-set column-index (tz-cal column-arg))
                           (i/named? column-arg) (.getTimestamp result-set column-index (tz-cal (i/as-str column-arg)))
                           :otherwise            (i/expected "nil, timezone keyword/string or java.util.Calendar" column-arg))
      :url               (.getURL                result-set column-index)
      (i/expected-result-type column-type))))


;; ----- read ResultSet columns with type information -----


(defn read-column-expr
  "Given column-type, result-set binding symbol and column index/label, return an expression to fetch a JDBC column
  value."
  [column-type result-set-sym col-ref col-arg]
  (i/expected symbol?  "a symbol" result-set-sym)
  (i/expected (some-fn string? integer?) "an integer or string column index/label" col-ref)
  (case (get t/single-typemap column-type)
    :nil               `(read-column-value    ~result-set-sym ~col-ref)
    :array             `(.getArray            ~result-set-sym ~col-ref)
    :ascii-stream      `(.getAsciiStream      ~result-set-sym ~col-ref)
    :big-decimal       `(.getBigDecimal       ~result-set-sym ~col-ref)
    :binary-stream     `(.getBinaryStream     ~result-set-sym ~col-ref)
    :blob              `(.getBlob             ~result-set-sym ~col-ref)
    :boolean           `(.getBoolean          ~result-set-sym ~col-ref)
    :byte              `(.getByte             ~result-set-sym ~col-ref)
    :byte-array        `(.getBytes            ~result-set-sym ~col-ref)
    :character-stream  `(.getCharacterStream  ~result-set-sym ~col-ref)
    :clob              `(.getClob             ~result-set-sym ~col-ref)
    :date              (cond
                         (nil? col-arg)     `(.getDate ~result-set-sym ~col-ref)
                         (string? col-arg)  `(.getDate ~result-set-sym ~col-ref (tz-cal ~col-arg))
                         (i/named? col-arg) `(.getDate ~result-set-sym ~col-ref (tz-cal ~(i/as-str col-arg)))
                         :otherwise         `(.getDate ~result-set-sym ~col-ref ~col-arg))
    :double            `(.getDouble           ~result-set-sym ~col-ref)
    :float             `(.getFloat            ~result-set-sym ~col-ref)
    :int               `(.getInt              ~result-set-sym ~col-ref)
    :long              `(.getLong             ~result-set-sym ~col-ref)
    :ncharacter-stream `(.getNCharacterStream ~result-set-sym ~col-ref)
    :nclob             `(.getNClob             ~result-set-sym ~col-ref)
    :nstring           `(.getNString          ~result-set-sym ~col-ref)
    :object            (if (nil? col-arg)
                         `(.getObject         ~result-set-sym ~col-ref)
                         `(let [col-arg# ~col-arg]
                            (if (class? col-arg#)
                              (.getObject     ~result-set-sym ~col-ref ^Class col-arg#)
                              (.getObject     ~result-set-sym ~col-ref ^Map col-arg#))))
    :ref               `(.getRef              ~result-set-sym ~col-ref)
    :row-id            `(.getRowId            ~result-set-sym ~col-ref)
    :string            `(.getString           ~result-set-sym ~col-ref)
    :sql-xml           `(.getSQLXML           ~result-set-sym ~col-ref)
    :time              (cond
                         (nil? col-arg)     `(.getTime ~result-set-sym ~col-ref)
                         (string? col-arg)  `(.getTime ~result-set-sym ~col-ref (tz-cal ~col-arg))
                         (i/named? col-arg) `(.getTime ~result-set-sym ~col-ref (tz-cal ~(i/as-str col-arg)))
                         :otherwise         `(.getTime ~result-set-sym ~col-ref ~col-arg))
    :timestamp         (cond
                         (nil? col-arg)     `(.getTimestamp ~result-set-sym ~col-ref)
                         (string? col-arg)  `(.getTimestamp ~result-set-sym ~col-ref (tz-cal ~col-arg))
                         (i/named? col-arg) `(.getTimestamp ~result-set-sym ~col-ref (tz-cal ~(i/as-str col-arg)))
                         :otherwise         `(.getTimestamp ~result-set-sym ~col-ref ~col-arg))
    :url               `(.getURL              ~result-set-sym ~col-ref)
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
         :nil               [assoc  :tag "java.lang.Object"]
         :array             [assoc  :tag "java.sql.Array"]
         :ascii-stream      [assoc  :tag "java.io.InputStream"]
         :big-decimal       [assoc  :tag "java.math.BigDecimal"]
         :binary-stream     [assoc  :tag "java.io.InputStream"]
         :blob              [assoc  :tag "java.sql.Blob"]
         :boolean           [dissoc :tag]
         :byte              [dissoc :tag]
         :byte-array        [assoc  :tag "bytes"]
         :character-stream  [assoc  :tag "java.io.Reader"]
         :clob              [assoc  :tag "java.sql.Clob"]
         :date              [assoc  :tag "java.sql.Date"]
         :double            [dissoc :tag]
         :float             [dissoc :tag]
         :int               [dissoc :tag]
         :long              [dissoc :tag]
         :ncharacter-stream [assoc  :tag "java.io.Reader"]
         :nclob             [assoc  :tag "java.sql.NClob"]
         :nstring           [assoc  :tag "java.lang.String"]
         :object            [assoc  :tag "lava.lang.Object"]
         :ref               [assoc  :tag "java.sql.Ref"]
         :row-id            [assoc  :tag "java.sql.RowId"]
         :string            [assoc  :tag "java.lang.String"]
         :sql-xml           [assoc  :tag "java.sql.SQLXML"]
         :time              [assoc  :tag "java.sql.Time"]
         :timestamp         [assoc  :tag "java.sql.Timestamp"]
         :url               [assoc  :tag "java.net.URL"]
         (i/expected-result-type column-type)))
     (read-column-expr column-type result-set-sym column-index-or-label col-arg)]))
