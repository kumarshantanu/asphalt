;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.internal.iparam
  (:require
    [asphalt.internal :as i]
    [asphalt.type :as t])
  (:import
    [java.io   InputStream Reader]
    [java.math BigDecimal]
    [java.net  URL]
    [java.sql  Array Blob Clob Date PreparedStatement Ref RowId SQLXML Time Timestamp]
    [java.util Calendar TimeZone]))


;; ----- date/time/calendar helpers -----


(defn tz-cal
  ^java.util.Calendar
  [^String x]
  (Calendar/getInstance (TimeZone/getTimeZone x)))


(defn resolve-cal
  ^java.util.Calendar
  [tz-or-cal]
  (cond
    (instance?
      Calendar tz-or-cal) tz-or-cal
    (instance?
      TimeZone tz-or-cal) (Calendar/getInstance ^TimeZone tz-or-cal)
    (string? tz-or-cal)   (tz-cal tz-or-cal)
    (i/named? tz-or-cal)  (tz-cal (i/as-str tz-or-cal))
    :otherwise            (i/expected "a TimeZone keyword/string or Calendar instance" tz-or-cal)))


;; ----- multi param helpers -----


(defn params-indices
  [param-keys-vec param-types-vec params-sym]
  (i/expected vector? "a vector of param keys" param-keys-vec)
  (i/expected vector? "a vector of param types" param-types-vec)
  (when-not (= (count param-keys-vec) (count param-types-vec))
    (i/expected (format "param keys (%d) and param types (%d) to be of same length"
                  (count param-keys-vec) (count param-types-vec))
      {:param-keys param-keys-vec
       :param-types param-types-vec}))
  (if (every? (partial contains? t/single-typemap) param-types-vec)
    (->> (iterate inc 1)
      (take (count param-types-vec))
      (apply vector-of :int))
    (let [multi-counts (map-indexed (fn [^long idx pt]
                                      (if (contains? t/single-typemap pt)
                                        `1
                                        `(let [pk# ~(get param-keys-vec idx)
                                               vs# (get ~params-sym pk#)]
                                           (i/expected coll? (str "multi-value collection against param key " pk#) vs#)
                                           (count vs#))))
                         param-types-vec)]
      `(reduce (fn [cv# ^long pcount#]
                 (conj cv# (unchecked-add (int (last cv#)) pcount#)))
         (vector-of :int 1)  ; JDBC param index begins at 1
         [~@multi-counts]))))


;; ----- param setting helpers -----


(def ^:const ba-type (Class/forName "[B"))  ;; byte-array class


(defn set-param-value
  [^PreparedStatement prepared-statement param-type ^long param-index param-value]
  (if (nil? param-value)
    (.setObject prepared-statement param-index nil)  ; param type doesn't matter for nil values
    (case (get t/single-typemap param-type)
      :nil               (if (instance? clojure.lang.BigInt param-value)
                           (.setLong           prepared-statement param-index ^long (long param-value))
                           (.setObject         prepared-statement param-index ^Object param-value))
      :array             (.setArray            prepared-statement param-index ^Array param-value) ; Array made using Connection
      :ascii-stream      (.setAsciiStream      prepared-statement param-index ^InputStream param-value)
      :big-decimal       (.setBigDecimal       prepared-statement param-index ^BigDecimal  param-value)
      :binary-stream     (.setBinaryStream     prepared-statement param-index ^InputStream param-value)
      :blob              (.setBlob             prepared-statement param-index ^Blob        param-value)
      :boolean           (.setBoolean          prepared-statement param-index (boolean     param-value))
      :byte              (.setByte             prepared-statement param-index (byte        param-value))
      :byte-array        (.setBytes            prepared-statement param-index ^bytes       param-value)
      :character-stream  (.setCharacterStream  prepared-statement param-index ^Reader      param-value)
      :clob              (.setClob             prepared-statement param-index ^Clob        param-value)
      :date              (if (instance? Calendar param-value)
                           (.setDate           prepared-statement param-index (Date. (.getTimeInMillis ^Calendar param-value))
                             ^Calendar param-value)
                           (.setDate           prepared-statement param-index ^java.sql.Date param-value))
      :double            (.setDouble           prepared-statement param-index (double     param-value))
      :float             (.setFloat            prepared-statement param-index (float      param-value))
      :int               (.setInt              prepared-statement param-index (int        param-value))
      :long              (.setLong             prepared-statement param-index (long       param-value))
      :ncharacter-stream (.setNCharacterStream prepared-statement param-index ^Reader     param-value)
      :nclob             (.setNClob            prepared-statement param-index ^Reader     param-value)
      :nstring           (.setNString          prepared-statement param-index ^String     param-value)
      :object            (.setObject           prepared-statement param-index ^Object     param-value)
      :ref               (.setRef              prepared-statement param-index ^Ref        param-value)
      :row-id            (.setRowId            prepared-statement param-index ^RowId      param-value)
      :string            (.setString           prepared-statement param-index ^String     param-value)
      :sql-xml           (.setSQLXML           prepared-statement param-index ^setSQLXML  param-value)
      :time              (if (instance? Calendar param-value)
                           (.setTime           prepared-statement param-index (Time. (.getTimeInMillis ^Calendar param-value))
                             ^Calendar param-value)
                           (.setTime           prepared-statement param-index ^java.sql.Time param-value))
      :timestamp         (if (instance? Calendar param-value)
                           (.setTimestamp      prepared-statement param-index (Timestamp. (.getTimeInMillis ^Calendar param-value))
                             ^Calendar param-value)
                           (.setTimestamp      prepared-statement param-index ^java.sql.Timestamp param-value))
      :url               (.setURL              prepared-statement param-index ^URL        param-value)
      (i/expected-single-param-type param-type))))


;; ----- param laying helpers -----


(defmacro try-param
  [pindex-sym type-label param-expr]
  `(try ~param-expr
     (catch Exception e#
       (throw (IllegalArgumentException.
                (str "Error resolving SQL param " ~pindex-sym
                  ~(format " as %s: %s" type-label param-expr))
                e#)))))


(defmacro try-set
  [prepared-stmt pindex-sym type-label pvalue-expr pvalue-sym & body]
  `(let [~pvalue-sym (try-param ~pindex-sym ~type-label ~pvalue-expr)]
     (if (nil? ~pvalue-sym)
       (.setObject ~prepared-stmt ~pindex-sym nil)
       (try
         ~@body
         (catch ClassCastException e#
           (throw (IllegalArgumentException.
                    (str "Error setting SQL param " ~pindex-sym ~(apply str " as " type-label ": " body))
                    e#)))
         (catch Exception e#
           (throw (IllegalStateException.
                    (str "Error setting SQL param " ~pindex-sym ~(apply str " as " type-label ": " body))
                    e#)))))))


(defn lay-param-expr
  "Given prepared statement symbol, parm type, param index expression/symbol and param value expression/symbol, return
  an expression to set JDBC prepared statement param."
  [pstmt-sym param-type pidx-sym pval-sym]
  (when-not (contains? t/all-typemap param-type) (i/expected-param-type param-type))
  (if (contains? t/multi-typemap param-type)
    (let [i-sym (gensym "i-")
          v-sym (gensym "v-")]
      `(i/each-indexed [~i-sym ~pidx-sym
                        ~v-sym ~pval-sym]
         ~(lay-param-expr pstmt-sym (get t/multi-typemap param-type) i-sym v-sym)))
    (case (get t/single-typemap param-type)
      :nil               `(set-param-value ~pstmt-sym :nil ~pidx-sym ~pval-sym)
      :array             `(try-set ~pstmt-sym ~pidx-sym "array"             ~pval-sym v# (.setArray            ~pstmt-sym ~pidx-sym v#))
      :ascii-stream      `(try-set ~pstmt-sym ~pidx-sym "ascii-stream"      ~pval-sym v# (.setAsciiStream      ~pstmt-sym ~pidx-sym v#))
      :big-decimal       `(try-set ~pstmt-sym ~pidx-sym "big-decimal"       ~pval-sym v# (.setBigDecimal       ~pstmt-sym ~pidx-sym v#))
      :binary-stream     `(try-set ~pstmt-sym ~pidx-sym "binary-stream"     ~pval-sym v# (.setBinaryStream     ~pstmt-sym ~pidx-sym v#))
      :blob              `(try-set ~pstmt-sym ~pidx-sym "blob"              ~pval-sym v# (.setBlob             ~pstmt-sym ~pidx-sym v#))
      :boolean           `(try-set ~pstmt-sym ~pidx-sym "boolean"           ~pval-sym v# (.setBoolean          ~pstmt-sym ~pidx-sym (boolean
                                                                                                                                      v#)))
      :byte              `(try-set ~pstmt-sym ~pidx-sym "byte"              ~pval-sym v# (.setByte             ~pstmt-sym ~pidx-sym (byte v#))) 
      :byte-array        `(try-set ~pstmt-sym ~pidx-sym "byte-array"        ~pval-sym v# (.setBytes            ~pstmt-sym ~pidx-sym v#))
      :character-stream  `(try-set ~pstmt-sym ~pidx-sym "character-stream"  ~pval-sym v# (.setCharacterStream  ~pstmt-sym ~pidx-sym v#))
      :clob              `(try-set ~pstmt-sym ~pidx-sym "clob"              ~pval-sym v# (.setClob             ~pstmt-sym ~pidx-sym v#))
      :date              `(let [p# (try-param ~pidx-sym "date" ~pval-sym)]
                            (cond
                              (instance? Date p#) (.setDate ~pstmt-sym ~pidx-sym ^java.sql.Date p#)
                              ;; calendar
                              (instance? Calendar p#) (.setDate ~pstmt-sym ~pidx-sym
                                                        (Date. (.getTimeInMillis ^java.util.Calendar p#))
                                                        ^java.util.Calendar p#)
                              (nil? p#)    (.setDate ~pstmt-sym ~pidx-sym nil)
                              :otherwise   (i/expected "java.sql.Date or java.util.Calendar instance" p#)))
      :double            `(try-set ~pstmt-sym ~pidx-sym "double"            ~pval-sym v# (.setDouble           ~pstmt-sym ~pidx-sym (double v#)))
      :float             `(try-set ~pstmt-sym ~pidx-sym "float"             ~pval-sym v# (.setFloat            ~pstmt-sym ~pidx-sym (float v#)))
      :int               `(try-set ~pstmt-sym ~pidx-sym "int"               ~pval-sym v# (.setInt              ~pstmt-sym ~pidx-sym (int v#)))
      :long              `(try-set ~pstmt-sym ~pidx-sym "long"              ~pval-sym v# (.setLong             ~pstmt-sym ~pidx-sym (long v#)))
      :ncharacter-stream `(try-set ~pstmt-sym ~pidx-sym "ncharacter-stream" ~pval-sym v# (.setNCharacterStream ~pstmt-sym ~pidx-sym v#))
      :nclob             `(try-set ~pstmt-sym ~pidx-sym "nclob"             ~pval-sym v# (.setNClob            ~pstmt-sym ~pidx-sym v#))
      :nstring           `(try-set ~pstmt-sym ~pidx-sym "nstring"           ~pval-sym v# (.setNString          ~pstmt-sym ~pidx-sym v#))
      :object            `(.setObject ~pstmt-sym ~pidx-sym ~pval-sym)
      :ref               `(try-set ~pstmt-sym ~pidx-sym "ref"               ~pval-sym v# (.setRef              ~pstmt-sym ~pidx-sym v#))
      :row-id            `(try-set ~pstmt-sym ~pidx-sym "row-id"            ~pval-sym v# (.setRowId            ~pstmt-sym ~pidx-sym v#))
      :string            `(try-set ~pstmt-sym ~pidx-sym "string"            ~pval-sym v# (.setString           ~pstmt-sym ~pidx-sym v#))
      :sql-xml           `(try-set ~pstmt-sym ~pidx-sym "sql-xml"           ~pval-sym v# (.setSQLXML           ~pstmt-sym ~pidx-sym v#))
      :time              `(let [p# (try-param ~pidx-sym "time" ~pval-sym)]
                            (cond
                              (instance? Date p#) (.setTime ~pstmt-sym ~pidx-sym ^java.sql.Time p#)
                              ;; calendar
                              (instance? Calendar p#) (.setTime ~pstmt-sym ~pidx-sym
                                                        (Time. (.getTimeInMillis ^java.util.Calendar p#))
                                                        ^java.util.Calendar p#)
                              (nil? p#)    (.setTime ~pstmt-sym ~pidx-sym nil)
                              :otherwise   (i/expected "java.sql.Time or java.util.Calendar instance" p#)))
      :timestamp         `(let [p# (try-param ~pidx-sym "timestamp" ~pval-sym)]
                            (cond
                              (instance? Timestamp p#) (.setTimestamp ~pstmt-sym ~pidx-sym ^java.sql.Timestamp p#)
                              ;; calendar
                              (instance? Calendar p#) (.setTimestamp ~pstmt-sym ~pidx-sym
                                                        (Timestamp. (.getTimeInMillis ^java.util.Calendar p#))
                                                        ^java.util.Calendar p#)
                              (nil? p#)    (.setTimestamp ~pstmt-sym ~pidx-sym nil)
                              :otherwise   (i/expected "java.sql.Timestamp or java.util.Calendar instance" p#)))
      :url               `(try-set ~pstmt-sym ~pidx-sym "url"               ~pval-sym v# (.setURL              ~pstmt-sym ~pidx-sym v#))
      (i/expected-single-param-type param-type))))


(def cached-indices
  (memoize (fn [^long n]
             (apply vector-of :int (range n)))))
