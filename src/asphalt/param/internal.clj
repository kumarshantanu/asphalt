;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.param.internal
  (:require
    [asphalt.internal :as i]
    [asphalt.type :as t])
  (:import
    [java.util Calendar TimeZone]
    [java.sql Date PreparedStatement Time Timestamp]))


(def sql-single-type-map {nil         t/sql-nil
                          :bool       t/sql-bool
                          :boolean    t/sql-boolean
                          :byte       t/sql-byte
                          :byte-array t/sql-byte-array
                          :date       t/sql-date
                          :double     t/sql-double
                          :float      t/sql-float
                          :int        t/sql-int
                          :integer    t/sql-integer
                          :long       t/sql-long
                          :nstring    t/sql-nstring
                          :object     t/sql-object
                          :string     t/sql-string
                          :time       t/sql-time
                          :timestamp  t/sql-timestamp})


(def sql-single-type-keys (-> sql-single-type-map
                            (dissoc nil)
                            keys
                            vec))


(def sql-single-type-reverse-map (zipmap (vals sql-single-type-map) (keys sql-single-type-map)))


(def sql-multi-type-map {:bools       (bit-or t/sql-multi-bit t/sql-bool)
                         :booleans    (bit-or t/sql-multi-bit t/sql-boolean)
                         :bytes       (bit-or t/sql-multi-bit t/sql-byte)
                         :byte-arrays (bit-or t/sql-multi-bit t/sql-byte-array)
                         :dates       (bit-or t/sql-multi-bit t/sql-date)
                         :doubles     (bit-or t/sql-multi-bit t/sql-double)
                         :floats      (bit-or t/sql-multi-bit t/sql-float)
                         :ints        (bit-or t/sql-multi-bit t/sql-int)
                         :integers    (bit-or t/sql-multi-bit t/sql-integer)
                         :longs       (bit-or t/sql-multi-bit t/sql-long)
                         :nstrings    (bit-or t/sql-multi-bit t/sql-nstring)
                         :objects     (bit-or t/sql-multi-bit t/sql-object)
                         :strings     (bit-or t/sql-multi-bit t/sql-string)
                         :times       (bit-or t/sql-multi-bit t/sql-time)
                         :timestamps  (bit-or t/sql-multi-bit t/sql-timestamp)})


(def sql-multi-single-map {:bools       :bool
                           :booleans    :boolean
                           :bytes       :byte
                           :byte-arrays :byte-array
                           :dates       :date
                           :doubles     :double
                           :floats      :float
                           :ints        :int
                           :integers    :integer
                           :longs       :long
                           :nstrings    :nstring
                           :objects     :object
                           :strings     :string
                           :times       :time
                           :timestamps  :timestamp})


(def sql-all-types-map (merge sql-single-type-map sql-multi-type-map))


(def sql-all-types-keys (-> sql-single-type-map
                          (dissoc nil)
                          (merge sql-multi-type-map)
                          keys
                          vec))


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


(defn params-vec-indices
  [param-types-vec params-sym]
  (i/expected vector? "a vector of param types" param-types-vec)
  (if (every? sql-single-type-map param-types-vec)
    (->> (iterate inc 1)
      (take (count param-types-vec))
      (apply vector-of :int))
    (let [multi-counts (map-indexed (fn [^long idx pt]
                                      (if (contains? sql-single-type-map pt)
                                        `1
                                        `(count (get ~params-sym ~idx))))
                         param-types-vec)]
      `(reduce (fn [cv# ^long pcount#]
                 (conj cv# (unchecked-add (last cv#) pcount#)))
         (vector-of :int 1)
         [~@multi-counts]))))


(defn params-map-indices
  [param-keys-vec param-types-vec params-sym]
  (i/expected vector? "a vector of param keys" param-keys-vec)
  (i/expected vector? "a vector of param types" param-types-vec)
  (when-not (= (count param-keys-vec) (count param-types-vec))
    (i/expected "param keys and param types to be of same length" {:param-keys param-keys-vec
                                                                   :param-types param-types-vec}))
  (if (every? sql-single-type-map param-types-vec)
    (->> (iterate inc 1)
      (take (count param-types-vec))
      (apply vector-of :int))
    (let [multi-counts (map-indexed (fn [^long idx pt]
                                      (if (contains? sql-single-type-map pt)
                                        `1
                                        `(count (get ~params-sym ~(get param-keys-vec idx)))))
                         param-types-vec)]
      `(reduce (fn [cv# ^long pcount#]
                 (conj cv# (unchecked-add (last cv#) pcount#)))
         (vector-of :int 1)
         [~@multi-counts]))))


;; ----- param laying helpers -----


(defmacro try-param
  [pindex-sym type-label param-expr]
  `(try ~param-expr
     (catch Exception e#
       (throw (IllegalArgumentException.
                (str "Error resolving SQL param " ~pindex-sym
                  ~(format " as %s: %s" type-label param-expr))
                e#)))))


(defmacro each-param-expr
  "Return param setter expression."
  [setter pstmt-sym pindex-sym type-label param-expr]
  `(~setter ~pstmt-sym ~pindex-sym
     (try-param ~pindex-sym ~type-label ~param-expr)))


(defn lay-param-expr
  "Given prepared statement symbol, parm type, param index expression/symbol and param value expression/symbol, return
  an expression to set JDBC prepared statement param."
  [pstmt-sym param-type pindex-sym pvalue-sym]
  (i/expected sql-all-types-map (str "valid SQL param type - either of " sql-all-types-keys) param-type)
  (let [ptype-num (int (get sql-all-types-map param-type))
        ptype-key (get sql-single-type-reverse-map (bit-and t/sql-type-bits ptype-num) :invalid)]
    (if (pos? (bit-and t/sql-multi-bit ptype-num))  ; multi-bit?
      (let [i-sym (gensym "i-")
            v-sym (gensym "v-")]
        `(i/loop-indexed [~i-sym ~pindex-sym
                          ~v-sym ~pvalue-sym]
           ~(lay-param-expr pstmt-sym ptype-key i-sym v-sym)))
      (case ptype-key
        nil         `(param-value! ~pstmt-sym ~pindex-sym ~pvalue-sym)
        :bool       `(each-param-expr ~'.setBoolean   ~pstmt-sym ~pindex-sym "boolean"    (boolean    ~pvalue-sym))
        :boolean    `(each-param-expr ~'.setBoolean   ~pstmt-sym ~pindex-sym "boolean"    (boolean    ~pvalue-sym))
        :byte       `(each-param-expr ~'.setByte      ~pstmt-sym ~pindex-sym "byte"       (byte       ~pvalue-sym))
        :byte-array `(each-param-expr ~'.setBytes     ~pstmt-sym ~pindex-sym "byte-array" (byte-array ~pvalue-sym))
        :date       `(let [p# (try-param ~pindex-sym "date" ~pvalue-sym)]
                       (cond
                         (instance? Date p#) (.setDate ~pstmt-sym ~pindex-sym ^java.sql.Date p#)
                         ;; calendar
                         (instance? Calendar p#) (.setDate ~pstmt-sym ~pindex-sym
                                                   (Date. (.getTimeInMillis ^java.util.Calendar p#))
                                                   ^java.util.Calendar p#)
                         (nil? p#)    (.setDate ~pstmt-sym ~pindex-sym nil)
                         :otherwise   (i/expected "java.sql.Date or java.util.Calendar instance" p#)))
        :double     `(each-param-expr ~'.setDouble    ~pstmt-sym ~pindex-sym "double"     (double     ~pvalue-sym))
        :float      `(each-param-expr ~'.setFloat     ~pstmt-sym ~pindex-sym "float"      (float      ~pvalue-sym))
        :int        `(each-param-expr ~'.setInt       ~pstmt-sym ~pindex-sym "int"        (int        ~pvalue-sym))
        :integer    `(each-param-expr ~'.setInt       ~pstmt-sym ~pindex-sym "int"        (int        ~pvalue-sym))
        :long       `(each-param-expr ~'.setLong      ~pstmt-sym ~pindex-sym "long"       (long       ~pvalue-sym))
        :nstring    `(each-param-expr ~'.setNString   ~pstmt-sym ~pindex-sym "string"     ~pvalue-sym)
        :object     `(each-param-expr ~'.setObject    ~pstmt-sym ~pindex-sym "object"     ~pvalue-sym)
        :string     `(each-param-expr ~'.setString    ~pstmt-sym ~pindex-sym "string"     ~pvalue-sym)
        :time       `(let [p# (try-param ~pindex-sym "time" ~pvalue-sym)]
                       (cond
                         (instance? Date p#) (.setTime ~pstmt-sym ~pindex-sym ^java.sql.Time p#)
                         ;; calendar
                         (instance? Calendar p#) (.setTime ~pstmt-sym ~pindex-sym
                                                   (Time. (.getTimeInMillis ^java.util.Calendar p#))
                                                   ^java.util.Calendar p#)
                         (nil? p#)    (.setTime ~pstmt-sym ~pindex-sym nil)
                         :otherwise   (i/expected "java.sql.Time or java.util.Calendar instance" p#)))
        :timestamp  `(let [p# (try-param ~pindex-sym "timestamp" ~pvalue-sym)]
                       (cond
                         (instance? Timestamp p#) (.setTimestamp ~pstmt-sym ~pindex-sym ^java.sql.Timestamp p#)
                         ;; calendar
                         (instance? Calendar p#) (.setTimestamp ~pstmt-sym ~pindex-sym
                                                   (Timestamp. (.getTimeInMillis ^java.util.Calendar p#))
                                                   ^java.util.Calendar p#)
                         (nil? p#)    (.setTimestamp ~pstmt-sym ~pindex-sym nil)
                         :otherwise   (i/expected "java.sql.Timestamp or java.util.Calendar instance" p#)))
        (i/expected (str "a valid SQL param type - either of " sql-single-type-keys) param-type)))))


(defn set-param-value
  [^PreparedStatement prepared-statement param-type ^long param-index param-value]
  (case param-type
    nil         (if (instance? clojure.lang.BigInt param-value)
                  (.setLong   prepared-statement param-index ^long (long param-value))
                  (.setObject prepared-statement param-index ^Object param-value))
    :bool       (.setBoolean   prepared-statement param-index (boolean    param-value))
    :boolean    (.setBoolean   prepared-statement param-index (boolean    param-value))
    :byte       (.setByte      prepared-statement param-index (byte       param-value))
    :byte-array (.setBytes     prepared-statement param-index (byte-array param-value))
    :date       (if (instance? Calendar param-value)
                  (.setDate    prepared-statement param-index (Date. (.getTimeInMillis ^Calendar param-value))
                    ^Calendar param-value)
                  (.setDate    prepared-statement param-index ^java.sql.Date param-value))
    :double     (.setDouble    prepared-statement param-index (double     param-value))
    :float      (.setFloat     prepared-statement param-index (float      param-value))
    :int        (.setInt       prepared-statement param-index (int        param-value))
    :integer    (.setInt       prepared-statement param-index (int        param-value))
    :long       (.setLong      prepared-statement param-index (long       param-value))
    :nstring    (.setNString   prepared-statement param-index ^String param-value)
    :object     (.setObject    prepared-statement param-index ^Object param-value)
    :string     (.setString    prepared-statement param-index ^String param-value)
    :time       (if (instance? Calendar param-value)
                  (.setTime      prepared-statement param-index (Time. (.getTimeInMillis ^Calendar param-value))
                    ^Calendar param-value)
                  (.setTime      prepared-statement param-index ^java.sql.Time param-value))
    :timestamp  (if (instance? Calendar param-value)
                  (.setTimestamp prepared-statement param-index (Timestamp. (.getTimeInMillis ^Calendar param-value))
                    ^Calendar param-value)
                  (.setTimestamp prepared-statement param-index ^java.sql.Timestamp param-value))
    (i/expected (str "a valid SQL param type - either of " sql-single-type-keys) param-type)))


(def cached-indices
  (memoize (fn [^long n]
             (apply vector-of :int (range n)))))
