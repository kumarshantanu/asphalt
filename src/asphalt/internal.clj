(ns asphalt.internal
  (:require
    [clojure.string :as str])
  (:import
    [java.util.regex Pattern]
    [java.sql Blob Clob Date Time Timestamp
              Connection PreparedStatement Statement
              ResultSet ResultSetMetaData]))


;; ----- error reporting -----


(defn illegal-arg
  [msg & more]
  (throw
    ^IllegalArgumentException (cond
                                (instance? Throwable msg)         (IllegalArgumentException.
                                                                    ^String (str/join \space more)
                                                                    ^Throwable msg)
                                (instance? Throwable (last more)) (IllegalArgumentException.
                                                                    ^String (str/join \space (cons msg (butlast more)))
                                                                    ^Throwable (last more))
                                :otherwise (IllegalArgumentException. ^String (str/join \space (cons msg more))))))


(defn unexpected
  "Report error about expectation mismatch."
  [msg found]
  (illegal-arg "Expected" msg "but found:" (class found) (pr-str found)))


(defn assert-symbols
  [& syms]
  (doseq [x syms]
    (when-not (symbol? x)
      (unexpected "a symbol" x))))


;; ----- utilities -----


(defmacro loop-indexed
  [[counter init-count iteratee coll & more-pairs] & body]
  (assert-symbols counter iteratee)
  (->> (partition 2 more-pairs)
    (map first)
    (apply assert-symbols))
  (when (odd? (count more-pairs))
    (unexpected "an even number of forms in binding vector" more-pairs))
  (let [more-syms (->> (partition 2 more-pairs)
                    (map first))
        more-seq  (map  (fn [x] `(seq ~x)) more-syms)
        more-rest (map  (fn [x] `(rest ~x)) more-syms)]
    `(loop [~counter ~init-count
            ~iteratee ~coll
            ~@more-pairs]
      (when (and (seq ~iteratee) ~@more-seq)
        (do ~@body)
        (recur (inc ~counter) (rest ~iteratee) ~@more-rest)))))


;; ----- type definitions -----


(def supported-sql-types #{nil :boolean :byte :byte-array :date :double :float :int :integer :long
                           :nstring :object :string :time :timestamp})


(defrecord SQLTemplate
  [^String sql param-pairs ^int param-count result-column-types ^int result-column-count])


(definline param-pairs [^SQLTemplate x] `(:param-pairs ~x))


(defn sql-template?
  "Return true if specified arg is an instance of SQLTemplate, false otherwise."
  [x]
  (instance? SQLTemplate x))


(definline resolve-sql [x] `(if (sql-template? ~x) (:sql ~x) ~x))


(defn make-sql-template
  "Given SQL template arguments create an SQLTemplate instance. Example args are below:
  sql - \"SELECT name, salary FROM emp WHERE salary > ? AND dept = ?\"
  param-pairs - [[:salary :int] [:dept]]
  result-column-types - [:string :int]"
  ^SQLTemplate [sql param-pairs result-column-types]
  (when-not (string? sql)
    (unexpected "SQL string" sql))
  (when-not (and (vector? param-pairs)
              (every? vector? param-pairs)
              (every? #(<= 1 (count %) 2) param-pairs))
    (unexpected "param-pairs vector with 1 or 2 element vectors of param name and optional type" param-pairs))
  (when-not (vector? result-column-types)
    (unexpected "result-column-types vector" result-column-types))
  (let [param-types (map second param-pairs)]
    (when-not (every? #(contains? supported-sql-types %) param-types)
      (unexpected (str "every param type to be one of " supported-sql-types) param-pairs)))
  (when-not (every? #(contains? supported-sql-types %) result-column-types)
    (unexpected (str "every result column type to be one of " supported-sql-types) result-column-types))
  (->SQLTemplate sql param-pairs (count param-pairs) result-column-types (count result-column-types)))


;; ----- SQL parsing helpers -----


(defn valid-type-char?
  [^String partial-name ch]
  (if (empty? partial-name)
    (Character/isJavaIdentifierStart ^char ch)
    (or
      (= ^char ch \-)  ; for keywords with hyphens
      (Character/isJavaIdentifierPart ^char ch))))


(defn valid-name-char?
  [type-start-char ^String partial-name ch]
  (cond
    (empty? partial-name)  (Character/isJavaIdentifierStart ^char ch)
    (= type-start-char
      (last partial-name)) (Character/isJavaIdentifierStart ^char ch)
    :otherwise             (or
                             (Character/isJavaIdentifierPart ^char ch)
                             (= ^char ch \-)  ; for keywords with hyphens
                             (and (seq partial-name)
                               (= -1 (.indexOf partial-name (int type-start-char)))
                               (= ^char ch ^char type-start-char)))))


(defn split-param-name-and-type
  "Given a string containing parameter name and (optional) type delimited by type-separator char, return either a
  two-element vector containing parameter name and type, or a one-element vector containing only the parameter name."
  [type-separator ^String param-name-and-type]  ; type is optional
  (let [ts-index (.indexOf param-name-and-type (int type-separator))]
    (when (= ts-index (dec (count param-name-and-type)))
      (illegal-arg "Param key cannot end with type-separator: " param-name-and-type))
    (when (not= ts-index (.lastIndexOf param-name-and-type (int type-separator)))
      (illegal-arg "Multiple type separators found in the same param key:" param-name-and-type))
    (str/split param-name-and-type
      (re-pattern (Pattern/quote ^String (str ^char type-separator))))))


;; ----- statement helpers -----


(defn prepare-statement
  ^PreparedStatement [^Connection connection ^String sql return-generated-keys?]
  (if return-generated-keys?
    (.prepareStatement connection sql Statement/RETURN_GENERATED_KEYS)
    (.prepareStatement connection sql)))


;; ----- params helpers -----


(defn set-param-value!
  ([^PreparedStatement prepared-statement ^long param-index value]
    (cond
      (instance? Integer             value) (.setInt       prepared-statement param-index ^int value)
      (instance? Long                value) (.setLong      prepared-statement param-index ^long value)
      (instance? clojure.lang.BigInt value) (.setLong     prepared-statement param-index ^long (long value))
      (instance? String              value) (.setString    prepared-statement param-index ^String value)
      (instance? Timestamp           value) (.setTimestamp prepared-statement param-index ^Timestamp value)
      :otherwise                            (.setObject    prepared-statement param-index ^Object value)))
  ([^PreparedStatement prepared-statement ^long param-index param-type param-value]
    (case param-type
      nil         (set-param-value! prepared-statement param-index param-value)
      :boolean    (.setBoolean   prepared-statement param-index (boolean    param-value))
      :byte       (.setByte      prepared-statement param-index (byte       param-value))
      :byte-array (.setBytes     prepared-statement param-index (byte-array param-value))
      :date       (.setDate      prepared-statement param-index ^java.sql.Date param-value)
      :double     (.setDouble    prepared-statement param-index (double     param-value))
      :float      (.setFloat     prepared-statement param-index (float      param-value))
      :int        (.setInt       prepared-statement param-index (int        param-value))
      :integer    (.setInt       prepared-statement param-index (int        param-value))
      :long       (.setLong      prepared-statement param-index (long       param-value))
      :nstring    (.setNString   prepared-statement param-index ^String param-value)
      :string     (.setString    prepared-statement param-index ^String param-value)
      :time       (.setTime      prepared-statement param-index ^java.sql.Time param-value)
      :timestamp  (.setTimestamp prepared-statement param-index ^java.sql.Timestamp param-value)
      (unexpected (str "either of " (pr-str supported-sql-types)) param-type))))


(defn set-params!
  ([^PreparedStatement prepared-statement params]
    (loop-indexed [i 1
                   ps params]
      (set-param-value! prepared-statement i (first ps))))
  ([^PreparedStatement prepared-statement param-types params]
    (if (map? params)
      (loop-indexed [i (int 1)
                     ts param-types]
        (let [[param-key param-type] (first ts)]
          (if (contains? params param-key)
            (set-param-value! prepared-statement i param-type (get params param-key))
            (illegal-arg "No value found for key:" param-key "in" (pr-str params)))))
      (loop-indexed [i (int 1)
                     ts param-types
                     ps params]
        (set-param-value! prepared-statement i (first ts) (first ps))))))


;; ----- result-set stuff -----


(defn read-column-value
  "Read column value from given ResultSet instance and integer column-index."
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
  ([^ResultSet result-set ^long column-index column-type]
    (case column-type
      nil         (read-column-value result-set column-index)
      :boolean    (.getBoolean   result-set column-index)
      :byte       (.getByte      result-set column-index)
      :byte-array (.getBytes     result-set column-index)
      :date       (.getDate      result-set column-index)
      :double     (.getDouble    result-set column-index)
      :float      (.getFloat     result-set column-index)
      :int        (.getInt       result-set column-index)
      :integer    (.getInt       result-set column-index)
      :long       (.getLong      result-set column-index)
      :nstring    (.getNString   result-set column-index)
      :string     (.getString    result-set column-index)
      :time       (.getTime      result-set column-index)
      :timestamp  (.getTimestamp result-set column-index)
      (unexpected (str "either of " (pr-str supported-sql-types)) column-type))))


(defn read-result-row
  "Given a java.sql.ResultSet instance, read exactly one row as a vector and return the same."
  [^ResultSet result-set column-count-or-types]
  (let [row (transient [])]
    (if (integer? column-count-or-types)
      (let [column-count (int column-count-or-types)]
        (loop [i (int 1)]
          (when (<= i column-count)
            (do
              (conj! row (read-column-value result-set i))
              (recur (inc i))))))
      (loop-indexed [index (int 1)
                     column-type column-count-or-types]
        (conj! row (read-column-value result-set index column-type))))
    (persistent! row)))


;; ----- transaction stuff -----


(def isolation-levels #{Connection/TRANSACTION_NONE
                        Connection/TRANSACTION_READ_COMMITTED
                        Connection/TRANSACTION_READ_UNCOMMITTED
                        Connection/TRANSACTION_REPEATABLE_READ
                        Connection/TRANSACTION_SERIALIZABLE})


(defn resolve-txn-isolation
  ^long [isolation]
  (if (isolation-levels isolation)
    isolation
    (condp = isolation
      :none             Connection/TRANSACTION_NONE
      :read-committed   Connection/TRANSACTION_READ_COMMITTED
      :read-uncommitted Connection/TRANSACTION_READ_UNCOMMITTED
      :repeatable-read  Connection/TRANSACTION_REPEATABLE_READ
      :serializable     Connection/TRANSACTION_SERIALIZABLE
      (illegal-arg "Expected either of" (pr-str isolation-levels)
        "or an integer representing a java.sql.Connection/TRANSACTION_XXX value, but found"
        (class isolation) (pr-str isolation)))))