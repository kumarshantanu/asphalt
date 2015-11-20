(ns asphalt.internal
  (:refer-clojure :exclude [update])
  (:require
    [clojure.string :as str]
    [asphalt.type   :as t])
  (:import
    [java.io         Writer]
    [java.sql        Blob Clob Date Time Timestamp
                     Connection DriverManager PreparedStatement Statement
                     ResultSet ResultSetMetaData
                     Savepoint]
    [java.util       Hashtable Map Properties]
    [java.util.regex Pattern]
    [javax.naming    Context InitialContext]
    [javax.sql       DataSource]
    [asphalt.instrument JdbcEventFactory JdbcEventListener]))


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
        (recur (unchecked-inc ~counter) (rest ~iteratee) ~@more-rest)))))


(defn as-str
  ^String [x]
  (if (instance? clojure.lang.Named x)
    (name x)
    (str x)))


;; ----- type definitions -----


(def sql-type-map {nil         t/sql-nil
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


(def supported-sql-types (str "either of " (->> (keys sql-type-map)
                                             (remove nil?)
                                             (map name)
                                             (str/join ", "))))


(def no-param-type-vec
  (let [param-types-nil (repeat t/sql-nil)]
    (memoize (fn [^long n]
               (->> (take n param-types-nil)
                 (apply vector-of :byte))))))


;; ----- SQL parsing helpers -----


(def encode-name keyword)


(defn encode-type
  [^String token ^String sql]
  (let [k (keyword token)]
    (when-not (contains? sql-type-map k)
      (unexpected (str supported-sql-types " in SQL string: " sql) token))
    (get sql-type-map k)))


(defn valid-name-char?
  [^StringBuilder partial-name ch]
  (if (empty? partial-name)
    (Character/isJavaIdentifierStart ^char ch)
    (or
      (Character/isJavaIdentifierPart ^char ch)
      ;; for keywords with hyphens
      (= ^char ch \-))))


(defn valid-type-char?
  [^StringBuilder partial-name ch]
  (if (empty? partial-name)
    (Character/isJavaIdentifierStart ^char ch)
    (or
      (Character/isJavaIdentifierPart ^char ch)
      ;; for keywords with hyphens
      (= ^char ch \-))))


(def initial-parser-state
  {:c? false ; SQL comment in progress?
   :d? false ; double-quote string in progress?
   :e? false ; current escape state
   :n? false ; named param in progress
   :ns nil   ; partial named param string
   :s? false ; single-quote string in progress?
   :t? false ; type-hinted token in progress
   :ts nil   ; partial type-hinted token string
   })


(defn finalize-parser-state
  "Verify that final state is sane and clean up any unfinished stuff."
  [sql ec name-handler parser-state]
  (when (:d? parser-state) (illegal-arg "SQL cannot end with incomplete double-quote token:" sql))
  (when (:e? parser-state) (illegal-arg (format "SQL cannot end with a dangling escape character '%s':" ec) sql))
  (let [parser-state (merge parser-state (when (:n? parser-state)
                                           (name-handler (:ns parser-state) (:ts parser-state))
                                           {:ts nil :n? false :ns nil}))]
    (when (:s? parser-state) (illegal-arg "SQL cannot end with incomplete single-quote token:" sql))
    (when (:t? parser-state) (illegal-arg "SQL cannot end with a type hint"))
    (when (:ts parser-state) (illegal-arg "SQL cannot end with a type hint"))))


(defn update-param-name!
  "Update named param name."
  [^StringBuilder sb parser-state ch mc special-chars name-handler delta-state]
  (let [^StringBuilder nsb (:ns parser-state)]
    (if (valid-name-char? nsb ch)
      (do (.append nsb ^char ch)
        nil)
      (if-let [c (special-chars ^char ch)]
        (illegal-arg (format "Named parameter cannot precede special chars %s: %s%s%s%s"
                       (pr-str special-chars) sb mc nsb c))
        (do
          (name-handler nsb (:ts parser-state))
          (.append sb ^char ch)
          delta-state)))))


(defn update-type-hint!
  [^StringBuilder sb parser-state ch tc delta-state]
  (let [^StringBuilder tsb (:ts parser-state)]
    (cond
      ;; type char
      (valid-type-char? tsb ^char ch)
      (do (.append tsb ^char ch)        nil)
      ;; whitespace implies type has ended
      (Character/isWhitespace ^char ch) delta-state
      ;; catch-all default case
      :otherwise (illegal-arg
                   (format "Expected type-hint '%s%s' to precede a whitespace, but found '%s': %s%s%s%s"
                     tc (:ts parser-state) ch
                     sb tc (:ts parser-state) ch)))))


(defn encounter-sql-comment
  [^StringBuilder sb delta-state]
  (let [idx (unchecked-subtract (.length sb) 2)]
    (when (and (>= idx 0)
            (= (.charAt sb idx) \-))
      delta-state)))


(defn parse-sql-str
  "Parse SQL string using escape char, named-param char and type-hint char, returning [sql named-params return-col-types]"
  [^String sql ec mc tc]
  (let [^char ec ec ^char mc mc ^char tc tc nn (count sql)
        ^StringBuilder sb (StringBuilder. nn)
        ks (transient [])  ; param keys
        ts (transient [])  ; result column types
        handle-named! (fn [^StringBuilder buff ^StringBuilder param-type]
                        (.append sb \?)
                        (conj! ks [(.toString buff) (when param-type (.toString param-type))]))
        handle-typed! (fn [^StringBuilder buff] (conj! ts (.toString buff)))
        special-chars #{ec mc tc \" \'}]
    (loop [i 0 ; current index
           s initial-parser-state]
      (if (>= i nn)
        (finalize-parser-state sql ec handle-named! s)
        (let [ch (.charAt sql i)
              ps (merge s
                   (condp s false
                    :c? (do (.append sb ch) (when (= ch \newline) {:c? false})) ; SQL comment in progress
                    :d? (do (.append sb ch) (when (= ch \")       {:d? false})) ; double-quote string in progress
                    :e? (do (.append sb ch)                       {:e? false})  ; escape state
                    :n? (update-param-name!  ; clear :ts at end
                          sb s ch mc special-chars handle-named!  {:ts nil :n? false :ns nil}) ; named param in progress
                    :s? (do (.append sb ch) (when (= ch \')       {:s? false})) ; single-quote string in progress
                    :t? (update-type-hint! sb s ch tc             {:t? false})  ; type-hint in progress (:ts intact)
                    (condp = ch  ; catch-all
                      mc                                          {:n? true :ns (StringBuilder.)}
                      (do (when-let [^StringBuilder tsb (:ts s)]
                            (handle-typed! tsb))
                        (merge {:ts nil}
                          (condp = ch
                            ec                                    {:e? true}
                            tc                                    {:t? true :ts (StringBuilder.)}
                            (do (.append sb ch)
                              (case ch
                                \"                                {:d? true}
                                \'                                {:s? true}
                                \- (encounter-sql-comment sb      {:c? true})
                                nil))))))))]
          (recur (unchecked-inc i) ps))))
    [(.toString sb) (persistent! ks) (persistent! ts)]))


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
  ([^PreparedStatement prepared-statement ^long param-index ^long param-type param-value]
    (case param-type
      #_sql-nil         0 (set-param-value! prepared-statement param-index param-value)
      #_sql-boolean     1 (.setBoolean   prepared-statement param-index (boolean    param-value))
      #_sql-byte        2 (.setByte      prepared-statement param-index (byte       param-value))
      #_sql-byte-array  3 (.setBytes     prepared-statement param-index (byte-array param-value))
      #_sql-date        4 (.setDate      prepared-statement param-index ^java.sql.Date param-value)
      #_sql-double      5 (.setDouble    prepared-statement param-index (double     param-value))
      #_sql-float       6 (.setFloat     prepared-statement param-index (float      param-value))
      #_sql-int         7 (.setInt       prepared-statement param-index (int        param-value))
      #_sql-long        8 (.setLong      prepared-statement param-index (long       param-value))
      #_sql-nstring     9 (.setNString   prepared-statement param-index ^String param-value)
      #_sql-object     10 (.setObject    prepared-statement param-index ^Object param-value)
      #_sql-string     11 (.setString    prepared-statement param-index ^String param-value)
      #_sql-time       12 (.setTime      prepared-statement param-index ^java.sql.Time param-value)
      #_sql-timestamp  13 (.setTimestamp prepared-statement param-index ^java.sql.Timestamp param-value)
      (unexpected supported-sql-types param-type))))


(defn set-params!
  ([^PreparedStatement prepared-statement params]
    (cond
      (vector? params) (let [param-count (count params)]
                         (loop [i (int 0)]
                           (when (< i param-count)
                             (let [j (unchecked-inc i)]
                               (set-param-value! prepared-statement j (get params i))
                               (recur j)))))
      (nil? params)    nil
      :otherwise       (unexpected "params vector" params)))
  ([^PreparedStatement prepared-statement ^objects param-keys ^bytes param-types params]
    (cond
      (map? params)    (let [param-count (alength param-keys)]
                         (loop [i (int 0)]
                           (when (< i param-count)
                             (let [param-key  (aget param-keys i)
                                   param-type (aget param-types i)
                                   j (unchecked-inc i)]
                               (if (contains? params param-key)
                                 (try
                                   (set-param-value! prepared-statement j param-type (get params param-key))
                                   (catch RuntimeException e
                                     (throw (ex-info (str "Error setting parameter #" j " (" param-key "): "
                                                       (.getMessage e))
                                              {:param-index j :param-key param-key} e))))
                                 (illegal-arg "No value found for key:" param-key "in" (pr-str params)))
                               (recur j)))))
      (vector? params) (let [types-count (alength param-keys)
                             param-count (count params)]
                         (loop [i (int 0)]
                           (when (< i param-count)
                             (let [j (unchecked-inc i)]
                               (if (< i types-count)
                                 (let [param-key  (aget param-keys i)
                                       param-type (aget param-types i)]
                                   (try
                                     (set-param-value! prepared-statement j param-type (get params i))
                                     (catch RuntimeException e
                                       (throw (ex-info (str "Error setting parameter #" j " (" param-key "): "
                                                         (.getMessage e))
                                                {:param-index j :param-key param-key} e)))))
                                 (try
                                   (set-param-value! prepared-statement j t/sql-nil (get params i))
                                   (catch RuntimeException e
                                     (throw (ex-info (str "Error setting parameter #" j ": " (.getMessage e))
                                              {:param-index j} e)))))
                               (recur j)))))
      (nil? params)    nil
      :otherwise       (unexpected "map or vector" params)))
  ([^PreparedStatement prepared-statement #_vector param-keys #_vector param-types _ params]
    (cond
      (map? params)    (let [param-count (count param-keys)]
                         (loop [i (int 0)]
                           (when (< i param-count)
                             (let [param-key  (get param-keys i)
                                   param-type (get param-types i)
                                   j (unchecked-inc i)]
                               (if (contains? params param-key)
                                 (try
                                   (set-param-value! prepared-statement j param-type (get params param-key))
                                   (catch RuntimeException e
                                     (throw (ex-info (str "Error setting parameter #" j " (" param-key "): "
                                                       (.getMessage e))
                                              {:param-index j :param-key param-key} e))))
                                 (illegal-arg "No value found for key:" param-key "in" (pr-str params)))
                               (recur j)))))
      (vector? params) (let [types-count (count param-keys)
                             param-count (count params)]
                         (loop [i (int 0)]
                           (when (< i param-count)
                             (let [j (unchecked-inc i)]
                               (if (< i types-count)
                                 (let [param-key  (get param-keys i)
                                       param-type (get param-types i)]
                                   (try
                                     (set-param-value! prepared-statement j param-type (get params i))
                                     (catch RuntimeException e
                                       (throw (ex-info (str "Error setting parameter #" j " (" param-key "): "
                                                         (.getMessage e))
                                                {:param-index j :param-key param-key} e))))
                                   (try
                                     (set-param-value! prepared-statement j t/sql-nil (get params i))
                                     (catch RuntimeException e
                                       (throw (ex-info (str "Error setting parameter #" j ": " (.getMessage e))
                                                {:param-index j} e))))))
                               (recur j)))))
      (nil? params)    nil
      :otherwise       (unexpected "map or vector" params))))


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
  ([^ResultSet result-set ^long column-index ^long column-type]
    (case column-type
      #_sql-nil         0 (read-column-value result-set column-index)
      #_sql-boolean     1 (.getBoolean   result-set column-index)
      #_sql-byte        2 (.getByte      result-set column-index)
      #_sql-byte-array  3 (.getBytes     result-set column-index)
      #_sql-date        4 (.getDate      result-set column-index)
      #_sql-double      5 (.getDouble    result-set column-index)
      #_sql-float       6 (.getFloat     result-set column-index)
      #_sql-int         7 (.getInt       result-set column-index)
      #_sql-long        8 (.getLong      result-set column-index)
      #_sql-nstring     9 (.getNString   result-set column-index)
      #_sql-object     10 (.getObject    result-set column-index)
      #_sql-string     11 (.getString    result-set column-index)
      #_sql-time       12 (.getTime      result-set column-index)
      #_sql-timestamp  13 (.getTimestamp result-set column-index)
      (unexpected supported-sql-types column-type))))


(defn read-columns
  (^objects [^ResultSet result-set ^long column-count]
    (let [^objects row (object-array column-count)]
      (loop [i (int 0)]
        (when (< i column-count)
          (let [j (unchecked-inc i)]
            (aset row i (read-column-value result-set j))
            (recur j))))
      row))
  (^objects [^ResultSet result-set ^long column-count ^bytes column-types]
    (let [^objects row (object-array column-count)]
      (loop [i (int 0)]
        (when (< i column-count)
          (let [j (unchecked-inc i)]
            (aset row i (read-column-value result-set j (aget ^bytes column-types i)))
            (recur j))))
      row))
  (^objects [^ResultSet result-set ^long column-count column-types _]
    (let [^objects row (object-array column-count)]
      (loop [i (int 0)]
        (when (< i column-count)
          (let [j (unchecked-inc i)]
            (aset row i (read-column-value result-set j (get column-types i)))
            (recur j))))
      row)))


;; ----- transaction stuff -----


(defn set-txn-info
  [^Connection connection options]
  (when (contains? options :auto-commit?)
    (.setAutoCommit connection (:auto-commit? options)))
  (when (contains? options :isolation)
    (.setTransactionIsolation connection (:isolation options))))


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


;; Spring (and EJB) transaction propagation
;; http://docs.spring.io/spring/docs/current/javadoc-api/org/springframework/transaction/annotation/Propagation.html
(def propagation #{:mandatory
                   :nested
                   :never
                   :not-supported
                   :required
                   :requires-new
                   :supports})


;; ----- protocol stuff -----


(extend-protocol t/IConnectionSource
  java.sql.Connection
  (obtain-connection      [this] this)
  (return-connection [this conn] (comment "do nothing"))
  javax.sql.DataSource
  (obtain-connection      [this] (.getConnection ^DataSource this))
  (return-connection [this conn] (.close ^Connection conn))
  java.util.Map
  (obtain-connection      [this] (let [{:keys [connection
                                               factory
                                               classname connection-uri
                                               subprotocol subname
                                               datasource username user password
                                               name context environment]} this]
                                   (cond
                                     connection           connection
                                     factory              (factory (dissoc this :factory))
                                     (or (and datasource username password)
                                       (and datasource
                                         user password))  (.getConnection ^DataSource datasource
                                                            ^String (or username user) ^String password)
                                     datasource           (.getConnection ^DataSource datasource)
                                     (or (and connection-uri username password)
                                       (and connection-uri
                                         user password))  (do
                                                            (when classname
                                                              (Class/forName ^String classname))
                                                            (DriverManager/getConnection connection-uri
                                                              ^String (or username user) ^String password))
                                     connection-uri       (do
                                                            (when classname
                                                              (Class/forName ^String classname))
                                                            (DriverManager/getConnection connection-uri))
                                     (and subprotocol
                                       subname)           (let [url (format "jdbc:%s:%s" subprotocol subname)
                                                                cfg (dissoc this :classname :subprotocol :subname)]
                                                            (when classname
                                                              (Class/forName classname))
                                                            (DriverManager/getConnection
                                                              url (let [p (Properties.)]
                                                                    (doseq [[k v] (seq cfg)]
                                                                      (.setProperty p (as-str k) v))
                                                                    p)))
                                     name                 (let [^Context c (or context
                                                                             (InitialContext.
                                                                               (and environment
                                                                                 (Hashtable. ^Map environment))))
                                                                ^DataSource d (.lookup c ^String name)]
                                                            (.getConnection d)))))
  (return-connection [this conn] (when-not (:connection this)
                                   (.close ^Connection conn))))


(defrecord SQLTemplate
  [^String sql ^objects param-keys ^bytes param-types ^bytes result-types])


(defmethod print-method SQLTemplate [^SQLTemplate obj ^Writer w]
  (let [m {:sql          (.-sql obj)
           :param-keys   (vec (.-param-keys obj))
           :param-types  (vec (.-param-types obj))
           :result-types (vec (.-result-types obj))}]
    (.write w (str "#SQLTemplate" (pr-str m)))))


(defn make-sql-template
  "Given SQL template arguments create an SQLTemplate instance. Example args are below:
  sql - \"SELECT name, salary FROM emp WHERE salary > ? AND dept = ?\"
  param-pairs - [[:salary :int] [:dept]]
  result-column-types - [:string :int]"
  ^SQLTemplate [sql param-pairs result-column-types]
  (when-not (string? sql)
    (unexpected "SQL string" sql))
  (let [param-keys (map first param-pairs)
        param-types (map second param-pairs)]
    (->SQLTemplate sql (object-array param-keys) (byte-array param-types) (byte-array result-column-types))))


(extend-protocol t/ISql
  ;;=========
  SQLTemplate
  ;;=========
  (get-sql    [template] (.-sql template))
  (set-params [template ^PreparedStatement prepared-statement params]
    (set-params! prepared-statement ^objects (.-param-keys template) ^bytes (.-param-types template) params))
  (read-col   [template ^ResultSet result-set ^long column-index]
    (let [^bytes types (.-result-types template)]
      (if (pos? (alength types))
        (let [column-type (aget types (unchecked-dec column-index))]
          (read-column-value result-set column-index column-type))
        (read-column-value result-set column-index))))
  (read-row   [template ^ResultSet result-set ^long column-count]
    (let [^bytes types (.-result-types template)]
      (if (pos? (alength types))
        (read-columns result-set column-count types)
        (read-columns result-set column-count))))
  ;;===========
  java.util.Map
  ;;===========
  (get-sql    [m] (:sql m))
  (set-params [m ^PreparedStatement prepared-statement params]
    (if-let [param-keys (:param-keys m)]
      (let [param-types (or (:param-types m) (no-param-type-vec (count param-keys)))]
        (set-params! prepared-statement param-keys param-types :vector params))
      (set-params! prepared-statement params)))
  (read-col   [m ^ResultSet result-set ^long column-index]
    (if-let [types (:result-types m)]
      (let [column-type (get types (unchecked-dec column-index))]
        (read-column-value result-set column-index column-type))
      (read-column-value result-set column-index)))
  (read-row   [m ^ResultSet result-set ^long column-count]
    (if-let [types (:result-types m)]
      (read-columns result-set column-count types :vector)
      (read-columns result-set column-count)))
  ;;====
  String
  ;;====
  (get-sql    [sql] sql)
  (set-params [sql ^PreparedStatement prepared-statement params] (set-params! prepared-statement params))
  (read-col   [sql ^ResultSet result-set ^long column-index]     (read-column-value result-set column-index))
  (read-row   [sql ^ResultSet result-set ^long column-count]     (read-columns result-set column-count)))


;; ----- data source instrumentation -----


(def ^JdbcEventFactory jdbc-event-factory (reify JdbcEventFactory
                                            ;; JDBC-statement creation
                                            (jdbcStatementCreationEvent [this]
                                              (t/->StmtCreationEvent nil :statement))
                                            (jdbcPreparedStatementCreationEvent [this sql]
                                              (t/->StmtCreationEvent sql :prepared-statement))
                                            (jdbcCallableStatementCreationEvent [this sql]
                                              (t/->StmtCreationEvent sql :callable-statement))
                                            ;; SQL statement execution
                                            (sqlExecutionEventForStatement [this sql]
                                              (t/->SQLExecutionEvent false sql :sql))
                                            (sqlQueryExecutionEventForStatement [this sql]
                                              (t/->SQLExecutionEvent false sql :sql-query))
                                            (sqlUpdateExecutionEventForStatement [this sql]
                                              (t/->SQLExecutionEvent false sql :sql-update))
                                            ;; SQL prepared statement execution
                                            (sqlExecutionEventForPreparedStatement [this sql]
                                              (t/->SQLExecutionEvent true sql :sql))
                                            (sqlQueryExecutionEventForPreparedStatement [this sql]
                                              (t/->SQLExecutionEvent true sql :sql-query))
                                            (sqlUpdateExecutionEventForPreparedStatement [this sql]
                                              (t/->SQLExecutionEvent true sql :sql-update))))


(defn make-jdbc-event-listener
  ^JdbcEventListener [{:keys [before on-success on-error lastly]
                       :or {before     (fn [event])
                            on-success (fn [^String id ^long nanos event])
                            on-error   (fn [^String id ^long nanos event ^Exception error])
                            lastly     (fn [^String id ^long nanos event])}}]
  (reify JdbcEventListener
    (before    [this event]                (before     event))
    (onSuccess [this id nanos event]       (on-success id nanos event))
    (onError   [this id nanos event error] (on-error   id nanos event error))
    (lastly    [this id nanos event]       (lastly     id nanos event))))
