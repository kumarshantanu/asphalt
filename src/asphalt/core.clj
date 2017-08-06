;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.core
  (:refer-clojure :exclude [update])
  (:require
    [clojure.string        :as str]
    [asphalt.param         :as p]
    [asphalt.result        :as r]
    [asphalt.type          :as t]
    [asphalt.internal      :as i]
    [asphalt.internal.isql :as isql])
  (:import
    [java.util.regex Pattern]
    [java.sql  Connection PreparedStatement ResultSet ResultSetMetaData]
    [javax.sql DataSource]
    [asphalt.instrument JdbcEventListener]
    [asphalt.instrument.wrapper ConnectionWrapper DataSourceWrapper]))


;; ----- instrumentation -----


(defn instrument-connection-source
  "Make instrumented connection source using connection-creation, statement-creation and SQL-execution listeners.

  Option :conn-creation corresponds to a map containing the following fns, triggered when JDBC connections are created:
  ;; event = :jdbc-connection-creation-event
  {:before     (fn [event])
   :on-success (fn [^String id ^long nanos event])
   :on-error   (fn [^String id ^long nanos event ^Exception error])
   :lastly     (fn [^String id ^long nanos event])}

  Option :stmt-creation corresponds to a map containing the following fns, triggered when JDBC statements are created:
  {:before     (fn [^asphalt.type.StmtCreationEvent event])
   :on-success (fn [^String id ^long nanos ^asphalt.type.StmtCreationEvent event])
   :on-error   (fn [^String id ^long nanos ^asphalt.type.StmtCreationEvent event ^Exception error])
   :lastly     (fn [^String id ^long nanos ^asphalt.type.StmtCreationEvent event])}

  Option :sql-execution corresponds to a map containing the following fns, triggered when SQL statements are executed:
  {:before     (fn [^asphalt.type.SQLExecutionEvent event])
   :on-success (fn [^String id ^long nanos ^asphalt.type.SQLExecutionEvent event])
   :on-error   (fn [^String id ^long nanos ^asphalt.type.SQLExecutionEvent event ^Exception error])
   :lastly     (fn [^String id ^long nanos ^asphalt.type.SQLExecutionEvent event])}"
  {:added "0.4.0"}
  [connection-source {:keys [conn-creation stmt-creation sql-execution]
                      :or {conn-creation JdbcEventListener/NOP
                           stmt-creation JdbcEventListener/NOP
                           sql-execution JdbcEventListener/NOP}}]
  (let [as-jdbc-event-listener (fn [x] (if (instance? JdbcEventListener x) x
                                         (i/make-jdbc-event-listener x)))
        ^JdbcEventListener conn-creation-listener (as-jdbc-event-listener conn-creation)
        ^JdbcEventListener stmt-creation-listener (as-jdbc-event-listener stmt-creation)
        ^JdbcEventListener sql-execution-listener (as-jdbc-event-listener sql-execution)
        nanos-now (fn (^long [] (System/nanoTime))
                    (^long [^long start] (- (System/nanoTime) start)))
        make-conn (fn [f] (let [event :jdbc-connection-creation-event
                                ^String id (.before conn-creation-listener event)
                                start (nanos-now)]
                            (try
                              (let [result (ConnectionWrapper.
                                             (f connection-source)
                                             i/jdbc-event-factory
                                             stmt-creation-listener sql-execution-listener)]
                                (.onSuccess conn-creation-listener id (nanos-now start) event)
                                result)
                              (catch Exception e
                                (.onError conn-creation-listener id (nanos-now start) event e)
                                (throw e))
                              (finally
                                (.lastly conn-creation-listener id (nanos-now start) event)))))]
    (reify t/IConnectionSource
      (create-connection            [this] (make-conn t/create-connection))
      (obtain-connection            [this] (make-conn t/obtain-connection))
      (return-connection [this connection] (t/return-connection connection-source connection)))))


(defn instrument-datasource
  "DEPRECATED: Use 'instrument-connection-source' instead.
  Make instrumented javax.sql.DataSource instance using statement-creation and SQL-execution listeners.

  Option :stmt-creation corresponds to a map containing the following fns, triggered when JDBC statements are created:
  {:before     (fn [^asphalt.type.StmtCreationEvent event])
   :on-success (fn [^String id ^long nanos ^asphalt.type.StmtCreationEvent event])
   :on-error   (fn [^String id ^long nanos ^asphalt.type.StmtCreationEvent event ^Exception error])
   :lastly     (fn [^String id ^long nanos ^asphalt.type.StmtCreationEvent event])}

  Option :sql-execution corresponds to a map containing the following fns, triggered when SQL statements are executed:
  {:before     (fn [^asphalt.type.SQLExecutionEvent event])
   :on-success (fn [^String id ^long nanos ^asphalt.type.SQLExecutionEvent event])
   :on-error   (fn [^String id ^long nanos ^asphalt.type.SQLExecutionEvent event ^Exception error])
   :lastly     (fn [^String id ^long nanos ^asphalt.type.SQLExecutionEvent event])}"
  ^javax.sql.DataSource
  {:added "0.3.0"
   :deprecated "0.4.0"}
  [^DataSource ds {:keys [stmt-creation sql-execution]
                   :or {stmt-creation JdbcEventListener/NOP
                        sql-execution      JdbcEventListener/NOP}}]
  (let [stmt-creation-listener (if (instance? JdbcEventListener stmt-creation)
                                 stmt-creation
                                 (i/make-jdbc-event-listener stmt-creation))
        sql-execution-listener (if (instance? JdbcEventListener sql-execution)
                                 sql-execution
                                 (i/make-jdbc-event-listener sql-execution))]
    (DataSourceWrapper. ds i/jdbc-event-factory stmt-creation-listener sql-execution-listener)))


;; ----- java.sql.ResultSet operations -----


(defn fetch-maps
  "Given asphalt.type.ISqlSource and java.sql.ResultSet instances fetch a collection of rows as maps. It is required
  for asphalt.type.ISqlSource/read-row to return a vector of all column values for the sql-source.
  Options:
    :fetch-size (positive integer, default: not applied)         fetch-size to be set on java.sql.ResultSet
    :key-maker  (arity-1 fn, default: asphalt.result/label->key) converts column label to column key"
  ([sql-source ^ResultSet result-set]
    (fetch-maps {} sql-source result-set))
  ([{:keys [fetch-size
            key-maker]
     :or {key-maker r/label->key}
     :as options}
    sql-source ^ResultSet result-set]
    ;; set fetch size on the JDBC driver
    (when fetch-size
      (.setFetchSize result-set (int fetch-size)))
    ;; fetch rows
    (let [rows (transient [])
          ^ResultSetMetaData rsmd (.getMetaData result-set)
          column-count (.getColumnCount rsmd)
          column-keys  (let [ks (object-array column-count)]
                         (loop [i 0]
                           (when (< i column-count)
                             (let [j (unchecked-inc i)]
                               (aset ks i (key-maker (.getColumnLabel rsmd j)))
                               (recur j))))
                         (vec ks))
          row-maker    t/read-row]
      (when-not (distinct? column-keys)
        (throw (ex-info "Column keys must be unique" {:column-count column-count
                                                      :column-keys column-keys})))
      (while (.next result-set)
        (conj! rows (zipmap column-keys (row-maker sql-source result-set column-count))))
      (persistent! rows))))


(defn fetch-rows
  "Given asphalt.type.ISqlSource and java.sql.ResultSet instances fetch a vector of rows.
  Options:
    :fetch-size (positive integer, default: not applied)         fetch-size to be set on java.sql.ResultSet
    :max-rows   (positive integer, default: not applied)         max result rows to read
    :row-maker  (function arity-3, default: returns row vector)  fn to build row by extracting column values"
  ([sql-source ^ResultSet result-set]
    (fetch-rows {} sql-source result-set))
  ([{:keys [fetch-size
            max-rows
            row-maker]
     :or {row-maker t/read-row}
     :as options}
    sql-source ^ResultSet result-set]
    ;; set fetch size on the JDBC driver
    (when fetch-size
      (.setFetchSize result-set (int fetch-size)))
    ;; fetch rows
    (let [rows (transient [])
          ^ResultSetMetaData rsmd (.getMetaData result-set)
          column-count (.getColumnCount rsmd)]
      (if max-rows
        (let [max-row-count (int max-rows)]
          (loop [i 0]
            (when (and (< i max-row-count) (.next result-set))
              (conj! rows (row-maker sql-source result-set column-count))
              (recur (unchecked-inc i)))))
        (while (.next result-set)
          (conj! rows (row-maker sql-source result-set column-count))))
      (persistent! rows))))


(defn fetch-single-row
  "Given asphalt.type.ISqlSource and java.sql.ResultSet instances fetch a single row.
  Options:
    :fetch-size (positive integer, default: not applied)         fetch-size to be set on java.sql.ResultSet
    :on-empty   (function arity-3, default: throws exception)    fn to handle the :on-empty event
    :on-multi   (function arity-1, default: throws exception)    fn to handle the :on-multi event
    :row-maker  (function arity-3, default: returns row vector)  fn to build row by extracting column values"
  ([sql-source ^ResultSet result-set]
    (fetch-single-row {} sql-source result-set))
  ([{:keys [fetch-size
            on-empty
            on-multi
            row-maker]
     :or {on-empty  r/throw-on-empty
          on-multi  r/throw-on-multi
          row-maker t/read-row}
     :as options}
    sql-source ^ResultSet result-set]
    ;; set fetch size on the JDBC driver
    (when fetch-size
      (.setFetchSize result-set (int fetch-size)))
    ;; fetch rows
    (if (.next result-set)
      (let [^ResultSetMetaData rsmd (.getMetaData result-set)
            column-count (.getColumnCount rsmd)
            row (row-maker sql-source result-set column-count)]
        (if (.next result-set)
          (on-multi sql-source result-set row)
          row))
      (on-empty sql-source result-set))))


(defn fetch-optional-row
  "Given asphalt.type.ISqlSource and java.sql.ResultSet instances fetch a single row if one exists, nil (or specified
  default) otherwise.
  Options:
    :default (any value) default value to return when no row is found
    (see `fetch-single-row` for other options)"
  ([sql-source ^ResultSet result-set]
    (fetch-single-row {:on-empty r/nil-on-empty} sql-source result-set))
  ([{:keys [default]
     :as options}
    sql-source ^ResultSet result-set]
    (fetch-single-row (assoc options :on-empty (fn [_ _] default))
      sql-source result-set)))


(defn fetch-single-value
  "Given asphalt.type.ISqlSource and java.sql.ResultSet instances fetch a single column value.
  Options:
    :column-reader (function arity-2, default: returns row vector)  fn to extract column value
    :fetch-size    (positive integer, default: not applied)         fetch-size to be set on java.sql.ResultSet
    :on-empty      (function arity-3, default: throws exception)    fn to handle the :on-empty event
    :on-multi      (function arity-1, default: throws exception)    fn to handle the :on-multi event"
  ([sql-source ^ResultSet result-set]
    (fetch-single-value {} sql-source result-set))
  ([{:keys [column-reader
            fetch-size
            on-empty
            on-multi]
     :or {column-reader t/read-col
          on-empty      r/throw-on-empty
          on-multi      r/throw-on-multi}
     :as options}
    sql-source ^ResultSet result-set]
    ;; set fetch size on the JDBC driver
    (when fetch-size
      (.setFetchSize result-set (int fetch-size)))
    ;; fetch rows
    (if (.next result-set)
      (let [column-value (column-reader sql-source result-set)]
        (if (.next result-set)
          (on-multi sql-source result-set column-value)
          column-value))
      (on-empty sql-source result-set))))


(defn fetch-optional-value
  "Given asphalt.type.ISqlSource and java.sql.ResultSet instances fetch a single column value if one exists, nil (or
  specified default) otherwise.
  Options:
    :default (any value) default value to return when no row is found
    (see `fetch-single-value` for other supported options)"
  ([sql-source ^ResultSet result-set]
    (fetch-single-value {:on-empty r/nil-on-empty} sql-source result-set))
  ([{:keys [default]
     :as options}
    sql-source ^ResultSet result-set]
    (fetch-single-value (assoc options :on-empty (fn [_ _] default))
      sql-source result-set)))


;; ----- java.sql.PreparedStatement (connection-worker) stuff -----


(defn query
  "Execute query with params and process the java.sql.ResultSet instance with result-set-worker. The java.sql.ResultSet
  instance is closed in the end, so result-set-worker should neither close it nor make a direct/indirect reference to
  it in the value it returns."
  ([connection-source sql-source params]
    (query t/set-params fetch-rows connection-source sql-source params))
  ([result-set-worker connection-source sql-source params]
    (query t/set-params result-set-worker connection-source sql-source params))
  ([params-setter result-set-worker connection-source sql-source params]
    (i/with-connection [connection connection-source]
      (with-open [^PreparedStatement pstmt (i/prepare-statement connection
                                             (t/get-sql sql-source params) false)]
        (params-setter sql-source pstmt params)
        (with-open [^ResultSet result-set (.executeQuery pstmt)]
          (result-set-worker sql-source result-set))))))


(defn genkey
  "Execute an update statement returning the keys generated by the statement. The generated keys are extracted from a
  java.sql.ResultSet instance using the optional result-set-worker argument."
  ([connection-source sql-source params]
    (genkey t/set-params fetch-single-value connection-source sql-source params))
  ([result-set-worker connection-source sql-source params]
    (genkey t/set-params result-set-worker connection-source sql-source params))
  ([params-setter result-set-worker connection-source sql-source params]
    (i/with-connection [connection connection-source]
      (with-open [^PreparedStatement pstmt (i/prepare-statement connection
                                             (t/get-sql sql-source params) true)]
        (params-setter sql-source pstmt params)
        (.executeUpdate pstmt)
        (with-open [^ResultSet generated-keys (.getGeneratedKeys pstmt)]
          (result-set-worker sql-source generated-keys))))))


(defn update
  "Execute an update statement returning the number of rows impacted."
  ([connection-source sql-source params]
    (update t/set-params connection-source sql-source params))
  ([params-setter connection-source sql-source params]
    (i/with-connection [connection connection-source]
      (with-open [^PreparedStatement pstmt (i/prepare-statement connection
                                             (t/get-sql sql-source params) false)]
        (params-setter sql-source pstmt params)
        (.executeUpdate pstmt)))))


(defn batch-update
  "Execute a SQL write statement with a batch of parameters returning the number of rows updated as a vector."
  ([connection-source sql-source batch-params]
    (batch-update t/set-params connection-source sql-source batch-params))
  ([params-setter connection-source sql-source batch-params]
    (i/with-connection [connection connection-source]
      (with-open [^PreparedStatement pstmt (i/prepare-statement connection
                                             (t/get-sql sql-source (first batch-params)) false)]
        (doseq [params batch-params]
          (params-setter sql-source pstmt params)
          (.addBatch pstmt))
        (vec (.executeBatch pstmt))))))


;; ----- parse SQL for named parameters and types -----


;; Parseable SQL:
;; "SELECT ^string name, ^int age, ^date joined FROM emp WHERE dept_id=^int $dept-id AND level IN (^ints $levels)"
;;
;; SQL template:
;; [["SELECT name, age, joined FROM emp WHERE dept_id=" [:dept-id :int] " AND level IN (" [:levels :ints] ")"]
;;  [:string :int :date]]


(defn parse-sql
  "Given a SQL statement with embedded parameter names return a two-element vector [sql-tokens result-types] where
  * sql-tokens is a vector of alternating string and param-pair vectors
  * param-pair is a vector of two elements [param-key param-type] (param keys are used to extract map params)
  * result-types is a vector of result types
  Options:
  :sql-name         (auto generated)  name of the SQL statement
  :escape-char      (default: \\)     escape character in the SQL to avoid interpretation
  :param-start-char (default: $)      character to mark the beginning of a named param
  :type-start-char  (default: ^)      character to mark the beginning of a type hint
  :param-types      (parsed from SQL) vector of param-type keywords
  :result-types     (parsed from SQL) vector of result-type keywords
  See:
    `compile-sql-template` to compile a SQL template for efficiency/enhancement
    asphalt.type for supported SQL types."
  ([^String sql]
    (parse-sql sql {}))
  ([^String sql {:keys [sql-name escape-char param-start-char type-start-char
                        param-types result-types]
                 :or {sql-name sql escape-char \\ param-start-char \$ type-start-char \^}
                 :as options}]
    (when param-types
      (i/expected vector? "vector of SQL param types" param-types)
      (doseq [ptype param-types]
        (i/expected #(contains? t/all-typemap %) "valid SQL param type" ptype)))
    (when result-types
      (i/expected vector? "vector of SQL result types" result-types)
      (doseq [rtype result-types]
        (i/expected #(contains? t/single-typemap %) "valid SQL result type" rtype)))
    (let [[sql-tokens parsed-result-types]  (isql/parse-sql-str sql escape-char param-start-char type-start-char)]
      (let [parsed-param-pairs (filter coll? sql-tokens)]
        (when (and param-types (not= (count param-types) (count parsed-param-pairs)))
          (i/expected (format "specified param-types %s (%d) and inferred param-types %s (%d) to be of same length"
                        param-types parsed-param-pairs))))
      [(->> sql-tokens
         (reduce (fn [[^long pindex st] token] (if (string? token)
                                                 [pindex (conj st token)]
                                                 [(inc pindex) (conj st (let [[pname ptype] token]
                                                                          [(isql/encode-name pname)
                                                                           (isql/encode-param-type sql
                                                                             (if param-types
                                                                               (nth param-types pindex)
                                                                               ptype))]))]))
           [0 []])
         second)
       (->> parsed-result-types
         (or result-types)
         (mapv (partial isql/encode-result-type sql)))])))


(defn compile-sql-template
  "Given a SQL template (SQL tokens and result types) compile it into a more efficient SQL source with the following
  enhancements:
    * associated params-setter
    * param placeholder for named, multi-value params
    * associated result-set-worker
    * associated row-maker
    * associated column-reader
    * associated conn-worker
    * act as arity-2 function (f conn-source params)
  Options:
    :result-set-worker  (fn [sql-source result-set])         - used when :make-conn-worker auto-defaults to query
    :param-placeholder  map {:param-name placeholder-string} - override placeholder ? for named multi-value params
    :params-setter      (fn [prepared-stmt params])          - used when :make-params-setter not specified
    :row-maker          (fn [result-set col-count])          - used when :make-row-maker not specified
    :column-reader      (fn [result-set])                    - used when :make-column-reader not specified
    :conn-worker        (fn [conn-source sql-source params]) - used when :make-conn-worker not specified
    :make-params-setter (fn [param-keys param-types]) -> (fn [prepared-stmt params]) to set params
    :make-row-maker     (fn [result-types]) -> (fn [result-set col-count]) to return row
    :make-column-reader (fn [result-types]) -> (fn [result-set]) to return column value
    :make-conn-worker   (fn [sql-tokens result-types]) -> (fn [conn-source sql-source params])
    :sql-name           string (or coerced as string) name for the template
  See:
    `parse-sql` for SQL-template format"
  [sql-tokens result-types
   {:keys [result-set-worker
           param-placeholder
           params-setter
           row-maker
           column-reader
           conn-worker
           make-params-setter
           make-row-maker
           make-column-reader
           make-conn-worker
           sql-name]
    :or {sql-name (gensym "sql-name-")}
    :as options}]
  (i/expected vector? "vector of SQL template tokens" sql-tokens)
  (i/expected vector? "vector of result column types" result-types)
  (let [make-params-setter (or make-params-setter
                             (fn [param-keys param-types] (or params-setter
                                                            (if (seq param-keys)
                                                              (p/make-params-layer param-keys param-types)
                                                              p/set-params))))
        make-row-maker     (or make-row-maker
                             (fn [result-types] (or row-maker
                                                  (if (seq result-types)
                                                    (r/make-columns-reader result-types)
                                                    r/read-columns))))
        make-column-reader (or make-column-reader
                             (fn [result-types] (or column-reader
                                                  (if (seq result-types)
                                                    (r/make-column-value-reader (first result-types) 1 nil)
                                                    (fn [^ResultSet result-set] (r/read-column-value result-set 1))))))
        make-conn-worker   (or make-conn-worker
                             (fn [sql-tokens result-types] (or conn-worker
                                                             (if (or (seq result-types) (-> (first sql-tokens)
                                                                                          str/trim
                                                                                          str/lower-case
                                                                                          (.startsWith "select")))
                                                               (if result-set-worker
                                                                 (partial query result-set-worker)
                                                                 query)
                                                               update))))
        sanitized-st (mapv (fn [token]
                             (cond
                               (string? token)  token
                               (keyword? token) [token :nil]
                               (vector? token)  (let [[param-key param-type] token]
                                                  (i/expected keyword? "param key (keyword)" param-key)
                                                  (when-not (contains? t/all-typemap param-type)
                                                    (i/expected-param-type param-type))
                                                  token)
                               :otherwise       (i/expected "string, param key or key/type vector" token)))
                       sql-tokens)]
    (let [kt-pairs (filter vector? sanitized-st)
          kt-map   (zipmap (map first kt-pairs) (map second kt-pairs))]
      (if (->> kt-pairs
           (map second)
           (every? (partial contains? t/single-typemap)))
        (do
          (i/expected empty? "option :param-placeholder only for named multi-value params" param-placeholder)
          (isql/->StaticSqlTemplate
            (i/as-str sql-name)
            (isql/make-sql sanitized-st nil (vec (repeat (count kt-pairs) nil)))
            (make-params-setter (mapv first kt-pairs) (mapv second kt-pairs))
            (make-row-maker result-types)
            (make-column-reader result-types)
            (make-conn-worker sanitized-st result-types)))
        (isql/->DynamicSqlTemplate
          (i/as-str sql-name)
          (reduce (fn shrink-template [st token]
                    (cond
                      (and (string? token) (string? (last st))) (conj (pop st) (str (last st) token))
                      (and (vector? token) (string? (last st))
                        (contains? t/single-typemap (second     ; pre-convert single value params to '?'
                                                      token)))  (conj (pop st) (str (last st) \?))
                      :otherwise                                (conj st token)))
            [] sanitized-st)
          (reduce-kv (fn [m k v]
                       (i/expected #(contains? t/multi-typemap (get kt-map %))
                         "only multi-value params in option :param-placeholder" k)
                       (i/expected string? "param placeholder string" v)
                       (assoc m k (memoize (fn [^long n] (str/join ", " (repeat n v))))))
            {} param-placeholder)
          (make-params-setter (mapv first kt-pairs) (mapv second kt-pairs))
          (make-row-maker result-types)
          (make-column-reader result-types)
          (make-conn-worker sanitized-st result-types))))))


(defmacro defsql
  "Define a parsed/compiled SQL template that can be used to execute it later."
  ([var-symbol sql]
    (when-not (symbol? var-symbol)
      (i/expected "a symbol" var-symbol))
    `(defsql ~var-symbol ~sql {}))
  ([var-symbol sql options]
    (when-not (symbol? var-symbol)
      (i/expected "a symbol" var-symbol))
    `(def ~var-symbol (let [opts# (merge {:sql-name ~(name var-symbol)} ~options)]
                        (->> opts#
                          (conj (parse-sql ~sql opts#))
                          (apply compile-sql-template ))))))
