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
    [clojure.string       :as str]
    [asphalt.param        :as p]
    [asphalt.result       :as r]
    [asphalt.type         :as t]
    [asphalt.internal     :as i]
    [asphalt.internal.sql :as isql])
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
  "Given asphalt.type.ISqlSource and java.sql.ResultSet instances fetch a collection of rows as maps."
  ([sql-source ^ResultSet result-set]
    (fetch-maps {} sql-source result-set))
  ([{:keys [fetch-size]
     :as options}
    sql-source ^ResultSet result-set]
    ;; set fetch size on the JDBC driver
    (when fetch-size
      (.setFetchSize result-set (int fetch-size)))
    ;; fetch rows
    (doall (resultset-seq result-set))))


(defn fetch-rows
  "Given asphalt.type.ISqlSource and java.sql.ResultSet instances fetch a vector of rows."
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


(defn default-fetch
  "Given a default value, return the option map to be used to fetch from a single row."
  [v]
  {:on-empty (constantly v)
   :on-multi (fn [_ _ v] v)})


(defn fetch-single-row
  "Given asphalt.type.ISqlSource and java.sql.ResultSet instances fetch a single row."
  ([sql-source ^ResultSet result-set]
    (fetch-single-row {} sql-source result-set))
  ([{:keys [fetch-size
            on-empty
            on-multi
            row-maker]
     :or {on-empty  i/on-empty-rows
          on-multi  i/on-multi-rows
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


(defn fetch-single-value
  "Given asphalt.type.ISqlSource and java.sql.ResultSet instances fetch a single column value."
  ([sql-source ^ResultSet result-set]
    (fetch-single-value {} sql-source result-set))
  ([{:keys [column-index
            column-reader
            fetch-size
            on-empty
            on-multi]
     :or {column-index  1
          column-reader t/read-col
          on-empty      i/on-empty-rows
          on-multi      i/on-multi-rows}
     :as options}
    sql-source ^ResultSet result-set]
    ;; set fetch size on the JDBC driver
    (when fetch-size
      (.setFetchSize result-set (int fetch-size)))
    ;; fetch rows
    (if (.next result-set)
      (let [column-value (column-reader sql-source result-set (int column-index))]
        (if (.next result-set)
          (on-multi sql-source result-set column-value)
          column-value))
      (on-empty sql-source result-set))))


;; ----- java.sql.PreparedStatement (connection-worker) stuff -----


(defn query
  "Execute query with params and process the java.sql.ResultSet instance with result-set-worker. The java.sql.ResultSet
  instance is closed in the end, so result-set-worker should neither close it nor make a direct/indirect reference to
  it in the value it returns."
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


(defn build-sql-source
  [sql-template result-types
   {:keys [make-params-setter
           make-row-maker
           make-column-reader
           sql-name]
    :or {make-params-setter (fn [param-keys param-types] (if (seq param-keys)
                                                           (p/make-params-layer param-keys param-types)
                                                           p/set-params))
         make-row-maker     (fn [result-types] (if (seq result-types)
                                                 (r/make-columns-reader result-types)
                                                 r/read-columns))
         make-column-reader (fn [result-types] (if (seq result-types)
                                                 (let [n (count result-types)
                                                       s (str "integer from 1 to " n)]
                                                   (fn [^ResultSet result-set ^long col-index]
                                                     (when-not (<= 1 col-index n)
                                                       (i/expected s col-index))
                                                     (r/read-column-value result-set col-index)))
                                                 r/read-column-value))
         sql-name           (gensym "sql-name-")}
    :as options}]
  (i/expected vector? "vector of SQL template tokens" sql-template)
  (i/expected vector? "vector of result column types" result-types)
  (let [sanitized-st (mapv (fn [token]
                             (cond
                               (string? token)  token
                               (keyword? token) [token :nil]
                               (vector? token)  (let [[param-key param-type] token]
                                                  (i/expected keyword? "param key (keyword)" param-key)
                                                  (when-not (contains? t/all-typemap param-type)
                                                    (i/expected-param-type param-type))
                                                  token)
                               :otherwise       (i/expected "string, param key or key/type vector" token)))
                       sql-template)]
    (let [kt-pairs (filter vector? sanitized-st)]
      (if (->> kt-pairs
           (map second)
           (every? (partial contains? t/single-typemap)))
       (isql/->StaticSqlTemplate
         sql-name
         (isql/make-sql sanitized-st (vec (repeat (count kt-pairs) nil)))
         (make-params-setter (mapv first kt-pairs) (mapv second kt-pairs))
         (make-row-maker result-types)
         (make-column-reader result-types))
       (isql/->DynamicSqlTemplate
         sql-name
         (reduce (fn [st token] (cond
                                  (and (string? token) (string? (last st))) (conj (pop st) (str (last st) token))
                                  (and (vector? token) (string? (last st))
                                    (contains? t/single-typemap (second     ; pre-convert single value params to '?'
                                                                  token)))  (conj (pop st) (str (last st) \?))
                                  :otherwise                                (conj st token)))
           [] sanitized-st)
         (make-params-setter (mapv first kt-pairs) (mapv second kt-pairs))
         (make-row-maker result-types)
         (make-column-reader result-types))))))


(defn parse-sql
  "Given a SQL statement with embedded parameter names return a three-element vector:
  [SQL-template-string
   param-pairs           (each pair is a two-element vector of param key and type)
   result-column-types]
  that can be used later to extract param values from maps."
  ([^String sql]
    (parse-sql sql {}))
  ([^String sql {:keys [sql-name escape-char param-start-char type-start-char name-encoder]
                 :or {sql-name sql escape-char \\ param-start-char \$ type-start-char \^}
                 :as options}]
    (let [[sql-template result-types]  (isql/parse-sql-str sql escape-char param-start-char type-start-char)]
      [(reduce (fn [st token] (conj st (if (string? token)
                                         token
                                         (let [[pname ptype] token]
                                           [(isql/encode-name pname) (isql/encode-param-type sql ptype)]))))
         [] sql-template)
       (mapv (partial isql/encode-result-type sql) result-types)])))


(defmacro defsql
  "Define a parsed SQL template that can be used to execute it later."
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
                          (apply build-sql-source ))))))


;; ----- convenience functions and macros -----


(defn set-params-with-query-timeout
  "Return a params setter fn usable with asphalt.core/query, that times out on query execution and throws a
  java.sql.SQLTimeoutException instance. Supported by JDBC 4.0 (and higher) drivers only."
  [^long n-seconds]
  (fn [sql-source ^PreparedStatement pstmt params]
    (.setQueryTimeout pstmt n-seconds)
    (t/set-params sql-source pstmt params)))
