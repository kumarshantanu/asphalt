(ns asphalt.core
  (:refer-clojure :exclude [update])
  (:require
    [clojure.string   :as str]
    [asphalt.type     :as t]
    [asphalt.internal :as i])
  (:import
    [java.util.regex Pattern]
    [java.sql  Connection PreparedStatement
               ResultSet ResultSetMetaData]
    [javax.sql DataSource]))


;; ----- parse SQL for named parameters and types -----


(defn parse-sql
  "Given a SQL statement with embedded parameter names return a three-element vector:
  [SQL-template-string
   param-pairs           (each pair is a two-element vector of param key and type)
   result-column-types]
  that can be used later to extract param values from maps."
  ([^String sql]
    (parse-sql sql {}))
  ([^String sql {:keys [escape-char param-start-char type-start-char name-encoder]
                 :or {escape-char \\ param-start-char \$ type-start-char \^}
                 :as options}]
    (let [[sql named-params return-col-types]  (i/parse-sql-str sql escape-char param-start-char type-start-char)]
      (i/make-sql-template
        sql
        (mapv #(let [[p-name p-type] %]
                 [(i/encode-name p-name) (i/encode-type p-type sql)])
          named-params)
        (mapv #(i/encode-type % sql) return-col-types)))))


(defmacro defsql
  "Define a parsed SQL template that can be used to execute it later."
  ([var-symbol sql]
    (when-not (symbol? var-symbol)
      (i/unexpected "a symbol" var-symbol))
    `(def ~var-symbol (parse-sql ~sql)))
  ([var-symbol sql options]
    (when-not (symbol? var-symbol)
      (i/unexpected "a symbol" var-symbol))
    `(def ~var-symbol (parse-sql ~sql ~options))))


;; ----- java.sql.ResultSet operations -----


(defn fetch-maps
  "Fetch a collection of maps."
  [isql ^ResultSet result-set]
  (doall (resultset-seq result-set)))


(defn fetch-rows
  "Given a java.sql.ResultSet and asphalt.internal.ISql instances fetch a vector of rows using ISql."
  ([isql ^ResultSet result-set]
    (fetch-rows t/read-row isql result-set))
  ([row-maker isql ^ResultSet result-set]
    (let [rows (transient [])
          ^ResultSetMetaData rsmd (.getMetaData result-set)
          column-count (.getColumnCount rsmd)]
      (while (.next result-set)
        (conj! rows (row-maker isql result-set column-count)))
      (persistent! rows))))


(defn fetch-single-row
  "Given java.sql.ResultSet and asphalt.internal.ISql instances ensure result has exactly one row and fetch it using
  ISql."
  ([isql ^ResultSet result-set]
    (fetch-single-row t/read-row isql result-set))
  ([row-maker isql ^ResultSet result-set]
    (if (.next result-set)
      (let [^ResultSetMetaData rsmd (.getMetaData result-set)
            column-count (.getColumnCount rsmd)
            row (row-maker isql result-set column-count)]
        (if (.next result-set)
          (throw (RuntimeException. (str "Expected exactly one JDBC result row, but found more than one for SQL: "
                                      (t/get-sql isql))))
          row))
      (throw (RuntimeException. (str "Expected exactly one JDBC result row, but found no result row for SQL: "
                                  (t/get-sql isql)))))))


(defn fetch-single-value
  "Given java.sql.ResultSet and asphalt.internal.ISql instances, ensure result has exactly one row and one column, and
  fetch it using ISql."
  ([isql ^ResultSet result-set]
    (fetch-single-value t/read-col isql result-set))
  ([column-reader isql ^ResultSet result-set]
    (let [^ResultSetMetaData rsmd (.getMetaData result-set)
          column-count (.getColumnCount rsmd)]
      (when (not= 1 column-count)
        (throw (RuntimeException. (str "Expected exactly one JDBC result column but found " column-count " for SQL: "
                                    (t/get-sql isql)))))
      (if (.next result-set)
        (let [column-value (column-reader isql result-set 1)]
          (if (.next result-set)
            (throw (RuntimeException. (str "Expected exactly one JDBC result row, but found more than one for SQL: "
                                        (t/get-sql isql))))
            column-value))
        (throw (RuntimeException. (str "Expected exactly one JDBC result row, but found no result row for SQL: "
                                    (t/get-sql isql))))))))


;; ----- working with javax.sql.DataSource -----


(defn invoke-with-connection
  "Obtain java.sql.Connection object from specified data-source, calling connection-worker (arity-1 function that
  accepts java.sql.Connection object) with the connection as argument. Close the connection in the end."
  [connection-worker data-source-or-connection]
  (cond
    (instance? DataSource
      data-source-or-connection) (with-open [^Connection connection (.getConnection
                                                                      ^DataSource data-source-or-connection)]
                                   (connection-worker connection))
    (instance? Connection
      data-source-or-connection) (connection-worker ^Connection data-source-or-connection)
    :otherwise                   (i/unexpected "javax.sql.DataSource or java.sql.Connection instance"
                                   data-source-or-connection)))


(defmacro with-connection
  "Bind `connection` (symbol) to a connection obtained from specified data-source, evaluating the body of code in that
  context. Close connection in the end.
  See also: invoke-with-connection"
  [[connection data-source] & body]
  (when-not (symbol? connection)
    (i/unexpected "a symbol" connection))
  `(invoke-with-connection
     (^:once fn* [~(vary-meta connection assoc :tag java.sql.Connection)] ~@body) ~data-source))


;; ----- transactions -----


(defn wrap-transaction
  "Given an arity-1 connection-worker fn, wrap it such that it prepares the specified transaction context on the
  java.sql.Connection argument first."
  ([connection-worker]
    (wrap-transaction connection-worker nil))
  ([connection-worker transaction-isolation]
    (fn wrapper [data-source-or-connection]
      (if (instance? DataSource data-source-or-connection)
        (with-open [^Connection connection (.getConnection ^DataSource data-source-or-connection)]
          (wrapper connection))
        (let [^Connection connection data-source-or-connection]
          (.setAutoCommit connection false)
          (when-not (nil? transaction-isolation)
            (.setTransactionIsolation connection ^int (i/resolve-txn-isolation transaction-isolation)))
          (try
            (let [result (connection-worker connection)]
              (.commit connection)
              result)
            (catch Exception e
              (try
                (.rollback connection)
                (catch Exception _)) ; ignore auto-rollback exceptions
              (throw e))))))))


(defn invoke-with-transaction
  "Obtain java.sql.Connection object from specified data-source, setup a transaction calling f with the connection as
  argument. Close the connection in the end and return what f returned. Transaction is committed if f returns normally,
  rolled back in the case of any exception."
  ([f ^DataSource data-source]
    (if (instance? DataSource data-source)
      (with-open [^Connection connection (.getConnection ^DataSource data-source)]
        (.setAutoCommit connection false)
        (try
          (let [result (f connection)]
            (.commit connection)
            result)
          (catch Exception e
            (try
              (.rollback connection)
              (catch Exception _)) ; ignore auto-rollback exceptions
            (throw e))))
      (i/unexpected "javax.sql.DataSource instance" data-source)))
  ([f ^DataSource data-source isolation]
    (if (instance? DataSource data-source)
      (with-open [^Connection connection (.getConnection ^DataSource data-source)]
        (.setAutoCommit connection false)
        (.setTransactionIsolation connection ^int (i/resolve-txn-isolation isolation))
        (try
          (let [result (f connection)]
            (.commit connection)
            result)
          (catch Exception e
            (try
              (.rollback connection)
              (catch Exception _)) ; ignore auto-rollback exceptions
            (throw e))))
      (i/unexpected "javax.sql.DataSource instance" data-source))))


(defmacro with-transaction
  "Bind `connection` (symbol) to a connection obtained from specified data-source, setup a transaction evaluating the
  body of code in that context. Close connection in the end. Transaction is committed if the code returns normally,
  rolled back in the case of any exception.
  See also: invoke-with-transaction"
  ([[connection data-source] expr]
    (when-not (symbol? connection)
      (i/unexpected "a symbol" connection))
    `(invoke-with-transaction
       (^:once fn* [~(vary-meta connection assoc :tag java.sql.Connection)] ~expr) ~data-source))
  ([[connection data-source] isolation & body]
    (when-not (symbol? connection)
      (i/unexpected "a symbol" connection))
    `(invoke-with-transaction
       (^:once fn* [~(vary-meta connection assoc :tag java.sql.Connection)] ~@body) ~data-source ~isolation)))


;; ----- java.sql.PreparedStatement (connection-worker) stuff -----


(defn query
  "Execute query with params and process the java.sql.ResultSet instance with result-set-worker. The java.sql.ResultSet
  instance is closed in the end, so result-set-worker should neither close it nor make a direct/indirect reference to
  it in the value it returns."
  ([result-set-worker data-source-or-connection sql-or-template params]
    (query t/set-params result-set-worker data-source-or-connection sql-or-template params))
  ([params-setter result-set-worker data-source-or-connection sql-or-template params]
    (if (instance? DataSource data-source-or-connection)
      (with-open [^Connection connection (.getConnection ^DataSource data-source-or-connection)]
        (query params-setter result-set-worker connection sql-or-template params))
      (with-open [^PreparedStatement pstmt (i/prepare-statement ^Connection data-source-or-connection
                                             (t/get-sql sql-or-template) false)]
        (params-setter sql-or-template pstmt params)
        (with-open [^ResultSet result-set (.executeQuery pstmt)]
          (result-set-worker sql-or-template result-set))))))


(defn genkey
  "Execute an update statement returning the keys generated by the statement. The generated keys are extracted from a
  java.sql.ResultSet instance using the optional result-set-worker argument."
  ([data-source-or-connection sql-or-template params]
    (genkey t/set-params fetch-single-value data-source-or-connection sql-or-template params))
  ([result-set-worker data-source-or-connection sql-or-template params]
    (genkey t/set-params result-set-worker data-source-or-connection sql-or-template params))
  ([params-setter result-set-worker data-source-or-connection sql-or-template params]
    (if (instance? DataSource data-source-or-connection)
      (with-open [^Connection connection (.getConnection ^DataSource data-source-or-connection)]
        (genkey params-setter result-set-worker connection sql-or-template params))
      (with-open [^PreparedStatement pstmt (i/prepare-statement ^Connection data-source-or-connection
                                             (t/get-sql sql-or-template) true)]
        (params-setter sql-or-template pstmt params)
        (.executeUpdate pstmt)
        (with-open [^ResultSet generated-keys (.getGeneratedKeys pstmt)]
          (result-set-worker sql-or-template generated-keys))))))


(defn update
  "Execute an update statement returning the number of rows impacted."
  ([data-source-or-connection sql-or-template params]
    (update t/set-params data-source-or-connection sql-or-template params))
  ([params-setter data-source-or-connection sql-or-template params]
    (if (instance? DataSource data-source-or-connection)
      (with-open [^Connection connection (.getConnection ^DataSource data-source-or-connection)]
        (update params-setter connection sql-or-template params))
      (with-open [^PreparedStatement pstmt (i/prepare-statement ^Connection data-source-or-connection
                                             (t/get-sql sql-or-template) false)]
        (params-setter sql-or-template pstmt params)
        (.executeUpdate pstmt)))))


(defn batch-update
  "Execute a SQL write statement with a batch of parameters returning the number of rows updated as a vector."
  ([data-source-or-connection sql-or-template batch-params]
    (batch-update t/set-params data-source-or-connection sql-or-template batch-params))
  ([params-setter data-source-or-connection sql-or-template batch-params]
    (if (instance? DataSource data-source-or-connection)
      (with-open [^Connection connection (.getConnection ^DataSource data-source-or-connection)]
        (batch-update params-setter connection sql-or-template batch-params))
      (with-open [^PreparedStatement pstmt (i/prepare-statement ^Connection data-source-or-connection
                                             (t/get-sql sql-or-template) false)]
        (doseq [params batch-params]
          (params-setter sql-or-template pstmt params)
          (.addBatch pstmt))
        (vec (.executeBatch pstmt))))))
