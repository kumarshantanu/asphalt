(ns asphalt.core
  (:refer-clojure :exclude [update])
  (:require
    [clojure.string   :as str]
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
  ([^String sql options]
    (let [{:keys [escape-char param-start-char type-start-char name-encoder]
           :or {escape-char \\ param-start-char \$ type-start-char \^}} options
        ec escape-char       ; escape char
        mc param-start-char  ; marker char
        tc type-start-char   ; type char
        nn (count sql)
        ^StringBuilder sb (StringBuilder. nn)
        ks (transient [])  ; param keys
        ts (transient [])  ; result types
        tlast (fn [t] (get t (dec (count t))))  ; `last` on a transient
        conj-new! (fn [t name] (conj! t name))
        conj-old! (fn [t name] (let [old (tlast t)] (conj! (pop! t) (str old name))))
        valid-nchar? (fn [ch] (let [partial-name (tlast ks)] (i/valid-name-char? type-start-char partial-name ch)))
        valid-tchar? (fn [ch] (let [partial-name (tlast ts)] (i/valid-type-char? partial-name ch)))
        handle-token (fn [token-name i valid-char? t retval]
                       (let [j (int (inc ^int i))]
                         (cond
                          (>= ^int i (dec nn)) (i/illegal-arg "SQL found ending with a dangling" token-name
                                                 "marker char:" sql)
                          (valid-char?
                            (.charAt sql j))   (do (conj-new! t "") retval)
                          :otherwise           (i/illegal-arg "Invalid first character in" token-name ":"
                                                 (str \' (.charAt sql j) \') "at position" j))))]
    (loop [i (int 0) ; current index
           e? false  ; current escape state
           n? false  ; parameter name in progress
           t? false  ; result type in progress
           ]
      (if (>= i nn)
        (cond
          e? (i/illegal-arg "SQL found ending with dangling escape character:" sql)
          n? (.append sb \?))
        (let [ch (.charAt sql i)
              [e? n? t?] (cond
                           ;; escape mode
                           e?              (do (.append sb ch) [false false false])
                           ;; param name in progress?
                           n?              (if (valid-nchar? ch)
                                             (do (conj-old! ks ch) [false true  false])
                                             (do (.append sb \?)
                                               (.append sb ch)     [false false false]))
                           ;; result type in progress?
                           t?              (if (valid-tchar? ch)
                                             (do (conj-old! ts ch) [false false true])
                                             (do (.append sb ch)   [false false false]))
                           ;; escape character encountered
                           (= ch ^char ec) (if (>= i (dec nn))
                                             (i/illegal-arg "SQL found ending with dangling escape character:" sql)
                                             [true false false])
                           ;; start of a param name?
                           (= ch ^char mc) (handle-token "param-name" i valid-nchar? ks [false true false])
                           ;; start of a result type?
                           (= ch ^char tc) (handle-token "result-column-type" i valid-tchar? ts [false false true])
                           ;; catch-all case
                           :else       (do (.append sb ch) [false false false]))]
          (recur (inc i) e? n? t?))))
    (i/make-sql-template
      (.toString sb)
      (mapv #(let [[p-name p-type] (i/split-param-name-and-type type-start-char %)]
               [(i/encode-name p-name) (i/encode-type p-type sql)])
        (persistent! ks))
      (mapv #(i/encode-type % sql) (persistent! ts))))))


(defmacro defsql
  ([var-symbol sql]
    (when-not (symbol? var-symbol)
      (i/unexpected "a symbol" var-symbol))
    `(def ~var-symbol (parse-sql ~sql)))
  ([var-symbol sql options]
    (when-not (symbol? var-symbol)
      (i/unexpected "a symbol" var-symbol))
    `(def ~var-symbol (parse-sql ~sql ~options))))


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
  ([connection-worker]
    (wrap-transaction connection-worker nil))
  ([connection-worker transaction-isolation]
    (fn [^Connection connection]
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
          (throw e))))))


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
  ([result-set-worker sql-or-template params ^Connection connection]
    (query result-set-worker i/set-params! sql-or-template params ^Connection connection))
  ([result-set-worker params-setter sql-or-template params ^Connection connection]
    (with-open [^PreparedStatement pstmt (i/prepare-statement connection (i/resolve-sql sql-or-template) false)]
      (if (i/sql-template? sql-or-template)
        (params-setter pstmt (i/param-pairs sql-or-template) params)
        (params-setter pstmt params))
      (with-open [^ResultSet result-set (.executeQuery pstmt)]
        (result-set-worker result-set)))))


(defn genkey
  ([result-set-worker sql-or-template params ^Connection connection]
    (genkey result-set-worker i/set-params! sql-or-template params connection))
  ([result-set-worker params-setter sql-or-template params ^Connection connection]
    (with-open [^PreparedStatement pstmt (i/prepare-statement connection (i/resolve-sql sql-or-template) true)]
      (if (i/sql-template? sql-or-template)
        (params-setter pstmt (i/param-pairs sql-or-template) params)
        (params-setter pstmt params))
      (.executeUpdate pstmt)
      (with-open [^ResultSet generated-keys (.getGeneratedKeys pstmt)]
        (result-set-worker generated-keys)))))


(defn update
  ([sql-or-template params ^Connection connection]
    (update i/set-params! sql-or-template params connection))
  ([params-setter sql-or-template params ^Connection connection]
    (with-open [^PreparedStatement pstmt (i/prepare-statement connection (i/resolve-sql sql-or-template) true)]
      (if (i/sql-template? sql-or-template)
        (params-setter pstmt (i/param-pairs sql-or-template) params)
        (params-setter pstmt params))
      (.executeUpdate pstmt))))


(defn batch-update
  "Execute a SQL write statement with a batch of parameters returning the number of rows updated as a vector."
  ([sql-or-template batch-params ^Connection connection]
    (batch-update i/set-params! sql-or-template batch-params ^Connection connection))
  ([params-setter sql-or-template batch-params ^Connection connection]
    (with-open [^PreparedStatement pstmt (i/prepare-statement connection (i/resolve-sql sql-or-template) true)]
      (if (i/sql-template? sql-or-template)
        (let [param-pairs (i/param-pairs sql-or-template)]
          (doseq [params batch-params]
            (params-setter pstmt param-pairs params)
            (.addBatch pstmt)))
        (doseq [params batch-params]
          (params-setter pstmt params)
          (.addBatch pstmt)))
      (vec (.executeBatch pstmt)))))


;; ----- java.sql.ResultSet operations -----


(def fetch-maps (comp doall resultset-seq))


(defn fetch-rows
  "Given a java.sql.ResultSet instance fetch a vector of rows using row-maker, an arity-2 function that accepts
  java.sql.ResultSet and column-count, and by default returns a vector of column values."
  ([^ResultSet result-set]
    (fetch-rows i/read-result-row nil result-set))
  ([result-column-types ^ResultSet result-set]
    (fetch-rows i/read-result-row result-column-types result-set))
  ([row-maker ^ints result-column-types ^ResultSet result-set]
    (let [rows (transient [])]
      (if (seq result-column-types)
        (while (.next result-set)
          (conj! rows (row-maker result-set result-column-types)))
        (let [^ResultSetMetaData rsmd (.getMetaData result-set)
              column-count (.getColumnCount rsmd)]
          (while (.next result-set)
            (conj! rows (row-maker result-set column-count)))))
      (persistent! rows))))


(defn fetch-single-row
  "Given a java.sql.ResultSet instance ensure it has exactly one row and fetch it using row-maker, an arity-2 function
  that accepts java.sql.ResultSet and column-count, and by default returns a vector of column values."
  ([^ResultSet result-set]
    (fetch-single-row i/read-result-row nil result-set))
  ([result-column-types ^ResultSet result-set]
    (fetch-single-row i/read-result-row result-column-types result-set))
  ([row-maker ^ints result-column-types ^ResultSet result-set]
    (if (.next result-set)
      (let [row (if (seq result-column-types)
                  (row-maker result-set result-column-types)
                  (let [^ResultSetMetaData rsmd (.getMetaData result-set)
                        column-count (.getColumnCount rsmd)]
                    (row-maker result-set column-count)))]
        (if (.next result-set)
          (throw (RuntimeException. "Expected exactly one JDBC result row, but found more than one."))
          row))
      (throw (RuntimeException. "Expected exactly one JDBC result row, but found no result row.")))))


(defn fetch-single-value
  "Given a java.sql.ResultSet instance ensure it has exactly one row and one column, and fetch it using column-reader,
  an arity-1 function that accepts java.sql.ResultSet and returns the column value."
  ([^ResultSet result-set]
    (fetch-single-value i/read-column-value nil result-set))
  ([result-column-types ^ResultSet result-set]
    (fetch-single-value i/read-column-value result-column-types result-set))
  ([column-reader result-column-types ^ResultSet result-set]
    (let [^ResultSetMetaData rsmd (.getMetaData result-set)
          column-count (.getColumnCount rsmd)]
      (when (not= 1 column-count)
        (throw (RuntimeException. (str "Expected exactly one JDBC result column but found: " column-count))))
      (if (.next result-set)
        (let [column-value (if (seq result-column-types)
                             (column-reader result-set 1 (first result-column-types))
                             (column-reader result-set 1))]
          (if (.next result-set)
            (throw (RuntimeException. "Expected exactly one JDBC result row, but found more than one."))
            column-value))
        (throw (RuntimeException. "Expected exactly one JDBC result row, but found no result row"))))))
