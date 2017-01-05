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
    [clojure.string   :as str]
    [asphalt.internal.result :as iresult]
    [asphalt.internal.sql    :as isql]
    [asphalt.type            :as t]
    [asphalt.internal        :as i])
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


(defmacro letcol
  "Destructure column values in the current row in a java.sql.ResultSet instance. Optional SQL type hints increase
  accuracy and speed. The date, time and timestamp types accept an additional timezone-string/calendar argument.

  Sequential destructuring with implicit column index lookup:
  (letcol [[^int foo ^string bar [^date baz cal]] result-set]  ; cal is TimeZone keyword/string or java.util.Calendar
    [foo bar baz])

  Associative destructuring with implicit column label lookup:
  (letcol [{:labels  [^int foo ^string bar [^date baz cal]]    ; cal is TimeZone keyword/string or java.util.Calendar
            :_labels [^date end-date ^timestamp audit-ts]]} result-set]
    ;; :_labels turns dash to underscore when looking up by column label: end_date, audit_ts
    [foo bar baz end-date audit-ts])

  Associative destructuring with explicit column reference:
  (letcol [{^int       foo 1  ; column index begins with 1
            ^string    bar 2
            ^boolean   baz 3
            [^date     end-date cal] \"end_date\"              ; cal is TimeZone keyword/string or java.util.Calendar
            ^timestamp audit-ts \"audit_ts\"} result-set]
    [foo bar baz end-date audit-ts])"
  [binding & body]
  (i/expected vector? "a binding vector" binding)
  (i/expected #(= 2 (count %)) "binding vector of two forms" binding)
  (let [[lhs result-set] binding  ; LHS = Left-Hand-Side in a binding pair, for the lack of a better expression
        result-set-sym (with-meta (gensym "result-set-") {:tag "java.sql.ResultSet"})
        make-bindings  #(mapcat (fn [[sym col-ref]]
                                  (iresult/read-column-binding sym result-set-sym col-ref)) %)]
    (cond
      (vector? lhs) (do (i/expected #(every? (complement #{:as '&}) %)
                          ":as and & not to be in sequential destructuring" lhs)
                      `(let [~result-set-sym ~result-set
                             ~@(->> lhs
                                 (map-indexed (fn [^long idx each] [each (inc idx)]))
                                 make-bindings)]
                         ~@body))
      (map? lhs)    (let [k-bindings (fn [k f] (->> (get lhs k)
                                                 (map #(vector % (f (first (i/as-vector %)))))  ; turn into pairs
                                                 make-bindings))]
                      (i/expected #(every? (some-fn #{:labels :_labels} (complement keyword?)) (keys %))
                        "keyword keys to be only :labels or :_labels in associative destructuring" lhs)
                      `(let [~result-set-sym ~result-set
                             ~@(k-bindings :labels  str)
                             ~@(k-bindings :_labels #(str/replace (str %) \- \_))
                             ~@(->> (seq lhs)
                                 (remove (comp keyword? first))  ; remove pairs where keys are keywords
                                 make-bindings)]
                         ~@body))
      :otherwise    (i/expected "vector (sequential) or map (associative) destructuring form" lhs))))


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
    (let [[sql named-params return-col-types]  (i/parse-sql-str sql escape-char param-start-char type-start-char)]
      (i/make-sql-template
        sql-name
        sql
        (mapv #(let [[p-name p-type] %]
                 [(i/encode-name p-name) (i/encode-type p-type sql)])
          named-params)
        (mapv #(i/encode-type % sql) return-col-types)))))


(defmacro defsql
  "Define a parsed SQL template that can be used to execute it later."
  ([var-symbol sql]
    (when-not (symbol? var-symbol)
      (i/expected "a symbol" var-symbol))
    `(defsql ~var-symbol ~sql {}))
  ([var-symbol sql options]
    (when-not (symbol? var-symbol)
      (i/expected "a symbol" var-symbol))
    `(def ~var-symbol (parse-sql ~sql (merge {:sql-name ~(name var-symbol)} ~options)))))


;; Parseable SQL:
;; "SELECT ^string name, ^int age, ^date joined FROM emp WHERE dept_id=^int $dept-id AND level IN (^ints $levels)"
;;
;; SQL template:
;; [["SELECT name, age, joined FROM emp WHERE dept_id=" [:dept-id :int] " AND level IN (" [:levels :ints] ")"]
;;  [:string :int :date]]


(defn build-sql-source
  [sql-template result-types
   {:keys [make-param-setter
           make-row-maker
           make-column-reader
           sql-name]
    :or {make-param-setter  (fn [param-keys param-types]
                              (eval `(fn [^PreparedStatement prepared-stmt# params#]
                                       (p/lay-params prepared-stmt# ~param-keys ~param-types params#))))
         make-row-maker     (fn [result-types]
                              (if (seq result-types)
                                (do
                                  (i/expected vector? "vector of result types" result-types)
                                  (doseq [t result-types]
                                    (when-not (contains? t/single-typemap t) (i/expected-result-type t)))
                                  (let [rsyms (-> (count result-types)
                                                (repeatedly gensym)
                                                vec)
                                        rlhs  (mapv vector rsyms result-types)]
                                    (eval `(fn [^ResultSet result-set# ^long col-count#]
                                             (when-not (= col-count# ~(count result-types))
                                               (i/expected ~(str (count result-types) " columns") col-count#))
                                             (letcol [~rlhs result-set#]
                                               ~rsyms)))))
                                iresult/read-columns))
         make-column-reader (fn [result-types]
                              (let [n ^long (if (seq result-types)
                                              (count result-types)
                                              Integer/MAX_VALUE)
                                    s (str "integer from 1 to " n)]
                                (fn [^ResultSet result-set ^long col-index]
                                  (when-not (<= 1 col-index n)
                                    (i/expected s col-index))
                                  (iresult/read-column-value result-set col-index))))
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
         (make-param-setter (mapv first kt-pairs) (mapv second kt-pairs))
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
         (make-param-setter (mapv first kt-pairs) (mapv second kt-pairs))
         (make-row-maker result-types)
         (make-column-reader result-types))))))


;; ----- convenience functions and macros -----


(defn set-params-with-query-timeout
  "Return a params setter fn usable with asphalt.core/query, that times out on query execution and throws a
  java.sql.SQLTimeoutException instance. Supported by JDBC 4.0 (and higher) drivers only."
  [^long n-seconds]
  (fn [sql-source ^PreparedStatement pstmt params]
    (.setQueryTimeout pstmt n-seconds)
    (t/set-params sql-source pstmt params)))


(defmacro defquery
  "Compose a SQL query template and a fetch fn into a convenience arity-2 (data-source-or-connection, params) fn.
  Option map may include :params-setter corresponding to an arity-3 fn for setting query parameters."
  ([var-symbol sql result-set-worker]
    `(defquery ~var-symbol ~sql ~result-set-worker {}))
  ([var-symbol sql result-set-worker options]
    (let [sql-template-sym (gensym "sql-template-")]
      `(let [~sql-template-sym (parse-sql ~sql (merge {:sql-name ~(name var-symbol)} ~options))
             query-fetch# (if-let [params-setter# (:params-setter ~options)]
                            (partial query params-setter# ~result-set-worker)
                            (partial query ~result-set-worker))]
         (defn ~var-symbol
           ~(str "Execute SQL query using fetch fn " result-set-worker)
           [data-source-or-connection# params#]
           (query-fetch# data-source-or-connection# ~sql-template-sym params#))))))
