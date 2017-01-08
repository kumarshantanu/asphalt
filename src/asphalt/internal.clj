;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


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
    [java.util       Calendar Hashtable Map Properties TimeZone]
    [java.util.regex Pattern]
    [javax.naming    Context InitialContext]
    [javax.sql       DataSource]
    [asphalt.instrument JdbcEventFactory JdbcEventListener]))


;; ----- error reporting -----


(defn expected
  "Throw illegal input exception citing `expectation` and what was `found` did not match. Optionally accept a predicate
  fn to test `found` before throwing the exception."
  ([expectation found]
    (throw (IllegalArgumentException.
             (format "Expected %s, but found (%s) %s" expectation (class found) (pr-str found)))))
  ([pred expectation found]
    (when-not (pred found)
      (expected expectation found))))


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


(defn assert-symbols
  [& syms]
  (doseq [x syms]
    (when-not (symbol? x)
      (expected "a symbol" x))))


;; ----- utilities -----


(defmacro loop-indexed
  "Given a binding vector of a counter and one or more sequence-iteratees, run the loop as long as all the sequences
  have elements.
  Example: (loop-indexed [i 0
                          j [10 20 30]
                          k [:foo :bar :baz :qux]]
             (println i j k))
  Output: 0 (10 20 30) (:foo :bar :baz)
          1 (20 30) (:bar :baz)
          2 (30) (:baz)"
  [[counter init-count iteratee coll & more-pairs] & body]
  (assert-symbols counter iteratee)
  (->> (partition 2 more-pairs)
    (map first)
    (apply assert-symbols))
  (when (odd? (count more-pairs))
    (expected "an even number of forms in binding vector" more-pairs))
  (let [more-syms (->> (partition 2 more-pairs)
                    (map first))
        more-seq  (map  (fn [x] `(seq ~x)) more-syms)
        more-next (map  (fn [x] `(next ~x)) more-syms)]
    `(loop [~counter (long ~init-count)
            ~iteratee (seq ~coll)
            ~@(->> more-pairs
                (partition 2)
                (mapcat (fn [[i c]] [i (list seq c)])))]
      (when (and ~iteratee ~@more-syms)
        (do ~@body)
        (recur (unchecked-inc ~counter) (next ~iteratee) ~@more-next)))))


(defmacro each-indexed
  "Given a binding vector of a counter and one or more sequence locals, run the loop as long as all the sequences
  have elements.
  Example: (each-indexed [i 0
                          j [10 20 30]
                          k [:foo :bar :baz :qux]]
             (println i j k))
  Output: 0 10 :foo
          1 20 :bar
          2 30 :baz"
  [[counter init-count each coll & more-pairs] & body]
  (assert-symbols counter each)
  (->> (partition 2 more-pairs)
    (map first)
    (apply assert-symbols))
  (when (odd? (count more-pairs))
    (expected "an even number of forms in binding vector" more-pairs))
  (let [[iter & more-iters] (repeatedly (inc (quot (count more-pairs) 2)) gensym)
        partition-pairs (partition 2 more-pairs)
        iter-pairs (interleave more-iters (map second partition-pairs))
        each-pairs (->> more-iters
                     (map (partial list first))
                     (interleave (map first partition-pairs)))]
    `(loop-indexed [~counter ~init-count ~iter ~coll ~@iter-pairs]
       (let [~each (first ~iter)
             ~@each-pairs]
         ~@body))))


(defn named?
  [x]
  (instance? clojure.lang.Named x))


(defn as-str
  ^String [x]
  (if (named? x)
    (->> [(namespace x) (name x)]
      (remove nil?)
      (str/join "/"))
    (str x)))


(defn as-vector
  [x]
  (if (coll? x) (vec x)
    [x]))


;; ----- type definitions -----


(let [msg (str "valid SQL result type - either of " (vec (keys t/single-typemap)))]
  (defn expected-result-type
    ([suffix found]
      (expected (str msg suffix) found))
    ([found]
      (expected msg found))))


(let [msg (str "valid SQL single-value param type - either of " (vec (keys t/single-typemap)))]
  (defn expected-single-param-type
    ([suffix found]
      (expected (str msg suffix) found))
    ([found]
      (expected msg found))))


(let [msg (str "valid SQL param type - either of " (vec (keys t/all-typemap)))]
  (defn expected-param-type
    ([suffix found]
      (expected (str msg suffix) found))
    ([found]
      (expected msg found))))


;; ----- statement helpers -----


(defn prepare-statement
  ^PreparedStatement [^Connection connection ^String sql return-generated-keys?]
  (if return-generated-keys?
    (.prepareStatement connection sql Statement/RETURN_GENERATED_KEYS)
    (.prepareStatement connection sql)))


;; ----- transaction stuff -----


(defn set-txn-info
  [^Connection connection options]
  (when (contains? options :auto-commit?)
    (.setAutoCommit connection (:auto-commit? options)))
  (when (contains? options :isolation)
    (.setTransactionIsolation connection (:isolation options))))


(defmacro with-txn-info
  [connection txn-info & body]
  `(let [^java.sql.Connection conn# ~connection
         tinf# ~txn-info
         oinf# (if (contains? tinf# :auto-commit?)
                 {:auto-commit? (.getAutoCommit conn#)}
                 {})
         oinf# (if (contains? tinf# :isolation)
                 (assoc oinf# :isolation (.getTransactionIsolation conn#))
                 oinf#)]
     (try
       (set-txn-info conn# tinf#)
       ~@body
       (finally
         (set-txn-info conn# oinf#)))))


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


;; ----- protocol stuff -----


(extend-protocol t/IConnectionSource
  java.sql.Connection
  (create-connection      [this] (throw (UnsupportedOperationException.
                                          "Cannot create connection from a raw JDBC connection")))
  (obtain-connection      [this] this)
  (return-connection [this conn] (comment "do nothing"))
  javax.sql.DataSource
  (create-connection      [this] (.getConnection ^DataSource this))
  (obtain-connection      [this] (t/create-connection this))
  (return-connection [this conn] (.close ^Connection conn))
  java.util.Map
  (create-connection      [this] (t/obtain-connection (dissoc this :connection)))
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


;; ----- connection source helpers -----


(defmacro with-connection
  "Bind `connection` (symbol) to a connection obtained from specified source, evaluating the body of code in that
  context. Return connection to source in the end."
  [[connection connection-source] & body]
  (when-not (symbol? connection)
    (expected "a symbol" connection))
  `(let [conn-source# ~connection-source
         ~(if (:tag (meta connection))
            connection
            (vary-meta connection assoc :tag java.sql.Connection)) (t/obtain-connection conn-source#)]
     (try ~@body
       (finally
         (t/return-connection conn-source# ~connection)))))


(defmacro with-new-connection
  "Bind `connection` (symbol) to a connection created from specified source, evaluating the body of code in that
  context. Return connection to source in the end."
  [[connection connection-source] & body]
  (when-not (symbol? connection)
    (expected "a symbol" connection))
  `(let [conn-source# ~connection-source
         ~(if (:tag (meta connection))
            connection
            (vary-meta connection assoc :tag java.sql.Connection)) (t/create-connection conn-source#)]
     (try ~@body
       (finally
         (t/return-connection conn-source# ~connection)))))


;; ----- connection source instrumentation -----


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
