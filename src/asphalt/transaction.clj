;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.transaction
  "Transaction handling and strategy (via `transaction propagation`). The following propagations are implemented:
  :mandatory, :nested, :never, :not-supported, :required, :requires-new, :supports
  See:
    http://docs.spring.io/spring/docs/current/javadoc-api/org/springframework/transaction/annotation/Propagation.html"
  (:require
    [asphalt.type     :as t]
    [asphalt.internal :as i])
  (:import
    [java.sql Connection Savepoint]))


;; ----- transaction propagation -----


(def ^{:doc "Use current transaction, throw exception if unavailable. Isolation is ignored."}
     tp-mandatory
  (reify t/ITransactionPropagation
    (execute-txn [this conn-source worker opts] (i/with-connection [^Connection connection conn-source]
                                                  (when (.getAutoCommit ^Connection connection)
                                                    (throw (ex-info "Expected a pre-existing transaction, found none"
                                                             {:txn-strategy :mandatory})))
                                                  (worker connection {})))
    (commit-txn   [this connection txn-context] (.commit ^Connection connection))
    (rollback-txn [this connection txn-context] (.rollback ^Connection connection))))


(declare tp-required)


(def ^{:doc "Start nested transaction if a transaction exists, behave like tp-required otherwise.
  Options:
    :isolation - transaction isolation"}
     tp-nested
  (reify t/ITransactionPropagation
    (execute-txn [this conn-source worker opts] (i/with-connection [^Connection conn conn-source]
                                                  (if (.getAutoCommit ^Connection conn)
                                                    (t/execute-txn tp-required conn-source worker opts)
                                                    (i/with-txn-info conn (assoc opts :auto-commit? false)
                                                      (let [^Savepoint savepoint (.setSavepoint ^Connection conn)]
                                                        (try
                                                          (worker conn {:savepoint savepoint})
                                                          (finally
                                                            (.releaseSavepoint ^Connection conn savepoint))))))))
    (commit-txn   [this connection txn-context] (.commit ^Connection connection))
    (rollback-txn [this connection txn-context] (if-let [^Savepoint savepoint (:savepoint txn-context)]
                                                  (.rollback ^Connection connection savepoint)
                                                  (.rollback ^Connection connection)))))


(def ^{:doc "Throw exception if a transaction exists, execute non-transactionally otherwise. Isolation is ignored."}
     tp-never
  (reify t/ITransactionPropagation
    (execute-txn [this conn-source worker opts] (i/with-connection [^Connection conn conn-source]
                                                  (when-not (.getAutoCommit ^Connection conn)
                                                    (throw (ex-info "Expected no pre-existing transaction, found one"
                                                             {:txn-strategy :never})))
                                                  (worker conn {})))
    (commit-txn   [this connection txn-context])
    (rollback-txn [this connection txn-context])))


(def ^{:doc "Execute non-transactionally regardless of whether a transaction exists. Requires new connection.
  Isolation is ignored."}
     tp-not-supported
  (reify t/ITransactionPropagation
    (execute-txn [this conn-source worker opts] (i/with-new-connection [^Connection conn conn-source]
                                                  (when-not (.getAutoCommit ^Connection conn)
                                                    (throw (ex-info "Expected no pre-existing transaction, found one"
                                                             {:txn-strategy :not-supported})))
                                                  (worker conn {})))
    (commit-txn   [this connection txn-context])
    (rollback-txn [this connection txn-context])))


(def ^{:doc "Use current transaction if it exists, create new transaction otherwise.
  Options:
    :isolation - the transaction isolation level"}
     tp-required
  (reify t/ITransactionPropagation
    (execute-txn [this conn-source worker opts] (i/with-connection [^Connection conn conn-source]
                                                  (if (.getAutoCommit ^Connection conn)
                                                    (i/with-txn-info conn (assoc opts :auto-commit? false)
                                                      (worker conn {}))
                                                    (i/with-txn-info conn (assoc opts :auto-commit? true)
                                                      (worker conn {})))))
    (commit-txn   [this connection txn-context] (.commit ^Connection connection))
    (rollback-txn [this connection txn-context] (.rollback ^Connection connection))))


(def ^{:doc "Create nested transaction if a transaction exists, create an independent transaction (using a new
  connection) otherwise.
  Options:
    :isolation - transaction isolation"}
     tp-requires-new
  (reify t/ITransactionPropagation
    (execute-txn [this conn-source worker opts] (i/with-connection [^Connection conn conn-source]
                                                  (if (.getAutoCommit ^Connection conn)
                                                    (i/with-new-connection [^Connection conn conn-source]
                                                      (i/with-txn-info conn (assoc opts :auto-commit? false)
                                                        (worker conn {})))
                                                    (i/with-txn-info conn (assoc opts :auto-commit? false)
                                                      (let [^Savepoint savepoint (.setSavepoint ^Connection conn)]
                                                        (try
                                                          (worker conn {:savepoint savepoint})
                                                          (finally
                                                            (.releaseSavepoint ^Connection conn savepoint))))))))
    (commit-txn   [this connection txn-context] (.commit ^Connection connection))
    (rollback-txn [this connection txn-context] (if-let [^Savepoint savepoint (:savepoint txn-context)]
                                                  (.rollback ^Connection connection savepoint)
                                                  (.rollback ^Connection connection)))))


(def ^{:doc "Use current transaction if one exists, execute non-transactionally otherwise. Isolation is ignored."}
     tp-supports
  (reify t/ITransactionPropagation
    (execute-txn [this conn-source worker opts] (i/with-connection [^Connection conn conn-source]
                                                  (if (.getAutoCommit ^Connection conn)
                                                    (worker conn nil)
                                                    (worker conn {}))))
    (commit-txn   [this connection txn-context] (when txn-context (.commit ^Connection connection)))
    (rollback-txn [this connection txn-context] (when txn-context (.rollback ^Connection connection)))))


;; ----- transactions -----


(defn invoke-with-transaction
  "Execute specified txn-worker in a transaction using connection-source and txn options:
  Arguments:
    txn-worker        | fn, accepts java.sql.Connection as argument
    connection-source | instance of asphalt.type.IConnectionSource
  Options:
    :isolation        | either of :none, :read-committed, :read-uncommitted, :repeatable-read, :serializable
    :propagation      | an asphalt.type.ITransactionPropagation instance (default: asphalt.transaction/tp-required)
    :success-result?  | fn that accepts result of txn-worker, returns true if it is a success, false otherwise
    :failure-error?   | fn that accepts txn-worker exception, returns true if it is a failure, false otherwise"
  [txn-worker connection-source {:keys [isolation propagation success-result? failure-error?]
                                 :or {propagation     tp-required
                                      success-result? (fn [result] true)
                                      failure-error?  (fn [error] true)}
                                 :as options}]
  (t/execute-txn propagation connection-source (fn [^Connection connection txn-context]
                                                 (try
                                                   (let [result (txn-worker connection)]
                                                     (if (success-result? result)
                                                       (t/commit-txn propagation connection txn-context)
                                                       (t/rollback-txn propagation connection txn-context))
                                                     result)
                                                   (catch Throwable error
                                                     (if (failure-error? error)
                                                       (t/rollback-txn propagation connection txn-context)
                                                       (t/commit-txn propagation connection txn-context))
                                                     (throw error))))
    (merge options
      (if isolation
        {:isolation (i/resolve-txn-isolation isolation)}
        {}))))


(defmacro with-transaction
  "Bind `connection` (symbol) to a connection obtained from specified source, setup a transaction evaluating the
  body of code in specified transaction context. Restore old transaction context upon exit on best effort basis.
  See: invoke-with-transaction"
  [[connection connection-source] options & body]
  (when-not (symbol? connection)
    (i/unexpected "a symbol" connection))
  `(invoke-with-transaction (^:once fn* [~(if (:tag (meta connection))
                                            connection
                                            (vary-meta connection assoc :tag java.sql.Connection))] ~@body)
     ~connection-source ~options))
