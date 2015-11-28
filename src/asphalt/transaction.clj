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
    [java.sql Connection Savepoint]
    [asphalt.type TxnConnectionSource]))


(defn assoc-connection
  "Associate specified connection with given asphalt.type.TxnConnectionSource instance."
  [txn-connection-source connection]
  (assoc txn-connection-source :connection connection))


(defmacro with-txn-connection-source
  "Bind `txn-connection-source` (symbol or destrucuring form) to a transactional connection source (an instance of
  asphalt.type.TxnConnectionSource) made from specified connection source, and execute body of code in that context."
  [[txn-connection-source connection-source] & body]
  `(let [conn-source# ~connection-source]
     (if (instance? TxnConnectionSource conn-source#)
       (let [~txn-connection-source conn-source#]
         ~@body)
       (i/with-connection [connection# conn-source#]
         (let [~txn-connection-source (t/->TxnConnectionSource connection# conn-source#)]
           ~@body)))))


;; ----- transaction propagation -----


(def ^{:doc "Use current transaction, throw exception if unavailable. Isolation is ignored."}
     tp-mandatory
  (reify t/ITransactionPropagation
    (execute-txn [this conn-source worker opts] (with-txn-connection-source [{:keys [connection] :as tcs} conn-source]
                                                  (when (.getAutoCommit ^Connection connection)
                                                    (throw (ex-info "Expected a pre-existing transaction, found none"
                                                             {:txn-strategy :mandatory})))
                                                  (worker tcs {})))
    (commit-txn   [this connection txn-context] (.commit ^Connection connection))
    (rollback-txn [this connection txn-context] (.rollback ^Connection connection))))


(declare tp-required)


(def ^{:doc "Start nested transaction if a transaction exists, behave like tp-required otherwise.
  Options:
    :isolation - transaction isolation"}
     tp-nested
  (reify t/ITransactionPropagation
    (execute-txn [this conn-source worker opts] (with-txn-connection-source [{:keys [connection] :as tcs} conn-source]
                                                  (if (.getAutoCommit ^Connection connection)
                                                    (t/execute-txn tp-required tcs worker opts)
                                                    (i/with-txn-info connection (assoc opts :auto-commit? false)
                                                      (let [^Savepoint savepoint (.setSavepoint ^Connection connection)]
                                                        (try
                                                          (worker tcs {:savepoint savepoint})
                                                          (finally
                                                            (.releaseSavepoint ^Connection connection savepoint))))))))
    (commit-txn   [this connection txn-context] (.commit ^Connection connection))
    (rollback-txn [this connection txn-context] (if-let [^Savepoint savepoint (:savepoint txn-context)]
                                                  (.rollback ^Connection connection savepoint)
                                                  (.rollback ^Connection connection)))))


(def ^{:doc "Throw exception if a transaction exists, execute non-transactionally otherwise. Isolation is ignored."}
     tp-never
  (reify t/ITransactionPropagation
    (execute-txn [this conn-source worker opts] (with-txn-connection-source [{:keys [connection] :as tcs} conn-source]
                                                  (when-not (.getAutoCommit ^Connection connection)
                                                    (throw (ex-info "Expected no pre-existing transaction, found one"
                                                             {:txn-strategy :never})))
                                                  (worker tcs {})))
    (commit-txn   [this connection txn-context])
    (rollback-txn [this connection txn-context])))


(def ^{:doc "Execute non-transactionally regardless of whether a transaction exists. Requires new connection.
  Isolation is ignored."}
     tp-not-supported
  (reify t/ITransactionPropagation
    (execute-txn [this conn-source worker opts] (i/with-new-connection [^Connection connection conn-source]
                                                  (let [tcs (if (instance? TxnConnectionSource conn-source)
                                                              (assoc-connection conn-source connection)
                                                              (t/->TxnConnectionSource connection conn-source))]
                                                    (worker tcs {}))))
    (commit-txn   [this connection txn-context])
    (rollback-txn [this connection txn-context])))


(def ^{:doc "Use current transaction if it exists, create new transaction otherwise.
  Options:
    :isolation - the transaction isolation level"}
     tp-required
  (reify t/ITransactionPropagation
    (execute-txn [this conn-source worker opts] (with-txn-connection-source [{:keys [connection] :as tcs} conn-source]
                                                  (i/with-txn-info connection (assoc opts :auto-commit? false)
                                                    (worker tcs {}))))
    (commit-txn   [this connection txn-context] (.commit ^Connection connection))
    (rollback-txn [this connection txn-context] (.rollback ^Connection connection))))


(def ^{:doc "Create nested transaction if a transaction exists, create an independent transaction (using a new
  connection) otherwise.
  Options:
    :isolation - transaction isolation"}
     tp-requires-new
  (reify t/ITransactionPropagation
    (execute-txn [this conn-source worker opts] (with-txn-connection-source [{:keys [connection] :as tcs} conn-source]
                                                  (if (.getAutoCommit ^Connection connection)
                                                    (i/with-new-connection [^Connection connection tcs]
                                                      (i/with-txn-info connection (assoc opts :auto-commit? false)
                                                        (worker (assoc-connection tcs connection) {})))
                                                    (i/with-txn-info connection (assoc opts :auto-commit? false)
                                                      (let [^Savepoint savepoint (.setSavepoint ^Connection connection)]
                                                        (try
                                                          (worker tcs {:savepoint savepoint})
                                                          (finally
                                                            (.releaseSavepoint ^Connection connection savepoint))))))))
    (commit-txn   [this connection txn-context] (.commit ^Connection connection))
    (rollback-txn [this connection txn-context] (if-let [^Savepoint savepoint (:savepoint txn-context)]
                                                  (.rollback ^Connection connection savepoint)
                                                  (.rollback ^Connection connection)))))


(def ^{:doc "Use current transaction if one exists, execute non-transactionally otherwise. Isolation is ignored."}
     tp-supports
  (reify t/ITransactionPropagation
    (execute-txn [this conn-source worker opts] (with-txn-connection-source [{:keys [connection] :as tcs} conn-source]
                                                  (let [auto-commit? (.getAutoCommit ^Connection connection)]
                                                    (worker tcs {:commit?   (not auto-commit?)
                                                                 :rollback? (not auto-commit?)}))))
    (commit-txn   [this connection txn-context] (when (:commit?   txn-context) (.commit   ^Connection connection)))
    (rollback-txn [this connection txn-context] (when (:rollback? txn-context) (.rollback ^Connection connection)))))


;; ----- transactions -----


(defn invoke-with-transaction
  "Execute specified txn-worker in a transaction using connection-source and txn options:
  Arguments:
    txn-worker        | fn, accepts asphalt.type.TxnConnectionSource as argument
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
  (t/execute-txn propagation connection-source
    (fn [^TxnConnectionSource txn-connection-source txn-context]
      (try
        (let [result (txn-worker txn-connection-source)]
          (if (success-result? result)
            (t/commit-txn   propagation (:connection txn-connection-source) txn-context)
            (t/rollback-txn propagation (:connection txn-connection-source) txn-context))
          result)
        (catch Throwable error
          (if (failure-error? error)
            (t/rollback-txn propagation (:connection txn-connection-source) txn-context)
            (t/commit-txn   propagation (:connection txn-connection-source) txn-context))
          (throw error))))
    (merge options
      (if isolation
        {:isolation (i/resolve-txn-isolation isolation)}
        {}))))


(defmacro with-transaction
  "Setup a transaction and evaluate the body of code in specified transaction context, binding `txn-connection-source`
  (symbol or destructuring form) to a transactional connection source (asphalt.type.TxnConnectionSource instance) made
  from specified connection source. Restore old transaction context upon exit on best effort basis.
  See: invoke-with-transaction"
  [[txn-connection-source connection-source] options & body]
  `(invoke-with-transaction (^:once fn* [~txn-connection-source] ~@body)
     ~connection-source ~options))


(defn wrap-transaction-options
  "Wrap a fn accepting connection-source as first argument with transaction options. Return the wrapped fn that accepts
  connection-source as first argument, but internally invokes f with transactional connection-source as first argument.
  See: invoke-with-transaction"
  [f txn-options]
  (fn [connection-source & args]
    (let [txn-worker (fn [^TxnConnectionSource txn-connection-source]
                       (apply f txn-connection-source args))]
      (invoke-with-transaction txn-worker connection-source txn-options))))
