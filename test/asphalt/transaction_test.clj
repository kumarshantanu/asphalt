;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.transaction-test
  (:require
    [clojure.test :refer :all]
    [asphalt.test-util   :as u]
    [asphalt.core        :as a]
    [asphalt.type        :as t]
    [asphalt.transaction :as x]
    [asphalt.core-test   :as ct]))


(use-fixtures :each ct/test-fixture)


;"CREATE TABLE emp
;id        INT PRIMARY KEY AUTO_INCREMENT,
;emp_name  VARCHAR(50) NOT NULL,
;salary    INT NOT NULL,
;dept      VARCHAR(50),
;join_date DATE NOT NULL"


(defn transaction-commit-helper
  [{:keys [target-sql-count
           target-sql-insert
           target-sql-select
           target-sql-selfew
           target-sql-update
           target-sql-delete]}]
  (let [jd1 (u/make-date)
        vs1 ["Joe Coder" 100000 "Accounts" jd1]
        upa {:new-salary 110000
             :dept "Accounts"}
        vs2 ["Harry Hacker" 90000 "R&D" jd1]]
    ;; insert one record
    (a/genkey u/ds target-sql-insert vs1)
    ;; transaction that commits
    (x/with-transaction [txn u/ds] {:isolation :read-committed}
      (a/update txn target-sql-update upa)
      (a/genkey txn target-sql-insert vs2))
    ;; verify result
    (is (= 2 (a/query a/fetch-single-value
               u/ds target-sql-count [])) "Verify that rows were inserted")))


(deftest test-transaction-commit-template
  (transaction-commit-helper ct/t-all))


(deftest test-transaction-commit-map
  (transaction-commit-helper ct/m-all))


(defn transaction-rollback-helper
  [{:keys [target-sql-count
           target-sql-insert
           target-sql-select
           target-sql-selfew
           target-sql-update
           target-sql-delete]}]
  (let [jd1 (u/make-date)
        vs1 ["Joe Coder" 100000 "Accounts" jd1]
        upa {:new-salary 110000
             :dept "Accounts"}
        vs2 ["Harry Hacker" 90000 "R&D" jd1]]
    ;; insert one record
    (a/genkey u/ds target-sql-insert vs1)
    ;; transaction that commits
    (is (thrown? IllegalStateException
          (x/with-transaction [txn u/ds] {:isolation :read-committed}
            (a/update txn target-sql-update upa)
            (throw (IllegalStateException. "boom!"))
            (a/genkey txn target-sql-insert vs2))))
    ;; verify result
    (is (= 1 (a/query a/fetch-single-value
               u/ds target-sql-count [])) "Second row should not be inserted")
    (is (= vs1
          (vec (a/query a/fetch-single-row
                 u/ds target-sql-select []))) "Only original values should exist")))


(deftest test-transaction-rollback-template
  (transaction-rollback-helper ct/t-all))


(deftest test-transaction-rollback-map
  (transaction-rollback-helper ct/m-all))


;; ----- propagation tests -----


(def jd1 (u/make-date))
(def vs1 ["Joe Coder"     100000 "Accounts" jd1])
(def vs2 ["Harry Hacker"   90000 "R&D" jd1])
(def vs3 ["Neal Designer"  85000 "UX" jd1])
(def vs4 ["Susan Manager" 135000 "Customer delivery" jd1])


(defn find-count [] (a/query a/fetch-single-value
                      u/ds ct/t-count []))


(defn txn-helper-inner
  ([txn]
    (txn-helper-inner txn #(do)))
  ([txn middle]
    (a/genkey txn ct/t-insert vs1)
    (middle)
    (a/genkey txn ct/t-insert vs2)))


(defn txn-helper-outer
  ([txn child]
    (a/genkey txn ct/t-insert vs3)
    (a/genkey txn ct/t-insert vs4)
    (child txn))
  ([txn child child-middle]
    (a/genkey txn ct/t-insert vs3)
    (a/genkey txn ct/t-insert vs4)
    (child txn child-middle)))


(deftest test-tp-mandatory
  (let [outer (x/wrap-transaction-options txn-helper-outer {:propagation x/tp-required})
        inner (x/wrap-transaction-options txn-helper-inner {:propagation x/tp-mandatory})]
    (testing "without ongoing transaction"
      (is (thrown? clojure.lang.ExceptionInfo
            (inner u/ds))    "tp-mandatory should barf if no current transaction found")
      (is (= 0 (find-count)) "No record should be inserted"))
    (testing "with ongoing transaction"
      (outer u/ds inner)
      (is (= 4 (find-count)) "4 rows should be inserted"))
    (testing "with ongoing transaction - exception in inner"
      (is (thrown? IllegalStateException
            (outer u/ds inner #(throw (IllegalStateException. "barf")))))
      (is (= 4 (find-count)) "No new row should be inserted, because joined transaction"))))


(deftest test-tp-required
  (let [outer (x/wrap-transaction-options txn-helper-outer {:propagation x/tp-required})
        inner (x/wrap-transaction-options txn-helper-inner {:propagation x/tp-required})]
    (testing "without ongoing transaction"
      (inner u/ds)
      (is (= 2 (find-count)) "2 rows should be inserted, marking previous txn a success"))
    (testing "with ongoing transaction"
      (outer u/ds inner)
      (is (= 6 (find-count)) "4 new rows should be inserted, marking previous txn a success"))
    (testing "with ongoing transaction - exception in inner"
      (is (thrown? IllegalStateException
            (outer u/ds inner #(throw (IllegalStateException. "barf")))))
      (is (= 6 (find-count)) "No new row should be inserted, because joined transaction"))))


(deftest test-tp-nested
  (let [outer (x/wrap-transaction-options txn-helper-outer {:propagation x/tp-required
                                                            :failure-error? (fn [e] (not (instance?
                                                                                           IllegalStateException e)))})
        inner (x/wrap-transaction-options txn-helper-inner {:propagation x/tp-nested})]
    (testing "without ongoing transaction"
      (inner u/ds)
      (is (= 2 (find-count)) "2 rows should be inserted, marking previous txn a success"))
    (testing "with ongoing transaction"
      (outer u/ds inner)
      (is (= 6 (find-count)) "4 new rows should be inserted, marking previous txn a success"))
    (testing "with ongoing transaction - exception in inner"
      (is (thrown? IllegalStateException
            (outer u/ds inner #(throw (IllegalStateException. "barf")))))
      (is (= 8 (find-count)) "2 new rows should be inserted due to outer txn success"))))


(deftest test-tp-never
  (let [outer (x/wrap-transaction-options txn-helper-outer {:propagation x/tp-required})
        inner (x/wrap-transaction-options txn-helper-inner {:propagation x/tp-never})]
    (testing "without ongoing transaction"
      (inner u/ds)
      (is (= 2 (find-count)) "2 rows should be inserted, marking previous txn a success"))
    (testing "with ongoing transaction"
      (is (thrown? clojure.lang.ExceptionInfo
           (outer u/ds inner)))
      (is (= 2 (find-count)) "No new row should be inserted due to outer txn failure"))))


(deftest test-tp-not-supported
  (let [outer (x/wrap-transaction-options txn-helper-outer {:propagation x/tp-required
                                                            :failure-error? (fn [e] (not (instance?
                                                                                           IllegalStateException e)))})
        inner (x/wrap-transaction-options txn-helper-inner {:propagation x/tp-not-supported})]
    (testing "without ongoing transaction"
      (inner u/ds)
      (is (= 2 (find-count)) "2 rows should be inserted, marking previous txn a success"))
    (testing "with ongoing transaction"
      (outer u/ds inner)
      (is (= 6 (find-count)) "4 new rows should be inserted, marking previous txn a success"))
    (testing "with ongoing transaction - exception in inner"
      (is (thrown? IllegalStateException
            (outer u/ds inner #(throw (IllegalStateException. "barf")))))
      (is (= 9 (find-count)) "3 new rows should be inserted due to outer success + inner(non-txn) partial success"))))


(deftest test-tp-requires-new
  (let [outer (x/wrap-transaction-options txn-helper-outer {:propagation x/tp-required
                                                            :failure-error? (fn [e] (not (instance?
                                                                                           IllegalStateException e)))})
        inner (x/wrap-transaction-options txn-helper-inner {:propagation x/tp-requires-new})]
    (testing "without ongoing transaction"
      (inner u/ds)
      (is (= 2 (find-count)) "2 rows should be inserted, marking previous txn a success"))
    (testing "without ongoing transaction - throws exception"
      (is (thrown? IllegalStateException
            (inner u/ds #(throw (IllegalStateException. "barf")))))
      (is (= 2 (find-count)) "No row should be inserted, marking previous txn a failure"))
    (testing "with ongoing transaction"
      (outer u/ds inner)
      (is (= 6 (find-count)) "4 new rows should be inserted, marking previous txn a success"))
    (testing "with ongoing transaction - exception in inner"
      (is (thrown? IllegalStateException
            (outer u/ds inner #(throw (IllegalStateException. "barf")))))
      (is (= 8 (find-count)) "2 new rows should be inserted, because outer success"))))


(deftest test-tp-supports
  (let [outer (x/wrap-transaction-options txn-helper-outer {:propagation x/tp-required
                                                            :failure-error? (fn [e] (not (instance?
                                                                                           IllegalStateException e)))})
        inner (x/wrap-transaction-options txn-helper-inner {:propagation x/tp-supports})]
    (testing "without ongoing transaction"
      (inner u/ds)
      (is (= 2 (find-count)) "2 rows should be inserted, marking previous txn a success"))
    (testing "without ongoing transaction - throws exception"
      (is (thrown? IllegalStateException
            (inner u/ds #(throw (IllegalStateException. "barf")))))
      (is (= 3 (find-count)) "1 row should be inserted, marking previous non-txn a partial success"))
    (testing "with ongoing transaction"
      (outer u/ds inner)
      (is (= 7 (find-count)) "4 new rows should be inserted, marking previous txn a success"))
    (testing "with ongoing transaction - exception in inner"
      (is (thrown? IllegalStateException
            (outer u/ds inner #(throw (IllegalStateException. "barf")))))
      (is (= 7 (find-count)) "No new rows should be inserted, marking outer failure and inner failure"))))
