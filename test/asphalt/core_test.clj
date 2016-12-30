;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.core-test
  (:require
    [clojure.test :refer :all]
    [asphalt.test-util :as u]
    [asphalt.core      :as a]
    [asphalt.type      :as t]
    [asphalt.transaction :as x])
  (:import
    [java.sql Date SQLTimeoutException]
    [clojure.lang ExceptionInfo]))


(defn test-fixture
  [f]
  (u/create-db)
  (f)
  (u/drop-db))


(use-fixtures :each test-fixture)


;"CREATE TABLE emp
;id        INT PRIMARY KEY AUTO_INCREMENT,
;emp_name  VARCHAR(50) NOT NULL,
;salary    INT NOT NULL,
;dept      VARCHAR(50),
;join_date DATE NOT NULL"


;; ----- templates -----

(a/defsql t-count  "SELECT COUNT(*) FROM emp")

(a/defsql t-insert "INSERT INTO emp (name, salary, dept, joined)
VALUES (^string $name, ^int $salary, ^string $dept, ^date $joined)")

(a/defsql t-select "SELECT ^string name, -- ^int age,
-- ^boolean gender,
^int salary, ^string dept, ^date joined FROM emp")

(a/defsql t-selfew "SELECT ^string name, ^int salary, ^string dept, ^date joined FROM emp WHERE name = ?")

(a/defquery t-qfetch "SELECT ^string name, ^int salary, ^string dept, ^date joined FROM emp WHERE name = ?"
  a/fetch-single-row {})

(a/defsql t-update "UPDATE emp SET salary = ^int $new-salary WHERE dept = ^string $dept")

(a/defsql t-delete "DELETE FROM emp")

(a/defsql t-batch-update "UPDATE emp SET salary = $new-salary WHERE id = $id")

(def t-all {:target-sql-count  t-count
            :target-sql-insert t-insert
            :target-sql-select t-select
            :target-sql-selfew t-selfew
            :target-sql-update t-update
            :target-sql-delete t-delete
            :target-sql-batch-update t-batch-update})


;; ----- maps -----

(def m-count  {:sql (:sql t-count)})

(def m-insert {:sql (:sql t-insert)
               :param-keys  (:param-keys t-insert)
               :param-types (:param-types t-insert)})

(def m-select {:sql (:sql t-select)
               :result-types (:result-types t-select)})

(def m-selfew {:sql (:sql t-selfew)
               :result-types (:result-types t-selfew)})

(def m-update {:sql (:sql t-update)
               :param-keys  (:param-keys t-update)
               :param-types (:param-types t-update)})

(def m-delete {:sql (:sql t-delete)})

(def m-batch-update {:sql (:sql t-batch-update)
                     :param-keys (:param-keys t-batch-update)
                     :param-types (:param-types t-batch-update)})

(def m-all {:target-sql-count  m-count
            :target-sql-insert m-insert
            :target-sql-select m-select
            :target-sql-selfew m-selfew
            :target-sql-update m-update
            :target-sql-delete m-delete
            :target-sql-batch-update m-batch-update})


;; ----- tests -----


(defn params-helper
  [{:keys [target-sql-count
           target-sql-insert
           target-sql-select
           target-sql-selfew
           target-sql-update
           target-sql-delete]}]
  (let [jd1 (u/make-date)
        vs1 ["Joe Coder" 100000 "Accounts" jd1]
        row (zipmap [:name :salary :dept :joined] vs1)
        vs2 ["Joe Coder" 110000 "Accounts" jd1]
        upa {:new-salary 110000
             :dept "Accounts"}
        upb [false "Accounts"]  ; bad params, first param should be int
        upv [110000 "Accounts"]]
    ;; create
    (let [params-setter (fn [sql-source prepared-statement params]
                          (a/lay-params prepared-statement [:string :int :string :date] [:name :salary :dept :joined]
                            params))
          generated-key (a/genkey params-setter a/fetch-single-value
                          u/ds target-sql-insert row)]
      (is (= 1 generated-key) "Verify that insertion generated a key"))
    (is (= 1 (a/query a/fetch-single-value
               u/ds target-sql-count [])) "Verify that row was inserted")
    ;; retrieve
    (is (= vs1
          (vec (a/query a/fetch-single-row
                 u/ds target-sql-select []))))
    (is (= vs1
          (vec (a/query (fn [sql-source prepared-statement params]
                          (a/lay-params prepared-statement [:string] [:name] params))
                 a/fetch-single-row
                 u/ds target-sql-selfew [(first vs1)]))))
    (is (= vs1 (vec (t-qfetch u/ds [(first vs1)]))))
    ;; update
    (let [update-setter (fn [sql-source prepared-statement params]
                          (a/lay-params prepared-statement [:int :string] [:new-salary :dept] params))]
      (a/update update-setter
        u/ds target-sql-update upa)
      (testing "bad vector params"
        (is (thrown? ExceptionInfo
              (a/update u/ds target-sql-update upb))))
      (testing "vanilla vector params"
        (a/update update-setter u/ds target-sql-update upv))
      (is (= vs2
            (vec (a/query a/fetch-single-row
                   u/ds target-sql-select [])))))))


(deftest test-params-template
  (params-helper t-all))


(deftest test-params-map
  (params-helper m-all))


(defn crud-helper
  [{:keys [target-sql-count
           target-sql-insert
           target-sql-select
           target-sql-selfew
           target-sql-update
           target-sql-delete]}]
  (let [jd1 (u/make-date)
        vs1 ["Joe Coder" 100000 "Accounts" jd1]
        row (zipmap [:name :salary :dept :joined] vs1)
        vs2 ["Joe Coder" 110000 "Accounts" jd1]
        upa {:new-salary 110000
             :dept "Accounts"}
        upb [false "Accounts"]  ; bad params, first param should be int
        upv [110000 "Accounts"]]
    ;; create
    (let [generated-key (a/genkey u/ds target-sql-insert row)]
      (is (= 1 generated-key) "Verify that insertion generated a key"))
    (is (= 1 (a/query a/fetch-single-value
               u/ds target-sql-count [])) "Verify that row was inserted")
    ;; retrieve
    (is (= vs1
          (vec (a/query a/fetch-single-row
                 u/ds target-sql-select []))))
    (is (= vs1
          (vec (a/query a/fetch-single-row
                 u/ds target-sql-selfew [(first vs1)]))))
    (is (= vs1 (vec (t-qfetch u/ds [(first vs1)]))))
    ;; update
    (a/update u/ds target-sql-update upa)
    (testing "bad vector params"
      (is (thrown? ExceptionInfo
            (a/update u/ds target-sql-update upb))))
    (testing "vanilla vector params"
      (a/update u/ds target-sql-update upv))
    (is (= vs2
          (vec (a/query a/fetch-single-row
                 u/ds target-sql-select []))))
    ;; delete
    (a/update u/ds target-sql-delete [])
    (is (= 0 (a/query a/fetch-single-value
               u/ds target-sql-count [])) "Verify that row was deleted")))


(deftest test-crud-template
  (crud-helper t-all))


(deftest test-crud-map
  (crud-helper m-all))


(defn rows-helper
  [{:keys [target-sql-count
           target-sql-insert
           target-sql-select
           target-sql-selfew
           target-sql-update
           target-sql-delete]}]
  (let [jd1 (u/make-date)
        vs1 ["Joe Coder" 100000 "Accounts" jd1]
        row (zipmap [:name :salary :dept :joined] vs1)]
    ;; fetch single row in absence of rows
    (is (= (vec (a/query (partial a/fetch-single-row (a/default-fetch vs1))
                  u/ds target-sql-select []))
          vs1))
    ;; fetch single column value in absence of rows
    (is (= (a/query (partial a/fetch-single-value (assoc (a/default-fetch 1000)
                                                    :column-index 2))
             u/ds target-sql-selfew ["Harry"])
          1000))
    ;; 50 rows
    (dotimes [_ 50]
      (a/update u/ds target-sql-insert row))
    (is (= 50 (a/query a/fetch-single-value
                u/ds target-sql-count [])) "Verify that all rows were inserted")
    (doseq [each (a/query a/fetch-rows
                   u/ds target-sql-select [])]
      (is (= (vec each) vs1)))
    ;; fetch single row in presence of multiple rows
    (is (= (vec (a/query (partial a/fetch-single-row (a/default-fetch nil))
                  u/ds target-sql-select []))
          vs1))
    ;; fetch single column value in absence of rows
    (is (= (a/query (partial a/fetch-single-value (assoc (a/default-fetch nil)
                                                    :column-index 2))
             u/ds target-sql-selfew ["Joe Coder"])
          100000))
    ;; test letcol
    (testing "letcol"
      (let [run-query (fn [row-maker]
                        (a/query (partial a/fetch-single-row {:row-maker row-maker
                                                              :on-multi (fn [_ _ v] v)})
                          u/ds target-sql-selfew ["Joe Coder"]))]
        (is (= vs1
              (run-query (fn [_ rs _]
                           (a/letcol [[name salary dept joined] rs]
                             [name salary dept joined])))))
        (is (= vs1
              (run-query (fn [_ rs _]
                           (a/letcol [[^string name ^int salary ^string dept ^date joined] rs]
                             [name salary dept joined])))))
        (is (= vs1
              (run-query (fn [_ rs _]
                           (a/letcol [{:labels  [^string name]
                                       :_labels [^int salary]
                                       ^string dept 3
                                       ^date   joined 4} rs]
                             [name salary dept joined])))))))))


(deftest test-rows-template
  (rows-helper t-all))


(deftest test-rows-map
  (rows-helper m-all))


(defn batch-update-helper
  [{:keys [target-sql-count
           target-sql-insert
           target-sql-select
           target-sql-selfew
           target-sql-update
           target-sql-delete
           target-sql-batch-update]}]
  (a/batch-update u/ds target-sql-insert [["Joe Coder"     100000 "Accounts"]
                                          ["Harry Hacker"   90000 "R&D"]
                                          ["Sam Librarian"  85000 "Library"]
                                          ["Kishore Newbie" 55000 "Sales"]
                                          ["Neal Manager"  110000 "Marketing"]])
  (a/batch-update u/ds target-sql-batch-update [[100001 1]
                                                [ 90002 2]
                                                [ 85003 3]
                                                [ 55004 4]
                                                [110005 5]])
  (is (= [[100001]
          [ 90002]
          [ 85003]
          [ 55004]
          [110005]]
        (mapv vec (a/query a/fetch-rows
                    u/ds "SELECT salary FROM emp" [])))))


(deftest test-batch-update-template
  (batch-update-helper t-all))


(deftest test-batch-update-map
  (batch-update-helper m-all))


(deftest test-query-timeout
  (let [jd1 (u/make-date)
        vs1 ["Joe Coder" 100000 "Accounts" jd1]
        row (zipmap [:name :salary :dept :joined] vs1)]
    (let [generated-key (a/genkey u/delay-ds t-insert row)]
      (is (= 1 generated-key) "Verify that insertion generated a key"))
    (is (thrown? SQLTimeoutException
          (a/query (a/set-params-with-query-timeout 1) a/fetch-single-value
            u/delay-ds t-count [])) "Query should throw exception on delay")))
