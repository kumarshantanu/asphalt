(ns asphalt.core-test
  (:require
    [clojure.test :refer :all]
    [asphalt.test-util :as u]
    [asphalt.core      :as a]
    [asphalt.type      :as t])
  (:import
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

(a/defsql t-insert "INSERT INTO emp (name, salary, dept) VALUES (^string $name, ^int $salary, ^string $dept)")

(a/defsql t-select "SELECT ^string name, -- ^int age,
-- ^boolean gender,
^int salary, ^string dept FROM emp")

(a/defsql t-selfew "SELECT ^string name, ^int salary, ^string dept FROM emp WHERE name = ?")

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

(defn crud-helper
  [{:keys [target-sql-count
           target-sql-insert
           target-sql-select
           target-sql-selfew
           target-sql-update
           target-sql-delete]}]
  (let [vs1 ["Joe Coder" 100000 "Accounts"]
        row (zipmap [:name :salary :dept] vs1)
        vs2 ["Joe Coder" 110000 "Accounts"]
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
    (a/with-connection [conn u/ds]
      (a/update conn target-sql-delete []))
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
  (let [vs1 ["Joe Coder" 100000 "Accounts"]
        row (zipmap [:name :salary :dept] vs1)]
    ;; 50 rows
    (dotimes [_ 50]
      (a/update u/ds target-sql-insert row))
    (is (= 50 (a/query a/fetch-single-value
                u/ds target-sql-count [])) "Verify that all rows were inserted")
    (doseq [each (a/query a/fetch-rows
                   u/ds target-sql-select [])]
      (is (= (vec each) vs1)))))


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


(defn transaction-commit-helper
  [{:keys [target-sql-count
           target-sql-insert
           target-sql-select
           target-sql-selfew
           target-sql-update
           target-sql-delete]}]
  (let [vs1 ["Joe Coder" 100000 "Accounts"]
        upa {:new-salary 110000
             :dept "Accounts"}
        vs2 ["Harry Hacker" 90000 "R&D"]]
    ;; insert one record
    (a/genkey u/ds target-sql-insert vs1)
    ;; transaction that commits
    (a/with-transaction [conn u/ds] :read-committed
      (a/update conn target-sql-update upa)
      (a/genkey conn target-sql-insert vs2))
    ;; verify result
    (is (= 2 (a/query a/fetch-single-value
               u/ds target-sql-count [])) "Verify that rows were inserted")))


(deftest test-transaction-commit-template
  (transaction-commit-helper t-all))


(deftest test-transaction-commit-map
  (transaction-commit-helper m-all))


(defn transaction-rollback-helper
  [{:keys [target-sql-count
           target-sql-insert
           target-sql-select
           target-sql-selfew
           target-sql-update
           target-sql-delete]}]
  (let [vs1 ["Joe Coder" 100000 "Accounts"]
        upa {:new-salary 110000
             :dept "Accounts"}
        vs2 ["Harry Hacker" 90000 "R&D"]]
    ;; insert one record
    (a/genkey u/ds target-sql-insert vs1)
    ;; transaction that commits
    (is (thrown? IllegalStateException
          (a/with-transaction [conn u/ds] :read-committed
            (a/update conn target-sql-update upa)
            (throw (IllegalStateException. "boom!"))
            (a/genkey conn target-sql-insert vs2))))
    ;; verify result
    (is (= 1 (a/query a/fetch-single-value
               u/ds target-sql-count [])) "Second row should not be inserted")
    (is (= vs1
          (vec (a/query a/fetch-single-row
                 u/ds target-sql-select []))) "Only original values should exist")))


(deftest test-transaction-rollback-template
  (transaction-rollback-helper t-all))


(deftest test-transaction-rollback-map
  (transaction-rollback-helper m-all))
