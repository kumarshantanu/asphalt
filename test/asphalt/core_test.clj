(ns asphalt.core-test
  (:require
    [clojure.test :refer :all]
    [asphalt.test-util :as u]
    [asphalt.core      :as a]))


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


(a/defsql sql-count  "SELECT COUNT(*) FROM emp")

(a/defsql sql-insert "INSERT INTO emp (name, salary, dept) VALUES (^string $name, ^int $salary, ^string $dept)")

(a/defsql sql-select "SELECT ^string name, ^int salary, ^string dept FROM emp")

(a/defsql sql-selfew "SELECT ^string name, ^int salary, ^string dept FROM emp WHERE name = ?")

(a/defsql sql-update "UPDATE emp SET salary = ^int $new-salary WHERE dept = ^string $dept")

(a/defsql sql-delete "DELETE FROM emp")

(deftest test-crud
  (let [vs1 ["Joe Coder" 100000 "Accounts"]
        row (zipmap [:name :salary :dept] vs1)
        vs2 ["Joe Coder" 110000 "Accounts"]
        upa {:new-salary 110000
             :dept "Accounts"}
        upv [110000 "Accounts"]]
    ;; create
    (let [generated-key (a/genkey u/ds sql-insert row)]
      (is (= 1 generated-key) "Verify that insertion generated a key"))
    (is (= 1 (a/query a/fetch-single-value
               u/ds sql-count [])) "Verify that row was inserted")
    ;; retrieve
    (is (= vs1
          (vec (a/query a/fetch-single-row
                 u/ds sql-select []))))
    (is (= vs1
          (vec (a/query a/fetch-single-row
                 u/ds sql-selfew [(first vs1)]))))
    ;; update
    (a/update u/ds sql-update upa)
    (testing "vanilla vector params"
      (a/update u/ds sql-update upv))
    (is (= vs2
          (vec (a/query a/fetch-single-row
                 u/ds sql-select []))))
    ;; delete
    (a/with-connection [conn u/ds]
      (a/update conn sql-delete []))
    (is (= 0 (a/query a/fetch-single-value
               u/ds sql-count [])) "Verify that row was deleted")))


(deftest test-rows
  (let [vs1 ["Joe Coder" 100000 "Accounts"]
        row (zipmap [:name :salary :dept] vs1)]
    ;; 50 rows
    (dotimes [_ 50]
      (a/update u/ds sql-insert row))
    (is (= 50 (a/query a/fetch-single-value
                u/ds sql-count [])) "Verify that all rows were inserted")
    (doseq [each (a/query a/fetch-rows
                   u/ds sql-select [])]
      (is (= (vec each) vs1)))))


(a/defsql sql-batch-update "UPDATE emp SET salary = $new-salary WHERE id = $id")


(deftest test-batch-update
  (a/batch-update u/ds sql-insert [["Joe Coder"     100000 "Accounts"]
                                   ["Harry Hacker"   90000 "R&D"]
                                   ["Sam Librarian"  85000 "Library"]
                                   ["Kishore Newbie" 55000 "Sales"]
                                   ["Neal Manager"  110000 "Marketing"]])
  (a/batch-update u/ds "UPDATE emp SET salary = ? WHERE id = ?" [[100001 1]
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


(deftest test-transaction-commit
  (let [vs1 ["Joe Coder" 100000 "Accounts"]
        upa {:new-salary 110000
             :dept "Accounts"}
        vs2 ["Harry Hacker" 90000 "R&D"]]
    ;; insert one record
    (a/genkey u/ds sql-insert vs1)
    ;; transaction that commits
    (a/with-transaction [conn u/ds] :read-committed
      (a/update conn sql-update upa)
      (a/genkey conn sql-insert vs2))
    ;; verify result
    (is (= 2 (a/query a/fetch-single-value
               u/ds sql-count [])) "Verify that rows were inserted")))


(deftest test-transaction-rollback
  (let [vs1 ["Joe Coder" 100000 "Accounts"]
        upa {:new-salary 110000
             :dept "Accounts"}
        vs2 ["Harry Hacker" 90000 "R&D"]]
    ;; insert one record
    (a/genkey u/ds sql-insert vs1)
    ;; transaction that commits
    (is (thrown? IllegalStateException
          (a/with-transaction [conn u/ds] :read-committed
            (a/update conn sql-update upa)
            (throw (IllegalStateException. "boom!"))
            (a/genkey conn sql-insert vs2))))
    ;; verify result
    (is (= 1 (a/query a/fetch-single-value
               u/ds sql-count [])) "Second row should not be inserted")
    (is (= vs1
          (vec (a/query a/fetch-single-row
                 u/ds sql-select []))) "Only original values should exist")))
