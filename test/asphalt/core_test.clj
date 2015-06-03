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

(a/defsql sql-insert "INSERT INTO emp (name, salary, dept) VALUES ($name^string, $salary^int, $dept^string)")

(a/defsql sql-select "SELECT name^string, salary^int, dept^string FROM emp")

(a/defsql sql-update "UPDATE emp SET salary = $new-salary^int WHERE dept = $dept^string")

(a/defsql sql-delete "DELETE FROM emp")

(deftest test-crud
  (let [vs1 ["Joe Coder" 100000 "Accounts"]
        row (zipmap [:name :salary :dept] vs1)
        vs2 ["Joe Coder" 110000 "Accounts"]
        upa {:new-salary 110000
             :dept "Accounts"}
        upv [110000 "Accounts"]]
    ;; create
    (let [generated-key (a/with-connection [conn u/ds]
                          (a/genkey a/fetch-single-value sql-insert row conn))]
      (is (= 1 generated-key) "Verify that insertion generated a key"))
    (is (= 1 (a/with-connection [conn u/ds]
               (a/query a/fetch-single-value sql-count [] conn))) "Verify that row was inserted")
    ;; retrieve
    (is (= vs1
          (vec (a/with-connection [conn u/ds]
                 (a/query a/fetch-single-row sql-select [] conn)))))
    ;; update
    (a/with-connection [conn u/ds]
      (a/update sql-update upa conn))
    (testing "vanilla vector params"
      (a/with-connection [conn u/ds]
        (a/update sql-update upv conn)))
    (is (= vs2
          (vec (a/with-connection [conn u/ds]
                 (a/query a/fetch-single-row sql-select [] conn)))))
    ;; delete
    (a/with-connection [conn u/ds]
      (a/update sql-delete [] conn))
    (is (= 0 (a/with-connection [conn u/ds]
               (a/query a/fetch-single-value sql-count [] conn))) "Verify that row was deleted")))


(deftest test-rows
  (let [vs1 ["Joe Coder" 100000 "Accounts"]
        row (zipmap [:name :salary :dept] vs1)]
    ;; 50 rows
    (a/with-connection [conn u/ds]
      (dotimes [_ 50]
        (a/update sql-insert row conn)))
    (is (= 50 (a/with-connection [conn u/ds]
                (a/query a/fetch-single-value sql-count [] conn))) "Verify that all rows were inserted")
    (doseq [each (a/with-connection [conn u/ds]
                   (a/query a/fetch-rows sql-select [] conn))]
      (is (= (vec each) vs1)))))


(a/defsql sql-batch-update "UPDATE emp SET salary = $new-salary WHERE id = $id")


(deftest test-batch-update
  (a/with-connection [conn u/ds]
    (a/batch-update sql-insert [["Joe Coder"     100000 "Accounts"]
                                ["Harry Hacker"   90000 "R&D"]
                                ["Sam Librarian"  85000 "Library"]
                                ["Kishore Newbie" 55000 "Sales"]
                                ["Neal Manager"  110000 "Marketing"]] conn))
  (a/with-connection [conn u/ds]
      (a/batch-update "UPDATE emp SET salary = ? WHERE id = ?" [[100001 1]
                                                                [ 90002 2]
                                                                [ 85003 3]
                                                                [ 55004 4]
                                                                [110005 5]] conn))
  (is (= [[100001]
          [ 90002]
          [ 85003]
          [ 55004]
          [110005]]
        (mapv vec (a/with-connection [conn u/ds]
                    (a/query a/fetch-rows "SELECT salary FROM emp" [] conn))))))


(deftest test-transaction-commit
  (let [vs1 ["Joe Coder" 100000 "Accounts"]
        upa {:new-salary 110000
             :dept "Accounts"}
        vs2 ["Harry Hacker" 90000 "R&D"]]
    ;; insert one record
    (a/with-connection [conn u/ds]
      (a/genkey a/fetch-single-value sql-insert vs1 conn))
    ;; transaction that commits
    (a/with-transaction [conn u/ds] :read-committed
      (a/update sql-update upa conn)
      (a/genkey a/fetch-single-value sql-insert vs2 conn))
    ;; verify result
    (is (= 2 (a/with-connection [conn u/ds]
               (a/query a/fetch-single-value sql-count [] conn))) "Verify that rows were inserted")))


(deftest test-transaction-rollback
  (let [vs1 ["Joe Coder" 100000 "Accounts"]
        upa {:new-salary 110000
             :dept "Accounts"}
        vs2 ["Harry Hacker" 90000 "R&D"]]
    ;; insert one record
    (a/with-connection [conn u/ds]
      (a/genkey a/fetch-single-value sql-insert vs1 conn))
    ;; transaction that commits
    (is (thrown? IllegalStateException
          (a/with-transaction [conn u/ds] :read-committed
            (a/update sql-update upa conn)
            (throw (IllegalStateException. "boom!"))
            (a/genkey a/fetch-single-value sql-insert vs2 conn))))
    ;; verify result
    (is (= 1 (a/with-connection [conn u/ds]
               (a/query a/fetch-single-value sql-count [] conn))) "Second row should not be inserted")
    (is (= vs1
          (vec (a/with-connection [conn u/ds]
                 (a/query a/fetch-single-row sql-select [] conn)))) "Only original values should exist")))
