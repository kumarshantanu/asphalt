(ns asphalt.test.perf
  (:require
    [clojure.test :refer :all]
    [clojure.java.jdbc :as jdbc]
    [criterium.core    :as cri]
    [asphalt.test-util :as u]
    [asphalt.core      :as a]))


(defn test-fixture
  [f]
  (u/create-db)
  (f)
  (u/drop-db))


(use-fixtures :each test-fixture)


(def db-spec {:datasource u/ds})

;"CREATE TABLE emp
;id     INT PRIMARY KEY AUTO_INCREMENT,
;name   VARCHAR(50) NOT NULL,
;salary INT NOT NULL,
;dept   VARCHAR(50)"


(a/defsql sql-insert "INSERT INTO emp (name, salary, dept) VALUES ($name^string, $salary^int, $dept^string)")
(a/defsql sql-delete "DELETE FROM emp")
(a/defsql sql-select "SELECT name^string, salary^int, dept^string FROM emp")

(def cjj-select "SELECT name, salary, dept FROM emp")


(deftest bench-insert-delete
  (testing "Insert and delete"
    (let [data {:name "Joe Coder"
               :salary 100000
               :dept "Accounts"}]
     (jdbc/with-db-connection [db-con db-spec]
       (cri/bench (do
                    (jdbc/insert! db-con :emp data)
                    (jdbc/delete! db-con :emp []))))
     (a/with-connection [conn u/ds]
       (cri/bench (do
                    (a/update sql-insert data conn)
                    (a/update sql-delete nil conn)))))))


(deftest bench-select
  (testing "Select row"
    (let [data {:name "Joe Coder"
                :salary 100000
                :dept "Accounts"}]
      (a/with-connection [conn u/ds]
        (a/update sql-insert data conn))
      ;; bench c.j.j
      (jdbc/with-db-connection [db-con db-spec]
        (cri/bench (jdbc/query db-con [cjj-select])))
      (jdbc/with-db-connection [db-con db-spec]
        (cri/bench (jdbc/query db-con [cjj-select] :as-arrays? true)))
      ;; bench asphalt
      (a/with-connection [conn u/ds]
        (cri/bench (a/query a/fetch-rows sql-select nil conn))))))