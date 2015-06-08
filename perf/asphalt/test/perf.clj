(ns asphalt.test.perf
  (:require
    [clojure.test :refer :all]
    [clojure.java.jdbc :as jdbc]
    [citius.core       :as c]
    [asphalt.test-util :as u]
    [asphalt.core      :as a]))


(use-fixtures :once (c/make-bench-wrapper ["clojure.java.jdbc" "Asphalt"]
                      {:chart-title "Clojure.java.jdbc vs Asphalt"
                       :chart-filename (format "bench-clj-%s.png" c/clojure-version-str)}))


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
       (a/with-connection [conn u/ds]
         (c/compare-perf "insert-delete"
           (do
             (jdbc/insert! db-con :emp data)
             (jdbc/delete! db-con :emp []))
           (do
             (a/update conn sql-insert data)
             (a/update conn sql-delete nil))))))))


(deftest bench-select
  (testing "Select row"
    (let [data {:name "Joe Coder"
                :salary 100000
                :dept "Accounts"}]
      (a/with-connection [conn u/ds]
        (a/update conn sql-insert data))
      ;; bench c.j.j normal with asphalt
      (jdbc/with-db-connection [db-con db-spec]
        (a/with-connection [conn u/ds]
          (c/compare-perf "select-row"
            (jdbc/query db-con [cjj-select])
            (a/query a/fetch-rows
              conn sql-select nil))))
      ;; bench c.j.j `:as-arrays? true` with asphalt
      (jdbc/with-db-connection [db-con db-spec]
        (a/with-connection [conn u/ds]
          (c/compare-perf "select-row-as-arrays"
            (jdbc/query db-con [cjj-select] :as-arrays? true)
            (a/query a/fetch-rows
              conn sql-select nil)))))))
