;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.test.perf
  (:require
    [clojure.test :refer :all]
    [clojure.java.jdbc :as jdbc]
    [citius.core       :as c]
    [asphalt.test-util :as u]
    [asphalt.core      :as a]
    [asphalt.type      :as t]))


(use-fixtures :once (c/make-bench-wrapper ["clojure.java.jdbc" "Asphalt-SQL" "Asphalt-template" "Asphalt-map"]
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


(a/defsql t-insert "INSERT INTO emp (name, salary, dept) VALUES (^string $name, ^int $salary, ^string $dept)")
(a/defsql t-delete "DELETE FROM emp")
(a/defsql t-select "SELECT ^string name, ^int salary, ^string dept FROM emp")

(def ^String s-insert (:sql t-insert))
(def ^String s-delete (:sql t-delete))
(def ^String s-select (:sql t-select))

(def m-insert {:sql s-insert
               :param-keys [:name :salary :dept]
               :param-types (apply vector-of :byte (:param-types t-insert))})
(def m-delete {:sql s-delete})
(def m-select {:sql s-select
               :result-types (apply vector-of :byte (:result-types t-select))})


(deftest bench-insert-delete
  (testing "Insert and delete"
    (let [data {:name "Joe Coder"
               :salary 100000
               :dept "Accounts"}
          vdat (vec (map data [:name :salary :dept]))]
     (jdbc/with-db-connection [db-con db-spec]
       (a/with-connection [conn u/ds]
         (c/compare-perf "insert-delete"
           (do
             (jdbc/insert! db-con :emp data)
             (jdbc/delete! db-con :emp []))
           (do
             (a/update conn s-insert vdat)
             (a/update conn s-delete nil))
           (do
             (a/update conn t-insert data)
             (a/update conn t-delete nil))
           (do
             (a/update conn m-insert data)
             (a/update conn m-delete nil))))))))


(deftest bench-select
  (testing "Select row"
    (let [data {:name "Joe Coder"
                :salary 100000
                :dept "Accounts"}]
      (a/with-connection [conn u/ds]
        (a/update conn t-insert data))
      ;; bench c.j.j normal with asphalt
      (jdbc/with-db-connection [db-con db-spec]
        (a/with-connection [conn u/ds]
          (c/compare-perf "select-row"
            (jdbc/query db-con [s-select])
            (a/query a/fetch-rows conn s-select nil)
            (a/query a/fetch-rows conn t-select nil)
            (a/query a/fetch-rows conn m-select nil))))
      ;; bench c.j.j `:as-arrays? true` with asphalt
      (jdbc/with-db-connection [db-con db-spec]
        (a/with-connection [conn u/ds]
          (c/compare-perf "select-row-as-arrays"
            (jdbc/query db-con [s-select] :as-arrays? true)
            (a/query a/fetch-rows conn s-select nil)
            (a/query a/fetch-rows conn t-select nil)
            (a/query a/fetch-rows conn m-select nil)))))))
