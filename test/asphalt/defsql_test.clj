;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.defsql-test
  (:require
    [clojure.test :refer :all]
    [asphalt.core :as a]
    [asphalt.type :as t]))


(a/defsql create-table "CREATE TABLE emp (empid INT NOT NULL PRIMARY KEY, empname VARCHAR(50) NOT NULL)")


(a/defsql insert-emp "INSERT INTO emp (empid, empname) VALUES (?, ?)"
  {:sql-name "add-new-emp"})


(deftest test-default
  (is (= (name create-table) "create-table"))
  (is (= (name insert-emp) "add-new-emp")))


(def sql-find-employees-by-dept "SELECT ^string name, ^int age, ^date joined FROM emp WHERE dept_id=^int $dept-id")
(a/defsql find-employees-by-dept sql-find-employees-by-dept)


(def sql-find-employees-by-dept-badhints "SELECT name, age, joined FROM emp WHERE dept_id=^long $dept-id")


(a/defsql find-employees-by-level
  "SELECT ^string name, ^int age, ^date joined FROM emp WHERE dept_id=^int $dept-id AND level IN (^ints $levels)")


(deftest test-defsql
  (is (= "SELECT name, age, joined FROM emp WHERE dept_id=?"
        (t/get-sql find-employees-by-dept {:dept-id 20})))
  (is (= "SELECT name, age, joined FROM emp WHERE dept_id=? AND level IN (?, ?, ?)"
        (t/get-sql find-employees-by-level {:dept-id 20
                                            :levels [10 20 30]}))))


(deftest test-parse-sql
  (is (= (a/parse-sql sql-find-employees-by-dept)
        (a/parse-sql sql-find-employees-by-dept-badhints {:param-types [:int]
                                                          :result-types [:string :int :date]}))))
