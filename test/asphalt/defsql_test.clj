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


(a/defsql find-selected-employees "SELECT name, age, joined FROM emp WHERE id IN (^objects $ids)"
  {:param-placeholder {:ids "UNHEX(?)"}})


(a/defsql insert-into-specified-table
  "INSERT INTO ^sql $table (foo, bar) VALUES ($foo, $bar)")


(deftest test-defsql
  (is (= "SELECT name, age, joined FROM emp WHERE dept_id=?"
        (t/get-sql find-employees-by-dept {:dept-id 20})))
  (is (= "SELECT name, age, joined FROM emp WHERE dept_id=? AND level IN (?, ?, ?)"
        (t/get-sql find-employees-by-level {:dept-id 20
                                            :levels [10 20 30]})))
  (is (= "SELECT name, age, joined FROM emp WHERE id IN (UNHEX(?), UNHEX(?), UNHEX(?))"
        (t/get-sql find-selected-employees {:ids ["abcd" "bcde" "cdef"]})))
  (is (= "INSERT INTO generic (foo, bar) VALUES (?, ?)"
        (t/get-sql insert-into-specified-table {:table :generic
                                                :foo "foo"
                                                :bar "bar"}))))


(deftest test-parse-sql
  (is (= (a/parse-sql sql-find-employees-by-dept)
        (a/parse-sql sql-find-employees-by-dept-badhints {:param-types [:int]
                                                          :result-types [:string :int :date]}))))


(deftest test-compile-sql-template
  (as-> "SELECT * FROM emp WHERE id = $id" <>
    (a/parse-sql <>)
    (conj <> {})
    (apply a/compile-sql-template <>))
  (is (thrown? IllegalArgumentException
       (as-> "SELECT * FROM emp WHERE id = $id" <>
         (a/parse-sql <>)
         (conj <> {:param-placeholder {:id "?"}})
         (apply a/compile-sql-template <>))))
  (is (thrown? IllegalArgumentException
       (as-> "SELECT * FROM emp WHERE id IN (^objects $ids) and dept_id = $dept-id" <>
         (a/parse-sql <>)
         (conj <> {:param-placeholder {:id "?"}})
         (apply a/compile-sql-template <>))))
  (is (thrown? IllegalArgumentException
       (as-> "SELECT * FROM emp WHERE id IN (^objects $ids) and dept_id = $dept-id" <>
         (a/parse-sql <>)
         (conj <> {:param-placeholder {:dept-id "?"}})
         (apply a/compile-sql-template <>)))))
