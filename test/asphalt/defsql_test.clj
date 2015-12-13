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
    [asphalt.core :as a]))


(a/defsql create-table "CREATE TABLE emp (empid INT NOT NULL PRIMARY KEY, empname VARCHAR(50) NOT NULL)")


(a/defsql insert-emp "INSERT INTO emp (empid, empname) VALUES (?, ?)"
  {:sql-name "add-new-emp"})


(deftest test-default
  (is (= (name create-table) "create-table"))
  (is (= (name insert-emp) "add-new-emp")))
