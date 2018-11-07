;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.entity-test
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [asphalt.entity :as e]
    [asphalt.entity.type :as t]
    [asphalt.test-util :as u])
  (:import
    [java.sql SQLException]))


(def mem-repo (atom {}))


(defn test-fixture
  [f]
  (reset! mem-repo {})
  (u/create-db)
  (f)
  (u/drop-db)
  (reset! mem-repo {}))


(use-fixtures :each test-fixture)


(e/defentity employee {:id   :emp
                       :name "emp"
                       :keyset #{:id}}
  [{:id :id      :type :integer :auto-inc? true}
   {:id :name    :type :string  :not-null? true}
   {:id :salary  :type :int     :not-null? true}
   {:id :dept    :type :string}
   {:id :doj     :name "j_date" :type :date}
   {:id :bio     :type :clob}
   {:id :pic     :type :blob}])


(deftest test-entity-construction
  (is (t/entity? employee) "entity created"))


(deftest test-entity-create
  (let [emp {:id 10
             :name "Munna Marwah"
             :salary 1000}]
    (testing "happy"
      (is (= 1
            (e/create-entity mem-repo employee emp)
            (e/create-entity u/orig-ds employee emp)))
      (is (= 1
            (e/count-entities mem-repo employee)
            (e/count-entities u/orig-ds employee)) "row count should match"))
    (testing "negative - missing required fields"
      (is (thrown? IllegalArgumentException
            (e/create-entity mem-repo employee {:id 20})) "required fields missing")
      (is (thrown? IllegalArgumentException
            (e/create-entity u/orig-ds employee {:id 20})) "required fields missing")
      (is (= 1
            (e/count-entities mem-repo employee)
            (e/count-entities u/orig-ds employee)) "row count should match"))
    (testing "negative - duplicate primary key"
      (is (thrown? IllegalArgumentException
            (e/create-entity mem-repo employee emp)) "duplicate primary key")
      (is (thrown? SQLException
            (e/create-entity u/orig-ds employee emp)) "duplicate primary key")
      (is (= 1
            (e/count-entities mem-repo employee)
            (e/count-entities u/orig-ds employee)) "row count should match"))))


(deftest test-entity-genkey)


(deftest test-entity-update)


(deftest test-entity-upsert)


(deftest test-entity-delete)


(deftest test-entity-query
  (testing "happy"
    (let [emp1 {:id 10
                :name "Munna Marwah"
                :salary 1000}
          emp2 {:id 15
               :name "Naresh Nishchal"
               :salary 2000}
          emp3 {:id 20
               :name "Pappu Puniya"
               :salary 3000}
          xtra {:doj nil
                :dept nil
                :bio nil
                :pic nil}]
      (doseq [each [emp1 emp2 emp3]]
        (is (= 1
              (e/create-entity mem-repo employee each)
              (e/create-entity u/orig-ds employee each))))
      (is (= [(merge emp1 xtra)
              (merge emp2 xtra)
              (merge emp3 xtra)]
            (e/query-entities mem-repo employee)
            (e/query-entities u/orig-ds employee))
        "fetched row should have all fields, even optional")
      (is (= [(merge emp1 xtra)
              (merge emp3 xtra)]
            (e/query-entities mem-repo employee  {:where [:or [:= :id 10] [:= :id 20]]})
            (e/query-entities u/orig-ds employee {:where [:or [:= :id 10] [:= :id 20]]}))
        "WHERE clause")
      (is (= [emp1 emp2 emp3]
            (e/query-entities mem-repo employee  {:fields [:id :name :salary]})
            (e/query-entities u/orig-ds employee {:fields [:id :name :salary]}))
        "select fields")
      (is (= [(merge emp3 xtra)
              (merge emp2 xtra)
              (merge emp1 xtra)]
            (e/query-entities mem-repo employee  {:order [[:id :desc]]})
            (e/query-entities u/orig-ds employee {:order [[:id :desc]]}))
        "ORDER BY - one field")
;      (is false "ORDER BY - two fields, ASC and DESC")
;      (is false "LIMIT")
;      (is false "OFFSET")
      )))
