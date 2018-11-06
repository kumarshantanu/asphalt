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
    [asphalt.test-util :as u]))


(defn test-fixture
  [f]
  (u/create-db)
  (f)
  (u/drop-db))


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


(def mem-repo (atom {}))


(deftest test-entity-construction
  (is (t/entity? employee) "entity created"))


(deftest test-entity-create
  (testing "happy"
    (let [emp {:id 10
               :name "Munna Marwah"
               :salary 1000}]
      (is (= 1
            (e/create-entity mem-repo employee emp)
            (e/create-entity u/orig-ds employee {:id 10
                                                 :name "Munna Marwah"
                                                 :salary 1000})))
      (is (= [{:id 10
               :name "Munna Marwah"
               :salary 1000
               :doj nil
               :dept nil
               :bio nil
               :pic nil}]
            (e/query-entities mem-repo employee)
            (e/query-entities u/orig-ds employee)) "fetched row should have all fields, even optional")))
  (testing "negative"
    (is (thrown? IllegalArgumentException
          (e/create-entity mem-repo employee {:id 20})) "required fields missing")
    ))


(deftest test-entity-genkey)


(deftest test-entity-update)


(deftest test-entity-upsert)


(deftest test-entity-delete)


(deftest test-entity-query)
