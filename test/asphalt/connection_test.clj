;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.connection-test
  (:require
    [clojure.test :refer :all]
    [asphalt.core     :as a]
    [asphalt.internal :as i]
    [asphalt.type     :as t])
  (:import
    [java.sql  Connection DriverManager]
    [javax.sql DataSource]))


(def h2-classname "org.h2.Driver")


(def h2-jdbc-url "jdbc:h2:mem:test_db")


(defn make-connection
  ^Connection []
  (DriverManager/getConnection h2-jdbc-url "sa" ""))


(def ds (reify DataSource
          (getConnection [this] (make-connection))
          (getConnection [this username password] (make-connection))))


(deftest test-with-connection
  (Class/forName h2-classname)
  (with-open [^Connection connection (make-connection)]
    (i/with-connection [^Connection conn connection]
      (is (instance? Connection conn)))
    (is (not (.isClosed connection)))))


(deftest test-connection
  (Class/forName h2-classname)
  (with-open [^Connection conn (t/obtain-connection (make-connection))]
    (is (instance? Connection conn))
    (t/return-connection conn conn)
    (is (not (.isClosed conn)))))


(deftest test-datasource
  (Class/forName h2-classname)
  (let [^Connection conn (t/obtain-connection ds)]
    (is (instance? Connection conn))
    (t/return-connection ds conn)
    (is (.isClosed conn))))


(deftest test-map
  (testing "connection"
    (Class/forName h2-classname)
    (with-open [^Connection conn (make-connection)]
      (let [db-spec {:connection conn}
            ^Connection db-conn (t/obtain-connection db-spec)]
        (is (instance? Connection db-conn))
        (t/return-connection db-conn db-conn)
        (is (not (.isClosed db-conn))))))
  (testing "datasource"
    (Class/forName h2-classname)
    (let [db-spec {:datasource ds}
          ^Connection conn (t/obtain-connection db-spec)]
      (is (instance? Connection conn))
      (t/return-connection db-spec conn)
      (is (.isClosed conn))))
  (testing "factory"
    (Class/forName h2-classname)
    (let [db-spec {:factory (fn [_] (make-connection))}
          ^Connection conn (t/obtain-connection db-spec)]
      (is (instance? Connection conn))
      (t/return-connection db-spec conn)
      (is (.isClosed conn))))
  (testing "connection-uri"
    (testing "without classname"
      (Class/forName h2-classname)
      (let [db-spec {:connection-uri h2-jdbc-url}
            ^Connection conn (t/obtain-connection db-spec)]
        (is (instance? Connection conn))
        (t/return-connection db-spec conn)
        (is (.isClosed conn))))
    (testing "with classname"
      (let [db-spec {:classname h2-classname
                     :connection-uri h2-jdbc-url}
            ^Connection conn (t/obtain-connection db-spec)]
        (is (instance? Connection conn))
        (t/return-connection db-spec conn)
        (is (.isClosed conn)))))
  (testing "subprotocol"
    (testing "without classname"
      (Class/forName h2-classname)
      (let [db-spec {:subprotocol "h2"
                     :subname "mem:test_db"}
            ^Connection conn (t/obtain-connection db-spec)]
        (is (instance? Connection conn))
        (t/return-connection db-spec conn)
        (is (.isClosed conn))))
    (testing "with classname"
      (let [db-spec {:classname h2-classname
                     :subprotocol "h2"
                     :subname "mem:test_db"}
            ^Connection conn (t/obtain-connection db-spec)]
        (is (instance? Connection conn))
        (t/return-connection db-spec conn)
        (is (.isClosed conn)))))
  (testing "jndi-name"
    (let [db-spec {:name "java:comp/env/myDataSource"}
          ^Connection conn (t/obtain-connection db-spec)]
      (is (instance? Connection conn))
      (t/return-connection db-spec conn)
      (is (.isClosed conn)))))
