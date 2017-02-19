;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.test-connpool
  "DataSource maker for C3P0 JDBC connection pool."
  (:import
    [com.mchange.v2.c3p0 ComboPooledDataSource]))


(defn make-datasource
  [{:keys [classname
           jdbc-url
           username
           password]
    :as config}]
  (doto (ComboPooledDataSource.)
    (.setDriverClass classname) 
    (.setJdbcUrl jdbc-url)
    (.setUser username)
    (.setPassword password)
    ;; expire excess connections after 30 minutes of inactivity:
    (.setMaxIdleTimeExcessConnections (* 30 60))
    ;; expire connections after 3 hours of inactivity:
    (.setMaxIdleTime (* 3 60 60))))
