;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.test-connpool
  "DataSource maker for BoneCP JDBC connection pool."
  (:import
    [com.jolbox.bonecp BoneCPDataSource]))


(defn make-datasource
  [{:keys [classname
           jdbc-url
           username
           password]
    :as config}]
  (doto (BoneCPDataSource.)
    (.setDriverClass classname)
    (.setJdbcUrl jdbc-url)
    (.setUsername username)
    (.setPassword password)
    ;; some default params
    (.setMinConnectionsPerPartition 2)
    (.setMaxConnectionsPerPartition 8)
    (.setPartitionCount 2)
    (.setStatisticsEnabled true)
    (.setIdleMaxAgeInMinutes (* 3 60))))
