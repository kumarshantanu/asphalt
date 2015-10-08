(ns asphalt.test-util
  (:require
    [clojure.edn     :as e]
    [clojure.java.io :as io]
    [clj-dbcp.core   :as d]
    [asphalt.core    :as a])
  (:import
    [java.util Date Calendar]
    [asphalt.type StmtCreationEvent SQLExecutionEvent]))


(defn echo
  [x]
  (println x)
  x)


(def config (->> (io/resource "database.edn")
              slurp
              e/read-string))


(def ds (a/instrument-datasource
          (d/make-datasource config)
          #_{}  ; uncomment this (and comment out the following) for debugging
          {:stmt-creation {:before     (fn [^StmtCreationEvent event]
                                         (println "Before:-" event))
                           :on-success (fn [^String id ^long nanos ^asphalt.type.StmtCreationEvent event]
                                         (println "Success:- ID:" id "- nanos:" nanos "-" event))
                           :on-error   (fn [^String id ^long nanos ^asphalt.type.StmtCreationEvent event ^Exception error]
                                         (println "Error:- ID:" id "- nanos:" nanos "-" event "- error:" error))
                           :lastly     (fn [^String id ^long nanos ^asphalt.type.StmtCreationEvent event]
                                         (println "Lastly:- ID:" id "- nanos:" nanos "-" event))}
           :sql-execution {:before     (fn [^SQLExecutionEvent event]
                                         (println "Before:-" event))
                           :on-success (fn [^String id ^long nanos ^asphalt.type.SQLExecutionEvent event]
                                         (println "Success:- ID:" id "- nanos:" nanos "-" event))
                           :on-error   (fn [^String id ^long nanos ^asphalt.type.SQLExecutionEvent event ^Exception error]
                                         (println "Error:- ID:" id "- nanos:" nanos "-" event "- error:" error))
                           :lastly     (fn [^String id ^long nanos ^asphalt.type.SQLExecutionEvent event]
                                         (println "Lastly:- ID:" id "- nanos:" nanos "-" event))}}))


(defn create-db
  []
  (a/update ds (:create-ddl config) []))


(defn drop-db
  []
  (a/update ds (:drop-ddl config) []))
