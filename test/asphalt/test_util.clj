(ns asphalt.test-util
  (:require
    [clojure.edn     :as e]
    [clojure.java.io :as io]
    [clj-dbcp.core   :as d]
    [asphalt.core    :as a])
  (:import [java.util Date Calendar]))


(defn echo
  [x]
  (println x)
  x)


(def config (->> (io/resource "database.edn")
              slurp
              e/read-string))


(def ds (d/make-datasource config))


(defn create-db
  []
  (a/update ds (:create-ddl config) []))


(defn drop-db
  []
  (a/update ds (:drop-ddl config) []))
