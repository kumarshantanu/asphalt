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


(defn echoln
  [& args]
  (when (when-let [verbose (System/getenv "ASPHALT_VERBOSE")]
          (Boolean/parseBoolean verbose))
    (apply println "[Echo] " args)))


(defn sleep
  ([^long millis]
    (when (when-let [verbose (System/getenv "ASPHALT_DELAY")]
            (Boolean/parseBoolean verbose))
      (print "Sleeping" millis "ms...")
      (try (Thread/sleep millis)
        (catch InterruptedException e
          (.interrupt (Thread/currentThread))))
      (println "woke up.")))
  ([]
    (sleep 1000)))


(def config (->> (io/resource "database.edn")
              slurp
              e/read-string))


(def orig-ds (d/make-datasource config))


(def ds
  (a/instrument-connection-source
    orig-ds
    {:conn-creation {:before     (fn [event]
                                   (echoln "Before:-" event))
                     :on-success (fn [^String id ^long nanos event]
                                   (echoln "Success:- ID:" id "- nanos:" nanos "-" event))
                     :on-error   (fn [^String id ^long nanos event ^Exception error]
                                   (echoln "Error:- ID:" id "- nanos:" nanos "-" event "- error:" error))
                     :lastly     (fn [^String id ^long nanos event]
                                   (echoln "Lastly:- ID:" id "- nanos:" nanos "-" event))}
     :stmt-creation {:before     (fn [^StmtCreationEvent event]
                                   (echoln "Before:-" event))
                     :on-success (fn [^String id ^long nanos ^asphalt.type.StmtCreationEvent event]
                                   (echoln "Success:- ID:" id "- nanos:" nanos "-" event))
                     :on-error   (fn [^String id ^long nanos ^asphalt.type.StmtCreationEvent event ^Exception error]
                                   (echoln "Error:- ID:" id "- nanos:" nanos "-" event "- error:" error))
                     :lastly     (fn [^String id ^long nanos ^asphalt.type.StmtCreationEvent event]
                                   (echoln "Lastly:- ID:" id "- nanos:" nanos "-" event))}
     :sql-execution {:before     (fn [^SQLExecutionEvent event]
                                   (echoln "Before:-" event))
                     :on-success (fn [^String id ^long nanos ^asphalt.type.SQLExecutionEvent event]
                                   (echoln "Success:- ID:" id "- nanos:" nanos "-" event))
                     :on-error   (fn [^String id ^long nanos ^asphalt.type.SQLExecutionEvent event ^Exception error]
                                   (echoln "Error:- ID:" id "- nanos:" nanos "-" event "- error:" error))
                     :lastly     (fn [^String id ^long nanos ^asphalt.type.SQLExecutionEvent event]
                                   (echoln "Lastly:- ID:" id "- nanos:" nanos "-" event))}}))


(def delay-ds
  (a/instrument-connection-source
    orig-ds
    {:conn-creation {:before     (fn [event]
                                   (echoln "Before:-" event)
                                   (sleep))
                     :on-success (fn [^String id ^long nanos event]
                                   (echoln "Success:- ID:" id "- nanos:" nanos "-" event)
                                   (sleep))
                     :on-error   (fn [^String id ^long nanos event ^Exception error]
                                   (echoln "Error:- ID:" id "- nanos:" nanos "-" event "- error:" error)
                                   (sleep))
                     :lastly     (fn [^String id ^long nanos event]
                                   (echoln "Lastly:- ID:" id "- nanos:" nanos "-" event)
                                   (sleep))}
     :stmt-creation {:before     (fn [^StmtCreationEvent event]
                                   (echoln "Before:-" event)
                                   (sleep))
                     :on-success (fn [^String id ^long nanos ^asphalt.type.StmtCreationEvent event]
                                   (echoln "Success:- ID:" id "- nanos:" nanos "-" event)
                                   (sleep))
                     :on-error   (fn [^String id ^long nanos ^asphalt.type.StmtCreationEvent event ^Exception error]
                                   (echoln "Error:- ID:" id "- nanos:" nanos "-" event "- error:" error)
                                   (sleep))
                     :lastly     (fn [^String id ^long nanos ^asphalt.type.StmtCreationEvent event]
                                   (echoln "Lastly:- ID:" id "- nanos:" nanos "-" event)
                                   (sleep))}
     :sql-execution {:before     (fn [^SQLExecutionEvent event]
                                   (echoln "Before:-" event)
                                   (sleep))
                     :on-success (fn [^String id ^long nanos ^asphalt.type.SQLExecutionEvent event]
                                   (echoln "Success:- ID:" id "- nanos:" nanos "-" event)
                                   (sleep))
                     :on-error   (fn [^String id ^long nanos ^asphalt.type.SQLExecutionEvent event ^Exception error]
                                   (echoln "Error:- ID:" id "- nanos:" nanos "-" event "- error:" error)
                                   (sleep))
                     :lastly     (fn [^String id ^long nanos ^asphalt.type.SQLExecutionEvent event]
                                   (echoln "Lastly:- ID:" id "- nanos:" nanos "-" event)
                                   (sleep))}}))


(defn create-db
  []
  (a/update ds (:create-ddl config) []))


(defn drop-db
  []
  (a/update ds (:drop-ddl config) []))
