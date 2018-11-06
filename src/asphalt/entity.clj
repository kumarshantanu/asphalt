;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.entity
  "SQL is a low level language for persisting data. This namespace allows working at a higher level with
  the entity model at the cost of SQL's power and flexibility. Entities may be accessed using this API.
  Unsupported SQL operations:
  - Aggregate operations: Group by
  - Joins
  - Calculated columns
  - Sub selects"
  (:require
    [asphalt.entity.impl :as m]
    [asphalt.entity.emem :as em]
    [asphalt.entity.esql :as es]
    [asphalt.entity.type :as t]
    [asphalt.internal :as i])
  (:import
    [clojure.lang Atom]
    [asphalt.entity.type Entity Field]))


;; --- entity definition ---


(defn make-entity-definition
  [options fields]
  (i/expected map? "option map" options)
  (i/expected :id "options to have the :id key" options)
  (i/expected seq "non-empty collection of fields" fields)
  (doseq [each fields]
    (i/expected map? "every field to be a Field instance" each))
  (let [id (:id options)  ; options must have the :id key
        {:keys [name keyset]
         :or {name (clojure.core/name id)
              keyset #{}}} options]
    (i/expected string? "entity name to be a string" name)
    (i/expected set? "unique keys to be a set" keyset)
    (when (empty? keyset)
      (binding [*out* *err*]  ; print out a one-time warning to STDERR
        (-> "
**********
** WARNING: Entity %s is defined without any unique fields (:keyset)
**********"
          (format id)
          println)))
    (let [field-ins (mapv t/map->Field fields)  ; field instances
          field-ids (mapv #(.-id ^Field %) field-ins)
          fid-set   (set field-ids)]
      (i/expected #(= (distinct %) %) "field IDs to be distinct from each other" field-ids)
      (i/expected #(every? fid-set %) "All unique keys must be one of the field IDs" keyset)
      (t/map->Entity {:id id
                      :name name
                      :keyset keyset
                      :fields (zipmap (map :id field-ins) field-ins)}))))


(defmacro defentity
  ([name-sym fields]
    (defentity name-sym {} fields))
  ([name-sym options fields]
    (i/expected symbol? "a var name symbol" name-sym)
    (i/expected map? "option map" options)
    (i/expected vector? "fields vector" fields)
    `(def ~name-sym (make-entity-definition (merge {:id ~(keyword name-sym)
                                                    :name ~(name name-sym)} ~options)
                      ~(mapv m/infer-field-definition fields)))))


;; --- entity operations ---


(defn genkey-entity
  [repo ^Entity entity kv-map]
  (i/expected t/entity? "an entity" entity)
  (i/expected map? "row data (to be inserted) as a map" kv-map)
  (t/r-genkey repo entity kv-map))


(defn genkey-entities
  [repo ^Entity entity vec-of-maps]
  (i/expected t/entity? "an entity" entity)
  (i/expected sequential? "a collection of rows" vec-of-maps)
  (doseq [each vec-of-maps]
    (i/expected map? "every row (for insertion) to be a map" each))
  (t/r-genkey repo entity vec-of-maps))


(defn create-entity
  [repo ^Entity entity kv-map]
  (i/expected t/entity? "an entity" entity)
  (i/expected map? "row data (to be inserted) as a map" kv-map)
  (t/r-insert repo entity kv-map))


(defn create-entities
  [repo ^Entity entity vec-of-maps]
  (i/expected t/entity? "an entity" entity)
  (i/expected sequential? "a collection of rows" vec-of-maps)
  (doseq [each vec-of-maps]
    (i/expected map? "every row (for insertion) to be a map" each))
  (t/r-multin repo entity vec-of-maps))


(defn update-entities
  ([repo ^Entity entity replacements]
    (update-entities repo entity replacements {}))
  ([repo ^Entity entity replacements {:keys [where]
                                      :as options}]
    (i/expected t/entity? "an entity" entity)
    (i/expected map? "field replacements to be a map" replacements)
    (t/r-update repo entity replacements where)))


(defn upsert-entity
  [repo ^Entity entity kv-map]
  (i/expected t/entity? "an entity" entity)
  (i/expected map? "row data (to be upserted) as a map" kv-map)
  (i/expected seq "entity to have one or more key fields" (.-keyset entity))
  (let [keyset (.-keyset entity)]
    (i/expected #(some keyset (keys %)) "row to have a key field" kv-map))
  (t/r-upsert repo entity kv-map))


(defn delete-entities
  ([repo ^Entity entity]
    (delete-entities repo entity {}))
  ([repo entity {:keys [where]
                 :as options}]
    (i/expected t/entity? "an entity" entity)
    (t/r-delete repo entity where)))


(defn query-entities
  ([repo ^Entity entity]
    (query-entities repo entity {}))
  ([repo ^Entity entity {:keys [fields
                                where
                                order-by
                                limit]
                         :as options}]
    (i/expected t/entity? "an entity" entity)
    (t/r-query repo entity options)))
