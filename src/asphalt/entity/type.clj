;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.entity.type
  (:import
    [clojure.lang IDeref IPersistentMap IPersistentSet IPersistentVector Keyword]))


(defprotocol IRepo
  (r-genkey [this entity row]        "Insert entity, returning generated keys")
  (r-multgk [this entity rows]       "Insert multiple entities, returning generated keys")
  (r-insert [this entity row]        "Insert entity")
  (r-multin [this entity rows]       "Insert multiple entities")
  (r-update [this entity subst opts] "Update entities")
  (r-upsert [this entity row]        "Insert/update entities - not supported by all repos")
  (r-query  [this entity opts]       "Query entities")
  (r-count  [this entity opts]       "Count entities")
  (r-delete [this entity opts]       "Delete entities"))


(defrecord Entity
  [^Keyword        id      ; identifier, unique among entities
   ^String         name    ; maps to table name
   ^IPersistentSet keyset  ; set of field IDs with unique values (e.g. primary key, unique key etc.)
   ^IPersistentMap fields  ; vector of field descriptors
   ])


(defn entity?
  [x]
  (instance? Entity x))


(defrecord Field
  [^Keyword           id        ; field identifier, unique among fields
   ^String            name      ; field name, maps to column name
   ^Keyword           type      ; field type, must be one of asphalt.type/single-typemap
   ^IPersistentVector param     ; SQL tokens, typically [[id type]]
   ^IDeref            counter   ; auto-incrementing integer generator
   ^boolean           not-null? ; whether field is nullable (w.r.t. `NOT NULL` constraint)
   ^Object            default   ; default value to insert when none supplied
   ])


(defn field?
  [x]
  (instance? Field x))
