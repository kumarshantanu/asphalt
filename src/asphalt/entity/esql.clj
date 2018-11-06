;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.entity.esql
  "SQL entity store implementation. For internal use only - subject to change."
  (:require
    [asphalt.core        :as a]
    [asphalt.entity.type :as et]
    [asphalt.internal    :as i]
    [asphalt.type        :as at])
  (:import
    [java.sql Connection ResultSet ResultSetMetaData]
    [asphalt.entity.type Entity Field]
    [asphalt.type TxnConnectionSource]))


(defn comma-separated
  [xs]
  (->> xs
    (interpose ", ")
    (apply str)))


(def insert-template (memoize
                       (fn [^Entity entity row-keyset]
                         (let [fields (remove (fn [^Field field]
                                                (let [id (.-id field)]
                                                  (if (contains? row-keyset id)
                                                    false
                                                    (or
                                                      (.-counter field)   ; auto-incremented
                                                      (.-default field)   ; having defaults
                                                      (not (.-not-null? field)) ; nullable fields
                                                      (i/expected (str "field " id " in the data row")
                                                        row-keyset)))))
                                        (vals (.-fields entity)))
                               tokens (concat
                                        [(format "INSERT INTO %s (%s) VALUES ("
                                           (.-name entity)
                                           (->> fields
                                             (mapv #(.-name ^Field %))
                                             comma-separated))]
                                        (->> fields
                                          (map #(do [(.-id ^Field %)
                                                     (.-type ^Field %)]))
                                          (interpose ", "))
                                        [")"])]
                           (a/compile-sql-template (vec tokens) [] {})))))


(defn bi-tokens
  "Return [template params-map]."
  [^Entity entity oper args]
  (i/expected #(= 2 (count %)) (format "two arguments for %s operator" oper) args)
  (let [[op1 op2] args
        fields (.-fields entity)
        fcheck  (fn [op & more]
                  (mapv (fn [k]
                          (i/expected fields (str "keyword arguments to be one of the fields " (keys fields)) k)
                          (get fields k))
                    (cons op more)))]
    (cond
      (every? keyword?
        [op1 op2])     (let [[^Field f1 ^Field f2] (fcheck op1 op2)] [[(.-name f1) " " oper " " (.-name f2)] {}])
      (keyword? op1)   (let [[^Field f1] (fcheck op1)
                             k (keyword (gensym))] [[(.-name f1) " " oper " " [k (.-type f1)]] {k op2}])
      (keyword? op2)   (let [[^Field f2] (fcheck op2)
                             k (keyword (gensym))] [[(.-name f2) " " oper " " [k (.-type f2)]] {k op1}])
      :otherwise       (i/expected "at least one of the operands to be a keyword (field ID)" [op1 oper op2]))))


(defn where-tokens
  [^Entity entity where-expr]
  (i/expected vector? "expression vector" where-expr)
  (i/expected seq     "non-empty expression vector" where-expr)
  (let [oper   (first where-expr)
        args   (next  where-expr)
        fields (.-fields entity)
        fcheck (fn [op & more]
                 (mapv (fn [k]
                         (i/expected keyword? "field ID to be a keyword" k)
                         (i/expected fields (str "keyword arguments to be one of the fields "
                                              (keys fields)) k)
                         (get fields k))
                   (cons op more)))
        xcatv  (fn [xs & more] (->> (cons xs more)
                                 (reduce (fn [a x] (if (sequential? x)
                                                     (vec (concat a x))
                                                     (conj a x)))
                                   [])))
        tjoin  (fn [delim args]
                 (as-> args $
                   (map #(where-tokens entity %) $)  ; => [[["a = " [:a :int]] {:a 10}] [["b = " [:b :int]] {:b 20}]]
                   (vector (map first $) (map second $))
                   (vector (interpose delim (first $)) (reduce conj {} (second $)))
                   (vector (xcatv "(" (first $) ")") (second $))))]
    (case oper
      :and (tjoin ") AND (" args)
      :or  (tjoin ") OR (" args)
      :=   (bi-tokens entity "="  args)
      :<>  (bi-tokens entity "<>" args)
      :>   (bi-tokens entity ">"  args)
      :>=  (bi-tokens entity ">=" args)
      :<   (bi-tokens entity "<"  args)
      :<=  (bi-tokens entity "<=" args)
      :in  (if (= 2 (count args))
             (let [[op1 op2] args
                   ^Field f1 (fcheck op1)
                   param-key (keyword gensym)
                   param-type (get at/multi-revmap (.-type f1))]
               (i/expected keyword? "discovered param type to be a keyword" param-type)
               (i/expected sequential? "second operand to be a collection for IN operator" op2)
               [[(.-name f1) "IN" [param-key param-type]] {param-key op2}])
             (i/expected "two arguments for IN operator" args))
      :between (if (= 3 (count args))
                 (let [^Field kf (fcheck (first args))
                       [_ p1 p2] args]
                   (cond
                     (every? keyword?
                       [p1 p2])       (let [[^Field f1 ^Field f2] (fcheck p1 p2)]
                                        [[(.-name kf) " BETWEEN " (.-name f1) " AND " (.-name f2)] {}])
                     (keyword? p1)    (let [^Field f1 (fcheck p1)
                                            k2 (keyword (gensym))]
                                        [[(.-name kf) " BETWEEN " (.-name f1) " AND " [k2 (.-type kf)]] {k2 p2}])
                     (keyword? p2)    (let [^Field f2 (fcheck p2)
                                            k1 (keyword (gensym))]
                                        [[(.-name kf) " BETWEEN " [k1 (.-type kf)] " AND " (.-name f2)] {k1 p2}])
                     :otherwise       (let [k1 (keyword (gensym))
                                            k2 (keyword (gensym))]
                                        [[(.-name kf) " BETWEEN " [k1 (.-type kf)] " AND " [k2 (.-type kf)]]
                                         {k1 p2 k2 p2}])))
                 (i/expected "three arguments for BETWEEN operator" args))
      (throw (UnsupportedOperationException. (str "Unsupported condition operator: "
                                               oper " in " (pr-str where-expr)))))))


#_(def where-template (memoize
                        (fn [^Entity entity expr param-keys]
                          (as-where-template [] entity expr param-keys))))


(def field-idset (memoize (fn [^Entity entity]
                            (->> (.-fields entity)
                              keys
                              set))))


(def update-template (memoize
                       (fn [^Entity entity subst-field-ids]
                         (let [all-field-ids (field-idset entity)]
                           (i/expected #(every? all-field-ids %)
                             "substitution fields to be one of field IDs" subst-field-ids)
                           ["UPDATE %s SET %s, %s "]
                           :FIXME))))


(defn query-template
  [^Entity entity {:keys [fields
                          where
                          order-by
                          offset
                          limit]
                   :as opts}]
  (let [all-fields (.-fields entity)
        all-keyset (.-keyset entity)]
    (i/expected et/entity? "an entity" entity)
    (when (some? fields)
      (i/expected sequential? "unspecified/nil or collection of field IDs" fields)
      (doseq [each fields]
        (i/expected all-fields
          (str "query field to be one of the field IDs " (keys all-fields)) each)))
    (when (some? order-by)
      (doseq [each order-by]
        (i/expected all-fields "order-by field to be among entity fields" each)))
    (let [{:keys [names
                  f-ids
                  types]} (->> (if fields
                                 (map all-fields fields)
                                 (vals all-fields))
                            (reduce (fn [{:keys [names f-ids types]} ^Field col]
                                      {:names (conj names (.-name col))
                                       :f-ids (conj f-ids (.-id   col))
                                       :types (conj types (.-type col))})
                              {:names []
                               :f-ids []
                               :types []}))
          [wh-tokens
           wh-params] (when where (where-tokens where))]
      [(a/sqlcat
         ["SELECT "
          (comma-separated names)
          " FROM "
          (.-name entity)
          (when where    [" WHERE " wh-tokens])
          (when order-by [" ORDER BY " (->> order-by
                                         (mapv all-fields)
                                         (mapv #(.-name ^Field %))
                                         comma-separated)])
          (when limit    [" LIMIT " (str (int limit))])])
       (conj {} wh-params)
       f-ids
       types])))


(defn sql-genkey [conn-source entity row]        "Insert entity, returning generated keys")
(defn sql-multgk [conn-source entity rows]       "Insert multiple entities, returning generated keys")
(defn sql-insert [conn-source entity row]        (let [template (insert-template entity (set (keys row)))]
                                                   (a/update conn-source template row)))
(defn sql-multin [conn-source entity rows]       "Insert multiple entities")
(defn sql-update [conn-source entity subst opts] "Update entities")
(defn sql-upsert [conn-source entity row]        "Insert/update entities - not supported by all repos")
(defn sql-query  [conn-source entity opts]       (let [[tokens params f-ids return] (query-template entity opts)]
                                                   (a/query (partial a/fetch-rows
                                                              {:row-maker (fn [& args]
                                                                            (->> (apply at/read-row args)
                                                                              (zipmap f-ids)))})
                                                     conn-source [tokens return] params)))
(defn sql-delete [conn-source entity opts]       "Delete entities")


(extend-protocol et/IRepo
  java.sql.Connection
  (r-genkey [this entity row]        "Insert entity, returning generated keys")
  (r-multgk [this entity rows]       "Insert multiple entities, returning generated keys")
  (r-insert [this entity row]        (sql-insert this entity row))
  (r-multin [this entity rows]       "Insert multiple entities")
  (r-update [this entity subst opts] "Update entities")
  (r-upsert [this entity row]        "Insert/update entities - not supported by all repos")
  (r-query  [this entity opts]       (sql-query this entity opts))
  (r-delete [this entity opts]       "Delete entities")
  javax.sql.DataSource
  (r-genkey [this entity row]        "Insert entity, returning generated keys")
  (r-multgk [this entity rows]       "Insert multiple entities, returning generated keys")
  (r-insert [this entity row]        (sql-insert this entity row))
  (r-multin [this entity rows]       "Insert multiple entities")
  (r-update [this entity subst opts] "Update entities")
  (r-upsert [this entity row]        "Insert/update entities - not supported by all repos")
  (r-query  [this entity opts]       (sql-query this entity opts))
  (r-delete [this entity opts]       "Delete entities")
  java.util.Map
  (r-genkey [this entity row]        "Insert entity, returning generated keys")
  (r-multgk [this entity rows]       "Insert multiple entities, returning generated keys")
  (r-insert [this entity row]        (sql-insert this entity row))
  (r-multin [this entity rows]       "Insert multiple entities")
  (r-update [this entity subst opts] "Update entities")
  (r-upsert [this entity row]        "Insert/update entities - not supported by all repos")
  (r-query  [this entity opts]       (sql-query this entity opts))
  (r-delete [this entity opts]       "Delete entities")
  TxnConnectionSource
  (r-genkey [this entity row]        "Insert entity, returning generated keys")
  (r-multgk [this entity rows]       "Insert multiple entities, returning generated keys")
  (r-insert [this entity row]        (sql-insert this entity row))
  (r-multin [this entity rows]       "Insert multiple entities")
  (r-update [this entity subst opts] "Update entities")
  (r-upsert [this entity row]        "Insert/update entities - not supported by all repos")
  (r-query  [this entity opts]       (sql-query this entity opts))
  (r-delete [this entity opts]       "Delete entities"))
