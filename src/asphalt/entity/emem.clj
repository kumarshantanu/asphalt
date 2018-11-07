;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.entity.emem
  "In-memory entity store implementation. For internal use only - subject to change."
  (:require
    [asphalt.internal    :as i]
    [asphalt.entity.type :as et])
  (:import
    [java.util Collection]
    [clojure.lang IAtom]
    [asphalt.entity.type Entity Field]))


(defn operate
  [f lookup args]
  (->> args
    (map #(get lookup % %))
    (apply f)))


(defn where?
  "Evaluate WHERE-clause expression, e.g. [:and [:= :foo 34] [:<> :bar 22]] or {:foo 34 :bar 22},
  as a boolean true or false."
  [where-expression m]
  (if (map? where-expression)
    ;; assume all pairs are to be matched
    (reduce-kv (fn [_ k v]
                 (if (= (get m k k) (get m v v))
                   true
                   (reduced false)))
      true where-expression)
    ;; evaluate expression node by node
    (if (seq where-expression)
      (let [oper (first where-expression)
            args (next where-expression)]
        (case oper
          :and (every? #(where? % m) args)
          :or  (some #(where? % m) args)
          :=   (operate =    m args)
          :<>  (operate not= m args)
          :>   (operate >    m args)
          :>=  (operate >=   m args)
          :<   (operate <    m args)
          :<=  (operate <=   m args)
          :between (if (= 2 (count args))
                     (operate <= m args)
                     (throw (IllegalArgumentException.
                              (str "Expected BETWEEN operator to have only two arguments, found "
                                (count args) ": " where-expression))))
          :in  (if (= 2 (count args))
                 (operate #(.contains ^Collection %2 %1) m args)  ; [:in :foo [10 20 30]]
                 (throw (IllegalArgumentException.
                          (str "ExpectedIN operator to have only two arguments, found "
                            (count args) ": " where-expression))))
          (throw (UnsupportedOperationException. (str "Unsupported condition operator: "
                                                   oper " in " (pr-str where-expression))))))
      true)))


(defn row-with-generated-fields
  [entity row]
  (if-let [gfields (->> (.-fields ^Entity entity)
                     vals
                     (filter :generator)
                     (remove #(contains? row (:id %)) )
                     seq)]
    (let [generated (transient {})
          final-row (reduce (fn [r ^Field f] (let [gval @(:generator f)]
                                               (assoc! generated (:name f) gval)
                                               (assoc r (.-id f) gval)))
                      row gfields)]
      [final-row (persistent! generated)])
    [row {}]))


(defn conjv
  "Ensure collection is a vector and conj/append item to it."
  [coll item]
  (cond
    (nil? coll)    [item]
    (vector? coll) (conj coll item)
    (sequential?
      coll)        (conj (vec coll) item)
    :otherwise     (i/expected "a vector, or sequential collection" coll)))


(defn long-inc
  ^long [^long n]
  (inc n))


(defn ensure-fields
  "Ensure that row (k/v map) has all required fields. Throw exception if not so."
  [^Entity entity row]
  (reduce-kv (fn [kv-map id ^Field each]
               (if (contains? kv-map id)
                 kv-map
                 (if (.-not-null? each)
                   (let [default (.-default each)]
                     (if (some? default)
                       (assoc kv-map id default)
                       (i/expected (format "field %s to exist" id) row)))
                   kv-map)))
    row (.-fields entity)))


(def default-vals (memoize
                    (fn [^Entity entity]
                      (reduce-kv (fn [m id ^Field each]
                                   (let [default (.-default each)]
                                     (if (some? default)
                                       (assoc m id default)
                                       (if (.-not-null? each)
                                         m
                                         (assoc m id nil)))))
                        {} (.-fields entity)))))


(defn conjv-row
  "Append given row to an all-rows vector after verifying the new row does not violate unique keys."
  [all-rows ^Entity entity new-row]
  (let [kset (.-keyset entity)]
    (doseq [f-id kset]
      (let [f-val (f-id new-row)]
        (when (.contains ^Collection (mapv f-id all-rows) f-val)
          (i/expected (format "field %s to be unique in entity %s" f-id (.-id entity)) f-val))))
    (conjv all-rows new-row)))


(defn any< [x y] (neg? (compare x y)))
(defn any> [x y] (pos? (compare x y)))

(defn as-order-field
  [^Entity entity order]
  (i/expected et/entity? "an entity" entity)
  (if (vector? order)
    (let [fields (.-fields entity)]
      (i/expected #(i/count= 2 %) "vector of two elements [field-id :asc|:desc]" order)
      (i/expected fields          "first element of order to be a field-ID" (first order))
      (i/expected #{:asc :desc}   "second element of order to be :asc or :desc" (second order))
      (update order 1 #(if (= :asc %) any< any>)))
    (as-order-field entity [order :asc])))


(extend-protocol et/IRepo
  clojure.lang.IAtom
  (r-genkey [this entity row]  (let [row (ensure-fields entity row)
                                     [final-row gen] (row-with-generated-fields entity row)]
                                 (swap! this update (.-id ^Entity entity)
                                   conjv final-row)
                                 gen))
  (r-multgk [this entity rows] (let [gvec (transient [])
                                     rows (mapv #(ensure-fields entity %) rows)]
                                 (swap! this update (.-id ^Entity entity)
                                   (comp vec concat)
                                   (mapv #(let [[final-row gen] (row-with-generated-fields
                                                                  entity %)]
                                            (conj! gvec gen)
                                            final-row) rows))
                                 (persistent! gvec)))
  (r-insert [this entity row]  (let [row (ensure-fields entity row)
                                     [final-row _] (row-with-generated-fields entity row)]
                                 (swap! this update (.-id ^Entity entity)
                                   conjv-row entity final-row)
                                 1))
  (r-multin [this entity rows] (let [gvec (transient [])
                                     rows (mapv #(ensure-fields entity %) rows)]
                                 (swap! this update (.-id ^Entity  entity)
                                   (comp vec concat)
                                   (mapv #(first (row-with-generated-fields entity %)) rows))
                                 (count rows)))
  (r-update [this entity
             subst opts]       (let [{:keys [where]} opts
                                     n (volatile! 0)]
                                 (swap! this update (.-id ^Entity entity)
                                   (fn [rows]
                                     (mapv #(if (where? where %)
                                              (do
                                                (vswap! n long-inc)
                                                (->> (vals %)
                                                  (replace subst)
                                                  (zipmap (keys %))))
                                              %) rows)))
                                 @n))
  (r-upsert [this entity row]  (let [id (.-id ^Entity entity)]
                                 (if-let [idval (get row id)]
                                   (if (zero? ^long (et/r-update this entity row [:= id idval]))
                                     (do
                                       (et/r-insert this entity row)
                                       1)
                                     2)
                                   (throw (IllegalArgumentException.
                                            (format "Expected key %s in params" id))))))
  (r-query  [this entity opts] (let [{:keys [fields
                                             where
                                             order
                                             limit]} opts
                                     | (fn [param f & more]
                                         (if param
                                           (apply f more)
                                           (last more)))
                                     default (default-vals entity)
                                     o-pairs (mapv #(as-order-field entity %) order)]
                                 (i/expected et/entity? "an entity" entity)
                                 (->> (get @this (.-id ^Entity entity))
                                   (map #(merge default %))
                                   (| where  filter #(where? where %))
                                   (| order  sort-by (fn [row]
                                                       (->> o-pairs
                                                         (map first)
                                                         (mapv row))) (fn [vs1 vs2]
                                                                        (->> o-pairs
                                                                          (map #((second %3) %1 %2) vs1 vs2)
                                                                          (reduce #(and %1 %2) true))))
                                   (| limit  take limit)
                                   (| fields map #(select-keys % fields))
                                   vec)))
  (r-count  [this entity opts] (let [{:keys [where]} opts
                                     | (fn [param f & more]
                                         (if param
                                           (apply f more)
                                           (last more)))
                                     default (default-vals entity)]
                                 (i/expected et/entity? "an entity" entity)
                                 (->> (get @this (.-id ^Entity entity))
                                   (map #(merge default %))
                                   (| where    filter #(where? where %))
                                   count)))
  (r-delete [this entity opts] (let [{:keys [where]} opts]
                                 (swap! this update (.-id ^Entity entity)
                                   (fn [rows]
                                     (->> rows
                                       (remove #(where? where %))
                                       vec))))))
