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
    [clojure.string      :as string]
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
          :like (do
                  (i/expected #(i/count= 2 %) "LIKE operator to have two arguments" args)
                  (operate (fn [^String string-token ^String like-pattern]
                             (-> like-pattern
                               (string/replace #"\%" ".*")
                               re-pattern
                               (re-find string-token)
                               boolean))
                    m args))
          (throw (UnsupportedOperationException. (str "Unsupported condition operator: "
                                                   oper " in " (pr-str where-expression))))))
      true)))


(defn row-with-generated-fields
  [entity row]
  (if-let [gfields (->> (.-fields ^Entity entity)
                     vals
                     (filter #(.-counter ^Field %))
                     (remove #(contains? row (:id %)) )
                     seq)]
    (let [generated (transient {})
          final-row (reduce (fn [r ^Field f] (let [gval @(.-counter f)]
                                               (assoc! generated (.-name f) gval)
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


(defn as-order-field
  [^Entity entity order]
  (i/expected et/entity? "an entity" entity)
  (if (vector? order)
    (let [[f-id o-token] order
          fields (.-fields entity)]
      (i/expected #(i/count= 2 %) "vector of two elements [field-id :asc|:desc]" order)
      (i/expected fields          "first element of order to be a field-ID" f-id)
      (i/expected #{:asc :desc}   "second element of order (order token) to be :asc or :desc" o-token)
      [f-id (if (= :asc o-token) #(compare %1 %2) #(compare %2 %1))])
    (as-order-field entity [order :asc])))


;; ===== atom store implementation =====


(defn atom-genkey
  [the-atom ^Entity entity row]
  (let [row (ensure-fields entity row)
        [final-row gen] (row-with-generated-fields entity row)]
    (swap! the-atom update (.-id entity)
      conjv final-row)
    ;; extract 10 from {:id 10}
    (first (nfirst gen))))


(defn atom-multgk
  [the-atom ^Entity entity rows]
  (let [gvec (transient [])
        rows (mapv #(ensure-fields entity %) rows)]
    (swap! the-atom update (.-id entity)
      (comp vec concat)
      (mapv #(let [[final-row gen] (row-with-generated-fields
                                     entity %)]
               (conj! gvec gen)
               final-row) rows))
    (persistent! gvec)))


(defn atom-insert
  [the-atom ^Entity entity row]
  (let [row (ensure-fields entity row)
        [final-row _] (row-with-generated-fields entity row)]
    (swap! the-atom update (.-id entity)
      conjv-row entity final-row)
    1))


(defn atom-multin
  [the-atom ^Entity entity rows]
  (let [gvec (transient [])
        rows (mapv #(ensure-fields entity %) rows)]
    (swap! the-atom update (.-id entity)
      (comp vec concat)
      (mapv #(first (row-with-generated-fields entity %)) rows))
    (count rows)))


(defn atom-update
  [the-atom ^Entity entity subst opts]
  (let [{:keys [where]} opts
        n (volatile! 0)]
    (swap! the-atom update (.-id entity)
      (fn [rows]
        (mapv #(if (where? where %)
                 (do
                   (vswap! n long-inc)
                   (->> (vals %)
                     (replace subst)
                     (zipmap (keys %))))
                 %) rows)))
    @n))


(defn atom-upsert
  [the-atom ^Entity entity row]
  (let [id (.-id entity)]
    (if-let [idval (get row id)]
      (if (zero? ^long (et/r-update the-atom entity row [:= id idval]))
        (do
          (et/r-insert the-atom entity row)
          1)
        2)
      (throw (IllegalArgumentException.
               (format "Expected key %s in params" id))))))


(defn atom-query
  [the-atom ^Entity entity opts]
  (let [{:keys [fields
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
    (->> (get @the-atom (.-id entity))
      (map #(merge default %))
      (| where  filter #(where? where %))
      (| order  sort-by (fn [row]
                          (->> o-pairs
                            (map first)
                            (mapv row))) (fn [vs1 vs2]
                                           (->> o-pairs
                                             (mapv #((second %3) %1 %2) vs1 vs2)
                                             (reduce (fn [a ^long x]
                                                       (if (zero? x)
                                                         0
                                                         (reduced x)))
                                               0))))
      (| limit  take limit)
      (| fields map #(select-keys % fields))
      vec)))


(defn atom-count
  [the-atom ^Entity entity opts]
  (let [{:keys [where]} opts
        | (fn [param f & more]
            (if param
              (apply f more)
              (last more)))
        default (default-vals entity)]
    (i/expected et/entity? "an entity" entity)
    (->> (get @the-atom (.-id entity))
      (map #(merge default %))
      (| where    filter #(where? where %))
      count)))


(defn atom-delete
  [the-atom ^Entity entity opts]
  (let [{:keys [where]} opts]
    (swap! the-atom update (.-id entity)
      (fn [rows]
        (->> rows
          (remove #(where? where %))
          vec)))))


(extend-protocol et/IRepo
  clojure.lang.IAtom
  (r-genkey [this entity row]  (atom-genkey this entity row))
  (r-multgk [this entity rows] (atom-multgk this entity rows))
  (r-insert [this entity row]  (atom-insert this entity row))
  (r-multin [this entity rows] (atom-multin this entity rows))
  (r-update [this entity
             subst opts]       (atom-update this entity subst opts))
  (r-upsert [this entity row]  (atom-upsert this entity row))
  (r-query  [this entity opts] (atom-query  this entity opts))
  (r-count  [this entity opts] (atom-count  this entity opts))
  (r-delete [this entity opts] (atom-delete this entity opts)))
