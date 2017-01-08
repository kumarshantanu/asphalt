;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.result
  (:require
    [clojure.string          :as str]
    [asphalt.internal        :as i]
    [asphalt.internal.result :as iresult]
    [asphalt.type            :as t])
  (:import
    [java.sql Blob Clob ResultSet ResultSetMetaData]))


;; ----- read java.sql.ResultSet columns for known SQL types -----


(defmacro letcol
  "Destructure column values in the current row in a java.sql.ResultSet instance. Optional SQL type hints increase
  accuracy and speed. The date, time and timestamp types accept an additional timezone-string/calendar argument.

  Sequential destructuring with implicit column index lookup:
  (letcol [[^int foo ^string bar [^date baz cal]] result-set]  ; cal is TimeZone keyword/string or java.util.Calendar
    [foo bar baz])

  Associative destructuring with implicit column label lookup:
  (letcol [{:labels  [^int foo ^string bar [^date baz cal]]    ; cal is TimeZone keyword/string or java.util.Calendar
            :_labels [^date end-date ^timestamp audit-ts]]} result-set]
    ;; :_labels turns dash to underscore when looking up by column label: end_date, audit_ts
    [foo bar baz end-date audit-ts])

  Associative destructuring with explicit column reference:
  (letcol [{^int       foo 1  ; column index begins with 1
            ^string    bar 2
            ^boolean   baz 3
            [^date     end-date cal] \"end_date\"              ; cal is TimeZone keyword/string or java.util.Calendar
            ^timestamp audit-ts \"audit_ts\"} result-set]
    [foo bar baz end-date audit-ts])"
  [binding & body]
  (i/expected vector? "a binding vector" binding)
  (i/expected #(= 2 (count %)) "binding vector of two forms" binding)
  (let [[lhs result-set] binding  ; LHS = Left-Hand-Side in a binding pair, for the lack of a better expression
        result-set-sym (with-meta (gensym "result-set-") {:tag "java.sql.ResultSet"})
        make-bindings  #(mapcat (fn [[sym col-ref]]
                                  (iresult/read-column-binding sym result-set-sym col-ref)) %)]
    (cond
      (vector? lhs) (do (i/expected #(every? (complement #{:as '&}) %)
                          ":as and & not to be in sequential destructuring" lhs)
                      `(let [~result-set-sym ~result-set
                             ~@(->> lhs
                                 (map-indexed (fn [^long idx each] [each (inc idx)]))
                                 make-bindings)]
                         ~@body))
      (map? lhs)    (let [k-bindings (fn [k f] (->> (get lhs k)
                                                 (map #(vector % (f (first (i/as-vector %)))))  ; turn into pairs
                                                 make-bindings))]
                      (i/expected #(every? (some-fn #{:labels :_labels} (complement keyword?)) (keys %))
                        "keyword keys to be only :labels or :_labels in associative destructuring" lhs)
                      `(let [~result-set-sym ~result-set
                             ~@(k-bindings :labels  str)
                             ~@(k-bindings :_labels #(str/replace (str %) \- \_))
                             ~@(->> (seq lhs)
                                 (remove (comp keyword? first))  ; remove pairs where keys are keywords
                                 make-bindings)]
                         ~@body))
      :otherwise    (i/expected "vector (sequential) or map (associative) destructuring form" lhs))))


(defn make-columns-reader
  "Given result column types, return a type-aware, efficient columns-reading function."
  ([result-types col-args]
    (i/expected vector? "vector of result types" result-types)
    (i/expected vector? "vector of column args" col-args)
    (when-not (= (count result-types) (count col-args))
      (i/expected (format "length of result-types (%d) and col-args (%d) to be the same"
                    (count result-types) (count col-args))
        {:result-types result-types :col-args col-args}))
    (doseq [t result-types]
      (when-not (contains? t/single-typemap t) (i/expected-result-type t)))
    (let [rsyms (-> (count result-types)
                  (repeatedly gensym)
                  vec)
          rlhs  (mapv vector rsyms result-types col-args)]
      (eval `(fn row-maker#
               ([^ResultSet result-set# ^long col-count#]
                 (when-not (= col-count# ~(count result-types))
                   (i/expected ~(str (count result-types) " columns") col-count#))
                 (letcol [~rlhs result-set#]
                   ~rsyms))
               ([sql-source# ^ResultSet result-set# ^long col-count#]
                 (row-maker# result-set# col-count#))))))
  ([result-types]
    (make-columns-reader result-types (mapv (constantly nil) result-types))))


(defn make-column-value-reader
  "Given result column type and column index, return a type-aware, efficient value-reading function."
  ([result-type ^long column-index col-arg]
    (when-not (contains? t/single-typemap result-type) (i/expected-result-type result-type))
    (i/expected (every-pred pos? integer?) "positive integer" column-index)
    (let [rs-sym (with-meta (gensym "result-set-") {:tag "java.sql.ResultSet"})]
      (eval `(fn col-reader#
               ([~rs-sym]
                 ~(iresult/read-column-expr result-type rs-sym column-index col-arg))
               ([sql-source# ~rs-sym]
                 (col-reader# ~rs-sym))))))
  ([result-types ^long column-index]
    (make-column-value-reader result-types column-index nil)))


;; ----- read java.sql.ResultSet columns at runtime -----


(def read-column-value iresult/read-column-value)


(defn read-columns
  ([^ResultSet result-set ^long column-count]
    (let [^objects row (object-array column-count)]
      (loop [i (int 0)]
        (when (< i column-count)
          (let [j (unchecked-inc i)]
            (aset row i (read-column-value result-set j))
            (recur j))))
      (vec row)))
  ([column-types ^ResultSet result-set ^long column-count]
    (let [^objects row (object-array column-count)]
      (loop [i (int 0)]
        (when (< i column-count)
          (let [j (unchecked-inc i)]
            (aset row i (read-column-value (get column-types i) result-set j))
            (recur j))))
      (vec row))))


;; ----- helper utility fns for asphalt.core/fetch-maps -----


(defn label->key
  "Convert given string (column) label to keyword."
  [^String label]
  (keyword (.toLowerCase label)))


(defn _label->key
  "Convert given string (column) label to keyword after replacing underscores with dashes."
  [^String label]
  (keyword (.replace (.toLowerCase label) \_ \-)))


;; ----- helper utility fns for asphalt.core/fetch-single-row/value -----


(defn throw-on-empty
  "Handle :on-empty event in fetch fns by throwing exception."
  [sql-source ^ResultSet result-set]
  (throw
    (ex-info (str "Expected exactly one JDBC result row, but found no result row for SQL-source: " (pr-str sql-source))
      {:sql-source sql-source :empty? true})))


(defn throw-on-multi
  "Handle :on-multi event in fetch fns by throwing exception."
  [sql-source ^ResultSet result-set value]
  (throw
    (ex-info (str "Expected exactly one JDBC result row, but found more than one for SQL-source: " (pr-str sql-source))
      {:sql-source sql-source :multi? true})))


(defn nil-on-empty
  "Handle asphalt.core/fetch-optional-row/value :on-empty event by returning nil."
  [_ _]
  nil)


(defn ignore-multi
  "Handle asphalt.core/fetch-optional-row/value :on-multi event by returning the first value."
  [_ _ v]
  v)
