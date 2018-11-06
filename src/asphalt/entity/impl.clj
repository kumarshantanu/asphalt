;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.entity.impl
  "Implementation detail for asphalt.entity namespace. For internal use only - subject to change."
  (:require
    [asphalt.internal :as i]
    [asphalt.type     :as at])
  (:import
    [java.util.concurrent.atomic AtomicLong]))


(defn make-int-counter
  ([]
    (make-int-counter 0))
  ([^long n]
    (let [^AtomicLong a (AtomicLong. n)]
      (reify
        clojure.lang.IDeref
        (deref [this] (.incrementAndGet a))))))


(defn infer-field-definition
  [field]
  (i/expected (some-fn keyword? symbol? vector? map?) "keyword/symbol/vector/map" field)
  (cond
    (keyword? field) (infer-field-definition {:id field})
    (symbol? field)  (infer-field-definition {:id (keyword field)})
    (vector? field)  (let [[id options] field]
                       (i/expected (some-fn nil? map?) "option map or nil" options)
                       (merge options (infer-field-definition id)))
    (map? field)     (let [type    (->> (:type field)
                                     (get at/single-typemap))
                           counter (when (:auto-inc? field)
                                     (i/expected #{:int :long}
                                       ":int or :long type for auto-incrementing field" type)
                                     (make-int-counter))]
                       (i/expected :id "map containing :id key" field)
                       (i/expected at/single-typemap
                         (str "field type to be one of " (pr-str (keys at/single-typemap))) (:type field))
                       (when (contains? field :default)
                         (i/expected some? "non-nil default value" (:default field)))
                       (merge {:name      (i/as-str (:id field))
                               :param     [[(:id field) type]]
                               :type      type
                               :counter   counter
                               :not-null? false
                               :default   nil}
                         field
                         {:type type}))
    :otherwise       (throw (IllegalStateException. (str "Unexpected field representation: "
                                                      (pr-str field))))))
