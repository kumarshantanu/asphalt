;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.internal.isql
  (:require
    [clojure.string   :as string]
    [asphalt.internal :as i]
    [asphalt.param    :as p]
    [asphalt.result   :as r]
    [asphalt.type     :as t])
  (:import
    [java.sql PreparedStatement ResultSet]))


;; ----- parsing helpers -----


(def encode-name keyword)


(defn encode-result-type
  [^String sql ^String token]
  (let [k (keyword token)]
    (when-not (contains? t/single-typemap k)
      (i/expected-result-type (str " in SQL string: " sql) token))
    (get t/single-typemap k)))


(defn encode-param-type
  [^String sql ^String token]
  (let [k (keyword token)]
    (when-not (contains? t/all-typemap k)
      (i/expected-param-type (str " in SQL string: " sql) token))
    (if (contains? t/multi-typemap k)
      k
      (get t/all-typemap k))))


(defn valid-name-char?
  [^StringBuilder partial-name ch]
  (if (empty? partial-name)
    (Character/isJavaIdentifierStart ^char ch)
    (or
      (Character/isJavaIdentifierPart ^char ch)
      ;; for keywords with hyphens
      (= ^char ch \-))))


(defn valid-type-char?
  [^StringBuilder partial-name ch]
  (if (empty? partial-name)
    (Character/isJavaIdentifierStart ^char ch)
    (or
      (Character/isJavaIdentifierPart ^char ch)
      ;; for keywords with hyphens
      (= ^char ch \-))))


(def initial-parser-state
  {:c? false ; SQL comment in progress?
   :d? false ; double-quote string in progress?
   :e? false ; current escape state
   :n? false ; named param in progress
   :ns nil   ; partial named param string
   :s? false ; single-quote string in progress?
   :t? false ; type-hinted token in progress
   :ts nil   ; partial type-hinted token string
   })


(defn finalize-parser-state
  "Verify that final state is sane and clean up any unfinished stuff."
  [sql ec name-handler parser-state]
  (when (:d? parser-state) (i/illegal-arg "SQL cannot end with incomplete double-quote token:" sql))
  (when (:e? parser-state) (i/illegal-arg (format "SQL cannot end with a dangling escape character '%s':" ec) sql))
  (let [parser-state (merge parser-state (when (:n? parser-state)
                                           (name-handler (:ns parser-state) (:ts parser-state))
                                           {:ts nil :n? false :ns nil}))]
    (when (:s? parser-state) (i/illegal-arg "SQL cannot end with incomplete single-quote token:" sql))
    (when (:t? parser-state) (i/illegal-arg "SQL cannot end with a type hint"))
    (when (:ts parser-state) (i/illegal-arg "SQL cannot end with a type hint"))))


(defn update-param-name!
  "Update named param name."
  [^StringBuilder sb parser-state ch mc special-chars name-handler delta-state]
  (let [^StringBuilder nsb (:ns parser-state)]
    (if (valid-name-char? nsb ch)
      (do (.append nsb ^char ch)
        nil)
      (if-let [c (special-chars ^char ch)]
        (i/illegal-arg (format "Named parameter cannot precede special chars %s: %s%s%s%s"
                         (pr-str special-chars) sb mc nsb c))
        (do
          (name-handler nsb (:ts parser-state))
          (.append sb ^char ch)
          delta-state)))))


(defn update-type-hint!
  [^StringBuilder sb parser-state ch tc delta-state]
  (let [^StringBuilder tsb (:ts parser-state)]
    (cond
      ;; type char
      (valid-type-char? tsb ^char ch) (do (.append tsb ^char ch)
                                        nil)
      ;; shortcut (for default)
      (and (zero? (.length tsb))
        (= tc ch))                    (do (.append tsb (i/as-str (get t/single-typemap nil)))
                                        nil)
      ;; whitespace implies type has ended
      (Character/isWhitespace ^char ch) delta-state
      ;; catch-all default case
      :otherwise (i/illegal-arg
                   (format "Expected type-hint '%s%s' to precede a whitespace, but found '%s': %s%s%s%s"
                     tc (:ts parser-state) ch
                     sb tc (:ts parser-state) ch)))))


(defn encounter-sql-comment
  [^StringBuilder sb delta-state]
  (let [idx (unchecked-subtract (.length sb) 2)]
    (when (and (>= idx 0)
            (= (.charAt sb idx) \-))
      delta-state)))


(defn parse-sql-str
  "Parse SQL string using escape char, named-param char and type-hint char, returning [sql named-params return-col-types]"
  [^String sql ec mc tc]
  (let [^char ec ec ^char mc mc ^char tc tc nn (count sql)
        st (transient [])  ; SQL template (alternating tokens of SQL-string and param-name/type vectors)
        ^StringBuilder sb (StringBuilder. nn)
        ts (transient [])  ; result column types
        handle-named! (fn [^StringBuilder buff ^StringBuilder param-type]
                        (conj! st (.toString sb))
                        (.setLength sb 0)
                        (conj! st [(.toString buff) (when param-type (.toString param-type))]))
        handle-typed! (fn [^StringBuilder buff] (conj! ts (.toString buff)))
        special-chars #{ec mc tc \" \'}]
    (loop [i 0 ; current index
           s initial-parser-state]
      (if (>= i nn)
        (finalize-parser-state sql ec handle-named! s)
        (let [ch (.charAt sql i)
              ps (merge s
                   (condp s false
                    :c? (do (.append sb ch) (when (= ch \newline) {:c? false})) ; SQL comment in progress
                    :d? (do (.append sb ch) (when (= ch \")       {:d? false})) ; double-quote string in progress
                    :e? (do (.append sb ch)                       {:e? false})  ; escape state
                    :n? (update-param-name!  ; clear :ts at end
                          sb s ch mc special-chars handle-named!  {:ts nil :n? false :ns nil}) ; named param in progress
                    :s? (do (.append sb ch) (when (= ch \')       {:s? false})) ; single-quote string in progress
                    :t? (update-type-hint! sb s ch tc             {:t? false})  ; type-hint in progress (:ts intact)
                    (condp = ch  ; catch-all
                      mc                                          {:n? true :ns (StringBuilder.)}
                      (do (when-let [^StringBuilder tsb (:ts s)]
                            (handle-typed! tsb))
                        (merge {:ts nil}
                          (condp = ch
                            ec                                    {:e? true}
                            tc                                    {:t? true :ts (StringBuilder.)}
                            (do (.append sb ch)
                              (case ch
                                \"                                {:d? true}
                                \'                                {:s? true}
                                \- (encounter-sql-comment sb      {:c? true})
                                nil))))))))]
          (recur (unchecked-inc i) ps))))
    (when (pos? (.length sb))
      (conj! st (.toString sb)))
    [(persistent! st) (persistent! ts)]))


;; ----- SQL generation -----


(def cached-qmarks
  (memoize (fn [^long n]
             (string/join ", " (repeat n \?)))))


(defn make-sql
  "Given SQL template tokens, param-placeholder functions {:param-key (fn [n]) -> placeholders-string} and SQL params,
  generate the SQL string."
  ^String
  [sql-tokens cached-param-placeholder params]
  (let [^StringBuilder sb (StringBuilder.)]
    (i/each-indexed [i 0
                     token sql-tokens]
      (if (string? token)
        (.append sb ^String token)
        (let [[param-key param-type] token]
          (if (contains? t/multi-typemap param-type)
            (let [k (if (vector? params) i param-key)]
              (if (contains? params k)
                (if-let [cached-holder (get cached-param-placeholder param-key)]
                  (.append sb ^String (cached-holder (count (get params k))))
                  (.append sb ^String (cached-qmarks (count (get params k)))))
                (i/illegal-arg "No value found for key:" k "in" (pr-str params))))
            (.append sb \?)))))
    (.toString sb)))


;; ----- SQL templates -----


(defn bad-st-arity
  [^long n ^String sql-name]
  (throw (clojure.lang.ArityException. n (str sql-name " (accepts 2 args)"))))


(defrecord StaticSqlTemplate
  [^String sql-name ^String sql param-setter row-maker column-reader connection-worker]
  clojure.lang.Named
  (getNamespace [_] nil)
  (getName      [_] sql-name)
  t/ISqlSource
  (get-sql    [this params] sql)
  (set-params [this prepared-stmt params] (param-setter prepared-stmt params))
  (read-col   [this result-set] (column-reader result-set))
  (read-row   [this result-set col-count] (row-maker result-set col-count))
  clojure.lang.IFn
  (applyTo    [this args] (let [n (count args)]
                            (if (= 2 n)
                              (apply connection-worker args)
                              (bad-st-arity n sql-name))))
  (invoke     [this] (bad-st-arity 0 sql-name))
  (invoke     [this connection-source] (connection-worker connection-source this []))
  (invoke     [this connection-source params] (connection-worker connection-source this params))
  (invoke     [this a b c] (bad-st-arity 3 sql-name))
  (invoke     [this a b c d] (bad-st-arity 4 sql-name))
  (invoke     [this a b c d e] (bad-st-arity 5 sql-name))
  (invoke     [this a b c d e f] (bad-st-arity 6 sql-name))
  (invoke     [this a b c d e f g] (bad-st-arity 7 sql-name))
  (invoke     [this a b c d e f g h] (bad-st-arity 8 sql-name))
  (invoke     [this a b c d e f g h i] (bad-st-arity 9 sql-name))
  (invoke     [this a b c d e f g h i j] (bad-st-arity 10 sql-name))
  (invoke     [this a b c d e f g h i j k] (bad-st-arity 11 sql-name))
  (invoke     [this a b c d e f g h i j k l] (bad-st-arity 12 sql-name))
  (invoke     [this a b c d e f g h i j k l m] (bad-st-arity 13 sql-name))
  (invoke     [this a b c d e f g h i j k l m n] (bad-st-arity 14 sql-name))
  (invoke     [this a b c d e f g h i j k l m n o] (bad-st-arity 15 sql-name))
  (invoke     [this a b c d e f g h i j k l m n o p] (bad-st-arity 16 sql-name))
  (invoke     [this a b c d e f g h i j k l m n o p q] (bad-st-arity 17 sql-name))
  (invoke     [this a b c d e f g h i j k l m n o p q r] (bad-st-arity 18 sql-name))
  (invoke     [this a b c d e f g h i j k l m n o p q r s] (bad-st-arity 19 sql-name))
  (invoke     [this a b c d e f g h i j k l m n o p q r s t] (bad-st-arity 20 sql-name))
  (invoke     [this a b c d e f g h i j k l m n o p q r s t u] (bad-st-arity (+ 20 (alength u)) sql-name)))


(defrecord DynamicSqlTemplate
  [^String sql-name sql-tokens placeholder-fns param-setter row-maker column-reader connection-worker]
  clojure.lang.Named
  (getNamespace [_] nil)
  (getName      [_] sql-name)
  t/ISqlSource
  (get-sql    [this params] (make-sql sql-tokens placeholder-fns params))
  (set-params [this prepared-stmt params] (param-setter prepared-stmt params))
  (read-col   [this result-set] (column-reader result-set))
  (read-row   [this result-set col-count] (row-maker result-set col-count))
  clojure.lang.IFn
  (applyTo    [this args] (let [n (count args)]
                            (if (= 2 n)
                              (apply connection-worker args)
                              (bad-st-arity n sql-name))))
  (invoke     [this] (bad-st-arity 0 sql-name))
  (invoke     [this connection-source] (connection-worker connection-source this []))
  (invoke     [this connection-source params] (connection-worker connection-source this params))
  (invoke     [this a b c] (bad-st-arity 3 sql-name))
  (invoke     [this a b c d] (bad-st-arity 4 sql-name))
  (invoke     [this a b c d e] (bad-st-arity 5 sql-name))
  (invoke     [this a b c d e f] (bad-st-arity 6 sql-name))
  (invoke     [this a b c d e f g] (bad-st-arity 7 sql-name))
  (invoke     [this a b c d e f g h] (bad-st-arity 8 sql-name))
  (invoke     [this a b c d e f g h i] (bad-st-arity 9 sql-name))
  (invoke     [this a b c d e f g h i j] (bad-st-arity 10 sql-name))
  (invoke     [this a b c d e f g h i j k] (bad-st-arity 11 sql-name))
  (invoke     [this a b c d e f g h i j k l] (bad-st-arity 12 sql-name))
  (invoke     [this a b c d e f g h i j k l m] (bad-st-arity 13 sql-name))
  (invoke     [this a b c d e f g h i j k l m n] (bad-st-arity 14 sql-name))
  (invoke     [this a b c d e f g h i j k l m n o] (bad-st-arity 15 sql-name))
  (invoke     [this a b c d e f g h i j k l m n o p] (bad-st-arity 16 sql-name))
  (invoke     [this a b c d e f g h i j k l m n o p q] (bad-st-arity 17 sql-name))
  (invoke     [this a b c d e f g h i j k l m n o p q r] (bad-st-arity 18 sql-name))
  (invoke     [this a b c d e f g h i j k l m n o p q r s] (bad-st-arity 19 sql-name))
  (invoke     [this a b c d e f g h i j k l m n o p q r s t] (bad-st-arity 20 sql-name))
  (invoke     [this a b c d e f g h i j k l m n o p q r s t u] (bad-st-arity (+ 20 (alength u)) sql-name)))


(extend-protocol t/ISqlSource
  ;;==============
  java.lang.String
  ;;==============
  (get-sql    [sql params] sql)
  (set-params [sql prepared-stmt params] (p/set-params prepared-stmt params))
  (read-col   [sql result-set] (r/read-column-value result-set 1))
  (read-row   [sql result-set column-count] (r/read-columns result-set column-count))
  ;;============
  java.util.List
  ;;============
  (get-sql    [this params] (make-sql (first this) nil params))
  (set-params [this prepared-stmt params] (if-let [kt-pairs (seq (filter vector? (first this)))]
                                            (p/set-params prepared-stmt
                                              (mapv first kt-pairs) (mapv second kt-pairs) params)
                                            (p/set-params prepared-stmt params)))
  (read-col   [this result-set] (r/read-column-value result-set 1))
  (read-row   [this result-set col-count] (if-let [ts (seq (second this))]
                                            (r/read-columns ts result-set col-count)
                                            (r/read-columns result-set col-count))))
