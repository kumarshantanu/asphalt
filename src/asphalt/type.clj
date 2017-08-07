;   Copyright (c) Shantanu Kumar. All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file LICENSE at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns asphalt.type
  (:import
    [java.sql Connection PreparedStatement ResultSet]))


(defprotocol IConnectionSource
  (^java.sql.Connection create-connection [this] "Create connection from the source")
  (^java.sql.Connection obtain-connection [this] "Obtain connection from the source")
  (return-connection  [this connection] "Return connection to the source"))


(defprotocol ISqlSource
  (get-sql    [this params] "Return SQL string to be executed")
  (set-params [this ^PreparedStatement prepared-stmt params] "Set prepared-statement params")
  (read-col   [this ^ResultSet result-set] "Read value column (column index is unspecified) from result-set")
  (read-row   [this ^ResultSet result-set ^long column-count] "Read given number of columns (starting at 1) as a row"))


(defprotocol ITransactionPropagation
  (execute-txn  [this connection-source txn-worker opts]  "Execute (worker connection txn-context) in a transaction")
  (commit-txn   [this ^Connection connection txn-context] "Commit current transaction")
  (rollback-txn [this ^Connection connection txn-context] "Rollback current transaction"))


(defrecord TxnConnectionSource
  [^Connection connection connection-source]
  IConnectionSource
  (create-connection      [this] (create-connection connection-source))
  (obtain-connection      [this] connection)
  (return-connection [this conn] (when-not (identical? conn connection) ; do not close current connection
                                   (return-connection connection-source conn))))


;; Supported SQL types (aliases not included)
;
; :nil (implies dynamic discovery)
; :array
; :ascii-stream
; :big-decimal
; :binary-stream
; :blob
; :boolean
; :byte
; :byte-array
; :character-stream
; :clob
; :date
; :double
; :float
; :int
; :long
; :ncharacter-stream
; :nclob
; :nstring
; :object
; :ref
; :row-id
; :string
; :sql-xml
; :time
; :timestamp
; :url


(def single-typemap {nil                :object   ; default type hint
                     :nil               :nil
                     :array             :array
                     :ascii-stream      :ascii-stream
                     :big-decimal       :big-decimal
                     :binary-stream     :binary-stream
                     :blob              :blob
                     :bool              :boolean  ; alias for :boolean
                     :boolean           :boolean
                     :byte              :byte
                     :byte-array        :byte-array
                     :character-stream  :character-stream
                     :clob              :clob
                     :date              :date
                     :double            :double
                     :float             :float
                     :int               :int
                     :integer           :int      ; alias for :int
                     :long              :long
                     :ncharacter-stream :ncharacter-stream
                     :nclob             :nclob
                     :nstring           :nstring
                     :object            :object
                     :ref               :ref
                     :row-id            :row-id
                     :string            :string
                     :sql-xml           :sql-xml
                     :time              :time
                     :timestamp         :timestamp
                     :url               :url})


(def multi-typemap {:arrays             :array
                    :ascii-streams      :ascii-stream
                    :big-decimals       :big-decimal
                    :binary-streams     :binary-stream
                    :blobs              :blob
                    :bools              :boolean  ; alias for :booleans
                    :booleans           :boolean
                    :bytes              :byte
                    :byte-arrays        :byte-array
                    :character-streams  :character-stream
                    :clobs              :clob
                    :dates              :date
                    :doubles            :double
                    :floats             :float
                    :ints               :int
                    :integers           :int      ; alias for :ints
                    :longs              :long
                    :ncharacter-streams :ncharacter-stream
                    :nclobs             :nclob
                    :nstrings           :nstring
                    :objects            :object
                    :refs               :ref
                    :row-ids            :row-id
                    :strings            :string
                    :sql-xmls           :sql-xml
                    :times              :time
                    :timestamps         :timestamp
                    :urls               :url})


(def all-typemap (merge single-typemap multi-typemap))


(defrecord StmtCreationEvent [^String sql
                              ;; #{:statement :prepared-statement :prepared-call}
                              jdbc-stmt-type])


(defrecord SQLExecutionEvent [^boolean prepared? ^String sql
                              ;; #{:sql :sql-query :sql-update}
                              sql-stmt-type])
