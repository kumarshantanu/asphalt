(ns asphalt.type
  (:import
    [java.sql PreparedStatement ResultSet]))


(defprotocol ISql
  (get-sql    [this] "Return SQL string to be executed")
  (set-params [this ^PreparedStatement prepared-statement params] "Set prepared-statement params")
  (read-col   [this ^ResultSet result-set column-index] "Read column at specified index (1 based) from result-set")
  (read-row   [this ^ResultSet result-set column-count] "Read specified number of columns (starting at 1) as a row"))


(def ^:const sql-nil        0)
(def ^:const sql-bool       1)
(def ^:const sql-boolean    1) ; duplicate of bool
(def ^:const sql-byte       2)
(def ^:const sql-byte-array 3)
(def ^:const sql-date       4)
(def ^:const sql-double     5)
(def ^:const sql-float      6)
(def ^:const sql-int        7)
(def ^:const sql-integer    7) ; duplicate for int
(def ^:const sql-long       8)
(def ^:const sql-nstring    9)
(def ^:const sql-object    10)
(def ^:const sql-string    11)
(def ^:const sql-time      12)
(def ^:const sql-timestamp 13)
