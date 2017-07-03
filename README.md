# asphalt

A Clojure library for JDBC access.


## Why Asphalt?

* [Simple](http://www.infoq.com/presentations/Simple-Made-Easy) (as in separation of concerns)
  * Extensible connection mechanism (via a protocol)
  * Extensible SQL source (via a protocol)
  * SQL params setter is orthogonal to SQL and operation
  * Retrieving data from query result is orthogonal to SQL query and operation
  * Extensible transaction strategy (via a protocol)
* Performance and Control
  * Aspects can be overridden independent of each other
  * Support for type hints in SQL
* Instrumentation support
  * Making connection
  * Making statement
  * Executing SQL
* Rich transaction support
  * Transaction propagation (borrowed from EJB, Spring)
  * Fine-grained control over commit and rollback
  * Transaction isolation
  * Declarative transaction


## Usage

Leiningen coordinates: `[asphalt "0.7.0-SNAPSHOT"]` (requires Java 7 or higher, Clojure 1.6 or higher)

```clojure
(require '[asphalt.core :as a])        ; for most common operations
(require '[asphalt.transaction :as t]) ; for transactions
```


### Connection source

You need a valid JDBC connection source (instance of `asphalt.type.IConnectionSource` protocol) to work with Asphalt.
The following are supported by default:

* A map containing connection parameters (any of the following key sets)
  * `:connection` (`java.sql.Connection` instance)
  * `:factory` (fn that accepts a map and returns a JDBC connection)
  * `:classname` (JDBC driver classname), `:connection-uri` (JDBC URL string)
  * `:subprotocol` (sub-protocol portion of JDBC URL string), `:subname` (rest of the JDBC URL string)
  * `:datasource` (`javax.sql.DataSource` instance) with following optional attributes
    * `:username` or `:user` (database user name)
    * `:password` (database password)
  * `:name` ([JNDI](https://en.wikipedia.org/wiki/Java_Naming_and_Directory_Interface) name) with optional attributes
    * `:context` (`javax.naming.Context`)
    * `:environment` (environment map)
* A JDBC URL string
* JDBC datasource (`javax.sql.DataSource` instance)
* JDBC connection (`java.sql.Connection` instance)

For development you may define a map based connection source:

```clojure
(def conn-source {:subprotocol "mysql"
                  :subname "//localhost/testdb"
                  :username "testdb_user"
                  :password "secret"})
```

Typically one would create a connection-pooled datasource as connection source for production use:

* [clj-dbcp](https://github.com/kumarshantanu/clj-dbcp)
* [c3p0](https://github.com/samphilipd/clojure.jdbc-c3p0)
* [bone-cp](https://github.com/myfreeweb/clj-bonecp-url)
* [hikari-cp](https://github.com/tomekw/hikari-cp)


### Simple usage

This section covers the minimal examples only. Advanced features are covered in subsequent sections.

```clojure
;; insert row, returning auto-generated keys
(a/genkey conn-source
  "INSERT INTO emp (name, salary, dept) VALUES (?, ?, ?)"
  ["Joe Coder" 100000 "Accounts"])

;; update rows, returning the number of rows updated
;; used for `INSERT`, `UPDATE`, `DELETE` statements, or DDL statements such as `ALTER TABLE`, `CREATE INDEX` etc.
(a/update conn-source "UPDATE emp SET salary = ? WHERE dept = ?" [110000 "Accounts"])

;; query zero (nil) or more rows (vector of rows)
(a/query a/fetch-rows
  conn-source
  "SELECT name, salary, dept FROM emp" [])

;; query zero (nil) or one row (exception is thrown if result-set has more than one row)
(a/query a/fetch-optional-row
  conn-source
  "SELECT name, salary, dept FROM emp" [])

;; query one row, and work with column values via de-structuring
(let [[name salary dept] (a/query a/fetch-single-row ...)]
  ;; work with the column values
  ...)
```


### SQL templates

Ordinary SQL with `?` place-holders may be boring and tedious to work with. Asphalt uses SQL-templates to fix that.

```clojure
(a/defsql sql-insert "INSERT INTO emp (name, salary, dept) VALUES ($name, $salary, $dept)")

(a/defsql sql-update "UPDATE emp SET salary = $new-salary WHERE dept = $dept")
```

With SQL-templates, you can pass param maps with keys as param names:

```clojure
(a/genkey conn-source sql-insert
  {:name "Joe Coder" :salary 100000 :dept "Accounts"})

(a/update conn-source sql-update {:new-salary 110000 :dept "Accounts"})
```


#### SQL-templates are functions

SQL-templates defined with `defsql` are invokable as functions:

```clojure
;; defsql infers connection worker as either update or query
(sql-update conn-source {:new-salary 110000 :dept "Accounts"})

;; for genkey we need to specify as such
(a/defsql sql-insert "INSERT INTO emp (name, salary, dept) VALUES ($name, $salary, $dept)"
  {:conn-worker a/genkey})

(sql-insert conn-source {:name "Joe Coder" :salary 100000 :dept "Accounts"})

;; same as above, but using positional params
(sql-insert conn-source ["Joe Coder" 100000 "Accounts"])
```


### SQL templates with type hints

The examples we saw above read and write values as objects, which means we depend on the JDBC driver for the conversion.
SQL-templates let you optionally specify the types of params and also the result columns in a query:

```clojure
(a/defsql sql-insert
  "INSERT INTO emp (name, salary, dept) VALUES (^string $name, ^int $salary, ^string $dept)")

(a/defsql sql-select "SELECT ^string name, ^int salary, ^string dept FROM emp")

;; multi-value param
(a/defsql sql-update "UPDATE emp SET salary = ^int $new-salary WHERE dept IN (^strings $depts)")
```

The operations on the type-hinted SQL-templates remain the same as non type-hinted SQL templates, but internally the
appropriate types are used when communicating with the JDBC driver.


#### Supported type hints

The following types are supported as type hints:

| Type       | Comments             | Multi-value | Result on NULL |
|------------|----------------------|-------------|----------------|
|`nil`       |Dynamic/slow discovery| none        | `nil`   |
|`bool`      |Duplicate of `boolean`|`bools`      | `false` |
|`boolean`   |                      |`booleans`   | `false` |
|`byte`      |                      |`bytes`      | `0`     |
|`byte-array`|                      |`byte-arrays`| `nil`   |
|`date`      |                      |`dates`      | `nil`   |
|`double`    |                      |`doubles`    | `0.0`   |
|`float`     |                      |`floats`     | `0.0`   |
|`int`       |                      |`ints`       | `0`     |
|`integer`   |Duplicate of `int`    |`integers`   | `0`     |
|`long`      |                      |`longs`      | `0`     |
|`nstring`   |                      |`nstrings`   | `nil`   |
|`object`    |Catch-all type        |`objects`    | `nil`   |
|`string`    |                      |`strings`    | `nil`   |
|`time`      |                      |`times`      | `nil`   |
|`timestamp` |                      |`timestamps` | `nil`   |

Note on type hints in result columns:
- Primitive type hints for result columns coerce `NULL` value as primitive default values as shown in the table.
- You may specify `^^` (shortcut) as type hint to imply default or no type hint, e.g.
  `SELECT ^^ name, ^^ age, ^string join_date FROM emp WHERE id = ^int $id`

Note on multi-value types:
- Only applicable for SQL params, not for query result types
- Corresponding param must be a vector of values
- Every multi-value param expands into comma-separated `?` placeholders


#### Caveats with SQL-template type hints

- Type hints are optional at each param level. However, when type-hinting the result columns in a query you should
  either type-hint every column, or not specify type-hints for any column at all.
- Wildcards (e.g. `SELECT *`) in return columns are tricky to use with return column type hints. You should hint
  every return column type as in `SELECT * ^int ^string ^int ^date` if the return columns are of that type.
- Queries that use `UNION` are also tricky to use with return column type hints. You should hint only one set of
  return columns, not in every `UNION` sub-query.


### Transactions

Simple example:

```clojure
(t/with-transaction [txn conn-source] {}
  (a/update txn sql-insert ["Joe Coder" 100000 "accounts"])
  (a/update txn sql-update {:new-salary new-salary :id id}))
```

By default, if the code doesn't throw any exception the transaction would be committed and on all exceptions the
transaction would be rolled back.


#### Advanced example

```clojure
(a/with-transaction [txn data-source] {:isolation :read-committed
                                       :propagation t/tp-mandatory}
  (let [[id salary dept] (a/query a/fetch-single-row txn sql-select-with-id [])
        new-salary (compute-new-salary salary dept)]
    (a/update txn sql-update {:new-salary new-salary :id id})))
```

Supported isolation levels:
* `:none`
* `:read-committed`
* `:read-uncommitted`
* `:repeatable-read`
* `:serializable`

Supported transaction [propagation types](http://ninjalj.blogspot.in/2011/09/spring-transactional-propagation.html):
* `t/tp-mandatory`
* `t/tp-nested`
* `t/tp-never`
* `t/tp-not-supported`
* `t/tp-required`
* `t/tp-requires-new`
* `t/tp-supports`


#### Declarative transaction

Given a fn that accepts a connection source as its first argument, it is possible to wrap it with transaction options
such that the fn is invoked in a transaction.

```clojure
(defn foo
  [conn-source emp-id]
  ..)


(def bar (t/wrap-transaction-options foo {:isolation :read-committed
                                          :propagation t/tp-requires-new}))

;; call foo in a transaction as per the transaction options
(bar conn-source emp-id)
```


## Development

Running tests: `lein do clean, test` or `lein with-profile c18,dev,dbcp test`

Running performance benchmarks: `lein with-profile c18,dev,dbcp,perf test`


## License

Copyright © 2015-2017 Shantanu Kumar (kumar.shantanu@gmail.com, shantanu.kumar@concur.com)

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
