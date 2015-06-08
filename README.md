# asphalt

A Clojure library for JDBC access.


## Usage

Leiningen coordinates: `[asphalt "0.1.0-SNAPSHOT"]`

Most of what you would typically need is in the namespace `asphalt.core`, so `require` it first:

```clojure
(require '[asphalt.core :as a])
```

You need [`javax.sql.DataSource`](https://docs.oracle.com/javase/8/docs/api/javax/sql/DataSource.html) instances to
work with Asphalt. Use either of the following for creating datasources:

* [clj-dbcp](https://github.com/kumarshantanu/clj-dbcp)
* [c3p0](https://github.com/samphilipd/clojure.jdbc-c3p0)
* [bone-cp](https://github.com/myfreeweb/clj-bonecp-url)
* [hikari-cp](https://github.com/tomekw/hikari-cp)


### Simple usage

This section covers the minimal examples only. Advanced features are covered in subsequent sections.

#### Insert with generated keys

```clojure
(a/genkey data-source
  "INSERT INTO emp (name, salary, dept) VALUES (?, ?, ?)"
  ["Joe Coder" 100000 "Accounts"])
```

Function `a/fetch-single-value` works on a `java.sql.ResultSet` object returning the value of single row, single column.

#### Update rows

```clojure
(a/update data-source "UPDATE emp SET salary = ? WHERE dept = ?" [110000 "Accounts"])
```

You may use the `a/update` function for `INSERT`, `UPDATE`, `DELETE` statements and DDL statements such as
`ALTER TABLE`, `CREATE INDEX` etc.

#### Query one row

```clojure
(vec (a/query a/fetch-single-row
       data-source
       "SELECT name, salary, dept FROM emp" []))
```

We wrap the call with `vec` here because `a/fetch-single-row` returns a Java array of column values. In programs you
may de-structure the column values directly from the returned Java array:

```clojure
(let [[name salary dept] (a/query a/fetch-single-row ...)]
  ;; work with the column values
  ...)
```

#### Query several rows

```clojure
(a/query a/fetch-rows
  data-source
  "SELECT name, salary, dept FROM emp" [])
```

This returns a vector of rows, where each row is a Java array of column values.


### SQL templates

Ordinary SQL with `?` place-holders may be boring and tedious to work with. Asphalt uses SQL-templates to fix that.

```clojure
(a/defsql sql-insert "INSERT INTO emp (name, salary, dept) VALUES ($name, $salary, $dept)")

(a/defsql sql-update "UPDATE emp SET salary = $new-salary WHERE dept = $dept")
```

With SQL-templates, you can pass param maps with keys as param names:

```clojure
(a/genkey data-source sql-insert
  {:name "Joe Coder" :salary 100000 :dept "Accounts"})
(a/update data-source sql-update {:new-salary 110000 :dept "Accounts"})
```

### SQL templates with type hints

The examples we saw above read and write values as objects, which means we depend on the JDBC driver for the conversion.
SQL-templates let you optionally specify the types of params and also return columns in a query: 

```clojure
(a/defsql sql-insert
  "INSERT INTO emp (name, salary, dept) VALUES ($name^string, $salary^int, $dept^string)")

(a/defsql sql-select "SELECT name^string, salary^int, dept^string FROM emp")

(a/defsql sql-update "UPDATE emp SET salary = $new-salary^int WHERE dept = $dept^string")
```

The operations on the type-hinted SQL-templates remain the same as non type-hinted SQL templates, but internally the
appropriate types are used when communicating with the JDBC driver.

#### Caveats with SQL-template type hints

Type hints are optional at each param level. However, when type-hinting the return columns in a query you should either
type-hint every column, or not specify type-hints for any column at all.

### Transactions

```clojure
(a/with-transaction [conn data-source] :read-committed
  (let [[id salary dept] (a/query a/fetch-single-row conn sql-select-with-id [])
        new-salary (compute-new-salary salary dept)]
    (a/update conn sql-update {:new-salary new-salary :id id})))
```

If the code doesn't throw any exception the transaction would be committed. On all exceptions the transaction would be
rolled back.

Supported isolation levels are: `:none`, `:read-committed`, `:read-uncommitted`, `:repeatable-read`, `:serializable`.


## License

Copyright © 2015 Shantanu Kumar (kumar.shantanu@gmail.com, shantanu.kumar@concur.com)

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
