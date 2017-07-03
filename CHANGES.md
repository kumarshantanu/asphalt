# Changelog

## TODO and Ideas

* [TODO] Support stored procedure call
* [TODO] Remove protocol fn `asphalt.type.ISqlSource/read-col` (supplant with `read-row`)
* [TODO] Support for more parameter types, e.g. `utc-date`, `utc-time`, `utc-timestamp`
* [TODO - BREAKING CHANGE] Make all type hints (even those with primitive Java types) null-safe for reads
  * Challenge: JDBC drivers do not implement this reliably, e.g.
    * H2 does not implement `ResultSet.getObject(int, Class)` at all
    * MySQL always delegates `ResultSet.getObject(int, Integer)` to `ResultSet.getInt(int)`
  * Challenge: All connection pool libraries do not support this, e.g.
    * Apache DBCP 1.x does not implement JDBC 4.2, and by extension `ResultSet.getObject(int, Class)`
  * [TODO] Deprecate/Remove support for primitive type hints (int, float, long, double, boolean)


## [WIP] 0.7.0 / 2017-July-??

* Enhance `defsql` and `compile-sql-template`
  * Accept options for direct use instead of factory functions
    * `:result-set-worker` (this has no corresponding factory fn)
    * `:params-setter`
    * `:row-maker`
    * `:column-reader`
    * `:conn-worker`
  * Make SQL templates behave as arity-1 fn for SQL that accepts no params
* Documentation fixes and enhancements


## 0.6.0 / 2017-February-20

* [BREAKING CHANGE] Default every unspecified hint to `^object` instead of dynamic discovery
  * This makes type hints unnecessary for achieving best performance
  * Dynamic discovery can now be enforced with explicit `^nil` type hint
  * Type hints are henceforth meant for result value coercion only
* Allow shortcut `^^` to imply default type hint (useful in hinting result columns)


## 0.5.1 / 2017-January-31

* Fix issue where result type hints are not used by generated row maker


## 0.5.0 / 2017-January-12

* Overhaul `asphalt.core/defsql`
  * Named by default
    * Implement `clojure.lang.Named` interface, hence `(name sql-source)` works
    * Name can be overridden via `:sql-name` option kwarg
  * Multi-value parameters (e.g. `IN (^ints $dept-ids)` clauses)
    * Applicable to type-hinted, named parameters only
    * Comma separated `?` placeholder
  * Configurable associations
    * Param setter  : auto-default to use param types when specified
    * Row maker     : auto-default to use result types when specified
    * Column reader : auto-default to read first column using result type when specified
    * Connection worker : auto-default to `query` vs `update` based on first SQL token
  * Behave as function `(f connection-source params)` using associated connection-worker
* Transactions
  * Do not override the exception causing rollback/commit by the exception in rollback/commit
* Types/Protocols
  * [BREAKING CHANGE] Now `asphalt.type.ISqlSource/get-sql` accepts params as argument (for dynamic SQL)
  * [BREAKING CHANGE] Drop support for map SQL templates
  * [BREAKING CHANGE] Now `asphalt.type.ISqlSource/read-col` no more accepts column-index argument
* Params setter utility in `asphalt.param` namespace
  * Macro `lay-params` (available at macro-expansion time, efficient)
  * Function `set-params` (available at run time, flexible)
  * Support for param types (single-value and multi-value)
  * Support for param position/name reference (vector/map/nil params)
  * Support for additional coercion arguments
  * Functions to convert date/time/timestamp to `java.util.Calendar` with timezone
  * [BREAKING CHANGE] Function `set-params-with-query-timeout` moved from `asphalt.core` to `asphalt.param`
* Result-columns reading utility in `asphalt.result` namespace
  * Efficient row-maker: `asphalt.result/letcol` (macro) with support for
    * Column types
    * Column index/label lookup
    * Additional arguments for specific result
  * Functions to read result-set column values
  * [BREAKING CHANGE] Default runtime row-maker returns a vector instead of an array
  * Column key maker fns for `asphalt.core/fetch-maps`
* Queries
  * [BREAKING CHANGE] Accept options in `asphalt.core` fetch fns (arity 3)
  * [BREAKING CHANGE] Remove `:column-index` option from `fetch-value`
  * [BREAKING CHANGE] Drop `asphalt.core/defquery` in favor of `asphalt.core/defsql` behaving as function
  * New fetch fns `fetch-optional-row` and `fetch-optional-value` in `asphalt.core` namespace
  * Default fetch fn in `asphalt.core/query` (arity 3) is now `asphalt.core/fetch-rows`
  * Re-implement `asphalt.core/fetch-maps` to use provided row-maker


## 0.4.0 / 2015-November-30

* Redundant/unwanted type hint was omitted to avoid compilation error with Clojure 1.8.0
* Fetching SQL statement
  * Rename `asphalt.type.ISql` to `asphalt.type.ISqlSource` (internal breaking change)
* JDBC connections are obtained from and returned to an abstraction `asphalt.type.IConnectionSource`
  * Extends to `java.sql.Connection` and `javax.sql.DataSource` (already supported)
  * Extends to map for [clojure/java.jdbc](https://github.com/clojure/java.jdbc) compatibility (new feature)
* Instrumentation endpoint `instrument-connection-source` for creating JDBC connections
  * Deprecate `instrument-datasource`
* Overhauled transaction API
  * All transaction management code moved to new namespace `asphalt.transaction` (breaking change)
  * Transactional connection source
  * `with-transaction` accepts an option map as second argument (breaking change)
  * Option keys `:result-success?`, `:error-failure?`, `:isolation`, `:propagation`
  * Transaction propagation is a first class abstraction `asphalt.type.ITransactionPropagation`
  * Nested transactions support (using savepoints)


## 0.3.0 / 2015-October-08

* Require Java 7 due to DataSource instrumentation
* Logic errors now throw `clojure.lang.ExceptionInfo` with meta data
* Pervasive instrumentation of `javax.sql.DataSource` objects
* Helper macro `defquery` to bind SQL templates to intended operations
* Helper fn `set-params-with-query-timeout` as `set-params` replacement


## 0.2.1 / 2015-September-23

* When error encountered setting a parameter, include parameter index/key in error message
* Use ex-info (`ExceptionInfo`) for runtime errors


## 0.2.0 / 2015-September-07

* [BREAKING CHANGE] Expect type hints _before_ subjects, consistent with Clojure
* New SQL abstraction: `asphalt.type.ISql`
* ISql supports string, map and `SQLTemplate` out of the box
* Performance improvement via primitive arrays in `SQLTemplate`
* Make type hint parsing robust (comments, string tokens etc.)


## 0.1.2 / 2015-July-07

* Version 0.1.1 introduced an array lookup bug, which caused the bug it meant to fix still open.
  This release fixes the array lookup bug, thereby also fixing the bug that 0.1.1 meant to fix.


## 0.1.1 / 2015-July-06

* Fix issue where default param setter wrongly assumed named params are always set in a SQL template


## 0.1.0 / 2015-June-19

* Support for SQL-template with type annotations
* Named parameters for SQL templates
* Functions (result-set-worker) to extract data from `java.sql.ResultSet`
* JDBC update and query operations (with `java.sql.Connection` and `javax.sql.DataSource`)
* Extraction of generated keys
* Transaction (auto-rollback on exceptions)
