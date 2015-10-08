# Changelog

## TODO and Ideas

* [Idea] Transaction propagation
* [Idea] Helpers (`defquery`, `defupdate`) to bind SQL templates with intended operations
* [TODO] Support stored procedure call


## 0.3.0 / 2015-October-??

* Logic errors now throw `clojure.lang.ExceptionInfo` with meta data
* Pervasive instrumentation of `javax.sql.DataSource` objects
* Helper macro `defquery` to bind SQL templates to intended operations
* [TODO] Helper fn `set-params-with-timeout` as `set-params` replacement


## 0.2.1 / 2015-September-23

* When error encountered setting a parameter, include parameter index/key in error message
* Use ex-info (`ExceptionInfo`) for runtime errors


## 0.2.0 / 2015-September-07

* Expect type hints _before_ subjects, consistent with Clojure
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
