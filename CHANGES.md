# Changelog

## TODO and Ideas

* [TODO] Stored procedure calls
* [Idea] Transaction propagation
* [Idea] Helpers (`defquery`, `defupdate`) to bind SQL templates with intended operations


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
