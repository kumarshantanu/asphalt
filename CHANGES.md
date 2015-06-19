# Changelog

## TODO and Ideas

* [TODO] Stored procedure calls
* [Idea] Transaction propagation
* [Idea] Helpers (`defquery`, `defupdate`) to bind SQL templates with intended operations


## 0.1.0 / 2015-June-19

* Support for SQL-template with type annotations
* Named parameters for SQL templates
* Functions (result-set-worker) to extract data from `java.sql.ResultSet`
* JDBC update and query operations (with `java.sql.Connection` and `javax.sql.DataSource`)
* Extraction of generated keys
* Transaction (auto-rollback on exceptions)
