# Changelog

All notable changes to DSMySQL are documented here.  
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).  
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

- `check_id<"name">` — compile-time CHECK constraint name type; pass as second argument to `table_constraint::check` to emit a named constraint
- `index_id<"name">` — compile-time index name type; used as the first template parameter wherever an index name is required (`create_index_on`, `drop_index_on`, `add_index`, `add_unique_index`, `add_fulltext_index`, `table_constraint::key`, `unique_key`, `fulltext_key`, `spatial_key`)
- `NamedIdType` concept — satisfied by `check_id<N>`, `index_id<N>`, and any user-defined type providing a static `name()` → `std::string_view`
- `table_constraint::check(check_expr)` / `check<check_id<"name">>(check_expr)` — typed CHECK constraint from a check-safe predicate; constraint name is a template parameter, not a function argument; the old `check(string_view, string_view)` overload has been removed
- `check_expr` — new predicate type for expressions valid in MySQL CHECK constraints (column-ref comparisons, `BETWEEN`, `IN` with literal lists, `LIKE`, `REGEXP`, logical `&`/`|`/`!`); produced by all column-ref predicate factories; implicitly converts to `sql_predicate` so it can be used everywhere a WHERE predicate is accepted
- `alter_table<T>().change_column<OldCol, NewCol>()` — `CHANGE COLUMN old new type [NOT NULL]`; `NewCol` is a `ColumnDescriptor` (e.g. `column_field<"new_name", T>`) providing the new column name and type; use `std::optional<T>` to make the column nullable
- `alter_table<T>().rename_column<OldCol, NewCol>()` — `RENAME COLUMN old TO new`; `NewCol` must share the same `value_type` as `OldCol` (enforced at compile time)
- `alter_table<T>().add_column<Col>()` and `.modify_column<Col>()` now infer SQL type and nullability from the column descriptor — no more string type or bool parameter
- `alter_table<T>().add_index<Cols...>(name)` — `ADD INDEX name (col1, ...)`
- `alter_table<T>().add_unique_index<Cols...>(name)` — `ADD UNIQUE INDEX name (col1, ...)`
- `alter_table<T>().add_fulltext_index<Cols...>(name)` — `ADD FULLTEXT INDEX name (col1, ...)`
- `alter_table<T>().enable_keys()` / `.disable_keys()` — `ENABLE KEYS` / `DISABLE KEYS`
- `alter_table<T>().convert_to_charset(Charset)` — `CONVERT TO CHARACTER SET charset`
- `alter_table<T>().set_engine(Engine)` — `ENGINE = engine`
- `alter_table<T>().set_auto_increment(n)` — `AUTO_INCREMENT = n`
- `savepoint(name)` — `SAVEPOINT name`
- `release_savepoint(name)` — `RELEASE SAVEPOINT name`
- `rollback_to_savepoint(name)` — `ROLLBACK TO SAVEPOINT name`
- `set_transaction_isolation_level(IsolationLevel)` — `SET TRANSACTION ISOLATION LEVEL ...` with `IsolationLevel::{ReadUncommitted, ReadCommitted, RepeatableRead, Serializable}`
- `show_databases()` — `SHOW DATABASES` (queryable, result is `std::tuple<std::string>` per row)
- `show_tables()` — `SHOW TABLES` (queryable, result is `std::tuple<std::string>` per row)
- `show_columns<T>()` — `SHOW COLUMNS FROM table` (queryable, same row type as `describe<T>()`)
- `show_create_table<T>()` — `SHOW CREATE TABLE table` (queryable, result is `std::tuple<std::string, std::string>`)
- `create_procedure(name, params, body)` — `CREATE PROCEDURE name(params) BEGIN body END`
- `drop_procedure(name)` / `.if_exists()` — `DROP PROCEDURE [IF EXISTS] name`
- `call_procedure(name [, args])` — `CALL name([args])`
- `trigger_id<"name">` — compile-time trigger name type (satisfies `NamedIdType`)
- `create_trigger<trigger_id<"name">, T>(TriggerTiming, TriggerEvent, body)` — `CREATE TRIGGER ... ON table FOR EACH ROW body`; `TriggerTiming::{Before, After}`, `TriggerEvent::{Insert, Update, Delete}`
- `drop_trigger<trigger_id<"name">, T>()` / `.if_exists()` — `DROP TRIGGER [IF EXISTS] name`
- `sql_dcl.hpp` — new dedicated header for DCL (GRANT / REVOKE); included automatically by `ds_mysql.hpp`
- `privilege` enum — all MySQL privilege types: `select`, `insert`, `update`, `delete_`, `create`, `drop`, `references`, `index`, `alter`, `create_view`, `show_view`, `create_routine`, `alter_routine`, `execute`, `trigger`, `event`, `create_temporary_tables`, `lock_tables`, `reload`, `shutdown`, `process`, `file`, `show_databases`, `super`, `replication_slave`, `replication_client`, `create_user`, `create_tablespace`, `all`
- `privilege_list` — composable runtime privilege set via rvalue-chained fluent builder: `privileges().select().insert()`
- `on::global()` — grant target for `*.*` (all databases)
- `on::schema(database_name)` — grant target for `db.*`
- `on::table<T>(database_name)` — grant target for `db.table`; table name derived at compile time from `ValidTable` type via `table_name_for<T>`
- `on::table<DB, T>()` — grant target for `db.table`; both database and table names derived at compile time from their types via `database_name_for<DB>` and `table_name_for<T>`
- `user(user_name).at(host_name)` / `.at_any_host()` / `.at_localhost()` — compose a MySQL account (`'user'@'host'`) as a strongly-typed `grant_user`
- `grant(privilege_list, grant_target, grant_user)` / `.with_grant_option()` — `GRANT ... ON ... TO ... [WITH GRANT OPTION]`
- `grant<privilege::select, ...>(grant_target, grant_user)` — compile-time privilege set overload of `grant`
- `revoke(privilege_list, grant_target, grant_user)` — `REVOKE ... ON ... FROM ...`
- `revoke<privilege::select, ...>(grant_target, grant_user)` — compile-time privilege set overload of `revoke`

### Changed

- Eliminated immediately invoked lambda expressions (IILEs) from fold expressions in `schema_generator.hpp`, `sql_ddl.hpp`, `sql_mutation_shared.hpp`, and `mysql_connection.hpp`; each per-element body is now a named helper function (`column_definition_for`, `column_def_for`, `fk_clause_for`, `validate_one_table`), and separator logic uses a plain index loop over a stack-allocated array
- `[[nodiscard]]` added to all internal SQL-building helpers and trait `get()`/`value()`/`sql_expr()` static members: `sql_type_name::value`, `sql_type_for`, `field_sql_type_override`, `column_definition_for`, `generate_create_table_impl`, `generate_create_table`, `generate_values_impl`, `generate_values`, `generate_column_list_impl`, `generate_column_list`, `column_def_for`, `fk_clause_for`, `table_constraints::get`, `table_attributes::get`, `escape_sql_string`, `format_datetime`, `format_time`, `to_sql_value`, and all `projection_traits::sql_expr` static members in `sql_dql.hpp`

- **Breaking:** Uniform value-argument API — all query builder methods previously requiring template parameters now accept value arguments instead. Every column descriptor, table type, and expression type is passed as a default-constructed instance. No runtime cost: these are empty structs or trivially constructed types that are fully elided by the optimizer at `-O1` or above.

  | Old (template params) | New (value args) |
  |---|---|
  | `select<col1, col2>()` | `select(col1{}, col2{})` |
  | `.from<Table>()` | `.from(Table{})` |
  | `.from<joined<Table>>()` | `.from(joined<Table>{})` |
  | `.group_by<Col1, Col2>()` | `.group_by(Col1{}, Col2{})` |
  | `.order_by<Col>()` | `.order_by(Col{})` |
  | `.order_by<Col, sort_order::desc>()` | `.order_by(desc(Col{}))` |
  | `.order_by_alias<Proj>()` | `.order_by_alias(Proj{})` |
  | `.order_by_alias<Proj, sort_order::desc>()` | `.order_by_alias(desc(Proj{}))` |
  | `.with_alias<Proj>("name")` | `.with_alias(Proj{}, "name")` |
  | `.inner_join<T, L, R>()` | `.inner_join(T{}, L{}, R{})` |
  | `.inner_join_on<T>(pred)` | `.inner_join(T{}, pred)` |
  | `.left_join<T, L, R>()` | `.left_join(T{}, L{}, R{})` |
  | `.left_join_on<T>(pred)` | `.left_join(T{}, pred)` |
  | `.cross_join<T>()` | `.cross_join(T{})` |
  | `.natural_join<T>()` | `.natural_join(T{})` |
  | `.inner_join_using<T, C1, C2>()` | `.inner_join_using(T{}, C1{}, C2{})` |
  | `.straight_join<T, L, R>()` | `.straight_join(T{}, L{}, R{})` |
  | `.straight_join_on<T>(pred)` | `.straight_join(T{}, pred)` |
  | `update<T>().order_by<Col, sort_order::desc>()` | `.order_by(desc(Col{}))` |
  | `delete_from<T>().order_by<Col>()` | `.order_by(Col{})` |

- **Breaking:** Scalar projection functions previously using string/integer template parameters now take runtime constructor arguments. Types used as string values must be passed as instances of their column type; date intervals use the new `interval::*` namespace.

  | Old (template params) | New (runtime constructor) |
  |---|---|
  | `left_of<Col, 3>` | `left_of<Col>(3)` |
  | `right_of<Col, 3>` | `right_of<Col>(3)` |
  | `replace_of<Col, "a", "b">` | `replace_of<Col>(Col{"a"}, Col{"b"})` |
  | `lpad_of<Col, 10, "0">` | `lpad_of<Col>(10, Col{"0"})` |
  | `date_format_of<Col, "%Y-%m-%d">` | `date_format_of<Col>("%Y-%m-%d")` |
  | `date_add_of<Col, 7, day>` | `date_add_of<Col>(interval::day{7})` |
  | `truncate_to<Col, 2>` | `truncate_to<Col>(2)` |
  | `json_extract_of<Col, "$.field">` | `json_extract_of<Col>("$.field")` |
  | `if_of<"cond", Then, Else>` | `sql_if<Then, Else>(predicate)` |

- **Breaking:** `*_join_on<T>(pred)` overloads removed; use `*_join(T{}, pred)` instead (same function, `_on` suffix dropped).

- `desc_order<T>` and `desc()` moved from `sql_dql.hpp` to `sql_core.hpp`; now available in DML and any other context that includes `sql_core.hpp`.

- **Breaking:** `where_condition` renamed to `sql_predicate`; update any code that names the type explicitly
- **Breaking:** `table_constraint::check()` now accepts `check_expr` instead of `sql_predicate`; subquery predicates (`in_subquery`, `exists`, etc.) and `match_against` do not produce `check_expr` and will no longer compile in a CHECK context
- Simple column-ref predicate factories (`equal`, `greater_than`, `like`, `between`, `in`, etc.) now return `check_expr` instead of `sql_predicate`; `check_expr` implicitly converts to `sql_predicate` so existing WHERE/HAVING/JOIN ON usage is unaffected
- Subquery predicate factories (`in_subquery`, `not_in_subquery`, `exists`, `not_exists`) and `match_against` return `sql_predicate` directly (not check-safe)

- **Breaking:** `grant` and `revoke` now require strongly-typed `privilege_list`, `grant_target`, and `grant_user` arguments instead of plain strings; the old string-based overloads have been removed

- `natural_join<T>()`, `natural_left_join<T>()`, `natural_right_join<T>()` — `NATURAL [LEFT|RIGHT] JOIN` with no ON/USING clause
- `inner_join_using<T, Cols...>()`, `left_join_using<T, Cols...>()`, `right_join_using<T, Cols...>()`, `full_join_using<T, Cols...>()` — `JOIN ... USING (col1, col2, ...)` with one or more column descriptors
- `lateral_join(sql, alias)`, `left_lateral_join(sql, alias)` — `[LEFT] JOIN LATERAL (subquery) AS alias` (MySQL 8.0+)
- `lateral_join_on(sql, alias, cond)`, `left_lateral_join_on(sql, alias, cond)` — lateral join variants with an explicit `ON` condition
- `straight_join<T, LeftCol, RightCol>()`, `straight_join_on<T>(cond)` — `STRAIGHT_JOIN` MySQL optimizer hint forcing left-to-right join evaluation order
- `not_regexp<Col>(pattern)` — `col NOT REGEXP 'pattern'`
- `rlike<Col>(pattern)` — `col RLIKE 'pattern'` (MySQL synonym for `REGEXP`)
- `not_rlike<Col>(pattern)` — `col NOT RLIKE 'pattern'`
- `sounds_like<Col>(word)` — `col SOUNDS LIKE 'word'`
- `match_against<Cols...>(expr [, modifier])` — full-text search via `MATCH(...) AGAINST (...)`,
  supporting `match_search_modifier::boolean_mode`, `query_expansion`, and
  `natural_language_with_query_expansion`
- All of the above are also available as methods on `col_expr` / `col_ref<Col>` (except `match_against`)
- MySQL alias free functions: `mysql_not_regexp`, `mysql_rlike`, `mysql_not_rlike`
- `ntile_over<N, PartitionCol, OrderCol>` — `NTILE(N) OVER (...)`
- `percent_rank_over<PartitionCol, OrderCol>` — `PERCENT_RANK() OVER (...)`
- `cume_dist_over<PartitionCol, OrderCol>` — `CUME_DIST() OVER (...)`
- `first_value_over<Col, PartitionCol, OrderCol>` — `FIRST_VALUE(col) OVER (...)`
- `last_value_over<Col, PartitionCol, OrderCol>` — `LAST_VALUE(col) OVER (...)`
- `nth_value_over<Col, N, PartitionCol, OrderCol>` — `NTH_VALUE(col, N) OVER (...)`
- **Conditional scalar functions**: `nullif_of<A,B>`, `greatest_of<Ps...>`, `least_of<Ps...>`, `if_of<"cond",Then,Else>` with short aliases `nullif`, `greatest`, `least`, `sql_if`
- **String scalar functions**: `char_length_of<P>`, `left_of<P,N>`, `right_of<P,N>`, `replace_of<P,"from","to">`, `lpad_of<P,N,"pad">`, `rpad_of<P,N,"pad">`, `repeat_of<P,N>`, `reverse_of<P>`, `locate_of<"sub",P>`, `instr_of<P,"sub">`, `space_of<P>`, `strcmp_of<A,B>` with matching short aliases
- **Math scalar functions**: `sqrt_of<P>`, `log_of<P>`, `log2_of<P>`, `log10_of<P>`, `exp_of<P>`, `sin_of<P>`, `cos_of<P>`, `tan_of<P>`, `degrees_of<P>`, `radians_of<P>`, `sign_of<P>`, `truncate_to<P,D>`, `pi_val` with matching short aliases
- **Extended date/time scalar functions**: `hour_of<P>`, `minute_of<P>`, `second_of<P>`, `microsecond_of<P>`, `quarter_of<P>`, `week_of<P>`, `weekday_of<P>`, `dayofweek_of<P>`, `dayofyear_of<P>`, `dayname_of<P>`, `monthname_of<P>`, `last_day_of<P>`, `str_to_date_of<P,"fmt">`, `from_unixtime_of<P>`, `unix_timestamp_of<P>`, `convert_tz_of<P,"from","to">`, `curtime_val`, `utc_timestamp_val`, `addtime_of<A,B>`, `subtime_of<A,B>`, `timediff_of<A,B>` with matching short aliases
- **JSON scalar functions**: `json_extract_of<P,"$.path">`, `json_object_of<Ps...>`, `json_array_of<Ps...>`, `json_contains_of<P,"val">`, `json_length_of<P>`, `json_unquote_of<P>` with matching short aliases
- `stddev<Col>` — `STDDEV(col)` → `double`
- `std_of<Col>` — `STD(col)` → `double` (MySQL synonym for `STDDEV`)
- `stddev_pop<Col>` — `STDDEV_POP(col)` → `double`
- `stddev_samp<Col>` — `STDDEV_SAMP(col)` → `double`
- `variance<Col>` — `VARIANCE(col)` → `double`
- `var_pop<Col>` — `VAR_POP(col)` → `double`
- `var_samp<Col>` — `VAR_SAMP(col)` → `double`
- `bit_and_of<Col>` / `bit_and<Col>` — `BIT_AND(col)` → `uint64_t`
- `bit_or_of<Col>` / `bit_or<Col>` — `BIT_OR(col)` → `uint64_t`
- `bit_xor_of<Col>` / `bit_xor<Col>` — `BIT_XOR(col)` → `uint64_t`
- `json_arrayagg<Col>` — `JSON_ARRAYAGG(col)` → `std::string`
- `json_objectagg<KeyCol, ValCol>` — `JSON_OBJECTAGG(key, val)` → `std::string`
- `any_value<Col>` — `ANY_VALUE(col)` → `value_type` of `Col`

---

## [2.1.1] – 2026-03-24

### Fixed

- `create_all_tables<DB>()` now qualifies each table name as `<db_name>.<table_name>`,
  fixing table creation when no current database is selected in MySQL.

---

## [2.1.0] – 2026-03-24

---

### Added

- Add int_type<W>, int_unsigned_type<W>, bigint_type<W>, and bigint_unsigned_type<W>
to allow display width similarly to floating point types.

### Fixed

- Fixed table_attributes specialization that was broken

## [2.0.0] – 2026-03-23

### Added

- `time_type` — maps to SQL `TIME`; wraps a `std::chrono::microseconds` duration with optional
  fractional-second precision (0–6). Supports `INSERT`/`UPDATE` serialization and `SELECT`
  deserialization, including negative durations.

### Changed

- **Breaking:** Renamed temporal value types to align with the `float_type` / `double_type` /
  `decimal_type` naming scheme introduced in v1.1.0:
  - `sql_datetime`  → `datetime_type`
  - `sql_timestamp` → `timestamp_type`
- **Breaking:** Removed monolithic `ds_mysql/sql.hpp`. Include `ds_mysql/ds_mysql.hpp` for the umbrella API,
  or include `ds_mysql/sql_ddl.hpp`, `ds_mysql/sql_dml.hpp`, and `ds_mysql/sql_dql.hpp` directly.

---

## [1.1.0] - 2026-03-23

### Added

- Typed formatted numeric column wrappers: `float_type`, `double_type`, and `decimal_type`.
- Support for `<>`, `<precision>`, and `<precision, scale>` numeric wrapper forms in schema generation.
- Nullable formatted numeric columns via `std::optional<...>` wrappers.
- Unit coverage for formatted numeric DDL generation and DML serialization/deserialization.

---

## [1.0.0] – 2026-03-21

### Added

- Header-only C++23 MySQL query builder and database wrapper.
- Type-safe SELECT / INSERT / UPDATE / DELETE query building with compile-time column verification.
- Compile-time schema generation (`CREATE TABLE` SQL derived from C++ structs).
- Schema validation against a live database (`validate_schema`).
- Boost.PFR powered compile-time reflection — no macros required for reflection.
- `COLUMN_FIELD` macro for ergonomic per-table, per-column unique type definitions.
- Strong-type wrappers: `host_name`, `database_name`, `port_number`, `user_name`,
  `user_password`, `varchar_type<N>`, `text_type`.
- `std::optional<T>` support mapping to nullable SQL columns.
- `std::expected`-based error handling throughout — no exceptions in the query path.
- SQL helper functions: `ROUND`, `FORMAT`, `COALESCE`, `IFNULL`, `DATE_FORMAT`,
  `NOW`, `CURDATE`, `CURRENT_TIMESTAMP`, and more.
- Runtime metadata helpers (`mysql_metadata`) for column introspection.
- `.env` file support for integration-test configuration (via laserpants/dotenv).
- CI matrix covering GCC 15, Clang 20, and MSVC (Visual Studio 2022).
- Automated release workflow: tag validation, unit-test gate, and header-archive publishing.
- `ds_mysql::version` struct providing `major`, `minor`, `patch`, `value`, and `string`
  compile-time constants.

[Unreleased]: https://github.com/DisciplinedSoftware/DSMySQL/compare/v2.1.1...HEAD
[2.1.1]: https://github.com/DisciplinedSoftware/DSMySQL/compare/v2.1.0...v2.1.1
[2.1.0]: https://github.com/DisciplinedSoftware/DSMySQL/compare/v2.0.0...v2.1.0
[2.0.0]: https://github.com/DisciplinedSoftware/DSMySQL/compare/v1.1.0...v2.0.0
[1.1.0]: https://github.com/DisciplinedSoftware/DSMySQL/releases/tag/v1.1.0
[1.0.0]: https://github.com/DisciplinedSoftware/DSMySQL/releases/tag/v1.0.0
