# Changelog

All notable changes to DSMySQL are documented here.  
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).  
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

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
