# Changelog

All notable changes to DSMySQL are documented here.  
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).  
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

- `time_type` — maps to SQL `TIME`; wraps a `std::chrono::microseconds` duration with optional
  fractional-second precision (0–6). Supports `INSERT`/`UPDATE` serialization and `SELECT`
  deserialization, including negative durations.

### Changed

- **Breaking:** Renamed temporal value types to align with the `float_type` / `double_type` /
  `decimal_type` naming scheme introduced in v1.1.0:
  - `sql_datetime`  → `datetime_type`
  - `sql_timestamp` → `timestamp_type`

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

[Unreleased]: https://github.com/DisciplinedSoftware/DSMySQL/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/DisciplinedSoftware/DSMySQL/releases/tag/v1.1.0
[1.0.0]: https://github.com/DisciplinedSoftware/DSMySQL/releases/tag/v1.0.0
