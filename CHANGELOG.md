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
