# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DSMySQL is a header-only C++23 type-safe MySQL query builder and database wrapper library. It provides compile-time type safety for SQL queries, schema generation from C++ structs via Boost.PFR reflection, and runtime schema validation against a live MySQL database.

All library code lives under `lib/include/ds_mysql/` — there are no `.cpp` files to compile for the library itself.

## Build Commands

```bash
# Configure and build (release preset is standard)
cmake --preset release
cmake --build build/release -j$(nproc)

# Run all tests
ctest --preset release

# Run only unit tests
ctest --preset release -R tests_unit_ds_mysql

# Run only integration tests (requires Docker or a live MySQL instance)
ctest --preset release -R tests_integration_ds_mysql
```

Available presets: `default`, `debug`, `release`, `coverage`, `ci` (CI skips Docker lifecycle).

Key CMake options:

- `-DBUILD_TESTING=ON` — builds unit and integration tests (default: ON)
- `-DBUILD_INTEGRATION_TESTS=ON` — builds MySQL integration tests (default: ON)
- `-DSKIP_DOCKER_MANAGEMENT=ON` — skips Docker container lifecycle in CTest (used in CI)
- `-DENABLE_COVERAGE=ON` — enables coverage instrumentation

## Code Formatting

```bash
scripts/util/format.sh   # applies clang-format to all headers
```

Style: Google-based, 120-char line limit, 4-space indent (`.clang-format` at root).

## Architecture

### Header Layout (`lib/include/ds_mysql/`)

- `ds_mysql.hpp` — umbrella include; users include only this
- `mysql_connection.hpp` — `mysql_config`, `connect_options`, and `mysql_connection` classes; `.query()` returns `std::expected<ResultType, std::string>`, `.execute()` returns `std::expected<uint64_t, std::string>` (affected row count; errors include the failing SQL statement); also provides `last_insert_id()`, `ping()`, `commit()`, `rollback()`, `autocommit()`, `select_db()`, `reset_connection()`, `escape_string()`, `warning_count()`, `info()`, `server_version()`, `server_info()`, `stat()`, `thread_id()`, `character_set()`, `set_character_set()`
- `sql_ddl.hpp` — `create_table(T{})`, `drop_table(T{})`, `create_database(DB{})`, `create_all_tables(DB{})`
- `sql_dml.hpp` — `insert_into(T{})`, `update(T{})`, `delete_from(T{})`, `truncate_table(T{})`
- `sql_dql.hpp` — `select(col1{}, col2{})`, `count(T{})`, `describe(T{})`
- `connect_options.hpp` — `connect_options` fluent builder and `ssl_mode` enum for pre-connect `mysql_options()` configuration (timeouts, SSL/TLS, charset, compression, etc.)
- `charset_name.hpp` — `charset_name` strong type for character set names
- `column_field.hpp` / `column_field_base_*.hpp` — `column_field<"name", Type>` descriptors and `COLUMN_FIELD(name, type)` macro
- `schema_generator.hpp` — derives CREATE TABLE SQL from a C++ struct at compile time using Boost.PFR
- `sql_varchar.hpp`, `sql_numeric.hpp`, `sql_temporal.hpp` — library-specific SQL types (`varchar_type<N>`, `decimal_type<P,S>`, `datetime_type<FSP>`, etc.)
- `metadata.hpp` — types for querying `information_schema`

### Type System Conventions

- Native C++ types map directly: `int32_t` → INT, `int64_t` → BIGINT, `double` → DOUBLE, `std::string` → TEXT
- `std::optional<T>` → nullable column
- `varchar_type<N>` → VARCHAR(N); `decimal_type<P,S>` → DECIMAL(P,S)
- Strong-typed wrappers (`host_name`, `database_name`, `port_number`, `charset_name`) enforce correct argument passing

### Schema Generation and Validation

Tables are described as C++ structs. `schema_generator.hpp` reflects struct fields via Boost.PFR at compile time. To override the SQL type for a specific field, specialize `field_schema<T, I>`. `mysql_connection::validate_table(T{})` / `validate_table<T>()` checks a live database schema against the C++ definition and returns detailed error messages on mismatch. `validate_database(DB{})` / `validate_database<DB>()` validates all tables in a database schema struct.

### Error Handling

The library uses `std::expected<T, std::string>` throughout — no exceptions in the query execution path.

## Testing

Unit tests use **boost.ut v2.1.0** (fetched via FetchContent). Test files are in `tests/unit/`. Each test file covers a major component: `test_ddl.cpp`, `test_dml.cpp`, `test_dql.cpp`, `test_field_types.cpp`, `test_valid_table.cpp`.

Integration tests require a MySQL instance. Configure via environment variables (see `tests/integration/.env.example`): `DS_MYSQL_TEST_HOST`, `DS_MYSQL_TEST_PORT`, `DS_MYSQL_TEST_DATABASE`, `DS_MYSQL_TEST_USER`, `DS_MYSQL_TEST_PASSWORD`. Docker Compose setup is in `tests/integration/docker/`.

## Useful Scripts

- `scripts/release/release.sh` — bumps version, updates changelog, commits, and tags
- `scripts/act/act-ci.sh` — runs a single CI job locally via `act`
- `scripts/act/act-ci-full.sh` — runs all Linux CI jobs locally

## Dependencies

- **Boost.PFR** — struct reflection (fetched by CMake, header-only)
- **libmysqlclient-dev** — MySQL C client (system package)
- **C++23 compiler** — GCC 13+, Clang 16+, or MSVC 2022+
- **CMake 3.25+**, **Ninja** (recommended)
