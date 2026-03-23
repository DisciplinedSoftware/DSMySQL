# DSMySQL Development Guide

## Project Structure

```text
DSMySQL/
├── lib/
│   ├── include/
│   │   └── ds_mysql/           # All public headers (header-only)
│   └── CMakeLists.txt
├── tests/
│   ├── unit/                   # boost.ut unit tests
│   └── integration/            # MySQL integration tests
│       ├── mysql/              # Test source files
│       ├── docker/             # Docker Compose + init scripts
│       └── cmake/              # DockerIntegration.cmake module
├── examples/                   # Example programs
├── docs/                       # Documentation
├── CMakeLists.txt
└── CMakePresets.json
```

## Adding New Headers

DSMySQL is a header-only library. All functionality lives in `lib/include/ds_mysql/`.

1. Create a new header under `lib/include/ds_mysql/`
2. Use `#pragma once`
3. Put all code in the `ds_mysql` namespace
4. Include it transitively through `ds_mysql/database.hpp`, `ds_mysql/metadata.hpp`, `ds_mysql/sql_ddl.hpp`, `ds_mysql/sql_dml.hpp`, or `ds_mysql/sql_dql.hpp` if appropriate (these are pulled in by the `ds_mysql/ds_mysql.hpp` umbrella header)

## Adding Unit Tests

1. Add a new `.cpp` file to `tests/unit/`
2. Register it in `tests/unit/CMakeLists.txt`
3. Use boost.ut syntax:

```cpp
#include <boost/ut.hpp>
#include "ds_mysql.hpp"

using namespace boost::ut;
using namespace ds_mysql;

suite<"My Feature Suite"> my_suite = [] {
    "some behaviour"_test = [] {
        expect(that % result == expected);
    };
};
```

## Adding Integration Tests

1. Add test cases to `tests/integration/mysql/test_query_builder.cpp`
2. Use `ds_mysql::mysql_config_from_env()` to read connection config

## SQL Type Overrides

For fields needing explicit SQL numeric formatting, specialise `field_schema<T, Index>`:

```cpp
struct quote_row {
    using id  = column_field<struct id_tag,  uint32_t>;
    using px  = column_field<struct px_tag,  float>;
    using vol = column_field<struct vol_tag, double>;

    id  id_;
    px  px_;
    vol vol_;
};

template <>
struct ds_mysql::field_schema<quote_row, 1> {
    static constexpr std::string_view name() { return quote_row::px::column_name(); }
    static std::string sql_type() { return sql_type_format::float_type(12, 4); }
};

template <>
struct ds_mysql::field_schema<quote_row, 2> {
    static constexpr std::string_view name() { return quote_row::vol::column_name(); }
    static std::string sql_type() { return sql_type_format::double_type(16, 8); }
};
```

Available helpers:

- `sql_type_format::float_type()` / `float_type(p)` / `float_type(p, s)`
- `sql_type_format::double_type()` / `double_type(p)` / `double_type(p, s)`
- `sql_type_format::decimal_type()` / `decimal_type(p)` / `decimal_type(p, s)`
- `sql_type_format::datetime_type()` / `datetime_type(fsp)` — `DATETIME` / `DATETIME(fsp)`
- `sql_type_format::timestamp_type()` / `timestamp_type(fsp)` — `TIMESTAMP` / `TIMESTAMP(fsp)`
- `sql_type_format::time_type()` / `time_type(fsp)` — `TIME` / `TIME(fsp)`

## Useful Commands

```bash
# Build (release)
cmake --preset release && cmake --build build/release -j$(nproc)

# Build (debug)
cmake --preset debug && cmake --build build/debug -j$(nproc)

# Run all tests
ctest --preset release

# Unit tests only (fast, no Docker needed)
ctest --preset release -R tests_unit_ds_mysql

# Integration tests only
ctest --preset release -R tests_integration_ds_mysql
```
