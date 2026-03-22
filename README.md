# DSMySQL

A portable C++23 type-safe MySQL query builder and database wrapper.

DSMySQL provides a **header-only** library for building type-safe SQL queries and interacting with MySQL databases using compile-time reflection via Boost.PFR. Define your schema as C++ structs and let the compiler verify column names, types, and nullability.

## Features

- **Header-only** — no compiled library to link against (beyond the MySQL C client)
- **Type-safe queries** — column types are verified at compile time
- **Compile-time schema generation** — `CREATE TABLE` SQL is derived from C++ structs
- **Schema validation** — verify a live database schema against your C++ definitions
- **C++23** — uses `std::expected`, concepts, ranges, and structured bindings
- **Zero-overhead abstractions** — all query building happens at compile time
- **Strong types** — `host_name`, `database_name`, `port_number`, `varchar_field<N>`, etc.
- **Optional support** — `std::optional<T>` maps to nullable SQL columns automatically

## Quick Start

```cpp
#include "ds_mysql/ds_mysql.hpp"

using namespace ds_mysql;

// 1. Define your table as a C++ struct.
//    Each field is declared with COLUMN_FIELD(name, type) — the recommended
//    one-liner that generates a nested tag struct, a type alias, and a member.
struct product {
    COLUMN_FIELD(id,    uint32_t)
    COLUMN_FIELD(sku,   varchar_field<64>)
    COLUMN_FIELD(name,  varchar_field<255>)
    COLUMN_FIELD(price, double)
};

// Three equivalent styles — pick what suits your team:

// a) COLUMN_FIELD macro (recommended — one-liner, guarantees per-table type uniqueness):
//   COLUMN_FIELD(price, double)
//   // expands to: struct price_tag {}; using price = tagged_column_field<price_tag, double>; price price_;

// b) Explicit tag struct — same guarantees as (a), more verbose, useful when the
//    SQL column name differs from the C++ alias (e.g., "order_id" alias → "id"):
//   struct price_tag {};
//   using price = tagged_column_field<price_tag, double>;
//   price price_;
//   // IMPORTANT: tag structs must be nested inside the table struct — not at global scope.

// c) Fixed string literal — column name embedded directly, no tag needed.
//    ⚠ Two tables with identically-named and -typed columns share the same C++ type,
//    so column_field cannot verify which table a column belongs to. This means
//    column_field columns cannot be used as select projections in from<Table>().
//    Prefer COLUMN_FIELD or explicit nested tags for query-building use cases.
//   using price = column_field<"price", double>;
//   price price_;

// **Note:** Tag structs must always be defined as nested classes of the table struct
// (not globally). This ensures each table's columns are distinct types at compile time,
// preventing accidental cross-table column mixing.

// 2. Connect and query
auto db = mysql_database::connect(mysql_config{
    host_name{"127.0.0.1"},
    database_name{"my_db"},
    auth_credentials{user_name{"root"}, user_password{"secret"}},
    port_number{3306},
}).value();

// CREATE TABLE IF NOT EXISTS product (...)
db.execute(create_table<product>().if_not_exists());

// Type-safe SELECT: returns std::expected<std::vector<std::tuple<uint32_t, varchar_field<255>>>, std::string>
auto rows = db.query(select<product::id, product::name>()
                         .from<product>()
                         .where(product::price{9.99})
                         .order_by<product::id>());

for (auto const& [id, name] : *rows) {
    std::println("{}: {}", id, name.to_string());
}
```

## Requirements

- CMake 3.25 or higher
- C++23 compiler (GCC 13+ or Clang 16+)
- MySQL client library (`libmysqlclient-dev`)
- Ninja build system (recommended)
- Docker (for integration tests only)

## Building

```bash
git clone https://github.com/DisciplinedSoftware/DSMySQL.git
cd DSMySQL

cmake --preset release
cmake --build build -j$(nproc)
ctest --preset release
```

### Build Options

| Option | Default | Description |
| ------ | ------- | ----------- |
| `BUILD_TESTING` | ON | Build unit and integration tests |
| `BUILD_INTEGRATION_TESTS` | ON | Build MySQL integration tests |
| `BUILD_EXAMPLES` | ON | Build example programs |
| `DSMYSQL_ENABLE_INSTALL` | `${PROJECT_IS_TOP_LEVEL}` | Enable install rules (headers + generated `version.hpp`) |
| `ENABLE_COVERAGE` | OFF | Enable code coverage instrumentation |
| `SKIP_DOCKER_MANAGEMENT` | OFF | Skip Docker lifecycle (for CI environments) |

```bash
# Unit tests only, skip integration tests
cmake --preset release -DBUILD_INTEGRATION_TESTS=OFF
cmake --build build -j$(nproc)
ctest --preset release

# Debug build
cmake --preset debug
cmake --build build -j$(nproc)
```

## Using as a Dependency

Since DSMySQL is header-only, integrate it with CMake FetchContent:

```cmake
include(FetchContent)
FetchContent_Declare(
    DSMySQL
    GIT_REPOSITORY https://github.com/DisciplinedSoftware/DSMySQL.git
    GIT_TAG main
    GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(DSMySQL)

target_link_libraries(my_target PRIVATE ds_mysql)
```

Optional install/package flow:

```bash
cmake --preset release -DDSMYSQL_ENABLE_INSTALL=ON
cmake --build build -j$(nproc)
cmake --install build --prefix /usr/local
```

```cmake
find_package(ds_mysql CONFIG REQUIRED)
target_link_libraries(my_target PRIVATE ds_mysql::ds_mysql)
```

If you are not using CMake, prefer the release archive (`ds_mysql-v<version>.tar.gz`):
it already contains generated `ds_mysql/version.hpp`. Cloning the raw repository
and copying headers directly will not provide `version.hpp` unless you run CMake
configure first.

## Key API

### Connecting

```cpp
// Explicit parameters
auto db = mysql_database::connect(
    host_name{"127.0.0.1"},
    database_name{"my_db"},
    auth_credentials{user_name{"root"}, user_password{"secret"}},
    port_number{3306}
).value();

// Via mysql_config
mysql_config cfg{host_name{"127.0.0.1"}, database_name{"my_db"},
                 auth_credentials{user_name{"root"}, user_password{""}},
                 port_number{3306}};
auto db = mysql_database::connect(cfg).value();
```

### DDL (CREATE, DROP)

```cpp
db.execute(create_table<product>().if_not_exists());
db.execute(drop_table<product>());
db.execute(create_database<my_db>().if_not_exists());
```

#### CREATE TABLE attributes (fluent API)

```cpp
auto sql = create_table<product>()
               .if_not_exists()
               .engine(Engine::InnoDB)
               .auto_increment(1)
               .default_charset(Charset::utf8mb4)
               .row_format(RowFormat::Dynamic)
               .comment("product catalog")
               .build_sql();

auto ctas = create_table<product_archive>()
                .as(select<product::id, product::name>().from<product>())
                .engine("MyCustomEngine")
                .default_charset("koi8r")
                .build_sql();
```

### DML (INSERT, UPDATE, DELETE)

```cpp
db.execute(insert_into<product>().values(row));
db.execute(update<product>().set(product::price{12.99}).where(product::id{1}));
db.execute(delete_from<product>().where(product::id{1}));
db.execute(truncate<product>());
```

### DQL (SELECT, COUNT, DESCRIBE)

```cpp
// Select specific columns
auto rows = db.query(select<product::id, product::name>().from<product>());

// Select all columns
auto all = db.query(select<product::id, product::sku, product::name, product::price>()
                        .from<product>()
                        .order_by<product::id>());

// Count
auto cnt = db.query(count<product>().where(product::price{9.99}));

// DESCRIBE (schema introspection)
auto cols = db.query(describe<product>());
```

### Schema Validation

```cpp
// Validate a single table
auto r = db.validate_table<product>();
if (!r) std::println(stderr, "Mismatch: {}", r.error());

// Validate all tables in a database schema
auto r2 = db.validate_database<my_db>();
```

### Numeric Precision Overrides

```cpp
template <>
struct ds_mysql::field_schema<order_row, 2> {   // field at index 2
    static constexpr std::string_view name() { return order_row::amount::column_name(); }
    static std::string sql_type() { return sql_type_format::decimal_type(18, 6); }
};
```

## Integration Tests

Integration tests connect to a real MySQL 8.0 instance managed via Docker Compose.

```bash
# Start Docker container, run all tests, stop container (automatic)
ctest --preset release

# Manual container management
cmake --build build --target docker_integration_up
ctest --preset release -R tests_integration_ds_mysql
cmake --build build --target docker_integration_down
```

See [docs/INTEGRATION_TESTS.md](docs/INTEGRATION_TESTS.md) for full details.

## Dependencies

Automatically fetched via CMake FetchContent:

- [Boost.PFR](https://github.com/boostorg/pfr) — compile-time struct reflection (header-only)
- [boost.ut](https://github.com/boost-ext/ut) v2.1.0 — unit testing (test builds only)

System dependencies:

- MySQL client library (`libmysqlclient-dev` on Ubuntu)

## License

See [LICENSE](LICENSE) for details.
