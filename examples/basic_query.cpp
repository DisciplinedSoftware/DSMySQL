// basic_query.cpp — DSMySQL basic usage example.
//
// Demonstrates how to:
//  1. Define a typed table struct using column_field<Tag, T>
//  2. Connect to a MySQL database
//  3. Create the table from the C++ struct
//  4. Insert rows
//  5. Query rows with type-safe SELECT
//  6. Validate the live schema against the C++ struct
//
// Build with:
//   cmake --preset release && cmake --build build -j$(nproc)
//   ./build/bin/examples/example_basic_query
//
// Edit the connection constants below to match your MySQL instance.

#include <optional>
#include <print>
#include <string>

#include "ds_mysql.hpp"

// ===================================================================
// Connection configuration — adjust to match your MySQL instance.
// ===================================================================

namespace {
constexpr auto host = "127.0.0.1";
constexpr auto database = "ds_mysql_example";
constexpr auto user = "root";
constexpr auto password = "";
constexpr unsigned int port = 3306;
}  // namespace

// ===================================================================
// Define a typed table struct.
//
// Each field is a type alias for column_field<"name", ValueType>.
// The string literal is the SQL column name.
// Member variables use a trailing underscore to avoid shadowing the
// column-descriptor aliases.
// ===================================================================

struct product {
    using id          = ds_mysql::column_field<"id",          uint32_t>;
    using sku         = ds_mysql::column_field<"sku",         ds_mysql::varchar_field<64>>;
    using name        = ds_mysql::column_field<"name",        ds_mysql::varchar_field<255>>;
    using price       = ds_mysql::column_field<"price",       double>;
    using stock       = ds_mysql::column_field<"stock",       uint32_t>;
    using description = ds_mysql::column_field<"description", std::optional<ds_mysql::varchar_field<512>>>;
    using created_at  = ds_mysql::column_field<"created_at",  ds_mysql::sql_datetime>;

    id          id_;
    sku         sku_;
    name        name_;
    price       price_;
    stock       stock_;
    description description_;
    created_at  created_at_;
};

// ===================================================================
// Main
// ===================================================================

int main() {
    // --- Connect ---
    auto db_result = ds_mysql::mysql_database::connect(ds_mysql::mysql_config{
        ds_mysql::host_name{host},
        ds_mysql::database_name{database},
        ds_mysql::auth_credentials{ds_mysql::user_name{user}, ds_mysql::user_password{password}},
        ds_mysql::port_number{port},
    });
    if (!db_result) {
        std::println(stderr, "Connection failed: {}", db_result.error());
        return 1;
    }
    auto& db = *db_result;
    std::println("Connected to {}:{}", host, port);

    // --- Create table ---
    if (auto r = db.execute(ds_mysql::create_table<product>().if_not_exists()); !r) {
        std::println(stderr, "CREATE TABLE failed: {}", r.error());
        return 1;
    }
    std::println("Table 'product' ready.");

    // --- Validate schema ---
    if (auto r = db.validate_table<product>(); !r) {
        std::println(stderr, "Schema mismatch: {}", r.error());
        return 1;
    }
    std::println("Schema validated.");

    // --- Insert a row ---
    product row;
    row.id_ = product::id{0};  // AUTO_INCREMENT — value ignored on INSERT
    row.sku_ = product::sku{"WIDGET-001"};
    row.name_ = product::name{"Blue Widget"};
    row.price_ = product::price{9.99};
    row.stock_ = product::stock{42};
    row.description_ = product::description{std::nullopt};
    row.created_at_ = product::created_at{ds_mysql::sql_now};

    if (auto r = db.execute(ds_mysql::insert_into<product>().values(row)); !r) {
        std::println(stderr, "INSERT failed: {}", r.error());
        return 1;
    }
    std::println("Inserted one product.");

    // --- Query all products ---
    auto rows = db.query(
        ds_mysql::select<product::id, product::sku, product::name, product::price, product::stock>().from<product>());
    if (!rows) {
        std::println(stderr, "SELECT failed: {}", rows.error());
        return 1;
    }

    std::println("\nProducts ({} row(s)):", rows->size());
    for (auto const& [id, sku, name, price, stock] : *rows) {
        std::println("  [{:>3}]  {:<20}  {:>8.2f}  stock={}", id, std::string_view{name}, price, stock);
    }

    // --- Count rows ---
    auto cnt = db.query(ds_mysql::count<product>());
    if (cnt && !cnt->empty()) {
        std::println("\nTotal products: {}", std::get<0>(cnt->front()));
    }

    return 0;
}
