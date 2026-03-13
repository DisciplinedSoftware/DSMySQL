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
// Set these environment variables to point at a real MySQL instance:
//   DS_MYSQL_TEST_HOST       (e.g. 127.0.0.1)
//   DS_MYSQL_TEST_PORT       (e.g. 3307)
//   DS_MYSQL_TEST_DATABASE   (e.g. ds_mysql_example)
//   DS_MYSQL_TEST_USER
//   DS_MYSQL_TEST_PASSWORD

#include <cstdlib>
#include <iostream>
#include <optional>
#include <print>
#include <string>

#include "ds_mysql.hpp"

// ===================================================================
// Define a typed table struct.
//
// Each field is a type alias for column_field<Tag, ValueType>.
// The tag name drives the SQL column name: id_tag → "id".
// Member variables use a trailing underscore to avoid shadowing the
// column-descriptor aliases.
// ===================================================================

struct product {
    struct id_tag {};
    struct sku_tag {};
    struct name_tag {};
    struct price_tag {};
    struct stock_tag {};
    struct description_tag {};
    struct created_at_tag {};

    using id = ds_mysql::column_field<id_tag, uint32_t>;
    using sku = ds_mysql::column_field<sku_tag, ds_mysql::varchar_field<64>>;
    using name = ds_mysql::column_field<name_tag, ds_mysql::varchar_field<255>>;
    using price = ds_mysql::column_field<price_tag, double>;
    using stock = ds_mysql::column_field<stock_tag, uint32_t>;
    using description = ds_mysql::column_field<description_tag, std::optional<ds_mysql::varchar_field<512>>>;
    using created_at = ds_mysql::column_field<created_at_tag, ds_mysql::sql_datetime>;

    id          id_;
    sku         sku_;
    name        name_;
    price       price_;
    stock       stock_;
    description description_;
    created_at  created_at_;
};

// ===================================================================
// Read connection config from environment variables.
// ===================================================================

[[nodiscard]] std::optional<ds_mysql::mysql_config> config_from_env() {
    auto get = [](char const* name, char const* fallback = "") -> std::string {
        char const* v = std::getenv(name);
        return v ? v : fallback;
    };

    const std::string host     = get("DS_MYSQL_TEST_HOST");
    const std::string database = get("DS_MYSQL_TEST_DATABASE", "ds_mysql_example");
    const std::string user     = get("DS_MYSQL_TEST_USER");
    const std::string password = get("DS_MYSQL_TEST_PASSWORD");

    if (host.empty() || user.empty()) {
        return std::nullopt;
    }

    const unsigned int port = [&] {
        char const* p = std::getenv("DS_MYSQL_TEST_PORT");
        return p ? static_cast<unsigned int>(std::stoul(p))
                 : ds_mysql::default_mysql_port.to_unsigned_int();
    }();

    return ds_mysql::mysql_config{
        ds_mysql::host_name{host},
        ds_mysql::database_name{database},
        ds_mysql::auth_credentials{ds_mysql::user_name{user}, ds_mysql::user_password{password}},
        ds_mysql::port_number{port},
    };
}

// ===================================================================
// Main
// ===================================================================

int main() {
    const auto config = config_from_env();
    if (!config) {
        std::println(stderr,
            "Set DS_MYSQL_TEST_HOST, DS_MYSQL_TEST_USER, DS_MYSQL_TEST_PASSWORD "
            "to run this example.");
        return 1;
    }

    // --- Connect ---
    auto db_result = ds_mysql::mysql_database::connect(*config);
    if (!db_result) {
        std::println(stderr, "Connection failed: {}", db_result.error());
        return 1;
    }
    auto& db = *db_result;
    std::println("Connected to {}:{}", config->host().to_string(), config->port().to_unsigned_int());

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
    row.id_          = product::id{0};      // AUTO_INCREMENT — value ignored on INSERT
    row.sku_         = product::sku{"WIDGET-001"};
    row.name_        = product::name{"Blue Widget"};
    row.price_       = product::price{9.99};
    row.stock_       = product::stock{42};
    row.description_ = product::description{std::nullopt};
    row.created_at_  = product::created_at{ds_mysql::sql_now};

    if (auto r = db.execute(ds_mysql::insert_into<product>().values(row)); !r) {
        std::println(stderr, "INSERT failed: {}", r.error());
        return 1;
    }
    std::println("Inserted one product.");

    // --- Query all products ---
    auto rows = db.query(
        ds_mysql::select<product::id, product::sku, product::name, product::price, product::stock>()
            .from<product>()
    );
    if (!rows) {
        std::println(stderr, "SELECT failed: {}", rows.error());
        return 1;
    }

    std::println("\nProducts ({} row(s)):", rows->size());
    for (auto const& [id, sku, name, price, stock] : *rows) {
        std::println("  [{:>3}]  {:<20}  {:>8.2f}  stock={}",
                     id, std::string_view{name}, price, stock);
    }

    // --- Count rows ---
    auto cnt = db.query(ds_mysql::count<product>());
    if (cnt && !cnt->empty()) {
        std::println("\nTotal products: {}", std::get<0>(cnt->front()));
    }

    return 0;
}

