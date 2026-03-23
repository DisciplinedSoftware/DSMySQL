// schema_generation.cpp — DSMySQL schema generation example.
//
// Demonstrates how to:
//  1. Define a multi-table database schema
//  2. Generate CREATE TABLE SQL at compile time
//  3. Override SQL types for numeric precision
//  4. Build type-safe SELECT queries
//
// Build with:
//   cmake --preset release && cmake --build build -j$(nproc)
//   ./build/bin/examples/example_schema_generation

#include <optional>
#include <print>
#include <string>

#include "ds_mysql/ds_mysql.hpp"

// ===================================================================
// Define a 'user' table
// ===================================================================

struct user {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(username, ds_mysql::varchar_type<64>)
    COLUMN_FIELD(email, ds_mysql::varchar_type<255>)
    COLUMN_FIELD(is_active, bool)
    COLUMN_FIELD(created_at, ds_mysql::datetime_type<>)
};

// ===================================================================
// Define an 'order_row' table with a numeric precision override
// ===================================================================

struct order_row {
    struct order_id_tag {};
    using id = ds_mysql::tagged_column_field<order_id_tag, uint32_t>;
    id id_;

    COLUMN_FIELD(user_id, uint32_t)
    COLUMN_FIELD(amount, double)
    COLUMN_FIELD(fee, std::optional<double>)
    COLUMN_FIELD(status, ds_mysql::varchar_type<32>)

    struct order_created_at_tag {};
    using created_at = ds_mysql::tagged_column_field<order_created_at_tag, ds_mysql::datetime_type<>>;
    created_at created_at_;
};

// Override 'amount' (index 2) to use DECIMAL(18,6) instead of DOUBLE
template <>
struct ds_mysql::field_schema<order_row, 2> {
    static constexpr std::string_view name() {
        return order_row::amount::column_name();
    }
    static std::string sql_type() {
        return ds_mysql::sql_type_format::decimal_type(18, 6);
    }
};

// Override 'fee' (index 3) to use DECIMAL(18,6)
template <>
struct ds_mysql::field_schema<order_row, 3> {
    static constexpr std::string_view name() {
        return order_row::fee::column_name();
    }
    static std::string sql_type() {
        return ds_mysql::sql_type_format::decimal_type(18, 6);
    }
};

// ===================================================================
// Bundle both tables into a database schema
// ===================================================================

struct example_db : ds_mysql::database_schema {
    using tables = std::tuple<user, order_row>;
};

template <>
struct ds_mysql::database_name_for<example_db> {
    static /*constexpr*/ std::string_view value() noexcept {
        return "specialized_db_name";
    }
};

// ===================================================================
// Main — print the generated DDL
// ===================================================================

int main() {
    std::println("=== Generated CREATE TABLE SQL ===\n");

    std::println("-- database:");
    std::println("{}\n", ds_mysql::create_database<example_db>().if_not_exists().build_sql());

    // Generate CREATE TABLE SQL for each table
    std::println("-- user table:");
    std::println("{}\n", ds_mysql::create_table<user>().if_not_exists().build_sql());

    std::println("-- order_row table:");
    std::println("{}\n", ds_mysql::create_table<order_row>().if_not_exists().build_sql());

    // Show SELECT query SQL
    std::println("=== SELECT query SQL ===\n");
    auto q = ds_mysql::select<user::id, user::username, user::email>()
                 .from<user>()
                 .where(ds_mysql::equal<user::is_active>(true))
                 .limit(10);
    std::println("{}\n", q.build_sql());

    std::println("Schema generation complete — no database connection required!");
    return 0;
}
