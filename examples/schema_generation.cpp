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

#include "ds_mysql/column_field.hpp"
#include "ds_mysql/mysql_database.hpp"
#include "ds_mysql/schema_generator.hpp"
#include "ds_mysql/sql.hpp"
#include "ds_mysql/sql_identifiers.hpp"
#include "ds_mysql/sql_temporal.hpp"
#include "ds_mysql/varchar_field.hpp"

// ===================================================================
// Define a 'user' table
// ===================================================================

struct user {
    struct id_tag {};
    struct username_tag {};
    struct email_tag {};
    struct is_active_tag {};
    struct created_at_tag {};

    using id = ds_mysql::column_field<id_tag, uint32_t>;
    using username = ds_mysql::column_field<username_tag, ds_mysql::varchar_field<64>>;
    using email = ds_mysql::column_field<email_tag, ds_mysql::varchar_field<255>>;
    using is_active = ds_mysql::column_field<is_active_tag, bool>;
    using created_at = ds_mysql::column_field<created_at_tag, ds_mysql::sql_datetime>;

    id         id_;
    username   username_;
    email      email_;
    is_active  is_active_;
    created_at created_at_;
};

// ===================================================================
// Define an 'order_row' table with a numeric precision override
// ===================================================================

struct order_row {
    struct order_id_tag {};          using id         = ds_mysql::column_field<order_id_tag,          uint32_t>;
    struct user_id_tag {};           using user_id    = ds_mysql::column_field<user_id_tag,           uint32_t>;
    struct amount_tag {};            using amount     = ds_mysql::column_field<amount_tag,            double>;
    struct fee_tag {};               using fee        = ds_mysql::column_field<fee_tag,               std::optional<double>>;
    struct status_tag {};            using status     = ds_mysql::column_field<status_tag,            ds_mysql::varchar_field<32>>;
    struct order_created_at_tag {};  using created_at = ds_mysql::column_field<order_created_at_tag, ds_mysql::sql_datetime>;

    id         id_;
    user_id    user_id_;
    amount     amount_;
    fee        fee_;
    status     status_;
    created_at created_at_;
};

// Override 'amount' (index 2) to use DECIMAL(18,6) instead of DOUBLE
template <>
struct ds_mysql::field_schema<order_row, 2> {
    static constexpr std::string_view name() { return order_row::amount::column_name(); }
    static std::string sql_type() { return ds_mysql::sql_type_format::decimal_type(18, 6); }
};

// Override 'fee' (index 3) to use DECIMAL(18,6)
template <>
struct ds_mysql::field_schema<order_row, 3> {
    static constexpr std::string_view name() { return order_row::fee::column_name(); }
    static std::string sql_type() { return ds_mysql::sql_type_format::decimal_type(18, 6); }
};

// ===================================================================
// Bundle both tables into a database schema
// ===================================================================

struct example_db : ds_mysql::database_schema {
    using tables = std::tuple<user, order_row>;
};

template <>
struct ds_mysql::database_name_for<example_db> {
    static constexpr std::string_view value() noexcept { return "example_db"; }
};

// ===================================================================
// Main — print the generated DDL
// ===================================================================

int main() {
    std::println("=== Generated CREATE TABLE SQL ===\n");

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

