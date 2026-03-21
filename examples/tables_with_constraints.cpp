// tables_with_constraints.cpp — DSMySQL table constraints example.
//
// Demonstrates how to:
//  1. Define table-level PRIMARY KEY, secondary indexes, and other constraints
//  2. Customize CREATE TABLE output with table_constraints<T> trait
//  3. Disable inline implicit primary key with table_inline_primary_key<T>
//  4. Use table_constraint helpers: primary_key, key, unique_key, check, etc.
//
// Build with:
//   cmake --preset release && cmake --build build -j$(nproc)
//   ./build/bin/examples/example_tables_with_constraints

#include <optional>
#include <print>
#include <string>

#include "ds_mysql/ds_mysql.hpp"

// ===================================================================
// Define a 'symbol' table matching the original user requirement
// ===================================================================

struct symbol {
    COLUMN_FIELD(id, int32_t)
    COLUMN_FIELD(exchange_id, std::optional<int32_t>)
    COLUMN_FIELD(ticker, ds_mysql::varchar_field<32>)
    COLUMN_FIELD(instrument, ds_mysql::varchar_field<64>)
    COLUMN_FIELD(name, std::optional<ds_mysql::varchar_field<255>>)
    COLUMN_FIELD(sector, std::optional<ds_mysql::varchar_field<255>>)
    COLUMN_FIELD(currency, std::optional<ds_mysql::varchar_field<32>>)
    COLUMN_FIELD(created_date, ds_mysql::sql_datetime)
    COLUMN_FIELD(last_updated_date, ds_mysql::sql_datetime)
};

// ===================================================================
// Define an 'order_detail' table with UNIQUE constraint and CHECK
// ===================================================================

struct order_detail {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(order_id, uint32_t)
    COLUMN_FIELD(line_number, uint32_t)
    COLUMN_FIELD(quantity, uint32_t)
    COLUMN_FIELD(unit_price, double)
};

// ===================================================================
// Customize symbol table: define table-level PRIMARY KEY and secondary
// indexes using the table_constraints<T> trait.
// ===================================================================

template <>
struct ds_mysql::table_inline_primary_key<symbol> {
    // Disable the default inline PRIMARY KEY AUTO_INCREMENT on first column
    static constexpr bool value = false;
};

template <>
struct ds_mysql::table_constraints<symbol> {
    static std::vector<std::string> get() {
        return {
            // Define table-level PRIMARY KEY(id)
            ds_mysql::table_constraint::primary_key<symbol::id>(),
            
            // Define secondary index on exchange_id
            ds_mysql::table_constraint::key<symbol::exchange_id>("index_exchange_id"),
            
            // Optional: add a unique index on ticker
            // ds_mysql::table_constraint::unique_key<symbol::ticker>("uq_ticker"),
        };
    }
};

// ===================================================================
// Customize order_detail table: define UNIQUE constraint and CHECK constraint
// ===================================================================

template <>
struct ds_mysql::table_inline_primary_key<order_detail> {
    static constexpr bool value = false;
};

template <>
struct ds_mysql::table_constraints<order_detail> {
    static std::vector<std::string> get() {
        return {
            // Define table-level PRIMARY KEY(id)
            ds_mysql::table_constraint::primary_key<order_detail::id>(),
            
            // Define unique constraint on (order_id, line_number)
            ds_mysql::table_constraint::unique_key<order_detail::order_id, order_detail::line_number>("uq_order_line"),
            
            // Define CHECK constraint: quantity > 0
            ds_mysql::table_constraint::check("quantity > 0", "chk_positive_quantity"),
            
            // Define CHECK constraint: unit_price >= 0
            ds_mysql::table_constraint::check("unit_price >= 0", "chk_non_negative_price"),
        };
    }
};

// ===================================================================
// Main — demonstrate SQL generation
// ===================================================================

int main() {
    using namespace ds_mysql;

    std::println("===============================================");
    std::println("Symbol table with PRIMARY KEY and secondary index");
    std::println("===============================================");
    std::println("{}", create_table<symbol>()
                           .engine(Engine::InnoDB)
                           .auto_increment(1)
                           .default_charset(Charset::utf8)
                           .build_sql());

    std::println("\n==================================================");
    std::println("Order detail table with UNIQUE and CHECK constraints");
    std::println("==================================================");
    std::println("{}", create_table<order_detail>()
                           .engine(Engine::InnoDB)
                           .auto_increment(1)
                           .default_charset(Charset::utf8)
                           .build_sql());

    return 0;
}
