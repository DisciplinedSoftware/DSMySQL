#include <boost/ut.hpp>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

#include "ds_mysql/sql_ddl.hpp"
#include "ds_mysql/sql_dql.hpp"
#include "ds_mysql/sql_text.hpp"

using namespace boost::ut;
using namespace ds_mysql;
using namespace std::string_literals;

// ===================================================================
// Test fixtures
// ===================================================================

namespace {
struct test_table {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(name, varchar_type<255>)
    COLUMN_FIELD(tag, std::optional<varchar_type<64>>)
};

struct new_table {
    COLUMN_FIELD(id, uint32_t)
};

struct numeric_format_table {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(price, float)
    COLUMN_FIELD(ratio, double)
    COLUMN_FIELD(amount, std::optional<double>)
};

struct formatted_numeric_column_table {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(value0, float_type<>)
    COLUMN_FIELD(value1, float_type<12, 4>)
    COLUMN_FIELD(value2a, double_type<12>)
    COLUMN_FIELD(value2, double_type<12, 4>)
    COLUMN_FIELD(value3a, decimal_type<>)
    COLUMN_FIELD(value3b, decimal_type<12>)
    COLUMN_FIELD(value3, decimal_type<12, 4>)
    COLUMN_FIELD(value4, std::optional<decimal_type<12, 4>>)
};

struct integer_column_table {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(count, int_type<>)
    COLUMN_FIELD(count_w, int_type<11>)
    COLUMN_FIELD(flags, int_unsigned_type<>)
    COLUMN_FIELD(flags_w, int_unsigned_type<10>)
    COLUMN_FIELD(big, bigint_type<>)
    COLUMN_FIELD(big_w, bigint_type<20>)
    COLUMN_FIELD(big_flags, bigint_unsigned_type<>)
    COLUMN_FIELD(big_flags_w, bigint_unsigned_type<20>)
    COLUMN_FIELD(opt_count, std::optional<int_type<>>)
    COLUMN_FIELD(opt_big, std::optional<bigint_unsigned_type<>>)
};

struct test_db : ds_mysql::database_schema {
    struct symbol {
        COLUMN_FIELD(id, uint32_t)
    };
};

struct schema_bootstrap_db : ds_mysql::database_schema {
    struct account {
        COLUMN_FIELD(id, uint32_t)
    };

    struct trade {
        COLUMN_FIELD(id, uint32_t)
    };

    using tables = std::tuple<account, trade>;
};

struct custom_named_db : ds_mysql::database_schema {};

struct renamed_table {
    struct renamed_id_tag {};
    using id = tagged_column_field<renamed_id_tag, uint32_t>;
    id id_;
};

struct temporal_table {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(created_at, std::chrono::system_clock::time_point)
    COLUMN_FIELD(updated_at, timestamp_type<>)
};

struct time_table {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(start_time, time_type<>)
    COLUMN_FIELD(end_time, std::optional<time_type<>>)
};

struct fsp_temporal_table {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(created_at, datetime_type<6>)
    COLUMN_FIELD(updated_at, timestamp_type<3>)
    COLUMN_FIELD(duration, time_type<4>)
    COLUMN_FIELD(opt_created_at, std::optional<datetime_type<3>>)
    COLUMN_FIELD(opt_duration, std::optional<time_type<6>>)
};

struct tagged_temporal_alias_table {
    struct id_tag {};
    using id = tagged_column_field<id_tag, uint32_t>;
    id id_;

    struct created_at_tag {};
    using created_at = tagged_column_field<created_at_tag, datetime_type_default>;
    created_at created_at_;

    struct updated_at_tag {};
    using updated_at = tagged_column_field<updated_at_tag, timestamp_type_default>;
    updated_at updated_at_;

    struct duration_tag {};
    using duration = tagged_column_field<duration_tag, time_type_default>;
    duration duration_;
};

struct tagged_numeric_alias_table {
    struct id_tag {};
    using id = tagged_column_field<id_tag, uint32_t>;
    id id_;

    struct value0_tag {};
    using value0 = tagged_column_field<value0_tag, float_type_default>;
    value0 value0_;

    struct value1_tag {};
    using value1 = tagged_column_field<value1_tag, double_type_default>;
    value1 value1_;

    struct value2_tag {};
    using value2 = tagged_column_field<value2_tag, decimal_type_default>;
    value2 value2_;
};

struct tagged_integer_alias_table {
    struct id_tag {};
    using id = tagged_column_field<id_tag, uint32_t>;
    id id_;

    struct count_tag {};
    using count = tagged_column_field<count_tag, int_type_default>;
    count count_;

    struct flags_tag {};
    using flags = tagged_column_field<flags_tag, int_unsigned_type_default>;
    flags flags_;

    struct big_tag {};
    using big = tagged_column_field<big_tag, bigint_type_default>;
    big big_;

    struct big_flags_tag {};
    using big_flags = tagged_column_field<big_flags_tag, bigint_unsigned_type_default>;
    big_flags big_flags_;
};

struct symbol_with_indexes {
    COLUMN_FIELD(id, int32_t)
    COLUMN_FIELD(exchange_id, std::optional<int32_t>)
    COLUMN_FIELD(ticker, varchar_type<32>)
    COLUMN_FIELD(instrument, varchar_type<64>)
    COLUMN_FIELD(name, std::optional<varchar_type<255>>)
    COLUMN_FIELD(sector, std::optional<varchar_type<255>>)
    COLUMN_FIELD(currency, std::optional<varchar_type<32>>)
    COLUMN_FIELD(created_date, std::chrono::system_clock::time_point)
    COLUMN_FIELD(last_updated_date, std::chrono::system_clock::time_point)
};

struct audit_log {
    COLUMN_FIELD(id, uint32_t, column_attr::auto_increment)
    COLUMN_FIELD(event_type, varchar_type<64>)
    COLUMN_FIELD(user_id, uint32_t, column_attr::comment<"User who triggered the event">)
    COLUMN_FIELD(description, varchar_type<255>)
    COLUMN_FIELD(created_at, std::chrono::system_clock::time_point, column_attr::default_current_timestamp,
                 column_attr::on_update_current_timestamp)
};

struct table_with_attrs {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(name, varchar_type<255>)
};
}  // namespace

template <>
struct ds_mysql::field_schema<numeric_format_table, 1> {
    static constexpr std::string_view name() {
        return numeric_format_table::price::column_name();
    }

    static std::string sql_type() {
        return sql_type_format::float_type(12, 4);
    }
};

template <>
struct ds_mysql::field_schema<numeric_format_table, 2> {
    static constexpr std::string_view name() {
        return numeric_format_table::ratio::column_name();
    }

    static std::string sql_type() {
        return sql_type_format::double_type(16, 8);
    }
};

template <>
struct ds_mysql::field_schema<numeric_format_table, 3> {
    static constexpr std::string_view name() {
        return numeric_format_table::amount::column_name();
    }

    static std::string sql_type() {
        return sql_type_format::decimal_type(18, 6);
    }
};

template <>
struct ds_mysql::database_name_for<custom_named_db> {
    static constexpr std::string_view value() noexcept {
        return "my_custom_db";
    }
};

template <>
struct ds_mysql::table_inline_primary_key<symbol_with_indexes> {
    static constexpr bool value = false;
};

template <>
struct ds_mysql::table_constraints<symbol_with_indexes> {
    static std::vector<std::string> get() {
        return {
            table_constraint::primary_key(symbol_with_indexes::id{}),
            table_constraint::key(index_id<"index_exchange_id">{}, symbol_with_indexes::exchange_id{}),
        };
    }
};

template <>
struct ds_mysql::table_attributes<table_with_attrs> {
    static create_table_option get() {
        return create_table_option{}.engine(Engine::InnoDB).default_charset(Charset::utf8mb4);
    }
};

// Disable inline primary key for audit_log since we're using column attributes
template <>
struct ds_mysql::table_inline_primary_key<audit_log> {
    static constexpr bool value = false;
};

// Define table constraints for audit_log
template <>
struct ds_mysql::table_constraints<audit_log> {
    static std::vector<std::string> get() {
        return {
            table_constraint::primary_key(audit_log::id{}),
        };
    }
};

// ===================================================================
// DDL — CREATE TABLE, DROP TABLE, TEMPORARY TABLE, chaining
// ===================================================================

suite<"DDL"> ddl_suite = [] {
    "sql_type_format helpers - format float/double/decimal types"_test = [] {
        expect(sql_type_format::float_type() == "FLOAT"s);
        expect(sql_type_format::float_type(10) == "FLOAT(10)"s);
        expect(sql_type_format::float_type(10, 4) == "FLOAT(10,4)"s);

        expect(sql_type_format::double_type() == "DOUBLE"s);
        expect(sql_type_format::double_type(12) == "DOUBLE(12)"s);
        expect(sql_type_format::double_type(12, 6) == "DOUBLE(12,6)"s);

        expect(sql_type_format::decimal_type() == "DECIMAL"s);
        expect(sql_type_format::decimal_type(18) == "DECIMAL(18)"s);
        expect(sql_type_format::decimal_type(18, 6) == "DECIMAL(18,6)"s);

        expect(sql_type_format::datetime_type() == "DATETIME"s);
        expect(sql_type_format::datetime_type(3) == "DATETIME(3)"s);

        expect(sql_type_format::timestamp_type() == "TIMESTAMP"s);
        expect(sql_type_format::timestamp_type(6) == "TIMESTAMP(6)"s);

        expect(sql_type_format::time_type() == "TIME"s);
        expect(sql_type_format::time_type(3) == "TIME(3)"s);
        expect(sql_type_format::time_type(6) == "TIME(6)"s);
    };

    "sql_type_format helpers - format int/bigint types"_test = [] {
        expect(sql_type_format::int_type() == "INT"s);
        expect(sql_type_format::int_type(11) == "INT(11)"s);

        expect(sql_type_format::int_unsigned_type() == "INT UNSIGNED"s);
        expect(sql_type_format::int_unsigned_type(10) == "INT(10) UNSIGNED"s);

        expect(sql_type_format::bigint_type() == "BIGINT"s);
        expect(sql_type_format::bigint_type(20) == "BIGINT(20)"s);

        expect(sql_type_format::bigint_unsigned_type() == "BIGINT UNSIGNED"s);
        expect(sql_type_format::bigint_unsigned_type(20) == "BIGINT(20) UNSIGNED"s);
    };

    "sql_type_for - integer wrapper types embed display width in type string"_test = [] {
        expect(sql_type_for<int_type<>>() == "INT"s);
        expect(sql_type_for<int_type<11>>() == "INT(11)"s);

        expect(sql_type_for<int_unsigned_type<>>() == "INT UNSIGNED"s);
        expect(sql_type_for<int_unsigned_type<10>>() == "INT(10) UNSIGNED"s);

        expect(sql_type_for<bigint_type<>>() == "BIGINT"s);
        expect(sql_type_for<bigint_type<20>>() == "BIGINT(20)"s);

        expect(sql_type_for<bigint_unsigned_type<>>() == "BIGINT UNSIGNED"s);
        expect(sql_type_for<bigint_unsigned_type<20>>() == "BIGINT(20) UNSIGNED"s);

        // optional wrappers preserve the display width
        expect(sql_type_for<std::optional<int_type<11>>>() == "INT(11)"s);
        expect(sql_type_for<std::optional<bigint_unsigned_type<20>>>() == "BIGINT(20) UNSIGNED"s);
    };

    "create_table with integer wrapper types - emits INT/BIGINT definitions"_test = [] {
        auto const sql = create_table(integer_column_table{}).build_sql();
        expect(sql ==
               "CREATE TABLE integer_column_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    count INT NOT NULL,\n"
               "    count_w INT(11) NOT NULL,\n"
               "    flags INT UNSIGNED NOT NULL,\n"
               "    flags_w INT(10) UNSIGNED NOT NULL,\n"
               "    big BIGINT NOT NULL,\n"
               "    big_w BIGINT(20) NOT NULL,\n"
               "    big_flags BIGINT UNSIGNED NOT NULL,\n"
               "    big_flags_w BIGINT(20) UNSIGNED NOT NULL,\n"
               "    opt_count INT,\n"
               "    opt_big BIGINT UNSIGNED\n"
               ");\n"s)
            << sql;
    };

    "tagged_column_field with integer default aliases - emits INT/BIGINT"_test = [] {
        expect(sql_type_for<int_type_default>() == "INT"s);
        expect(sql_type_for<int_unsigned_type_default>() == "INT UNSIGNED"s);
        expect(sql_type_for<bigint_type_default>() == "BIGINT"s);
        expect(sql_type_for<bigint_unsigned_type_default>() == "BIGINT UNSIGNED"s);

        auto const sql = create_table(tagged_integer_alias_table{}).build_sql();
        expect(sql ==
               "CREATE TABLE tagged_integer_alias_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    count INT NOT NULL,\n"
               "    flags INT UNSIGNED NOT NULL,\n"
               "    big BIGINT NOT NULL,\n"
               "    big_flags BIGINT UNSIGNED NOT NULL\n"
               ");\n"s)
            << sql;
    };

    "create_table temporal types - emits DATETIME and TIMESTAMP definitions"_test = [] {
        auto const sql = create_table(temporal_table{}).build_sql();
        expect(sql ==
               "CREATE TABLE temporal_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    created_at DATETIME NOT NULL,\n"
               "    updated_at TIMESTAMP NOT NULL\n"
               ");\n"s)
            << sql;
    };

    "create_table time type - emits TIME definitions"_test = [] {
        auto const sql = create_table(time_table{}).build_sql();
        expect(sql ==
               "CREATE TABLE time_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    start_time TIME NOT NULL,\n"
               "    end_time TIME\n"
               ");\n"s)
            << sql;
    };

    "sql_type_for - templated temporal types embed FSP in the type string"_test = [] {
        expect(sql_type_for<datetime_type<0>>() == "DATETIME"s);
        expect(sql_type_for<datetime_type<3>>() == "DATETIME(3)"s);
        expect(sql_type_for<datetime_type<6>>() == "DATETIME(6)"s);

        expect(sql_type_for<timestamp_type<0>>() == "TIMESTAMP"s);
        expect(sql_type_for<timestamp_type<1>>() == "TIMESTAMP(1)"s);
        expect(sql_type_for<timestamp_type<6>>() == "TIMESTAMP(6)"s);

        expect(sql_type_for<time_type<0>>() == "TIME"s);
        expect(sql_type_for<time_type<3>>() == "TIME(3)"s);
        expect(sql_type_for<time_type<6>>() == "TIME(6)"s);

        // std::optional wrappers preserve the FSP
        expect(sql_type_for<std::optional<datetime_type<6>>>() == "DATETIME(6)"s);
        expect(sql_type_for<std::optional<time_type<4>>>() == "TIME(4)"s);
    };

    "create_table with templated temporal FSP columns - emits types with precision"_test = [] {
        auto const sql = create_table(fsp_temporal_table{}).build_sql();
        expect(sql ==
               "CREATE TABLE fsp_temporal_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    created_at DATETIME(6) NOT NULL,\n"
               "    updated_at TIMESTAMP(3) NOT NULL,\n"
               "    duration TIME(4) NOT NULL,\n"
               "    opt_created_at DATETIME(3),\n"
               "    opt_duration TIME(6)\n"
               ");\n"s)
            << sql;
    };

    "tagged_column_field with temporal default aliases - emits DATETIME/TIMESTAMP/TIME"_test = [] {
        expect(sql_type_for<datetime_type_default>() == "DATETIME"s);
        expect(sql_type_for<timestamp_type_default>() == "TIMESTAMP"s);
        expect(sql_type_for<time_type_default>() == "TIME"s);

        auto const sql = create_table(tagged_temporal_alias_table{}).build_sql();
        expect(sql ==
               "CREATE TABLE tagged_temporal_alias_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    created_at DATETIME NOT NULL,\n"
               "    updated_at TIMESTAMP NOT NULL,\n"
               "    duration TIME NOT NULL\n"
               ");\n"s)
            << sql;
    };

    "tagged_column_field with numeric default aliases - emits FLOAT/DOUBLE/DECIMAL"_test = [] {
        expect(sql_type_for<float_type_default>() == "FLOAT"s);
        expect(sql_type_for<double_type_default>() == "DOUBLE"s);
        expect(sql_type_for<decimal_type_default>() == "DECIMAL"s);

        auto const sql = create_table(tagged_numeric_alias_table{}).build_sql();
        expect(sql ==
               "CREATE TABLE tagged_numeric_alias_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    value0 FLOAT NOT NULL,\n"
               "    value1 DOUBLE NOT NULL,\n"
               "    value2 DECIMAL NOT NULL\n"
               ");\n"s)
            << sql;
    };

    "create_table with numeric sql formatting options - emits FLOAT/DOUBLE/DECIMAL definitions"_test = [] {
        auto const sql = create_table(numeric_format_table{}).build_sql();
        expect(sql ==
               "CREATE TABLE numeric_format_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    price FLOAT(12,4) NOT NULL,\n"
               "    ratio DOUBLE(16,8) NOT NULL,\n"
               "    amount DECIMAL(18,6)\n"
               ");\n"s)
            << sql;
    };

    "create_table with formatted numeric wrapper types - emits inferred FLOAT/DOUBLE/DECIMAL definitions"_test = [] {
        auto const sql = create_table(formatted_numeric_column_table{}).build_sql();
        expect(sql ==
               "CREATE TABLE formatted_numeric_column_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    value0 FLOAT NOT NULL,\n"
               "    value1 FLOAT(12,4) NOT NULL,\n"
               "    value2a DOUBLE(12) NOT NULL,\n"
               "    value2 DOUBLE(12,4) NOT NULL,\n"
               "    value3a DECIMAL NOT NULL,\n"
               "    value3b DECIMAL(12) NOT NULL,\n"
               "    value3 DECIMAL(12,4) NOT NULL,\n"
               "    value4 DECIMAL(12,4)\n"
               ");\n"s)
            << sql;
    };

    // -------------------------------------------------------------------
    // create_table
    // -------------------------------------------------------------------

    "create_table - generates CREATE TABLE"_test = [] {
        auto const sql = create_table(test_table{}).build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ");\n"s)
            << sql;
    };

    "create_table.if_not_exists - generates CREATE TABLE IF NOT EXISTS"_test = [] {
        auto const sql = create_table(test_table{}).if_not_exists().build_sql();
        expect(sql ==
               "CREATE TABLE IF NOT EXISTS test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ");\n"s)
            << sql;
    };

    "create_table - SQL contains first column as PRIMARY KEY"_test = [] {
        auto const sql = create_table(test_table{}).build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ");\n"s)
            << sql;
    };

    // -------------------------------------------------------------------
    // drop_table
    // -------------------------------------------------------------------

    "drop_table - generates DROP TABLE"_test = [] {
        auto const sql = drop_table(test_table{}).build_sql();
        expect(sql == "DROP TABLE test_table;\n"s) << sql;
    };

    "drop_table.if_exists - generates DROP TABLE IF EXISTS"_test = [] {
        auto const sql = drop_table(test_table{}).if_exists().build_sql();
        expect(sql == "DROP TABLE IF EXISTS test_table;\n"s) << sql;
    };

    // -------------------------------------------------------------------
    // .then() chaining
    // -------------------------------------------------------------------

    "drop_table.if_exists.then.create_table - generates both statements"_test = [] {
        auto const sql =
            drop_table(test_table{}).if_exists().then().create_table(test_table{}).if_not_exists().build_sql();
        expect(sql ==
               "DROP TABLE IF EXISTS test_table;\n"
               "CREATE TABLE IF NOT EXISTS test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ");\n"s)
            << sql;
    };

    "drop_table.then.create_table - bare DROP then bare CREATE"_test = [] {
        auto const sql = drop_table(test_table{}).then().create_table(test_table{}).build_sql();
        expect(sql ==
               "DROP TABLE test_table;\n"
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ");\n"s)
            << sql;
    };

    // -------------------------------------------------------------------
    // create_temporary_table
    // -------------------------------------------------------------------

    "create_temporary_table - generates CREATE TEMPORARY TABLE"_test = [] {
        auto const sql = create_temporary_table(test_table{}).build_sql();
        expect(sql ==
               "CREATE TEMPORARY TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ");\n"s)
            << sql;
    };

    "create_temporary_table.if_not_exists - generates CREATE TEMPORARY TABLE IF NOT EXISTS"_test = [] {
        auto const sql = create_temporary_table(test_table{}).if_not_exists().build_sql();
        expect(sql ==
               "CREATE TEMPORARY TABLE IF NOT EXISTS test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ");\n"s)
            << sql;
    };

    // -------------------------------------------------------------------
    // drop_temporary_table
    // -------------------------------------------------------------------

    "drop_temporary_table - generates DROP TEMPORARY TABLE"_test = [] {
        auto const sql = drop_temporary_table(test_table{}).build_sql();
        expect(sql == "DROP TEMPORARY TABLE test_table;\n"s) << sql;
    };

    "drop_temporary_table.if_exists - generates DROP TEMPORARY TABLE IF EXISTS"_test = [] {
        auto const sql = drop_temporary_table(test_table{}).if_exists().build_sql();
        expect(sql == "DROP TEMPORARY TABLE IF EXISTS test_table;\n"s) << sql;
    };

    // -------------------------------------------------------------------
    // .then() chaining with TEMPORARY TABLE
    // -------------------------------------------------------------------

    "drop_temporary_table.if_exists.then.create_temporary_table - generates both statements"_test = [] {
        auto const sql = drop_temporary_table(test_table{})
                             .if_exists()
                             .then()
                             .create_temporary_table(test_table{})
                             .if_not_exists()
                             .build_sql();
        expect(sql ==
               "DROP TEMPORARY TABLE IF EXISTS test_table;\n"
               "CREATE TEMPORARY TABLE IF NOT EXISTS test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ");\n"s)
            << sql;
    };

    "drop_temporary_table.then.create_temporary_table - bare DROP TEMPORARY then bare CREATE TEMPORARY"_test = [] {
        auto const sql = drop_temporary_table(test_table{}).then().create_temporary_table(test_table{}).build_sql();
        expect(sql ==
               "DROP TEMPORARY TABLE test_table;\n"
               "CREATE TEMPORARY TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ");\n"s)
            << sql;
    };

    // -------------------------------------------------------------------
    // create_table(T{}).as(select_query)
    // -------------------------------------------------------------------

    "create_table.as(select) - generates CREATE TABLE AS SELECT"_test = [] {
        auto const sql =
            create_table(new_table{}).as(select(test_table::id{}, test_table::name{}).from(test_table{})).build_sql();
        expect(sql == "CREATE TABLE new_table AS SELECT id, name FROM test_table;\n"s) << sql;
    };

    "create_table.if_not_exists.as(select) - generates CREATE TABLE IF NOT EXISTS AS SELECT"_test = [] {
        auto const sql = create_table(new_table{})
                             .if_not_exists()
                             .as(select(test_table::id{}, test_table::name{}).from(test_table{}))
                             .build_sql();
        expect(sql == "CREATE TABLE IF NOT EXISTS new_table AS SELECT id, name FROM test_table;\n"s) << sql;
    };

    "create_temporary_table.as(select) - generates CREATE TEMPORARY TABLE AS SELECT"_test = [] {
        auto const sql = create_temporary_table(new_table{})
                             .as(select(test_table::id{}, test_table::name{}).from(test_table{}))
                             .build_sql();
        expect(sql == "CREATE TEMPORARY TABLE new_table AS SELECT id, name FROM test_table;\n"s) << sql;
    };

    "create_temporary_table.if_not_exists.as(select) - generates CREATE TEMPORARY TABLE IF NOT EXISTS AS SELECT"_test =
        [] {
            auto const sql = create_temporary_table(new_table{})
                                 .if_not_exists()
                                 .as(select(test_table::id{}, test_table::name{}).from(test_table{}))
                                 .build_sql();
            expect(sql == "CREATE TEMPORARY TABLE IF NOT EXISTS new_table AS SELECT id, name FROM test_table;\n"s)
                << sql;
        };

    "create_table.as(select).with_where - generates CREATE TABLE AS SELECT WHERE"_test = [] {
        auto const sql = create_table(new_table{})
                             .as(select(test_table::id{}, test_table::name{})
                                     .from(test_table{})
                                     .where(equal<test_table::name>("name")))
                             .build_sql();
        expect(sql == "CREATE TABLE new_table AS SELECT id, name FROM test_table WHERE name = 'name';\n"s) << sql;
    };

    "create_table.as(select).with_where(is_not_null) - generates CREATE TABLE AS SELECT WHERE"_test = [] {
        auto const sql = create_table(new_table{})
                             .as(select(test_table::id{}, test_table::name{})
                                     .from(test_table{})
                                     .where(is_not_null<test_table::tag>()))
                             .build_sql();
        expect(sql == "CREATE TABLE new_table AS SELECT id, name FROM test_table WHERE tag IS NOT NULL;\n"s) << sql;
    };

    "create_table.as(select) with fluent table attributes"_test = [] {
        auto const sql = create_table(new_table{})
                             .as(select(test_table::id{}, test_table::name{}).from(test_table{}))
                             .engine(Engine::InnoDB)
                             .auto_increment(1)
                             .default_charset(Charset::utf8mb4)
                             .build_sql();
        expect(sql ==
               "CREATE TABLE new_table ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 AS SELECT id, name "
               "FROM test_table;\n"s)
            << sql;
    };

    "create_table with string overload attributes"_test = [] {
        auto const sql = create_table(test_table{})
                             .engine("MyCustomEngine")
                             .default_charset("koi8r")
                             .row_format("DYNAMIC")
                             .collate("koi8r_general_ci")
                             .build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") ENGINE=MyCustomEngine DEFAULT CHARSET=koi8r ROW_FORMAT=DYNAMIC COLLATE=koi8r_general_ci;\n"s)
            << sql;
    };

    "create_table.collate(Collation) - emits COLLATE with enum value"_test = [] {
        auto const sql = create_table(test_table{}).collate(Collation::utf8mb4_unicode_ci).build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") COLLATE=utf8mb4_unicode_ci;\n"s)
            << sql;
    };

    "create_table with table_constraints trait - emits table-level PRIMARY KEY and KEY"_test = [] {
        auto const sql = create_table(symbol_with_indexes{})
                             .engine(Engine::InnoDB)
                             .auto_increment(1)
                             .default_charset(Charset::utf8)
                             .build_sql();
        expect(sql ==
               "CREATE TABLE symbol_with_indexes (\n"
               "    id INT NOT NULL,\n"
               "    exchange_id INT,\n"
               "    ticker VARCHAR(32) NOT NULL,\n"
               "    instrument VARCHAR(64) NOT NULL,\n"
               "    name VARCHAR(255),\n"
               "    sector VARCHAR(255),\n"
               "    currency VARCHAR(32),\n"
               "    created_date DATETIME NOT NULL,\n"
               "    last_updated_date DATETIME NOT NULL,\n"
               "    PRIMARY KEY (id),\n"
               "    KEY index_exchange_id (exchange_id)\n"
               ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;\n"s)
            << sql;
    };

    "table_constraint helper SQL fragments - renders UNIQUE/FULLTEXT/SPATIAL"_test = [] {
        expect(table_constraint::unique_key(index_id<"uq_test_name">{}, test_table::name{}) ==
               "UNIQUE KEY uq_test_name (name)"s);
        expect(table_constraint::fulltext_key(index_id<"ft_test_name">{}, test_table::name{}) ==
               "FULLTEXT KEY ft_test_name (name)"s);
        expect(table_constraint::spatial_key(index_id<"sp_test_id">{}, test_table::id{}) ==
               "SPATIAL KEY sp_test_id (id)"s);
    };

    "table_constraint::check - typed predicate with check_id"_test = [] {
        // Simple comparison predicate
        auto const sql = table_constraint::check(check_id<"chk_positive_id">{}, greater_than<test_table::id>(0u));
        expect(sql == "CONSTRAINT chk_positive_id CHECK (id > 0)"s) << sql;
    };

    "table_constraint::check - typed predicate without constraint name"_test = [] {
        auto const sql = table_constraint::check(greater_than<test_table::id>(0u));
        expect(sql == "CHECK (id > 0)"s) << sql;
    };

    "table_constraint::check - compound predicate with & operator"_test = [] {
        auto const sql = table_constraint::check(
            check_id<"chk_id_range">{}, greater_than<test_table::id>(0u) & less_than_or_equal<test_table::id>(100u));
        expect(sql == "CONSTRAINT chk_id_range CHECK ((id > 0 AND id <= 100))"s) << sql;
    };

    "table_constraint::check - nullable column predicate"_test = [] {
        // Using a nullable column (std::optional<varchar_type<64>>)
        auto const sql = table_constraint::check(check_id<"chk_tag_present">{}, is_not_null<test_table::tag>());
        expect(sql == "CONSTRAINT chk_tag_present CHECK (tag IS NOT NULL)"s) << sql;
    };

    "create_table with column_attributes - emits AUTO_INCREMENT, COMMENT, DEFAULT, ON UPDATE"_test = [] {
        auto const sql = create_table(audit_log{}).default_charset(Charset::utf8).build_sql();
        expect(sql ==
               "CREATE TABLE audit_log (\n"
               "    id INT UNSIGNED NOT NULL AUTO_INCREMENT,\n"
               "    event_type VARCHAR(64) NOT NULL,\n"
               "    user_id INT UNSIGNED NOT NULL COMMENT 'User who triggered the event',\n"
               "    description VARCHAR(255) NOT NULL,\n"
               "    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
               "    PRIMARY KEY (id)\n"
               ") DEFAULT CHARSET=utf8;\n"s)
            << sql;
    };

    "create_table with table_attributes trait - emits default ENGINE and CHARSET"_test = [] {
        auto const sql = create_table(table_with_attrs{}).build_sql();
        expect(sql ==
               "CREATE TABLE table_with_attrs (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL\n"
               ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"s)
            << sql;
    };

    "create_table with table_attributes trait + fluent override - fluent replaces default"_test = [] {
        auto const sql = create_table(table_with_attrs{}).engine(Engine::MyISAM).build_sql();
        expect(sql ==
               "CREATE TABLE table_with_attrs (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL\n"
               ") ENGINE=MyISAM DEFAULT CHARSET=utf8mb4;\n"s)
            << sql;
    };

    "create_table with table_attributes trait + if_not_exists - propagates defaults"_test = [] {
        auto const sql = create_table(table_with_attrs{}).if_not_exists().build_sql();
        expect(sql ==
               "CREATE TABLE IF NOT EXISTS table_with_attrs (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL\n"
               ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"s)
            << sql;
    };

    "drop_table.then.create_table.as(select) - chains DROP and CREATE AS SELECT"_test = [] {
        auto const sql = drop_table(new_table{})
                             .if_exists()
                             .then()
                             .create_table(new_table{})
                             .as(select(test_table::id{}, test_table::name{}).from(test_table{}))
                             .build_sql();
        expect(sql ==
               "DROP TABLE IF EXISTS new_table;\n"
               "CREATE TABLE new_table AS SELECT id, name FROM test_table;\n"s)
            << sql;
    };

    // -------------------------------------------------------------------
    // create_table fluent table options (create_table_attributes_mixin)
    // -------------------------------------------------------------------

    "create_table.avg_row_length - emits AVG_ROW_LENGTH"_test = [] {
        auto const sql = create_table(test_table{}).avg_row_length(512).build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") AVG_ROW_LENGTH=512;\n"s)
            << sql;
    };

    "create_table.checksum(bool) - emits CHECKSUM=1"_test = [] {
        auto const sql = create_table(test_table{}).checksum(true).build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") CHECKSUM=1;\n"s)
            << sql;
    };

    "create_table.checksum(bool false) - emits CHECKSUM=0"_test = [] {
        auto const sql = create_table(test_table{}).checksum(false).build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") CHECKSUM=0;\n"s)
            << sql;
    };

    "create_table.comment - emits COMMENT='...'"_test = [] {
        auto const sql = create_table(test_table{}).comment("my table comment").build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") COMMENT='my table comment';\n"s)
            << sql;
    };

    "create_table.comment with single quotes - escapes quotes"_test = [] {
        auto const sql = create_table(test_table{}).comment("it's a table").build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") COMMENT='it''s a table';\n"s)
            << sql;
    };

    "create_table.compression(Compression) - emits COMPRESSION enum values"_test = [] {
        auto const sql_zlib = create_table(test_table{}).compression(Compression::Zlib).build_sql();
        expect(sql_zlib ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") COMPRESSION='ZLIB';\n"s)
            << sql_zlib;

        auto const sql_lz4 = create_table(test_table{}).compression(Compression::Lz4).build_sql();
        expect(sql_lz4 ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") COMPRESSION='LZ4';\n"s)
            << sql_lz4;

        auto const sql_none = create_table(test_table{}).compression(Compression::None).build_sql();
        expect(sql_none ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") COMPRESSION='NONE';\n"s)
            << sql_none;
    };

    "create_table.compression(string_view) - emits COMPRESSION with custom value"_test = [] {
        auto const sql = create_table(test_table{}).compression("LZ4").build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") COMPRESSION='LZ4';\n"s)
            << sql;
    };

    "create_table.connection - emits CONNECTION='...'"_test = [] {
        auto const sql = create_table(test_table{}).connection("mysql://remote/db/table").build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") CONNECTION='mysql://remote/db/table';\n"s)
            << sql;
    };

    "create_table.data_directory - emits DATA DIRECTORY='...'"_test = [] {
        auto const sql = create_table(test_table{}).data_directory("/var/lib/mysql/data").build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") DATA DIRECTORY='/var/lib/mysql/data';\n"s)
            << sql;
    };

    "create_table.index_directory - emits INDEX DIRECTORY='...'"_test = [] {
        auto const sql = create_table(test_table{}).index_directory("/var/lib/mysql/index").build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") INDEX DIRECTORY='/var/lib/mysql/index';\n"s)
            << sql;
    };

    "create_table.delay_key_write(bool) - emits DELAY_KEY_WRITE=1/0"_test = [] {
        auto const sql_on = create_table(test_table{}).delay_key_write(true).build_sql();
        expect(sql_on ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") DELAY_KEY_WRITE=1;\n"s)
            << sql_on;

        auto const sql_off = create_table(test_table{}).delay_key_write(false).build_sql();
        expect(sql_off ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") DELAY_KEY_WRITE=0;\n"s)
            << sql_off;
    };

    "create_table.encryption(Encryption) - emits ENCRYPTION='Y'/'N'"_test = [] {
        auto const sql_y = create_table(test_table{}).encryption(Encryption::Y).build_sql();
        expect(sql_y ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") ENCRYPTION='Y';\n"s)
            << sql_y;

        auto const sql_n = create_table(test_table{}).encryption(Encryption::N).build_sql();
        expect(sql_n ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") ENCRYPTION='N';\n"s)
            << sql_n;
    };

    "create_table.encryption(string_view) - emits ENCRYPTION with custom value"_test = [] {
        auto const sql = create_table(test_table{}).encryption("Y").build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") ENCRYPTION='Y';\n"s)
            << sql;
    };

    "create_table.insert_method(InsertMethod) - emits INSERT_METHOD enum values"_test = [] {
        auto const sql_no = create_table(test_table{}).insert_method(InsertMethod::No).build_sql();
        expect(sql_no ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") INSERT_METHOD=NO;\n"s)
            << sql_no;

        auto const sql_first = create_table(test_table{}).insert_method(InsertMethod::First).build_sql();
        expect(sql_first ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") INSERT_METHOD=FIRST;\n"s)
            << sql_first;

        auto const sql_last = create_table(test_table{}).insert_method(InsertMethod::Last).build_sql();
        expect(sql_last ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") INSERT_METHOD=LAST;\n"s)
            << sql_last;
    };

    "create_table.insert_method(string_view) - emits INSERT_METHOD with custom value"_test = [] {
        auto const sql = create_table(test_table{}).insert_method("FIRST").build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") INSERT_METHOD=FIRST;\n"s)
            << sql;
    };

    "create_table.key_block_size - emits KEY_BLOCK_SIZE=N"_test = [] {
        auto const sql = create_table(test_table{}).key_block_size(8).build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") KEY_BLOCK_SIZE=8;\n"s)
            << sql;
    };

    "create_table.max_rows - emits MAX_ROWS=N"_test = [] {
        auto const sql = create_table(test_table{}).max_rows(1000000).build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") MAX_ROWS=1000000;\n"s)
            << sql;
    };

    "create_table.min_rows - emits MIN_ROWS=N"_test = [] {
        auto const sql = create_table(test_table{}).min_rows(100).build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") MIN_ROWS=100;\n"s)
            << sql;
    };

    "create_table.pack_keys(PackKeys) - emits PACK_KEYS enum values"_test = [] {
        auto const sql_default = create_table(test_table{}).pack_keys(PackKeys::Default).build_sql();
        expect(sql_default ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") PACK_KEYS=DEFAULT;\n"s)
            << sql_default;

        auto const sql_zero = create_table(test_table{}).pack_keys(PackKeys::Zero).build_sql();
        expect(sql_zero ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") PACK_KEYS=0;\n"s)
            << sql_zero;

        auto const sql_one = create_table(test_table{}).pack_keys(PackKeys::One).build_sql();
        expect(sql_one ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") PACK_KEYS=1;\n"s)
            << sql_one;
    };

    "create_table.pack_keys(string_view) - emits PACK_KEYS with custom value"_test = [] {
        auto const sql = create_table(test_table{}).pack_keys("1").build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") PACK_KEYS=1;\n"s)
            << sql;
    };

    "create_table.password - emits PASSWORD='...'"_test = [] {
        auto const sql = create_table(test_table{}).password("secret").build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") PASSWORD='secret';\n"s)
            << sql;
    };

    "create_table.row_format(RowFormat) - emits ROW_FORMAT enum values"_test = [] {
        auto const sql_dynamic = create_table(test_table{}).row_format(RowFormat::Dynamic).build_sql();
        expect(sql_dynamic ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") ROW_FORMAT=DYNAMIC;\n"s)
            << sql_dynamic;

        auto const sql_compressed = create_table(test_table{}).row_format(RowFormat::Compressed).build_sql();
        expect(sql_compressed ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") ROW_FORMAT=COMPRESSED;\n"s)
            << sql_compressed;

        auto const sql_compact = create_table(test_table{}).row_format(RowFormat::Compact).build_sql();
        expect(sql_compact ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") ROW_FORMAT=COMPACT;\n"s)
            << sql_compact;

        auto const sql_fixed = create_table(test_table{}).row_format(RowFormat::Fixed).build_sql();
        expect(sql_fixed ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") ROW_FORMAT=FIXED;\n"s)
            << sql_fixed;

        auto const sql_redundant = create_table(test_table{}).row_format(RowFormat::Redundant).build_sql();
        expect(sql_redundant ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") ROW_FORMAT=REDUNDANT;\n"s)
            << sql_redundant;

        auto const sql_default = create_table(test_table{}).row_format(RowFormat::Default).build_sql();
        expect(sql_default ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") ROW_FORMAT=DEFAULT;\n"s)
            << sql_default;
    };

    "create_table.stats_auto_recalc(bool) - emits STATS_AUTO_RECALC=1/0"_test = [] {
        auto const sql_on = create_table(test_table{}).stats_auto_recalc(true).build_sql();
        expect(sql_on ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") STATS_AUTO_RECALC=1;\n"s)
            << sql_on;

        auto const sql_off = create_table(test_table{}).stats_auto_recalc(false).build_sql();
        expect(sql_off ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") STATS_AUTO_RECALC=0;\n"s)
            << sql_off;
    };

    "create_table.stats_auto_recalc_default - emits STATS_AUTO_RECALC=DEFAULT"_test = [] {
        auto const sql = create_table(test_table{}).stats_auto_recalc_default().build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") STATS_AUTO_RECALC=DEFAULT;\n"s)
            << sql;
    };

    "create_table.stats_persistent(bool) - emits STATS_PERSISTENT=1/0"_test = [] {
        auto const sql_on = create_table(test_table{}).stats_persistent(true).build_sql();
        expect(sql_on ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") STATS_PERSISTENT=1;\n"s)
            << sql_on;

        auto const sql_off = create_table(test_table{}).stats_persistent(false).build_sql();
        expect(sql_off ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") STATS_PERSISTENT=0;\n"s)
            << sql_off;
    };

    "create_table.stats_persistent_default - emits STATS_PERSISTENT=DEFAULT"_test = [] {
        auto const sql = create_table(test_table{}).stats_persistent_default().build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") STATS_PERSISTENT=DEFAULT;\n"s)
            << sql;
    };

    "create_table.stats_sample_pages - emits STATS_SAMPLE_PAGES=N"_test = [] {
        auto const sql = create_table(test_table{}).stats_sample_pages(200).build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") STATS_SAMPLE_PAGES=200;\n"s)
            << sql;
    };

    "create_table.tablespace - emits TABLESPACE=value"_test = [] {
        auto const sql = create_table(test_table{}).tablespace("innodb_system").build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") TABLESPACE=innodb_system;\n"s)
            << sql;
    };

    "create_table.union_tables - emits UNION=(t1,t2,t3)"_test = [] {
        auto const sql =
            create_table(test_table{}).union_tables(test_table{}, new_table{}, table_with_attrs{}).build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") UNION=(test_table,new_table,table_with_attrs);\n"s)
            << sql;
    };

    "create_table with multiple options - emits all in order"_test = [] {
        auto const sql = create_table(test_table{})
                             .engine(Engine::InnoDB)
                             .auto_increment(100)
                             .default_charset(Charset::utf8mb4)
                             .collate("utf8mb4_unicode_ci")
                             .comment("main table")
                             .row_format(RowFormat::Dynamic)
                             .compression(Compression::Zlib)
                             .key_block_size(4)
                             .build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") ENGINE=InnoDB AUTO_INCREMENT=100 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci "
               "COMMENT='main table' ROW_FORMAT=DYNAMIC COMPRESSION='ZLIB' KEY_BLOCK_SIZE=4;\n"s)
            << sql;
    };

    "create_table duplicate option key - later call replaces earlier"_test = [] {
        auto const sql = create_table(test_table{}).engine(Engine::InnoDB).engine(Engine::MyISAM).build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ") ENGINE=MyISAM;\n"s)
            << sql;
    };

    "create_table.engine all enum values - emits correct strings"_test = [] {
        auto check_engine = [](Engine e, std::string_view expected_name) {
            auto const sql = create_table(new_table{}).engine(e).build_sql();
            auto const expected =
                "CREATE TABLE new_table (\n"
                "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT\n"
                ") ENGINE=" +
                std::string(expected_name) + ";\n";
            expect(sql == expected) << sql;
        };
        check_engine(Engine::InnoDB, "InnoDB");
        check_engine(Engine::MyISAM, "MyISAM");
        check_engine(Engine::Memory, "MEMORY");
        check_engine(Engine::Ndb, "NDB");
        check_engine(Engine::Archive, "ARCHIVE");
        check_engine(Engine::Csv, "CSV");
        check_engine(Engine::Merge, "MERGE");
        check_engine(Engine::Blackhole, "BLACKHOLE");
        check_engine(Engine::Federated, "FEDERATED");
    };

    "create_table.default_charset all enum values - emits correct strings"_test = [] {
        auto check_charset = [](Charset c, std::string_view expected_name) {
            auto const sql = create_table(new_table{}).default_charset(c).build_sql();
            auto const expected =
                "CREATE TABLE new_table (\n"
                "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT\n"
                ") DEFAULT CHARSET=" +
                std::string(expected_name) + ";\n";
            expect(sql == expected) << sql;
        };
        check_charset(Charset::utf8mb4, "utf8mb4");
        check_charset(Charset::utf8, "utf8");
        check_charset(Charset::latin1, "latin1");
        check_charset(Charset::ascii, "ascii");
        check_charset(Charset::ucs2, "ucs2");
        check_charset(Charset::utf16, "utf16");
        check_charset(Charset::utf32, "utf32");
    };
};

// ===================================================================
// DDL Foreign Keys
// ===================================================================

namespace {
struct parent_table {
    COLUMN_FIELD(id, uint32_t)
};

struct child_table {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(parent_id, uint32_t, fk_attr::references<parent_table, parent_table::id>)
};

struct child_table_cascade {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(parent_id, uint32_t, fk_attr::references<parent_table, parent_table::id>, fk_attr::on_delete_cascade,
                 fk_attr::on_update_cascade)
};
}  // namespace

suite<"DDL Foreign Keys"> ddl_foreign_keys_suite = [] {
    "create_table with FK - emits FOREIGN KEY constraint"_test = [] {
        auto const sql = create_table(child_table{}).build_sql();
        expect(sql ==
               "CREATE TABLE child_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    parent_id INT UNSIGNED NOT NULL,\n"
               "    FOREIGN KEY (parent_id) REFERENCES parent_table(id)\n"
               ");\n"s)
            << sql;
    };

    "create_table with FK CASCADE - emits ON DELETE CASCADE ON UPDATE CASCADE"_test = [] {
        auto const sql = create_table(child_table_cascade{}).build_sql();
        expect(sql ==
               "CREATE TABLE child_table_cascade (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    parent_id INT UNSIGNED NOT NULL,\n"
               "    FOREIGN KEY (parent_id) REFERENCES parent_table(id) ON DELETE CASCADE ON UPDATE CASCADE\n"
               ");\n"s)
            << sql;
    };

    "create_table without FK - no FOREIGN KEY clause"_test = [] {
        auto const sql = create_table(test_table{}).build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ");\n"s)
            << sql;
    };
};

// ===================================================================
// SqlBuilder concept — compile-time checks
// ===================================================================

static_assert(SqlBuilder<ddl_detail::create_table_builder<test_table>>, "create_table_builder must satisfy SqlBuilder");
static_assert(SqlBuilder<ddl_detail::create_table_cond_builder<test_table>>,
              "create_table_cond_builder must satisfy SqlBuilder");
static_assert(SqlBuilder<ddl_detail::drop_table_builder<test_table>>, "drop_table_builder must satisfy SqlBuilder");
static_assert(SqlBuilder<ddl_detail::drop_table_cond_builder<test_table>>,
              "drop_table_cond_builder must satisfy SqlBuilder");
static_assert(SqlBuilder<ddl_detail::create_table_as_builder<test_table>>,
              "create_table_as_builder must satisfy SqlBuilder");

// ===================================================================
// DDL create_database
// ===================================================================

suite<"DDL CREATE DATABASE"> ddl_create_database_suite = [] {
    "create_database - generates CREATE DATABASE"_test = [] {
        auto const sql = create_database(test_db{}).build_sql();
        expect(sql == "CREATE DATABASE test_db;\n"s) << sql;
    };

    "create_database.if_not_exists - generates CREATE DATABASE IF NOT EXISTS"_test = [] {
        auto const sql = create_database(test_db{}).if_not_exists().build_sql();
        expect(sql == "CREATE DATABASE IF NOT EXISTS test_db;\n"s) << sql;
    };

    "database_name_for custom specialization - uses override name"_test = [] {
        auto const sql = create_database(custom_named_db{}).build_sql();
        expect(sql == "CREATE DATABASE my_custom_db;\n"s) << sql;
    };

    "create_database.then.create_table - chains CREATE DATABASE and CREATE TABLE"_test = [] {
        auto const sql = create_database(test_db{})
                             .if_not_exists()
                             .then()
                             .create_table(test_db::symbol{})
                             .if_not_exists()
                             .build_sql();
        expect(sql ==
               "CREATE DATABASE IF NOT EXISTS test_db;\n"
               "CREATE TABLE IF NOT EXISTS symbol (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT\n"
               ");\n"s)
            << sql;
    };

    "create_database.then.create_all_tables - chains CREATE DATABASE and CREATE TABLE for all DB tables"_test = [] {
        auto const sql = create_database(schema_bootstrap_db{})
                             .if_not_exists()
                             .then()
                             .create_all_tables(schema_bootstrap_db{})
                             .build_sql();
        expect(sql ==
               "CREATE DATABASE IF NOT EXISTS schema_bootstrap_db;\n"
               "CREATE TABLE schema_bootstrap_db.account (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT\n"
               ");\n"
               "CREATE TABLE schema_bootstrap_db.trade (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT\n"
               ");\n"s)
            << sql;
    };

    "create_database.then.create_all_tables.if_not_exists - emits IF NOT EXISTS for each table"_test = [] {
        auto const sql = create_database(schema_bootstrap_db{})
                             .if_not_exists()
                             .then()
                             .create_all_tables(schema_bootstrap_db{})
                             .if_not_exists()
                             .build_sql();
        expect(sql ==
               "CREATE DATABASE IF NOT EXISTS schema_bootstrap_db;\n"
               "CREATE TABLE IF NOT EXISTS schema_bootstrap_db.account (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT\n"
               ");\n"
               "CREATE TABLE IF NOT EXISTS schema_bootstrap_db.trade (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT\n"
               ");\n"s)
            << sql;
    };

    "create_database(database_name) - generates CREATE DATABASE with runtime name"_test = [] {
        auto const sql = create_database(database_name{"runtime_db"}).build_sql();
        expect(sql == "CREATE DATABASE runtime_db;\n"s) << sql;
    };

    "create_database(database_name).if_not_exists - generates CREATE DATABASE IF NOT EXISTS with runtime name"_test =
        [] {
            auto const sql = create_database(database_name{"runtime_db"}).if_not_exists().build_sql();
            expect(sql == "CREATE DATABASE IF NOT EXISTS runtime_db;\n"s) << sql;
        };

    "create_database with fluent charset - uses fluent API to set DEFAULT CHARACTER SET"_test = [] {
        auto const sql = create_database(test_db{}).default_charset(Charset::utf8mb4).build_sql();
        expect(sql == "CREATE DATABASE test_db DEFAULT CHARACTER SET utf8mb4;\n"s) << sql;
    };

    "create_database.if_not_exists with fluent charset and default_collate - chains both attributes"_test = [] {
        auto const sql = create_database(test_db{})
                             .default_charset(Charset::utf8mb4)
                             .default_collate("utf8mb4_unicode_ci")
                             .if_not_exists()
                             .build_sql();
        expect(sql ==
               "CREATE DATABASE IF NOT EXISTS test_db DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE "
               "utf8mb4_unicode_ci;\n"s)
            << sql;
    };

    "create_database with string overload default_charset - accepts string_view charset"_test = [] {
        auto const sql = create_database(test_db{}).default_charset("koi8r").build_sql();
        expect(sql == "CREATE DATABASE test_db DEFAULT CHARACTER SET koi8r;\n"s) << sql;
    };

    "create_database(database_name) with fluent attributes - runtime name with charset and default_collate"_test = [] {
        auto const sql = create_database(database_name{"my_db"})
                             .default_charset(Charset::utf8mb4)
                             .default_collate("utf8mb4_bin")
                             .build_sql();
        expect(sql == "CREATE DATABASE my_db DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_bin;\n"s) << sql;
    };
};

// ===================================================================
// DDL drop_database
// ===================================================================

suite<"DDL DROP DATABASE"> ddl_drop_database_suite = [] {
    "drop_database - generates DROP DATABASE"_test = [] {
        auto const sql = drop_database(test_db{}).build_sql();
        expect(sql == "DROP DATABASE test_db;\n"s) << sql;
    };

    "drop_database.if_exists - generates DROP DATABASE IF EXISTS"_test = [] {
        auto const sql = drop_database(test_db{}).if_exists().build_sql();
        expect(sql == "DROP DATABASE IF EXISTS test_db;\n"s) << sql;
    };

    "drop_database(database_name) - generates DROP DATABASE with runtime name"_test = [] {
        auto const sql = drop_database(database_name{"runtime_db"}).build_sql();
        expect(sql == "DROP DATABASE runtime_db;\n"s) << sql;
    };

    "drop_database(database_name).if_exists - generates DROP DATABASE IF EXISTS with runtime name"_test = [] {
        auto const sql = drop_database(database_name{"runtime_db"}).if_exists().build_sql();
        expect(sql == "DROP DATABASE IF EXISTS runtime_db;\n"s) << sql;
    };

    "drop_database.then.create_database - chains DROP DATABASE and CREATE DATABASE"_test = [] {
        auto const sql = drop_database(test_db{}).if_exists().then().create_database(test_db{}).build_sql();
        expect(sql ==
               "DROP DATABASE IF EXISTS test_db;\n"
               "CREATE DATABASE test_db;\n"s)
            << sql;
    };
};

suite<"DDL CREATE ALL TABLES"> ddl_create_all_tables_suite = [] {
    "create_all_tables - emits CREATE TABLE for every table in DB"_test = [] {
        auto const sql = create_all_tables(schema_bootstrap_db{}).build_sql();
        expect(sql ==
               "CREATE TABLE schema_bootstrap_db.account (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT\n"
               ");\n"
               "CREATE TABLE schema_bootstrap_db.trade (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT\n"
               ");\n"s)
            << sql;
    };

    "create_all_tables.if_not_exists - emits IF NOT EXISTS for each table"_test = [] {
        auto const sql = create_all_tables(schema_bootstrap_db{}).if_not_exists().build_sql();
        expect(sql ==
               "CREATE TABLE IF NOT EXISTS schema_bootstrap_db.account (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT\n"
               ");\n"
               "CREATE TABLE IF NOT EXISTS schema_bootstrap_db.trade (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT\n"
               ");\n"s)
            << sql;
    };

    "create_all_tables.then - chains into further DDL"_test = [] {
        auto const sql =
            create_all_tables(schema_bootstrap_db{}).then().create_database(schema_bootstrap_db{}).build_sql();
        expect(sql ==
               "CREATE TABLE schema_bootstrap_db.account (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT\n"
               ");\n"
               "CREATE TABLE schema_bootstrap_db.trade (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT\n"
               ");\n"
               "CREATE DATABASE schema_bootstrap_db;\n"s)
            << sql;
    };
};

suite<"DDL Features"> ddl_features_suite = [] {
    "create_view.as(select) - generates CREATE VIEW AS SELECT"_test = [] {
        auto const sql = create_view(new_table{}).as(select(test_table::id{}).from(test_table{})).build_sql();
        expect(sql == "CREATE VIEW new_table AS SELECT id FROM test_table;\n"s) << sql;
    };

    "create_view.or_replace.as(select) - generates CREATE OR REPLACE VIEW"_test = [] {
        auto const sql =
            create_view(new_table{}).or_replace().as(select(test_table::id{}).from(test_table{})).build_sql();
        expect(sql == "CREATE OR REPLACE VIEW new_table AS SELECT id FROM test_table;\n"s) << sql;
    };

    "drop_view.if_exists - generates DROP VIEW IF EXISTS"_test = [] {
        auto const sql = drop_view(new_table{}).if_exists().build_sql();
        expect(sql == "DROP VIEW IF EXISTS new_table;\n"s) << sql;
    };

    "rename_table - generates RENAME TABLE old TO new"_test = [] {
        auto const sql = rename_table(test_table{}, renamed_table{}).build_sql();
        expect(sql == "RENAME TABLE test_table TO renamed_table;\n"s) << sql;
    };

    "create_index_on - generates CREATE INDEX"_test = [] {
        auto const sql =
            create_index_on<test_table::id, test_table::name>(index_id<"idx_test_table_id_name">{}, test_table{})
                .build_sql();
        expect(sql == "CREATE INDEX idx_test_table_id_name ON test_table (id, name);\n"s) << sql;
    };

    "create_index_on.unique - generates CREATE UNIQUE INDEX"_test = [] {
        auto const sql =
            create_index_on<test_table::name>(index_id<"uq_test_table_name">{}, test_table{}).unique().build_sql();
        expect(sql == "CREATE UNIQUE INDEX uq_test_table_name ON test_table (name);\n"s) << sql;
    };

    "create_index_on instance-based - generates CREATE INDEX"_test = [] {
        auto const sql =
            create_index_on(index_id<"idx_test_table_id_name">{}, test_table{}, test_table::id{}, test_table::name{})
                .build_sql();
        expect(sql == "CREATE INDEX idx_test_table_id_name ON test_table (id, name);\n"s) << sql;
    };

    "create_index_on instance-based.unique - generates CREATE UNIQUE INDEX"_test = [] {
        auto const sql =
            create_index_on(index_id<"uq_test_table_name">{}, test_table{}, test_table::name{}).unique().build_sql();
        expect(sql == "CREATE UNIQUE INDEX uq_test_table_name ON test_table (name);\n"s) << sql;
    };

    "drop_index_on - generates DROP INDEX ON table"_test = [] {
        auto const sql = drop_index_on(index_id<"idx_test_table_id_name">{}, test_table{}).build_sql();
        expect(sql == "DROP INDEX idx_test_table_id_name ON test_table;\n"s) << sql;
    };

    "alter_table - generates combined ALTER TABLE actions"_test = [] {
        auto const sql = alter_table(test_table{})
                             .add_column<test_table::name>()
                             .modify_column<test_table::name>()
                             .rename_column<test_table::name, column_field<"display_name", varchar_type<255>>>()
                             .drop_column<test_table::name>()
                             .rename_to<renamed_table>()
                             .build_sql();
        expect(sql ==
               "ALTER TABLE test_table ADD COLUMN name VARCHAR(255) NOT NULL, MODIFY COLUMN name VARCHAR(255) NOT "
               "NULL, RENAME COLUMN name TO display_name, DROP COLUMN name, RENAME TO renamed_table;\n"s)
            << sql;
    };

    "alter_table constraint helpers - add/drop constraint and drop fk"_test = [] {
        auto const sql = alter_table(test_table{})
                             .add_constraint("fk_test_table_related FOREIGN KEY (id) REFERENCES related_table(id)")
                             .drop_constraint("chk_test_table_name")
                             .drop_foreign_key("fk_test_table_related")
                             .build_sql();
        expect(sql ==
               "ALTER TABLE test_table ADD CONSTRAINT fk_test_table_related FOREIGN KEY (id) REFERENCES "
               "related_table(id), DROP CONSTRAINT chk_test_table_name, DROP FOREIGN KEY fk_test_table_related;\n"s)
            << sql;
    };
};

// ===================================================================
// DDL — ALTER TABLE extended actions
// ===================================================================

suite<"DDL alter_table extended"> alter_table_extended_suite = [] {
    "alter_table.change_column - renames and retypes a column"_test = [] {
        auto const sql = alter_table(test_table{})
                             .change_column<test_table::name, column_field<"display_name", varchar_type<512>>>()
                             .build_sql();
        expect(sql == "ALTER TABLE test_table CHANGE COLUMN name display_name VARCHAR(512) NOT NULL;\n"s) << sql;
    };

    "alter_table.change_column nullable - std::optional makes column nullable"_test = [] {
        auto const sql =
            alter_table(test_table{})
                .change_column<test_table::name, column_field<"display_name", std::optional<varchar_type<512>>>>()
                .build_sql();
        expect(sql == "ALTER TABLE test_table CHANGE COLUMN name display_name VARCHAR(512);\n"s) << sql;
    };

    "alter_table.add_index - generates ADD INDEX"_test = [] {
        auto const sql =
            alter_table(test_table{}).add_index<test_table::name>(index_id<"idx_test_table_name">{}).build_sql();
        expect(sql == "ALTER TABLE test_table ADD INDEX idx_test_table_name (name);\n"s) << sql;
    };

    "alter_table.add_unique_index - generates ADD UNIQUE INDEX"_test = [] {
        auto const sql =
            alter_table(test_table{}).add_unique_index<test_table::name>(index_id<"uq_test_table_name">{}).build_sql();
        expect(sql == "ALTER TABLE test_table ADD UNIQUE INDEX uq_test_table_name (name);\n"s) << sql;
    };

    "alter_table.add_fulltext_index - generates ADD FULLTEXT INDEX"_test = [] {
        auto const sql = alter_table(test_table{})
                             .add_fulltext_index<test_table::name>(index_id<"ft_test_table_name">{})
                             .build_sql();
        expect(sql == "ALTER TABLE test_table ADD FULLTEXT INDEX ft_test_table_name (name);\n"s) << sql;
    };

    "alter_table.enable_keys - generates ENABLE KEYS"_test = [] {
        auto const sql = alter_table(test_table{}).enable_keys().build_sql();
        expect(sql == "ALTER TABLE test_table ENABLE KEYS;\n"s) << sql;
    };

    "alter_table.disable_keys - generates DISABLE KEYS"_test = [] {
        auto const sql = alter_table(test_table{}).disable_keys().build_sql();
        expect(sql == "ALTER TABLE test_table DISABLE KEYS;\n"s) << sql;
    };

    "alter_table.convert_to_charset - generates CONVERT TO CHARACTER SET"_test = [] {
        auto const sql = alter_table(test_table{}).convert_to_charset(Charset::utf8mb4).build_sql();
        expect(sql == "ALTER TABLE test_table CONVERT TO CHARACTER SET utf8mb4;\n"s) << sql;
    };

    "alter_table.set_engine - generates ENGINE = ..."_test = [] {
        auto const sql = alter_table(test_table{}).set_engine(Engine::InnoDB).build_sql();
        expect(sql == "ALTER TABLE test_table ENGINE = InnoDB;\n"s) << sql;
    };

    "alter_table.set_auto_increment - generates AUTO_INCREMENT = n"_test = [] {
        auto const sql = alter_table(test_table{}).set_auto_increment(1000).build_sql();
        expect(sql == "ALTER TABLE test_table AUTO_INCREMENT = 1000;\n"s) << sql;
    };

    "alter_table - combines multiple new actions"_test = [] {
        auto const sql = alter_table(test_table{})
                             .change_column<test_table::name, column_field<"label", text_type<>>>()
                             .add_index<test_table::name>(index_id<"idx_label">{})
                             .set_engine(Engine::MyISAM)
                             .set_auto_increment(500)
                             .build_sql();
        expect(sql ==
               "ALTER TABLE test_table CHANGE COLUMN name label TEXT NOT NULL, ADD INDEX idx_label (name), "
               "ENGINE = MyISAM, AUTO_INCREMENT = 500;\n"s)
            << sql;
    };
};

static_assert(SqlBuilder<ddl_detail::create_database_builder<test_db>>,
              "create_database_builder must satisfy SqlBuilder");
static_assert(SqlBuilder<ddl_detail::create_view_as_builder<new_table>>,
              "create_view_as_builder must satisfy SqlBuilder");
static_assert(SqlBuilder<ddl_detail::drop_view_builder<new_table>>, "drop_view_builder must satisfy SqlBuilder");
static_assert(SqlBuilder<ddl_detail::rename_table_builder<test_table, renamed_table>>,
              "rename_table_builder must satisfy SqlBuilder");
static_assert(SqlBuilder<ddl_detail::create_database_if_not_exists_builder<test_db>>,
              "create_database_if_not_exists_builder must satisfy SqlBuilder");
static_assert(SqlBuilder<ddl_detail::create_database_named_builder<>>,
              "create_database_named_builder must satisfy SqlBuilder");
static_assert(SqlBuilder<ddl_detail::create_database_named_if_not_exists_builder<>>,
              "create_database_named_if_not_exists_builder must satisfy SqlBuilder");
static_assert(SqlBuilder<ddl_detail::drop_database_builder<test_db>>, "drop_database_builder must satisfy SqlBuilder");
static_assert(SqlBuilder<ddl_detail::drop_database_if_exists_builder<test_db>>,
              "drop_database_if_exists_builder must satisfy SqlBuilder");
static_assert(SqlBuilder<ddl_detail::drop_database_named_builder<>>,
              "drop_database_named_builder must satisfy SqlBuilder");
static_assert(SqlBuilder<ddl_detail::drop_database_named_if_exists_builder<>>,
              "drop_database_named_if_exists_builder must satisfy SqlBuilder");
static_assert(Database<test_db>, "test_db must satisfy Database concept");
static_assert(!Database<test_table>, "plain table must NOT satisfy Database concept");

// ===================================================================
// DDL — text_type column types (TEXT, MEDIUMTEXT, LONGTEXT)
// ===================================================================

namespace {
struct text_table {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(description, text_type<>)
    COLUMN_FIELD(notes, mediumtext_type)
    COLUMN_FIELD(body, longtext_type)
    COLUMN_FIELD(opt_notes, std::optional<text_type<>>)
};
}  // namespace

suite<"DDL text_type types"> ddl_text_type_suite = [] {
    "create_table with text_type - emits TEXT column"_test = [] {
        auto const sql = create_table(text_table{}).build_sql();
        expect(sql ==
               "CREATE TABLE text_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    description TEXT NOT NULL,\n"
               "    notes MEDIUMTEXT NOT NULL,\n"
               "    body LONGTEXT NOT NULL,\n"
               "    opt_notes TEXT\n"
               ");\n"s)
            << sql;
    };

    "create_table.if_not_exists with text columns - emits IF NOT EXISTS"_test = [] {
        auto const sql = create_table(text_table{}).if_not_exists().build_sql();
        expect(sql ==
               "CREATE TABLE IF NOT EXISTS text_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    description TEXT NOT NULL,\n"
               "    notes MEDIUMTEXT NOT NULL,\n"
               "    body LONGTEXT NOT NULL,\n"
               "    opt_notes TEXT\n"
               ");\n"s)
            << sql;
    };
};

// ===================================================================
// DDL USE
// ===================================================================

suite<"DDL USE"> ddl_use_suite = [] {
    "use - generates USE"_test = [] {
        auto const sql = use(test_db{}).build_sql();
        expect(sql == "USE test_db;\n"s) << sql;
    };

    "use with custom name - uses override name"_test = [] {
        auto const sql = use(custom_named_db{}).build_sql();
        expect(sql == "USE my_custom_db;\n"s) << sql;
    };

    "create_database.then.use - chains CREATE DATABASE and USE"_test = [] {
        auto const sql = create_database(test_db{}).if_not_exists().then().use(test_db{}).build_sql();
        expect(sql ==
               "CREATE DATABASE IF NOT EXISTS test_db;\n"
               "USE test_db;\n"s)
            << sql;
    };

    "create_database.then.use.then.create_table - full setup chain"_test = [] {
        auto const sql = create_database(test_db{})
                             .if_not_exists()
                             .then()
                             .use(test_db{})
                             .then()
                             .create_table(test_db::symbol{})
                             .if_not_exists()
                             .build_sql();
        expect(sql ==
               "CREATE DATABASE IF NOT EXISTS test_db;\n"
               "USE test_db;\n"
               "CREATE TABLE IF NOT EXISTS symbol (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT\n"
               ");\n"s)
            << sql;
    };
};

static_assert(SqlBuilder<ddl_detail::use_builder<test_db>>, "use_builder must satisfy SqlBuilder");

// ===================================================================
// DDL — SHOW statements
// ===================================================================

suite<"DDL SHOW"> ddl_show_suite = [] {
    "show_databases - generates SHOW DATABASES"_test = [] {
        auto const sql = show_databases().build_sql();
        expect(sql == "SHOW DATABASES"s) << sql;
    };

    "show_tables - generates SHOW TABLES"_test = [] {
        auto const sql = show_tables().build_sql();
        expect(sql == "SHOW TABLES"s) << sql;
    };

    "show_columns - generates SHOW COLUMNS FROM table"_test = [] {
        auto const sql = show_columns(test_table{}).build_sql();
        expect(sql == "SHOW COLUMNS FROM test_table"s) << sql;
    };

    "show_create_table - generates SHOW CREATE TABLE"_test = [] {
        auto const sql = show_create_table(test_table{}).build_sql();
        expect(sql == "SHOW CREATE TABLE test_table"s) << sql;
    };
};

// ===================================================================
// DDL — Stored Procedures
// ===================================================================

suite<"DDL procedures"> ddl_procedure_suite = [] {
    "create_procedure - generates CREATE PROCEDURE"_test = [] {
        auto const sql = create_procedure(procedure_id<"usp_hello">{}, "", "SELECT 1;").build_sql();
        expect(sql == "CREATE PROCEDURE usp_hello()\nBEGIN\nSELECT 1;\nEND"s) << sql;
    };

    "create_procedure with params - includes params"_test = [] {
        auto const sql = create_procedure(procedure_id<"usp_add">{}, "IN a INT, IN b INT", "SELECT a + b;").build_sql();
        expect(sql == "CREATE PROCEDURE usp_add(IN a INT, IN b INT)\nBEGIN\nSELECT a + b;\nEND"s) << sql;
    };

    "drop_procedure - generates DROP PROCEDURE"_test = [] {
        auto const sql = drop_procedure(procedure_id<"usp_hello">{}).build_sql();
        expect(sql == "DROP PROCEDURE usp_hello"s) << sql;
    };

    "drop_procedure.if_exists - generates DROP PROCEDURE IF EXISTS"_test = [] {
        auto const sql = drop_procedure(procedure_id<"usp_hello">{}).if_exists().build_sql();
        expect(sql == "DROP PROCEDURE IF EXISTS usp_hello"s) << sql;
    };

    "call_procedure no args - generates CALL name()"_test = [] {
        auto const sql = call_procedure(procedure_id<"usp_hello">{}).build_sql();
        expect(sql == "CALL usp_hello()"s) << sql;
    };

    "call_procedure with args - generates CALL name(args)"_test = [] {
        auto const sql = call_procedure(procedure_id<"usp_add">{}, "1, 2").build_sql();
        expect(sql == "CALL usp_add(1, 2)"s) << sql;
    };

    "create_procedure instance-based - deduces Name from named variable"_test = [] {
        constexpr auto proc = procedure_id<"usp_greet">{};
        auto const sql = create_procedure(proc, "IN name VARCHAR(50)", "SELECT CONCAT('Hello, ', name);").build_sql();
        expect(sql == "CREATE PROCEDURE usp_greet(IN name VARCHAR(50))\nBEGIN\nSELECT CONCAT('Hello, ', name);\nEND"s)
            << sql;
    };

    "drop_procedure instance-based - deduces Name from named variable"_test = [] {
        constexpr auto proc = procedure_id<"usp_greet">{};
        auto const sql = drop_procedure(proc).build_sql();
        expect(sql == "DROP PROCEDURE usp_greet"s) << sql;
    };

    "call_procedure instance-based - deduces Name from named variable"_test = [] {
        constexpr auto proc = procedure_id<"usp_greet">{};
        auto const sql = call_procedure(proc, "'World'").build_sql();
        expect(sql == "CALL usp_greet('World')"s) << sql;
    };
};

// ===================================================================
// DDL — Triggers
// ===================================================================

suite<"DDL triggers"> ddl_trigger_suite = [] {
    "create_trigger BEFORE INSERT - generates correct SQL"_test = [] {
        auto const sql = create_trigger<test_table>(trigger_id<"trg_before_insert">{}, TriggerTiming::Before,
                                                    TriggerEvent::Insert, "SET NEW.name = UPPER(NEW.name);")
                             .build_sql();
        expect(sql ==
               "CREATE TRIGGER trg_before_insert BEFORE INSERT ON test_table FOR EACH ROW\n"
               "SET NEW.name = UPPER(NEW.name);"s)
            << sql;
    };

    "create_trigger AFTER UPDATE - generates correct SQL"_test = [] {
        auto const sql =
            create_trigger<test_table>(trigger_id<"trg_after_update">{}, TriggerTiming::After, TriggerEvent::Update,
                                       "INSERT INTO audit_log VALUES (OLD.id, NOW());")
                .build_sql();
        expect(sql ==
               "CREATE TRIGGER trg_after_update AFTER UPDATE ON test_table FOR EACH ROW\n"
               "INSERT INTO audit_log VALUES (OLD.id, NOW());"s)
            << sql;
    };

    "drop_trigger - generates DROP TRIGGER"_test = [] {
        auto const sql = drop_trigger<test_table>(trigger_id<"trg_before_insert">{}).build_sql();
        expect(sql == "DROP TRIGGER trg_before_insert"s) << sql;
    };

    "drop_trigger.if_exists - generates DROP TRIGGER IF EXISTS"_test = [] {
        auto const sql = drop_trigger<test_table>(trigger_id<"trg_before_insert">{}).if_exists().build_sql();
        expect(sql == "DROP TRIGGER IF EXISTS trg_before_insert"s) << sql;
    };
};
