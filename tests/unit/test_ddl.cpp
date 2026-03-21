#include <boost/ut.hpp>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

#include "ds_mysql/sql.hpp"
#include "ds_mysql/sql_extension.hpp"
#include "ds_mysql/text_field.hpp"

using namespace boost::ut;
using namespace ds_mysql;
using namespace std::string_literals;

// ===================================================================
// Test fixtures
// ===================================================================

namespace {
struct test_table {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(name, varchar_field<255>)
    COLUMN_FIELD(tag, std::optional<varchar_field<64>>)
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
    COLUMN_FIELD(updated_at, sql_timestamp)
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
    };

    "create_table temporal types - emits DATETIME and TIMESTAMP definitions"_test = [] {
        auto const sql = create_table<temporal_table>().build_sql();
        expect(sql ==
               "CREATE TABLE temporal_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    created_at DATETIME NOT NULL,\n"
               "    updated_at TIMESTAMP NOT NULL\n"
               ");\n"s)
            << sql;
    };

    "create_table with numeric sql formatting options - emits FLOAT/DOUBLE/DECIMAL definitions"_test = [] {
        auto const sql = create_table<numeric_format_table>().build_sql();
        expect(sql ==
               "CREATE TABLE numeric_format_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    price FLOAT(12,4) NOT NULL,\n"
               "    ratio DOUBLE(16,8) NOT NULL,\n"
               "    amount DECIMAL(18,6)\n"
               ");\n"s)
            << sql;
    };

    // -------------------------------------------------------------------
    // create_table
    // -------------------------------------------------------------------

    "create_table - generates CREATE TABLE"_test = [] {
        auto const sql = create_table<test_table>().build_sql();
        expect(sql ==
               "CREATE TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ");\n"s)
            << sql;
    };

    "create_table.if_not_exists - generates CREATE TABLE IF NOT EXISTS"_test = [] {
        auto const sql = create_table<test_table>().if_not_exists().build_sql();
        expect(sql ==
               "CREATE TABLE IF NOT EXISTS test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ");\n"s)
            << sql;
    };

    "create_table - SQL contains first column as PRIMARY KEY"_test = [] {
        auto const sql = create_table<test_table>().build_sql();
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
        auto const sql = drop_table<test_table>().build_sql();
        expect(sql == "DROP TABLE test_table;\n"s) << sql;
    };

    "drop_table.if_exists - generates DROP TABLE IF EXISTS"_test = [] {
        auto const sql = drop_table<test_table>().if_exists().build_sql();
        expect(sql == "DROP TABLE IF EXISTS test_table;\n"s) << sql;
    };

    // -------------------------------------------------------------------
    // .then() chaining
    // -------------------------------------------------------------------

    "drop_table.if_exists.then.create_table - generates both statements"_test = [] {
        auto const sql =
            drop_table<test_table>().if_exists().then().create_table<test_table>().if_not_exists().build_sql();
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
        auto const sql = drop_table<test_table>().then().create_table<test_table>().build_sql();
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
        auto const sql = create_temporary_table<test_table>().build_sql();
        expect(sql ==
               "CREATE TEMPORARY TABLE test_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    name VARCHAR(255) NOT NULL,\n"
               "    tag VARCHAR(64)\n"
               ");\n"s)
            << sql;
    };

    "create_temporary_table.if_not_exists - generates CREATE TEMPORARY TABLE IF NOT EXISTS"_test = [] {
        auto const sql = create_temporary_table<test_table>().if_not_exists().build_sql();
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
        auto const sql = drop_temporary_table<test_table>().build_sql();
        expect(sql == "DROP TEMPORARY TABLE test_table;\n"s) << sql;
    };

    "drop_temporary_table.if_exists - generates DROP TEMPORARY TABLE IF EXISTS"_test = [] {
        auto const sql = drop_temporary_table<test_table>().if_exists().build_sql();
        expect(sql == "DROP TEMPORARY TABLE IF EXISTS test_table;\n"s) << sql;
    };

    // -------------------------------------------------------------------
    // .then() chaining with TEMPORARY TABLE
    // -------------------------------------------------------------------

    "drop_temporary_table.if_exists.then.create_temporary_table - generates both statements"_test = [] {
        auto const sql = drop_temporary_table<test_table>()
                             .if_exists()
                             .then()
                             .create_temporary_table<test_table>()
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
        auto const sql = drop_temporary_table<test_table>().then().create_temporary_table<test_table>().build_sql();
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
    // create_table<T>().as(select_query)
    // -------------------------------------------------------------------

    "create_table.as(select) - generates CREATE TABLE AS SELECT"_test = [] {
        auto const sql =
            create_table<new_table>().as(select<test_table::id, test_table::name>().from<test_table>()).build_sql();
        expect(sql == "CREATE TABLE new_table AS SELECT id, name FROM test_table;\n"s) << sql;
    };

    "create_table.if_not_exists.as(select) - generates CREATE TABLE IF NOT EXISTS AS SELECT"_test = [] {
        auto const sql = create_table<new_table>()
                             .if_not_exists()
                             .as(select<test_table::id, test_table::name>().from<test_table>())
                             .build_sql();
        expect(sql == "CREATE TABLE IF NOT EXISTS new_table AS SELECT id, name FROM test_table;\n"s) << sql;
    };

    "create_temporary_table.as(select) - generates CREATE TEMPORARY TABLE AS SELECT"_test = [] {
        auto const sql = create_temporary_table<new_table>()
                             .as(select<test_table::id, test_table::name>().from<test_table>())
                             .build_sql();
        expect(sql == "CREATE TEMPORARY TABLE new_table AS SELECT id, name FROM test_table;\n"s) << sql;
    };

    "create_temporary_table.if_not_exists.as(select) - generates CREATE TEMPORARY TABLE IF NOT EXISTS AS SELECT"_test =
        [] {
            auto const sql = create_temporary_table<new_table>()
                                 .if_not_exists()
                                 .as(select<test_table::id, test_table::name>().from<test_table>())
                                 .build_sql();
            expect(sql == "CREATE TEMPORARY TABLE IF NOT EXISTS new_table AS SELECT id, name FROM test_table;\n"s)
                << sql;
        };

    "create_table.as(select).with_where - generates CREATE TABLE AS SELECT WHERE"_test = [] {
        auto const sql = create_table<new_table>()
                             .as(select<test_table::id, test_table::name>().from<test_table>().where(
                                 equal<test_table::name>(test_table::name{"name"})))
                             .build_sql();
        expect(sql == "CREATE TABLE new_table AS SELECT id, name FROM test_table WHERE name = 'name';\n"s) << sql;
    };

    "create_table.as(select).with_where(is_not_null) - generates CREATE TABLE AS SELECT WHERE"_test = [] {
        auto const sql =
            create_table<new_table>()
                .as(select<test_table::id, test_table::name>().from<test_table>().where(is_not_null<test_table::tag>()))
                .build_sql();
        expect(sql == "CREATE TABLE new_table AS SELECT id, name FROM test_table WHERE tag IS NOT NULL;\n"s) << sql;
    };

    "create_table.as(select) with fluent table attributes"_test = [] {
        auto const sql = create_table<new_table>()
                             .as(select<test_table::id, test_table::name>().from<test_table>())
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
        auto const sql = create_table<test_table>()
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

    "drop_table.then.create_table.as(select) - chains DROP and CREATE AS SELECT"_test = [] {
        auto const sql = drop_table<new_table>()
                             .if_exists()
                             .then()
                             .create_table<new_table>()
                             .as(select<test_table::id, test_table::name>().from<test_table>())
                             .build_sql();
        expect(sql ==
               "DROP TABLE IF EXISTS new_table;\n"
               "CREATE TABLE new_table AS SELECT id, name FROM test_table;\n"s)
            << sql;
    };
};

// ===================================================================
// DDL Foreign Keys
// ===================================================================

namespace {
struct child_table {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(parent_id, uint32_t)
};

struct parent_table {
    COLUMN_FIELD(id, uint32_t)
};
}  // namespace

// Declare FK: child_table.parent_id → parent_table.id (RESTRICT behaviour)
template <>
struct ds_mysql::foreign_key_schema<child_table, 1> {
    static constexpr std::string_view referenced_table() {
        return "parent_table";
    }
    static constexpr std::string_view referenced_column() {
        return "id";
    }
    static constexpr std::string_view on_delete() {
        return "";
    }
    static constexpr std::string_view on_update() {
        return "";
    }
};

struct child_table_cascade {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(parent_id, uint32_t)
};

template <>
struct ds_mysql::foreign_key_schema<child_table_cascade, 1> {
    static constexpr std::string_view referenced_table() {
        return "parent_table";
    }
    static constexpr std::string_view referenced_column() {
        return "id";
    }
    static constexpr std::string_view on_delete() {
        return "CASCADE";
    }
    static constexpr std::string_view on_update() {
        return "CASCADE";
    }
};

suite<"DDL Foreign Keys"> ddl_foreign_keys_suite = [] {
    "create_table with FK - emits FOREIGN KEY constraint"_test = [] {
        auto const sql = create_table<child_table>().build_sql();
        expect(sql ==
               "CREATE TABLE child_table (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    parent_id INT UNSIGNED NOT NULL,\n"
               "    FOREIGN KEY (parent_id) REFERENCES parent_table(id)\n"
               ");\n"s)
            << sql;
    };

    "create_table with FK CASCADE - emits ON DELETE CASCADE ON UPDATE CASCADE"_test = [] {
        auto const sql = create_table<child_table_cascade>().build_sql();
        expect(sql ==
               "CREATE TABLE child_table_cascade (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"
               "    parent_id INT UNSIGNED NOT NULL,\n"
               "    FOREIGN KEY (parent_id) REFERENCES parent_table(id) ON DELETE CASCADE ON UPDATE CASCADE\n"
               ");\n"s)
            << sql;
    };

    "create_table without FK - no FOREIGN KEY clause"_test = [] {
        auto const sql = create_table<test_table>().build_sql();
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
// SqlStatement concept — compile-time checks
// ===================================================================

static_assert(SqlStatement<ddl_detail::create_table_builder<test_table>>,
              "create_table_builder must satisfy SqlStatement");
static_assert(SqlStatement<ddl_detail::create_table_cond_builder<test_table>>,
              "create_table_cond_builder must satisfy SqlStatement");
static_assert(SqlStatement<ddl_detail::drop_table_builder<test_table>>, "drop_table_builder must satisfy SqlStatement");
static_assert(SqlStatement<ddl_detail::drop_table_cond_builder<test_table>>,
              "drop_table_cond_builder must satisfy SqlStatement");
static_assert(SqlStatement<ddl_detail::create_table_as_builder<test_table>>,
              "create_table_as_builder must satisfy SqlStatement");

// ===================================================================
// DDL create_database
// ===================================================================

suite<"DDL CREATE DATABASE"> ddl_create_database_suite = [] {
    "create_database - generates CREATE DATABASE"_test = [] {
        auto const sql = create_database<test_db>().build_sql();
        expect(sql == "CREATE DATABASE test_db;\n"s) << sql;
    };

    "create_database.if_not_exists - generates CREATE DATABASE IF NOT EXISTS"_test = [] {
        auto const sql = create_database<test_db>().if_not_exists().build_sql();
        expect(sql == "CREATE DATABASE IF NOT EXISTS test_db;\n"s) << sql;
    };

    "database_name_for custom specialization - uses override name"_test = [] {
        auto const sql = create_database<custom_named_db>().build_sql();
        expect(sql == "CREATE DATABASE my_custom_db;\n"s) << sql;
    };

    "create_database.then.create_table - chains CREATE DATABASE and CREATE TABLE"_test = [] {
        auto const sql = create_database<test_db>()
                             .if_not_exists()
                             .then()
                             .create_table<test_db::symbol>()
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
        auto const sql = create_database<schema_bootstrap_db>()
                             .if_not_exists()
                             .then()
                             .create_all_tables<schema_bootstrap_db>()
                             .build_sql();
        expect(sql ==
               "CREATE DATABASE IF NOT EXISTS schema_bootstrap_db;\n"
               "CREATE TABLE account (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT\n"
               ");\n"
               "CREATE TABLE trade (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT\n"
               ");\n"s)
            << sql;
    };

    "create_database.then.create_all_tables.if_not_exists - emits IF NOT EXISTS for each table"_test = [] {
        auto const sql = create_database<schema_bootstrap_db>()
                             .if_not_exists()
                             .then()
                             .create_all_tables<schema_bootstrap_db>()
                             .if_not_exists()
                             .build_sql();
        expect(sql ==
               "CREATE DATABASE IF NOT EXISTS schema_bootstrap_db;\n"
               "CREATE TABLE IF NOT EXISTS account (\n"
               "    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT\n"
               ");\n"
               "CREATE TABLE IF NOT EXISTS trade (\n"
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
};

// ===================================================================
// DDL drop_database
// ===================================================================

suite<"DDL DROP DATABASE"> ddl_drop_database_suite = [] {
    "drop_database - generates DROP DATABASE"_test = [] {
        auto const sql = drop_database<test_db>().build_sql();
        expect(sql == "DROP DATABASE test_db;\n"s) << sql;
    };

    "drop_database.if_exists - generates DROP DATABASE IF EXISTS"_test = [] {
        auto const sql = drop_database<test_db>().if_exists().build_sql();
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
        auto const sql = drop_database<test_db>().if_exists().then().create_database<test_db>().build_sql();
        expect(sql ==
               "DROP DATABASE IF EXISTS test_db;\n"
               "CREATE DATABASE test_db;\n"s)
            << sql;
    };
};

suite<"DDL Extensions"> ddl_extensions_suite = [] {
    "create_view.as(select) - generates CREATE VIEW AS SELECT"_test = [] {
        auto const sql = create_view<new_table>().as(select<test_table::id>().from<test_table>()).build_sql();
        expect(sql == "CREATE VIEW new_table AS SELECT id FROM test_table;\n"s) << sql;
    };

    "create_view.or_replace.as(select) - generates CREATE OR REPLACE VIEW"_test = [] {
        auto const sql =
            create_view<new_table>().or_replace().as(select<test_table::id>().from<test_table>()).build_sql();
        expect(sql == "CREATE OR REPLACE VIEW new_table AS SELECT id FROM test_table;\n"s) << sql;
    };

    "drop_view.if_exists - generates DROP VIEW IF EXISTS"_test = [] {
        auto const sql = drop_view<new_table>().if_exists().build_sql();
        expect(sql == "DROP VIEW IF EXISTS new_table;\n"s) << sql;
    };

    "rename_table - generates RENAME TABLE old TO new"_test = [] {
        auto const sql = rename_table<test_table, renamed_table>().build_sql();
        expect(sql == "RENAME TABLE test_table TO renamed_table;\n"s) << sql;
    };

    "create_index_on - generates CREATE INDEX"_test = [] {
        auto const sql =
            create_index_on<test_table, test_table::id, test_table::name>("idx_test_table_id_name").build_sql();
        expect(sql == "CREATE INDEX idx_test_table_id_name ON test_table (id, name);\n"s) << sql;
    };

    "create_index_on.unique - generates CREATE UNIQUE INDEX"_test = [] {
        auto const sql = create_index_on<test_table, test_table::name>("uq_test_table_name").unique().build_sql();
        expect(sql == "CREATE UNIQUE INDEX uq_test_table_name ON test_table (name);\n"s) << sql;
    };

    "drop_index_on - generates DROP INDEX ON table"_test = [] {
        auto const sql = drop_index_on<test_table>("idx_test_table_id_name").build_sql();
        expect(sql == "DROP INDEX idx_test_table_id_name ON test_table;\n"s) << sql;
    };

    "alter_table - generates combined ALTER TABLE actions"_test = [] {
        auto const sql = alter_table<test_table>()
                             .add_column<test_table::name>("VARCHAR(255)")
                             .modify_column<test_table::name>("VARCHAR(512)")
                             .rename_column<test_table::name>("display_name")
                             .drop_column<test_table::name>()
                             .rename_to<renamed_table>()
                             .build_sql();
        expect(sql ==
               "ALTER TABLE test_table ADD COLUMN name VARCHAR(255) NOT NULL, MODIFY COLUMN name VARCHAR(512) NOT "
               "NULL, RENAME COLUMN name TO display_name, DROP COLUMN name, RENAME TO renamed_table;\n"s)
            << sql;
    };
};

static_assert(SqlStatement<ddl_detail::create_database_builder<test_db>>,
              "create_database_builder must satisfy SqlStatement");
static_assert(SqlStatement<ddl_detail::create_view_as_builder<new_table>>,
              "create_view_as_builder must satisfy SqlStatement");
static_assert(SqlStatement<ddl_detail::drop_view_builder<new_table>>, "drop_view_builder must satisfy SqlStatement");
static_assert(SqlStatement<ddl_detail::rename_table_builder<test_table, renamed_table>>,
              "rename_table_builder must satisfy SqlStatement");
static_assert(SqlStatement<ddl_detail::create_database_if_not_exists_builder<test_db>>,
              "create_database_if_not_exists_builder must satisfy SqlStatement");
static_assert(SqlStatement<ddl_detail::create_database_named_builder>,
              "create_database_named_builder must satisfy SqlStatement");
static_assert(SqlStatement<ddl_detail::create_database_named_if_not_exists_builder>,
              "create_database_named_if_not_exists_builder must satisfy SqlStatement");
static_assert(SqlStatement<ddl_detail::drop_database_builder<test_db>>,
              "drop_database_builder must satisfy SqlStatement");
static_assert(SqlStatement<ddl_detail::drop_database_if_exists_builder<test_db>>,
              "drop_database_if_exists_builder must satisfy SqlStatement");
static_assert(SqlStatement<ddl_detail::drop_database_named_builder>,
              "drop_database_named_builder must satisfy SqlStatement");
static_assert(SqlStatement<ddl_detail::drop_database_named_if_exists_builder>,
              "drop_database_named_if_exists_builder must satisfy SqlStatement");
static_assert(Database<test_db>, "test_db must satisfy Database concept");
static_assert(!Database<test_table>, "plain table must NOT satisfy Database concept");

// ===================================================================
// DDL — text_field column types (TEXT, MEDIUMTEXT, LONGTEXT)
// ===================================================================

namespace {
struct text_table {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(description, text_field<>)
    COLUMN_FIELD(notes, mediumtext_field)
    COLUMN_FIELD(body, longtext_field)
    COLUMN_FIELD(opt_notes, std::optional<text_field<>>)
};
}  // namespace

suite<"DDL text_field types"> ddl_text_field_suite = [] {
    "create_table with text_field - emits TEXT column"_test = [] {
        auto const sql = create_table<text_table>().build_sql();
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
        auto const sql = create_table<text_table>().if_not_exists().build_sql();
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
        auto const sql = use<test_db>().build_sql();
        expect(sql == "USE test_db;\n"s) << sql;
    };

    "use with custom name - uses override name"_test = [] {
        auto const sql = use<custom_named_db>().build_sql();
        expect(sql == "USE my_custom_db;\n"s) << sql;
    };

    "create_database.then.use - chains CREATE DATABASE and USE"_test = [] {
        auto const sql = create_database<test_db>().if_not_exists().then().use<test_db>().build_sql();
        expect(sql ==
               "CREATE DATABASE IF NOT EXISTS test_db;\n"
               "USE test_db;\n"s)
            << sql;
    };

    "create_database.then.use.then.create_table - full setup chain"_test = [] {
        auto const sql = create_database<test_db>()
                             .if_not_exists()
                             .then()
                             .use<test_db>()
                             .then()
                             .create_table<test_db::symbol>()
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

static_assert(SqlStatement<ddl_detail::use_builder<test_db>>, "use_builder must satisfy SqlStatement");