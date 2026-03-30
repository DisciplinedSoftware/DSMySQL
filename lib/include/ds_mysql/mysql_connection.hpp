#pragma once

#include <mysql.h>

#include <algorithm>
#include <cassert>
#include <cctype>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "ds_mysql/charset_name.hpp"
#include "ds_mysql/config.hpp"
#include "ds_mysql/connect_options.hpp"
#include "ds_mysql/credentials.hpp"
#include "ds_mysql/database_name.hpp"
#include "ds_mysql/host_name.hpp"
#include "ds_mysql/port_number.hpp"
#include "ds_mysql/prepared_statement.hpp"
#include "ds_mysql/sql_ddl.hpp"
#include "ds_mysql/sql_dml.hpp"
#include "ds_mysql/sql_dql.hpp"

namespace ds_mysql {

// ===================================================================
// sql_raw — explicit escape hatch for raw SQL strings.
//
// Use this only when none of the typed builders in sql_ddl.hpp, sql_dml.hpp,
// or sql_dql.hpp cover your
// case. Always returns std::expected<std::vector<std::vector<std::optional<std::string>>>, std::string>.
// Each inner vector is a row; each element is a nullable cell value.
// DDL / DML statements that produce no result set return an empty outer vector.
//
//   db.query(sql_raw{"SELECT @@version"})
//   db.query(sql_raw{"DROP TABLE foo"})
// ===================================================================

class sql_raw {
public:
    explicit sql_raw(std::string s) : sql(std::move(s)) {
    }
    explicit sql_raw(std::string_view s) : sql(s) {
    }
    // NOLINTNEXTLINE(google-explicit-constructor)
    explicit sql_raw(char const* s) : sql(s) {
    }

    [[nodiscard]] std::string const& build_sql() const noexcept {
        return sql;
    }

private:
    std::string sql;
};

// ===================================================================
// detail — schema validation helpers.
// ===================================================================

namespace detail {

// Normalize a C++ SQL type string for comparison with MySQL DESCRIBE output.
// MySQL stores BOOLEAN as TINYINT(1), so we map it accordingly.
[[nodiscard]] inline std::string normalize_cpp_sql_type(std::string const& cpp_type) {
    if (cpp_type == "BOOLEAN") {
        return "TINYINT(1)";
    }
    return cpp_type;
}

// Normalize a MySQL DESCRIBE type string to uppercase for comparison.
[[nodiscard]] inline std::string normalize_db_sql_type(std::string const& db_type) {
    std::string upper;
    upper.resize(db_type.size());
    std::transform(db_type.begin(), db_type.end(), upper.begin(), [](unsigned char c) {
        return static_cast<char>(std::toupper(c));
    });
    return upper;
}

using describe_row_type =
    std::tuple<std::string, std::string, std::string, std::string, std::optional<std::string>, std::string>;

// Compare one DESCRIBE row against the C++ field definition at compile-time index I.
// Appends any mismatch descriptions to `errors`.
template <typename T, std::size_t I>
void validate_field(describe_row_type const& row, std::string& errors) {
    using field_type = boost::pfr::tuple_element_t<I, T>;
    auto const& [db_field, db_type, db_null, db_key, db_default, db_extra] = row;

    std::string const expected_name(field_schema<T, I>::name());
    std::string const sql_override = field_sql_type_override<T, I>();
    std::string const expected_type =
        normalize_cpp_sql_type(sql_override.empty() ? sql_type_for<field_type>() : sql_override);
    bool const expected_nullable = is_field_nullable_v<field_type>;

    std::string const actual_type = normalize_db_sql_type(db_type);
    bool const actual_nullable = (db_null == "YES");

    if (db_field != expected_name) {
        errors += "  column " + std::to_string(I) + ": name expected '" + expected_name + "', got '" + db_field + "'\n";
    }
    if (actual_type != expected_type) {
        errors += "  column " + std::to_string(I) + " '" + expected_name + "': type expected '" + expected_type +
                  "', got '" + actual_type + "'\n";
    }
    if (actual_nullable != expected_nullable) {
        errors += "  column " + std::to_string(I) + " '" + expected_name + "': nullability expected " +
                  (expected_nullable ? "nullable" : "NOT NULL") + ", got " +
                  (actual_nullable ? "nullable" : "NOT NULL") + "\n";
    }
}

template <typename T, std::size_t... Is>
void validate_table_fields(std::vector<describe_row_type> const& rows, std::string& errors,
                           std::index_sequence<Is...>) {
    (validate_field<T, Is>(rows[Is], errors), ...);
}

}  // namespace detail

// ===================================================================
// mysql_connection — thin wrapper around the MySQL C API.
//
// Provides type-safe query() and execute() methods that accept any builder
// from sql_ddl.hpp, sql_dml.hpp, and sql_dql.hpp, plus a sql_raw overload
// for unhandled cases.
//
// Factory methods:
//   mysql_connection::connect(host, database, credentials, port)
//   mysql_connection::connect(mysql_config)
//
// Typed query — SELECT, DESCRIBE, COUNT, aggregate queries:
//   db.query(select<symbol::id, symbol::ticker>().from<symbol>())
//   db.query(describe<T>())
//   db.query(count<T>().where(...))
//   → returns std::expected<std::vector<result_row_type>, std::string>
//
// DDL / DML — CREATE, INSERT, UPDATE, DELETE, DROP, ...:
//   db.execute(create_table<T>().if_not_exists())
//   db.execute(insert_into<T>().values(row))
//   → returns std::expected<uint64_t, std::string> (affected row count)
//
// Raw SQL (not type-safe — use only as a last resort):
//   db.query(sql_raw{"DROP TABLE foo"})     → empty vector (DDL / DML)
//   db.query(sql_raw{"SELECT @@version"})  → vector<vector<optional<string>>>
// ===================================================================

class mysql_connection {
public:
    mysql_connection(mysql_connection const&) = delete;
    mysql_connection& operator=(mysql_connection const&) = delete;

    mysql_connection(mysql_connection&&) noexcept = default;
    mysql_connection& operator=(mysql_connection&&) noexcept = default;
    ~mysql_connection() = default;

    // Factory: connect with explicit parameters.
    [[nodiscard]] static std::expected<mysql_connection, std::string> connect(host_name const& host,
                                                                              database_name const& database,
                                                                              auth_credentials const& credentials,
                                                                              port_number port = default_mysql_port) {
        return connect_impl(host, database, credentials, port, nullptr);
    }

    // Factory: connect with explicit parameters and pre-connect options.
    [[nodiscard]] static std::expected<mysql_connection, std::string> connect(host_name const& host,
                                                                              database_name const& database,
                                                                              auth_credentials const& credentials,
                                                                              port_number port,
                                                                              connect_options const& options) {
        return connect_impl(host, database, credentials, port, &options);
    }

    // Factory: connect with a mysql_config object.
    [[nodiscard]] static std::expected<mysql_connection, std::string> connect(mysql_config const& config) {
        return connect_impl(config.host(), config.database(), config.credentials(), config.port(), nullptr);
    }

    // Factory: connect with a mysql_config object and pre-connect options.
    [[nodiscard]] static std::expected<mysql_connection, std::string> connect(mysql_config const& config,
                                                                              connect_options const& options) {
        return connect_impl(config.host(), config.database(), config.credentials(), config.port(), &options);
    }

    // SELECT, DESCRIBE, COUNT, aggregate queries — deserializes rows into typed tuples.
    // Each inner vector is a row; each element is a nullable cell value.
    // DDL / DML statements that produce no result set return as an empty outer vector.
    template <TypedSelectQuery Stmt>
    [[nodiscard]] auto query(Stmt const& stmt) const
        -> std::expected<std::vector<typename Stmt::result_row_type>, std::string> {
        using result_row_type = typename Stmt::result_row_type;
        return query(stmt.build_sql())
            .and_then([this]() {
                return store_result();
            })
            .and_then([this](result_ptr result) -> std::expected<std::vector<result_row_type>, std::string> {
                if (!result) {
                    // DDL/DML — no result set
                    return {};
                }

                return convert_result<result_row_type>(*result);
            });
    }

    using raw_result_row_type = std::vector<std::optional<std::string>>;

    // Raw SQL — not type-safe, use only when no typed builder covers the case.
    // Each inner vector is a row; each element is a nullable cell value.
    // DDL / DML statements that produce no result set return as an empty outer vector.
    [[nodiscard]] std::expected<std::vector<raw_result_row_type>, std::string> query(sql_raw const& stmt) const {
        return query(stmt.build_sql())
            .and_then([this]() {
                return store_result();
            })
            .and_then([this](result_ptr result) -> std::expected<std::vector<raw_result_row_type>, std::string> {
                if (!result) {
                    // DDL/DML — no result set
                    return {};
                }

                return convert_result_raw(*result);
            });
    }

    // DDL / DML — CREATE, INSERT, UPDATE, DELETE, DROP, ...
    // Multi-statement SQL separated by semicolons is supported.
    // Returns std::expected<uint64_t, std::string> where the value is mysql_affected_rows().
    template <SqlExecuteStatement Stmt>
    [[nodiscard]] std::expected<uint64_t, std::string> execute(Stmt const& stmt) const {
        std::string const sql = stmt.build_sql();  // must outlive the string_views below
        std::string_view remaining{sql};
        uint64_t total_affected = 0;
        while (true) {
            auto const semi = remaining.find(';');
            auto const fragment = (semi != std::string_view::npos) ? remaining.substr(0, semi) : remaining;
            if (fragment.find_first_not_of(" \t\n\r") != std::string_view::npos) {
                std::string const statement{fragment};
                if (mysql_query(connection_.get(), statement.c_str()) != 0) {
                    return std::unexpected(last_error() + "\n  Statement: " + statement);
                }
                total_affected += mysql_affected_rows(connection_.get());
            }
            if (semi == std::string_view::npos) {
                break;
            }
            remaining = remaining.substr(semi + 1);
        }
        return total_affected;
    }

    // Returns the AUTO_INCREMENT id generated by the most recent INSERT statement.
    [[nodiscard]] uint64_t last_insert_id() const noexcept {
        return mysql_insert_id(connection_.get());
    }

    // ---------------------------------------------------------------
    // Transaction control (direct C API — more efficient than SQL strings)
    // ---------------------------------------------------------------

    // Enable or disable autocommit mode.
    [[nodiscard]] std::expected<void, std::string> autocommit(bool enable) const {
        if (mysql_autocommit(connection_.get(), enable)) {
            return std::unexpected(last_error());
        }
        return {};
    }

    // Commit the current transaction.
    [[nodiscard]] std::expected<void, std::string> commit() const {
        if (mysql_commit(connection_.get())) {
            return std::unexpected(last_error());
        }
        return {};
    }

    // Rollback the current transaction.
    [[nodiscard]] std::expected<void, std::string> rollback() const {
        if (mysql_rollback(connection_.get())) {
            return std::unexpected(last_error());
        }
        return {};
    }

    // ---------------------------------------------------------------
    // Connection health & navigation
    // ---------------------------------------------------------------

    // Check if the connection is alive; reconnect if down.
    [[nodiscard]] std::expected<void, std::string> ping() const {
        if (mysql_ping(connection_.get()) != 0) {
            return std::unexpected(last_error());
        }
        return {};
    }

    // Switch the default database for this connection.
    [[nodiscard]] std::expected<void, std::string> select_db(database_name const& database) const {
        if (mysql_select_db(connection_.get(), database.to_string().c_str()) != 0) {
            return std::unexpected(last_error());
        }
        return {};
    }

    // Reset the connection to a clean state without re-authenticating.
    [[nodiscard]] std::expected<void, std::string> reset_connection() const {
        if (mysql_reset_connection(connection_.get()) != 0) {
            return std::unexpected(last_error());
        }
        return {};
    }

    // ---------------------------------------------------------------
    // Query metadata
    // ---------------------------------------------------------------

    // Number of warnings generated by the most recent statement.
    [[nodiscard]] uint32_t warning_count() const noexcept {
        return mysql_warning_count(connection_.get());
    }

    // Information string about the most recent INSERT/UPDATE/DELETE/ALTER.
    // Returns std::nullopt when the server reports no info string.
    [[nodiscard]] std::optional<std::string> info() const noexcept {
        char const* s = mysql_info(connection_.get());
        if (s == nullptr) {
            return std::nullopt;
        }
        return std::string(s);
    }

    // ---------------------------------------------------------------
    // Server info
    // ---------------------------------------------------------------

    // Server version as an integer (e.g. 80036 for 8.0.36).
    [[nodiscard]] uint64_t server_version() const noexcept {
        return mysql_get_server_version(connection_.get());
    }

    // Server version as a human-readable string (e.g. "8.0.36").
    [[nodiscard]] std::string_view server_info() const noexcept {
        return mysql_get_server_info(connection_.get());
    }

    // Server uptime, threads, queries, and other status information.
    [[nodiscard]] std::expected<std::string, std::string> stat() const {
        char const* s = mysql_stat(connection_.get());
        if (s == nullptr) {
            return std::unexpected(last_error());
        }
        return std::string(s);
    }

    // Connection thread ID — useful for debugging and KILL commands.
    [[nodiscard]] uint64_t thread_id() const noexcept {
        return mysql_thread_id(connection_.get());
    }

    // ---------------------------------------------------------------
    // Character set
    // ---------------------------------------------------------------

    // Current character set name for this connection.
    [[nodiscard]] charset_name character_set() const noexcept {
        return charset_name{mysql_character_set_name(connection_.get())};
    }

    // Change the default character set for this connection.
    [[nodiscard]] std::expected<void, std::string> set_character_set(charset_name const& cs) const {
        if (mysql_set_character_set(connection_.get(), cs.to_string().c_str()) != 0) {
            return std::unexpected(last_error());
        }
        return {};
    }

    // ---------------------------------------------------------------
    // String escaping
    // ---------------------------------------------------------------

    // Escape special characters using the connection's character set.
    // The result is safe to embed in a SQL string literal.
    [[nodiscard]] std::string escape_string(std::string_view input) const {
        std::string out(input.size() * 2 + 1, '\0');
        unsigned long const len = mysql_real_escape_string(connection_.get(), out.data(), input.data(),
                                                           static_cast<unsigned long>(input.size()));
        out.resize(len);
        return out;
    }

    // ---------------------------------------------------------------
    // Prepared statements
    // ---------------------------------------------------------------

    // Prepare a SQL string for repeated execution with bound parameters.
    [[nodiscard]] std::expected<prepared_statement, std::string> prepare(std::string_view sql) const {
        auto stmt = std::unique_ptr<MYSQL_STMT, prepared_statement::stmt_deleter>(mysql_stmt_init(connection_.get()));
        if (!stmt) {
            return std::unexpected(last_error());
        }
        if (mysql_stmt_prepare(stmt.get(), sql.data(), static_cast<unsigned long>(sql.size())) != 0) {
            return std::unexpected(std::string(mysql_stmt_error(stmt.get())));
        }
        return prepared_statement{std::move(stmt)};
    }

    // Prepare a typed query builder for repeated execution.
    template <SqlBuilder Stmt>
    [[nodiscard]] std::expected<prepared_statement, std::string> prepare(Stmt const& stmt) const {
        return prepare(stmt.build_sql());
    }

    // Validate that the C++ table struct T matches the live schema in the database.
    //
    // Runs DESCRIBE <table> and checks:
    //   - column count
    //   - column name (by position)
    //   - SQL type (C++ type mapping vs database-reported type)
    //   - nullability (optional vs NOT NULL)
    //
    // Returns std::expected<void, std::string>.
    // On mismatch, the error string lists every discrepancy found.
    template <ValidTable T>
    [[nodiscard]] std::expected<void, std::string> validate_table(T const&) const {
        return validate_table<T>();
    }

    template <ValidTable T>
    [[nodiscard]] std::expected<void, std::string> validate_table() const {
        auto describe_result = query(describe(T{}));
        if (!describe_result) {
            return std::unexpected("Failed to describe table '" +
                                   std::string(table_name_for<T>::value().to_string_view()) +
                                   "': " + describe_result.error());
        }

        constexpr std::size_t expected_cols = boost::pfr::tuple_size_v<T>;
        const std::size_t actual_cols = describe_result->size();
        if (actual_cols != expected_cols) {
            return std::unexpected("Table '" + std::string(table_name_for<T>::value().to_string_view()) +
                                   "': column count mismatch: C++ struct has " + std::to_string(expected_cols) +
                                   " column(s), database has " + std::to_string(actual_cols));
        }

        std::string errors;
        detail::validate_table_fields<T>(*describe_result, errors, std::make_index_sequence<expected_cols>{});
        if (!errors.empty()) {
            return std::unexpected("Table '" + std::string(table_name_for<T>::value().to_string_view()) +
                                   "' schema mismatch:\n" + errors);
        }
        return {};
    }

    // Validate all tables declared in a database schema struct against the live database.
    //
    // DB must satisfy the Database concept (inherit from database_schema).
    // Tables are discovered via database_tables<DB>::type — either by specializing
    // database_tables<DB>, or by defining a nested `using tables = std::tuple<...>;`
    // inside the database struct.
    //
    // Returns std::expected<void, std::string>.
    // On mismatch, the error string aggregates every discrepancy across all tables.
    template <Database DB>
    [[nodiscard]] std::expected<void, std::string> validate_database(DB const&) const {
        return validate_database<DB>();
    }

    template <Database DB>
    [[nodiscard]] std::expected<void, std::string> validate_database() const {
        using tables_tuple = typename database_tables<DB>::type;
        std::string errors;
        validate_all_tables<tables_tuple>(errors, std::make_index_sequence<std::tuple_size_v<tables_tuple>>{});
        if (!errors.empty()) {
            return std::unexpected("Database '" + std::string(database_name_for<DB>::value()) +
                                   "' schema validation failed:\n" + errors);
        }
        return {};
    }

private:
    using result_ptr = std::unique_ptr<MYSQL_RES, decltype(&mysql_free_result)>;

    [[nodiscard]] std::expected<void, std::string> query(std::string const& sql) const {
        if (mysql_query(connection_.get(), sql.c_str()) != 0) {
            return std::unexpected(last_error());
        }

        return {};
    }

    [[nodiscard]] std::expected<result_ptr, std::string> store_result() const {
        auto result = result_ptr(mysql_store_result(connection_.get()), mysql_free_result);
        if (!result) {
            if (mysql_errno(connection_.get()) != 0) {
                return std::unexpected(last_error());
            }

            // Query had no result set (e.g. DDL/DML)
            return result_ptr(nullptr, mysql_free_result);
        }

        return result;
    }

    template <typename ResultRowType>
    [[nodiscard]] auto convert_result(MYSQL_RES& result) const
        -> std::expected<std::vector<ResultRowType>, std::string> {
        constexpr std::size_t expected_cols = std::tuple_size_v<ResultRowType>;
        unsigned int const num_fields = mysql_num_fields(&result);
        if (num_fields != expected_cols) {
            return std::unexpected(std::string("Column count mismatch: expected ") + std::to_string(expected_cols) +
                                   ", got " + std::to_string(num_fields));
        }

        std::vector<ResultRowType> results;
        results.reserve(mysql_num_rows(&result));
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(&result)) != nullptr) {
            unsigned long const* lengths = mysql_fetch_lengths(&result);
            if (lengths == nullptr) {
                return std::unexpected(last_error());
            }
            results.push_back(
                deserialize_mysql_row<ResultRowType>(row, lengths, std::make_index_sequence<expected_cols>{}));
        }

        return results;
    }

    [[nodiscard]] auto convert_result_raw(MYSQL_RES& result) const
        -> std::expected<std::vector<raw_result_row_type>, std::string> {
        unsigned int const num_fields = mysql_num_fields(&result);
        std::vector<raw_result_row_type> results;

        MYSQL_ROW row;
        while ((row = mysql_fetch_row(&result)) != nullptr) {
            raw_result_row_type row_data;
            row_data.reserve(num_fields);
            for (unsigned int i = 0; i < num_fields; ++i) {
                row_data.emplace_back(row[i] ? std::optional<std::string>{row[i]} : std::nullopt);
            }
            results.push_back(std::move(row_data));
        }

        return results;
    }

    template <typename FieldType>
    [[nodiscard]] static FieldType deserialize_mysql_cell(char const* cell, unsigned long length) {
        if constexpr (is_optional_v<FieldType>) {
            using inner_type = unwrap_optional_t<FieldType>;
            if (cell == nullptr) {
                return std::nullopt;
            }
            return std::optional<inner_type>{
                detail::from_mysql_value_nonnull<inner_type>(std::string_view{cell, length})};
        } else {
            assert(cell != nullptr && "Non-optional field received NULL from database — schema mismatch?");
            return detail::from_mysql_value_nonnull<FieldType>(std::string_view{cell, length});
        }
    }

    template <typename RowType, std::size_t... Is>
    [[nodiscard]] static RowType deserialize_mysql_row(MYSQL_ROW row, unsigned long const* lengths,
                                                       std::index_sequence<Is...>) {
        return RowType{deserialize_mysql_cell<std::tuple_element_t<Is, RowType>>(row[Is], lengths[Is])...};
    }

    template <typename table_t>
    void validate_one_table(std::string& errors) const {
        auto const result = validate_table<table_t>();
        if (!result) {
            errors += result.error() + "\n";
        }
    }

    template <typename TablesTuple, std::size_t... Is>
    void validate_all_tables(std::string& errors, std::index_sequence<Is...>) const {
        (validate_one_table<std::tuple_element_t<Is, TablesTuple>>(errors), ...);
    }

    [[nodiscard]] static std::expected<mysql_connection, std::string> connect_impl(host_name const& host,
                                                                                   database_name const& database,
                                                                                   auth_credentials const& credentials,
                                                                                   port_number port,
                                                                                   connect_options const* options) {
        auto conn = std::unique_ptr<MYSQL, decltype(&mysql_close)>(mysql_init(nullptr), mysql_close);
        if (!conn) {
            return std::unexpected(std::string("mysql_init failed"));
        }

        if (options != nullptr) {
            auto result = options->apply(conn.get());
            if (!result) {
                return std::unexpected(result.error());
            }
        }

        if (!mysql_real_connect(conn.get(), host.to_string().c_str(), credentials.user().to_string().c_str(),
                                credentials.password().to_string().c_str(), database.to_string().c_str(),
                                port.to_unsigned_int(), nullptr, 0)) {
            return std::unexpected(std::string(mysql_error(conn.get())));
        }

        return mysql_connection{std::move(conn)};
    }

    explicit mysql_connection(std::unique_ptr<MYSQL, decltype(&mysql_close)> connection)
        : connection_(std::move(connection)) {
    }

    [[nodiscard]] std::string last_error() const {
        return std::string(mysql_error(connection_.get()));
    }

    std::unique_ptr<MYSQL, decltype(&mysql_close)> connection_;
};

// ===================================================================
// transaction_guard — RAII scoped transaction helper.
//
// This is a C++ resource-management utility, not a MySQL semantic.
// Disables autocommit on construction and automatically rolls back on
// destruction unless commit() has been called.  Move-only.
//
//   {
//       auto guard = transaction_guard::begin(conn);
//       if (!guard) { /* handle error */ }
//       conn.execute(insert_into(t{}).values(row));
//       auto result = guard->commit();
//       if (!result) { /* handle commit error */ }
//   }
//   // If commit() was never called, destructor rolls back.
// ===================================================================

class transaction_guard {
public:
    transaction_guard(transaction_guard const&) = delete;
    transaction_guard& operator=(transaction_guard const&) = delete;

    transaction_guard(transaction_guard&& other) noexcept : conn_(other.conn_), committed_(other.committed_) {
        other.conn_ = nullptr;
    }

    transaction_guard& operator=(transaction_guard&& other) noexcept {
        if (this != &other) {
            if (conn_ && !committed_) {
                (void)conn_->rollback();
            }
            conn_ = other.conn_;
            committed_ = other.committed_;
            other.conn_ = nullptr;
        }
        return *this;
    }

    ~transaction_guard() {
        if (conn_ && !committed_) {
            (void)conn_->rollback();
        }
    }

    // Factory: begin a transaction by disabling autocommit.
    [[nodiscard]] static std::expected<transaction_guard, std::string> begin(mysql_connection const& conn) {
        auto result = conn.autocommit(false);
        if (!result) {
            return std::unexpected(result.error());
        }
        return transaction_guard{&conn};
    }

    // Commit the transaction and re-enable autocommit.
    [[nodiscard]] std::expected<void, std::string> commit() {
        if (!conn_) {
            return std::unexpected("transaction_guard: no active connection");
        }
        auto result = conn_->commit();
        if (!result) {
            return std::unexpected(result.error());
        }
        committed_ = true;
        // Re-enable autocommit so the connection returns to its default state.
        return conn_->autocommit(true);
    }

    // Explicitly roll back the transaction and re-enable autocommit.
    [[nodiscard]] std::expected<void, std::string> rollback() {
        if (!conn_) {
            return std::unexpected("transaction_guard: no active connection");
        }
        auto result = conn_->rollback();
        if (!result) {
            return std::unexpected(result.error());
        }
        committed_ = true;  // prevent double-rollback in destructor
        return conn_->autocommit(true);
    }

    // Check whether commit() has been called.
    [[nodiscard]] bool is_committed() const noexcept {
        return committed_;
    }

private:
    explicit transaction_guard(mysql_connection const* conn) : conn_(conn) {
    }

    mysql_connection const* conn_ = nullptr;
    bool committed_ = false;
};

}  // namespace ds_mysql
