#pragma once

#include <mysql.h>

#include <cassert>
#include <cstring>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <vector>

#include "ds_mysql/column_field.hpp"
#include "ds_mysql/schema_generator.hpp"
#include "ds_mysql/sql_numeric.hpp"
#include "ds_mysql/sql_temporal.hpp"
#include "ds_mysql/sql_text.hpp"
#include "ds_mysql/sql_varchar.hpp"

namespace ds_mysql {

// ===================================================================
// prepared_statement — RAII wrapper for MySQL prepared statements.
//
// Wraps MYSQL_STMT* with type-safe parameter binding and result
// fetching. Move-only, automatically closes the statement handle
// on destruction.
//
// Created via mysql_connection::prepare():
//   auto stmt = conn.prepare("SELECT id, name FROM t WHERE id = ?").value();
//   auto rows = stmt.query<std::tuple<uint32_t, std::string>>(42u).value();
//
// DML usage:
//   auto stmt = conn.prepare("INSERT INTO t (name) VALUES (?)").value();
//   auto affected = stmt.execute(std::string{"widget"}).value();
// ===================================================================

namespace stmt_detail {

// Map C++ types to MySQL buffer types for MYSQL_BIND.
template <typename T>
struct mysql_type_traits;

template <>
struct mysql_type_traits<bool> {
    static constexpr enum_field_types field_type = MYSQL_TYPE_TINY;
    static constexpr bool is_unsigned = false;
};

template <>
struct mysql_type_traits<int8_t> {
    static constexpr enum_field_types field_type = MYSQL_TYPE_TINY;
    static constexpr bool is_unsigned = false;
};

template <>
struct mysql_type_traits<uint8_t> {
    static constexpr enum_field_types field_type = MYSQL_TYPE_TINY;
    static constexpr bool is_unsigned = true;
};

template <>
struct mysql_type_traits<int16_t> {
    static constexpr enum_field_types field_type = MYSQL_TYPE_SHORT;
    static constexpr bool is_unsigned = false;
};

template <>
struct mysql_type_traits<uint16_t> {
    static constexpr enum_field_types field_type = MYSQL_TYPE_SHORT;
    static constexpr bool is_unsigned = true;
};

template <>
struct mysql_type_traits<int32_t> {
    static constexpr enum_field_types field_type = MYSQL_TYPE_LONG;
    static constexpr bool is_unsigned = false;
};

template <>
struct mysql_type_traits<uint32_t> {
    static constexpr enum_field_types field_type = MYSQL_TYPE_LONG;
    static constexpr bool is_unsigned = true;
};

template <>
struct mysql_type_traits<int64_t> {
    static constexpr enum_field_types field_type = MYSQL_TYPE_LONGLONG;
    static constexpr bool is_unsigned = false;
};

template <>
struct mysql_type_traits<uint64_t> {
    static constexpr enum_field_types field_type = MYSQL_TYPE_LONGLONG;
    static constexpr bool is_unsigned = true;
};

template <>
struct mysql_type_traits<float> {
    static constexpr enum_field_types field_type = MYSQL_TYPE_FLOAT;
    static constexpr bool is_unsigned = false;
};

template <>
struct mysql_type_traits<double> {
    static constexpr enum_field_types field_type = MYSQL_TYPE_DOUBLE;
    static constexpr bool is_unsigned = false;
};

template <>
struct mysql_type_traits<std::string> {
    static constexpr enum_field_types field_type = MYSQL_TYPE_STRING;
    static constexpr bool is_unsigned = false;
};

// Unwrap column_field, optional, varchar, text, and numeric wrappers to the
// underlying type that mysql_type_traits knows about.
template <typename T>
struct unwrap_param_type {
    using type = T;
};

template <ColumnFieldType T>
struct unwrap_param_type<T> {
    using type = typename unwrap_param_type<typename T::value_type>::type;
};

template <typename T>
    requires is_optional_v<T>
struct unwrap_param_type<T> {
    using type = typename unwrap_param_type<unwrap_optional_t<T>>::type;
};

template <typename T>
    requires is_varchar_type_v<T>
struct unwrap_param_type<T> {
    using type = std::string;
};

template <typename T>
    requires is_text_type_v<T>
struct unwrap_param_type<T> {
    using type = std::string;
};

template <typename T>
    requires is_formatted_numeric_type_v<T>
struct unwrap_param_type<T> {
    using type = typename T::underlying_type;
};

template <typename T>
using unwrap_param_type_t = typename unwrap_param_type<T>::type;

// bind_input — set up a MYSQL_BIND for an input parameter.
template <typename T>
void bind_input(MYSQL_BIND& bind, T const& value, bool& null_flag) {
    using raw = unwrap_param_type_t<std::decay_t<T>>;
    std::memset(&bind, 0, sizeof(MYSQL_BIND));

    if constexpr (is_optional_v<std::decay_t<T>>) {
        if (!value.has_value()) {
            bind.buffer_type = MYSQL_TYPE_NULL;
            null_flag = true;
            bind.is_null = &null_flag;
            return;
        }
        bind_input(bind, *value, null_flag);
    } else if constexpr (ColumnFieldType<std::decay_t<T>>) {
        bind_input(bind, value.value, null_flag);
    } else if constexpr (is_varchar_type_v<std::decay_t<T>> || is_text_type_v<std::decay_t<T>>) {
        using traits = mysql_type_traits<std::string>;
        bind.buffer_type = traits::field_type;
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
        bind.buffer = const_cast<char*>(value.view().data());
        bind.buffer_length = static_cast<unsigned long>(value.view().size());
        bind.length_value = bind.buffer_length;
        bind.length = &bind.length_value;
    } else if constexpr (is_formatted_numeric_type_v<std::decay_t<T>>) {
        using underlying = typename std::decay_t<T>::underlying_type;
        using traits = mysql_type_traits<underlying>;
        bind.buffer_type = traits::field_type;
        bind.is_unsigned = traits::is_unsigned;
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
        bind.buffer = const_cast<underlying*>(static_cast<underlying const*>(static_cast<void const*>(&value)));
        bind.buffer_length = sizeof(underlying);
    } else if constexpr (std::same_as<raw, std::string>) {
        using traits = mysql_type_traits<std::string>;
        bind.buffer_type = traits::field_type;
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
        bind.buffer = const_cast<char*>(value.data());
        bind.buffer_length = static_cast<unsigned long>(value.size());
        bind.length_value = bind.buffer_length;
        bind.length = &bind.length_value;
    } else {
        using traits = mysql_type_traits<raw>;
        bind.buffer_type = traits::field_type;
        bind.is_unsigned = traits::is_unsigned;
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
        bind.buffer = const_cast<raw*>(&value);
        bind.buffer_length = sizeof(raw);
    }
}

}  // namespace stmt_detail

class prepared_statement {
public:
    prepared_statement(prepared_statement const&) = delete;
    prepared_statement& operator=(prepared_statement const&) = delete;

    prepared_statement(prepared_statement&& other) noexcept : stmt_(std::move(other.stmt_)) {
    }

    prepared_statement& operator=(prepared_statement&& other) noexcept {
        stmt_ = std::move(other.stmt_);
        return *this;
    }

    ~prepared_statement() = default;

    // Number of parameter placeholders in the prepared statement.
    [[nodiscard]] unsigned long param_count() const noexcept {
        return mysql_stmt_param_count(stmt_.get());
    }

    // Execute a DML statement (INSERT, UPDATE, DELETE) with bound parameters.
    // Returns the number of affected rows.
    template <typename... Params>
    [[nodiscard]] std::expected<uint64_t, std::string> execute(Params const&... params) const {
        if (auto r = bind_and_execute(params...); !r) {
            return std::unexpected(r.error());
        }
        return mysql_stmt_affected_rows(stmt_.get());
    }

    // Execute a SELECT and fetch typed results.
    // RowType must be a std::tuple matching the result columns.
    template <typename RowType, typename... Params>
    [[nodiscard]] std::expected<std::vector<RowType>, std::string> query(Params const&... params) const {
        if (auto r = bind_and_execute(params...); !r) {
            return std::unexpected(r.error());
        }

        // Store the result set so we can iterate it.
        if (mysql_stmt_store_result(stmt_.get()) != 0) {
            return std::unexpected(last_error());
        }

        constexpr auto num_cols = std::tuple_size_v<RowType>;
        auto const actual_cols = mysql_stmt_field_count(stmt_.get());
        if (actual_cols != num_cols) {
            return std::unexpected("prepared_statement::query: column count mismatch: expected " +
                                   std::to_string(num_cols) + ", got " + std::to_string(actual_cols));
        }

        return fetch_results<RowType>(std::make_index_sequence<num_cols>{});
    }

    // Returns the AUTO_INCREMENT id generated by the most recent INSERT.
    [[nodiscard]] uint64_t last_insert_id() const noexcept {
        return mysql_stmt_insert_id(stmt_.get());
    }

private:
    friend class mysql_connection;

    struct stmt_deleter {
        void operator()(MYSQL_STMT* s) const noexcept {
            if (s)
                mysql_stmt_close(s);
        }
    };

    explicit prepared_statement(std::unique_ptr<MYSQL_STMT, stmt_deleter> stmt) : stmt_(std::move(stmt)) {
    }

    [[nodiscard]] std::string last_error() const {
        return std::string(mysql_stmt_error(stmt_.get()));
    }

    template <typename... Params>
    [[nodiscard]] std::expected<void, std::string> bind_and_execute(Params const&... params) const {
        constexpr auto N = sizeof...(Params);
        if (N != mysql_stmt_param_count(stmt_.get())) {
            return std::unexpected("prepared_statement: parameter count mismatch: expected " +
                                   std::to_string(mysql_stmt_param_count(stmt_.get())) + ", got " + std::to_string(N));
        }

        if constexpr (N > 0) {
            std::array<MYSQL_BIND, N> binds{};
            std::array<bool, N> null_flags{};
            auto params_tuple = std::tie(params...);
            [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                (stmt_detail::bind_input(binds[Is], std::get<Is>(params_tuple), null_flags[Is]), ...);
            }(std::make_index_sequence<N>{});

            if (mysql_stmt_bind_param(stmt_.get(), binds.data())) {
                return std::unexpected(last_error());
            }
        }

        if (mysql_stmt_execute(stmt_.get()) != 0) {
            return std::unexpected(last_error());
        }
        return {};
    }

    // Fetch all result rows into a vector of RowType tuples.
    template <typename RowType, std::size_t... Is>
    [[nodiscard]] std::expected<std::vector<RowType>, std::string> fetch_results(std::index_sequence<Is...>) const {
        constexpr auto N = sizeof...(Is);

        // Allocate buffers for each column.
        std::array<MYSQL_BIND, N> result_binds{};
        std::array<unsigned long, N> lengths{};
        std::array<bool, N> nulls{};
        std::array<bool, N> errors{};

        // For string columns, we need dynamic buffers.
        // First pass: allocate fixed-size buffers, then refetch strings.
        std::array<std::vector<char>, N> string_bufs;
        constexpr std::size_t initial_string_buf_size = 256;

        (setup_result_bind<std::tuple_element_t<Is, RowType>>(result_binds[Is], lengths[Is], nulls[Is], errors[Is],
                                                              string_bufs[Is], initial_string_buf_size),
         ...);

        if (mysql_stmt_bind_result(stmt_.get(), result_binds.data())) {
            return std::unexpected(last_error());
        }

        std::vector<RowType> results;
        while (true) {
            int const status = mysql_stmt_fetch(stmt_.get());
            if (status == MYSQL_NO_DATA)
                break;
            if (status != 0 && status != MYSQL_DATA_TRUNCATED) {
                return std::unexpected(last_error());
            }

            // Re-fetch truncated string columns.
            if (status == MYSQL_DATA_TRUNCATED) {
                (refetch_if_truncated<std::tuple_element_t<Is, RowType>>(result_binds[Is], lengths[Is], string_bufs[Is],
                                                                         Is),
                 ...);
            }

            results.push_back(RowType{extract_value<std::tuple_element_t<Is, RowType>>(result_binds[Is], lengths[Is],
                                                                                       nulls[Is], string_bufs[Is])...});
        }

        mysql_stmt_free_result(stmt_.get());
        return results;
    }

    template <typename T>
    static void setup_result_bind(MYSQL_BIND& bind, unsigned long& length, bool& null_flag, bool& error_flag,
                                  std::vector<char>& str_buf, std::size_t initial_size) {
        std::memset(&bind, 0, sizeof(MYSQL_BIND));
        bind.length = &length;
        bind.is_null = &null_flag;
        bind.error = &error_flag;

        using raw = stmt_detail::unwrap_param_type_t<T>;
        if constexpr (std::same_as<raw, std::string>) {
            str_buf.resize(initial_size);
            bind.buffer_type = MYSQL_TYPE_STRING;
            bind.buffer = str_buf.data();
            bind.buffer_length = static_cast<unsigned long>(str_buf.size());
        } else {
            bind.buffer_type = stmt_detail::mysql_type_traits<raw>::field_type;
            bind.is_unsigned = stmt_detail::mysql_type_traits<raw>::is_unsigned;
            // Buffer will be set per-row via a static storage in extract_value
            // For fixed-size types, we use the bind's own storage.
            str_buf.resize(sizeof(raw));
            bind.buffer = str_buf.data();
            bind.buffer_length = sizeof(raw);
        }
    }

    template <typename T>
    void refetch_if_truncated(MYSQL_BIND& bind, unsigned long length, std::vector<char>& str_buf,
                              std::size_t col_idx) const {
        using raw = stmt_detail::unwrap_param_type_t<T>;
        if constexpr (std::same_as<raw, std::string>) {
            if (length > bind.buffer_length) {
                str_buf.resize(length);
                bind.buffer = str_buf.data();
                bind.buffer_length = static_cast<unsigned long>(str_buf.size());
                mysql_stmt_fetch_column(stmt_.get(), &bind, static_cast<unsigned int>(col_idx), 0);
            }
        }
    }

    template <typename T>
    static T extract_value(MYSQL_BIND const&, unsigned long length, bool is_null, std::vector<char> const& buf) {
        if constexpr (is_optional_v<T>) {
            if (is_null)
                return std::nullopt;
            using inner = unwrap_optional_t<T>;
            return extract_value<inner>(MYSQL_BIND{}, length, false, buf);
        } else if constexpr (std::same_as<stmt_detail::unwrap_param_type_t<T>, std::string>) {
            return std::string(buf.data(), length);
        } else {
            using raw = stmt_detail::unwrap_param_type_t<T>;
            raw val{};
            std::memcpy(&val, buf.data(), sizeof(raw));
            return static_cast<T>(val);
        }
    }

    std::unique_ptr<MYSQL_STMT, stmt_deleter> stmt_;
};

}  // namespace ds_mysql
