#pragma once

#include <mysql.h>

#include <cstring>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include "ds_mysql/prepared_statement.hpp"

namespace ds_mysql {

// ===================================================================
// server_cursor — streaming result set via MySQL server-side cursors.
//
// Unlike prepared_statement::query() which loads all rows into memory,
// a server_cursor fetches rows one-at-a-time (or in prefetch batches)
// from the server.  Ideal for large result sets.
//
// Created via mysql_connection::open_cursor():
//   auto cursor = conn.open_cursor<RowType>("SELECT * FROM t WHERE x = ?", 42u);
//   while (auto row = cursor->fetch()) {
//       process(*row);
//   }
//
// Prefetch hint (number of rows to buffer at once):
//   auto cursor = conn.open_cursor<RowType>(sql, prefetch_rows{100}, 42u);
// ===================================================================

struct prefetch_rows {
    unsigned long value = 1;
};

template <typename RowType>
class server_cursor {
public:
    server_cursor(server_cursor const&) = delete;
    server_cursor& operator=(server_cursor const&) = delete;

    server_cursor(server_cursor&& other) noexcept
        : stmt_(std::move(other.stmt_)),
          result_binds_(std::move(other.result_binds_)),
          lengths_(std::move(other.lengths_)),
          nulls_(std::move(other.nulls_)),
          errors_(std::move(other.errors_)),
          string_bufs_(std::move(other.string_bufs_)),
          done_(other.done_) {
        other.done_ = true;
    }

    server_cursor& operator=(server_cursor&& other) noexcept {
        if (this != &other) {
            close();
            stmt_ = std::move(other.stmt_);
            result_binds_ = std::move(other.result_binds_);
            lengths_ = std::move(other.lengths_);
            nulls_ = std::move(other.nulls_);
            errors_ = std::move(other.errors_);
            string_bufs_ = std::move(other.string_bufs_);
            done_ = other.done_;
            other.done_ = true;
        }
        return *this;
    }

    ~server_cursor() {
        close();
    }

    /// Fetch the next row.  Returns std::nullopt when no more rows.
    [[nodiscard]] std::expected<std::optional<RowType>, std::string> fetch() {
        if (done_) {
            return std::optional<RowType>{std::nullopt};
        }

        int const status = mysql_stmt_fetch(stmt_.get());
        if (status == MYSQL_NO_DATA) {
            done_ = true;
            return std::optional<RowType>{std::nullopt};
        }
        if (status != 0 && status != MYSQL_DATA_TRUNCATED) {
            return std::unexpected(std::string(mysql_stmt_error(stmt_.get())));
        }

        return extract_row(std::make_index_sequence<std::tuple_size_v<RowType>>{}, status == MYSQL_DATA_TRUNCATED);
    }

    /// True when all rows have been fetched.
    [[nodiscard]] bool done() const noexcept {
        return done_;
    }

private:
    friend class mysql_connection;

    struct stmt_deleter {
        void operator()(MYSQL_STMT* s) const noexcept {
            if (s)
                mysql_stmt_close(s);
        }
    };

    static constexpr auto N = std::tuple_size_v<RowType>;
    static constexpr std::size_t initial_string_buf_size = 256;

    explicit server_cursor(std::unique_ptr<MYSQL_STMT, stmt_deleter> stmt)
        : stmt_(std::move(stmt)),
          result_binds_(N),
          lengths_(N),
          nulls_(std::make_unique<bool[]>(N)),
          errors_(std::make_unique<bool[]>(N)),
          string_bufs_(N) {
        std::fill_n(nulls_.get(), N, false);
        std::fill_n(errors_.get(), N, false);
    }

    void close() {
        if (stmt_) {
            mysql_stmt_free_result(stmt_.get());
        }
    }

    std::expected<void, std::string> bind_results() {
        [this]<std::size_t... Is>(std::index_sequence<Is...>) {
            (this->setup_bind<std::tuple_element_t<Is, RowType>>(Is), ...);
        }(std::make_index_sequence<N>{});

        if (mysql_stmt_bind_result(stmt_.get(), result_binds_.data())) {
            return std::unexpected(std::string(mysql_stmt_error(stmt_.get())));
        }
        return {};
    }

    template <typename T>
    void setup_bind(std::size_t idx) {
        auto& bind = result_binds_[idx];
        std::memset(&bind, 0, sizeof(MYSQL_BIND));
        bind.length = &lengths_[idx];
        bind.is_null = &nulls_.get()[idx];
        bind.error = &errors_.get()[idx];

        using raw = stmt_detail::unwrap_param_type_t<T>;
        if constexpr (std::same_as<raw, std::string>) {
            string_bufs_[idx].resize(initial_string_buf_size);
            bind.buffer_type = MYSQL_TYPE_STRING;
            bind.buffer = string_bufs_[idx].data();
            bind.buffer_length = static_cast<unsigned long>(string_bufs_[idx].size());
        } else {
            bind.buffer_type = stmt_detail::mysql_type_traits<raw>::field_type;
            bind.is_unsigned = stmt_detail::mysql_type_traits<raw>::is_unsigned;
            string_bufs_[idx].resize(sizeof(raw));
            bind.buffer = string_bufs_[idx].data();
            bind.buffer_length = sizeof(raw);
        }
    }

    template <std::size_t... Is>
    std::expected<std::optional<RowType>, std::string> extract_row(std::index_sequence<Is...>, bool truncated) {
        if (truncated) {
            (refetch_truncated<std::tuple_element_t<Is, RowType>>(Is), ...);
        }
        return std::optional<RowType>{RowType{extract_value<std::tuple_element_t<Is, RowType>>(Is)...}};
    }

    template <typename T>
    void refetch_truncated(std::size_t idx) {
        using raw = stmt_detail::unwrap_param_type_t<T>;
        if constexpr (std::same_as<raw, std::string>) {
            if (lengths_[idx] > result_binds_[idx].buffer_length) {
                string_bufs_[idx].resize(lengths_[idx]);
                result_binds_[idx].buffer = string_bufs_[idx].data();
                result_binds_[idx].buffer_length = static_cast<unsigned long>(string_bufs_[idx].size());
                mysql_stmt_fetch_column(stmt_.get(), &result_binds_[idx], static_cast<unsigned int>(idx), 0);
            }
        }
    }

    template <typename T>
    T extract_value(std::size_t idx) const {
        if constexpr (is_optional_v<T>) {
            if (nulls_.get()[idx])
                return std::nullopt;
            using inner = unwrap_optional_t<T>;
            return extract_value<inner>(idx);
        } else if constexpr (std::same_as<stmt_detail::unwrap_param_type_t<T>, std::string>) {
            return std::string(string_bufs_[idx].data(), lengths_[idx]);
        } else {
            using raw = stmt_detail::unwrap_param_type_t<T>;
            raw val{};
            std::memcpy(&val, string_bufs_[idx].data(), sizeof(raw));
            return static_cast<T>(val);
        }
    }

    std::unique_ptr<MYSQL_STMT, stmt_deleter> stmt_;
    std::vector<MYSQL_BIND> result_binds_;
    std::vector<unsigned long> lengths_;
    std::unique_ptr<bool[]> nulls_;
    std::unique_ptr<bool[]> errors_;
    std::vector<std::vector<char>> string_bufs_;
    bool done_ = false;
};

}  // namespace ds_mysql
