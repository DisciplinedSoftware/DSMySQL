#pragma once

#include <future>

#include "ds_mysql/connection_pool.hpp"

namespace ds_mysql {

// ===================================================================
// Async query/execute — pool-based, thread-safe.
//
// Acquires a connection from the pool, runs the query on a worker
// thread via std::async, and returns the connection to the pool when
// done.  The returned std::future holds the result.
//
// Usage:
//   auto future = async_query(pool, select(col{}).from(table{}));
//   auto result = future.get();  // blocks until complete
//
//   auto future = async_execute(pool, insert_into(t{}).values(row));
//   auto affected = future.get();
//
// Note: each call acquires its own connection.  Multiple async calls
// run concurrently on different connections.  The pool blocks if all
// connections are in use.
// ===================================================================

/// Run a typed SELECT query asynchronously via the connection pool.
template <TypedSelectQuery Stmt>
[[nodiscard]] auto async_query(connection_pool& pool, Stmt stmt) {
    using row_type = typename Stmt::result_row_type;
    using result_type = std::expected<std::vector<row_type>, std::string>;
    return std::async(std::launch::async, [&pool, stmt = std::move(stmt)]() -> result_type {
        auto conn = pool.acquire();
        return conn->query(stmt);
    });
}

/// Run a raw SQL query asynchronously via the connection pool.
[[nodiscard]] inline auto async_query(connection_pool& pool, sql_raw stmt) {
    using result_type = std::expected<std::vector<mysql_connection::raw_result_row_type>, std::string>;
    return std::async(std::launch::async, [&pool, stmt = std::move(stmt)]() -> result_type {
        auto conn = pool.acquire();
        return conn->query(stmt);
    });
}

/// Run a DDL/DML statement asynchronously via the connection pool.
template <SqlExecuteStatement Stmt>
[[nodiscard]] auto async_execute(connection_pool& pool, Stmt stmt) {
    using result_type = std::expected<uint64_t, std::string>;
    return std::async(std::launch::async, [&pool, stmt = std::move(stmt)]() -> result_type {
        auto conn = pool.acquire();
        return conn->execute(stmt);
    });
}

}  // namespace ds_mysql
