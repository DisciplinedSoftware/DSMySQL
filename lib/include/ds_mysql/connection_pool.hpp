#pragma once

#include <condition_variable>
#include <expected>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <vector>

#include "ds_mysql/mysql_connection.hpp"

namespace ds_mysql {

class connection_pool;

// ===================================================================
// pooled_connection — RAII handle that returns the connection to the
// pool on destruction.  Move-only.  Use operator-> to access the
// underlying mysql_connection.
// ===================================================================

class pooled_connection {
public:
    pooled_connection(pooled_connection const&) = delete;
    pooled_connection& operator=(pooled_connection const&) = delete;

    pooled_connection(pooled_connection&& other) noexcept : pool_(other.pool_), conn_(std::move(other.conn_)) {
        other.pool_ = nullptr;
    }

    pooled_connection& operator=(pooled_connection&& other) noexcept {
        if (this != &other) {
            release();
            pool_ = other.pool_;
            conn_ = std::move(other.conn_);
            other.pool_ = nullptr;
        }
        return *this;
    }

    ~pooled_connection() {
        release();
    }

    mysql_connection* operator->() noexcept {
        return &*conn_;
    }

    mysql_connection const* operator->() const noexcept {
        return &*conn_;
    }

    mysql_connection& operator*() noexcept {
        return *conn_;
    }

    mysql_connection const& operator*() const noexcept {
        return *conn_;
    }

private:
    friend class connection_pool;

    pooled_connection(connection_pool* pool, mysql_connection conn) : pool_(pool), conn_(std::move(conn)) {
    }

    // Defined after connection_pool is complete.
    inline void release();

    connection_pool* pool_;
    std::optional<mysql_connection> conn_;
};

// ===================================================================
// connection_pool — thread-safe pool of mysql_connection instances.
//
// Factory:
//   auto pool = connection_pool::create(config, pool_size);
//   auto pool = connection_pool::create(config, pool_size, options);
//
// Usage:
//   auto conn = pool->acquire();           // blocks until a connection is available
//   conn->query(select(col{}).from(t{}));   // use like a mysql_connection*
//   // connection returned to pool when conn goes out of scope
//
//   auto conn = pool->try_acquire();        // non-blocking; returns std::nullopt if none available
// ===================================================================

class connection_pool {
public:
    connection_pool(connection_pool const&) = delete;
    connection_pool& operator=(connection_pool const&) = delete;
    connection_pool(connection_pool&&) = delete;
    connection_pool& operator=(connection_pool&&) = delete;
    ~connection_pool() = default;

    /// Create a pool with `pool_size` connections using the given config.
    [[nodiscard]] static std::expected<std::unique_ptr<connection_pool>, std::string> create(mysql_config const& config,
                                                                                             std::size_t pool_size) {
        return create_impl(config, pool_size, nullptr);
    }

    /// Create a pool with `pool_size` connections using the given config and pre-connect options.
    [[nodiscard]] static std::expected<std::unique_ptr<connection_pool>, std::string> create(
        mysql_config const& config, std::size_t pool_size, connect_options const& options) {
        return create_impl(config, pool_size, &options);
    }

    /// Acquire a connection from the pool (blocks until one is available).
    [[nodiscard]] pooled_connection acquire() {
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [this] {
            return !pool_.empty();
        });
        auto conn = std::move(pool_.front());
        pool_.pop();
        return pooled_connection{this, std::move(conn)};
    }

    /// Try to acquire a connection without blocking.
    /// Returns std::nullopt if no connections are available.
    [[nodiscard]] std::optional<pooled_connection> try_acquire() {
        std::lock_guard lock(mutex_);
        if (pool_.empty()) {
            return std::nullopt;
        }
        auto conn = std::move(pool_.front());
        pool_.pop();
        return pooled_connection{this, std::move(conn)};
    }

    /// Total number of connections managed by the pool.
    [[nodiscard]] std::size_t size() const noexcept {
        return total_size_;
    }

    /// Number of connections currently available (not checked out).
    [[nodiscard]] std::size_t available() const {
        std::lock_guard lock(mutex_);
        return pool_.size();
    }

private:
    friend class pooled_connection;

    explicit connection_pool(std::queue<mysql_connection> pool, std::size_t total_size)
        : pool_(std::move(pool)), total_size_(total_size) {
    }

    void release(mysql_connection conn) {
        {
            std::lock_guard lock(mutex_);
            pool_.push(std::move(conn));
        }
        cv_.notify_one();
    }

    [[nodiscard]] static std::expected<std::unique_ptr<connection_pool>, std::string> create_impl(
        mysql_config const& config, std::size_t pool_size, connect_options const* options) {
        if (pool_size == 0) {
            return std::unexpected(std::string("pool_size must be > 0"));
        }

        std::queue<mysql_connection> pool;
        for (std::size_t i = 0; i < pool_size; ++i) {
            auto conn = options ? mysql_connection::connect(config, *options) : mysql_connection::connect(config);
            if (!conn) {
                return std::unexpected("Failed to create connection " + std::to_string(i + 1) + "/" +
                                       std::to_string(pool_size) + ": " + conn.error());
            }
            pool.push(std::move(*conn));
        }

        return std::unique_ptr<connection_pool>(new connection_pool{std::move(pool), pool_size});
    }

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<mysql_connection> pool_;
    std::size_t total_size_;
};

// ---------------------------------------------------------------
// pooled_connection::release() — deferred definition
// ---------------------------------------------------------------

inline void pooled_connection::release() {
    if (pool_ && conn_.has_value()) {
        pool_->release(std::move(*conn_));
        conn_.reset();
        pool_ = nullptr;
    }
}

}  // namespace ds_mysql
