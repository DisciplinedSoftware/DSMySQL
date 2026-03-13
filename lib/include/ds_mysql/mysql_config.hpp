#pragma once

#include <utility>

#include "ds_mysql/credentials.hpp"
#include "ds_mysql/database_name.hpp"
#include "ds_mysql/host_name.hpp"
#include "ds_mysql/port_number.hpp"

namespace ds_mysql {

inline constexpr port_number default_mysql_port = port_number{3306};

struct mysql_config {
private:
    host_name host_;
    database_name database_;
    auth_credentials credentials_;
    port_number port_{default_mysql_port};

public:
    mysql_config() = default;

    inline mysql_config(host_name host, database_name database, auth_credentials credentials,
                        port_number port = default_mysql_port)
        : host_(std::move(host)), database_(std::move(database)), credentials_(std::move(credentials)), port_(port) {
    }

    [[nodiscard]] inline host_name const& host() const noexcept {
        return host_;
    }

    [[nodiscard]] inline database_name const& database() const noexcept {
        return database_;
    }

    [[nodiscard]] inline auth_credentials const& credentials() const noexcept {
        return credentials_;
    }

    [[nodiscard]] inline port_number const& port() const noexcept {
        return port_;
    }
};

}  // namespace ds_mysql
