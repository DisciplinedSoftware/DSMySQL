#pragma once

#include <utility>

#include "ds_mysql/user_name.hpp"
#include "ds_mysql/user_password.hpp"

namespace ds_mysql {

struct auth_credentials {
private:
    user_name user_;
    user_password password_;

public:
    auth_credentials() = default;

    inline auth_credentials(user_name user, user_password password)
        : user_(std::move(user)), password_(std::move(password)) {
    }

    [[nodiscard]] inline user_name const& user() const noexcept {
        return user_;
    }

    [[nodiscard]] inline user_password const& password() const noexcept {
        return password_;
    }
};

}  // namespace ds_mysql
