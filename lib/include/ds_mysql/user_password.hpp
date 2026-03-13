#pragma once

#include <string>
#include <utility>

namespace ds_mysql {

class user_password {
private:
    std::string value_;

public:
    user_password() = default;
    explicit user_password(std::string value) : value_(std::move(value)) {
    }

    [[nodiscard]] inline std::string const& to_string() const noexcept {
        return value_;
    }
};

}  // namespace ds_mysql
