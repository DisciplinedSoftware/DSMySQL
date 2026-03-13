#pragma once

#include <string>
#include <utility>

namespace ds_mysql {

class user_name {
private:
    std::string value_;

public:
    user_name() = default;
    explicit user_name(std::string value) : value_(std::move(value)) {
    }

    [[nodiscard]] inline std::string const& to_string() const noexcept {
        return value_;
    }
};

}  // namespace ds_mysql
