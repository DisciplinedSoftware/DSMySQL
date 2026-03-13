#pragma once

#include <string>
#include <utility>

namespace ds_mysql {

class database_name {
private:
    std::string value_;

public:
    database_name() = default;
    explicit database_name(std::string value) : value_(std::move(value)) {
    }

    [[nodiscard]] inline std::string const& to_string() const noexcept {
        return value_;
    }
};

}  // namespace ds_mysql
