#pragma once

#include <string>
#include <utility>

namespace ds_mysql {

class host_name {
private:
    std::string value_;

public:
    host_name() = default;
    explicit host_name(std::string value) : value_(std::move(value)) {
    }

    [[nodiscard]] inline std::string const& to_string() const noexcept {
        return value_;
    }
};

}  // namespace ds_mysql
