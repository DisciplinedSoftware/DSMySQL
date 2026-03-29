#pragma once

#include <string>
#include <utility>

namespace ds_mysql {

class charset_name {
private:
    std::string value_;

public:
    charset_name() = default;
    explicit charset_name(std::string value) : value_(std::move(value)) {
    }

    [[nodiscard]] inline std::string const& to_string() const noexcept {
        return value_;
    }
};

}  // namespace ds_mysql
