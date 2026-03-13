#pragma once

namespace ds_mysql {

class port_number {
private:
    unsigned int value_;

public:
    explicit constexpr port_number(unsigned int value) noexcept : value_(value) {
    }

    [[nodiscard]] inline constexpr unsigned int to_unsigned_int() const noexcept {
        return value_;
    }
};

}  // namespace ds_mysql
