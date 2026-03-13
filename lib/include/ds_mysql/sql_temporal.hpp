#pragma once

#include <chrono>
#include <cstdint>
#include <variant>

namespace ds_mysql {

/**
 * sql_now_t — sentinel type representing the SQL NOW() function.
 *
 * Use the sql_now constant for convenience:
 *   symbol::created_date{sql_now}
 */
struct sql_now_t {};

inline constexpr sql_now_t sql_now{};

/**
 * sql_datetime — a typed SQL datetime value holding either:
 *   - sql_now_t: rendered as SQL's NOW() function call
 *   - std::chrono::system_clock::time_point: rendered as
 *     'YYYY-MM-DD HH:MM:SS[.fraction]' (UTC) based on fractional_second_precision
 *
 * Default-constructed sql_datetime holds sql_now_t, so leaving a datetime
 * field unset produces the idiomatic INSERT/UPDATE pattern of NOW().
 *
 * Example:
 *   symbol row;
 *   row.created_date_.value = sql_now;            // → NOW()
 *   row.created_date_.value = some_time_point;    // → '2024-01-01 12:00:00'
 *   row.created_date_.value = sql_datetime{some_time_point, 3}; // → '2024-01-01 12:00:00.123'
 */
class sql_datetime {
public:
    sql_datetime() noexcept : value_(sql_now_t{}), fractional_second_precision_(0) {
    }
    sql_datetime(sql_now_t, uint32_t fractional_second_precision = 0) noexcept
        : value_(sql_now_t{}), fractional_second_precision_(fractional_second_precision) {
    }
    sql_datetime(std::chrono::system_clock::time_point tp, uint32_t fractional_second_precision = 0) noexcept
        : value_(tp), fractional_second_precision_(fractional_second_precision) {
    }

    [[nodiscard]] bool is_now() const noexcept {
        return std::holds_alternative<sql_now_t>(value_);
    }

    [[nodiscard]] std::chrono::system_clock::time_point time_point() const {
        return std::get<std::chrono::system_clock::time_point>(value_);
    }

    [[nodiscard]] uint32_t fractional_second_precision() const noexcept {
        return fractional_second_precision_;
    }

private:
    std::variant<sql_now_t, std::chrono::system_clock::time_point> value_;
    uint32_t fractional_second_precision_;
};

class sql_timestamp {
public:
    sql_timestamp() noexcept : value_(sql_now_t{}), fractional_second_precision_(0) {
    }
    sql_timestamp(sql_now_t, uint32_t fractional_second_precision = 0) noexcept
        : value_(sql_now_t{}), fractional_second_precision_(fractional_second_precision) {
    }
    sql_timestamp(std::chrono::system_clock::time_point tp, uint32_t fractional_second_precision = 0) noexcept
        : value_(tp), fractional_second_precision_(fractional_second_precision) {
    }

    [[nodiscard]] bool is_now() const noexcept {
        return std::holds_alternative<sql_now_t>(value_);
    }

    [[nodiscard]] std::chrono::system_clock::time_point time_point() const {
        return std::get<std::chrono::system_clock::time_point>(value_);
    }

    [[nodiscard]] uint32_t fractional_second_precision() const noexcept {
        return fractional_second_precision_;
    }

private:
    std::variant<sql_now_t, std::chrono::system_clock::time_point> value_;
    uint32_t fractional_second_precision_;
};

}  // namespace ds_mysql
