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
 * datetime_type — a typed SQL datetime value holding either:
 *   - sql_now_t: rendered as SQL's NOW() function call
 *   - std::chrono::system_clock::time_point: rendered as
 *     'YYYY-MM-DD HH:MM:SS[.fraction]' (UTC) based on fractional_second_precision
 *
 * Default-constructed datetime_type holds sql_now_t, so leaving a datetime
 * field unset produces the idiomatic INSERT/UPDATE pattern of NOW().
 *
 * Example:
 *   symbol row;
 *   row.created_date_.value = sql_now;            // → NOW()
 *   row.created_date_.value = some_time_point;    // → '2024-01-01 12:00:00'
 *   row.created_date_.value = datetime_type{some_time_point, 3}; // → '2024-01-01 12:00:00.123'
 */
class datetime_type {
public:
    datetime_type() noexcept : value_(sql_now_t{}), fractional_second_precision_(0) {
    }
    datetime_type(sql_now_t, uint32_t fractional_second_precision = 0) noexcept
        : value_(sql_now_t{}), fractional_second_precision_(fractional_second_precision) {
    }
    datetime_type(std::chrono::system_clock::time_point tp, uint32_t fractional_second_precision = 0) noexcept
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

class timestamp_type {
public:
    timestamp_type() noexcept : value_(sql_now_t{}), fractional_second_precision_(0) {
    }
    timestamp_type(sql_now_t, uint32_t fractional_second_precision = 0) noexcept
        : value_(sql_now_t{}), fractional_second_precision_(fractional_second_precision) {
    }
    timestamp_type(std::chrono::system_clock::time_point tp, uint32_t fractional_second_precision = 0) noexcept
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

/**
 * time_type — a typed SQL TIME value holding a signed duration.
 *
 * MySQL's TIME type stores a duration in the range -838:59:59 to 838:59:59,
 * with optional fractional-second precision (0–6).
 *
 * Rendered as 'HHH:MM:SS[.fraction]' (or '-HHH:MM:SS[.fraction]' for negative
 * durations) in SQL literals.
 *
 * Example:
 *   using namespace std::chrono_literals;
 *   row.duration_.value = time_type{8h + 30min};         // → '08:30:00'
 *   row.duration_.value = time_type{-(10h + 5min), 3};   // → '-10:05:00.000'
 */
class time_type {
public:
    time_type() noexcept = default;
    explicit time_type(std::chrono::microseconds duration, uint32_t fractional_second_precision = 0) noexcept
        : duration_(duration), fractional_second_precision_(fractional_second_precision) {
    }

    [[nodiscard]] std::chrono::microseconds duration() const noexcept {
        return duration_;
    }

    [[nodiscard]] uint32_t fractional_second_precision() const noexcept {
        return fractional_second_precision_;
    }

private:
    std::chrono::microseconds duration_{};
    uint32_t fractional_second_precision_{0};
};

}  // namespace ds_mysql
