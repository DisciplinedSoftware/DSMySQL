#pragma once

#include <chrono>
#include <cstdint>
#include <type_traits>
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

template <uint32_t Fsp = 0>
class datetime_type {
public:
    static constexpr uint32_t column_fsp = Fsp;

    datetime_type() noexcept : value_(sql_now_t{}), fractional_second_precision_(Fsp) {
    }
    datetime_type(sql_now_t, uint32_t fractional_second_precision = Fsp) noexcept
        : value_(sql_now_t{}), fractional_second_precision_(fractional_second_precision) {
    }
    datetime_type(std::chrono::system_clock::time_point tp, uint32_t fractional_second_precision = Fsp) noexcept
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

template <uint32_t Fsp = 0>
class timestamp_type {
public:
    static constexpr uint32_t column_fsp = Fsp;

    timestamp_type() noexcept : value_(sql_now_t{}), fractional_second_precision_(Fsp) {
    }
    timestamp_type(sql_now_t, uint32_t fractional_second_precision = Fsp) noexcept
        : value_(sql_now_t{}), fractional_second_precision_(fractional_second_precision) {
    }
    timestamp_type(std::chrono::system_clock::time_point tp, uint32_t fractional_second_precision = Fsp) noexcept
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

template <uint32_t Fsp = 0>
class time_type {
public:
    static constexpr uint32_t column_fsp = Fsp;

    time_type() noexcept : duration_(), fractional_second_precision_(Fsp) {
    }
    explicit time_type(std::chrono::microseconds duration, uint32_t fractional_second_precision = Fsp) noexcept
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
    uint32_t fractional_second_precision_;
};

// ===================================================================
// Type detection traits
// ===================================================================

template <typename T>
struct is_datetime_type : std::false_type {};
template <uint32_t Fsp>
struct is_datetime_type<datetime_type<Fsp>> : std::true_type {};
template <typename T>
inline constexpr bool is_datetime_type_v = is_datetime_type<T>::value;

template <typename T>
struct is_timestamp_type : std::false_type {};
template <uint32_t Fsp>
struct is_timestamp_type<timestamp_type<Fsp>> : std::true_type {};
template <typename T>
inline constexpr bool is_timestamp_type_v = is_timestamp_type<T>::value;

template <typename T>
struct is_time_type : std::false_type {};
template <uint32_t Fsp>
struct is_time_type<time_type<Fsp>> : std::true_type {};
template <typename T>
inline constexpr bool is_time_type_v = is_time_type<T>::value;

}  // namespace ds_mysql
