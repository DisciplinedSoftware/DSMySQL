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

// ===================================================================
// Internal class templates — not for direct use by library users.
// Use the datetime_type<Fsp>, timestamp_type<Fsp>, time_type<Fsp>
// template aliases below (or bare datetime_type / timestamp_type /
// time_type, which default to Fsp = 0).
// ===================================================================

namespace detail {

template <uint32_t Fsp>
class basic_datetime_type {
public:
    static constexpr uint32_t column_fsp = Fsp;

    basic_datetime_type() noexcept : value_(sql_now_t{}), fractional_second_precision_(Fsp) {
    }
    basic_datetime_type(sql_now_t, uint32_t fractional_second_precision = Fsp) noexcept
        : value_(sql_now_t{}), fractional_second_precision_(fractional_second_precision) {
    }
    basic_datetime_type(std::chrono::system_clock::time_point tp, uint32_t fractional_second_precision = Fsp) noexcept
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

template <uint32_t Fsp>
class basic_timestamp_type {
public:
    static constexpr uint32_t column_fsp = Fsp;

    basic_timestamp_type() noexcept : value_(sql_now_t{}), fractional_second_precision_(Fsp) {
    }
    basic_timestamp_type(sql_now_t, uint32_t fractional_second_precision = Fsp) noexcept
        : value_(sql_now_t{}), fractional_second_precision_(fractional_second_precision) {
    }
    basic_timestamp_type(std::chrono::system_clock::time_point tp, uint32_t fractional_second_precision = Fsp) noexcept
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

template <uint32_t Fsp>
class basic_time_type {
public:
    static constexpr uint32_t column_fsp = Fsp;

    basic_time_type() noexcept : duration_(), fractional_second_precision_(Fsp) {
    }
    explicit basic_time_type(std::chrono::microseconds duration, uint32_t fractional_second_precision = Fsp) noexcept
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

}  // namespace detail

// ===================================================================
// Public template aliases
//
//   datetime_type      — DATETIME      (equivalent to datetime_type<0>)
//   datetime_type<3>   — DATETIME(3)
//   datetime_type<6>   — DATETIME(6)
//
// The same scheme applies to timestamp_type and time_type.
// ===================================================================

/**
 * datetime_type<Fsp> — typed SQL DATETIME value.
 *
 * Fsp (0–6) sets the fractional-second precision used in DDL generation and
 * as the default when serializing values.  Default-constructs to sql_now.
 *
 *   COLUMN_FIELD(created_at, datetime_type)     // DATETIME
 *   COLUMN_FIELD(created_at, datetime_type<3>)  // DATETIME(3)
 */
template <uint32_t Fsp = 0>
using datetime_type = detail::basic_datetime_type<Fsp>;

/**
 * timestamp_type<Fsp> — typed SQL TIMESTAMP value.
 *
 *   COLUMN_FIELD(updated_at, timestamp_type)     // TIMESTAMP
 *   COLUMN_FIELD(updated_at, timestamp_type<6>)  // TIMESTAMP(6)
 */
template <uint32_t Fsp = 0>
using timestamp_type = detail::basic_timestamp_type<Fsp>;

/**
 * time_type<Fsp> — typed SQL TIME value.
 *
 * Holds a signed std::chrono::microseconds duration and an optional
 * fractional-second precision (0–6).
 *
 *   COLUMN_FIELD(duration, time_type)     // TIME
 *   COLUMN_FIELD(duration, time_type<3>)  // TIME(3)
 */
template <uint32_t Fsp = 0>
using time_type = detail::basic_time_type<Fsp>;

// ===================================================================
// Type detection traits
// ===================================================================

template <typename T>
struct is_datetime_type : std::false_type {};
template <uint32_t Fsp>
struct is_datetime_type<detail::basic_datetime_type<Fsp>> : std::true_type {};
template <typename T>
inline constexpr bool is_datetime_type_v = is_datetime_type<T>::value;

template <typename T>
struct is_timestamp_type : std::false_type {};
template <uint32_t Fsp>
struct is_timestamp_type<detail::basic_timestamp_type<Fsp>> : std::true_type {};
template <typename T>
inline constexpr bool is_timestamp_type_v = is_timestamp_type<T>::value;

template <typename T>
struct is_time_type : std::false_type {};
template <uint32_t Fsp>
struct is_time_type<detail::basic_time_type<Fsp>> : std::true_type {};
template <typename T>
inline constexpr bool is_time_type_v = is_time_type<T>::value;

}  // namespace ds_mysql
