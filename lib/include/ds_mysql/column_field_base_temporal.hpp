#pragma once

#include <chrono>
#include <optional>

#include "ds_mysql/column_field_base_core.hpp"
#include "ds_mysql/sql_temporal.hpp"

namespace ds_mysql {

namespace column_field_detail {

// ===================================================================
// base<datetime_type<Fsp>> specializations
// ===================================================================

template <uint32_t Fsp>
struct base<datetime_type<Fsp>> : column_field_tag {
    using value_type = datetime_type<Fsp>;

    datetime_type<Fsp> value{};

    constexpr base() = default;
    base(sql_now_t) noexcept : value(sql_now_t{}) {
    }
    base(std::chrono::system_clock::time_point tp) noexcept : value(tp) {
    }
    base(datetime_type<Fsp> v) noexcept : value(v) {
    }

    base& operator=(sql_now_t) noexcept {
        value = datetime_type<Fsp>{sql_now_t{}};
        return *this;
    }
    base& operator=(std::chrono::system_clock::time_point tp) noexcept {
        value = datetime_type<Fsp>{tp};
        return *this;
    }
    base& operator=(datetime_type<Fsp> v) noexcept {
        value = v;
        return *this;
    }

    [[nodiscard]] constexpr datetime_type<Fsp> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr datetime_type<Fsp>& get() noexcept {
        return value;
    }

    constexpr operator datetime_type<Fsp> const&() const noexcept {
        return value;
    }
    constexpr operator datetime_type<Fsp>&() noexcept {
        return value;
    }
};

template <uint32_t Fsp>
struct base<std::optional<datetime_type<Fsp>>> : column_field_tag {
    using value_type = std::optional<datetime_type<Fsp>>;

    std::optional<datetime_type<Fsp>> value{};

    constexpr base() = default;
    constexpr base(std::nullopt_t) noexcept : value(std::nullopt) {
    }
    base(sql_now_t) noexcept : value(datetime_type<Fsp>{sql_now_t{}}) {
    }
    base(std::chrono::system_clock::time_point tp) noexcept : value(datetime_type<Fsp>{tp}) {
    }
    base(datetime_type<Fsp> v) noexcept : value(std::move(v)) {
    }
    base(std::optional<datetime_type<Fsp>> v) noexcept : value(std::move(v)) {
    }

    constexpr base& operator=(std::nullopt_t) noexcept {
        value = std::nullopt;
        return *this;
    }
    constexpr base& operator=(sql_now_t) noexcept {
        value = datetime_type<Fsp>{sql_now_t{}};
        return *this;
    }
    constexpr base& operator=(std::chrono::system_clock::time_point tp) noexcept {
        value = datetime_type<Fsp>{tp};
        return *this;
    }
    constexpr base& operator=(datetime_type<Fsp> v) noexcept {
        value = std::move(v);
        return *this;
    }
    constexpr base& operator=(std::optional<datetime_type<Fsp>> v) noexcept {
        value = std::move(v);
        return *this;
    }

    [[nodiscard]] constexpr std::optional<datetime_type<Fsp>> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr std::optional<datetime_type<Fsp>>& get() noexcept {
        return value;
    }

    constexpr operator std::optional<datetime_type<Fsp>> const&() const noexcept {
        return value;
    }
    constexpr operator std::optional<datetime_type<Fsp>>&() noexcept {
        return value;
    }
};

// ===================================================================
// base<timestamp_type<Fsp>> specializations
// ===================================================================

template <uint32_t Fsp>
struct base<timestamp_type<Fsp>> : column_field_tag {
    using value_type = timestamp_type<Fsp>;

    timestamp_type<Fsp> value{};

    constexpr base() = default;
    base(sql_now_t) noexcept : value(sql_now_t{}) {
    }
    base(std::chrono::system_clock::time_point tp) noexcept : value(tp) {
    }
    base(timestamp_type<Fsp> v) noexcept : value(v) {
    }

    base& operator=(sql_now_t) noexcept {
        value = timestamp_type<Fsp>{sql_now_t{}};
        return *this;
    }
    base& operator=(std::chrono::system_clock::time_point tp) noexcept {
        value = timestamp_type<Fsp>{tp};
        return *this;
    }
    base& operator=(timestamp_type<Fsp> v) noexcept {
        value = v;
        return *this;
    }

    [[nodiscard]] constexpr timestamp_type<Fsp> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr timestamp_type<Fsp>& get() noexcept {
        return value;
    }

    constexpr operator timestamp_type<Fsp> const&() const noexcept {
        return value;
    }
    constexpr operator timestamp_type<Fsp>&() noexcept {
        return value;
    }
};

template <uint32_t Fsp>
struct base<std::optional<timestamp_type<Fsp>>> : column_field_tag {
    using value_type = std::optional<timestamp_type<Fsp>>;

    std::optional<timestamp_type<Fsp>> value{};

    constexpr base() = default;
    constexpr base(std::nullopt_t) noexcept : value(std::nullopt) {
    }
    base(sql_now_t) noexcept : value(timestamp_type<Fsp>{sql_now_t{}}) {
    }
    base(std::chrono::system_clock::time_point tp) noexcept : value(timestamp_type<Fsp>{tp}) {
    }
    base(timestamp_type<Fsp> v) noexcept : value(std::move(v)) {
    }
    base(std::optional<timestamp_type<Fsp>> v) noexcept : value(std::move(v)) {
    }

    constexpr base& operator=(std::nullopt_t) noexcept {
        value = std::nullopt;
        return *this;
    }
    constexpr base& operator=(sql_now_t) noexcept {
        value = timestamp_type<Fsp>{sql_now_t{}};
        return *this;
    }
    constexpr base& operator=(std::chrono::system_clock::time_point tp) noexcept {
        value = timestamp_type<Fsp>{tp};
        return *this;
    }
    constexpr base& operator=(timestamp_type<Fsp> v) noexcept {
        value = std::move(v);
        return *this;
    }
    constexpr base& operator=(std::optional<timestamp_type<Fsp>> v) noexcept {
        value = std::move(v);
        return *this;
    }

    [[nodiscard]] constexpr std::optional<timestamp_type<Fsp>> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr std::optional<timestamp_type<Fsp>>& get() noexcept {
        return value;
    }

    constexpr operator std::optional<timestamp_type<Fsp>> const&() const noexcept {
        return value;
    }
    constexpr operator std::optional<timestamp_type<Fsp>>&() noexcept {
        return value;
    }
};

// ===================================================================
// base<date_type> specializations
// ===================================================================

template <>
struct base<date_type> : column_field_tag {
    using value_type = date_type;

    date_type value{};

    constexpr base() = default;
    base(std::chrono::sys_days d) noexcept : value(d) {
    }
    base(date_type v) noexcept : value(v) {
    }

    base& operator=(std::chrono::sys_days d) noexcept {
        value = date_type{d};
        return *this;
    }
    base& operator=(date_type v) noexcept {
        value = v;
        return *this;
    }

    [[nodiscard]] constexpr date_type const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr date_type& get() noexcept {
        return value;
    }

    constexpr operator date_type const&() const noexcept {
        return value;
    }
    constexpr operator date_type&() noexcept {
        return value;
    }
};

template <>
struct base<std::optional<date_type>> : column_field_tag {
    using value_type = std::optional<date_type>;

    std::optional<date_type> value{};

    constexpr base() = default;
    constexpr base(std::nullopt_t) noexcept : value(std::nullopt) {
    }
    base(std::chrono::sys_days d) noexcept : value(date_type{d}) {
    }
    base(date_type v) noexcept : value(std::move(v)) {
    }
    base(std::optional<date_type> v) noexcept : value(std::move(v)) {
    }

    constexpr base& operator=(std::nullopt_t) noexcept {
        value = std::nullopt;
        return *this;
    }
    constexpr base& operator=(std::chrono::sys_days d) noexcept {
        value = date_type{d};
        return *this;
    }
    constexpr base& operator=(date_type v) noexcept {
        value = std::move(v);
        return *this;
    }
    constexpr base& operator=(std::optional<date_type> v) noexcept {
        value = std::move(v);
        return *this;
    }

    [[nodiscard]] constexpr std::optional<date_type> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr std::optional<date_type>& get() noexcept {
        return value;
    }

    constexpr operator std::optional<date_type> const&() const noexcept {
        return value;
    }
    constexpr operator std::optional<date_type>&() noexcept {
        return value;
    }
};

}  // namespace column_field_detail

}  // namespace ds_mysql
