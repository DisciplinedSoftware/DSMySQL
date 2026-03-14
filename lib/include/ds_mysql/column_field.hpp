#pragma once

#include <chrono>
#include <concepts>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "ds_mysql/fixed_string.hpp"
#include "ds_mysql/sql_temporal.hpp"
#include "ds_mysql/varchar_field.hpp"

namespace ds_mysql {

/**
 * column_field_tag — empty base for type-detection via std::derived_from.
 */
struct column_field_tag {};

// column_field<Name, T> — the only user-facing form.
// Name is the SQL column name as a string literal; T is the stored value type.
//   using id = column_field<"id", uint32_t>;
template <fixed_string Name, typename T>
struct column_field;

// ===================================================================
// column_field_detail — internal base specializations
//
// These are implementation bases, not user-facing API.
// Users must always use the tagged two-parameter form:
//   using id = column_field<struct id_tag, uint32_t>;
// ===================================================================

namespace column_field_detail {

template <typename T>
struct base : column_field_tag {
    using value_type = T;

    T value{};

    constexpr base() = default;
    constexpr base(T v) noexcept(std::is_nothrow_move_constructible_v<T>) : value(std::move(v)) {
    }

    constexpr base& operator=(T v) noexcept(std::is_nothrow_move_assignable_v<T>) {
        value = std::move(v);
        return *this;
    }

    [[nodiscard]] constexpr T const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr T& get() noexcept {
        return value;
    }

    constexpr operator T const&() const noexcept {
        return value;
    }
    constexpr operator T&() noexcept {
        return value;
    }
};

template <typename T>
struct base<std::optional<T>> : column_field_tag {
    using value_type = std::optional<T>;

    std::optional<T> value{};

    constexpr base() = default;
    constexpr base(std::nullopt_t) noexcept : value(std::nullopt) {
    }
    constexpr base(T v) noexcept(std::is_nothrow_move_constructible_v<T>) : value(std::move(v)) {
    }
    constexpr base(std::optional<T> v) noexcept(std::is_nothrow_move_constructible_v<std::optional<T>>)
        : value(std::move(v)) {
    }

    constexpr base& operator=(std::nullopt_t) noexcept {
        value = std::nullopt;
        return *this;
    }
    constexpr base& operator=(T v) noexcept(std::is_nothrow_move_assignable_v<T>) {
        value = std::move(v);
        return *this;
    }
    constexpr base& operator=(std::optional<T> v) noexcept(std::is_nothrow_move_assignable_v<std::optional<T>>) {
        value = std::move(v);
        return *this;
    }

    [[nodiscard]] constexpr std::optional<T> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr std::optional<T>& get() noexcept {
        return value;
    }

    constexpr operator std::optional<T> const&() const noexcept {
        return value;
    }
    constexpr operator std::optional<T>&() noexcept {
        return value;
    }
};

template <>
struct base<std::string> : column_field_tag {
    using value_type = std::string;

    std::string value{};

    base() = default;
    base(std::string_view sv) : value(sv) {
    }
    base(std::string const& str) : value(str) {
    }
    base(std::string&& str) noexcept(std::is_nothrow_move_constructible_v<std::string>) : value(std::move(str)) {
    }
    base(char const* str) : value(str) {
    }

    base& operator=(std::string_view sv) {
        value = sv;
        return *this;
    }
    base& operator=(std::string const& str) {
        value = str;
        return *this;
    }
    base& operator=(std::string&& str) noexcept(std::is_nothrow_move_assignable_v<std::string>) {
        value = std::move(str);
        return *this;
    }
    base& operator=(char const* str) {
        value = str;
        return *this;
    }

    [[nodiscard]] std::string const& get() const noexcept {
        return value;
    }
    [[nodiscard]] std::string& get() noexcept {
        return value;
    }

    operator std::string const&() const noexcept {
        return value;
    }
    operator std::string&() noexcept {
        return value;
    }
};

template <std::size_t N>
struct base<varchar_field<N>> : column_field_tag {
    using value_type = varchar_field<N>;

    varchar_field<N> value{};

    constexpr base() = default;
    constexpr base(varchar_field<N> v) noexcept(std::is_nothrow_move_constructible_v<varchar_field<N>>)
        : value(std::move(v)) {
    }

    template <std::size_t M>
    constexpr base(char const (&v)[M]) noexcept(std::is_nothrow_move_constructible_v<varchar_field<N>>) : value(v) {
    }

    constexpr base& operator=(varchar_field<N> v) noexcept(std::is_nothrow_move_assignable_v<varchar_field<N>>) {
        value = std::move(v);
        return *this;
    }

    template <std::size_t M>
    constexpr base& operator=(char const (&v)[M]) noexcept(std::is_nothrow_move_assignable_v<varchar_field<N>>) {
        value = v;
        return *this;
    }

    [[nodiscard]] constexpr varchar_field<N> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr varchar_field<N>& get() noexcept {
        return value;
    }

    constexpr operator varchar_field<N> const&() const noexcept {
        return value;
    }
    constexpr operator varchar_field<N>&() noexcept {
        return value;
    }
    constexpr operator std::string_view() const noexcept {
        return value.view();
    }
};

template <std::size_t N>
struct base<std::optional<varchar_field<N>>> : column_field_tag {
    using value_type = std::optional<varchar_field<N>>;

    std::optional<varchar_field<N>> value{};

    constexpr base() = default;
    constexpr base(std::nullopt_t) noexcept : value(std::nullopt) {
    }
    constexpr base(varchar_field<N> v) noexcept(std::is_nothrow_move_constructible_v<varchar_field<N>>)
        : value(std::move(v)) {
    }
    constexpr base(std::optional<varchar_field<N>> v) noexcept(
        std::is_nothrow_move_constructible_v<std::optional<varchar_field<N>>>)
        : value(std::move(v)) {
    }

    template <std::size_t M>
    constexpr base(char const (&v)[M]) noexcept(std::is_nothrow_move_constructible_v<varchar_field<N>>) : value(v) {
    }

    constexpr base& operator=(std::nullopt_t) noexcept {
        value = std::nullopt;
        return *this;
    }
    constexpr base& operator=(varchar_field<N> v) noexcept(std::is_nothrow_move_assignable_v<varchar_field<N>>) {
        value = std::move(v);
        return *this;
    }
    constexpr base& operator=(std::optional<varchar_field<N>> v) noexcept(
        std::is_nothrow_move_assignable_v<std::optional<varchar_field<N>>>) {
        value = std::move(v);
        return *this;
    }

    template <std::size_t M>
    constexpr base& operator=(char const (&v)[M]) noexcept(std::is_nothrow_move_assignable_v<varchar_field<N>>) {
        value = varchar_field<N>{v};
        return *this;
    }

    [[nodiscard]] constexpr std::optional<varchar_field<N>> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr std::optional<varchar_field<N>>& get() noexcept {
        return value;
    }

    constexpr operator std::optional<varchar_field<N>> const&() const noexcept {
        return value;
    }
    constexpr operator std::optional<varchar_field<N>>&() noexcept {
        return value;
    }
};

template <>
struct base<sql_datetime> : column_field_tag {
    using value_type = sql_datetime;

    sql_datetime value{};

    constexpr base() = default;
    base(sql_now_t) noexcept : value(sql_now_t{}) {
    }
    base(std::chrono::system_clock::time_point tp) noexcept : value(tp) {
    }
    base(sql_datetime v) noexcept : value(v) {
    }

    base& operator=(sql_now_t) noexcept {
        value = sql_datetime{sql_now_t{}};
        return *this;
    }
    base& operator=(std::chrono::system_clock::time_point tp) noexcept {
        value = sql_datetime{tp};
        return *this;
    }
    base& operator=(sql_datetime v) noexcept {
        value = v;
        return *this;
    }

    [[nodiscard]] constexpr sql_datetime const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr sql_datetime& get() noexcept {
        return value;
    }

    constexpr operator sql_datetime const&() const noexcept {
        return value;
    }
    constexpr operator sql_datetime&() noexcept {
        return value;
    }
};

template <>
struct base<std::optional<sql_datetime>> : column_field_tag {
    using value_type = std::optional<sql_datetime>;

    std::optional<sql_datetime> value{};

    constexpr base() = default;
    constexpr base(std::nullopt_t) noexcept : value(std::nullopt) {
    }
    base(sql_now_t) noexcept : value(sql_datetime{sql_now_t{}}) {
    }
    base(std::chrono::system_clock::time_point tp) noexcept : value(sql_datetime{tp}) {
    }
    base(sql_datetime v) noexcept : value(std::move(v)) {
    }
    base(std::optional<sql_datetime> v) noexcept : value(std::move(v)) {
    }

    constexpr base& operator=(std::nullopt_t) noexcept {
        value = std::nullopt;
        return *this;
    }
    constexpr base& operator=(sql_now_t) noexcept {
        value = sql_datetime{sql_now_t{}};
        return *this;
    }
    constexpr base& operator=(std::chrono::system_clock::time_point tp) noexcept {
        value = sql_datetime{tp};
        return *this;
    }
    constexpr base& operator=(sql_datetime v) noexcept {
        value = std::move(v);
        return *this;
    }
    constexpr base& operator=(std::optional<sql_datetime> v) noexcept {
        value = std::move(v);
        return *this;
    }

    [[nodiscard]] constexpr std::optional<sql_datetime> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr std::optional<sql_datetime>& get() noexcept {
        return value;
    }

    constexpr operator std::optional<sql_datetime> const&() const noexcept {
        return value;
    }
    constexpr operator std::optional<sql_datetime>&() noexcept {
        return value;
    }
};

template <>
struct base<sql_timestamp> : column_field_tag {
    using value_type = sql_timestamp;

    sql_timestamp value{};

    constexpr base() = default;
    base(sql_now_t) noexcept : value(sql_now_t{}) {
    }
    base(std::chrono::system_clock::time_point tp) noexcept : value(tp) {
    }
    base(sql_timestamp v) noexcept : value(v) {
    }

    base& operator=(sql_now_t) noexcept {
        value = sql_timestamp{sql_now_t{}};
        return *this;
    }
    base& operator=(std::chrono::system_clock::time_point tp) noexcept {
        value = sql_timestamp{tp};
        return *this;
    }
    base& operator=(sql_timestamp v) noexcept {
        value = v;
        return *this;
    }

    [[nodiscard]] constexpr sql_timestamp const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr sql_timestamp& get() noexcept {
        return value;
    }

    constexpr operator sql_timestamp const&() const noexcept {
        return value;
    }
    constexpr operator sql_timestamp&() noexcept {
        return value;
    }
};

template <>
struct base<std::optional<sql_timestamp>> : column_field_tag {
    using value_type = std::optional<sql_timestamp>;

    std::optional<sql_timestamp> value{};

    constexpr base() = default;
    constexpr base(std::nullopt_t) noexcept : value(std::nullopt) {
    }
    base(sql_now_t) noexcept : value(sql_timestamp{sql_now_t{}}) {
    }
    base(std::chrono::system_clock::time_point tp) noexcept : value(sql_timestamp{tp}) {
    }
    base(sql_timestamp v) noexcept : value(std::move(v)) {
    }
    base(std::optional<sql_timestamp> v) noexcept : value(std::move(v)) {
    }

    constexpr base& operator=(std::nullopt_t) noexcept {
        value = std::nullopt;
        return *this;
    }
    constexpr base& operator=(sql_now_t) noexcept {
        value = sql_timestamp{sql_now_t{}};
        return *this;
    }
    constexpr base& operator=(std::chrono::system_clock::time_point tp) noexcept {
        value = sql_timestamp{tp};
        return *this;
    }
    constexpr base& operator=(sql_timestamp v) noexcept {
        value = std::move(v);
        return *this;
    }
    constexpr base& operator=(std::optional<sql_timestamp> v) noexcept {
        value = std::move(v);
        return *this;
    }

    [[nodiscard]] constexpr std::optional<sql_timestamp> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr std::optional<sql_timestamp>& get() noexcept {
        return value;
    }

    constexpr operator std::optional<sql_timestamp> const&() const noexcept {
        return value;
    }
    constexpr operator std::optional<sql_timestamp>&() noexcept {
        return value;
    }
};

}  // namespace column_field_detail

// ===================================================================
// column_field<Name, T> — the only user-facing form
// ===================================================================

/**
 * column_field<Name, T> — named column descriptor.
 *
 * Name is the SQL column name embedded directly as a string literal.
 * T is the stored value type. All constructors and operators are inherited
 * from the internal base type.
 *
 *   using id     = column_field<"id",     uint32_t>;
 *   using ticker = column_field<"ticker", varchar_field<32>>;
 *   using sector = column_field<"sector", std::optional<varchar_field<64>>>;
 *
 *   id     id_;
 *   ticker ticker_;
 *   sector sector_;
 *
 * SQL column names: "id", "ticker", "sector"
 */
template <fixed_string Name, typename T>
struct column_field : column_field_detail::base<T> {
    using column_field_detail::base<T>::base;
    using column_field_detail::base<T>::operator=;

    [[nodiscard]] static constexpr std::string_view column_name() noexcept {
        return Name;
    }
};

// ===================================================================
// ColumnFieldType concept
// ===================================================================

/**
 * ColumnFieldType<T> — satisfied by any type that publicly derives from
 * column_field_tag and exposes a value_type alias.
 *
 * Only satisfied by the named form:
 *   using id = column_field<"id", uint32_t>;
 */
template <typename T>
concept ColumnFieldType = std::derived_from<T, column_field_tag> && requires { typename T::value_type; };

// ===================================================================
// unwrap_column_field
// ===================================================================

/**
 * unwrap_column_field<T>::type — strips the column_field<T> wrapper if
 * present; identity for all other types.
 *
 * Allows generic code (schema generation, col<> value_type) to work with
 * both raw types (uint32_t) and column field wrappers (symbol::id).
 */
template <typename T>
struct unwrap_column_field {
    using type = T;
};

template <ColumnFieldType T>
struct unwrap_column_field<T> {
    using type = typename T::value_type;
};

template <typename T>
using unwrap_column_field_t = typename unwrap_column_field<T>::type;

}  // namespace ds_mysql

// clang-format off
/**
 * COLUMN_FIELD(tag, type) — convenience macro to declare a column field member.
 *
 * Expands to a type alias and a member variable in one line:
 *
 *   COLUMN_FIELD(id, uint32_t)
 *   // equivalent to:
 *   using id = ::ds_mysql::column_field<"id", uint32_t>;
 *   id id_;
 */
#define COLUMN_FIELD(tag, type)                         \
    using tag = ::ds_mysql::column_field<#tag, type>;   \
    tag tag##_;
// clang-format on
