#pragma once

#include <optional>
#include <string_view>

#include "ds_mysql/column_field_base_core.hpp"
#include "ds_mysql/sql_varchar.hpp"

namespace ds_mysql {

namespace column_field_detail {

// ===================================================================
// base<varchar_type<N>> specializations
// ===================================================================

template <std::size_t N>
struct base<varchar_type<N>> : column_field_tag {
    using value_type = varchar_type<N>;

    varchar_type<N> value{};

    constexpr base() = default;
    constexpr base(varchar_type<N> v) noexcept(std::is_nothrow_move_constructible_v<varchar_type<N>>)
        : value(std::move(v)) {
    }

    template <std::size_t M>
    constexpr base(char const (&v)[M]) noexcept(std::is_nothrow_move_constructible_v<varchar_type<N>>) : value(v) {
    }

    constexpr base& operator=(varchar_type<N> v) noexcept(std::is_nothrow_move_assignable_v<varchar_type<N>>) {
        value = std::move(v);
        return *this;
    }

    template <std::size_t M>
    constexpr base& operator=(char const (&v)[M]) noexcept(std::is_nothrow_move_assignable_v<varchar_type<N>>) {
        value = v;
        return *this;
    }

    [[nodiscard]] constexpr varchar_type<N> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr varchar_type<N>& get() noexcept {
        return value;
    }

    constexpr operator varchar_type<N> const&() const noexcept {
        return value;
    }
    constexpr operator varchar_type<N>&() noexcept {
        return value;
    }
    constexpr operator std::string_view() const noexcept {
        return value.view();
    }
};

template <std::size_t N>
struct base<std::optional<varchar_type<N>>> : column_field_tag {
    using value_type = std::optional<varchar_type<N>>;

    std::optional<varchar_type<N>> value{};

    constexpr base() = default;
    constexpr base(std::nullopt_t) noexcept : value(std::nullopt) {
    }
    constexpr base(varchar_type<N> v) noexcept(std::is_nothrow_move_constructible_v<varchar_type<N>>)
        : value(std::move(v)) {
    }
    constexpr base(std::optional<varchar_type<N>> v) noexcept(
        std::is_nothrow_move_constructible_v<std::optional<varchar_type<N>>>)
        : value(std::move(v)) {
    }

    template <std::size_t M>
    constexpr base(char const (&v)[M]) noexcept(std::is_nothrow_move_constructible_v<varchar_type<N>>) : value(v) {
    }

    constexpr base& operator=(std::nullopt_t) noexcept {
        value = std::nullopt;
        return *this;
    }
    constexpr base& operator=(varchar_type<N> v) noexcept(std::is_nothrow_move_assignable_v<varchar_type<N>>) {
        value = std::move(v);
        return *this;
    }
    constexpr base& operator=(std::optional<varchar_type<N>> v) noexcept(
        std::is_nothrow_move_assignable_v<std::optional<varchar_type<N>>>) {
        value = std::move(v);
        return *this;
    }

    template <std::size_t M>
    constexpr base& operator=(char const (&v)[M]) noexcept(std::is_nothrow_move_assignable_v<varchar_type<N>>) {
        value = varchar_type<N>{v};
        return *this;
    }

    [[nodiscard]] constexpr std::optional<varchar_type<N>> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr std::optional<varchar_type<N>>& get() noexcept {
        return value;
    }

    constexpr operator std::optional<varchar_type<N>> const&() const noexcept {
        return value;
    }
    constexpr operator std::optional<varchar_type<N>>&() noexcept {
        return value;
    }
};

}  // namespace column_field_detail

}  // namespace ds_mysql
