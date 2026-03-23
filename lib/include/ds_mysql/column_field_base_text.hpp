#pragma once

#include <optional>
#include <string_view>

#include "ds_mysql/column_field_base_core.hpp"
#include "ds_mysql/sql_text.hpp"

namespace ds_mysql {

namespace column_field_detail {

// ===================================================================
// base<text_type<Kind>> specializations
// ===================================================================

template <text_kind Kind>
struct base<text_type<Kind>> : column_field_tag {
    using value_type = text_type<Kind>;

    text_type<Kind> value{};

    base() = default;
    base(std::string_view sv) : value(sv) {
    }
    base(std::string const& str) : value(str) {
    }
    base(std::string&& str) noexcept(std::is_nothrow_move_constructible_v<std::string>) : value(std::move(str)) {
    }
    base(char const* str) : value(str) {
    }
    base(text_type<Kind> v) noexcept(std::is_nothrow_move_constructible_v<text_type<Kind>>) : value(std::move(v)) {
    }

    base& operator=(std::string_view sv) {
        value = text_type<Kind>{sv};
        return *this;
    }
    base& operator=(std::string const& str) {
        value = text_type<Kind>{str};
        return *this;
    }
    base& operator=(std::string&& str) noexcept(std::is_nothrow_move_assignable_v<std::string>) {
        value = text_type<Kind>{std::move(str)};
        return *this;
    }
    base& operator=(char const* str) {
        value = text_type<Kind>{str};
        return *this;
    }
    base& operator=(text_type<Kind> v) noexcept(std::is_nothrow_move_assignable_v<text_type<Kind>>) {
        value = std::move(v);
        return *this;
    }

    [[nodiscard]] text_type<Kind> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] text_type<Kind>& get() noexcept {
        return value;
    }

    operator text_type<Kind> const&() const noexcept {
        return value;
    }
    operator text_type<Kind>&() noexcept {
        return value;
    }
    operator std::string_view() const noexcept {
        return value.view();
    }
};

template <text_kind Kind>
struct base<std::optional<text_type<Kind>>> : column_field_tag {
    using value_type = std::optional<text_type<Kind>>;

    std::optional<text_type<Kind>> value{};

    base() = default;
    base(std::nullopt_t) noexcept : value(std::nullopt) {
    }
    base(std::string_view sv) : value(text_type<Kind>{sv}) {
    }
    base(std::string const& str) : value(text_type<Kind>{str}) {
    }
    base(std::string&& str) noexcept(std::is_nothrow_move_constructible_v<std::string>)
        : value(text_type<Kind>{std::move(str)}) {
    }
    base(char const* str) : value(text_type<Kind>{str}) {
    }
    base(text_type<Kind> v) noexcept(std::is_nothrow_move_constructible_v<text_type<Kind>>) : value(std::move(v)) {
    }
    base(std::optional<text_type<Kind>> v) noexcept(
        std::is_nothrow_move_constructible_v<std::optional<text_type<Kind>>>)
        : value(std::move(v)) {
    }

    base& operator=(std::nullopt_t) noexcept {
        value = std::nullopt;
        return *this;
    }
    base& operator=(std::string_view sv) {
        value = text_type<Kind>{sv};
        return *this;
    }
    base& operator=(std::string const& str) {
        value = text_type<Kind>{str};
        return *this;
    }
    base& operator=(std::string&& str) noexcept(std::is_nothrow_move_assignable_v<std::string>) {
        value = text_type<Kind>{std::move(str)};
        return *this;
    }
    base& operator=(char const* str) {
        value = text_type<Kind>{str};
        return *this;
    }
    base& operator=(text_type<Kind> v) noexcept(std::is_nothrow_move_assignable_v<text_type<Kind>>) {
        value = std::move(v);
        return *this;
    }
    base& operator=(std::optional<text_type<Kind>> v) noexcept(
        std::is_nothrow_move_assignable_v<std::optional<text_type<Kind>>>) {
        value = std::move(v);
        return *this;
    }

    [[nodiscard]] std::optional<text_type<Kind>> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] std::optional<text_type<Kind>>& get() noexcept {
        return value;
    }

    operator std::optional<text_type<Kind>> const&() const noexcept {
        return value;
    }
    operator std::optional<text_type<Kind>>&() noexcept {
        return value;
    }
};

}  // namespace column_field_detail

}  // namespace ds_mysql
