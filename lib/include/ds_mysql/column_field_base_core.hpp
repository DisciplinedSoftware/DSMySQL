#pragma once

#include <optional>
#include <string>
#include <type_traits>

#include "ds_mysql/fixed_string.hpp"

namespace ds_mysql {

/**
 * column_field_tag — empty base for type-detection via std::derived_from.
 */
struct column_field_tag {};

namespace column_attr {

struct primary_key {};
struct auto_increment {};
struct unique {};
struct default_current_timestamp {};
struct on_update_current_timestamp {};

template <fixed_string Text>
struct comment {};

template <fixed_string Name>
struct collate {};

}  // namespace column_attr

namespace fk_attr {

template <typename RefTable, typename RefColumn>
struct references {};

struct on_delete_restrict {};
struct on_delete_cascade {};
struct on_delete_set_null {};
struct on_delete_no_action {};

struct on_update_restrict {};
struct on_update_cascade {};
struct on_update_set_null {};
struct on_update_no_action {};

}  // namespace fk_attr

// ===================================================================
// column_field_detail — internal base specializations (core)
// ===================================================================

namespace column_field_detail {

template <typename... Attrs>
struct comment_attr_value {
    static constexpr std::string_view value = "";
};

template <fixed_string Text, typename... Rest>
struct comment_attr_value<column_attr::comment<Text>, Rest...> {
    static constexpr std::string_view value = Text;
};

template <typename First, typename... Rest>
struct comment_attr_value<First, Rest...> {
    static constexpr std::string_view value = comment_attr_value<Rest...>::value;
};

template <typename... Attrs>
struct collate_attr_value {
    static constexpr std::string_view value = "";
};

template <fixed_string Name, typename... Rest>
struct collate_attr_value<column_attr::collate<Name>, Rest...> {
    static constexpr std::string_view value = Name;
};

template <typename First, typename... Rest>
struct collate_attr_value<First, Rest...> {
    static constexpr std::string_view value = collate_attr_value<Rest...>::value;
};

// fk_references_attr — detects fk_attr::references<RefTable, RefColumn> in Attrs.
template <typename... Attrs>
struct fk_references_attr {
    static constexpr bool has_value = false;
    using ref_table = void;
    using ref_column = void;
};

template <typename RefTable, typename RefColumn, typename... Rest>
struct fk_references_attr<fk_attr::references<RefTable, RefColumn>, Rest...> {
    static constexpr bool has_value = true;
    using ref_table = RefTable;
    using ref_column = RefColumn;
};

template <typename First, typename... Rest>
struct fk_references_attr<First, Rest...> : fk_references_attr<Rest...> {};

// fk_on_delete_attr — extracts the on_delete referential action string.
template <typename... Attrs>
struct fk_on_delete_attr {
    static constexpr std::string_view value = "";
};

template <typename... Rest>
struct fk_on_delete_attr<fk_attr::on_delete_restrict, Rest...> {
    static constexpr std::string_view value = "RESTRICT";
};

template <typename... Rest>
struct fk_on_delete_attr<fk_attr::on_delete_cascade, Rest...> {
    static constexpr std::string_view value = "CASCADE";
};

template <typename... Rest>
struct fk_on_delete_attr<fk_attr::on_delete_set_null, Rest...> {
    static constexpr std::string_view value = "SET NULL";
};

template <typename... Rest>
struct fk_on_delete_attr<fk_attr::on_delete_no_action, Rest...> {
    static constexpr std::string_view value = "NO ACTION";
};

template <typename First, typename... Rest>
struct fk_on_delete_attr<First, Rest...> : fk_on_delete_attr<Rest...> {};

// fk_on_update_attr — extracts the on_update referential action string.
template <typename... Attrs>
struct fk_on_update_attr {
    static constexpr std::string_view value = "";
};

template <typename... Rest>
struct fk_on_update_attr<fk_attr::on_update_restrict, Rest...> {
    static constexpr std::string_view value = "RESTRICT";
};

template <typename... Rest>
struct fk_on_update_attr<fk_attr::on_update_cascade, Rest...> {
    static constexpr std::string_view value = "CASCADE";
};

template <typename... Rest>
struct fk_on_update_attr<fk_attr::on_update_set_null, Rest...> {
    static constexpr std::string_view value = "SET NULL";
};

template <typename... Rest>
struct fk_on_update_attr<fk_attr::on_update_no_action, Rest...> {
    static constexpr std::string_view value = "NO ACTION";
};

template <typename First, typename... Rest>
struct fk_on_update_attr<First, Rest...> : fk_on_update_attr<Rest...> {};

// ===================================================================
// Primary base<T> template and generic specializations
// ===================================================================

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

}  // namespace column_field_detail

}  // namespace ds_mysql
