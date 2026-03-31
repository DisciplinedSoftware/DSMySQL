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

// ===================================================================
// is_fixed_string — type trait for fixed_string<N> detection
// ===================================================================

template <typename T>
struct is_fixed_string : std::false_type {};

template <std::size_t N>
struct is_fixed_string<fixed_string<N>> : std::true_type {};

template <typename T>
inline constexpr bool is_fixed_string_v = is_fixed_string<T>::value;

/// Sentinel type for SQL CURRENT_TIMESTAMP keyword.
struct current_timestamp_t {};
inline constexpr current_timestamp_t current_timestamp{};

/// Sentinel type for SQL DEFAULT keyword in INSERT statements.
/// Use with the field-based insert API or assign to auto_increment / default_value columns:
///   insert_into(table{}).values(sql_default(), col2{"val"}, ...).build_sql()
///   table::id field{sql_default()};
struct sql_default_t {};
[[nodiscard]] inline constexpr sql_default_t sql_default() noexcept {
    return {};
}

namespace column_attr {

struct primary_key {};
struct auto_increment {};
struct unique {};

/**
 * default_value<T> — typed column DEFAULT attribute.
 *
 * The value type must match the column's value type (or underlying type
 * for wrapper types). String defaults use fixed_string via CTAD from
 * string literals:
 *
 *   column_attr::default_value(0)                    // integral
 *   column_attr::default_value(1.0)                  // floating-point
 *   column_attr::default_value("active")             // string (auto-quoted in SQL)
 *   default_value(ds_mysql::current_timestamp)    // temporal
 */
template <typename T>
struct default_value {
    T val;
    consteval default_value(T v) : val(v) {
    }
};

template <std::size_t N>
default_value(char const (&)[N]) -> default_value<fixed_string<N>>;

/**
 * on_update<T> — typed column ON UPDATE attribute.
 *
 *   on_update(ds_mysql::current_timestamp)    // ON UPDATE CURRENT_TIMESTAMP
 */
template <typename T>
struct on_update {
    T val;
    consteval on_update(T v) : val(v) {
    }
};

template <std::size_t N>
struct comment {
    fixed_string<N> value;
    consteval comment(char const (&str)[N]) : value(str) {
    }
};
template <std::size_t N>
comment(char const (&)[N]) -> comment<N>;

template <std::size_t N>
struct collate {
    fixed_string<N> value;
    consteval collate(char const (&str)[N]) : value(str) {
    }
};
template <std::size_t N>
collate(char const (&)[N]) -> collate<N>;

}  // namespace column_attr

namespace fk_attr {

template <typename RefTable, typename RefColumn>
struct references {};

// --- Flat attribute types (original API) ---

struct on_delete_restrict {};
struct on_delete_cascade {};
struct on_delete_set_null {};
struct on_delete_no_action {};

struct on_update_restrict {};
struct on_update_cascade {};
struct on_update_set_null {};
struct on_update_no_action {};

// --- Composable action tags ---
//
// Two alternative styles are provided alongside the flat types above:
//
//   Intermediate:   fk_attr::on_delete(fk_attr::cascade)
//   Fully composed: fk_attr::on(fk_attr::delete_(fk_attr::cascade))
//
// Both return the same flat attribute types, so they are interchangeable
// in COLUMN_FIELD declarations:
//
//   COLUMN_FIELD(fk_col, uint32_t,
//                fk_attr::references<other, other::id>{},
//                fk_attr::on_delete(fk_attr::cascade),   // intermediate
//                fk_attr::on(fk_attr::update_(fk_attr::set_null)))  // composed
//
// Design note: the `on(update_(...))` syntax reads closer to SQL
// ("ON UPDATE CASCADE") and separates the event from the action.
// The flat types remain for backward compatibility and brevity when
// the extra composition isn't needed.

struct cascade_t {};
struct restrict_t {};
struct set_null_t {};
struct no_action_t {};

inline constexpr cascade_t cascade{};
inline constexpr restrict_t restrict_{};
inline constexpr set_null_t set_null{};
inline constexpr no_action_t no_action{};

// on_delete(action) — intermediate composable style
constexpr on_delete_cascade on_delete(cascade_t) {
    return {};
}
constexpr on_delete_restrict on_delete(restrict_t) {
    return {};
}
constexpr on_delete_set_null on_delete(set_null_t) {
    return {};
}
constexpr on_delete_no_action on_delete(no_action_t) {
    return {};
}

// on_update(action) — intermediate composable style
constexpr on_update_cascade on_update(cascade_t) {
    return {};
}
constexpr on_update_restrict on_update(restrict_t) {
    return {};
}
constexpr on_update_set_null on_update(set_null_t) {
    return {};
}
constexpr on_update_no_action on_update(no_action_t) {
    return {};
}

// Descriptors for fully composable on(delete_(...)) / on(update_(...)) style
template <typename Action>
struct delete_action {};

template <typename Action>
struct update_action {};

constexpr delete_action<cascade_t> delete_(cascade_t) {
    return {};
}
constexpr delete_action<restrict_t> delete_(restrict_t) {
    return {};
}
constexpr delete_action<set_null_t> delete_(set_null_t) {
    return {};
}
constexpr delete_action<no_action_t> delete_(no_action_t) {
    return {};
}

constexpr update_action<cascade_t> update_(cascade_t) {
    return {};
}
constexpr update_action<restrict_t> update_(restrict_t) {
    return {};
}
constexpr update_action<set_null_t> update_(set_null_t) {
    return {};
}
constexpr update_action<no_action_t> update_(no_action_t) {
    return {};
}

// on(delete_action<A>) / on(update_action<A>) — dispatch to flat types
template <typename A>
constexpr auto on(delete_action<A>) {
    return fk_attr::on_delete(A{});
}

template <typename A>
constexpr auto on(update_action<A>) {
    return fk_attr::on_update(A{});
}

}  // namespace fk_attr

// ===================================================================
// column_field_detail — attribute extraction for auto... NTTP packs
// ===================================================================

namespace column_field_detail {

// is_attr_instance — true when T is Tmpl<N> for some N.
template <typename T, template <std::size_t> class Tmpl>
struct is_attr_instance : std::false_type {};

template <std::size_t N, template <std::size_t> class Tmpl>
struct is_attr_instance<Tmpl<N>, Tmpl> : std::true_type {};

// is_default_value_attr — true when T is column_attr::default_value<V> for some V.
template <typename T>
struct is_default_value_attr : std::false_type {};

template <typename V>
struct is_default_value_attr<column_attr::default_value<V>> : std::true_type {};

template <typename T>
inline constexpr bool is_default_value_attr_v = is_default_value_attr<T>::value;

// is_on_update_attr — true when T is column_attr::on_update<V> for some V.
template <typename T>
struct is_on_update_attr : std::false_type {};

template <typename V>
struct is_on_update_attr<column_attr::on_update<V>> : std::true_type {};

template <typename T>
inline constexpr bool is_on_update_attr_v = is_on_update_attr<T>::value;

// string_attr_value — extracts the string value from the first matching
// value-carrying attribute (comment, collate) in an auto... pack.
template <template <std::size_t> class AttrTmpl, auto... Attrs>
struct string_attr_value {
    static constexpr std::string_view value = "";
};

template <template <std::size_t> class AttrTmpl, auto First, auto... Rest>
struct string_attr_value<AttrTmpl, First, Rest...> {
    static constexpr std::string_view value = [] {
        if constexpr (is_attr_instance<decltype(First), AttrTmpl>::value) {
            return std::string_view(First.value);
        } else {
            return string_attr_value<AttrTmpl, Rest...>::value;
        }
    }();
};

// ===================================================================
// default_value SQL literal formatting
// ===================================================================

template <typename T>
    requires std::is_integral_v<T>
std::string format_sql_default(T val) {
    return std::to_string(val);
}

template <typename T>
    requires std::is_floating_point_v<T>
std::string format_sql_default(T val) {
    auto s = std::to_string(val);
    auto dot = s.find('.');
    if (dot != std::string::npos) {
        auto last = s.find_last_not_of('0');
        if (last == dot)
            last++;
        s.erase(last + 1);
    }
    return s;
}

template <std::size_t N>
std::string format_sql_default(fixed_string<N> const& val) {
    std::string result = "'";
    result += std::string_view(val);
    result += "'";
    return result;
}

inline std::string format_sql_default(current_timestamp_t) {
    return "CURRENT_TIMESTAMP";
}

// default_value_sql — finds the default_value attr and formats its value as SQL.
template <auto... Attrs>
struct default_value_sql {
    static std::string get() {
        return "";
    }
};

template <auto First, auto... Rest>
struct default_value_sql<First, Rest...> {
    static std::string get() {
        if constexpr (is_default_value_attr_v<decltype(First)>) {
            return format_sql_default(First.val);
        } else {
            return default_value_sql<Rest...>::get();
        }
    }
};

// on_update_sql — finds the on_update attr and formats its value as SQL.
template <auto... Attrs>
struct on_update_sql {
    static std::string get() {
        return "";
    }
};

template <auto First, auto... Rest>
struct on_update_sql<First, Rest...> {
    static std::string get() {
        if constexpr (is_on_update_attr_v<decltype(First)>) {
            return format_sql_default(First.val);
        } else {
            return on_update_sql<Rest...>::get();
        }
    }
};

// ===================================================================
// FK attribute extraction
// ===================================================================

// is_fk_references — true when T is fk_attr::references<RT, RC>.
template <typename T>
struct is_fk_references : std::false_type {};

template <typename RT, typename RC>
struct is_fk_references<fk_attr::references<RT, RC>> : std::true_type {};

// fk_ref_from_type — extracts ref_table / ref_column from a references type.
template <typename T>
struct fk_ref_from_type {
    static constexpr bool has_value = false;
    using ref_table = void;
    using ref_column = void;
};

template <typename RT, typename RC>
struct fk_ref_from_type<fk_attr::references<RT, RC>> {
    static constexpr bool has_value = true;
    using ref_table = RT;
    using ref_column = RC;
};

// find_fk_ref — searches an auto... pack for an fk_attr::references<> instance.
template <auto... Attrs>
struct find_fk_ref : fk_ref_from_type<void> {};

template <auto First, auto... Rest>
struct find_fk_ref<First, Rest...> : std::conditional_t<is_fk_references<decltype(First)>::value,
                                                        fk_ref_from_type<decltype(First)>, find_fk_ref<Rest...>> {};

// fk_on_delete_value — extracts the ON DELETE action string from an auto... pack.
template <auto... Attrs>
struct fk_on_delete_value {
    static constexpr std::string_view value = "";
};

template <auto First, auto... Rest>
struct fk_on_delete_value<First, Rest...> {
    static constexpr std::string_view value = [] {
        using FT = decltype(First);
        if constexpr (std::is_same_v<FT, fk_attr::on_delete_restrict>)
            return std::string_view("RESTRICT");
        else if constexpr (std::is_same_v<FT, fk_attr::on_delete_cascade>)
            return std::string_view("CASCADE");
        else if constexpr (std::is_same_v<FT, fk_attr::on_delete_set_null>)
            return std::string_view("SET NULL");
        else if constexpr (std::is_same_v<FT, fk_attr::on_delete_no_action>)
            return std::string_view("NO ACTION");
        else
            return fk_on_delete_value<Rest...>::value;
    }();
};

// fk_on_update_value — extracts the ON UPDATE action string from an auto... pack.
template <auto... Attrs>
struct fk_on_update_value {
    static constexpr std::string_view value = "";
};

template <auto First, auto... Rest>
struct fk_on_update_value<First, Rest...> {
    static constexpr std::string_view value = [] {
        using FT = decltype(First);
        if constexpr (std::is_same_v<FT, fk_attr::on_update_restrict>)
            return std::string_view("RESTRICT");
        else if constexpr (std::is_same_v<FT, fk_attr::on_update_cascade>)
            return std::string_view("CASCADE");
        else if constexpr (std::is_same_v<FT, fk_attr::on_update_set_null>)
            return std::string_view("SET NULL");
        else if constexpr (std::is_same_v<FT, fk_attr::on_update_no_action>)
            return std::string_view("NO ACTION");
        else
            return fk_on_update_value<Rest...>::value;
    }();
};

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
