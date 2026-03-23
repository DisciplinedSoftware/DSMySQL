#pragma once

#include <chrono>
#include <concepts>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "ds_mysql/fixed_string.hpp"
#include "ds_mysql/name_reflection.hpp"
#include "ds_mysql/numeric_field.hpp"
#include "ds_mysql/sql_temporal.hpp"
#include "ds_mysql/varchar_field.hpp"

namespace ds_mysql {

/**
 * column_field_tag — empty base for type-detection via std::derived_from.
 */
struct column_field_tag {};

namespace column_attr {

struct auto_increment {};
struct unique {};
struct default_current_timestamp {};
struct on_update_current_timestamp {};

template <fixed_string Text>
struct comment {};

template <fixed_string Name>
struct collate {};

}  // namespace column_attr

// ===================================================================
// fk_attr — typed DDL modifiers for FOREIGN KEY constraints
//
// Use as Attrs on column_field / COLUMN_FIELD to declare a FK inline:
//
//   COLUMN_FIELD(parent_id, uint32_t,
//                fk_attr::references<parent_table, parent_table::id>,
//                fk_attr::on_delete_cascade,
//                fk_attr::on_update_cascade)
//
// references<RefTable, RefColumn>
//   RefTable   — the C++ struct type of the referenced table.
//   RefColumn  — the column_field type within RefTable (e.g. parent_table::id).
//   The SQL table name is derived via table_name_for<RefTable> and the
//   SQL column name via RefColumn::column_name().
//
// on_delete_* / on_update_* — referential actions (default: no clause = RESTRICT).
// ===================================================================
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

// column_field<Name, T> — the only user-facing form.
// Name is the SQL column name as a string literal; T is the stored value type.
//   using id = column_field<"id", uint32_t>;
// Optional Attrs are typed DDL modifiers (e.g. column_attr::auto_increment).
template <fixed_string Name, typename T, typename... Attrs>
struct column_field;

// ===================================================================
// column_field_detail — internal base specializations
//
// These are implementation bases, not user-facing API.
// Users must always use the tagged two-parameter form:
//   using id = column_field<struct id_tag, uint32_t>;
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
struct base<datetime_type> : column_field_tag {
    using value_type = datetime_type;

    datetime_type value{};

    constexpr base() = default;
    base(sql_now_t) noexcept : value(sql_now_t{}) {
    }
    base(std::chrono::system_clock::time_point tp) noexcept : value(tp) {
    }
    base(datetime_type v) noexcept : value(v) {
    }

    base& operator=(sql_now_t) noexcept {
        value = datetime_type{sql_now_t{}};
        return *this;
    }
    base& operator=(std::chrono::system_clock::time_point tp) noexcept {
        value = datetime_type{tp};
        return *this;
    }
    base& operator=(datetime_type v) noexcept {
        value = v;
        return *this;
    }

    [[nodiscard]] constexpr datetime_type const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr datetime_type& get() noexcept {
        return value;
    }

    constexpr operator datetime_type const&() const noexcept {
        return value;
    }
    constexpr operator datetime_type&() noexcept {
        return value;
    }
};

template <>
struct base<std::optional<datetime_type>> : column_field_tag {
    using value_type = std::optional<datetime_type>;

    std::optional<datetime_type> value{};

    constexpr base() = default;
    constexpr base(std::nullopt_t) noexcept : value(std::nullopt) {
    }
    base(sql_now_t) noexcept : value(datetime_type{sql_now_t{}}) {
    }
    base(std::chrono::system_clock::time_point tp) noexcept : value(datetime_type{tp}) {
    }
    base(datetime_type v) noexcept : value(std::move(v)) {
    }
    base(std::optional<datetime_type> v) noexcept : value(std::move(v)) {
    }

    constexpr base& operator=(std::nullopt_t) noexcept {
        value = std::nullopt;
        return *this;
    }
    constexpr base& operator=(sql_now_t) noexcept {
        value = datetime_type{sql_now_t{}};
        return *this;
    }
    constexpr base& operator=(std::chrono::system_clock::time_point tp) noexcept {
        value = datetime_type{tp};
        return *this;
    }
    constexpr base& operator=(datetime_type v) noexcept {
        value = std::move(v);
        return *this;
    }
    constexpr base& operator=(std::optional<datetime_type> v) noexcept {
        value = std::move(v);
        return *this;
    }

    [[nodiscard]] constexpr std::optional<datetime_type> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr std::optional<datetime_type>& get() noexcept {
        return value;
    }

    constexpr operator std::optional<datetime_type> const&() const noexcept {
        return value;
    }
    constexpr operator std::optional<datetime_type>&() noexcept {
        return value;
    }
};

template <>
struct base<timestamp_type> : column_field_tag {
    using value_type = timestamp_type;

    timestamp_type value{};

    constexpr base() = default;
    base(sql_now_t) noexcept : value(sql_now_t{}) {
    }
    base(std::chrono::system_clock::time_point tp) noexcept : value(tp) {
    }
    base(timestamp_type v) noexcept : value(v) {
    }

    base& operator=(sql_now_t) noexcept {
        value = timestamp_type{sql_now_t{}};
        return *this;
    }
    base& operator=(std::chrono::system_clock::time_point tp) noexcept {
        value = timestamp_type{tp};
        return *this;
    }
    base& operator=(timestamp_type v) noexcept {
        value = v;
        return *this;
    }

    [[nodiscard]] constexpr timestamp_type const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr timestamp_type& get() noexcept {
        return value;
    }

    constexpr operator timestamp_type const&() const noexcept {
        return value;
    }
    constexpr operator timestamp_type&() noexcept {
        return value;
    }
};

template <>
struct base<std::optional<timestamp_type>> : column_field_tag {
    using value_type = std::optional<timestamp_type>;

    std::optional<timestamp_type> value{};

    constexpr base() = default;
    constexpr base(std::nullopt_t) noexcept : value(std::nullopt) {
    }
    base(sql_now_t) noexcept : value(timestamp_type{sql_now_t{}}) {
    }
    base(std::chrono::system_clock::time_point tp) noexcept : value(timestamp_type{tp}) {
    }
    base(timestamp_type v) noexcept : value(std::move(v)) {
    }
    base(std::optional<timestamp_type> v) noexcept : value(std::move(v)) {
    }

    constexpr base& operator=(std::nullopt_t) noexcept {
        value = std::nullopt;
        return *this;
    }
    constexpr base& operator=(sql_now_t) noexcept {
        value = timestamp_type{sql_now_t{}};
        return *this;
    }
    constexpr base& operator=(std::chrono::system_clock::time_point tp) noexcept {
        value = timestamp_type{tp};
        return *this;
    }
    constexpr base& operator=(timestamp_type v) noexcept {
        value = std::move(v);
        return *this;
    }
    constexpr base& operator=(std::optional<timestamp_type> v) noexcept {
        value = std::move(v);
        return *this;
    }

    [[nodiscard]] constexpr std::optional<timestamp_type> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr std::optional<timestamp_type>& get() noexcept {
        return value;
    }

    constexpr operator std::optional<timestamp_type> const&() const noexcept {
        return value;
    }
    constexpr operator std::optional<timestamp_type>&() noexcept {
        return value;
    }
};

}  // namespace column_field_detail

// ===================================================================
// column_field<Name, T> — the only user-facing form
// ===================================================================

/**
 * column_field<Name, T, Attrs...> — named column descriptor.
 *
 * Name is the SQL column name embedded directly as a string literal.
 * T is the stored value type. Attrs are optional typed DDL modifiers
 * (column_attr::*). All constructors and operators are inherited from the
 * internal base type.
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
template <fixed_string Name, typename T, typename... Attrs>
struct column_field : column_field_detail::base<T> {
    using column_field_detail::base<T>::base;
    using column_field_detail::base<T>::operator=;

    static constexpr bool ddl_auto_increment = (std::same_as<Attrs, column_attr::auto_increment> || ...);
    static constexpr bool ddl_unique = (std::same_as<Attrs, column_attr::unique> || ...);
    static constexpr bool ddl_default_current_timestamp =
        (std::same_as<Attrs, column_attr::default_current_timestamp> || ...);
    static constexpr bool ddl_on_update_current_timestamp =
        (std::same_as<Attrs, column_attr::on_update_current_timestamp> || ...);
    static constexpr std::string_view ddl_comment = column_field_detail::comment_attr_value<Attrs...>::value;
    static constexpr std::string_view ddl_collate = column_field_detail::collate_attr_value<Attrs...>::value;

    // Foreign key attributes — populated only when an fk_attr::references<> is present.
    static constexpr bool ddl_has_fk = column_field_detail::fk_references_attr<Attrs...>::has_value;
    using ddl_fk_ref_table = typename column_field_detail::fk_references_attr<Attrs...>::ref_table;
    using ddl_fk_ref_column = typename column_field_detail::fk_references_attr<Attrs...>::ref_column;
    static constexpr std::string_view ddl_fk_on_delete = column_field_detail::fk_on_delete_attr<Attrs...>::value;
    static constexpr std::string_view ddl_fk_on_update = column_field_detail::fk_on_update_attr<Attrs...>::value;

    template <typename Attr>
    [[nodiscard]] static consteval bool has_attribute() noexcept {
        return (std::same_as<Attr, Attrs> || ...);
    }

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
 * Satisfied by named column descriptors (with or without typed attributes):
 *   using id = column_field<"id", uint32_t>;
 *   using id = column_field<"id", uint32_t, column_attr::auto_increment>;
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

// ===================================================================
// tagged_column_field — tag-struct based column descriptor
// ===================================================================

namespace ds_mysql {

/**
 * tagged_column_field<Tag, T> — tag-struct based column descriptor.
 *
 * The SQL column name is derived at compile time from the tag type name by
 * stripping a trailing "_tag" suffix.  Unlike a plain type alias, this is a
 * *distinct* type for every unique Tag, which preserves cross-table type
 * isolation: two tables that each declare an "id" column end up with
 * different C++ types, so mixing them is a compile-time error.
 *
 * The tag struct MUST be defined as a nested class of the owning table —
 * not globally and not inline in the template argument list.  Defining it
 * inside the table struct ensures uniqueness and enables the compile-time
 * membership check in from<Table>().
 *
 * Preferred usage via the COLUMN_FIELD macro (which generates the nested
 * tag struct automatically):
 *
 *   struct my_table {
 *       COLUMN_FIELD(id,    uint32_t)
 *       COLUMN_FIELD(price, double)
 *   };
 *
 * Or explicitly:
 *
 *   struct my_table {
 *       struct id_tag {};
 *       using id = tagged_column_field<id_tag, uint32_t>;
 *       id id_;
 *   };
 *
 * Both produce the SQL column name "id" (derived from the tag name).
 */
template <typename Tag, typename T, typename... Attrs>
struct tagged_column_field : column_field<detail::column_name_from_tag<Tag>(), T, Attrs...> {
    using tag_type = Tag;

private:
    using base = column_field<detail::column_name_from_tag<Tag>(), T, Attrs...>;

public:
    using base::base;
    using base::operator=;
};

}  // namespace ds_mysql

/**
 * COLUMN_FIELD(tag, type[, attrs...]) — one-liner macro to declare a tagged
 * column field, optionally with typed DDL attribute tags.
 *
 * Generates a nested tag struct, a type alias, and a member variable:
 *
 *   COLUMN_FIELD(id, uint32_t)
 *   // equivalent to:
 *   struct id_tag {};
 *   using id = ::ds_mysql::tagged_column_field<id_tag, uint32_t>;
 *   id id_;

 *   COLUMN_FIELD(created_at,
 *                ::ds_mysql::timestamp_type,
 *                ::ds_mysql::column_attr::default_current_timestamp,
 *                ::ds_mysql::column_attr::on_update_current_timestamp)
 *
 * The tag struct is nested inside the enclosing class, guaranteeing
 * per-table type uniqueness and satisfying the compile-time membership
 * check in from<Table>().
 */
#define COLUMN_FIELD(tag, type, ...)                                                         \
    struct tag##_tag {};                                                                     \
    using tag = ::ds_mysql::tagged_column_field<tag##_tag, type __VA_OPT__(, ) __VA_ARGS__>; \
    tag tag##_;
