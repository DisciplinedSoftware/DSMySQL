#pragma once

#include <concepts>
#include <type_traits>

#include "ds_mysql/fixed_string.hpp"
#include "ds_mysql/name_reflection.hpp"

// Include base type adapters (in order: core first, then type-specific adapters)
#include "ds_mysql/column_field_base_core.hpp"
#include "ds_mysql/column_field_base_numeric.hpp"
#include "ds_mysql/column_field_base_temporal.hpp"
#include "ds_mysql/column_field_base_text.hpp"
#include "ds_mysql/column_field_base_varchar.hpp"

namespace ds_mysql {

// ===================================================================
// column_field<Name, T> — the only user-facing form
// ===================================================================

/**
 * column_field<Name, T, Attrs...> — named column descriptor.
 *
 * Name is the SQL column name embedded directly as a string literal.
 * T is the stored value type. Attrs are optional typed DDL modifiers
 * (column_attr::*). All constructors and operators are inherited from the
 * internal base type (defined in the adapter headers).
 *
 *   using id     = column_field<"id",     uint32_t>;
 *   using ticker = column_field<"ticker", varchar_type<32>>;
 *   using sector = column_field<"sector", std::optional<varchar_type<64>>>;
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

    static constexpr bool ddl_primary_key = (std::same_as<Attrs, column_attr::primary_key> || ...);
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

// ===================================================================
// tagged_column_field — tag-struct based column descriptor
// ===================================================================

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
 *
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
