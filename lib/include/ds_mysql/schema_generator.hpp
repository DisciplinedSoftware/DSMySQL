#pragma once

#include <boost/pfr.hpp>
#include <chrono>
#include <concepts>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

#include "ds_mysql/column_field.hpp"
#include "ds_mysql/name_reflection.hpp"
#include "ds_mysql/sql_identifiers.hpp"
#include "ds_mysql/sql_numeric.hpp"
#include "ds_mysql/sql_temporal.hpp"
#include "ds_mysql/sql_text.hpp"
#include "ds_mysql/sql_varchar.hpp"

namespace ds_mysql {

// ===================================================================
// Type to SQL Type Mapping
// ===================================================================

template <typename T>
concept optional_type = requires {
    typename T::value_type;
    requires std::same_as<T, std::optional<typename T::value_type>>;
};

template <typename T>
struct unwrap_optional {
    using type = T;
};

template <optional_type T>
struct unwrap_optional<T> {
    using type = typename T::value_type;
};

template <typename T>
using unwrap_optional_t = typename unwrap_optional<T>::type;

template <typename T>
constexpr bool is_optional_v = optional_type<T>;

// True if the field (possibly a column_field wrapper) is SQL-nullable.
template <typename T>
constexpr bool is_field_nullable_v = is_optional_v<T> || (ColumnFieldType<T> && is_optional_v<typename T::value_type>);

template <typename T>
struct sql_type_name {
    [[nodiscard]] static std::string value() {
        // Unwrap column_field wrapper first, then proceed with the stored value type.
        if constexpr (ColumnFieldType<T>) {
            return sql_type_name<typename T::value_type>::value();
        } else {
            using base_type = unwrap_optional_t<T>;

            if constexpr (std::same_as<base_type, uint32_t>) {
                return "INT UNSIGNED";
            } else if constexpr (std::same_as<base_type, int32_t>) {
                return "INT";
            } else if constexpr (std::same_as<base_type, uint64_t>) {
                return "BIGINT UNSIGNED";
            } else if constexpr (std::same_as<base_type, int64_t>) {
                return "BIGINT";
            } else if constexpr (is_formatted_numeric_type_v<base_type>) {
                return base_type::sql_type();
            } else if constexpr (std::same_as<base_type, float>) {
                return "FLOAT";
            } else if constexpr (std::same_as<base_type, double>) {
                return "DOUBLE";
            } else if constexpr (std::same_as<base_type, bool>) {
                return "BOOLEAN";
            } else if constexpr (is_varchar_type_v<base_type> || is_text_type_v<base_type>) {
                return base_type::sql_type();
            } else if constexpr (std::same_as<base_type, std::chrono::system_clock::time_point>) {
                return "DATETIME";
            } else if constexpr (is_datetime_type_v<base_type>) {
                if constexpr (base_type::column_fsp == 0) {
                    return "DATETIME";
                } else {
                    return "DATETIME(" + std::to_string(base_type::column_fsp) + ")";
                }
            } else if constexpr (is_timestamp_type_v<base_type>) {
                if constexpr (base_type::column_fsp == 0) {
                    return "TIMESTAMP";
                } else {
                    return "TIMESTAMP(" + std::to_string(base_type::column_fsp) + ")";
                }
            } else if constexpr (is_time_type_v<base_type>) {
                if constexpr (base_type::column_fsp == 0) {
                    return "TIME";
                } else {
                    return "TIME(" + std::to_string(base_type::column_fsp) + ")";
                }
            } else {
                static_assert(false,
                              "Unsupported type for SQL mapping. Supported: uint32_t, int32_t, uint64_t, "
                              "int64_t, float, double, int_type<W>, int_unsigned_type<W>, "
                              "bigint_type<W>, bigint_unsigned_type<W>, "
                              "float_type<P,S>, double_type<P,S>, decimal_type<P,S>, "
                              "bool, varchar_type<N>, text_type (TEXT), "
                              "mediumtext_type (MEDIUMTEXT, MySQL), longtext_type (LONGTEXT, MySQL), "
                              "std::chrono::system_clock::time_point, datetime_type<Fsp>, timestamp_type<Fsp>, "
                              "time_type<Fsp>, and their std::optional variants");
            }
        }  // end else (not ColumnFieldType)
    }
};

/**
 * Maps C++ type to SQL type string
 */
template <typename T>
[[nodiscard]] std::string sql_type_for() {
    return sql_type_name<T>::value();
}

namespace sql_type_format {

[[nodiscard]] inline std::string float_type() {
    return "FLOAT";
}

[[nodiscard]] inline std::string float_type(uint32_t precision) {
    return "FLOAT(" + std::to_string(precision) + ")";
}

[[nodiscard]] inline std::string float_type(uint32_t precision, uint32_t scale) {
    return "FLOAT(" + std::to_string(precision) + "," + std::to_string(scale) + ")";
}

[[nodiscard]] inline std::string double_type() {
    return "DOUBLE";
}

[[nodiscard]] inline std::string double_type(uint32_t precision) {
    return "DOUBLE(" + std::to_string(precision) + ")";
}

[[nodiscard]] inline std::string double_type(uint32_t precision, uint32_t scale) {
    return "DOUBLE(" + std::to_string(precision) + "," + std::to_string(scale) + ")";
}

[[nodiscard]] inline std::string decimal_type() {
    return "DECIMAL";
}

[[nodiscard]] inline std::string decimal_type(uint32_t precision) {
    return "DECIMAL(" + std::to_string(precision) + ")";
}

[[nodiscard]] inline std::string decimal_type(uint32_t precision, uint32_t scale) {
    return "DECIMAL(" + std::to_string(precision) + "," + std::to_string(scale) + ")";
}

[[nodiscard]] inline std::string int_type() {
    return "INT";
}

[[nodiscard]] inline std::string int_type(uint32_t display_width) {
    return "INT(" + std::to_string(display_width) + ")";
}

[[nodiscard]] inline std::string int_unsigned_type() {
    return "INT UNSIGNED";
}

[[nodiscard]] inline std::string int_unsigned_type(uint32_t display_width) {
    return "INT(" + std::to_string(display_width) + ") UNSIGNED";
}

[[nodiscard]] inline std::string bigint_type() {
    return "BIGINT";
}

[[nodiscard]] inline std::string bigint_type(uint32_t display_width) {
    return "BIGINT(" + std::to_string(display_width) + ")";
}

[[nodiscard]] inline std::string bigint_unsigned_type() {
    return "BIGINT UNSIGNED";
}

[[nodiscard]] inline std::string bigint_unsigned_type(uint32_t display_width) {
    return "BIGINT(" + std::to_string(display_width) + ") UNSIGNED";
}

[[nodiscard]] inline std::string datetime_type() {
    return "DATETIME";
}

[[nodiscard]] inline std::string datetime_type(uint32_t fractional_second_precision) {
    return "DATETIME(" + std::to_string(fractional_second_precision) + ")";
}

[[nodiscard]] inline std::string timestamp_type() {
    return "TIMESTAMP";
}

[[nodiscard]] inline std::string timestamp_type(uint32_t fractional_second_precision) {
    return "TIMESTAMP(" + std::to_string(fractional_second_precision) + ")";
}

[[nodiscard]] inline std::string time_type() {
    return "TIME";
}

[[nodiscard]] inline std::string time_type(uint32_t fractional_second_precision) {
    return "TIME(" + std::to_string(fractional_second_precision) + ")";
}

}  // namespace sql_type_format

// ===================================================================
// Table Schema - Table Name via Reflection
// ===================================================================

/**
 * table_name_for - Automatically derive table name from struct type name via reflection
 *
 * Extracts the struct name using compiler intrinsics (__PRETTY_FUNCTION__ / __FUNCSIG__).
 * Returns the unmodified type name (e.g., Symbol → "Symbol").
 *
 * For custom table names or lowercase conversions, specialize this template:
 *
 * template <>
 * struct table_name_for<MyStruct> {
 *     static constexpr table_name value() noexcept { return table_name{"custom_table"}; }
 * };
 */
template <typename T>
struct table_name_for {
    static constexpr table_name value() noexcept {
        return table_name{detail::extract_type_name<T>()};
    }
};

/**
 * database_name_for - Automatically derive database name from struct type name via reflection
 *
 * Extracts the struct name using compiler intrinsics (__PRETTY_FUNCTION__ / __FUNCSIG__).
 * Returns the unmodified type name (e.g., my_db → "my_db").
 *
 * For custom database names, specialize this template:
 *
 * template <>
 * struct database_name_for<MyDb> {
 *     static constexpr std::string_view value() noexcept { return "custom_db"; }
 * };
 */
template <typename T>
struct database_name_for {
    static constexpr std::string_view value() noexcept {
        return detail::extract_type_name<T>();
    }
};

// Database concept — satisfied by any type that inherits from database_schema.
// Use it to mark a struct as a database container and enable type-safe
// create_database<T>() and use<T>() calls.
template <typename T>
concept Database = std::derived_from<T, database_schema>;

// ===================================================================
// Field Schema - Name and Optional SQL Type Override
// ===================================================================

/**
 * field_schema - Template to provide field metadata for struct types
 *
 * The primary template works generically for any struct whose fields are
 * derived from column_field<T>: the column name is extracted from the nested
 * type's name via compile-time reflection (e.g. symbol::id → "id").
 *
 * To override a field name or SQL type for a specific struct, specialize:
 *
 * template <std::size_t Index>
 * struct field_schema<MyStruct, Index> {
 *     static constexpr std::string_view name() { ... }
 *     // Optional: override SQL type (otherwise inferred from C++ type)
 *     static constexpr std::string_view sql_type() { return "VARCHAR(32)"; }
 * };
 */
template <typename T, std::size_t Index>
struct field_schema {
    static constexpr std::string_view name() {
        using field_t = boost::pfr::tuple_element_t<Index, T>;
        static_assert(ColumnFieldType<field_t>,
                      "field_schema: field must be a column_field<\"name\", T>. "
                      "Use: using foo = column_field<\"foo\", T>;");
        return field_t::column_name();
    }

    // Optional: return empty string to use default type mapping
    static constexpr std::string_view sql_type() {
        return "";
    }
};

// Backward compatibility alias
template <typename T, std::size_t Index>
using field_name_extractor = field_schema<T, Index>;

template <typename T, std::size_t Index>
[[nodiscard]] std::string field_sql_type_override() {
    if constexpr (requires {
                      { field_schema<T, Index>::sql_type() } -> std::convertible_to<std::string_view>;
                  }) {
        return std::string(field_schema<T, Index>::sql_type());
    } else if constexpr (requires {
                             { field_schema<T, Index>::sql_type() } -> std::convertible_to<std::string>;
                         }) {
        return std::string(field_schema<T, Index>::sql_type());
    } else {
        return {};
    }
}

// ===================================================================
// has_foreign_key_v — true when the field at Index in table T carries
// an fk_attr::references<> attribute.
//
// FK constraints are declared directly on the column field using typed
// fk_attr attributes instead of a separate schema specialization:
//
//   struct child_table {
//       COLUMN_FIELD(id, uint32_t)
//       COLUMN_FIELD(parent_id, uint32_t,
//                    fk_attr::references<parent_table, parent_table::id>,
//                    fk_attr::on_delete_cascade,
//                    fk_attr::on_update_cascade)
//   };
//
// The SQL referenced table name is derived from table_name_for<RefTable>
// and the column name from RefColumn::column_name() — both resolved at
// compile time, so typos become build errors.
// ===================================================================

template <typename T, std::size_t Index>
constexpr bool has_foreign_key_v = [] {
    using field_type = boost::pfr::tuple_element_t<Index, T>;
    if constexpr (requires { field_type::ddl_has_fk; })
        return field_type::ddl_has_fk;
    else
        return false;
}();

// ===================================================================
// ValidTable — concept for a well-formed table struct
//
// Satisfied when every tagged_column_field member of T has its tag struct
// defined as a nested class of T (not at global/namespace scope).
//
// Empty tables are allowed only if they explicitly declare:
//
//   using ds_mysql_table_tag = ds_mysql::table_schema;
//
// Use COLUMN_FIELD inside your struct to satisfy this automatically:
//
//   struct my_table {          // ✓ — COLUMN_FIELD generates nested tags
//       COLUMN_FIELD(id,    uint32_t)
//       COLUMN_FIELD(price, double)
//   };
// ===================================================================

template <typename Tag, typename Table>
consteval bool tag_is_nested_in_table() noexcept {
    constexpr std::string_view full = detail::raw_type_name<Tag>();
    constexpr auto last_scope = full.rfind("::");
    if constexpr (last_scope == std::string_view::npos)
        return false;
    return detail::strip_type_qualifiers(full.substr(0, last_scope)) == detail::extract_type_name<Table>();
}

template <typename Proj, typename Table>
consteval bool tag_nested_or_untagged() noexcept {
    if constexpr (requires { typename Proj::tag_type; })
        return tag_is_nested_in_table<typename Proj::tag_type, Table>();
    else
        return ColumnFieldType<Proj>;  // NTTP column_field<> passes; plain int/double/etc. does not
}

template <typename T>
concept EmptyTableOptIn =
    requires { typename T::ds_mysql_table_tag; } && std::same_as<typename T::ds_mysql_table_tag, table_schema>;

template <typename T>
consteval bool all_table_tags_nested() noexcept {
    if constexpr (!std::is_aggregate_v<T>)
        return false;
    else if constexpr (boost::pfr::tuple_size_v<T> == 0)
        return EmptyTableOptIn<T>;
    else
        return []<std::size_t... Is>(std::index_sequence<Is...>) {
            return (tag_nested_or_untagged<boost::pfr::tuple_element_t<Is, T>, T>() && ...);
        }(std::make_index_sequence<boost::pfr::tuple_size_v<T>>{});
}

template <typename T>
concept ValidTable = !ColumnFieldType<T> && all_table_tags_nested<T>();

// ===================================================================
// database_tables<DB> — register the table types for a database.
//
// Enables validate_database<DB>() on mysql_connection.
//
// Option 1 — nested type alias in the database struct (auto-detected):
//
//   struct my_db : database_schema {
//       struct symbol { ... };
//       using tables = std::tuple<symbol>;
//   };
//
// Option 2 — explicit specialization outside the struct:
//
//   template <>
//   struct database_tables<my_db> {
//       using type = std::tuple<my_db::symbol, my_db::market_data>;
//   };
//
// The primary template defaults to std::tuple<> (no tables registered).
// ===================================================================

namespace detail {

template <typename DB>
concept HasTablesAlias = requires { typename DB::tables; };

template <typename Tuple>
struct all_valid_tables_in_tuple;

template <typename... Ts>
struct all_valid_tables_in_tuple<std::tuple<Ts...>> : std::bool_constant<(ValidTable<Ts> && ...)> {};

}  // namespace detail

template <Database DB>
struct database_tables {
    using type = std::tuple<>;
};

template <Database DB>
    requires detail::HasTablesAlias<DB>
struct database_tables<DB> {
    static_assert(detail::all_valid_tables_in_tuple<typename DB::tables>::value,
                  "database_schema::tables must be a std::tuple of ValidTable types. "
                  "Each table must either be a non-empty valid table struct or "
                  "an empty aggregate with 'using ds_mysql_table_tag = ds_mysql::table_schema;'.");
    using type = typename DB::tables;
};

// ===================================================================
// Schema Generation
// ===================================================================

template <typename T, std::size_t I>
[[nodiscard]] std::string column_definition_for() {
    using field_type = boost::pfr::tuple_element_t<I, T>;
    std::string_view field_name = field_schema<T, I>::name();
    std::string sql_type_override = field_sql_type_override<T, I>();
    std::string col = "    ";
    col += field_name;
    col += ' ';
    col += sql_type_override.empty() ? sql_type_for<field_type>() : sql_type_override;
    if constexpr (!is_optional_v<field_type>) {
        col += " NOT NULL";
    }
    return col;
}

template <typename T, std::size_t... Is>
[[nodiscard]] std::string generate_create_table_impl(std::string_view table_name, std::index_sequence<Is...>) {
    if constexpr (sizeof...(Is) == 0) {
        return std::string{"CREATE TABLE "}.append(table_name).append(" (\n)");
    }
    std::string col_defs[] = {column_definition_for<T, Is>()...};
    std::string sql = "CREATE TABLE ";
    sql += table_name;
    sql += " (\n";
    for (std::size_t i = 0; i < sizeof...(Is); ++i) {
        if (i > 0)
            sql += ",\n";
        sql += col_defs[i];
    }
    sql += "\n)";
    return sql;
}

/**
 * Generate CREATE TABLE statement from struct type using Boost.PFR reflection
 *
 * @tparam T Aggregate struct type (must be reflectable by PFR)
 * @param table_name Name of the table to create
 * @return SQL CREATE TABLE statement
 *
 * @example
 *   auto sql = generate_create_table<Symbol>("symbol");
 */
template <typename T>
[[nodiscard]] std::string generate_create_table(std::string_view table_name) {
    constexpr std::size_t field_count = boost::pfr::tuple_size_v<T>;
    return generate_create_table_impl<T>(table_name, std::make_index_sequence<field_count>{});
}

}  // namespace ds_mysql
