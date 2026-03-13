#pragma once

#include <boost/pfr.hpp>
#include <chrono>
#include <concepts>
#include <cstdint>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>

#include "ds_mysql/column_field.hpp"
#include "ds_mysql/sql_identifiers.hpp"
#include "ds_mysql/sql_temporal.hpp"
#include "ds_mysql/text_field.hpp"
#include "ds_mysql/varchar_field.hpp"

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
    static std::string value() {
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
            } else if constexpr (std::same_as<base_type, float>) {
                return "FLOAT";
            } else if constexpr (std::same_as<base_type, double>) {
                return "DOUBLE";
            } else if constexpr (std::same_as<base_type, bool>) {
                return "BOOLEAN";
            } else if constexpr (is_varchar_field_v<base_type>) {
                return "VARCHAR(" + std::to_string(is_varchar_field<base_type>::capacity) + ")";
            } else if constexpr (is_text_field_v<base_type>) {
                constexpr auto kind = is_text_field<base_type>::kind;
                if constexpr (kind == text_kind::mediumtext) {
                    return "MEDIUMTEXT";
                } else if constexpr (kind == text_kind::longtext) {
                    return "LONGTEXT";
                } else {
                    return "TEXT";
                }
            } else if constexpr (std::same_as<base_type, std::chrono::system_clock::time_point>) {
                return "DATETIME";
            } else if constexpr (std::same_as<base_type, sql_datetime>) {
                return "DATETIME";
            } else if constexpr (std::same_as<base_type, sql_timestamp>) {
                return "TIMESTAMP";
            } else {
                static_assert(false,
                              "Unsupported type for SQL mapping. Supported: uint32_t, int32_t, uint64_t, "
                              "int64_t, float, double, bool, varchar_field<N>, text_field (TEXT), "
                              "mediumtext_field (MEDIUMTEXT, MySQL), longtext_field (LONGTEXT, MySQL), "
                              "std::chrono::system_clock::time_point, sql_datetime, sql_timestamp, and their "
                              "std::optional variants");
            }
        }  // end else (not ColumnFieldType)
    }
};

/**
 * Maps C++ type to SQL type string
 */
template <typename T>
std::string sql_type_for() {
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

}  // namespace sql_type_format

// ===================================================================
// Table Schema - Table Name via Reflection
// ===================================================================

namespace detail {

/**
 * Extract struct type name from compiler intrinsics (__PRETTY_FUNCTION__ or __FUNCSIG__)
 */
template <typename T>
consteval std::string_view extract_type_name() {
#if defined(__clang__) || defined(__GNUC__)
    constexpr std::string_view signature = __PRETTY_FUNCTION__;
    constexpr std::string_view marker = "T = ";
#elif defined(_MSC_VER)
    constexpr std::string_view signature = __FUNCSIG__;
    constexpr std::string_view marker = "extract_type_name<";
#else
    return "unknown";
#endif

    constexpr auto npos = std::string_view::npos;
    constexpr auto marker_pos = signature.find(marker);
    if constexpr (marker_pos == npos) {
        return "unknown";
    }

    constexpr auto start = marker_pos + marker.size();

#if defined(__clang__) || defined(__GNUC__)
    // GCC/Clang format: [with T = TYPE] or [with T = TYPE; ALIAS = EXPANSION, ...]
    constexpr auto end_semicolon = signature.find(';', start);
    constexpr auto end_bracket   = signature.find(']', start);
    constexpr auto end = (end_semicolon != npos && end_bracket != npos)
                             ? (end_semicolon < end_bracket ? end_semicolon : end_bracket)
                         : (end_semicolon != npos ? end_semicolon
                          : end_bracket != npos   ? end_bracket : npos);
#else
    // MSVC format: "...extract_type_name<TYPE>(void)..."
    // The type ends at '>' (template arg close) or ',' (comma in multi-param templates).
    constexpr auto end_bracket = signature.find('>', start);
    constexpr auto end_comma   = signature.find(',', start);
    constexpr auto end = (end_bracket != npos && end_comma != npos)
                             ? (end_bracket < end_comma ? end_bracket : end_comma)
                         : (end_bracket != npos ? end_bracket
                          : end_comma != npos   ? end_comma : npos);
#endif

    if constexpr (end == npos || end <= start) {
        return "unknown";
    }

    auto name = signature.substr(start, end - start);

    // Remove namespace qualifiers (keep only the final name)
    auto const scope_pos = name.rfind("::");
    if (scope_pos != std::string_view::npos && scope_pos + 2 < name.size()) {
        name = name.substr(scope_pos + 2);
    }

    return name;
}

}  // namespace detail

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
                      "field_schema: field must be a column_field<Tag, T>. "
                      "Use the tagged form: using foo = column_field<struct foo_tag, T>;");
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
std::string field_sql_type_override() {
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
// Foreign Key Schema
//
// Declare a FOREIGN KEY constraint for a specific field of a table by
// specialising foreign_key_schema<T, Index>:
//
//   template <>
//   struct foreign_key_schema<market_data, 1> {
//       // referenced_table() — name of the table being referenced
//       static constexpr std::string_view referenced_table() { return "symbol"; }
//       // referenced_column() — column in the referenced table
//       static constexpr std::string_view referenced_column() { return "id"; }
//       // on_delete() / on_update() — optional referential actions
//       static constexpr std::string_view on_delete() { return "CASCADE"; }
//       static constexpr std::string_view on_update() { return "CASCADE"; }
//   };
//
// The primary template returns empty strings = no FK constraint.
// ===================================================================

template <typename T, std::size_t Index>
struct foreign_key_schema {
    static constexpr std::string_view referenced_table() {
        return "";
    }
    static constexpr std::string_view referenced_column() {
        return "";
    }
    static constexpr std::string_view on_delete() {
        return "";
    }
    static constexpr std::string_view on_update() {
        return "";
    }
};

template <typename T, std::size_t Index>
constexpr bool has_foreign_key_v = !foreign_key_schema<T, Index>::referenced_table().empty();

// ===================================================================
// database_tables<DB> — register the table types for a database.
//
// Enables validate_database<DB>() on mysql_database.
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

}  // namespace detail

template <Database DB>
struct database_tables {
    using type = std::tuple<>;
};

template <Database DB>
    requires detail::HasTablesAlias<DB>
struct database_tables<DB> {
    using type = typename DB::tables;
};

// ===================================================================
// Schema Generation
// ===================================================================

template <typename T, std::size_t... Is>
std::string generate_create_table_impl(std::string_view table_name, std::index_sequence<Is...>) {
    std::ostringstream sql;
    sql << "CREATE TABLE " << table_name << " (\n";

    std::size_t field_count = 0;
    (
        [&] {
            using field_type = std::tuple_element_t<Is, decltype(boost::pfr::structure_to_tuple(std::declval<T>()))>;

            if (field_count > 0) {
                sql << ",\n";
            }

            // Get field name
            std::string_view field_name = field_schema<T, Is>::name();

            // Get SQL type (use override if provided, otherwise infer from C++ type)
            std::string sql_type_override = field_sql_type_override<T, Is>();
            std::string sql_type = sql_type_override.empty() ? sql_type_for<field_type>() : sql_type_override;

            sql << "    " << field_name << " " << sql_type;

            // Handle nullable vs NOT NULL
            if constexpr (!is_optional_v<field_type>) {
                sql << " NOT NULL";
            }

            // Special handling for first field as PRIMARY KEY
            if (Is == 0) {
                sql << " PRIMARY KEY AUTO_INCREMENT";
            }

            ++field_count;
        }(),
        ...);

    sql << "\n)";
    return sql.str();
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
std::string generate_create_table(std::string_view table_name) {
    constexpr std::size_t field_count = boost::pfr::tuple_size_v<T>;
    return generate_create_table_impl<T>(table_name, std::make_index_sequence<field_count>{});
}

}  // namespace ds_mysql
