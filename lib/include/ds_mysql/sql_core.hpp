#pragma once

#include <algorithm>
#include <array>
#include <boost/pfr.hpp>
#include <cassert>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <format>
#include <functional>
#include <iomanip>
#include <map>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <vector>

#include "ds_mysql/column_field.hpp"
#include "ds_mysql/database_name.hpp"
#include "ds_mysql/fixed_string.hpp"
#include "ds_mysql/name_reflection.hpp"
#include "ds_mysql/schema_generator.hpp"
#include "ds_mysql/sql_temporal.hpp"
#include "ds_mysql/sql_varchar.hpp"

namespace ds_mysql {

enum class sql_date_part {
    year,
    quarter,
    month,
    week,
    day,
    hour,
    minute,
    second,
};

enum class sql_cast_type {
    signed_integer,
    unsigned_integer,
    decimal,
    double_precision,
    float_precision,
    char_type,
    binary,
    date,
    datetime,
    timestamp,
    time,
    json,
};

enum class sort_order { asc, desc };

// desc_order<ColSpec> — wraps a column spec to request DESC ordering.
// Created via desc():
//   order_by(desc(product::id{}))      — ORDER BY id DESC
//   order_by(desc(nulls_last<Col>{}))  — ORDER BY (col IS NULL) ASC, col DESC
template <typename ColSpec>
struct desc_order {
    using col_spec = ColSpec;
};

template <typename ColSpec>
[[nodiscard]] constexpr desc_order<std::decay_t<ColSpec>> desc(ColSpec&&) noexcept {
    return {};
}

template <typename T>
concept DescOrder = requires { typename T::col_spec; } && std::is_same_v<T, desc_order<typename T::col_spec>>;

enum class match_search_modifier {
    natural_language,                       // MATCH(...) AGAINST ('expr') — default, no modifier emitted
    boolean_mode,                           // MATCH(...) AGAINST ('expr' IN BOOLEAN MODE)
    query_expansion,                        // MATCH(...) AGAINST ('expr' WITH QUERY EXPANSION)
    natural_language_with_query_expansion,  // MATCH(...) AGAINST ('expr' IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION)
};

template <typename... Cols>
struct grouping_set {};

template <typename T>
struct is_grouping_set : std::false_type {};

template <typename... Cols>
struct is_grouping_set<grouping_set<Cols...>> : std::true_type {};

template <typename T>
inline constexpr bool is_grouping_set_v = is_grouping_set<T>::value;

// ===================================================================
// GROUP BY modifier wrappers
//
// Pass a single wrapper as the template argument to group_by<> to
// select the grouping modifier — exactly like nulls_last/nulls_first
// work with order_by<>.
//
//   group_by<rollup<Col1, Col2>>()             — GROUP BY col1, col2 WITH ROLLUP
//   group_by<cube<Col1, Col2>>()               — GROUP BY CUBE(col1, col2)
//   group_by<grouping_sets<Set1, Set2, ...>>() — GROUP BY GROUPING SETS(...)
// ===================================================================

template <typename... Cols>
    requires((ColumnFieldType<Cols> && ...) && sizeof...(Cols) > 0)
struct rollup {};

template <typename... Cols>
    requires((ColumnFieldType<Cols> && ...) && sizeof...(Cols) > 0)
struct cube {};

template <typename... Sets>
    requires((is_grouping_set_v<Sets> && ...) && sizeof...(Sets) > 0)
struct grouping_sets {};

// joined<Table> — suppresses column-membership check in from<>
//
//   select<...>().from<joined<Table>>()  is equivalent to the former
//   select<...>().from_joined<Table>()   (which is now removed).
template <typename Table>
struct joined {
    using table_type = Table;
};

template <typename T>
struct is_joined : std::false_type {};
template <typename T>
struct is_joined<joined<T>> : std::true_type {};
template <typename T>
inline constexpr bool is_joined_v = is_joined<T>::value;

// subquery_source — phantom Table type for derived-table builders.
// Used as the Table template argument of select_query_builder when the
// source is a subquery rather than a real table.  The name is never
// emitted; build_sql() uses the pre-built from_subquery_ string instead.
struct subquery_source {};

// ===================================================================
// Shared SQL detail helpers
// ===================================================================

namespace sql_detail {

[[nodiscard]] constexpr uint32_t normalize_fractional_second_precision(uint32_t precision) {
    return precision <= 6U ? precision : 6U;
}

[[nodiscard]] inline std::string escape_sql_string(std::string_view s) {
    std::string result;
    result.reserve(s.size());
    for (char const c : s) {
        if (c == '\'') {  // Double the quote character for SQL escaping
            result += '\'';
        }
        result += c;
    }
    return result;
}

[[nodiscard]] inline std::string format_datetime(std::chrono::system_clock::time_point tp,
                                                 uint32_t fractional_second_precision = 0) {
    auto const precision = normalize_fractional_second_precision(fractional_second_precision);
    auto const micros = std::chrono::floor<std::chrono::microseconds>(tp);
    auto const secs = std::chrono::floor<std::chrono::seconds>(micros);
    if (precision == 0U) {
        return std::format("'{:%Y-%m-%d %H:%M:%S}'", secs);
    }

    constexpr std::array<uint32_t, 7> divisors{1000000U, 100000U, 10000U, 1000U, 100U, 10U, 1U};
    auto const fractional_us =
        static_cast<uint32_t>(std::chrono::duration_cast<std::chrono::microseconds>(micros - secs).count());
    auto const scaled_fraction = fractional_us / divisors[precision];
    return std::format("'{:%Y-%m-%d %H:%M:%S}.{:0{}d}'", secs, scaled_fraction, precision);
}

[[nodiscard]] inline std::string format_time(std::chrono::microseconds dur, uint32_t fractional_second_precision = 0) {
    auto const precision = normalize_fractional_second_precision(fractional_second_precision);
    bool const negative = dur.count() < 0;
    auto const abs_dur = negative ? -dur : dur;
    auto const total_secs = std::chrono::duration_cast<std::chrono::seconds>(abs_dur).count();
    auto const hours = total_secs / 3600;
    auto const mins = (total_secs % 3600) / 60;
    auto const secs = total_secs % 60;
    std::string result = std::format("'{}{:02d}:{:02d}:{:02d}", negative ? "-" : "", hours, mins, secs);
    if (precision > 0U) {
        constexpr std::array<uint32_t, 7> divisors{1000000U, 100000U, 10000U, 1000U, 100U, 10U, 1U};
        auto const fractional_us =
            static_cast<uint32_t>((abs_dur - std::chrono::duration_cast<std::chrono::seconds>(abs_dur)).count());
        auto const scaled_fraction = fractional_us / divisors[precision];
        result += std::format(".{:0{}d}", scaled_fraction, precision);
    }
    result += '\'';
    return result;
}

/**
 * to_sql_value — convert a typed C++ value to its SQL literal representation.
 *
 * Handles: column_field<T> wrappers, std::optional<T>, datetime_type, bool,
 * integral types, floating-point types, varchar_type<N>, text_type, std::string, and
 * std::chrono::system_clock::time_point.
 */
template <typename T>
[[nodiscard]] std::string to_sql_value(T const& v) {
    if constexpr (std::same_as<T, sql_default_t>) {
        return "DEFAULT";
    } else if constexpr (ColumnFieldType<T>) {
        if constexpr (requires(T const& x) { x.is_sql_default_; }) {
            if (v.is_sql_default_) {
                return "DEFAULT";
            }
        }
        return to_sql_value(v.value);
    } else if constexpr (is_optional_v<T>) {
        if (!v.has_value()) {
            return "NULL";
        }
        return to_sql_value(v.value());
    } else if constexpr (is_datetime_type_v<T>) {
        if (v.is_now()) {
            auto const precision = normalize_fractional_second_precision(v.fractional_second_precision());
            if (precision == 0U) {
                return "NOW()";
            }
            return "NOW(" + std::to_string(precision) + ")";
        }
        return format_datetime(v.time_point(), v.fractional_second_precision());
    } else if constexpr (is_timestamp_type_v<T>) {
        if (v.is_now()) {
            auto const precision = normalize_fractional_second_precision(v.fractional_second_precision());
            if (precision == 0U) {
                return "CURRENT_TIMESTAMP";
            }
            return "CURRENT_TIMESTAMP(" + std::to_string(precision) + ")";
        }
        return format_datetime(v.time_point(), v.fractional_second_precision());
    } else if constexpr (std::same_as<T, std::chrono::system_clock::time_point>) {
        return format_datetime(v);
    } else if constexpr (is_time_type_v<T>) {
        return format_time(v.duration(), v.fractional_second_precision());
    } else if constexpr (is_date_type_v<T>) {
        return std::format("'{:%Y-%m-%d}'", v.days());
    } else if constexpr (std::same_as<T, bool>) {
        return v ? "1" : "0";
    } else if constexpr (is_formatted_numeric_type_v<T>) {
        return to_sql_value(static_cast<typename T::underlying_type>(v));
    } else if constexpr (std::integral<T>) {
        return std::to_string(v);
    } else if constexpr (std::floating_point<T>) {
        return std::to_string(v);
    } else if constexpr (is_varchar_type_v<T> || is_text_type_v<T>) {
        return "'" + escape_sql_string(v.view()) + "'";
    } else if constexpr (std::same_as<T, std::string>) {
        return "'" + escape_sql_string(v) + "'";
    } else {
        static_assert(false,
                      "to_sql_value: unsupported type. "
                      "Supported: column_field<T>, optional<T>, datetime_type, timestamp_type, date_type, bool, "
                      "integral types, floating-point types, float_type<P,S>, double_type<P,S>, "
                      "decimal_type<P,S>, varchar_type<N>, text_type, std::string, "
                      "std::chrono::system_clock::time_point, time_type");
    }
}

}  // namespace sql_detail

// ===================================================================
// SqlValue — concept matching every type accepted by sql_detail::to_sql_value.
// Constrains template parameters that must produce a valid SQL literal.
// ===================================================================
template <typename T>
concept SqlValue =
    std::same_as<T, sql_default_t> || ColumnFieldType<T> || is_optional_v<T> || is_datetime_type_v<T> ||
    is_timestamp_type_v<T> || is_date_type_v<T> || std::same_as<T, std::chrono::system_clock::time_point> ||
    is_time_type_v<T> || std::same_as<T, bool> || std::integral<T> || std::floating_point<T> ||
    is_formatted_numeric_type_v<T> || is_varchar_type_v<T> || is_text_type_v<T> || std::same_as<T, std::string>;

// ===================================================================
// check_id<"name">     — compile-time CHECK constraint name type.
// index_id<"name">     — compile-time index name type.
// trigger_id<"name">   — compile-time trigger name type.
// procedure_id<"name"> — compile-time stored procedure name type.
// savepoint_id<"name"> — compile-time savepoint name type.
//
// Each type provides a static name() method returning the compile-time name.
// Functions accept specific id types as parameters to enforce type safety.
//
// Examples:
//   table_constraint::check(check_id<"chk_positive_price">{}, greater_than<my_table::price>(0.0))
//   // → CONSTRAINT chk_positive_price CHECK (price > 0)
//
//   create_index_on<my_table, my_table::col>(index_id<"idx_foo">{})
//   // → CREATE INDEX idx_foo ON my_table (col);
//
//   create_trigger<my_table>(trigger_id<"trg_before_insert">{}, TriggerTiming::Before, ...)
//   // → CREATE TRIGGER trg_before_insert BEFORE INSERT ON my_table FOR EACH ROW ...
//
//   create_procedure(procedure_id<"usp_hello">{}, "", "SELECT 1;")
//   // → CREATE PROCEDURE usp_hello() BEGIN SELECT 1; END
//
//   savepoint(savepoint_id<"sp1">{})
//   // → SAVEPOINT sp1
// ===================================================================

template <fixed_string Name>
struct check_id {
    [[nodiscard]] static constexpr std::string_view name() noexcept {
        return Name;
    }
};

template <fixed_string Name>
struct index_id {
    [[nodiscard]] static constexpr std::string_view name() noexcept {
        return Name;
    }
};

template <fixed_string Name>
struct trigger_id {
    [[nodiscard]] static constexpr std::string_view name() noexcept {
        return Name;
    }
};

template <fixed_string Name>
struct procedure_id {
    [[nodiscard]] static constexpr std::string_view name() noexcept {
        return Name;
    }
};

template <fixed_string Name>
struct function_id {
    [[nodiscard]] static constexpr std::string_view name() noexcept {
        return Name;
    }
};

template <fixed_string Name>
struct savepoint_id {
    [[nodiscard]] static constexpr std::string_view name() noexcept {
        return Name;
    }
};

}  // namespace ds_mysql

// Predicates, operators, and col_expr — split out for readability.
// Included after the namespace close because sql_predicates.hpp has its own
// namespace ds_mysql {} block.
#include "ds_mysql/sql_predicates.hpp"
