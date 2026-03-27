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

#include "ds_mysql/col.hpp"
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
    requires((ColumnDescriptor<Cols> && ...) && sizeof...(Cols) > 0)
struct rollup {};

template <typename... Cols>
    requires((ColumnDescriptor<Cols> && ...) && sizeof...(Cols) > 0)
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

[[nodiscard]] inline std::string format_datetime(std::chrono::system_clock::time_point tp, uint32_t fractional_second_precision = 0) {
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
    if constexpr (ColumnFieldType<T>) {
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
                      "Supported: column_field<T>, optional<T>, datetime_type, timestamp_type, bool, "
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
    ColumnFieldType<T> || is_optional_v<T> || is_datetime_type_v<T> || is_timestamp_type_v<T> ||
    std::same_as<T, std::chrono::system_clock::time_point> || is_time_type_v<T> || std::same_as<T, bool> ||
    std::integral<T> || std::floating_point<T> || is_formatted_numeric_type_v<T> || is_varchar_type_v<T> ||
    is_text_type_v<T> || std::same_as<T, std::string>;

// ===================================================================
// check_expr — a SQL predicate valid in CHECK constraint context.
//
// Covers column-ref comparisons and their logical compositions (&, |, !).
// Does NOT include subqueries, non-deterministic functions, or aggregate
// expressions — those are sql_predicate-only constructs.
//
// Produced by the typed predicate free functions marked "check-safe" below.
// Implicitly converts to sql_predicate, so check_expr values can be passed
// anywhere sql_predicate is expected (WHERE, HAVING, JOIN ON, etc.).
// ===================================================================

struct check_expr {
    std::string_view col_name;  // compile-time column name; empty for composed conditions
    std::string_view op;        // operator literal (" = ", " IS NULL", etc.)
    std::string value;          // serialized RHS value, or full SQL for composed conditions

    check_expr() = default;
    check_expr(std::string_view col_name_, std::string_view op_, std::string value_)
        : col_name(col_name_), op(op_), value(std::move(value_)) {
    }

    [[nodiscard]] std::string build_sql() const {
        if (col_name.empty())
            return value;
        std::string s;
        s.reserve(col_name.size() + op.size() + value.size());
        s += col_name;
        s += op;
        s += value;
        return s;
    }
};

// ===================================================================
// sql_predicate — a typed SQL predicate fragment.
//
// Used in WHERE, HAVING, JOIN ON, and CASE WHEN contexts.
// Produced by typed predicate free functions below; check_expr
// implicitly converts to sql_predicate so check-safe predicates
// can be used everywhere a sql_predicate is required.
//
//   .where(equal<symbol::ticker>("AAPL"))        ← typed predicate
//   .having(count() > 5)                         ← aggregate predicate
// ===================================================================

struct sql_predicate {
    std::string_view col_name;  // compile-time column name; empty for composed/raw conditions
    std::string_view op;        // operator literal (" = ", " IS NULL", etc.)
    std::string value;          // serialized RHS value, or full SQL for composed/raw conditions

    sql_predicate() = default;
    sql_predicate(std::string_view col_name_, std::string_view op_, std::string value_)
        : col_name(col_name_), op(op_), value(std::move(value_)) {
    }

    // Every check_expr is a valid sql_predicate.
    // NOLINTNEXTLINE(google-explicit-constructor)
    sql_predicate(check_expr e) : col_name(e.col_name), op(e.op), value(std::move(e.value)) {
    }

    [[nodiscard]] std::string build_sql() const {
        if (col_name.empty())
            return value;
        std::string s;
        s.reserve(col_name.size() + op.size() + value.size());
        s += col_name;
        s += op;
        s += value;
        return s;
    }
};

// ===================================================================
// SQL predicates
//
// Check-safe (return check_expr — valid in WHERE, HAVING, and CHECK):
//   equal<Col>(value)                    — col = val
//   not_equal<Col>(value)                — col != val
//   less_than<Col>(value)                — col < val
//   greater_than<Col>(value)             — col > val
//   less_than_or_equal<Col>(value)       — col <= val
//   greater_than_or_equal<Col>(value)    — col >= val
//   is_null<Col>()                       — col IS NULL
//   is_not_null<Col>()                   — col IS NOT NULL
//   like<Col>(pattern)                   — col LIKE 'pattern'
//   not_like<Col>(pattern)               — col NOT LIKE 'pattern'
//   regexp<Col>(pattern)                 — col REGEXP 'pattern'
//   not_regexp<Col>(pattern)             — col NOT REGEXP 'pattern'
//   rlike<Col>(pattern)                  — col RLIKE 'pattern'  (MySQL synonym for REGEXP)
//   not_rlike<Col>(pattern)              — col NOT RLIKE 'pattern'
//   sounds_like<Col>(word)               — col SOUNDS LIKE 'word'
//   between<Col>(low, high)              — col BETWEEN low AND high
//   in<Col>({v1, v2, ...})               — col IN (v1, v2, ...)
//   not_in<Col>({v1, v2, ...})           — col NOT IN (v1, v2, ...)
//
// WHERE/HAVING only (return sql_predicate — not valid in CHECK):
//   match_against<C1,C2,...>(expr)       — MATCH(c1,c2,...) AGAINST ('expr')
//   in_subquery<Col>(query)              — col IN (SELECT ...)
//   not_in_subquery<Col>(query)          — col NOT IN (SELECT ...)
//   exists(query)                        — EXISTS (SELECT ...)
//   not_exists(query)                    — NOT EXISTS (SELECT ...)
//
// Composition (return same type as inputs):
//   and_(a, b)                           — (a AND b)
//   or_(a, b)                            — (a OR b)
//   not_(cond)                           — NOT (cond)
// ===================================================================

template <ColumnFieldType Col>
[[nodiscard]] check_expr equal(Col const& value) {
    return {column_traits<Col>::column_name(), " = ", sql_detail::to_sql_value(value.value)};
}

template <ColumnFieldType Col>
[[nodiscard]] check_expr not_equal(Col const& value) {
    return {column_traits<Col>::column_name(), " != ", sql_detail::to_sql_value(value.value)};
}

template <ColumnFieldType Col>
[[nodiscard]] check_expr less_than(Col const& value) {
    return {column_traits<Col>::column_name(), " < ", sql_detail::to_sql_value(value.value)};
}

template <ColumnFieldType Col>
[[nodiscard]] check_expr greater_than(Col const& value) {
    return {column_traits<Col>::column_name(), " > ", sql_detail::to_sql_value(value.value)};
}

template <ColumnFieldType Col>
[[nodiscard]] check_expr less_than_or_equal(Col const& value) {
    return {column_traits<Col>::column_name(), " <= ", sql_detail::to_sql_value(value.value)};
}

template <ColumnFieldType Col>
[[nodiscard]] check_expr greater_than_or_equal(Col const& value) {
    return {column_traits<Col>::column_name(), " >= ", sql_detail::to_sql_value(value.value)};
}

template <ColumnFieldType Col>
    requires is_field_nullable_v<Col>
[[nodiscard]] check_expr is_null() {
    return {column_traits<Col>::column_name(), " IS NULL", {}};
}

template <ColumnFieldType Col>
    requires is_field_nullable_v<Col>
[[nodiscard]] check_expr is_not_null() {
    return {column_traits<Col>::column_name(), " IS NOT NULL", {}};
}

template <ColumnFieldType Col>
[[nodiscard]] check_expr like(std::string_view pattern) {
    auto escaped = sql_detail::escape_sql_string(pattern);
    std::string rhs;
    rhs.reserve(2 + escaped.size());
    rhs += '\'';
    rhs += std::move(escaped);
    rhs += '\'';
    return {column_traits<Col>::column_name(), " LIKE ", std::move(rhs)};
}

[[nodiscard]] inline check_expr and_(check_expr a, check_expr b) {
    auto as = a.build_sql();
    auto bs = b.build_sql();
    std::string s;
    s.reserve(1 + as.size() + 5 + bs.size() + 1);
    s += '(';
    s += std::move(as);
    s += " AND ";
    s += std::move(bs);
    s += ')';
    return {{}, {}, std::move(s)};
}

[[nodiscard]] inline sql_predicate and_(sql_predicate a, sql_predicate b) {
    auto as = a.build_sql();
    auto bs = b.build_sql();
    std::string s;
    s.reserve(1 + as.size() + 5 + bs.size() + 1);
    s += '(';
    s += std::move(as);
    s += " AND ";
    s += std::move(bs);
    s += ')';
    return {{}, {}, std::move(s)};
}

[[nodiscard]] inline check_expr or_(check_expr a, check_expr b) {
    auto as = a.build_sql();
    auto bs = b.build_sql();
    std::string s;
    s.reserve(1 + as.size() + 4 + bs.size() + 1);
    s += '(';
    s += std::move(as);
    s += " OR ";
    s += std::move(bs);
    s += ')';
    return {{}, {}, std::move(s)};
}

[[nodiscard]] inline sql_predicate or_(sql_predicate a, sql_predicate b) {
    auto as = a.build_sql();
    auto bs = b.build_sql();
    std::string s;
    s.reserve(1 + as.size() + 4 + bs.size() + 1);
    s += '(';
    s += std::move(as);
    s += " OR ";
    s += std::move(bs);
    s += ')';
    return {{}, {}, std::move(s)};
}

[[nodiscard]] inline check_expr not_(check_expr cond) {
    auto cs = cond.build_sql();
    std::string s;
    s.reserve(5 + cs.size() + 1);
    s += "NOT (";
    s += std::move(cs);
    s += ')';
    return {{}, {}, std::move(s)};
}

[[nodiscard]] inline sql_predicate not_(sql_predicate cond) {
    auto cs = cond.build_sql();
    std::string s;
    s.reserve(5 + cs.size() + 1);
    s += "NOT (";
    s += std::move(cs);
    s += ')';
    return {{}, {}, std::move(s)};
}

// ===================================================================
// Operator overloads for natural predicate composition
//
// &, |, and ! can be overloaded safely for DSL types like check_expr and
// sql_predicate because they carry no implicit short-circuit or sequencing
// expectations.
//
// &&  and || are intentionally NOT overloaded: their overloaded forms lose
// short-circuit evaluation (both operands are always evaluated), which
// contradicts the expectation all C++ readers have of those operators. The
// C++ Core Guidelines (ES.62) explicitly discourage this.  The bitwise
// spelling (&, |) is unambiguous in a query-building context and has the
// correct relative precedence: ! > & > |, matching NOT > AND > OR.
//
//   !a            → NOT (a)
//   a & b         → (a AND b)       — & binds tighter than |
//   a | b         → (a OR b)
//   (a | b) & c   → ((a OR b) AND c) — parentheses work naturally
//
// check_expr & check_expr → check_expr  (stays check-safe)
// check_expr & sql_predicate → sql_predicate  (via implicit conversion)
// ===================================================================

[[nodiscard]] inline check_expr operator!(check_expr cond) {
    return not_(std::move(cond));
}

[[nodiscard]] inline check_expr operator&(check_expr a, check_expr b) {
    return and_(std::move(a), std::move(b));
}

[[nodiscard]] inline check_expr operator|(check_expr a, check_expr b) {
    return or_(std::move(a), std::move(b));
}

[[nodiscard]] inline sql_predicate operator!(sql_predicate cond) {
    return not_(std::move(cond));
}

[[nodiscard]] inline sql_predicate operator&(sql_predicate a, sql_predicate b) {
    return and_(std::move(a), std::move(b));
}

[[nodiscard]] inline sql_predicate operator|(sql_predicate a, sql_predicate b) {
    return or_(std::move(a), std::move(b));
}

// Concept for any query that can produce SQL — used for subquery predicates.
// Forward-declared here so subquery predicates can reference it before the full
// SELECT builder is defined.
template <typename Q>
concept AnySelectQuery = requires(Q const& q) {
    { q.build_sql() } -> std::convertible_to<std::string>;
};

// check-safe: literal value list
template <ColumnFieldType Col>
[[nodiscard]] check_expr in(std::initializer_list<typename Col::value_type> values) {
    std::string rhs;
    rhs.reserve(2 + values.size() * 4);
    rhs += '(';
    bool first = true;
    for (auto const& v : values) {
        if (!first) {
            rhs += ", ";
        }
        rhs += sql_detail::to_sql_value(v);
        first = false;
    }
    rhs += ')';
    return {column_traits<Col>::column_name(), " IN ", std::move(rhs)};
}

// check-safe: literal value list
template <ColumnFieldType Col>
[[nodiscard]] check_expr not_in(std::initializer_list<typename Col::value_type> values) {
    std::string rhs;
    rhs.reserve(2 + values.size() * 4);
    rhs += '(';
    bool first = true;
    for (auto const& v : values) {
        if (!first) {
            rhs += ", ";
        }
        rhs += sql_detail::to_sql_value(v);
        first = false;
    }
    rhs += ')';
    return {column_traits<Col>::column_name(), " NOT IN ", std::move(rhs)};
}

// check-safe
template <ColumnFieldType Col>
[[nodiscard]] check_expr between(Col const& low, Col const& high) {
    auto low_val = sql_detail::to_sql_value(low.value);
    auto high_val = sql_detail::to_sql_value(high.value);
    std::string rhs;
    rhs.reserve(low_val.size() + 5 + high_val.size());
    rhs += std::move(low_val);
    rhs += " AND ";
    rhs += std::move(high_val);
    return {column_traits<Col>::column_name(), " BETWEEN ", std::move(rhs)};
}

// check-safe
template <ColumnFieldType Col>
[[nodiscard]] check_expr not_like(std::string_view pattern) {
    auto escaped = sql_detail::escape_sql_string(pattern);
    std::string rhs;
    rhs.reserve(2 + escaped.size());
    rhs += '\'';
    rhs += std::move(escaped);
    rhs += '\'';
    return {column_traits<Col>::column_name(), " NOT LIKE ", std::move(rhs)};
}

// null_safe_equal<Col>(value) — col <=> val (MySQL NULL-safe equality) — check-safe
template <ColumnFieldType Col>
[[nodiscard]] check_expr null_safe_equal(Col const& value) {
    return {column_traits<Col>::column_name(), " <=> ", sql_detail::to_sql_value(value.value)};
}

// regexp<Col>(pattern) — col REGEXP 'pattern' — check-safe
template <ColumnFieldType Col>
[[nodiscard]] check_expr regexp(std::string_view pattern) {
    auto escaped = sql_detail::escape_sql_string(pattern);
    std::string rhs;
    rhs.reserve(2 + escaped.size());
    rhs += '\'';
    rhs += std::move(escaped);
    rhs += '\'';
    return {column_traits<Col>::column_name(), " REGEXP ", std::move(rhs)};
}

// not_regexp<Col>(pattern) — col NOT REGEXP 'pattern' — check-safe
template <ColumnFieldType Col>
[[nodiscard]] check_expr not_regexp(std::string_view pattern) {
    auto escaped = sql_detail::escape_sql_string(pattern);
    std::string rhs;
    rhs.reserve(2 + escaped.size());
    rhs += '\'';
    rhs += std::move(escaped);
    rhs += '\'';
    return {column_traits<Col>::column_name(), " NOT REGEXP ", std::move(rhs)};
}

// rlike<Col>(pattern) — col RLIKE 'pattern'  (MySQL synonym for REGEXP) — check-safe
template <ColumnFieldType Col>
[[nodiscard]] check_expr rlike(std::string_view pattern) {
    return regexp<Col>(pattern);
}

// not_rlike<Col>(pattern) — col NOT RLIKE 'pattern' — check-safe
template <ColumnFieldType Col>
[[nodiscard]] check_expr not_rlike(std::string_view pattern) {
    return not_regexp<Col>(pattern);
}

// sounds_like<Col>(word) — col SOUNDS LIKE 'word' — check-safe
template <ColumnFieldType Col>
[[nodiscard]] check_expr sounds_like(std::string_view word) {
    auto escaped = sql_detail::escape_sql_string(word);
    std::string rhs;
    rhs.reserve(2 + escaped.size());
    rhs += '\'';
    rhs += std::move(escaped);
    rhs += '\'';
    return {column_traits<Col>::column_name(), " SOUNDS LIKE ", std::move(rhs)};
}

// match_against<Cols...>(expr [, modifier]) — MATCH(col1, col2, ...) AGAINST ('expr' [modifier])
//
// modifier is one of:
//   match_search_modifier::natural_language             (default)  — no modifier emitted
//   match_search_modifier::boolean_mode                            — IN BOOLEAN MODE
//   match_search_modifier::query_expansion                         — WITH QUERY EXPANSION
//   match_search_modifier::natural_language_with_query_expansion   — IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION
template <ColumnFieldType... Cols>
    requires(sizeof...(Cols) > 0)
[[nodiscard]] sql_predicate match_against(std::string_view expr,
                                          match_search_modifier modifier = match_search_modifier::natural_language) {
    std::string s;
    s.reserve(64 + expr.size());
    s += "MATCH(";
    bool first = true;
    auto add_col = [&](std::string_view name) {
        if (!first)
            s += ", ";
        s += name;
        first = false;
    };
    (add_col(column_traits<Cols>::column_name()), ...);
    s += ") AGAINST ('";
    s += sql_detail::escape_sql_string(expr);
    s += '\'';
    switch (modifier) {
        case match_search_modifier::boolean_mode:
            s += " IN BOOLEAN MODE";
            break;
        case match_search_modifier::query_expansion:
            s += " WITH QUERY EXPANSION";
            break;
        case match_search_modifier::natural_language_with_query_expansion:
            s += " IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION";
            break;
        case match_search_modifier::natural_language:
            break;
    }
    s += ')';
    return {{}, {}, std::move(s)};
}

template <ColumnFieldType Col, AnySelectQuery Query>
[[nodiscard]] sql_predicate in_subquery(Query const& subquery) {
    auto sub = subquery.build_sql();
    std::string rhs;
    rhs.reserve(2 + sub.size());
    rhs += '(';
    rhs += std::move(sub);
    rhs += ')';
    return {column_traits<Col>::column_name(), " IN ", std::move(rhs)};
}

template <ColumnFieldType Col, AnySelectQuery Query>
[[nodiscard]] sql_predicate not_in_subquery(Query const& subquery) {
    auto sub = subquery.build_sql();
    std::string rhs;
    rhs.reserve(2 + sub.size());
    rhs += '(';
    rhs += std::move(sub);
    rhs += ')';
    return {column_traits<Col>::column_name(), " NOT IN ", std::move(rhs)};
}

template <AnySelectQuery Query>
[[nodiscard]] sql_predicate exists(Query const& subquery) {
    auto sub = subquery.build_sql();
    std::string s;
    s.reserve(8 + sub.size() + 1);
    s += "EXISTS (";
    s += std::move(sub);
    s += ')';
    return {{}, {}, std::move(s)};
}

template <AnySelectQuery Query>
[[nodiscard]] sql_predicate not_exists(Query const& subquery) {
    auto sub = subquery.build_sql();
    std::string s;
    s.reserve(12 + sub.size() + 1);
    s += "NOT EXISTS (";
    s += std::move(sub);
    s += ')';
    return {{}, {}, std::move(s)};
}

// ===================================================================
// col_expr<Col> / col_ref<Col> — natural operator syntax for predicates
//
// col_ref<Col> is an inline constexpr variable template that exposes
// comparison and predicate operators. Simple column-ref operators return
// check_expr (usable in WHERE, HAVING, CHECK); subquery operators return
// sql_predicate (WHERE/HAVING only). Results compose with &, |, ! (see above).
//
// Single-column predicates (return check_expr):
//   col_ref<trade::code> == "AAPL"             → equal<trade::code>("AAPL")
//   col_ref<trade::id> != 0u                   → not_equal<trade::id>(0u)
//   col_ref<trade::id> < 100u                  → less_than<trade::id>(100u)
//   col_ref<trade::id> > 0u                    → greater_than<trade::id>(0u)
//   col_ref<trade::id> <= 100u                 → less_than_or_equal<trade::id>(100u)
//   col_ref<trade::id> >= 1u                   → greater_than_or_equal<trade::id>(1u)
//   col_ref<trade::name>.is_null()             → is_null<trade::name>()
//   col_ref<trade::name>.is_not_null()         → is_not_null<trade::name>()
//   col_ref<trade::code>.like("AAPL%")         → like<trade::code>("AAPL%")
//   col_ref<trade::code>.not_like("X%")        → not_like<trade::code>("X%")
//   col_ref<trade::id>.between(1u, 10u)           → between<trade::id>(1u, 10u)
//   col_ref<trade::code>.in({"AAPL", "GOOGL"})    → in<trade::code>({"AAPL", "GOOGL"})
//   col_ref<trade::code>.not_in({"X", "Y"})       → not_in<trade::code>({"X", "Y"})
//   col_ref<trade::code>.regexp("^A")             → regexp<trade::code>("^A")
//   col_ref<trade::code>.not_regexp("^A")         → not_regexp<trade::code>("^A")
//   col_ref<trade::code>.rlike("^A")              → rlike<trade::code>("^A")
//   col_ref<trade::code>.not_rlike("^A")          → not_rlike<trade::code>("^A")
//   col_ref<trade::name>.sounds_like("word")      → sounds_like<trade::name>("word")
//
// Composing with &, |, !:
//   col_ref<trade::code> == "AAPL" | col_ref<trade::code> == "GOOGL"
//   col_ref<trade::id> >= 1u & col_ref<trade::id> <= 100u
//   !(col_ref<trade::code> == "AAPL")
//   (col_ref<trade::code> == "AAPL" | col_ref<trade::code> == "GOOGL") &
//       col_ref<trade::type> == "Stock"
//
// Note: the exact syntax `Col = value` (e.g. `trade::code = "AAPL"`) is not
// achievable in C++ — `trade::code` is a type alias, not an object, so no
// operator can be invoked on it directly.
// ===================================================================

template <ColumnFieldType Col>
struct col_expr {
    using value_type = typename Col::value_type;

    // Operators accept value_type directly; callers must construct explicitly
    // (e.g. col_ref<trade::code> == varchar_type<32>{"AAPL"}).
    [[nodiscard]] check_expr operator==(value_type const& val) const {
        return equal<Col>(Col{val});
    }
    [[nodiscard]] check_expr operator!=(value_type const& val) const {
        return not_equal<Col>(Col{val});
    }
    [[nodiscard]] check_expr operator<(value_type const& val) const {
        return less_than<Col>(Col{val});
    }
    [[nodiscard]] check_expr operator>(value_type const& val) const {
        return greater_than<Col>(Col{val});
    }
    [[nodiscard]] check_expr operator<=(value_type const& val) const {
        return less_than_or_equal<Col>(Col{val});
    }
    [[nodiscard]] check_expr operator>=(value_type const& val) const {
        return greater_than_or_equal<Col>(Col{val});
    }

    [[nodiscard]] check_expr is_null() const noexcept
        requires is_field_nullable_v<Col>
    {
        return ds_mysql::is_null<Col>();
    }
    [[nodiscard]] check_expr is_not_null() const noexcept
        requires is_field_nullable_v<Col>
    {
        return ds_mysql::is_not_null<Col>();
    }

    [[nodiscard]] check_expr like(std::string_view pattern) const {
        return ds_mysql::like<Col>(pattern);
    }
    [[nodiscard]] check_expr not_like(std::string_view pattern) const {
        return ds_mysql::not_like<Col>(pattern);
    }
    [[nodiscard]] check_expr between(value_type const& low, value_type const& high) const {
        return ds_mysql::between<Col>(Col{low}, Col{high});
    }
    [[nodiscard]] check_expr in(std::initializer_list<value_type> vals) const {
        return ds_mysql::in<Col>(vals);
    }
    [[nodiscard]] check_expr not_in(std::initializer_list<value_type> vals) const {
        return ds_mysql::not_in<Col>(vals);
    }

    [[nodiscard]] check_expr regexp(std::string_view pattern) const {
        return ds_mysql::regexp<Col>(pattern);
    }
    [[nodiscard]] check_expr not_regexp(std::string_view pattern) const {
        return ds_mysql::not_regexp<Col>(pattern);
    }
    [[nodiscard]] check_expr rlike(std::string_view pattern) const {
        return ds_mysql::rlike<Col>(pattern);
    }
    [[nodiscard]] check_expr not_rlike(std::string_view pattern) const {
        return ds_mysql::not_rlike<Col>(pattern);
    }
    [[nodiscard]] check_expr sounds_like(std::string_view word) const {
        return ds_mysql::sounds_like<Col>(word);
    }

    [[nodiscard]] check_expr null_safe_equal(value_type const& val) const {
        return ds_mysql::null_safe_equal<Col>(Col{val});
    }

    template <AnySelectQuery Query>
    [[nodiscard]] sql_predicate in_subquery(Query const& subquery) const {
        return ds_mysql::in_subquery<Col>(subquery);
    }

    template <AnySelectQuery Query>
    [[nodiscard]] sql_predicate not_in_subquery(Query const& subquery) const {
        return ds_mysql::not_in_subquery<Col>(subquery);
    }

    template <AnySelectQuery Query>
    [[nodiscard]] sql_predicate eq_subquery(Query const& subquery) const {
        auto sub = subquery.build_sql();
        std::string rhs;
        rhs.reserve(2 + sub.size());
        rhs += '(';
        rhs += std::move(sub);
        rhs += ')';
        return {column_traits<Col>::column_name(), " = ", std::move(rhs)};
    }

    template <AnySelectQuery Query>
    [[nodiscard]] sql_predicate ne_subquery(Query const& subquery) const {
        auto sub = subquery.build_sql();
        std::string rhs;
        rhs.reserve(2 + sub.size());
        rhs += '(';
        rhs += std::move(sub);
        rhs += ')';
        return {column_traits<Col>::column_name(), " != ", std::move(rhs)};
    }

    template <AnySelectQuery Query>
    [[nodiscard]] sql_predicate lt_subquery(Query const& subquery) const {
        auto sub = subquery.build_sql();
        std::string rhs;
        rhs.reserve(2 + sub.size());
        rhs += '(';
        rhs += std::move(sub);
        rhs += ')';
        return {column_traits<Col>::column_name(), " < ", std::move(rhs)};
    }

    template <AnySelectQuery Query>
    [[nodiscard]] sql_predicate gt_subquery(Query const& subquery) const {
        auto sub = subquery.build_sql();
        std::string rhs;
        rhs.reserve(2 + sub.size());
        rhs += '(';
        rhs += std::move(sub);
        rhs += ')';
        return {column_traits<Col>::column_name(), " > ", std::move(rhs)};
    }

    template <AnySelectQuery Query>
    [[nodiscard]] sql_predicate le_subquery(Query const& subquery) const {
        auto sub = subquery.build_sql();
        std::string rhs;
        rhs.reserve(2 + sub.size());
        rhs += '(';
        rhs += std::move(sub);
        rhs += ')';
        return {column_traits<Col>::column_name(), " <= ", std::move(rhs)};
    }

    template <AnySelectQuery Query>
    [[nodiscard]] sql_predicate ge_subquery(Query const& subquery) const {
        auto sub = subquery.build_sql();
        std::string rhs;
        rhs.reserve(2 + sub.size());
        rhs += '(';
        rhs += std::move(sub);
        rhs += ')';
        return {column_traits<Col>::column_name(), " >= ", std::move(rhs)};
    }
};

// col_ref<Col> — inline variable for natural operator-based WHERE expressions.
template <ColumnFieldType Col>
inline constexpr col_expr<Col> col_ref{};

// ===================================================================
// SqlStatement — unified concept for all SQL builder types.
// Satisfied by any builder stage that exposes build_sql() -> std::string.
// ===================================================================

template <typename T>
concept SqlStatement = requires(T const& t) {
    { t.build_sql() } -> std::convertible_to<std::string>;
};

}  // namespace ds_mysql
