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

inline std::string escape_sql_string(std::string_view s) {
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

inline std::string format_datetime(std::chrono::system_clock::time_point tp, uint32_t fractional_second_precision = 0) {
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

inline std::string format_time(std::chrono::microseconds dur, uint32_t fractional_second_precision = 0) {
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
std::string to_sql_value(T const& v) {
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
// where_condition — a typed SQL WHERE fragment
//
// Stores the column name (as a compile-time std::string_view),
// the operator literal (e.g. " = ", " IS NULL"), and the serialized
// value — building the final SQL string lazily via build_sql().
//
// Produced exclusively by the typed predicate free functions below.
//
//   .where(equal<symbol::ticker>("AAPL"))        ← typed predicate
//   .having(count() > 5)                         ← aggregate predicate
// ===================================================================

struct where_condition {
    std::string_view col_name;  // compile-time column name; empty for composed/raw conditions
    std::string_view op;        // operator literal (" = ", " IS NULL", etc.)
    std::string value;          // serialized RHS value, or full SQL for composed/raw conditions

    where_condition() = default;
    where_condition(std::string_view col_name_, std::string_view op_, std::string value_)
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
// WHERE predicates
//
//   equal<Col>(value)                    — col = val
//   not_equal<Col>(value)                — col != val
//   less_than<Col>(value)                — col < val
//   greater_than<Col>(value)             — col > val
//   less_than_or_equal<Col>(value)       — col <= val
//   greater_than_or_equal<Col>(value)    — col >= val
//   is_null<Col>()                       — col IS NULL
//   is_not_null<Col>()                   — col IS NOT NULL
//   like<Col>(pattern)                   — col LIKE 'pattern'
//   and_(a, b)                           — (a AND b)
//   or_(a, b)                            — (a OR b)
//   not_(cond)                           — NOT (cond)
// ===================================================================

template <ColumnFieldType Col>
[[nodiscard]] where_condition equal(Col const& value) {
    return {column_traits<Col>::column_name(), " = ", sql_detail::to_sql_value(value.value)};
}

template <ColumnFieldType Col>
[[nodiscard]] where_condition not_equal(Col const& value) {
    return {column_traits<Col>::column_name(), " != ", sql_detail::to_sql_value(value.value)};
}

template <ColumnFieldType Col>
[[nodiscard]] where_condition less_than(Col const& value) {
    return {column_traits<Col>::column_name(), " < ", sql_detail::to_sql_value(value.value)};
}

template <ColumnFieldType Col>
[[nodiscard]] where_condition greater_than(Col const& value) {
    return {column_traits<Col>::column_name(), " > ", sql_detail::to_sql_value(value.value)};
}

template <ColumnFieldType Col>
[[nodiscard]] where_condition less_than_or_equal(Col const& value) {
    return {column_traits<Col>::column_name(), " <= ", sql_detail::to_sql_value(value.value)};
}

template <ColumnFieldType Col>
[[nodiscard]] where_condition greater_than_or_equal(Col const& value) {
    return {column_traits<Col>::column_name(), " >= ", sql_detail::to_sql_value(value.value)};
}

template <ColumnFieldType Col>
    requires is_field_nullable_v<Col>
[[nodiscard]] where_condition is_null() {
    return {column_traits<Col>::column_name(), " IS NULL", {}};
}

template <ColumnFieldType Col>
    requires is_field_nullable_v<Col>
[[nodiscard]] where_condition is_not_null() {
    return {column_traits<Col>::column_name(), " IS NOT NULL", {}};
}

template <ColumnFieldType Col>
[[nodiscard]] where_condition like(std::string_view pattern) {
    auto escaped = sql_detail::escape_sql_string(pattern);
    std::string rhs;
    rhs.reserve(2 + escaped.size());
    rhs += '\'';
    rhs += std::move(escaped);
    rhs += '\'';
    return {column_traits<Col>::column_name(), " LIKE ", std::move(rhs)};
}

[[nodiscard]] inline where_condition and_(where_condition a, where_condition b) {
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

[[nodiscard]] inline where_condition or_(where_condition a, where_condition b) {
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

[[nodiscard]] inline where_condition not_(where_condition cond) {
    auto cs = cond.build_sql();
    std::string s;
    s.reserve(5 + cs.size() + 1);
    s += "NOT (";
    s += std::move(cs);
    s += ')';
    return {{}, {}, std::move(s)};
}

// ===================================================================
// Operator overloads for natural WHERE composition
//
// &, |, and ! can be overloaded safely for a DSL type like where_condition
// because they carry no implicit short-circuit or sequencing expectations.
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
// ===================================================================

[[nodiscard]] inline where_condition operator!(where_condition cond) {
    return not_(std::move(cond));
}

[[nodiscard]] inline where_condition operator&(where_condition a, where_condition b) {
    return and_(std::move(a), std::move(b));
}

[[nodiscard]] inline where_condition operator|(where_condition a, where_condition b) {
    return or_(std::move(a), std::move(b));
}

// ===================================================================
// WHERE predicates (continued)
//
//   in<Col>({v1, v2, ...})               — col IN (v1, v2, ...)
//   not_in<Col>({v1, v2, ...})           — col NOT IN (v1, v2, ...)
//   between<Col>(low, high)              — col BETWEEN low AND high
//   not_like<Col>(pattern)               — col NOT LIKE 'pattern'
//   in_subquery<Col>(query)              — col IN (SELECT ...)
//   not_in_subquery<Col>(query)          — col NOT IN (SELECT ...)
//   exists(query)                       — EXISTS (SELECT ...)
//   not_exists(query)                   — NOT EXISTS (SELECT ...)
// ===================================================================

// Concept for any query that can produce SQL — used for subquery predicates.
// Forward-declared here so subquery predicates can reference it before the full
// SELECT builder is defined.
template <typename Q>
concept AnySelectQuery = requires(Q const& q) {
    { q.build_sql() } -> std::convertible_to<std::string>;
};

template <ColumnFieldType Col>
[[nodiscard]] where_condition in(std::initializer_list<typename Col::value_type> values) {
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

template <ColumnFieldType Col>
[[nodiscard]] where_condition not_in(std::initializer_list<typename Col::value_type> values) {
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

template <ColumnFieldType Col>
[[nodiscard]] where_condition between(Col const& low, Col const& high) {
    auto low_val = sql_detail::to_sql_value(low.value);
    auto high_val = sql_detail::to_sql_value(high.value);
    std::string rhs;
    rhs.reserve(low_val.size() + 5 + high_val.size());
    rhs += std::move(low_val);
    rhs += " AND ";
    rhs += std::move(high_val);
    return {column_traits<Col>::column_name(), " BETWEEN ", std::move(rhs)};
}

template <ColumnFieldType Col>
[[nodiscard]] where_condition not_like(std::string_view pattern) {
    auto escaped = sql_detail::escape_sql_string(pattern);
    std::string rhs;
    rhs.reserve(2 + escaped.size());
    rhs += '\'';
    rhs += std::move(escaped);
    rhs += '\'';
    return {column_traits<Col>::column_name(), " NOT LIKE ", std::move(rhs)};
}

// null_safe_equal<Col>(value) — col <=> val (MySQL NULL-safe equality)
template <ColumnFieldType Col>
[[nodiscard]] where_condition null_safe_equal(Col const& value) {
    return {column_traits<Col>::column_name(), " <=> ", sql_detail::to_sql_value(value.value)};
}

// regexp<Col>(pattern) — col REGEXP 'pattern'
template <ColumnFieldType Col>
[[nodiscard]] where_condition regexp(std::string_view pattern) {
    auto escaped = sql_detail::escape_sql_string(pattern);
    std::string rhs;
    rhs.reserve(2 + escaped.size());
    rhs += '\'';
    rhs += std::move(escaped);
    rhs += '\'';
    return {column_traits<Col>::column_name(), " REGEXP ", std::move(rhs)};
}

template <ColumnFieldType Col, AnySelectQuery Query>
[[nodiscard]] where_condition in_subquery(Query const& subquery) {
    auto sub = subquery.build_sql();
    std::string rhs;
    rhs.reserve(2 + sub.size());
    rhs += '(';
    rhs += std::move(sub);
    rhs += ')';
    return {column_traits<Col>::column_name(), " IN ", std::move(rhs)};
}

template <ColumnFieldType Col, AnySelectQuery Query>
[[nodiscard]] where_condition not_in_subquery(Query const& subquery) {
    auto sub = subquery.build_sql();
    std::string rhs;
    rhs.reserve(2 + sub.size());
    rhs += '(';
    rhs += std::move(sub);
    rhs += ')';
    return {column_traits<Col>::column_name(), " NOT IN ", std::move(rhs)};
}

template <AnySelectQuery Query>
[[nodiscard]] where_condition exists(Query const& subquery) {
    auto sub = subquery.build_sql();
    std::string s;
    s.reserve(8 + sub.size() + 1);
    s += "EXISTS (";
    s += std::move(sub);
    s += ')';
    return {{}, {}, std::move(s)};
}

template <AnySelectQuery Query>
[[nodiscard]] where_condition not_exists(Query const& subquery) {
    auto sub = subquery.build_sql();
    std::string s;
    s.reserve(12 + sub.size() + 1);
    s += "NOT EXISTS (";
    s += std::move(sub);
    s += ')';
    return {{}, {}, std::move(s)};
}

// ===================================================================
// col_expr<Col> / col_ref<Col> — natural operator syntax for WHERE clauses
//
// col_ref<Col> is an inline constexpr variable template that exposes
// comparison and predicate operators, each returning a where_condition.
// The resulting where_condition values compose with &, |, ! (see above).
//
// Single-column predicates:
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
//   col_ref<trade::id>.between(1u, 10u)        → between<trade::id>(1u, 10u)
//   col_ref<trade::code>.in({"AAPL", "GOOGL"}) → in<trade::code>({"AAPL", "GOOGL"})
//   col_ref<trade::code>.not_in({"X", "Y"})    → not_in<trade::code>({"X", "Y"})
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
    [[nodiscard]] where_condition operator==(value_type const& val) const {
        return equal<Col>(Col{val});
    }
    [[nodiscard]] where_condition operator!=(value_type const& val) const {
        return not_equal<Col>(Col{val});
    }
    [[nodiscard]] where_condition operator<(value_type const& val) const {
        return less_than<Col>(Col{val});
    }
    [[nodiscard]] where_condition operator>(value_type const& val) const {
        return greater_than<Col>(Col{val});
    }
    [[nodiscard]] where_condition operator<=(value_type const& val) const {
        return less_than_or_equal<Col>(Col{val});
    }
    [[nodiscard]] where_condition operator>=(value_type const& val) const {
        return greater_than_or_equal<Col>(Col{val});
    }

    [[nodiscard]] where_condition is_null() const noexcept
        requires is_field_nullable_v<Col>
    {
        return ds_mysql::is_null<Col>();
    }
    [[nodiscard]] where_condition is_not_null() const noexcept
        requires is_field_nullable_v<Col>
    {
        return ds_mysql::is_not_null<Col>();
    }

    [[nodiscard]] where_condition like(std::string_view pattern) const {
        return ds_mysql::like<Col>(pattern);
    }
    [[nodiscard]] where_condition not_like(std::string_view pattern) const {
        return ds_mysql::not_like<Col>(pattern);
    }
    [[nodiscard]] where_condition between(value_type const& low, value_type const& high) const {
        return ds_mysql::between<Col>(Col{low}, Col{high});
    }
    [[nodiscard]] where_condition in(std::initializer_list<value_type> vals) const {
        return ds_mysql::in<Col>(vals);
    }
    [[nodiscard]] where_condition not_in(std::initializer_list<value_type> vals) const {
        return ds_mysql::not_in<Col>(vals);
    }

    [[nodiscard]] where_condition regexp(std::string_view pattern) const {
        return ds_mysql::regexp<Col>(pattern);
    }

    [[nodiscard]] where_condition null_safe_equal(value_type const& val) const {
        return ds_mysql::null_safe_equal<Col>(Col{val});
    }

    template <AnySelectQuery Query>
    [[nodiscard]] where_condition in_subquery(Query const& subquery) const {
        return ds_mysql::in_subquery<Col>(subquery);
    }

    template <AnySelectQuery Query>
    [[nodiscard]] where_condition not_in_subquery(Query const& subquery) const {
        return ds_mysql::not_in_subquery<Col>(subquery);
    }

    template <AnySelectQuery Query>
    [[nodiscard]] where_condition eq_subquery(Query const& subquery) const {
        auto sub = subquery.build_sql();
        std::string rhs;
        rhs.reserve(2 + sub.size());
        rhs += '(';
        rhs += std::move(sub);
        rhs += ')';
        return {column_traits<Col>::column_name(), " = ", std::move(rhs)};
    }

    template <AnySelectQuery Query>
    [[nodiscard]] where_condition ne_subquery(Query const& subquery) const {
        auto sub = subquery.build_sql();
        std::string rhs;
        rhs.reserve(2 + sub.size());
        rhs += '(';
        rhs += std::move(sub);
        rhs += ')';
        return {column_traits<Col>::column_name(), " != ", std::move(rhs)};
    }

    template <AnySelectQuery Query>
    [[nodiscard]] where_condition lt_subquery(Query const& subquery) const {
        auto sub = subquery.build_sql();
        std::string rhs;
        rhs.reserve(2 + sub.size());
        rhs += '(';
        rhs += std::move(sub);
        rhs += ')';
        return {column_traits<Col>::column_name(), " < ", std::move(rhs)};
    }

    template <AnySelectQuery Query>
    [[nodiscard]] where_condition gt_subquery(Query const& subquery) const {
        auto sub = subquery.build_sql();
        std::string rhs;
        rhs.reserve(2 + sub.size());
        rhs += '(';
        rhs += std::move(sub);
        rhs += ')';
        return {column_traits<Col>::column_name(), " > ", std::move(rhs)};
    }

    template <AnySelectQuery Query>
    [[nodiscard]] where_condition le_subquery(Query const& subquery) const {
        auto sub = subquery.build_sql();
        std::string rhs;
        rhs.reserve(2 + sub.size());
        rhs += '(';
        rhs += std::move(sub);
        rhs += ')';
        return {column_traits<Col>::column_name(), " <= ", std::move(rhs)};
    }

    template <AnySelectQuery Query>
    [[nodiscard]] where_condition ge_subquery(Query const& subquery) const {
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
