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
#include "ds_mysql/varchar_field.hpp"

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

enum class Engine {
    InnoDB,
    MyISAM,
    Memory,
    Ndb,
    Archive,
    Csv,
    Merge,
    Blackhole,
    Federated,
};

enum class Charset {
    utf8mb4,
    utf8,
    latin1,
    ascii,
    ucs2,
    utf16,
    utf32,
};

enum class RowFormat {
    Default,
    Dynamic,
    Fixed,
    Compressed,
    Redundant,
    Compact,
};

enum class InsertMethod {
    No,
    First,
    Last,
};

enum class PackKeys {
    Default,
    Zero,
    One,
};

enum class StatsPolicy {
    Default,
    Zero,
    One,
};

enum class Compression {
    None,
    Zlib,
    Lz4,
};

enum class Encryption {
    Y,
    N,
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

/**
 * to_sql_value — convert a typed C++ value to its SQL literal representation.
 *
 * Handles: column_field<T> wrappers, std::optional<T>, sql_datetime, bool,
 * integral types, floating-point types, varchar_field<N>, std::string, and
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
    } else if constexpr (std::same_as<T, sql_datetime>) {
        if (v.is_now()) {
            auto const precision = normalize_fractional_second_precision(v.fractional_second_precision());
            if (precision == 0U) {
                return "NOW()";
            }
            return "NOW(" + std::to_string(precision) + ")";
        }
        return format_datetime(v.time_point(), v.fractional_second_precision());
    } else if constexpr (std::same_as<T, sql_timestamp>) {
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
    } else if constexpr (std::same_as<T, bool>) {
        return v ? "1" : "0";
    } else if constexpr (std::integral<T>) {
        return std::to_string(v);
    } else if constexpr (std::floating_point<T>) {
        return std::to_string(v);
    } else if constexpr (is_varchar_field_v<T>) {
        return "'" + escape_sql_string(v.view()) + "'";
    } else if constexpr (std::same_as<T, std::string>) {
        return "'" + escape_sql_string(v) + "'";
    } else {
        static_assert(false,
                      "to_sql_value: unsupported type. "
                      "Supported: column_field<T>, optional<T>, sql_datetime, sql_timestamp, bool, "
                      "integral types, floating-point types, varchar_field<N>, std::string, "
                      "std::chrono::system_clock::time_point");
    }
}

}  // namespace sql_detail

// ===================================================================
// SqlValue — concept matching every type accepted by sql_detail::to_sql_value.
// Constrains template parameters that must produce a valid SQL literal.
// ===================================================================
template <typename T>
concept SqlValue =
    ColumnFieldType<T> || is_optional_v<T> || std::same_as<T, sql_datetime> || std::same_as<T, sql_timestamp> ||
    std::same_as<T, std::chrono::system_clock::time_point> || std::same_as<T, bool> || std::integral<T> ||
    std::floating_point<T> || is_varchar_field_v<T> || std::same_as<T, std::string>;

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
    // (e.g. col_ref<trade::code> == varchar_field<32>{"AAPL"}).
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
// CREATE TABLE constraint customization
//
// table_inline_primary_key<T>
//   - true  (default): first column is emitted as `PRIMARY KEY AUTO_INCREMENT`
//   - false          : first column is emitted as `AUTO_INCREMENT` only,
//                      allowing table_constraints<T> to define table-level PK
//
// table_constraints<T>
//   - return additional table-level constraints/index definitions appended
//     inside CREATE TABLE (...)
// ===================================================================

template <typename T>
struct table_inline_primary_key {
    static constexpr bool value = true;
};

template <typename T>
struct table_constraints {
    static std::vector<std::string> get() {
        return {};
    }
};

namespace table_constraint {

template <ColumnDescriptor... Cols>
    requires(sizeof...(Cols) > 0)
[[nodiscard]] inline std::string columns_sql() {
    std::string s;
    s += '(';
    bool first = true;
    (((s += (first ? "" : ", "), s += column_traits<Cols>::column_name(), first = false)), ...);
    s += ')';
    return s;
}

template <ColumnDescriptor... Cols>
    requires(sizeof...(Cols) > 0)
[[nodiscard]] inline std::string primary_key(std::string_view constraint_name = {}) {
    std::string s;
    if (!constraint_name.empty()) {
        s += "CONSTRAINT ";
        s += constraint_name;
        s += ' ';
    }
    s += "PRIMARY KEY ";
    s += columns_sql<Cols...>();
    return s;
}

template <ColumnDescriptor... Cols>
    requires(sizeof...(Cols) > 0)
[[nodiscard]] inline std::string key(std::string_view index_name) {
    std::string s;
    s += "KEY ";
    s += index_name;
    s += ' ';
    s += columns_sql<Cols...>();
    return s;
}

template <ColumnDescriptor... Cols>
    requires(sizeof...(Cols) > 0)
[[nodiscard]] inline std::string unique_key(std::string_view index_name) {
    std::string s;
    s += "UNIQUE KEY ";
    s += index_name;
    s += ' ';
    s += columns_sql<Cols...>();
    return s;
}

template <ColumnDescriptor... Cols>
    requires(sizeof...(Cols) > 0)
[[nodiscard]] inline std::string fulltext_key(std::string_view index_name) {
    std::string s;
    s += "FULLTEXT KEY ";
    s += index_name;
    s += ' ';
    s += columns_sql<Cols...>();
    return s;
}

template <ColumnDescriptor... Cols>
    requires(sizeof...(Cols) > 0)
[[nodiscard]] inline std::string spatial_key(std::string_view index_name) {
    std::string s;
    s += "SPATIAL KEY ";
    s += index_name;
    s += ' ';
    s += columns_sql<Cols...>();
    return s;
}

template <ColumnDescriptor... Cols>
    requires(sizeof...(Cols) > 0)
[[nodiscard]] inline std::string foreign_key(std::string_view constraint_name, std::string_view referenced_table,
                                             std::string_view referenced_columns,
                                             std::string_view on_delete_action = {},
                                             std::string_view on_update_action = {}) {
    std::string s;
    s += "CONSTRAINT ";
    s += constraint_name;
    s += " FOREIGN KEY ";
    s += columns_sql<Cols...>();
    s += " REFERENCES ";
    s += referenced_table;
    s += '(';
    s += referenced_columns;
    s += ')';
    if (!on_delete_action.empty()) {
        s += " ON DELETE ";
        s += on_delete_action;
    }
    if (!on_update_action.empty()) {
        s += " ON UPDATE ";
        s += on_update_action;
    }
    return s;
}

[[nodiscard]] inline std::string check(std::string_view expression, std::string_view constraint_name = {}) {
    std::string s;
    if (!constraint_name.empty()) {
        s += "CONSTRAINT ";
        s += constraint_name;
        s += ' ';
    }
    s += "CHECK (";
    s += expression;
    s += ')';
    return s;
}

[[nodiscard]] inline std::string raw(std::string sql_fragment) {
    return sql_fragment;
}

}  // namespace table_constraint

// ===================================================================
// DDL Command Builders — CREATE TABLE, DROP TABLE, CREATE DATABASE, etc.
//
// Entry points are free functions:
//   create_database<DB>()                                         — CREATE DATABASE db
//   create_database<DB>().if_not_exists()                         — CREATE DATABASE IF NOT EXISTS db
//   create_table<T>()                                              — CREATE TABLE T (...)
//   create_table<T>().if_not_exists()                             — CREATE TABLE IF NOT EXISTS T (...)
//   create_temporary_table<T>()                                   — CREATE TEMPORARY TABLE T (...)
//   create_temporary_table<T>().if_not_exists()                   — CREATE TEMPORARY TABLE IF NOT EXISTS T (...)
//   drop_table<T>()                                               — DROP TABLE T
//   drop_table<T>().if_exists()                                   — DROP TABLE IF EXISTS T
//   drop_temporary_table<T>()                                     — DROP TEMPORARY TABLE T
//   drop_temporary_table<T>().if_exists()                         — DROP TEMPORARY TABLE IF EXISTS T
//   create_table<NewT>().as(select_query)                         — CREATE TABLE NewT AS <select>
//   create_table<NewT>().if_not_exists().as(select_query)         — CREATE TABLE IF NOT EXISTS NewT AS <select>
//   create_temporary_table<NewT>().as(select_query)               — CREATE TEMPORARY TABLE NewT AS <select>
//   create_temporary_table<NewT>().if_not_exists().as(select_query) — CREATE TEMPORARY TABLE IF NOT EXISTS NewT AS
//   <select>
//   create_table<NewT>().like<SourceT>()                          — CREATE TABLE NewT LIKE SourceT
//   create_table<NewT>().if_not_exists().like<SourceT>()          — CREATE TABLE IF NOT EXISTS NewT LIKE SourceT
//
// Commands can be sequenced with .then():
//   create_database<DB>().then().create_table<T>().if_not_exists()
//   drop_table<T>().if_exists().then().create_table<T>().if_not_exists()
//
// All stages terminate with .build_sql() -> std::string.
// Note: use<DB>() (MySQL extension) is in sql_extension.hpp.
// ===================================================================

namespace ddl_detail {

inline std::string escape_sql_single_quotes(std::string_view value) {
    std::string out;
    out.reserve(value.size());
    for (const char ch : value) {
        if (ch == '\'') {
            out += "''";
        } else {
            out += ch;
        }
    }
    return out;
}

inline std::string quote_sql_string(std::string_view value) {
    std::string out;
    out.reserve(value.size() + 2);
    out += '\'';
    out += escape_sql_single_quotes(value);
    out += '\'';
    return out;
}

inline std::string to_sql_engine(Engine value) {
    switch (value) {
        case Engine::InnoDB:
            return "InnoDB";
        case Engine::MyISAM:
            return "MyISAM";
        case Engine::Memory:
            return "MEMORY";
        case Engine::Ndb:
            return "NDB";
        case Engine::Archive:
            return "ARCHIVE";
        case Engine::Csv:
            return "CSV";
        case Engine::Merge:
            return "MERGE";
        case Engine::Blackhole:
            return "BLACKHOLE";
        case Engine::Federated:
            return "FEDERATED";
    }
    return "InnoDB";
}

inline std::string to_sql_charset(Charset value) {
    switch (value) {
        case Charset::utf8mb4:
            return "utf8mb4";
        case Charset::utf8:
            return "utf8";
        case Charset::latin1:
            return "latin1";
        case Charset::ascii:
            return "ascii";
        case Charset::ucs2:
            return "ucs2";
        case Charset::utf16:
            return "utf16";
        case Charset::utf32:
            return "utf32";
    }
    return "utf8mb4";
}

inline std::string to_sql_row_format(RowFormat value) {
    switch (value) {
        case RowFormat::Default:
            return "DEFAULT";
        case RowFormat::Dynamic:
            return "DYNAMIC";
        case RowFormat::Fixed:
            return "FIXED";
        case RowFormat::Compressed:
            return "COMPRESSED";
        case RowFormat::Redundant:
            return "REDUNDANT";
        case RowFormat::Compact:
            return "COMPACT";
    }
    return "DEFAULT";
}

inline std::string to_sql_insert_method(InsertMethod value) {
    switch (value) {
        case InsertMethod::No:
            return "NO";
        case InsertMethod::First:
            return "FIRST";
        case InsertMethod::Last:
            return "LAST";
    }
    return "NO";
}

inline std::string to_sql_pack_keys(PackKeys value) {
    switch (value) {
        case PackKeys::Default:
            return "DEFAULT";
        case PackKeys::Zero:
            return "0";
        case PackKeys::One:
            return "1";
    }
    return "DEFAULT";
}

inline std::string to_sql_stats_policy(StatsPolicy value) {
    switch (value) {
        case StatsPolicy::Default:
            return "DEFAULT";
        case StatsPolicy::Zero:
            return "0";
        case StatsPolicy::One:
            return "1";
    }
    return "DEFAULT";
}

inline std::string to_sql_compression(Compression value) {
    switch (value) {
        case Compression::None:
            return "NONE";
        case Compression::Zlib:
            return "ZLIB";
        case Compression::Lz4:
            return "LZ4";
    }
    return "NONE";
}

inline std::string to_sql_encryption(Encryption value) {
    switch (value) {
        case Encryption::Y:
            return "Y";
        case Encryption::N:
            return "N";
    }
    return "N";
}

namespace table_option_sql {

inline constexpr std::string_view engine_key = "ENGINE";
inline constexpr std::string_view engine_prefix = "ENGINE=";

inline constexpr std::string_view auto_increment_key = "AUTO_INCREMENT";
inline constexpr std::string_view auto_increment_prefix = "AUTO_INCREMENT=";

inline constexpr std::string_view avg_row_length_key = "AVG_ROW_LENGTH";
inline constexpr std::string_view avg_row_length_prefix = "AVG_ROW_LENGTH=";

inline constexpr std::string_view default_charset_key = "DEFAULT CHARSET";
inline constexpr std::string_view default_charset_prefix = "DEFAULT CHARSET=";

inline constexpr std::string_view collate_key = "COLLATE";
inline constexpr std::string_view collate_prefix = "COLLATE=";

inline constexpr std::string_view checksum_key = "CHECKSUM";
inline constexpr std::string_view checksum_prefix = "CHECKSUM=";

inline constexpr std::string_view comment_key = "COMMENT";
inline constexpr std::string_view comment_prefix = "COMMENT=";

inline constexpr std::string_view compression_key = "COMPRESSION";
inline constexpr std::string_view compression_prefix = "COMPRESSION=";

inline constexpr std::string_view connection_key = "CONNECTION";
inline constexpr std::string_view connection_prefix = "CONNECTION=";

inline constexpr std::string_view data_directory_key = "DATA DIRECTORY";
inline constexpr std::string_view data_directory_prefix = "DATA DIRECTORY=";

inline constexpr std::string_view index_directory_key = "INDEX DIRECTORY";
inline constexpr std::string_view index_directory_prefix = "INDEX DIRECTORY=";

inline constexpr std::string_view delay_key_write_key = "DELAY_KEY_WRITE";
inline constexpr std::string_view delay_key_write_prefix = "DELAY_KEY_WRITE=";

inline constexpr std::string_view encryption_key = "ENCRYPTION";
inline constexpr std::string_view encryption_prefix = "ENCRYPTION=";

inline constexpr std::string_view insert_method_key = "INSERT_METHOD";
inline constexpr std::string_view insert_method_prefix = "INSERT_METHOD=";

inline constexpr std::string_view key_block_size_key = "KEY_BLOCK_SIZE";
inline constexpr std::string_view key_block_size_prefix = "KEY_BLOCK_SIZE=";

inline constexpr std::string_view max_rows_key = "MAX_ROWS";
inline constexpr std::string_view max_rows_prefix = "MAX_ROWS=";

inline constexpr std::string_view min_rows_key = "MIN_ROWS";
inline constexpr std::string_view min_rows_prefix = "MIN_ROWS=";

inline constexpr std::string_view pack_keys_key = "PACK_KEYS";
inline constexpr std::string_view pack_keys_prefix = "PACK_KEYS=";

inline constexpr std::string_view password_key = "PASSWORD";
inline constexpr std::string_view password_prefix = "PASSWORD=";

inline constexpr std::string_view row_format_key = "ROW_FORMAT";
inline constexpr std::string_view row_format_prefix = "ROW_FORMAT=";

inline constexpr std::string_view stats_auto_recalc_key = "STATS_AUTO_RECALC";
inline constexpr std::string_view stats_auto_recalc_prefix = "STATS_AUTO_RECALC=";

inline constexpr std::string_view stats_persistent_key = "STATS_PERSISTENT";
inline constexpr std::string_view stats_persistent_prefix = "STATS_PERSISTENT=";

inline constexpr std::string_view stats_sample_pages_key = "STATS_SAMPLE_PAGES";
inline constexpr std::string_view stats_sample_pages_prefix = "STATS_SAMPLE_PAGES=";

inline constexpr std::string_view tablespace_key = "TABLESPACE";
inline constexpr std::string_view tablespace_prefix = "TABLESPACE=";

inline constexpr std::string_view union_key = "UNION";

[[nodiscard]] inline std::string assign(std::string_view prefix, std::string_view value) {
    std::string out;
    out.reserve(prefix.size() + value.size());
    out += prefix;
    out += value;
    return out;
}

[[nodiscard]] inline std::string assign_quoted(std::string_view prefix, std::string_view value) {
    std::string out;
    out.reserve(prefix.size() + value.size() + 2);
    out += prefix;
    out += quote_sql_string(value);
    return out;
}

[[nodiscard]] inline std::string assign_numeric(std::string_view prefix, std::size_t value) {
    return std::format("{}{}", prefix, value);
}

[[nodiscard]] inline std::string assign_bool(std::string_view prefix, bool enabled) {
    std::string out;
    out.reserve(prefix.size() + 1);
    out += prefix;
    out += enabled ? "1" : "0";
    return out;
}

[[nodiscard]] inline std::string union_tables_value(std::vector<std::string> const& table_names) {
    std::string sql = "UNION=(";
    for (std::size_t i = 0; i < table_names.size(); ++i) {
        if (i > 0) {
            sql += ",";
        }
        sql += table_names[i];
    }
    sql += ")";
    return sql;
}

}  // namespace table_option_sql

struct create_table_option {
    void set(std::string key, std::string value_sql) {
        auto it = std::find_if(options.begin(), options.end(), [&](auto const& kv) {
            return kv.first == key;
        });
        if (it != options.end()) {
            it->second = std::move(value_sql);
        } else {
            options.emplace_back(std::move(key), std::move(value_sql));
        }
    }

    [[nodiscard]] std::string to_sql() const {
        std::string out;
        for (auto const& [_, value_sql] : options) {
            out += " ";
            out += value_sql;
        }
        return out;
    }

    // Fluent API for table attributes
    create_table_option& engine(Engine value) {
        return engine(to_sql_engine(value));
    }

    create_table_option& engine(std::string_view value) {
        set(std::string{table_option_sql::engine_key},
            table_option_sql::assign(table_option_sql::engine_prefix, value));
        return *this;
    }

    create_table_option& auto_increment(std::size_t value) {
        set(std::string{table_option_sql::auto_increment_key},
            table_option_sql::assign_numeric(table_option_sql::auto_increment_prefix, value));
        return *this;
    }

    create_table_option& avg_row_length(std::size_t value) {
        set(std::string{table_option_sql::avg_row_length_key},
            table_option_sql::assign_numeric(table_option_sql::avg_row_length_prefix, value));
        return *this;
    }

    create_table_option& default_charset(Charset value) {
        return default_charset(to_sql_charset(value));
    }

    create_table_option& default_charset(std::string_view value) {
        set(std::string{table_option_sql::default_charset_key},
            table_option_sql::assign(table_option_sql::default_charset_prefix, value));
        return *this;
    }

    create_table_option& collate(std::string_view value) {
        set(std::string{table_option_sql::collate_key},
            table_option_sql::assign(table_option_sql::collate_prefix, value));
        return *this;
    }

    create_table_option& checksum(bool enabled) {
        set(std::string{table_option_sql::checksum_key},
            table_option_sql::assign_bool(table_option_sql::checksum_prefix, enabled));
        return *this;
    }

    create_table_option& checksum(std::size_t value) {
        set(std::string{table_option_sql::checksum_key},
            table_option_sql::assign_numeric(table_option_sql::checksum_prefix, value));
        return *this;
    }

    create_table_option& comment(std::string_view value) {
        set(std::string{table_option_sql::comment_key},
            table_option_sql::assign_quoted(table_option_sql::comment_prefix, value));
        return *this;
    }

    create_table_option& compression(Compression value) {
        return compression(to_sql_compression(value));
    }

    create_table_option& compression(std::string_view value) {
        set(std::string{table_option_sql::compression_key},
            table_option_sql::assign_quoted(table_option_sql::compression_prefix, value));
        return *this;
    }

    create_table_option& connection(std::string_view value) {
        set(std::string{table_option_sql::connection_key},
            table_option_sql::assign_quoted(table_option_sql::connection_prefix, value));
        return *this;
    }

    create_table_option& data_directory(std::string_view value) {
        set(std::string{table_option_sql::data_directory_key},
            table_option_sql::assign_quoted(table_option_sql::data_directory_prefix, value));
        return *this;
    }

    create_table_option& index_directory(std::string_view value) {
        set(std::string{table_option_sql::index_directory_key},
            table_option_sql::assign_quoted(table_option_sql::index_directory_prefix, value));
        return *this;
    }

    create_table_option& delay_key_write(bool enabled) {
        set(std::string{table_option_sql::delay_key_write_key},
            table_option_sql::assign_bool(table_option_sql::delay_key_write_prefix, enabled));
        return *this;
    }

    create_table_option& delay_key_write(std::size_t value) {
        set(std::string{table_option_sql::delay_key_write_key},
            table_option_sql::assign_numeric(table_option_sql::delay_key_write_prefix, value));
        return *this;
    }

    create_table_option& encryption(Encryption value) {
        return encryption(to_sql_encryption(value));
    }

    create_table_option& encryption(std::string_view value) {
        set(std::string{table_option_sql::encryption_key},
            table_option_sql::assign_quoted(table_option_sql::encryption_prefix, value));
        return *this;
    }

    create_table_option& insert_method(InsertMethod value) {
        return insert_method(to_sql_insert_method(value));
    }

    create_table_option& insert_method(std::string_view value) {
        set(std::string{table_option_sql::insert_method_key},
            table_option_sql::assign(table_option_sql::insert_method_prefix, value));
        return *this;
    }

    create_table_option& key_block_size(std::size_t value) {
        set(std::string{table_option_sql::key_block_size_key},
            table_option_sql::assign_numeric(table_option_sql::key_block_size_prefix, value));
        return *this;
    }

    create_table_option& max_rows(std::size_t value) {
        set(std::string{table_option_sql::max_rows_key},
            table_option_sql::assign_numeric(table_option_sql::max_rows_prefix, value));
        return *this;
    }

    create_table_option& min_rows(std::size_t value) {
        set(std::string{table_option_sql::min_rows_key},
            table_option_sql::assign_numeric(table_option_sql::min_rows_prefix, value));
        return *this;
    }

    create_table_option& pack_keys(PackKeys value) {
        return pack_keys(to_sql_pack_keys(value));
    }

    create_table_option& pack_keys(std::string_view value) {
        set(std::string{table_option_sql::pack_keys_key},
            table_option_sql::assign(table_option_sql::pack_keys_prefix, value));
        return *this;
    }

    create_table_option& password(std::string_view value) {
        set(std::string{table_option_sql::password_key},
            table_option_sql::assign_quoted(table_option_sql::password_prefix, value));
        return *this;
    }

    create_table_option& row_format(RowFormat value) {
        return row_format(to_sql_row_format(value));
    }

    create_table_option& row_format(std::string_view value) {
        set(std::string{table_option_sql::row_format_key},
            table_option_sql::assign(table_option_sql::row_format_prefix, value));
        return *this;
    }

    create_table_option& stats_auto_recalc(StatsPolicy value) {
        return stats_auto_recalc(to_sql_stats_policy(value));
    }

    create_table_option& stats_auto_recalc(std::string_view value) {
        set(std::string{table_option_sql::stats_auto_recalc_key},
            table_option_sql::assign(table_option_sql::stats_auto_recalc_prefix, value));
        return *this;
    }

    create_table_option& stats_persistent(StatsPolicy value) {
        return stats_persistent(to_sql_stats_policy(value));
    }

    create_table_option& stats_persistent(std::string_view value) {
        set(std::string{table_option_sql::stats_persistent_key},
            table_option_sql::assign(table_option_sql::stats_persistent_prefix, value));
        return *this;
    }

    create_table_option& stats_sample_pages(std::size_t value) {
        set(std::string{table_option_sql::stats_sample_pages_key},
            table_option_sql::assign_numeric(table_option_sql::stats_sample_pages_prefix, value));
        return *this;
    }

    create_table_option& tablespace(std::string_view value) {
        set(std::string{table_option_sql::tablespace_key},
            table_option_sql::assign(table_option_sql::tablespace_prefix, value));
        return *this;
    }

    create_table_option& union_tables(std::vector<std::string> table_names) {
        set(std::string{table_option_sql::union_key}, table_option_sql::union_tables_value(table_names));
        return *this;
    }

    std::vector<std::pair<std::string, std::string>> options;
};

template <typename Derived>
class create_table_attributes_mixin {
public:
    Derived& engine(Engine value) {
        return engine(to_sql_engine(value));
    }

    Derived& engine(std::string_view value) {
        return set(std::string{table_option_sql::engine_key},
                   table_option_sql::assign(table_option_sql::engine_prefix, value));
    }

    Derived& auto_increment(std::size_t value) {
        return set(std::string{table_option_sql::auto_increment_key},
                   table_option_sql::assign_numeric(table_option_sql::auto_increment_prefix, value));
    }

    Derived& avg_row_length(std::size_t value) {
        return set(std::string{table_option_sql::avg_row_length_key},
                   table_option_sql::assign_numeric(table_option_sql::avg_row_length_prefix, value));
    }

    Derived& default_charset(Charset value) {
        return default_charset(to_sql_charset(value));
    }

    Derived& default_charset(std::string_view value) {
        return set(std::string{table_option_sql::default_charset_key},
                   table_option_sql::assign(table_option_sql::default_charset_prefix, value));
    }

    Derived& collate(std::string_view value) {
        return set(std::string{table_option_sql::collate_key},
                   table_option_sql::assign(table_option_sql::collate_prefix, value));
    }

    Derived& checksum(bool enabled) {
        return set(std::string{table_option_sql::checksum_key},
                   table_option_sql::assign_bool(table_option_sql::checksum_prefix, enabled));
    }

    Derived& checksum(std::size_t value) {
        return set(std::string{table_option_sql::checksum_key},
                   table_option_sql::assign_numeric(table_option_sql::checksum_prefix, value));
    }

    Derived& comment(std::string_view value) {
        return set(std::string{table_option_sql::comment_key},
                   table_option_sql::assign_quoted(table_option_sql::comment_prefix, value));
    }

    Derived& compression(Compression value) {
        return compression(to_sql_compression(value));
    }

    Derived& compression(std::string_view value) {
        return set(std::string{table_option_sql::compression_key},
                   table_option_sql::assign_quoted(table_option_sql::compression_prefix, value));
    }

    Derived& connection(std::string_view value) {
        return set(std::string{table_option_sql::connection_key},
                   table_option_sql::assign_quoted(table_option_sql::connection_prefix, value));
    }

    Derived& data_directory(std::string_view value) {
        return set(std::string{table_option_sql::data_directory_key},
                   table_option_sql::assign_quoted(table_option_sql::data_directory_prefix, value));
    }

    Derived& index_directory(std::string_view value) {
        return set(std::string{table_option_sql::index_directory_key},
                   table_option_sql::assign_quoted(table_option_sql::index_directory_prefix, value));
    }

    Derived& delay_key_write(bool enabled) {
        return set(std::string{table_option_sql::delay_key_write_key},
                   table_option_sql::assign_bool(table_option_sql::delay_key_write_prefix, enabled));
    }

    Derived& delay_key_write(std::size_t value) {
        return set(std::string{table_option_sql::delay_key_write_key},
                   table_option_sql::assign_numeric(table_option_sql::delay_key_write_prefix, value));
    }

    Derived& encryption(Encryption value) {
        return encryption(to_sql_encryption(value));
    }

    Derived& encryption(std::string_view value) {
        return set(std::string{table_option_sql::encryption_key},
                   table_option_sql::assign_quoted(table_option_sql::encryption_prefix, value));
    }

    Derived& insert_method(InsertMethod value) {
        return insert_method(to_sql_insert_method(value));
    }

    Derived& insert_method(std::string_view value) {
        return set(std::string{table_option_sql::insert_method_key},
                   table_option_sql::assign(table_option_sql::insert_method_prefix, value));
    }

    Derived& key_block_size(std::size_t value) {
        return set(std::string{table_option_sql::key_block_size_key},
                   table_option_sql::assign_numeric(table_option_sql::key_block_size_prefix, value));
    }

    Derived& max_rows(std::size_t value) {
        return set(std::string{table_option_sql::max_rows_key},
                   table_option_sql::assign_numeric(table_option_sql::max_rows_prefix, value));
    }

    Derived& min_rows(std::size_t value) {
        return set(std::string{table_option_sql::min_rows_key},
                   table_option_sql::assign_numeric(table_option_sql::min_rows_prefix, value));
    }

    Derived& pack_keys(PackKeys value) {
        return pack_keys(to_sql_pack_keys(value));
    }

    Derived& pack_keys(std::string_view value) {
        return set(std::string{table_option_sql::pack_keys_key},
                   table_option_sql::assign(table_option_sql::pack_keys_prefix, value));
    }

    Derived& password(std::string_view value) {
        return set(std::string{table_option_sql::password_key},
                   table_option_sql::assign_quoted(table_option_sql::password_prefix, value));
    }

    Derived& row_format(RowFormat value) {
        return row_format(to_sql_row_format(value));
    }

    Derived& row_format(std::string_view value) {
        return set(std::string{table_option_sql::row_format_key},
                   table_option_sql::assign(table_option_sql::row_format_prefix, value));
    }

    Derived& stats_auto_recalc(StatsPolicy value) {
        return stats_auto_recalc(to_sql_stats_policy(value));
    }

    Derived& stats_auto_recalc(std::string_view value) {
        return set(std::string{table_option_sql::stats_auto_recalc_key},
                   table_option_sql::assign(table_option_sql::stats_auto_recalc_prefix, value));
    }

    Derived& stats_persistent(StatsPolicy value) {
        return stats_persistent(to_sql_stats_policy(value));
    }

    Derived& stats_persistent(std::string_view value) {
        return set(std::string{table_option_sql::stats_persistent_key},
                   table_option_sql::assign(table_option_sql::stats_persistent_prefix, value));
    }

    Derived& stats_sample_pages(std::size_t value) {
        return set(std::string{table_option_sql::stats_sample_pages_key},
                   table_option_sql::assign_numeric(table_option_sql::stats_sample_pages_prefix, value));
    }

    Derived& tablespace(std::string_view value) {
        return set(std::string{table_option_sql::tablespace_key},
                   table_option_sql::assign(table_option_sql::tablespace_prefix, value));
    }

    Derived& union_tables(std::vector<std::string> table_names) {
        return set(std::string{table_option_sql::union_key}, table_option_sql::union_tables_value(table_names));
    }

protected:
    Derived& set(std::string key, std::string value_sql) {
        derived().set_table_option(std::move(key), std::move(value_sql));
        return derived();
    }

private:
    Derived& derived() {
        return static_cast<Derived&>(*this);
    }
};

// Table attributes trait - specialize for each table to define default CREATE TABLE options
template <typename T>
struct table_attributes {
    static create_table_option get() {
        return {};  // Empty by default
    }
};

namespace database_option_sql {

inline constexpr std::string_view default_character_set_key = "DEFAULT CHARACTER SET";
inline constexpr std::string_view default_character_set_prefix = "DEFAULT CHARACTER SET ";

inline constexpr std::string_view default_collate_key = "DEFAULT COLLATE";
inline constexpr std::string_view default_collate_prefix = "DEFAULT COLLATE ";

[[nodiscard]] inline std::string assign(std::string_view prefix, std::string_view value) {
    std::string out;
    out.reserve(prefix.size() + value.size());
    out += prefix;
    out += value;
    return out;
}

}  // namespace database_option_sql

struct create_database_option {
    [[nodiscard]] std::string to_sql() const {
        std::string out;
        for (auto const& [_, value_sql] : options) {
            out += " ";
            out += value_sql;
        }
        return out;
    }

    create_database_option& default_charset(Charset value) {
        return default_charset(to_sql_charset(value));
    }

    create_database_option& default_charset(std::string_view value) {
        set(std::string{database_option_sql::default_character_set_key},
            database_option_sql::assign(database_option_sql::default_character_set_prefix, value));
        return *this;
    }

    create_database_option& default_collate(std::string_view value) {
        set(std::string{database_option_sql::default_collate_key},
            database_option_sql::assign(database_option_sql::default_collate_prefix, value));
        return *this;
    }

    std::vector<std::pair<std::string, std::string>> options;

private:
    void set(std::string key, std::string value_sql) {
        auto it = std::find_if(options.begin(), options.end(), [&](auto const& kv) {
            return kv.first == key;
        });
        if (it != options.end()) {
            it->second = std::move(value_sql);
        } else {
            options.emplace_back(std::move(key), std::move(value_sql));
        }
    }
};

template <typename Derived>
class create_database_attributes_mixin {
public:
    Derived& default_charset(Charset value) {
        return default_charset(to_sql_charset(value));
    }

    Derived& default_charset(std::string_view value) {
        options_.default_charset(value);
        return derived();
    }

    Derived& default_collate(std::string_view value) {
        options_.default_collate(value);
        return derived();
    }

protected:
    explicit create_database_attributes_mixin(create_database_option options = {}) : options_(std::move(options)) {
    }

    create_database_option options_;

private:
    Derived& derived() {
        return static_cast<Derived&>(*this);
    }
};

template <typename T>
struct database_attributes {
    static create_database_option get() {
        return {};  // Empty by default
    }
};

template <typename T, std::size_t... Is>
void append_column_defs(std::string& sql, std::index_sequence<Is...>) {
    std::size_t count = 0;
    (
        [&] {
            using field_type = std::tuple_element_t<Is, decltype(boost::pfr::structure_to_tuple(std::declval<T>()))>;
            if (count > 0) {
                sql += ",\n";
            }
            std::string_view field_name = field_schema<T, Is>::name();
            std::string sql_type_override = field_sql_type_override<T, Is>();
            std::string sql_type = sql_type_override.empty() ? sql_type_for<field_type>() : sql_type_override;
            sql += "    ";
            sql += field_name;
            sql += " ";
            sql += sql_type;
            if constexpr (!is_field_nullable_v<field_type>) {
                sql += " NOT NULL";
            }
            if constexpr (Is == 0) {
                if constexpr (table_inline_primary_key<T>::value) {
                    sql += " PRIMARY KEY AUTO_INCREMENT";
                }
                // When inline PK is disabled, typed column attributes should handle AUTO_INCREMENT
            }
            if constexpr (requires { field_type::ddl_auto_increment; } && field_type::ddl_auto_increment) {
                sql += " ";
                sql += "AUTO_INCREMENT";
            }
            if constexpr (requires { field_type::ddl_unique; } && field_type::ddl_unique) {
                sql += " ";
                sql += "UNIQUE";
            }
            if constexpr (requires { field_type::ddl_default_current_timestamp; } &&
                          field_type::ddl_default_current_timestamp) {
                sql += " ";
                sql += "DEFAULT CURRENT_TIMESTAMP";
            }
            if constexpr (requires { field_type::ddl_collate; } && !field_type::ddl_collate.empty()) {
                sql += " ";
                sql += "COLLATE ";
                sql += field_type::ddl_collate;
            }
            if constexpr (requires { field_type::ddl_on_update_current_timestamp; } &&
                          field_type::ddl_on_update_current_timestamp) {
                sql += " ";
                sql += "ON UPDATE CURRENT_TIMESTAMP";
            }
            if constexpr (requires { field_type::ddl_comment; } && !field_type::ddl_comment.empty()) {
                sql += " ";
                sql += "COMMENT ";
                sql += quote_sql_string(field_type::ddl_comment);
            }
            ++count;
        }(),
        ...);

    // Emit FOREIGN KEY constraints for fields that have them declared.
    (
        [&] {
            using field_type = std::tuple_element_t<Is, decltype(boost::pfr::structure_to_tuple(std::declval<T>()))>;
            if constexpr (requires { field_type::ddl_has_fk; } && field_type::ddl_has_fk) {
                sql += ",\n    FOREIGN KEY (";
                sql += field_schema<T, Is>::name();
                sql += ") REFERENCES ";
                sql += table_name_for<typename field_type::ddl_fk_ref_table>::value().to_string_view();
                sql += "(";
                sql += field_type::ddl_fk_ref_column::column_name();
                sql += ")";
                if constexpr (!field_type::ddl_fk_on_delete.empty()) {
                    sql += " ON DELETE ";
                    sql += field_type::ddl_fk_on_delete;
                }
                if constexpr (!field_type::ddl_fk_on_update.empty()) {
                    sql += " ON UPDATE ";
                    sql += field_type::ddl_fk_on_update;
                }
            }
        }(),
        ...);

    // Emit additional table-level constraints and index definitions.
    for (auto const& constraint_sql : table_constraints<T>::get()) {
        if (!constraint_sql.empty()) {
            sql += ",\n    ";
            sql += constraint_sql;
        }
    }
}

template <typename T>
std::string make_column_defs() {
    std::string sql;
    append_column_defs<T>(sql, std::make_index_sequence<boost::pfr::tuple_size_v<T>>{});
    return sql;
}

template <typename T>
concept BuildsSql = requires(T const& t) {
    { t.build_sql() } -> std::convertible_to<std::string>;
};

template <typename Table>
void append_create_table_sql(std::string& sql, bool if_not_exists) {
    const auto table_name = table_name_for<Table>::value().to_string_view();
    const auto column_defs = make_column_defs<Table>();
    sql += "CREATE TABLE ";
    if (if_not_exists) {
        sql += "IF NOT EXISTS ";
    }
    sql += table_name;
    sql += " (\n";
    sql += column_defs;
    sql += "\n);\n";
}

template <Database DB, bool IfNotExists, std::size_t... Is>
std::string build_create_all_tables_sql_impl(std::string prior_sql, std::index_sequence<Is...>) {
    using tables_tuple = typename database_tables<DB>::type;
    (append_create_table_sql<std::tuple_element_t<Is, tables_tuple>>(prior_sql, IfNotExists), ...);
    return prior_sql;
}

template <Database DB, bool IfNotExists>
std::string build_create_all_tables_sql(std::string prior_sql) {
    using tables_tuple = typename database_tables<DB>::type;
    return build_create_all_tables_sql_impl<DB, IfNotExists>(
        std::move(prior_sql), std::make_index_sequence<std::tuple_size_v<tables_tuple>>{});
}

// Forward declarations
class ddl_continuation;
template <typename T>
class create_table_builder;
template <typename T>
class create_table_cond_builder;
template <typename T>
class create_table_as_builder;
template <typename T, typename SourceT>
class create_table_like_builder;
template <typename T>
class drop_table_builder;
template <typename T>
class drop_table_cond_builder;
template <typename T>
class create_database_builder;
template <typename T>
class create_database_if_not_exists_builder;
template <typename T>
class drop_database_builder;
template <typename T>
class drop_database_if_exists_builder;
class create_database_named_builder;
class create_database_named_if_not_exists_builder;
class drop_database_named_builder;
class drop_database_named_if_exists_builder;
template <Database DB>
class create_all_tables_builder;
template <Database DB>
class create_all_tables_cond_builder;
template <typename T>
class use_builder;  // defined in sql_extension.hpp

// ---------------------------------------------------------------
// ddl_continuation — returned by .then(), allows chaining
// ---------------------------------------------------------------
class ddl_continuation {
public:
    explicit ddl_continuation(std::string sql) : sql_(std::move(sql)) {
    }

    template <typename T>
    [[nodiscard]] create_table_builder<T> create_table() const;

    template <typename T>
    [[nodiscard]] create_table_builder<T> create_temporary_table() const;

    template <typename T>
    [[nodiscard]] drop_table_builder<T> drop_table() const;

    template <typename T>
    [[nodiscard]] drop_table_builder<T> drop_temporary_table() const;

    template <Database T>
    [[nodiscard]] create_database_builder<T> create_database() const;

    [[nodiscard]] create_database_named_builder create_database(database_name const& name) const;

    template <Database T>
    [[nodiscard]] drop_database_builder<T> drop_database() const;

    [[nodiscard]] drop_database_named_builder drop_database(database_name const& name) const;

    // Defined in sql_extension.hpp — include it to enable this chain.
    template <Database T>
    [[nodiscard]] use_builder<T> use() const;

    template <typename T>
    [[nodiscard]] auto create_view() const;

    template <typename T>
    [[nodiscard]] auto drop_view() const;

    template <typename From, typename To>
    [[nodiscard]] auto rename_table() const;

    template <Database DB>
    [[nodiscard]] create_all_tables_builder<DB> create_all_tables() const;

private:
    std::string sql_;
};

template <typename T>
class create_view_as_builder;

template <typename T>
class create_view_builder {
public:
    using ddl_tag_type = void;

    explicit create_view_builder(std::string prior = {}, bool or_replace = false)
        : prior_sql_(std::move(prior)), or_replace_(or_replace) {
    }

    [[nodiscard]] create_view_builder or_replace() const {
        return create_view_builder{prior_sql_, true};
    }

    template <BuildsSql SelectQuery>
    [[nodiscard]] create_view_as_builder<T> as(SelectQuery const& query) const {
        return create_view_as_builder<T>{prior_sql_, or_replace_, query.build_sql()};
    }

private:
    std::string prior_sql_;
    bool or_replace_;
};

template <typename T>
class create_view_as_builder {
public:
    using ddl_tag_type = void;

    create_view_as_builder(std::string prior, bool or_replace, std::string select_sql)
        : prior_sql_(std::move(prior)), or_replace_(or_replace), select_sql_(std::move(select_sql)) {
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto view_name = table_name_for<T>::value().to_string_view();
        std::string s;
        s.reserve(prior_sql_.size() + 20 + view_name.size() + select_sql_.size());
        s += prior_sql_;
        s += "CREATE ";
        if (or_replace_) {
            s += "OR REPLACE ";
        }
        s += "VIEW ";
        s += view_name;
        s += " AS ";
        s += select_sql_;
        s += ";\n";
        return s;
    }

private:
    std::string prior_sql_;
    bool or_replace_;
    std::string select_sql_;
};

template <typename T>
class drop_view_cond_builder {
public:
    using ddl_tag_type = void;

    explicit drop_view_cond_builder(std::string prefix) : prefix_(std::move(prefix)) {
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        return prefix_ + "IF EXISTS " + std::string(table_name_for<T>::value().to_string_view()) + ";\n";
    }

private:
    std::string prefix_;
};

template <typename T>
class drop_view_builder {
public:
    using ddl_tag_type = void;

    explicit drop_view_builder(std::string prior = {}) : prior_sql_(std::move(prior)) {
    }

    [[nodiscard]] drop_view_cond_builder<T> if_exists() const {
        return drop_view_cond_builder<T>{prior_sql_ + "DROP VIEW "};
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        return prior_sql_ + "DROP VIEW " + std::string(table_name_for<T>::value().to_string_view()) + ";\n";
    }

private:
    std::string prior_sql_;
};

template <typename From, typename To>
class rename_table_builder {
public:
    using ddl_tag_type = void;

    explicit rename_table_builder(std::string prior = {}) : prior_sql_(std::move(prior)) {
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        return prior_sql_ + "RENAME TABLE " + std::string(table_name_for<From>::value().to_string_view()) + " TO " +
               std::string(table_name_for<To>::value().to_string_view()) + ";\n";
    }

private:
    std::string prior_sql_;
};

template <typename Table, ColumnDescriptor... Cols>
class create_index_builder {
public:
    using ddl_tag_type = void;

    create_index_builder(std::string name, std::string prior = {}, bool unique = false)
        : prior_sql_(std::move(prior)), name_(std::move(name)), unique_(unique) {
    }

    [[nodiscard]] create_index_builder unique() const {
        return create_index_builder{name_, prior_sql_, true};
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        std::string s;
        s.reserve(prior_sql_.size() + 24 + name_.size() + table_name_for<Table>::value().to_string_view().size() + 4);
        s += prior_sql_;
        s += "CREATE ";
        if (unique_) {
            s += "UNIQUE ";
        }
        s += "INDEX ";
        s += name_;
        s += " ON ";
        s += table_name_for<Table>::value().to_string_view();
        s += " (";
        bool first = true;
        (((s += (first ? "" : ", "), s += column_traits<Cols>::column_name(), first = false)), ...);
        s += ");\n";
        return s;
    }

private:
    std::string prior_sql_;
    std::string name_;
    bool unique_;
};

template <typename Table>
class drop_index_builder {
public:
    using ddl_tag_type = void;

    drop_index_builder(std::string name, std::string prior = {})
        : prior_sql_(std::move(prior)), name_(std::move(name)) {
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        return prior_sql_ + "DROP INDEX " + name_ + " ON " +
               std::string(table_name_for<Table>::value().to_string_view()) + ";\n";
    }

private:
    std::string prior_sql_;
    std::string name_;
};

template <typename Table>
class alter_table_builder {
public:
    using ddl_tag_type = void;

    explicit alter_table_builder(std::string prior = {}, std::vector<std::string> actions = {})
        : prior_sql_(std::move(prior)), actions_(std::move(actions)) {
    }

    template <ColumnDescriptor Col>
    [[nodiscard]] alter_table_builder add_column(std::string sql_type, bool not_null = true) const {
        auto copy = *this;
        std::string action = "ADD COLUMN " + std::string(column_traits<Col>::column_name()) + " " + std::move(sql_type);
        if (not_null) {
            action += " NOT NULL";
        }
        copy.actions_.push_back(std::move(action));
        return copy;
    }

    template <ColumnDescriptor Col>
    [[nodiscard]] alter_table_builder drop_column() const {
        auto copy = *this;
        copy.actions_.push_back("DROP COLUMN " + std::string(column_traits<Col>::column_name()));
        return copy;
    }

    template <ColumnDescriptor Col>
    [[nodiscard]] alter_table_builder rename_column(std::string new_name) const {
        auto copy = *this;
        copy.actions_.push_back("RENAME COLUMN " + std::string(column_traits<Col>::column_name()) + " TO " +
                                std::move(new_name));
        return copy;
    }

    template <ColumnDescriptor Col>
    [[nodiscard]] alter_table_builder modify_column(std::string sql_type, bool not_null = true) const {
        auto copy = *this;
        std::string action =
            "MODIFY COLUMN " + std::string(column_traits<Col>::column_name()) + " " + std::move(sql_type);
        if (not_null) {
            action += " NOT NULL";
        }
        copy.actions_.push_back(std::move(action));
        return copy;
    }

    template <typename NewTable>
    [[nodiscard]] alter_table_builder rename_to() const {
        auto copy = *this;
        copy.actions_.push_back("RENAME TO " + std::string(table_name_for<NewTable>::value().to_string_view()));
        return copy;
    }

    [[nodiscard]] alter_table_builder action(std::string sql_action) const {
        auto copy = *this;
        copy.actions_.push_back(std::move(sql_action));
        return copy;
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        std::string s;
        s.reserve(prior_sql_.size() + 16 + table_name_for<Table>::value().to_string_view().size() + 16);
        s += prior_sql_;
        s += "ALTER TABLE ";
        s += table_name_for<Table>::value().to_string_view();
        s += ' ';
        bool first = true;
        for (auto const& action : actions_) {
            if (!first) {
                s += ", ";
            }
            s += action;
            first = false;
        }
        s += ";\n";
        return s;
    }

private:
    std::string prior_sql_;
    std::vector<std::string> actions_;
};

// ---------------------------------------------------------------
// create_table_cond_builder — after create_table<T>().if_not_exists()
// ---------------------------------------------------------------
template <typename T>
class create_table_cond_builder : public create_table_attributes_mixin<create_table_cond_builder<T>> {
public:
    using ddl_tag_type = void;

    create_table_cond_builder(std::string create_prefix, std::string cols, create_table_option options = {})
        : create_prefix_(std::move(create_prefix)), column_defs_(std::move(cols)), options_(std::move(options)) {
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    template <BuildsSql SelectQuery>
    [[nodiscard]] create_table_as_builder<T> as(SelectQuery const& query) const;

    template <typename SourceT>
    [[nodiscard]] create_table_like_builder<T, SourceT> like() const {
        return create_table_like_builder<T, SourceT>{create_prefix_ + "IF NOT EXISTS "};
    }

    void set_table_option(std::string key, std::string value_sql) {
        options_.set(std::move(key), std::move(value_sql));
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        std::string s;
        s.reserve(create_prefix_.size() + 14 + table_name.size() + 4 + column_defs_.size() + 4);
        s += create_prefix_;
        s += "IF NOT EXISTS ";
        s += table_name;
        s += " (\n";
        s += column_defs_;
        s += "\n)";
        s += options_.to_sql();
        s += ";\n";
        return s;
    }

private:
    std::string create_prefix_;
    std::string column_defs_;
    create_table_option options_;
};

// ---------------------------------------------------------------
// create_table_builder — after create_table<T>() or create_temporary_table<T>()
// ---------------------------------------------------------------
template <typename T>
class create_table_builder : public create_table_attributes_mixin<create_table_builder<T>> {
public:
    using ddl_tag_type = void;

    explicit create_table_builder(std::string prior = {}, bool temporary = false)
        : prior_sql_(std::move(prior)),
          is_temporary_(temporary),
          column_defs_(ddl_detail::make_column_defs<T>()),
          options_(table_attributes<T>::get()) {
    }

    [[nodiscard]] create_table_cond_builder<T> if_not_exists() const {
        return create_table_cond_builder<T>{build_create_prefix(), column_defs_, options_};
    }

    template <BuildsSql SelectQuery>
    [[nodiscard]] create_table_as_builder<T> as(SelectQuery const& query) const {
        return create_table_as_builder<T>{build_create_prefix(), query.build_sql(), false, options_};
    }

    template <typename SourceT>
    [[nodiscard]] create_table_like_builder<T, SourceT> like() const {
        return create_table_like_builder<T, SourceT>{build_create_prefix()};
    }

    void set_table_option(std::string key, std::string value_sql) {
        options_.set(std::move(key), std::move(value_sql));
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        const auto prefix = build_create_prefix();
        std::string s;
        s.reserve(prefix.size() + table_name.size() + 4 + column_defs_.size() + 4);
        s += prefix;
        s += table_name;
        s += " (\n";
        s += column_defs_;
        s += "\n)";
        s += options_.to_sql();
        s += ";\n";
        return s;
    }

private:
    [[nodiscard]] std::string build_create_prefix() const {
        return prior_sql_ + "CREATE " + (is_temporary_ ? "TEMPORARY " : "") + "TABLE ";
    }

    std::string prior_sql_;
    bool is_temporary_;
    std::string column_defs_;
    create_table_option options_;
};

// ---------------------------------------------------------------
// create_table_like_builder — after create_table<NewT>().like<SourceT>() or
//                                    create_table<NewT>().if_not_exists().like<SourceT>()
// ---------------------------------------------------------------
template <typename T, typename SourceT>
class create_table_like_builder {
public:
    using ddl_tag_type = void;

    explicit create_table_like_builder(std::string create_prefix) : create_prefix_(std::move(create_prefix)) {
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto new_table = table_name_for<T>::value().to_string_view();
        const auto src_table = table_name_for<SourceT>::value().to_string_view();
        std::string s;
        s.reserve(create_prefix_.size() + new_table.size() + 7 + src_table.size() + 2);
        s += create_prefix_;
        s += new_table;
        s += " LIKE ";
        s += src_table;
        s += ";\n";
        return s;
    }

private:
    std::string create_prefix_;
};

// ---------------------------------------------------------------
// drop_table_cond_builder — after drop_table<T>().if_exists()
// ---------------------------------------------------------------
template <typename T>
class drop_table_cond_builder {
public:
    using ddl_tag_type = void;

    explicit drop_table_cond_builder(std::string drop_prefix) : drop_prefix_(std::move(drop_prefix)) {
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        std::string s;
        s.reserve(drop_prefix_.size() + 11 + table_name.size() + 2);
        s += drop_prefix_;
        s += "IF EXISTS ";
        s += table_name;
        s += ";\n";
        return s;
    }

private:
    std::string drop_prefix_;
};

// ---------------------------------------------------------------
// drop_table_builder — after drop_table<T>() or drop_temporary_table<T>()
// ---------------------------------------------------------------
template <typename T>
class drop_table_builder {
public:
    using ddl_tag_type = void;

    explicit drop_table_builder(std::string prior = {}, bool temporary = false)
        : prior_sql_(std::move(prior)), is_temporary_(temporary) {
    }

    [[nodiscard]] drop_table_cond_builder<T> if_exists() const {
        return drop_table_cond_builder<T>{build_drop_prefix()};
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        const auto prefix = build_drop_prefix();
        std::string s;
        s.reserve(prefix.size() + table_name.size() + 2);
        s += prefix;
        s += table_name;
        s += ";\n";
        return s;
    }

private:
    [[nodiscard]] std::string build_drop_prefix() const {
        return prior_sql_ + "DROP " + (is_temporary_ ? "TEMPORARY " : "") + "TABLE ";
    }

    std::string prior_sql_;
    bool is_temporary_;
};

// ---------------------------------------------------------------
// create_database_if_not_exists_builder — after create_database<DB>().if_not_exists()
// ---------------------------------------------------------------
template <typename T>
class create_database_if_not_exists_builder
    : public create_database_attributes_mixin<create_database_if_not_exists_builder<T>> {
public:
    using ddl_tag_type = void;

    explicit create_database_if_not_exists_builder(std::string prior = {},
                                                   create_database_option options = database_attributes<T>::get())
        : create_database_attributes_mixin<create_database_if_not_exists_builder<T>>(std::move(options)),
          prior_sql_(std::move(prior)) {
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto db_name = database_name_for<T>::value();
        std::string s;
        s.reserve(prior_sql_.size() + 22 + db_name.size() + this->options_.to_sql().size() + 2);
        s += prior_sql_;
        s += "CREATE DATABASE IF NOT EXISTS ";
        s += db_name;
        s += this->options_.to_sql();
        s += ";\n";
        return s;
    }

private:
    std::string prior_sql_;
};

// ---------------------------------------------------------------
// create_database_builder — after create_database<DB>()
// ---------------------------------------------------------------
template <typename T>
class create_database_builder : public create_database_attributes_mixin<create_database_builder<T>> {
public:
    using ddl_tag_type = void;

    explicit create_database_builder(std::string prior = {},
                                     create_database_option options = database_attributes<T>::get())
        : create_database_attributes_mixin<create_database_builder<T>>(std::move(options)),
          prior_sql_(std::move(prior)) {
    }

    [[nodiscard]] create_database_if_not_exists_builder<T> if_not_exists() const {
        return create_database_if_not_exists_builder<T>{prior_sql_, this->options_};
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto db_name = database_name_for<T>::value();
        std::string s;
        s.reserve(prior_sql_.size() + 16 + db_name.size() + this->options_.to_sql().size() + 2);
        s += prior_sql_;
        s += "CREATE DATABASE ";
        s += db_name;
        s += this->options_.to_sql();
        s += ";\n";
        return s;
    }

private:
    std::string prior_sql_;
};

// ---------------------------------------------------------------
// create_database_named_if_not_exists_builder — runtime database name
// ---------------------------------------------------------------
class create_database_named_if_not_exists_builder
    : public create_database_attributes_mixin<create_database_named_if_not_exists_builder> {
public:
    using ddl_tag_type = void;

    explicit create_database_named_if_not_exists_builder(database_name name, std::string prior = {},
                                                         create_database_option options = {})
        : create_database_attributes_mixin<create_database_named_if_not_exists_builder>(std::move(options)),
          prior_sql_(std::move(prior)),
          name_(std::move(name)) {
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        return prior_sql_ + "CREATE DATABASE IF NOT EXISTS " + name_.to_string() + this->options_.to_sql() + ";\n";
    }

private:
    std::string prior_sql_;
    database_name name_;
};

// ---------------------------------------------------------------
// create_database_named_builder — runtime database name
// ---------------------------------------------------------------
class create_database_named_builder : public create_database_attributes_mixin<create_database_named_builder> {
public:
    using ddl_tag_type = void;

    explicit create_database_named_builder(database_name name, std::string prior = {},
                                           create_database_option options = {})
        : create_database_attributes_mixin<create_database_named_builder>(std::move(options)),
          prior_sql_(std::move(prior)),
          name_(std::move(name)) {
    }

    [[nodiscard]] create_database_named_if_not_exists_builder if_not_exists() const {
        return create_database_named_if_not_exists_builder{name_, prior_sql_, this->options_};
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        return prior_sql_ + "CREATE DATABASE " + name_.to_string() + this->options_.to_sql() + ";\n";
    }

private:
    std::string prior_sql_;
    database_name name_;
};

// ---------------------------------------------------------------
// create_all_tables_cond_builder — after create_all_tables<DB>().if_not_exists()
// ---------------------------------------------------------------
template <Database DB>
class create_all_tables_cond_builder {
public:
    using ddl_tag_type = void;

    explicit create_all_tables_cond_builder(std::string prior = {}) : prior_sql_(std::move(prior)) {
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        return build_create_all_tables_sql<DB, true>(prior_sql_);
    }

private:
    std::string prior_sql_;
};

// ---------------------------------------------------------------
// create_all_tables_builder — emits CREATE TABLE for every table in DB
// ---------------------------------------------------------------
template <Database DB>
class create_all_tables_builder {
public:
    using ddl_tag_type = void;

    explicit create_all_tables_builder(std::string prior = {}) : prior_sql_(std::move(prior)) {
    }

    [[nodiscard]] create_all_tables_cond_builder<DB> if_not_exists() const {
        return create_all_tables_cond_builder<DB>{prior_sql_};
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        return build_create_all_tables_sql<DB, false>(prior_sql_);
    }

private:
    std::string prior_sql_;
};

// ---------------------------------------------------------------
// drop_database_if_exists_builder — after drop_database<DB>().if_exists()
// ---------------------------------------------------------------
template <typename T>
class drop_database_if_exists_builder {
public:
    using ddl_tag_type = void;

    explicit drop_database_if_exists_builder(std::string prior = {}) : prior_sql_(std::move(prior)) {
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto db_name = database_name_for<T>::value();
        std::string s;
        s.reserve(prior_sql_.size() + 21 + db_name.size() + 2);
        s += prior_sql_;
        s += "DROP DATABASE IF EXISTS ";
        s += db_name;
        s += ";\n";
        return s;
    }

private:
    std::string prior_sql_;
};

// ---------------------------------------------------------------
// drop_database_builder — after drop_database<DB>()
// ---------------------------------------------------------------
template <typename T>
class drop_database_builder {
public:
    using ddl_tag_type = void;

    explicit drop_database_builder(std::string prior = {}) : prior_sql_(std::move(prior)) {
    }

    [[nodiscard]] drop_database_if_exists_builder<T> if_exists() const {
        return drop_database_if_exists_builder<T>{prior_sql_};
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto db_name = database_name_for<T>::value();
        std::string s;
        s.reserve(prior_sql_.size() + 14 + db_name.size() + 2);
        s += prior_sql_;
        s += "DROP DATABASE ";
        s += db_name;
        s += ";\n";
        return s;
    }

private:
    std::string prior_sql_;
};

// ---------------------------------------------------------------
// drop_database_named_if_exists_builder — runtime database name
// ---------------------------------------------------------------
class drop_database_named_if_exists_builder {
public:
    using ddl_tag_type = void;

    explicit drop_database_named_if_exists_builder(database_name name, std::string prior = {})
        : prior_sql_(std::move(prior)), name_(std::move(name)) {
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        return prior_sql_ + "DROP DATABASE IF EXISTS " + name_.to_string() + ";\n";
    }

private:
    std::string prior_sql_;
    database_name name_;
};

// ---------------------------------------------------------------
// drop_database_named_builder — runtime database name
// ---------------------------------------------------------------
class drop_database_named_builder {
public:
    using ddl_tag_type = void;

    explicit drop_database_named_builder(database_name name, std::string prior = {})
        : prior_sql_(std::move(prior)), name_(std::move(name)) {
    }

    [[nodiscard]] drop_database_named_if_exists_builder if_exists() const {
        return drop_database_named_if_exists_builder{name_, prior_sql_};
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        return prior_sql_ + "DROP DATABASE " + name_.to_string() + ";\n";
    }

private:
    std::string prior_sql_;
    database_name name_;
};

// Out-of-line definitions for ddl_continuation
template <typename T>
[[nodiscard]] create_table_builder<T> ddl_continuation::create_table() const {
    return create_table_builder<T>{sql_};
}

template <typename T>
[[nodiscard]] create_table_builder<T> ddl_continuation::create_temporary_table() const {
    return create_table_builder<T>{sql_, true};
}

template <typename T>
[[nodiscard]] drop_table_builder<T> ddl_continuation::drop_table() const {
    return drop_table_builder<T>{sql_};
}

template <typename T>
[[nodiscard]] drop_table_builder<T> ddl_continuation::drop_temporary_table() const {
    return drop_table_builder<T>{sql_, true};
}

template <Database T>
[[nodiscard]] create_database_builder<T> ddl_continuation::create_database() const {
    return create_database_builder<T>{sql_};
}

inline create_database_named_builder ddl_continuation::create_database(database_name const& name) const {
    return create_database_named_builder{name, sql_};
}

template <Database T>
[[nodiscard]] drop_database_builder<T> ddl_continuation::drop_database() const {
    return drop_database_builder<T>{sql_};
}

inline drop_database_named_builder ddl_continuation::drop_database(database_name const& name) const {
    return drop_database_named_builder{name, sql_};
}

template <typename T>
auto ddl_continuation::create_view() const {
    return create_view_builder<T>{sql_};
}

template <typename T>
auto ddl_continuation::drop_view() const {
    return drop_view_builder<T>{sql_};
}

template <typename From, typename To>
auto ddl_continuation::rename_table() const {
    return rename_table_builder<From, To>{sql_};
}

template <Database DB>
create_all_tables_builder<DB> ddl_continuation::create_all_tables() const {
    return create_all_tables_builder<DB>{sql_};
}

// ---------------------------------------------------------------
// create_table_as_builder — after create_table<T>().as(query) or
//                           create_table<T>().if_not_exists().as(query)
// ---------------------------------------------------------------
template <typename T>
class create_table_as_builder : public create_table_attributes_mixin<create_table_as_builder<T>> {
public:
    using ddl_tag_type = void;

    create_table_as_builder(std::string create_prefix, std::string select_sql, bool if_not_exists,
                            create_table_option options = {})
        : create_prefix_(std::move(create_prefix)),
          select_sql_(std::move(select_sql)),
          if_not_exists_(if_not_exists),
          options_(std::move(options)) {
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    void set_table_option(std::string key, std::string value_sql) {
        options_.set(std::move(key), std::move(value_sql));
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        std::string_view cond = if_not_exists_ ? std::string_view{"IF NOT EXISTS "} : std::string_view{};
        std::string s;
        s.reserve(create_prefix_.size() + cond.size() + table_name.size() + 5 + select_sql_.size() + 2);
        s += create_prefix_;
        s += cond;
        s += table_name;
        s += options_.to_sql();
        s += " AS ";
        s += select_sql_;
        s += ";\n";
        return s;
    }

private:
    std::string create_prefix_;
    std::string select_sql_;
    bool if_not_exists_;
    create_table_option options_;
};

// Out-of-line .as() definition for create_table_cond_builder (needs create_table_as_builder to be complete)
template <typename T>
template <BuildsSql SelectQuery>
[[nodiscard]] create_table_as_builder<T> create_table_cond_builder<T>::as(SelectQuery const& query) const {
    return create_table_as_builder<T>{create_prefix_, query.build_sql(), true, options_};
}

}  // namespace ddl_detail

template <ValidTable T>
[[nodiscard]] ddl_detail::create_table_builder<T> create_table() {
    return ddl_detail::create_table_builder<T>{};
}

template <ValidTable T>
[[nodiscard]] ddl_detail::create_table_builder<T> create_temporary_table() {
    return ddl_detail::create_table_builder<T>{{}, true};
}

template <ValidTable T>
[[nodiscard]] ddl_detail::drop_table_builder<T> drop_table() {
    return ddl_detail::drop_table_builder<T>{};
}

template <ValidTable T>
[[nodiscard]] ddl_detail::drop_table_builder<T> drop_temporary_table() {
    return ddl_detail::drop_table_builder<T>{{}, true};
}

template <ValidTable T>
[[nodiscard]] ddl_detail::create_view_builder<T> create_view() {
    return ddl_detail::create_view_builder<T>{};
}

template <ValidTable T>
[[nodiscard]] ddl_detail::drop_view_builder<T> drop_view() {
    return ddl_detail::drop_view_builder<T>{};
}

template <ValidTable From, ValidTable To>
[[nodiscard]] ddl_detail::rename_table_builder<From, To> rename_table() {
    return ddl_detail::rename_table_builder<From, To>{};
}

template <ValidTable Table, ColumnDescriptor... Cols>
[[nodiscard]] ddl_detail::create_index_builder<Table, Cols...> create_index_on(std::string name) {
    return ddl_detail::create_index_builder<Table, Cols...>{std::move(name)};
}

template <ValidTable Table>
[[nodiscard]] ddl_detail::drop_index_builder<Table> drop_index_on(std::string name) {
    return ddl_detail::drop_index_builder<Table>{std::move(name)};
}

template <ValidTable Table>
[[nodiscard]] ddl_detail::alter_table_builder<Table> alter_table() {
    return ddl_detail::alter_table_builder<Table>{};
}

/**
 * create_database<DB>() — CREATE DATABASE <db> [IF NOT EXISTS].
 *
 * DB must inherit from database_schema.
 *
 * Example:
 *   struct my_db : ds_mysql::database_schema {
 *       struct symbol { ... };
 *   };
 *
 *   db.execute(create_database<my_db>());
 *   db.execute(create_database<my_db>().if_not_exists());
 *   db.execute(create_database<my_db>().then().create_table<my_db::symbol>());
 */
template <Database T>
[[nodiscard]] ddl_detail::create_database_builder<T> create_database() {
    using _ = typename database_tables<T>::type;
    (void)sizeof(_);
    return ddl_detail::create_database_builder<T>{};
}

[[nodiscard]] inline ddl_detail::create_database_named_builder create_database(database_name const& name) {
    return ddl_detail::create_database_named_builder{name};
}

/**
 * drop_database<DB>() — DROP DATABASE <db> [IF EXISTS].
 *
 * DB must inherit from database_schema.
 */
template <Database T>
[[nodiscard]] ddl_detail::drop_database_builder<T> drop_database() {
    using _ = typename database_tables<T>::type;
    (void)sizeof(_);
    return ddl_detail::drop_database_builder<T>{};
}

[[nodiscard]] inline ddl_detail::drop_database_named_builder drop_database(database_name const& name) {
    return ddl_detail::drop_database_named_builder{name};
}

// ===================================================================
// DML Query Builders
//
// Entry points:
//   describe<T>()
//       → build_sql()                           — DESCRIBE T
//
//   insert_into<T>()
//       .values(row)
//       → build_sql()                           — INSERT INTO T (...) VALUES (...)
//       → .on_duplicate_key_update(col1, ...)   — ... ON DUPLICATE KEY UPDATE ...
//       .values(rows) where rows is std::ranges::input_range<T>
//       → build_sql()                           — INSERT INTO T (...) VALUES (...), (...), ...
//
//   update<T>()
//       .set(col1, col2, ...)
//       → build_sql()                           — UPDATE T SET ...
//       .where(cond) → build_sql()              — UPDATE T SET ... WHERE cond
//
//   delete_from<T>()
//       → build_sql()                           — DELETE FROM T
//       .where(cond) → build_sql()              — DELETE FROM T WHERE cond
//
//   count<T>()
//       → build_sql()                           — SELECT COUNT(*) FROM T
//       .where(cond) → build_sql()              — SELECT COUNT(*) FROM T WHERE cond
//
//   truncate_table<T>()
//       → build_sql()                           — TRUNCATE TABLE T
// ===================================================================

namespace dml_detail {

// FieldOf<Col, T> — true when Col is a ColumnFieldType that is a direct field of T.
template <typename Col, typename T, std::size_t... Is>
consteval bool is_field_of_impl(std::index_sequence<Is...>) {
    return (std::same_as<boost::pfr::tuple_element_t<Is, T>, Col> || ...);
}

template <typename Col, typename T>
concept FieldOf =
    ColumnFieldType<Col> && is_field_of_impl<Col, T>(std::make_index_sequence<boost::pfr::tuple_size_v<T>>{});

template <typename T, std::size_t... Is>
std::string generate_values_impl(T const& row, std::index_sequence<Is...>) {
    std::string values;
    if constexpr (sizeof...(Is) > 0) {
        values.reserve(sizeof...(Is) * 8);
    }
    std::size_t count = 0;
    (
        [&] {
            if (count > 0) {
                values += ", ";
            }
            values += sql_detail::to_sql_value(boost::pfr::get<Is>(row));
            ++count;
        }(),
        ...);
    return values;
}

template <typename T>
std::string generate_values(T const& row) {
    return generate_values_impl(row, std::make_index_sequence<boost::pfr::tuple_size_v<T>>{});
}

template <typename T, std::size_t... Is>
std::string generate_column_list_impl(std::index_sequence<Is...>) {
    constexpr std::size_t field_count = sizeof...(Is);
    const std::size_t names_len = (std::size_t{} + ... + field_schema<T, Is>::name().size());
    std::string columns;
    if constexpr (field_count > 0) {
        columns.reserve(names_len + (field_count - 1) * 2);
    }
    std::size_t count = 0;
    (
        [&] {
            if (count > 0) {
                columns += ", ";
            }
            columns += field_schema<T, Is>::name();
            ++count;
        }(),
        ...);
    return columns;
}

template <typename T>
std::string const& generate_column_list() {
    static const std::string cached =
        generate_column_list_impl<T>(std::make_index_sequence<boost::pfr::tuple_size_v<T>>{});
    return cached;
}

// ---------------------------------------------------------------
// describe_builder
// ---------------------------------------------------------------
template <typename T>
class describe_builder {
public:
    // MySQL DESCRIBE returns: Field, Type, Null, Key, Default (nullable), Extra.
    using result_row_type =
        std::tuple<std::string, std::string, std::string, std::string, std::optional<std::string>, std::string>;

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        std::string s;
        s.reserve(9 + table_name.size());
        s += "DESCRIBE ";
        s += table_name;
        return s;
    }
};

// ---------------------------------------------------------------
// insert_into_values_builder / insert_into_builder
// ---------------------------------------------------------------
template <typename T>
class insert_into_values_builder;

template <typename T>
class insert_into_builder;

template <typename... Cols>
[[nodiscard]] std::string build_assignment_sql_from_tuple(std::tuple<Cols...> const& assignments);

template <typename T, typename... Cols>
class insert_into_upsert_builder {
public:
    insert_into_upsert_builder(insert_into_values_builder<T> values_builder, std::tuple<Cols...> assignments)
        : values_builder_(std::move(values_builder)), assignments_(std::move(assignments)) {
    }

    [[nodiscard]] std::string build_sql() const {
        auto insert_sql = values_builder_.build_sql();
        auto update_clause = build_assignment_sql_from_tuple(assignments_);
        std::string sql;
        sql.reserve(insert_sql.size() + 26 + update_clause.size());
        sql += insert_sql;
        sql += " ON DUPLICATE KEY UPDATE ";
        sql += update_clause;
        return sql;
    }

private:
    insert_into_values_builder<T> values_builder_;
    std::tuple<Cols...> assignments_;
};

template <typename T>
class insert_into_values_builder {
public:
    explicit insert_into_values_builder(T row) : rows_{std::move(row)}, bulk_(false) {
    }

    insert_into_values_builder(std::vector<T> rows, bool bulk) : rows_(std::move(rows)), bulk_(bulk) {
    }

    // on_duplicate_key_update(col1, col2, ...) — appends ON DUPLICATE KEY UPDATE clause.
    // Each argument must be a FieldOf<T> column_field instance carrying the new value.
    template <FieldOf<T>... Cols>
        requires(sizeof...(Cols) > 0)
    [[nodiscard]] insert_into_upsert_builder<T, Cols...> on_duplicate_key_update(Cols const&... assignments) const {
        return insert_into_upsert_builder<T, Cols...>{*this, std::tuple<Cols...>{assignments...}};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        const auto& column_list = dml_detail::generate_column_list<T>();
        std::string s;
        if (bulk_) {
            std::string values;
            values.reserve(rows_.size() * 4);
            bool first = true;
            for (auto const& row : rows_) {
                if (!first) {
                    values += ", ";
                }
                values += '(';
                values += dml_detail::generate_values(row);
                values += ')';
                first = false;
            }
            s.reserve(12 + table_name.size() + 2 + column_list.size() + 9 + values.size());
            s += "INSERT INTO ";
            s += table_name;
            s += " (";
            s += column_list;
            s += ") VALUES ";
            s += values;
        } else {
            auto values = rows_.empty() ? std::string{} : dml_detail::generate_values(rows_.front());
            s.reserve(12 + table_name.size() + 2 + column_list.size() + 10 + values.size() + 1);
            s += "INSERT INTO ";
            s += table_name;
            s += " (";
            s += column_list;
            s += ") VALUES (";
            s += values;
            s += ')';
        }
        return s;
    }

private:
    std::vector<T> rows_;
    bool bulk_ = false;
};

template <typename T>
class insert_into_builder {
public:
    [[nodiscard]] insert_into_values_builder<T> values(T const& row) const {
        return insert_into_values_builder<T>{row};
    }

    template <std::ranges::input_range Rows>
        requires std::same_as<std::remove_cvref_t<std::ranges::range_value_t<Rows>>, T>
    [[nodiscard]] insert_into_values_builder<T> values(Rows&& rows) const {
        std::vector<T> collected_rows;
        for (auto const& row : rows) {
            collected_rows.push_back(row);
        }
        if (collected_rows.empty()) {
            return {std::vector<T>{}, false};
        }
        return {std::move(collected_rows), /*bulk=*/true};
    }
};

// ---------------------------------------------------------------
// update builders
// ---------------------------------------------------------------

template <typename Col>
void append_assignment_sql(std::string& s, bool& first, Col const& field) {
    if (!first) {
        s += ", ";
    }
    using C = std::decay_t<Col>;
    s += column_traits<C>::column_name();
    s += " = ";
    s += sql_detail::to_sql_value(field.value);
    first = false;
}

template <typename... Cols>
[[nodiscard]] std::string build_assignment_sql(Cols const&... assignments) {
    std::string s;
    if constexpr (sizeof...(Cols) > 0) {
        s.reserve(sizeof...(Cols) * 12);
    }
    bool first = true;
    (append_assignment_sql(s, first, assignments), ...);
    return s;
}

template <typename Tuple, std::size_t... Is>
[[nodiscard]] std::string build_assignment_sql_from_tuple_impl(Tuple const& assignments, std::index_sequence<Is...>) {
    return build_assignment_sql(std::get<Is>(assignments)...);
}

template <typename... Cols>
[[nodiscard]] std::string build_assignment_sql_from_tuple(std::tuple<Cols...> const& assignments) {
    return build_assignment_sql_from_tuple_impl(assignments, std::index_sequence_for<Cols...>{});
}

template <typename T, typename... Cols>
class update_set_where_builder {
public:
    update_set_where_builder(std::tuple<Cols...> assignments, where_condition where)
        : assignments_(std::move(assignments)), where_(std::move(where)) {
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        auto set_sql = build_assignment_sql_from_tuple(assignments_);
        auto where_sql = where_.build_sql();
        std::string s;
        s.reserve(7 + table_name.size() + 5 + set_sql.size() + 7 + where_sql.size());
        s += "UPDATE ";
        s += table_name;
        s += " SET ";
        s += set_sql;
        s += " WHERE ";
        s += std::move(where_sql);
        return s;
    }

private:
    std::tuple<Cols...> assignments_;
    where_condition where_;
};

template <typename T, typename... Cols>
class update_set_builder {
public:
    explicit update_set_builder(std::tuple<Cols...> assignments) : assignments_(std::move(assignments)) {
    }

    [[nodiscard]] update_set_where_builder<T, Cols...> where(where_condition condition) const {
        return {assignments_, std::move(condition)};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        auto set_sql = build_assignment_sql_from_tuple(assignments_);
        std::string s;
        s.reserve(7 + table_name.size() + 5 + set_sql.size());
        s += "UPDATE ";
        s += table_name;
        s += " SET ";
        s += set_sql;
        return s;
    }

private:
    std::tuple<Cols...> assignments_;
};

template <typename T>
class update_builder {
public:
    template <FieldOf<T>... Cols>
        requires(sizeof...(Cols) > 0)
    [[nodiscard]] update_set_builder<T, Cols...> set(Cols const&... assignments) const {
        return update_set_builder<T, Cols...>{std::tuple<Cols...>{assignments...}};
    }
};

// ---------------------------------------------------------------
// delete_from builders
// ---------------------------------------------------------------
template <typename T>
class delete_from_where_builder {
public:
    explicit delete_from_where_builder(where_condition where) : where_(std::move(where)) {
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        auto where_sql = where_.build_sql();
        std::string s;
        s.reserve(12 + table_name.size() + 7 + where_sql.size());
        s += "DELETE FROM ";
        s += table_name;
        s += " WHERE ";
        s += std::move(where_sql);
        return s;
    }

private:
    where_condition where_;
};

template <typename T>
class delete_from_builder {
public:
    [[nodiscard]] delete_from_where_builder<T> where(where_condition condition) const {
        return delete_from_where_builder<T>{std::move(condition)};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        std::string s;
        s.reserve(12 + table_name.size());
        s += "DELETE FROM ";
        s += table_name;
        return s;
    }
};

// ---------------------------------------------------------------
// truncate_table_builder
// ---------------------------------------------------------------
template <typename T>
class truncate_table_builder {
public:
    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        std::string s;
        s.reserve(15 + table_name.size());
        s += "TRUNCATE TABLE ";
        s += table_name;
        return s;
    }
};

}  // namespace dml_detail

/**
 * describe<T>() — DESCRIBE <table>.
 */
template <ValidTable T>
[[nodiscard]] dml_detail::describe_builder<T> describe() {
    return {};
}

/**
 * insert_into<T>() — INSERT INTO <table> (...) VALUES (...).
 *
 * Single-row insert:
 *   insert_into<T>().values(row).build_sql()
 *
 * Bulk insert (multiple rows in one statement):
 *   insert_into<T>().values(rows).build_sql()   // rows is std::ranges::input_range<T>
 *
 * Upsert (INSERT … ON DUPLICATE KEY UPDATE):
 *   insert_into<T>().values(row).on_duplicate_key_update(T::col1{v1}, T::col2{v2}, ...)
 *
 * Example:
 *   symbol row;
 *   row.ticker_     = "AAPL";
 *   row.instrument_ = "Stock";
 *   db.execute(insert_into<symbol>().values(row));
 *
 *   // Upsert:
 *   db.execute(insert_into<symbol>().values(row)
 *       .on_duplicate_key_update(symbol::ticker{"AAPL"}, symbol::instrument{"Stock"}));
 *
 *   // Bulk:
 *   db.execute(insert_into<symbol>().values(std::array{row1, row2}).build_sql());
 */
template <ValidTable T>
[[nodiscard]] dml_detail::insert_into_builder<T> insert_into() {
    return {};
}

/**
 * update<T>() — UPDATE <table> SET ... [WHERE ...].
 *
 * Advance with .set(col1, col2, ...) supplying typed column field instances,
 * then optionally .where(cond).
 *
 * Example:
 *   db.execute(update<symbol>().set(symbol::ticker{"MSFT"}).where(equal<symbol::id>(1u)));
 */
template <ValidTable T>
[[nodiscard]] dml_detail::update_builder<T> update() {
    return {};
}

/**
 * delete_from<T>() — DELETE FROM <table> [WHERE ...].
 *
 * Example:
 *   db.execute(delete_from<symbol>().where(equal<symbol::id>(1u)));
 */
template <ValidTable T>
[[nodiscard]] dml_detail::delete_from_builder<T> delete_from() {
    return {};
}

// ===================================================================
// Aggregate projections — used with select<Proj...>().from<Table>()
//
//   count_all      — COUNT(*)       → uint64_t
//   count_col<Col> — COUNT(col)     → uint64_t
//   sum<Col>       — SUM(col)       → double
//   avg<Col>       — AVG(col)       → double
//   min_of<Col>    — MIN(col)       → value_type of Col
//   max_of<Col>    — MAX(col)       → value_type of Col
//
// Aggregate tags also support typed comparison operators via free functions
// (defined after projection_traits), enabling HAVING predicate syntax:
//   sum<Col>() > 100.0       → SUM(col) > 100.000000
//   avg<Col>() >= 4.0        → AVG(col) >= 4.000000
//   min_of<Col>() < 100u     → MIN(col) < 100
// For COUNT(*), prefer the count() factory which returns an agg_expr with
// additional named methods (like .greater_than()).
// ===================================================================

struct count_all {};

template <ColumnDescriptor Col>
struct count_col {};

template <ColumnDescriptor Col>
struct sum {};

template <ColumnDescriptor Col>
struct avg {};

template <ColumnDescriptor Col>
struct min_of {};

template <ColumnDescriptor Col>
struct max_of {};

// count_distinct<Col> — COUNT(DISTINCT col) → uint64_t
template <ColumnDescriptor Col>
struct count_distinct {};

// group_concat<Col> — GROUP_CONCAT(col) → std::string  (MySQL-specific)
template <ColumnDescriptor Col>
struct group_concat {};

// rand_val — RAND() projection for ORDER BY clauses (e.g. order_by<rand_val>())
struct rand_val {};

// ===================================================================
// aliased_projection — wraps any Projection with a SQL alias
//
// Uses a runtime string for the alias name, stored via a static helper.
// For use with select builder's .with_alias<Index>("name") method.
// ===================================================================

// ===================================================================
// coalesce<Col1, Col2> / ifnull<Col1, Col2>
//
//   select<coalesce_of<Col1, Col2>>()  → COALESCE(col1, col2)
//   select<ifnull_of<Col1, Col2>>()    → IFNULL(col1, col2)
// ===================================================================

template <ColumnDescriptor Col1, ColumnDescriptor Col2>
struct coalesce_of {};

template <ColumnDescriptor Col1, ColumnDescriptor Col2>
struct ifnull_of {};

template <typename P>
struct abs_of {};

template <typename P>
struct floor_of {};

template <typename P>
struct ceil_of {};

template <typename P>
struct upper_of {};

template <typename P>
struct lower_of {};

template <typename P>
struct trim_of {};

template <typename P>
struct length_of {};

template <typename P, int Scale>
struct round_to {};

template <typename P, int Scale>
struct format_to {};

template <typename A, typename B>
struct mod_of {};

template <typename A, typename B>
struct power_of {};

template <typename... Ps>
struct concat_of {};

template <typename P, std::size_t Start, std::size_t Length>
struct substring_of {};

template <typename P, fixed_string Format>
struct date_format_of {};

template <sql_date_part Part, typename P>
struct extract_of {};

template <typename P, sql_cast_type Type>
struct cast_as {};

template <typename P, sql_cast_type Type>
struct convert_as {};

// Convenience aliases for shorter SQL function projection names.
template <ColumnDescriptor Col1, ColumnDescriptor Col2>
using coalesce = coalesce_of<Col1, Col2>;

template <ColumnDescriptor Col1, ColumnDescriptor Col2>
using ifnull = ifnull_of<Col1, Col2>;

template <typename P>
using abs = abs_of<P>;

template <typename P>
using floor = floor_of<P>;

template <typename P>
using ceil = ceil_of<P>;

template <typename P>
using upper = upper_of<P>;

template <typename P>
using lower = lower_of<P>;

template <typename P>
using trim = trim_of<P>;

template <typename P>
using length = length_of<P>;

template <typename P, int Scale>
using round = round_to<P, Scale>;

template <typename P, int Scale>
using format = format_to<P, Scale>;

template <typename A, typename B>
using mod = mod_of<A, B>;

template <typename A, typename B>
using power = power_of<A, B>;

template <typename... Ps>
using concat = concat_of<Ps...>;

template <typename P, std::size_t Start, std::size_t Length>
using substring = substring_of<P, Start, Length>;

template <typename P, fixed_string Format>
using date_format = date_format_of<P, Format>;

template <sql_date_part Part, typename P>
using extract = extract_of<Part, P>;

template <typename P, sql_cast_type Type>
using cast = cast_as<P, Type>;

template <typename P, sql_cast_type Type>
using convert = convert_as<P, Type>;

// ===================================================================
// Arithmetic expression projections
//
//   arith_add<A, B>  → (A + B)
//   arith_sub<A, B>  → (A - B)
//   arith_mul<A, B>  → (A * B)
//   arith_div<A, B>  → (A / B)
//
// A and B can be any Projection types. projection_traits specialisations
// are defined after the Projection concept.
// ===================================================================

template <typename A, typename B>
struct arith_add {};

template <typename A, typename B>
struct arith_sub {};

template <typename A, typename B>
struct arith_mul {};

template <typename A, typename B>
struct arith_div {};

// ===================================================================
// case_when_expr — CASE WHEN ... THEN ... [ELSE ...] END expression
//
// Built at runtime via a fluent builder; stored in a type-erased wrapper
// so it can be used with the SELECT builder.
//
//   case_when(cond1, "val1").when(cond2, "val2").else_("default").end()
//   → CASE WHEN cond1 THEN 'val1' WHEN cond2 THEN 'val2' ELSE 'default' END
// ===================================================================

template <SqlValue ValueType>
class case_when_builder {
public:
    case_when_builder(where_condition cond, ValueType then_val) {
        branches_.emplace_back(std::move(cond), std::move(then_val));
    }

    [[nodiscard]] case_when_builder when(where_condition cond, ValueType then_val) && {
        branches_.emplace_back(std::move(cond), std::move(then_val));
        return std::move(*this);
    }

    [[nodiscard]] case_when_builder when(where_condition cond, ValueType then_val) const& {
        auto copy = *this;
        copy.branches_.emplace_back(std::move(cond), std::move(then_val));
        return copy;
    }

    [[nodiscard]] case_when_builder else_(ValueType val) && {
        else_val_ = std::move(val);
        return std::move(*this);
    }

    [[nodiscard]] case_when_builder else_(ValueType val) const& {
        auto copy = *this;
        copy.else_val_ = std::move(val);
        return copy;
    }

    [[nodiscard]] std::string build_sql() const {
        std::string s;
        s += "CASE";
        for (auto const& [cond, val] : branches_) {
            s += " WHEN ";
            s += cond.build_sql();
            s += " THEN ";
            s += sql_detail::to_sql_value(val);
        }
        if (else_val_.has_value()) {
            s += " ELSE ";
            s += sql_detail::to_sql_value(*else_val_);
        }
        s += " END";
        return s;
    }

    using value_type = ValueType;

private:
    std::vector<std::pair<where_condition, ValueType>> branches_;
    std::optional<ValueType> else_val_;
};

template <SqlValue ValueType>
[[nodiscard]] case_when_builder<ValueType> case_when(where_condition cond, ValueType then_val) {
    return case_when_builder<ValueType>{std::move(cond), std::move(then_val)};
}

// case_when_expr<VT> — type-erased wrapper for use as a Projection in select<>.
// Constructed from a case_when_builder via .end().
template <SqlValue ValueType>
class case_when_expr {
public:
    using value_type = ValueType;

    explicit case_when_expr(std::string sql) : sql_(std::move(sql)) {
    }

    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

// ===================================================================
// sort_order — column ordering direction for ORDER BY clauses
// ===================================================================

enum class sort_order { asc, desc };

template <ColumnDescriptor Col>
struct qual {
    using col_type = Col;
    static constexpr bool is_qualified = true;
};

template <typename T>
concept QualifiedCol = requires {
    typename T::col_type;
    { T::is_qualified } -> std::convertible_to<bool>;
    requires T::is_qualified;
} && ColumnDescriptor<typename T::col_type>;

template <typename T>
struct unwrap_order_col {
    using type = T;
};

template <QualifiedCol T>
struct unwrap_order_col<T> {
    using type = typename T::col_type;
};

template <typename T>
using unwrap_order_col_t = typename unwrap_order_col<T>::type;

template <typename T>
constexpr bool is_qualified_col_v = false;

template <QualifiedCol T>
constexpr bool is_qualified_col_v<T> = true;

// nulls_last<Col> / nulls_first<Col> — NULL-placement modifiers for order_by
//
// MySQL has no NULLS LAST / NULLS FIRST syntax. These wrappers are accepted
// by order_by<> and expand to the standard two-term workaround:
//
//   order_by<nulls_last<Col>>()              — (col IS NULL) ASC, col ASC
//   order_by<nulls_last<Col>, sort_order::desc>() — (col IS NULL) ASC, col DESC
//   order_by<nulls_first<Col>>()             — (col IS NULL) DESC, col ASC
//
// NullsWrapper<T> — satisfied by nulls_last<Col> and nulls_first<Col>.

template <typename Col>
struct nulls_last {
    using col_type = Col;
    static constexpr bool puts_nulls_last = true;
};

template <typename Col>
struct nulls_first {
    using col_type = Col;
    static constexpr bool puts_nulls_last = false;
};

template <typename T>
concept NullsWrapper = requires {
    typename T::col_type;
    { T::puts_nulls_last } -> std::convertible_to<bool>;
} && ColumnDescriptor<unwrap_order_col_t<typename T::col_type>>;

// position<Proj> — ORDER BY the 1-based ordinal index of Proj in the SELECT list.
//
//   order_by<position<count_all>>()                  — ORDER BY 2 ASC
//   order_by<position<count_all>, sort_order::desc>() — ORDER BY 2 DESC
//
// PositionWrapper concept is defined after the Projection concept (forward dependency).
template <typename Proj>
struct position {
    using proj_type = Proj;
};

// col_index<N> — ORDER BY the Nth column (1-based) by literal numeric index.
//
//   order_by<col_index<2>>()                   — ORDER BY 2 ASC
//   order_by<col_index<1>, sort_order::desc>() — ORDER BY 1 DESC
//
// Unlike position<Proj>, the index is fixed and does not track SELECT list reorderings.
// ColIndexWrapper concept is defined after the Projection concept (forward dependency).
template <std::size_t N>
struct col_index {
    static constexpr std::size_t value = N;
    using col_index_tag = void;  // marker for ColIndexWrapper
    static_assert(N >= 1, "col_index: column index must be >= 1 (1-based)");
};

// field<Col> — ORDER BY FIELD(col, v1, v2, ...) with runtime value list.
//
// Used with order_by<field<Col>>(values, dir):
//
//   order_by<field<product::type>>({"electronics", "clothing"})
//   order_by<field<product::type>>({"electronics", "clothing"}, sort_order::desc)
template <ColumnDescriptor Col>
struct field {
    using col_type = Col;
    using field_order_tag = void;
};

// ===================================================================
// Window function projections
//
//   window_func<Agg, PartitionCol, OrderCol>
//      → AGG_EXPR OVER (PARTITION BY partition_col ORDER BY order_col)
//
//   row_number_over<PartitionCol, OrderCol>
//      → ROW_NUMBER() OVER (PARTITION BY partition_col ORDER BY order_col)
//
// projection_traits specialisations are defined after the Projection concept.
// ===================================================================

template <typename Agg, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir = sort_order::asc>
struct window_func {};

template <ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir = sort_order::asc>
struct row_number_over {};

template <ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir = sort_order::asc>
struct rank_over {};

template <ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir = sort_order::asc>
struct dense_rank_over {};

template <ColumnDescriptor Col, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, std::size_t Offset = 1,
          sort_order Dir = sort_order::asc>
struct lag_over {};

template <ColumnDescriptor Col, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, std::size_t Offset = 1,
          sort_order Dir = sort_order::asc>
struct lead_over {};

// projection_traits<P> — uniform interface over column descriptors and aggregates.
//
// Specialise this for custom aggregate types to integrate them with the
// SELECT builder:
//
//   template <>
//   struct projection_traits<my_agg> {
//       using value_type = double;
//       static std::string sql_expr() { return "MY_AGG(...)"; }
//   };
template <typename P>
struct projection_traits;  // undefined — must be specialised

template <ColumnDescriptor P>
struct projection_traits<P> {
    using value_type = typename column_traits<P>::value_type;
    static constexpr std::string_view sql_expr() {
        return column_traits<P>::column_name();
    }
};

template <>
struct projection_traits<count_all> {
    using value_type = uint64_t;
    static constexpr std::string_view sql_expr() {
        return "COUNT(*)";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<count_col<Col>> {
    using value_type = uint64_t;
    static std::string sql_expr() {
        return "COUNT(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<sum<Col>> {
    using value_type = double;
    static std::string sql_expr() {
        return "SUM(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<avg<Col>> {
    using value_type = double;
    static std::string sql_expr() {
        return "AVG(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<min_of<Col>> {
    using value_type = typename column_traits<Col>::value_type;
    static std::string sql_expr() {
        return "MIN(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<max_of<Col>> {
    using value_type = typename column_traits<Col>::value_type;
    static std::string sql_expr() {
        return "MAX(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<count_distinct<Col>> {
    using value_type = uint64_t;
    static std::string sql_expr() {
        return "COUNT(DISTINCT " + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<group_concat<Col>> {
    using value_type = std::string;
    static std::string sql_expr() {
        return "GROUP_CONCAT(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <>
struct projection_traits<rand_val> {
    using value_type = double;
    static constexpr std::string_view sql_expr() {
        return "RAND()";
    }
};

template <ColumnDescriptor Col1, ColumnDescriptor Col2>
struct projection_traits<coalesce_of<Col1, Col2>> {
    using value_type = typename column_traits<Col1>::value_type;
    static std::string sql_expr() {
        return "COALESCE(" + std::string(column_traits<Col1>::column_name()) + ", " +
               std::string(column_traits<Col2>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col1, ColumnDescriptor Col2>
struct projection_traits<ifnull_of<Col1, Col2>> {
    using value_type = typename column_traits<Col1>::value_type;
    static std::string sql_expr() {
        return "IFNULL(" + std::string(column_traits<Col1>::column_name()) + ", " +
               std::string(column_traits<Col2>::column_name()) + ")";
    }
};

template <typename T>
struct sql_numeric_projection_value {
    using type = double;
};

template <typename T>
using sql_numeric_projection_value_t = typename sql_numeric_projection_value<T>::type;

template <sql_date_part Part>
[[nodiscard]] constexpr std::string_view sql_date_part_name() {
    if constexpr (Part == sql_date_part::year) {
        return "YEAR";
    } else if constexpr (Part == sql_date_part::quarter) {
        return "QUARTER";
    } else if constexpr (Part == sql_date_part::month) {
        return "MONTH";
    } else if constexpr (Part == sql_date_part::week) {
        return "WEEK";
    } else if constexpr (Part == sql_date_part::day) {
        return "DAY";
    } else if constexpr (Part == sql_date_part::hour) {
        return "HOUR";
    } else if constexpr (Part == sql_date_part::minute) {
        return "MINUTE";
    } else {
        return "SECOND";
    }
}

template <sql_cast_type Type>
struct sql_cast_type_traits;

template <>
struct sql_cast_type_traits<sql_cast_type::signed_integer> {
    using value_type = int64_t;
    static constexpr std::string_view sql_name() {
        return "SIGNED";
    }
};

template <>
struct sql_cast_type_traits<sql_cast_type::unsigned_integer> {
    using value_type = uint64_t;
    static constexpr std::string_view sql_name() {
        return "UNSIGNED";
    }
};

template <>
struct sql_cast_type_traits<sql_cast_type::decimal> {
    using value_type = double;
    static constexpr std::string_view sql_name() {
        return "DECIMAL";
    }
};

template <>
struct sql_cast_type_traits<sql_cast_type::double_precision> {
    using value_type = double;
    static constexpr std::string_view sql_name() {
        return "DOUBLE";
    }
};

template <>
struct sql_cast_type_traits<sql_cast_type::float_precision> {
    using value_type = float;
    static constexpr std::string_view sql_name() {
        return "FLOAT";
    }
};

template <>
struct sql_cast_type_traits<sql_cast_type::char_type> {
    using value_type = std::string;
    static constexpr std::string_view sql_name() {
        return "CHAR";
    }
};

template <>
struct sql_cast_type_traits<sql_cast_type::binary> {
    using value_type = std::string;
    static constexpr std::string_view sql_name() {
        return "BINARY";
    }
};

template <>
struct sql_cast_type_traits<sql_cast_type::date> {
    using value_type = std::string;
    static constexpr std::string_view sql_name() {
        return "DATE";
    }
};

template <>
struct sql_cast_type_traits<sql_cast_type::datetime> {
    using value_type = std::string;
    static constexpr std::string_view sql_name() {
        return "DATETIME";
    }
};

template <>
struct sql_cast_type_traits<sql_cast_type::timestamp> {
    using value_type = std::string;
    static constexpr std::string_view sql_name() {
        return "TIMESTAMP";
    }
};

template <>
struct sql_cast_type_traits<sql_cast_type::time> {
    using value_type = std::string;
    static constexpr std::string_view sql_name() {
        return "TIME";
    }
};

template <>
struct sql_cast_type_traits<sql_cast_type::json> {
    using value_type = std::string;
    static constexpr std::string_view sql_name() {
        return "JSON";
    }
};

// Projection — satisfied by any type that has a projection_traits specialisation.
template <typename P>
concept Projection = requires {
    typename projection_traits<P>::value_type;
    { projection_traits<P>::sql_expr() } -> std::convertible_to<std::string_view>;
};

// AggregateProjection — a Projection that is not a ColumnDescriptor.
template <typename P>
concept AggregateProjection = Projection<P> && !ColumnDescriptor<P>;

// PositionWrapper — satisfied by position<Proj> where Proj is a Projection.
template <typename T>
concept PositionWrapper = requires { typename T::proj_type; } && Projection<typename T::proj_type>;

// ColIndexWrapper — satisfied by col_index<N>.
template <typename T>
concept ColIndexWrapper = requires {
    typename T::col_index_tag;
    { T::value } -> std::same_as<const std::size_t&>;
};

// FieldOrderBy — satisfied by field<Col> where Col is a ColumnDescriptor.
template <typename T>
concept FieldOrderBy = requires {
    typename T::field_order_tag;
    typename T::col_type;
} && ColumnDescriptor<typename T::col_type>;

// ===================================================================
// projection_traits — specialisations for types that depend on Projection
// ===================================================================

template <Projection A, Projection B>
struct projection_traits<arith_add<A, B>> {
    using value_type = double;
    static std::string sql_expr() {
        return "(" + std::string(projection_traits<A>::sql_expr()) + " + " +
               std::string(projection_traits<B>::sql_expr()) + ")";
    }
};

template <Projection A, Projection B>
struct projection_traits<arith_sub<A, B>> {
    using value_type = double;
    static std::string sql_expr() {
        return "(" + std::string(projection_traits<A>::sql_expr()) + " - " +
               std::string(projection_traits<B>::sql_expr()) + ")";
    }
};

template <Projection A, Projection B>
struct projection_traits<arith_mul<A, B>> {
    using value_type = double;
    static std::string sql_expr() {
        return "(" + std::string(projection_traits<A>::sql_expr()) + " * " +
               std::string(projection_traits<B>::sql_expr()) + ")";
    }
};

template <Projection A, Projection B>
struct projection_traits<arith_div<A, B>> {
    using value_type = double;
    static std::string sql_expr() {
        return "(" + std::string(projection_traits<A>::sql_expr()) + " / " +
               std::string(projection_traits<B>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<abs_of<P>> {
    using value_type = sql_numeric_projection_value_t<typename projection_traits<P>::value_type>;
    static std::string sql_expr() {
        return "ABS(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<floor_of<P>> {
    using value_type = sql_numeric_projection_value_t<typename projection_traits<P>::value_type>;
    static std::string sql_expr() {
        return "FLOOR(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<ceil_of<P>> {
    using value_type = sql_numeric_projection_value_t<typename projection_traits<P>::value_type>;
    static std::string sql_expr() {
        return "CEIL(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<upper_of<P>> {
    using value_type = std::string;
    static std::string sql_expr() {
        return "UPPER(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<lower_of<P>> {
    using value_type = std::string;
    static std::string sql_expr() {
        return "LOWER(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<trim_of<P>> {
    using value_type = std::string;
    static std::string sql_expr() {
        return "TRIM(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<length_of<P>> {
    using value_type = uint64_t;
    static std::string sql_expr() {
        return "LENGTH(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P, int Scale>
struct projection_traits<round_to<P, Scale>> {
    using value_type = double;
    static std::string sql_expr() {
        return "ROUND(" + std::string(projection_traits<P>::sql_expr()) + ", " + std::to_string(Scale) + ")";
    }
};

template <Projection P, int Scale>
struct projection_traits<format_to<P, Scale>> {
    using value_type = std::string;
    static std::string sql_expr() {
        return "FORMAT(" + std::string(projection_traits<P>::sql_expr()) + ", " + std::to_string(Scale) + ")";
    }
};

template <Projection A, Projection B>
struct projection_traits<mod_of<A, B>> {
    using value_type = double;
    static std::string sql_expr() {
        return "MOD(" + std::string(projection_traits<A>::sql_expr()) + ", " +
               std::string(projection_traits<B>::sql_expr()) + ")";
    }
};

template <Projection A, Projection B>
struct projection_traits<power_of<A, B>> {
    using value_type = double;
    static std::string sql_expr() {
        return "POWER(" + std::string(projection_traits<A>::sql_expr()) + ", " +
               std::string(projection_traits<B>::sql_expr()) + ")";
    }
};

template <Projection... Ps>
    requires(sizeof...(Ps) > 0)
struct projection_traits<concat_of<Ps...>> {
    using value_type = std::string;
    static std::string sql_expr() {
        std::string s;
        s.reserve(8 + sizeof...(Ps) * 4);
        s += "CONCAT(";
        bool first = true;
        (((s += (first ? "" : ", "), s += projection_traits<Ps>::sql_expr(), first = false)), ...);
        s += ")";
        return s;
    }
};

template <Projection P, std::size_t Start, std::size_t Length>
struct projection_traits<substring_of<P, Start, Length>> {
    using value_type = std::string;
    static std::string sql_expr() {
        return "SUBSTRING(" + std::string(projection_traits<P>::sql_expr()) + ", " + std::to_string(Start) + ", " +
               std::to_string(Length) + ")";
    }
};

template <Projection P, fixed_string Format>
struct projection_traits<date_format_of<P, Format>> {
    using value_type = std::string;
    static std::string sql_expr() {
        return "DATE_FORMAT(" + std::string(projection_traits<P>::sql_expr()) + ", '" +
               sql_detail::escape_sql_string(std::string_view{Format}) + "')";
    }
};

template <sql_date_part Part, Projection P>
struct projection_traits<extract_of<Part, P>> {
    using value_type = uint32_t;
    static std::string sql_expr() {
        return "EXTRACT(" + std::string(sql_date_part_name<Part>()) + " FROM " +
               std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P, sql_cast_type Type>
struct projection_traits<cast_as<P, Type>> {
    using value_type = typename sql_cast_type_traits<Type>::value_type;
    static std::string sql_expr() {
        return "CAST(" + std::string(projection_traits<P>::sql_expr()) + " AS " +
               std::string(sql_cast_type_traits<Type>::sql_name()) + ")";
    }
};

template <Projection P, sql_cast_type Type>
struct projection_traits<convert_as<P, Type>> {
    using value_type = typename sql_cast_type_traits<Type>::value_type;
    static std::string sql_expr() {
        return "CONVERT(" + std::string(projection_traits<P>::sql_expr()) + ", " +
               std::string(sql_cast_type_traits<Type>::sql_name()) + ")";
    }
};

template <Projection Agg, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir>
struct projection_traits<window_func<Agg, PartitionCol, OrderCol, Dir>> {
    using value_type = typename projection_traits<Agg>::value_type;
    static std::string sql_expr() {
        return std::string(projection_traits<Agg>::sql_expr()) + " OVER (PARTITION BY " +
               std::string(column_traits<PartitionCol>::column_name()) + " ORDER BY " +
               std::string(column_traits<OrderCol>::column_name()) + (Dir == sort_order::asc ? " ASC)" : " DESC)");
    }
};

template <ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir>
struct projection_traits<row_number_over<PartitionCol, OrderCol, Dir>> {
    using value_type = uint64_t;
    static std::string sql_expr() {
        return "ROW_NUMBER() OVER (PARTITION BY " + std::string(column_traits<PartitionCol>::column_name()) +
               " ORDER BY " + std::string(column_traits<OrderCol>::column_name()) +
               (Dir == sort_order::asc ? " ASC)" : " DESC)");
    }
};

template <ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir>
struct projection_traits<rank_over<PartitionCol, OrderCol, Dir>> {
    using value_type = uint64_t;
    static std::string sql_expr() {
        return "RANK() OVER (PARTITION BY " + std::string(column_traits<PartitionCol>::column_name()) + " ORDER BY " +
               std::string(column_traits<OrderCol>::column_name()) + (Dir == sort_order::asc ? " ASC)" : " DESC)");
    }
};

template <ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir>
struct projection_traits<dense_rank_over<PartitionCol, OrderCol, Dir>> {
    using value_type = uint64_t;
    static std::string sql_expr() {
        return "DENSE_RANK() OVER (PARTITION BY " + std::string(column_traits<PartitionCol>::column_name()) +
               " ORDER BY " + std::string(column_traits<OrderCol>::column_name()) +
               (Dir == sort_order::asc ? " ASC)" : " DESC)");
    }
};

template <ColumnDescriptor Col, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, std::size_t Offset,
          sort_order Dir>
struct projection_traits<lag_over<Col, PartitionCol, OrderCol, Offset, Dir>> {
    using value_type = typename column_traits<Col>::value_type;
    static std::string sql_expr() {
        return "LAG(" + std::string(column_traits<Col>::column_name()) + ", " + std::to_string(Offset) +
               ") OVER (PARTITION BY " + std::string(column_traits<PartitionCol>::column_name()) + " ORDER BY " +
               std::string(column_traits<OrderCol>::column_name()) + (Dir == sort_order::asc ? " ASC)" : " DESC)");
    }
};

template <ColumnDescriptor Col, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, std::size_t Offset,
          sort_order Dir>
struct projection_traits<lead_over<Col, PartitionCol, OrderCol, Offset, Dir>> {
    using value_type = typename column_traits<Col>::value_type;
    static std::string sql_expr() {
        return "LEAD(" + std::string(column_traits<Col>::column_name()) + ", " + std::to_string(Offset) +
               ") OVER (PARTITION BY " + std::string(column_traits<PartitionCol>::column_name()) + " ORDER BY " +
               std::string(column_traits<OrderCol>::column_name()) + (Dir == sort_order::asc ? " ASC)" : " DESC)");
    }
};

// ===================================================================
// agg_expr<Agg> — natural operator and method syntax for HAVING predicates
//
// Wraps an aggregate projection type and exposes comparison operators and
// named methods, each returning a where_condition suitable for .having().
//
// Operators (mirror the col_expr<Col> API):
//   count() == 0                → COUNT(*) = 0
//   count() != 0                → COUNT(*) != 0
//   count() < 10                → COUNT(*) < 10
//   count() > 5                 → COUNT(*) > 5
//   count() <= 10               → COUNT(*) <= 10
//   count() >= 1                → COUNT(*) >= 1
//
// Named methods:
//   count().equal_to(0)         → COUNT(*) = 0
//   count().not_equal_to(0)     → COUNT(*) != 0
//   count().less_than(10)       → COUNT(*) < 10
//   count().greater_than(5)     → COUNT(*) > 5
//   count().less_than_or_equal(10)    → COUNT(*) <= 10
//   count().greater_than_or_equal(1)  → COUNT(*) >= 1
//
// Factory functions:
//   count()           → agg_expr<count_all>        COUNT(*)
//   count<Col>()      → agg_expr<count_col<Col>>   COUNT(col)
//
// For SUM/AVG/MIN/MAX, use the aggregate tag directly — free-function operators
// (defined after projection_traits) allow the same operator syntax:
//   sum<Col>() > 100.0       — SUM(col) > 100.000000
//   avg<Col>() >= 4.0        — AVG(col) >= 4.000000
//
// The resulting where_condition composes with &, |, ! operators.
// ===================================================================

template <typename Agg>
struct agg_expr {
    using value_type = typename projection_traits<Agg>::value_type;

    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] where_condition operator==(V const& val) const {
        return {{},
                {},
                std::string(projection_traits<Agg>::sql_expr()) + " = " + sql_detail::to_sql_value(value_type{val})};
    }
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] where_condition operator!=(V const& val) const {
        return {{},
                {},
                std::string(projection_traits<Agg>::sql_expr()) + " != " + sql_detail::to_sql_value(value_type{val})};
    }
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] where_condition operator<(V const& val) const {
        return {{},
                {},
                std::string(projection_traits<Agg>::sql_expr()) + " < " + sql_detail::to_sql_value(value_type{val})};
    }
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] where_condition operator>(V const& val) const {
        return {{},
                {},
                std::string(projection_traits<Agg>::sql_expr()) + " > " + sql_detail::to_sql_value(value_type{val})};
    }
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] where_condition operator<=(V const& val) const {
        return {{},
                {},
                std::string(projection_traits<Agg>::sql_expr()) + " <= " + sql_detail::to_sql_value(value_type{val})};
    }
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] where_condition operator>=(V const& val) const {
        return {{},
                {},
                std::string(projection_traits<Agg>::sql_expr()) + " >= " + sql_detail::to_sql_value(value_type{val})};
    }

    // Named method equivalents
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] where_condition equal_to(V const& val) const {
        return *this == val;
    }
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] where_condition not_equal_to(V const& val) const {
        return *this != val;
    }
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] where_condition less_than(V const& val) const {
        return *this < val;
    }
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] where_condition greater_than(V const& val) const {
        return *this > val;
    }
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] where_condition less_than_or_equal(V const& val) const {
        return *this <= val;
    }
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] where_condition greater_than_or_equal(V const& val) const {
        return *this >= val;
    }

    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] where_condition between(V const& low, V const& high) const {
        return {{},
                {},
                std::string(projection_traits<Agg>::sql_expr()) + " BETWEEN " +
                    sql_detail::to_sql_value(value_type{low}) + " AND " + sql_detail::to_sql_value(value_type{high})};
    }

    [[nodiscard]] where_condition in(std::initializer_list<value_type> vals) const {
        std::string s;
        s += std::string(projection_traits<Agg>::sql_expr());
        s += " IN (";
        bool first = true;
        for (auto const& v : vals) {
            if (!first) {
                s += ", ";
            }
            s += sql_detail::to_sql_value(v);
            first = false;
        }
        s += ')';
        return {{}, {}, std::move(s)};
    }

    [[nodiscard]] where_condition is_null() const noexcept {
        return {{}, {}, std::string(projection_traits<Agg>::sql_expr()) + " IS NULL"};
    }

    [[nodiscard]] where_condition is_not_null() const noexcept {
        return {{}, {}, std::string(projection_traits<Agg>::sql_expr()) + " IS NOT NULL"};
    }
};

// count() — COUNT(*) aggregate expression
[[nodiscard]] inline agg_expr<count_all> count() noexcept {
    return {};
}

// count<Col>() — COUNT(col) aggregate expression
// Note: count_col<Col> is the class template tag; count<Col>() is the factory function.
template <ColumnDescriptor Col>
[[nodiscard]] agg_expr<count_col<Col>> count() noexcept {
    return {};
}

// ===================================================================
// Free-function comparison operators for aggregate tags
//
// Allow natural HAVING syntax without requiring factory functions:
//   sum<Col>() > 100.0       → SUM(col) > 100.000000
//   avg<Col>() >= 4.0        → AVG(col) >= 4.000000
//   min_of<Col>() < n        → MIN(col) < n
//   max_of<Col>() > n        → MAX(col) > n
//
// These compose with &, |, ! like any other where_condition.
// ===================================================================

template <AggregateProjection Agg, typename V>
    requires std::constructible_from<typename projection_traits<Agg>::value_type, V const&>
[[nodiscard]] where_condition operator==(Agg const&, V const& val) {
    using vt = typename projection_traits<Agg>::value_type;
    return {{}, {}, std::string(projection_traits<Agg>::sql_expr()) + " = " + sql_detail::to_sql_value(vt{val})};
}

template <AggregateProjection Agg, typename V>
    requires std::constructible_from<typename projection_traits<Agg>::value_type, V const&>
[[nodiscard]] where_condition operator!=(Agg const&, V const& val) {
    using vt = typename projection_traits<Agg>::value_type;
    return {{}, {}, std::string(projection_traits<Agg>::sql_expr()) + " != " + sql_detail::to_sql_value(vt{val})};
}

template <AggregateProjection Agg, typename V>
    requires std::constructible_from<typename projection_traits<Agg>::value_type, V const&>
[[nodiscard]] where_condition operator<(Agg const&, V const& val) {
    using vt = typename projection_traits<Agg>::value_type;
    return {{}, {}, std::string(projection_traits<Agg>::sql_expr()) + " < " + sql_detail::to_sql_value(vt{val})};
}

template <AggregateProjection Agg, typename V>
    requires std::constructible_from<typename projection_traits<Agg>::value_type, V const&>
[[nodiscard]] where_condition operator>(Agg const&, V const& val) {
    using vt = typename projection_traits<Agg>::value_type;
    return {{}, {}, std::string(projection_traits<Agg>::sql_expr()) + " > " + sql_detail::to_sql_value(vt{val})};
}

template <AggregateProjection Agg, typename V>
    requires std::constructible_from<typename projection_traits<Agg>::value_type, V const&>
[[nodiscard]] where_condition operator<=(Agg const&, V const& val) {
    using vt = typename projection_traits<Agg>::value_type;
    return {{}, {}, std::string(projection_traits<Agg>::sql_expr()) + " <= " + sql_detail::to_sql_value(vt{val})};
}

template <AggregateProjection Agg, typename V>
    requires std::constructible_from<typename projection_traits<Agg>::value_type, V const&>
[[nodiscard]] where_condition operator>=(Agg const&, V const& val) {
    using vt = typename projection_traits<Agg>::value_type;
    return {{}, {}, std::string(projection_traits<Agg>::sql_expr()) + " >= " + sql_detail::to_sql_value(vt{val})};
}

// ===================================================================
// SELECT Builder
//
// Entry points:
//   select<Projs...>().from<Table>()
//       .[distinct()]
//       .[where(cond)]
//       .[group_by<Col1, Col2, ...>()]
//       .[having(cond)]
//       .[order_by<Col>()]                   ASC by default
//       .[order_by<Col, sort_order::desc>()]  DESC
//       .[limit(n)]
//       .[offset(n)]
//       .build_sql()
//
//   select<Projs...>().from_joined<Table>()  — no table-membership check, for
//       .[inner_join<RightT, LeftCol, RightCol>()]  JOIN queries spanning
//       .[left_join<RightT, LeftCol, RightCol>()]   multiple tables.
//       .[right_join<RightT, LeftCol, RightCol>()]
//       (all other clauses above also available)
//
//   union_(q1, q2)                           — (q1) UNION (q2)
//   union_all(q1, q2)                        — (q1) UNION ALL (q2)
//
// TypedSelectQuery — any builder stage that can produce typed rows.
// ===================================================================

template <typename T>
concept TypedSelectQuery = requires(T const& t) {
    typename T::result_row_type;
    { t.build_sql() } -> std::convertible_to<std::string>;
};

namespace detail {

enum class group_by_mode {
    standard,
    cube,
    grouping_sets,
};

// Free helpers used by both group_by_spec_traits and select_query_builder.
template <ColumnDescriptor... Cols>
[[nodiscard]] std::string build_group_by_cols_string() {
    constexpr std::size_t col_count = sizeof...(Cols);
    const std::size_t names_len = (std::size_t{} + ... + column_traits<Cols>::column_name().size());
    std::string s;
    if constexpr (col_count > 0) {
        s.reserve(names_len + (col_count - 1) * 2);
    }
    bool first = true;
    (
        [&](std::string_view name) {
            if (!first) {
                s += ", ";
            }
            s += name;
            first = false;
        }(column_traits<Cols>::column_name()),
        ...);
    return s;
}

template <typename Set>
struct grouping_set_sql;

template <typename... Cols>
struct grouping_set_sql<grouping_set<Cols...>> {
    [[nodiscard]] static std::string value() {
        constexpr std::size_t col_count = sizeof...(Cols);
        const std::size_t names_len = (std::size_t{} + ... + column_traits<Cols>::column_name().size());
        std::string s;
        s.reserve(2 + names_len + (col_count > 0 ? (col_count - 1) * 2 : 0));
        s += '(';
        bool first = true;
        (((s += (first ? "" : ", "), s += column_traits<Cols>::column_name(), first = false)), ...);
        s += ')';
        return s;
    }
};

template <>
struct grouping_set_sql<grouping_set<>> {
    [[nodiscard]] static std::string value() {
        return "()";
    }
};

// group_by_spec_traits — associates each GROUP BY wrapper type with the
// information needed by group_by() to set group_by_, group_by_mode_ and
// with_rollup_ on the builder.
template <typename T>
struct group_by_spec_traits;  // undefined → not a GroupBySpec

template <typename... Cols>
struct group_by_spec_traits<rollup<Cols...>> {
    static std::string cols_string() {
        return build_group_by_cols_string<Cols...>();
    }
    static constexpr group_by_mode mode = group_by_mode::standard;
    static constexpr bool with_rollup = true;
};

template <typename... Cols>
struct group_by_spec_traits<cube<Cols...>> {
    static std::string cols_string() {
        return build_group_by_cols_string<Cols...>();
    }
    static constexpr group_by_mode mode = group_by_mode::cube;
    static constexpr bool with_rollup = false;
};

template <typename... Sets>
struct group_by_spec_traits<grouping_sets<Sets...>> {
    static std::string cols_string() {
        std::string s;
        s.reserve(sizeof...(Sets) * 8);
        bool first = true;
        (((s += (first ? "" : ", "), s += grouping_set_sql<Sets>::value(), first = false)), ...);
        return s;
    }
    static constexpr group_by_mode mode = group_by_mode::grouping_sets;
    static constexpr bool with_rollup = false;
};

// ===================================================================
// MySQL value deserialization
// ===================================================================

template <typename T>
T from_mysql_value_nonnull(std::string_view sv) {
    if constexpr (std::same_as<T, uint32_t>) {
        return static_cast<uint32_t>(std::stoul(std::string{sv}));
    } else if constexpr (std::same_as<T, int32_t>) {
        return static_cast<int32_t>(std::stol(std::string{sv}));
    } else if constexpr (std::same_as<T, uint64_t>) {
        return static_cast<uint64_t>(std::stoull(std::string{sv}));
    } else if constexpr (std::same_as<T, int64_t>) {
        return static_cast<int64_t>(std::stoll(std::string{sv}));
    } else if constexpr (std::same_as<T, float>) {
        return std::stof(std::string{sv});
    } else if constexpr (std::same_as<T, double>) {
        return std::stod(std::string{sv});
    } else if constexpr (std::same_as<T, bool>) {
        return sv != "0" && sv != "false" && !sv.empty();
    } else if constexpr (std::same_as<T, std::string>) {
        return std::string{sv};
    } else if constexpr (is_varchar_field_v<T>) {
        return T::create(sv).value_or(T{});
    } else if constexpr (std::same_as<T, std::chrono::system_clock::time_point>) {
        std::istringstream ss{std::string{sv}};
        std::tm tm{};
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        return std::chrono::system_clock::from_time_t(std::mktime(&tm));
    } else {
        static_assert(false,
                      "Unsupported type for MySQL deserialization. "
                      "Supported: uint32_t, int32_t, uint64_t, int64_t, float, double, bool, "
                      "std::string, varchar_field<N>, std::chrono::system_clock::time_point, "
                      "and their std::optional variants.");
    }
}

template <typename T>
T from_mysql_value(std::optional<std::string> const& raw) {
    if constexpr (is_optional_v<T>) {
        if (!raw.has_value()) {
            return std::nullopt;
        }
        using inner = unwrap_optional_t<T>;
        return std::optional<inner>{from_mysql_value_nonnull<inner>(*raw)};
    } else {
        assert(raw.has_value() && "Non-optional field received NULL from database — schema mismatch?");
        return from_mysql_value_nonnull<T>(*raw);
    }
}

template <typename TupleType, std::size_t... Is>
TupleType deserialize_row_impl(std::vector<std::optional<std::string>> const& row, std::index_sequence<Is...>) {
    return TupleType{from_mysql_value<std::tuple_element_t<Is, TupleType>>(row[Is])...};
}

template <typename TupleType>
TupleType deserialize_row(std::vector<std::optional<std::string>> const& row) {
    return deserialize_row_impl<TupleType>(row, std::make_index_sequence<std::tuple_size_v<TupleType>>{});
}

// ===================================================================
// column_belongs_to_table_v — compile-time membership predicate
//
// For tagged_column_field: true iff the tag struct is nested inside Table
// (i.e. tag_is_nested_in_table<T::tag_type, Table>() returns true).
// For bare column_field (no tag_type): always false — type identity alone
// cannot establish table ownership when different tables share the same
// NTTP name and value type.
// For col<T, I> descriptors: true iff T == Table (existing behaviour).
// ===================================================================

template <typename Col, typename Table>
constexpr bool column_belongs_to_table_v = false;

template <typename T, std::size_t I, typename Table>
constexpr bool column_belongs_to_table_v<col<T, I>, Table> = std::is_same_v<T, Table>;

template <typename T, typename Table>
    requires ColumnFieldType<T> && requires { typename T::tag_type; }
constexpr bool column_belongs_to_table_v<T, Table> = tag_is_nested_in_table<typename T::tag_type, Table>();

// ===================================================================
// qualified_col_name<Col> — table-qualified column name for JOINs
//
// For col<T, I> descriptors (which carry table info): "table_name.col_name".
// For ColumnFieldType descriptors (no table info):    bare "col_name".
// ===================================================================

template <typename Col>
std::string qualified_col_name() {
    if constexpr (requires { Col::table_name_str(); }) {
        return std::string(Col::table_name_str()) + "." + std::string(column_traits<Col>::column_name());
    } else {
        return std::string(column_traits<Col>::column_name());
    }
}

// ===================================================================
// proj_index_in_pack<Proj, Projs...> — compile-time index of Proj in pack
//
// Returns the 0-based position of Proj within Projs...
// Used by select_query_builder::with_alias<Proj>() to resolve the alias slot
// at compile time without requiring the caller to know numeric indices.
// ===================================================================

namespace sql_detail {

template <typename Needle, typename... Haystack>
consteval std::size_t proj_index_in_pack() {
    const std::array matches{std::is_same_v<Needle, Haystack>...};
    for (std::size_t i = 0; i < matches.size(); ++i)
        if (matches[i])
            return i;
    throw "proj_index_in_pack: type is not in the projection pack";
}

}  // namespace sql_detail

// GroupBySpec — satisfied by rollup<>, cube<>, grouping_sets<>.
template <typename T>
concept GroupBySpec = requires {
    { detail::group_by_spec_traits<T>::cols_string() } -> std::convertible_to<std::string>;
};

// alias_order_entry — deferred alias lookup for order_by_alias<Proj>()
// Resolved at build_sql() time: emits the alias name if set, otherwise
// falls back to the projection's sql_expr().
struct alias_order_entry {
    std::size_t proj_index;
    std::string fallback_expr;  // sql_expr() of the projection
    sort_order dir;
};

using order_by_item = std::variant<std::string, alias_order_entry>;

// ===================================================================
// select_query_builder — unified builder for all SELECT clauses
// ===================================================================

template <typename Table, typename... Projs>
struct select_query_builder {
    using result_row_type = std::tuple<typename projection_traits<Projs>::value_type...>;

    select_query_builder() = default;

    // ---- Optional clause modifiers ----

    [[nodiscard]] select_query_builder distinct() const& {
        auto copy = *this;
        copy.distinct_ = true;
        return copy;
    }
    [[nodiscard]] select_query_builder distinct() && {
        distinct_ = true;
        return std::move(*this);
    }

    [[nodiscard]] select_query_builder where(where_condition cond) const& {
        auto copy = *this;
        copy.where_ = std::move(cond);
        return copy;
    }
    [[nodiscard]] select_query_builder where(where_condition cond) && {
        where_ = std::move(cond);
        return std::move(*this);
    }

    // group_by<Col1, Col2, ...>()          — GROUP BY col1, col2, ...
    // group_by<rollup<Col1, Col2>>()        — GROUP BY col1, col2 WITH ROLLUP
    // group_by<cube<Col1, Col2>>()          — GROUP BY CUBE(col1, col2)
    // group_by<grouping_sets<Set1, Set2>>() — GROUP BY GROUPING SETS(...)
    template <typename... Specs>
        requires(sizeof...(Specs) > 0 &&
                 ((sizeof...(Specs) == 1 && GroupBySpec<std::tuple_element_t<0, std::tuple<Specs...>>>) ||
                  (ColumnDescriptor<Specs> && ...)))
    [[nodiscard]] select_query_builder group_by() const& {
        auto copy = *this;
        if constexpr (sizeof...(Specs) == 1 && GroupBySpec<std::tuple_element_t<0, std::tuple<Specs...>>>) {
            using Spec = std::tuple_element_t<0, std::tuple<Specs...>>;
            copy.group_by_ = detail::group_by_spec_traits<Spec>::cols_string();
            copy.group_by_mode_ = detail::group_by_spec_traits<Spec>::mode;
            copy.with_rollup_ = detail::group_by_spec_traits<Spec>::with_rollup;
        } else {
            copy.group_by_ = detail::build_group_by_cols_string<Specs...>();
            copy.group_by_mode_ = detail::group_by_mode::standard;
            copy.with_rollup_ = false;
        }
        return copy;
    }
    template <typename... Specs>
        requires(sizeof...(Specs) > 0 &&
                 ((sizeof...(Specs) == 1 && GroupBySpec<std::tuple_element_t<0, std::tuple<Specs...>>>) ||
                  (ColumnDescriptor<Specs> && ...)))
    [[nodiscard]] select_query_builder group_by() && {
        if constexpr (sizeof...(Specs) == 1 && GroupBySpec<std::tuple_element_t<0, std::tuple<Specs...>>>) {
            using Spec = std::tuple_element_t<0, std::tuple<Specs...>>;
            group_by_ = detail::group_by_spec_traits<Spec>::cols_string();
            group_by_mode_ = detail::group_by_spec_traits<Spec>::mode;
            with_rollup_ = detail::group_by_spec_traits<Spec>::with_rollup;
        } else {
            group_by_ = detail::build_group_by_cols_string<Specs...>();
            group_by_mode_ = detail::group_by_mode::standard;
            with_rollup_ = false;
        }
        return std::move(*this);
    }

    [[nodiscard]] select_query_builder having(where_condition cond) const& {
        auto copy = *this;
        copy.having_ = std::move(cond);
        return copy;
    }
    [[nodiscard]] select_query_builder having(where_condition cond) && {
        having_ = std::move(cond);
        return std::move(*this);
    }

    // order_by<Col>()                              — appends "col ASC" to ORDER BY
    // order_by<Col, sort_order::desc>()            — appends "col DESC" to ORDER BY
    // order_by<nulls_last<Col>>()                  — (col IS NULL) ASC, col ASC
    // order_by<nulls_last<Col>, sort_order::desc>() — (col IS NULL) ASC, col DESC
    // order_by<nulls_first<Col>>()                 — (col IS NULL) DESC, col ASC
    // order_by<qual<col<Table, I>>>()              — table.column ASC  (qualified JOIN column)
    // order_by<position<Proj>>()                   — ORDER BY <ordinal of Proj in SELECT> ASC
    // order_by<col_index<N>>()                     — ORDER BY N ASC (literal 1-based index)
    // order_by<Proj>()                             — ORDER BY any projection expression ASC
    // order_by<Proj, sort_order::desc>()           — ORDER BY any projection expression DESC
    // Can be chained multiple times for multi-column ordering.
    template <typename ColSpec, sort_order Dir = sort_order::asc>
        requires(ColumnDescriptor<ColSpec> || NullsWrapper<ColSpec> || QualifiedCol<ColSpec> ||
                 PositionWrapper<ColSpec> || ColIndexWrapper<ColSpec> || Projection<ColSpec>)
    [[nodiscard]] select_query_builder order_by() const& {
        auto copy = *this;
        if constexpr (NullsWrapper<ColSpec>) {
            using ColLike = typename ColSpec::col_type;
            using Col = unwrap_order_col_t<ColLike>;
            const std::string col_name = [] {
                if constexpr (is_qualified_col_v<ColLike>) {
                    return qualified_col_name<Col>();
                } else {
                    return std::string(column_traits<Col>::column_name());
                }
            }();
            copy.order_by_clauses_.push_back("(" + col_name +
                                             (ColSpec::puts_nulls_last ? " IS NULL) ASC" : " IS NULL) DESC"));
            copy.order_by_clauses_.push_back(col_name + (Dir == sort_order::asc ? " ASC" : " DESC"));
        } else if constexpr (QualifiedCol<ColSpec>) {
            copy.order_by_clauses_.push_back(qualified_col_name<typename ColSpec::col_type>() +
                                             (Dir == sort_order::asc ? " ASC" : " DESC"));
        } else if constexpr (PositionWrapper<ColSpec>) {
            static_assert((std::is_same_v<typename ColSpec::proj_type, Projs> || ...),
                          "order_by<position<Proj>>: Proj must be one of the projections in this SELECT");
            static constexpr std::size_t Index =
                sql_detail::proj_index_in_pack<typename ColSpec::proj_type, Projs...>() + 1;
            copy.order_by_clauses_.push_back(std::to_string(Index) + (Dir == sort_order::asc ? " ASC" : " DESC"));
        } else if constexpr (ColIndexWrapper<ColSpec>) {
            static_assert(ColSpec::value <= sizeof...(Projs),
                          "order_by<col_index<N>>: N is out of range for this SELECT list");
            copy.order_by_clauses_.push_back(std::to_string(ColSpec::value) +
                                             (Dir == sort_order::asc ? " ASC" : " DESC"));
        } else if constexpr (Projection<ColSpec>) {
            copy.order_by_clauses_.push_back(std::string(projection_traits<ColSpec>::sql_expr()) +
                                             (Dir == sort_order::asc ? " ASC" : " DESC"));
        } else {
            copy.order_by_clauses_.push_back(std::string(column_traits<ColSpec>::column_name()) +
                                             (Dir == sort_order::asc ? " ASC" : " DESC"));
        }
        return copy;
    }
    template <typename ColSpec, sort_order Dir = sort_order::asc>
        requires(ColumnDescriptor<ColSpec> || NullsWrapper<ColSpec> || QualifiedCol<ColSpec> ||
                 PositionWrapper<ColSpec> || ColIndexWrapper<ColSpec> || Projection<ColSpec>)
    [[nodiscard]] select_query_builder order_by() && {
        if constexpr (NullsWrapper<ColSpec>) {
            using ColLike = typename ColSpec::col_type;
            using Col = unwrap_order_col_t<ColLike>;
            const std::string col_name = [] {
                if constexpr (is_qualified_col_v<ColLike>) {
                    return qualified_col_name<Col>();
                } else {
                    return std::string(column_traits<Col>::column_name());
                }
            }();
            order_by_clauses_.push_back("(" + col_name +
                                        (ColSpec::puts_nulls_last ? " IS NULL) ASC" : " IS NULL) DESC"));
            order_by_clauses_.push_back(col_name + (Dir == sort_order::asc ? " ASC" : " DESC"));
        } else if constexpr (QualifiedCol<ColSpec>) {
            order_by_clauses_.push_back(qualified_col_name<typename ColSpec::col_type>() +
                                        (Dir == sort_order::asc ? " ASC" : " DESC"));
        } else if constexpr (PositionWrapper<ColSpec>) {
            static_assert((std::is_same_v<typename ColSpec::proj_type, Projs> || ...),
                          "order_by<position<Proj>>: Proj must be one of the projections in this SELECT");
            static constexpr std::size_t Index =
                sql_detail::proj_index_in_pack<typename ColSpec::proj_type, Projs...>() + 1;
            order_by_clauses_.push_back(std::to_string(Index) + (Dir == sort_order::asc ? " ASC" : " DESC"));
        } else if constexpr (ColIndexWrapper<ColSpec>) {
            static_assert(ColSpec::value <= sizeof...(Projs),
                          "order_by<col_index<N>>: N is out of range for this SELECT list");
            order_by_clauses_.push_back(std::to_string(ColSpec::value) + (Dir == sort_order::asc ? " ASC" : " DESC"));
        } else if constexpr (Projection<ColSpec>) {
            order_by_clauses_.push_back(std::string(projection_traits<ColSpec>::sql_expr()) +
                                        (Dir == sort_order::asc ? " ASC" : " DESC"));
        } else {
            order_by_clauses_.push_back(std::string(column_traits<ColSpec>::column_name()) +
                                        (Dir == sort_order::asc ? " ASC" : " DESC"));
        }
        return std::move(*this);
    }

    // order_by(case_when_builder, dir) — ORDER BY CASE WHEN ... END
    // Accepts a case_when_builder directly; calls .build_sql() at call time.
    template <typename ValueType>
    [[nodiscard]] select_query_builder order_by(case_when_builder<ValueType> expr,
                                                sort_order dir = sort_order::asc) const& {
        auto copy = *this;
        copy.order_by_clauses_.push_back(expr.build_sql() + (dir == sort_order::asc ? " ASC" : " DESC"));
        return copy;
    }
    template <typename ValueType>
    [[nodiscard]] select_query_builder order_by(case_when_builder<ValueType> expr,
                                                sort_order dir = sort_order::asc) && {
        order_by_clauses_.push_back(expr.build_sql() + (dir == sort_order::asc ? " ASC" : " DESC"));
        return std::move(*this);
    }

    // order_by<field<Col>>(values, dir) — ORDER BY FIELD(col, v1, v2, ...) [ASC|DESC]
    // Emits MySQL FIELD() for custom value-based ordering.
    // Example: .order_by<field<product::type>>({"electronics", "clothing", "food"})
    //          → ORDER BY FIELD(type, 'electronics', 'clothing', 'food') ASC
    template <typename ColSpec, SqlValue ValueType>
        requires FieldOrderBy<ColSpec>
    [[nodiscard]] select_query_builder order_by(std::initializer_list<ValueType> values,
                                                sort_order dir = sort_order::asc) const& {
        auto copy = *this;
        using Col = typename ColSpec::col_type;
        std::string expr = "FIELD(" + std::string(column_traits<Col>::column_name());
        for (auto const& v : values) {
            expr += ", " + ::ds_mysql::sql_detail::to_sql_value(v);
        }
        expr += ")";
        copy.order_by_clauses_.push_back(std::move(expr) + (dir == sort_order::asc ? " ASC" : " DESC"));
        return copy;
    }
    template <typename ColSpec, SqlValue ValueType>
        requires FieldOrderBy<ColSpec>
    [[nodiscard]] select_query_builder order_by(std::initializer_list<ValueType> values,
                                                sort_order dir = sort_order::asc) && {
        using Col = typename ColSpec::col_type;
        std::string expr = "FIELD(" + std::string(column_traits<Col>::column_name());
        for (auto const& v : values) {
            expr += ", " + ::ds_mysql::sql_detail::to_sql_value(v);
        }
        expr += ")";
        order_by_clauses_.push_back(std::move(expr) + (dir == sort_order::asc ? " ASC" : " DESC"));
        return std::move(*this);
    }

    // order_by_alias<Proj>()        — ORDER BY the alias assigned to Proj via with_alias<Proj>()
    // order_by_alias<Proj, desc>()  — ORDER BY alias DESC
    // Falls back to the projection's sql_expr() if no alias has been set.
    template <Projection Proj, sort_order Dir = sort_order::asc>
        requires((std::is_same_v<Proj, Projs> || ...))
    [[nodiscard]] select_query_builder order_by_alias() const& {
        static constexpr std::size_t Index = sql_detail::proj_index_in_pack<Proj, Projs...>();
        auto copy = *this;
        copy.order_by_clauses_.push_back(
            alias_order_entry{Index, std::string(projection_traits<Proj>::sql_expr()), Dir});
        return copy;
    }
    template <Projection Proj, sort_order Dir = sort_order::asc>
        requires((std::is_same_v<Proj, Projs> || ...))
    [[nodiscard]] select_query_builder order_by_alias() && {
        static constexpr std::size_t Index = sql_detail::proj_index_in_pack<Proj, Projs...>();
        order_by_clauses_.push_back(alias_order_entry{Index, std::string(projection_traits<Proj>::sql_expr()), Dir});
        return std::move(*this);
    }

    // with_alias<Proj>("name") — add AS alias to the projection matching type Proj
    //
    // Proj must be one of the types in Projs...; a mismatch is a compile error.
    // Example: .with_alias<sum<product::price_val>>("total")
    template <Projection Proj>
        requires((std::is_same_v<Proj, Projs> || ...))
    [[nodiscard]] select_query_builder with_alias(std::string alias) const& {
        static constexpr std::size_t Index = sql_detail::proj_index_in_pack<Proj, Projs...>();
        auto copy = *this;
        copy.aliases_[Index] = std::move(alias);
        return copy;
    }
    template <Projection Proj>
        requires((std::is_same_v<Proj, Projs> || ...))
    [[nodiscard]] select_query_builder with_alias(std::string alias) && {
        static constexpr std::size_t Index = sql_detail::proj_index_in_pack<Proj, Projs...>();
        aliases_[Index] = std::move(alias);
        return std::move(*this);
    }

    [[nodiscard]] select_query_builder limit(std::size_t n) const& {
        auto copy = *this;
        copy.limit_ = n;
        return copy;
    }
    [[nodiscard]] select_query_builder limit(std::size_t n) && {
        limit_ = n;
        return std::move(*this);
    }

    [[nodiscard]] select_query_builder offset(std::size_t n) const& {
        auto copy = *this;
        copy.offset_ = n;
        return copy;
    }
    [[nodiscard]] select_query_builder offset(std::size_t n) && {
        offset_ = n;
        return std::move(*this);
    }

    // ---- JOIN clauses ----
    //
    // Use col<Table, Index> descriptors for the join columns to produce
    // table-qualified ON conditions (e.g. "symbol.id = price.symbol_id").
    // ColumnFieldType descriptors (e.g. symbol::id) produce bare column names.

    template <typename RightTable, ColumnDescriptor LeftCol, ColumnDescriptor RightCol>
    [[nodiscard]] select_query_builder inner_join() const& {
        return add_join("INNER JOIN", table_name_for<RightTable>::value().to_string_view(),
                        qualified_col_name<LeftCol>(), qualified_col_name<RightCol>());
    }
    template <typename RightTable, ColumnDescriptor LeftCol, ColumnDescriptor RightCol>
    [[nodiscard]] select_query_builder inner_join() && {
        return std::move(*this).add_join("INNER JOIN", table_name_for<RightTable>::value().to_string_view(),
                                         qualified_col_name<LeftCol>(), qualified_col_name<RightCol>());
    }

    template <typename RightTable, ColumnDescriptor LeftCol, ColumnDescriptor RightCol>
    [[nodiscard]] select_query_builder left_join() const& {
        return add_join("LEFT JOIN", table_name_for<RightTable>::value().to_string_view(),
                        qualified_col_name<LeftCol>(), qualified_col_name<RightCol>());
    }
    template <typename RightTable, ColumnDescriptor LeftCol, ColumnDescriptor RightCol>
    [[nodiscard]] select_query_builder left_join() && {
        return std::move(*this).add_join("LEFT JOIN", table_name_for<RightTable>::value().to_string_view(),
                                         qualified_col_name<LeftCol>(), qualified_col_name<RightCol>());
    }

    template <typename RightTable, ColumnDescriptor LeftCol, ColumnDescriptor RightCol>
    [[nodiscard]] select_query_builder right_join() const& {
        return add_join("RIGHT JOIN", table_name_for<RightTable>::value().to_string_view(),
                        qualified_col_name<LeftCol>(), qualified_col_name<RightCol>());
    }
    template <typename RightTable, ColumnDescriptor LeftCol, ColumnDescriptor RightCol>
    [[nodiscard]] select_query_builder right_join() && {
        return std::move(*this).add_join("RIGHT JOIN", table_name_for<RightTable>::value().to_string_view(),
                                         qualified_col_name<LeftCol>(), qualified_col_name<RightCol>());
    }

    // full_join — FULL OUTER JOIN
    template <typename RightTable, ColumnDescriptor LeftCol, ColumnDescriptor RightCol>
    [[nodiscard]] select_query_builder full_join() const& {
        return add_join("FULL OUTER JOIN", table_name_for<RightTable>::value().to_string_view(),
                        qualified_col_name<LeftCol>(), qualified_col_name<RightCol>());
    }
    template <typename RightTable, ColumnDescriptor LeftCol, ColumnDescriptor RightCol>
    [[nodiscard]] select_query_builder full_join() && {
        return std::move(*this).add_join("FULL OUTER JOIN", table_name_for<RightTable>::value().to_string_view(),
                                         qualified_col_name<LeftCol>(), qualified_col_name<RightCol>());
    }

    // cross_join — CROSS JOIN (no ON clause)
    template <typename RightTable>
    [[nodiscard]] select_query_builder cross_join() const& {
        auto copy = *this;
        copy.joins_ += " CROSS JOIN " + std::string(table_name_for<RightTable>::value().to_string_view());
        return copy;
    }
    template <typename RightTable>
    [[nodiscard]] select_query_builder cross_join() && {
        joins_ += " CROSS JOIN " + std::string(table_name_for<RightTable>::value().to_string_view());
        return std::move(*this);
    }

    // join_on — JOIN with arbitrary ON condition (compound ON support)
    template <typename RightTable>
    [[nodiscard]] select_query_builder inner_join_on(where_condition on_cond) const& {
        return add_join_on("INNER JOIN", table_name_for<RightTable>::value().to_string_view(), std::move(on_cond));
    }
    template <typename RightTable>
    [[nodiscard]] select_query_builder inner_join_on(where_condition on_cond) && {
        return std::move(*this).add_join_on("INNER JOIN", table_name_for<RightTable>::value().to_string_view(),
                                            std::move(on_cond));
    }

    template <typename RightTable>
    [[nodiscard]] select_query_builder left_join_on(where_condition on_cond) const& {
        return add_join_on("LEFT JOIN", table_name_for<RightTable>::value().to_string_view(), std::move(on_cond));
    }
    template <typename RightTable>
    [[nodiscard]] select_query_builder left_join_on(where_condition on_cond) && {
        return std::move(*this).add_join_on("LEFT JOIN", table_name_for<RightTable>::value().to_string_view(),
                                            std::move(on_cond));
    }

    template <typename RightTable>
    [[nodiscard]] select_query_builder right_join_on(where_condition on_cond) const& {
        return add_join_on("RIGHT JOIN", table_name_for<RightTable>::value().to_string_view(), std::move(on_cond));
    }
    template <typename RightTable>
    [[nodiscard]] select_query_builder right_join_on(where_condition on_cond) && {
        return std::move(*this).add_join_on("RIGHT JOIN", table_name_for<RightTable>::value().to_string_view(),
                                            std::move(on_cond));
    }

    template <typename RightTable>
    [[nodiscard]] select_query_builder full_join_on(where_condition on_cond) const& {
        return add_join_on("FULL OUTER JOIN", table_name_for<RightTable>::value().to_string_view(), std::move(on_cond));
    }
    template <typename RightTable>
    [[nodiscard]] select_query_builder full_join_on(where_condition on_cond) && {
        return std::move(*this).add_join_on("FULL OUTER JOIN", table_name_for<RightTable>::value().to_string_view(),
                                            std::move(on_cond));
    }

    // ---- Terminal ----

    [[nodiscard]] std::string build_sql() const {
        std::string sql;
        sql.reserve(64 + joins_.size() + group_by_.size() + from_subquery_.size());
        sql += "SELECT ";
        if (distinct_) {
            sql += "DISTINCT ";
        }
        std::size_t proj_idx = 0;
        bool first = true;
        (
            [&](auto expr) {
                if (!first) {
                    sql += ", ";
                }
                sql += expr;
                auto it = aliases_.find(proj_idx);
                if (it != aliases_.end()) {
                    sql += " AS ";
                    sql += it->second;
                }
                first = false;
                ++proj_idx;
            }(projection_traits<Projs>::sql_expr()),
            ...);

        if (!from_subquery_.empty()) {
            sql += " FROM ";
            sql += from_subquery_;
        } else {
            if constexpr (!std::is_same_v<Table, subquery_source>) {
                sql += " FROM ";
                sql += table_name_for<Table>::value().to_string_view();
            }
        }
        sql += joins_;

        if (where_.has_value()) {
            sql += " WHERE ";
            sql += where_->build_sql();
        }
        if (!group_by_.empty()) {
            sql += " GROUP BY ";
            if (group_by_mode_ == group_by_mode::cube) {
                sql += "CUBE(";
                sql += group_by_;
                sql += ")";
            } else if (group_by_mode_ == group_by_mode::grouping_sets) {
                sql += "GROUPING SETS (";
                sql += group_by_;
                sql += ")";
            } else {
                sql += group_by_;
            }
            if (with_rollup_) {
                sql += " WITH ROLLUP";
            }
        }
        if (having_.has_value()) {
            sql += " HAVING ";
            sql += having_->build_sql();
        }
        if (!order_by_clauses_.empty()) {
            sql += " ORDER BY ";
            bool f = true;
            for (auto const& item : order_by_clauses_) {
                if (!f) {
                    sql += ", ";
                }
                if (std::holds_alternative<std::string>(item)) {
                    sql += std::get<std::string>(item);
                } else {
                    auto const& e = std::get<alias_order_entry>(item);
                    auto it = aliases_.find(e.proj_index);
                    sql += (it != aliases_.end() ? it->second : e.fallback_expr);
                    sql += (e.dir == sort_order::asc ? " ASC" : " DESC");
                }
                f = false;
            }
        }
        if (limit_.has_value()) {
            sql += " LIMIT ";
            sql += std::to_string(*limit_);
        }
        if (offset_.has_value()) {
            sql += " OFFSET ";
            sql += std::to_string(*offset_);
        }
        return sql;
    }

    [[nodiscard]] select_query_builder add_join(std::string_view join_type, std::string_view right_table,
                                                std::string left_col, std::string right_col) const& {
        auto copy = *this;
        copy.joins_ += " " + std::string(join_type) + " " + std::string(right_table) + " ON " + std::move(left_col) +
                       " = " + std::move(right_col);
        return copy;
    }
    [[nodiscard]] select_query_builder add_join(std::string_view join_type, std::string_view right_table,
                                                std::string left_col, std::string right_col) && {
        joins_ += " " + std::string(join_type) + " " + std::string(right_table) + " ON " + std::move(left_col) + " = " +
                  std::move(right_col);
        return std::move(*this);
    }

    [[nodiscard]] select_query_builder add_join_on(std::string_view join_type, std::string_view right_table,
                                                   where_condition on_cond) const& {
        auto copy = *this;
        copy.joins_ += " " + std::string(join_type) + " " + std::string(right_table) + " ON " + on_cond.build_sql();
        return copy;
    }
    [[nodiscard]] select_query_builder add_join_on(std::string_view join_type, std::string_view right_table,
                                                   where_condition on_cond) && {
        joins_ += " " + std::string(join_type) + " " + std::string(right_table) + " ON " + on_cond.build_sql();
        return std::move(*this);
    }

    // Private helper: builds a comma-separated GROUP BY column name string.
    template <ColumnDescriptor... GroupCols>
    static std::string build_group_by_string() {
        return detail::build_group_by_cols_string<GroupCols...>();
    }

    template <typename... Sets>
    static std::string build_grouping_sets_string() {
        std::string s;
        s.reserve(sizeof...(Sets) * 8);
        bool first = true;
        (((s += (first ? "" : ", "), s += detail::grouping_set_sql<Sets>::value(), first = false)), ...);
        return s;
    }

    std::optional<where_condition> where_;
    std::string group_by_;
    bool with_rollup_ = false;
    group_by_mode group_by_mode_ = group_by_mode::standard;
    std::optional<where_condition> having_;
    std::vector<order_by_item> order_by_clauses_;
    std::optional<std::size_t> limit_;
    std::optional<std::size_t> offset_;
    bool distinct_ = false;
    std::string joins_;
    std::map<std::size_t, std::string> aliases_;
    std::string from_subquery_;
};

// ===================================================================
// select_builder — Stage 1: select<Projs...>()
// ===================================================================

template <typename... Projs>
class select_builder {
public:
    // Checked: all column projections must belong to Table.
    // For JOIN queries use from<joined<Table>>() to skip the check.
    template <typename TableSpec>
    [[nodiscard]] auto from() const {
        if constexpr (is_joined_v<TableSpec>) {
            return select_query_builder<typename TableSpec::table_type, Projs...>{};
        } else {
            static_assert(ValidTable<TableSpec>);
            static_assert(((AggregateProjection<Projs> || column_belongs_to_table_v<Projs, TableSpec>) && ...),
                          "All column projections must belong to the table passed to from<Table>(). "
                          "For JOIN queries spanning multiple tables, use from<joined<Table>>() instead.");
            return select_query_builder<TableSpec, Projs...>{};
        }
    }

    // from(subquery, "alias") — derived table: SELECT ... FROM (subquery) AS alias
    template <AnySelectQuery Query>
    [[nodiscard]] select_query_builder<subquery_source, Projs...> from(Query const& subquery, std::string alias) const {
        select_query_builder<subquery_source, Projs...> builder;
        auto sub_sql = subquery.build_sql();
        builder.from_subquery_.reserve(2 + sub_sql.size() + 5 + alias.size());
        builder.from_subquery_ += '(';
        builder.from_subquery_ += std::move(sub_sql);
        builder.from_subquery_ += ") AS ";
        builder.from_subquery_ += std::move(alias);
        return builder;
    }
};

// ===================================================================
// union_query — wraps two compatible SELECT queries with UNION / UNION ALL
// ===================================================================

template <typename RowType>
class union_query {
public:
    using result_row_type = RowType;

    explicit union_query(std::string sql) : sql_(std::move(sql)) {
    }

    [[nodiscard]] std::string build_sql() const {
        return sql_;
    }

private:
    std::string sql_;
};

}  // namespace detail

// ===================================================================
// union_ / union_all — combine two TypedSelectQuery results
//
//   union_(q1, q2)    — (q1) UNION (q2)      de-duplicates rows
//   union_all(q1, q2) — (q1) UNION ALL (q2)  preserves duplicates
//
// Both queries must have the same result_row_type.
// ===================================================================

template <TypedSelectQuery Q1, TypedSelectQuery Q2>
    requires std::same_as<typename Q1::result_row_type, typename Q2::result_row_type>
[[nodiscard]] detail::union_query<typename Q1::result_row_type> union_(Q1 const& q1, Q2 const& q2) {
    return detail::union_query<typename Q1::result_row_type>{"(" + q1.build_sql() + ") UNION (" + q2.build_sql() + ")"};
}

template <TypedSelectQuery Q1, TypedSelectQuery Q2>
    requires std::same_as<typename Q1::result_row_type, typename Q2::result_row_type>
[[nodiscard]] detail::union_query<typename Q1::result_row_type> union_all(Q1 const& q1, Q2 const& q2) {
    return detail::union_query<typename Q1::result_row_type>{"(" + q1.build_sql() + ") UNION ALL (" + q2.build_sql() +
                                                             ")"};
}

template <TypedSelectQuery Q1, TypedSelectQuery Q2>
    requires std::same_as<typename Q1::result_row_type, typename Q2::result_row_type>
[[nodiscard]] detail::union_query<typename Q1::result_row_type> intersect_(Q1 const& q1, Q2 const& q2) {
    return detail::union_query<typename Q1::result_row_type>{"(" + q1.build_sql() + ") INTERSECT (" + q2.build_sql() +
                                                             ")"};
}

template <TypedSelectQuery Q1, TypedSelectQuery Q2>
    requires std::same_as<typename Q1::result_row_type, typename Q2::result_row_type>
[[nodiscard]] detail::union_query<typename Q1::result_row_type> except_(Q1 const& q1, Q2 const& q2) {
    return detail::union_query<typename Q1::result_row_type>{"(" + q1.build_sql() + ") EXCEPT (" + q2.build_sql() +
                                                             ")"};
}

/**
 * select<Projs...>() — begin a type-safe SELECT query.
 *
 * Each Proj must satisfy Projection — either a ColumnDescriptor (typed column)
 * or an aggregate such as count_all, sum<Col>, avg<Col>, etc.
 *
 * Next: .from<Table>() (enforces column membership) or
 *       .from_joined<Table>() (skips the check, for multi-table JOINs).
 *
 * Example (single table):
 *   auto rows = db.query_typed(
 *       select<symbol::id, symbol::ticker>()
 *           .from<symbol>()
 *           .where(equal<symbol::ticker>("AAPL"))
 *           .order_by<symbol::id>()
 *           .limit(10));
 *
 * Example (JOIN — use col<T,I> for table-qualified ON columns):
 *   auto rows = db.query_typed(
 *       select<col<symbol, 1>, col<price, 2>>()
 *           .from_joined<symbol>()
 *           .inner_join<price, col<symbol, 0>, col<price, 1>>()
 *           .order_by<col<price, 3>, sort_order::desc>()
 *           .limit(100));
 */
template <Projection... Projs>
[[nodiscard]] detail::select_builder<Projs...> select() {
    return {};
}

/**
 * count<T>() — SELECT COUNT(*) FROM <table> [WHERE ...].
 *
 * Shorthand for select<count_all>().from<T>(). Supports all optional clauses.
 *
 * Example:
 *   auto rows = db.query(count<symbol>());
 *   auto rows = db.query(count<symbol>().where(equal<symbol::ticker>("AAPL")));
 */
template <ValidTable T>
[[nodiscard]] detail::select_query_builder<T, count_all> count() {
    return {};
}

/**
 * truncate_table<T>() — TRUNCATE TABLE <table>.
 *
 * Removes all rows from the table. Faster than DELETE FROM with no WHERE.
 *
 * Example:
 *   db.execute(truncate_table<symbol>());
 */
template <ValidTable T>
[[nodiscard]] dml_detail::truncate_table_builder<T> truncate_table() {
    return {};
}

// ===================================================================
// CTE (Common Table Expressions) — WITH clause
//
//   with_cte("name", subquery).select<Projs...>().from_cte<Table>("name")
//   → WITH name AS (subquery) SELECT ... FROM name
//
//   with_cte("n1", q1).with_cte("n2", q2).select<>().from_cte<T>("n1")
//   → WITH n1 AS (...), n2 AS (...) SELECT ... FROM n1
// ===================================================================

class cte_builder {
public:
    template <AnySelectQuery Q>
    cte_builder(std::string name, Q const& query) {
        ctes_.emplace_back(std::move(name), query.build_sql());
    }

    template <AnySelectQuery Q>
    [[nodiscard]] cte_builder with_cte(std::string name, Q const& query) const {
        auto copy = *this;
        copy.ctes_.emplace_back(std::move(name), query.build_sql());
        return copy;
    }

    [[nodiscard]] cte_builder recursive() const {
        auto copy = *this;
        copy.recursive_ = true;
        return copy;
    }

    template <typename Table, Projection... Projs>
    [[nodiscard]] detail::select_query_builder<Table, Projs...> select_from_cte(std::string const& cte_name) const {
        detail::select_query_builder<Table, Projs...> builder;
        builder.from_subquery_ = cte_name;
        return builder;
    }

    [[nodiscard]] std::string with_prefix() const {
        std::string s;
        s.reserve(16 + ctes_.size() * 8);
        s += (recursive_ ? "WITH RECURSIVE " : "WITH ");
        bool first = true;
        for (auto const& [name, sql_builder] : ctes_) {
            if (!first) {
                s += ", ";
            }
            s += name;
            s += " AS (";
            s += sql_builder;
            s += ")";
            first = false;
        }
        s += " ";
        return s;
    }

    // Terminal: build a full CTE query
    template <AnySelectQuery MainQuery>
    [[nodiscard]] std::string build_sql(MainQuery const& main) const {
        return with_prefix() + main.build_sql();
    }

private:
    std::vector<std::pair<std::string, std::string>> ctes_;
    bool recursive_ = false;
};

// cte_select_query — wraps a CTE prefix + main SELECT query
template <typename MainQuery>
class cte_select_query {
public:
    using result_row_type = typename MainQuery::result_row_type;

    cte_select_query(std::string with_prefix, MainQuery main)
        : with_prefix_(std::move(with_prefix)), main_(std::move(main)) {
    }

    [[nodiscard]] std::string build_sql() const {
        return with_prefix_ + main_.build_sql();
    }

private:
    std::string with_prefix_;
    MainQuery main_;
};

template <AnySelectQuery Q>
[[nodiscard]] cte_builder with_cte(std::string name, Q const& query) {
    return cte_builder{std::move(name), query};
}

template <AnySelectQuery Q>
[[nodiscard]] cte_builder with_recursive_cte(std::string name, Q const& query) {
    return cte_builder{std::move(name), query}.recursive();
}

// ===================================================================
// DML extensions
//
// INSERT IGNORE, INSERT ... SELECT, REPLACE INTO, UPDATE with JOIN
// ===================================================================

namespace dml_detail {

// ---------------------------------------------------------------
// insert_ignore_into builders
// ---------------------------------------------------------------
template <typename T>
class insert_ignore_values_builder {
public:
    explicit insert_ignore_values_builder(T row) : rows_{std::move(row)}, bulk_(false) {
    }

    insert_ignore_values_builder(std::vector<T> rows, bool bulk) : rows_(std::move(rows)), bulk_(bulk) {
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        const auto& column_list = generate_column_list<T>();
        std::string s;
        if (bulk_) {
            std::string values;
            values.reserve(rows_.size() * 4);
            bool first = true;
            for (auto const& row : rows_) {
                if (!first) {
                    values += ", ";
                }
                values += '(';
                values += generate_values(row);
                values += ')';
                first = false;
            }
            s.reserve(19 + table_name.size() + 2 + column_list.size() + 9 + values.size());
            s += "INSERT IGNORE INTO ";
            s += table_name;
            s += " (";
            s += column_list;
            s += ") VALUES ";
            s += values;
        } else {
            auto values = rows_.empty() ? std::string{} : generate_values(rows_.front());
            s.reserve(19 + table_name.size() + 2 + column_list.size() + 10 + values.size() + 1);
            s += "INSERT IGNORE INTO ";
            s += table_name;
            s += " (";
            s += column_list;
            s += ") VALUES (";
            s += values;
            s += ')';
        }
        return s;
    }

private:
    std::vector<T> rows_;
    bool bulk_ = false;
};

template <typename T>
class insert_ignore_into_builder {
public:
    [[nodiscard]] insert_ignore_values_builder<T> values(T const& row) const {
        return insert_ignore_values_builder<T>{row};
    }

    template <std::ranges::input_range Rows>
        requires std::same_as<std::remove_cvref_t<std::ranges::range_value_t<Rows>>, T>
    [[nodiscard]] insert_ignore_values_builder<T> values(Rows&& rows) const {
        std::vector<T> collected_rows;
        for (auto const& row : rows) {
            collected_rows.push_back(row);
        }
        if (collected_rows.empty()) {
            return {std::vector<T>{}, false};
        }
        return {std::move(collected_rows), true};
    }
};

// ---------------------------------------------------------------
// insert_into_select_builder — INSERT INTO T (...) SELECT ...
// ---------------------------------------------------------------
template <typename T, AnySelectQuery Query>
class insert_into_select_builder {
public:
    explicit insert_into_select_builder(Query query) : query_(std::move(query)) {
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        const auto columns = generate_column_list<T>();
        auto select_sql = query_.build_sql();
        std::string s;
        s.reserve(12 + table_name.size() + 2 + columns.size() + 2 + select_sql.size());
        s += "INSERT INTO ";
        s += table_name;
        s += " (";
        s += columns;
        s += ") ";
        s += select_sql;
        return s;
    }

private:
    Query query_;
};

// ---------------------------------------------------------------
// replace_into builders — REPLACE INTO T (...) VALUES (...)
// ---------------------------------------------------------------
template <typename T>
class replace_into_values_builder {
public:
    explicit replace_into_values_builder(T row) : rows_{std::move(row)}, bulk_(false) {
    }

    replace_into_values_builder(std::vector<T> rows, bool bulk) : rows_(std::move(rows)), bulk_(bulk) {
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        const auto& column_list = generate_column_list<T>();
        std::string s;
        if (bulk_) {
            std::string values;
            values.reserve(rows_.size() * 4);
            bool first = true;
            for (auto const& row : rows_) {
                if (!first) {
                    values += ", ";
                }
                values += '(';
                values += generate_values(row);
                values += ')';
                first = false;
            }
            s.reserve(13 + table_name.size() + 2 + column_list.size() + 9 + values.size());
            s += "REPLACE INTO ";
            s += table_name;
            s += " (";
            s += column_list;
            s += ") VALUES ";
            s += values;
        } else {
            auto values = rows_.empty() ? std::string{} : generate_values(rows_.front());
            s.reserve(13 + table_name.size() + 2 + column_list.size() + 10 + values.size() + 1);
            s += "REPLACE INTO ";
            s += table_name;
            s += " (";
            s += column_list;
            s += ") VALUES (";
            s += values;
            s += ')';
        }
        return s;
    }

private:
    std::vector<T> rows_;
    bool bulk_ = false;
};

template <typename T>
class replace_into_builder {
public:
    [[nodiscard]] replace_into_values_builder<T> values(T const& row) const {
        return replace_into_values_builder<T>{row};
    }

    template <std::ranges::input_range Rows>
        requires std::same_as<std::remove_cvref_t<std::ranges::range_value_t<Rows>>, T>
    [[nodiscard]] replace_into_values_builder<T> values(Rows&& rows) const {
        std::vector<T> collected_rows;
        for (auto const& row : rows) {
            collected_rows.push_back(row);
        }
        if (collected_rows.empty()) {
            return {std::vector<T>{}, false};
        }
        return {std::move(collected_rows), true};
    }
};

// ---------------------------------------------------------------
// update_join_builder — UPDATE T1 JOIN T2 ON ... SET ... [WHERE ...]
// ---------------------------------------------------------------
template <typename T, typename... Cols>
class update_join_set_where_builder {
public:
    update_join_set_where_builder(std::string join_clause, std::tuple<Cols...> assignments, where_condition where)
        : join_clause_(std::move(join_clause)), assignments_(std::move(assignments)), where_(std::move(where)) {
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        auto set_sql = std::apply(
            [](auto const&... fields) {
                return build_assignment_sql(fields...);
            },
            assignments_);
        auto where_sql = where_.build_sql();
        std::string s;
        s.reserve(7 + table_name.size() + join_clause_.size() + 5 + set_sql.size() + 7 + where_sql.size());
        s += "UPDATE ";
        s += table_name;
        s += join_clause_;
        s += " SET ";
        s += set_sql;
        s += " WHERE ";
        s += std::move(where_sql);
        return s;
    }

private:
    std::string join_clause_;
    std::tuple<Cols...> assignments_;
    where_condition where_;
};

template <typename T, typename... Cols>
class update_join_set_builder {
public:
    update_join_set_builder(std::string join_clause, std::tuple<Cols...> assignments)
        : join_clause_(std::move(join_clause)), assignments_(std::move(assignments)) {
    }

    [[nodiscard]] update_join_set_where_builder<T, Cols...> where(where_condition condition) const {
        return {join_clause_, assignments_, std::move(condition)};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        auto set_sql = std::apply(
            [](auto const&... fields) {
                return build_assignment_sql(fields...);
            },
            assignments_);
        std::string s;
        s.reserve(7 + table_name.size() + join_clause_.size() + 5 + set_sql.size());
        s += "UPDATE ";
        s += table_name;
        s += join_clause_;
        s += " SET ";
        s += set_sql;
        return s;
    }

private:
    std::string join_clause_;
    std::tuple<Cols...> assignments_;
};

template <typename T1, typename T2>
class update_join_builder {
public:
    explicit update_join_builder(std::string join_clause) : join_clause_(std::move(join_clause)) {
    }

    template <FieldOf<T1>... Cols>
        requires(sizeof...(Cols) > 0)
    [[nodiscard]] update_join_set_builder<T1, Cols...> set(Cols... assignments) const {
        return update_join_set_builder<T1, Cols...>{join_clause_, std::tuple<Cols...>{assignments...}};
    }

private:
    std::string join_clause_;
};

}  // namespace dml_detail

// insert_ignore_into<T>() — INSERT IGNORE INTO <table> (...) VALUES (...)
template <ValidTable T>
[[nodiscard]] dml_detail::insert_ignore_into_builder<T> insert_ignore_into() {
    return {};
}

// insert_into_select<T>(query) — INSERT INTO <table> (...) SELECT ...
template <ValidTable T, AnySelectQuery Query>
[[nodiscard]] dml_detail::insert_into_select_builder<T, std::decay_t<Query>> insert_into_select(Query&& query) {
    return dml_detail::insert_into_select_builder<T, std::decay_t<Query>>{std::forward<Query>(query)};
}

// replace_into<T>() — REPLACE INTO <table> (...) VALUES (...)
template <ValidTable T>
[[nodiscard]] dml_detail::replace_into_builder<T> replace_into() {
    return {};
}

// update_join<T1, T2>() — UPDATE T1 JOIN T2 ON ... SET ... [WHERE ...]
template <ValidTable T1, ValidTable T2, ColumnDescriptor LeftCol, ColumnDescriptor RightCol>
[[nodiscard]] dml_detail::update_join_builder<T1, T2> update_join() {
    std::string join_clause = " INNER JOIN " + std::string(table_name_for<T2>::value().to_string_view()) + " ON " +
                              detail::qualified_col_name<LeftCol>() + " = " + detail::qualified_col_name<RightCol>();
    return dml_detail::update_join_builder<T1, T2>{std::move(join_clause)};
}

// ===================================================================
// SqlStatement — unified concept for all SQL builder types.
// Satisfied by any builder stage that exposes build_sql() -> std::string.
//
// SqlExecuteStatement — DDL / DML builders (CREATE, INSERT, UPDATE, DELETE, DROP, ...).
// Any SqlStatement that is not a TypedSelectQuery (i.e. carries no result_row_type).
// Used by mysql_database::execute() to enforce that callers cannot pass a SELECT
// builder to execute() and cannot pass a DDL/DML builder to query().
// ===================================================================

template <typename T>
concept SqlStatement = requires(T const& t) {
    { t.build_sql() } -> std::convertible_to<std::string>;
};

template <typename T>
concept SqlExecuteStatement = SqlStatement<T> && !TypedSelectQuery<T>;

}  // namespace ds_mysql
