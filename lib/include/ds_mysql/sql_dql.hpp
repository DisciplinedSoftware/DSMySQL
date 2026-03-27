#pragma once

#include "ds_mysql/sql_core.hpp"
#include "ds_mysql/sql_mutation_shared.hpp"

namespace ds_mysql {

// ===================================================================
// Aggregate projections — used with select<Proj...>().from<Table>()
//
//   count_all         — COUNT(*)              → uint64_t
//   count_col<Col>    — COUNT(col)            → uint64_t
//   sum<Col>          — SUM(col)              → double
//   avg<Col>          — AVG(col)              → double
//   min_of<Col>       — MIN(col)              → value_type of Col
//   max_of<Col>       — MAX(col)              → value_type of Col
//   count_distinct<Col> — COUNT(DISTINCT col) → uint64_t
//   group_concat<Col> — GROUP_CONCAT(col)     → std::string
//   stddev<Col>       — STDDEV(col)           → double
//   std_of<Col>       — STD(col)              → double
//   stddev_pop<Col>   — STDDEV_POP(col)       → double
//   stddev_samp<Col>  — STDDEV_SAMP(col)      → double
//   variance<Col>     — VARIANCE(col)         → double
//   var_pop<Col>      — VAR_POP(col)          → double
//   var_samp<Col>     — VAR_SAMP(col)         → double
//   bit_and_of<Col>   — BIT_AND(col)          → uint64_t  (alias: bit_and)
//   bit_or_of<Col>    — BIT_OR(col)           → uint64_t  (alias: bit_or)
//   bit_xor_of<Col>   — BIT_XOR(col)          → uint64_t  (alias: bit_xor)
//   json_arrayagg<Col>         — JSON_ARRAYAGG(col)      → std::string
//   json_objectagg<Key, Val>   — JSON_OBJECTAGG(key,val) → std::string
//   any_value<Col>    — ANY_VALUE(col)         → value_type of Col
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

// stddev<Col> — STDDEV(col) → double
template <ColumnDescriptor Col>
struct stddev {};

// std_of<Col> — STD(col) → double  (STD is a MySQL synonym for STDDEV)
template <ColumnDescriptor Col>
struct std_of {};

// stddev_pop<Col> — STDDEV_POP(col) → double
template <ColumnDescriptor Col>
struct stddev_pop {};

// stddev_samp<Col> — STDDEV_SAMP(col) → double
template <ColumnDescriptor Col>
struct stddev_samp {};

// variance<Col> — VARIANCE(col) → double
template <ColumnDescriptor Col>
struct variance {};

// var_pop<Col> — VAR_POP(col) → double
template <ColumnDescriptor Col>
struct var_pop {};

// var_samp<Col> — VAR_SAMP(col) → double
template <ColumnDescriptor Col>
struct var_samp {};

// bit_and_of<Col> — BIT_AND(col) → uint64_t  (alias: bit_and)
template <ColumnDescriptor Col>
struct bit_and_of {};

// bit_or_of<Col> — BIT_OR(col) → uint64_t  (alias: bit_or)
template <ColumnDescriptor Col>
struct bit_or_of {};

// bit_xor_of<Col> — BIT_XOR(col) → uint64_t  (alias: bit_xor)
template <ColumnDescriptor Col>
struct bit_xor_of {};

// json_arrayagg<Col> — JSON_ARRAYAGG(col) → std::string  (MySQL 5.7.22+)
template <ColumnDescriptor Col>
struct json_arrayagg {};

// json_objectagg<KeyCol, ValCol> — JSON_OBJECTAGG(key, val) → std::string  (MySQL 5.7.22+)
template <ColumnDescriptor KeyCol, ColumnDescriptor ValCol>
struct json_objectagg {};

// any_value<Col> — ANY_VALUE(col) → value_type of Col
//   Suppresses ONLY_FULL_GROUP_BY errors for non-aggregated columns.
template <ColumnDescriptor Col>
struct any_value {};

// rand_val — RAND() projection for ORDER BY clauses (e.g. order_by<rand_val>())
struct rand_val {};

// ===================================================================
// interval — typed INTERVAL values for DATE_ADD / DATE_SUB
// ===================================================================
namespace interval {
struct day {
    int amount;
    static constexpr std::string_view part_name() {
        return "DAY";
    }
};
struct week {
    int amount;
    static constexpr std::string_view part_name() {
        return "WEEK";
    }
};
struct month {
    int amount;
    static constexpr std::string_view part_name() {
        return "MONTH";
    }
};
struct year {
    int amount;
    static constexpr std::string_view part_name() {
        return "YEAR";
    }
};
struct quarter {
    int amount;
    static constexpr std::string_view part_name() {
        return "QUARTER";
    }
};
struct hour {
    int amount;
    static constexpr std::string_view part_name() {
        return "HOUR";
    }
};
struct minute {
    int amount;
    static constexpr std::string_view part_name() {
        return "MINUTE";
    }
};
struct second {
    int amount;
    static constexpr std::string_view part_name() {
        return "SECOND";
    }
};
struct microsecond {
    int amount;
    static constexpr std::string_view part_name() {
        return "MICROSECOND";
    }
};
}  // namespace interval

template <typename T>
concept SqlInterval = requires(T const& t) {
    { t.amount } -> std::convertible_to<int>;
    { T::part_name() } -> std::convertible_to<std::string_view>;
};

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

// round_to and format_to are now runtime classes defined after the Projection concept

template <typename A, typename B>
struct mod_of {};

template <typename A, typename B>
struct power_of {};

template <typename... Ps>
struct concat_of {};

// substring_of and date_format_of are now runtime classes defined after the Projection concept

template <sql_date_part Part, typename P>
struct extract_of {};

template <typename P>
struct date_of {};

template <typename P>
struct time_of {};

template <typename P>
struct year_of {};

template <typename P>
struct month_of {};

template <typename P>
struct day_of {};

template <typename A, typename B>
struct datediff_of {};

// date_add_of and date_sub_of are now runtime classes defined after the Projection concept

template <sql_date_part Part, typename A, typename B>
struct timestampdiff_of {};

template <typename P, sql_cast_type Type>
struct cast_as {};

template <typename P, sql_cast_type Type>
struct convert_as {};

// ===================================================================
// Conditional scalar functions
// ===================================================================

template <typename A, typename B>
struct nullif_of {};

template <typename... Ps>
struct greatest_of {};

template <typename... Ps>
struct least_of {};

// if_expr is now a runtime class defined after the Projection concept

// ===================================================================
// String scalar functions
// ===================================================================

template <typename P>
struct char_length_of {};

// left_of, right_of, replace_of, lpad_of, rpad_of, repeat_of, locate_of, instr_of
// are now runtime classes defined after the Projection concept

template <typename P>
struct reverse_of {};

// SPACE(col) — returns col spaces
template <typename P>
struct space_of {};

template <typename A, typename B>
struct strcmp_of {};

// ===================================================================
// Math scalar functions
// ===================================================================

template <typename P>
struct sqrt_of {};

template <typename P>
struct log_of {};

template <typename P>
struct log2_of {};

template <typename P>
struct log10_of {};

template <typename P>
struct exp_of {};

template <typename P>
struct sin_of {};

template <typename P>
struct cos_of {};

template <typename P>
struct tan_of {};

template <typename P>
struct degrees_of {};

template <typename P>
struct radians_of {};

template <typename P>
struct sign_of {};

// truncate_to is now a runtime class defined after the Projection concept

struct pi_val {};

// ===================================================================
// Date/Time scalar functions
// ===================================================================

template <typename P>
struct hour_of {};

template <typename P>
struct minute_of {};

template <typename P>
struct second_of {};

template <typename P>
struct microsecond_of {};

template <typename P>
struct quarter_of {};

template <typename P>
struct week_of {};

template <typename P>
struct weekday_of {};

template <typename P>
struct dayofweek_of {};

template <typename P>
struct dayofyear_of {};

template <typename P>
struct dayname_of {};

template <typename P>
struct monthname_of {};

template <typename P>
struct last_day_of {};

// str_to_date_of is now a runtime class defined after the Projection concept

template <typename P>
struct from_unixtime_of {};

template <typename P>
struct unix_timestamp_of {};

// convert_tz_of is now a runtime class defined after the Projection concept

struct curtime_val {};

struct utc_timestamp_val {};

template <typename A, typename B>
struct addtime_of {};

template <typename A, typename B>
struct subtime_of {};

template <typename A, typename B>
struct timediff_of {};

// ===================================================================
// JSON scalar functions
// ===================================================================

// json_extract_of is now a runtime class defined after the Projection concept

// JSON_OBJECT(p1, p2, ...) — alternating key/value projections
template <typename... Ps>
struct json_object_of {};

// JSON_ARRAY(p1, p2, ...) — value projections
template <typename... Ps>
struct json_array_of {};

// json_contains_of is now a runtime class defined after the Projection concept

template <typename P>
struct json_length_of {};

template <typename P>
struct json_unquote_of {};

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

// round and format aliases defined after the Projection concept

template <typename A, typename B>
using mod = mod_of<A, B>;

template <typename A, typename B>
using power = power_of<A, B>;

template <typename... Ps>
using concat = concat_of<Ps...>;

// substring and date_format aliases defined after the Projection concept

template <sql_date_part Part, typename P>
using extract = extract_of<Part, P>;

template <typename A, typename B>
using datediff = datediff_of<A, B>;

// date_add and date_sub aliases defined after the Projection concept

template <sql_date_part Part, typename A, typename B>
using timestampdiff = timestampdiff_of<Part, A, B>;

template <typename P, sql_cast_type Type>
using cast = cast_as<P, Type>;

template <typename P, sql_cast_type Type>
using convert = convert_as<P, Type>;

// --- Conditional ---
template <typename A, typename B>
using nullif = nullif_of<A, B>;

template <typename... Ps>
using greatest = greatest_of<Ps...>;

template <typename... Ps>
using least = least_of<Ps...>;

// sql_if is now a function template (not a type alias)

// --- String ---
template <typename P>
using char_length = char_length_of<P>;

// left, right, replace, lpad, rpad, repeat aliases defined after the Projection concept

template <typename P>
using reverse = reverse_of<P>;

// locate and instr aliases defined after the Projection concept

template <typename P>
using space = space_of<P>;

template <typename A, typename B>
using strcmp = strcmp_of<A, B>;

// --- Math ---
template <typename P>
using sqrt = sqrt_of<P>;

template <typename P>
using log = log_of<P>;

template <typename P>
using log2 = log2_of<P>;

template <typename P>
using log10 = log10_of<P>;

template <typename P>
using exp = exp_of<P>;

template <typename P>
using sin = sin_of<P>;

template <typename P>
using cos = cos_of<P>;

template <typename P>
using tan = tan_of<P>;

template <typename P>
using degrees = degrees_of<P>;

template <typename P>
using radians = radians_of<P>;

template <typename P>
using sign = sign_of<P>;

// truncate alias defined after the Projection concept

// --- Date/Time ---
template <typename P>
using hour = hour_of<P>;

template <typename P>
using minute = minute_of<P>;

template <typename P>
using second = second_of<P>;

template <typename P>
using microsecond = microsecond_of<P>;

template <typename P>
using quarter = quarter_of<P>;

template <typename P>
using week = week_of<P>;

template <typename P>
using weekday = weekday_of<P>;

template <typename P>
using dayofweek = dayofweek_of<P>;

template <typename P>
using dayofyear = dayofyear_of<P>;

template <typename P>
using dayname = dayname_of<P>;

template <typename P>
using monthname = monthname_of<P>;

template <typename P>
using last_day = last_day_of<P>;

// str_to_date alias defined after the Projection concept

template <typename P>
using from_unixtime = from_unixtime_of<P>;

template <typename P>
using unix_timestamp = unix_timestamp_of<P>;

// convert_tz alias defined after the Projection concept

template <typename A, typename B>
using addtime = addtime_of<A, B>;

template <typename A, typename B>
using subtime = subtime_of<A, B>;

template <typename A, typename B>
using timediff = timediff_of<A, B>;

// --- JSON ---
// json_extract alias defined after the Projection concept

template <typename... Ps>
using json_object = json_object_of<Ps...>;

template <typename... Ps>
using json_array = json_array_of<Ps...>;

// json_contains alias defined after the Projection concept

template <typename P>
using json_length = json_length_of<P>;

template <typename P>
using json_unquote = json_unquote_of<P>;

// Convenience aliases for bitwise aggregate names that shadow std:: functors.
template <ColumnDescriptor Col>
using bit_and = bit_and_of<Col>;

template <ColumnDescriptor Col>
using bit_or = bit_or_of<Col>;

template <ColumnDescriptor Col>
using bit_xor = bit_xor_of<Col>;

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
    case_when_builder(sql_predicate cond, ValueType then_val) {
        branches_.emplace_back(std::move(cond), std::move(then_val));
    }

    [[nodiscard]] case_when_builder when(sql_predicate cond, ValueType then_val) && {
        branches_.emplace_back(std::move(cond), std::move(then_val));
        return std::move(*this);
    }

    [[nodiscard]] case_when_builder when(sql_predicate cond, ValueType then_val) const& {
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
    std::vector<std::pair<sql_predicate, ValueType>> branches_;
    std::optional<ValueType> else_val_;
};

template <SqlValue ValueType>
[[nodiscard]] case_when_builder<ValueType> case_when(sql_predicate cond, ValueType then_val) {
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

template <fixed_string Clause>
struct window_frame_clause {
    static constexpr std::string_view sql_clause() {
        return Clause;
    }
};

using rows_unbounded_preceding_to_current_row = window_frame_clause<"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW">;
using rows_unbounded_preceding_to_unbounded_following =
    window_frame_clause<"ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING">;
using rows_current_row_to_unbounded_following = window_frame_clause<"ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING">;

template <typename T>
concept WindowFrameSpec = requires {
    { T::sql_clause() } -> std::convertible_to<std::string_view>;
};

// ===================================================================
// Window function projections
//
//   window_func<Agg, PartitionCol, OrderCol>
//      → AGG_EXPR OVER (PARTITION BY partition_col ORDER BY order_col)
//
//   row_number_over<PartitionCol, OrderCol>
//      → ROW_NUMBER() OVER (PARTITION BY partition_col ORDER BY order_col)
//   rank_over<PartitionCol, OrderCol>
//      → RANK() OVER (PARTITION BY partition_col ORDER BY order_col)
//   dense_rank_over<PartitionCol, OrderCol>
//      → DENSE_RANK() OVER (PARTITION BY partition_col ORDER BY order_col)
//   ntile_over<N, PartitionCol, OrderCol>
//      → NTILE(N) OVER (PARTITION BY partition_col ORDER BY order_col)
//   percent_rank_over<PartitionCol, OrderCol>
//      → PERCENT_RANK() OVER (PARTITION BY partition_col ORDER BY order_col)
//   cume_dist_over<PartitionCol, OrderCol>
//      → CUME_DIST() OVER (PARTITION BY partition_col ORDER BY order_col)
//   lag_over<Col, PartitionCol, OrderCol, Offset>
//      → LAG(col, Offset) OVER (PARTITION BY partition_col ORDER BY order_col)
//   lead_over<Col, PartitionCol, OrderCol, Offset>
//      → LEAD(col, Offset) OVER (PARTITION BY partition_col ORDER BY order_col)
//   first_value_over<Col, PartitionCol, OrderCol>
//      → FIRST_VALUE(col) OVER (PARTITION BY partition_col ORDER BY order_col)
//   last_value_over<Col, PartitionCol, OrderCol>
//      → LAST_VALUE(col) OVER (PARTITION BY partition_col ORDER BY order_col)
//   nth_value_over<Col, N, PartitionCol, OrderCol>
//      → NTH_VALUE(col, N) OVER (PARTITION BY partition_col ORDER BY order_col)
//
// projection_traits specialisations are defined after the Projection concept.
// ===================================================================

template <typename Agg, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir = sort_order::asc,
          typename Frame = void>
struct window_func {};

template <ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir = sort_order::asc,
          typename Frame = void>
struct row_number_over {};

template <ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir = sort_order::asc,
          typename Frame = void>
struct rank_over {};

template <ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir = sort_order::asc,
          typename Frame = void>
struct dense_rank_over {};

template <std::size_t N, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir = sort_order::asc,
          typename Frame = void>
struct ntile_over {};

template <ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir = sort_order::asc,
          typename Frame = void>
struct percent_rank_over {};

template <ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir = sort_order::asc,
          typename Frame = void>
struct cume_dist_over {};

template <ColumnDescriptor Col, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, std::size_t Offset = 1,
          sort_order Dir = sort_order::asc, typename Frame = void>
struct lag_over {};

template <ColumnDescriptor Col, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, std::size_t Offset = 1,
          sort_order Dir = sort_order::asc, typename Frame = void>
struct lead_over {};

template <ColumnDescriptor Col, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol,
          sort_order Dir = sort_order::asc, typename Frame = void>
struct first_value_over {};

template <ColumnDescriptor Col, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol,
          sort_order Dir = sort_order::asc, typename Frame = void>
struct last_value_over {};

template <ColumnDescriptor Col, std::size_t N, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol,
          sort_order Dir = sort_order::asc, typename Frame = void>
struct nth_value_over {};

// projection_traits<P> — uniform interface over column descriptors and aggregates.
//
// Specialise this for custom aggregate types to integrate them with the
// SELECT builder:
//
//   template <>
//   struct projection_traits<my_agg> {
//       using value_type = double;
//       [[nodiscard]] static std::string sql_expr() { return "MY_AGG(...)"; }
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
    [[nodiscard]] static std::string sql_expr() {
        return "COUNT(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<sum<Col>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "SUM(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<avg<Col>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "AVG(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<min_of<Col>> {
    using value_type = typename column_traits<Col>::value_type;
    [[nodiscard]] static std::string sql_expr() {
        return "MIN(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<max_of<Col>> {
    using value_type = typename column_traits<Col>::value_type;
    [[nodiscard]] static std::string sql_expr() {
        return "MAX(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<count_distinct<Col>> {
    using value_type = uint64_t;
    [[nodiscard]] static std::string sql_expr() {
        return "COUNT(DISTINCT " + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<group_concat<Col>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        return "GROUP_CONCAT(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<stddev<Col>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "STDDEV(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<std_of<Col>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "STD(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<stddev_pop<Col>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "STDDEV_POP(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<stddev_samp<Col>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "STDDEV_SAMP(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<variance<Col>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "VARIANCE(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<var_pop<Col>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "VAR_POP(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<var_samp<Col>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "VAR_SAMP(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<bit_and_of<Col>> {
    using value_type = uint64_t;
    [[nodiscard]] static std::string sql_expr() {
        return "BIT_AND(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<bit_or_of<Col>> {
    using value_type = uint64_t;
    [[nodiscard]] static std::string sql_expr() {
        return "BIT_OR(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<bit_xor_of<Col>> {
    using value_type = uint64_t;
    [[nodiscard]] static std::string sql_expr() {
        return "BIT_XOR(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<json_arrayagg<Col>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        return "JSON_ARRAYAGG(" + std::string(column_traits<Col>::column_name()) + ")";
    }
};

template <ColumnDescriptor KeyCol, ColumnDescriptor ValCol>
struct projection_traits<json_objectagg<KeyCol, ValCol>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        return "JSON_OBJECTAGG(" + std::string(column_traits<KeyCol>::column_name()) + ", " +
               std::string(column_traits<ValCol>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col>
struct projection_traits<any_value<Col>> {
    using value_type = typename column_traits<Col>::value_type;
    [[nodiscard]] static std::string sql_expr() {
        return "ANY_VALUE(" + std::string(column_traits<Col>::column_name()) + ")";
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
    [[nodiscard]] static std::string sql_expr() {
        return "COALESCE(" + std::string(column_traits<Col1>::column_name()) + ", " +
               std::string(column_traits<Col2>::column_name()) + ")";
    }
};

template <ColumnDescriptor Col1, ColumnDescriptor Col2>
struct projection_traits<ifnull_of<Col1, Col2>> {
    using value_type = typename column_traits<Col1>::value_type;
    [[nodiscard]] static std::string sql_expr() {
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

template <typename Frame>
[[nodiscard]] std::string sql_window_frame_clause() {
    if constexpr (std::same_as<Frame, void>) {
        return "";
    } else {
        static_assert(WindowFrameSpec<Frame>, "Frame must satisfy WindowFrameSpec");
        return " " + std::string(Frame::sql_clause());
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

// InstanceProjection — runtime projection with pre-computed SQL, non-static sql_expr()
template <typename P>
concept InstanceProjection = requires(P const& p) {
    typename P::value_type;
    { p.sql_expr() } -> std::convertible_to<std::string>;
} && !Projection<P>;

// AnyProjection — satisfied by either Projection (static) or InstanceProjection (runtime)
template <typename P>
concept AnyProjection = Projection<P> || InstanceProjection<P>;

// ===================================================================
// Runtime projection classes (InstanceProjection)
// These require the Projection concept, so are defined here.
// ===================================================================

// IF(cond, then, else) — typed predicate-based IF expression
// Created via sql_if<Then, Else>(predicate).
template <Projection Then, Projection Else>
class if_expr {
public:
    using value_type = typename projection_traits<Then>::value_type;

    explicit if_expr(sql_predicate cond)
        : sql_("IF(" + cond.build_sql() + ", " + std::string(projection_traits<Then>::sql_expr()) + ", " +
               std::string(projection_traits<Else>::sql_expr()) + ")") {
    }

    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

template <Projection Then, Projection Else>
[[nodiscard]] if_expr<Then, Else> sql_if(sql_predicate cond) {
    return if_expr<Then, Else>{std::move(cond)};
}

template <Projection P>
class left_of {
public:
    using value_type = std::string;
    explicit left_of(std::size_t n)
        : sql_("LEFT(" + std::string(projection_traits<P>::sql_expr()) + ", " + std::to_string(n) + ")") {
    }
    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

template <Projection P>
class right_of {
public:
    using value_type = std::string;
    explicit right_of(std::size_t n)
        : sql_("RIGHT(" + std::string(projection_traits<P>::sql_expr()) + ", " + std::to_string(n) + ")") {
    }
    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

template <Projection P>
class repeat_of {
public:
    using value_type = std::string;
    explicit repeat_of(std::size_t n)
        : sql_("REPEAT(" + std::string(projection_traits<P>::sql_expr()) + ", " + std::to_string(n) + ")") {
    }
    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

template <ColumnDescriptor P>
class replace_of {
public:
    using value_type = std::string;
    replace_of(P from, P to)
        : sql_("REPLACE(" + std::string(column_traits<P>::column_name()) + ", " + sql_detail::to_sql_value(from) +
               ", " + sql_detail::to_sql_value(to) + ")") {
    }
    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

template <ColumnDescriptor P>
class lpad_of {
public:
    using value_type = std::string;
    lpad_of(std::size_t len, P pad)
        : sql_("LPAD(" + std::string(column_traits<P>::column_name()) + ", " + std::to_string(len) + ", " +
               sql_detail::to_sql_value(pad) + ")") {
    }
    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

template <ColumnDescriptor P>
class rpad_of {
public:
    using value_type = std::string;
    rpad_of(std::size_t len, P pad)
        : sql_("RPAD(" + std::string(column_traits<P>::column_name()) + ", " + std::to_string(len) + ", " +
               sql_detail::to_sql_value(pad) + ")") {
    }
    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

template <ColumnDescriptor P>
class locate_of {
public:
    using value_type = uint64_t;
    explicit locate_of(P substr)
        : sql_("LOCATE(" + sql_detail::to_sql_value(substr) + ", " + std::string(column_traits<P>::column_name()) +
               ")") {
    }
    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

template <ColumnDescriptor P>
class instr_of {
public:
    using value_type = uint64_t;
    explicit instr_of(P substr)
        : sql_("INSTR(" + std::string(column_traits<P>::column_name()) + ", " + sql_detail::to_sql_value(substr) +
               ")") {
    }
    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

template <Projection P>
class substring_of {
public:
    using value_type = std::string;
    substring_of(std::size_t start, std::size_t length)
        : sql_("SUBSTRING(" + std::string(projection_traits<P>::sql_expr()) + ", " + std::to_string(start) + ", " +
               std::to_string(length) + ")") {
    }
    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

template <Projection P>
class date_format_of {
public:
    using value_type = std::string;
    explicit date_format_of(std::string_view fmt)
        : sql_("DATE_FORMAT(" + std::string(projection_traits<P>::sql_expr()) + ", '" +
               sql_detail::escape_sql_string(fmt) + "')") {
    }
    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

template <Projection P>
class date_add_of {
public:
    using value_type = std::string;
    template <SqlInterval Interval>
    explicit date_add_of(Interval const& iv)
        : sql_("DATE_ADD(" + std::string(projection_traits<P>::sql_expr()) + ", INTERVAL " + std::to_string(iv.amount) +
               " " + std::string(Interval::part_name()) + ")") {
    }
    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

template <Projection P>
class date_sub_of {
public:
    using value_type = std::string;
    template <SqlInterval Interval>
    explicit date_sub_of(Interval const& iv)
        : sql_("DATE_SUB(" + std::string(projection_traits<P>::sql_expr()) + ", INTERVAL " + std::to_string(iv.amount) +
               " " + std::string(Interval::part_name()) + ")") {
    }
    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

template <Projection P>
class truncate_to {
public:
    using value_type = double;
    explicit truncate_to(int d)
        : sql_("TRUNCATE(" + std::string(projection_traits<P>::sql_expr()) + ", " + std::to_string(d) + ")") {
    }
    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

template <Projection P>
class round_to {
public:
    using value_type = double;
    explicit round_to(int scale)
        : sql_("ROUND(" + std::string(projection_traits<P>::sql_expr()) + ", " + std::to_string(scale) + ")") {
    }
    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

template <Projection P>
class format_to {
public:
    using value_type = std::string;
    explicit format_to(int scale)
        : sql_("FORMAT(" + std::string(projection_traits<P>::sql_expr()) + ", " + std::to_string(scale) + ")") {
    }
    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

template <Projection P>
class str_to_date_of {
public:
    using value_type = std::string;
    explicit str_to_date_of(std::string_view fmt)
        : sql_("STR_TO_DATE(" + std::string(projection_traits<P>::sql_expr()) + ", '" +
               sql_detail::escape_sql_string(fmt) + "')") {
    }
    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

template <Projection P>
class convert_tz_of {
public:
    using value_type = std::string;
    convert_tz_of(std::string_view from_tz, std::string_view to_tz)
        : sql_("CONVERT_TZ(" + std::string(projection_traits<P>::sql_expr()) + ", '" +
               sql_detail::escape_sql_string(from_tz) + "', '" + sql_detail::escape_sql_string(to_tz) + "')") {
    }
    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

template <Projection P>
class json_extract_of {
public:
    using value_type = std::string;
    explicit json_extract_of(std::string_view path)
        : sql_("JSON_EXTRACT(" + std::string(projection_traits<P>::sql_expr()) + ", '" +
               sql_detail::escape_sql_string(path) + "')") {
    }
    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

template <Projection P>
class json_contains_of {
public:
    using value_type = uint32_t;
    explicit json_contains_of(std::string_view val)
        : sql_("JSON_CONTAINS(" + std::string(projection_traits<P>::sql_expr()) + ", '" +
               sql_detail::escape_sql_string(val) + "')") {
    }
    [[nodiscard]] std::string sql_expr() const {
        return sql_;
    }

private:
    std::string sql_;
};

// Convenience aliases for the new runtime projection classes
template <Projection P>
using substring = substring_of<P>;

template <Projection P>
using date_format = date_format_of<P>;

template <Projection P>
using date_add = date_add_of<P>;

template <Projection P>
using date_sub = date_sub_of<P>;

template <Projection P>
using left = left_of<P>;

template <Projection P>
using right = right_of<P>;

template <ColumnDescriptor P>
using replace = replace_of<P>;

template <ColumnDescriptor P>
using lpad = lpad_of<P>;

template <ColumnDescriptor P>
using rpad = rpad_of<P>;

template <Projection P>
using repeat = repeat_of<P>;

template <ColumnDescriptor P>
using locate = locate_of<P>;

template <ColumnDescriptor P>
using instr = instr_of<P>;

template <Projection P>
using truncate = truncate_to<P>;

template <Projection P>
using round = round_to<P>;

template <Projection P>
using format = format_to<P>;

template <Projection P>
using str_to_date = str_to_date_of<P>;

template <Projection P>
using convert_tz = convert_tz_of<P>;

template <Projection P>
using json_extract = json_extract_of<P>;

template <Projection P>
using json_contains = json_contains_of<P>;

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
    [[nodiscard]] static std::string sql_expr() {
        return "(" + std::string(projection_traits<A>::sql_expr()) + " + " +
               std::string(projection_traits<B>::sql_expr()) + ")";
    }
};

template <Projection A, Projection B>
struct projection_traits<arith_sub<A, B>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "(" + std::string(projection_traits<A>::sql_expr()) + " - " +
               std::string(projection_traits<B>::sql_expr()) + ")";
    }
};

template <Projection A, Projection B>
struct projection_traits<arith_mul<A, B>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "(" + std::string(projection_traits<A>::sql_expr()) + " * " +
               std::string(projection_traits<B>::sql_expr()) + ")";
    }
};

template <Projection A, Projection B>
struct projection_traits<arith_div<A, B>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "(" + std::string(projection_traits<A>::sql_expr()) + " / " +
               std::string(projection_traits<B>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<abs_of<P>> {
    using value_type = sql_numeric_projection_value_t<typename projection_traits<P>::value_type>;
    [[nodiscard]] static std::string sql_expr() {
        return "ABS(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<floor_of<P>> {
    using value_type = sql_numeric_projection_value_t<typename projection_traits<P>::value_type>;
    [[nodiscard]] static std::string sql_expr() {
        return "FLOOR(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<ceil_of<P>> {
    using value_type = sql_numeric_projection_value_t<typename projection_traits<P>::value_type>;
    [[nodiscard]] static std::string sql_expr() {
        return "CEIL(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<upper_of<P>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        return "UPPER(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<lower_of<P>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        return "LOWER(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<trim_of<P>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        return "TRIM(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<length_of<P>> {
    using value_type = uint64_t;
    [[nodiscard]] static std::string sql_expr() {
        return "LENGTH(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

// round_to and format_to are now runtime classes (InstanceProjection) — no projection_traits needed

template <Projection A, Projection B>
struct projection_traits<mod_of<A, B>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "MOD(" + std::string(projection_traits<A>::sql_expr()) + ", " +
               std::string(projection_traits<B>::sql_expr()) + ")";
    }
};

template <Projection A, Projection B>
struct projection_traits<power_of<A, B>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "POWER(" + std::string(projection_traits<A>::sql_expr()) + ", " +
               std::string(projection_traits<B>::sql_expr()) + ")";
    }
};

template <Projection... Ps>
    requires(sizeof...(Ps) > 0)
struct projection_traits<concat_of<Ps...>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        std::string s;
        s.reserve(8 + sizeof...(Ps) * 4);
        s += "CONCAT(";
        bool first = true;
        (((s += (first ? "" : ", "), s += projection_traits<Ps>::sql_expr(), first = false)), ...);
        s += ")";
        return s;
    }
};

// substring_of and date_format_of are now runtime classes (InstanceProjection) — no projection_traits needed

template <sql_date_part Part, Projection P>
struct projection_traits<extract_of<Part, P>> {
    using value_type = uint32_t;
    [[nodiscard]] static std::string sql_expr() {
        return "EXTRACT(" + std::string(sql_date_part_name<Part>()) + " FROM " +
               std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<date_of<P>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        return "DATE(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<time_of<P>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        return "TIME(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<year_of<P>> {
    using value_type = uint32_t;
    [[nodiscard]] static std::string sql_expr() {
        return "YEAR(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<month_of<P>> {
    using value_type = uint32_t;
    [[nodiscard]] static std::string sql_expr() {
        return "MONTH(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<day_of<P>> {
    using value_type = uint32_t;
    [[nodiscard]] static std::string sql_expr() {
        return "DAY(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection A, Projection B>
struct projection_traits<datediff_of<A, B>> {
    using value_type = int64_t;
    [[nodiscard]] static std::string sql_expr() {
        return "DATEDIFF(" + std::string(projection_traits<A>::sql_expr()) + ", " +
               std::string(projection_traits<B>::sql_expr()) + ")";
    }
};

// date_add_of and date_sub_of are now runtime classes (InstanceProjection) — no projection_traits needed

template <sql_date_part Part, Projection A, Projection B>
struct projection_traits<timestampdiff_of<Part, A, B>> {
    using value_type = int64_t;
    [[nodiscard]] static std::string sql_expr() {
        return "TIMESTAMPDIFF(" + std::string(sql_date_part_name<Part>()) + ", " +
               std::string(projection_traits<A>::sql_expr()) + ", " + std::string(projection_traits<B>::sql_expr()) +
               ")";
    }
};

template <Projection P, sql_cast_type Type>
struct projection_traits<cast_as<P, Type>> {
    using value_type = typename sql_cast_type_traits<Type>::value_type;
    [[nodiscard]] static std::string sql_expr() {
        return "CAST(" + std::string(projection_traits<P>::sql_expr()) + " AS " +
               std::string(sql_cast_type_traits<Type>::sql_name()) + ")";
    }
};

template <Projection P, sql_cast_type Type>
struct projection_traits<convert_as<P, Type>> {
    using value_type = typename sql_cast_type_traits<Type>::value_type;
    [[nodiscard]] static std::string sql_expr() {
        return "CONVERT(" + std::string(projection_traits<P>::sql_expr()) + ", " +
               std::string(sql_cast_type_traits<Type>::sql_name()) + ")";
    }
};

// ===================================================================
// Conditional scalar function traits
// ===================================================================

template <Projection A, Projection B>
struct projection_traits<nullif_of<A, B>> {
    using value_type = typename projection_traits<A>::value_type;
    [[nodiscard]] static std::string sql_expr() {
        return "NULLIF(" + std::string(projection_traits<A>::sql_expr()) + ", " +
               std::string(projection_traits<B>::sql_expr()) + ")";
    }
};

template <Projection... Ps>
    requires(sizeof...(Ps) > 0)
struct projection_traits<greatest_of<Ps...>> {
    using value_type = typename projection_traits<std::tuple_element_t<0, std::tuple<Ps...>>>::value_type;
    [[nodiscard]] static std::string sql_expr() {
        std::string s = "GREATEST(";
        bool first = true;
        (((s += (first ? "" : ", "), s += projection_traits<Ps>::sql_expr(), first = false)), ...);
        s += ")";
        return s;
    }
};

template <Projection... Ps>
    requires(sizeof...(Ps) > 0)
struct projection_traits<least_of<Ps...>> {
    using value_type = typename projection_traits<std::tuple_element_t<0, std::tuple<Ps...>>>::value_type;
    [[nodiscard]] static std::string sql_expr() {
        std::string s = "LEAST(";
        bool first = true;
        (((s += (first ? "" : ", "), s += projection_traits<Ps>::sql_expr(), first = false)), ...);
        s += ")";
        return s;
    }
};

// if_expr is now a runtime class (InstanceProjection) — no projection_traits needed

// ===================================================================
// String scalar function traits
// ===================================================================

template <Projection P>
struct projection_traits<char_length_of<P>> {
    using value_type = uint64_t;
    [[nodiscard]] static std::string sql_expr() {
        return "CHAR_LENGTH(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

// left_of, right_of, replace_of, lpad_of, rpad_of, repeat_of are now runtime classes — no projection_traits needed

template <Projection P>
struct projection_traits<reverse_of<P>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        return "REVERSE(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

// locate_of and instr_of are now runtime classes — no projection_traits needed

template <Projection P>
struct projection_traits<space_of<P>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        return "SPACE(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection A, Projection B>
struct projection_traits<strcmp_of<A, B>> {
    using value_type = int32_t;
    [[nodiscard]] static std::string sql_expr() {
        return "STRCMP(" + std::string(projection_traits<A>::sql_expr()) + ", " +
               std::string(projection_traits<B>::sql_expr()) + ")";
    }
};

// ===================================================================
// Math scalar function traits
// ===================================================================

template <Projection P>
struct projection_traits<sqrt_of<P>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "SQRT(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<log_of<P>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "LOG(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<log2_of<P>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "LOG2(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<log10_of<P>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "LOG10(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<exp_of<P>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "EXP(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<sin_of<P>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "SIN(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<cos_of<P>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "COS(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<tan_of<P>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "TAN(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<degrees_of<P>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "DEGREES(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<radians_of<P>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "RADIANS(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<sign_of<P>> {
    using value_type = int32_t;
    [[nodiscard]] static std::string sql_expr() {
        return "SIGN(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

// truncate_to is now a runtime class — no projection_traits needed

template <>
struct projection_traits<pi_val> {
    using value_type = double;
    static constexpr std::string_view sql_expr() {
        return "PI()";
    }
};

// ===================================================================
// Date/Time scalar function traits
// ===================================================================

template <Projection P>
struct projection_traits<hour_of<P>> {
    using value_type = uint32_t;
    [[nodiscard]] static std::string sql_expr() {
        return "HOUR(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<minute_of<P>> {
    using value_type = uint32_t;
    [[nodiscard]] static std::string sql_expr() {
        return "MINUTE(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<second_of<P>> {
    using value_type = uint32_t;
    [[nodiscard]] static std::string sql_expr() {
        return "SECOND(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<microsecond_of<P>> {
    using value_type = uint32_t;
    [[nodiscard]] static std::string sql_expr() {
        return "MICROSECOND(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<quarter_of<P>> {
    using value_type = uint32_t;
    [[nodiscard]] static std::string sql_expr() {
        return "QUARTER(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<week_of<P>> {
    using value_type = uint32_t;
    [[nodiscard]] static std::string sql_expr() {
        return "WEEK(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<weekday_of<P>> {
    using value_type = uint32_t;
    [[nodiscard]] static std::string sql_expr() {
        return "WEEKDAY(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<dayofweek_of<P>> {
    using value_type = uint32_t;
    [[nodiscard]] static std::string sql_expr() {
        return "DAYOFWEEK(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<dayofyear_of<P>> {
    using value_type = uint32_t;
    [[nodiscard]] static std::string sql_expr() {
        return "DAYOFYEAR(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<dayname_of<P>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        return "DAYNAME(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<monthname_of<P>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        return "MONTHNAME(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<last_day_of<P>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        return "LAST_DAY(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

// str_to_date_of is now a runtime class — no projection_traits needed

template <Projection P>
struct projection_traits<from_unixtime_of<P>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        return "FROM_UNIXTIME(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<unix_timestamp_of<P>> {
    using value_type = int64_t;
    [[nodiscard]] static std::string sql_expr() {
        return "UNIX_TIMESTAMP(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

// convert_tz_of is now a runtime class — no projection_traits needed

template <>
struct projection_traits<curtime_val> {
    using value_type = std::string;
    static constexpr std::string_view sql_expr() {
        return "CURTIME()";
    }
};

template <>
struct projection_traits<utc_timestamp_val> {
    using value_type = std::string;
    static constexpr std::string_view sql_expr() {
        return "UTC_TIMESTAMP()";
    }
};

template <Projection A, Projection B>
struct projection_traits<addtime_of<A, B>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        return "ADDTIME(" + std::string(projection_traits<A>::sql_expr()) + ", " +
               std::string(projection_traits<B>::sql_expr()) + ")";
    }
};

template <Projection A, Projection B>
struct projection_traits<subtime_of<A, B>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        return "SUBTIME(" + std::string(projection_traits<A>::sql_expr()) + ", " +
               std::string(projection_traits<B>::sql_expr()) + ")";
    }
};

template <Projection A, Projection B>
struct projection_traits<timediff_of<A, B>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        return "TIMEDIFF(" + std::string(projection_traits<A>::sql_expr()) + ", " +
               std::string(projection_traits<B>::sql_expr()) + ")";
    }
};

// ===================================================================
// JSON scalar function traits
// ===================================================================

// json_extract_of is now a runtime class — no projection_traits needed

template <Projection... Ps>
    requires(sizeof...(Ps) > 0)
struct projection_traits<json_object_of<Ps...>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        std::string s = "JSON_OBJECT(";
        bool first = true;
        (((s += (first ? "" : ", "), s += projection_traits<Ps>::sql_expr(), first = false)), ...);
        s += ")";
        return s;
    }
};

template <Projection... Ps>
    requires(sizeof...(Ps) > 0)
struct projection_traits<json_array_of<Ps...>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        std::string s = "JSON_ARRAY(";
        bool first = true;
        (((s += (first ? "" : ", "), s += projection_traits<Ps>::sql_expr(), first = false)), ...);
        s += ")";
        return s;
    }
};

// json_contains_of is now a runtime class — no projection_traits needed

template <Projection P>
struct projection_traits<json_length_of<P>> {
    using value_type = uint64_t;
    [[nodiscard]] static std::string sql_expr() {
        return "JSON_LENGTH(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<json_unquote_of<P>> {
    using value_type = std::string;
    [[nodiscard]] static std::string sql_expr() {
        return "JSON_UNQUOTE(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection Agg, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir, typename Frame>
struct projection_traits<window_func<Agg, PartitionCol, OrderCol, Dir, Frame>> {
    using value_type = typename projection_traits<Agg>::value_type;
    [[nodiscard]] static std::string sql_expr() {
        return std::string(projection_traits<Agg>::sql_expr()) + " OVER (PARTITION BY " +
               std::string(column_traits<PartitionCol>::column_name()) + " ORDER BY " +
               std::string(column_traits<OrderCol>::column_name()) + (Dir == sort_order::asc ? " ASC" : " DESC") +
               sql_window_frame_clause<Frame>() + ")";
    }
};

template <ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir, typename Frame>
struct projection_traits<row_number_over<PartitionCol, OrderCol, Dir, Frame>> {
    using value_type = uint64_t;
    [[nodiscard]] static std::string sql_expr() {
        return "ROW_NUMBER() OVER (PARTITION BY " + std::string(column_traits<PartitionCol>::column_name()) +
               " ORDER BY " + std::string(column_traits<OrderCol>::column_name()) +
               (Dir == sort_order::asc ? " ASC" : " DESC") + sql_window_frame_clause<Frame>() + ")";
    }
};

template <ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir, typename Frame>
struct projection_traits<rank_over<PartitionCol, OrderCol, Dir, Frame>> {
    using value_type = uint64_t;
    [[nodiscard]] static std::string sql_expr() {
        return "RANK() OVER (PARTITION BY " + std::string(column_traits<PartitionCol>::column_name()) + " ORDER BY " +
               std::string(column_traits<OrderCol>::column_name()) + (Dir == sort_order::asc ? " ASC" : " DESC") +
               sql_window_frame_clause<Frame>() + ")";
    }
};

template <ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir, typename Frame>
struct projection_traits<dense_rank_over<PartitionCol, OrderCol, Dir, Frame>> {
    using value_type = uint64_t;
    [[nodiscard]] static std::string sql_expr() {
        return "DENSE_RANK() OVER (PARTITION BY " + std::string(column_traits<PartitionCol>::column_name()) +
               " ORDER BY " + std::string(column_traits<OrderCol>::column_name()) +
               (Dir == sort_order::asc ? " ASC" : " DESC") + sql_window_frame_clause<Frame>() + ")";
    }
};

template <ColumnDescriptor Col, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, std::size_t Offset,
          sort_order Dir, typename Frame>
struct projection_traits<lag_over<Col, PartitionCol, OrderCol, Offset, Dir, Frame>> {
    using value_type = typename column_traits<Col>::value_type;
    [[nodiscard]] static std::string sql_expr() {
        return "LAG(" + std::string(column_traits<Col>::column_name()) + ", " + std::to_string(Offset) +
               ") OVER (PARTITION BY " + std::string(column_traits<PartitionCol>::column_name()) + " ORDER BY " +
               std::string(column_traits<OrderCol>::column_name()) + (Dir == sort_order::asc ? " ASC" : " DESC") +
               sql_window_frame_clause<Frame>() + ")";
    }
};

template <ColumnDescriptor Col, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, std::size_t Offset,
          sort_order Dir, typename Frame>
struct projection_traits<lead_over<Col, PartitionCol, OrderCol, Offset, Dir, Frame>> {
    using value_type = typename column_traits<Col>::value_type;
    [[nodiscard]] static std::string sql_expr() {
        return "LEAD(" + std::string(column_traits<Col>::column_name()) + ", " + std::to_string(Offset) +
               ") OVER (PARTITION BY " + std::string(column_traits<PartitionCol>::column_name()) + " ORDER BY " +
               std::string(column_traits<OrderCol>::column_name()) + (Dir == sort_order::asc ? " ASC" : " DESC") +
               sql_window_frame_clause<Frame>() + ")";
    }
};

template <std::size_t N, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir, typename Frame>
struct projection_traits<ntile_over<N, PartitionCol, OrderCol, Dir, Frame>> {
    using value_type = uint64_t;
    [[nodiscard]] static std::string sql_expr() {
        return "NTILE(" + std::to_string(N) + ") OVER (PARTITION BY " +
               std::string(column_traits<PartitionCol>::column_name()) + " ORDER BY " +
               std::string(column_traits<OrderCol>::column_name()) + (Dir == sort_order::asc ? " ASC" : " DESC") +
               sql_window_frame_clause<Frame>() + ")";
    }
};

template <ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir, typename Frame>
struct projection_traits<percent_rank_over<PartitionCol, OrderCol, Dir, Frame>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "PERCENT_RANK() OVER (PARTITION BY " + std::string(column_traits<PartitionCol>::column_name()) +
               " ORDER BY " + std::string(column_traits<OrderCol>::column_name()) +
               (Dir == sort_order::asc ? " ASC" : " DESC") + sql_window_frame_clause<Frame>() + ")";
    }
};

template <ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir, typename Frame>
struct projection_traits<cume_dist_over<PartitionCol, OrderCol, Dir, Frame>> {
    using value_type = double;
    [[nodiscard]] static std::string sql_expr() {
        return "CUME_DIST() OVER (PARTITION BY " + std::string(column_traits<PartitionCol>::column_name()) +
               " ORDER BY " + std::string(column_traits<OrderCol>::column_name()) +
               (Dir == sort_order::asc ? " ASC" : " DESC") + sql_window_frame_clause<Frame>() + ")";
    }
};

template <ColumnDescriptor Col, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir,
          typename Frame>
struct projection_traits<first_value_over<Col, PartitionCol, OrderCol, Dir, Frame>> {
    using value_type = typename column_traits<Col>::value_type;
    [[nodiscard]] static std::string sql_expr() {
        return "FIRST_VALUE(" + std::string(column_traits<Col>::column_name()) + ") OVER (PARTITION BY " +
               std::string(column_traits<PartitionCol>::column_name()) + " ORDER BY " +
               std::string(column_traits<OrderCol>::column_name()) + (Dir == sort_order::asc ? " ASC" : " DESC") +
               sql_window_frame_clause<Frame>() + ")";
    }
};

template <ColumnDescriptor Col, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir,
          typename Frame>
struct projection_traits<last_value_over<Col, PartitionCol, OrderCol, Dir, Frame>> {
    using value_type = typename column_traits<Col>::value_type;
    [[nodiscard]] static std::string sql_expr() {
        return "LAST_VALUE(" + std::string(column_traits<Col>::column_name()) + ") OVER (PARTITION BY " +
               std::string(column_traits<PartitionCol>::column_name()) + " ORDER BY " +
               std::string(column_traits<OrderCol>::column_name()) + (Dir == sort_order::asc ? " ASC" : " DESC") +
               sql_window_frame_clause<Frame>() + ")";
    }
};

template <ColumnDescriptor Col, std::size_t N, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir,
          typename Frame>
struct projection_traits<nth_value_over<Col, N, PartitionCol, OrderCol, Dir, Frame>> {
    using value_type = typename column_traits<Col>::value_type;
    [[nodiscard]] static std::string sql_expr() {
        return "NTH_VALUE(" + std::string(column_traits<Col>::column_name()) + ", " + std::to_string(N) +
               ") OVER (PARTITION BY " + std::string(column_traits<PartitionCol>::column_name()) + " ORDER BY " +
               std::string(column_traits<OrderCol>::column_name()) + (Dir == sort_order::asc ? " ASC" : " DESC") +
               sql_window_frame_clause<Frame>() + ")";
    }
};

// ===================================================================
// agg_expr<Agg> — natural operator and method syntax for HAVING predicates
//
// Wraps an aggregate projection type and exposes comparison operators and
// named methods, each returning a sql_predicate suitable for .having().
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
// The resulting sql_predicate composes with &, |, ! operators.
// ===================================================================

template <typename Agg>
struct agg_expr {
    using value_type = typename projection_traits<Agg>::value_type;

    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] sql_predicate operator==(V const& val) const {
        return {{},
                {},
                std::string(projection_traits<Agg>::sql_expr()) + " = " + sql_detail::to_sql_value(value_type{val})};
    }
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] sql_predicate operator!=(V const& val) const {
        return {{},
                {},
                std::string(projection_traits<Agg>::sql_expr()) + " != " + sql_detail::to_sql_value(value_type{val})};
    }
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] sql_predicate operator<(V const& val) const {
        return {{},
                {},
                std::string(projection_traits<Agg>::sql_expr()) + " < " + sql_detail::to_sql_value(value_type{val})};
    }
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] sql_predicate operator>(V const& val) const {
        return {{},
                {},
                std::string(projection_traits<Agg>::sql_expr()) + " > " + sql_detail::to_sql_value(value_type{val})};
    }
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] sql_predicate operator<=(V const& val) const {
        return {{},
                {},
                std::string(projection_traits<Agg>::sql_expr()) + " <= " + sql_detail::to_sql_value(value_type{val})};
    }
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] sql_predicate operator>=(V const& val) const {
        return {{},
                {},
                std::string(projection_traits<Agg>::sql_expr()) + " >= " + sql_detail::to_sql_value(value_type{val})};
    }

    // Named method equivalents
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] sql_predicate equal_to(V const& val) const {
        return *this == val;
    }
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] sql_predicate not_equal_to(V const& val) const {
        return *this != val;
    }
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] sql_predicate less_than(V const& val) const {
        return *this < val;
    }
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] sql_predicate greater_than(V const& val) const {
        return *this > val;
    }
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] sql_predicate less_than_or_equal(V const& val) const {
        return *this <= val;
    }
    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] sql_predicate greater_than_or_equal(V const& val) const {
        return *this >= val;
    }

    template <typename V>
        requires std::constructible_from<value_type, V const&>
    [[nodiscard]] sql_predicate between(V const& low, V const& high) const {
        return {{},
                {},
                std::string(projection_traits<Agg>::sql_expr()) + " BETWEEN " +
                    sql_detail::to_sql_value(value_type{low}) + " AND " + sql_detail::to_sql_value(value_type{high})};
    }

    [[nodiscard]] sql_predicate in(std::initializer_list<value_type> vals) const {
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

    [[nodiscard]] sql_predicate is_null() const noexcept {
        return {{}, {}, std::string(projection_traits<Agg>::sql_expr()) + " IS NULL"};
    }

    [[nodiscard]] sql_predicate is_not_null() const noexcept {
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
// These compose with &, |, ! like any other sql_predicate.
// ===================================================================

template <AggregateProjection Agg, typename V>
    requires std::constructible_from<typename projection_traits<Agg>::value_type, V const&>
[[nodiscard]] sql_predicate operator==(Agg const&, V const& val) {
    using vt = typename projection_traits<Agg>::value_type;
    return {{}, {}, std::string(projection_traits<Agg>::sql_expr()) + " = " + sql_detail::to_sql_value(vt{val})};
}

template <AggregateProjection Agg, typename V>
    requires std::constructible_from<typename projection_traits<Agg>::value_type, V const&>
[[nodiscard]] sql_predicate operator!=(Agg const&, V const& val) {
    using vt = typename projection_traits<Agg>::value_type;
    return {{}, {}, std::string(projection_traits<Agg>::sql_expr()) + " != " + sql_detail::to_sql_value(vt{val})};
}

template <AggregateProjection Agg, typename V>
    requires std::constructible_from<typename projection_traits<Agg>::value_type, V const&>
[[nodiscard]] sql_predicate operator<(Agg const&, V const& val) {
    using vt = typename projection_traits<Agg>::value_type;
    return {{}, {}, std::string(projection_traits<Agg>::sql_expr()) + " < " + sql_detail::to_sql_value(vt{val})};
}

template <AggregateProjection Agg, typename V>
    requires std::constructible_from<typename projection_traits<Agg>::value_type, V const&>
[[nodiscard]] sql_predicate operator>(Agg const&, V const& val) {
    using vt = typename projection_traits<Agg>::value_type;
    return {{}, {}, std::string(projection_traits<Agg>::sql_expr()) + " > " + sql_detail::to_sql_value(vt{val})};
}

template <AggregateProjection Agg, typename V>
    requires std::constructible_from<typename projection_traits<Agg>::value_type, V const&>
[[nodiscard]] sql_predicate operator<=(Agg const&, V const& val) {
    using vt = typename projection_traits<Agg>::value_type;
    return {{}, {}, std::string(projection_traits<Agg>::sql_expr()) + " <= " + sql_detail::to_sql_value(vt{val})};
}

template <AggregateProjection Agg, typename V>
    requires std::constructible_from<typename projection_traits<Agg>::value_type, V const&>
[[nodiscard]] sql_predicate operator>=(Agg const&, V const& val) {
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
    } else if constexpr (is_formatted_numeric_type_v<T>) {
        return T{from_mysql_value_nonnull<typename T::underlying_type>(sv)};
    } else if constexpr (std::same_as<T, float>) {
        return std::stof(std::string{sv});
    } else if constexpr (std::same_as<T, double>) {
        return std::stod(std::string{sv});
    } else if constexpr (std::same_as<T, bool>) {
        return sv != "0" && sv != "false" && !sv.empty();
    } else if constexpr (std::same_as<T, std::string>) {
        return std::string{sv};
    } else if constexpr (is_varchar_type_v<T>) {
        return T::create(sv).value_or(T{});
    } else if constexpr (is_text_type_v<T>) {
        return T{sv};
    } else if constexpr (std::same_as<T, std::chrono::system_clock::time_point>) {
        std::istringstream ss{std::string{sv}};
        std::tm tm{};
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        return std::chrono::system_clock::from_time_t(std::mktime(&tm));
    } else if constexpr (is_time_type_v<T>) {
        bool const negative = !sv.empty() && sv[0] == '-';
        std::string_view const s = negative ? sv.substr(1) : sv;
        auto const c1 = s.find(':');
        auto const c2 = s.find(':', c1 + 1);
        int64_t const hours = std::stoll(std::string{s.substr(0, c1)});
        int64_t const mins = std::stoll(std::string{s.substr(c1 + 1, c2 - c1 - 1)});
        auto const secs_frac = s.substr(c2 + 1);
        auto const dot = secs_frac.find('.');
        int64_t const secs =
            std::stoll(std::string{secs_frac.substr(0, dot == std::string_view::npos ? secs_frac.size() : dot)});
        int64_t frac_us = 0;
        if (dot != std::string_view::npos) {
            std::string frac{secs_frac.substr(dot + 1)};
            frac.resize(6, '0');
            frac_us = std::stoll(frac);
        }
        int64_t total_us = (hours * 3600LL + mins * 60LL + secs) * 1000000LL + frac_us;
        if (negative)
            total_us = -total_us;
        return T{std::chrono::microseconds{total_us}};
    } else {
        static_assert(false,
                      "Unsupported type for MySQL deserialization. "
                      "Supported: uint32_t, int32_t, uint64_t, int64_t, float, double, float_type<P,S>, "
                      "double_type<P,S>, decimal_type<P,S>, bool, std::string, varchar_type<N>, text_type, "
                      "std::chrono::system_clock::time_point (for DATETIME/TIMESTAMP), time_type, "
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

// ===================================================================
// proj_value_type_t / proj_sql_expr — uniform access for Projection and InstanceProjection
// ===================================================================

template <typename P>
struct proj_value_type {
    using type = typename projection_traits<P>::value_type;
};

template <InstanceProjection P>
struct proj_value_type<P> {
    using type = typename P::value_type;
};

template <typename P>
using proj_value_type_t = typename proj_value_type<P>::type;

template <Projection P>
[[nodiscard]] std::string proj_sql_expr(P const&) {
    return std::string(projection_traits<P>::sql_expr());
}

template <InstanceProjection P>
[[nodiscard]] std::string proj_sql_expr(P const& p) {
    return p.sql_expr();
}

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

enum class select_lock_mode {
    none,
    for_update,
    for_share,
    lock_in_share_mode,
};

// ===================================================================
// select_query_builder — unified builder for all SELECT clauses
// ===================================================================

template <typename Table, typename... Projs>
struct select_query_builder {
    using result_row_type = std::tuple<proj_value_type_t<Projs>...>;

    select_query_builder() = default;

    explicit select_query_builder(std::tuple<Projs...> projs) : projs_(std::move(projs)) {
    }

    std::tuple<Projs...> projs_;

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

    [[nodiscard]] select_query_builder where(sql_predicate cond) const& {
        auto copy = *this;
        copy.where_ = std::move(cond);
        return copy;
    }
    [[nodiscard]] select_query_builder where(sql_predicate cond) && {
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
    [[nodiscard]] select_query_builder group_by(Specs...) const& {
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
    [[nodiscard]] select_query_builder group_by(Specs...) && {
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

    [[nodiscard]] select_query_builder having(sql_predicate cond) const& {
        auto copy = *this;
        copy.having_ = std::move(cond);
        return copy;
    }
    [[nodiscard]] select_query_builder having(sql_predicate cond) && {
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
    template <typename Spec>
        requires(ColumnDescriptor<Spec> || NullsWrapper<Spec> || QualifiedCol<Spec> || PositionWrapper<Spec> ||
                 ColIndexWrapper<Spec> || Projection<Spec> || DescOrder<Spec>)
    [[nodiscard]] select_query_builder order_by(Spec) const& {
        auto copy = *this;
        if constexpr (DescOrder<Spec>) {
            using ColSpec = typename Spec::col_spec;
            constexpr sort_order Dir = sort_order::desc;
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
                              "order_by(position<Proj>): Proj must be one of the projections in this SELECT");
                static constexpr std::size_t Index =
                    sql_detail::proj_index_in_pack<typename ColSpec::proj_type, Projs...>() + 1;
                copy.order_by_clauses_.push_back(std::to_string(Index) + (Dir == sort_order::asc ? " ASC" : " DESC"));
            } else if constexpr (ColIndexWrapper<ColSpec>) {
                static_assert(ColSpec::value <= sizeof...(Projs),
                              "order_by(col_index<N>): N is out of range for this SELECT list");
                copy.order_by_clauses_.push_back(std::to_string(ColSpec::value) +
                                                 (Dir == sort_order::asc ? " ASC" : " DESC"));
            } else if constexpr (Projection<ColSpec>) {
                copy.order_by_clauses_.push_back(std::string(projection_traits<ColSpec>::sql_expr()) +
                                                 (Dir == sort_order::asc ? " ASC" : " DESC"));
            } else {
                copy.order_by_clauses_.push_back(std::string(column_traits<ColSpec>::column_name()) +
                                                 (Dir == sort_order::asc ? " ASC" : " DESC"));
            }
        } else {
            using ColSpec = Spec;
            constexpr sort_order Dir = sort_order::asc;
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
                              "order_by(position<Proj>): Proj must be one of the projections in this SELECT");
                static constexpr std::size_t Index =
                    sql_detail::proj_index_in_pack<typename ColSpec::proj_type, Projs...>() + 1;
                copy.order_by_clauses_.push_back(std::to_string(Index) + (Dir == sort_order::asc ? " ASC" : " DESC"));
            } else if constexpr (ColIndexWrapper<ColSpec>) {
                static_assert(ColSpec::value <= sizeof...(Projs),
                              "order_by(col_index<N>): N is out of range for this SELECT list");
                copy.order_by_clauses_.push_back(std::to_string(ColSpec::value) +
                                                 (Dir == sort_order::asc ? " ASC" : " DESC"));
            } else if constexpr (Projection<ColSpec>) {
                copy.order_by_clauses_.push_back(std::string(projection_traits<ColSpec>::sql_expr()) +
                                                 (Dir == sort_order::asc ? " ASC" : " DESC"));
            } else {
                copy.order_by_clauses_.push_back(std::string(column_traits<ColSpec>::column_name()) +
                                                 (Dir == sort_order::asc ? " ASC" : " DESC"));
            }
        }
        return copy;
    }
    template <typename Spec>
        requires(ColumnDescriptor<Spec> || NullsWrapper<Spec> || QualifiedCol<Spec> || PositionWrapper<Spec> ||
                 ColIndexWrapper<Spec> || Projection<Spec> || DescOrder<Spec>)
    [[nodiscard]] select_query_builder order_by(Spec) && {
        if constexpr (DescOrder<Spec>) {
            using ColSpec = typename Spec::col_spec;
            constexpr sort_order Dir = sort_order::desc;
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
                              "order_by(position<Proj>): Proj must be one of the projections in this SELECT");
                static constexpr std::size_t Index =
                    sql_detail::proj_index_in_pack<typename ColSpec::proj_type, Projs...>() + 1;
                order_by_clauses_.push_back(std::to_string(Index) + (Dir == sort_order::asc ? " ASC" : " DESC"));
            } else if constexpr (ColIndexWrapper<ColSpec>) {
                static_assert(ColSpec::value <= sizeof...(Projs),
                              "order_by(col_index<N>): N is out of range for this SELECT list");
                order_by_clauses_.push_back(std::to_string(ColSpec::value) +
                                            (Dir == sort_order::asc ? " ASC" : " DESC"));
            } else if constexpr (Projection<ColSpec>) {
                order_by_clauses_.push_back(std::string(projection_traits<ColSpec>::sql_expr()) +
                                            (Dir == sort_order::asc ? " ASC" : " DESC"));
            } else {
                order_by_clauses_.push_back(std::string(column_traits<ColSpec>::column_name()) +
                                            (Dir == sort_order::asc ? " ASC" : " DESC"));
            }
        } else {
            using ColSpec = Spec;
            constexpr sort_order Dir = sort_order::asc;
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
                              "order_by(position<Proj>): Proj must be one of the projections in this SELECT");
                static constexpr std::size_t Index =
                    sql_detail::proj_index_in_pack<typename ColSpec::proj_type, Projs...>() + 1;
                order_by_clauses_.push_back(std::to_string(Index) + (Dir == sort_order::asc ? " ASC" : " DESC"));
            } else if constexpr (ColIndexWrapper<ColSpec>) {
                static_assert(ColSpec::value <= sizeof...(Projs),
                              "order_by(col_index<N>): N is out of range for this SELECT list");
                order_by_clauses_.push_back(std::to_string(ColSpec::value) +
                                            (Dir == sort_order::asc ? " ASC" : " DESC"));
            } else if constexpr (Projection<ColSpec>) {
                order_by_clauses_.push_back(std::string(projection_traits<ColSpec>::sql_expr()) +
                                            (Dir == sort_order::asc ? " ASC" : " DESC"));
            } else {
                order_by_clauses_.push_back(std::string(column_traits<ColSpec>::column_name()) +
                                            (Dir == sort_order::asc ? " ASC" : " DESC"));
            }
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
    template <FieldOrderBy ColSpec, SqlValue ValueType>
    [[nodiscard]] select_query_builder order_by(ColSpec, std::initializer_list<ValueType> values,
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
    template <FieldOrderBy ColSpec, SqlValue ValueType>
    [[nodiscard]] select_query_builder order_by(ColSpec, std::initializer_list<ValueType> values,
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
    template <AnyProjection Proj>
        requires((std::is_same_v<Proj, Projs> || ...))
    [[nodiscard]] select_query_builder order_by_alias(Proj) const& {
        static constexpr std::size_t Index = sql_detail::proj_index_in_pack<Proj, Projs...>();
        auto copy = *this;
        copy.order_by_clauses_.push_back(
            alias_order_entry{Index, proj_sql_expr(std::get<Index>(projs_)), sort_order::asc});
        return copy;
    }
    template <AnyProjection Proj>
        requires((std::is_same_v<Proj, Projs> || ...))
    [[nodiscard]] select_query_builder order_by_alias(Proj) && {
        static constexpr std::size_t Index = sql_detail::proj_index_in_pack<Proj, Projs...>();
        order_by_clauses_.push_back(alias_order_entry{Index, proj_sql_expr(std::get<Index>(projs_)), sort_order::asc});
        return std::move(*this);
    }
    template <AnyProjection Proj>
        requires((std::is_same_v<Proj, Projs> || ...))
    [[nodiscard]] select_query_builder order_by_alias(desc_order<Proj>) const& {
        static constexpr std::size_t Index = sql_detail::proj_index_in_pack<Proj, Projs...>();
        auto copy = *this;
        copy.order_by_clauses_.push_back(
            alias_order_entry{Index, proj_sql_expr(std::get<Index>(projs_)), sort_order::desc});
        return copy;
    }
    template <AnyProjection Proj>
        requires((std::is_same_v<Proj, Projs> || ...))
    [[nodiscard]] select_query_builder order_by_alias(desc_order<Proj>) && {
        static constexpr std::size_t Index = sql_detail::proj_index_in_pack<Proj, Projs...>();
        order_by_clauses_.push_back(alias_order_entry{Index, proj_sql_expr(std::get<Index>(projs_)), sort_order::desc});
        return std::move(*this);
    }

    // with_alias<Proj>("name") — add AS alias to the projection matching type Proj
    //
    // Proj must be one of the types in Projs...; a mismatch is a compile error.
    // Example: .with_alias<sum<product::price_val>>("total")
    template <AnyProjection Proj>
        requires((std::is_same_v<Proj, Projs> || ...))
    [[nodiscard]] select_query_builder with_alias(Proj, std::string alias) const& {
        static constexpr std::size_t Index = sql_detail::proj_index_in_pack<Proj, Projs...>();
        auto copy = *this;
        copy.aliases_[Index] = std::move(alias);
        return copy;
    }
    template <AnyProjection Proj>
        requires((std::is_same_v<Proj, Projs> || ...))
    [[nodiscard]] select_query_builder with_alias(Proj, std::string alias) && {
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

    [[nodiscard]] select_query_builder for_update() const& {
        auto copy = *this;
        copy.lock_mode_ = select_lock_mode::for_update;
        return copy;
    }
    [[nodiscard]] select_query_builder for_update() && {
        lock_mode_ = select_lock_mode::for_update;
        return std::move(*this);
    }

    [[nodiscard]] select_query_builder for_share() const& {
        auto copy = *this;
        copy.lock_mode_ = select_lock_mode::for_share;
        return copy;
    }
    [[nodiscard]] select_query_builder for_share() && {
        lock_mode_ = select_lock_mode::for_share;
        return std::move(*this);
    }

    [[nodiscard]] select_query_builder lock_in_share_mode() const& {
        auto copy = *this;
        copy.lock_mode_ = select_lock_mode::lock_in_share_mode;
        return copy;
    }
    [[nodiscard]] select_query_builder lock_in_share_mode() && {
        lock_mode_ = select_lock_mode::lock_in_share_mode;
        return std::move(*this);
    }

    // ---- JOIN clauses ----
    //
    // Use col<Table, Index> descriptors for the join columns to produce
    // table-qualified ON conditions (e.g. "symbol.id = price.symbol_id").
    // ColumnFieldType descriptors (e.g. symbol::id) produce bare column names.

    template <typename RightTable, ColumnDescriptor LeftCol, ColumnDescriptor RightCol>
    [[nodiscard]] select_query_builder inner_join(RightTable, LeftCol, RightCol) const& {
        return add_join("INNER JOIN", table_name_for<RightTable>::value().to_string_view(),
                        qualified_col_name<LeftCol>(), qualified_col_name<RightCol>());
    }
    template <typename RightTable, ColumnDescriptor LeftCol, ColumnDescriptor RightCol>
    [[nodiscard]] select_query_builder inner_join(RightTable, LeftCol, RightCol) && {
        return std::move(*this).add_join("INNER JOIN", table_name_for<RightTable>::value().to_string_view(),
                                         qualified_col_name<LeftCol>(), qualified_col_name<RightCol>());
    }
    template <typename RightTable>
    [[nodiscard]] select_query_builder inner_join(RightTable, sql_predicate on_cond) const& {
        return add_join_on("INNER JOIN", table_name_for<RightTable>::value().to_string_view(), std::move(on_cond));
    }
    template <typename RightTable>
    [[nodiscard]] select_query_builder inner_join(RightTable, sql_predicate on_cond) && {
        return std::move(*this).add_join_on("INNER JOIN", table_name_for<RightTable>::value().to_string_view(),
                                            std::move(on_cond));
    }

    template <typename RightTable, ColumnDescriptor LeftCol, ColumnDescriptor RightCol>
    [[nodiscard]] select_query_builder left_join(RightTable, LeftCol, RightCol) const& {
        return add_join("LEFT JOIN", table_name_for<RightTable>::value().to_string_view(),
                        qualified_col_name<LeftCol>(), qualified_col_name<RightCol>());
    }
    template <typename RightTable, ColumnDescriptor LeftCol, ColumnDescriptor RightCol>
    [[nodiscard]] select_query_builder left_join(RightTable, LeftCol, RightCol) && {
        return std::move(*this).add_join("LEFT JOIN", table_name_for<RightTable>::value().to_string_view(),
                                         qualified_col_name<LeftCol>(), qualified_col_name<RightCol>());
    }
    template <typename RightTable>
    [[nodiscard]] select_query_builder left_join(RightTable, sql_predicate on_cond) const& {
        return add_join_on("LEFT JOIN", table_name_for<RightTable>::value().to_string_view(), std::move(on_cond));
    }
    template <typename RightTable>
    [[nodiscard]] select_query_builder left_join(RightTable, sql_predicate on_cond) && {
        return std::move(*this).add_join_on("LEFT JOIN", table_name_for<RightTable>::value().to_string_view(),
                                            std::move(on_cond));
    }

    template <typename RightTable, ColumnDescriptor LeftCol, ColumnDescriptor RightCol>
    [[nodiscard]] select_query_builder right_join(RightTable, LeftCol, RightCol) const& {
        return add_join("RIGHT JOIN", table_name_for<RightTable>::value().to_string_view(),
                        qualified_col_name<LeftCol>(), qualified_col_name<RightCol>());
    }
    template <typename RightTable, ColumnDescriptor LeftCol, ColumnDescriptor RightCol>
    [[nodiscard]] select_query_builder right_join(RightTable, LeftCol, RightCol) && {
        return std::move(*this).add_join("RIGHT JOIN", table_name_for<RightTable>::value().to_string_view(),
                                         qualified_col_name<LeftCol>(), qualified_col_name<RightCol>());
    }
    template <typename RightTable>
    [[nodiscard]] select_query_builder right_join(RightTable, sql_predicate on_cond) const& {
        return add_join_on("RIGHT JOIN", table_name_for<RightTable>::value().to_string_view(), std::move(on_cond));
    }
    template <typename RightTable>
    [[nodiscard]] select_query_builder right_join(RightTable, sql_predicate on_cond) && {
        return std::move(*this).add_join_on("RIGHT JOIN", table_name_for<RightTable>::value().to_string_view(),
                                            std::move(on_cond));
    }

    // full_join — FULL OUTER JOIN
    template <typename RightTable, ColumnDescriptor LeftCol, ColumnDescriptor RightCol>
    [[nodiscard]] select_query_builder full_join(RightTable, LeftCol, RightCol) const& {
        return add_join("FULL OUTER JOIN", table_name_for<RightTable>::value().to_string_view(),
                        qualified_col_name<LeftCol>(), qualified_col_name<RightCol>());
    }
    template <typename RightTable, ColumnDescriptor LeftCol, ColumnDescriptor RightCol>
    [[nodiscard]] select_query_builder full_join(RightTable, LeftCol, RightCol) && {
        return std::move(*this).add_join("FULL OUTER JOIN", table_name_for<RightTable>::value().to_string_view(),
                                         qualified_col_name<LeftCol>(), qualified_col_name<RightCol>());
    }
    template <typename RightTable>
    [[nodiscard]] select_query_builder full_join(RightTable, sql_predicate on_cond) const& {
        return add_join_on("FULL OUTER JOIN", table_name_for<RightTable>::value().to_string_view(), std::move(on_cond));
    }
    template <typename RightTable>
    [[nodiscard]] select_query_builder full_join(RightTable, sql_predicate on_cond) && {
        return std::move(*this).add_join_on("FULL OUTER JOIN", table_name_for<RightTable>::value().to_string_view(),
                                            std::move(on_cond));
    }

    // cross_join — CROSS JOIN (no ON clause)
    template <typename RightTable>
    [[nodiscard]] select_query_builder cross_join(RightTable) const& {
        auto copy = *this;
        copy.joins_ += " CROSS JOIN " + std::string(table_name_for<RightTable>::value().to_string_view());
        return copy;
    }
    template <typename RightTable>
    [[nodiscard]] select_query_builder cross_join(RightTable) && {
        joins_ += " CROSS JOIN " + std::string(table_name_for<RightTable>::value().to_string_view());
        return std::move(*this);
    }

    // natural_join — NATURAL JOIN (implicit column matching, no ON/USING clause)
    template <typename RightTable>
    [[nodiscard]] select_query_builder natural_join(RightTable) const& {
        auto copy = *this;
        copy.joins_ += " NATURAL JOIN " + std::string(table_name_for<RightTable>::value().to_string_view());
        return copy;
    }
    template <typename RightTable>
    [[nodiscard]] select_query_builder natural_join(RightTable) && {
        joins_ += " NATURAL JOIN " + std::string(table_name_for<RightTable>::value().to_string_view());
        return std::move(*this);
    }

    // natural_left_join — NATURAL LEFT JOIN
    template <typename RightTable>
    [[nodiscard]] select_query_builder natural_left_join(RightTable) const& {
        auto copy = *this;
        copy.joins_ += " NATURAL LEFT JOIN " + std::string(table_name_for<RightTable>::value().to_string_view());
        return copy;
    }
    template <typename RightTable>
    [[nodiscard]] select_query_builder natural_left_join(RightTable) && {
        joins_ += " NATURAL LEFT JOIN " + std::string(table_name_for<RightTable>::value().to_string_view());
        return std::move(*this);
    }

    // natural_right_join — NATURAL RIGHT JOIN
    template <typename RightTable>
    [[nodiscard]] select_query_builder natural_right_join(RightTable) const& {
        auto copy = *this;
        copy.joins_ += " NATURAL RIGHT JOIN " + std::string(table_name_for<RightTable>::value().to_string_view());
        return copy;
    }
    template <typename RightTable>
    [[nodiscard]] select_query_builder natural_right_join(RightTable) && {
        joins_ += " NATURAL RIGHT JOIN " + std::string(table_name_for<RightTable>::value().to_string_view());
        return std::move(*this);
    }

    // join_using — JOIN ... USING (col1, col2, ...) variants
    template <typename RightTable, ColumnDescriptor... UsingCols>
    [[nodiscard]] select_query_builder inner_join_using(RightTable, UsingCols...) const& {
        return add_join_using("INNER JOIN", table_name_for<RightTable>::value().to_string_view(),
                              build_using_cols_string<UsingCols...>());
    }
    template <typename RightTable, ColumnDescriptor... UsingCols>
    [[nodiscard]] select_query_builder inner_join_using(RightTable, UsingCols...) && {
        return std::move(*this).add_join_using("INNER JOIN", table_name_for<RightTable>::value().to_string_view(),
                                               build_using_cols_string<UsingCols...>());
    }

    template <typename RightTable, ColumnDescriptor... UsingCols>
    [[nodiscard]] select_query_builder left_join_using(RightTable, UsingCols...) const& {
        return add_join_using("LEFT JOIN", table_name_for<RightTable>::value().to_string_view(),
                              build_using_cols_string<UsingCols...>());
    }
    template <typename RightTable, ColumnDescriptor... UsingCols>
    [[nodiscard]] select_query_builder left_join_using(RightTable, UsingCols...) && {
        return std::move(*this).add_join_using("LEFT JOIN", table_name_for<RightTable>::value().to_string_view(),
                                               build_using_cols_string<UsingCols...>());
    }

    template <typename RightTable, ColumnDescriptor... UsingCols>
    [[nodiscard]] select_query_builder right_join_using(RightTable, UsingCols...) const& {
        return add_join_using("RIGHT JOIN", table_name_for<RightTable>::value().to_string_view(),
                              build_using_cols_string<UsingCols...>());
    }
    template <typename RightTable, ColumnDescriptor... UsingCols>
    [[nodiscard]] select_query_builder right_join_using(RightTable, UsingCols...) && {
        return std::move(*this).add_join_using("RIGHT JOIN", table_name_for<RightTable>::value().to_string_view(),
                                               build_using_cols_string<UsingCols...>());
    }

    template <typename RightTable, ColumnDescriptor... UsingCols>
    [[nodiscard]] select_query_builder full_join_using(RightTable, UsingCols...) const& {
        return add_join_using("FULL OUTER JOIN", table_name_for<RightTable>::value().to_string_view(),
                              build_using_cols_string<UsingCols...>());
    }
    template <typename RightTable, ColumnDescriptor... UsingCols>
    [[nodiscard]] select_query_builder full_join_using(RightTable, UsingCols...) && {
        return std::move(*this).add_join_using("FULL OUTER JOIN", table_name_for<RightTable>::value().to_string_view(),
                                               build_using_cols_string<UsingCols...>());
    }

    // lateral_join — JOIN LATERAL (subquery) AS alias (MySQL 8.0+)
    // Pass a pre-built SQL string as the subquery (e.g. inner_query.build_sql()).
    [[nodiscard]] select_query_builder lateral_join(std::string subquery_sql, std::string_view alias) const& {
        auto copy = *this;
        copy.joins_ += " JOIN LATERAL (" + std::move(subquery_sql) + ") AS " + std::string(alias);
        return copy;
    }
    [[nodiscard]] select_query_builder lateral_join(std::string subquery_sql, std::string_view alias) && {
        joins_ += " JOIN LATERAL (" + std::move(subquery_sql) + ") AS " + std::string(alias);
        return std::move(*this);
    }

    // left_lateral_join — LEFT JOIN LATERAL (subquery) AS alias (MySQL 8.0+)
    [[nodiscard]] select_query_builder left_lateral_join(std::string subquery_sql, std::string_view alias) const& {
        auto copy = *this;
        copy.joins_ += " LEFT JOIN LATERAL (" + std::move(subquery_sql) + ") AS " + std::string(alias);
        return copy;
    }
    [[nodiscard]] select_query_builder left_lateral_join(std::string subquery_sql, std::string_view alias) && {
        joins_ += " LEFT JOIN LATERAL (" + std::move(subquery_sql) + ") AS " + std::string(alias);
        return std::move(*this);
    }

    // lateral_join_on — JOIN LATERAL (subquery) AS alias ON condition
    [[nodiscard]] select_query_builder lateral_join_on(std::string subquery_sql, std::string_view alias,
                                                       sql_predicate on_cond) const& {
        auto copy = *this;
        copy.joins_ +=
            " JOIN LATERAL (" + std::move(subquery_sql) + ") AS " + std::string(alias) + " ON " + on_cond.build_sql();
        return copy;
    }
    [[nodiscard]] select_query_builder lateral_join_on(std::string subquery_sql, std::string_view alias,
                                                       sql_predicate on_cond) && {
        joins_ +=
            " JOIN LATERAL (" + std::move(subquery_sql) + ") AS " + std::string(alias) + " ON " + on_cond.build_sql();
        return std::move(*this);
    }

    // left_lateral_join_on — LEFT JOIN LATERAL (subquery) AS alias ON condition
    [[nodiscard]] select_query_builder left_lateral_join_on(std::string subquery_sql, std::string_view alias,
                                                            sql_predicate on_cond) const& {
        auto copy = *this;
        copy.joins_ += " LEFT JOIN LATERAL (" + std::move(subquery_sql) + ") AS " + std::string(alias) + " ON " +
                       on_cond.build_sql();
        return copy;
    }
    [[nodiscard]] select_query_builder left_lateral_join_on(std::string subquery_sql, std::string_view alias,
                                                            sql_predicate on_cond) && {
        joins_ += " LEFT JOIN LATERAL (" + std::move(subquery_sql) + ") AS " + std::string(alias) + " ON " +
                  on_cond.build_sql();
        return std::move(*this);
    }

    // straight_join — STRAIGHT_JOIN (MySQL optimizer hint: forces left-to-right join order)
    template <typename RightTable, ColumnDescriptor LeftCol, ColumnDescriptor RightCol>
    [[nodiscard]] select_query_builder straight_join(RightTable, LeftCol, RightCol) const& {
        return add_join("STRAIGHT_JOIN", table_name_for<RightTable>::value().to_string_view(),
                        qualified_col_name<LeftCol>(), qualified_col_name<RightCol>());
    }
    template <typename RightTable, ColumnDescriptor LeftCol, ColumnDescriptor RightCol>
    [[nodiscard]] select_query_builder straight_join(RightTable, LeftCol, RightCol) && {
        return std::move(*this).add_join("STRAIGHT_JOIN", table_name_for<RightTable>::value().to_string_view(),
                                         qualified_col_name<LeftCol>(), qualified_col_name<RightCol>());
    }
    template <typename RightTable>
    [[nodiscard]] select_query_builder straight_join(RightTable, sql_predicate on_cond) const& {
        return add_join_on("STRAIGHT_JOIN", table_name_for<RightTable>::value().to_string_view(), std::move(on_cond));
    }
    template <typename RightTable>
    [[nodiscard]] select_query_builder straight_join(RightTable, sql_predicate on_cond) && {
        return std::move(*this).add_join_on("STRAIGHT_JOIN", table_name_for<RightTable>::value().to_string_view(),
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
        std::apply(
            [&](auto const&... projs) {
                (
                    [&](auto const& p) {
                        if (!first)
                            sql += ", ";
                        sql += proj_sql_expr(p);
                        auto it = aliases_.find(proj_idx);
                        if (it != aliases_.end()) {
                            sql += " AS ";
                            sql += it->second;
                        }
                        first = false;
                        ++proj_idx;
                    }(projs),
                    ...);
            },
            projs_);

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
        if (lock_mode_ == select_lock_mode::for_update) {
            sql += " FOR UPDATE";
        } else if (lock_mode_ == select_lock_mode::for_share) {
            sql += " FOR SHARE";
        } else if (lock_mode_ == select_lock_mode::lock_in_share_mode) {
            sql += " LOCK IN SHARE MODE";
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
                                                   sql_predicate on_cond) const& {
        auto copy = *this;
        copy.joins_ += " " + std::string(join_type) + " " + std::string(right_table) + " ON " + on_cond.build_sql();
        return copy;
    }
    [[nodiscard]] select_query_builder add_join_on(std::string_view join_type, std::string_view right_table,
                                                   sql_predicate on_cond) && {
        joins_ += " " + std::string(join_type) + " " + std::string(right_table) + " ON " + on_cond.build_sql();
        return std::move(*this);
    }

    [[nodiscard]] select_query_builder add_join_using(std::string_view join_type, std::string_view right_table,
                                                      std::string using_cols) const& {
        auto copy = *this;
        copy.joins_ +=
            " " + std::string(join_type) + " " + std::string(right_table) + " USING (" + std::move(using_cols) + ")";
        return copy;
    }
    [[nodiscard]] select_query_builder add_join_using(std::string_view join_type, std::string_view right_table,
                                                      std::string using_cols) && {
        joins_ +=
            " " + std::string(join_type) + " " + std::string(right_table) + " USING (" + std::move(using_cols) + ")";
        return std::move(*this);
    }

    template <ColumnDescriptor... UsingCols>
    static std::string build_using_cols_string() {
        std::string s;
        bool first = true;
        ((s += (first ? "" : ", "), s += std::string(column_traits<UsingCols>::column_name()), first = false), ...);
        return s;
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

    std::optional<sql_predicate> where_;
    std::string group_by_;
    bool with_rollup_ = false;
    group_by_mode group_by_mode_ = group_by_mode::standard;
    std::optional<sql_predicate> having_;
    std::vector<order_by_item> order_by_clauses_;
    std::optional<std::size_t> limit_;
    std::optional<std::size_t> offset_;
    select_lock_mode lock_mode_ = select_lock_mode::none;
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
    select_builder() = default;

    explicit select_builder(std::tuple<Projs...> projs) : projs_(std::move(projs)) {
    }

    // Checked: all column projections must belong to Table.
    // For JOIN queries use from<joined<Table>>() to skip the check.
    template <typename TableSpec>
    [[nodiscard]] auto from(TableSpec) const {
        if constexpr (is_joined_v<TableSpec>) {
            return select_query_builder<typename TableSpec::table_type, Projs...>{projs_};
        } else {
            static_assert(ValidTable<TableSpec>);
            static_assert(((AggregateProjection<Projs> || InstanceProjection<Projs> ||
                            column_belongs_to_table_v<Projs, TableSpec>) &&
                           ...),
                          "All column projections must belong to the table passed to from<Table>(). "
                          "For JOIN queries spanning multiple tables, use from<joined<Table>>() instead.");
            return select_query_builder<TableSpec, Projs...>{projs_};
        }
    }

    // from(subquery, "alias") — derived table: SELECT ... FROM (subquery) AS alias
    template <AnySelectQuery Query>
    [[nodiscard]] select_query_builder<subquery_source, Projs...> from(Query const& subquery, std::string alias) const {
        select_query_builder<subquery_source, Projs...> builder{projs_};
        auto sub_sql = subquery.build_sql();
        builder.from_subquery_.reserve(2 + sub_sql.size() + 5 + alias.size());
        builder.from_subquery_ += '(';
        builder.from_subquery_ += std::move(sub_sql);
        builder.from_subquery_ += ") AS ";
        builder.from_subquery_ += std::move(alias);
        return builder;
    }

private:
    std::tuple<Projs...> projs_{};
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

class statement_query {
public:
    explicit statement_query(std::string sql) : sql_(std::move(sql)) {
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

[[nodiscard]] inline detail::statement_query start_transaction() {
    return detail::statement_query{"START TRANSACTION"};
}

[[nodiscard]] inline detail::statement_query begin_transaction() {
    return detail::statement_query{"BEGIN"};
}

[[nodiscard]] inline detail::statement_query commit_transaction() {
    return detail::statement_query{"COMMIT"};
}

[[nodiscard]] inline detail::statement_query rollback_transaction() {
    return detail::statement_query{"ROLLBACK"};
}

template <typename Query>
    requires requires(Query const& q) {
        { q.build_sql() } -> std::convertible_to<std::string>;
    }
[[nodiscard]] inline detail::statement_query explain(Query const& query) {
    return detail::statement_query{"EXPLAIN " + query.build_sql()};
}

template <typename Query>
    requires requires(Query const& q) {
        { q.build_sql() } -> std::convertible_to<std::string>;
    }
[[nodiscard]] inline detail::statement_query explain_analyze(Query const& query) {
    return detail::statement_query{"EXPLAIN ANALYZE " + query.build_sql()};
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
 *       select(symbol::id{}, symbol::ticker{})
 *           .from<symbol>()
 *           .where(equal<symbol::ticker>("AAPL"))
 *           .order_by<symbol::id>()
 *           .limit(10));
 *
 * Example (JOIN — use col<T,I> for table-qualified ON columns):
 *   auto rows = db.query_typed(
 *       select(col<symbol, 1>{}, col<price, 2>{})
 *           .from_joined<symbol>()
 *           .inner_join<price, col<symbol, 0>, col<price, 1>>()
 *           .order_by<col<price, 3>, sort_order::desc>()
 *           .limit(100));
 */
template <AnyProjection... Projs>
[[nodiscard]] detail::select_builder<std::decay_t<Projs>...> select(Projs&&... projs) {
    return detail::select_builder<std::decay_t<Projs>...>{
        std::tuple<std::decay_t<Projs>...>{std::forward<Projs>(projs)...}};
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
// DML features
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
    update_join_set_where_builder(std::string join_clause, std::tuple<Cols...> assignments, sql_predicate where)
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
    sql_predicate where_;
};

template <typename T, typename... Cols>
class update_join_set_builder {
public:
    update_join_set_builder(std::string join_clause, std::tuple<Cols...> assignments)
        : join_clause_(std::move(join_clause)), assignments_(std::move(assignments)) {
    }

    [[nodiscard]] update_join_set_where_builder<T, Cols...> where(sql_predicate condition) const {
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

template <typename T>
concept SqlExecuteStatement = SqlStatement<T> && !TypedSelectQuery<T>;

// ===================================================================
// MySQL-specific template aliases
// ===================================================================
template <ColumnDescriptor Col>
using mysql_group_concat = group_concat<Col>;

template <ColumnDescriptor Col1, ColumnDescriptor Col2>
using mysql_ifnull_of = ifnull_of<Col1, Col2>;

template <Projection P>
using mysql_format_to = format_to<P>;

template <Projection P>
using mysql_date_format_of = date_format_of<P>;

template <Projection P, sql_cast_type Type>
using mysql_convert_as = convert_as<P, Type>;

template <ColumnDescriptor Col, typename Value>
[[nodiscard]] check_expr mysql_null_safe_equal(Value&& value) {
    return null_safe_equal<Col>(std::forward<Value>(value));
}

template <ColumnDescriptor Col>
[[nodiscard]] check_expr mysql_regexp(std::string_view pattern) {
    return regexp<Col>(pattern);
}

template <ColumnDescriptor Col>
[[nodiscard]] check_expr mysql_not_regexp(std::string_view pattern) {
    return not_regexp<Col>(pattern);
}

template <ColumnDescriptor Col>
[[nodiscard]] check_expr mysql_rlike(std::string_view pattern) {
    return rlike<Col>(pattern);
}

template <ColumnDescriptor Col>
[[nodiscard]] check_expr mysql_not_rlike(std::string_view pattern) {
    return not_rlike<Col>(pattern);
}

}  // namespace ds_mysql
