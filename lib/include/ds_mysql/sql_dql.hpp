#pragma once

#include "ds_mysql/sql_core.hpp"
#include "ds_mysql/sql_mutation_shared.hpp"

namespace ds_mysql {

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

template <typename P, int Amount, sql_date_part Part>
struct date_add_of {};

template <typename P, int Amount, sql_date_part Part>
struct date_sub_of {};

template <sql_date_part Part, typename A, typename B>
struct timestampdiff_of {};

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

template <typename A, typename B>
using datediff = datediff_of<A, B>;

template <typename P, int Amount, sql_date_part Part>
using date_add = date_add_of<P, Amount, Part>;

template <typename P, int Amount, sql_date_part Part>
using date_sub = date_sub_of<P, Amount, Part>;

template <sql_date_part Part, typename A, typename B>
using timestampdiff = timestampdiff_of<Part, A, B>;

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

template <ColumnDescriptor Col, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, std::size_t Offset = 1,
          sort_order Dir = sort_order::asc, typename Frame = void>
struct lag_over {};

template <ColumnDescriptor Col, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, std::size_t Offset = 1,
          sort_order Dir = sort_order::asc, typename Frame = void>
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

template <Projection P>
struct projection_traits<date_of<P>> {
    using value_type = std::string;
    static std::string sql_expr() {
        return "DATE(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<time_of<P>> {
    using value_type = std::string;
    static std::string sql_expr() {
        return "TIME(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<year_of<P>> {
    using value_type = uint32_t;
    static std::string sql_expr() {
        return "YEAR(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<month_of<P>> {
    using value_type = uint32_t;
    static std::string sql_expr() {
        return "MONTH(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection P>
struct projection_traits<day_of<P>> {
    using value_type = uint32_t;
    static std::string sql_expr() {
        return "DAY(" + std::string(projection_traits<P>::sql_expr()) + ")";
    }
};

template <Projection A, Projection B>
struct projection_traits<datediff_of<A, B>> {
    using value_type = int64_t;
    static std::string sql_expr() {
        return "DATEDIFF(" + std::string(projection_traits<A>::sql_expr()) + ", " +
               std::string(projection_traits<B>::sql_expr()) + ")";
    }
};

template <Projection P, int Amount, sql_date_part Part>
struct projection_traits<date_add_of<P, Amount, Part>> {
    using value_type = std::string;
    static std::string sql_expr() {
        return "DATE_ADD(" + std::string(projection_traits<P>::sql_expr()) + ", INTERVAL " + std::to_string(Amount) +
               " " + std::string(sql_date_part_name<Part>()) + ")";
    }
};

template <Projection P, int Amount, sql_date_part Part>
struct projection_traits<date_sub_of<P, Amount, Part>> {
    using value_type = std::string;
    static std::string sql_expr() {
        return "DATE_SUB(" + std::string(projection_traits<P>::sql_expr()) + ", INTERVAL " + std::to_string(Amount) +
               " " + std::string(sql_date_part_name<Part>()) + ")";
    }
};

template <sql_date_part Part, Projection A, Projection B>
struct projection_traits<timestampdiff_of<Part, A, B>> {
    using value_type = int64_t;
    static std::string sql_expr() {
        return "TIMESTAMPDIFF(" + std::string(sql_date_part_name<Part>()) + ", " +
               std::string(projection_traits<A>::sql_expr()) + ", " + std::string(projection_traits<B>::sql_expr()) +
               ")";
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

template <Projection Agg, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir, typename Frame>
struct projection_traits<window_func<Agg, PartitionCol, OrderCol, Dir, Frame>> {
    using value_type = typename projection_traits<Agg>::value_type;
    static std::string sql_expr() {
        return std::string(projection_traits<Agg>::sql_expr()) + " OVER (PARTITION BY " +
               std::string(column_traits<PartitionCol>::column_name()) + " ORDER BY " +
               std::string(column_traits<OrderCol>::column_name()) + (Dir == sort_order::asc ? " ASC" : " DESC") +
               sql_window_frame_clause<Frame>() + ")";
    }
};

template <ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir, typename Frame>
struct projection_traits<row_number_over<PartitionCol, OrderCol, Dir, Frame>> {
    using value_type = uint64_t;
    static std::string sql_expr() {
        return "ROW_NUMBER() OVER (PARTITION BY " + std::string(column_traits<PartitionCol>::column_name()) +
               " ORDER BY " + std::string(column_traits<OrderCol>::column_name()) +
               (Dir == sort_order::asc ? " ASC" : " DESC") + sql_window_frame_clause<Frame>() + ")";
    }
};

template <ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir, typename Frame>
struct projection_traits<rank_over<PartitionCol, OrderCol, Dir, Frame>> {
    using value_type = uint64_t;
    static std::string sql_expr() {
        return "RANK() OVER (PARTITION BY " + std::string(column_traits<PartitionCol>::column_name()) + " ORDER BY " +
               std::string(column_traits<OrderCol>::column_name()) + (Dir == sort_order::asc ? " ASC" : " DESC") +
               sql_window_frame_clause<Frame>() + ")";
    }
};

template <ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, sort_order Dir, typename Frame>
struct projection_traits<dense_rank_over<PartitionCol, OrderCol, Dir, Frame>> {
    using value_type = uint64_t;
    static std::string sql_expr() {
        return "DENSE_RANK() OVER (PARTITION BY " + std::string(column_traits<PartitionCol>::column_name()) +
               " ORDER BY " + std::string(column_traits<OrderCol>::column_name()) +
               (Dir == sort_order::asc ? " ASC" : " DESC") + sql_window_frame_clause<Frame>() + ")";
    }
};

template <ColumnDescriptor Col, ColumnDescriptor PartitionCol, ColumnDescriptor OrderCol, std::size_t Offset,
          sort_order Dir, typename Frame>
struct projection_traits<lag_over<Col, PartitionCol, OrderCol, Offset, Dir, Frame>> {
    using value_type = typename column_traits<Col>::value_type;
    static std::string sql_expr() {
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
    static std::string sql_expr() {
        return "LEAD(" + std::string(column_traits<Col>::column_name()) + ", " + std::to_string(Offset) +
               ") OVER (PARTITION BY " + std::string(column_traits<PartitionCol>::column_name()) + " ORDER BY " +
               std::string(column_traits<OrderCol>::column_name()) + (Dir == sort_order::asc ? " ASC" : " DESC") +
               sql_window_frame_clause<Frame>() + ")";
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

template <typename T>
concept SqlExecuteStatement = SqlStatement<T> && !TypedSelectQuery<T>;

// ===================================================================
// MySQL-specific template aliases
// ===================================================================
template <ColumnDescriptor Col>
using mysql_group_concat = group_concat<Col>;

template <ColumnDescriptor Col1, ColumnDescriptor Col2>
using mysql_ifnull_of = ifnull_of<Col1, Col2>;

template <Projection P, int Scale>
using mysql_format_to = format_to<P, Scale>;

template <Projection P, fixed_string Format>
using mysql_date_format_of = date_format_of<P, Format>;

template <Projection P, sql_cast_type Type>
using mysql_convert_as = convert_as<P, Type>;

template <ColumnDescriptor Col, typename Value>
[[nodiscard]] where_condition mysql_null_safe_equal(Value&& value) {
    return null_safe_equal<Col>(std::forward<Value>(value));
}

template <ColumnDescriptor Col>
[[nodiscard]] where_condition mysql_regexp(std::string_view pattern) {
    return regexp<Col>(pattern);
}

template <ColumnDescriptor Col>
[[nodiscard]] where_condition mysql_not_regexp(std::string_view pattern) {
    return not_regexp<Col>(pattern);
}

template <ColumnDescriptor Col>
[[nodiscard]] where_condition mysql_rlike(std::string_view pattern) {
    return rlike<Col>(pattern);
}

template <ColumnDescriptor Col>
[[nodiscard]] where_condition mysql_not_rlike(std::string_view pattern) {
    return not_rlike<Col>(pattern);
}

}  // namespace ds_mysql
