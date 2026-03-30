#pragma once

// This header is included from sql_core.hpp after its definitions.
// All sql_core.hpp types (ColumnFieldType, sql_detail, enums, etc.) are
// available when this file is parsed.

namespace ds_mysql {

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
    return {Col::column_name(), " = ", sql_detail::to_sql_value(value.value)};
}

template <ColumnFieldType Col>
[[nodiscard]] check_expr not_equal(Col const& value) {
    return {Col::column_name(), " != ", sql_detail::to_sql_value(value.value)};
}

template <ColumnFieldType Col>
[[nodiscard]] check_expr less_than(Col const& value) {
    return {Col::column_name(), " < ", sql_detail::to_sql_value(value.value)};
}

template <ColumnFieldType Col>
[[nodiscard]] check_expr greater_than(Col const& value) {
    return {Col::column_name(), " > ", sql_detail::to_sql_value(value.value)};
}

template <ColumnFieldType Col>
[[nodiscard]] check_expr less_than_or_equal(Col const& value) {
    return {Col::column_name(), " <= ", sql_detail::to_sql_value(value.value)};
}

template <ColumnFieldType Col>
[[nodiscard]] check_expr greater_than_or_equal(Col const& value) {
    return {Col::column_name(), " >= ", sql_detail::to_sql_value(value.value)};
}

template <ColumnFieldType Col>
    requires is_field_nullable_v<Col>
[[nodiscard]] check_expr is_null() {
    return {Col::column_name(), " IS NULL", {}};
}

template <ColumnFieldType Col>
    requires is_field_nullable_v<Col>
[[nodiscard]] check_expr is_not_null() {
    return {Col::column_name(), " IS NOT NULL", {}};
}

template <ColumnFieldType Col>
[[nodiscard]] check_expr like(std::string_view pattern) {
    auto escaped = sql_detail::escape_sql_string(pattern);
    std::string rhs;
    rhs.reserve(2 + escaped.size());
    rhs += '\'';
    rhs += std::move(escaped);
    rhs += '\'';
    return {Col::column_name(), " LIKE ", std::move(rhs)};
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

// SqlBuilder — any type that can produce a SQL string via build_sql().
// Defined early so subquery predicates can reference it before the full SELECT builder is defined.
template <typename T>
concept SqlBuilder = requires(T const& t) {
    { t.build_sql() } -> std::convertible_to<std::string>;
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
    return {Col::column_name(), " IN ", std::move(rhs)};
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
    return {Col::column_name(), " NOT IN ", std::move(rhs)};
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
    return {Col::column_name(), " BETWEEN ", std::move(rhs)};
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
    return {Col::column_name(), " NOT LIKE ", std::move(rhs)};
}

// null_safe_equal<Col>(value) — col <=> val (MySQL NULL-safe equality) — check-safe
template <ColumnFieldType Col>
[[nodiscard]] check_expr null_safe_equal(Col const& value) {
    return {Col::column_name(), " <=> ", sql_detail::to_sql_value(value.value)};
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
    return {Col::column_name(), " REGEXP ", std::move(rhs)};
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
    return {Col::column_name(), " NOT REGEXP ", std::move(rhs)};
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
    return {Col::column_name(), " SOUNDS LIKE ", std::move(rhs)};
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
    (add_col(Cols::column_name()), ...);
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

template <ColumnFieldType Col, SqlBuilder Query>
[[nodiscard]] sql_predicate in_subquery(Query const& subquery) {
    auto sub = subquery.build_sql();
    std::string rhs;
    rhs.reserve(2 + sub.size());
    rhs += '(';
    rhs += std::move(sub);
    rhs += ')';
    return {Col::column_name(), " IN ", std::move(rhs)};
}

template <ColumnFieldType Col, SqlBuilder Query>
[[nodiscard]] sql_predicate not_in_subquery(Query const& subquery) {
    auto sub = subquery.build_sql();
    std::string rhs;
    rhs.reserve(2 + sub.size());
    rhs += '(';
    rhs += std::move(sub);
    rhs += ')';
    return {Col::column_name(), " NOT IN ", std::move(rhs)};
}

template <SqlBuilder Query>
[[nodiscard]] sql_predicate exists(Query const& subquery) {
    auto sub = subquery.build_sql();
    std::string s;
    s.reserve(8 + sub.size() + 1);
    s += "EXISTS (";
    s += std::move(sub);
    s += ')';
    return {{}, {}, std::move(s)};
}

template <SqlBuilder Query>
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
// col_expr<Col> / col_ref(Col{}) — natural operator syntax for predicates
//
// col_ref(Col{}) is a constexpr factory function that returns a col_expr<Col>,
// exposing comparison and predicate operators. Simple column-ref operators
// return check_expr (usable in WHERE, HAVING, CHECK); subquery operators
// return sql_predicate (WHERE/HAVING only). Results compose with &, |, !
// (see above).
//
// Single-column predicates (return check_expr):
//   col_ref(trade::code{}) == "AAPL"             → equal<trade::code>("AAPL")
//   col_ref(trade::id{}) != 0u                   → not_equal<trade::id>(0u)
//   col_ref(trade::id{}) < 100u                  → less_than<trade::id>(100u)
//   col_ref(trade::id{}) > 0u                    → greater_than<trade::id>(0u)
//   col_ref(trade::id{}) <= 100u                 → less_than_or_equal<trade::id>(100u)
//   col_ref(trade::id{}) >= 1u                   → greater_than_or_equal<trade::id>(1u)
//   col_ref(trade::name{}).is_null()             → is_null<trade::name>()
//   col_ref(trade::name{}).is_not_null()         → is_not_null<trade::name>()
//   col_ref(trade::code{}).like("AAPL%")         → like<trade::code>("AAPL%")
//   col_ref(trade::code{}).not_like("X%")        → not_like<trade::code>("X%")
//   col_ref(trade::id{}).between(1u, 10u)           → between<trade::id>(1u, 10u)
//   col_ref(trade::code{}).in({"AAPL", "GOOGL"})    → in<trade::code>({"AAPL", "GOOGL"})
//   col_ref(trade::code{}).not_in({"X", "Y"})       → not_in<trade::code>({"X", "Y"})
//   col_ref(trade::code{}).regexp("^A")             → regexp<trade::code>("^A")
//   col_ref(trade::code{}).not_regexp("^A")         → not_regexp<trade::code>("^A")
//   col_ref(trade::code{}).rlike("^A")              → rlike<trade::code>("^A")
//   col_ref(trade::code{}).not_rlike("^A")          → not_rlike<trade::code>("^A")
//   col_ref(trade::name{}).sounds_like("word")      → sounds_like<trade::name>("word")
//
// Composing with &, |, !:
//   col_ref(trade::code{}) == "AAPL" | col_ref(trade::code{}) == "GOOGL"
//   col_ref(trade::id{}) >= 1u & col_ref(trade::id{}) <= 100u
//   !(col_ref(trade::code{}) == "AAPL")
//   (col_ref(trade::code{}) == "AAPL" | col_ref(trade::code{}) == "GOOGL") &
//       col_ref(trade::type{}) == "Stock"
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

    template <typename T>
        requires(!std::is_convertible_v<T, value_type> &&
                 requires {
                     { T::name() } -> std::convertible_to<std::string_view>;
                 })
    [[nodiscard]] check_expr operator==(T const&) const {
        return equal<Col>(Col{value_type(T::name())});
    }
    template <typename T>
        requires(!std::is_convertible_v<T, value_type> &&
                 requires {
                     { T::name() } -> std::convertible_to<std::string_view>;
                 })
    [[nodiscard]] check_expr operator!=(T const&) const {
        return not_equal<Col>(Col{value_type(T::name())});
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

    template <SqlBuilder Query>
    [[nodiscard]] sql_predicate in_subquery(Query const& subquery) const {
        return ds_mysql::in_subquery<Col>(subquery);
    }

    template <SqlBuilder Query>
    [[nodiscard]] sql_predicate not_in_subquery(Query const& subquery) const {
        return ds_mysql::not_in_subquery<Col>(subquery);
    }

    template <SqlBuilder Query>
    [[nodiscard]] sql_predicate eq_subquery(Query const& subquery) const {
        auto sub = subquery.build_sql();
        std::string rhs;
        rhs.reserve(2 + sub.size());
        rhs += '(';
        rhs += std::move(sub);
        rhs += ')';
        return {Col::column_name(), " = ", std::move(rhs)};
    }

    template <SqlBuilder Query>
    [[nodiscard]] sql_predicate ne_subquery(Query const& subquery) const {
        auto sub = subquery.build_sql();
        std::string rhs;
        rhs.reserve(2 + sub.size());
        rhs += '(';
        rhs += std::move(sub);
        rhs += ')';
        return {Col::column_name(), " != ", std::move(rhs)};
    }

    template <SqlBuilder Query>
    [[nodiscard]] sql_predicate lt_subquery(Query const& subquery) const {
        auto sub = subquery.build_sql();
        std::string rhs;
        rhs.reserve(2 + sub.size());
        rhs += '(';
        rhs += std::move(sub);
        rhs += ')';
        return {Col::column_name(), " < ", std::move(rhs)};
    }

    template <SqlBuilder Query>
    [[nodiscard]] sql_predicate gt_subquery(Query const& subquery) const {
        auto sub = subquery.build_sql();
        std::string rhs;
        rhs.reserve(2 + sub.size());
        rhs += '(';
        rhs += std::move(sub);
        rhs += ')';
        return {Col::column_name(), " > ", std::move(rhs)};
    }

    template <SqlBuilder Query>
    [[nodiscard]] sql_predicate le_subquery(Query const& subquery) const {
        auto sub = subquery.build_sql();
        std::string rhs;
        rhs.reserve(2 + sub.size());
        rhs += '(';
        rhs += std::move(sub);
        rhs += ')';
        return {Col::column_name(), " <= ", std::move(rhs)};
    }

    template <SqlBuilder Query>
    [[nodiscard]] sql_predicate ge_subquery(Query const& subquery) const {
        auto sub = subquery.build_sql();
        std::string rhs;
        rhs.reserve(2 + sub.size());
        rhs += '(';
        rhs += std::move(sub);
        rhs += ')';
        return {Col::column_name(), " >= ", std::move(rhs)};
    }
};

// col_ref(Col{}) — instance-based factory for natural operator-based WHERE expressions.
template <ColumnFieldType Col>
[[nodiscard]] constexpr col_expr<Col> col_ref(Col const&) noexcept {
    return {};
}

}  // namespace ds_mysql
