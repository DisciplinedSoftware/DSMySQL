#pragma once

#include "ds_mysql/sql_core.hpp"

namespace ds_mysql {
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
[[nodiscard]] std::string generate_values_impl(T const& row, std::index_sequence<Is...>) {
    if constexpr (sizeof...(Is) == 0)
        return {};
    std::string parts[] = {sql_detail::to_sql_value(boost::pfr::get<Is>(row))...};
    std::string values;
    values.reserve(sizeof...(Is) * 8);
    for (std::size_t i = 0; i < sizeof...(Is); ++i) {
        if (i > 0)
            values += ", ";
        values += parts[i];
    }
    return values;
}

template <typename T>
[[nodiscard]] std::string generate_values(T const& row) {
    return generate_values_impl(row, std::make_index_sequence<boost::pfr::tuple_size_v<T>>{});
}

template <typename T, std::size_t... Is>
[[nodiscard]] std::string generate_column_list_impl(std::index_sequence<Is...>) {
    if constexpr (sizeof...(Is) == 0)
        return {};
    constexpr std::size_t names_len = (std::size_t{} + ... + field_schema<T, Is>::name().size());
    constexpr std::string_view parts[] = {field_schema<T, Is>::name()...};
    std::string columns;
    columns.reserve(names_len + (sizeof...(Is) - 1) * 2);
    for (std::size_t i = 0; i < sizeof...(Is); ++i) {
        if (i > 0)
            columns += ", ";
        columns += parts[i];
    }
    return columns;
}

template <typename T>
[[nodiscard]] std::string const& generate_column_list() {
    static const std::string cached =
        generate_column_list_impl<T>(std::make_index_sequence<boost::pfr::tuple_size_v<T>>{});
    return cached;
}

// ---------------------------------------------------------------
// Positional insert validation — ValidFieldArgs<T, Args...>
// Each arg at position I must be sql_default_t or the exact column
// field type at that struct position.
// ---------------------------------------------------------------
template <typename T, std::size_t I, typename Arg>
concept ValidFieldArg = std::same_as<std::decay_t<Arg>, sql_default_t> ||
                        std::same_as<std::decay_t<Arg>, boost::pfr::tuple_element_t<I, T>>;

template <typename T, typename... Args, std::size_t... Is>
consteval bool valid_field_args_impl(std::index_sequence<Is...>) {
    return (ValidFieldArg<T, Is, Args> && ...);
}

template <typename T, typename... Args>
concept ValidFieldArgs = (sizeof...(Args) == boost::pfr::tuple_size_v<T>) &&
                         valid_field_args_impl<T, Args...>(std::make_index_sequence<sizeof...(Args)>{});

// ---------------------------------------------------------------
// Column-specific insert validation — ValidColumnValue<Col, Val>
// A value is valid for a column if it is sql_default_t or the
// column field type is constructible from it.
// ---------------------------------------------------------------
template <typename Col, typename Val>
concept ValidColumnValue = std::same_as<std::decay_t<Val>, sql_default_t> || std::constructible_from<Col, Val const&>;

template <typename ColsTuple, typename... Vals, std::size_t... Is>
consteval bool valid_column_values_impl(std::index_sequence<Is...>) {
    return (ValidColumnValue<std::tuple_element_t<Is, ColsTuple>, Vals> && ...);
}

// ---------------------------------------------------------------
// Field-based value generation from tuple
// ---------------------------------------------------------------
template <typename Tuple, std::size_t... Is>
[[nodiscard]] std::string generate_field_values_impl(Tuple const& args, std::index_sequence<Is...>) {
    if constexpr (sizeof...(Is) == 0)
        return {};
    std::string parts[] = {sql_detail::to_sql_value(std::get<Is>(args))...};
    std::string values;
    values.reserve(sizeof...(Is) * 8);
    for (std::size_t i = 0; i < sizeof...(Is); ++i) {
        if (i > 0)
            values += ", ";
        values += parts[i];
    }
    return values;
}

// ---------------------------------------------------------------
// Column name list from explicit column types
// ---------------------------------------------------------------
template <typename... Cols>
[[nodiscard]] std::string generate_column_names() {
    if constexpr (sizeof...(Cols) == 0)
        return {};
    std::string_view names[] = {Cols::column_name()...};
    std::string result;
    result.reserve(sizeof...(Cols) * 12);
    for (std::size_t i = 0; i < sizeof...(Cols); ++i) {
        if (i > 0)
            result += ", ";
        result += names[i];
    }
    return result;
}

// ---------------------------------------------------------------
// make_insert_value — wraps a raw value into a column field, or
// passes through sql_default_t unchanged.
// ---------------------------------------------------------------
template <typename Col, typename Val>
[[nodiscard]] auto make_insert_value(Val const& v) {
    if constexpr (std::same_as<std::decay_t<Val>, sql_default_t>) {
        return sql_default_t{};
    } else {
        return Col{v};
    }
}

template <typename Col>
void append_assignment_sql(std::string& s, bool& first, Col const& field) {
    if (!first) {
        s += ", ";
    }
    using C = std::decay_t<Col>;
    s += C::column_name();
    s += " = ";
    s += sql_detail::to_sql_value(field.value);
    first = false;
}

// ---------------------------------------------------------------
// case_set — wraps a column + CASE expression for UPDATE SET.
//
// Usage:
//   update(t{}).set_case(t::status{}, case_when(cond, "active").else_("inactive"))
//
// Produces:
//   UPDATE t SET status = CASE WHEN cond THEN 'active' ELSE 'inactive' END
// ---------------------------------------------------------------

template <ColumnFieldType Col, typename CaseBuilder>
struct case_set {
    CaseBuilder case_expr;

    [[nodiscard]] static constexpr std::string_view column_name() {
        return Col::column_name();
    }
};

template <ColumnFieldType Col, typename CaseBuilder>
void append_assignment_sql(std::string& s, bool& first, case_set<Col, CaseBuilder> const& assign) {
    if (!first) {
        s += ", ";
    }
    s += Col::column_name();
    s += " = ";
    s += assign.case_expr.build_sql();
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
}  // namespace ds_mysql
