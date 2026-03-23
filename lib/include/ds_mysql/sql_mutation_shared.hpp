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
