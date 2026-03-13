#pragma once

#include <boost/pfr.hpp>
#include <cstddef>
#include <string_view>

#include "ds_mysql/column_field.hpp"
#include "ds_mysql/member_name_reflection.hpp"
#include "ds_mysql/schema_generator.hpp"

namespace ds_mysql {

/**
 * col<Table, Index> — Compile-time column descriptor.
 *
 * Represents a typed reference to the Nth field of a struct-mapped table.
 * Carries the C++ value type (preserving std::optional for nullable fields),
 * the column name (from field_schema), and the table name (from table_name_for).
 *
 * Intended for use with the typed SELECT builder:
 *
 *   using id_col     = col<symbol, 0>;
 *   using ticker_col = col<symbol, 2>;
 *   auto q = select<id_col, ticker_col>().from<symbol>().build_sql();
 *
 * NOTE: field_schema<Table, Index> must be specialized before column_name()
 * is called. Include the table header (e.g. symbol.hpp) before using its columns.
 */
template <typename Table, std::size_t Index>
struct col {
    using table_type = Table;
    static constexpr std::size_t field_index = Index;

    // If the PFR field type is a column_field wrapper, expose the stored value
    // type (e.g. uint32_t) rather than the wrapper struct (e.g. symbol::id).
    using value_type = unwrap_column_field_t<boost::pfr::tuple_element_t<Index, Table>>;

    [[nodiscard]] static constexpr std::string_view column_name() {
        return field_schema<Table, Index>::name();
    }

    [[nodiscard]] static constexpr std::string_view table_name_str() {
        return table_name_for<Table>::value().to_string_view();
    }
};

// Concept: satisfied by both col<Table, Index> (old style) and
// column_field-derived nested types like symbol::id (new style).
template <typename T>
concept ColumnDescriptor = ColumnFieldType<T> || requires {
    typename T::value_type;
    { T::column_name() } -> std::convertible_to<std::string_view>;
};

// ===================================================================
// column_traits<T> — uniform access to column name and value type
// ===================================================================

/**
 * column_traits<T> — adaptor that provides a uniform interface over both
 * column descriptor styles:
 *
 *   col<symbol, 0>         old style — column_name() via field_schema
 *   symbol::id             new style — column_name() derived from type name
 *
 * Use column_traits<Col>::column_name() and column_traits<Col>::value_type
 * everywhere generic query building code needs these two pieces.
 */
template <typename T>
struct column_traits {
    // Old-style col<Table, Index>: column_name() already exists on T.
    using value_type = typename T::value_type;
    static constexpr std::string_view column_name() noexcept {
        return T::column_name();
    }
};

template <ColumnFieldType T>
struct column_traits<T> {
    using value_type = typename T::value_type;
    static constexpr std::string_view column_name() noexcept {
        return T::column_name();
    }
};

// ===================================================================
// col_of<&T::field> — reflect index from member pointer
// ===================================================================

namespace detail {

template <typename S, std::size_t... Is>
consteval std::size_t find_field_index_impl(std::string_view target, std::index_sequence<Is...>) {
    std::size_t result = sizeof...(Is);  // sentinel: not found
    ((boost::pfr::get_name<Is, S>() == target ? (result = Is, true) : false) || ...);
    return result;
}

template <typename S>
consteval std::size_t find_field_index(std::string_view target) {
    return find_field_index_impl<S>(target, std::make_index_sequence<boost::pfr::tuple_size_v<S>>{});
}

template <typename T>
struct member_ptr_traits;

template <typename S, typename M>
struct member_ptr_traits<M S::*> {
    using struct_type = S;
};

}  // namespace detail

/**
 * col_of<&T::field> — derive a col<T, N> from a member pointer.
 *
 * The field index is located by matching the member name via Boost.PFR
 * at compile time, so col_of remains correct even if struct fields are
 * reordered.
 *
 * Example:
 *   using ticker_col = col_of<&symbol::ticker>;  // same as col<symbol, 2>
 */
template <auto MemberPtr>
using col_of = col<typename detail::member_ptr_traits<decltype(MemberPtr)>::struct_type,
                   detail::find_field_index<typename detail::member_ptr_traits<decltype(MemberPtr)>::struct_type>(
                       reflection::member_name_from_signature<MemberPtr>())>;

}  // namespace ds_mysql
