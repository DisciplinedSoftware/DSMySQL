#pragma once

#include <array>
#include <boost/pfr.hpp>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

namespace ds_mysql::reflection {

template <auto MemberPtr>
consteval std::string_view member_name_from_signature() {
#if defined(__clang__) || defined(__GNUC__)
    constexpr std::string_view signature = __PRETTY_FUNCTION__;
    constexpr std::string_view marker = "MemberPtr = ";
#elif defined(_MSC_VER)
    constexpr std::string_view signature = __FUNCSIG__;
    constexpr std::string_view marker = "MemberPtr=";
#else
    return {};
#endif

    auto const marker_pos = signature.find(marker);
    if (marker_pos == std::string_view::npos) {
        return {};
    }

    auto const expr_begin = marker_pos + marker.size();
    auto const semicolon_pos = signature.find(';', expr_begin);
    auto const bracket_pos = signature.find(']', expr_begin);

    auto expr_end = std::string_view::npos;
    if (semicolon_pos != std::string_view::npos && bracket_pos != std::string_view::npos) {
        expr_end = semicolon_pos < bracket_pos ? semicolon_pos : bracket_pos;
    } else if (semicolon_pos != std::string_view::npos) {
        expr_end = semicolon_pos;
    } else {
        expr_end = bracket_pos;
    }

    if (expr_end == std::string_view::npos || expr_end <= expr_begin) {
        return {};
    }

    auto const expression = signature.substr(expr_begin, expr_end - expr_begin);
    auto const scope_pos = expression.rfind("::");
    if (scope_pos == std::string_view::npos || scope_pos + 2 >= expression.size()) {
        return {};
    }

    return expression.substr(scope_pos + 2);
}

template <auto... MemberPtrs>
struct member_pointer_list {
    static constexpr std::array<std::string_view, sizeof...(MemberPtrs)> names = {
        member_name_from_signature<MemberPtrs>()...,
    };
};

template <typename T>
struct member_pointer_value;

template <typename C, typename M>
struct member_pointer_value<M C::*> {
    using type = M;
};

template <typename MemberList, std::size_t Index>
constexpr std::string_view member_name_at() {
    static_assert(Index < MemberList::names.size(), "Field index out of range");
    return MemberList::names[Index];
}

template <typename StructType, std::size_t Index>
constexpr std::string_view pfr_member_name_at() {
    static_assert(Index < boost::pfr::tuple_size_v<StructType>, "Field index out of range");
    return boost::pfr::get_name<Index, StructType>();
}

template <typename StructType, typename MemberList>
struct member_list_matcher;

template <typename StructType, auto... MemberPtrs>
struct member_list_matcher<StructType, member_pointer_list<MemberPtrs...>> {
    template <std::size_t... Is>
    static consteval bool evaluate_types(std::index_sequence<Is...>) {
        using tuple_type = decltype(boost::pfr::structure_to_tuple(std::declval<StructType>()));

        return ((std::is_same_v<std::remove_cvref_t<std::tuple_element_t<Is, tuple_type>>,
                                std::remove_cvref_t<typename member_pointer_value<decltype(MemberPtrs)>::type>>) &&
                ...);
    }

    template <std::size_t... Is>
    static consteval bool evaluate_names(std::index_sequence<Is...>) {
        return ((member_name_from_signature<MemberPtrs>() == boost::pfr::get_name<Is, StructType>()) && ...);
    }

    static consteval bool count_matches() {
        constexpr std::size_t member_count = sizeof...(MemberPtrs);
        return member_count == boost::pfr::tuple_size_v<StructType>;
    }

    static consteval bool portable_value() {
        constexpr std::size_t member_count = sizeof...(MemberPtrs);
        if constexpr (!count_matches()) {
            return false;
        } else {
            return evaluate_types(std::make_index_sequence<member_count>{});
        }
    }

    static consteval bool strict_value() {
        constexpr std::size_t member_count = sizeof...(MemberPtrs);
        if constexpr (!count_matches()) {
            return false;
        } else {
            return portable_value() && evaluate_names(std::make_index_sequence<member_count>{});
        }
    }
};

template <typename StructType, typename MemberList>
consteval bool member_list_matches_struct_portable() {
    return member_list_matcher<StructType, MemberList>::portable_value();
}

template <typename StructType, typename MemberList>
consteval bool member_list_matches_struct_strict() {
    return member_list_matcher<StructType, MemberList>::strict_value();
}

template <typename StructType, typename MemberList>
consteval bool member_list_matches_struct() {
    return member_list_matches_struct_strict<StructType, MemberList>();
}

}  // namespace ds_mysql::reflection
