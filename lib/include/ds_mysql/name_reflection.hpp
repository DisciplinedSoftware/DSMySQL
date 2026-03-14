#pragma once

#include <string_view>

namespace ds_mysql {

namespace detail {

/**
 * min_find_pos — returns the earlier of two string_view::find() positions.
 * Treats npos as "not found": if only one is valid it is returned; if neither
 * is valid npos is returned.
 */
[[nodiscard]] constexpr std::size_t min_find_pos(std::size_t p1, std::size_t p2) noexcept {
    constexpr auto npos = std::string_view::npos;
    return (p1 != npos && p2 != npos) ? (p1 < p2 ? p1 : p2) : (p1 != npos ? p1 : p2);
}

/**
 * strip_type_qualifiers — given a raw type-name substring from a compiler
 * function-signature intrinsic, returns the final unqualified identifier:
 *
 *   - (MSVC only) strips a leading 'struct ' or 'class ' keyword
 *   - strips any leading namespace/scope qualifier up to the last '::'
 *
 * Example inputs → outputs:
 *   "struct `anonymous-namespace'::ticker_tag"  →  "ticker_tag"   (MSVC)
 *   "(anonymous namespace)::ticker_tag"          →  "ticker_tag"   (GCC/Clang)
 *   "child_table_cascade"                        →  "child_table_cascade"
 */
[[nodiscard]] constexpr std::string_view strip_type_qualifiers(std::string_view name) noexcept {
#if defined(_MSC_VER)
    if (name.starts_with("struct "))
        name = name.substr(7);
    else if (name.starts_with("class "))
        name = name.substr(6);
#endif

    auto const scope = name.rfind("::");
    if (scope != std::string_view::npos && scope + 2 < name.size())
        name = name.substr(scope + 2);
    return name;
}

/**
 * Extract struct type name from compiler intrinsics (__PRETTY_FUNCTION__ or __FUNCSIG__)
 */
template <typename T>
consteval std::string_view extract_type_name() {
#if defined(__clang__) || defined(__GNUC__)
    constexpr std::string_view signature = __PRETTY_FUNCTION__;
    constexpr std::string_view marker = "T = ";
#elif defined(_MSC_VER)
    constexpr std::string_view signature = __FUNCSIG__;
    constexpr std::string_view marker = "extract_type_name<";
#else
#error ("extract_type_name requires Clang, GCC, or MSVC")
#endif

    constexpr auto npos = std::string_view::npos;
    constexpr auto marker_pos = signature.find(marker);
    if constexpr (marker_pos == npos) {
        static_assert(false, "Failed to locate marker in compiler function signature");
        return {};
    }

    constexpr auto start = marker_pos + marker.size();

#if defined(__clang__) || defined(__GNUC__)
    // GCC/Clang format: [with T = TYPE] or [with T = TYPE; ALIAS = EXPANSION, ...]
    constexpr auto end = min_find_pos(signature.find(';', start), signature.find(']', start));
#elif defined(_MSC_VER)
    // MSVC format: "...extract_type_name<TYPE>(void)..."
    // The type ends at '>' (template arg close) or ',' (comma in multi-param templates).
    constexpr auto end = min_find_pos(signature.find('>', start), signature.find(',', start));
#else
#error ("extract_type_name requires Clang, GCC, or MSVC")
#endif

    if constexpr (end == npos || end <= start) {
        static_assert(false, "Failed to locate end of type name in compiler function signature");
        return {};
    }

    return strip_type_qualifiers(signature.substr(start, end - start));
}

}  // namespace detail

}  // namespace ds_mysql