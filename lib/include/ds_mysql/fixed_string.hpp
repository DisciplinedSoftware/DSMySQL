#pragma once

#include <string_view>

namespace ds_mysql {

/**
 * fixed_string<N> — compile-time string literal for use as a non-type template parameter.
 *
 * Satisfies C++20 structural type requirements, enabling use as an NTTP:
 *
 *   template <fixed_string Name, typename T>
 *   struct column_field { ... };
 *
 *   using id = column_field<"id", uint32_t>;
 *
 * The deduction guide allows the size to be inferred from a string literal:
 *
 *   fixed_string s{"hello"};  // fixed_string<6>
 */
template <std::size_t N>
struct fixed_string {
    char data[N]{};

    // NOLINTNEXTLINE(google-explicit-constructor)
    consteval fixed_string(char const (&str)[N]) noexcept {
        for (std::size_t i = 0; i < N; ++i)
            data[i] = str[i];
    }

    [[nodiscard]] constexpr operator std::string_view() const noexcept {
        return {data, N - 1};  // exclude null terminator
    }

    [[nodiscard]] constexpr std::string_view view() const noexcept {
        return {data, N - 1};
    }

    template <std::size_t M>
    [[nodiscard]] constexpr bool operator==(fixed_string<M> const& other) const noexcept {
        if constexpr (N != M)
            return false;
        else {
            for (std::size_t i = 0; i < N; ++i)
                if (data[i] != other.data[i])
                    return false;
            return true;
        }
    }
};

template <std::size_t N>
fixed_string(char const (&)[N]) -> fixed_string<N>;

}  // namespace ds_mysql
