#pragma once

#include <boost/ut.hpp>
#include <expected>
#include <sstream>
#include <string>
#include <type_traits>

namespace ds_mysql {

template <class T, class E>
struct fatal_expected {
    std::expected<T, E> const* value;
};

struct fatal_fn {
    template <class T, class E>
    [[nodiscard]] constexpr auto operator()(std::expected<T, E> const& value) const {
        return fatal_expected<T, E>{&value};
    }

    template <class T>
    [[nodiscard]] constexpr auto operator()(T const& value) const {
        return boost::ut::fatal(value);
    }
};

struct expect_fn {
    template <class T, class E>
    auto operator()(fatal_expected<T, E> const& expr, boost::ut::reflection::source_location const& sl =
                                                          boost::ut::reflection::source_location::current()) const {
        auto assertion = boost::ut::expect(boost::ut::fatal(expr.value->has_value()), sl);

        // boost.ut logs invocable messages lazily, so error() is read only on failure.
        assertion << [value = expr.value]() -> std::string {
            if constexpr (std::is_constructible_v<std::string, E const&>) {
                return std::string(value->error());
            } else if constexpr (requires(std::ostream& os, E const& e) { os << e; }) {
                std::ostringstream out;
                out << value->error();
                return out.str();
            } else {
                static_assert(false, "Error type is not streamable or convertible to string");
            }
        };

        return assertion;
    }

    template <class TExpr>
    auto operator()(TExpr const& expr, boost::ut::reflection::source_location const& sl =
                                           boost::ut::reflection::source_location::current()) const {
        return boost::ut::expect(expr, sl);
    }
};

inline constexpr fatal_fn fatal{};
inline constexpr expect_fn expect{};

}  // namespace ds_mysql
