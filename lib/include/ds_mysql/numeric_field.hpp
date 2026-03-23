#pragma once

#include <cstdint>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>

namespace ds_mysql {

namespace detail {

inline constexpr uint32_t unspecified_numeric_scale = std::numeric_limits<uint32_t>::max();

template <typename Derived, typename Repr>
struct formatted_numeric_base {
    using underlying_type = Repr;

    Repr value{};

    constexpr formatted_numeric_base() = default;
    constexpr formatted_numeric_base(Repr v) noexcept(std::is_nothrow_move_constructible_v<Repr>) : value(v) {
    }

    constexpr formatted_numeric_base& operator=(Repr v) noexcept(std::is_nothrow_assignable_v<Repr&, Repr>) {
        value = v;
        return *this;
    }

    [[nodiscard]] constexpr Repr const& get() const noexcept {
        return value;
    }

    [[nodiscard]] constexpr Repr& get() noexcept {
        return value;
    }

    constexpr operator Repr const&() const noexcept {
        return value;
    }

    constexpr operator Repr&() noexcept {
        return value;
    }
};

template <uint32_t Precision, uint32_t Scale>
consteval void validate_numeric_precision_scale() {
    static_assert(Precision > 0U || Scale == unspecified_numeric_scale,
                  "formatted numeric types require precision when specifying scale");
    static_assert(Scale == unspecified_numeric_scale || Scale <= Precision,
                  "formatted numeric scale must be <= precision");
}

template <uint32_t Precision, uint32_t Scale>
[[nodiscard]] inline std::string format_numeric_sql_type(char const* name) {
    if constexpr (Precision == 0U) {
        return name;
    } else if constexpr (Scale == unspecified_numeric_scale) {
        return std::string{name} + "(" + std::to_string(Precision) + ")";
    } else {
        return std::string{name} + "(" + std::to_string(Precision) + "," + std::to_string(Scale) + ")";
    }
}

}  // namespace detail

template <uint32_t Precision = 0U, uint32_t Scale = detail::unspecified_numeric_scale>
struct float_type : detail::formatted_numeric_base<float_type<Precision, Scale>, float> {
    using base = detail::formatted_numeric_base<float_type<Precision, Scale>, float>;
    using base::base;
    using base::operator=;

    static constexpr uint32_t precision = Precision;
    static constexpr uint32_t scale = Scale;

    [[nodiscard]] static std::string sql_type() {
        detail::validate_numeric_precision_scale<Precision, Scale>();
        return detail::format_numeric_sql_type<Precision, Scale>("FLOAT");
    }
};

template <uint32_t Precision = 0U, uint32_t Scale = detail::unspecified_numeric_scale>
struct double_type : detail::formatted_numeric_base<double_type<Precision, Scale>, double> {
    using base = detail::formatted_numeric_base<double_type<Precision, Scale>, double>;
    using base::base;
    using base::operator=;

    static constexpr uint32_t precision = Precision;
    static constexpr uint32_t scale = Scale;

    [[nodiscard]] static std::string sql_type() {
        detail::validate_numeric_precision_scale<Precision, Scale>();
        return detail::format_numeric_sql_type<Precision, Scale>("DOUBLE");
    }
};

template <uint32_t Precision = 0U, uint32_t Scale = detail::unspecified_numeric_scale>
struct decimal_type : detail::formatted_numeric_base<decimal_type<Precision, Scale>, double> {
    using base = detail::formatted_numeric_base<decimal_type<Precision, Scale>, double>;
    using base::base;
    using base::operator=;

    static constexpr uint32_t precision = Precision;
    static constexpr uint32_t scale = Scale;

    [[nodiscard]] static std::string sql_type() {
        detail::validate_numeric_precision_scale<Precision, Scale>();
        return detail::format_numeric_sql_type<Precision, Scale>("DECIMAL");
    }
};

template <typename T>
struct is_formatted_numeric_type : std::false_type {};

template <uint32_t Precision, uint32_t Scale>
struct is_formatted_numeric_type<float_type<Precision, Scale>> : std::true_type {};

template <uint32_t Precision, uint32_t Scale>
struct is_formatted_numeric_type<double_type<Precision, Scale>> : std::true_type {};

template <uint32_t Precision, uint32_t Scale>
struct is_formatted_numeric_type<decimal_type<Precision, Scale>> : std::true_type {};

template <typename T>
inline constexpr bool is_formatted_numeric_type_v = is_formatted_numeric_type<T>::value;

}  // namespace ds_mysql