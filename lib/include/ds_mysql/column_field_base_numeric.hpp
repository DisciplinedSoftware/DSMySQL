#pragma once

#include <optional>

#include "ds_mysql/column_field_base_core.hpp"
#include "ds_mysql/sql_numeric.hpp"

namespace ds_mysql {

namespace column_field_detail {

// ===================================================================
// base<float_type<P, S>> specializations
// ===================================================================

template <uint32_t Precision, uint32_t Scale>
struct base<float_type<Precision, Scale>> : column_field_tag {
    using value_type = float_type<Precision, Scale>;

    float_type<Precision, Scale> value{};

    constexpr base() = default;
    constexpr base(float_type<Precision, Scale> v) noexcept(
        std::is_nothrow_move_constructible_v<float_type<Precision, Scale>>)
        : value(std::move(v)) {
    }

    constexpr base& operator=(float_type<Precision, Scale> v) noexcept(
        std::is_nothrow_move_assignable_v<float_type<Precision, Scale>>) {
        value = std::move(v);
        return *this;
    }

    // Constructors and assignments from POD float type
    constexpr base(float v) noexcept : value(v) {
    }

    constexpr base& operator=(float v) noexcept {
        value = v;
        return *this;
    }

    [[nodiscard]] constexpr float_type<Precision, Scale> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr float_type<Precision, Scale>& get() noexcept {
        return value;
    }

    constexpr operator float_type<Precision, Scale> const&() const noexcept {
        return value;
    }
    constexpr operator float_type<Precision, Scale>&() noexcept {
        return value;
    }
};

template <uint32_t Precision, uint32_t Scale>
struct base<std::optional<float_type<Precision, Scale>>> : column_field_tag {
    using value_type = std::optional<float_type<Precision, Scale>>;

    std::optional<float_type<Precision, Scale>> value{};

    constexpr base() = default;
    constexpr base(std::nullopt_t) noexcept : value(std::nullopt) {
    }
    constexpr base(float_type<Precision, Scale> v) noexcept(
        std::is_nothrow_move_constructible_v<float_type<Precision, Scale>>)
        : value(std::move(v)) {
    }
    constexpr base(std::optional<float_type<Precision, Scale>> v) noexcept(
        std::is_nothrow_move_constructible_v<std::optional<float_type<Precision, Scale>>>)
        : value(std::move(v)) {
    }

    constexpr base& operator=(std::nullopt_t) noexcept {
        value = std::nullopt;
        return *this;
    }
    constexpr base& operator=(float_type<Precision, Scale> v) noexcept(
        std::is_nothrow_move_assignable_v<float_type<Precision, Scale>>) {
        value = std::move(v);
        return *this;
    }
    constexpr base& operator=(std::optional<float_type<Precision, Scale>> v) noexcept(
        std::is_nothrow_move_assignable_v<std::optional<float_type<Precision, Scale>>>) {
        value = std::move(v);
        return *this;
    }

    // Constructors and assignments from POD float type
    constexpr base(float v) noexcept : value(v) {
    }

    constexpr base& operator=(float v) noexcept {
        value = v;
        return *this;
    }

    [[nodiscard]] constexpr std::optional<float_type<Precision, Scale>> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr std::optional<float_type<Precision, Scale>>& get() noexcept {
        return value;
    }

    constexpr operator std::optional<float_type<Precision, Scale>> const&() const noexcept {
        return value;
    }
    constexpr operator std::optional<float_type<Precision, Scale>>&() noexcept {
        return value;
    }
};

// ===================================================================
// base<double_type<P, S>> specializations
// ===================================================================

template <uint32_t Precision, uint32_t Scale>
struct base<double_type<Precision, Scale>> : column_field_tag {
    using value_type = double_type<Precision, Scale>;

    double_type<Precision, Scale> value{};

    constexpr base() = default;
    constexpr base(double_type<Precision, Scale> v) noexcept(
        std::is_nothrow_move_constructible_v<double_type<Precision, Scale>>)
        : value(std::move(v)) {
    }

    constexpr base& operator=(double_type<Precision, Scale> v) noexcept(
        std::is_nothrow_move_assignable_v<double_type<Precision, Scale>>) {
        value = std::move(v);
        return *this;
    }

    // Constructors and assignments from POD double type
    constexpr base(double v) noexcept : value(v) {
    }

    constexpr base& operator=(double v) noexcept {
        value = v;
        return *this;
    }

    [[nodiscard]] constexpr double_type<Precision, Scale> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr double_type<Precision, Scale>& get() noexcept {
        return value;
    }

    constexpr operator double_type<Precision, Scale> const&() const noexcept {
        return value;
    }
    constexpr operator double_type<Precision, Scale>&() noexcept {
        return value;
    }
};

template <uint32_t Precision, uint32_t Scale>
struct base<std::optional<double_type<Precision, Scale>>> : column_field_tag {
    using value_type = std::optional<double_type<Precision, Scale>>;

    std::optional<double_type<Precision, Scale>> value{};

    constexpr base() = default;
    constexpr base(std::nullopt_t) noexcept : value(std::nullopt) {
    }
    constexpr base(double_type<Precision, Scale> v) noexcept(
        std::is_nothrow_move_constructible_v<double_type<Precision, Scale>>)
        : value(std::move(v)) {
    }
    constexpr base(std::optional<double_type<Precision, Scale>> v) noexcept(
        std::is_nothrow_move_constructible_v<std::optional<double_type<Precision, Scale>>>)
        : value(std::move(v)) {
    }

    constexpr base& operator=(std::nullopt_t) noexcept {
        value = std::nullopt;
        return *this;
    }
    constexpr base& operator=(double_type<Precision, Scale> v) noexcept(
        std::is_nothrow_move_assignable_v<double_type<Precision, Scale>>) {
        value = std::move(v);
        return *this;
    }
    constexpr base& operator=(std::optional<double_type<Precision, Scale>> v) noexcept(
        std::is_nothrow_move_assignable_v<std::optional<double_type<Precision, Scale>>>) {
        value = std::move(v);
        return *this;
    }

    // Constructors and assignments from POD double type
    constexpr base(double v) noexcept : value(v) {
    }

    constexpr base& operator=(double v) noexcept {
        value = v;
        return *this;
    }

    [[nodiscard]] constexpr std::optional<double_type<Precision, Scale>> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr std::optional<double_type<Precision, Scale>>& get() noexcept {
        return value;
    }

    constexpr operator std::optional<double_type<Precision, Scale>> const&() const noexcept {
        return value;
    }
    constexpr operator std::optional<double_type<Precision, Scale>>&() noexcept {
        return value;
    }
};

// ===================================================================
// base<decimal_type<P, S>> specializations
// ===================================================================

template <uint32_t Precision, uint32_t Scale>
struct base<decimal_type<Precision, Scale>> : column_field_tag {
    using value_type = decimal_type<Precision, Scale>;

    decimal_type<Precision, Scale> value{};

    constexpr base() = default;
    constexpr base(decimal_type<Precision, Scale> v) noexcept(
        std::is_nothrow_move_constructible_v<decimal_type<Precision, Scale>>)
        : value(std::move(v)) {
    }

    constexpr base& operator=(decimal_type<Precision, Scale> v) noexcept(
        std::is_nothrow_move_assignable_v<decimal_type<Precision, Scale>>) {
        value = std::move(v);
        return *this;
    }

    // Constructors and assignments from POD double and long double types
    constexpr base(double v) noexcept : value(v) {
    }

    constexpr base(long double v) noexcept : value(static_cast<double>(v)) {
    }

    constexpr base& operator=(double v) noexcept {
        value = v;
        return *this;
    }

    constexpr base& operator=(long double v) noexcept {
        value = static_cast<double>(v);
        return *this;
    }

    [[nodiscard]] constexpr decimal_type<Precision, Scale> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr decimal_type<Precision, Scale>& get() noexcept {
        return value;
    }

    constexpr operator decimal_type<Precision, Scale> const&() const noexcept {
        return value;
    }
    constexpr operator decimal_type<Precision, Scale>&() noexcept {
        return value;
    }
};

template <uint32_t Precision, uint32_t Scale>
struct base<std::optional<decimal_type<Precision, Scale>>> : column_field_tag {
    using value_type = std::optional<decimal_type<Precision, Scale>>;

    std::optional<decimal_type<Precision, Scale>> value{};

    constexpr base() = default;
    constexpr base(std::nullopt_t) noexcept : value(std::nullopt) {
    }
    constexpr base(decimal_type<Precision, Scale> v) noexcept(
        std::is_nothrow_move_constructible_v<decimal_type<Precision, Scale>>)
        : value(std::move(v)) {
    }
    constexpr base(std::optional<decimal_type<Precision, Scale>> v) noexcept(
        std::is_nothrow_move_constructible_v<std::optional<decimal_type<Precision, Scale>>>)
        : value(std::move(v)) {
    }

    constexpr base& operator=(std::nullopt_t) noexcept {
        value = std::nullopt;
        return *this;
    }
    constexpr base& operator=(decimal_type<Precision, Scale> v) noexcept(
        std::is_nothrow_move_assignable_v<decimal_type<Precision, Scale>>) {
        value = std::move(v);
        return *this;
    }
    constexpr base& operator=(std::optional<decimal_type<Precision, Scale>> v) noexcept(
        std::is_nothrow_move_assignable_v<std::optional<decimal_type<Precision, Scale>>>) {
        value = std::move(v);
        return *this;
    }

    // Constructors and assignments from POD double and long double types
    constexpr base(double v) noexcept : value(v) {
    }

    constexpr base(long double v) noexcept : value(static_cast<double>(v)) {
    }

    constexpr base& operator=(double v) noexcept {
        value = v;
        return *this;
    }

    constexpr base& operator=(long double v) noexcept {
        value = static_cast<double>(v);
        return *this;
    }

    [[nodiscard]] constexpr std::optional<decimal_type<Precision, Scale>> const& get() const noexcept {
        return value;
    }
    [[nodiscard]] constexpr std::optional<decimal_type<Precision, Scale>>& get() noexcept {
        return value;
    }

    constexpr operator std::optional<decimal_type<Precision, Scale>> const&() const noexcept {
        return value;
    }
    constexpr operator std::optional<decimal_type<Precision, Scale>>&() noexcept {
        return value;
    }
};

}  // namespace column_field_detail

}  // namespace ds_mysql
