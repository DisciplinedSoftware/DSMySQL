#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <expected>
#include <string_view>

namespace ds_mysql {

enum class varchar_error {
    none,
    too_long,
};

namespace detail {
struct varchar_unchecked_tag {};
}  // namespace detail

template <std::size_t max_length>
class varchar_field {
public:
    static constexpr std::size_t capacity = max_length;

    constexpr varchar_field() noexcept = default;

    // Compile-time constructor for string literals — capacity validated at compile time.
    template <std::size_t M>
    constexpr varchar_field(char const (&str)[M]) noexcept : storage_(str, M - 1) {
        static_assert(M - 1 <= capacity, "string literal exceeds varchar_field capacity");
    }

    // Factory for runtime strings — validates capacity, returns std::expected.
    [[nodiscard]] static std::expected<varchar_field, varchar_error> create(std::string_view value) noexcept {
        if (value.size() > capacity) {
            return std::unexpected(varchar_error::too_long);
        }
        return varchar_field{detail::varchar_unchecked_tag{}, value};
    }

    [[nodiscard]] constexpr std::size_t size() const noexcept {
        return storage_.size();
    }

    [[nodiscard]] static consteval std::size_t max_size() noexcept {
        return capacity;
    }

    [[nodiscard]] constexpr std::string_view view() const noexcept {
        return {storage_.data(), storage_.size()};
    }

    [[nodiscard]] constexpr char const* c_str() const noexcept {
        return storage_.data();
    }

    [[nodiscard]] constexpr bool empty() const noexcept {
        return storage_.empty();
    }

    constexpr operator std::string_view() const noexcept {
        return view();
    }

    [[nodiscard]] constexpr bool operator==(varchar_field const& other) const noexcept {
        return view() == other.view();
    }

    [[nodiscard]] constexpr bool operator==(std::string_view other) const noexcept {
        return view() == other;
    }

    template <std::size_t M>
    [[nodiscard]] constexpr bool operator==(char const (&other)[M]) const noexcept {
        return view() == std::string_view{other, M - 1};
    }

private:
    varchar_field(detail::varchar_unchecked_tag, std::string_view value) noexcept : storage_(value) {
    }

    std::string_view storage_;
};

template <typename T>
struct is_varchar_field : std::false_type {};

template <std::size_t max_length>
struct is_varchar_field<varchar_field<max_length>> : std::true_type {
    static constexpr std::size_t capacity = max_length;
};

template <typename T>
constexpr bool is_varchar_field_v = is_varchar_field<T>::value;

}  // namespace ds_mysql
