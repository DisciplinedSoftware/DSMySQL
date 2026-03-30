#pragma once

#include <cstddef>
#include <expected>
#include <string>
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
class varchar_type {
public:
    static constexpr std::size_t capacity = max_length;

    constexpr varchar_type() noexcept = default;

    // Compile-time constructor for string literals — capacity validated at compile time.
    template <std::size_t M>
    constexpr varchar_type(char const (&str)[M]) : storage_(str, M - 1) {
        static_assert(M - 1 <= capacity, "string literal exceeds varchar_type capacity");
    }

    // Factory for runtime strings — validates capacity, returns std::expected.
    [[nodiscard]] static std::expected<varchar_type, varchar_error> create(std::string_view value) {
        if (value.size() > capacity) {
            return std::unexpected(varchar_error::too_long);
        }
        return varchar_type{detail::varchar_unchecked_tag{}, value};
    }

    [[nodiscard]] static std::string sql_type() {
        return "VARCHAR(" + std::to_string(capacity) + ")";
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return storage_.size();
    }

    [[nodiscard]] static consteval std::size_t max_size() noexcept {
        return capacity;
    }

    [[nodiscard]] std::string_view view() const noexcept {
        return storage_;
    }

    [[nodiscard]] char const* c_str() const noexcept {
        return storage_.c_str();
    }

    [[nodiscard]] bool empty() const noexcept {
        return storage_.empty();
    }

    operator std::string_view() const noexcept {
        return view();
    }

    [[nodiscard]] bool operator==(varchar_type const& other) const noexcept {
        return view() == other.view();
    }

    [[nodiscard]] bool operator==(std::string_view other) const noexcept {
        return view() == other;
    }

    template <std::size_t M>
    [[nodiscard]] bool operator==(char const (&other)[M]) const noexcept {
        return view() == std::string_view{other, M - 1};
    }

private:
    varchar_type(detail::varchar_unchecked_tag, std::string_view value) : storage_(value) {
    }

    std::string storage_;
};

template <typename T>
struct is_varchar_type : std::false_type {};

template <std::size_t max_length>
struct is_varchar_type<varchar_type<max_length>> : std::true_type {
    static constexpr std::size_t capacity = max_length;
};

template <typename T>
constexpr bool is_varchar_type_v = is_varchar_type<T>::value;

}  // namespace ds_mysql
