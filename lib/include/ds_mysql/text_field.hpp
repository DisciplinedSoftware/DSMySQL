#pragma once

#include <string>
#include <string_view>
#include <utility>

namespace ds_mysql {

enum class text_kind {
    text,        // standard SQL TEXT      (~65 KB)
    mediumtext,  // MySQL MEDIUMTEXT       (~16 MB)
    longtext,    // MySQL LONGTEXT         (~4 GB)
};

template <text_kind Kind = text_kind::text>
class text_field {
public:
    text_field() = default;
    explicit text_field(std::string value) : value_(std::move(value)) {
    }
    explicit text_field(std::string_view value) : value_(value) {
    }
    explicit text_field(char const* value) : value_(value) {
    }

    [[nodiscard]] std::string const& str() const noexcept {
        return value_;
    }
    [[nodiscard]] std::string_view view() const noexcept {
        return value_;
    }
    [[nodiscard]] bool empty() const noexcept {
        return value_.empty();
    }
    [[nodiscard]] std::size_t size() const noexcept {
        return value_.size();
    }

    operator std::string_view() const noexcept {
        return view();
    }

    [[nodiscard]] bool operator==(text_field const& other) const noexcept {
        return value_ == other.value_;
    }
    [[nodiscard]] bool operator==(std::string_view other) const noexcept {
        return value_ == other;
    }

private:
    std::string value_;
};

using mediumtext_field = text_field<text_kind::mediumtext>;
using longtext_field = text_field<text_kind::longtext>;

template <typename T>
struct is_text_field : std::false_type {};

template <text_kind Kind>
struct is_text_field<text_field<Kind>> : std::true_type {
    static constexpr text_kind kind = Kind;
};

template <typename T>
constexpr bool is_text_field_v = is_text_field<T>::value;

}  // namespace ds_mysql
