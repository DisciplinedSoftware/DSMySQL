#pragma once

#include <string_view>

namespace ds_mysql {

// Marker base class — inherit to identify a type as a database schema.
// Enables the Database concept and constrains create_database<T>() and use<T>().
struct database_schema {};

// Marker token type — use as a nested alias to explicitly identify an empty
// aggregate type as a table:
//
//   struct audit_log {
//       using ds_mysql_table_tag = ds_mysql::table_schema;
//   };
struct table_schema {};

// Strong type for SQL table identifiers.
class table_name {
    std::string_view value_;

public:
    constexpr explicit table_name(std::string_view sv) noexcept : value_(sv) {
    }

    [[nodiscard]] constexpr std::string_view to_string_view() const noexcept {
        return value_;
    }
};

// Strong type for SQL column identifiers.
class column_name {
    std::string_view value_;

public:
    constexpr explicit column_name(std::string_view sv) noexcept : value_(sv) {
    }

    [[nodiscard]] constexpr std::string_view to_string_view() const noexcept {
        return value_;
    }
};

// Strong type for SQL column alias identifiers (used with AS).
class column_alias {
    std::string_view value_;

public:
    constexpr explicit column_alias(std::string_view sv) noexcept : value_(sv) {
    }

    [[nodiscard]] constexpr std::string_view to_string_view() const noexcept {
        return value_;
    }
};

}  // namespace ds_mysql
