#pragma once

#include "ds_mysql/sql_core.hpp"

namespace ds_mysql {

enum class Engine {
    InnoDB,
    MyISAM,
    Memory,
    Ndb,
    Archive,
    Csv,
    Merge,
    Blackhole,
    Federated,
};

enum class Charset {
    utf8mb4,
    utf8,
    latin1,
    ascii,
    ucs2,
    utf16,
    utf32,
};

enum class RowFormat {
    Default,
    Dynamic,
    Fixed,
    Compressed,
    Redundant,
    Compact,
};

enum class InsertMethod {
    No,
    First,
    Last,
};

enum class PackKeys {
    Default,
    Zero,
    One,
};

enum class StatsPolicy {
    Default,
    Zero,
    One,
};

enum class Compression {
    None,
    Zlib,
    Lz4,
};

enum class Encryption {
    Y,
    N,
};

// ===================================================================
// check_id<"name">   — compile-time CHECK constraint name type.
// index_id<"name">   — compile-time index name type.
// trigger_id<"name"> — compile-time trigger name type.
//
// All satisfy NamedIdType: a static name() method returns the compile-time name.
// User-defined tagged types also satisfy NamedIdType if they provide name().
//
// Examples:
//   table_constraint::check<check_id<"chk_positive_price">>(greater_than<my_table::price>(0.0))
//   // → CONSTRAINT chk_positive_price CHECK (price > 0)
//
//   create_index_on<index_id<"idx_foo">, my_table, my_table::col>()
//   // → CREATE INDEX idx_foo ON my_table (col);
//
//   create_trigger<trigger_id<"trg_before_insert">, my_table>(TriggerTiming::Before, ...)
//   // → CREATE TRIGGER trg_before_insert BEFORE INSERT ON my_table FOR EACH ROW ...
// ===================================================================

template <fixed_string Name>
struct check_id {
    [[nodiscard]] static constexpr std::string_view name() noexcept {
        return Name;
    }
};

template <fixed_string Name>
struct index_id {
    [[nodiscard]] static constexpr std::string_view name() noexcept {
        return Name;
    }
};

template <fixed_string Name>
struct trigger_id {
    [[nodiscard]] static constexpr std::string_view name() noexcept {
        return Name;
    }
};

// NamedIdType — satisfied by check_id<N>, index_id<N>, trigger_id<N>, and any type with static name() → string_view.
template <typename T>
concept NamedIdType = requires {
    { T::name() } -> std::convertible_to<std::string_view>;
};

// ===================================================================
// CREATE TABLE constraint customization
//
// table_inline_primary_key<T>
//   - true  (default): first column is emitted as `PRIMARY KEY AUTO_INCREMENT`
//   - false          : first column is emitted as `AUTO_INCREMENT` only,
//                      allowing table_constraints<T> to define table-level PK
//
// table_constraints<T>
//   - return additional table-level constraints/index definitions appended
//     inside CREATE TABLE (...)
// ===================================================================

template <typename T>
struct table_inline_primary_key {
    static constexpr bool value = true;
};

template <typename T>
struct table_constraints {
    [[nodiscard]] static std::vector<std::string> get() {
        return {};
    }
};

namespace table_constraint {

template <ColumnDescriptor... Cols>
    requires(sizeof...(Cols) > 0)
[[nodiscard]] inline std::string columns_sql() {
    std::string s;
    s += '(';
    bool first = true;
    (((s += (first ? "" : ", "), s += column_traits<Cols>::column_name(), first = false)), ...);
    s += ')';
    return s;
}

template <ColumnDescriptor... Cols>
    requires(sizeof...(Cols) > 0)
[[nodiscard]] inline std::string primary_key() {
    return "PRIMARY KEY " + columns_sql<Cols...>();
}

template <NamedIdType IndexId, ColumnDescriptor... Cols>
    requires(sizeof...(Cols) > 0)
[[nodiscard]] inline std::string key() {
    return "KEY " + std::string(IndexId::name()) + ' ' + columns_sql<Cols...>();
}

template <NamedIdType IndexId, ColumnDescriptor... Cols>
    requires(sizeof...(Cols) > 0)
[[nodiscard]] inline std::string unique_key() {
    return "UNIQUE KEY " + std::string(IndexId::name()) + ' ' + columns_sql<Cols...>();
}

template <NamedIdType IndexId, ColumnDescriptor... Cols>
    requires(sizeof...(Cols) > 0)
[[nodiscard]] inline std::string fulltext_key() {
    return "FULLTEXT KEY " + std::string(IndexId::name()) + ' ' + columns_sql<Cols...>();
}

template <NamedIdType IndexId, ColumnDescriptor... Cols>
    requires(sizeof...(Cols) > 0)
[[nodiscard]] inline std::string spatial_key() {
    return "SPATIAL KEY " + std::string(IndexId::name()) + ' ' + columns_sql<Cols...>();
}

template <ColumnDescriptor... Cols>
    requires(sizeof...(Cols) > 0)
[[nodiscard]] inline std::string foreign_key(std::string_view constraint_name, std::string_view referenced_table,
                                             std::string_view referenced_columns,
                                             std::string_view on_delete_action = {},
                                             std::string_view on_update_action = {}) {
    std::string s;
    s += "CONSTRAINT ";
    s += constraint_name;
    s += " FOREIGN KEY ";
    s += columns_sql<Cols...>();
    s += " REFERENCES ";
    s += referenced_table;
    s += '(';
    s += referenced_columns;
    s += ')';
    if (!on_delete_action.empty()) {
        s += " ON DELETE ";
        s += on_delete_action;
    }
    if (!on_update_action.empty()) {
        s += " ON UPDATE ";
        s += on_update_action;
    }
    return s;
}

// Accepts a check_expr predicate; column name, operator, and value are all
// derived from the typed predicate.
//
// Without constraint name:
//   table_constraint::check(greater_than<my_table::price>(0.0))
//   // → CHECK (price > 0)
//
// With constraint name as template parameter:
//   table_constraint::check<check_id<"chk_positive_price">>(greater_than<my_table::price>(0.0))
//   // → CONSTRAINT chk_positive_price CHECK (price > 0)
[[nodiscard]] inline std::string check(check_expr const& expr) {
    return "CHECK (" + expr.build_sql() + ')';
}

template <NamedIdType CId>
[[nodiscard]] inline std::string check(check_expr const& expr) {
    return "CONSTRAINT " + std::string(CId::name()) + " CHECK (" + expr.build_sql() + ')';
}

[[nodiscard]] inline std::string raw(std::string sql_fragment) {
    return sql_fragment;
}

}  // namespace table_constraint

// ===================================================================
// DDL Command Builders — CREATE TABLE, DROP TABLE, CREATE DATABASE, etc.
//
// Entry points are free functions:
//   create_database<DB>()                                         — CREATE DATABASE db
//   create_database<DB>().if_not_exists()                         — CREATE DATABASE IF NOT EXISTS db
//   create_table<T>()                                              — CREATE TABLE T (...)
//   create_table<T>().if_not_exists()                             — CREATE TABLE IF NOT EXISTS T (...)
//   create_temporary_table<T>()                                   — CREATE TEMPORARY TABLE T (...)
//   create_temporary_table<T>().if_not_exists()                   — CREATE TEMPORARY TABLE IF NOT EXISTS T (...)
//   drop_table<T>()                                               — DROP TABLE T
//   drop_table<T>().if_exists()                                   — DROP TABLE IF EXISTS T
//   drop_temporary_table<T>()                                     — DROP TEMPORARY TABLE T
//   drop_temporary_table<T>().if_exists()                         — DROP TEMPORARY TABLE IF EXISTS T
//   create_table<NewT>().as(select_query)                         — CREATE TABLE NewT AS <select>
//   create_table<NewT>().if_not_exists().as(select_query)         — CREATE TABLE IF NOT EXISTS NewT AS <select>
//   create_temporary_table<NewT>().as(select_query)               — CREATE TEMPORARY TABLE NewT AS <select>
//   create_temporary_table<NewT>().if_not_exists().as(select_query) — CREATE TEMPORARY TABLE IF NOT EXISTS NewT AS
//   <select>
//   create_table<NewT>().like<SourceT>()                          — CREATE TABLE NewT LIKE SourceT
//   create_table<NewT>().if_not_exists().like<SourceT>()          — CREATE TABLE IF NOT EXISTS NewT LIKE SourceT
//
// Commands can be sequenced with .then():
//   create_database<DB>().then().create_table<T>().if_not_exists()
//   drop_table<T>().if_exists().then().create_table<T>().if_not_exists()
//
// All stages terminate with .build_sql() -> std::string.
// Note: use<DB>() sets the default database for the session.
// ==================================================================="}

namespace ddl_detail {

[[nodiscard]] inline std::string escape_sql_single_quotes(std::string_view value) {
    std::string out;
    out.reserve(value.size());
    for (const char ch : value) {
        if (ch == '\'') {
            out += "''";
        } else {
            out += ch;
        }
    }
    return out;
}

[[nodiscard]] inline std::string quote_sql_string(std::string_view value) {
    std::string out;
    out.reserve(value.size() + 2);
    out += '\'';
    out += escape_sql_single_quotes(value);
    out += '\'';
    return out;
}

[[nodiscard]] inline std::string to_sql_engine(Engine value) {
    switch (value) {
        case Engine::InnoDB:
            return "InnoDB";
        case Engine::MyISAM:
            return "MyISAM";
        case Engine::Memory:
            return "MEMORY";
        case Engine::Ndb:
            return "NDB";
        case Engine::Archive:
            return "ARCHIVE";
        case Engine::Csv:
            return "CSV";
        case Engine::Merge:
            return "MERGE";
        case Engine::Blackhole:
            return "BLACKHOLE";
        case Engine::Federated:
            return "FEDERATED";
    }
    return "InnoDB";
}

[[nodiscard]] inline std::string to_sql_charset(Charset value) {
    switch (value) {
        case Charset::utf8mb4:
            return "utf8mb4";
        case Charset::utf8:
            return "utf8";
        case Charset::latin1:
            return "latin1";
        case Charset::ascii:
            return "ascii";
        case Charset::ucs2:
            return "ucs2";
        case Charset::utf16:
            return "utf16";
        case Charset::utf32:
            return "utf32";
    }
    return "utf8mb4";
}

[[nodiscard]] inline std::string to_sql_row_format(RowFormat value) {
    switch (value) {
        case RowFormat::Default:
            return "DEFAULT";
        case RowFormat::Dynamic:
            return "DYNAMIC";
        case RowFormat::Fixed:
            return "FIXED";
        case RowFormat::Compressed:
            return "COMPRESSED";
        case RowFormat::Redundant:
            return "REDUNDANT";
        case RowFormat::Compact:
            return "COMPACT";
    }
    return "DEFAULT";
}

[[nodiscard]] inline std::string to_sql_insert_method(InsertMethod value) {
    switch (value) {
        case InsertMethod::No:
            return "NO";
        case InsertMethod::First:
            return "FIRST";
        case InsertMethod::Last:
            return "LAST";
    }
    return "NO";
}

[[nodiscard]] inline std::string to_sql_pack_keys(PackKeys value) {
    switch (value) {
        case PackKeys::Default:
            return "DEFAULT";
        case PackKeys::Zero:
            return "0";
        case PackKeys::One:
            return "1";
    }
    return "DEFAULT";
}

[[nodiscard]] inline std::string to_sql_stats_policy(StatsPolicy value) {
    switch (value) {
        case StatsPolicy::Default:
            return "DEFAULT";
        case StatsPolicy::Zero:
            return "0";
        case StatsPolicy::One:
            return "1";
    }
    return "DEFAULT";
}

[[nodiscard]] inline std::string to_sql_compression(Compression value) {
    switch (value) {
        case Compression::None:
            return "NONE";
        case Compression::Zlib:
            return "ZLIB";
        case Compression::Lz4:
            return "LZ4";
    }
    return "NONE";
}

[[nodiscard]] inline std::string to_sql_encryption(Encryption value) {
    switch (value) {
        case Encryption::Y:
            return "Y";
        case Encryption::N:
            return "N";
    }
    return "N";
}

namespace table_option_sql {

inline constexpr std::string_view engine_key = "ENGINE";
inline constexpr std::string_view engine_prefix = "ENGINE=";

inline constexpr std::string_view auto_increment_key = "AUTO_INCREMENT";
inline constexpr std::string_view auto_increment_prefix = "AUTO_INCREMENT=";

inline constexpr std::string_view avg_row_length_key = "AVG_ROW_LENGTH";
inline constexpr std::string_view avg_row_length_prefix = "AVG_ROW_LENGTH=";

inline constexpr std::string_view default_charset_key = "DEFAULT CHARSET";
inline constexpr std::string_view default_charset_prefix = "DEFAULT CHARSET=";

inline constexpr std::string_view collate_key = "COLLATE";
inline constexpr std::string_view collate_prefix = "COLLATE=";

inline constexpr std::string_view checksum_key = "CHECKSUM";
inline constexpr std::string_view checksum_prefix = "CHECKSUM=";

inline constexpr std::string_view comment_key = "COMMENT";
inline constexpr std::string_view comment_prefix = "COMMENT=";

inline constexpr std::string_view compression_key = "COMPRESSION";
inline constexpr std::string_view compression_prefix = "COMPRESSION=";

inline constexpr std::string_view connection_key = "CONNECTION";
inline constexpr std::string_view connection_prefix = "CONNECTION=";

inline constexpr std::string_view data_directory_key = "DATA DIRECTORY";
inline constexpr std::string_view data_directory_prefix = "DATA DIRECTORY=";

inline constexpr std::string_view index_directory_key = "INDEX DIRECTORY";
inline constexpr std::string_view index_directory_prefix = "INDEX DIRECTORY=";

inline constexpr std::string_view delay_key_write_key = "DELAY_KEY_WRITE";
inline constexpr std::string_view delay_key_write_prefix = "DELAY_KEY_WRITE=";

inline constexpr std::string_view encryption_key = "ENCRYPTION";
inline constexpr std::string_view encryption_prefix = "ENCRYPTION=";

inline constexpr std::string_view insert_method_key = "INSERT_METHOD";
inline constexpr std::string_view insert_method_prefix = "INSERT_METHOD=";

inline constexpr std::string_view key_block_size_key = "KEY_BLOCK_SIZE";
inline constexpr std::string_view key_block_size_prefix = "KEY_BLOCK_SIZE=";

inline constexpr std::string_view max_rows_key = "MAX_ROWS";
inline constexpr std::string_view max_rows_prefix = "MAX_ROWS=";

inline constexpr std::string_view min_rows_key = "MIN_ROWS";
inline constexpr std::string_view min_rows_prefix = "MIN_ROWS=";

inline constexpr std::string_view pack_keys_key = "PACK_KEYS";
inline constexpr std::string_view pack_keys_prefix = "PACK_KEYS=";

inline constexpr std::string_view password_key = "PASSWORD";
inline constexpr std::string_view password_prefix = "PASSWORD=";

inline constexpr std::string_view row_format_key = "ROW_FORMAT";
inline constexpr std::string_view row_format_prefix = "ROW_FORMAT=";

inline constexpr std::string_view stats_auto_recalc_key = "STATS_AUTO_RECALC";
inline constexpr std::string_view stats_auto_recalc_prefix = "STATS_AUTO_RECALC=";

inline constexpr std::string_view stats_persistent_key = "STATS_PERSISTENT";
inline constexpr std::string_view stats_persistent_prefix = "STATS_PERSISTENT=";

inline constexpr std::string_view stats_sample_pages_key = "STATS_SAMPLE_PAGES";
inline constexpr std::string_view stats_sample_pages_prefix = "STATS_SAMPLE_PAGES=";

inline constexpr std::string_view tablespace_key = "TABLESPACE";
inline constexpr std::string_view tablespace_prefix = "TABLESPACE=";

inline constexpr std::string_view union_key = "UNION";

[[nodiscard]] inline std::string assign(std::string_view prefix, std::string_view value) {
    std::string out;
    out.reserve(prefix.size() + value.size());
    out += prefix;
    out += value;
    return out;
}

[[nodiscard]] inline std::string assign_quoted(std::string_view prefix, std::string_view value) {
    std::string out;
    out.reserve(prefix.size() + value.size() + 2);
    out += prefix;
    out += quote_sql_string(value);
    return out;
}

[[nodiscard]] inline std::string assign_numeric(std::string_view prefix, std::size_t value) {
    return std::format("{}{}", prefix, value);
}

[[nodiscard]] inline std::string assign_bool(std::string_view prefix, bool enabled) {
    std::string out;
    out.reserve(prefix.size() + 1);
    out += prefix;
    out += enabled ? "1" : "0";
    return out;
}

[[nodiscard]] inline std::string union_tables_value(std::vector<std::string> const& table_names) {
    std::string sql = "UNION=(";
    for (std::size_t i = 0; i < table_names.size(); ++i) {
        if (i > 0) {
            sql += ",";
        }
        sql += table_names[i];
    }
    sql += ")";
    return sql;
}

}  // namespace table_option_sql

}  // namespace ddl_detail

struct create_table_option {
    void set(std::string key, std::string value_sql) {
        auto it = std::find_if(options.begin(), options.end(), [&](auto const& kv) {
            return kv.first == key;
        });
        if (it != options.end()) {
            it->second = std::move(value_sql);
        } else {
            options.emplace_back(std::move(key), std::move(value_sql));
        }
    }

    [[nodiscard]] std::string to_sql() const {
        std::string out;
        for (auto const& [_, value_sql] : options) {
            out += " ";
            out += value_sql;
        }
        return out;
    }

    // Fluent API for table attributes
    create_table_option& engine(Engine value) {
        return engine(ddl_detail::to_sql_engine(value));
    }

    create_table_option& engine(std::string_view value) {
        set(std::string{ddl_detail::table_option_sql::engine_key},
            ddl_detail::table_option_sql::assign(ddl_detail::table_option_sql::engine_prefix, value));
        return *this;
    }

    create_table_option& auto_increment(std::size_t value) {
        set(std::string{ddl_detail::table_option_sql::auto_increment_key},
            ddl_detail::table_option_sql::assign_numeric(ddl_detail::table_option_sql::auto_increment_prefix, value));
        return *this;
    }

    create_table_option& avg_row_length(std::size_t value) {
        set(std::string{ddl_detail::table_option_sql::avg_row_length_key},
            ddl_detail::table_option_sql::assign_numeric(ddl_detail::table_option_sql::avg_row_length_prefix, value));
        return *this;
    }

    create_table_option& default_charset(Charset value) {
        return default_charset(ddl_detail::to_sql_charset(value));
    }

    create_table_option& default_charset(std::string_view value) {
        set(std::string{ddl_detail::table_option_sql::default_charset_key},
            ddl_detail::table_option_sql::assign(ddl_detail::table_option_sql::default_charset_prefix, value));
        return *this;
    }

    create_table_option& collate(std::string_view value) {
        set(std::string{ddl_detail::table_option_sql::collate_key},
            ddl_detail::table_option_sql::assign(ddl_detail::table_option_sql::collate_prefix, value));
        return *this;
    }

    create_table_option& checksum(bool enabled) {
        set(std::string{ddl_detail::table_option_sql::checksum_key},
            ddl_detail::table_option_sql::assign_bool(ddl_detail::table_option_sql::checksum_prefix, enabled));
        return *this;
    }

    create_table_option& checksum(std::size_t value) {
        set(std::string{ddl_detail::table_option_sql::checksum_key},
            ddl_detail::table_option_sql::assign_numeric(ddl_detail::table_option_sql::checksum_prefix, value));
        return *this;
    }

    create_table_option& comment(std::string_view value) {
        set(std::string{ddl_detail::table_option_sql::comment_key},
            ddl_detail::table_option_sql::assign_quoted(ddl_detail::table_option_sql::comment_prefix, value));
        return *this;
    }

    create_table_option& compression(Compression value) {
        return compression(ddl_detail::to_sql_compression(value));
    }

    create_table_option& compression(std::string_view value) {
        set(std::string{ddl_detail::table_option_sql::compression_key},
            ddl_detail::table_option_sql::assign_quoted(ddl_detail::table_option_sql::compression_prefix, value));
        return *this;
    }

    create_table_option& connection(std::string_view value) {
        set(std::string{ddl_detail::table_option_sql::connection_key},
            ddl_detail::table_option_sql::assign_quoted(ddl_detail::table_option_sql::connection_prefix, value));
        return *this;
    }

    create_table_option& data_directory(std::string_view value) {
        set(std::string{ddl_detail::table_option_sql::data_directory_key},
            ddl_detail::table_option_sql::assign_quoted(ddl_detail::table_option_sql::data_directory_prefix, value));
        return *this;
    }

    create_table_option& index_directory(std::string_view value) {
        set(std::string{ddl_detail::table_option_sql::index_directory_key},
            ddl_detail::table_option_sql::assign_quoted(ddl_detail::table_option_sql::index_directory_prefix, value));
        return *this;
    }

    create_table_option& delay_key_write(bool enabled) {
        set(std::string{ddl_detail::table_option_sql::delay_key_write_key},
            ddl_detail::table_option_sql::assign_bool(ddl_detail::table_option_sql::delay_key_write_prefix, enabled));
        return *this;
    }

    create_table_option& delay_key_write(std::size_t value) {
        set(std::string{ddl_detail::table_option_sql::delay_key_write_key},
            ddl_detail::table_option_sql::assign_numeric(ddl_detail::table_option_sql::delay_key_write_prefix, value));
        return *this;
    }

    create_table_option& encryption(Encryption value) {
        return encryption(ddl_detail::to_sql_encryption(value));
    }

    create_table_option& encryption(std::string_view value) {
        set(std::string{ddl_detail::table_option_sql::encryption_key},
            ddl_detail::table_option_sql::assign_quoted(ddl_detail::table_option_sql::encryption_prefix, value));
        return *this;
    }

    create_table_option& insert_method(InsertMethod value) {
        return insert_method(ddl_detail::to_sql_insert_method(value));
    }

    create_table_option& insert_method(std::string_view value) {
        set(std::string{ddl_detail::table_option_sql::insert_method_key},
            ddl_detail::table_option_sql::assign(ddl_detail::table_option_sql::insert_method_prefix, value));
        return *this;
    }

    create_table_option& key_block_size(std::size_t value) {
        set(std::string{ddl_detail::table_option_sql::key_block_size_key},
            ddl_detail::table_option_sql::assign_numeric(ddl_detail::table_option_sql::key_block_size_prefix, value));
        return *this;
    }

    create_table_option& max_rows(std::size_t value) {
        set(std::string{ddl_detail::table_option_sql::max_rows_key},
            ddl_detail::table_option_sql::assign_numeric(ddl_detail::table_option_sql::max_rows_prefix, value));
        return *this;
    }

    create_table_option& min_rows(std::size_t value) {
        set(std::string{ddl_detail::table_option_sql::min_rows_key},
            ddl_detail::table_option_sql::assign_numeric(ddl_detail::table_option_sql::min_rows_prefix, value));
        return *this;
    }

    create_table_option& pack_keys(PackKeys value) {
        return pack_keys(ddl_detail::to_sql_pack_keys(value));
    }

    create_table_option& pack_keys(std::string_view value) {
        set(std::string{ddl_detail::table_option_sql::pack_keys_key},
            ddl_detail::table_option_sql::assign(ddl_detail::table_option_sql::pack_keys_prefix, value));
        return *this;
    }

    create_table_option& password(std::string_view value) {
        set(std::string{ddl_detail::table_option_sql::password_key},
            ddl_detail::table_option_sql::assign_quoted(ddl_detail::table_option_sql::password_prefix, value));
        return *this;
    }

    create_table_option& row_format(RowFormat value) {
        return row_format(ddl_detail::to_sql_row_format(value));
    }

    create_table_option& row_format(std::string_view value) {
        set(std::string{ddl_detail::table_option_sql::row_format_key},
            ddl_detail::table_option_sql::assign(ddl_detail::table_option_sql::row_format_prefix, value));
        return *this;
    }

    create_table_option& stats_auto_recalc(StatsPolicy value) {
        return stats_auto_recalc(ddl_detail::to_sql_stats_policy(value));
    }

    create_table_option& stats_auto_recalc(std::string_view value) {
        set(std::string{ddl_detail::table_option_sql::stats_auto_recalc_key},
            ddl_detail::table_option_sql::assign(ddl_detail::table_option_sql::stats_auto_recalc_prefix, value));
        return *this;
    }

    create_table_option& stats_persistent(StatsPolicy value) {
        return stats_persistent(ddl_detail::to_sql_stats_policy(value));
    }

    create_table_option& stats_persistent(std::string_view value) {
        set(std::string{ddl_detail::table_option_sql::stats_persistent_key},
            ddl_detail::table_option_sql::assign(ddl_detail::table_option_sql::stats_persistent_prefix, value));
        return *this;
    }

    create_table_option& stats_sample_pages(std::size_t value) {
        set(std::string{ddl_detail::table_option_sql::stats_sample_pages_key},
            ddl_detail::table_option_sql::assign_numeric(ddl_detail::table_option_sql::stats_sample_pages_prefix,
                                                         value));
        return *this;
    }

    create_table_option& tablespace(std::string_view value) {
        set(std::string{ddl_detail::table_option_sql::tablespace_key},
            ddl_detail::table_option_sql::assign(ddl_detail::table_option_sql::tablespace_prefix, value));
        return *this;
    }

    create_table_option& union_tables(std::vector<std::string> table_names) {
        set(std::string{ddl_detail::table_option_sql::union_key},
            ddl_detail::table_option_sql::union_tables_value(table_names));
        return *this;
    }

    std::vector<std::pair<std::string, std::string>> options;
};

namespace ddl_detail {

template <typename Derived>
class create_table_attributes_mixin {
public:
    Derived& engine(Engine value) {
        return engine(to_sql_engine(value));
    }

    Derived& engine(std::string_view value) {
        return set(std::string{table_option_sql::engine_key},
                   table_option_sql::assign(table_option_sql::engine_prefix, value));
    }

    Derived& auto_increment(std::size_t value) {
        return set(std::string{table_option_sql::auto_increment_key},
                   table_option_sql::assign_numeric(table_option_sql::auto_increment_prefix, value));
    }

    Derived& avg_row_length(std::size_t value) {
        return set(std::string{table_option_sql::avg_row_length_key},
                   table_option_sql::assign_numeric(table_option_sql::avg_row_length_prefix, value));
    }

    Derived& default_charset(Charset value) {
        return default_charset(to_sql_charset(value));
    }

    Derived& default_charset(std::string_view value) {
        return set(std::string{table_option_sql::default_charset_key},
                   table_option_sql::assign(table_option_sql::default_charset_prefix, value));
    }

    Derived& collate(std::string_view value) {
        return set(std::string{table_option_sql::collate_key},
                   table_option_sql::assign(table_option_sql::collate_prefix, value));
    }

    Derived& checksum(bool enabled) {
        return set(std::string{table_option_sql::checksum_key},
                   table_option_sql::assign_bool(table_option_sql::checksum_prefix, enabled));
    }

    Derived& checksum(std::size_t value) {
        return set(std::string{table_option_sql::checksum_key},
                   table_option_sql::assign_numeric(table_option_sql::checksum_prefix, value));
    }

    Derived& comment(std::string_view value) {
        return set(std::string{table_option_sql::comment_key},
                   table_option_sql::assign_quoted(table_option_sql::comment_prefix, value));
    }

    Derived& compression(Compression value) {
        return compression(to_sql_compression(value));
    }

    Derived& compression(std::string_view value) {
        return set(std::string{table_option_sql::compression_key},
                   table_option_sql::assign_quoted(table_option_sql::compression_prefix, value));
    }

    Derived& connection(std::string_view value) {
        return set(std::string{table_option_sql::connection_key},
                   table_option_sql::assign_quoted(table_option_sql::connection_prefix, value));
    }

    Derived& data_directory(std::string_view value) {
        return set(std::string{table_option_sql::data_directory_key},
                   table_option_sql::assign_quoted(table_option_sql::data_directory_prefix, value));
    }

    Derived& index_directory(std::string_view value) {
        return set(std::string{table_option_sql::index_directory_key},
                   table_option_sql::assign_quoted(table_option_sql::index_directory_prefix, value));
    }

    Derived& delay_key_write(bool enabled) {
        return set(std::string{table_option_sql::delay_key_write_key},
                   table_option_sql::assign_bool(table_option_sql::delay_key_write_prefix, enabled));
    }

    Derived& delay_key_write(std::size_t value) {
        return set(std::string{table_option_sql::delay_key_write_key},
                   table_option_sql::assign_numeric(table_option_sql::delay_key_write_prefix, value));
    }

    Derived& encryption(Encryption value) {
        return encryption(to_sql_encryption(value));
    }

    Derived& encryption(std::string_view value) {
        return set(std::string{table_option_sql::encryption_key},
                   table_option_sql::assign_quoted(table_option_sql::encryption_prefix, value));
    }

    Derived& insert_method(InsertMethod value) {
        return insert_method(to_sql_insert_method(value));
    }

    Derived& insert_method(std::string_view value) {
        return set(std::string{table_option_sql::insert_method_key},
                   table_option_sql::assign(table_option_sql::insert_method_prefix, value));
    }

    Derived& key_block_size(std::size_t value) {
        return set(std::string{table_option_sql::key_block_size_key},
                   table_option_sql::assign_numeric(table_option_sql::key_block_size_prefix, value));
    }

    Derived& max_rows(std::size_t value) {
        return set(std::string{table_option_sql::max_rows_key},
                   table_option_sql::assign_numeric(table_option_sql::max_rows_prefix, value));
    }

    Derived& min_rows(std::size_t value) {
        return set(std::string{table_option_sql::min_rows_key},
                   table_option_sql::assign_numeric(table_option_sql::min_rows_prefix, value));
    }

    Derived& pack_keys(PackKeys value) {
        return pack_keys(to_sql_pack_keys(value));
    }

    Derived& pack_keys(std::string_view value) {
        return set(std::string{table_option_sql::pack_keys_key},
                   table_option_sql::assign(table_option_sql::pack_keys_prefix, value));
    }

    Derived& password(std::string_view value) {
        return set(std::string{table_option_sql::password_key},
                   table_option_sql::assign_quoted(table_option_sql::password_prefix, value));
    }

    Derived& row_format(RowFormat value) {
        return row_format(to_sql_row_format(value));
    }

    Derived& row_format(std::string_view value) {
        return set(std::string{table_option_sql::row_format_key},
                   table_option_sql::assign(table_option_sql::row_format_prefix, value));
    }

    Derived& stats_auto_recalc(StatsPolicy value) {
        return stats_auto_recalc(to_sql_stats_policy(value));
    }

    Derived& stats_auto_recalc(std::string_view value) {
        return set(std::string{table_option_sql::stats_auto_recalc_key},
                   table_option_sql::assign(table_option_sql::stats_auto_recalc_prefix, value));
    }

    Derived& stats_persistent(StatsPolicy value) {
        return stats_persistent(to_sql_stats_policy(value));
    }

    Derived& stats_persistent(std::string_view value) {
        return set(std::string{table_option_sql::stats_persistent_key},
                   table_option_sql::assign(table_option_sql::stats_persistent_prefix, value));
    }

    Derived& stats_sample_pages(std::size_t value) {
        return set(std::string{table_option_sql::stats_sample_pages_key},
                   table_option_sql::assign_numeric(table_option_sql::stats_sample_pages_prefix, value));
    }

    Derived& tablespace(std::string_view value) {
        return set(std::string{table_option_sql::tablespace_key},
                   table_option_sql::assign(table_option_sql::tablespace_prefix, value));
    }

    Derived& union_tables(std::vector<std::string> table_names) {
        return set(std::string{table_option_sql::union_key}, table_option_sql::union_tables_value(table_names));
    }

protected:
    Derived& set(std::string key, std::string value_sql) {
        derived().set_table_option(std::move(key), std::move(value_sql));
        return derived();
    }

private:
    [[nodiscard]] Derived& derived() {
        return static_cast<Derived&>(*this);
    }
};

namespace database_option_sql {

inline constexpr std::string_view default_character_set_key = "DEFAULT CHARACTER SET";
inline constexpr std::string_view default_character_set_prefix = "DEFAULT CHARACTER SET ";

inline constexpr std::string_view default_collate_key = "DEFAULT COLLATE";
inline constexpr std::string_view default_collate_prefix = "DEFAULT COLLATE ";

[[nodiscard]] inline std::string assign(std::string_view prefix, std::string_view value) {
    std::string out;
    out.reserve(prefix.size() + value.size());
    out += prefix;
    out += value;
    return out;
}

}  // namespace database_option_sql

}  // namespace ddl_detail

struct create_database_option {
    [[nodiscard]] std::string to_sql() const {
        std::string out;
        for (auto const& [_, value_sql] : options) {
            out += " ";
            out += value_sql;
        }
        return out;
    }

    create_database_option& default_charset(Charset value) {
        return default_charset(ddl_detail::to_sql_charset(value));
    }

    create_database_option& default_charset(std::string_view value) {
        set(std::string{ddl_detail::database_option_sql::default_character_set_key},
            ddl_detail::database_option_sql::assign(ddl_detail::database_option_sql::default_character_set_prefix,
                                                    value));
        return *this;
    }

    create_database_option& default_collate(std::string_view value) {
        set(std::string{ddl_detail::database_option_sql::default_collate_key},
            ddl_detail::database_option_sql::assign(ddl_detail::database_option_sql::default_collate_prefix, value));
        return *this;
    }

    std::vector<std::pair<std::string, std::string>> options;

private:
    void set(std::string key, std::string value_sql) {
        auto it = std::find_if(options.begin(), options.end(), [&](auto const& kv) {
            return kv.first == key;
        });
        if (it != options.end()) {
            it->second = std::move(value_sql);
        } else {
            options.emplace_back(std::move(key), std::move(value_sql));
        }
    }
};

namespace ddl_detail {

template <typename Derived>
class create_database_attributes_mixin {
public:
    Derived& default_charset(Charset value) {
        return default_charset(to_sql_charset(value));
    }

    Derived& default_charset(std::string_view value) {
        options_.default_charset(value);
        return derived();
    }

    Derived& default_collate(std::string_view value) {
        options_.default_collate(value);
        return derived();
    }

protected:
    explicit create_database_attributes_mixin(create_database_option options = {}) : options_(std::move(options)) {
    }

    create_database_option options_;

private:
    [[nodiscard]] Derived& derived() {
        return static_cast<Derived&>(*this);
    }
};

template <typename T>
struct database_attributes {
    [[nodiscard]] static create_database_option get() {
        return {};  // Empty by default
    }
};

template <typename T, std::size_t I>
[[nodiscard]] std::string column_def_for() {
    using field_type = boost::pfr::tuple_element_t<I, T>;
    std::string_view field_name = field_schema<T, I>::name();
    std::string sql_type_override = field_sql_type_override<T, I>();
    std::string col = "    ";
    col += field_name;
    col += " ";
    col += sql_type_override.empty() ? sql_type_for<field_type>() : sql_type_override;
    if constexpr (!is_field_nullable_v<field_type>) {
        col += " NOT NULL";
    }
    if constexpr (I == 0) {
        if constexpr (table_inline_primary_key<T>::value) {
            col += " PRIMARY KEY AUTO_INCREMENT";
        }
        // When inline PK is disabled, typed column attributes should handle AUTO_INCREMENT
    }
    if constexpr (requires { field_type::ddl_auto_increment; } && field_type::ddl_auto_increment) {
        col += " AUTO_INCREMENT";
    }
    if constexpr (requires { field_type::ddl_unique; } && field_type::ddl_unique) {
        col += " UNIQUE";
    }
    if constexpr (requires { field_type::ddl_default_current_timestamp; } &&
                  field_type::ddl_default_current_timestamp) {
        col += " DEFAULT CURRENT_TIMESTAMP";
    }
    if constexpr (requires { field_type::ddl_collate; } && !field_type::ddl_collate.empty()) {
        col += " COLLATE ";
        col += field_type::ddl_collate;
    }
    if constexpr (requires { field_type::ddl_on_update_current_timestamp; } &&
                  field_type::ddl_on_update_current_timestamp) {
        col += " ON UPDATE CURRENT_TIMESTAMP";
    }
    if constexpr (requires { field_type::ddl_comment; } && !field_type::ddl_comment.empty()) {
        col += " COMMENT ";
        col += quote_sql_string(field_type::ddl_comment);
    }
    return col;
}

template <typename T, std::size_t I>
[[nodiscard]] std::string fk_clause_for() {
    using field_type = boost::pfr::tuple_element_t<I, T>;
    if constexpr (requires { field_type::ddl_has_fk; } && field_type::ddl_has_fk) {
        std::string fk = ",\n    FOREIGN KEY (";
        fk += field_schema<T, I>::name();
        fk += ") REFERENCES ";
        fk += table_name_for<typename field_type::ddl_fk_ref_table>::value().to_string_view();
        fk += "(";
        fk += field_type::ddl_fk_ref_column::column_name();
        fk += ")";
        if constexpr (!field_type::ddl_fk_on_delete.empty()) {
            fk += " ON DELETE ";
            fk += field_type::ddl_fk_on_delete;
        }
        if constexpr (!field_type::ddl_fk_on_update.empty()) {
            fk += " ON UPDATE ";
            fk += field_type::ddl_fk_on_update;
        }
        return fk;
    } else {
        return {};
    }
}

template <typename T, std::size_t... Is>
void append_column_defs(std::string& sql, std::index_sequence<Is...>) {
    if constexpr (sizeof...(Is) > 0) {
        std::string col_defs[] = {column_def_for<T, Is>()...};
        for (std::size_t i = 0; i < sizeof...(Is); ++i) {
            if (i > 0)
                sql += ",\n";
            sql += col_defs[i];
        }
        std::string fk_clauses[] = {fk_clause_for<T, Is>()...};
        for (auto const& fk : fk_clauses) {
            sql += fk;
        }
    }
    // Emit additional table-level constraints and index definitions.
    for (auto const& constraint_sql : table_constraints<T>::get()) {
        if (!constraint_sql.empty()) {
            sql += ",\n    ";
            sql += constraint_sql;
        }
    }
}

template <typename T>
[[nodiscard]] std::string make_column_defs() {
    std::string sql;
    append_column_defs<T>(sql, std::make_index_sequence<boost::pfr::tuple_size_v<T>>{});
    return sql;
}

// Sentinel prior type for top-level builders (no preceding DDL statement)
struct no_prior {
    [[nodiscard]] std::string build_sql() const {
        return {};
    }
};

// Prior type for sub-step builders that bake their SQL into a string
// (used when a builder cannot carry the full prior chain as a template parameter)
struct sql_string_builder {
    explicit sql_string_builder(std::string sql) : sql_(std::move(sql)) {
    }
    [[nodiscard]] std::string build_sql() const {
        return sql_;
    }

private:
    std::string sql_;
};

template <typename Table>
void append_create_table_sql(std::string& sql, bool if_not_exists, std::string_view db_name = {}) {
    const auto table_name = table_name_for<Table>::value().to_string_view();
    const auto column_defs = make_column_defs<Table>();
    sql += "CREATE TABLE ";
    if (if_not_exists) {
        sql += "IF NOT EXISTS ";
    }
    if (!db_name.empty()) {
        sql += db_name;
        sql += '.';
    }
    sql += table_name;
    sql += " (\n";
    sql += column_defs;
    sql += "\n);\n";
}

template <Database DB, bool IfNotExists, std::size_t... Is>
[[nodiscard]] std::string build_create_all_tables_sql_impl(std::string prior_sql, std::index_sequence<Is...>) {
    using tables_tuple = typename database_tables<DB>::type;
    const auto db_name = database_name_for<DB>::value();
    (append_create_table_sql<std::tuple_element_t<Is, tables_tuple>>(prior_sql, IfNotExists, db_name), ...);
    return prior_sql;
}

template <Database DB, bool IfNotExists>
[[nodiscard]] std::string build_create_all_tables_sql(std::string prior_sql) {
    using tables_tuple = typename database_tables<DB>::type;
    return build_create_all_tables_sql_impl<DB, IfNotExists>(
        std::move(prior_sql), std::make_index_sequence<std::tuple_size_v<tables_tuple>>{});
}

// Forward declarations
template <SqlBuilder Prior>
class ddl_continuation;
template <typename T, SqlBuilder Prior>
class create_table_builder;
template <typename T>
class create_table_cond_builder;
template <typename T>
class create_table_as_builder;
template <typename T, typename SourceT>
class create_table_like_builder;
template <typename T, SqlBuilder Prior>
class drop_table_builder;
template <typename T>
class drop_table_cond_builder;
template <typename T, SqlBuilder Prior>
class create_database_builder;
template <typename T, SqlBuilder Prior>
class create_database_if_not_exists_builder;
template <typename T, SqlBuilder Prior>
class drop_database_builder;
template <typename T, SqlBuilder Prior>
class drop_database_if_exists_builder;
template <SqlBuilder Prior>
class create_database_named_builder;
template <SqlBuilder Prior>
class create_database_named_if_not_exists_builder;
template <SqlBuilder Prior>
class drop_database_named_builder;
template <SqlBuilder Prior>
class drop_database_named_if_exists_builder;
template <Database DB, SqlBuilder Prior>
class create_all_tables_builder;
template <Database DB, SqlBuilder Prior>
class create_all_tables_cond_builder;
template <typename T, SqlBuilder Prior>
class use_builder;
template <SqlBuilder Prior>
class use_name_builder;

// ---------------------------------------------------------------
// ddl_continuation — returned by .then(), allows chaining DDL statements.
// Stores the prior builder and delegates build_sql() to it.
// ---------------------------------------------------------------
template <SqlBuilder Prior = no_prior>
class ddl_continuation {
public:
    using ddl_tag_type = void;

    explicit ddl_continuation(Prior builder) : builder_(std::move(builder)) {
    }

    [[nodiscard]] std::string build_sql() const {
        return builder_.build_sql();
    }

    template <typename T>
    [[nodiscard]] auto create_table() const;

    template <typename T>
    [[nodiscard]] auto create_temporary_table() const;

    template <typename T>
    [[nodiscard]] auto drop_table() const;

    template <typename T>
    [[nodiscard]] auto drop_temporary_table() const;

    template <Database T>
    [[nodiscard]] auto create_database() const;

    [[nodiscard]] auto create_database(database_name name) const;

    template <Database T>
    [[nodiscard]] auto drop_database() const;

    [[nodiscard]] auto drop_database(database_name name) const;

    // USE <database> — sets default database for the session.
    template <Database T>
    [[nodiscard]] auto use() const;

    [[nodiscard]] auto use(database_name name) const;

    template <typename T>
    [[nodiscard]] auto create_view() const;

    template <typename T>
    [[nodiscard]] auto drop_view() const;

    template <typename From, typename To>
    [[nodiscard]] auto rename_table() const;

    template <Database DB>
    [[nodiscard]] auto create_all_tables() const;

private:
    Prior builder_;
};

template <typename T>
class create_view_as_builder;

template <typename T, SqlBuilder Prior = no_prior>
class create_view_builder {
public:
    using ddl_tag_type = void;

    explicit create_view_builder(Prior prior = {}, bool or_replace = false)
        : prior_(std::move(prior)), or_replace_(or_replace) {
    }

    [[nodiscard]] create_view_builder or_replace() const {
        return create_view_builder{prior_, true};
    }

    template <SqlBuilder SelectQuery>
    [[nodiscard]] create_view_as_builder<T> as(SelectQuery const& query) const {
        return create_view_as_builder<T>{prior_.build_sql(), or_replace_, query.build_sql()};
    }

private:
    Prior prior_;
    bool or_replace_;
};

template <typename T>
class create_view_as_builder {
public:
    using ddl_tag_type = void;

    create_view_as_builder(std::string prior, bool or_replace, std::string select_sql)
        : prior_sql_(std::move(prior)), or_replace_(or_replace), select_sql_(std::move(select_sql)) {
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<sql_string_builder>{sql_string_builder{build_sql()}};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto view_name = table_name_for<T>::value().to_string_view();
        std::string s;
        s.reserve(prior_sql_.size() + 20 + view_name.size() + select_sql_.size());
        s += prior_sql_;
        s += "CREATE ";
        if (or_replace_) {
            s += "OR REPLACE ";
        }
        s += "VIEW ";
        s += view_name;
        s += " AS ";
        s += select_sql_;
        s += ";\n";
        return s;
    }

private:
    std::string prior_sql_;
    bool or_replace_;
    std::string select_sql_;
};

template <typename T>
class drop_view_cond_builder {
public:
    using ddl_tag_type = void;

    explicit drop_view_cond_builder(std::string prefix) : prefix_(std::move(prefix)) {
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<sql_string_builder>{sql_string_builder{build_sql()}};
    }

    [[nodiscard]] std::string build_sql() const {
        return prefix_ + "IF EXISTS " + std::string(table_name_for<T>::value().to_string_view()) + ";\n";
    }

private:
    std::string prefix_;
};

template <typename T, SqlBuilder Prior = no_prior>
class drop_view_builder {
public:
    using ddl_tag_type = void;

    explicit drop_view_builder(Prior prior = {}) : prior_(std::move(prior)) {
    }

    [[nodiscard]] drop_view_cond_builder<T> if_exists() const {
        return drop_view_cond_builder<T>{prior_.build_sql() + "DROP VIEW "};
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<drop_view_builder<T, Prior>>{*this};
    }

    [[nodiscard]] std::string build_sql() const {
        return prior_.build_sql() + "DROP VIEW " + std::string(table_name_for<T>::value().to_string_view()) + ";\n";
    }

private:
    Prior prior_;
};

template <typename From, typename To, SqlBuilder Prior = no_prior>
class rename_table_builder {
public:
    using ddl_tag_type = void;

    explicit rename_table_builder(Prior prior = {}) : prior_(std::move(prior)) {
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<rename_table_builder<From, To, Prior>>{*this};
    }

    [[nodiscard]] std::string build_sql() const {
        return prior_.build_sql() + "RENAME TABLE " + std::string(table_name_for<From>::value().to_string_view()) +
               " TO " + std::string(table_name_for<To>::value().to_string_view()) + ";\n";
    }

private:
    Prior prior_;
};

template <typename Table, ColumnDescriptor... Cols>
class create_index_builder {
public:
    using ddl_tag_type = void;

    create_index_builder(std::string name, bool unique = false) : name_(std::move(name)), unique_(unique) {
    }

    [[nodiscard]] create_index_builder unique() const {
        return create_index_builder{name_, true};
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<sql_string_builder>{sql_string_builder{build_sql()}};
    }

    [[nodiscard]] std::string build_sql() const {
        std::string s;
        s.reserve(24 + name_.size() + table_name_for<Table>::value().to_string_view().size() + 4);
        s += "CREATE ";
        if (unique_) {
            s += "UNIQUE ";
        }
        s += "INDEX ";
        s += name_;
        s += " ON ";
        s += table_name_for<Table>::value().to_string_view();
        s += " (";
        bool first = true;
        (((s += (first ? "" : ", "), s += column_traits<Cols>::column_name(), first = false)), ...);
        s += ");\n";
        return s;
    }

private:
    std::string name_;
    bool unique_;
};

template <typename Table>
class drop_index_builder {
public:
    using ddl_tag_type = void;

    explicit drop_index_builder(std::string name) : name_(std::move(name)) {
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<sql_string_builder>{sql_string_builder{build_sql()}};
    }

    [[nodiscard]] std::string build_sql() const {
        return "DROP INDEX " + name_ + " ON " + std::string(table_name_for<Table>::value().to_string_view()) + ";\n";
    }

private:
    std::string name_;
};

template <typename Table, SqlBuilder Prior = no_prior>
class alter_table_builder {
public:
    using ddl_tag_type = void;

    explicit alter_table_builder(Prior prior = {}, std::vector<std::string> actions = {})
        : prior_(std::move(prior)), actions_(std::move(actions)) {
    }

    template <ColumnDescriptor Col>
    [[nodiscard]] alter_table_builder add_column() const {
        auto copy = *this;
        std::string action =
            "ADD COLUMN " + std::string(column_traits<Col>::column_name()) + " " + sql_type_name<Col>::value();
        if (!is_field_nullable_v<Col>) {
            action += " NOT NULL";
        }
        copy.actions_.push_back(std::move(action));
        return copy;
    }

    template <ColumnDescriptor Col>
    [[nodiscard]] alter_table_builder drop_column() const {
        auto copy = *this;
        copy.actions_.push_back("DROP COLUMN " + std::string(column_traits<Col>::column_name()));
        return copy;
    }

    // RENAME COLUMN old_name TO new_name
    // NewCol must be a ColumnDescriptor with the same value_type as OldCol.
    template <ColumnDescriptor OldCol, ColumnDescriptor NewCol>
        requires std::same_as<typename OldCol::value_type, typename NewCol::value_type>
    [[nodiscard]] alter_table_builder rename_column() const {
        auto copy = *this;
        copy.actions_.push_back("RENAME COLUMN " + std::string(column_traits<OldCol>::column_name()) + " TO " +
                                std::string(column_traits<NewCol>::column_name()));
        return copy;
    }

    template <ColumnDescriptor Col>
    [[nodiscard]] alter_table_builder modify_column() const {
        auto copy = *this;
        std::string action =
            "MODIFY COLUMN " + std::string(column_traits<Col>::column_name()) + " " + sql_type_name<Col>::value();
        if (!is_field_nullable_v<Col>) {
            action += " NOT NULL";
        }
        copy.actions_.push_back(std::move(action));
        return copy;
    }

    template <typename NewTable>
    [[nodiscard]] alter_table_builder rename_to() const {
        auto copy = *this;
        copy.actions_.push_back("RENAME TO " + std::string(table_name_for<NewTable>::value().to_string_view()));
        return copy;
    }

    [[nodiscard]] alter_table_builder action(std::string sql_action) const {
        auto copy = *this;
        copy.actions_.push_back(std::move(sql_action));
        return copy;
    }

    [[nodiscard]] alter_table_builder add_constraint(std::string constraint_sql) const {
        auto copy = *this;
        copy.actions_.push_back("ADD CONSTRAINT " + std::move(constraint_sql));
        return copy;
    }

    [[nodiscard]] alter_table_builder drop_constraint(std::string constraint_name) const {
        auto copy = *this;
        copy.actions_.push_back("DROP CONSTRAINT " + std::move(constraint_name));
        return copy;
    }

    [[nodiscard]] alter_table_builder drop_foreign_key(std::string constraint_name) const {
        auto copy = *this;
        copy.actions_.push_back("DROP FOREIGN KEY " + std::move(constraint_name));
        return copy;
    }

    // CHANGE COLUMN old_name new_name new_type [NOT NULL]
    // OldCol: existing column (provides the current name).
    // NewCol: ColumnDescriptor providing the new column name and type; use std::optional<T> in NewCol's
    //         value type to make the column nullable.
    template <ColumnDescriptor OldCol, ColumnDescriptor NewCol>
    [[nodiscard]] alter_table_builder change_column() const {
        auto copy = *this;
        std::string act = "CHANGE COLUMN " + std::string(column_traits<OldCol>::column_name()) + " " +
                          std::string(column_traits<NewCol>::column_name()) + " " + sql_type_name<NewCol>::value();
        if (!is_field_nullable_v<NewCol>) {
            act += " NOT NULL";
        }
        copy.actions_.push_back(std::move(act));
        return copy;
    }

    // ADD INDEX name (col1, col2, ...)
    template <NamedIdType IndexId, ColumnDescriptor... Cols>
        requires(sizeof...(Cols) > 0)
    [[nodiscard]] alter_table_builder add_index() const {
        auto copy = *this;
        copy.actions_.push_back("ADD INDEX " + std::string(IndexId::name()) + " " +
                                table_constraint::columns_sql<Cols...>());
        return copy;
    }

    // ADD UNIQUE INDEX name (col1, col2, ...)
    template <NamedIdType IndexId, ColumnDescriptor... Cols>
        requires(sizeof...(Cols) > 0)
    [[nodiscard]] alter_table_builder add_unique_index() const {
        auto copy = *this;
        copy.actions_.push_back("ADD UNIQUE INDEX " + std::string(IndexId::name()) + " " +
                                table_constraint::columns_sql<Cols...>());
        return copy;
    }

    // ADD FULLTEXT INDEX name (col1, col2, ...)
    template <NamedIdType IndexId, ColumnDescriptor... Cols>
        requires(sizeof...(Cols) > 0)
    [[nodiscard]] alter_table_builder add_fulltext_index() const {
        auto copy = *this;
        copy.actions_.push_back("ADD FULLTEXT INDEX " + std::string(IndexId::name()) + " " +
                                table_constraint::columns_sql<Cols...>());
        return copy;
    }

    // ENABLE KEYS — re-enable non-unique index updates (MyISAM)
    [[nodiscard]] alter_table_builder enable_keys() const {
        auto copy = *this;
        copy.actions_.push_back("ENABLE KEYS");
        return copy;
    }

    // DISABLE KEYS — disable non-unique index updates (MyISAM)
    [[nodiscard]] alter_table_builder disable_keys() const {
        auto copy = *this;
        copy.actions_.push_back("DISABLE KEYS");
        return copy;
    }

    // CONVERT TO CHARACTER SET charset
    [[nodiscard]] alter_table_builder convert_to_charset(Charset charset) const {
        auto copy = *this;
        copy.actions_.push_back("CONVERT TO CHARACTER SET " + to_sql_charset(charset));
        return copy;
    }

    // ENGINE = new_engine
    [[nodiscard]] alter_table_builder set_engine(Engine engine) const {
        auto copy = *this;
        copy.actions_.push_back("ENGINE = " + to_sql_engine(engine));
        return copy;
    }

    // AUTO_INCREMENT = n
    [[nodiscard]] alter_table_builder set_auto_increment(std::size_t n) const {
        auto copy = *this;
        copy.actions_.push_back("AUTO_INCREMENT = " + std::to_string(n));
        return copy;
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<alter_table_builder<Table, Prior>>{*this};
    }

    [[nodiscard]] std::string build_sql() const {
        std::string s = prior_.build_sql();
        s += "ALTER TABLE ";
        s += table_name_for<Table>::value().to_string_view();
        s += ' ';
        bool first = true;
        for (auto const& action : actions_) {
            if (!first) {
                s += ", ";
            }
            s += action;
            first = false;
        }
        s += ";\n";
        return s;
    }

private:
    Prior prior_;
    std::vector<std::string> actions_;
};

// ---------------------------------------------------------------
// create_table_cond_builder — after create_table<T>().if_not_exists()
// ---------------------------------------------------------------
template <typename T>
class create_table_cond_builder : public create_table_attributes_mixin<create_table_cond_builder<T>> {
public:
    using ddl_tag_type = void;

    create_table_cond_builder(std::string create_prefix, std::string cols, create_table_option options = {})
        : create_prefix_(std::move(create_prefix)), column_defs_(std::move(cols)), options_(std::move(options)) {
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<sql_string_builder>{sql_string_builder{build_sql()}};
    }

    template <SqlBuilder SelectQuery>
    [[nodiscard]] create_table_as_builder<T> as(SelectQuery const& query) const;

    template <typename SourceT>
    [[nodiscard]] create_table_like_builder<T, SourceT> like() const {
        return create_table_like_builder<T, SourceT>{create_prefix_ + "IF NOT EXISTS "};
    }

    void set_table_option(std::string key, std::string value_sql) {
        options_.set(std::move(key), std::move(value_sql));
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        std::string s;
        s.reserve(create_prefix_.size() + 14 + table_name.size() + 4 + column_defs_.size() + 4);
        s += create_prefix_;
        s += "IF NOT EXISTS ";
        s += table_name;
        s += " (\n";
        s += column_defs_;
        s += "\n)";
        s += options_.to_sql();
        s += ";\n";
        return s;
    }

private:
    std::string create_prefix_;
    std::string column_defs_;
    create_table_option options_;
};

}  // namespace ddl_detail

// Table attributes trait - specialize for each table to define default CREATE TABLE options
// e.g.: template <> struct ds_mysql::table_attributes<MyTable> {
//           static create_table_option get() {
//               return create_table_option{}.engine(Engine::InnoDB).default_charset(Charset::utf8mb4);
//           }
//       };
template <typename T>
struct table_attributes {
    [[nodiscard]] static create_table_option get() {
        return {};  // Empty by default
    }
};

namespace ddl_detail {

// ---------------------------------------------------------------
// create_table_builder — after create_table<T>() or create_temporary_table<T>()
// ---------------------------------------------------------------
template <typename T, SqlBuilder Prior = no_prior>
class create_table_builder : public create_table_attributes_mixin<create_table_builder<T, Prior>> {
public:
    using ddl_tag_type = void;

    explicit create_table_builder(bool temporary = false, Prior prior = {})
        : prior_(std::move(prior)),
          is_temporary_(temporary),
          column_defs_(ddl_detail::make_column_defs<T>()),
          options_(table_attributes<T>::get()) {
    }

    [[nodiscard]] create_table_cond_builder<T> if_not_exists() const {
        return create_table_cond_builder<T>{build_create_prefix(), column_defs_, options_};
    }

    template <SqlBuilder SelectQuery>
    [[nodiscard]] create_table_as_builder<T> as(SelectQuery const& query) const {
        return create_table_as_builder<T>{build_create_prefix(), query.build_sql(), false, options_};
    }

    template <typename SourceT>
    [[nodiscard]] create_table_like_builder<T, SourceT> like() const {
        return create_table_like_builder<T, SourceT>{build_create_prefix()};
    }

    void set_table_option(std::string key, std::string value_sql) {
        options_.set(std::move(key), std::move(value_sql));
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<create_table_builder<T, Prior>>{*this};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        const auto prefix = build_create_prefix();
        std::string s;
        s.reserve(prefix.size() + table_name.size() + 4 + column_defs_.size() + 4);
        s += prefix;
        s += table_name;
        s += " (\n";
        s += column_defs_;
        s += "\n)";
        s += options_.to_sql();
        s += ";\n";
        return s;
    }

private:
    [[nodiscard]] std::string build_create_prefix() const {
        return prior_.build_sql() + "CREATE " + (is_temporary_ ? "TEMPORARY " : "") + "TABLE ";
    }

    Prior prior_;
    bool is_temporary_;
    std::string column_defs_;
    create_table_option options_;
};

// ---------------------------------------------------------------
// create_table_like_builder — after create_table<NewT>().like<SourceT>() or
//                                    create_table<NewT>().if_not_exists().like<SourceT>()
// ---------------------------------------------------------------
template <typename T, typename SourceT>
class create_table_like_builder {
public:
    using ddl_tag_type = void;

    explicit create_table_like_builder(std::string create_prefix) : create_prefix_(std::move(create_prefix)) {
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<sql_string_builder>{sql_string_builder{build_sql()}};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto new_table = table_name_for<T>::value().to_string_view();
        const auto src_table = table_name_for<SourceT>::value().to_string_view();
        std::string s;
        s.reserve(create_prefix_.size() + new_table.size() + 7 + src_table.size() + 2);
        s += create_prefix_;
        s += new_table;
        s += " LIKE ";
        s += src_table;
        s += ";\n";
        return s;
    }

private:
    std::string create_prefix_;
};

// ---------------------------------------------------------------
// drop_table_cond_builder — after drop_table<T>().if_exists()
// ---------------------------------------------------------------
template <typename T>
class drop_table_cond_builder {
public:
    using ddl_tag_type = void;

    explicit drop_table_cond_builder(std::string drop_prefix) : drop_prefix_(std::move(drop_prefix)) {
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<sql_string_builder>{sql_string_builder{build_sql()}};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        std::string s;
        s.reserve(drop_prefix_.size() + 11 + table_name.size() + 2);
        s += drop_prefix_;
        s += "IF EXISTS ";
        s += table_name;
        s += ";\n";
        return s;
    }

private:
    std::string drop_prefix_;
};

// ---------------------------------------------------------------
// drop_table_builder — after drop_table<T>() or drop_temporary_table<T>()
// ---------------------------------------------------------------
template <typename T, SqlBuilder Prior = no_prior>
class drop_table_builder {
public:
    using ddl_tag_type = void;

    explicit drop_table_builder(bool temporary = false, Prior prior = {})
        : prior_(std::move(prior)), is_temporary_(temporary) {
    }

    [[nodiscard]] drop_table_cond_builder<T> if_exists() const {
        return drop_table_cond_builder<T>{build_drop_prefix()};
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<drop_table_builder<T, Prior>>{*this};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        const auto prefix = build_drop_prefix();
        std::string s;
        s.reserve(prefix.size() + table_name.size() + 2);
        s += prefix;
        s += table_name;
        s += ";\n";
        return s;
    }

private:
    [[nodiscard]] std::string build_drop_prefix() const {
        return prior_.build_sql() + "DROP " + (is_temporary_ ? "TEMPORARY " : "") + "TABLE ";
    }

    Prior prior_;
    bool is_temporary_;
};

// ---------------------------------------------------------------
// create_database_if_not_exists_builder — after create_database<DB>().if_not_exists()
// ---------------------------------------------------------------
template <typename T, SqlBuilder Prior = no_prior>
class create_database_if_not_exists_builder
    : public create_database_attributes_mixin<create_database_if_not_exists_builder<T, Prior>> {
public:
    using ddl_tag_type = void;

    explicit create_database_if_not_exists_builder(Prior prior = {},
                                                   create_database_option options = database_attributes<T>::get())
        : create_database_attributes_mixin<create_database_if_not_exists_builder<T, Prior>>(std::move(options)),
          prior_(std::move(prior)) {
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<create_database_if_not_exists_builder<T, Prior>>{*this};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto db_name = database_name_for<T>::value();
        std::string s = prior_.build_sql();
        s += "CREATE DATABASE IF NOT EXISTS ";
        s += db_name;
        s += this->options_.to_sql();
        s += ";\n";
        return s;
    }

private:
    Prior prior_;
};

// ---------------------------------------------------------------
// create_database_builder — after create_database<DB>()
// ---------------------------------------------------------------
template <typename T, SqlBuilder Prior = no_prior>
class create_database_builder : public create_database_attributes_mixin<create_database_builder<T, Prior>> {
public:
    using ddl_tag_type = void;

    explicit create_database_builder(Prior prior = {}, create_database_option options = database_attributes<T>::get())
        : create_database_attributes_mixin<create_database_builder<T, Prior>>(std::move(options)),
          prior_(std::move(prior)) {
    }

    [[nodiscard]] create_database_if_not_exists_builder<T, Prior> if_not_exists() const {
        return create_database_if_not_exists_builder<T, Prior>{prior_, this->options_};
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<create_database_builder<T, Prior>>{*this};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto db_name = database_name_for<T>::value();
        std::string s = prior_.build_sql();
        s += "CREATE DATABASE ";
        s += db_name;
        s += this->options_.to_sql();
        s += ";\n";
        return s;
    }

private:
    Prior prior_;
};

// ---------------------------------------------------------------
// create_database_named_if_not_exists_builder — runtime database name
// ---------------------------------------------------------------
template <SqlBuilder Prior = no_prior>
class create_database_named_if_not_exists_builder
    : public create_database_attributes_mixin<create_database_named_if_not_exists_builder<Prior>> {
public:
    using ddl_tag_type = void;

    explicit create_database_named_if_not_exists_builder(database_name name, Prior prior = {},
                                                         create_database_option options = {})
        : create_database_attributes_mixin<create_database_named_if_not_exists_builder<Prior>>(std::move(options)),
          prior_(std::move(prior)),
          name_(std::move(name)) {
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<create_database_named_if_not_exists_builder<Prior>>{*this};
    }

    [[nodiscard]] std::string build_sql() const {
        return prior_.build_sql() + "CREATE DATABASE IF NOT EXISTS " + name_.to_string() + this->options_.to_sql() +
               ";\n";
    }

private:
    Prior prior_;
    database_name name_;
};

// ---------------------------------------------------------------
// create_database_named_builder — runtime database name
// ---------------------------------------------------------------
template <SqlBuilder Prior = no_prior>
class create_database_named_builder : public create_database_attributes_mixin<create_database_named_builder<Prior>> {
public:
    using ddl_tag_type = void;

    explicit create_database_named_builder(database_name name, Prior prior = {}, create_database_option options = {})
        : create_database_attributes_mixin<create_database_named_builder<Prior>>(std::move(options)),
          prior_(std::move(prior)),
          name_(std::move(name)) {
    }

    [[nodiscard]] create_database_named_if_not_exists_builder<Prior> if_not_exists() const {
        return create_database_named_if_not_exists_builder<Prior>{name_, prior_, this->options_};
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<create_database_named_builder<Prior>>{*this};
    }

    [[nodiscard]] std::string build_sql() const {
        return prior_.build_sql() + "CREATE DATABASE " + name_.to_string() + this->options_.to_sql() + ";\n";
    }

private:
    Prior prior_;
    database_name name_;
};

// ---------------------------------------------------------------
// create_all_tables_cond_builder — after create_all_tables<DB>().if_not_exists()
// ---------------------------------------------------------------
template <Database DB, SqlBuilder Prior = no_prior>
class create_all_tables_cond_builder {
public:
    using ddl_tag_type = void;

    explicit create_all_tables_cond_builder(Prior prior = {}) : prior_(std::move(prior)) {
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<create_all_tables_cond_builder<DB, Prior>>{*this};
    }

    [[nodiscard]] std::string build_sql() const {
        return build_create_all_tables_sql<DB, true>(prior_.build_sql());
    }

private:
    Prior prior_;
};

// ---------------------------------------------------------------
// create_all_tables_builder — emits CREATE TABLE for every table in DB
// ---------------------------------------------------------------
template <Database DB, SqlBuilder Prior = no_prior>
class create_all_tables_builder {
public:
    using ddl_tag_type = void;

    explicit create_all_tables_builder(Prior prior = {}) : prior_(std::move(prior)) {
    }

    [[nodiscard]] create_all_tables_cond_builder<DB, Prior> if_not_exists() const {
        return create_all_tables_cond_builder<DB, Prior>{prior_};
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<create_all_tables_builder<DB, Prior>>{*this};
    }

    [[nodiscard]] std::string build_sql() const {
        return build_create_all_tables_sql<DB, false>(prior_.build_sql());
    }

private:
    Prior prior_;
};

// ---------------------------------------------------------------
// drop_database_if_exists_builder — after drop_database<DB>().if_exists()
// ---------------------------------------------------------------
template <typename T, SqlBuilder Prior = no_prior>
class drop_database_if_exists_builder {
public:
    using ddl_tag_type = void;

    explicit drop_database_if_exists_builder(Prior prior = {}) : prior_(std::move(prior)) {
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<drop_database_if_exists_builder<T, Prior>>{*this};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto db_name = database_name_for<T>::value();
        std::string s = prior_.build_sql();
        s += "DROP DATABASE IF EXISTS ";
        s += db_name;
        s += ";\n";
        return s;
    }

private:
    Prior prior_;
};

// ---------------------------------------------------------------
// drop_database_builder — after drop_database<DB>()
// ---------------------------------------------------------------
template <typename T, SqlBuilder Prior = no_prior>
class drop_database_builder {
public:
    using ddl_tag_type = void;

    explicit drop_database_builder(Prior prior = {}) : prior_(std::move(prior)) {
    }

    [[nodiscard]] drop_database_if_exists_builder<T, Prior> if_exists() const {
        return drop_database_if_exists_builder<T, Prior>{prior_};
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<drop_database_builder<T, Prior>>{*this};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto db_name = database_name_for<T>::value();
        std::string s = prior_.build_sql();
        s += "DROP DATABASE ";
        s += db_name;
        s += ";\n";
        return s;
    }

private:
    Prior prior_;
};

// ---------------------------------------------------------------
// drop_database_named_if_exists_builder — runtime database name
// ---------------------------------------------------------------
template <SqlBuilder Prior = no_prior>
class drop_database_named_if_exists_builder {
public:
    using ddl_tag_type = void;

    explicit drop_database_named_if_exists_builder(database_name name, Prior prior = {})
        : prior_(std::move(prior)), name_(std::move(name)) {
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<drop_database_named_if_exists_builder<Prior>>{*this};
    }

    [[nodiscard]] std::string build_sql() const {
        return prior_.build_sql() + "DROP DATABASE IF EXISTS " + name_.to_string() + ";\n";
    }

private:
    Prior prior_;
    database_name name_;
};

// ---------------------------------------------------------------
// drop_database_named_builder — runtime database name
// ---------------------------------------------------------------
template <SqlBuilder Prior = no_prior>
class drop_database_named_builder {
public:
    using ddl_tag_type = void;

    explicit drop_database_named_builder(database_name name, Prior prior = {})
        : prior_(std::move(prior)), name_(std::move(name)) {
    }

    [[nodiscard]] drop_database_named_if_exists_builder<Prior> if_exists() const {
        return drop_database_named_if_exists_builder<Prior>{name_, prior_};
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<drop_database_named_builder<Prior>>{*this};
    }

    [[nodiscard]] std::string build_sql() const {
        return prior_.build_sql() + "DROP DATABASE " + name_.to_string() + ";\n";
    }

private:
    Prior prior_;
    database_name name_;
};

// Out-of-line definitions for ddl_continuation<Prior>
template <SqlBuilder Prior>
template <typename T>
[[nodiscard]] auto ddl_continuation<Prior>::create_table() const {
    return create_table_builder<T, ddl_continuation<Prior>>{false, *this};
}

template <SqlBuilder Prior>
template <typename T>
[[nodiscard]] auto ddl_continuation<Prior>::create_temporary_table() const {
    return create_table_builder<T, ddl_continuation<Prior>>{true, *this};
}

template <SqlBuilder Prior>
template <typename T>
[[nodiscard]] auto ddl_continuation<Prior>::drop_table() const {
    return drop_table_builder<T, ddl_continuation<Prior>>{false, *this};
}

template <SqlBuilder Prior>
template <typename T>
[[nodiscard]] auto ddl_continuation<Prior>::drop_temporary_table() const {
    return drop_table_builder<T, ddl_continuation<Prior>>{true, *this};
}

template <SqlBuilder Prior>
template <Database T>
[[nodiscard]] auto ddl_continuation<Prior>::create_database() const {
    return create_database_builder<T, ddl_continuation<Prior>>{*this};
}

template <SqlBuilder Prior>
[[nodiscard]] auto ddl_continuation<Prior>::create_database(database_name name) const {
    return create_database_named_builder<ddl_continuation<Prior>>{std::move(name), *this};
}

template <SqlBuilder Prior>
template <Database T>
[[nodiscard]] auto ddl_continuation<Prior>::drop_database() const {
    return drop_database_builder<T, ddl_continuation<Prior>>{*this};
}

template <SqlBuilder Prior>
[[nodiscard]] auto ddl_continuation<Prior>::drop_database(database_name name) const {
    return drop_database_named_builder<ddl_continuation<Prior>>{std::move(name), *this};
}

template <SqlBuilder Prior>
template <typename T>
[[nodiscard]] auto ddl_continuation<Prior>::create_view() const {
    return create_view_builder<T, ddl_continuation<Prior>>{*this};
}

template <SqlBuilder Prior>
template <typename T>
[[nodiscard]] auto ddl_continuation<Prior>::drop_view() const {
    return drop_view_builder<T, ddl_continuation<Prior>>{*this};
}

template <SqlBuilder Prior>
template <typename From, typename To>
[[nodiscard]] auto ddl_continuation<Prior>::rename_table() const {
    return rename_table_builder<From, To, ddl_continuation<Prior>>{*this};
}

template <SqlBuilder Prior>
template <Database DB>
[[nodiscard]] auto ddl_continuation<Prior>::create_all_tables() const {
    return create_all_tables_builder<DB, ddl_continuation<Prior>>{*this};
}

// ---------------------------------------------------------------
// create_table_as_builder — after create_table<T>().as(query) or
//                           create_table<T>().if_not_exists().as(query)
// ---------------------------------------------------------------
template <typename T>
class create_table_as_builder : public create_table_attributes_mixin<create_table_as_builder<T>> {
public:
    using ddl_tag_type = void;

    create_table_as_builder(std::string create_prefix, std::string select_sql, bool if_not_exists,
                            create_table_option options = {})
        : create_prefix_(std::move(create_prefix)),
          select_sql_(std::move(select_sql)),
          if_not_exists_(if_not_exists),
          options_(std::move(options)) {
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<sql_string_builder>{sql_string_builder{build_sql()}};
    }

    void set_table_option(std::string key, std::string value_sql) {
        options_.set(std::move(key), std::move(value_sql));
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        std::string_view cond = if_not_exists_ ? std::string_view{"IF NOT EXISTS "} : std::string_view{};
        std::string s;
        s.reserve(create_prefix_.size() + cond.size() + table_name.size() + 5 + select_sql_.size() + 2);
        s += create_prefix_;
        s += cond;
        s += table_name;
        s += options_.to_sql();
        s += " AS ";
        s += select_sql_;
        s += ";\n";
        return s;
    }

private:
    std::string create_prefix_;
    std::string select_sql_;
    bool if_not_exists_;
    create_table_option options_;
};

// Out-of-line .as() definition for create_table_cond_builder (needs create_table_as_builder to be complete)
template <typename T>
template <SqlBuilder SelectQuery>
[[nodiscard]] create_table_as_builder<T> create_table_cond_builder<T>::as(SelectQuery const& query) const {
    return create_table_as_builder<T>{create_prefix_, query.build_sql(), true, options_};
}

}  // namespace ddl_detail

template <ValidTable T>
[[nodiscard]] ddl_detail::create_table_builder<T> create_table() {
    return ddl_detail::create_table_builder<T>{};
}

template <ValidTable T>
[[nodiscard]] ddl_detail::create_table_builder<T> create_table(T const&) {
    return ddl_detail::create_table_builder<T>{};
}

template <ValidTable T>
[[nodiscard]] ddl_detail::create_table_builder<T> create_temporary_table() {
    return ddl_detail::create_table_builder<T>{true};
}

template <ValidTable T>
[[nodiscard]] ddl_detail::create_table_builder<T> create_temporary_table(T const&) {
    return ddl_detail::create_table_builder<T>{true};
}

template <ValidTable T>
[[nodiscard]] ddl_detail::drop_table_builder<T> drop_table() {
    return ddl_detail::drop_table_builder<T>{};
}

template <ValidTable T>
[[nodiscard]] ddl_detail::drop_table_builder<T> drop_table(T const&) {
    return ddl_detail::drop_table_builder<T>{};
}

template <ValidTable T>
[[nodiscard]] ddl_detail::drop_table_builder<T> drop_temporary_table() {
    return ddl_detail::drop_table_builder<T>{true};
}

template <ValidTable T>
[[nodiscard]] ddl_detail::drop_table_builder<T> drop_temporary_table(T const&) {
    return ddl_detail::drop_table_builder<T>{true};
}

template <ValidTable T>
[[nodiscard]] ddl_detail::create_view_builder<T> create_view() {
    return ddl_detail::create_view_builder<T>{};
}

template <ValidTable T>
[[nodiscard]] ddl_detail::create_view_builder<T> create_view(T const&) {
    return ddl_detail::create_view_builder<T>{};
}

template <ValidTable T>
[[nodiscard]] ddl_detail::drop_view_builder<T> drop_view() {
    return ddl_detail::drop_view_builder<T>{};
}

template <ValidTable T>
[[nodiscard]] ddl_detail::drop_view_builder<T> drop_view(T const&) {
    return ddl_detail::drop_view_builder<T>{};
}

template <ValidTable From, ValidTable To>
[[nodiscard]] ddl_detail::rename_table_builder<From, To> rename_table() {
    return ddl_detail::rename_table_builder<From, To>{};
}

template <ValidTable From, ValidTable To>
[[nodiscard]] ddl_detail::rename_table_builder<From, To> rename_table(From const&, To const&) {
    return ddl_detail::rename_table_builder<From, To>{};
}

template <NamedIdType IndexId, ValidTable Table, ColumnDescriptor... Cols>
[[nodiscard]] ddl_detail::create_index_builder<Table, Cols...> create_index_on() {
    return ddl_detail::create_index_builder<Table, Cols...>{std::string(IndexId::name())};
}

template <NamedIdType IndexId, ColumnDescriptor... Cols, ValidTable Table>
[[nodiscard]] ddl_detail::create_index_builder<Table, Cols...> create_index_on(Table const&) {
    return ddl_detail::create_index_builder<Table, Cols...>{std::string(IndexId::name())};
}

template <NamedIdType IndexId, ValidTable Table>
[[nodiscard]] ddl_detail::drop_index_builder<Table> drop_index_on() {
    return ddl_detail::drop_index_builder<Table>{std::string(IndexId::name())};
}

template <NamedIdType IndexId, ValidTable Table>
[[nodiscard]] ddl_detail::drop_index_builder<Table> drop_index_on(Table const&) {
    return ddl_detail::drop_index_builder<Table>{std::string(IndexId::name())};
}

template <ValidTable Table>
[[nodiscard]] ddl_detail::alter_table_builder<Table> alter_table() {
    return ddl_detail::alter_table_builder<Table>{};
}

template <ValidTable Table>
[[nodiscard]] ddl_detail::alter_table_builder<Table> alter_table(Table const&) {
    return ddl_detail::alter_table_builder<Table>{};
}

/**
 * create_database<DB>() — CREATE DATABASE <db> [IF NOT EXISTS].
 *
 * DB must inherit from database_schema.
 *
 * Example:
 *   struct my_db : ds_mysql::database_schema {
 *       struct symbol { ... };
 *   };
 *
 *   db.execute(create_database<my_db>());
 *   db.execute(create_database<my_db>().if_not_exists());
 *   db.execute(create_database<my_db>().then().create_table<my_db::symbol>());
 */
template <Database T>
[[nodiscard]] ddl_detail::create_database_builder<T> create_database() {
    using _ = typename database_tables<T>::type;
    (void)sizeof(_);
    return ddl_detail::create_database_builder<T>{};
}

[[nodiscard]] inline ddl_detail::create_database_named_builder<> create_database(database_name name) {
    return ddl_detail::create_database_named_builder<>{std::move(name)};
}

/**
 * drop_database<DB>() — DROP DATABASE <db> [IF EXISTS].
 *
 * DB must inherit from database_schema.
 */
template <Database T>
[[nodiscard]] ddl_detail::drop_database_builder<T> drop_database() {
    using _ = typename database_tables<T>::type;
    (void)sizeof(_);
    return ddl_detail::drop_database_builder<T>{};
}

[[nodiscard]] inline ddl_detail::drop_database_named_builder<> drop_database(database_name name) {
    return ddl_detail::drop_database_named_builder<>{std::move(name)};
}

/**
 * create_all_tables<DB>() — CREATE TABLE for every table in DB.
 *
 * DB must inherit from database_schema and define a `tables` tuple.
 *
 * Example:
 *   create_all_tables<my_db>().build_sql()
 *   create_all_tables<my_db>().if_not_exists().build_sql()
 *   create_all_tables<my_db>().then().create_table<my_db::t>()...
 */
template <Database DB>
[[nodiscard]] ddl_detail::create_all_tables_builder<DB> create_all_tables() {
    using _ = typename database_tables<DB>::type;
    (void)sizeof(_);
    return ddl_detail::create_all_tables_builder<DB>{};
}

namespace ddl_detail {

// ---------------------------------------------------------------
// use_builder — USE <database>
// ---------------------------------------------------------------
template <typename T, SqlBuilder Prior = no_prior>
class use_builder {
public:
    using ddl_tag_type = void;

    explicit use_builder(Prior prior = {}) : prior_(std::move(prior)) {
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<use_builder<T, Prior>>{*this};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto db_name = database_name_for<T>::value();
        std::string s = prior_.build_sql();
        s += "USE ";
        s += db_name;
        s += ";\n";
        return s;
    }

private:
    Prior prior_;
};

// Out-of-line definition for ddl_continuation<Prior>::use<T>()
// (declared in ddl_continuation; defined here after use_builder<T, Prior> is complete)
template <SqlBuilder Prior>
template <Database T>
[[nodiscard]] auto ddl_continuation<Prior>::use() const {
    return use_builder<T, ddl_continuation<Prior>>{*this};
}

}  // namespace ddl_detail

/**
 * use<DB>() — USE <database>.
 *
 * Sets the default database for the session.
 * DB must inherit from database_schema.
 *
 * Example:
 *   db.execute(use<my_db>());
 *   db.execute(create_database<my_db>().if_not_exists()
 *                  .then().use<my_db>());
 */
template <Database T>
[[nodiscard]] constexpr ddl_detail::use_builder<T> use() {
    return ddl_detail::use_builder<T>{};
}

namespace ddl_detail {

// ---------------------------------------------------------------
// use_name_builder — USE <database> (runtime name)
// ---------------------------------------------------------------
template <SqlBuilder Prior = no_prior>
class use_name_builder {
public:
    using ddl_tag_type = void;

    use_name_builder(database_name db_name, Prior prior = {}) : db_name_(std::move(db_name)), prior_(std::move(prior)) {
    }

    [[nodiscard]] auto then() const {
        return ddl_continuation<use_name_builder<Prior>>{*this};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto db_name = db_name_.to_string();
        std::string s = prior_.build_sql();
        s += "USE ";
        s += db_name;
        s += ";\n";
        return s;
    }

private:
    database_name db_name_;
    Prior prior_;
};

// Out-of-line definition for ddl_continuation<Prior>::use(database_name)
// (declared in ddl_continuation; defined here after use_name_builder is complete)
template <SqlBuilder Prior>
[[nodiscard]] auto ddl_continuation<Prior>::use(database_name name) const {
    return use_name_builder<ddl_continuation<Prior>>{std::move(name), *this};
}

}  // namespace ddl_detail

/**
 * use(database) — USE <database>.
 *
 * Sets the default database for the session.
 * DB must inherit from database_schema.
 *
 * Example:
 *   db.execute(use("my_db"));
 *   db.execute(create_database<my_db>().if_not_exists()
 *                  .then().use("my_db"));
 */
[[nodiscard]] inline ddl_detail::use_name_builder<> use(database_name name) {
    return ddl_detail::use_name_builder<>{std::move(name)};
}

// ===================================================================
// SHOW Statements
//
// Entry points (all queryable via db.query()):
//   show_databases()                 — SHOW DATABASES
//   show_tables()                    — SHOW TABLES (current database)
//   show_columns<T>()                — SHOW COLUMNS FROM table
//   show_create_table<T>()           — SHOW CREATE TABLE table
// ===================================================================

namespace ddl_detail {

class show_databases_builder {
public:
    using result_row_type = std::tuple<std::string>;

    [[nodiscard]] std::string build_sql() const {
        return "SHOW DATABASES";
    }
};

class show_tables_builder {
public:
    using result_row_type = std::tuple<std::string>;

    [[nodiscard]] std::string build_sql() const {
        return "SHOW TABLES";
    }
};

template <typename T>
class show_columns_builder {
public:
    // Field, Type, Null, Key, Default (nullable), Extra
    using result_row_type =
        std::tuple<std::string, std::string, std::string, std::string, std::optional<std::string>, std::string>;

    [[nodiscard]] std::string build_sql() const {
        return "SHOW COLUMNS FROM " + std::string(table_name_for<T>::value().to_string_view());
    }
};

template <typename T>
class show_create_table_builder {
public:
    // Table name, CREATE TABLE statement
    using result_row_type = std::tuple<std::string, std::string>;

    [[nodiscard]] std::string build_sql() const {
        return "SHOW CREATE TABLE " + std::string(table_name_for<T>::value().to_string_view());
    }
};

}  // namespace ddl_detail

[[nodiscard]] inline ddl_detail::show_databases_builder show_databases() {
    return {};
}

[[nodiscard]] inline ddl_detail::show_tables_builder show_tables() {
    return {};
}

template <ValidTable T>
[[nodiscard]] ddl_detail::show_columns_builder<T> show_columns() {
    return {};
}

template <ValidTable T>
[[nodiscard]] ddl_detail::show_columns_builder<T> show_columns(T const&) {
    return {};
}

template <ValidTable T>
[[nodiscard]] ddl_detail::show_create_table_builder<T> show_create_table() {
    return {};
}

template <ValidTable T>
[[nodiscard]] ddl_detail::show_create_table_builder<T> show_create_table(T const&) {
    return {};
}

// ===================================================================
// Stored Procedures
//
// Entry points (executeable via db.execute()):
//   create_procedure(name, params, body)   — CREATE PROCEDURE name(params) BEGIN body END
//   drop_procedure(name)                   — DROP PROCEDURE name
//   drop_procedure(name).if_exists()       — DROP PROCEDURE IF EXISTS name
//   call_procedure(name)                   — CALL name()
//   call_procedure(name, args)             — CALL name(args)
// ===================================================================

namespace ddl_detail {

class drop_procedure_if_exists_builder {
public:
    explicit drop_procedure_if_exists_builder(std::string name) : name_(std::move(name)) {
    }

    [[nodiscard]] std::string build_sql() const {
        return "DROP PROCEDURE IF EXISTS " + name_;
    }

private:
    std::string name_;
};

class drop_procedure_builder {
public:
    explicit drop_procedure_builder(std::string name) : name_(std::move(name)) {
    }

    [[nodiscard]] drop_procedure_if_exists_builder if_exists() const {
        return drop_procedure_if_exists_builder{name_};
    }

    [[nodiscard]] std::string build_sql() const {
        return "DROP PROCEDURE " + name_;
    }

private:
    std::string name_;
};

class create_procedure_builder {
public:
    create_procedure_builder(std::string name, std::string params, std::string body)
        : name_(std::move(name)), params_(std::move(params)), body_(std::move(body)) {
    }

    [[nodiscard]] std::string build_sql() const {
        return "CREATE PROCEDURE " + name_ + "(" + params_ + ")\nBEGIN\n" + body_ + "\nEND";
    }

private:
    std::string name_;
    std::string params_;
    std::string body_;
};

class call_builder {
public:
    explicit call_builder(std::string name, std::string args = {}) : name_(std::move(name)), args_(std::move(args)) {
    }

    [[nodiscard]] std::string build_sql() const {
        return "CALL " + name_ + "(" + args_ + ")";
    }

private:
    std::string name_;
    std::string args_;
};

}  // namespace ddl_detail

[[nodiscard]] inline ddl_detail::create_procedure_builder create_procedure(std::string name, std::string params,
                                                                           std::string body) {
    return {std::move(name), std::move(params), std::move(body)};
}

[[nodiscard]] inline ddl_detail::drop_procedure_builder drop_procedure(std::string name) {
    return ddl_detail::drop_procedure_builder{std::move(name)};
}

[[nodiscard]] inline ddl_detail::call_builder call_procedure(std::string name, std::string args = {}) {
    return ddl_detail::call_builder{std::move(name), std::move(args)};
}

// ===================================================================
// Triggers
//
// Entry points (executeable via db.execute()):
//   create_trigger<T>(name, timing, event, body)  — CREATE TRIGGER name timing event ON table FOR EACH ROW body
//   drop_trigger<T>(name)                          — DROP TRIGGER name
//   drop_trigger<T>(name).if_exists()              — DROP TRIGGER IF EXISTS name
// ===================================================================

enum class TriggerTiming {
    Before,
    After,
};

enum class TriggerEvent {
    Insert,
    Update,
    Delete,
};

namespace ddl_detail {

[[nodiscard]] inline std::string to_sql_trigger_timing(TriggerTiming timing) {
    return timing == TriggerTiming::Before ? "BEFORE" : "AFTER";
}

[[nodiscard]] inline std::string to_sql_trigger_event(TriggerEvent event) {
    switch (event) {
        case TriggerEvent::Insert:
            return "INSERT";
        case TriggerEvent::Update:
            return "UPDATE";
        case TriggerEvent::Delete:
            return "DELETE";
    }
    return "INSERT";
}

template <typename T>
class drop_trigger_if_exists_builder {
public:
    explicit drop_trigger_if_exists_builder(std::string trigger_name) : trigger_name_(std::move(trigger_name)) {
    }

    [[nodiscard]] std::string build_sql() const {
        return "DROP TRIGGER IF EXISTS " + trigger_name_;
    }

private:
    std::string trigger_name_;
};

template <typename T>
class drop_trigger_builder {
public:
    explicit drop_trigger_builder(std::string trigger_name) : trigger_name_(std::move(trigger_name)) {
    }

    [[nodiscard]] drop_trigger_if_exists_builder<T> if_exists() const {
        return drop_trigger_if_exists_builder<T>{trigger_name_};
    }

    [[nodiscard]] std::string build_sql() const {
        return "DROP TRIGGER " + trigger_name_;
    }

private:
    std::string trigger_name_;
};

template <typename T>
class create_trigger_builder {
public:
    create_trigger_builder(std::string trigger_name, TriggerTiming timing, TriggerEvent event, std::string body)
        : trigger_name_(std::move(trigger_name)), timing_(timing), event_(event), body_(std::move(body)) {
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        return "CREATE TRIGGER " + trigger_name_ + " " + to_sql_trigger_timing(timing_) + " " +
               to_sql_trigger_event(event_) + " ON " + std::string(table_name) + " FOR EACH ROW\n" + body_;
    }

private:
    std::string trigger_name_;
    TriggerTiming timing_;
    TriggerEvent event_;
    std::string body_;
};

}  // namespace ddl_detail

template <NamedIdType TrigId, ValidTable T>
[[nodiscard]] ddl_detail::create_trigger_builder<T> create_trigger(TriggerTiming timing, TriggerEvent event,
                                                                   std::string body) {
    return {std::string(TrigId::name()), timing, event, std::move(body)};
}

template <NamedIdType TrigId, ValidTable T>
[[nodiscard]] ddl_detail::drop_trigger_builder<T> drop_trigger() {
    return ddl_detail::drop_trigger_builder<T>{std::string(TrigId::name())};
}

}  // namespace ds_mysql
