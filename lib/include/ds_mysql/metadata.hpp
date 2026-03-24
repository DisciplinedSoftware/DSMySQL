#pragma once

#include <string>

#include "ds_mysql/column_field.hpp"
#include "ds_mysql/schema_generator.hpp"
#include "ds_mysql/sql_identifiers.hpp"

namespace ds_mysql::mysql_metadata {

// MySQL metadata catalogs exposed as typed database schemas.
struct information_schema : database_schema {
    struct schemata {
        COLUMN_FIELD(catalog_name, std::string)
        COLUMN_FIELD(schema_name, std::string)
        COLUMN_FIELD(default_character_set_name, std::string)
        COLUMN_FIELD(default_collation_name, std::string)
    };

    struct tables {
        COLUMN_FIELD(table_schema, std::string)
        COLUMN_FIELD(table_name, std::string)
        COLUMN_FIELD(table_type, std::string)
    };

    struct columns {
        COLUMN_FIELD(table_schema, std::string)
        COLUMN_FIELD(table_name, std::string)
        COLUMN_FIELD(column_name, std::string)
        COLUMN_FIELD(data_type, std::string)
    };

    struct views {
        COLUMN_FIELD(table_schema, std::string)
        COLUMN_FIELD(table_name, std::string)
    };

    struct table_constraints {
        COLUMN_FIELD(constraint_schema, std::string)
        COLUMN_FIELD(table_name, std::string)
        COLUMN_FIELD(constraint_name, std::string)
        COLUMN_FIELD(constraint_type, std::string)
    };

    struct key_column_usage {
        COLUMN_FIELD(constraint_schema, std::string)
        COLUMN_FIELD(table_name, std::string)
        COLUMN_FIELD(column_name, std::string)
    };

    struct referential_constraints {
        COLUMN_FIELD(constraint_schema, std::string)
        COLUMN_FIELD(constraint_name, std::string)
        COLUMN_FIELD(unique_constraint_schema, std::string)
        COLUMN_FIELD(unique_constraint_name, std::string)
    };

    struct statistics {
        COLUMN_FIELD(table_schema, std::string)
        COLUMN_FIELD(table_name, std::string)
        COLUMN_FIELD(index_name, std::string)
        COLUMN_FIELD(column_name, std::string)
    };
};

struct mysql_system : database_schema {};
struct performance_schema : database_schema {};
struct sys_schema : database_schema {};

}  // namespace ds_mysql::mysql_metadata

namespace ds_mysql {

template <>
struct database_name_for<mysql_metadata::information_schema> {
    static constexpr std::string_view value() noexcept {
        return "information_schema";
    }
};

template <>
struct database_name_for<mysql_metadata::mysql_system> {
    static constexpr std::string_view value() noexcept {
        return "mysql";
    }
};

template <>
struct database_name_for<mysql_metadata::performance_schema> {
    static constexpr std::string_view value() noexcept {
        return "performance_schema";
    }
};

template <>
struct database_name_for<mysql_metadata::sys_schema> {
    static constexpr std::string_view value() noexcept {
        return "sys";
    }
};

namespace detail {
inline const std::string& information_schema_prefix() noexcept {
    static const std::string s{std::string(database_name_for<mysql_metadata::information_schema>::value()) + "."};
    return s;
}
}  // namespace detail

template <>
struct table_name_for<mysql_metadata::information_schema::schemata> {
    static table_name value() noexcept {
        static const std::string name = detail::information_schema_prefix() + "schemata";
        return table_name{name};
    }
};

template <>
struct table_name_for<mysql_metadata::information_schema::tables> {
    static table_name value() noexcept {
        static const std::string name = detail::information_schema_prefix() + "tables";
        return table_name{name};
    }
};

template <>
struct table_name_for<mysql_metadata::information_schema::columns> {
    static table_name value() noexcept {
        static const std::string name = detail::information_schema_prefix() + "columns";
        return table_name{name};
    }
};

template <>
struct table_name_for<mysql_metadata::information_schema::views> {
    static table_name value() noexcept {
        static const std::string name = detail::information_schema_prefix() + "views";
        return table_name{name};
    }
};

template <>
struct table_name_for<mysql_metadata::information_schema::table_constraints> {
    static table_name value() noexcept {
        static const std::string name = detail::information_schema_prefix() + "table_constraints";
        return table_name{name};
    }
};

template <>
struct table_name_for<mysql_metadata::information_schema::key_column_usage> {
    static table_name value() noexcept {
        static const std::string name = detail::information_schema_prefix() + "key_column_usage";
        return table_name{name};
    }
};

template <>
struct table_name_for<mysql_metadata::information_schema::referential_constraints> {
    static table_name value() noexcept {
        static const std::string name = detail::information_schema_prefix() + "referential_constraints";
        return table_name{name};
    }
};

template <>
struct table_name_for<mysql_metadata::information_schema::statistics> {
    static table_name value() noexcept {
        static const std::string name = detail::information_schema_prefix() + "statistics";
        return table_name{name};
    }
};

}  // namespace ds_mysql
