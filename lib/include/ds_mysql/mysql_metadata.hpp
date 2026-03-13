#pragma once

#include <string>

#include "ds_mysql/column_field.hpp"
#include "ds_mysql/schema_generator.hpp"
#include "ds_mysql/sql_identifiers.hpp"

namespace ds_mysql::mysql_metadata {

// MySQL metadata catalogs exposed as typed database schemas.
struct information_schema : database_schema {
    struct schemata {
        using catalog_name = column_field<struct catalog_name_tag, std::string>;
        using schema_name = column_field<struct schema_name_tag, std::string>;
        using default_character_set_name = column_field<struct default_character_set_name_tag, std::string>;
        using default_collation_name = column_field<struct default_collation_name_tag, std::string>;

        catalog_name catalog_name_;
        schema_name schema_name_;
        default_character_set_name default_character_set_name_;
        default_collation_name default_collation_name_;
    };

    struct tables {
        using table_schema = column_field<struct table_schema_tag, std::string>;
        using table_name = column_field<struct table_name_tag, std::string>;
        using table_type = column_field<struct table_type_tag, std::string>;

        table_schema table_schema_;
        table_name table_name_;
        table_type table_type_;
    };

    struct columns {
        using table_schema = column_field<struct table_schema_tag, std::string>;
        using table_name = column_field<struct table_name_tag, std::string>;
        using column_name = column_field<struct column_name_tag, std::string>;
        using data_type = column_field<struct data_type_tag, std::string>;

        table_schema table_schema_;
        table_name table_name_;
        column_name column_name_;
        data_type data_type_;
    };

    struct views {
        using table_schema = column_field<struct table_schema_tag, std::string>;
        using table_name = column_field<struct table_name_tag, std::string>;

        table_schema table_schema_;
        table_name table_name_;
    };

    struct table_constraints {
        using constraint_schema = column_field<struct constraint_schema_tag, std::string>;
        using table_name = column_field<struct table_name_tag, std::string>;
        using constraint_name = column_field<struct constraint_name_tag, std::string>;
        using constraint_type = column_field<struct constraint_type_tag, std::string>;

        constraint_schema constraint_schema_;
        table_name table_name_;
        constraint_name constraint_name_;
        constraint_type constraint_type_;
    };

    struct key_column_usage {
        using constraint_schema = column_field<struct constraint_schema_tag, std::string>;
        using table_name = column_field<struct table_name_tag, std::string>;
        using column_name = column_field<struct column_name_tag, std::string>;

        constraint_schema constraint_schema_;
        table_name table_name_;
        column_name column_name_;
    };

    struct referential_constraints {
        using constraint_schema = column_field<struct constraint_schema_tag, std::string>;
        using constraint_name = column_field<struct constraint_name_tag, std::string>;
        using unique_constraint_schema = column_field<struct unique_constraint_schema_tag, std::string>;
        using unique_constraint_name = column_field<struct unique_constraint_name_tag, std::string>;

        constraint_schema constraint_schema_;
        constraint_name constraint_name_;
        unique_constraint_schema unique_constraint_schema_;
        unique_constraint_name unique_constraint_name_;
    };

    struct statistics {
        using table_schema = column_field<struct table_schema_tag, std::string>;
        using table_name = column_field<struct table_name_tag, std::string>;
        using index_name = column_field<struct index_name_tag, std::string>;
        using column_name = column_field<struct column_name_tag, std::string>;

        table_schema table_schema_;
        table_name table_name_;
        index_name index_name_;
        column_name column_name_;
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
