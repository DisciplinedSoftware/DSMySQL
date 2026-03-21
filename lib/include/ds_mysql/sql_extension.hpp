#pragma once

#include <utility>

// MySQL-specific SQL extensions.
//
// Include this header (in addition to sql.hpp) to enable USE <database>,
// MySQL-specific SQL aliases, and chaining through ddl_continuation::use<DB>().
//
// Example:
//   #include "ds_mysql/sql_extension.hpp"
//
//   struct my_db : ds_mysql::database_schema {
//       struct symbol { ... };
//   };
//
//   // Standalone USE:
//   db.execute(use<my_db>());
//
//   // Chained (setup script):
//   db.execute(
//       create_database<my_db>().if_not_exists()
//           .then().use<my_db>()
//           .then().create_table<my_db::symbol>().if_not_exists());

#include "ds_mysql/sql.hpp"

namespace ds_mysql {

template <ColumnDescriptor Col>
using mysql_group_concat = group_concat<Col>;

template <ColumnDescriptor Col1, ColumnDescriptor Col2>
using mysql_ifnull_of = ifnull_of<Col1, Col2>;

template <Projection P, int Scale>
using mysql_format_to = format_to<P, Scale>;

template <Projection P, fixed_string Format>
using mysql_date_format_of = date_format_of<P, Format>;

template <Projection P, sql_cast_type Type>
using mysql_convert_as = convert_as<P, Type>;

template <ColumnDescriptor Col, typename Value>
[[nodiscard]] where_condition mysql_null_safe_equal(Value&& value) {
    return null_safe_equal<Col>(std::forward<Value>(value));
}

template <ColumnDescriptor Col>
[[nodiscard]] where_condition mysql_regexp(std::string_view pattern) {
    return regexp<Col>(pattern);
}

namespace ddl_detail {

// ---------------------------------------------------------------
// use_builder — USE <database> (MySQL extension)
// ---------------------------------------------------------------
template <typename T>
class use_builder {
public:
    using ddl_tag_type = void;

    explicit use_builder(std::string prior = {}) : prior_sql_(std::move(prior)) {
    }

    [[nodiscard]] ddl_continuation then() const {
        return ddl_continuation{build_sql()};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto db_name = database_name_for<T>::value();
        std::string s;
        s.reserve(prior_sql_.size() + 4 + db_name.size() + 2);
        s += prior_sql_;
        s += "USE ";
        s += db_name;
        s += ";\n";
        return s;
    }

private:
    std::string prior_sql_;
};

// Out-of-line definition for ddl_continuation::use<T>()
// (declared in sql.hpp; defined here after use_builder<T> is complete)
template <Database T>
[[nodiscard]] use_builder<T> ddl_continuation::use() const {
    return use_builder<T>{sql_};
}

}  // namespace ddl_detail

/**
 * use<DB>() — USE <database> (MySQL extension).
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
[[nodiscard]] ddl_detail::use_builder<T> use() {
    using _ = typename database_tables<T>::type;
    (void)sizeof(_);
    return ddl_detail::use_builder<T>{};
}

}  // namespace ds_mysql
