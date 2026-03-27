#include <boost/ut.hpp>
#include <string>

#include "ds_mysql/sql_dcl.hpp"

using namespace boost::ut;
using namespace ds_mysql;
using namespace std::string_literals;

namespace {

struct employees {
    using ds_mysql_table_tag = ds_mysql::table_schema;
};

struct catalog_db : ds_mysql::database_schema {};

}  // namespace

// ===================================================================
// DCL — GRANT
// ===================================================================

suite<"DCL grant"> dcl_grant_suite = [] {
    "grant - runtime privileges - generates GRANT ... ON ... TO ..."_test = [] {
        auto const sql = grant(privileges().select().insert(), on::schema(database_name("mydb")),
                               user(user_name("user")).at(host_name("localhost")))
                             .build_sql();
        expect(sql == "GRANT SELECT, INSERT ON mydb.* TO 'user'@'localhost'"s) << sql;
    };

    "grant - all privileges on global"_test = [] {
        auto const sql =
            grant(privileges().all(), on::global(), user(user_name("admin")).at(host_name("%"))).build_sql();
        expect(sql == "GRANT ALL PRIVILEGES ON *.* TO 'admin'@'%'"s) << sql;
    };

    "grant - with_grant_option - appends WITH GRANT OPTION"_test = [] {
        auto const sql = grant(privileges().all(), on::global(), user(user_name("admin")).at_any_host())
                             .with_grant_option()
                             .build_sql();
        expect(sql == "GRANT ALL PRIVILEGES ON *.* TO 'admin'@'%' WITH GRANT OPTION"s) << sql;
    };

    "grant - template privileges"_test = [] {
        auto const sql = grant<privilege::select, privilege::insert>(on::schema(database_name("mydb")),
                                                                     user(user_name("user")).at_localhost())
                             .build_sql();
        expect(sql == "GRANT SELECT, INSERT ON mydb.* TO 'user'@'localhost'"s) << sql;
    };

    "grant - template all privilege"_test = [] {
        auto const sql = grant<privilege::all>(on::global(), user(user_name("admin")).at_any_host()).build_sql();
        expect(sql == "GRANT ALL PRIVILEGES ON *.* TO 'admin'@'%'"s) << sql;
    };

    "grant - template with_grant_option"_test = [] {
        auto const sql =
            grant<privilege::all>(on::global(), user(user_name("admin")).at_any_host()).with_grant_option().build_sql();
        expect(sql == "GRANT ALL PRIVILEGES ON *.* TO 'admin'@'%' WITH GRANT OPTION"s) << sql;
    };
};

// ===================================================================
// DCL — REVOKE
// ===================================================================

suite<"DCL revoke"> dcl_revoke_suite = [] {
    "revoke - runtime privileges - generates REVOKE ... ON ... FROM ..."_test = [] {
        auto const sql = revoke(privileges().select(), on::schema(database_name("mydb")),
                                user(user_name("user")).at(host_name("localhost")))
                             .build_sql();
        expect(sql == "REVOKE SELECT ON mydb.* FROM 'user'@'localhost'"s) << sql;
    };

    "revoke - template privileges"_test = [] {
        auto const sql =
            revoke<privilege::select, privilege::insert>(on::global(), user(user_name("user")).at_any_host())
                .build_sql();
        expect(sql == "REVOKE SELECT, INSERT ON *.* FROM 'user'@'%'"s) << sql;
    };
};

// ===================================================================
// DCL — grant_target (on::)
// ===================================================================

suite<"DCL grant_target"> dcl_grant_target_suite = [] {
    "on::global - generates *.*"_test = [] {
        expect(on::global().to_sql() == "*.*"s);
    };

    "on::schema - generates db.*"_test = [] {
        expect(on::schema(database_name("mydb")).to_sql() == "mydb.*"s);
    };

    "on::table<T> - derives table name from type"_test = [] {
        expect(on::table<employees>(database_name("mydb")).to_sql() == "mydb.employees"s);
    };

    "on::table<DB, T> - derives both names from types"_test = [] {
        expect(on::table<catalog_db, employees>().to_sql() == "catalog_db.employees"s);
    };
};

// ===================================================================
// DCL — grant_user
// ===================================================================

suite<"DCL grant_user"> dcl_grant_user_suite = [] {
    "user.at - formats 'user'@'host'"_test = [] {
        expect(user(user_name("alice")).at(host_name("localhost")).to_sql() == "'alice'@'localhost'"s);
    };

    "user.at_any_host - formats 'user'@'%'"_test = [] {
        expect(user(user_name("alice")).at_any_host().to_sql() == "'alice'@'%'"s);
    };

    "user.at_localhost - formats 'user'@'localhost'"_test = [] {
        expect(user(user_name("alice")).at_localhost().to_sql() == "'alice'@'localhost'"s);
    };
};

// ===================================================================
// DCL — privilege_list
// ===================================================================

suite<"DCL privilege_list"> dcl_privilege_list_suite = [] {
    "privileges().select() - single privilege"_test = [] {
        expect(privileges().select().to_sql() == "SELECT"s);
    };

    "privileges().select().insert().update() - multiple privileges"_test = [] {
        expect(privileges().select().insert().update().to_sql() == "SELECT, INSERT, UPDATE"s);
    };

    "privileges().delete_() - reserved-word privilege"_test = [] {
        expect(privileges().delete_().to_sql() == "DELETE"s);
    };

    "privileges().all() - ALL PRIVILEGES"_test = [] {
        expect(privileges().all().to_sql() == "ALL PRIVILEGES"s);
    };
};
