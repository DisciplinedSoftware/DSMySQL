#pragma once

#include "ds_mysql/database_name.hpp"
#include "ds_mysql/host_name.hpp"
#include "ds_mysql/schema_generator.hpp"
#include "ds_mysql/user_name.hpp"

#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace ds_mysql {

// ===================================================================
// privilege — enumeration of MySQL privilege types
// ===================================================================

enum class privilege {
    select,
    insert,
    update,
    delete_,                   // DELETE (reserved keyword in C++)
    create,
    drop,
    references,
    index,
    alter,
    create_view,
    show_view,
    create_routine,
    alter_routine,
    execute,
    trigger,
    event,
    create_temporary_tables,
    lock_tables,
    reload,
    shutdown,
    process,
    file,
    show_databases,
    super,
    replication_slave,
    replication_client,
    create_user,
    create_tablespace,
    all,                       // ALL PRIVILEGES
};

namespace dcl_detail {

[[nodiscard]] inline std::string_view privilege_to_sql(privilege p) noexcept {
    switch (p) {
        case privilege::select:                  return "SELECT";
        case privilege::insert:                  return "INSERT";
        case privilege::update:                  return "UPDATE";
        case privilege::delete_:                 return "DELETE";
        case privilege::create:                  return "CREATE";
        case privilege::drop:                    return "DROP";
        case privilege::references:              return "REFERENCES";
        case privilege::index:                   return "INDEX";
        case privilege::alter:                   return "ALTER";
        case privilege::create_view:             return "CREATE VIEW";
        case privilege::show_view:               return "SHOW VIEW";
        case privilege::create_routine:          return "CREATE ROUTINE";
        case privilege::alter_routine:           return "ALTER ROUTINE";
        case privilege::execute:                 return "EXECUTE";
        case privilege::trigger:                 return "TRIGGER";
        case privilege::event:                   return "EVENT";
        case privilege::create_temporary_tables: return "CREATE TEMPORARY TABLES";
        case privilege::lock_tables:             return "LOCK TABLES";
        case privilege::reload:                  return "RELOAD";
        case privilege::shutdown:                return "SHUTDOWN";
        case privilege::process:                 return "PROCESS";
        case privilege::file:                    return "FILE";
        case privilege::show_databases:          return "SHOW DATABASES";
        case privilege::super:                   return "SUPER";
        case privilege::replication_slave:       return "REPLICATION SLAVE";
        case privilege::replication_client:      return "REPLICATION CLIENT";
        case privilege::create_user:             return "CREATE USER";
        case privilege::create_tablespace:       return "CREATE TABLESPACE";
        case privilege::all:                     return "ALL PRIVILEGES";
    }
    return "";
}

template <privilege... Privs>
[[nodiscard]] std::string static_privileges_sql() {
    std::string result;
    bool first = true;
    for (privilege p : {Privs...}) {
        if (!first) result += ", ";
        result += privilege_to_sql(p);
        first = false;
    }
    return result;
}

}  // namespace dcl_detail

// ===================================================================
// privilege_list — composable runtime privilege set
//
//   privileges().select().insert()
//   privileges().all()
// ===================================================================

class privilege_list {
    std::vector<privilege> privs_;

    [[nodiscard]] privilege_list with(privilege p) && {
        privs_.push_back(p);
        return std::move(*this);
    }

public:
    privilege_list() = default;

    // clang-format off
    [[nodiscard]] privilege_list select() &&                  { return std::move(*this).with(privilege::select); }
    [[nodiscard]] privilege_list insert() &&                  { return std::move(*this).with(privilege::insert); }
    [[nodiscard]] privilege_list update() &&                  { return std::move(*this).with(privilege::update); }
    [[nodiscard]] privilege_list delete_() &&                 { return std::move(*this).with(privilege::delete_); }
    [[nodiscard]] privilege_list create() &&                  { return std::move(*this).with(privilege::create); }
    [[nodiscard]] privilege_list drop() &&                    { return std::move(*this).with(privilege::drop); }
    [[nodiscard]] privilege_list references() &&              { return std::move(*this).with(privilege::references); }
    [[nodiscard]] privilege_list index() &&                   { return std::move(*this).with(privilege::index); }
    [[nodiscard]] privilege_list alter() &&                   { return std::move(*this).with(privilege::alter); }
    [[nodiscard]] privilege_list create_view() &&             { return std::move(*this).with(privilege::create_view); }
    [[nodiscard]] privilege_list show_view() &&               { return std::move(*this).with(privilege::show_view); }
    [[nodiscard]] privilege_list create_routine() &&          { return std::move(*this).with(privilege::create_routine); }
    [[nodiscard]] privilege_list alter_routine() &&           { return std::move(*this).with(privilege::alter_routine); }
    [[nodiscard]] privilege_list execute() &&                 { return std::move(*this).with(privilege::execute); }
    [[nodiscard]] privilege_list trigger() &&                 { return std::move(*this).with(privilege::trigger); }
    [[nodiscard]] privilege_list event() &&                   { return std::move(*this).with(privilege::event); }
    [[nodiscard]] privilege_list create_temporary_tables() && { return std::move(*this).with(privilege::create_temporary_tables); }
    [[nodiscard]] privilege_list lock_tables() &&             { return std::move(*this).with(privilege::lock_tables); }
    [[nodiscard]] privilege_list reload() &&                  { return std::move(*this).with(privilege::reload); }
    [[nodiscard]] privilege_list shutdown() &&                { return std::move(*this).with(privilege::shutdown); }
    [[nodiscard]] privilege_list process() &&                 { return std::move(*this).with(privilege::process); }
    [[nodiscard]] privilege_list file() &&                    { return std::move(*this).with(privilege::file); }
    [[nodiscard]] privilege_list show_databases() &&          { return std::move(*this).with(privilege::show_databases); }
    [[nodiscard]] privilege_list super() &&                   { return std::move(*this).with(privilege::super); }
    [[nodiscard]] privilege_list replication_slave() &&       { return std::move(*this).with(privilege::replication_slave); }
    [[nodiscard]] privilege_list replication_client() &&      { return std::move(*this).with(privilege::replication_client); }
    [[nodiscard]] privilege_list create_user() &&             { return std::move(*this).with(privilege::create_user); }
    [[nodiscard]] privilege_list create_tablespace() &&       { return std::move(*this).with(privilege::create_tablespace); }
    [[nodiscard]] privilege_list all() &&                     { return std::move(*this).with(privilege::all); }
    // clang-format on

    [[nodiscard]] std::string to_sql() const {
        std::string result;
        for (std::size_t i = 0; i < privs_.size(); ++i) {
            if (i > 0) result += ", ";
            result += dcl_detail::privilege_to_sql(privs_[i]);
        }
        return result;
    }
};

[[nodiscard]] inline privilege_list privileges() {
    return {};
}

// ===================================================================
// grant_target — the SQL object a privilege applies to
//
// Factory functions in namespace `on`:
//   on::global()                              → *.*
//   on::schema(database_name("mydb"))         → mydb.*
//   on::table<employees>(database_name("mydb")) → mydb.employees  (table name from type)
//   on::table<catalog_db, employees>()        → catalog_db.employees  (both names from types)
// ===================================================================

class grant_target {
    std::string sql_;

public:
    explicit grant_target(std::string sql) : sql_(std::move(sql)) {
    }

    [[nodiscard]] std::string const& to_sql() const noexcept {
        return sql_;
    }
};

namespace on {

[[nodiscard]] inline grant_target global() {
    return grant_target{"*.*"};
}

[[nodiscard]] inline grant_target schema(database_name const& db) {
    return grant_target{db.to_string() + ".*"};
}

template <ValidTable T>
[[nodiscard]] grant_target table(database_name const& db) {
    return grant_target{db.to_string() + "." + std::string(table_name_for<T>::value().to_string_view())};
}

template <Database DB, ValidTable T>
[[nodiscard]] grant_target table() {
    return grant_target{std::string(database_name_for<DB>::value()) + "." +
                        std::string(table_name_for<T>::value().to_string_view())};
}

}  // namespace on

// ===================================================================
// grant_user — strongly-typed MySQL account ('user'@'host')
//
//   user(user_name("alice")).at(host_name("localhost"))
//   user(user_name("alice")).at_any_host()
//   user(user_name("alice")).at_localhost()
// ===================================================================

class grant_user {
    user_name user_;
    host_name host_;

public:
    grant_user(user_name user, host_name host) : user_(std::move(user)), host_(std::move(host)) {
    }

    [[nodiscard]] std::string to_sql() const {
        return "'" + user_.to_string() + "'@'" + host_.to_string() + "'";
    }
};

class grant_user_builder {
    user_name user_;

public:
    explicit grant_user_builder(user_name user) : user_(std::move(user)) {
    }

    [[nodiscard]] grant_user at(host_name host) const {
        return {user_, std::move(host)};
    }

    [[nodiscard]] grant_user at_any_host() const {
        return {user_, host_name{"%"}};
    }

    [[nodiscard]] grant_user at_localhost() const {
        return {user_, host_name{"localhost"}};
    }
};

[[nodiscard]] inline grant_user_builder user(user_name u) {
    return grant_user_builder{std::move(u)};
}

// ===================================================================
// Builders
// ===================================================================

namespace dcl_detail {

class grant_with_option_builder {
    std::string privs_sql_;
    std::string target_sql_;
    std::string grantee_sql_;

public:
    grant_with_option_builder(std::string privs_sql, std::string target_sql, std::string grantee_sql)
        : privs_sql_(std::move(privs_sql)), target_sql_(std::move(target_sql)), grantee_sql_(std::move(grantee_sql)) {
    }

    [[nodiscard]] std::string build_sql() const {
        return "GRANT " + privs_sql_ + " ON " + target_sql_ + " TO " + grantee_sql_ + " WITH GRANT OPTION";
    }
};

class grant_builder {
    std::string privs_sql_;
    std::string target_sql_;
    std::string grantee_sql_;

public:
    grant_builder(std::string privs_sql, std::string target_sql, std::string grantee_sql)
        : privs_sql_(std::move(privs_sql)), target_sql_(std::move(target_sql)), grantee_sql_(std::move(grantee_sql)) {
    }

    [[nodiscard]] grant_with_option_builder with_grant_option() const {
        return {privs_sql_, target_sql_, grantee_sql_};
    }

    [[nodiscard]] std::string build_sql() const {
        return "GRANT " + privs_sql_ + " ON " + target_sql_ + " TO " + grantee_sql_;
    }
};

class revoke_builder {
    std::string privs_sql_;
    std::string target_sql_;
    std::string grantee_sql_;

public:
    revoke_builder(std::string privs_sql, std::string target_sql, std::string grantee_sql)
        : privs_sql_(std::move(privs_sql)), target_sql_(std::move(target_sql)), grantee_sql_(std::move(grantee_sql)) {
    }

    [[nodiscard]] std::string build_sql() const {
        return "REVOKE " + privs_sql_ + " ON " + target_sql_ + " FROM " + grantee_sql_;
    }
};

}  // namespace dcl_detail

// ===================================================================
// Entry points
//
// Runtime privilege list:
//   grant(privileges().select().insert(), on::schema(database_name("mydb")), user(user_name("u")).at(host_name("h")))
//   revoke(privileges().select(), on::global(), user(user_name("u")).at_any_host())
//
// Compile-time privilege set (template parameters):
//   grant<privilege::select, privilege::insert>(on::schema(database_name("mydb")), user(user_name("u")).at(host_name("h")))
//   revoke<privilege::select>(on::global(), user(user_name("u")).at_any_host())
// ===================================================================

[[nodiscard]] inline dcl_detail::grant_builder grant(privilege_list privs, grant_target const& target,
                                                      grant_user const& grantee) {
    return {privs.to_sql(), target.to_sql(), grantee.to_sql()};
}

[[nodiscard]] inline dcl_detail::revoke_builder revoke(privilege_list privs, grant_target const& target,
                                                        grant_user const& grantee) {
    return {privs.to_sql(), target.to_sql(), grantee.to_sql()};
}

template <privilege... Privs>
[[nodiscard]] dcl_detail::grant_builder grant(grant_target const& target, grant_user const& grantee) {
    return {dcl_detail::static_privileges_sql<Privs...>(), target.to_sql(), grantee.to_sql()};
}

template <privilege... Privs>
[[nodiscard]] dcl_detail::revoke_builder revoke(grant_target const& target, grant_user const& grantee) {
    return {dcl_detail::static_privileges_sql<Privs...>(), target.to_sql(), grantee.to_sql()};
}

}  // namespace ds_mysql
