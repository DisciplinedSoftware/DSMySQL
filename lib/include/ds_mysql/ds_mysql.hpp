#pragma once

// Convenience header — include everything needed to work with DSMySQL.
//
// Transitively provides:
//   mysql_connection.hpp — database connection, query execution, result sets
//   sql_ddl.hpp         — DDL query builder (CREATE / DROP / ALTER / ...)
//   sql_dml.hpp         — DML query builder (INSERT / UPDATE / DELETE / DESCRIBE / ...)
//   sql_dql.hpp         — DQL query builder (SELECT / projections / expressions / ...)
//   metadata.hpp        — runtime metadata helpers (column info, schema introspection)
//
// All other library headers (column_field, schema_generator, sql_identifiers,
// sql_temporal, sql_varchar, sql_text, config, credentials, …) are
// pulled in transitively by the headers above.

#include "ds_mysql/connection_pool.hpp"
#include "ds_mysql/metadata.hpp"
#include "ds_mysql/mysql_connection.hpp"
#include "ds_mysql/prepared_statement.hpp"
#include "ds_mysql/sql_dcl.hpp"
#include "ds_mysql/sql_ddl.hpp"
#include "ds_mysql/sql_dml.hpp"
#include "ds_mysql/sql_dql.hpp"
#include "ds_mysql/version.hpp"
