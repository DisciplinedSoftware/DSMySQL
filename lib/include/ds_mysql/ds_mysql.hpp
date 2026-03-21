#pragma once

// Convenience header — include everything needed to work with DSMySQL.
//
// Transitively provides:
//   mysql_database.hpp  — database connection, query execution, result sets
//   sql.hpp             — query builder (SELECT / INSERT / UPDATE / DELETE / ...)
//   sql_extension.hpp   — additional query-builder helpers (ROUND, FORMAT, ...)
//   mysql_metadata.hpp  — runtime metadata helpers (column info, schema introspection)
//
// All other library headers (column_field, schema_generator, sql_identifiers,
// sql_temporal, varchar_field, text_field, mysql_config, credentials, …) are
// pulled in transitively by the four headers above.

#include "ds_mysql/mysql_database.hpp"
#include "ds_mysql/mysql_metadata.hpp"
#include "ds_mysql/sql.hpp"
#include "ds_mysql/sql_extension.hpp"
#include "ds_mysql/version.hpp"
