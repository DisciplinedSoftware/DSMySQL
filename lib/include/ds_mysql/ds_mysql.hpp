#pragma once

// Convenience header — include everything needed to work with DSMySQL.
//
// Transitively provides:
//   database.hpp        — database connection, query execution, result sets
//   sql.hpp             — query builder (SELECT / INSERT / UPDATE / DELETE / ...) and helpers
//   metadata.hpp        — runtime metadata helpers (column info, schema introspection)
//
// All other library headers (column_field, schema_generator, sql_identifiers,
// sql_temporal, sql_varchar, sql_text, config, credentials, …) are
// pulled in transitively by the three headers above.

#include "ds_mysql/database.hpp"
#include "ds_mysql/metadata.hpp"
#include "ds_mysql/sql_ddl.hpp"
#include "ds_mysql/sql_dml.hpp"
#include "ds_mysql/sql_dql.hpp"
#include "ds_mysql/version.hpp"
