#include <boost/ut.hpp>
#include <tuple>
#include <type_traits>

#include "ds_mysql/mysql_metadata.hpp"
#include "ds_mysql/sql.hpp"

using namespace boost::ut;
using namespace ds_mysql;
using namespace std::string_literals;

// ===================================================================
// Test fixtures
// ===================================================================

namespace {

struct product {
    using id              = column_field<"id",              uint32_t>;
    using category_id     = column_field<"category_id",     std::optional<uint32_t>>;
    using sku             = column_field<"sku",             varchar_field<32>>;
    using type            = column_field<"type",            varchar_field<64>>;
    using name            = column_field<"name",            std::optional<varchar_field<255>>>;
    using tag             = column_field<"tag",             std::optional<varchar_field<255>>>;
    using unit            = column_field<"unit",            std::optional<varchar_field<32>>>;
    using created_at      = column_field<"created_at",      sql_datetime>;
    using last_updated_at = column_field<"last_updated_at", sql_datetime>;

    id              id_;
    category_id     category_id_;
    sku             sku_;
    type            type_;
    name            name_;
    tag             tag_;
    unit            unit_;
    created_at      created_at_;
    last_updated_at last_updated_at_;
};

}  // namespace

// ===================================================================
// DQL
// ===================================================================

suite<"DQL"> dql_suite = [] {
    "select single column - generates correct SQL"_test = [] {
        auto const sql = select<product::id>().from<product>().build_sql();
        expect(sql == "SELECT id FROM product"s) << sql;
    };

    "select two columns - generates correct SQL"_test = [] {
        auto const sql = select<product::id, product::sku>().from<product>().build_sql();
        expect(sql == "SELECT id, sku FROM product"s) << sql;
    };

    "select with WHERE clause - generates correct SQL"_test = [] {
        auto const sql =
            select<product::id, product::sku>().from<product>().where(equal<product::sku>("WIDGET")).build_sql();
        expect(sql == "SELECT id, sku FROM product WHERE sku = 'WIDGET'"s) << sql;
    };

    "select with typed equal WHERE - generates correct SQL"_test = [] {
        auto const sql = select<product::id, product::sku>()
                             .from<product>()
                             .where(equal<product::sku>(product::sku{"WIDGET"}))
                             .build_sql();
        expect(sql == "SELECT id, sku FROM product WHERE sku = 'WIDGET'"s) << sql;
    };

    "select with typed is_null WHERE - generates correct SQL"_test = [] {
        auto const sql = select<product::id, product::sku>().from<product>().where(is_null<product::tag>()).build_sql();
        expect(sql == "SELECT id, sku FROM product WHERE tag IS NULL"s) << sql;
    };

    "select with compound typed WHERE and LIMIT - generates correct SQL"_test = [] {
        auto const sql = select<product::id, product::sku>()
                             .from<product>()
                             .where(and_(equal<product::category_id>(1u), is_not_null<product::tag>()))
                             .limit(5)
                             .build_sql();
        expect(sql == "SELECT id, sku FROM product WHERE (category_id = 1 AND tag IS NOT NULL) LIMIT 5"s) << sql;
    };

    "select with LIMIT clause - generates correct SQL"_test = [] {
        auto const sql = select<product::id, product::sku>().from<product>().limit(10).build_sql();
        expect(sql == "SELECT id, sku FROM product LIMIT 10"s) << sql;
    };

    "select with WHERE and LIMIT - generates correct SQL"_test = [] {
        auto const sql = select<product::id, product::sku>()
                             .from<product>()
                             .where(equal<product::sku>("WIDGET"))
                             .limit(10)
                             .build_sql();
        expect(sql == "SELECT id, sku FROM product WHERE sku = 'WIDGET' LIMIT 10"s) << sql;
    };

    "select all product fields - generates correct SQL"_test = [] {
        auto const sql = select<product::id, product::category_id, product::sku, product::type, product::name,
                                product::tag, product::unit, product::created_at, product::last_updated_at>()
                             .from<product>()
                             .build_sql();
        expect(sql == "SELECT id, category_id, sku, type, name, tag, unit, created_at, last_updated_at FROM product"s)
            << sql;
    };

    "select<count_all> - generates correct SQL"_test = [] {
        auto const sql = select<count_all>().from<product>().build_sql();
        expect(sql == "SELECT COUNT(*) FROM product"s) << sql;
    };

    "select<count_all> with WHERE - generates correct SQL"_test = [] {
        auto const sql = select<count_all>().from<product>().where(equal<product::sku>("WIDGET")).build_sql();
        expect(sql == "SELECT COUNT(*) FROM product WHERE sku = 'WIDGET'"s) << sql;
    };

    "select<count_all> and count<T> produce identical SQL"_test = [] {
        auto const via_select = select<count_all>().from<product>().build_sql();
        auto const via_count = count<product>().build_sql();
        expect(via_select == via_count) << "select<count_all> and count<T> must produce the same SQL";
    };
};

// ===================================================================
// DQL WHERE predicates — IN, BETWEEN, NOT LIKE, subqueries
// ===================================================================

suite<"DQL WHERE Predicates"> dql_where_predicates_suite = [] {
    "in - generates IN list"_test = [] {
        auto const cond = in<product::id>({1u, 2u, 3u});
        expect(cond.build_sql() == "id IN (1, 2, 3)"s) << cond.build_sql();
    };

    "not_in - generates NOT IN list"_test = [] {
        auto const cond = not_in<product::id>({10u, 20u});
        expect(cond.build_sql() == "id NOT IN (10, 20)"s) << cond.build_sql();
    };

    "between - generates BETWEEN clause"_test = [] {
        auto const cond = between(product::id{1u}, product::id{100u});
        expect(cond.build_sql() == "id BETWEEN 1 AND 100"s) << cond.build_sql();
    };

    "not_like - generates NOT LIKE clause"_test = [] {
        auto const cond = not_like<product::sku>("WID%");
        expect(cond.build_sql() == "sku NOT LIKE 'WID%'"s) << cond.build_sql();
    };

    "in used in select WHERE - generates correct SQL"_test = [] {
        auto const sql =
            select<product::id, product::sku>().from<product>().where(in<product::id>({1u, 2u, 3u})).build_sql();
        expect(sql == "SELECT id, sku FROM product WHERE id IN (1, 2, 3)"s) << sql;
    };

    "in_subquery - generates IN (SELECT ...) clause"_test = [] {
        auto const subquery = select<product::id>().from<product>().where(equal<product::type>("widget"));
        auto const cond = in_subquery<product::category_id>(subquery);
        expect(cond.build_sql() == "category_id IN (SELECT id FROM product WHERE type = 'widget')"s)
            << cond.build_sql();
    };

    "not_in_subquery - generates NOT IN (SELECT ...) clause"_test = [] {
        auto const subquery = select<product::id>().from<product>().where(equal<product::type>("widget"));
        auto const cond = not_in_subquery<product::category_id>(subquery);
        expect(cond.build_sql() == "category_id NOT IN (SELECT id FROM product WHERE type = 'widget')"s)
            << cond.build_sql();
    };

    "exists - generates EXISTS (SELECT ...) clause"_test = [] {
        auto const subquery = select<product::id>().from<product>().where(equal<product::type>("widget"));
        auto const cond = exists(subquery);
        expect(cond.build_sql() == "EXISTS (SELECT id FROM product WHERE type = 'widget')"s) << cond.build_sql();
    };

    "not_exists - generates NOT EXISTS (SELECT ...) clause"_test = [] {
        auto const subquery = select<product::id>().from<product>().where(equal<product::type>("widget"));
        auto const cond = not_exists(subquery);
        expect(cond.build_sql() == "NOT EXISTS (SELECT id FROM product WHERE type = 'widget')"s) << cond.build_sql();
    };
};

// ===================================================================
// DQL Aggregate projections
// ===================================================================

suite<"DQL Aggregate Projections"> dql_aggregate_projections_suite = [] {
    "select count_col - generates COUNT(col)"_test = [] {
        auto const sql = select<count_col<product::id>>().from<product>().build_sql();
        expect(sql == "SELECT COUNT(id) FROM product"s) << sql;
    };

    "select sum - generates SUM(col)"_test = [] {
        auto const sql = select<sum<product::id>>().from<product>().build_sql();
        expect(sql == "SELECT SUM(id) FROM product"s) << sql;
    };

    "select avg - generates AVG(col)"_test = [] {
        auto const sql = select<avg<product::id>>().from<product>().build_sql();
        expect(sql == "SELECT AVG(id) FROM product"s) << sql;
    };

    "select min_of - generates MIN(col)"_test = [] {
        auto const sql = select<min_of<product::id>>().from<product>().build_sql();
        expect(sql == "SELECT MIN(id) FROM product"s) << sql;
    };

    "select max_of - generates MAX(col)"_test = [] {
        auto const sql = select<max_of<product::id>>().from<product>().build_sql();
        expect(sql == "SELECT MAX(id) FROM product"s) << sql;
    };

    "select mixed column and aggregate - generates correct SQL"_test = [] {
        auto const sql = select<product::sku, count_all>().from<product>().group_by<product::sku>().build_sql();
        expect(sql == "SELECT sku, COUNT(*) FROM product GROUP BY sku"s) << sql;
    };
};

// ===================================================================
// DQL Extended clauses
// ===================================================================

suite<"DQL Extended Clauses"> dql_extended_clauses_suite = [] {
    "distinct - prepends DISTINCT keyword"_test = [] {
        auto const sql = select<product::type>().from<product>().distinct().build_sql();
        expect(sql == "SELECT DISTINCT type FROM product"s) << sql;
    };

    "order_by ASC (default) - appends ORDER BY col ASC"_test = [] {
        auto const sql = select<product::id>().from<product>().order_by<product::id>().build_sql();
        expect(sql == "SELECT id FROM product ORDER BY id ASC"s) << sql;
    };

    "order_by DESC - appends ORDER BY col DESC"_test = [] {
        auto const sql = select<product::id>().from<product>().order_by<product::id, sort_order::desc>().build_sql();
        expect(sql == "SELECT id FROM product ORDER BY id DESC"s) << sql;
    };

    "order_by two columns - appends both columns"_test = [] {
        auto const sql = select<product::id, product::sku>()
                             .from<product>()
                             .order_by<product::type>()
                             .order_by<product::id, sort_order::desc>()
                             .build_sql();
        expect(sql == "SELECT id, sku FROM product ORDER BY type ASC, id DESC"s) << sql;
    };

    "group_by single column - generates GROUP BY"_test = [] {
        auto const sql = select<product::type, count_all>().from<product>().group_by<product::type>().build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM product GROUP BY type"s) << sql;
    };

    "group_by two columns - generates GROUP BY col1, col2"_test = [] {
        auto const sql = select<product::type, product::unit, count_all>()
                             .from<product>()
                             .group_by<product::type, product::unit>()
                             .build_sql();
        expect(sql == "SELECT type, unit, COUNT(*) FROM product GROUP BY type, unit"s) << sql;
    };

    "having - appends HAVING clause"_test = [] {
        auto const sql = select<product::type, count_all>()
                             .from<product>()
                             .group_by<product::type>()
                             .having(count() > 5ULL)
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM product GROUP BY type HAVING COUNT(*) > 5"s) << sql;
    };

    "where + group_by + having + order_by + limit - full clause chain"_test = [] {
        auto const sql = select<product::type, count_all>()
                             .from<product>()
                             .where(is_not_null<product::unit>())
                             .group_by<product::type>()
                             .having(count() > 2ULL)
                             .order_by<product::type>()
                             .limit(10)
                             .build_sql();
        expect(
            sql ==
            "SELECT type, COUNT(*) FROM product WHERE unit IS NOT NULL GROUP BY type HAVING COUNT(*) > 2 ORDER BY type ASC LIMIT 10"s)
            << sql;
    };

    "offset alone - appends OFFSET clause"_test = [] {
        auto const sql = select<product::id>().from<product>().offset(20).build_sql();
        expect(sql == "SELECT id FROM product OFFSET 20"s) << sql;
    };

    "limit + offset - appends LIMIT then OFFSET"_test = [] {
        auto const sql = select<product::id>().from<product>().limit(10).offset(20).build_sql();
        expect(sql == "SELECT id FROM product LIMIT 10 OFFSET 20"s) << sql;
    };

    "where + limit + offset - generates correct SQL"_test = [] {
        auto const sql = select<product::id, product::sku>()
                             .from<product>()
                             .where(equal<product::type>("widget"))
                             .limit(5)
                             .offset(15)
                             .build_sql();
        expect(sql == "SELECT id, sku FROM product WHERE type = 'widget' LIMIT 5 OFFSET 15"s) << sql;
    };

    "order_by + limit + offset - pagination query"_test = [] {
        auto const sql = select<product::id, product::sku>()
                             .from<product>()
                             .order_by<product::id>()
                             .limit(10)
                             .offset(30)
                             .build_sql();
        expect(sql == "SELECT id, sku FROM product ORDER BY id ASC LIMIT 10 OFFSET 30"s) << sql;
    };

    "where + group_by + having + order_by + limit + offset - full clause chain"_test = [] {
        auto const sql = select<product::type, count_all>()
                             .from<product>()
                             .where(is_not_null<product::unit>())
                             .group_by<product::type>()
                             .having(count() > 2ULL)
                             .order_by<product::type>()
                             .limit(10)
                             .offset(20)
                             .build_sql();
        expect(
            sql ==
            "SELECT type, COUNT(*) FROM product WHERE unit IS NOT NULL GROUP BY type HAVING COUNT(*) > 2 ORDER BY type ASC LIMIT 10 OFFSET 20"s)
            << sql;
    };

    "union_ - combines two queries with UNION"_test = [] {
        auto const q1 = select<product::id>().from<product>().where(equal<product::type>("gadget"));
        auto const q2 = select<product::id>().from<product>().where(equal<product::type>("widget"));
        auto const sql = union_(q1, q2).build_sql();
        expect(sql ==
               "(SELECT id FROM product WHERE type = 'gadget') UNION (SELECT id FROM product WHERE type = 'widget')"s)
            << sql;
    };

    "union_all - combines two queries with UNION ALL"_test = [] {
        auto const q1 = select<product::id>().from<product>().where(equal<product::type>("gadget"));
        auto const q2 = select<product::id>().from<product>().where(equal<product::type>("widget"));
        auto const sql = union_all(q1, q2).build_sql();
        expect(
            sql ==
            "(SELECT id FROM product WHERE type = 'gadget') UNION ALL (SELECT id FROM product WHERE type = 'widget')"s)
            << sql;
    };
};

// ===================================================================
// DQL HAVING predicates
// ===================================================================

suite<"DQL HAVING Predicates"> dql_having_predicates_suite = [] {
    "count() > n - operator syntax"_test = [] {
        auto const sql = select<product::type, count_all>()
                             .from<product>()
                             .group_by<product::type>()
                             .having(count() > 5u)
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM product GROUP BY type HAVING COUNT(*) > 5"s) << sql;
    };

    "count() > n - named method syntax"_test = [] {
        auto const sql = select<product::type, count_all>()
                             .from<product>()
                             .group_by<product::type>()
                             .having(count().greater_than(5u))
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM product GROUP BY type HAVING COUNT(*) > 5"s) << sql;
    };

    "count() >= n - operator syntax"_test = [] {
        auto const cond = count() >= 1u;
        expect(cond.build_sql() == "COUNT(*) >= 1"s) << cond.build_sql();
    };

    "count() < n - operator syntax"_test = [] {
        auto const cond = count() < 10u;
        expect(cond.build_sql() == "COUNT(*) < 10"s) << cond.build_sql();
    };

    "count() <= n - operator syntax"_test = [] {
        auto const cond = count() <= 10u;
        expect(cond.build_sql() == "COUNT(*) <= 10"s) << cond.build_sql();
    };

    "count() == n - operator syntax"_test = [] {
        auto const cond = count() == 0u;
        expect(cond.build_sql() == "COUNT(*) = 0"s) << cond.build_sql();
    };

    "count() != n - operator syntax"_test = [] {
        auto const cond = count() != 0u;
        expect(cond.build_sql() == "COUNT(*) != 0"s) << cond.build_sql();
    };

    "count<Col>() > n - COUNT(col) operator syntax"_test = [] {
        auto const cond = count<product::id>() > 0u;
        expect(cond.build_sql() == "COUNT(id) > 0"s) << cond.build_sql();
    };

    "count<Col>().greater_than - named method"_test = [] {
        auto const cond = count<product::id>().greater_than(0u);
        expect(cond.build_sql() == "COUNT(id) > 0"s) << cond.build_sql();
    };

    "sum<Col>() > n - SUM operator syntax"_test = [] {
        auto const cond = sum<product::id>() > 100.0;
        expect(cond.build_sql() == "SUM(id) > 100.000000"s) << cond.build_sql();
    };

    "avg<Col>() >= n - AVG operator syntax"_test = [] {
        auto const cond = avg<product::id>() >= 4.0;
        expect(cond.build_sql() == "AVG(id) >= 4.000000"s) << cond.build_sql();
    };

    "min_of<Col>() < n - MIN operator syntax"_test = [] {
        auto const cond = min_of<product::id>() < 100u;
        expect(cond.build_sql() == "MIN(id) < 100"s) << cond.build_sql();
    };

    "max_of<Col>() > n - MAX operator syntax"_test = [] {
        auto const cond = max_of<product::id>() > 0u;
        expect(cond.build_sql() == "MAX(id) > 0"s) << cond.build_sql();
    };

    "count() composed with & - AND over HAVING predicates"_test = [] {
        auto const cond = (count() > 2u) & (count() < 100u);
        expect(cond.build_sql() == "(COUNT(*) > 2 AND COUNT(*) < 100)"s) << cond.build_sql();
    };

    "full clause chain with count() > n"_test = [] {
        auto const sql = select<product::type, count_all>()
                             .from<product>()
                             .where(is_not_null<product::unit>())
                             .group_by<product::type>()
                             .having(count() > 2u)
                             .order_by<product::type>()
                             .limit(10)
                             .build_sql();
        expect(
            sql ==
            "SELECT type, COUNT(*) FROM product WHERE unit IS NOT NULL GROUP BY type HAVING COUNT(*) > 2 ORDER BY type ASC LIMIT 10"s)
            << sql;
    };
};

// ===================================================================
// DQL MySQL metadata queries
// ===================================================================

suite<"DQL MySQL Metadata Queries"> dql_mysql_metadata_suite = [] {
    "typed metadata query for database existence - generates correct SQL"_test = [] {
        auto const sql = select<count_all>()
                             .from<mysql_metadata::information_schema::schemata>()
                             .where(equal<mysql_metadata::information_schema::schemata::schema_name>("database_name"))
                             .build_sql();

        expect(sql == "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'database_name'"s) << sql;
    };

    "typed metadata query for all databases - generates correct SQL"_test = [] {
        auto const sql = select<mysql_metadata::information_schema::schemata::schema_name>()
                             .from<mysql_metadata::information_schema::schemata>()
                             .order_by<mysql_metadata::information_schema::schemata::schema_name>()
                             .build_sql();

        expect(sql == "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name ASC"s) << sql;
    };

    "typed metadata query for all tables across all databases - generates correct SQL"_test = [] {
        auto const sql = select<mysql_metadata::information_schema::tables::table_schema,
                                mysql_metadata::information_schema::tables::table_name>()
                             .from<mysql_metadata::information_schema::tables>()
                             .order_by<mysql_metadata::information_schema::tables::table_schema>()
                             .order_by<mysql_metadata::information_schema::tables::table_name>()
                             .build_sql();

        expect(sql ==
               "SELECT table_schema, table_name FROM information_schema.tables ORDER BY table_schema ASC, table_name "
               "ASC"s)
            << sql;
    };
};

// ===================================================================
// DQL JOIN queries
// ===================================================================

namespace {
struct category {
    using id    = column_field<"id",    uint32_t>;
    using label = column_field<"label", varchar_field<64>>;
    id    id_;
    label label_;
};
}  // namespace

suite<"DQL JOIN Queries"> dql_join_queries_suite = [] {
    "inner_join - generates INNER JOIN ON clause"_test = [] {
        // product.category_id FK → category.id
        auto const sql = select<product::sku, category::label>()
                             .from_joined<product>()
                             .inner_join<category, product::category_id, category::id>()
                             .build_sql();
        expect(sql == "SELECT sku, label FROM product INNER JOIN category ON category_id = id"s) << sql;
    };

    "left_join - generates LEFT JOIN ON clause"_test = [] {
        auto const sql = select<product::sku, category::label>()
                             .from_joined<product>()
                             .left_join<category, product::category_id, category::id>()
                             .build_sql();
        expect(sql == "SELECT sku, label FROM product LEFT JOIN category ON category_id = id"s) << sql;
    };

    "right_join - generates RIGHT JOIN ON clause"_test = [] {
        auto const sql = select<product::sku, category::label>()
                             .from_joined<product>()
                             .right_join<category, product::category_id, category::id>()
                             .build_sql();
        expect(sql == "SELECT sku, label FROM product RIGHT JOIN category ON category_id = id"s) << sql;
    };

    "inner_join with WHERE and ORDER BY - generates full query"_test = [] {
        auto const sql = select<product::sku, category::label>()
                             .from_joined<product>()
                             .inner_join<category, product::category_id, category::id>()
                             .where(is_not_null<product::category_id>())
                             .order_by<product::sku>()
                             .limit(20)
                             .build_sql();
        expect(
            sql ==
            "SELECT sku, label FROM product INNER JOIN category ON category_id = id WHERE category_id IS NOT NULL ORDER BY sku ASC LIMIT 20"s)
            << sql;
    };

    "inner_join with col<T,I> descriptors - generates qualified ON condition"_test = [] {
        // col<product, 1> = category_id field, col<category, 0> = id field
        auto const sql = select<col<product, 2>, col<category, 1>>()
                             .from_joined<product>()
                             .inner_join<category, col<product, 1>, col<category, 0>>()
                             .build_sql();
        expect(sql == "SELECT sku, label FROM product INNER JOIN category ON product.category_id = category.id"s)
            << sql;
    };
};

// ===================================================================
// result_row_type — compile-time checks
// ===================================================================

static_assert(std::is_same_v<decltype(select<product::id, product::sku>().from<product>())::result_row_type,
                             std::tuple<uint32_t, varchar_field<32>>>,
              "result_row_type must be std::tuple<uint32_t, varchar_field<32>>");
static_assert(std::is_same_v<decltype(select<count_all>().from<product>())::result_row_type, std::tuple<uint64_t>>,
              "result_row_type must be std::tuple<uint64_t>");

// ===================================================================
// column_belongs_to_table — compile-time checks
// ===================================================================

// Custom table types used to verify column membership enforcement.
// With the fixed_string design, column_field<"name", T> is the same type everywhere,
// so columns are only distinguishable across tables when their names differ.
namespace {
struct table_a {
    using id   = column_field<"id",     uint32_t>;
    using name = column_field<"a_name", std::string>;
    id   id_;
    name name_;
};

struct table_b {
    using id   = column_field<"id",     uint32_t>;
    using name = column_field<"b_name", std::string>;
    id   id_;
    name name_;
};
}  // namespace

// Columns belong to their own table.
static_assert(ds_mysql::detail::column_belongs_to_table_v<table_a::id, table_a>,
              "table_a::id must belong to table_a");
static_assert(ds_mysql::detail::column_belongs_to_table_v<table_a::name, table_a>,
              "table_a::name must belong to table_a");
static_assert(ds_mysql::detail::column_belongs_to_table_v<table_b::id, table_b>,
              "table_b::id must belong to table_b");

// Columns with the same name+type are the same type and therefore match any table
// containing that field — this is the expected trade-off of the fixed_string design.
static_assert(ds_mysql::detail::column_belongs_to_table_v<table_a::id, table_b>,
              "table_a::id == table_b::id (same type), so it belongs to both");

// Columns with distinct names are unique types and do NOT match across tables.
static_assert(!ds_mysql::detail::column_belongs_to_table_v<table_a::name, table_b>,
              "table_a::name (\"a_name\") must not belong to table_b");
static_assert(!ds_mysql::detail::column_belongs_to_table_v<table_b::name, table_a>,
              "table_b::name (\"b_name\") must not belong to table_a");

// ===================================================================
// DQL col_ref operators
// ===================================================================

suite<"DQL col_ref Operators"> dql_col_ref_suite = [] {
    // -------------------------------------------------------------------
    // comparison operators
    // -------------------------------------------------------------------

    "col_ref == string literal — generates equal WHERE SQL"_test = [] {
        auto const sql = select<product::sku>().from<product>().where(col_ref<product::sku> == "WIDGET").build_sql();
        expect(sql == "SELECT sku FROM product WHERE sku = 'WIDGET'"s) << sql;
    };

    "col_ref != string literal — generates not_equal WHERE SQL"_test = [] {
        auto const sql = select<product::sku>().from<product>().where(col_ref<product::sku> != "WIDGET").build_sql();
        expect(sql == "SELECT sku FROM product WHERE sku != 'WIDGET'"s) << sql;
    };

    "col_ref == integer — generates equal WHERE SQL"_test = [] {
        auto const sql = select<product::id>().from<product>().where(col_ref<product::id> == 42u).build_sql();
        expect(sql == "SELECT id FROM product WHERE id = 42"s) << sql;
    };

    "col_ref < integer — generates less_than WHERE SQL"_test = [] {
        auto const sql = select<product::id>().from<product>().where(col_ref<product::id> < 100u).build_sql();
        expect(sql == "SELECT id FROM product WHERE id < 100"s) << sql;
    };

    "col_ref > integer — generates greater_than WHERE SQL"_test = [] {
        auto const sql = select<product::id>().from<product>().where(col_ref<product::id> > 0u).build_sql();
        expect(sql == "SELECT id FROM product WHERE id > 0"s) << sql;
    };

    "col_ref <= integer — generates less_than_or_equal WHERE SQL"_test = [] {
        auto const sql = select<product::id>().from<product>().where(col_ref<product::id> <= 99u).build_sql();
        expect(sql == "SELECT id FROM product WHERE id <= 99"s) << sql;
    };

    "col_ref >= integer — generates greater_than_or_equal WHERE SQL"_test = [] {
        auto const sql = select<product::id>().from<product>().where(col_ref<product::id> >= 1u).build_sql();
        expect(sql == "SELECT id FROM product WHERE id >= 1"s) << sql;
    };

    // -------------------------------------------------------------------
    // named predicates
    // -------------------------------------------------------------------

    "col_ref.is_null() — generates IS NULL WHERE SQL"_test = [] {
        auto const sql = select<product::id>().from<product>().where(col_ref<product::name>.is_null()).build_sql();
        expect(sql == "SELECT id FROM product WHERE name IS NULL"s) << sql;
    };

    "col_ref.is_not_null() — generates IS NOT NULL WHERE SQL"_test = [] {
        auto const sql = select<product::id>().from<product>().where(col_ref<product::tag>.is_not_null()).build_sql();
        expect(sql == "SELECT id FROM product WHERE tag IS NOT NULL"s) << sql;
    };

    "col_ref.like() — generates LIKE WHERE SQL"_test = [] {
        auto const sql = select<product::sku>().from<product>().where(col_ref<product::sku>.like("WID%")).build_sql();
        expect(sql == "SELECT sku FROM product WHERE sku LIKE 'WID%'"s) << sql;
    };

    "col_ref.not_like() — generates NOT LIKE WHERE SQL"_test = [] {
        auto const sql = select<product::sku>().from<product>().where(col_ref<product::sku>.not_like("X%")).build_sql();
        expect(sql == "SELECT sku FROM product WHERE sku NOT LIKE 'X%'"s) << sql;
    };

    "col_ref.between() — generates BETWEEN WHERE SQL"_test = [] {
        auto const sql =
            select<product::id>().from<product>().where(col_ref<product::id>.between(1u, 100u)).build_sql();
        expect(sql == "SELECT id FROM product WHERE id BETWEEN 1 AND 100"s) << sql;
    };

    "col_ref.in() — generates IN WHERE SQL"_test = [] {
        auto const sql = select<product::id>().from<product>().where(col_ref<product::id>.in({1u, 2u, 3u})).build_sql();
        expect(sql == "SELECT id FROM product WHERE id IN (1, 2, 3)"s) << sql;
    };

    "col_ref.not_in() — generates NOT IN WHERE SQL"_test = [] {
        auto const sql =
            select<product::id>().from<product>().where(col_ref<product::id>.not_in({10u, 20u})).build_sql();
        expect(sql == "SELECT id FROM product WHERE id NOT IN (10, 20)"s) << sql;
    };

    // -------------------------------------------------------------------
    // logical composition operators
    // -------------------------------------------------------------------

    "! — generates NOT WHERE SQL"_test = [] {
        auto const sql = select<product::sku>().from<product>().where(!(col_ref<product::sku> == "WIDGET")).build_sql();
        expect(sql == "SELECT sku FROM product WHERE NOT (sku = 'WIDGET')"s) << sql;
    };

    "& — generates AND WHERE SQL"_test = [] {
        auto const sql = select<product::id>()
                             .from<product>()
                             .where((col_ref<product::id> >= 1u) & (col_ref<product::id> <= 100u))
                             .build_sql();
        expect(sql == "SELECT id FROM product WHERE (id >= 1 AND id <= 100)"s) << sql;
    };

    "| — generates OR WHERE SQL"_test = [] {
        auto const sql = select<product::sku>()
                             .from<product>()
                             .where((col_ref<product::sku> == "WIDGET") | (col_ref<product::sku> == "GADGET"))
                             .build_sql();
        expect(sql == "SELECT sku FROM product WHERE (sku = 'WIDGET' OR sku = 'GADGET')"s) << sql;
    };

    // -------------------------------------------------------------------
    // operator precedence
    // -------------------------------------------------------------------

    // (a | b) & c — without explicit parentheses the engine would evaluate
    // a | (b & c) because & binds tighter than | in C++. The parens fix that.
    "(a | b) & c — parentheses override & > | precedence"_test = [] {
        auto const sql = select<product::sku>()
                             .from<product>()
                             .where(((col_ref<product::sku> == "WIDGET") | (col_ref<product::type> == "gadget")) &
                                    (col_ref<product::name>.is_not_null()))
                             .build_sql();
        expect(sql == "SELECT sku FROM product WHERE ((sku = 'WIDGET' OR type = 'gadget') AND name IS NOT NULL)"s)
            << sql;
    };

    // Without parens, & binds first: a | (b & c).
    // Verify the SQL differs so the test above is meaningful.
    "a | (b & c) — default precedence without parens"_test = [] {
        auto const sql = select<product::sku>()
                             .from<product>()
                             .where((col_ref<product::sku> == "WIDGET") |
                                    ((col_ref<product::type> == "gadget") & (col_ref<product::name>.is_not_null())))
                             .build_sql();
        expect(sql == "SELECT sku FROM product WHERE (sku = 'WIDGET' OR (type = 'gadget' AND name IS NOT NULL))"s)
            << sql;
    };
};
