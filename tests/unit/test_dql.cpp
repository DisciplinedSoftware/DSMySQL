#include <boost/ut.hpp>
#include <string>
#include <tuple>
#include <type_traits>

#include "ds_mysql/metadata.hpp"
#include "ds_mysql/sql_dql.hpp"

using namespace boost::ut;
using namespace ds_mysql;
using namespace std::string_literals;

// ===================================================================
// Test fixtures
// ===================================================================

namespace {

struct product {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(category_id, std::optional<uint32_t>)
    COLUMN_FIELD(sku, varchar_type<32>)
    COLUMN_FIELD(type, varchar_type<64>)
    COLUMN_FIELD(name, std::optional<varchar_type<255>>)
    COLUMN_FIELD(tag, std::optional<varchar_type<255>>)
    COLUMN_FIELD(unit, std::optional<varchar_type<32>>)
    COLUMN_FIELD(created_at, datetime_type<>)
    COLUMN_FIELD(last_updated_at, datetime_type<>)
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

    "select all product fields with order_by and limit - composes with standard DQL clauses"_test = [] {
        auto const sql = select<product::id, product::category_id, product::sku, product::type, product::name,
                                product::tag, product::unit, product::created_at, product::last_updated_at>()
                             .from<product>()
                             .order_by<product::id>()
                             .limit(3)
                             .build_sql();
        expect(
            sql ==
            "SELECT id, category_id, sku, type, name, tag, unit, created_at, last_updated_at FROM product ORDER BY id ASC LIMIT 3"s)
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

    "for_update - appends FOR UPDATE lock clause"_test = [] {
        auto const sql = select<product::id>().from<product>().where(equal<product::id>(1u)).for_update().build_sql();
        expect(sql == "SELECT id FROM product WHERE id = 1 FOR UPDATE"s) << sql;
    };

    "for_share - appends FOR SHARE lock clause"_test = [] {
        auto const sql = select<product::id>().from<product>().order_by<product::id>().for_share().build_sql();
        expect(sql == "SELECT id FROM product ORDER BY id ASC FOR SHARE"s) << sql;
    };

    "lock_in_share_mode - appends LOCK IN SHARE MODE lock clause"_test = [] {
        auto const sql = select<product::id>().from<product>().limit(5).lock_in_share_mode().build_sql();
        expect(sql == "SELECT id FROM product LIMIT 5 LOCK IN SHARE MODE"s) << sql;
    };

    "lock clause precedence - last lock method wins"_test = [] {
        auto const sql = select<product::id>().from<product>().for_update().for_share().build_sql();
        expect(sql == "SELECT id FROM product FOR SHARE"s) << sql;
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
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(label, varchar_type<64>)
};
}  // namespace

suite<"DQL JOIN Queries"> dql_join_queries_suite = [] {
    "inner_join - generates INNER JOIN ON clause"_test = [] {
        // product.category_id FK → category.id
        auto const sql = select<product::sku, category::label>()
                             .from<joined<product>>()
                             .inner_join<category, product::category_id, category::id>()
                             .build_sql();
        expect(sql == "SELECT sku, label FROM product INNER JOIN category ON category_id = id"s) << sql;
    };

    "left_join - generates LEFT JOIN ON clause"_test = [] {
        auto const sql = select<product::sku, category::label>()
                             .from<joined<product>>()
                             .left_join<category, product::category_id, category::id>()
                             .build_sql();
        expect(sql == "SELECT sku, label FROM product LEFT JOIN category ON category_id = id"s) << sql;
    };

    "right_join - generates RIGHT JOIN ON clause"_test = [] {
        auto const sql = select<product::sku, category::label>()
                             .from<joined<product>>()
                             .right_join<category, product::category_id, category::id>()
                             .build_sql();
        expect(sql == "SELECT sku, label FROM product RIGHT JOIN category ON category_id = id"s) << sql;
    };

    "inner_join with WHERE and ORDER BY - generates full query"_test = [] {
        auto const sql = select<product::sku, category::label>()
                             .from<joined<product>>()
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
                             .from<joined<product>>()
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
                             std::tuple<uint32_t, varchar_type<32>>>,
              "result_row_type must be std::tuple<uint32_t, varchar_type<32>>");
static_assert(std::is_same_v<decltype(select<count_all>().from<product>())::result_row_type, std::tuple<uint64_t>>,
              "result_row_type must be std::tuple<uint64_t>");

// ===================================================================
// column_belongs_to_table — compile-time checks
// ===================================================================

// Custom table types used to verify cross-table type isolation with tagged fields.
namespace {
struct table_a {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(name, std::string)
};

struct table_b {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(name, std::string)
};
}  // namespace

// Different tags → different types: cross-table isolation is guaranteed.
static_assert(!std::is_same_v<table_a::id, table_b::id>);

// Columns belong to their own table.
static_assert(ds_mysql::detail::column_belongs_to_table_v<table_a::id, table_a>, "table_a::id must belong to table_a");
static_assert(ds_mysql::detail::column_belongs_to_table_v<table_a::name, table_a>,
              "table_a::name must belong to table_a");
static_assert(ds_mysql::detail::column_belongs_to_table_v<table_b::id, table_b>, "table_b::id must belong to table_b");

// Cross-table isolation: tagged columns do NOT belong to the other table.
static_assert(!ds_mysql::detail::column_belongs_to_table_v<table_a::id, table_b>,
              "table_a::id must not belong to table_b (different tag types)");
static_assert(!ds_mysql::detail::column_belongs_to_table_v<table_b::id, table_a>,
              "table_b::id must not belong to table_a (different tag types)");
static_assert(!ds_mysql::detail::column_belongs_to_table_v<table_a::name, table_b>,
              "table_a::name must not belong to table_b");
static_assert(!ds_mysql::detail::column_belongs_to_table_v<table_b::name, table_a>,
              "table_b::name must not belong to table_a");

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

namespace {

// Test schema reused across merged feature suites
struct ext_product {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(category_id, std::optional<uint32_t>)
    COLUMN_FIELD(sku, varchar_type<32>)
    COLUMN_FIELD(type, varchar_type<64>)
    COLUMN_FIELD(name, std::optional<varchar_type<255>>)
    COLUMN_FIELD(price_val, double)
};

struct ext_category {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(label, varchar_type<64>)
};

struct ext_event_log {
    struct event_id_tag {};
    using id = tagged_column_field<event_id_tag, uint32_t>;
    id id_;
    COLUMN_FIELD(created_at, datetime_type<>)
};

}  // namespace

// ===================================================================
// agg_expr — new BETWEEN, IN, IS NULL/IS NOT NULL predicates
// ===================================================================

suite<"DQL agg_expr Predicates"> dql_agg_expr_predicates_suite = [] {
    "count().between(lo, hi) — generates BETWEEN HAVING"_test = [] {
        auto const cond = count().between(1u, 10u);
        expect(cond.build_sql() == "COUNT(*) BETWEEN 1 AND 10"s) << cond.build_sql();
    };

    "count() BETWEEN in HAVING clause"_test = [] {
        auto const sql = select<ext_product::type, count_all>()
                             .from<ext_product>()
                             .group_by<ext_product::type>()
                             .having(count().between(2u, 50u))
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM ext_product GROUP BY type HAVING COUNT(*) BETWEEN 2 AND 50"s) << sql;
    };

    "count().in({1,2,3}) — generates IN HAVING"_test = [] {
        auto const cond = count().in({1u, 2u, 3u});
        expect(cond.build_sql() == "COUNT(*) IN (1, 2, 3)"s) << cond.build_sql();
    };

    "count().in in HAVING clause"_test = [] {
        auto const sql = select<ext_product::type, count_all>()
                             .from<ext_product>()
                             .group_by<ext_product::type>()
                             .having(count().in({1u, 5u, 10u}))
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM ext_product GROUP BY type HAVING COUNT(*) IN (1, 5, 10)"s) << sql;
    };

    "count().is_null() — generates IS NULL HAVING"_test = [] {
        auto const cond = count().is_null();
        expect(cond.build_sql() == "COUNT(*) IS NULL"s) << cond.build_sql();
    };

    "count().is_not_null() — generates IS NOT NULL HAVING"_test = [] {
        auto const cond = count().is_not_null();
        expect(cond.build_sql() == "COUNT(*) IS NOT NULL"s) << cond.build_sql();
    };

    "sum<Col>().between — SUM BETWEEN"_test = [] {
        auto const cond = agg_expr<sum<ext_product::price_val>>{}.between(0.0, 1000.0);
        expect(cond.build_sql() == "SUM(price_val) BETWEEN 0.000000 AND 1000.000000"s) << cond.build_sql();
    };

    "count().between composes with & — compound HAVING"_test = [] {
        auto const cond = (count().between(1u, 10u)) & (count() != 5u);
        expect(cond.build_sql() == "(COUNT(*) BETWEEN 1 AND 10 AND COUNT(*) != 5)"s) << cond.build_sql();
    };
};

// ===================================================================
// NULL-safe equality (<=>)
// ===================================================================

suite<"DQL NULL-safe Equality"> dql_null_safe_equality_suite = [] {
    "null_safe_equal<Col>(value) — generates <=> SQL"_test = [] {
        auto const cond = null_safe_equal<ext_product::id>(42u);
        expect(cond.build_sql() == "id <=> 42"s) << cond.build_sql();
    };

    "null_safe_equal with optional column and NULL — uses NULL literal"_test = [] {
        auto const cond = null_safe_equal<ext_product::category_id>(std::nullopt);
        expect(cond.build_sql() == "category_id <=> NULL"s) << cond.build_sql();
    };

    "col_ref.null_safe_equal() — operator method on col_expr"_test = [] {
        auto const cond = col_ref<ext_product::id>.null_safe_equal(42u);
        expect(cond.build_sql() == "id <=> 42"s) << cond.build_sql();
    };

    "null_safe_equal in WHERE clause"_test = [] {
        auto const sql =
            select<ext_product::id>().from<ext_product>().where(null_safe_equal<ext_product::id>(1u)).build_sql();
        expect(sql == "SELECT id FROM ext_product WHERE id <=> 1"s) << sql;
    };

    "mysql_null_safe_equal<Col>(value) — MySQL alias generates <=> SQL"_test = [] {
        auto const cond = mysql_null_safe_equal<ext_product::category_id>(std::nullopt);
        expect(cond.build_sql() == "category_id <=> NULL"s) << cond.build_sql();
    };
};

// ===================================================================
// REGEXP / RLIKE
// ===================================================================

suite<"DQL REGEXP Predicate"> dql_regexp_predicate_suite = [] {
    "regexp<Col>(pattern) — free function generates REGEXP SQL"_test = [] {
        auto const cond = regexp<ext_product::sku>("^A.*");
        expect(cond.build_sql() == "sku REGEXP '^A.*'"s) << cond.build_sql();
    };

    "col_ref.regexp() — method on col_expr"_test = [] {
        auto const cond = col_ref<ext_product::sku>.regexp("^WIDGET");
        expect(cond.build_sql() == "sku REGEXP '^WIDGET'"s) << cond.build_sql();
    };

    "regexp in WHERE clause"_test = [] {
        auto const sql = select<ext_product::id, ext_product::sku>()
                             .from<ext_product>()
                             .where(regexp<ext_product::sku>("^[A-Z]"))
                             .build_sql();
        expect(sql == "SELECT id, sku FROM ext_product WHERE sku REGEXP '^[A-Z]'"s) << sql;
    };

    "regexp escapes single-quote in pattern"_test = [] {
        auto const cond = regexp<ext_product::sku>("it's");
        expect(cond.build_sql() == "sku REGEXP 'it''s'"s) << cond.build_sql();
    };

    "mysql_regexp<Col>(pattern) — MySQL alias generates REGEXP SQL"_test = [] {
        auto const cond = mysql_regexp<ext_product::sku>("^MYSQL");
        expect(cond.build_sql() == "sku REGEXP '^MYSQL'"s) << cond.build_sql();
    };
};

// ===================================================================
// Scalar subquery comparison methods on col_expr
// ===================================================================

suite<"DQL Scalar Subquery Methods"> dql_scalar_subquery_suite = [] {
    "col_ref.eq_subquery — col = (SELECT ...)"_test = [] {
        auto const sub = select<max_of<ext_product::price_val>>().from<ext_product>();
        auto const cond = col_ref<ext_product::price_val>.eq_subquery(sub);
        expect(cond.build_sql() == "price_val = (SELECT MAX(price_val) FROM ext_product)"s) << cond.build_sql();
    };

    "col_ref.ne_subquery — col != (SELECT ...)"_test = [] {
        auto const sub = select<min_of<ext_product::id>>().from<ext_product>();
        auto const cond = col_ref<ext_product::id>.ne_subquery(sub);
        expect(cond.build_sql() == "id != (SELECT MIN(id) FROM ext_product)"s) << cond.build_sql();
    };

    "col_ref.gt_subquery — col > (SELECT ...)"_test = [] {
        auto const sub = select<avg<ext_product::price_val>>().from<ext_product>();
        auto const cond = col_ref<ext_product::price_val>.gt_subquery(sub);
        expect(cond.build_sql() == "price_val > (SELECT AVG(price_val) FROM ext_product)"s) << cond.build_sql();
    };

    "col_ref.lt_subquery — col < (SELECT ...)"_test = [] {
        auto const sub = select<avg<ext_product::price_val>>().from<ext_product>();
        auto const cond = col_ref<ext_product::price_val>.lt_subquery(sub);
        expect(cond.build_sql() == "price_val < (SELECT AVG(price_val) FROM ext_product)"s) << cond.build_sql();
    };

    "col_ref.ge_subquery — col >= (SELECT ...)"_test = [] {
        auto const sub = select<min_of<ext_product::price_val>>().from<ext_product>();
        auto const cond = col_ref<ext_product::price_val>.ge_subquery(sub);
        expect(cond.build_sql() == "price_val >= (SELECT MIN(price_val) FROM ext_product)"s) << cond.build_sql();
    };

    "col_ref.le_subquery — col <= (SELECT ...)"_test = [] {
        auto const sub = select<max_of<ext_product::price_val>>().from<ext_product>();
        auto const cond = col_ref<ext_product::price_val>.le_subquery(sub);
        expect(cond.build_sql() == "price_val <= (SELECT MAX(price_val) FROM ext_product)"s) << cond.build_sql();
    };

    "col_ref.in_subquery — col IN (SELECT ...)"_test = [] {
        auto const sub =
            select<ext_product::id>().from<ext_product>().where(equal<ext_product::type>(ext_product::type{"widget"}));
        auto const cond = col_ref<ext_product::category_id>.in_subquery(sub);
        expect(cond.build_sql() == "category_id IN (SELECT id FROM ext_product WHERE type = 'widget')"s)
            << cond.build_sql();
    };

    "col_ref.not_in_subquery — col NOT IN (SELECT ...)"_test = [] {
        auto const sub =
            select<ext_product::id>().from<ext_product>().where(equal<ext_product::type>(ext_product::type{"widget"}));
        auto const cond = col_ref<ext_product::category_id>.not_in_subquery(sub);
        expect(cond.build_sql() == "category_id NOT IN (SELECT id FROM ext_product WHERE type = 'widget')"s)
            << cond.build_sql();
    };

    "eq_subquery in full WHERE clause"_test = [] {
        auto const sub = select<max_of<ext_product::price_val>>().from<ext_product>();
        auto const sql = select<ext_product::id, ext_product::sku>()
                             .from<ext_product>()
                             .where(col_ref<ext_product::price_val>.eq_subquery(sub))
                             .build_sql();
        expect(sql == "SELECT id, sku FROM ext_product WHERE price_val = (SELECT MAX(price_val) FROM ext_product)"s)
            << sql;
    };
};

// ===================================================================
// Aggregate projections — count_distinct, group_concat
// ===================================================================

suite<"DQL Aggregate Features"> dql_aggregate_features_suite = [] {
    "count_distinct<Col> — generates COUNT(DISTINCT col)"_test = [] {
        auto const sql = select<count_distinct<ext_product::type>>().from<ext_product>().build_sql();
        expect(sql == "SELECT COUNT(DISTINCT type) FROM ext_product"s) << sql;
    };

    "count_distinct in HAVING — count distinct comparison"_test = [] {
        auto const cond = agg_expr<count_distinct<ext_product::type>>{} > 3u;
        expect(cond.build_sql() == "COUNT(DISTINCT type) > 3"s) << cond.build_sql();
    };

    "group_concat<Col> — generates GROUP_CONCAT(col)"_test = [] {
        auto const sql = select<ext_product::type, group_concat<ext_product::sku>>()
                             .from<ext_product>()
                             .group_by<ext_product::type>()
                             .build_sql();
        expect(sql == "SELECT type, GROUP_CONCAT(sku) FROM ext_product GROUP BY type"s) << sql;
    };

    "coalesce_of<Col1, Col2> — generates COALESCE(col1, col2)"_test = [] {
        auto const sql =
            select<coalesce_of<ext_product::category_id, ext_product::id>>().from<ext_product>().build_sql();
        expect(sql == "SELECT COALESCE(category_id, id) FROM ext_product"s) << sql;
    };

    "ifnull_of<Col1, Col2> — generates IFNULL(col1, col2)"_test = [] {
        auto const sql = select<ifnull_of<ext_product::name, ext_product::sku>>().from<ext_product>().build_sql();
        expect(sql == "SELECT IFNULL(name, sku) FROM ext_product"s) << sql;
    };

    "coalesce/ifnull aliases — generate expected SQL"_test = [] {
        auto const sql =
            select<coalesce<ext_product::category_id, ext_product::id>, ifnull<ext_product::name, ext_product::sku>>()
                .from<ext_product>()
                .build_sql();
        expect(sql == "SELECT COALESCE(category_id, id), IFNULL(name, sku) FROM ext_product"s) << sql;
    };

    "mysql_group_concat/mysql_ifnull_of — MySQL aliases generate aggregate SQL"_test = [] {
        auto const sql = select<ext_product::type, mysql_group_concat<ext_product::sku>,
                                mysql_ifnull_of<ext_product::name, ext_product::sku>>()
                             .from<ext_product>()
                             .group_by<ext_product::type>()
                             .build_sql();
        expect(sql == "SELECT type, GROUP_CONCAT(sku), IFNULL(name, sku) FROM ext_product GROUP BY type"s) << sql;
    };
};

// ===================================================================
// Scalar SQL functions
// ===================================================================

suite<"DQL Scalar Functions"> dql_scalar_functions_suite = [] {
    "round_to<Col,2> — generates ROUND(col, 2)"_test = [] {
        auto const sql = select<round_to<ext_product::price_val, 2>>().from<ext_product>().build_sql();
        expect(sql == "SELECT ROUND(price_val, 2) FROM ext_product"s) << sql;
    };

    "format_to<Col,2> — generates FORMAT(col, 2)"_test = [] {
        auto const sql = select<format_to<ext_product::price_val, 2>>().from<ext_product>().build_sql();
        expect(sql == "SELECT FORMAT(price_val, 2) FROM ext_product"s) << sql;
    };

    "round/format aliases — generate expected SQL"_test = [] {
        auto const sql =
            select<ds_mysql::round<ext_product::price_val, 2>, ds_mysql::format<ext_product::price_val, 2>>()
                .from<ext_product>()
                .build_sql();
        expect(sql == "SELECT ROUND(price_val, 2), FORMAT(price_val, 2) FROM ext_product"s) << sql;
    };

    "common scalar function wrappers — generate expected SQL"_test = [] {
        auto const sql =
            select<abs_of<ext_product::price_val>, floor_of<ext_product::price_val>, ceil_of<ext_product::price_val>,
                   upper_of<ext_product::sku>, lower_of<ext_product::type>, trim_of<ext_product::sku>,
                   length_of<ext_product::sku>, substring_of<ext_product::sku, 2, 3>,
                   mod_of<ext_product::price_val, ext_product::id>, power_of<ext_product::id, ext_product::id>,
                   concat_of<ext_product::sku, ext_product::type>,
                   cast_as<ext_product::id, sql_cast_type::signed_integer>,
                   convert_as<ext_product::sku, sql_cast_type::char_type>>()
                .from<ext_product>()
                .build_sql();
        expect(sql ==
               "SELECT ABS(price_val), FLOOR(price_val), CEIL(price_val), UPPER(sku), LOWER(type), TRIM(sku), "
               "LENGTH(sku), SUBSTRING(sku, 2, 3), MOD(price_val, id), POWER(id, id), CONCAT(sku, type), "
               "CAST(id AS SIGNED), CONVERT(sku, CHAR) FROM ext_product"s)
            << sql;
    };

    "short scalar aliases — generate expected SQL"_test = [] {
        auto const sql = select<ds_mysql::abs<ext_product::price_val>, ds_mysql::floor<ext_product::price_val>,
                                ds_mysql::ceil<ext_product::price_val>, ds_mysql::upper<ext_product::sku>,
                                ds_mysql::lower<ext_product::type>, ds_mysql::trim<ext_product::sku>,
                                ds_mysql::length<ext_product::sku>, ds_mysql::substring<ext_product::sku, 2, 3>,
                                ds_mysql::mod<ext_product::price_val, ext_product::id>,
                                ds_mysql::power<ext_product::id, ext_product::id>,
                                ds_mysql::concat<ext_product::sku, ext_product::type>,
                                ds_mysql::cast<ext_product::id, sql_cast_type::signed_integer>,
                                ds_mysql::convert<ext_product::sku, sql_cast_type::char_type>>()
                             .from<ext_product>()
                             .build_sql();
        expect(sql ==
               "SELECT ABS(price_val), FLOOR(price_val), CEIL(price_val), UPPER(sku), LOWER(type), TRIM(sku), "
               "LENGTH(sku), SUBSTRING(sku, 2, 3), MOD(price_val, id), POWER(id, id), CONCAT(sku, type), "
               "CAST(id AS SIGNED), CONVERT(sku, CHAR) FROM ext_product"s)
            << sql;
    };

    "date_format_of and extract_of — generate date functions"_test = [] {
        auto const sql = select<date_format_of<ext_event_log::created_at, "%Y-%m">,
                                extract_of<sql_date_part::year, ext_event_log::created_at>>()
                             .from<ext_event_log>()
                             .build_sql();
        expect(sql == "SELECT DATE_FORMAT(created_at, '%Y-%m'), EXTRACT(YEAR FROM created_at) FROM ext_event_log"s)
            << sql;
    };

    "date_format/extract aliases — generate expected SQL"_test = [] {
        auto const sql = select<date_format<ext_event_log::created_at, "%Y-%m">,
                                extract<sql_date_part::year, ext_event_log::created_at>>()
                             .from<ext_event_log>()
                             .build_sql();
        expect(sql == "SELECT DATE_FORMAT(created_at, '%Y-%m'), EXTRACT(YEAR FROM created_at) FROM ext_event_log"s)
            << sql;
    };

    "mysql_format_to/mysql_convert_as — MySQL aliases generate scalar SQL"_test = [] {
        auto const sql = select<mysql_format_to<ext_product::price_val, 2>,
                                mysql_convert_as<ext_product::sku, sql_cast_type::char_type>>()
                             .from<ext_product>()
                             .build_sql();
        expect(sql == "SELECT FORMAT(price_val, 2), CONVERT(sku, CHAR) FROM ext_product"s) << sql;
    };

    "cast_as/convert_as TIMESTAMP — generates TIMESTAMP SQL type"_test = [] {
        auto const sql = select<cast_as<ext_event_log::created_at, sql_cast_type::timestamp>,
                                convert_as<ext_event_log::created_at, sql_cast_type::timestamp>>()
                             .from<ext_event_log>()
                             .build_sql();
        expect(sql == "SELECT CAST(created_at AS TIMESTAMP), CONVERT(created_at, TIMESTAMP) FROM ext_event_log"s)
            << sql;
    };

    "mysql_date_format_of — MySQL alias generates date SQL"_test = [] {
        auto const sql =
            select<mysql_date_format_of<ext_event_log::created_at, "%Y-%m">>().from<ext_event_log>().build_sql();
        expect(sql == "SELECT DATE_FORMAT(created_at, '%Y-%m') FROM ext_event_log"s) << sql;
    };

    "date/time extraction helpers — generate expected SQL"_test = [] {
        auto const sql = select<date_of<ext_event_log::created_at>, time_of<ext_event_log::created_at>,
                                year_of<ext_event_log::created_at>, month_of<ext_event_log::created_at>,
                                day_of<ext_event_log::created_at>>()
                             .from<ext_event_log>()
                             .build_sql();
        expect(sql ==
               "SELECT DATE(created_at), TIME(created_at), YEAR(created_at), MONTH(created_at), DAY(created_at) "
               "FROM ext_event_log"s)
            << sql;
    };

    "datediff/date_add/date_sub/timestampdiff — generate expected SQL"_test = [] {
        auto const sql =
            select<datediff_of<ext_event_log::created_at, ext_event_log::created_at>,
                   date_add_of<ext_event_log::created_at, 7, sql_date_part::day>,
                   date_sub_of<ext_event_log::created_at, 1, sql_date_part::month>,
                   timestampdiff_of<sql_date_part::hour, ext_event_log::created_at, ext_event_log::created_at>>()
                .from<ext_event_log>()
                .build_sql();
        expect(
            sql ==
            "SELECT DATEDIFF(created_at, created_at), DATE_ADD(created_at, INTERVAL 7 DAY), "
            "DATE_SUB(created_at, INTERVAL 1 MONTH), TIMESTAMPDIFF(HOUR, created_at, created_at) FROM ext_event_log"s)
            << sql;
    };

    "date/time aliases — generate expected SQL"_test = [] {
        auto const sql =
            select<date_of<ext_event_log::created_at>, time_of<ext_event_log::created_at>,
                   year_of<ext_event_log::created_at>, month_of<ext_event_log::created_at>,
                   day_of<ext_event_log::created_at>, datediff<ext_event_log::created_at, ext_event_log::created_at>,
                   date_add<ext_event_log::created_at, 2, sql_date_part::week>,
                   date_sub<ext_event_log::created_at, 3, sql_date_part::day>,
                   timestampdiff<sql_date_part::minute, ext_event_log::created_at, ext_event_log::created_at>>()
                .from<ext_event_log>()
                .build_sql();
        expect(
            sql ==
            "SELECT DATE(created_at), TIME(created_at), YEAR(created_at), MONTH(created_at), DAY(created_at), "
            "DATEDIFF(created_at, created_at), DATE_ADD(created_at, INTERVAL 2 WEEK), "
            "DATE_SUB(created_at, INTERVAL 3 DAY), TIMESTAMPDIFF(MINUTE, created_at, created_at) FROM ext_event_log"s)
            << sql;
    };
};

// ===================================================================
// Arithmetic expression projections
// ===================================================================

suite<"DQL Arithmetic Projections"> dql_arithmetic_projections_suite = [] {
    "arith_add<Col, Col> — generates (col + col)"_test = [] {
        auto const sql = select<arith_add<ext_product::id, ext_product::price_val>>().from<ext_product>().build_sql();
        expect(sql == "SELECT (id + price_val) FROM ext_product"s) << sql;
    };

    "arith_sub<Col, Col> — generates (col - col)"_test = [] {
        auto const sql = select<arith_sub<ext_product::price_val, ext_product::id>>().from<ext_product>().build_sql();
        expect(sql == "SELECT (price_val - id) FROM ext_product"s) << sql;
    };

    "arith_mul<Col, Col> — generates (col * col)"_test = [] {
        auto const sql = select<arith_mul<ext_product::id, ext_product::price_val>>().from<ext_product>().build_sql();
        expect(sql == "SELECT (id * price_val) FROM ext_product"s) << sql;
    };

    "arith_div<Col, Col> — generates (col / col)"_test = [] {
        auto const sql = select<arith_div<ext_product::price_val, ext_product::id>>().from<ext_product>().build_sql();
        expect(sql == "SELECT (price_val / id) FROM ext_product"s) << sql;
    };

    "nested arithmetic — (col + col) * col"_test = [] {
        auto const sql = select<arith_mul<arith_add<ext_product::id, ext_product::price_val>, ext_product::id>>()
                             .from<ext_product>()
                             .build_sql();
        expect(sql == "SELECT ((id + price_val) * id) FROM ext_product"s) << sql;
    };
};

// ===================================================================
// Column alias — with_alias<Proj>("name")
// ===================================================================

suite<"DQL Column Alias"> dql_column_alias_suite = [] {
    "with_alias<Proj>(\"n\") — appends AS to matching projection"_test = [] {
        auto const sql = select<sum<ext_product::price_val>>()
                             .from<ext_product>()
                             .with_alias<sum<ext_product::price_val>>("total")
                             .build_sql();
        expect(sql == "SELECT SUM(price_val) AS total FROM ext_product"s) << sql;
    };

    "with_alias second projection — appends AS to second projection"_test = [] {
        auto const sql = select<ext_product::type, count_all>()
                             .from<ext_product>()
                             .group_by<ext_product::type>()
                             .with_alias<count_all>("cnt")
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) AS cnt FROM ext_product GROUP BY type"s) << sql;
    };

    "multiple aliases — both projections aliased"_test = [] {
        auto const sql = select<sum<ext_product::price_val>, count_all>()
                             .from<ext_product>()
                             .with_alias<sum<ext_product::price_val>>("total")
                             .with_alias<count_all>("qty")
                             .build_sql();
        expect(sql == "SELECT SUM(price_val) AS total, COUNT(*) AS qty FROM ext_product"s) << sql;
    };
};

// Projection type not in SELECT list is rejected at compile time.
// The concept variable separates the constraint check from the static_assert
// so that GCC treats the requires-clause violation as a substitution failure.
template <typename Builder, typename Proj>
inline constexpr bool can_alias_with = requires(Builder b) { b.template with_alias<Proj>(std::string{}); };

static_assert(!can_alias_with<decltype(select<ext_product::id>().from<ext_product>()), ext_product::sku>);

// ===================================================================
// case_when expression
// ===================================================================

suite<"DQL CASE WHEN"> dql_case_when_suite = [] {
    "case_when single branch — CASE WHEN ... THEN ... END"_test = [] {
        auto const expr =
            case_when(equal<ext_product::type>(ext_product::type{"widget"}), std::string{"Widget Type"}).build_sql();
        expect(expr == "CASE WHEN type = 'widget' THEN 'Widget Type' END"s) << expr;
    };

    "case_when with else — CASE WHEN ... THEN ... ELSE ... END"_test = [] {
        auto const expr = case_when(equal<ext_product::type>(ext_product::type{"widget"}), std::string{"Widget Type"})
                              .else_(std::string{"Other"})
                              .build_sql();
        expect(expr == "CASE WHEN type = 'widget' THEN 'Widget Type' ELSE 'Other' END"s) << expr;
    };

    "case_when multiple branches — CASE WHEN ... THEN ... WHEN ... THEN ... ELSE ... END"_test = [] {
        auto const expr = case_when(equal<ext_product::type>(ext_product::type{"widget"}), std::string{"Widget"})
                              .when(equal<ext_product::type>(ext_product::type{"gadget"}), std::string{"Gadget"})
                              .else_(std::string{"Unknown"})
                              .build_sql();
        expect(expr == "CASE WHEN type = 'widget' THEN 'Widget' WHEN type = 'gadget' THEN 'Gadget' ELSE 'Unknown' END"s)
            << expr;
    };
};

// ===================================================================
// Window functions
// ===================================================================

suite<"DQL Window Functions"> dql_window_function_suite = [] {
    "row_number_over — ROW_NUMBER() OVER (...)"_test = [] {
        auto const sql = select<ext_product::id, row_number_over<ext_product::type, ext_product::id>>()
                             .from<ext_product>()
                             .build_sql();
        expect(sql == "SELECT id, ROW_NUMBER() OVER (PARTITION BY type ORDER BY id ASC) FROM ext_product"s) << sql;
    };

    "row_number_over desc — ROW_NUMBER() OVER (... ORDER BY col DESC)"_test = [] {
        auto const sql =
            select<ext_product::id, row_number_over<ext_product::type, ext_product::id, sort_order::desc>>()
                .from<ext_product>()
                .build_sql();
        expect(sql == "SELECT id, ROW_NUMBER() OVER (PARTITION BY type ORDER BY id DESC) FROM ext_product"s) << sql;
    };

    "window_func<sum> — SUM OVER (...)"_test = [] {
        auto const sql =
            select<ext_product::id, window_func<sum<ext_product::price_val>, ext_product::type, ext_product::id>>()
                .from<ext_product>()
                .build_sql();
        expect(sql == "SELECT id, SUM(price_val) OVER (PARTITION BY type ORDER BY id ASC) FROM ext_product"s) << sql;
    };

    "rank/dense_rank/lag/lead — generate extended window functions"_test = [] {
        auto const sql =
            select<rank_over<ext_product::type, ext_product::id>, dense_rank_over<ext_product::type, ext_product::id>,
                   lag_over<ext_product::price_val, ext_product::type, ext_product::id>,
                   lead_over<ext_product::price_val, ext_product::type, ext_product::id, 2, sort_order::desc>>()
                .from<ext_product>()
                .build_sql();
        expect(sql ==
               "SELECT RANK() OVER (PARTITION BY type ORDER BY id ASC), DENSE_RANK() OVER (PARTITION BY type ORDER BY "
               "id ASC), LAG(price_val, 1) OVER (PARTITION BY type ORDER BY id ASC), LEAD(price_val, 2) OVER "
               "(PARTITION BY type ORDER BY id DESC) FROM ext_product"s)
            << sql;
    };

    "window frame clauses — append frame SQL in OVER()"_test = [] {
        auto const sql = select<row_number_over<ext_product::type, ext_product::id, sort_order::asc,
                                                rows_unbounded_preceding_to_current_row>,
                                window_func<sum<ext_product::price_val>, ext_product::type, ext_product::id,
                                            sort_order::asc, rows_current_row_to_unbounded_following>,
                                lag_over<ext_product::price_val, ext_product::type, ext_product::id, 1, sort_order::asc,
                                         rows_unbounded_preceding_to_unbounded_following>>()
                             .from<ext_product>()
                             .build_sql();
        expect(sql ==
               "SELECT ROW_NUMBER() OVER (PARTITION BY type ORDER BY id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND "
               "CURRENT ROW), SUM(price_val) OVER (PARTITION BY type ORDER BY id ASC ROWS BETWEEN CURRENT ROW AND "
               "UNBOUNDED FOLLOWING), LAG(price_val, 1) OVER (PARTITION BY type ORDER BY id ASC ROWS BETWEEN "
               "UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM ext_product"s)
            << sql;
    };
};

// ===================================================================
// NTILE, PERCENT_RANK, CUME_DIST, FIRST_VALUE, LAST_VALUE, NTH_VALUE
// ===================================================================

suite<"DQL Window Functions Extended"> dql_window_extended_suite = [] {
    // NTILE
    "ntile_over — NTILE(N) OVER (...)"_test = [] {
        auto const sql = select<ext_product::id, ntile_over<4, ext_product::type, ext_product::id>>()
                             .from<ext_product>()
                             .build_sql();
        expect(sql == "SELECT id, NTILE(4) OVER (PARTITION BY type ORDER BY id ASC) FROM ext_product"s) << sql;
    };

    "ntile_over desc — NTILE(N) OVER (... ORDER BY col DESC)"_test = [] {
        auto const sql =
            select<ext_product::id, ntile_over<10, ext_product::type, ext_product::id, sort_order::desc>>()
                .from<ext_product>()
                .build_sql();
        expect(sql == "SELECT id, NTILE(10) OVER (PARTITION BY type ORDER BY id DESC) FROM ext_product"s) << sql;
    };

    "ntile_over with frame clause"_test = [] {
        auto const sql =
            select<ntile_over<4, ext_product::type, ext_product::id, sort_order::asc,
                              rows_unbounded_preceding_to_current_row>>()
                .from<ext_product>()
                .build_sql();
        expect(sql ==
               "SELECT NTILE(4) OVER (PARTITION BY type ORDER BY id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND "
               "CURRENT ROW) FROM ext_product"s)
            << sql;
    };

    // PERCENT_RANK
    "percent_rank_over — PERCENT_RANK() OVER (...)"_test = [] {
        auto const sql = select<ext_product::id, percent_rank_over<ext_product::type, ext_product::id>>()
                             .from<ext_product>()
                             .build_sql();
        expect(sql == "SELECT id, PERCENT_RANK() OVER (PARTITION BY type ORDER BY id ASC) FROM ext_product"s) << sql;
    };

    "percent_rank_over desc — PERCENT_RANK() OVER (... ORDER BY col DESC)"_test = [] {
        auto const sql =
            select<ext_product::id, percent_rank_over<ext_product::type, ext_product::id, sort_order::desc>>()
                .from<ext_product>()
                .build_sql();
        expect(sql == "SELECT id, PERCENT_RANK() OVER (PARTITION BY type ORDER BY id DESC) FROM ext_product"s) << sql;
    };

    // CUME_DIST
    "cume_dist_over — CUME_DIST() OVER (...)"_test = [] {
        auto const sql = select<ext_product::id, cume_dist_over<ext_product::type, ext_product::id>>()
                             .from<ext_product>()
                             .build_sql();
        expect(sql == "SELECT id, CUME_DIST() OVER (PARTITION BY type ORDER BY id ASC) FROM ext_product"s) << sql;
    };

    "cume_dist_over desc — CUME_DIST() OVER (... ORDER BY col DESC)"_test = [] {
        auto const sql =
            select<ext_product::id, cume_dist_over<ext_product::type, ext_product::id, sort_order::desc>>()
                .from<ext_product>()
                .build_sql();
        expect(sql == "SELECT id, CUME_DIST() OVER (PARTITION BY type ORDER BY id DESC) FROM ext_product"s) << sql;
    };

    // FIRST_VALUE
    "first_value_over — FIRST_VALUE(col) OVER (...)"_test = [] {
        auto const sql =
            select<ext_product::id, first_value_over<ext_product::price_val, ext_product::type, ext_product::id>>()
                .from<ext_product>()
                .build_sql();
        expect(sql ==
               "SELECT id, FIRST_VALUE(price_val) OVER (PARTITION BY type ORDER BY id ASC) FROM ext_product"s)
            << sql;
    };

    "first_value_over desc — FIRST_VALUE(col) OVER (... ORDER BY col DESC)"_test = [] {
        auto const sql =
            select<first_value_over<ext_product::price_val, ext_product::type, ext_product::id, sort_order::desc>>()
                .from<ext_product>()
                .build_sql();
        expect(sql ==
               "SELECT FIRST_VALUE(price_val) OVER (PARTITION BY type ORDER BY id DESC) FROM ext_product"s)
            << sql;
    };

    "first_value_over with frame clause"_test = [] {
        auto const sql =
            select<first_value_over<ext_product::price_val, ext_product::type, ext_product::id, sort_order::asc,
                                    rows_unbounded_preceding_to_current_row>>()
                .from<ext_product>()
                .build_sql();
        expect(sql ==
               "SELECT FIRST_VALUE(price_val) OVER (PARTITION BY type ORDER BY id ASC ROWS BETWEEN UNBOUNDED "
               "PRECEDING AND CURRENT ROW) FROM ext_product"s)
            << sql;
    };

    // LAST_VALUE
    "last_value_over — LAST_VALUE(col) OVER (...)"_test = [] {
        auto const sql =
            select<ext_product::id, last_value_over<ext_product::price_val, ext_product::type, ext_product::id>>()
                .from<ext_product>()
                .build_sql();
        expect(sql ==
               "SELECT id, LAST_VALUE(price_val) OVER (PARTITION BY type ORDER BY id ASC) FROM ext_product"s)
            << sql;
    };

    "last_value_over with frame clause"_test = [] {
        auto const sql =
            select<last_value_over<ext_product::price_val, ext_product::type, ext_product::id, sort_order::asc,
                                   rows_unbounded_preceding_to_unbounded_following>>()
                .from<ext_product>()
                .build_sql();
        expect(sql ==
               "SELECT LAST_VALUE(price_val) OVER (PARTITION BY type ORDER BY id ASC ROWS BETWEEN UNBOUNDED "
               "PRECEDING AND UNBOUNDED FOLLOWING) FROM ext_product"s)
            << sql;
    };

    // NTH_VALUE
    "nth_value_over — NTH_VALUE(col, N) OVER (...)"_test = [] {
        auto const sql =
            select<ext_product::id,
                   nth_value_over<ext_product::price_val, 3, ext_product::type, ext_product::id>>()
                .from<ext_product>()
                .build_sql();
        expect(sql ==
               "SELECT id, NTH_VALUE(price_val, 3) OVER (PARTITION BY type ORDER BY id ASC) FROM ext_product"s)
            << sql;
    };

    "nth_value_over desc — NTH_VALUE(col, N) OVER (... ORDER BY col DESC)"_test = [] {
        auto const sql =
            select<nth_value_over<ext_product::price_val, 2, ext_product::type, ext_product::id, sort_order::desc>>()
                .from<ext_product>()
                .build_sql();
        expect(sql ==
               "SELECT NTH_VALUE(price_val, 2) OVER (PARTITION BY type ORDER BY id DESC) FROM ext_product"s)
            << sql;
    };

    "nth_value_over with frame clause"_test = [] {
        auto const sql =
            select<nth_value_over<ext_product::price_val, 1, ext_product::type, ext_product::id, sort_order::asc,
                                  rows_unbounded_preceding_to_current_row>>()
                .from<ext_product>()
                .build_sql();
        expect(sql ==
               "SELECT NTH_VALUE(price_val, 1) OVER (PARTITION BY type ORDER BY id ASC ROWS BETWEEN UNBOUNDED "
               "PRECEDING AND CURRENT ROW) FROM ext_product"s)
            << sql;
    };

    // Mixed — all 6 new functions in one query
    "all new window functions combined in one SELECT"_test = [] {
        auto const sql =
            select<ntile_over<4, ext_product::type, ext_product::id>,
                   percent_rank_over<ext_product::type, ext_product::id>,
                   cume_dist_over<ext_product::type, ext_product::id>,
                   first_value_over<ext_product::price_val, ext_product::type, ext_product::id>,
                   last_value_over<ext_product::price_val, ext_product::type, ext_product::id>,
                   nth_value_over<ext_product::price_val, 2, ext_product::type, ext_product::id>>()
                .from<ext_product>()
                .build_sql();
        expect(sql ==
               "SELECT NTILE(4) OVER (PARTITION BY type ORDER BY id ASC), "
               "PERCENT_RANK() OVER (PARTITION BY type ORDER BY id ASC), "
               "CUME_DIST() OVER (PARTITION BY type ORDER BY id ASC), "
               "FIRST_VALUE(price_val) OVER (PARTITION BY type ORDER BY id ASC), "
               "LAST_VALUE(price_val) OVER (PARTITION BY type ORDER BY id ASC), "
               "NTH_VALUE(price_val, 2) OVER (PARTITION BY type ORDER BY id ASC) "
               "FROM ext_product"s)
            << sql;
    };
};

// ===================================================================
// ORDER BY features
// ===================================================================

suite<"DQL ORDER BY Features"> dql_order_by_features_suite = [] {
    "order_by<count_all>() — ORDER BY COUNT(*)"_test = [] {
        auto const sql = select<ext_product::type, count_all>()
                             .from<ext_product>()
                             .group_by<ext_product::type>()
                             .order_by<count_all>()
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM ext_product GROUP BY type ORDER BY COUNT(*) ASC"s) << sql;
    };

    "order_by<count_all, desc>() — ORDER BY COUNT(*) DESC"_test = [] {
        auto const sql = select<ext_product::type, count_all>()
                             .from<ext_product>()
                             .group_by<ext_product::type>()
                             .order_by<count_all, sort_order::desc>()
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM ext_product GROUP BY type ORDER BY COUNT(*) DESC"s) << sql;
    };

    "order_by<sum<Col>, desc>() — ORDER BY SUM(col)"_test = [] {
        auto const sql = select<ext_product::type, sum<ext_product::price_val>>()
                             .from<ext_product>()
                             .group_by<ext_product::type>()
                             .order_by<sum<ext_product::price_val>, sort_order::desc>()
                             .build_sql();
        expect(sql == "SELECT type, SUM(price_val) FROM ext_product GROUP BY type ORDER BY SUM(price_val) DESC"s)
            << sql;
    };

    "order_by<Col>() — supports plain column projections"_test = [] {
        auto const sql = select<ext_product::id>().from<ext_product>().order_by<ext_product::id>().build_sql();
        expect(sql == "SELECT id FROM ext_product ORDER BY id ASC"s) << sql;
    };

    "order_by<Proj>() — supports non-aggregate projections"_test = [] {
        auto const sql =
            select<ext_product::id>().from<ext_product>().order_by<upper_of<ext_product::sku>>().build_sql();
        expect(sql == "SELECT id FROM ext_product ORDER BY UPPER(sku) ASC"s) << sql;
    };

    "order_by<Proj, desc>() — supports non-aggregate projections DESC"_test = [] {
        auto const sql = select<ext_product::id>()
                             .from<ext_product>()
                             .order_by<upper_of<ext_product::sku>, sort_order::desc>()
                             .build_sql();
        expect(sql == "SELECT id FROM ext_product ORDER BY UPPER(sku) DESC"s) << sql;
    };

    // order_by_alias
    "order_by_alias — emits alias when with_alias is set"_test = [] {
        auto const sql = select<ext_product::type, sum<ext_product::price_val>>()
                             .from<ext_product>()
                             .group_by<ext_product::type>()
                             .with_alias<sum<ext_product::price_val>>("total")
                             .order_by_alias<sum<ext_product::price_val>>()
                             .build_sql();
        expect(sql == "SELECT type, SUM(price_val) AS total FROM ext_product GROUP BY type ORDER BY total ASC"s) << sql;
    };

    "order_by_alias desc — emits alias DESC"_test = [] {
        auto const sql = select<ext_product::type, sum<ext_product::price_val>>()
                             .from<ext_product>()
                             .group_by<ext_product::type>()
                             .with_alias<sum<ext_product::price_val>>("revenue")
                             .order_by_alias<sum<ext_product::price_val>, sort_order::desc>()
                             .build_sql();
        expect(sql == "SELECT type, SUM(price_val) AS revenue FROM ext_product GROUP BY type ORDER BY revenue DESC"s)
            << sql;
    };

    "order_by_alias fallback — emits sql_expr() when no alias is set"_test = [] {
        auto const sql = select<ext_product::type, count_all>()
                             .from<ext_product>()
                             .group_by<ext_product::type>()
                             .order_by_alias<count_all, sort_order::desc>()
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM ext_product GROUP BY type ORDER BY COUNT(*) DESC"s) << sql;
    };

    // order_by<position<Proj>> — positional ORDER BY
    "order_by<position<Proj>> — emits 1-based column index"_test = [] {
        auto const sql = select<ext_product::type, count_all>()
                             .from<ext_product>()
                             .group_by<ext_product::type>()
                             .order_by<position<count_all>>()
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM ext_product GROUP BY type ORDER BY 2 ASC"s) << sql;
    };

    "order_by<position<Proj>, desc> — emits index DESC"_test = [] {
        auto const sql = select<ext_product::type, count_all>()
                             .from<ext_product>()
                             .group_by<ext_product::type>()
                             .order_by<position<count_all>, sort_order::desc>()
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM ext_product GROUP BY type ORDER BY 2 DESC"s) << sql;
    };

    "order_by<position<Proj>> first projection — emits index 1"_test = [] {
        auto const sql = select<ext_product::type, count_all>()
                             .from<ext_product>()
                             .order_by<position<ext_product::type>>()
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM ext_product ORDER BY 1 ASC"s) << sql;
    };

    // order_by<col_index<N>> — literal numeric positional ORDER BY
    "order_by<col_index<2>> — emits literal index 2 ASC"_test = [] {
        auto const sql = select<ext_product::type, count_all>()
                             .from<ext_product>()
                             .group_by<ext_product::type>()
                             .order_by<col_index<2>>()
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM ext_product GROUP BY type ORDER BY 2 ASC"s) << sql;
    };

    "order_by<col_index<1>, desc> — emits literal index 1 DESC"_test = [] {
        auto const sql = select<ext_product::type, count_all>()
                             .from<ext_product>()
                             .group_by<ext_product::type>()
                             .order_by<col_index<1>, sort_order::desc>()
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM ext_product GROUP BY type ORDER BY 1 DESC"s) << sql;
    };

    "order_by<col_index<N>> chained — emits multiple positional clauses"_test = [] {
        auto const sql = select<ext_product::type, count_all>()
                             .from<ext_product>()
                             .group_by<ext_product::type>()
                             .order_by<col_index<1>>()
                             .order_by<col_index<2>, sort_order::desc>()
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM ext_product GROUP BY type ORDER BY 1 ASC, 2 DESC"s) << sql;
    };

    // order_by<rand_val> (random ordering)
    "order_by<rand_val>() — ORDER BY RAND()"_test = [] {
        auto const sql = select<ext_product::id>().from<ext_product>().order_by<rand_val>().build_sql();
        expect(sql == "SELECT id FROM ext_product ORDER BY RAND() ASC"s) << sql;
    };

    // order_by<nulls_last<Col>>
    "order_by<nulls_last<Col>> — NULLs sorted after non-NULLs"_test = [] {
        auto const sql =
            select<ext_product::id>().from<ext_product>().order_by<nulls_last<ext_product::name>>().build_sql();
        expect(sql == "SELECT id FROM ext_product ORDER BY (name IS NULL) ASC, name ASC"s) << sql;
    };

    "order_by<nulls_last<Col>, desc> — col sorted DESC, NULLs last"_test = [] {
        auto const sql = select<ext_product::id>()
                             .from<ext_product>()
                             .order_by<nulls_last<ext_product::name>, sort_order::desc>()
                             .build_sql();
        expect(sql == "SELECT id FROM ext_product ORDER BY (name IS NULL) ASC, name DESC"s) << sql;
    };

    // order_by<nulls_first<Col>>
    "order_by<nulls_first<Col>> — NULLs sorted before non-NULLs"_test = [] {
        auto const sql =
            select<ext_product::id>().from<ext_product>().order_by<nulls_first<ext_product::name>>().build_sql();
        expect(sql == "SELECT id FROM ext_product ORDER BY (name IS NULL) DESC, name ASC"s) << sql;
    };

    "order_by<nulls_first<Col>, desc> — col sorted DESC, NULLs first"_test = [] {
        auto const sql = select<ext_product::id>()
                             .from<ext_product>()
                             .order_by<nulls_first<ext_product::name>, sort_order::desc>()
                             .build_sql();
        expect(sql == "SELECT id FROM ext_product ORDER BY (name IS NULL) DESC, name DESC"s) << sql;
    };

    // chaining
    "order_by<nulls_last<Col>> chained with order_by<Col> — combined ORDER BY"_test = [] {
        auto const sql = select<ext_product::id>()
                             .from<ext_product>()
                             .order_by<ext_product::type>()
                             .order_by<nulls_last<ext_product::name>>()
                             .build_sql();
        expect(sql == "SELECT id FROM ext_product ORDER BY type ASC, (name IS NULL) ASC, name ASC"s) << sql;
    };

    // order_by(case_when_builder) — case 4
    "order_by(case_when) — ORDER BY CASE WHEN ... END ASC"_test = [] {
        auto const expr =
            case_when(equal<ext_product::type>("widget"), 0).when(equal<ext_product::type>("gadget"), 1).else_(2);
        auto const sql = select<ext_product::id>().from<ext_product>().order_by(expr).build_sql();
        expect(
            sql ==
            "SELECT id FROM ext_product ORDER BY CASE WHEN type = 'widget' THEN 0 WHEN type = 'gadget' THEN 1 ELSE 2 END ASC"s)
            << sql;
    };

    "order_by(case_when, desc) — ORDER BY CASE WHEN ... END DESC"_test = [] {
        auto const expr = case_when(equal<ext_product::type>("widget"), 0).else_(1);
        auto const sql = select<ext_product::id>().from<ext_product>().order_by(expr, sort_order::desc).build_sql();
        expect(sql == "SELECT id FROM ext_product ORDER BY CASE WHEN type = 'widget' THEN 0 ELSE 1 END DESC"s) << sql;
    };

    // order_by<field<Col>>(...) — case 4 (FIELD custom ordering)
    "order_by<field<Col>>(...) — ORDER BY FIELD(col, ...) ASC"_test = [] {
        auto const sql =
            select<ext_product::id>()
                .from<ext_product>()
                .order_by<field<ext_product::type>>(
                    {ext_product::type{"electronics"}, ext_product::type{"clothing"}, ext_product::type{"food"}})
                .build_sql();
        expect(sql == "SELECT id FROM ext_product ORDER BY FIELD(type, 'electronics', 'clothing', 'food') ASC"s) << sql;
    };

    "order_by<field<Col>>(...) desc — ORDER BY FIELD(col, ...) DESC"_test = [] {
        auto const sql = select<ext_product::id>()
                             .from<ext_product>()
                             .order_by<field<ext_product::type>>(
                                 {ext_product::type{"electronics"}, ext_product::type{"clothing"}}, sort_order::desc)
                             .build_sql();
        expect(sql == "SELECT id FROM ext_product ORDER BY FIELD(type, 'electronics', 'clothing') DESC"s) << sql;
    };

    // order_by<qual<Col>> — case 7 (qualified join column)
    "order_by<qual<col<T,I>>> — qualified table.column for JOIN ORDER BY"_test = [] {
        auto const sql = select<ext_product::sku, ext_category::label>()
                             .from<joined<ext_product>>()
                             .inner_join<ext_category, ext_product::category_id, ext_category::id>()
                             .order_by<qual<col<ext_category, 1>>>()
                             .build_sql();
        expect(
            sql ==
            "SELECT sku, label FROM ext_product INNER JOIN ext_category ON category_id = id ORDER BY ext_category.label ASC"s)
            << sql;
    };

    "order_by<qual<col<T,I>>, desc> — qualified column DESC"_test = [] {
        auto const sql = select<ext_product::sku>()
                             .from<joined<ext_product>>()
                             .inner_join<ext_category, ext_product::category_id, ext_category::id>()
                             .order_by<qual<col<ext_category, 1>>, sort_order::desc>()
                             .build_sql();
        expect(
            sql ==
            "SELECT sku FROM ext_product INNER JOIN ext_category ON category_id = id ORDER BY ext_category.label DESC"s)
            << sql;
    };

    "order_by<nulls_last<qual<col<T,I>>>> — NULL handling supports qualified columns"_test = [] {
        auto const sql = select<ext_product::sku>()
                             .from<joined<ext_product>>()
                             .inner_join<ext_category, ext_product::category_id, ext_category::id>()
                             .order_by<nulls_last<qual<col<ext_category, 1>>>>()
                             .build_sql();
        expect(sql ==
               "SELECT sku FROM ext_product INNER JOIN ext_category ON category_id = id ORDER BY (ext_category.label "
               "IS NULL) ASC, "
               "ext_category.label ASC"s)
            << sql;
    };

    "order_by<nulls_first<qual<col<T,I>>>, desc> — qualified NULL handling with DESC"_test = [] {
        auto const sql = select<ext_product::sku>()
                             .from<joined<ext_product>>()
                             .inner_join<ext_category, ext_product::category_id, ext_category::id>()
                             .order_by<nulls_first<qual<col<ext_category, 1>>>, sort_order::desc>()
                             .build_sql();
        expect(sql ==
               "SELECT sku FROM ext_product INNER JOIN ext_category ON category_id = id ORDER BY (ext_category.label "
               "IS NULL) DESC, "
               "ext_category.label DESC"s)
            << sql;
    };
};

// ===================================================================
// GROUP BY WITH ROLLUP
// ===================================================================

suite<"DQL GROUP BY ROLLUP"> dql_group_by_rollup_suite = [] {
    "group_by<rollup<Col>> — GROUP BY col WITH ROLLUP"_test = [] {
        auto const sql = select<ext_product::type, count_all>()
                             .from<ext_product>()
                             .group_by<rollup<ext_product::type>>()
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM ext_product GROUP BY type WITH ROLLUP"s) << sql;
    };

    "group_by<rollup<Col1, Col2>> — GROUP BY col1, col2 WITH ROLLUP"_test = [] {
        auto const sql = select<ext_product::type, ext_product::sku, count_all>()
                             .from<ext_product>()
                             .group_by<rollup<ext_product::type, ext_product::sku>>()
                             .build_sql();
        expect(sql == "SELECT type, sku, COUNT(*) FROM ext_product GROUP BY type, sku WITH ROLLUP"s) << sql;
    };

    "group_by<rollup<Col>> with HAVING"_test = [] {
        auto const sql = select<ext_product::type, count_all>()
                             .from<ext_product>()
                             .group_by<rollup<ext_product::type>>()
                             .having(count() > 2u)
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM ext_product GROUP BY type WITH ROLLUP HAVING COUNT(*) > 2"s) << sql;
    };

    "group_by<cube<Col1, Col2>> — GROUP BY CUBE(...)"_test = [] {
        auto const sql = select<ext_product::type, ext_product::sku, count_all>()
                             .from<ext_product>()
                             .group_by<cube<ext_product::type, ext_product::sku>>()
                             .build_sql();
        expect(sql == "SELECT type, sku, COUNT(*) FROM ext_product GROUP BY CUBE(type, sku)"s) << sql;
    };

    "group_by<grouping_sets<...>> — GROUPING SETS clause"_test = [] {
        auto const sql =
            select<ext_product::type, ext_product::sku, count_all>()
                .from<ext_product>()
                .group_by<
                    grouping_sets<grouping_set<ext_product::type>, grouping_set<ext_product::sku>, grouping_set<>>>()
                .build_sql();
        expect(sql == "SELECT type, sku, COUNT(*) FROM ext_product GROUP BY GROUPING SETS ((type), (sku), ())"s) << sql;
    };
};

// ===================================================================
// JOIN features — FULL OUTER JOIN, CROSS JOIN, compound ON
// ===================================================================

suite<"DQL JOIN Features"> dql_join_features_suite = [] {
    "full_join — generates FULL OUTER JOIN ON"_test = [] {
        auto const sql = select<ext_product::sku, ext_category::label>()
                             .from<joined<ext_product>>()
                             .full_join<ext_category, ext_product::category_id, ext_category::id>()
                             .build_sql();
        expect(sql == "SELECT sku, label FROM ext_product FULL OUTER JOIN ext_category ON category_id = id"s) << sql;
    };

    "cross_join — generates CROSS JOIN (no ON)"_test = [] {
        auto const sql = select<ext_product::sku, ext_category::label>()
                             .from<joined<ext_product>>()
                             .cross_join<ext_category>()
                             .build_sql();
        expect(sql == "SELECT sku, label FROM ext_product CROSS JOIN ext_category"s) << sql;
    };

    "inner_join_on with compound ON — generates INNER JOIN ... ON (a AND b)"_test = [] {
        auto const on = (col_ref<ext_product::category_id> == 1u) & is_not_null<ext_product::category_id>();
        auto const sql = select<ext_product::sku, ext_category::label>()
                             .from<joined<ext_product>>()
                             .inner_join_on<ext_category>(std::move(on))
                             .build_sql();
        expect(
            sql ==
            "SELECT sku, label FROM ext_product INNER JOIN ext_category ON (category_id = 1 AND category_id IS NOT NULL)"s)
            << sql;
    };

    "left_join_on — generates LEFT JOIN ... ON compound"_test = [] {
        auto const on = col_ref<ext_product::category_id> == 2u;
        auto const sql = select<ext_product::sku>()
                             .from<joined<ext_product>>()
                             .left_join_on<ext_category>(std::move(on))
                             .build_sql();
        expect(sql == "SELECT sku FROM ext_product LEFT JOIN ext_category ON category_id = 2"s) << sql;
    };

    "right_join_on — generates RIGHT JOIN ... ON compound"_test = [] {
        auto const on = col_ref<ext_product::category_id> == 3u;
        auto const sql = select<ext_product::sku>()
                             .from<joined<ext_product>>()
                             .right_join_on<ext_category>(std::move(on))
                             .build_sql();
        expect(sql == "SELECT sku FROM ext_product RIGHT JOIN ext_category ON category_id = 3"s) << sql;
    };

    "full_join_on — generates FULL OUTER JOIN ... ON compound"_test = [] {
        auto const on = col_ref<ext_product::category_id> == 4u;
        auto const sql = select<ext_product::sku>()
                             .from<joined<ext_product>>()
                             .full_join_on<ext_category>(std::move(on))
                             .build_sql();
        expect(sql == "SELECT sku FROM ext_product FULL OUTER JOIN ext_category ON category_id = 4"s) << sql;
    };
};

// ===================================================================
// Derived table (subquery in FROM)
// ===================================================================

suite<"DQL Derived Table"> dql_derived_table_suite = [] {
    "from(subquery, alias) — SELECT FROM (SELECT ...) AS t"_test = [] {
        auto const sub = select<ext_product::type, count_all>().from<ext_product>().group_by<ext_product::type>();
        auto const sql = select<ext_product::type, count_all>().from(sub, "t").build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM (SELECT type, COUNT(*) FROM ext_product GROUP BY type) AS t"s) << sql;
    };

    "from(subquery, alias) with WHERE on outer query"_test = [] {
        auto const sub = select<ext_product::type>().from<ext_product>().where(is_not_null<ext_product::name>());
        auto const sql = select<ext_product::type>()
                             .from(sub, "filtered")
                             .where(equal<ext_product::type>(ext_product::type{"widget"}))
                             .build_sql();
        expect(
            sql ==
            "SELECT type FROM (SELECT type FROM ext_product WHERE name IS NOT NULL) AS filtered WHERE type = 'widget'"s)
            << sql;
    };
};

// ===================================================================
// CTE (WITH clause)
// ===================================================================

suite<"DQL CTE"> dql_cte_suite = [] {
    "with_cte single CTE — WITH name AS (q) main_select"_test = [] {
        auto const cte_q = select<ext_product::type, count_all>().from<ext_product>().group_by<ext_product::type>();
        auto const main_q = select<ext_product::type>().from<ext_product>();
        auto const cte = with_cte("counts", cte_q);
        auto const sql = cte.build_sql(main_q);
        expect(sql ==
               "WITH counts AS (SELECT type, COUNT(*) FROM ext_product GROUP BY type) SELECT type FROM ext_product"s)
            << sql;
    };

    "with_cte two CTEs — WITH cte1 AS (...), cte2 AS (...) SELECT ..."_test = [] {
        auto const cte1 = select<ext_product::type, count_all>().from<ext_product>().group_by<ext_product::type>();
        auto const cte2 = select<ext_product::sku>().from<ext_product>().where(is_not_null<ext_product::name>());
        auto const main = select<ext_product::id>().from<ext_product>();
        auto const cte = with_cte("counts", cte1).with_cte("skus", cte2);
        auto const sql = cte.build_sql(main);
        expect(sql ==
               "WITH counts AS (SELECT type, COUNT(*) FROM ext_product GROUP BY type), "
               "skus AS (SELECT sku FROM ext_product WHERE name IS NOT NULL) "
               "SELECT id FROM ext_product"s)
            << sql;
    };

    "with_recursive_cte — emits WITH RECURSIVE"_test = [] {
        auto const seed =
            select<ext_product::id>().from<ext_product>().where(equal<ext_product::id>(ext_product::id{1u}));
        auto const main = select<ext_product::id>().from<ext_product>();
        auto const sql = with_recursive_cte("seed_ids", seed).build_sql(main);
        expect(sql ==
               "WITH RECURSIVE seed_ids AS (SELECT id FROM ext_product WHERE id = 1) SELECT id FROM ext_product"s)
            << sql;
    };
};

suite<"DQL Set Operators"> dql_set_operators_suite = [] {
    "intersect_ — combines two queries with INTERSECT"_test = [] {
        auto const q1 =
            select<ext_product::id>().from<ext_product>().where(equal<ext_product::type>(ext_product::type{"gadget"}));
        auto const q2 =
            select<ext_product::id>().from<ext_product>().where(equal<ext_product::type>(ext_product::type{"widget"}));
        auto const sql = intersect_(q1, q2).build_sql();
        expect(sql ==
               "(SELECT id FROM ext_product WHERE type = 'gadget') INTERSECT (SELECT id FROM ext_product WHERE type = "
               "'widget')"s)
            << sql;
    };

    "except_ — combines two queries with EXCEPT"_test = [] {
        auto const q1 =
            select<ext_product::id>().from<ext_product>().where(equal<ext_product::type>(ext_product::type{"gadget"}));
        auto const q2 =
            select<ext_product::id>().from<ext_product>().where(equal<ext_product::type>(ext_product::type{"widget"}));
        auto const sql = except_(q1, q2).build_sql();
        expect(sql ==
               "(SELECT id FROM ext_product WHERE type = 'gadget') EXCEPT (SELECT id FROM ext_product WHERE type = "
               "'widget')"s)
            << sql;
    };
};

// ===================================================================
// DML features — INSERT IGNORE, INSERT...SELECT, REPLACE INTO, UPDATE JOIN
// ===================================================================

suite<"DML Features"> dml_features_suite = [] {
    "insert_ignore_into single row — generates INSERT IGNORE INTO SQL"_test = [] {
        ext_product p;
        p.id_ = 1u;
        p.category_id_ = std::nullopt;
        p.sku_ = "WIDGET";
        p.type_ = "gadget";
        p.name_ = std::nullopt;
        p.price_val_ = 9.99;

        auto const sql = insert_ignore_into<ext_product>().values(p).build_sql();
        expect(sql.substr(0, 19) == "INSERT IGNORE INTO "s) << sql.substr(0, 19);
        expect(
            sql ==
            "INSERT IGNORE INTO ext_product (id, category_id, sku, type, name, price_val) VALUES (1, NULL, 'WIDGET', "
            "'gadget', NULL, 9.990000)")
            << sql;
    };

    "replace_into single row — generates REPLACE INTO SQL"_test = [] {
        ext_product p;
        p.id_ = 1u;
        p.category_id_ = std::nullopt;
        p.sku_ = "WIDGET";
        p.type_ = "gadget";
        p.name_ = std::nullopt;
        p.price_val_ = 9.99;

        auto const sql = replace_into<ext_product>().values(p).build_sql();
        expect(sql.substr(0, 13) == "REPLACE INTO "s) << sql.substr(0, 13);
        expect(sql ==
               "REPLACE INTO ext_product (id, category_id, sku, type, name, price_val) VALUES (1, NULL, 'WIDGET', "
               "'gadget', NULL, 9.990000)")
            << sql;
    };

    "replace_into contains table name and values"_test = [] {
        ext_product p;
        p.id_ = 2u;
        p.category_id_ = 3u;
        p.sku_ = "SKU2";
        p.type_ = "type2";
        p.name_ = std::nullopt;
        p.price_val_ = 19.99;

        auto const sql = replace_into<ext_product>().values(p).build_sql();
        expect(sql ==
               "REPLACE INTO ext_product (id, category_id, sku, type, name, price_val) VALUES (2, 3, 'SKU2', 'type2', "
               "NULL, 19.990000)")
            << sql;
    };

    "insert_into_select — generates INSERT INTO ... SELECT SQL"_test = [] {
        auto const select_q = select<ext_product::id, ext_product::category_id, ext_product::sku, ext_product::type,
                                     ext_product::name, ext_product::price_val>()
                                  .from<ext_product>()
                                  .where(equal<ext_product::type>(ext_product::type{"widget"}));
        auto const sql = insert_into_select<ext_product>(select_q).build_sql();
        expect(
            sql ==
            "INSERT INTO ext_product (id, category_id, sku, type, name, price_val) SELECT id, category_id, sku, type, "
            "name, price_val FROM ext_product WHERE type = 'widget'")
            << sql;
    };

    "update_join — generates UPDATE T1 JOIN T2 ON ... SET ..."_test = [] {
        auto const sql = update_join<ext_product, ext_category, ext_product::category_id, ext_category::id>()
                             .set(ext_product::sku{"UPDATED"})
                             .build_sql();
        expect(sql == "UPDATE ext_product INNER JOIN ext_category ON category_id = id SET sku = 'UPDATED'") << sql;
    };

    "update_join with WHERE — generates UPDATE JOIN WHERE SQL"_test = [] {
        auto const sql = update_join<ext_product, ext_category, ext_product::category_id, ext_category::id>()
                             .set(ext_product::sku{"NEW_SKU"})
                             .where(equal<ext_product::id>(ext_product::id{42u}))
                             .build_sql();
        expect(sql ==
               "UPDATE ext_product INNER JOIN ext_category ON category_id = id SET sku = 'NEW_SKU' WHERE id = 42")
            << sql;
    };

    "update_join set multiple columns — generates comma-separated SET"_test = [] {
        auto const sql = update_join<ext_product, ext_category, ext_product::category_id, ext_category::id>()
                             .set(ext_product::sku{"SKU1"}, ext_product::type{"gadget"})
                             .build_sql();
        expect(sql ==
               "UPDATE ext_product INNER JOIN ext_category ON category_id = id SET sku = 'SKU1', type = 'gadget'")
            << sql;
    };

    "insert_ignore_into bulk — generates INSERT IGNORE INTO ... VALUES (row1), (row2)"_test = [] {
        ext_product p1;
        p1.id_ = 1u;
        p1.sku_ = "SKU1";
        p1.type_ = "type1";
        p1.price_val_ = 1.0;
        ext_product p2;
        p2.id_ = 2u;
        p2.sku_ = "SKU2";
        p2.type_ = "type2";
        p2.price_val_ = 2.0;
        auto const sql = insert_ignore_into<ext_product>().values(std::initializer_list{p1, p2}).build_sql();
        expect(sql ==
               "INSERT IGNORE INTO ext_product (id, category_id, sku, type, name, price_val) VALUES (1, NULL, 'SKU1', "
               "'type1', NULL, 1.000000), (2, NULL, 'SKU2', 'type2', NULL, 2.000000)")
            << sql;
    };

    "replace_into bulk — generates REPLACE INTO ... VALUES (row1), (row2)"_test = [] {
        ext_product p1;
        p1.id_ = 1u;
        p1.sku_ = "RSKU1";
        p1.type_ = "rtype1";
        p1.price_val_ = 10.0;
        ext_product p2;
        p2.id_ = 2u;
        p2.sku_ = "RSKU2";
        p2.type_ = "rtype2";
        p2.price_val_ = 20.0;
        auto const sql = replace_into<ext_product>().values(std::initializer_list<ext_product>{p1, p2}).build_sql();
        expect(sql ==
               "REPLACE INTO ext_product (id, category_id, sku, type, name, price_val) VALUES (1, NULL, 'RSKU1', "
               "'rtype1', "
               "NULL, 10.000000), (2, NULL, 'RSKU2', 'rtype2', NULL, 20.000000)")
            << sql;
    };

    "insert_ignore_into empty vector — returns column list with empty values"_test = [] {
        auto const sql = insert_ignore_into<ext_product>().values(std::initializer_list<ext_product>{}).build_sql();
        expect(sql == "INSERT IGNORE INTO ext_product (id, category_id, sku, type, name, price_val) VALUES ()") << sql;
    };

    "replace_into empty vector — returns column list with empty values"_test = [] {
        auto const sql = replace_into<ext_product>().values(std::initializer_list<ext_product>{}).build_sql();
        expect(sql == "REPLACE INTO ext_product (id, category_id, sku, type, name, price_val) VALUES ()") << sql;
    };
};

// ===================================================================
// NOT REGEXP / RLIKE / NOT RLIKE
// ===================================================================

suite<"DQL NOT REGEXP / RLIKE Predicates"> dql_not_regexp_rlike_suite = [] {
    "not_regexp<Col>(pattern) — free function generates NOT REGEXP SQL"_test = [] {
        auto const cond = not_regexp<ext_product::sku>("^A.*");
        expect(cond.build_sql() == "sku NOT REGEXP '^A.*'"s) << cond.build_sql();
    };

    "col_ref.not_regexp() — method on col_expr"_test = [] {
        auto const cond = col_ref<ext_product::sku>.not_regexp("^WIDGET");
        expect(cond.build_sql() == "sku NOT REGEXP '^WIDGET'"s) << cond.build_sql();
    };

    "not_regexp in WHERE clause"_test = [] {
        auto const sql = select<ext_product::id, ext_product::sku>()
                             .from<ext_product>()
                             .where(not_regexp<ext_product::sku>("^[A-Z]"))
                             .build_sql();
        expect(sql == "SELECT id, sku FROM ext_product WHERE sku NOT REGEXP '^[A-Z]'"s) << sql;
    };

    "not_regexp escapes single-quote in pattern"_test = [] {
        auto const cond = not_regexp<ext_product::sku>("it's");
        expect(cond.build_sql() == "sku NOT REGEXP 'it''s'"s) << cond.build_sql();
    };

    "rlike<Col>(pattern) — generates REGEXP SQL (MySQL synonym)"_test = [] {
        auto const cond = rlike<ext_product::sku>("^RLIKE");
        expect(cond.build_sql() == "sku REGEXP '^RLIKE'"s) << cond.build_sql();
    };

    "col_ref.rlike() — method on col_expr"_test = [] {
        auto const cond = col_ref<ext_product::sku>.rlike("^R");
        expect(cond.build_sql() == "sku REGEXP '^R'"s) << cond.build_sql();
    };

    "not_rlike<Col>(pattern) — generates NOT REGEXP SQL"_test = [] {
        auto const cond = not_rlike<ext_product::sku>("^X");
        expect(cond.build_sql() == "sku NOT REGEXP '^X'"s) << cond.build_sql();
    };

    "col_ref.not_rlike() — method on col_expr"_test = [] {
        auto const cond = col_ref<ext_product::sku>.not_rlike("^X");
        expect(cond.build_sql() == "sku NOT REGEXP '^X'"s) << cond.build_sql();
    };

    "mysql_not_regexp alias — generates NOT REGEXP SQL"_test = [] {
        auto const cond = mysql_not_regexp<ext_product::sku>("^A");
        expect(cond.build_sql() == "sku NOT REGEXP '^A'"s) << cond.build_sql();
    };

    "mysql_rlike alias — generates REGEXP SQL"_test = [] {
        auto const cond = mysql_rlike<ext_product::sku>("^M");
        expect(cond.build_sql() == "sku REGEXP '^M'"s) << cond.build_sql();
    };

    "mysql_not_rlike alias — generates NOT REGEXP SQL"_test = [] {
        auto const cond = mysql_not_rlike<ext_product::sku>("^M");
        expect(cond.build_sql() == "sku NOT REGEXP '^M'"s) << cond.build_sql();
    };
};

// ===================================================================
// SOUNDS LIKE
// ===================================================================

suite<"DQL SOUNDS LIKE Predicate"> dql_sounds_like_suite = [] {
    "sounds_like<Col>(word) — free function generates SOUNDS LIKE SQL"_test = [] {
        auto const cond = sounds_like<ext_product::sku>("widget");
        expect(cond.build_sql() == "sku SOUNDS LIKE 'widget'"s) << cond.build_sql();
    };

    "col_ref.sounds_like() — method on col_expr"_test = [] {
        auto const cond = col_ref<ext_product::sku>.sounds_like("widget");
        expect(cond.build_sql() == "sku SOUNDS LIKE 'widget'"s) << cond.build_sql();
    };

    "sounds_like in WHERE clause"_test = [] {
        auto const sql = select<ext_product::id, ext_product::sku>()
                             .from<ext_product>()
                             .where(sounds_like<ext_product::sku>("widget"))
                             .build_sql();
        expect(sql == "SELECT id, sku FROM ext_product WHERE sku SOUNDS LIKE 'widget'"s) << sql;
    };

    "sounds_like escapes single-quote in word"_test = [] {
        auto const cond = sounds_like<ext_product::sku>("it's");
        expect(cond.build_sql() == "sku SOUNDS LIKE 'it''s'"s) << cond.build_sql();
    };
};

// ===================================================================
// MATCH ... AGAINST
// ===================================================================

suite<"DQL MATCH AGAINST Predicate"> dql_match_against_suite = [] {
    "match_against single column — default natural language mode"_test = [] {
        auto const cond = match_against<ext_product::sku>("widget");
        expect(cond.build_sql() == "MATCH(sku) AGAINST ('widget')"s) << cond.build_sql();
    };

    "match_against multiple columns — default natural language mode"_test = [] {
        auto const cond = match_against<ext_product::sku, ext_product::type>("blue widget");
        expect(cond.build_sql() == "MATCH(sku, type) AGAINST ('blue widget')"s) << cond.build_sql();
    };

    "match_against IN BOOLEAN MODE"_test = [] {
        auto const cond = match_against<ext_product::sku>("widget", match_search_modifier::boolean_mode);
        expect(cond.build_sql() == "MATCH(sku) AGAINST ('widget' IN BOOLEAN MODE)"s) << cond.build_sql();
    };

    "match_against IN NATURAL LANGUAGE MODE — explicit modifier"_test = [] {
        auto const cond = match_against<ext_product::sku>("widget", match_search_modifier::natural_language);
        expect(cond.build_sql() == "MATCH(sku) AGAINST ('widget')"s) << cond.build_sql();
    };

    "match_against WITH QUERY EXPANSION"_test = [] {
        auto const cond = match_against<ext_product::sku>("widget", match_search_modifier::query_expansion);
        expect(cond.build_sql() == "MATCH(sku) AGAINST ('widget' WITH QUERY EXPANSION)"s) << cond.build_sql();
    };

    "match_against IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION"_test = [] {
        auto const cond =
            match_against<ext_product::sku>("widget", match_search_modifier::natural_language_with_query_expansion);
        expect(cond.build_sql() == "MATCH(sku) AGAINST ('widget' IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION)"s)
            << cond.build_sql();
    };

    "match_against escapes single-quote in expression"_test = [] {
        auto const cond = match_against<ext_product::sku>("it's");
        expect(cond.build_sql() == "MATCH(sku) AGAINST ('it''s')"s) << cond.build_sql();
    };

    "match_against boolean mode in WHERE clause"_test = [] {
        auto const sql = select<ext_product::id, ext_product::sku>()
                             .from<ext_product>()
                             .where(match_against<ext_product::sku, ext_product::type>("+widget -cheap",
                                                                                       match_search_modifier::boolean_mode))
                             .build_sql();
        expect(sql ==
               "SELECT id, sku FROM ext_product WHERE MATCH(sku, type) AGAINST ('+widget -cheap' IN BOOLEAN MODE)"s)
            << sql;
    };
};
