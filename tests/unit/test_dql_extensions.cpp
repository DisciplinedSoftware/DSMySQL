#include <boost/ut.hpp>
#include <string>
#include <tuple>
#include <type_traits>

#include "ds_mysql/sql.hpp"
#include "ds_mysql/sql_extension.hpp"
#include "ds_mysql/varchar_field.hpp"

using namespace boost::ut;
using namespace ds_mysql;
using namespace std::string_literals;

namespace {

// Test schema reused across all suites
struct product {
    using id          = column_field<"id",          uint32_t>;
    using category_id = column_field<"category_id", std::optional<uint32_t>>;
    using sku         = column_field<"sku",         varchar_field<32>>;
    using type        = column_field<"type",        varchar_field<64>>;
    using name        = column_field<"name",        std::optional<varchar_field<255>>>;
    using price_val   = column_field<"price_val",   double>;

    id          id_;
    category_id category_id_;
    sku         sku_;
    type        type_;
    name        name_;
    price_val   price_val_;
};

struct category {
    using id    = column_field<"id",    uint32_t>;
    using label = column_field<"label", varchar_field<64>>;
    id    id_;
    label label_;
};

struct event_log {
    using id         = column_field<"event_id",   uint32_t>;
    using created_at = column_field<"created_at", sql_datetime>;

    id         id_;
    created_at created_at_;
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
        auto const sql = select<product::type, count_all>()
                             .from<product>()
                             .group_by<product::type>()
                             .having(count().between(2u, 50u))
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM product GROUP BY type HAVING COUNT(*) BETWEEN 2 AND 50"s) << sql;
    };

    "count().in({1,2,3}) — generates IN HAVING"_test = [] {
        auto const cond = count().in({1u, 2u, 3u});
        expect(cond.build_sql() == "COUNT(*) IN (1, 2, 3)"s) << cond.build_sql();
    };

    "count().in in HAVING clause"_test = [] {
        auto const sql = select<product::type, count_all>()
                             .from<product>()
                             .group_by<product::type>()
                             .having(count().in({1u, 5u, 10u}))
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM product GROUP BY type HAVING COUNT(*) IN (1, 5, 10)"s) << sql;
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
        auto const cond = agg_expr<sum<product::price_val>>{}.between(0.0, 1000.0);
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
        auto const cond = null_safe_equal<product::id>(42u);
        expect(cond.build_sql() == "id <=> 42"s) << cond.build_sql();
    };

    "null_safe_equal with optional column and NULL — uses NULL literal"_test = [] {
        auto const cond = null_safe_equal<product::category_id>(std::nullopt);
        expect(cond.build_sql() == "category_id <=> NULL"s) << cond.build_sql();
    };

    "col_ref.null_safe_equal() — operator method on col_expr"_test = [] {
        auto const cond = col_ref<product::id>.null_safe_equal(42u);
        expect(cond.build_sql() == "id <=> 42"s) << cond.build_sql();
    };

    "null_safe_equal in WHERE clause"_test = [] {
        auto const sql = select<product::id>().from<product>().where(null_safe_equal<product::id>(1u)).build_sql();
        expect(sql == "SELECT id FROM product WHERE id <=> 1"s) << sql;
    };

    "mysql_null_safe_equal<Col>(value) — extension alias generates <=> SQL"_test = [] {
        auto const cond = mysql_null_safe_equal<product::category_id>(std::nullopt);
        expect(cond.build_sql() == "category_id <=> NULL"s) << cond.build_sql();
    };
};

// ===================================================================
// REGEXP / RLIKE
// ===================================================================

suite<"DQL REGEXP Predicate"> dql_regexp_predicate_suite = [] {
    "regexp<Col>(pattern) — free function generates REGEXP SQL"_test = [] {
        auto const cond = regexp<product::sku>("^A.*");
        expect(cond.build_sql() == "sku REGEXP '^A.*'"s) << cond.build_sql();
    };

    "col_ref.regexp() — method on col_expr"_test = [] {
        auto const cond = col_ref<product::sku>.regexp("^WIDGET");
        expect(cond.build_sql() == "sku REGEXP '^WIDGET'"s) << cond.build_sql();
    };

    "regexp in WHERE clause"_test = [] {
        auto const sql =
            select<product::id, product::sku>().from<product>().where(regexp<product::sku>("^[A-Z]")).build_sql();
        expect(sql == "SELECT id, sku FROM product WHERE sku REGEXP '^[A-Z]'"s) << sql;
    };

    "regexp escapes single-quote in pattern"_test = [] {
        auto const cond = regexp<product::sku>("it's");
        expect(cond.build_sql() == "sku REGEXP 'it''s'"s) << cond.build_sql();
    };

    "mysql_regexp<Col>(pattern) — extension alias generates REGEXP SQL"_test = [] {
        auto const cond = mysql_regexp<product::sku>("^MYSQL");
        expect(cond.build_sql() == "sku REGEXP '^MYSQL'"s) << cond.build_sql();
    };
};

// ===================================================================
// Scalar subquery comparison methods on col_expr
// ===================================================================

suite<"DQL Scalar Subquery Methods"> dql_scalar_subquery_suite = [] {
    "col_ref.eq_subquery — col = (SELECT ...)"_test = [] {
        auto const sub = select<max_of<product::price_val>>().from<product>();
        auto const cond = col_ref<product::price_val>.eq_subquery(sub);
        expect(cond.build_sql() == "price_val = (SELECT MAX(price_val) FROM product)"s) << cond.build_sql();
    };

    "col_ref.ne_subquery — col != (SELECT ...)"_test = [] {
        auto const sub = select<min_of<product::id>>().from<product>();
        auto const cond = col_ref<product::id>.ne_subquery(sub);
        expect(cond.build_sql() == "id != (SELECT MIN(id) FROM product)"s) << cond.build_sql();
    };

    "col_ref.gt_subquery — col > (SELECT ...)"_test = [] {
        auto const sub = select<avg<product::price_val>>().from<product>();
        auto const cond = col_ref<product::price_val>.gt_subquery(sub);
        expect(cond.build_sql() == "price_val > (SELECT AVG(price_val) FROM product)"s) << cond.build_sql();
    };

    "col_ref.lt_subquery — col < (SELECT ...)"_test = [] {
        auto const sub = select<avg<product::price_val>>().from<product>();
        auto const cond = col_ref<product::price_val>.lt_subquery(sub);
        expect(cond.build_sql() == "price_val < (SELECT AVG(price_val) FROM product)"s) << cond.build_sql();
    };

    "col_ref.ge_subquery — col >= (SELECT ...)"_test = [] {
        auto const sub = select<min_of<product::price_val>>().from<product>();
        auto const cond = col_ref<product::price_val>.ge_subquery(sub);
        expect(cond.build_sql() == "price_val >= (SELECT MIN(price_val) FROM product)"s) << cond.build_sql();
    };

    "col_ref.le_subquery — col <= (SELECT ...)"_test = [] {
        auto const sub = select<max_of<product::price_val>>().from<product>();
        auto const cond = col_ref<product::price_val>.le_subquery(sub);
        expect(cond.build_sql() == "price_val <= (SELECT MAX(price_val) FROM product)"s) << cond.build_sql();
    };

    "col_ref.in_subquery — col IN (SELECT ...)"_test = [] {
        auto const sub = select<product::id>().from<product>().where(equal<product::type>(product::type{"widget"}));
        auto const cond = col_ref<product::category_id>.in_subquery(sub);
        expect(cond.build_sql() == "category_id IN (SELECT id FROM product WHERE type = 'widget')"s)
            << cond.build_sql();
    };

    "col_ref.not_in_subquery — col NOT IN (SELECT ...)"_test = [] {
        auto const sub = select<product::id>().from<product>().where(equal<product::type>(product::type{"widget"}));
        auto const cond = col_ref<product::category_id>.not_in_subquery(sub);
        expect(cond.build_sql() == "category_id NOT IN (SELECT id FROM product WHERE type = 'widget')"s)
            << cond.build_sql();
    };

    "eq_subquery in full WHERE clause"_test = [] {
        auto const sub = select<max_of<product::price_val>>().from<product>();
        auto const sql = select<product::id, product::sku>()
                             .from<product>()
                             .where(col_ref<product::price_val>.eq_subquery(sub))
                             .build_sql();
        expect(sql == "SELECT id, sku FROM product WHERE price_val = (SELECT MAX(price_val) FROM product)"s) << sql;
    };
};

// ===================================================================
// Aggregate projections — count_distinct, group_concat
// ===================================================================

suite<"DQL Aggregate Extensions"> dql_aggregate_extensions_suite = [] {
    "count_distinct<Col> — generates COUNT(DISTINCT col)"_test = [] {
        auto const sql = select<count_distinct<product::type>>().from<product>().build_sql();
        expect(sql == "SELECT COUNT(DISTINCT type) FROM product"s) << sql;
    };

    "count_distinct in HAVING — count distinct comparison"_test = [] {
        auto const cond = agg_expr<count_distinct<product::type>>{} > 3u;
        expect(cond.build_sql() == "COUNT(DISTINCT type) > 3"s) << cond.build_sql();
    };

    "group_concat<Col> — generates GROUP_CONCAT(col)"_test = [] {
        auto const sql =
            select<product::type, group_concat<product::sku>>().from<product>().group_by<product::type>().build_sql();
        expect(sql == "SELECT type, GROUP_CONCAT(sku) FROM product GROUP BY type"s) << sql;
    };

    "coalesce_of<Col1, Col2> — generates COALESCE(col1, col2)"_test = [] {
        auto const sql = select<coalesce_of<product::category_id, product::id>>().from<product>().build_sql();
        expect(sql == "SELECT COALESCE(category_id, id) FROM product"s) << sql;
    };

    "ifnull_of<Col1, Col2> — generates IFNULL(col1, col2)"_test = [] {
        auto const sql = select<ifnull_of<product::name, product::sku>>().from<product>().build_sql();
        expect(sql == "SELECT IFNULL(name, sku) FROM product"s) << sql;
    };

    "coalesce/ifnull aliases — generate expected SQL"_test = [] {
        auto const sql = select<coalesce<product::category_id, product::id>, ifnull<product::name, product::sku>>()
                             .from<product>()
                             .build_sql();
        expect(sql == "SELECT COALESCE(category_id, id), IFNULL(name, sku) FROM product"s) << sql;
    };

    "mysql_group_concat/mysql_ifnull_of — extension aliases generate MySQL aggregate SQL"_test = [] {
        auto const sql =
            select<product::type, mysql_group_concat<product::sku>, mysql_ifnull_of<product::name, product::sku>>()
                .from<product>()
                .group_by<product::type>()
                .build_sql();
        expect(sql == "SELECT type, GROUP_CONCAT(sku), IFNULL(name, sku) FROM product GROUP BY type"s) << sql;
    };
};

// ===================================================================
// Scalar SQL functions
// ===================================================================

suite<"DQL Scalar Functions"> dql_scalar_functions_suite = [] {
    "round_to<Col,2> — generates ROUND(col, 2)"_test = [] {
        auto const sql = select<round_to<product::price_val, 2>>().from<product>().build_sql();
        expect(sql == "SELECT ROUND(price_val, 2) FROM product"s) << sql;
    };

    "format_to<Col,2> — generates FORMAT(col, 2)"_test = [] {
        auto const sql = select<format_to<product::price_val, 2>>().from<product>().build_sql();
        expect(sql == "SELECT FORMAT(price_val, 2) FROM product"s) << sql;
    };

    "round/format aliases — generate expected SQL"_test = [] {
        auto const sql =
            select<ds_mysql::round<product::price_val, 2>, ds_mysql::format<product::price_val, 2>>()
                .from<product>()
                .build_sql();
        expect(sql == "SELECT ROUND(price_val, 2), FORMAT(price_val, 2) FROM product"s) << sql;
    };

    "common scalar function wrappers — generate expected SQL"_test = [] {
        auto const sql =
            select<abs_of<product::price_val>, floor_of<product::price_val>, ceil_of<product::price_val>,
                   upper_of<product::sku>, lower_of<product::type>, trim_of<product::sku>, length_of<product::sku>,
                   substring_of<product::sku, 2, 3>, mod_of<product::price_val, product::id>,
                   power_of<product::id, product::id>, concat_of<product::sku, product::type>,
                   cast_as<product::id, sql_cast_type::signed_integer>,
                   convert_as<product::sku, sql_cast_type::char_type>>()
                .from<product>()
                .build_sql();
        expect(sql ==
               "SELECT ABS(price_val), FLOOR(price_val), CEIL(price_val), UPPER(sku), LOWER(type), TRIM(sku), "
               "LENGTH(sku), SUBSTRING(sku, 2, 3), MOD(price_val, id), POWER(id, id), CONCAT(sku, type), "
               "CAST(id AS SIGNED), CONVERT(sku, CHAR) FROM product"s)
            << sql;
    };

    "short scalar aliases — generate expected SQL"_test = [] {
        auto const sql =
            select<ds_mysql::abs<product::price_val>, ds_mysql::floor<product::price_val>,
                   ds_mysql::ceil<product::price_val>, ds_mysql::upper<product::sku>,
                   ds_mysql::lower<product::type>, ds_mysql::trim<product::sku>,
                   ds_mysql::length<product::sku>, ds_mysql::substring<product::sku, 2, 3>,
                   ds_mysql::mod<product::price_val, product::id>, ds_mysql::power<product::id, product::id>,
                   ds_mysql::concat<product::sku, product::type>,
                   ds_mysql::cast<product::id, sql_cast_type::signed_integer>,
                   ds_mysql::convert<product::sku, sql_cast_type::char_type>>()
                .from<product>()
                .build_sql();
        expect(sql ==
               "SELECT ABS(price_val), FLOOR(price_val), CEIL(price_val), UPPER(sku), LOWER(type), TRIM(sku), "
               "LENGTH(sku), SUBSTRING(sku, 2, 3), MOD(price_val, id), POWER(id, id), CONCAT(sku, type), "
               "CAST(id AS SIGNED), CONVERT(sku, CHAR) FROM product"s)
            << sql;
    };

    "date_format_of and extract_of — generate date functions"_test = [] {
        auto const sql = select<date_format_of<event_log::created_at, "%Y-%m">,
                                extract_of<sql_date_part::year, event_log::created_at>>()
                             .from<event_log>()
                             .build_sql();
        expect(sql == "SELECT DATE_FORMAT(created_at, '%Y-%m'), EXTRACT(YEAR FROM created_at) FROM event_log"s) << sql;
    };

    "date_format/extract aliases — generate expected SQL"_test = [] {
        auto const sql =
            select<date_format<event_log::created_at, "%Y-%m">, extract<sql_date_part::year, event_log::created_at>>()
                .from<event_log>()
                .build_sql();
        expect(sql == "SELECT DATE_FORMAT(created_at, '%Y-%m'), EXTRACT(YEAR FROM created_at) FROM event_log"s) << sql;
    };

    "mysql_format_to/mysql_convert_as — extension aliases generate MySQL scalar SQL"_test = [] {
        auto const sql =
            select<mysql_format_to<product::price_val, 2>, mysql_convert_as<product::sku, sql_cast_type::char_type>>()
                .from<product>()
                .build_sql();
        expect(sql == "SELECT FORMAT(price_val, 2), CONVERT(sku, CHAR) FROM product"s) << sql;
    };

    "cast_as/convert_as TIMESTAMP — generates TIMESTAMP SQL type"_test = [] {
        auto const sql = select<cast_as<event_log::created_at, sql_cast_type::timestamp>,
                                convert_as<event_log::created_at, sql_cast_type::timestamp>>()
                             .from<event_log>()
                             .build_sql();
        expect(sql == "SELECT CAST(created_at AS TIMESTAMP), CONVERT(created_at, TIMESTAMP) FROM event_log"s) << sql;
    };

    "mysql_date_format_of — extension alias generates MySQL date SQL"_test = [] {
        auto const sql = select<mysql_date_format_of<event_log::created_at, "%Y-%m">>().from<event_log>().build_sql();
        expect(sql == "SELECT DATE_FORMAT(created_at, '%Y-%m') FROM event_log"s) << sql;
    };
};

// ===================================================================
// Arithmetic expression projections
// ===================================================================

suite<"DQL Arithmetic Projections"> dql_arithmetic_projections_suite = [] {
    "arith_add<Col, Col> — generates (col + col)"_test = [] {
        auto const sql = select<arith_add<product::id, product::price_val>>().from<product>().build_sql();
        expect(sql == "SELECT (id + price_val) FROM product"s) << sql;
    };

    "arith_sub<Col, Col> — generates (col - col)"_test = [] {
        auto const sql = select<arith_sub<product::price_val, product::id>>().from<product>().build_sql();
        expect(sql == "SELECT (price_val - id) FROM product"s) << sql;
    };

    "arith_mul<Col, Col> — generates (col * col)"_test = [] {
        auto const sql = select<arith_mul<product::id, product::price_val>>().from<product>().build_sql();
        expect(sql == "SELECT (id * price_val) FROM product"s) << sql;
    };

    "arith_div<Col, Col> — generates (col / col)"_test = [] {
        auto const sql = select<arith_div<product::price_val, product::id>>().from<product>().build_sql();
        expect(sql == "SELECT (price_val / id) FROM product"s) << sql;
    };

    "nested arithmetic — (col + col) * col"_test = [] {
        auto const sql =
            select<arith_mul<arith_add<product::id, product::price_val>, product::id>>().from<product>().build_sql();
        expect(sql == "SELECT ((id + price_val) * id) FROM product"s) << sql;
    };
};

// ===================================================================
// Column alias — with_alias<Proj>("name")
// ===================================================================

suite<"DQL Column Alias"> dql_column_alias_suite = [] {
    "with_alias<Proj>(\"n\") — appends AS to matching projection"_test = [] {
        auto const sql =
            select<sum<product::price_val>>().from<product>().with_alias<sum<product::price_val>>("total").build_sql();
        expect(sql == "SELECT SUM(price_val) AS total FROM product"s) << sql;
    };

    "with_alias second projection — appends AS to second projection"_test = [] {
        auto const sql = select<product::type, count_all>()
                             .from<product>()
                             .group_by<product::type>()
                             .with_alias<count_all>("cnt")
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) AS cnt FROM product GROUP BY type"s) << sql;
    };

    "multiple aliases — both projections aliased"_test = [] {
        auto const sql = select<sum<product::price_val>, count_all>()
                             .from<product>()
                             .with_alias<sum<product::price_val>>("total")
                             .with_alias<count_all>("qty")
                             .build_sql();
        expect(sql == "SELECT SUM(price_val) AS total, COUNT(*) AS qty FROM product"s) << sql;
    };
};

// Projection type not in SELECT list is rejected at compile time.
// The concept variable separates the constraint check from the static_assert
// so that GCC treats the requires-clause violation as a substitution failure.
template <typename Builder, typename Proj>
inline constexpr bool can_alias_with = requires(Builder b) { b.template with_alias<Proj>(std::string{}); };

static_assert(!can_alias_with<decltype(select<product::id>().from<product>()), product::sku>);

// ===================================================================
// case_when expression
// ===================================================================

suite<"DQL CASE WHEN"> dql_case_when_suite = [] {
    "case_when single branch — CASE WHEN ... THEN ... END"_test = [] {
        auto const expr =
            case_when(equal<product::type>(product::type{"widget"}), std::string{"Widget Type"}).build_sql();
        expect(expr == "CASE WHEN type = 'widget' THEN 'Widget Type' END"s) << expr;
    };

    "case_when with else — CASE WHEN ... THEN ... ELSE ... END"_test = [] {
        auto const expr = case_when(equal<product::type>(product::type{"widget"}), std::string{"Widget Type"})
                              .else_(std::string{"Other"})
                              .build_sql();
        expect(expr == "CASE WHEN type = 'widget' THEN 'Widget Type' ELSE 'Other' END"s) << expr;
    };

    "case_when multiple branches — CASE WHEN ... THEN ... WHEN ... THEN ... ELSE ... END"_test = [] {
        auto const expr = case_when(equal<product::type>(product::type{"widget"}), std::string{"Widget"})
                              .when(equal<product::type>(product::type{"gadget"}), std::string{"Gadget"})
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
        auto const sql = select<product::id, row_number_over<product::type, product::id>>().from<product>().build_sql();
        expect(sql == "SELECT id, ROW_NUMBER() OVER (PARTITION BY type ORDER BY id ASC) FROM product"s) << sql;
    };

    "row_number_over desc — ROW_NUMBER() OVER (... ORDER BY col DESC)"_test = [] {
        auto const sql = select<product::id, row_number_over<product::type, product::id, sort_order::desc>>()
                             .from<product>()
                             .build_sql();
        expect(sql == "SELECT id, ROW_NUMBER() OVER (PARTITION BY type ORDER BY id DESC) FROM product"s) << sql;
    };

    "window_func<sum> — SUM OVER (...)"_test = [] {
        auto const sql = select<product::id, window_func<sum<product::price_val>, product::type, product::id>>()
                             .from<product>()
                             .build_sql();
        expect(sql == "SELECT id, SUM(price_val) OVER (PARTITION BY type ORDER BY id ASC) FROM product"s) << sql;
    };

    "rank/dense_rank/lag/lead — generate extended window functions"_test = [] {
        auto const sql = select<rank_over<product::type, product::id>, dense_rank_over<product::type, product::id>,
                                lag_over<product::price_val, product::type, product::id>,
                                lead_over<product::price_val, product::type, product::id, 2, sort_order::desc>>()
                             .from<product>()
                             .build_sql();
        expect(sql ==
               "SELECT RANK() OVER (PARTITION BY type ORDER BY id ASC), DENSE_RANK() OVER (PARTITION BY type ORDER BY "
               "id ASC), LAG(price_val, 1) OVER (PARTITION BY type ORDER BY id ASC), LEAD(price_val, 2) OVER "
               "(PARTITION BY type ORDER BY id DESC) FROM product"s)
            << sql;
    };
};

// ===================================================================
// ORDER BY extensions
// ===================================================================

suite<"DQL ORDER BY Extensions"> dql_order_by_extensions_suite = [] {
    "order_by_agg<count_all>() — ORDER BY COUNT(*)"_test = [] {
        auto const sql = select<product::type, count_all>()
                             .from<product>()
                             .group_by<product::type>()
                             .order_by_agg<count_all>()
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM product GROUP BY type ORDER BY COUNT(*) ASC"s) << sql;
    };

    "order_by_agg<count_all, desc>() — ORDER BY COUNT(*) DESC"_test = [] {
        auto const sql = select<product::type, count_all>()
                             .from<product>()
                             .group_by<product::type>()
                             .order_by_agg<count_all, sort_order::desc>()
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM product GROUP BY type ORDER BY COUNT(*) DESC"s) << sql;
    };

    "order_by_agg<sum<Col>>() — ORDER BY SUM(col)"_test = [] {
        auto const sql = select<product::type, sum<product::price_val>>()
                             .from<product>()
                             .group_by<product::type>()
                             .order_by_agg<sum<product::price_val>, sort_order::desc>()
                             .build_sql();
        expect(sql == "SELECT type, SUM(price_val) FROM product GROUP BY type ORDER BY SUM(price_val) DESC"s) << sql;
    };

    "order_by_raw(\"total DESC\") — raw ORDER BY expression"_test = [] {
        auto const sql = select<product::type, count_all>()
                             .from<product>()
                             .group_by<product::type>()
                             .order_by_raw("COUNT(*) DESC")
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM product GROUP BY type ORDER BY COUNT(*) DESC"s) << sql;
    };

    "order_by_raw chained after column order_by"_test = [] {
        auto const sql =
            select<product::id>().from<product>().order_by<product::type>().order_by_raw("price_val DESC").build_sql();
        expect(sql == "SELECT id FROM product ORDER BY type ASC, price_val DESC"s) << sql;
    };
};

// ===================================================================
// GROUP BY WITH ROLLUP
// ===================================================================

suite<"DQL GROUP BY ROLLUP"> dql_group_by_rollup_suite = [] {
    "group_by_rollup<Col> — generates GROUP BY col WITH ROLLUP"_test = [] {
        auto const sql =
            select<product::type, count_all>().from<product>().group_by_rollup<product::type>().build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM product GROUP BY type WITH ROLLUP"s) << sql;
    };

    "group_by_rollup two columns — generates GROUP BY col1, col2 WITH ROLLUP"_test = [] {
        auto const sql = select<product::type, product::sku, count_all>()
                             .from<product>()
                             .group_by_rollup<product::type, product::sku>()
                             .build_sql();
        expect(sql == "SELECT type, sku, COUNT(*) FROM product GROUP BY type, sku WITH ROLLUP"s) << sql;
    };

    "group_by_rollup with HAVING"_test = [] {
        auto const sql = select<product::type, count_all>()
                             .from<product>()
                             .group_by_rollup<product::type>()
                             .having(count() > 2u)
                             .build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM product GROUP BY type WITH ROLLUP HAVING COUNT(*) > 2"s) << sql;
    };

    "group_by_cube — generates GROUP BY CUBE(...)"_test = [] {
        auto const sql = select<product::type, product::sku, count_all>()
                             .from<product>()
                             .group_by_cube<product::type, product::sku>()
                             .build_sql();
        expect(sql == "SELECT type, sku, COUNT(*) FROM product GROUP BY CUBE(type, sku)"s) << sql;
    };

    "group_by_grouping_sets — generates GROUPING SETS clause"_test = [] {
        auto const sql =
            select<product::type, product::sku, count_all>()
                .from<product>()
                .group_by_grouping_sets<grouping_set<product::type>, grouping_set<product::sku>, grouping_set<>>()
                .build_sql();
        expect(sql == "SELECT type, sku, COUNT(*) FROM product GROUP BY GROUPING SETS ((type), (sku), ())"s) << sql;
    };
};

// ===================================================================
// JOIN extensions — FULL OUTER JOIN, CROSS JOIN, compound ON
// ===================================================================

suite<"DQL JOIN Extensions"> dql_join_extensions_suite = [] {
    "full_join — generates FULL OUTER JOIN ON"_test = [] {
        auto const sql = select<product::sku, category::label>()
                             .from_joined<product>()
                             .full_join<category, product::category_id, category::id>()
                             .build_sql();
        expect(sql == "SELECT sku, label FROM product FULL OUTER JOIN category ON category_id = id"s) << sql;
    };

    "cross_join — generates CROSS JOIN (no ON)"_test = [] {
        auto const sql =
            select<product::sku, category::label>().from_joined<product>().cross_join<category>().build_sql();
        expect(sql == "SELECT sku, label FROM product CROSS JOIN category"s) << sql;
    };

    "inner_join_on with compound ON — generates INNER JOIN ... ON (a AND b)"_test = [] {
        auto const on = (col_ref<product::category_id> == 1u) & is_not_null<product::category_id>();
        auto const sql = select<product::sku, category::label>()
                             .from_joined<product>()
                             .inner_join_on<category>(std::move(on))
                             .build_sql();
        expect(sql ==
               "SELECT sku, label FROM product INNER JOIN category ON (category_id = 1 AND category_id IS NOT NULL)"s)
            << sql;
    };

    "left_join_on — generates LEFT JOIN ... ON compound"_test = [] {
        auto const on = col_ref<product::category_id> == 2u;
        auto const sql =
            select<product::sku>().from_joined<product>().left_join_on<category>(std::move(on)).build_sql();
        expect(sql == "SELECT sku FROM product LEFT JOIN category ON category_id = 2"s) << sql;
    };

    "right_join_on — generates RIGHT JOIN ... ON compound"_test = [] {
        auto const on = col_ref<product::category_id> == 3u;
        auto const sql =
            select<product::sku>().from_joined<product>().right_join_on<category>(std::move(on)).build_sql();
        expect(sql == "SELECT sku FROM product RIGHT JOIN category ON category_id = 3"s) << sql;
    };

    "full_join_on — generates FULL OUTER JOIN ... ON compound"_test = [] {
        auto const on = col_ref<product::category_id> == 4u;
        auto const sql =
            select<product::sku>().from_joined<product>().full_join_on<category>(std::move(on)).build_sql();
        expect(sql == "SELECT sku FROM product FULL OUTER JOIN category ON category_id = 4"s) << sql;
    };
};

// ===================================================================
// Derived table (subquery in FROM)
// ===================================================================

suite<"DQL Derived Table"> dql_derived_table_suite = [] {
    "from_subquery — SELECT FROM (SELECT ...) AS t"_test = [] {
        auto const sub = select<product::type, count_all>().from<product>().group_by<product::type>();
        auto const sql = select<product::type, count_all>().from_subquery<product>(sub, "t").build_sql();
        expect(sql == "SELECT type, COUNT(*) FROM (SELECT type, COUNT(*) FROM product GROUP BY type) AS t"s) << sql;
    };

    "from_subquery with WHERE on outer query"_test = [] {
        auto const sub = select<product::type>().from<product>().where(is_not_null<product::name>());
        auto const sql = select<product::type>()
                             .from_subquery<product>(sub, "filtered")
                             .where(equal<product::type>(product::type{"widget"}))
                             .build_sql();
        expect(sql ==
               "SELECT type FROM (SELECT type FROM product WHERE name IS NOT NULL) AS filtered WHERE type = 'widget'"s)
            << sql;
    };
};

// ===================================================================
// CTE (WITH clause)
// ===================================================================

suite<"DQL CTE"> dql_cte_suite = [] {
    "with_cte single CTE — WITH name AS (q) main_select"_test = [] {
        auto const cte_q = select<product::type, count_all>().from<product>().group_by<product::type>();
        auto const main_q = select<product::type>().from<product>();
        auto const cte = with_cte("counts", cte_q);
        auto const sql = cte.build_sql(main_q);
        expect(sql == "WITH counts AS (SELECT type, COUNT(*) FROM product GROUP BY type) SELECT type FROM product"s)
            << sql;
    };

    "with_cte two CTEs — WITH cte1 AS (...), cte2 AS (...) SELECT ..."_test = [] {
        auto const cte1 = select<product::type, count_all>().from<product>().group_by<product::type>();
        auto const cte2 = select<product::sku>().from<product>().where(is_not_null<product::name>());
        auto const main = select<product::id>().from<product>();
        auto const cte = with_cte("counts", cte1).with_cte("skus", cte2);
        auto const sql = cte.build_sql(main);
        expect(sql ==
               "WITH counts AS (SELECT type, COUNT(*) FROM product GROUP BY type), "
               "skus AS (SELECT sku FROM product WHERE name IS NOT NULL) "
               "SELECT id FROM product"s)
            << sql;
    };

    "with_recursive_cte — emits WITH RECURSIVE"_test = [] {
        auto const seed = select<product::id>().from<product>().where(equal<product::id>(product::id{1u}));
        auto const main = select<product::id>().from<product>();
        auto const sql = with_recursive_cte("seed_ids", seed).build_sql(main);
        expect(sql == "WITH RECURSIVE seed_ids AS (SELECT id FROM product WHERE id = 1) SELECT id FROM product"s)
            << sql;
    };
};

suite<"DQL Set Operators"> dql_set_operators_suite = [] {
    "intersect_ — combines two queries with INTERSECT"_test = [] {
        auto const q1 = select<product::id>().from<product>().where(equal<product::type>(product::type{"gadget"}));
        auto const q2 = select<product::id>().from<product>().where(equal<product::type>(product::type{"widget"}));
        auto const sql = intersect_(q1, q2).build_sql();
        expect(sql ==
               "(SELECT id FROM product WHERE type = 'gadget') INTERSECT (SELECT id FROM product WHERE type = "
               "'widget')"s)
            << sql;
    };

    "except_ — combines two queries with EXCEPT"_test = [] {
        auto const q1 = select<product::id>().from<product>().where(equal<product::type>(product::type{"gadget"}));
        auto const q2 = select<product::id>().from<product>().where(equal<product::type>(product::type{"widget"}));
        auto const sql = except_(q1, q2).build_sql();
        expect(sql ==
               "(SELECT id FROM product WHERE type = 'gadget') EXCEPT (SELECT id FROM product WHERE type = "
               "'widget')"s)
            << sql;
    };
};

// ===================================================================
// DML extensions — INSERT IGNORE, INSERT...SELECT, REPLACE INTO, UPDATE JOIN
// ===================================================================

suite<"DML Extensions"> dml_extensions_suite = [] {
    "insert_ignore_into single row — generates INSERT IGNORE INTO SQL"_test = [] {
        product p;
        p.id_ = 1u;
        p.category_id_ = std::nullopt;
        p.sku_ = "WIDGET";
        p.type_ = "gadget";
        p.name_ = std::nullopt;
        p.price_val_ = 9.99;

        auto const sql = insert_ignore_into<product>().values(p).build_sql();
        expect(sql.substr(0, 19) == "INSERT IGNORE INTO "s) << sql.substr(0, 19);
        expect(sql ==
               "INSERT IGNORE INTO product (id, category_id, sku, type, name, price_val) VALUES (1, NULL, 'WIDGET', "
               "'gadget', NULL, 9.990000)")
            << sql;
    };

    "replace_into single row — generates REPLACE INTO SQL"_test = [] {
        product p;
        p.id_ = 1u;
        p.category_id_ = std::nullopt;
        p.sku_ = "WIDGET";
        p.type_ = "gadget";
        p.name_ = std::nullopt;
        p.price_val_ = 9.99;

        auto const sql = replace_into<product>().values(p).build_sql();
        expect(sql.substr(0, 13) == "REPLACE INTO "s) << sql.substr(0, 13);
        expect(sql ==
               "REPLACE INTO product (id, category_id, sku, type, name, price_val) VALUES (1, NULL, 'WIDGET', "
               "'gadget', NULL, 9.990000)")
            << sql;
    };

    "replace_into contains table name and values"_test = [] {
        product p;
        p.id_ = 2u;
        p.category_id_ = 3u;
        p.sku_ = "SKU2";
        p.type_ = "type2";
        p.name_ = std::nullopt;
        p.price_val_ = 19.99;

        auto const sql = replace_into<product>().values(p).build_sql();
        expect(sql ==
               "REPLACE INTO product (id, category_id, sku, type, name, price_val) VALUES (2, 3, 'SKU2', 'type2', "
               "NULL, 19.990000)")
            << sql;
    };

    "insert_into_select — generates INSERT INTO ... SELECT SQL"_test = [] {
        auto const select_q =
            select<product::id, product::category_id, product::sku, product::type, product::name, product::price_val>()
                .from<product>()
                .where(equal<product::type>(product::type{"widget"}));
        auto const sql = insert_into_select<product>(select_q).build_sql();
        expect(sql ==
               "INSERT INTO product (id, category_id, sku, type, name, price_val) SELECT id, category_id, sku, type, "
               "name, price_val FROM product WHERE type = 'widget'")
            << sql;
    };

    "update_join — generates UPDATE T1 JOIN T2 ON ... SET ..."_test = [] {
        auto const sql = update_join<product, category, product::category_id, category::id>()
                             .set(product::sku{"UPDATED"})
                             .build_sql();
        expect(sql == "UPDATE product INNER JOIN category ON category_id = id SET sku = 'UPDATED'") << sql;
    };

    "update_join with WHERE — generates UPDATE JOIN WHERE SQL"_test = [] {
        auto const sql = update_join<product, category, product::category_id, category::id>()
                             .set(product::sku{"NEW_SKU"})
                             .where(equal<product::id>(product::id{42u}))
                             .build_sql();
        expect(sql == "UPDATE product INNER JOIN category ON category_id = id SET sku = 'NEW_SKU' WHERE id = 42")
            << sql;
    };

    "update_join set multiple columns — generates comma-separated SET"_test = [] {
        auto const sql = update_join<product, category, product::category_id, category::id>()
                             .set(product::sku{"SKU1"}, product::type{"gadget"})
                             .build_sql();
        expect(sql == "UPDATE product INNER JOIN category ON category_id = id SET sku = 'SKU1', type = 'gadget'")
            << sql;
    };

    "insert_ignore_into bulk — generates INSERT IGNORE INTO ... VALUES (row1), (row2)"_test = [] {
        product p1;
        p1.id_ = 1u;
        p1.sku_ = "SKU1";
        p1.type_ = "type1";
        p1.price_val_ = 1.0;
        product p2;
        p2.id_ = 2u;
        p2.sku_ = "SKU2";
        p2.type_ = "type2";
        p2.price_val_ = 2.0;
        auto const sql = insert_ignore_into<product>().values(std::initializer_list{p1, p2}).build_sql();
        expect(sql ==
               "INSERT IGNORE INTO product (id, category_id, sku, type, name, price_val) VALUES (1, NULL, 'SKU1', "
               "'type1', NULL, 1.000000), (2, NULL, 'SKU2', 'type2', NULL, 2.000000)")
            << sql;
    };

    "replace_into bulk — generates REPLACE INTO ... VALUES (row1), (row2)"_test = [] {
        product p1;
        p1.id_ = 1u;
        p1.sku_ = "RSKU1";
        p1.type_ = "rtype1";
        p1.price_val_ = 10.0;
        product p2;
        p2.id_ = 2u;
        p2.sku_ = "RSKU2";
        p2.type_ = "rtype2";
        p2.price_val_ = 20.0;
        auto const sql = replace_into<product>().values(std::initializer_list<product>{p1, p2}).build_sql();
        expect(sql ==
               "REPLACE INTO product (id, category_id, sku, type, name, price_val) VALUES (1, NULL, 'RSKU1', 'rtype1', "
               "NULL, 10.000000), (2, NULL, 'RSKU2', 'rtype2', NULL, 20.000000)")
            << sql;
    };

    "insert_ignore_into empty vector — returns column list with empty values"_test = [] {
        auto const sql = insert_ignore_into<product>().values(std::initializer_list<product>{}).build_sql();
        expect(sql == "INSERT IGNORE INTO product (id, category_id, sku, type, name, price_val) VALUES ()") << sql;
    };

    "replace_into empty vector — returns column list with empty values"_test = [] {
        auto const sql = replace_into<product>().values(std::initializer_list<product>{}).build_sql();
        expect(sql == "REPLACE INTO product (id, category_id, sku, type, name, price_val) VALUES ()") << sql;
    };
};
