#include <boost/ut.hpp>

#include "ds_mysql/ds_mysql.hpp"

using namespace boost::ut;
using namespace ds_mysql;
using namespace std::string_literals;

namespace {

struct api_product {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(sku, varchar_type<64>)
    COLUMN_FIELD(name, std::optional<varchar_type<255>>)
};

}  // namespace

suite<"public api umbrella header"> public_api_umbrella_header_suite = [] {
    "ds_mysql.hpp exposes query builder and support types needed by quick start"_test = [] {
        mysql_config const cfg{host_name{"127.0.0.1"}, database_name{"catalog"},
                               auth_credentials{user_name{"root"}, user_password{"secret"}}, port_number{3306}};

        expect(cfg.host().to_string() == "127.0.0.1"s);
        expect(cfg.database().to_string() == "catalog"s);
        expect(cfg.credentials().user().to_string() == "root"s);
        expect(cfg.port().to_unsigned_int() == 3306_u);

        auto const sql = select(api_product::id{}, api_product::name{})
                             .from(api_product{})
                             .where(equal<api_product::sku>(api_product::sku{"ABC"}))
                             .order_by(api_product::id{})
                             .build_sql();

        expect(sql == "SELECT id, name FROM api_product WHERE sku = 'ABC' ORDER BY id ASC"s) << sql;
    };

    "ds_mysql.hpp exposes transaction_guard"_test = [] {
        // transaction_guard is move-only and constructed via begin() — verify it's available.
        static_assert(!std::is_copy_constructible_v<transaction_guard>);
        static_assert(std::is_move_constructible_v<transaction_guard>);
    };

    "ds_mysql.hpp exposes prepared_statement"_test = [] {
        // prepared_statement is move-only and constructed via mysql_connection::prepare().
        static_assert(!std::is_copy_constructible_v<prepared_statement>);
        static_assert(std::is_move_constructible_v<prepared_statement>);
    };

    "ds_mysql.hpp exposes CTE builders"_test = [] {
        auto const sql = with(cte("t", "SELECT 1 AS x")).select(count_all{}).from(cte_ref{"t"}).build_sql();
        expect(sql == "WITH t AS (SELECT 1 AS x) SELECT COUNT(*) FROM t"s) << sql;

        auto const rsql = with(recursive(cte("r", "SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<3")))
                              .select(count_all{})
                              .from(cte_ref{"r"})
                              .build_sql();
        expect(rsql == "WITH RECURSIVE r AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<3) SELECT COUNT(*) FROM r"s)
            << rsql;
    };
};