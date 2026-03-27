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
};