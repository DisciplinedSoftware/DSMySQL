#include <boost/ut.hpp>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <optional>
#include <string>

#include "ds_mysql/sql.hpp"

using namespace boost::ut;
using namespace ds_mysql;
using namespace std::string_literals;

// ===================================================================
// Test fixtures
// ===================================================================

namespace {
struct asset {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(exchange_id, std::optional<uint32_t>)
    COLUMN_FIELD(ticker, varchar_field<32>)
    COLUMN_FIELD(instrument, varchar_field<64>)
    COLUMN_FIELD(name, std::optional<varchar_field<255>>)
    COLUMN_FIELD(sector, std::optional<varchar_field<255>>)
    COLUMN_FIELD(currency, std::optional<varchar_field<32>>)
    COLUMN_FIELD(created_date, datetime_type<>)
    COLUMN_FIELD(last_updated_date, datetime_type<>)
};

struct formatted_numeric_asset {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(value1, float_type<12, 4>)
    COLUMN_FIELD(value2, std::optional<double_type<12, 4>>)
    COLUMN_FIELD(value3, decimal_type<12, 4>)
};
}  // namespace

// ===================================================================
// DML — DESCRIBE, INSERT, UPDATE, DELETE, COUNT, WHERE, TRUNCATE
// ===================================================================

suite<"DML"> dml_suite = [] {
    // -------------------------------------------------------------------
    // describe<T>
    // -------------------------------------------------------------------

    "describe - generates correct SQL"_test = [] {
        auto const sql = describe<asset>().build_sql();
        expect(sql == "DESCRIBE asset"s) << sql;
    };

    "start_transaction - generates correct SQL"_test = [] {
        auto const sql = start_transaction().build_sql();
        expect(sql == "START TRANSACTION"s) << sql;
    };

    "begin_transaction - generates correct SQL"_test = [] {
        auto const sql = begin_transaction().build_sql();
        expect(sql == "BEGIN"s) << sql;
    };

    "commit_transaction - generates correct SQL"_test = [] {
        auto const sql = commit_transaction().build_sql();
        expect(sql == "COMMIT"s) << sql;
    };

    "rollback_transaction - generates correct SQL"_test = [] {
        auto const sql = rollback_transaction().build_sql();
        expect(sql == "ROLLBACK"s) << sql;
    };

    "explain(select) - prefixes EXPLAIN"_test = [] {
        auto const sql = explain(select<asset::id>().from<asset>().where(equal<asset::id>(1u))).build_sql();
        expect(sql == "EXPLAIN SELECT id FROM asset WHERE id = 1"s) << sql;
    };

    "explain_analyze(select) - prefixes EXPLAIN ANALYZE"_test = [] {
        auto const sql = explain_analyze(select<asset::id>().from<asset>().limit(5)).build_sql();
        expect(sql == "EXPLAIN ANALYZE SELECT id FROM asset LIMIT 5"s) << sql;
    };

    // -------------------------------------------------------------------
    // insert_into<T>
    // -------------------------------------------------------------------

    "insert into - typed values with sql_now datetime"_test = [] {
        asset row;
        row.exchange_id_ = 1u;
        row.ticker_ = "AAPL";
        row.instrument_ = "Stock";
        // created_date_ and last_updated_date_ default to sql_now
        auto const sql = insert_into<asset>().values(row).build_sql();
        expect(
            sql ==
            "INSERT INTO asset (id, exchange_id, ticker, instrument, name, sector, currency, created_date, last_updated_date) VALUES (0, 1, 'AAPL', 'Stock', NULL, NULL, NULL, NOW(), NOW())"s)
            << sql;
    };

    "insert into - typed values with explicit datetime"_test = [] {
        using namespace std::chrono;
        asset row;
        row.id_ = 1u;
        row.exchange_id_ = 2u;
        row.ticker_ = "ticker";
        row.instrument_ = "instrument";
        row.name_ = "name";
        row.currency_ = "currency";
        row.created_date_ = datetime_type<>{system_clock::time_point{sys_days{year{2024} / January / 1}}};
        row.last_updated_date_ = sql_now;
        auto const sql = insert_into<asset>().values(row).build_sql();
        expect(
            sql ==
            "INSERT INTO asset (id, exchange_id, ticker, instrument, name, sector, currency, created_date, last_updated_date) VALUES (1, 2, 'ticker', 'instrument', 'name', NULL, 'currency', '2024-01-01 00:00:00', NOW())"s)
            << sql;
    };

    "insert into - typed values with fractional-second datetime"_test = [] {
        using namespace std::chrono;
        asset row;
        row.id_ = 42u;
        row.exchange_id_ = 3u;
        row.ticker_ = "ticker";
        row.instrument_ = "instrument";
        row.created_date_ =
            datetime_type<>{system_clock::time_point{sys_days{year{2024} / January / 1}} + microseconds{123456}, 6};
        row.last_updated_date_ = sql_now;
        auto const sql = insert_into<asset>().values(row).build_sql();
        expect(
            sql ==
            "INSERT INTO asset (id, exchange_id, ticker, instrument, name, sector, currency, created_date, last_updated_date) VALUES (42, 3, 'ticker', 'instrument', NULL, NULL, NULL, '2024-01-01 00:00:00.123456', NOW())"s)
            << sql;
    };

    "datetime/timestamp serialization reflects configured fractional-second precision"_test = [] {
        using namespace std::chrono;
        auto const tp = system_clock::time_point{sys_days{year{2024} / January / 1}} + microseconds{987654};

        expect(sql_detail::to_sql_value(datetime_type<>{tp}) == "'2024-01-01 00:00:00'"s);
        expect(sql_detail::to_sql_value(datetime_type<>{tp, 3}) == "'2024-01-01 00:00:00.987'"s);
        expect(sql_detail::to_sql_value(timestamp_type<>{tp, 2}) == "'2024-01-01 00:00:00.98'"s);

        expect(sql_detail::to_sql_value(datetime_type<>{sql_now}) == "NOW()"s);
        expect(sql_detail::to_sql_value(datetime_type<>{sql_now, 4}) == "NOW(4)"s);
        expect(sql_detail::to_sql_value(timestamp_type<>{sql_now}) == "CURRENT_TIMESTAMP"s);
        expect(sql_detail::to_sql_value(timestamp_type<>{sql_now, 5}) == "CURRENT_TIMESTAMP(5)"s);
    };

    "time_type serialization reflects duration and fractional-second precision"_test = [] {
        using namespace std::chrono;
        // 08:30:00
        auto const t1 = time_type<>{microseconds{(8 * 3600 + 30 * 60) * 1000000LL}};
        expect(sql_detail::to_sql_value(t1) == "'08:30:00'"s);

        // 00:00:00
        expect(sql_detail::to_sql_value(time_type<>{}) == "'00:00:00'"s);

        // 838:59:59 (MySQL maximum)
        auto const t_max = time_type<>{microseconds{(838 * 3600LL + 59 * 60LL + 59LL) * 1000000LL}};
        expect(sql_detail::to_sql_value(t_max) == "'838:59:59'"s);

        // Negative duration: -10:05:00
        auto const t_neg = time_type<>{microseconds{-(10 * 3600 + 5 * 60) * 1000000LL}};
        expect(sql_detail::to_sql_value(t_neg) == "'-10:05:00'"s);

        // Fractional seconds precision=3: 01:02:03.456000 → '01:02:03.456'
        auto const t_frac = time_type<>{microseconds{(3600LL + 2 * 60LL + 3LL) * 1000000LL + 456789LL}, 3};
        expect(sql_detail::to_sql_value(t_frac) == "'01:02:03.456'"s);

        // Fractional seconds precision=6: '01:02:03.456789'
        auto const t_frac6 = time_type<>{microseconds{(3600LL + 2 * 60LL + 3LL) * 1000000LL + 456789LL}, 6};
        expect(sql_detail::to_sql_value(t_frac6) == "'01:02:03.456789'"s);
    };

    "time_type deserialization parses MySQL TIME strings correctly"_test = [] {
        using namespace std::chrono;
        auto const t1 = ::ds_mysql::detail::from_mysql_value_nonnull<time_type<>>("08:30:00");
        expect(t1.duration() == microseconds{(8 * 3600 + 30 * 60) * 1000000LL});

        auto const t2 = ::ds_mysql::detail::from_mysql_value_nonnull<time_type<>>("00:00:00");
        expect(t2.duration() == microseconds{0});

        auto const t3 = ::ds_mysql::detail::from_mysql_value_nonnull<time_type<>>("-10:05:00");
        expect(t3.duration() == microseconds{-(10 * 3600 + 5 * 60) * 1000000LL});

        auto const t4 = ::ds_mysql::detail::from_mysql_value_nonnull<time_type<>>("01:02:03.456789");
        expect(t4.duration() == microseconds{(3600LL + 2 * 60LL + 3LL) * 1000000LL + 456789LL});

        auto const t5 = ::ds_mysql::detail::from_mysql_value_nonnull<time_type<>>("838:59:59");
        expect(t5.duration() == microseconds{(838 * 3600LL + 59 * 60LL + 59LL) * 1000000LL});
    };

    "formatted numeric wrapper types serialize and deserialize like their underlying values"_test = [] {
        expect(sql_type_for<float_type<>>() == "FLOAT"s);
        expect(sql_type_for<float_type<12, 4>>() == "FLOAT(12,4)"s);
        expect(sql_type_for<double_type<12>>() == "DOUBLE(12)"s);
        expect(sql_type_for<double_type<12, 4>>() == "DOUBLE(12,4)"s);
        expect(sql_type_for<decimal_type<>>() == "DECIMAL"s);
        expect(sql_type_for<decimal_type<12>>() == "DECIMAL(12)"s);
        expect(sql_type_for<decimal_type<12, 4>>() == "DECIMAL(12,4)"s);

        expect(sql_detail::to_sql_value(float_type<12, 4>{12.5f}) == "12.500000"s);
        expect(sql_detail::to_sql_value(double_type<12, 4>{42.125}) == "42.125000"s);
        expect(sql_detail::to_sql_value(decimal_type<12, 4>{99.5}) == "99.500000"s);

        auto const float_value = ::ds_mysql::detail::from_mysql_value_nonnull<float_type<12, 4>>("12.5");
        auto const double_value = ::ds_mysql::detail::from_mysql_value_nonnull<double_type<12, 4>>("42.125");
        auto const decimal_value = ::ds_mysql::detail::from_mysql_value_nonnull<decimal_type<12, 4>>("99.5");

        expect(std::fabs(static_cast<float>(float_value) - 12.5f) < 0.0001f);
        expect(std::fabs(static_cast<double>(double_value) - 42.125) < 0.0000001);
        expect(std::fabs(static_cast<double>(decimal_value) - 99.5) < 0.0000001);
    };

    "insert into - typed formatted numeric wrappers generate SQL literals and preserve nullability"_test = [] {
        formatted_numeric_asset row;
        row.id_ = 7u;
        row.value1_ = 12.5f;
        row.value2_ = std::nullopt;
        row.value3_ = 99.5;

        auto const sql = insert_into<formatted_numeric_asset>().values(row).build_sql();
        expect(sql ==
               "INSERT INTO formatted_numeric_asset (id, value1, value2, value3) VALUES (7, 12.500000, NULL, "
               "99.500000)"s)
            << sql;
    };

    // -------------------------------------------------------------------
    // update<T>
    // -------------------------------------------------------------------

    "update - set only - generates correct SQL"_test = [] {
        auto const sql = update<asset>().set(asset::ticker{"MSFT"}).build_sql();
        expect(sql == "UPDATE asset SET ticker = 'MSFT'"s) << sql;
    };

    "update - set with where - generates correct SQL"_test = [] {
        auto const sql = update<asset>().set(asset::ticker{"MSFT"}).where(equal<asset::id>(1u)).build_sql();
        expect(sql == "UPDATE asset SET ticker = 'MSFT' WHERE id = 1"s) << sql;
    };

    "update - set with complex where - generates correct SQL"_test = [] {
        auto const sql = update<asset>()
                             .set(asset::name{"Apple Inc"})
                             .where(and_(equal<asset::ticker>(asset::ticker{"AAPL"}), equal<asset::exchange_id>(2u)))
                             .build_sql();
        expect(sql == "UPDATE asset SET name = 'Apple Inc' WHERE (ticker = 'AAPL' AND exchange_id = 2)"s) << sql;
    };

    "update - order_by + limit - generates correct SQL"_test = [] {
        auto const sql =
            update<asset>().set(asset::ticker{"MSFT"}).order_by<asset::id, sort_order::desc>().limit(1).build_sql();
        expect(sql == "UPDATE asset SET ticker = 'MSFT' ORDER BY id DESC LIMIT 1"s) << sql;
    };

    "update - where + order_by + limit - generates correct SQL"_test = [] {
        auto const sql = update<asset>()
                             .set(asset::ticker{"MSFT"})
                             .where(is_not_null<asset::sector>())
                             .order_by<asset::id>()
                             .limit(3)
                             .build_sql();
        expect(sql == "UPDATE asset SET ticker = 'MSFT' WHERE sector IS NOT NULL ORDER BY id ASC LIMIT 3"s) << sql;
    };

    // -------------------------------------------------------------------
    // delete_from<T>
    // -------------------------------------------------------------------

    "delete from - no where - generates correct SQL"_test = [] {
        auto const sql = delete_from<asset>().build_sql();
        expect(sql == "DELETE FROM asset"s) << sql;
    };

    "delete from - with where - generates correct SQL"_test = [] {
        auto const sql = delete_from<asset>().where(equal<asset::id>(1u)).build_sql();
        expect(sql == "DELETE FROM asset WHERE id = 1"s) << sql;
    };

    "delete from - with complex where - generates correct SQL"_test = [] {
        auto const sql = delete_from<asset>()
                             .where(and_(equal<asset::ticker>(asset::ticker{"AAPL"}), equal<asset::exchange_id>(2u)))
                             .build_sql();
        expect(sql == "DELETE FROM asset WHERE (ticker = 'AAPL' AND exchange_id = 2)"s) << sql;
    };

    "delete from - order_by + limit - generates correct SQL"_test = [] {
        auto const sql = delete_from<asset>().order_by<asset::id, sort_order::desc>().limit(2).build_sql();
        expect(sql == "DELETE FROM asset ORDER BY id DESC LIMIT 2"s) << sql;
    };

    "delete from - where + order_by + limit - generates correct SQL"_test = [] {
        auto const sql =
            delete_from<asset>().where(is_not_null<asset::sector>()).order_by<asset::id>().limit(10).build_sql();
        expect(sql == "DELETE FROM asset WHERE sector IS NOT NULL ORDER BY id ASC LIMIT 10"s) << sql;
    };

    // -------------------------------------------------------------------
    // count<T>
    // -------------------------------------------------------------------

    "count - no where - generates correct SQL"_test = [] {
        auto const sql = count<asset>().build_sql();
        expect(sql == "SELECT COUNT(*) FROM asset"s) << sql;
    };

    "count - with where - generates correct SQL"_test = [] {
        auto const sql = count<asset>().where(equal<asset::ticker>(asset::ticker{"AAPL"})).build_sql();
        expect(sql == "SELECT COUNT(*) FROM asset WHERE ticker = 'AAPL'"s) << sql;
    };

    "count - with complex where - generates correct SQL"_test = [] {
        auto const sql =
            count<asset>().where(and_(equal<asset::exchange_id>(1u), is_not_null<asset::sector>())).build_sql();
        expect(sql == "SELECT COUNT(*) FROM asset WHERE (exchange_id = 1 AND sector IS NOT NULL)"s) << sql;
    };

    // -------------------------------------------------------------------
    // Typed where_condition predicates
    // -------------------------------------------------------------------

    "equal - varchar column - generates correct SQL"_test = [] {
        auto const cond = equal<asset::ticker>(asset::ticker{"AAPL"});
        expect(cond.build_sql() == "ticker = 'AAPL'"s) << cond.build_sql();
    };

    "equal - uint column - generates correct SQL"_test = [] {
        auto const cond = equal<asset::id>(5u);
        expect(cond.build_sql() == "id = 5"s) << cond.build_sql();
    };

    "equal - optional uint column - generates correct SQL"_test = [] {
        auto const cond = equal<asset::exchange_id>(2u);
        expect(cond.build_sql() == "exchange_id = 2"s) << cond.build_sql();
    };

    "not_equal - generates correct SQL"_test = [] {
        auto const cond = not_equal<asset::ticker>(asset::ticker{"AAPL"});
        expect(cond.build_sql() == "ticker != 'AAPL'"s) << cond.build_sql();
    };

    "less_than - generates correct SQL"_test = [] {
        auto const cond = less_than<asset::id>(100u);
        expect(cond.build_sql() == "id < 100"s) << cond.build_sql();
    };

    "greater_than - generates correct SQL"_test = [] {
        auto const cond = greater_than<asset::id>(0u);
        expect(cond.build_sql() == "id > 0"s) << cond.build_sql();
    };

    "less_than_or_equal - generates correct SQL"_test = [] {
        auto const cond = less_than_or_equal<asset::id>(50u);
        expect(cond.build_sql() == "id <= 50"s) << cond.build_sql();
    };

    "greater_than_or_equal - generates correct SQL"_test = [] {
        auto const cond = greater_than_or_equal<asset::id>(1u);
        expect(cond.build_sql() == "id >= 1"s) << cond.build_sql();
    };

    "is_null - generates correct SQL"_test = [] {
        auto const cond = is_null<asset::sector>();
        expect(cond.build_sql() == "sector IS NULL"s) << cond.build_sql();
    };

    "is_not_null - generates correct SQL"_test = [] {
        auto const cond = is_not_null<asset::sector>();
        expect(cond.build_sql() == "sector IS NOT NULL"s) << cond.build_sql();
    };

    "like - generates correct SQL"_test = [] {
        auto const cond = like<asset::ticker>("AAPL%");
        expect(cond.build_sql() == "ticker LIKE 'AAPL%'"s) << cond.build_sql();
    };

    "and_ - generates correct SQL"_test = [] {
        auto const cond = and_(equal<asset::exchange_id>(1u), greater_than<asset::id>(0u));
        expect(cond.build_sql() == "(exchange_id = 1 AND id > 0)"s) << cond.build_sql();
    };

    "or_ - generates correct SQL"_test = [] {
        auto const cond = or_(equal<asset::ticker>(asset::ticker{"AAPL"}), equal<asset::ticker>(asset::ticker{"MSFT"}));
        expect(cond.build_sql() == "(ticker = 'AAPL' OR ticker = 'MSFT')"s) << cond.build_sql();
    };

    "not_ - generates correct SQL"_test = [] {
        auto const cond = not_(equal<asset::ticker>(asset::ticker{"AAPL"}));
        expect(cond.build_sql() == "NOT (ticker = 'AAPL')"s) << cond.build_sql();
    };

    "update - typed where with equal - generates correct SQL"_test = [] {
        auto const sql = update<asset>().set(asset::ticker{"MSFT"}).where(equal<asset::id>(1u)).build_sql();
        expect(sql == "UPDATE asset SET ticker = 'MSFT' WHERE id = 1"s) << sql;
    };

    "delete_from - typed where with equal - generates correct SQL"_test = [] {
        auto const sql = delete_from<asset>().where(equal<asset::id>(1u)).build_sql();
        expect(sql == "DELETE FROM asset WHERE id = 1"s) << sql;
    };

    "count - typed where with is_null - generates correct SQL"_test = [] {
        auto const sql = count<asset>().where(is_null<asset::sector>()).build_sql();
        expect(sql == "SELECT COUNT(*) FROM asset WHERE sector IS NULL"s) << sql;
    };

    "count - compound typed where - generates correct SQL"_test = [] {
        auto const sql =
            count<asset>().where(and_(equal<asset::exchange_id>(1u), is_not_null<asset::sector>())).build_sql();
        expect(sql == "SELECT COUNT(*) FROM asset WHERE (exchange_id = 1 AND sector IS NOT NULL)"s) << sql;
    };

    "delete - typed where with numeric literal - generates correct SQL"_test = [] {
        auto const sql = delete_from<asset>().where(equal<asset::id>(42u)).build_sql();
        expect(sql == "DELETE FROM asset WHERE id = 42"s) << sql;
    };

    // -------------------------------------------------------------------
    // truncate_table<T>
    // -------------------------------------------------------------------

    "truncate_table - generates TRUNCATE TABLE"_test = [] {
        auto const sql = truncate_table<asset>().build_sql();
        expect(sql == "TRUNCATE TABLE asset"s) << sql;
    };

    // -------------------------------------------------------------------
    // insert_into<T> bulk values
    // -------------------------------------------------------------------

    "insert_into bulk - two rows - generates VALUES (...), (...)"_test = [] {
        asset r1;
        r1.exchange_id_ = 1u;
        r1.ticker_ = "AAPL";
        r1.instrument_ = "Stock";
        asset r2;
        r2.exchange_id_ = 2u;
        r2.ticker_ = "MSFT";
        r2.instrument_ = "Stock";
        auto const sql = insert_into<asset>().values(std::vector{r1, r2}).build_sql();
        expect(sql ==
               "INSERT INTO asset (id, exchange_id, ticker, instrument, name, sector, currency, created_date, "
               "last_updated_date) VALUES "
               "(0, 1, 'AAPL', 'Stock', NULL, NULL, NULL, NOW(), NOW()), "
               "(0, 2, 'MSFT', 'Stock', NULL, NULL, NULL, NOW(), NOW())"s)
            << sql;
    };

    // -------------------------------------------------------------------
    // on_duplicate_key_update
    // -------------------------------------------------------------------

    "on_duplicate_key_update - appends upsert clause"_test = [] {
        asset row;
        row.exchange_id_ = 1u;
        row.ticker_ = "AAPL";
        row.instrument_ = "Stock";
        auto const sql = insert_into<asset>()
                             .values(row)
                             .on_duplicate_key_update(asset::ticker{"AAPL"}, asset::instrument{"Stock"})
                             .build_sql();
        expect(sql ==
               "INSERT INTO asset (id, exchange_id, ticker, instrument, name, sector, currency, created_date, "
               "last_updated_date) "
               "VALUES (0, 1, 'AAPL', 'Stock', NULL, NULL, NULL, NOW(), NOW()) "
               "ON DUPLICATE KEY UPDATE ticker = 'AAPL', instrument = 'Stock'"s)
            << sql;
    };

    // -------------------------------------------------------------------
    // insert_into<T> with empty vector
    // -------------------------------------------------------------------

    "insert_into - empty vector - generates INSERT with empty VALUES"_test = [] {
        auto const sql = insert_into<asset>().values(std::vector<asset>{}).build_sql();
        expect(sql ==
               "INSERT INTO asset (id, exchange_id, ticker, instrument, name, sector, currency, created_date, "
               "last_updated_date) VALUES ()"s)
            << sql;
    };

    // -------------------------------------------------------------------
    // update<T> with multiple SET columns
    // -------------------------------------------------------------------

    "update - set multiple columns - generates comma-separated SET"_test = [] {
        auto const sql = update<asset>().set(asset::ticker{"MSFT"}, asset::instrument{"Software"}).build_sql();
        expect(sql == "UPDATE asset SET ticker = 'MSFT', instrument = 'Software'"s) << sql;
    };

    "update - set three columns - generates all columns in SET"_test = [] {
        auto const sql = update<asset>()
                             .set(asset::ticker{"AMZN"}, asset::instrument{"E-Commerce"}, asset::currency{"USD"})
                             .build_sql();
        expect(sql == "UPDATE asset SET ticker = 'AMZN', instrument = 'E-Commerce', currency = 'USD'"s) << sql;
    };
};
