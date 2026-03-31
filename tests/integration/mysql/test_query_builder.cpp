#include <boost/ut.hpp>
#include <chrono>
#include <cstdlib>
#include <optional>
#include <string>
#include <vector>

#include "ds_mysql/async_query.hpp"
#include "ds_mysql/connection_pool.hpp"
#include "ds_mysql/mysql_connection.hpp"
#include "ut_expected_expect.hpp"

using namespace boost::ut;
using namespace std::string_literals;

namespace ds_mysql {

// ===================================================================
// Test fixtures
// ===================================================================

namespace {

struct trade {
    COLUMN_FIELD(id, uint32_t, column_attr::primary_key{}, column_attr::auto_increment{})
    COLUMN_FIELD(account_id, std::optional<uint32_t>)
    COLUMN_FIELD(code, varchar_type<32>)
    COLUMN_FIELD(type, varchar_type<64>)
    COLUMN_FIELD(name, std::optional<varchar_type<255>>)
    COLUMN_FIELD(category, std::optional<varchar_type<255>>)
    COLUMN_FIELD(currency, std::optional<varchar_type<32>>)
    COLUMN_FIELD(executed_at, datetime_type<>)
    COLUMN_FIELD(recorded_at, datetime_type<>)
};

[[nodiscard]] std::optional<mysql_config> mysql_config_from_env() {
    auto getenv_or = [](char const* key, char const* fallback = "") -> std::string {
#ifdef _MSC_VER
        char* v = nullptr;
        size_t len = 0;
        if (::_dupenv_s(&v, &len, key) != 0 || v == nullptr) {
            return fallback;
        }
        std::string result{v};
        std::free(v);
        return result;
#else
        char const* v = std::getenv(key);
        return v ? v : fallback;
#endif
    };

    const std::string host = getenv_or("DS_MYSQL_TEST_HOST");
    const std::string database = getenv_or("DS_MYSQL_TEST_DATABASE", "ds_mysql_test");
    const std::string user = getenv_or("DS_MYSQL_TEST_USER");
    const std::string password = getenv_or("DS_MYSQL_TEST_PASSWORD");
    const std::string port_str =
        getenv_or("DS_MYSQL_TEST_PORT", std::to_string(default_mysql_port.to_unsigned_int()).c_str());

    if (host.empty() || user.empty() || password.empty()) {
        return std::nullopt;
    }

    unsigned int parsed_port = 0;
    try {
        parsed_port = static_cast<unsigned int>(std::stoul(port_str));
    } catch (std::exception const&) {
        return std::nullopt;
    }

    return mysql_config{
        host_name{host},
        database_name{database},
        auth_credentials{user_name{user}, user_password{password}},
        port_number{parsed_port},
    };
}

template <typename F>
struct scope_guard {
    explicit scope_guard(F fn) : fn_(std::move(fn)) {
    }
    ~scope_guard() noexcept {
        fn_();
    }
    scope_guard(scope_guard const&) = delete;
    scope_guard& operator=(scope_guard const&) = delete;
    scope_guard(scope_guard&&) = delete;
    scope_guard& operator=(scope_guard&&) = delete;

private:
    F fn_;
};
template <typename F>
scope_guard(F) -> scope_guard<F>;

// A database struct registering `trade` for validate_database<> tests.
struct trade_db : ds_mysql::database_schema {
    using tables = std::tuple<trade>;
};

// A struct with only two columns; its auto-derived table name is "trade_few_columns".
// The DB table is created (via raw SQL) with 9 columns in the mismatch tests, so
// validate_table / validate_database will detect the count discrepancy.
struct trade_few_columns {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(code, varchar_type<32>)
};

// A database struct wrapping trade_few_columns for validate_database mismatch tests.
struct trade_few_columns_db : ds_mysql::database_schema {
    using tables = std::tuple<trade_few_columns>;
};

// A second table used for multi-table validate_database tests.
struct account {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(name, varchar_type<64>)
};

// A database struct with two tables for multi-table validation tests.
struct multi_table_db : ds_mysql::database_schema {
    using tables = std::tuple<trade, account>;
};

struct temporal_precision_trade {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(code, varchar_type<32>)
    COLUMN_FIELD(executed_at, datetime_type<>)
    COLUMN_FIELD(recorded_at, timestamp_type<>)
};

struct integer_width_table {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(count, int_type<11>)
    COLUMN_FIELD(flags, int_unsigned_type<10>)
    COLUMN_FIELD(big, bigint_type<20>)
    COLUMN_FIELD(big_flags, bigint_unsigned_type<20>)
    COLUMN_FIELD(opt_count, std::optional<int_type<>>)
};

}  // namespace

template <>
struct field_schema<temporal_precision_trade, 2> {
    static constexpr std::string_view name() {
        return temporal_precision_trade::executed_at::column_name();
    }

    static std::string sql_type() {
        return sql_type_format::datetime_type(6);
    }
};

template <>
struct field_schema<temporal_precision_trade, 3> {
    static constexpr std::string_view name() {
        return temporal_precision_trade::recorded_at::column_name();
    }

    static std::string sql_type() {
        return sql_type_format::timestamp_type(6);
    }
};

// ===================================================================
// DDL Integration — CREATE TABLE, DESCRIBE, DROP TABLE
// ===================================================================

suite<"DDL Integration"> ddl_integration_suite = [] {
    "create and describe table"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value())) << "Missing MySQL environment variables";

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(drop_table(trade{}).if_exists()).has_value()) << "Failed to drop table";
        expect(db->execute(create_table(trade{})).has_value()) << "Failed to create trade table";

        auto const describe_result = db->query(describe(trade{}));
        expect(fatal(describe_result.has_value())) << "DESCRIBE trade failed";
        expect(describe_result->size() == 9u) << "trade should have 9 columns";
    };

    "create table if not exists is idempotent"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
    };

    "drop table if exists then create table via chaining"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(drop_table(trade{}).if_exists().then().create_table(trade{})).has_value())
            << "Chained DROP/CREATE should succeed";

        auto const result = db->query(count(trade{}));
        expect(fatal(result.has_value()));
        expect(result->size() == 1u) << "Should get count result";
    };
};

// ===================================================================
// INSERT Integration
// ===================================================================

suite<"INSERT Integration"> insert_integration_suite = [] {
    "insert single row and verify"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(drop_table(trade{}).if_exists().then().create_table(trade{})).has_value());

        trade row;
        row.code_ = "AAPL";
        row.type_ = "Stock";
        row.name_ = "Apple Inc.";
        row.category_ = "Technology";
        row.currency_ = "USD";
        expect(db->execute(insert_into(trade{}).values(row)).has_value()) << "Insert should succeed";

        auto const count_result = db->query(count(trade{}));
        expect(fatal(count_result.has_value()));
        expect(fatal(!count_result->empty()));
        expect(std::get<0>((*count_result)[0]) == 1u) << "Should have 1 row after INSERT";
    };

    "insert multiple rows"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(drop_table(trade{}).if_exists().then().create_table(trade{})).has_value());

        for (auto const& code : {varchar_type<32>{"AAPL"}, varchar_type<32>{"GOOGL"}, varchar_type<32>{"MSFT"}}) {
            trade row;
            row.code_ = code;
            row.type_ = "Stock";
            expect(db->execute(insert_into(trade{}).values(row)).has_value());
        }

        auto const count_result = db->query(count(trade{}));
        expect(fatal(count_result.has_value()));
        expect(std::get<0>((*count_result)[0]) == 3u) << "Should have 3 rows";
    };

    "insert temporal values with configured precision and verify stored fractions"_test = [] {
        using namespace std::chrono;

        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(temporal_precision_trade{}).if_exists()));
        }};

        expect(db->execute(drop_table(temporal_precision_trade{}).if_exists()).has_value());
        expect(db->execute(create_table(temporal_precision_trade{})).has_value());

        temporal_precision_trade row;
        row.code_ = "PREC";
        auto const tp = system_clock::time_point{sys_days{year{2024} / January / 1}} + microseconds{987654};
        row.executed_at_ = datetime_type<>{tp, 3};
        row.recorded_at_ = timestamp_type<>{tp, 2};

        expect(db->execute(insert_into(temporal_precision_trade{}).values(row)).has_value());

        auto const results =
            db->query(select(date_format_of<temporal_precision_trade::executed_at>("%Y-%m-%d %H:%i:%s.%f"),
                             date_format_of<temporal_precision_trade::recorded_at>("%Y-%m-%d %H:%i:%s.%f"))
                          .from(temporal_precision_trade{})
                          .where(equal<temporal_precision_trade::code>("PREC"))
                          .limit(1));

        expect(fatal(results.has_value()));
        expect(fatal(results->size() == 1u));
        expect(std::get<0>((*results)[0]) == "2024-01-01 00:00:00.987000") << std::get<0>((*results)[0]);
        expect(std::get<1>((*results)[0]) == "2024-01-01 00:00:00.980000") << std::get<1>((*results)[0]);
    };
};

// ===================================================================
// SELECT Integration
// ===================================================================

suite<"SELECT Integration"> select_integration_suite = [] {
    "select specific columns"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(delete_from(trade{})).has_value());

        for (auto const& code : {varchar_type<32>{"AAPL"}, varchar_type<32>{"GOOGL"}, varchar_type<32>{"MSFT"}}) {
            trade row;
            row.code_ = code;
            row.type_ = "Stock";
            expect(db->execute(insert_into(trade{}).values(row)).has_value());
        }

        auto const results = db->query(select(trade::id{}, trade::code{}).from(trade{}));
        expect(fatal(results.has_value()));
        expect(results->size() == 3u) << "Should have 3 trades";
        expect(std::get<1>((*results)[0]) == "AAPL") << "First code should be AAPL";
        expect(std::get<1>((*results)[1]) == "GOOGL") << "Second code should be GOOGL";
        expect(std::get<1>((*results)[2]) == "MSFT") << "Third code should be MSFT";
    };

    "select of all declared columns returns full row in declared order"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(delete_from(trade{})).has_value());

        trade row;
        row.code_ = "AAPL";
        row.type_ = "Stock";
        row.name_ = "Apple Inc.";
        row.category_ = "Technology";
        row.currency_ = "USD";
        expect(db->execute(insert_into(trade{}).values(row)).has_value());

        auto const results = db->query(select(trade::id{}, trade::account_id{}, trade::code{}, trade::type{},
                                              trade::name{}, trade::category{}, trade::currency{},
                                              date_format_of<trade::executed_at>("%Y-%m-%d %H:%i:%s"),
                                              date_format_of<trade::recorded_at>("%Y-%m-%d %H:%i:%s"))
                                           .from(trade{})
                                           .order_by(trade::id{})
                                           .limit(1));
        expect(fatal(results.has_value()));
        expect(results->size() == 1u);

        auto const& [id, account_id, code, type, name, category, currency, executed_at, recorded_at] = (*results)[0];
        expect(id == 1u) << id;
        expect(!account_id.has_value());
        expect(code == varchar_type<32>{"AAPL"});
        expect(type == varchar_type<64>{"Stock"});
        expect(name.has_value());
        expect(name.value() == varchar_type<255>{"Apple Inc."});
        expect(category.has_value());
        expect(category.value() == varchar_type<255>{"Technology"});
        expect(currency.has_value());
        expect(currency.value() == varchar_type<32>{"USD"});
        expect(!executed_at.empty());
        expect(!recorded_at.empty());
    };

    "select with equal WHERE clause"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(delete_from(trade{})).has_value());
        for (auto const& code : {varchar_type<32>{"AAPL"}, varchar_type<32>{"GOOGL"}}) {
            trade row;
            row.code_ = code;
            row.type_ = "Stock";
            expect(db->execute(insert_into(trade{}).values(row)).has_value());
        }

        auto const results = db->query(select(trade::code{}).from(trade{}).where(equal<trade::code>("AAPL")));
        expect(fatal(results.has_value()));
        expect(results->size() == 1u) << "Should find only AAPL";
        expect(std::get<0>((*results)[0]) == "AAPL");
    };

    "select with category WHERE clause"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(delete_from(trade{})).has_value());

        trade aapl;
        aapl.code_ = "AAPL";
        aapl.type_ = "Stock";
        aapl.category_ = "Technology";
        expect(db->execute(insert_into(trade{}).values(aapl)).has_value());

        trade googl;
        googl.code_ = "GOOGL";
        googl.type_ = "Stock";
        googl.category_ = "Technology";
        expect(db->execute(insert_into(trade{}).values(googl)).has_value());

        auto const rows =
            db->query(select(trade::code{}, trade::name{}).from(trade{}).where(equal<trade::category>("Technology")));
        expect(fatal(rows.has_value()));
        expect(rows->size() == 2u) << "Should have 2 technology trades";
    };

    "select with LIMIT"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(delete_from(trade{})).has_value());
        for (auto const& code : {varchar_type<32>{"AAPL"}, varchar_type<32>{"GOOGL"}, varchar_type<32>{"MSFT"},
                                 varchar_type<32>{"AMZN"}, varchar_type<32>{"TSLA"}}) {
            trade row;
            row.code_ = code;
            row.type_ = "Stock";
            expect(db->execute(insert_into(trade{}).values(row)).has_value());
        }

        auto const results = db->query(select(trade::id{}, trade::code{}).from(trade{}).limit(3));
        expect(fatal(results.has_value()));
        expect(results->size() <= 3u) << "LIMIT 3 should return at most 3 rows";

        for (auto const& result : *results) {
            expect(!std::get<1>(result).empty()) << "Code should not be empty";
        }
    };
};

// ===================================================================
// UPDATE Integration
// ===================================================================

suite<"UPDATE Integration"> update_integration_suite = [] {
    "update single field and verify"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(delete_from(trade{})).has_value());

        trade row;
        row.code_ = varchar_type<32>{"AAPL"};
        row.type_ = varchar_type<64>{"Stock"};
        row.name_ = varchar_type<255>{"Apple Inc."};
        expect(db->execute(insert_into(trade{}).values(row)).has_value());

        expect(db->execute(update(trade{})
                               .set(trade::name{varchar_type<255>{"Apple Corporation"}})
                               .where(equal<trade::code>("AAPL")))
                   .has_value())
            << "Update should succeed";

        auto const updated_count =
            db->query(count(trade{}).where(and_(equal<trade::code>("AAPL"), equal<trade::name>("Apple Corporation"))));
        expect(fatal(updated_count.has_value()));
        expect(fatal(!updated_count->empty()));
        expect(std::get<0>((*updated_count)[0]) == 1u) << "Name should be updated";
    };

    "update type field and verify"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(delete_from(trade{})).has_value());

        trade row;
        row.code_ = "AAPL";
        row.type_ = "Stock";
        expect(db->execute(insert_into(trade{}).values(row)).has_value());

        expect(
            db->execute(update(trade{}).set<trade::type>("Tech Stock").where(equal<trade::code>("AAPL"))).has_value());

        auto const results = db->query(select(trade::type{}).from(trade{}).where(equal<trade::code>("AAPL")));
        expect(fatal(results.has_value()));
        expect(results->size() >= 1u);
        expect(std::get<0>((*results)[0]) == "Tech Stock");
    };
};

// ===================================================================
// DELETE Integration
// ===================================================================

suite<"DELETE Integration"> delete_integration_suite = [] {
    "delete single row and verify"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(delete_from(trade{})).has_value());
        for (auto const& code : {varchar_type<32>{"AAPL"}, varchar_type<32>{"GOOGL"}, varchar_type<32>{"MSFT"}}) {
            trade row;
            row.code_ = code;
            row.type_ = "Stock";
            expect(db->execute(insert_into(trade{}).values(row)).has_value());
        }

        expect(db->execute(delete_from(trade{}).where(equal<trade::code>("AAPL"))).has_value());

        auto const results = db->query(select(trade::code{}).from(trade{}));
        expect(fatal(results.has_value()));
        expect(results->size() == 2u) << "Should have 2 trades left";

        bool found_aapl = false;
        for (auto const& result : *results) {
            if (std::get<0>(result) == "AAPL"s) {
                found_aapl = true;
            }
        }
        expect(!found_aapl) << "AAPL should be deleted";
    };

    "delete all rows"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(delete_from(trade{})).has_value());

        auto const count_result = db->query(count(trade{}));
        expect(fatal(count_result.has_value()));
        expect(std::get<0>((*count_result)[0]) == 0u) << "Table should be empty";
    };
};

// ===================================================================
// WHERE Operator Syntax Integration
// ===================================================================

suite<"WHERE Operator Syntax Integration"> where_operator_integration_suite = [] {
    "col_ref == filters correctly"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(delete_from(trade{})).has_value());
        for (auto const& code : {varchar_type<32>{"AAPL"}, varchar_type<32>{"GOOGL"}, varchar_type<32>{"MSFT"}}) {
            trade row;
            row.code_ = code;
            row.type_ = "Stock";
            expect(db->execute(insert_into(trade{}).values(row)).has_value());
        }

        auto const results = db->query(select(trade::code{}).from(trade{}).where(col_ref(trade::code{}) == "AAPL"));
        expect(fatal(results.has_value()));
        expect(results->size() == 1u) << "col_ref == should filter to AAPL only";
        expect(std::get<0>((*results)[0]) == "AAPL"s);
    };

    "col_ref != excludes correctly"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(delete_from(trade{})).has_value());
        for (auto const& code : {varchar_type<32>{"AAPL"}, varchar_type<32>{"GOOGL"}, varchar_type<32>{"MSFT"}}) {
            trade row;
            row.code_ = code;
            row.type_ = "Stock";
            expect(db->execute(insert_into(trade{}).values(row)).has_value());
        }

        auto const results = db->query(select(trade::code{}).from(trade{}).where(col_ref(trade::code{}) != "AAPL"));
        expect(fatal(results.has_value()));
        expect(results->size() == 2u) << "col_ref != should exclude AAPL";
        expect(std::get<0>((*results)[0]) == "GOOGL"s);
        expect(std::get<0>((*results)[1]) == "MSFT"s);
    };

    "OR operator combines conditions"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(delete_from(trade{})).has_value());
        for (auto const& code : {varchar_type<32>{"AAPL"}, varchar_type<32>{"GOOGL"}, varchar_type<32>{"MSFT"}}) {
            trade row;
            row.code_ = code;
            row.type_ = "Stock";
            expect(db->execute(insert_into(trade{}).values(row)).has_value());
        }

        auto const results =
            db->query(select(trade::code{})
                          .from(trade{})
                          .where((col_ref(trade::code{}) == "AAPL") | (col_ref(trade::code{}) == "GOOGL")));
        expect(fatal(results.has_value()));
        expect(results->size() == 2u) << "| should match AAPL or GOOGL";
        expect(std::get<0>((*results)[0]) == "AAPL"s);
        expect(std::get<0>((*results)[1]) == "GOOGL"s);
    };

    "AND operator narrows results"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(delete_from(trade{})).has_value());
        for (auto const& code : {varchar_type<32>{"AAPL"}, varchar_type<32>{"GOOGL"}, varchar_type<32>{"MSFT"}}) {
            trade row;
            row.code_ = code;
            row.type_ = "Stock";
            expect(db->execute(insert_into(trade{}).values(row)).has_value());
        }

        auto const results =
            db->query(select(trade::code{})
                          .from(trade{})
                          .where((col_ref(trade::code{}) != "AAPL") & (col_ref(trade::code{}) != "GOOGL")));
        expect(fatal(results.has_value()));
        expect(results->size() == 1u) << "& should leave only MSFT";
        expect(std::get<0>((*results)[0]) == "MSFT"s);
    };

    "NOT operator negates condition"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(delete_from(trade{})).has_value());
        for (auto const& code : {varchar_type<32>{"AAPL"}, varchar_type<32>{"GOOGL"}, varchar_type<32>{"MSFT"}}) {
            trade row;
            row.code_ = code;
            row.type_ = "Stock";
            expect(db->execute(insert_into(trade{}).values(row)).has_value());
        }

        auto const results = db->query(select(trade::code{}).from(trade{}).where(!(col_ref(trade::code{}) == "AAPL")));
        expect(fatal(results.has_value()));
        expect(results->size() == 2u) << "! should exclude AAPL";
        expect(std::get<0>((*results)[0]) == "GOOGL"s);
        expect(std::get<0>((*results)[1]) == "MSFT"s);
    };

    "parenthesised priority (a | b) & c"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(delete_from(trade{})).has_value());

        trade aapl;
        aapl.code_ = "AAPL";
        aapl.type_ = "Bond";
        expect(db->execute(insert_into(trade{}).values(aapl)).has_value());

        trade googl;
        googl.code_ = "GOOGL";
        googl.type_ = "Stock";
        expect(db->execute(insert_into(trade{}).values(googl)).has_value());

        auto const results =
            db->query(select(trade::code{})
                          .from(trade{})
                          .where(((col_ref(trade::code{}) == "AAPL") | (col_ref(trade::code{}) == "GOOGL")) &
                                 (col_ref(trade::type{}) == "Bond")));
        expect(fatal(results.has_value()));
        expect(results->size() == 1u) << "(a | b) & c priority: only AAPL should survive";
        expect(std::get<0>((*results)[0]) == "AAPL"s);
    };
};

// ===================================================================
// Schema Validation Integration
// ===================================================================

suite<"Schema Validation Integration"> schema_validation_suite = [] {
    "validate_table succeeds when table matches C++ struct"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());

        auto const result = db->validate_table<trade>();
        expect(result.has_value()) << "validate_table<trade> should succeed — " +
                                          (result.has_value() ? "" : result.error());
    };

    "validate_table instance-based succeeds when table matches C++ struct"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());

        auto const result = db->validate_table(trade{});
        expect(result.has_value()) << "validate_table(trade{}) should succeed — " +
                                          (result.has_value() ? "" : result.error());
    };

    "validate_table fails when table does not exist"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(drop_table(trade{}).if_exists()).has_value());

        auto const result = db->validate_table<trade>();
        expect(!result.has_value()) << "validate_table<trade> should fail when table is absent";
    };

    "validate_table detects column count mismatch"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade_few_columns{}).if_exists()));
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        // Create the trade_few_columns DB table with the same 9 columns as trade —
        // the C++ struct only defines 2, so validate_table must detect the count mismatch.
        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(drop_table(trade_few_columns{}).if_exists()).has_value());
        expect(db->execute(create_table(trade_few_columns{}).like<trade>()).has_value());

        auto const result = db->validate_table<trade_few_columns>();
        expect(!result.has_value()) << "validate_table should fail: 2-column struct vs 9-column DB table";
        expect(result.has_value() || result.error().find("column count") != std::string::npos)
            << "Error should mention column count — got: " + (result.has_value() ? "" : result.error());
    };

    "validate_database succeeds when all registered tables match"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());

        auto const result = db->validate_database<trade_db>();
        expect(result.has_value()) << "validate_database<trade_db> should succeed — " +
                                          (result.has_value() ? "" : result.error());
    };

    "validate_database instance-based succeeds when all registered tables match"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());

        auto const result = db->validate_database(trade_db{});
        expect(result.has_value()) << "validate_database(trade_db{}) should succeed — " +
                                          (result.has_value() ? "" : result.error());
    };

    "validate_database reports error when a table is absent"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(drop_table(trade{}).if_exists()).has_value());

        auto const result = db->validate_database<trade_db>();
        expect(!result.has_value()) << "validate_database<trade_db> should fail when trade table is missing";
    };

    "validate_database reports error when a table column count mismatch"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade_few_columns{}).if_exists()));
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        // Ensure trade_few_columns DB table has 9 columns; the C++ struct defines only 2.
        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(drop_table(trade_few_columns{}).if_exists()).has_value());
        expect(db->execute(create_table(trade_few_columns{}).like<trade>()).has_value());

        auto const result = db->validate_database<trade_few_columns_db>();
        expect(!result.has_value()) << "validate_database<trade_few_columns_db> should fail on column count mismatch";
        expect(result.has_value() || result.error().find("column count") != std::string::npos)
            << "Error should mention column count — got: " + (result.has_value() ? "" : result.error());
    };

    "validate_database succeeds with multiple tables"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
            (void)(db->execute(drop_table(account{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(create_table(account{}).if_not_exists()).has_value());

        auto const result = db->validate_database<multi_table_db>();
        expect(result.has_value()) << "validate_database<multi_table_db> should succeed — " +
                                          (result.has_value() ? "" : result.error());
    };

    "validate_database reports error when one of multiple tables is absent"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
            (void)(db->execute(drop_table(account{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(drop_table(account{}).if_exists()).has_value());

        auto const result = db->validate_database<multi_table_db>();
        expect(!result.has_value()) << "validate_database<multi_table_db> should fail when account table is absent";
    };
};

// ===================================================================
// Connection Error
// ===================================================================

suite<"Connection Error"> connection_error_suite = [] {
    "connect with explicit parameters succeeds"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db =
            mysql_connection::connect(config->host(), config->database(), config->credentials(), config->port());
        expect(fatal(db)) << "Explicit overload should succeed with valid connection parameters";

        auto const rows = db->query(sql_raw{"SELECT 1"});
        expect(fatal(rows.has_value()));
        expect(rows->size() == 1u);
        expect((*rows)[0].size() == 1u);
        expect((*rows)[0][0].has_value());
        expect((*rows)[0][0].value() == "1"s);
    };

    "connect with wrong credentials fails"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        // Use the same host/port as valid config but wrong credentials.
        auto const bad_db = mysql_connection::connect(
            config->host(), config->database(),
            auth_credentials{user_name{"nonexistent_user___"}, user_password{"wrong_pass___"}}, config->port());
        expect(!bad_db.has_value()) << "Should fail with wrong credentials";
        expect(!bad_db.has_value() || bad_db.error().empty() == false) << "Error message should not be empty";
    };
};

// ===================================================================
// Execute Failure
// ===================================================================

suite<"Execute Failure"> execute_failure_suite = [] {
    "execute fails when table already exists"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(drop_table(trade{}).if_exists()).has_value());
        expect(db->execute(create_table(trade{})).has_value());

        // Creating the same table again without IF NOT EXISTS triggers MySQL error 1050.
        auto const fail = db->execute(create_table(trade{}));
        expect(!fail.has_value()) << "Should fail: table already exists";
        expect(fail.has_value() || !fail.error().empty()) << "Error should not be empty";

        expect(db->execute(drop_table(trade{}).if_exists()).has_value());
    };

    "execute error includes the failing SQL statement"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(drop_table(trade{}).if_exists()).has_value());
        expect(db->execute(create_table(trade{})).has_value());

        auto const fail = db->execute(create_table(trade{}));
        expect(!fail.has_value()) << "Should fail: table already exists";
        expect(fail.has_value() || fail.error().find("Statement:") != std::string::npos)
            << "Error should include the failing statement — got: " + (fail.has_value() ? "" : fail.error());
        expect(fail.has_value() || fail.error().find("CREATE TABLE") != std::string::npos)
            << "Error should include the CREATE TABLE SQL — got: " + (fail.has_value() ? "" : fail.error());
    };

    "execute error on multi-statement SQL identifies the failing table"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
            (void)(db->execute(drop_table(account{}).if_exists()));
        }};

        // Create trade so that create_all_tables for multi_table_db fails on trade.
        expect(db->execute(drop_table(trade{}).if_exists()).has_value());
        expect(db->execute(drop_table(account{}).if_exists()).has_value());
        expect(db->execute(create_table(trade{})).has_value());

        auto const fail = db->execute(create_all_tables(multi_table_db{}));
        expect(!fail.has_value()) << "Should fail: trade table already exists";
        expect(fail.has_value() || fail.error().find("trade") != std::string::npos)
            << "Error should name the failing table — got: " + (fail.has_value() ? "" : fail.error());
    };
};

// ===================================================================
// Raw SQL DML via query() — covers query_raw_nullable DDL/DML path
// ===================================================================

suite<"Raw SQL DML"> raw_sql_dml_suite = [] {
    "query(sql_raw DML) returns empty result set"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());

        // A DML statement via sql_raw triggers the DDL/DML no-result-set path.
        auto const result = db->query(sql_raw{"DELETE FROM trade WHERE id = 0"});
        expect(result.has_value()) << "DML via sql_raw should succeed";
        expect(result->empty()) << "DML result should be empty (no result set)";
    };

    "query(sql_raw DDL) returns empty result set"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->query(sql_raw{"DROP TABLE IF EXISTS raw_ddl_test"}));
        }};

        // DDL via sql_raw also returns no result set.
        auto const result =
            db->query(sql_raw{"CREATE TABLE IF NOT EXISTS raw_ddl_test (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY)"});
        expect(result.has_value()) << "DDL via sql_raw should succeed";
        expect(result->empty()) << "DDL result should be empty (no result set)";

        auto const drop = db->query(sql_raw{"DROP TABLE IF EXISTS raw_ddl_test"});
        expect(drop.has_value());
    };
};

// ===================================================================
// Boolean column validation — covers normalize_cpp_sql_type("BOOLEAN")
// ===================================================================

namespace {
struct bool_table {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(is_active, bool)
};
}  // namespace

suite<"Boolean Column Coverage"> boolean_column_suite = [] {
    "validate_table with bool column — normalize_cpp_sql_type maps BOOLEAN to TINYINT(1)"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(bool_table{}).if_exists()));
        }};

        expect(db->execute(drop_table(bool_table{}).if_exists()).has_value());
        expect(db->execute(create_table(bool_table{})).has_value());

        auto const result = db->validate_table<bool_table>();
        expect(result.has_value()) << "validate_table<bool_table> should succeed — " +
                                          (result.has_value() ? "" : result.error());

        expect(db->execute(drop_table(bool_table{}).if_exists()).has_value());
    };
};

// ===================================================================
// validate_field error path coverage — name, type, nullability mismatches
// ===================================================================

namespace {
// C++ struct matching the layout of the tables created in validate_field tests.
struct mismatch_table {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(right_name, varchar_type<32>)
};

struct type_mismatch_struct {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(code, varchar_type<32>)  // expects VARCHAR(32)
};

struct nullability_mismatch_struct {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(code, varchar_type<32>)  // expects NOT NULL
};
}  // namespace

template <>
struct table_name_for<mismatch_table> {
    static constexpr table_name value() noexcept {
        return table_name{"mismatch_name_tbl"};
    }
};

template <>
struct table_name_for<type_mismatch_struct> {
    static constexpr table_name value() noexcept {
        return table_name{"type_mismatch_tbl"};
    }
};

template <>
struct table_name_for<nullability_mismatch_struct> {
    static constexpr table_name value() noexcept {
        return table_name{"nullable_mismatch_tbl"};
    }
};

suite<"validate_field Error Paths"> validate_field_errors_suite = [] {
    "validate_table detects column name mismatch"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->query(sql_raw{"DROP TABLE IF EXISTS mismatch_name_tbl"}));
        }};

        // Create a table where column 2 is 'wrong_col_name' but struct expects 'right_name'.
        auto drop = db->query(sql_raw{"DROP TABLE IF EXISTS mismatch_name_tbl"});
        expect(drop.has_value());
        auto create =
            db->query(sql_raw{"CREATE TABLE mismatch_name_tbl "
                              "(id INT UNSIGNED NOT NULL AUTO_INCREMENT, "
                              "wrong_col_name VARCHAR(32) NOT NULL, "
                              "PRIMARY KEY (id))"});
        expect(create.has_value());

        auto const result = db->validate_table<mismatch_table>();
        expect(!result.has_value()) << "validate_table should fail on name mismatch";
        expect(!result.has_value() && result.error().find("name expected") != std::string::npos)
            << "Error should mention 'name expected' — got: " + (result.has_value() ? "" : result.error());

        auto cleanup = db->query(sql_raw{"DROP TABLE IF EXISTS mismatch_name_tbl"});
        expect(cleanup.has_value());
    };

    "validate_table detects column type mismatch"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->query(sql_raw{"DROP TABLE IF EXISTS type_mismatch_tbl"}));
        }};

        // Create a table where 'code' is INT but struct expects VARCHAR(32).
        auto drop = db->query(sql_raw{"DROP TABLE IF EXISTS type_mismatch_tbl"});
        expect(drop.has_value());
        auto create =
            db->query(sql_raw{"CREATE TABLE type_mismatch_tbl "
                              "(id INT UNSIGNED NOT NULL AUTO_INCREMENT, "
                              "code INT NOT NULL, "
                              "PRIMARY KEY (id))"});
        expect(create.has_value());

        auto const result = db->validate_table<type_mismatch_struct>();
        expect(!result.has_value()) << "validate_table should fail on type mismatch";
        expect(!result.has_value() && result.error().find("type expected") != std::string::npos)
            << "Error should mention 'type expected' — got: " + (result.has_value() ? "" : result.error());

        auto cleanup = db->query(sql_raw{"DROP TABLE IF EXISTS type_mismatch_tbl"});
        expect(cleanup.has_value());
    };

    "validate_table detects nullability mismatch"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->query(sql_raw{"DROP TABLE IF EXISTS nullable_mismatch_tbl"}));
        }};

        // Create a table where 'code' is nullable but struct expects NOT NULL.
        auto drop = db->query(sql_raw{"DROP TABLE IF EXISTS nullable_mismatch_tbl"});
        expect(drop.has_value());
        auto create =
            db->query(sql_raw{"CREATE TABLE nullable_mismatch_tbl "
                              "(id INT UNSIGNED NOT NULL AUTO_INCREMENT, "
                              "code VARCHAR(32) NULL, "
                              "PRIMARY KEY (id))"});
        expect(create.has_value());

        auto const result = db->validate_table<nullability_mismatch_struct>();
        expect(!result.has_value()) << "validate_table should fail on nullability mismatch";
        expect(!result.has_value() && result.error().find("nullability expected") != std::string::npos)
            << "Error should mention 'nullability expected' — got: " + (result.has_value() ? "" : result.error());

        auto cleanup = db->query(sql_raw{"DROP TABLE IF EXISTS nullable_mismatch_tbl"});
        expect(cleanup.has_value());
    };
};

// ===================================================================
// Typed query column count mismatch — covers mysql_connection::query() guard
// ===================================================================

namespace {
struct one_col_query {
    using result_row_type = std::tuple<uint32_t>;

    [[nodiscard]] std::string build_sql() const {
        return "SELECT id, code FROM trade LIMIT 1";
    }
};
}  // namespace

// ===================================================================
// Integer wrapper type integration — int_type<W>, bigint_type<W>, etc.
// ===================================================================

suite<"Integer Type Integration"> integer_type_integration_suite = [] {
    "create and describe table with integer display-width columns"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value())) << "Missing MySQL environment variables";

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(integer_width_table{}).if_exists()));
        }};

        expect(db->execute(drop_table(integer_width_table{}).if_exists()).has_value());
        expect(db->execute(create_table(integer_width_table{})).has_value()) << "Failed to create integer_width_table";

        auto const describe_result = db->query(describe(integer_width_table{}));
        expect(fatal(describe_result.has_value())) << "DESCRIBE failed";
        expect(describe_result->size() == 6u) << "integer_width_table should have 6 columns";
    };

    "insert and select integer typed wrapper values round-trip"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(integer_width_table{}).if_exists()));
        }};

        expect(db->execute(create_table(integer_width_table{}).if_not_exists()).has_value());
        expect(db->execute(delete_from(integer_width_table{})).has_value());

        integer_width_table row;
        row.count_ = -42;
        row.flags_ = 100u;
        row.big_ = -9000000000LL;
        row.big_flags_ = 18000000000ULL;
        row.opt_count_ = std::nullopt;

        expect(db->execute(insert_into(integer_width_table{}).values(row)).has_value()) << "Insert should succeed";

        auto const results =
            db->query(select(integer_width_table::count{}, integer_width_table::flags{}, integer_width_table::big{},
                             integer_width_table::big_flags{}, integer_width_table::opt_count{})
                          .from(integer_width_table{})
                          .limit(1));

        expect(fatal(results.has_value()));
        expect(fatal(results->size() == 1u));
        auto const& [count, flags, big, big_flags, opt_count] = (*results)[0];
        expect(static_cast<int32_t>(count) == -42) << static_cast<int32_t>(count);
        expect(static_cast<uint32_t>(flags) == 100u) << static_cast<uint32_t>(flags);
        expect(static_cast<int64_t>(big) == -9000000000LL) << static_cast<int64_t>(big);
        expect(static_cast<uint64_t>(big_flags) == 18000000000ULL) << static_cast<uint64_t>(big_flags);
        expect(!opt_count.has_value()) << "opt_count should be NULL";
    };

    "nullable int_type round-trips value and null"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(integer_width_table{}).if_exists()));
        }};

        expect(db->execute(create_table(integer_width_table{}).if_not_exists()).has_value());
        expect(db->execute(delete_from(integer_width_table{})).has_value());

        integer_width_table with_value;
        with_value.count_ = 1;
        with_value.opt_count_ = int_type<>{7};
        expect(db->execute(insert_into(integer_width_table{}).values(with_value)).has_value());

        integer_width_table without_value;
        without_value.count_ = 2;
        without_value.opt_count_ = std::nullopt;
        expect(db->execute(insert_into(integer_width_table{}).values(without_value)).has_value());

        auto const results = db->query(select(integer_width_table::count{}, integer_width_table::opt_count{})
                                           .from(integer_width_table{})
                                           .order_by(integer_width_table::count{}));

        expect(fatal(results.has_value()));
        expect(fatal(results->size() == 2u));

        auto const& [count0, opt0] = (*results)[0];
        expect(static_cast<int32_t>(count0) == 1);
        expect(fatal(opt0.has_value()));
        expect(static_cast<int32_t>(*opt0) == 7) << static_cast<int32_t>(*opt0);

        auto const& [count1, opt1] = (*results)[1];
        expect(static_cast<int32_t>(count1) == 2);
        expect(!opt1.has_value()) << "Second row opt_count should be NULL";
    };

    // format<col, 0> wraps MySQL FORMAT(expr, 0) — adds thousands separators,
    // no decimal places. The integer column's display width (INT(11) etc.) is a
    // schema-only hint and does not affect the FORMAT output.
    "format<col, 0> on bigint column produces thousands-separated string"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(integer_width_table{}).if_exists()));
        }};

        expect(db->execute(create_table(integer_width_table{}).if_not_exists()).has_value());
        expect(db->execute(delete_from(integer_width_table{})).has_value());

        integer_width_table row;
        row.big_ = 1234567LL;
        expect(db->execute(insert_into(integer_width_table{}).values(row)).has_value());

        auto const results =
            db->query(select(format_to<integer_width_table::big>(0)).from(integer_width_table{}).limit(1));

        expect(fatal(results.has_value()));
        expect(fatal(results->size() == 1u));
        expect(std::get<0>((*results)[0]) == "1,234,567"s) << std::get<0>((*results)[0]);
    };
};

suite<"Typed Query Column Count Coverage"> typed_query_col_count_suite = [] {
    "query with mismatched column count returns error"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(trade{}).if_exists()));
        }};

        // Ensure at least one row exists so the result set is non-empty.
        expect(db->execute(create_table(trade{}).if_not_exists()).has_value());
        expect(db->execute(delete_from(trade{})).has_value());
        trade row;
        row.code_ = varchar_type<32>{"AAPL"};
        row.type_ = varchar_type<64>{"Stock"};
        expect(db->execute(insert_into(trade{}).values(row)).has_value());

        // one_col_query::result_row_type has 1 column but the SQL returns 2.
        auto const result = db->query(one_col_query{});
        expect(!result.has_value()) << "Should detect column count mismatch";
        expect(!result.has_value() && result.error().find("Column count mismatch") != std::string::npos)
            << "Error should mention 'Column count mismatch' — got: " + (result.has_value() ? "" : result.error());

        expect(db->execute(delete_from(trade{})).has_value());
    };
};

// ===================================================================
// last_insert_id / execute affected rows
// ===================================================================

struct auto_inc_row {
    COLUMN_FIELD(id, uint32_t, column_attr::primary_key{}, column_attr::auto_increment{})
    COLUMN_FIELD(label, varchar_type<64>)
};

suite<"last_insert_id Integration"> last_insert_id_suite = [] {
    "last_insert_id returns auto-generated id after INSERT"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(auto_inc_row{}).if_exists()));
        }};

        expect(db->execute(drop_table(auto_inc_row{}).if_exists()).has_value());
        expect(db->execute(create_table(auto_inc_row{})).has_value());

        auto_inc_row row;
        row.label_ = "first";
        expect(db->execute(insert_into(auto_inc_row{}).values(row)).has_value());

        auto const first_id = db->last_insert_id();
        expect(first_id >= 1u) << "first insert should produce id >= 1, got " + std::to_string(first_id);

        row.label_ = "second";
        expect(db->execute(insert_into(auto_inc_row{}).values(row)).has_value());

        auto const second_id = db->last_insert_id();
        expect(second_id == first_id + 1u) << "second id should be first + 1, got " + std::to_string(second_id);
    };

    "last_insert_id is zero after DDL"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(auto_inc_row{}).if_exists()));
        }};

        expect(db->execute(drop_table(auto_inc_row{}).if_exists()).has_value());
        expect(db->execute(create_table(auto_inc_row{})).has_value());

        expect(db->last_insert_id() == 0u) << "last_insert_id should be 0 after DDL";
    };

    "execute returns affected row count"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(auto_inc_row{}).if_exists()));
        }};

        expect(db->execute(drop_table(auto_inc_row{}).if_exists()).has_value());
        expect(db->execute(create_table(auto_inc_row{})).has_value());

        auto_inc_row row;
        row.label_ = "one";
        auto const insert_result = db->execute(insert_into(auto_inc_row{}).values(row));
        expect(fatal(insert_result.has_value()));
        expect(*insert_result == 1u) << "INSERT of 1 row should return 1, got " + std::to_string(*insert_result);

        auto const update_result = db->execute(
            update(auto_inc_row{}).set<auto_inc_row::label>("updated").where(equal<auto_inc_row::label>("one")));
        expect(fatal(update_result.has_value()));
        expect(*update_result == 1u) << "UPDATE of 1 row should return 1, got " + std::to_string(*update_result);

        auto const delete_result =
            db->execute(delete_from(auto_inc_row{}).where(equal<auto_inc_row::label>("updated")));
        expect(fatal(delete_result.has_value()));
        expect(*delete_result == 1u) << "DELETE of 1 row should return 1, got " + std::to_string(*delete_result);

        auto const empty_delete =
            db->execute(delete_from(auto_inc_row{}).where(equal<auto_inc_row::label>("nonexistent")));
        expect(fatal(empty_delete.has_value()));
        expect(*empty_delete == 0u) << "DELETE of 0 rows should return 0, got " + std::to_string(*empty_delete);
    };
};

// ===================================================================
// transaction_guard integration tests
// ===================================================================

suite<"transaction_guard Integration"> transaction_guard_integration_suite = [] {
    "transaction_guard commit persists data"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)db->execute(drop_table(trade{}).if_exists());
        }};

        expect(db->execute(drop_table(trade{}).if_exists()).has_value());
        expect(db->execute(create_table(trade{})).has_value());

        {
            auto guard = transaction_guard::begin(*db);
            expect(fatal(guard.has_value())) << "begin() should succeed";

            trade row{};
            row.code_ = varchar_type<32>{"AAPL"};
            row.type_ = varchar_type<64>{"Stock"};
            row.executed_at_ = datetime_type<>{};
            row.recorded_at_ = datetime_type<>{};
            expect(db->execute(insert_into(trade{}).values(row)).has_value());

            auto commit_result = guard->commit();
            expect(commit_result.has_value()) << "commit() should succeed";
            expect(guard->is_committed());
        }

        auto cnt = db->query(count(trade{}));
        expect(fatal(cnt.has_value()));
        expect(std::get<0>(cnt->at(0)) == 1u) << "committed row should persist";
    };

    "transaction_guard destructor rolls back uncommitted work"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)db->execute(drop_table(trade{}).if_exists());
        }};

        expect(db->execute(drop_table(trade{}).if_exists()).has_value());
        expect(db->execute(create_table(trade{})).has_value());

        {
            auto guard = transaction_guard::begin(*db);
            expect(fatal(guard.has_value()));

            trade row{};
            row.code_ = varchar_type<32>{"ROLLME"};
            row.type_ = varchar_type<64>{"Test"};
            row.executed_at_ = datetime_type<>{};
            row.recorded_at_ = datetime_type<>{};
            expect(db->execute(insert_into(trade{}).values(row)).has_value());
            // no commit — guard destructor should rollback
        }

        auto cnt = db->query(count(trade{}));
        expect(fatal(cnt.has_value()));
        expect(std::get<0>(cnt->at(0)) == 0u) << "uncommitted row should be rolled back";
    };

    "transaction_guard explicit rollback"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)db->execute(drop_table(trade{}).if_exists());
        }};

        expect(db->execute(drop_table(trade{}).if_exists()).has_value());
        expect(db->execute(create_table(trade{})).has_value());

        {
            auto guard = transaction_guard::begin(*db);
            expect(fatal(guard.has_value()));

            trade row{};
            row.code_ = varchar_type<32>{"EXPLICIT"};
            row.type_ = varchar_type<64>{"Test"};
            row.executed_at_ = datetime_type<>{};
            row.recorded_at_ = datetime_type<>{};
            expect(db->execute(insert_into(trade{}).values(row)).has_value());

            auto rb = guard->rollback();
            expect(rb.has_value()) << "explicit rollback should succeed";
        }

        auto cnt = db->query(count(trade{}));
        expect(fatal(cnt.has_value()));
        expect(std::get<0>(cnt->at(0)) == 0u) << "rolled-back row should not persist";
    };
};

// ===================================================================
// prepared_statement integration tests
// ===================================================================

suite<"prepared_statement Integration"> prepared_statement_integration_suite = [] {
    "prepare and execute DML — insert with parameters"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)db->execute(drop_table(trade{}).if_exists());
        }};

        expect(db->execute(drop_table(trade{}).if_exists()).has_value());
        expect(db->execute(create_table(trade{})).has_value());

        auto stmt = db->prepare("INSERT INTO trade (code, type, executed_at, recorded_at) VALUES (?, ?, NOW(), NOW())");
        expect(fatal(stmt.has_value())) << "prepare should succeed: " + (stmt.has_value() ? "" : stmt.error());
        expect(stmt->param_count() == 2u);

        auto result = stmt->execute(std::string{"AAPL"}, std::string{"Stock"});
        expect(fatal(result.has_value())) << "execute should succeed: " + (result.has_value() ? "" : result.error());
        expect(*result == 1u) << "should affect 1 row";

        result = stmt->execute(std::string{"GOOGL"}, std::string{"Stock"});
        expect(fatal(result.has_value()));
        expect(*result == 1u);

        auto cnt = db->query(count(trade{}));
        expect(fatal(cnt.has_value()));
        expect(std::get<0>(cnt->at(0)) == 2u) << "two rows should be inserted";
    };

    "prepare and query — select with parameters"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)db->execute(drop_table(trade{}).if_exists());
        }};

        expect(db->execute(drop_table(trade{}).if_exists()).has_value());
        expect(db->execute(create_table(trade{})).has_value());

        // Insert test data using regular execute
        auto ins = db->prepare("INSERT INTO trade (code, type, executed_at, recorded_at) VALUES (?, ?, NOW(), NOW())");
        expect(fatal(ins.has_value()));
        expect(ins->execute(std::string{"AAPL"}, std::string{"Stock"}).has_value());
        expect(ins->execute(std::string{"GOOGL"}, std::string{"Stock"}).has_value());
        expect(ins->execute(std::string{"MSFT"}, std::string{"Stock"}).has_value());

        // Query with prepared statement
        auto sel = db->prepare("SELECT id, code FROM trade WHERE type = ? ORDER BY code");
        expect(fatal(sel.has_value())) << "prepare SELECT should succeed";

        using row_t = std::tuple<uint32_t, std::string>;
        auto rows = sel->query<row_t>(std::string{"Stock"});
        expect(fatal(rows.has_value())) << "query should succeed: " + (rows.has_value() ? "" : rows.error());
        expect(rows->size() == 3u) << "should return 3 rows, got " + std::to_string(rows->size());

        // Verify ordered by code: AAPL, GOOGL, MSFT
        expect(std::get<1>(rows->at(0)) == "AAPL"s);
        expect(std::get<1>(rows->at(1)) == "GOOGL"s);
        expect(std::get<1>(rows->at(2)) == "MSFT"s);
    };

    "prepare with zero parameters"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)db->execute(drop_table(trade{}).if_exists());
        }};

        expect(db->execute(drop_table(trade{}).if_exists()).has_value());
        expect(db->execute(create_table(trade{})).has_value());

        auto stmt = db->prepare("SELECT COUNT(*) FROM trade");
        expect(fatal(stmt.has_value()));
        expect(stmt->param_count() == 0u);

        using row_t = std::tuple<uint64_t>;
        auto rows = stmt->query<row_t>();
        expect(fatal(rows.has_value()));
        expect(rows->size() == 1u);
        expect(std::get<0>(rows->at(0)) == 0u);
    };

    "prepared statement last_insert_id"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)db->execute(drop_table(trade{}).if_exists());
        }};

        expect(db->execute(drop_table(trade{}).if_exists()).has_value());
        expect(db->execute(create_table(trade{})).has_value());

        auto stmt = db->prepare("INSERT INTO trade (code, type, executed_at, recorded_at) VALUES (?, ?, NOW(), NOW())");
        expect(fatal(stmt.has_value()));

        expect(stmt->execute(std::string{"AAPL"}, std::string{"Stock"}).has_value());
        auto const first_id = stmt->last_insert_id();
        expect(first_id > 0u) << "first insert should produce a non-zero id";

        expect(stmt->execute(std::string{"GOOGL"}, std::string{"Stock"}).has_value());
        auto const second_id = stmt->last_insert_id();
        expect(second_id == first_id + 1u) << "second id should be first + 1";
    };

    "parameter count mismatch returns error"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto db = mysql_connection::connect(*config);
        expect(fatal(db));

        auto stmt = db->prepare("SELECT 1 WHERE 1 = ?");
        expect(fatal(stmt.has_value()));

        // Pass 0 params when 1 is expected
        using row_t = std::tuple<uint64_t>;
        auto rows = stmt->query<row_t>();
        expect(!rows.has_value()) << "should fail with parameter mismatch";
    };
};

// ===================================================================
// Column attribute defaults integration
// ===================================================================

struct default_values_table {
    COLUMN_FIELD(id, uint32_t, column_attr::primary_key{}, column_attr::auto_increment{})
    COLUMN_FIELD(status, varchar_type<20>, column_attr::default_value("active"))
    COLUMN_FIELD(counter, int32_t, column_attr::default_value(0))
    COLUMN_FIELD(ratio, double, column_attr::default_value(1.5))
    COLUMN_FIELD(created_at, datetime_type<>, column_attr::default_value(current_timestamp))
    COLUMN_FIELD(updated_at, datetime_type<>, column_attr::default_value(current_timestamp),
                 column_attr::on_update(current_timestamp))
};

suite<"Column Attribute Defaults Integration"> column_attr_defaults_suite = [] {
    "create_table with typed defaults succeeds on live MySQL"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(default_values_table{}).if_exists()));
        }};

        expect(db->execute(drop_table(default_values_table{}).if_exists()).has_value());
        auto result = db->execute(create_table(default_values_table{}));
        expect(result.has_value()) << "CREATE TABLE with defaults failed";
    };

    "validate_table accepts table created with typed defaults"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table(default_values_table{}).if_exists()));
        }};

        expect(db->execute(drop_table(default_values_table{}).if_exists()).has_value());
        expect(db->execute(create_table(default_values_table{})).has_value());

        auto validation = db->validate_table(default_values_table{});
        expect(validation.has_value()) << "validate_table failed";
    };
};

// ===================================================================
// sql_default and column-specific INSERT integration tests
// ===================================================================

struct sql_default_test_table {
    COLUMN_FIELD(id, uint32_t, column_attr::primary_key{}, column_attr::auto_increment{})
    COLUMN_FIELD(label, varchar_type<64>)
    COLUMN_FIELD(status, varchar_type<20>, column_attr::default_value("active"))
};

suite<"sql_default INSERT Integration"> sql_default_insert_suite = [] {
    "positional insert with sql_default for auto_increment"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto cleanup = scope_guard{[&] {
            (void)(db->execute(drop_table(sql_default_test_table{}).if_exists()));
        }};

        expect(db->execute(drop_table(sql_default_test_table{}).if_exists()).has_value());
        expect(db->execute(create_table(sql_default_test_table{})).has_value());

        // Positional insert with sql_default() for auto-increment id
        auto result = db->execute(insert_into(sql_default_test_table{})
                                      .values(sql_default(), sql_default_test_table::label{"hello"},
                                              sql_default_test_table::status{"custom"}));
        expect(result.has_value()) << "positional insert with sql_default should succeed";

        auto const id1 = db->last_insert_id();
        expect(id1 >= 1u) << "auto-increment should produce id >= 1";

        // Verify the inserted row
        auto rows = db->query(
            select(sql_default_test_table::id{}, sql_default_test_table::label{}, sql_default_test_table::status{})
                .from(sql_default_test_table{}));
        expect(fatal(rows.has_value()));
        expect(rows->size() == 1u);
        auto const& [id, label, status] = rows->at(0);
        expect(id == id1);
        expect(std::string_view(label) == "hello");
        expect(std::string_view(status) == "custom");
    };

    "positional insert with sql_default for default_value column"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto cleanup = scope_guard{[&] {
            (void)(db->execute(drop_table(sql_default_test_table{}).if_exists()));
        }};

        expect(db->execute(drop_table(sql_default_test_table{}).if_exists()).has_value());
        expect(db->execute(create_table(sql_default_test_table{})).has_value());

        // Both id and status use DEFAULT
        auto result = db->execute(insert_into(sql_default_test_table{})
                                      .values(sql_default(), sql_default_test_table::label{"test"}, sql_default()));
        expect(result.has_value()) << "insert with multiple defaults should succeed";

        auto rows = db->query(
            select(sql_default_test_table::label{}, sql_default_test_table::status{}).from(sql_default_test_table{}));
        expect(fatal(rows.has_value()));
        expect(rows->size() == 1u);
        auto const& [label, status] = rows->at(0);
        expect(std::string_view(label) == "test");
        expect(std::string_view(status) == "active") << "status should use default 'active'";
    };

    "column-specific insert omitting auto_increment column"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto cleanup = scope_guard{[&] {
            (void)(db->execute(drop_table(sql_default_test_table{}).if_exists()));
        }};

        expect(db->execute(drop_table(sql_default_test_table{}).if_exists()).has_value());
        expect(db->execute(create_table(sql_default_test_table{})).has_value());

        // Column-specific: only label and status, id auto-increments
        auto result = db->execute(insert_into(sql_default_test_table{})
                                      .columns(sql_default_test_table::label{}, sql_default_test_table::status{})
                                      .values("col_specific", "pending"));
        expect(result.has_value()) << "column-specific insert should succeed";

        auto const id1 = db->last_insert_id();
        expect(id1 >= 1u);

        auto rows = db->query(
            select(sql_default_test_table::label{}, sql_default_test_table::status{}).from(sql_default_test_table{}));
        expect(fatal(rows.has_value()));
        expect(rows->size() == 1u);
        auto const& [label, status] = rows->at(0);
        expect(std::string_view(label) == "col_specific");
        expect(std::string_view(status) == "pending");
    };

    "column field assigned sql_default works in struct-based insert"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto cleanup = scope_guard{[&] {
            (void)(db->execute(drop_table(sql_default_test_table{}).if_exists()));
        }};

        expect(db->execute(drop_table(sql_default_test_table{}).if_exists()).has_value());
        expect(db->execute(create_table(sql_default_test_table{})).has_value());

        // Assign sql_default() to column fields, then use struct-based insert
        sql_default_test_table row;
        row.id_ = sql_default();
        row.label_ = "assigned";
        row.status_ = sql_default();
        auto result = db->execute(insert_into(sql_default_test_table{}).values(row));
        expect(result.has_value()) << "struct-based insert with sql_default fields should succeed";

        auto rows = db->query(
            select(sql_default_test_table::label{}, sql_default_test_table::status{}).from(sql_default_test_table{}));
        expect(fatal(rows.has_value()));
        expect(rows->size() == 1u);
        auto const& [label, status] = rows->at(0);
        expect(std::string_view(label) == "assigned");
        expect(std::string_view(status) == "active") << "status should use default 'active'";
    };
};

// ===================================================================
// Connection pool integration tests
// ===================================================================

suite<"connection pool"> pool_suite = [] {
    "connection_pool::create - creates pool with given size"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value())) << "Missing MySQL environment variables";

        auto pool = connection_pool::create(*config, 3);
        expect(fatal(pool.has_value())) << pool.error();
        expect((*pool)->size() == 3u);
        expect((*pool)->available() == 3u);
    };

    "connection_pool::acquire - checks out a connection"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value())) << "Missing MySQL environment variables";

        auto pool = connection_pool::create(*config, 2);
        expect(fatal(pool.has_value())) << pool.error();

        {
            auto conn = (*pool)->acquire();
            expect((*pool)->available() == 1u);

            auto result = conn->ping();
            expect(result.has_value()) << "acquired connection should be alive";
        }
        // Connection returned to pool after scope exit
        expect((*pool)->available() == 2u);
    };

    "connection_pool::try_acquire - returns nullopt when pool exhausted"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value())) << "Missing MySQL environment variables";

        auto pool = connection_pool::create(*config, 1);
        expect(fatal(pool.has_value())) << pool.error();

        auto conn1 = (*pool)->acquire();
        expect((*pool)->available() == 0u);

        auto conn2 = (*pool)->try_acquire();
        expect(!conn2.has_value()) << "try_acquire should return nullopt when pool is empty";
    };

    "connection_pool - query through pooled connection"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value())) << "Missing MySQL environment variables";

        auto pool = connection_pool::create(*config, 1);
        expect(fatal(pool.has_value())) << pool.error();

        auto conn = (*pool)->acquire();
        auto result = conn->query(sql_raw{"SELECT 1"});
        expect(result.has_value()) << "query through pooled connection should succeed";
    };
};

// ===================================================================
// Async query integration tests
// ===================================================================

suite<"async query"> async_suite = [] {
    "async_query raw - runs query on worker thread"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value())) << "Missing MySQL environment variables";

        auto pool = connection_pool::create(*config, 2);
        expect(fatal(pool.has_value())) << pool.error();

        auto future = async_query(**pool, sql_raw{"SELECT 1 AS val"});
        auto result = future.get();
        expect(result.has_value()) << "async raw query should succeed";
        expect(result->size() == 1u);
    };

    "async_execute - runs DDL on worker thread"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value())) << "Missing MySQL environment variables";

        auto pool = connection_pool::create(*config, 2);
        expect(fatal(pool.has_value())) << pool.error();

        auto future = async_execute(**pool, drop_table(trade{}).if_exists());
        auto result = future.get();
        expect(result.has_value()) << "async execute should succeed";
    };

    "async_query - multiple concurrent queries"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value())) << "Missing MySQL environment variables";

        auto pool = connection_pool::create(*config, 3);
        expect(fatal(pool.has_value())) << pool.error();

        auto f1 = async_query(**pool, sql_raw{"SELECT 1"});
        auto f2 = async_query(**pool, sql_raw{"SELECT 2"});
        auto f3 = async_query(**pool, sql_raw{"SELECT 3"});

        auto r1 = f1.get();
        auto r2 = f2.get();
        auto r3 = f3.get();

        expect(r1.has_value());
        expect(r2.has_value());
        expect(r3.has_value());
    };
};

}  // namespace ds_mysql
