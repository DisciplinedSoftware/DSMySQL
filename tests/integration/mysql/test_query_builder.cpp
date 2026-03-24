#include <boost/ut.hpp>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

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
    COLUMN_FIELD(id, uint32_t)
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
        std::cerr << "[ds_mysql][integration] Missing MySQL environment variables: "
                  << "DS_MYSQL_TEST_HOST=" << (host.empty() ? "<empty>" : host) << ", "
                  << "DS_MYSQL_TEST_PORT=" << port_str << ", "
                  << "DS_MYSQL_TEST_DATABASE=" << database << ", "
                  << "DS_MYSQL_TEST_USER=" << (user.empty() ? "<empty>" : user) << ", "
                  << "DS_MYSQL_TEST_PASSWORD=" << (password.empty() ? "<empty>" : "<set>") << '\n';
        return std::nullopt;
    }

    unsigned int parsed_port = 0;
    try {
        parsed_port = static_cast<unsigned int>(std::stoul(port_str));
    } catch (std::exception const& ex) {
        std::cerr << "[ds_mysql][integration] Invalid DS_MYSQL_TEST_PORT='" << port_str
                  << "': " << ex.what() << '\n';
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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(drop_table<trade>().if_exists()).has_value()) << "Failed to drop table";
        expect(db->execute(create_table<trade>()).has_value()) << "Failed to create trade table";

        auto const describe_result = db->query(describe<trade>());
        expect(fatal(describe_result.has_value())) << "DESCRIBE trade failed";
        expect(describe_result->size() == 9u) << "trade should have 9 columns";
    };

    "create table if not exists is idempotent"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
    };

    "drop table if exists then create table via chaining"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(drop_table<trade>().if_exists().then().create_table<trade>()).has_value())
            << "Chained DROP/CREATE should succeed";

        auto const result = db->query(count<trade>());
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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(drop_table<trade>().if_exists().then().create_table<trade>()).has_value());

        trade row;
        row.code_ = "AAPL";
        row.type_ = "Stock";
        row.name_ = "Apple Inc.";
        row.category_ = "Technology";
        row.currency_ = "USD";
        expect(db->execute(insert_into<trade>().values(row)).has_value()) << "Insert should succeed";

        auto const count_result = db->query(count<trade>());
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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(drop_table<trade>().if_exists().then().create_table<trade>()).has_value());

        for (auto const code : {varchar_type<32>{"AAPL"}, varchar_type<32>{"GOOGL"}, varchar_type<32>{"MSFT"}}) {
            trade row;
            row.code_ = code;
            row.type_ = "Stock";
            expect(db->execute(insert_into<trade>().values(row)).has_value());
        }

        auto const count_result = db->query(count<trade>());
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
            (void)(db->execute(drop_table<temporal_precision_trade>().if_exists()));
        }};

        expect(db->execute(drop_table<temporal_precision_trade>().if_exists()).has_value());
        expect(db->execute(create_table<temporal_precision_trade>()).has_value());

        temporal_precision_trade row;
        row.code_ = "PREC";
        auto const tp = system_clock::time_point{sys_days{year{2024} / January / 1}} + microseconds{987654};
        row.executed_at_ = datetime_type<>{tp, 3};
        row.recorded_at_ = timestamp_type<>{tp, 2};

        expect(db->execute(insert_into<temporal_precision_trade>().values(row)).has_value());

        auto const results =
            db->query(select<date_format_of<temporal_precision_trade::executed_at, "%Y-%m-%d %H:%i:%s.%f">,
                             date_format_of<temporal_precision_trade::recorded_at, "%Y-%m-%d %H:%i:%s.%f">>()
                          .from<temporal_precision_trade>()
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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(delete_from<trade>()).has_value());

        for (auto const code : {varchar_type<32>{"AAPL"}, varchar_type<32>{"GOOGL"}, varchar_type<32>{"MSFT"}}) {
            trade row;
            row.code_ = code;
            row.type_ = "Stock";
            expect(db->execute(insert_into<trade>().values(row)).has_value());
        }

        auto const results = db->query(select<trade::id, trade::code>().from<trade>());
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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(delete_from<trade>()).has_value());

        trade row;
        row.code_ = "AAPL";
        row.type_ = "Stock";
        row.name_ = "Apple Inc.";
        row.category_ = "Technology";
        row.currency_ = "USD";
        expect(db->execute(insert_into<trade>().values(row)).has_value());

        auto const results =
            db->query(select<trade::id, trade::account_id, trade::code, trade::type, trade::name, trade::category,
                             trade::currency, date_format_of<trade::executed_at, "%Y-%m-%d %H:%i:%s">,
                             date_format_of<trade::recorded_at, "%Y-%m-%d %H:%i:%s">>()
                          .from<trade>()
                          .order_by<trade::id>()
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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(delete_from<trade>()).has_value());
        for (auto const code : {varchar_type<32>{"AAPL"}, varchar_type<32>{"GOOGL"}}) {
            trade row;
            row.code_ = code;
            row.type_ = "Stock";
            expect(db->execute(insert_into<trade>().values(row)).has_value());
        }

        auto const results = db->query(select<trade::code>().from<trade>().where(equal<trade::code>("AAPL")));
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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(delete_from<trade>()).has_value());

        trade aapl;
        aapl.code_ = "AAPL";
        aapl.type_ = "Stock";
        aapl.category_ = "Technology";
        expect(db->execute(insert_into<trade>().values(aapl)).has_value());

        trade googl;
        googl.code_ = "GOOGL";
        googl.type_ = "Stock";
        googl.category_ = "Technology";
        expect(db->execute(insert_into<trade>().values(googl)).has_value());

        auto const rows =
            db->query(select<trade::code, trade::name>().from<trade>().where(equal<trade::category>("Technology")));
        expect(fatal(rows.has_value()));
        expect(rows->size() == 2u) << "Should have 2 technology trades";
    };

    "select with LIMIT"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(delete_from<trade>()).has_value());
        for (auto const code : {varchar_type<32>{"AAPL"}, varchar_type<32>{"GOOGL"}, varchar_type<32>{"MSFT"},
                                varchar_type<32>{"AMZN"}, varchar_type<32>{"TSLA"}}) {
            trade row;
            row.code_ = code;
            row.type_ = "Stock";
            expect(db->execute(insert_into<trade>().values(row)).has_value());
        }

        auto const results = db->query(select<trade::id, trade::code>().from<trade>().limit(3));
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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(delete_from<trade>()).has_value());

        trade row;
        row.code_ = varchar_type<32>{"AAPL"};
        row.type_ = varchar_type<64>{"Stock"};
        row.name_ = varchar_type<255>{"Apple Inc."};
        expect(db->execute(insert_into<trade>().values(row)).has_value());

        expect(db->execute(update<trade>()
                               .set(trade::name{varchar_type<255>{"Apple Corporation"}})
                               .where(equal<trade::code>("AAPL")))
                   .has_value())
            << "Update should succeed";

        auto const updated_count =
            db->query(count<trade>().where(and_(equal<trade::code>("AAPL"), equal<trade::name>("Apple Corporation"))));
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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(delete_from<trade>()).has_value());

        trade row;
        row.code_ = "AAPL";
        row.type_ = "Stock";
        expect(db->execute(insert_into<trade>().values(row)).has_value());

        expect(
            db->execute(update<trade>().set<trade::type>("Tech Stock").where(equal<trade::code>("AAPL"))).has_value());

        auto const results = db->query(select<trade::type>().from<trade>().where(equal<trade::code>("AAPL")));
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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(delete_from<trade>()).has_value());
        for (auto const code : {varchar_type<32>{"AAPL"}, varchar_type<32>{"GOOGL"}, varchar_type<32>{"MSFT"}}) {
            trade row;
            row.code_ = code;
            row.type_ = "Stock";
            expect(db->execute(insert_into<trade>().values(row)).has_value());
        }

        expect(db->execute(delete_from<trade>().where(equal<trade::code>("AAPL"))).has_value());

        auto const results = db->query(select<trade::code>().from<trade>());
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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(delete_from<trade>()).has_value());

        auto const count_result = db->query(count<trade>());
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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(delete_from<trade>()).has_value());
        for (auto const code : {varchar_type<32>{"AAPL"}, varchar_type<32>{"GOOGL"}, varchar_type<32>{"MSFT"}}) {
            trade row;
            row.code_ = code;
            row.type_ = "Stock";
            expect(db->execute(insert_into<trade>().values(row)).has_value());
        }

        auto const results = db->query(select<trade::code>().from<trade>().where(col_ref<trade::code> == "AAPL"));
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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(delete_from<trade>()).has_value());
        for (auto const code : {varchar_type<32>{"AAPL"}, varchar_type<32>{"GOOGL"}, varchar_type<32>{"MSFT"}}) {
            trade row;
            row.code_ = code;
            row.type_ = "Stock";
            expect(db->execute(insert_into<trade>().values(row)).has_value());
        }

        auto const results = db->query(select<trade::code>().from<trade>().where(col_ref<trade::code> != "AAPL"));
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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(delete_from<trade>()).has_value());
        for (auto const code : {varchar_type<32>{"AAPL"}, varchar_type<32>{"GOOGL"}, varchar_type<32>{"MSFT"}}) {
            trade row;
            row.code_ = code;
            row.type_ = "Stock";
            expect(db->execute(insert_into<trade>().values(row)).has_value());
        }

        auto const results = db->query(select<trade::code>().from<trade>().where((col_ref<trade::code> == "AAPL") |
                                                                                 (col_ref<trade::code> == "GOOGL")));
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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(delete_from<trade>()).has_value());
        for (auto const code : {varchar_type<32>{"AAPL"}, varchar_type<32>{"GOOGL"}, varchar_type<32>{"MSFT"}}) {
            trade row;
            row.code_ = code;
            row.type_ = "Stock";
            expect(db->execute(insert_into<trade>().values(row)).has_value());
        }

        auto const results = db->query(select<trade::code>().from<trade>().where((col_ref<trade::code> != "AAPL") &
                                                                                 (col_ref<trade::code> != "GOOGL")));
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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(delete_from<trade>()).has_value());
        for (auto const code : {varchar_type<32>{"AAPL"}, varchar_type<32>{"GOOGL"}, varchar_type<32>{"MSFT"}}) {
            trade row;
            row.code_ = code;
            row.type_ = "Stock";
            expect(db->execute(insert_into<trade>().values(row)).has_value());
        }

        auto const results = db->query(select<trade::code>().from<trade>().where(!(col_ref<trade::code> == "AAPL")));
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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(delete_from<trade>()).has_value());

        trade aapl;
        aapl.code_ = "AAPL";
        aapl.type_ = "Bond";
        expect(db->execute(insert_into<trade>().values(aapl)).has_value());

        trade googl;
        googl.code_ = "GOOGL";
        googl.type_ = "Stock";
        expect(db->execute(insert_into<trade>().values(googl)).has_value());

        auto const results = db->query(select<trade::code>().from<trade>().where(
            ((col_ref<trade::code> == "AAPL") | (col_ref<trade::code> == "GOOGL")) & (col_ref<trade::type> == "Bond")));
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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());

        auto const result = db->validate_table<trade>();
        expect(result.has_value()) << "validate_table<trade> should succeed — " +
                                          (result.has_value() ? "" : result.error());
    };

    "validate_table fails when table does not exist"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(drop_table<trade>().if_exists()).has_value());

        auto const result = db->validate_table<trade>();
        expect(!result.has_value()) << "validate_table<trade> should fail when table is absent";
    };

    "validate_table detects column count mismatch"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table<trade_few_columns>().if_exists()));
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        // Create the trade_few_columns DB table with the same 9 columns as trade —
        // the C++ struct only defines 2, so validate_table must detect the count mismatch.
        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(drop_table<trade_few_columns>().if_exists()).has_value());
        expect(db->execute(create_table<trade_few_columns>().like<trade>()).has_value());

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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());

        auto const result = db->validate_database<trade_db>();
        expect(result.has_value()) << "validate_database<trade_db> should succeed — " +
                                          (result.has_value() ? "" : result.error());
    };

    "validate_database reports error when a table is absent"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(drop_table<trade>().if_exists()).has_value());

        auto const result = db->validate_database<trade_db>();
        expect(!result.has_value()) << "validate_database<trade_db> should fail when trade table is missing";
    };

    "validate_database reports error when a table column count mismatch"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table<trade_few_columns>().if_exists()));
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        // Ensure trade_few_columns DB table has 9 columns; the C++ struct defines only 2.
        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(drop_table<trade_few_columns>().if_exists()).has_value());
        expect(db->execute(create_table<trade_few_columns>().like<trade>()).has_value());

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
            (void)(db->execute(drop_table<trade>().if_exists()));
            (void)(db->execute(drop_table<account>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(create_table<account>().if_not_exists()).has_value());

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
            (void)(db->execute(drop_table<trade>().if_exists()));
            (void)(db->execute(drop_table<account>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(drop_table<account>().if_exists()).has_value());

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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(drop_table<trade>().if_exists()).has_value());
        expect(db->execute(create_table<trade>()).has_value());

        // Creating the same table again without IF NOT EXISTS triggers MySQL error 1050.
        auto const fail = db->execute(create_table<trade>());
        expect(!fail.has_value()) << "Should fail: table already exists";
        expect(fail.has_value() || !fail.error().empty()) << "Error should not be empty";

        expect(db->execute(drop_table<trade>().if_exists()).has_value());
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
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        expect(db->execute(create_table<trade>().if_not_exists()).has_value());

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
            (void)(db->execute(drop_table<bool_table>().if_exists()));
        }};

        expect(db->execute(drop_table<bool_table>().if_exists()).has_value());
        expect(db->execute(create_table<bool_table>()).has_value());

        auto const result = db->validate_table<bool_table>();
        expect(result.has_value()) << "validate_table<bool_table> should succeed — " +
                                          (result.has_value() ? "" : result.error());

        expect(db->execute(drop_table<bool_table>().if_exists()).has_value());
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

suite<"Typed Query Column Count Coverage"> typed_query_col_count_suite = [] {
    "query with mismatched column count returns error"_test = [] {
        auto const config = mysql_config_from_env();
        expect(fatal(config.has_value()));

        auto const db = mysql_connection::connect(*config);
        expect(fatal(db));
        auto _ = scope_guard{[&] {
            (void)(db->execute(drop_table<trade>().if_exists()));
        }};

        // Ensure at least one row exists so the result set is non-empty.
        expect(db->execute(create_table<trade>().if_not_exists()).has_value());
        expect(db->execute(delete_from<trade>()).has_value());
        trade row;
        row.code_ = varchar_type<32>{"AAPL"};
        row.type_ = varchar_type<64>{"Stock"};
        expect(db->execute(insert_into<trade>().values(row)).has_value());

        // one_col_query::result_row_type has 1 column but the SQL returns 2.
        auto const result = db->query(one_col_query{});
        expect(!result.has_value()) << "Should detect column count mismatch";
        expect(!result.has_value() && result.error().find("Column count mismatch") != std::string::npos)
            << "Error should mention 'Column count mismatch' — got: " + (result.has_value() ? "" : result.error());

        expect(db->execute(delete_from<trade>()).has_value());
    };
};

}  // namespace ds_mysql
