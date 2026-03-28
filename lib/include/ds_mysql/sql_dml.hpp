#pragma once

#include "ds_mysql/sql_core.hpp"
#include "ds_mysql/sql_mutation_shared.hpp"

namespace ds_mysql {

// ===================================================================
// DML Query Builders
//
// Entry points:
//   describe<T>()
//       → build_sql()                           — DESCRIBE T
//
//   insert_into<T>()
//       .values(row)
//       → build_sql()                           — INSERT INTO T (...) VALUES (...)
//       → .on_duplicate_key_update(col1, ...)   — ... ON DUPLICATE KEY UPDATE ...
//       .values(rows) where rows is std::ranges::input_range<T>
//       → build_sql()                           — INSERT INTO T (...) VALUES (...), (...), ...
//
//   update<T>()
//       .set(col1, col2, ...)                   — all columns in one call
//       .set(col1).set(col2)                    — chained calls (equivalent)
//       → build_sql()                           — UPDATE T SET ...
//       .where(cond) → build_sql()              — UPDATE T SET ... WHERE cond
//
//   delete_from<T>()
//       → build_sql()                           — DELETE FROM T
//       .where(cond) → build_sql()              — DELETE FROM T WHERE cond
//
//   count<T>()
//       → build_sql()                           — SELECT COUNT(*) FROM T
//       .where(cond) → build_sql()              — SELECT COUNT(*) FROM T WHERE cond
//
//   truncate_table<T>()
//       → build_sql()                           — TRUNCATE TABLE T
// ===================================================================

namespace dml_detail {

// ---------------------------------------------------------------
// describe_builder
// ---------------------------------------------------------------
template <typename T>
class describe_builder {
public:
    // MySQL DESCRIBE returns: Field, Type, Null, Key, Default (nullable), Extra.
    using result_row_type =
        std::tuple<std::string, std::string, std::string, std::string, std::optional<std::string>, std::string>;

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        std::string s;
        s.reserve(9 + table_name.size());
        s += "DESCRIBE ";
        s += table_name;
        return s;
    }
};

// ---------------------------------------------------------------
// insert_into_values_builder / insert_into_builder
// ---------------------------------------------------------------
template <typename T>
class insert_into_values_builder;

template <typename T>
class insert_into_builder;

template <typename... Cols>
[[nodiscard]] std::string build_assignment_sql_from_tuple(std::tuple<Cols...> const& assignments);

template <typename T, typename... Cols>
class insert_into_upsert_builder {
public:
    insert_into_upsert_builder(insert_into_values_builder<T> values_builder, std::tuple<Cols...> assignments)
        : values_builder_(std::move(values_builder)), assignments_(std::move(assignments)) {
    }

    [[nodiscard]] std::string build_sql() const {
        auto insert_sql = values_builder_.build_sql();
        auto update_clause = build_assignment_sql_from_tuple(assignments_);
        std::string sql;
        sql.reserve(insert_sql.size() + 26 + update_clause.size());
        sql += insert_sql;
        sql += " ON DUPLICATE KEY UPDATE ";
        sql += update_clause;
        return sql;
    }

private:
    insert_into_values_builder<T> values_builder_;
    std::tuple<Cols...> assignments_;
};

template <typename T>
class insert_into_values_builder {
public:
    explicit insert_into_values_builder(T row) : rows_{std::move(row)}, bulk_(false) {
    }

    insert_into_values_builder(std::vector<T> rows, bool bulk) : rows_(std::move(rows)), bulk_(bulk) {
    }

    // on_duplicate_key_update(col1, col2, ...) — appends ON DUPLICATE KEY UPDATE clause.
    // Each argument must be a FieldOf<T> column_field instance carrying the new value.
    template <FieldOf<T>... Cols>
        requires(sizeof...(Cols) > 0)
    [[nodiscard]] insert_into_upsert_builder<T, Cols...> on_duplicate_key_update(Cols const&... assignments) const {
        return insert_into_upsert_builder<T, Cols...>{*this, std::tuple<Cols...>{assignments...}};
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        const auto& column_list = dml_detail::generate_column_list<T>();
        std::string s;
        if (bulk_) {
            std::string values;
            values.reserve(rows_.size() * 4);
            bool first = true;
            for (auto const& row : rows_) {
                if (!first) {
                    values += ", ";
                }
                values += '(';
                values += dml_detail::generate_values(row);
                values += ')';
                first = false;
            }
            s.reserve(12 + table_name.size() + 2 + column_list.size() + 9 + values.size());
            s += "INSERT INTO ";
            s += table_name;
            s += " (";
            s += column_list;
            s += ") VALUES ";
            s += values;
        } else {
            auto values = rows_.empty() ? std::string{} : dml_detail::generate_values(rows_.front());
            s.reserve(12 + table_name.size() + 2 + column_list.size() + 10 + values.size() + 1);
            s += "INSERT INTO ";
            s += table_name;
            s += " (";
            s += column_list;
            s += ") VALUES (";
            s += values;
            s += ')';
        }
        return s;
    }

private:
    std::vector<T> rows_;
    bool bulk_ = false;
};

template <typename T>
class insert_into_builder {
public:
    [[nodiscard]] insert_into_values_builder<T> values(T const& row) const {
        return insert_into_values_builder<T>{row};
    }

    template <std::ranges::input_range Rows>
        requires std::same_as<std::remove_cvref_t<std::ranges::range_value_t<Rows>>, T>
    [[nodiscard]] insert_into_values_builder<T> values(Rows&& rows) const {
        std::vector<T> collected_rows;
        for (auto const& row : rows) {
            collected_rows.push_back(row);
        }
        if (collected_rows.empty()) {
            return {std::vector<T>{}, false};
        }
        return {std::move(collected_rows), /*bulk=*/true};
    }
};

// ---------------------------------------------------------------
// update builders
// ---------------------------------------------------------------

template <typename Tuple, std::size_t... Is>
[[nodiscard]] std::string build_assignment_sql_from_tuple_impl(Tuple const& assignments, std::index_sequence<Is...>) {
    return build_assignment_sql(std::get<Is>(assignments)...);
}

template <typename... Cols>
[[nodiscard]] std::string build_assignment_sql_from_tuple(std::tuple<Cols...> const& assignments) {
    return build_assignment_sql_from_tuple_impl(assignments, std::index_sequence_for<Cols...>{});
}

template <typename T, typename... Cols>
class update_set_where_builder {
public:
    update_set_where_builder(std::tuple<Cols...> assignments, std::optional<sql_predicate> where)
        : assignments_(std::move(assignments)), where_(std::move(where)) {
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        auto set_sql = build_assignment_sql_from_tuple(assignments_);
        std::string s;
        s.reserve(7 + table_name.size() + 5 + set_sql.size() + 48);
        s += "UPDATE ";
        s += table_name;
        s += " SET ";
        s += set_sql;
        if (where_.has_value()) {
            s += " WHERE ";
            s += where_->build_sql();
        }
        if (!order_by_clauses_.empty()) {
            s += " ORDER BY ";
            bool first = true;
            for (auto const& clause : order_by_clauses_) {
                if (!first) {
                    s += ", ";
                }
                s += clause;
                first = false;
            }
        }
        if (limit_.has_value()) {
            s += " LIMIT ";
            s += std::to_string(*limit_);
        }
        return s;
    }

    template <typename Spec>
        requires(ColumnDescriptor<Spec> || DescOrder<Spec>)
    [[nodiscard]] update_set_where_builder order_by(Spec) const {
        auto copy = *this;
        if constexpr (DescOrder<Spec>) {
            using Col = typename Spec::col_spec;
            copy.order_by_clauses_.push_back(std::string(column_traits<Col>::column_name()) + " DESC");
        } else {
            copy.order_by_clauses_.push_back(std::string(column_traits<Spec>::column_name()) + " ASC");
        }
        return copy;
    }

    [[nodiscard]] update_set_where_builder limit(std::size_t n) const {
        auto copy = *this;
        copy.limit_ = n;
        return copy;
    }

private:
    std::tuple<Cols...> assignments_;
    std::optional<sql_predicate> where_;
    std::vector<std::string> order_by_clauses_;
    std::optional<std::size_t> limit_;
};

template <typename T, typename... Cols>
class update_set_builder {
public:
    explicit update_set_builder(std::tuple<Cols...> assignments) : assignments_(std::move(assignments)) {
    }

    template <FieldOf<T>... NewCols>
        requires(sizeof...(NewCols) > 0)
    [[nodiscard]] update_set_builder<T, Cols..., NewCols...> set(NewCols const&... new_assignments) const {
        return update_set_builder<T, Cols..., NewCols...>{
            std::tuple_cat(assignments_, std::tuple<NewCols...>{new_assignments...})};
    }

    [[nodiscard]] update_set_where_builder<T, Cols...> where(sql_predicate condition) const {
        return {assignments_, std::move(condition)};
    }

    template <typename Spec>
        requires(ColumnDescriptor<Spec> || DescOrder<Spec>)
    [[nodiscard]] update_set_where_builder<T, Cols...> order_by(Spec s) const {
        return update_set_where_builder<T, Cols...>{assignments_, std::nullopt}.order_by(s);
    }

    [[nodiscard]] update_set_where_builder<T, Cols...> limit(std::size_t n) const {
        return update_set_where_builder<T, Cols...>{assignments_, std::nullopt}.limit(n);
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        auto set_sql = build_assignment_sql_from_tuple(assignments_);
        std::string s;
        s.reserve(7 + table_name.size() + 5 + set_sql.size());
        s += "UPDATE ";
        s += table_name;
        s += " SET ";
        s += set_sql;
        return s;
    }

private:
    std::tuple<Cols...> assignments_;
};

template <typename T>
class update_builder {
public:
    template <FieldOf<T>... Cols>
        requires(sizeof...(Cols) > 0)
    [[nodiscard]] update_set_builder<T, Cols...> set(Cols const&... assignments) const {
        return update_set_builder<T, Cols...>{std::tuple<Cols...>{assignments...}};
    }
};

// ---------------------------------------------------------------
// delete_from builders
// ---------------------------------------------------------------
template <typename T>
class delete_from_where_builder {
public:
    explicit delete_from_where_builder(std::optional<sql_predicate> where) : where_(std::move(where)) {
    }

    [[nodiscard]] std::string build_sql() const {
        const auto table_name = table_name_for<T>::value().to_string_view();
        std::string s;
        s.reserve(12 + table_name.size() + 32);
        s += "DELETE FROM ";
        s += table_name;
        if (where_.has_value()) {
            s += " WHERE ";
            s += where_->build_sql();
        }
        if (!order_by_clauses_.empty()) {
            s += " ORDER BY ";
            bool first = true;
            for (auto const& clause : order_by_clauses_) {
                if (!first) {
                    s += ", ";
                }
                s += clause;
                first = false;
            }
        }
        if (limit_.has_value()) {
            s += " LIMIT ";
            s += std::to_string(*limit_);
        }
        return s;
    }

    template <typename Spec>
        requires(ColumnDescriptor<Spec> || DescOrder<Spec>)
    [[nodiscard]] delete_from_where_builder order_by(Spec) const {
        auto copy = *this;
        if constexpr (DescOrder<Spec>) {
            using Col = typename Spec::col_spec;
            copy.order_by_clauses_.push_back(std::string(column_traits<Col>::column_name()) + " DESC");
        } else {
            copy.order_by_clauses_.push_back(std::string(column_traits<Spec>::column_name()) + " ASC");
        }
        return copy;
    }

    [[nodiscard]] delete_from_where_builder limit(std::size_t n) const {
        auto copy = *this;
        copy.limit_ = n;
        return copy;
    }

private:
    std::optional<sql_predicate> where_;
    std::vector<std::string> order_by_clauses_;
    std::optional<std::size_t> limit_;
};

template <typename T>
class delete_from_builder {
public:
    [[nodiscard]] delete_from_where_builder<T> where(sql_predicate condition) const {
        return delete_from_where_builder<T>{std::move(condition)};
    }

    template <typename Spec>
        requires(ColumnDescriptor<Spec> || DescOrder<Spec>)
    [[nodiscard]] delete_from_where_builder<T> order_by(Spec s) const {
        return delete_from_where_builder<T>{std::nullopt}.order_by(s);
    }

    [[nodiscard]] delete_from_where_builder<T> limit(std::size_t n) const {
        return delete_from_where_builder<T>{std::nullopt}.limit(n);
    }

    [[nodiscard]] std::string build_sql() const {
        return delete_from_where_builder<T>{std::nullopt}.build_sql();
    }
};

}  // namespace dml_detail

/**
 * describe<T>() — DESCRIBE <table>.
 */
template <ValidTable T>
[[nodiscard]] dml_detail::describe_builder<T> describe() {
    return {};
}

template <ValidTable T>
[[nodiscard]] dml_detail::describe_builder<T> describe(T const&) {
    return {};
}

/**
 * insert_into<T>() — INSERT INTO <table> (...) VALUES (...).
 *
 * Single-row insert:
 *   insert_into<T>().values(row).build_sql()
 *
 * Bulk insert (multiple rows in one statement):
 *   insert_into<T>().values(rows).build_sql()   // rows is std::ranges::input_range<T>
 *
 * Upsert (INSERT … ON DUPLICATE KEY UPDATE):
 *   insert_into<T>().values(row).on_duplicate_key_update(T::col1{v1}, T::col2{v2}, ...)
 *
 * Example:
 *   symbol row;
 *   row.ticker_     = "AAPL";
 *   row.instrument_ = "Stock";
 *   db.execute(insert_into<symbol>().values(row));
 *
 *   // Upsert:
 *   db.execute(insert_into<symbol>().values(row)
 *       .on_duplicate_key_update(symbol::ticker{"AAPL"}, symbol::instrument{"Stock"}));
 *
 *   // Bulk:
 *   db.execute(insert_into<symbol>().values(std::array{row1, row2}).build_sql());
 */
template <ValidTable T>
[[nodiscard]] dml_detail::insert_into_builder<T> insert_into() {
    return {};
}

template <ValidTable T>
[[nodiscard]] dml_detail::insert_into_builder<T> insert_into(T const&) {
    return {};
}

/**
 * update<T>() — UPDATE <table> SET ... [WHERE ...].
 *
 * Advance with .set(col1, col2, ...) supplying typed column field instances,
 * then optionally .where(cond).
 *
 * Example:
 *   db.execute(update<symbol>().set(symbol::ticker{"MSFT"}).where(equal<symbol::id>(1u)));
 */
template <ValidTable T>
[[nodiscard]] dml_detail::update_builder<T> update() {
    return {};
}

template <ValidTable T>
[[nodiscard]] dml_detail::update_builder<T> update(T const&) {
    return {};
}

/**
 * delete_from<T>() — DELETE FROM <table> [WHERE ...].
 *
 * Example:
 *   db.execute(delete_from<symbol>().where(equal<symbol::id>(1u)));
 */
template <ValidTable T>
[[nodiscard]] dml_detail::delete_from_builder<T> delete_from() {
    return {};
}

template <ValidTable T>
[[nodiscard]] dml_detail::delete_from_builder<T> delete_from(T const&) {
    return {};
}

// ===================================================================
// Transaction Control Statements
//
// Entry points (executeable via db.execute()):
//   savepoint(name)                                   — SAVEPOINT name
//   release_savepoint(name)                           — RELEASE SAVEPOINT name
//   rollback_to_savepoint(name)                       — ROLLBACK TO SAVEPOINT name
//   set_transaction_isolation_level(IsolationLevel)   — SET TRANSACTION ISOLATION LEVEL ...
// ===================================================================

enum class IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
};

namespace dml_detail {

class savepoint_builder {
public:
    explicit savepoint_builder(std::string name) : name_(std::move(name)) {
    }

    [[nodiscard]] std::string build_sql() const {
        return "SAVEPOINT " + name_;
    }

private:
    std::string name_;
};

class release_savepoint_builder {
public:
    explicit release_savepoint_builder(std::string name) : name_(std::move(name)) {
    }

    [[nodiscard]] std::string build_sql() const {
        return "RELEASE SAVEPOINT " + name_;
    }

private:
    std::string name_;
};

class rollback_to_savepoint_builder {
public:
    explicit rollback_to_savepoint_builder(std::string name) : name_(std::move(name)) {
    }

    [[nodiscard]] std::string build_sql() const {
        return "ROLLBACK TO SAVEPOINT " + name_;
    }

private:
    std::string name_;
};

class set_transaction_isolation_level_builder {
public:
    explicit set_transaction_isolation_level_builder(IsolationLevel level) : level_(level) {
    }

    [[nodiscard]] std::string build_sql() const {
        std::string s = "SET TRANSACTION ISOLATION LEVEL ";
        switch (level_) {
            case IsolationLevel::ReadUncommitted:
                s += "READ UNCOMMITTED";
                break;
            case IsolationLevel::ReadCommitted:
                s += "READ COMMITTED";
                break;
            case IsolationLevel::RepeatableRead:
                s += "REPEATABLE READ";
                break;
            case IsolationLevel::Serializable:
                s += "SERIALIZABLE";
                break;
        }
        return s;
    }

private:
    IsolationLevel level_;
};

}  // namespace dml_detail

[[nodiscard]] inline dml_detail::savepoint_builder savepoint(std::string name) {
    return dml_detail::savepoint_builder{std::move(name)};
}

[[nodiscard]] inline dml_detail::release_savepoint_builder release_savepoint(std::string name) {
    return dml_detail::release_savepoint_builder{std::move(name)};
}

[[nodiscard]] inline dml_detail::rollback_to_savepoint_builder rollback_to_savepoint(std::string name) {
    return dml_detail::rollback_to_savepoint_builder{std::move(name)};
}

[[nodiscard]] inline dml_detail::set_transaction_isolation_level_builder set_transaction_isolation_level(
    IsolationLevel level) {
    return dml_detail::set_transaction_isolation_level_builder{level};
}

}  // namespace ds_mysql
