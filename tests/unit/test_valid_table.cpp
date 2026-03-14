#include <boost/ut.hpp>
#include <cstdint>
#include <string_view>
#include <type_traits>

#include "ds_mysql/column_field.hpp"
#include "ds_mysql/name_reflection.hpp"
#include "ds_mysql/sql.hpp"

using namespace boost::ut;
using namespace ds_mysql;
using namespace std::string_view_literals;

// ===================================================================
// Test fixtures
// ===================================================================

namespace {

// ── Tables that SHOULD satisfy ValidTable ──────────────────────────

// 1. COLUMN_FIELD macro — nested tags generated automatically
struct table_with_macro {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(quantity, uint32_t)
    COLUMN_FIELD(price, double)
};

// 2. Explicit nested tag structs (macro-free style)
struct table_with_explicit_tags {
    struct id_tag {};
    struct price_tag {};
    using id    = tagged_column_field<id_tag, uint32_t>;
    using price = tagged_column_field<price_tag, double>;
    id    id_;
    price price_;
};

// 3. Untagged columns — fixed_string NTTP style (no tags to check)
struct table_with_nttp_columns {
    using id    = column_field<"id",    uint32_t>;
    using price = column_field<"price", double>;
    id    id_;
    price price_;
};

// 4. Empty struct
struct empty_table {};

// 5. Second table that also has an "id" column (cross-table isolation)
struct other_table {
    COLUMN_FIELD(id, uint32_t)
    COLUMN_FIELD(value, double)
};

// ── Structures that should NOT satisfy ValidTable ──────────────────

// Tag defined outside the owning table (at anonymous-namespace scope)
struct outer_tag {};

struct table_with_outer_tag {
    using id = tagged_column_field<outer_tag, uint32_t>;
    id id_;
};

// Tag nested in a *different* table
struct table_with_borrowed_tag {
    using id = tagged_column_field<table_with_macro::id_tag, uint32_t>;
    id id_;
};

}  // namespace

// ===================================================================
// ValidTable — compile-time concept checks
// ===================================================================

// Positive: well-formed tables satisfy the concept
static_assert(ValidTable<table_with_macro>);
static_assert(ValidTable<table_with_explicit_tags>);
static_assert(ValidTable<table_with_nttp_columns>);
static_assert(ValidTable<empty_table>);
static_assert(ValidTable<other_table>);

// Negative: column field types are not tables
static_assert(!ValidTable<column_field<"id", uint32_t>>);
static_assert(!ValidTable<tagged_column_field<table_with_macro::id_tag, uint32_t>>);

// Negative: tag defined outside the owning table
static_assert(!ValidTable<table_with_outer_tag>);

// Negative: tag nested in a *different* table (borrowed tag)
static_assert(!ValidTable<table_with_borrowed_tag>);

// Cross-table isolation: same column name but different tables → different types
static_assert(!std::is_same_v<table_with_macro::id, other_table::id>);

// ===================================================================
// tag_is_nested_in_table — compile-time checks
// ===================================================================

// Nested tag in its own table → true
static_assert(tag_is_nested_in_table<table_with_macro::id_tag, table_with_macro>());
static_assert(tag_is_nested_in_table<table_with_macro::quantity_tag, table_with_macro>());
static_assert(tag_is_nested_in_table<table_with_macro::price_tag, table_with_macro>());
static_assert(tag_is_nested_in_table<table_with_explicit_tags::id_tag, table_with_explicit_tags>());
static_assert(tag_is_nested_in_table<table_with_explicit_tags::price_tag, table_with_explicit_tags>());
static_assert(tag_is_nested_in_table<other_table::id_tag, other_table>());

// Outer (non-nested) tag → false
static_assert(!tag_is_nested_in_table<outer_tag, table_with_outer_tag>());

// Tag nested in the *wrong* table → false
static_assert(!tag_is_nested_in_table<table_with_macro::id_tag, other_table>());
static_assert(!tag_is_nested_in_table<other_table::id_tag, table_with_macro>());
static_assert(!tag_is_nested_in_table<table_with_macro::id_tag, table_with_explicit_tags>());

// ===================================================================
// tag_nested_or_untagged — compile-time checks
// ===================================================================

// tagged_column_field with nested tag → true
static_assert(tag_nested_or_untagged<table_with_macro::id, table_with_macro>());
static_assert(tag_nested_or_untagged<table_with_explicit_tags::id, table_with_explicit_tags>());

// tagged_column_field with outer tag → false
static_assert(!tag_nested_or_untagged<tagged_column_field<outer_tag, uint32_t>, table_with_outer_tag>());

// Untagged column_field — always true regardless of table
static_assert(tag_nested_or_untagged<column_field<"id", uint32_t>, table_with_macro>());
static_assert(tag_nested_or_untagged<column_field<"id", uint32_t>, empty_table>());

// ===================================================================
// tag_is_nested_in_table — runtime suite (verifies consteval values)
// ===================================================================

suite<"tag_is_nested_in_table"> tag_nested_suite = [] {
    "nested tag in its own table returns true"_test = [] {
        constexpr bool result = tag_is_nested_in_table<table_with_macro::id_tag, table_with_macro>();
        expect(result);
    };

    "all COLUMN_FIELD tags are nested in their table"_test = [] {
        expect(tag_is_nested_in_table<table_with_macro::id_tag,       table_with_macro>());
        expect(tag_is_nested_in_table<table_with_macro::quantity_tag,  table_with_macro>());
        expect(tag_is_nested_in_table<table_with_macro::price_tag,     table_with_macro>());
    };

    "explicit nested tags are recognised"_test = [] {
        expect(tag_is_nested_in_table<table_with_explicit_tags::id_tag,    table_with_explicit_tags>());
        expect(tag_is_nested_in_table<table_with_explicit_tags::price_tag, table_with_explicit_tags>());
    };

    "outer (non-nested) tag returns false"_test = [] {
        constexpr bool result = tag_is_nested_in_table<outer_tag, table_with_outer_tag>();
        expect(!result);
    };

    "tag nested in wrong table returns false"_test = [] {
        expect(!tag_is_nested_in_table<table_with_macro::id_tag, other_table>());
        expect(!tag_is_nested_in_table<other_table::id_tag,      table_with_macro>());
    };
};

// ===================================================================
// raw_type_name — runtime suite
// ===================================================================

suite<"raw_type_name"> raw_type_name_suite = [] {
    "raw_type_name of nested tag includes parent class name"_test = [] {
        auto const name = ds_mysql::detail::raw_type_name<table_with_macro::id_tag>();
        expect(name.find("table_with_macro") != std::string_view::npos)
            << "expected parent class in raw name, got: " << name;
        expect(name.find("id_tag") != std::string_view::npos)
            << "expected tag name in raw name, got: " << name;
    };

    "raw_type_name of outer tag does not include table name"_test = [] {
        auto const name = ds_mysql::detail::raw_type_name<outer_tag>();
        expect(name.find("table_with_outer_tag") == std::string_view::npos)
            << "outer tag should not reference its table, got: " << name;
        expect(name.find("outer_tag") != std::string_view::npos)
            << "tag name itself should appear in raw name, got: " << name;
    };

    "raw_type_name distinguishes same-named tags in different tables"_test = [] {
        auto const name_a = ds_mysql::detail::raw_type_name<table_with_macro::id_tag>();
        auto const name_b = ds_mysql::detail::raw_type_name<other_table::id_tag>();
        expect(name_a != name_b)
            << "same-named tags in different tables must have different raw names";
    };
};

// ===================================================================
// ValidTable — runtime suite
// ===================================================================

suite<"ValidTable"> valid_table_suite = [] {
    "COLUMN_FIELD table satisfies ValidTable at runtime"_test = [] {
        expect(constant<ValidTable<table_with_macro>>);
        expect(constant<ValidTable<other_table>>);
    };

    "explicit nested-tag table satisfies ValidTable"_test = [] {
        expect(constant<ValidTable<table_with_explicit_tags>>);
    };

    "NTTP column table satisfies ValidTable"_test = [] {
        expect(constant<ValidTable<table_with_nttp_columns>>);
    };

    "empty table satisfies ValidTable"_test = [] {
        expect(constant<ValidTable<empty_table>>);
    };

    "column field types do not satisfy ValidTable"_test = [] {
        expect(constant<!ValidTable<column_field<"id", uint32_t>>>);
        expect(constant<!ValidTable<tagged_column_field<table_with_macro::id_tag, uint32_t>>>);
    };

    "table with outer tag does not satisfy ValidTable"_test = [] {
        expect(constant<!ValidTable<table_with_outer_tag>>);
    };

    "table with borrowed tag from other table does not satisfy ValidTable"_test = [] {
        expect(constant<!ValidTable<table_with_borrowed_tag>>);
    };

    "cross-table isolation: same column name yields distinct types"_test = [] {
        expect(constant<!std::is_same_v<table_with_macro::id, other_table::id>>);
    };
};

// ===================================================================
// ValidTable-gated entry points — smoke tests confirming they compile
// and produce correct SQL for well-formed tables
// ===================================================================

suite<"ValidTable entry points"> entry_points_suite = [] {
    "create_table compiles for valid table"_test = [] {
        auto const sql = create_table<table_with_macro>().build_sql();
        expect(sql.find("table_with_macro") != std::string::npos);
    };

    "drop_table compiles for valid table"_test = [] {
        auto const sql = drop_table<table_with_macro>().build_sql();
        expect(sql.find("table_with_macro") != std::string::npos);
    };

    "insert_into compiles for valid table"_test = [] {
        table_with_macro row;
        row.id_       = 1u;
        row.quantity_ = 10u;
        row.price_    = 9.99;
        auto const sql = insert_into<table_with_macro>().values(row).build_sql();
        expect(sql.find("INSERT INTO") != std::string::npos);
        expect(sql.find("table_with_macro") != std::string::npos);
    };

    "select from valid table compiles"_test = [] {
        auto const sql = select<table_with_macro::id>()
            .from<table_with_macro>()
            .build_sql();
        expect(sql.find("SELECT") != std::string::npos);
        expect(sql.find("id") != std::string::npos);
    };

    "count<Table> compiles for valid table"_test = [] {
        auto const sql = count<table_with_macro>().build_sql();
        expect(sql.find("COUNT(*)") != std::string::npos);
    };

    "truncate_table compiles for valid table"_test = [] {
        auto const sql = truncate_table<table_with_macro>().build_sql();
        expect(sql.find("TRUNCATE") != std::string::npos);
    };
};
