#include <boost/ut.hpp>
#include <string>
#include <string_view>

#include "ds_mysql/column_field.hpp"
#include "ds_mysql/text_field.hpp"
#include "ds_mysql/varchar_field.hpp"

using namespace boost::ut;
using namespace ds_mysql;
using namespace std::string_view_literals;

// ===================================================================
// text_field
// ===================================================================

suite<"text_field"> text_field_suite = [] {
    "text_field default-constructed is empty"_test = [] {
        text_field<> f;
        expect(f.empty());
        expect(f.size() == 0_u);
    };

    "text_field constructed from string_view stores value"_test = [] {
        text_field<> f{"hello"};
        expect(f.view() == "hello"sv);
        expect(f.size() == 5_u);
        expect(!f.empty());
    };

    "text_field equality - same value"_test = [] {
        text_field<> a{"foo"};
        text_field<> b{"foo"};
        expect(a == b);
    };

    "text_field equality - different value"_test = [] {
        text_field<> a{"foo"};
        text_field<> b{"bar"};
        expect(!(a == b));
    };

    "text_field equality with string_view"_test = [] {
        text_field<> f{"hello"};
        expect(f == "hello"sv);
    };

    "text_field implicit conversion to string_view"_test = [] {
        text_field<> f{"world"};
        std::string_view sv = f;
        expect(sv == "world"sv);
    };

    "mediumtext_field stores and retrieves value"_test = [] {
        mediumtext_field f{"medium content"};
        expect(f.view() == "medium content"sv);
    };

    "longtext_field stores and retrieves value"_test = [] {
        longtext_field f{"long content"};
        expect(f.view() == "long content"sv);
    };

    // Compile-time trait checks
    static_assert(is_text_field_v<text_field<>>);
    static_assert(is_text_field_v<mediumtext_field>);
    static_assert(is_text_field_v<longtext_field>);
    static_assert(!is_text_field_v<int>);
    static_assert(is_text_field<text_field<>>::kind == text_kind::text);
    static_assert(is_text_field<mediumtext_field>::kind == text_kind::mediumtext);
    static_assert(is_text_field<longtext_field>::kind == text_kind::longtext);
};

// ===================================================================
// varchar_field
// ===================================================================

suite<"varchar_field"> varchar_field_suite = [] {
    "varchar_field create — valid string within capacity"_test = [] {
        auto result = varchar_field<8>::create("AAPL");
        expect(fatal(result.has_value()));
        expect(result->view() == "AAPL"sv);
        expect(result->size() == 4_u);
    };

    "varchar_field create — string at exact capacity"_test = [] {
        auto result = varchar_field<4>::create("AAPL");
        expect(fatal(result.has_value()));
        expect(result->view() == "AAPL"sv);
    };

    "varchar_field create — string exceeds capacity returns error"_test = [] {
        auto result = varchar_field<3>::create("AAPL");
        expect(!result.has_value());
        expect(result.error() == varchar_error::too_long);
    };

    "varchar_field create — empty string"_test = [] {
        auto result = varchar_field<32>::create("");
        expect(fatal(result.has_value()));
        expect(result->empty());
    };

    "varchar_field default-constructed is empty"_test = [] {
        varchar_field<32> f;
        expect(f.empty());
        expect(f.size() == 0_u);
        expect(f.view() == ""sv);
    };

    "varchar_field constructed from string literal stores value"_test = [] {
        varchar_field<32> f{"AAPL"};
        expect(f.view() == "AAPL"sv);
        expect(f.size() == 4_u);
        expect(!f.empty());
    };

    "varchar_field equality — same value"_test = [] {
        varchar_field<32> a{"AAPL"};
        varchar_field<32> b{"AAPL"};
        expect(a == b);
    };

    "varchar_field equality — different value"_test = [] {
        varchar_field<32> a{"AAPL"};
        varchar_field<32> b{"MSFT"};
        expect(!(a == b));
    };

    "varchar_field equality with string_view"_test = [] {
        varchar_field<32> f{"AAPL"};
        expect(f == "AAPL"sv);
    };

    "varchar_field implicit conversion to string_view"_test = [] {
        varchar_field<32> f{"AAPL"};
        std::string_view sv = f;
        expect(sv == "AAPL"sv);
    };

    "varchar_field c_str is null-terminated"_test = [] {
        varchar_field<32> f{"hi"};
        expect(std::string_view{f.c_str()} == "hi"sv);
    };

    // Compile-time trait checks
    static_assert(is_varchar_field_v<varchar_field<32>>);
    static_assert(is_varchar_field_v<varchar_field<255>>);
    static_assert(!is_varchar_field_v<int>);
    static_assert(!is_varchar_field_v<std::string_view>);
    static_assert(is_varchar_field<varchar_field<64>>::capacity == 64);

    // varchar_field capacity matches template parameter
    static_assert(varchar_field<32>::capacity == 32);
    static_assert(varchar_field<255>::capacity == 255);
    static_assert(varchar_field<32>::max_size() == 32);
};

// ===================================================================
// tagged_column_field — tag-struct alias
// ===================================================================

namespace {
struct price_tag {};
struct order_id_tag {};
struct no_suffix {};

// Different tags → different types (cross-table isolation)
static_assert(!std::is_same_v<tagged_column_field<price_tag, double>, tagged_column_field<order_id_tag, double>>);
// Same tag + same value type → same type
static_assert(std::is_same_v<tagged_column_field<price_tag, double>, tagged_column_field<price_tag, double>>);
// tagged_column_field satisfies ColumnFieldType (derived from column_field_tag)
static_assert(ds_mysql::ColumnFieldType<tagged_column_field<price_tag, double>>);
}  // namespace

suite<"tagged_column_field"> tagged_column_field_suite = [] {
    "tagged_column_field column_name matches derived tag name"_test = [] {
        using price = tagged_column_field<price_tag, double>;
        expect(price::column_name() == "price"sv);
    };

    "tagged_column_field with multi-word tag"_test = [] {
        using order_id = tagged_column_field<order_id_tag, uint32_t>;
        expect(order_id::column_name() == "order_id"sv);
    };

    "tagged_column_field without _tag suffix keeps full name"_test = [] {
        using ns = tagged_column_field<no_suffix, int>;
        expect(ns::column_name() == "no_suffix"sv);
    };
};

namespace {
using string_column = column_field<"string_col", std::string>;
}  // namespace

suite<"column_field string constructors"> column_field_string_suite = [] {
    "column_field<std::string> - construct from const string ref"_test = [] {
        std::string const str = "hello world";
        string_column col{str};
        expect(col.value == "hello world");
    };

    "column_field<std::string> - assign from const string ref"_test = [] {
        string_column col;
        std::string const str = "assigned";
        col = str;
        expect(col.value == "assigned");
    };
};
