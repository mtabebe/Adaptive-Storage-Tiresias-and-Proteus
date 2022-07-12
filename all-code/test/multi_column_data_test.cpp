#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/common/packed_pointer.h"
#include "../src/common/string_utils.h"
#include "../src/common/thread_utils.h"
#include "../src/data-site/db/multi_column_data.h"

class multi_column_data_test : public ::testing::Test {};

TEST_F( multi_column_data_test, multi_column_update_and_get ) {
    multi_column_data empty_mc;
    EXPECT_EQ( 0, empty_mc.get_num_columns() );
    auto found_u64 = empty_mc.get_uint64_data( 0 );
    EXPECT_FALSE( std::get<0>( found_u64 ) );

    std::vector<multi_column_data_type> col_types;

    col_types.emplace_back(
        multi_column_data_type( cell_data_type::UINT64, sizeof( uint64_t ) ) );
    col_types.emplace_back(
        multi_column_data_type( cell_data_type::STRING, 6 /*foobar */ ) );
    col_types.emplace_back(
        multi_column_data_type( cell_data_type::DOUBLE, sizeof( double ) ) );

    multi_column_data mc1( col_types );

    EXPECT_EQ( 3, mc1.get_num_columns() );
    for( uint32_t pos = 0; pos < col_types.size(); pos++ ) {
        EXPECT_EQ( col_types.at( pos ), mc1.get_column_type( pos ) );
    }
    found_u64 = mc1.get_uint64_data( 0 );
    EXPECT_FALSE( std::get<0>( found_u64 ) );
    EXPECT_EQ( 0, std::get<1>( found_u64 ) );

    auto found_str = mc1.get_string_data( 1 );
    EXPECT_FALSE( std::get<0>( found_str ) );

    auto found_double = mc1.get_double_data( 2 );
    EXPECT_FALSE( std::get<0>( found_double ) );
    EXPECT_DOUBLE_EQ( 0, std::get<1>( found_double ) );

    // set 0 to 15
    bool inserted = mc1.set_uint64_data( 0, 15 );
    EXPECT_TRUE( inserted );

    found_u64 = mc1.get_uint64_data( 0 );
    EXPECT_TRUE( std::get<0>( found_u64 ) );
    EXPECT_EQ( 15, std::get<1>( found_u64 ) );

    // remove then add to 25
    mc1.remove_data( 0 );
    found_u64 = mc1.get_uint64_data( 0 );
    EXPECT_FALSE( std::get<0>( found_u64 ) );
    EXPECT_EQ( 0, std::get<1>( found_u64 ) );

    inserted = mc1.set_uint64_data( 0, 25 );
    EXPECT_TRUE( inserted );

    found_u64 = mc1.get_uint64_data( 0 );
    EXPECT_TRUE( std::get<0>( found_u64 ) );
    EXPECT_EQ( 25, std::get<1>( found_u64 ) );

    found_str = mc1.get_string_data( 1 );
    EXPECT_FALSE( std::get<0>( found_str ) );

    found_double = mc1.get_double_data( 2 );
    EXPECT_FALSE( std::get<0>( found_double ) );
    EXPECT_DOUBLE_EQ( 0, std::get<1>( found_double ) );

    // set 2 to 25.5
    inserted = mc1.set_double_data( 2, 25.5 );
    EXPECT_TRUE( inserted );

    found_u64 = mc1.get_uint64_data( 0 );
    EXPECT_TRUE( std::get<0>( found_u64 ) );
    EXPECT_EQ( 25, std::get<1>( found_u64 ) );

    found_str = mc1.get_string_data( 1 );
    EXPECT_FALSE( std::get<0>( found_str ) );

    found_double = mc1.get_double_data( 2 );
    EXPECT_TRUE( std::get<0>( found_double ) );
    EXPECT_DOUBLE_EQ( 25.5, std::get<1>( found_double ) );

    // set 1 to foobar
    std::string str = "foobar";
    inserted = mc1.set_string_data( 1, str );
    EXPECT_TRUE( inserted );

    found_u64 = mc1.get_uint64_data( 0 );
    EXPECT_TRUE( std::get<0>( found_u64 ) );
    EXPECT_EQ( 25, std::get<1>( found_u64 ) );

    found_str = mc1.get_string_data( 1 );
    EXPECT_TRUE( std::get<0>( found_str ) );
    EXPECT_EQ( str, std::get<1>( found_str ) );

    found_double = mc1.get_double_data( 2 );
    EXPECT_TRUE( std::get<0>( found_double ) );
    EXPECT_DOUBLE_EQ( 25.5, std::get<1>( found_double ) );

    // set 1 to foo
    str = "foo";
    inserted = mc1.set_string_data( 1, str );
    EXPECT_TRUE( inserted );

    found_u64 = mc1.get_uint64_data( 0 );
    EXPECT_TRUE( std::get<0>( found_u64 ) );
    EXPECT_EQ( 25, std::get<1>( found_u64 ) );

    found_str = mc1.get_string_data( 1 );
    EXPECT_TRUE( std::get<0>( found_str ) );
    EXPECT_EQ( str, std::get<1>( found_str ) );

    found_double = mc1.get_double_data( 2 );
    EXPECT_TRUE( std::get<0>( found_double ) );
    EXPECT_DOUBLE_EQ( 25.5, std::get<1>( found_double ) );

    // set 1 to foo
    str = "horizondb";
    inserted = mc1.set_string_data( 1, str );
    EXPECT_TRUE( inserted );

    found_u64 = mc1.get_uint64_data( 0 );
    EXPECT_TRUE( std::get<0>( found_u64 ) );
    EXPECT_EQ( 25, std::get<1>( found_u64 ) );

    found_str = mc1.get_string_data( 1 );
    EXPECT_TRUE( std::get<0>( found_str ) );
    EXPECT_EQ( str, std::get<1>( found_str ) );

    found_double = mc1.get_double_data( 2 );
    EXPECT_TRUE( std::get<0>( found_double ) );
    EXPECT_DOUBLE_EQ( 25.5, std::get<1>( found_double ) );

    // lets change the type
    // set 2 to adaptivehtap
    std::string str2 = "adaptivehtap";
    inserted = mc1.set_string_data( 2, str2 );
    EXPECT_TRUE( inserted );

    found_u64 = mc1.get_uint64_data( 0 );
    EXPECT_TRUE( std::get<0>( found_u64 ) );
    EXPECT_EQ( 25, std::get<1>( found_u64 ) );

    found_str = mc1.get_string_data( 1 );
    EXPECT_TRUE( std::get<0>( found_str ) );
    EXPECT_EQ( str, std::get<1>( found_str ) );

    found_double = mc1.get_double_data( 2 );
    EXPECT_FALSE( std::get<0>( found_double ) );

    found_str = mc1.get_string_data( 2 );
    EXPECT_TRUE( std::get<0>( found_str ) );
    EXPECT_EQ( str2, std::get<1>( found_str ) );

    // lets erase stuff
    // set 2 to ""
    str2 = "";
    inserted = mc1.set_string_data( 2, str2 );
    EXPECT_TRUE( inserted );

    found_u64 = mc1.get_uint64_data( 0 );
    EXPECT_TRUE( std::get<0>( found_u64 ) );
    EXPECT_EQ( 25, std::get<1>( found_u64 ) );

    found_str = mc1.get_string_data( 1 );
    EXPECT_TRUE( std::get<0>( found_str ) );
    EXPECT_EQ( str, std::get<1>( found_str ) );

    found_double = mc1.get_double_data( 2 );
    EXPECT_FALSE( std::get<0>( found_double ) );

    found_str = mc1.get_string_data( 2 );
    EXPECT_TRUE( std::get<0>( found_str ) );
    EXPECT_EQ( str2, std::get<1>( found_str ) );

    // lets change the type back

    inserted = mc1.set_double_data( 2, 35.5 );
    EXPECT_TRUE( inserted );

    found_u64 = mc1.get_uint64_data( 0 );
    EXPECT_TRUE( std::get<0>( found_u64 ) );
    EXPECT_EQ( 25, std::get<1>( found_u64 ) );

    found_str = mc1.get_string_data( 1 );
    EXPECT_TRUE( std::get<0>( found_str ) );
    EXPECT_EQ( str, std::get<1>( found_str ) );

    found_double = mc1.get_double_data( 2 );
    EXPECT_TRUE( std::get<0>( found_double ) );
    EXPECT_DOUBLE_EQ( 35.5, std::get<1>( found_double ) );

    found_str = mc1.get_string_data( 2 );
    EXPECT_FALSE( std::get<0>( found_str ) );
}

TEST_F( multi_column_data_test, multi_column_type_comparison ) {
    multi_column_data_type u64_type( cell_data_type::UINT64,
                                     sizeof( uint64_t ) );
    multi_column_data_type u64_type2( cell_data_type::UINT64,
                                     sizeof( uint64_t ) );
    multi_column_data_type i64_type( cell_data_type::INT64, sizeof( int64_t ) );
    multi_column_data_type str5_type( cell_data_type::STRING, 5 );
    multi_column_data_type str3_type( cell_data_type::STRING, 3 );

    EXPECT_EQ( u64_type, u64_type );
    EXPECT_EQ( u64_type, u64_type2 );

    EXPECT_NE( u64_type, i64_type);
    EXPECT_LT( u64_type, i64_type);
    EXPECT_LE( u64_type, i64_type);
    EXPECT_GT( i64_type, u64_type );
    EXPECT_GE( i64_type, u64_type );

    EXPECT_EQ( i64_type, i64_type );
    EXPECT_LT( i64_type, str5_type );

    EXPECT_EQ( str5_type, str5_type);
    EXPECT_NE( str5_type, str3_type );
    EXPECT_LT( str3_type, str5_type );
    EXPECT_LE( str3_type, str5_type );
    EXPECT_GT( str5_type, str3_type );
    EXPECT_GE( str5_type, str3_type );

}

TEST_F( multi_column_data_test, multi_column_comparison ) {
    multi_column_data empty_mc1;
    multi_column_data empty_mc2;

    EXPECT_EQ( empty_mc1, empty_mc2 );

    std::vector<multi_column_data_type> col_types;

    col_types.emplace_back(
        multi_column_data_type( cell_data_type::UINT64, sizeof( uint64_t ) ) );


    multi_column_data mc3( col_types );
    multi_column_data mc5( col_types );

    col_types.emplace_back(
        multi_column_data_type( cell_data_type::STRING, 6 /*foobar */ ) );

    multi_column_data mc4( col_types );
    multi_column_data mc6( col_types );

    col_types.emplace_back(
        multi_column_data_type( cell_data_type::DOUBLE, sizeof( double ) ) );

    multi_column_data mc1( col_types );
    multi_column_data mc2( col_types );

    bool inserted = mc1.set_uint64_data( 0, 25 );
    EXPECT_TRUE( inserted );
    inserted = mc1.set_string_data( 1, "foobar" );
    EXPECT_TRUE( inserted );
    inserted = mc1.set_double_data( 2, 25.5 );
    EXPECT_TRUE( inserted );

    inserted = mc2.set_uint64_data( 0, 25 );
    EXPECT_TRUE( inserted );
    inserted = mc2.set_string_data( 1, "foobar" );
    EXPECT_TRUE( inserted );
    inserted = mc2.set_double_data( 2, 25.5 );
    EXPECT_TRUE( inserted );

    inserted = mc3.set_uint64_data( 0, 20 );
    EXPECT_TRUE( inserted );

    inserted = mc4.set_uint64_data( 0, 25 );
    EXPECT_TRUE( inserted );
    inserted = mc4.set_string_data( 1, "barfoo" );
    EXPECT_TRUE( inserted );

    inserted = mc5.set_uint64_data( 0, 25 );
    EXPECT_TRUE( inserted );

    inserted = mc6.set_uint64_data( 0, 25 );
    EXPECT_TRUE( inserted );
    inserted = mc6.set_string_data( 1, "foo" );
    EXPECT_TRUE( inserted );

    // order
    // empty_mc1 = empty_mc2 < mc3 < mc4 < m6 < mc1 = mc2


    EXPECT_NE( empty_mc1, mc1 );
    EXPECT_LT( empty_mc1, mc3 );
    EXPECT_LT( empty_mc1, mc4 );
    EXPECT_LT( empty_mc1, mc6 );
    EXPECT_LT( empty_mc1, mc1 );
    EXPECT_LT( empty_mc1, mc2 );

    EXPECT_EQ( empty_mc1, empty_mc2 );
    EXPECT_LE( empty_mc1, empty_mc2 );
    EXPECT_GE( empty_mc1, empty_mc2 );

    EXPECT_NE( mc3, mc1 );
    EXPECT_LT( mc3, mc1 );
    EXPECT_LE( mc3, mc1 );

    EXPECT_LT( mc3, mc4 );
    EXPECT_LT( mc3, mc6 );
    EXPECT_LT( mc3, mc2 );


    EXPECT_NE( mc4, mc1 );
    EXPECT_LT( mc4, mc1 );
    EXPECT_LE( mc4, mc1 );

    EXPECT_LT( mc4, mc6 );
    EXPECT_LT( mc4, mc2 );

    EXPECT_NE( mc6, mc1 );
    EXPECT_LT( mc6, mc1 );
    EXPECT_LE( mc6, mc1 );

    EXPECT_LT( mc6, mc2 );

    EXPECT_EQ( mc1, mc2 );
    EXPECT_LE( mc1, mc2 );
    EXPECT_GE( mc1, mc2 );
}

TEST_F( multi_column_data_test, multi_column_arithmetic ) {
    multi_column_data empty_mc;

    std::vector<multi_column_data_type> col_types;

    col_types.emplace_back(
        multi_column_data_type( cell_data_type::UINT64, sizeof( uint64_t ) ) );
    col_types.emplace_back(
        multi_column_data_type( cell_data_type::STRING, 6 /*foobar */ ) );
    col_types.emplace_back(
        multi_column_data_type( cell_data_type::DOUBLE, sizeof( double ) ) );

    multi_column_data mc1( col_types );
    multi_column_data mc2( col_types );

    bool inserted = mc1.set_uint64_data( 0, 25 );
    EXPECT_TRUE( inserted );
    inserted = mc1.set_string_data( 1, "foobar" );
    EXPECT_TRUE( inserted );
    inserted = mc1.set_double_data( 2, 25.5 );
    EXPECT_TRUE( inserted );

    inserted = mc2.set_uint64_data( 0, 50 );
    EXPECT_TRUE( inserted );
    inserted = mc2.set_string_data( 1, "barfoo" );
    EXPECT_TRUE( inserted );
    inserted = mc2.set_double_data( 2, 52.5 );
    EXPECT_TRUE( inserted );

    multi_column_data adder;
    adder.add_value_to_data( empty_mc, 1 );

    EXPECT_EQ( 0, adder.get_num_columns() );

    adder.add_value_to_data( mc1, 1 );
    EXPECT_EQ( 3, adder.get_num_columns() );

    col_types.at( 1 ).size_ = 0;
    for( uint32_t pos = 0; pos < col_types.size(); pos++ ) {
        EXPECT_EQ( col_types.at( pos ), adder.get_column_type( pos ) );
    }
    auto found_u64 = adder.get_uint64_data( 0 );
    EXPECT_TRUE( std::get<0>( found_u64 ) );
    EXPECT_EQ( 25, std::get<1>( found_u64 ) );

    auto found_str = adder.get_string_data( 1 );
    EXPECT_TRUE( std::get<0>( found_str ) );

    auto found_double = adder.get_double_data( 2 );
    EXPECT_TRUE( std::get<0>( found_double ) );
    EXPECT_DOUBLE_EQ( 25.5, std::get<1>( found_double ) );

    adder.add_value_to_data( mc2, 1 );
    EXPECT_EQ( 3, adder.get_num_columns() );

    for( uint32_t pos = 0; pos < col_types.size(); pos++ ) {
        EXPECT_EQ( col_types.at( pos ), adder.get_column_type( pos ) );
    }
    found_u64 = adder.get_uint64_data( 0 );
    EXPECT_TRUE( std::get<0>( found_u64 ) );
    EXPECT_EQ( 75, std::get<1>( found_u64 ) );

    found_double = adder.get_double_data( 2 );
    EXPECT_TRUE( std::get<0>( found_double ) );
    EXPECT_DOUBLE_EQ( 78.0, std::get<1>( found_double ) );

    adder.add_value_to_data( mc1, -1 );
    EXPECT_EQ( 3, adder.get_num_columns() );

    for( uint32_t pos = 0; pos < col_types.size(); pos++ ) {
        EXPECT_EQ( col_types.at( pos ), adder.get_column_type( pos ) );
    }
    found_u64 = adder.get_uint64_data( 0 );
    EXPECT_TRUE( std::get<0>( found_u64 ) );
    EXPECT_EQ( 50, std::get<1>( found_u64 ) );

    found_double = adder.get_double_data( 2 );
    EXPECT_TRUE( std::get<0>( found_double ) );
    EXPECT_DOUBLE_EQ( 52.5, std::get<1>( found_double ) );

    adder.divide_by_constant(10);

    found_u64 = adder.get_uint64_data( 0 );
    EXPECT_TRUE( std::get<0>( found_u64 ) );
    EXPECT_EQ( 5, std::get<1>( found_u64 ) );

    found_double = adder.get_double_data( 2 );
    EXPECT_TRUE( std::get<0>( found_double ) );
    EXPECT_DOUBLE_EQ( 5.25, std::get<1>( found_double ) );
}

