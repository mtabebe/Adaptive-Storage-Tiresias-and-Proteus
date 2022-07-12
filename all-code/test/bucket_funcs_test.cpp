#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/common/bucket_funcs.h"

class bucket_funcs_test : public ::testing::Test {};

TEST_F( bucket_funcs_test, comparators ) {
    bucket_key bk1;
    bk1.map_key = 1;
    bk1.range_key = 0;

    bucket_key bk2;
    bk2.map_key = 1;
    bk2.range_key = 5;

    bucket_key bk3;
    bk3.map_key = 2;
    bk3.range_key = 3;

    bucket_key_sort_functor  sorter;
    bucket_key_equal_functor equalizer;
    bucket_key_hasher        hasher;

    EXPECT_TRUE( sorter( bk1, bk2 ) );
    EXPECT_FALSE( sorter( bk2, bk1 ) );
    EXPECT_FALSE( sorter( bk1, bk1 ) );
    EXPECT_TRUE( sorter( bk1, bk3 ) );

    EXPECT_TRUE( equalizer( bk1, bk1 ) );
    EXPECT_FALSE( equalizer( bk2, bk1 ) );

    EXPECT_EQ( hasher( bk1 ), hasher( bk1 ) );
    EXPECT_NE( hasher( bk1 ), hasher( bk2 ) );
}

TEST_F( bucket_funcs_test, conversions ) {
    uint64_t                                 bucket_size = 5;
    std::vector<table_partition_information> table_sizes;
    std::vector<cell_data_type> col_types = {cell_data_type::UINT64,
                                             cell_data_type::INT64};
    table_partition_information tpi = std::make_tuple<>(
        0 /*table id*/, 100 /* table size*/, bucket_size /*part size*/,
        2 /*col size*/, 2 /*num columns*/, col_types, partition_type::type::ROW,
        storage_tier_type::type::MEMORY );
    table_sizes.push_back( tpi );

    primary_key pk1;
    pk1.table_id = 0;
    pk1.row_id = 0;
    bucket_key bk1 = get_bucket_key( pk1, table_sizes );
    EXPECT_EQ( 0, bk1.map_key );
    EXPECT_EQ( 0, bk1.range_key );

    primary_key pk4;
    pk4.table_id = 0;
    pk4.row_id = 4;
    bucket_key bk4 = get_bucket_key( pk4, table_sizes );
    EXPECT_EQ( 0, bk4.map_key );
    EXPECT_EQ( 0, bk4.range_key );

    primary_key pk5;
    pk5.table_id = 0;
    pk5.row_id = 5;
    bucket_key bk5 = get_bucket_key( pk5, table_sizes );
    EXPECT_EQ( 0, bk5.map_key );
    EXPECT_EQ( 1, bk5.range_key );

    std::vector<bucket_key>  bks = {bk1, bk5};
    std::vector<primary_key> pks =
        convert_site_bucket_keys_to_primary_keys( bks, table_sizes );
    EXPECT_EQ( 10, pks.size() );
    for( int pos = 0; pos < (int) pks.size(); pos++ ) {
        const primary_key& pk = pks.at( pos );
        EXPECT_EQ( pk.table_id, 0 );
        EXPECT_EQ( pk.row_id, pos );
    }
}
