#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/common/packed_pointer.h"
#include "../src/common/string_utils.h"
#include "../src/common/thread_utils.h"
#include "../src/data-site/db/packed_column_records.h"

class column_record_test : public ::testing::Test {};

void check_stats( const packed_column_data<uint64_t>& packed, uint32_t count,
                  uint64_t min, uint64_t max, uint64_t avg, uint64_t total ) {
    DVLOG( 40 ) << "Check Stats:" << packed;
    EXPECT_EQ( packed.stats_.count_, count );
    if( count > 0 ) {
        EXPECT_EQ( packed.stats_.min_, min );
        EXPECT_EQ( packed.stats_.max_, max );
        EXPECT_EQ( packed.stats_.average_, avg );
        EXPECT_EQ( packed.stats_.sum_, total );
    }
}

void check_found( uint64_t lookup, bool is_sorted,
                  const packed_column_data<uint64_t>& packed, bool is_found,
                  int32_t pos ) {
    auto found = packed.find_data_position( lookup, is_sorted );
    EXPECT_EQ( is_found, std::get<0>( found ) );
    EXPECT_EQ( pos, std::get<1>( found ) );
    if( is_found ) {
        EXPECT_EQ( lookup, packed.data_.at( pos ) );
    }
}

TEST_F( column_record_test, unsorted_data_test ) {
    packed_column_data<uint64_t> packed;
    bool is_sorted = false;

    // [ ]
    check_found( 7, is_sorted, packed, false, 0 );
    check_stats( packed, 0, 0, 0, 0, 0 );

    packed.insert_data_at_position( 0, 7, 1 /*count*/, true /* do maintenance*/,
                                    is_sorted );
    LOG( INFO ) << "Insert 7:" << packed;
    // [ 7 ]
    check_stats( packed, 1, 7, 7, 7, 7 );

    check_found( 7, is_sorted, packed, true, 0 );
    check_found( 9, is_sorted, packed, false, 1 );

    // [ 7, 9 ]
    packed.insert_data_at_position( 1, 9, 1 /*count*/, true /* do maintenance*/,
                                    is_sorted );
    LOG( INFO ) << "Insert 9:" << packed;
    check_stats( packed, 2, 7, 9, 8, 16 );

    check_found( 7, is_sorted, packed, true, 0 );
    check_found( 9, is_sorted, packed, true, 1 );
    check_found( 8, is_sorted, packed, false, 2 );

    // [ 7, 9, 8 ]
    packed.insert_data_at_position( 2, 8, 1 /*count*/, true /* do maintenance*/,
                                    is_sorted );
    LOG( INFO ) << "Insert 8:" << packed;
    check_stats( packed, 3, 7, 9, 8, 24 );

    check_found( 7, is_sorted, packed, true, 0 );
    check_found( 9, is_sorted, packed, true, 1 );
    check_found( 8, is_sorted, packed, true, 2 );

    check_found( 3, is_sorted, packed, false, 3 );

    // [ 7, 9, 8, 3]
    packed.insert_data_at_position( 3, 3, 1 /* count */,
                                    true /* do maintenance*/, is_sorted );
    LOG( INFO ) << "Insert 3:" << packed;

    check_found( 7, is_sorted, packed, true, 0 );
    check_found( 9, is_sorted, packed, true, 1 );
    check_found( 8, is_sorted, packed, true, 2 );
    check_found( 3, is_sorted, packed, true, 3 );

    check_stats( packed, 4, 3, 9, 6, 27 );

    check_found( 5, is_sorted, packed, false, 4 );

    // [ 7, 9, 8, 3, 5]
    packed.insert_data_at_position( 4, 5, 1 /*count*/, true /* do maintenance*/,
                                    is_sorted );
    LOG( INFO ) << "Insert 5:" << packed;

    check_found( 7, is_sorted, packed, true, 0 );
    check_found( 9, is_sorted, packed, true, 1 );
    check_found( 8, is_sorted, packed, true, 2 );
    check_found( 3, is_sorted, packed, true, 3 );
    check_found( 5, is_sorted, packed, true, 4 );

    check_stats( packed, 5, 3, 9, 6, 32 );

    check_found( 10, is_sorted, packed, false, 5 );

    // [ 7, 9, 8, 3, 5, 10]
    packed.insert_data_at_position( 5, 10, 1 /*count*/,
                                    true /* do maintenance*/, is_sorted );
    LOG( INFO ) << "Insert 10:" << packed;

    check_found( 7, is_sorted, packed, true, 0 );
    check_found( 9, is_sorted, packed, true, 1 );
    check_found( 8, is_sorted, packed, true, 2 );
    check_found( 3, is_sorted, packed, true, 3 );
    check_found( 5, is_sorted, packed, true, 4 );
    check_found( 10, is_sorted, packed, true, 5 );

    check_stats( packed, 6, 3, 10, 7, 42 );

    // [ 7, 9, 8, 3, 5, 10, 7]
    // now lets add some duplicates
    packed.insert_data_at_position( 6, 7, 1 /*count */,
                                    true /* do maintenance*/, is_sorted );
    LOG( INFO ) << "Dupl 7:" << packed;
    check_stats( packed, 7, 3, 10, 7, 49 );

    EXPECT_EQ( 7, packed.data_.at( 0 ) );
    check_found( 9, is_sorted, packed, true, 1 );
    check_found( 8, is_sorted, packed, true, 2 );
    check_found( 3, is_sorted, packed, true, 3 );
    check_found( 5, is_sorted, packed, true, 4 );
    check_found( 10, is_sorted, packed, true, 5 );
    check_found( 7, is_sorted, packed, true, 6 );

    // [ 7, 9, 8, 3, 5, 10, 7, 7]
    // now lets add some duplicates
    packed.insert_data_at_position( 6, 7, 1 /* count */,
                                    true /* do maintenance*/, is_sorted );
    LOG( INFO ) << "Dupl 7:" << packed;
    check_stats( packed, 8, 3, 10, 7, 56 );

    EXPECT_EQ( 7, packed.data_.at( 0 ) );
    check_found( 9, is_sorted, packed, true, 1 );
    check_found( 8, is_sorted, packed, true, 2 );
    check_found( 3, is_sorted, packed, true, 3 );
    check_found( 5, is_sorted, packed, true, 4 );
    check_found( 10, is_sorted, packed, true, 5 );
    EXPECT_EQ( 7, packed.data_.at( 6 ) );
    check_found( 7, is_sorted, packed, true, 7 );

    // [ 7, 9, 8, 3, 5, 10, 7 ]
    packed.remove_data_from_position( 6, true /* do maintenance */, is_sorted );
    LOG( INFO ) << "Remove 7:" << packed;
    check_stats( packed, 7, 3, 10, 7, 49 );

    EXPECT_EQ( 7, packed.data_.at( 0 ) );
    check_found( 9, is_sorted, packed, true, 1 );
    check_found( 8, is_sorted, packed, true, 2 );
    check_found( 3, is_sorted, packed, true, 3 );
    check_found( 5, is_sorted, packed, true, 4 );
    check_found( 10, is_sorted, packed, true, 5 );
    check_found( 7, is_sorted, packed, true, 6 );

    // [ 9, 8, 3, 5, 10, 7 ]
    packed.remove_data_from_position( 0, true /* do maintenance */, is_sorted );
    LOG( INFO ) << "Remove 7:" << packed;
    check_stats( packed, 6, 3, 10, 7, 42 );

    check_found( 9, is_sorted, packed, true, 0 );
    check_found( 8, is_sorted, packed, true, 1 );
    check_found( 3, is_sorted, packed, true, 2 );
    check_found( 5, is_sorted, packed, true, 3 );
    check_found( 10, is_sorted, packed, true, 4 );
    check_found( 7, is_sorted, packed, true, 5 );

    // [ 9, 8, 3, 5, 10 ]
    packed.remove_data_from_position( 5, true /* do maintenance */, is_sorted );
    LOG( INFO ) << "Remove 7:" << packed;
    check_stats( packed, 5, 3, 10, 7, 35 );

    check_found( 9, is_sorted, packed, true, 0 );
    check_found( 8, is_sorted, packed, true, 1 );
    check_found( 3, is_sorted, packed, true, 2 );
    check_found( 5, is_sorted, packed, true, 3 );
    check_found( 10, is_sorted, packed, true, 4 );

    // [ 9, 8, 5, 10 ]
    packed.remove_data_from_position( 2, true /* do maintenance */, is_sorted );
    LOG( INFO ) << "Remove 3:" << packed;

    check_found( 9, is_sorted, packed, true, 0 );
    check_found( 8, is_sorted, packed, true, 1 );
    check_found( 5, is_sorted, packed, true, 2 );
    check_found( 10, is_sorted, packed, true, 3 );

    check_stats( packed, 4, 5, 10, 8, 32 );

    packed.remove_data_from_position( 0, true /* do maintenance */, is_sorted );
    LOG( INFO ) << "Remove 9:" << packed;

    check_found( 8, is_sorted, packed, true, 0 );
    check_found( 5, is_sorted, packed, true, 1 );
    check_found( 10, is_sorted, packed, true, 2 );
    check_found( 9, is_sorted, packed, false, 3 );

    check_stats( packed, 3, 5, 10, 7, 23 );

    // [ 5, 10]
    packed.remove_data_from_position( 0, true /* do maintenance */, is_sorted );
    LOG( INFO ) << "Remove 8:" << packed;

    check_found( 5, is_sorted, packed, true, 0 );
    check_found( 10, is_sorted, packed, true, 1 );
    check_found( 7, is_sorted, packed, false, 2 );

    check_stats( packed, 2, 5, 10, 7, 15 );

    // [ 10]
    packed.remove_data_from_position( 0, true /* do maintenance */, is_sorted );
    LOG( INFO ) << "Remove 5:" << packed;

    check_found( 10, is_sorted, packed, true, 0 );
    check_found( 5, is_sorted, packed, false, 1 );
    check_stats( packed, 1, 10, 10, 10, 10 );

    // []
    packed.remove_data_from_position( 0, true /* do maintenance */, is_sorted );
    LOG( INFO ) << "Remove 10:" << packed;
    check_found( 10, is_sorted, packed, false, 0 );
    check_stats( packed, 0, 0, 0, 0, 0 );
}

TEST_F( column_record_test, sorted_data_test ) {
    packed_column_data<uint64_t> packed;

    LOG( INFO ) << "Init:" << packed;

    bool is_sorted = true;

    // [ ]
    check_found( 7, is_sorted, packed, false, 0 );
    check_stats( packed, 0, 0, 0, 0, 0 );

    packed.insert_data_at_position( 0, 7, 1 /*count */,
                                    true /* do maintenance*/, is_sorted );
    LOG( INFO ) << "Insert 7:" << packed;
    // [ 7 ]
    check_stats( packed, 1, 7, 7, 7, 7 );

    check_found( 7, is_sorted, packed, true, 0 );
    check_found( 9, is_sorted, packed, false, 1 );

    // [ 7, 9 ]
    packed.insert_data_at_position( 1, 9, 1 /* count */,
                                    true /* do maintenance*/, is_sorted );
    LOG( INFO ) << "Insert 9:" << packed;
    check_stats( packed, 2, 7, 9, 8, 16 );

    check_found( 7, is_sorted, packed, true, 0 );
    check_found( 9, is_sorted, packed, true, 1 );
    check_found( 8, is_sorted, packed, false, 1 );

    // [ 7, 8, 9 ]
    packed.insert_data_at_position( 1, 8, 1 /* count */,
                                    true /* do maintenance*/, is_sorted );
    LOG( INFO ) << "Insert 8:" << packed;
    check_stats( packed, 3, 7, 9, 8, 24 );

    check_found( 7, is_sorted, packed, true, 0 );
    check_found( 8, is_sorted, packed, true, 1 );
    check_found( 9, is_sorted, packed, true, 2 );

    check_found( 3, is_sorted, packed, false, 0 );

    // [ 3, 7, 8, 9]
    packed.insert_data_at_position( 0, 3, 1 /* count */,
                                    true /* do maintenance*/, is_sorted );
    LOG( INFO ) << "Insert 3:" << packed;

    check_found( 3, is_sorted, packed, true, 0 );
    check_found( 7, is_sorted, packed, true, 1 );
    check_found( 8, is_sorted, packed, true, 2 );
    check_found( 9, is_sorted, packed, true, 3 );

    check_stats( packed, 4, 3, 9, 6, 27 );

    check_found( 5, is_sorted, packed, false, 1 );

    // [ 3, 5, 7, 8, 9]
    packed.insert_data_at_position( 1, 5, 1 /* count */,
                                    true /* do maintenance*/, is_sorted );
    LOG( INFO ) << "Insert 5:" << packed;

    check_found( 3, is_sorted, packed, true, 0 );
    check_found( 5, is_sorted, packed, true, 1 );
    check_found( 7, is_sorted, packed, true, 2 );
    check_found( 8, is_sorted, packed, true, 3 );
    check_found( 9, is_sorted, packed, true, 4 );

    check_stats( packed, 5, 3, 9, 6, 32 );

    check_found( 10, is_sorted, packed, false, 5 );

    // [ 3, 5, 7, 8, 9, 10]
    packed.insert_data_at_position( 5, 10, 1 /* count */,
                                    true /* do maintenance*/, is_sorted );
    LOG( INFO ) << "Insert 10:" << packed;

    check_found( 3, is_sorted, packed, true, 0 );
    check_found( 5, is_sorted, packed, true, 1 );
    check_found( 7, is_sorted, packed, true, 2 );
    check_found( 8, is_sorted, packed, true, 3 );
    check_found( 9, is_sorted, packed, true, 4 );
    check_found( 10, is_sorted, packed, true, 5 );

    check_stats( packed, 6, 3, 10, 7, 42 );

    // now lets add some duplicates
    // [ 3, 5, 7x2, 8, 9, 10]
    packed.add_data_to_metadata( 2, 1 /*count */ );
    LOG( INFO ) << "Dupl 7:" << packed;
    check_stats( packed, 7, 3, 10, 7, 49 );

    check_found( 3, is_sorted, packed, true, 0 );
    check_found( 5, is_sorted, packed, true, 1 );
    check_found( 7, is_sorted, packed, true, 2 );
    check_found( 8, is_sorted, packed, true, 3 );
    check_found( 9, is_sorted, packed, true, 4 );
    check_found( 10, is_sorted, packed, true, 5 );

    // [ 3, 5, 7x3, 8, 9, 10]
    packed.add_data_to_metadata( 2, 1 /* count */ );
    LOG( INFO ) << "Tripl 7:" << packed;
    check_stats( packed, 8, 3, 10, 7, 56 );

    // [ 3, 5, 7x2, 8, 9, 10]
    packed.remove_data_from_metadata( 2 );
    LOG( INFO ) << "Dupl 7:" << packed;
    check_stats( packed, 7, 3, 10, 7, 49 );

    check_found( 3, is_sorted, packed, true, 0 );
    check_found( 5, is_sorted, packed, true, 1 );
    check_found( 7, is_sorted, packed, true, 2 );
    check_found( 8, is_sorted, packed, true, 3 );
    check_found( 9, is_sorted, packed, true, 4 );
    check_found( 10, is_sorted, packed, true, 5 );

    // [ 3, 5, 7, 8, 9, 10]
    packed.remove_data_from_metadata( 2 );
    LOG( INFO ) << "Dupl 7:" << packed;
    check_stats( packed, 6, 3, 10, 7, 42 );

    // [ 3, 5, 8, 9, 10]
    packed.remove_data_from_position( 2, true /* do maintenance */, is_sorted );
    LOG( INFO ) << "Remove 7:" << packed;

    check_found( 3, is_sorted, packed, true, 0 );
    check_found( 5, is_sorted, packed, true, 1 );
    check_found( 8, is_sorted, packed, true, 2 );
    check_found( 9, is_sorted, packed, true, 3 );
    check_found( 10, is_sorted, packed, true, 4 );

    check_stats( packed, 5, 3, 10, 7, 35 );

    // [ 5, 8, 9, 10]
    packed.remove_data_from_position( 0, true /* do maintenance */, is_sorted );
    LOG( INFO ) << "Remove 3:" << packed;

    check_found( 5, is_sorted, packed, true, 0 );
    check_found( 8, is_sorted, packed, true, 1 );
    check_found( 9, is_sorted, packed, true, 2 );
    check_found( 10, is_sorted, packed, true, 3 );

    check_stats( packed, 4, 5, 10, 8, 32 );

    // [ 5, 8, 10]
    packed.remove_data_from_position( 2, true /* do maintenance */, is_sorted );
    LOG( INFO ) << "Remove 9:" << packed;

    check_found( 5, is_sorted, packed, true, 0 );
    check_found( 8, is_sorted, packed, true, 1 );
    check_found( 10, is_sorted, packed, true, 2 );

    check_stats( packed, 3, 5, 10, 7, 23 );

    // [ 5, 10]
    packed.remove_data_from_position( 1, true /* do maintenance */, is_sorted );
    LOG( INFO ) << "Remove 8:" << packed;

    check_found( 5, is_sorted, packed, true, 0 );
    check_found( 7, is_sorted, packed, false, 1 );
    check_found( 10, is_sorted, packed, true, 1 );

    check_stats( packed, 2, 5, 10, 7, 15 );

    // [ 10]
    packed.remove_data_from_position( 0, true /* do maintenance */, is_sorted );
    LOG( INFO ) << "Remove 5:" << packed;

    check_found( 5, is_sorted, packed, false, 0 );
    check_found( 10, is_sorted, packed, true, 0 );
    check_stats( packed, 1, 10, 10, 10, 10 );

    // []
    packed.remove_data_from_position( 0, true /* do maintenance */, is_sorted );
    LOG( INFO ) << "Remove 10:" << packed;
    check_found( 10, is_sorted, packed, false, 0 );
    check_stats( packed, 0, 0, 0, 0, 0 );
}

void check_column_stats( const packed_column_records& col_rec,
                         bool do_maintenance, uint32_t count, uint64_t min,
                         uint64_t max, uint64_t avg, uint64_t total ) {
    DVLOG( 40 ) << "Check stats:" << col_rec;
    auto col_stats = col_rec.get_uint64_column_stats();

    EXPECT_EQ( col_stats.count_, count );

    if( count > 0 ) {
        EXPECT_EQ( col_stats.average_, avg );
        EXPECT_EQ( col_stats.sum_, total );

        if( do_maintenance ) {
            EXPECT_EQ( col_stats.min_, min );
            EXPECT_EQ( col_stats.max_, max );
        }
    }
}
void check_value_count( const packed_column_records& col_rec, uint64_t data,
                        uint32_t expected ) {

    DVLOG( 40 ) << "check_value_count data:" << data
                << ", expected: " << expected << ", col_rec:" << col_rec;

    uint32_t count = col_rec.get_value_count( data );
    EXPECT_EQ( count, expected );
}

void check_lookup_value( const packed_column_records& col_rec, uint64_t key,
                         bool expected_found, uint64_t data ) {
    DVLOG( 40 ) << "check_lookup_value key:" << key
                << ", expected_found: " << expected_found
                << ", expected_data:" << data << ", col_rec:" << col_rec;
    auto found = col_rec.get_uint64_data( key );
    EXPECT_EQ( expected_found, std::get<0>( found ) );
    if( expected_found ) {
        EXPECT_EQ( data, std::get<1>( found ) );
    }
}

void test_column_records( bool is_sorted, bool do_maintenance ) {
    packed_column_records recs( is_sorted /*is_column_sorted*/,
                                cell_data_type::UINT64, 5 /*key_start*/,
                                25 /* key_end*/ );

    check_column_stats( recs, do_maintenance, 0, 0, 0, 0, 0 );

    check_value_count( recs, 5, 0 );
    check_lookup_value( recs, 6, false, 7 );

    // [6:7]
    bool insert_ok = recs.insert_data( 6, (uint64_t) 7, do_maintenance );
    EXPECT_TRUE( insert_ok );
    check_column_stats( recs, do_maintenance, 1, 7, 7, 7, 7 );

    check_value_count( recs, 5, 0 );
    check_value_count( recs, 8, 0 );
    if( do_maintenance ) {
        check_value_count( recs, 7, 1 );
    }
    check_lookup_value( recs, 6, true, 7 );
    check_lookup_value( recs, 10, false, 9 );

    // [6:7, 10:9]
    insert_ok = recs.insert_data( 10, (uint64_t) 9, do_maintenance );
    EXPECT_TRUE( insert_ok );
    check_column_stats( recs, do_maintenance, 2, 7, 9, 8, 16 );

    recs.update_statistics();
    check_column_stats( recs, true, 2, 7, 9, 8, 16 );

    check_value_count( recs, 5, 0 );
    check_value_count( recs, 8, 0 );
    check_value_count( recs, 7, 1 );
    check_value_count( recs, 9, 1 );

    check_lookup_value( recs, 6, true, 7 );
    check_lookup_value( recs, 10, true, 9 );
    check_lookup_value( recs, 12, false, 8 );

    // [6:7, 10:9, 12:8, ]
    insert_ok = recs.insert_data( 12, (uint64_t) 8, do_maintenance );
    EXPECT_TRUE( insert_ok );
    check_column_stats( recs, do_maintenance, 3, 7, 9, 8, 24 );

    check_value_count( recs, 5, 0 );
    check_value_count( recs, 8, 1 );
    check_value_count( recs, 7, 1 );
    check_value_count( recs, 9, 1 );

    check_lookup_value( recs, 6, true, 7 );
    check_lookup_value( recs, 10, true, 9 );
    check_lookup_value( recs, 12, true, 8 );
    check_lookup_value( recs, 7, false, 3 );

    // [6:7, 10:9, 12:8, 7:3]
    insert_ok = recs.insert_data( 7, (uint64_t) 3, do_maintenance );
    EXPECT_TRUE( insert_ok );
    check_column_stats( recs, do_maintenance, 4, 3, 9, 6, 27 );

    check_value_count( recs, 5, 0 );
    check_value_count( recs, 8, 1 );
    check_value_count( recs, 7, 1 );
    check_value_count( recs, 9, 1 );
    if( do_maintenance) {
        check_value_count( recs, 3, 1 );
    }

    check_lookup_value( recs, 6, true, 7 );
    check_lookup_value( recs, 10, true, 9 );
    check_lookup_value( recs, 12, true, 8 );
    check_lookup_value( recs, 7, true, 3 );

    recs.update_statistics();
    check_column_stats( recs, do_maintenance, 4, 3, 9, 6, 27 );
    check_value_count( recs, 3, 1 );

    // [6:7, 10:9, 12:8, 7:4]
    insert_ok = recs.update_data( 7, (uint64_t) 4, do_maintenance );
    EXPECT_TRUE( insert_ok );
    check_column_stats( recs, do_maintenance, 4, 4, 9, 7, 28 );

    check_value_count( recs, 5, 0 );
    check_value_count( recs, 8, 1 );
    check_value_count( recs, 7, 1 );
    check_value_count( recs, 9, 1 );
    check_value_count( recs, 4, 1 );
    check_value_count( recs, 3, 0 );

    check_lookup_value( recs, 6, true, 7 );
    check_lookup_value( recs, 10, true, 9 );
    check_lookup_value( recs, 12, true, 8 );
    check_lookup_value( recs, 7, true, 4 );

    // now lets add some duplicates
    // [6:7, 10:9, 12:8, 7:4, 13:7]
    insert_ok = recs.insert_data( 13, (uint64_t) 7, do_maintenance );
    EXPECT_TRUE( insert_ok );
    check_column_stats( recs, do_maintenance, 5, 4, 9, 7, 35 );

    check_value_count( recs, 5, 0 );
    check_value_count( recs, 8, 1 );
    check_value_count( recs, 7, 2 );
    check_value_count( recs, 9, 1 );
    check_value_count( recs, 4, 1 );
    check_value_count( recs, 3, 0 );

    check_lookup_value( recs, 6, true, 7 );
    check_lookup_value( recs, 10, true, 9 );
    check_lookup_value( recs, 12, true, 8 );
    check_lookup_value( recs, 7, true, 4 );
    check_lookup_value( recs, 13, true, 7 );

    // now lets add some duplicates
    // [6:7, 10:9, 12:8, 7:4, 13:7, 15:7]
    insert_ok = recs.insert_data( 15, (uint64_t) 7, do_maintenance );
    EXPECT_TRUE( insert_ok );
    check_column_stats( recs, do_maintenance, 6, 4, 9, 7, 42 );

    check_value_count( recs, 5, 0 );
    check_value_count( recs, 8, 1 );
    check_value_count( recs, 7, 3 );
    check_value_count( recs, 9, 1 );
    check_value_count( recs, 4, 1 );
    check_value_count( recs, 3, 0 );

    check_lookup_value( recs, 6, true, 7 );
    check_lookup_value( recs, 10, true, 9 );
    check_lookup_value( recs, 12, true, 8 );
    check_lookup_value( recs, 7, true, 4 );
    check_lookup_value( recs, 13, true, 7 );
    check_lookup_value( recs, 15, true, 7 );

    // lets delete some stuff now

    // [6:7, 10:9, 12:8, 7:4, 15:7]
    bool remove_ok = recs.remove_data( 13, do_maintenance );
    EXPECT_TRUE( remove_ok );
    check_column_stats( recs, do_maintenance, 5, 4, 9, 7, 35 );

    check_value_count( recs, 5, 0 );
    check_value_count( recs, 8, 1 );
    check_value_count( recs, 7, 2 );
    check_value_count( recs, 9, 1 );
    check_value_count( recs, 4, 1 );
    check_value_count( recs, 3, 0 );

    check_lookup_value( recs, 6, true, 7 );
    check_lookup_value( recs, 10, true, 9 );
    check_lookup_value( recs, 12, true, 8 );
    check_lookup_value( recs, 7, true, 4 );
    check_lookup_value( recs, 13, false, 7 );
    check_lookup_value( recs, 15, true, 7 );

    // [6:7, 12:8, 7:4, 15:7]
    remove_ok = recs.remove_data( 10, do_maintenance );
    EXPECT_TRUE( remove_ok );
    check_column_stats( recs, do_maintenance, 4, 4, 8, 6, 26 );

    check_value_count( recs, 8, 1 );
    check_value_count( recs, 7, 2 );
    check_value_count( recs, 9, 0 );
    check_value_count( recs, 4, 1 );

    check_lookup_value( recs, 6, true, 7 );
    check_lookup_value( recs, 10, false, 9 );
    check_lookup_value( recs, 12, true, 8 );
    check_lookup_value( recs, 7, true, 4 );
    check_lookup_value( recs, 15, true, 7 );

    // [6:7, 7:4, 15:7]
    remove_ok = recs.remove_data( 12, do_maintenance );
    EXPECT_TRUE( remove_ok );
    check_column_stats( recs, do_maintenance, 3, 4, 7, 6, 18 );

    check_value_count( recs, 8, 0 );
    check_value_count( recs, 7, 2 );
    check_value_count( recs, 4, 1 );

    check_lookup_value( recs, 6, true, 7 );
    check_lookup_value( recs, 12, false, 8 );
    check_lookup_value( recs, 7, true, 4 );
    check_lookup_value( recs, 15, true, 7 );

    recs.update_statistics();
    check_column_stats( recs, true, 3, 4, 7, 6, 18 );

    // [ 7:4, 15:7]
    remove_ok = recs.remove_data( 6, do_maintenance );
    EXPECT_TRUE( remove_ok );
    check_column_stats( recs, do_maintenance, 2, 4, 7, 5, 11 );

    check_value_count( recs, 7, 1 );
    check_value_count( recs, 4, 1 );

    check_lookup_value( recs, 6, false, 7 );
    check_lookup_value( recs, 7, true, 4 );
    check_lookup_value( recs, 15, true, 7 );

    recs.update_statistics();
    check_column_stats( recs, do_maintenance, 2, 4, 7, 5, 11 );

    // [ 7:4]
    remove_ok = recs.remove_data( 15, do_maintenance );
    EXPECT_TRUE( remove_ok );
    check_column_stats( recs, do_maintenance, 1, 4, 4, 4, 4 );

    check_value_count( recs, 7, 0 );
    check_value_count( recs, 4, 1 );

    check_lookup_value( recs, 7, true, 4 );
    check_lookup_value( recs, 15, false, 7 );

    recs.update_statistics();
    check_column_stats( recs, do_maintenance, 1, 4, 4, 4, 4 );

    // [ ]
    remove_ok = recs.remove_data( 7, do_maintenance );
    EXPECT_TRUE( remove_ok );
    check_column_stats( recs, do_maintenance, 0, 0, 0, 0, 0 );

    check_value_count( recs, 4, 0 );

    check_lookup_value( recs, 7, false, 4 );

    recs.update_statistics();
    check_column_stats( recs, true, 0, 0, 0, 0, 0 );
}
TEST_F( column_record_test, sorted_column_with_maintenance_test ) {
    test_column_records( true, true );
}

TEST_F( column_record_test, sorted_column_without_maintenance_test ) {
    test_column_records( true, false );
}

TEST_F( column_record_test, unsorted_column_with_maintenance_test ) {
    test_column_records( false, true );
}

TEST_F( column_record_test, unsorted_column_without_maintenance_test ) {
    test_column_records( false, false );
}
