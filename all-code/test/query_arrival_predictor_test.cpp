#define GTEST_HAS_TR1_TUPLE 0

#include "../src/site-selector/query_arrival_predictor.h"
#include "../src/common/gdcheck.h"
#include "../src/site-selector/query_arrival_access_bytes.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

class query_arrival_predictor_test : public ::testing::Test {};

bool check_gen_query_arrival( const cell_key_ranges& ckr, bool is_read,
                              bool is_scan, bool is_write,
                              const std::vector<table_metadata>&  table_metas,
                              const std::vector<cell_key_ranges>& expected ) {
    auto gen_bytes = generate_query_arrival_access_bytes(
        ckr, is_read, is_scan, is_write, table_metas );
    EXPECT_EQ( gen_bytes.size(), expected.size() );
    if( gen_bytes.size() != expected.size() ) {
        return false;
    }
    for( uint32_t pos = 0; pos < gen_bytes.size(); pos++ ) {
        bool match =
            ( is_read == is_query_arrival_access_read( gen_bytes.at( pos ) ) );
        EXPECT_TRUE( match );
        if( !match ) {
            return false;
        }
        match =
            ( is_scan == is_query_arrival_access_scan( gen_bytes.at( pos ) ) );
        EXPECT_TRUE( match );
        if( !match ) {
            return false;
        }
        match = ( is_write ==
                  is_query_arrival_access_write( gen_bytes.at( pos ) ) );
        EXPECT_TRUE( match );
        if( !match ) {
            return false;
        }

        auto gen_ckr =
            get_query_arrival_access_ckr( gen_bytes.at( pos ), table_metas );
        EXPECT_EQ( gen_ckr, expected.at( pos ) );
        if( gen_ckr != expected.at( pos ) ) {
            return false;
        }
    }

    return true;
}

TEST_F( query_arrival_predictor_test, test_query_arrival_access_bytes ) {
    std::vector<cell_data_type> col_types;
    auto                        t1_meta = create_table_metadata(
        "t0", 0, 10 /* num cols */, col_types, 5 /* num_records_in_chain */,
        5 /* num_records_in_snapshot_chain */, k_unassigned_master,
        100 /* default_partition_size */, 2 /* default_column_size */,
        100 /* default_tracking_partition_size */,
        2 /* default_tracking_column_size */ );
    auto t2_meta = create_table_metadata(
        "t1", 1, 10 /* num cols */, col_types, 5 /* num_records_in_chain */,
        5 /* num_records_in_snapshot_chain */, k_unassigned_master,
        100 /* default_partition_size */, 1 /* default_column_size */,
        100 /* default_tracking_partition_size */,
        1 /* default_tracking_column_size */ );
    std::vector<table_metadata> table_metas = {t1_meta, t2_meta};

    auto ckr_0_0_0_0_0 =
        create_cell_key_ranges( 0, 0, 0, 0, 0 );  // {0, 0, 99, 0, 1 }
    auto ckr_0_0_50_0_0 =
        create_cell_key_ranges( 0, 0, 50, 0, 0 );  // {0, 0, 99, 0, 1 }
    auto ckr_0_0_99_0_0 =
        create_cell_key_ranges( 0, 0, 99, 0, 0 );  // {0, 0, 99, 0, 1 }
    auto ckr_0_0_199_0_0 = create_cell_key_ranges(
        0, 0, 199, 0, 0 );  // {0, 0, 99, 0, 1 }, { 0, 100, 199, 0, 1}
    auto ckr_0_150_250_0_0 = create_cell_key_ranges(
        0, 150, 250, 0, 0 );  // { 0, 100, 199, 0, 1}, {, 0, 200, 299, 0, 1}

    auto ckr_0_0_0_1_1 =
        create_cell_key_ranges( 0, 0, 0, 1, 1 );  // {0, 0, 99, 0, 1 }
    auto ckr_0_0_0_2_2 =
        create_cell_key_ranges( 0, 0, 0, 2, 2 );  // {0, 0, 99, 2, 3 }
    auto ckr_0_0_0_2_3 =
        create_cell_key_ranges( 0, 0, 0, 2, 3 );  // {0, 0, 99, 2, 3 }
    auto ckr_0_0_0_2_5 = create_cell_key_ranges(
        0, 0, 0, 2, 5 );  // {0, 0, 99, 2, 3 }, {0, 0, 99, 4, 5}

    auto ckr_0_150_250_2_5 = create_cell_key_ranges( 0, 150, 250, 2, 5 );
    // { 0, 100, 199, 2, 3}, {0, 0, 200, 299, 2, 3}
    // { 0, 100, 199, 4, 5}, {0, 0, 200, 299, 4, 5}

    auto ckr_1_150_250_2_5 = create_cell_key_ranges( 1, 150, 250, 2, 5 );
    // { 1, 100, 199, 2, 2}, {1, 0, 200, 299, 2, 2}
    // { 1, 100, 199, 3, 3}, {1, 0, 200, 299, 3, 3}
    // { 1, 100, 199, 4, 4}, {1, 0, 200, 299, 4, 4}
    // { 1, 100, 199, 5, 5}, {1, 0, 200, 299, 5, 5}
    //

    // match
    auto ckr_0_0_99_0_1 = create_cell_key_ranges( 0, 0, 99, 0, 1 );
    auto ckr_0_0_99_2_3 = create_cell_key_ranges( 0, 0, 99, 2, 3 );
    auto ckr_0_0_99_4_5 = create_cell_key_ranges( 0, 0, 99, 4, 5 );
    auto ckr_0_100_199_0_1 = create_cell_key_ranges( 0, 100, 199, 0, 1 );
    auto ckr_0_100_199_2_3 = create_cell_key_ranges( 0, 100, 199, 2, 3 );
    auto ckr_0_100_199_4_5 = create_cell_key_ranges( 0, 100, 199, 4, 5 );
    auto ckr_0_200_299_0_1 = create_cell_key_ranges( 0, 200, 299, 0, 1 );
    auto ckr_0_200_299_2_3 = create_cell_key_ranges( 0, 200, 299, 2, 3 );
    auto ckr_0_200_299_4_5 = create_cell_key_ranges( 0, 200, 299, 4, 5 );

    auto ckr_1_100_199_0_0 = create_cell_key_ranges( 1, 100, 199, 0, 0 );
    auto ckr_1_100_199_1_1 = create_cell_key_ranges( 1, 100, 199, 1, 1 );
    auto ckr_1_100_199_2_2 = create_cell_key_ranges( 1, 100, 199, 2, 2 );
    auto ckr_1_100_199_3_3 = create_cell_key_ranges( 1, 100, 199, 3, 3 );
    auto ckr_1_100_199_4_4 = create_cell_key_ranges( 1, 100, 199, 4, 4 );
    auto ckr_1_100_199_5_5 = create_cell_key_ranges( 1, 100, 199, 5, 5 );
    auto ckr_1_200_299_0_0 = create_cell_key_ranges( 1, 200, 299, 0, 0 );
    auto ckr_1_200_299_1_1 = create_cell_key_ranges( 1, 200, 299, 1, 1 );
    auto ckr_1_200_299_2_2 = create_cell_key_ranges( 1, 200, 299, 2, 2 );
    auto ckr_1_200_299_3_3 = create_cell_key_ranges( 1, 200, 299, 3, 3 );
    auto ckr_1_200_299_4_4 = create_cell_key_ranges( 1, 200, 299, 4, 4 );
    auto ckr_1_200_299_5_5 = create_cell_key_ranges( 1, 200, 299, 5, 5 );

    // check all the combinations of true
    bool matched = check_gen_query_arrival( ckr_0_0_0_0_0, true, true, true,
                                            table_metas, {ckr_0_0_99_0_1} );
    EXPECT_TRUE( matched );
    matched = check_gen_query_arrival( ckr_0_0_0_0_0, true, true, false,
                                       table_metas, {ckr_0_0_99_0_1} );
    EXPECT_TRUE( matched );
    matched = check_gen_query_arrival( ckr_0_0_0_0_0, true, false, true,
                                       table_metas, {ckr_0_0_99_0_1} );
    EXPECT_TRUE( matched );
    matched = check_gen_query_arrival( ckr_0_0_0_0_0, true, false, false,
                                       table_metas, {ckr_0_0_99_0_1} );
    matched = check_gen_query_arrival( ckr_0_0_0_0_0, false, true, true,
                                       table_metas, {ckr_0_0_99_0_1} );
    EXPECT_TRUE( matched );
    matched = check_gen_query_arrival( ckr_0_0_0_0_0, false, true, false,
                                       table_metas, {ckr_0_0_99_0_1} );
    EXPECT_TRUE( matched );
    matched = check_gen_query_arrival( ckr_0_0_0_0_0, false, false, true,
                                       table_metas, {ckr_0_0_99_0_1} );
    EXPECT_TRUE( matched );
    matched = check_gen_query_arrival( ckr_0_0_0_0_0, false, false, false,
                                       table_metas, {ckr_0_0_99_0_1} );
    EXPECT_TRUE( matched );

    // check other things
    matched = check_gen_query_arrival( ckr_0_0_50_0_0, false, true, false,
                                       table_metas, {ckr_0_0_99_0_1} );
    EXPECT_TRUE( matched );
    matched = check_gen_query_arrival( ckr_0_0_99_0_0, true, true, false,
                                       table_metas, {ckr_0_0_99_0_1} );
    EXPECT_TRUE( matched );
    matched = check_gen_query_arrival( ckr_0_0_199_0_0, false, true, false,
                                       table_metas,
                                       {ckr_0_0_99_0_1, ckr_0_100_199_0_1} );
    EXPECT_TRUE( matched );
    matched = check_gen_query_arrival( ckr_0_0_199_0_0, false, true, false,
                                       table_metas,
                                       {ckr_0_0_99_0_1, ckr_0_100_199_0_1} );
    EXPECT_TRUE( matched );
    matched = check_gen_query_arrival( ckr_0_150_250_0_0, true, true, false,
                                       table_metas,
                                       {ckr_0_100_199_0_1, ckr_0_200_299_0_1} );
    EXPECT_TRUE( matched );

    matched = check_gen_query_arrival( ckr_0_0_0_1_1, true, true, false,
                                       table_metas, {ckr_0_0_99_0_1} );
    EXPECT_TRUE( matched );
    matched = check_gen_query_arrival( ckr_0_0_0_2_2, true, true, false,
                                       table_metas, {ckr_0_0_99_2_3} );
    EXPECT_TRUE( matched );
    matched = check_gen_query_arrival( ckr_0_0_0_2_3, false, true, false,
                                       table_metas, {ckr_0_0_99_2_3} );
    EXPECT_TRUE( matched );
    matched = check_gen_query_arrival( ckr_0_0_0_2_5, false, false, false,
                                       table_metas,
                                       {ckr_0_0_99_2_3, ckr_0_0_99_4_5} );
    EXPECT_TRUE( matched );
    matched = check_gen_query_arrival( ckr_0_0_0_2_5, false, false, false,
                                       table_metas,
                                       {ckr_0_0_99_2_3, ckr_0_0_99_4_5} );
    EXPECT_TRUE( matched );
    matched = check_gen_query_arrival( ckr_0_150_250_2_5, false, false, false,
                                       table_metas,
                                       {ckr_0_100_199_2_3, ckr_0_100_199_4_5,
                                        ckr_0_200_299_2_3, ckr_0_200_299_4_5} );
    EXPECT_TRUE( matched );
    matched = check_gen_query_arrival(
        ckr_1_150_250_2_5, false, false, false, table_metas,
        {ckr_1_100_199_2_2, ckr_1_100_199_3_3, ckr_1_100_199_4_4,
         ckr_1_100_199_5_5, ckr_1_200_299_2_2, ckr_1_200_299_3_3,
         ckr_1_200_299_4_4, ckr_1_200_299_5_5} );
    EXPECT_TRUE( matched );
}

TEST_F( query_arrival_predictor_test, test_simple_query_arrival ) {
    std::vector<cell_data_type> col_types;
    auto                        t1_meta = create_table_metadata(
        "t0", 0, 10 /* num cols */, col_types, 5 /* num_records_in_chain */,
        5 /* num_records_in_snapshot_chain */, k_unassigned_master,
        100 /* default_partition_size */, 2 /* default_column_size */,
        100 /* default_tracking_partition_size */,
        2 /* default_tracking_column_size */ );
    auto t2_meta = create_table_metadata(
        "t1", 1, 10 /* num cols */, col_types, 5 /* num_records_in_chain */,
        5 /* num_records_in_snapshot_chain */, k_unassigned_master,
        100 /* default_partition_size */, 1 /* default_column_size */,
        100 /* default_tracking_partition_size */,
        1 /* default_tracking_column_size */ );
    std::vector<table_metadata> table_metas = {t1_meta, t2_meta};

    auto ckr_0_0_50_0_0 =
        create_cell_key_ranges( 0, 0, 50, 0, 0 );  // {0, 0, 99, 0, 1 }
    auto ckr_0_0_199_0_0 = create_cell_key_ranges(
        0, 0, 199, 0, 0 );  // {0, 0, 99, 0, 1 }, { 0, 100, 199, 0, 1}
    auto ckr_0_150_250_0_0 = create_cell_key_ranges(
        0, 150, 250, 0, 0 );  // { 0, 100, 199, 0, 1}, {, 0, 200, 299, 0, 1}
    auto ckr_0_150_250_2_5 = create_cell_key_ranges( 0, 150, 250, 2, 5 );
    // { 0, 100, 199, 2, 3}, {0, 0, 200, 299, 2, 3}
    // { 0, 100, 199, 4, 5}, {0, 0, 200, 299, 4, 5}

    auto pid = create_partition_column_identifier( 0, 25, 125, 0, 1 );
    auto missing_pid = create_partition_column_identifier( 0, 325, 525, 0, 1 );

    std::vector<cell_key_ranges> write_set = {ckr_0_0_199_0_0,
                                              ckr_0_150_250_2_5};
    std::vector<cell_key_ranges> read_set = {ckr_0_0_50_0_0};
    std::vector<cell_key_ranges> empty_set;

    query_arrival_predictor predictor(
        construct_query_arrival_predictor_configs(
            true /* use query arrival predictor */,
            query_arrival_predictor_type::SIMPLE_QUERY_PREDICTOR,
            0.5 /* threshold */, 30 /* time bucket */, 10 /* count bucket */,
            1.0 /* score scalar */, 0.001 /* default score */ ) );
    predictor.add_table_metadata( t1_meta );
    predictor.add_table_metadata( t2_meta );

    TimePoint min_time( std::chrono::seconds( 0 ) );
    TimePoint offset_time( std::chrono::seconds( 500 ) );

    TimePoint obs_time( std::chrono::seconds( 1000 ) );
    TimePoint period_time( std::chrono::seconds( 4 ) );

    predictor.add_query_observation( write_set, read_set, obs_time );
    predictor.add_query_schedule( empty_set, read_set, period_time );

    TimePoint expected_time( std::chrono::seconds( 1004 ) );
    TimePoint expected_offset_time( std::chrono::seconds( 504 ) );
    auto      next_access =
        predictor.get_next_access( pid, true /* read */, false /* scan */,
                                   false /* writes */, min_time, offset_time );
    EXPECT_TRUE( next_access.found_ );
    EXPECT_EQ( expected_time, next_access.predicted_time_ );
    EXPECT_EQ( expected_offset_time, next_access.offset_time_ );
    EXPECT_DOUBLE_EQ( 1, next_access.count_ );

    next_access = predictor.get_next_access(
        missing_pid, true /* read */, false /* scan */, false /* writes */,
        min_time, offset_time );
    EXPECT_FALSE( next_access.found_ );

    obs_time = TimePoint( std::chrono::seconds( 1200 ) );
    predictor.add_query_observation( write_set, read_set, obs_time );

    period_time = TimePoint( std::chrono::seconds( 1 ) );
    predictor.add_query_observation( write_set, read_set, obs_time );
    predictor.add_query_schedule( write_set, empty_set, period_time );

    expected_time = TimePoint( std::chrono::seconds( 1204 ) );
    expected_offset_time = TimePoint( std::chrono::seconds( 704 ) );
    next_access =
        predictor.get_next_access( pid, true /* read */, false /* scan */,
                                   false /* writes */, min_time, offset_time );
    EXPECT_TRUE( next_access.found_ );
    EXPECT_EQ( expected_time, next_access.predicted_time_ );
    EXPECT_EQ( expected_offset_time, next_access.offset_time_ );
    EXPECT_DOUBLE_EQ( 1, next_access.count_ );

    expected_time = TimePoint( std::chrono::seconds( 1201 ) );
    expected_offset_time = TimePoint( std::chrono::seconds( 701 ) );
    next_access =
        predictor.get_next_access( pid, true /* read */, false /* scan */,
                                   true /* writes */, min_time, offset_time );
    EXPECT_TRUE( next_access.found_ );
    EXPECT_EQ( expected_time, next_access.predicted_time_ );
    EXPECT_EQ( expected_offset_time, next_access.offset_time_ );
    EXPECT_DOUBLE_EQ( 3 /*0-99 R/W, 100-199 W*/, next_access.count_ );

    obs_time = TimePoint( std::chrono::seconds( 1400 ) );
    predictor.add_query_observation( write_set, read_set, obs_time );

    next_access = predictor.get_next_access(
        missing_pid, true /* read */, false /* scan */, false /* writes */,
        min_time, offset_time );
    EXPECT_FALSE( next_access.found_ );

    expected_time = TimePoint( std::chrono::seconds( 1404 ) );
    expected_offset_time = TimePoint( std::chrono::seconds( 904 ) );
    next_access =
        predictor.get_next_access( pid, true /* read */, false /* scan */,
                                   false /* writes */, min_time, offset_time );
    EXPECT_TRUE( next_access.found_ );
    EXPECT_EQ( expected_time, next_access.predicted_time_ );
    EXPECT_EQ( expected_offset_time, next_access.offset_time_ );
    EXPECT_DOUBLE_EQ( 1, next_access.count_ );

    expected_time = TimePoint( std::chrono::seconds( 1401 ) );
    expected_offset_time = TimePoint( std::chrono::seconds( 901 ) );
    next_access =
        predictor.get_next_access( pid, true /* read */, false /* scan */,
                                   true /* writes */, min_time, offset_time );
    EXPECT_TRUE( next_access.found_ );
    EXPECT_EQ( expected_time, next_access.predicted_time_ );
    EXPECT_EQ( expected_offset_time, next_access.offset_time_ );
    EXPECT_DOUBLE_EQ( 3 /*0-99 R/W, 100-199 W*/, next_access.count_ );

    min_time = TimePoint( std::chrono::seconds( 1200 ) );
    offset_time = TimePoint( std::chrono::seconds( 1200 ) );
    expected_time = TimePoint( std::chrono::seconds( 1401 ) );
    expected_offset_time = TimePoint( std::chrono::seconds( 201 ) );
    next_access =
        predictor.get_next_access( pid, true /* read */, false /* scan */,
                                   true /* writes */, min_time, offset_time );
    EXPECT_TRUE( next_access.found_ );
    EXPECT_EQ( expected_time, next_access.predicted_time_ );
    EXPECT_EQ( expected_offset_time, next_access.offset_time_ );
    EXPECT_DOUBLE_EQ( 3 /*0-99 R/W, 100-199 W*/, next_access.count_  );

    min_time = TimePoint( std::chrono::seconds( 1402 ) );
    offset_time = TimePoint( std::chrono::seconds( 1500 ) );
    expected_time = TimePoint( std::chrono::seconds( 1404 ) );
    expected_offset_time = TimePoint( std::chrono::seconds( 0 ) );
    next_access =
        predictor.get_next_access( pid, true /* read */, false /* scan */,
                                   true /* writes */, min_time, offset_time );
    EXPECT_TRUE( next_access.found_ );
    EXPECT_EQ( expected_time, next_access.predicted_time_ );
    EXPECT_EQ( expected_offset_time, next_access.offset_time_ );
    EXPECT_DOUBLE_EQ( 3 /*0-99 R/W, 100-199 W*/, next_access.count_ );

    min_time = TimePoint( std::chrono::seconds( 1500 ) );
    next_access =
        predictor.get_next_access( pid, true /* read */, false /* scan */,
                                   true /* writes */, min_time, offset_time );
    EXPECT_FALSE( next_access.found_ );
}

