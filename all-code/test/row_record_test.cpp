#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/common/string_utils.h"
#include "../src/common/thread_utils.h"
#include "../src/data-site/db/row_record.h"

#include "../src/benchmark/tpcc/record-types/tpcc_record_types.h"

class row_record_test : public ::testing::Test {};

TEST_F( row_record_test, row_record_test ) {
    row_record            r;
    transaction_state txn_state;

    EXPECT_EQ( K_NOT_COMMITTED, r.get_version() );

    r.set_transaction_state( &txn_state );
    EXPECT_EQ( K_NOT_COMMITTED, r.get_version() );

    EXPECT_FALSE( r.is_present() );
    EXPECT_EQ( 0, r.get_num_columns() );
    EXPECT_EQ( nullptr, r.get_row_data() );

    r.init_num_columns( 1, false );

    packed_cell_data* packed_cells = r.get_row_data();
    EXPECT_NE( nullptr, packed_cells );

    packed_cell_data& pc = packed_cells[0];

    EXPECT_FALSE( pc.is_present() );
    pc.set_int64_data( 100 );
    EXPECT_EQ( 100, pc.get_int64_data() );

    txn_state.set_committing();

    EXPECT_EQ( K_IN_COMMIT, r.get_version() );

    txn_state.set_committed( 10 );
    EXPECT_EQ( 10, r.get_version() );
    EXPECT_EQ( 10, r.spin_for_committed_version() );

    EXPECT_EQ( 100, r.get_row_data()[0].get_int64_data() );

    packed_cell_data pcd;
    bool             found = r.get_cell_data( 0, pcd );
    EXPECT_TRUE( found );
    EXPECT_EQ( 100, pcd.get_int64_data());

    r.set_version( 10 );
    EXPECT_EQ( 10, r.get_version() );

    EXPECT_EQ( 10, txn_state.get_version() );

    txn_state.set_not_committed();
    EXPECT_EQ( 10, r.get_version() );
    EXPECT_EQ( K_NOT_COMMITTED, txn_state.get_version() );
}

TEST_F( row_record_test, spin_for_version_test ) {
    row_record            r;
    transaction_state txn_state;

    EXPECT_EQ( K_NOT_COMMITTED, r.get_version() );

    // one thread that is going to spin until it gets a version
    std::thread get_thread(
        [&r]() { EXPECT_EQ( 1, r.spin_for_committed_version() ); } );

    // override the pointer
    r.set_transaction_state( &txn_state );

    int32_t sleep_time = 50;

    // say we are going to commit
    txn_state.set_committing();
    std::this_thread::sleep_for( std::chrono::milliseconds( sleep_time ) );

    txn_state.set_committed( 1 );
    join_thread( get_thread );

    EXPECT_EQ( 1, r.get_version() );

    txn_state.set_not_committed();
    EXPECT_EQ( K_NOT_COMMITTED, r.get_version() );

    std::thread spin_thread(
        [&r]() { EXPECT_EQ( 2, r.spin_for_committed_version() ); } );

    // say we are going to commit
    txn_state.set_committing();
    std::this_thread::sleep_for( std::chrono::milliseconds( sleep_time ) );

    r.set_version( 2 );
    join_thread( spin_thread );

    EXPECT_EQ( 2, r.get_version() );
}
