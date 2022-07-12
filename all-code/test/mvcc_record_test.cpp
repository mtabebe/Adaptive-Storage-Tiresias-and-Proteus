#define GTEST_HAS_TR1_TUPLE 0

#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/common/thread_utils.h"
#include "../src/data-site/db/mvcc_chain.h"

class mvcc_record_test : public ::testing::Test {};

void write_into_record( row_record* r, int64_t val ) {
    EXPECT_NE( nullptr, r );

    EXPECT_FALSE( r->is_present() );
    EXPECT_EQ( 0, r->get_num_columns() );
    EXPECT_EQ( nullptr, r->get_row_data() );

    r->init_num_columns( 1, false );

    packed_cell_data* packed_cells = r->get_row_data();
    EXPECT_NE( nullptr, packed_cells );

    packed_cell_data& pc = packed_cells[0];

    EXPECT_FALSE( pc.is_present() );
    pc.set_int64_data( val );
    EXPECT_EQ( val, pc.get_int64_data() );
}
void check_record( row_record* r, int64_t expected_version ) {
    EXPECT_NE( nullptr, r );
    EXPECT_EQ( 1, r->get_num_columns() );
    packed_cell_data* packed_cells = r->get_row_data();
    EXPECT_NE( nullptr, packed_cells );

    packed_cell_data& pc = packed_cells[0];

    EXPECT_TRUE( pc.is_present() );
    EXPECT_EQ( expected_version, pc.get_int64_data() );

}

void check_found( mvcc_chain* chain, std::vector<bool> expect_ptrs,
                  std::vector<snapshot_vector> snapshots,
                  std::vector<int64_t>         expected_versions ) {
    EXPECT_LE( expect_ptrs.size(), snapshots.size() );
    EXPECT_LE( expect_ptrs.size(), expected_versions.size() );
    int32_t comp_count = expect_ptrs.size();
    for( int32_t pos = 0; pos < comp_count; pos++ ) {
        bool                   expect_ptr = expect_ptrs.at( pos );
        const snapshot_vector& snapshot = snapshots.at( pos );
        int64_t                expected_version = expected_versions.at( pos );
        row_record*            r = chain->find_row_record( snapshot );
        if( expect_ptr ) {
            check_record( r, expected_version );

        } else {
            EXPECT_EQ( nullptr, r );
        }
    }
}

TEST_F( mvcc_record_test, mvcc_chain_test ) {
    mvcc_chain*       chain = new mvcc_chain();
    transaction_state txn_state;

    int32_t  num_records = 2;
    uint32_t partition_1 = 1;
    uint32_t partition_0 = 0;

    // Chain is partition_1
    chain->init( num_records, partition_1, nullptr );

    std::vector<snapshot_vector>      snapshots;
    std::vector<std::vector<uint64_t>> versions = {
        {0, 0, 0}, {0, 1, 0}, {0, 2, 0}, {0, 3, 0},
        {0, 4, 0}, {0, 5, 0}, {0, 5, 0}, {7, 5, 0}};
    for ( const auto& version : versions) {
        snapshot_vector snapshot;
        for( uint64_t pos = 0; pos < version.size(); pos++ ) {
            snapshot[pos] = version[pos];
        }
        snapshots.push_back( snapshot );
    }
    std::vector<int64_t> expected_versions = {0, 0, 2, 2, 2, 5, 5, 7};

    EXPECT_FALSE( chain->is_chain_full() );
    EXPECT_TRUE( chain->is_chain_empty() );
    // no records are assigned and there is no next chain
    check_found( chain, {false, false, false}, snapshots, expected_versions );

    EXPECT_EQ( partition_1, chain->get_partition_id() );

    EXPECT_FALSE( chain->is_chain_full() );
    row_record* write_record = chain->create_new_row_record( &txn_state );
    EXPECT_NE( nullptr, write_record );
    // chain: empty, not committed

    // still nothing to see statewise
    write_into_record( write_record, 2 );
    // Write 2 at version 2
    write_record->set_version( 2 );
    // chain: empty, not committed

    // now we should find something for 2's only
    check_found( chain, {false, false, true, true}, snapshots,
                 expected_versions );

    EXPECT_FALSE( chain->is_chain_full() );
    write_record = chain->create_new_row_record( &txn_state );
    EXPECT_NE( nullptr, write_record );
    // chain: not committed, 2
    write_into_record(write_record, 5);
    write_record->set_version( 5 );
    // chain: 5, 2

    // we should find something for everythign in the range
    check_found( chain, {false, false, true, true, true, true, true}, snapshots,
                 expected_versions );

    EXPECT_TRUE( chain->is_chain_full() );
    write_record = chain->create_new_row_record( &txn_state );
    EXPECT_EQ( nullptr, write_record );

    mvcc_chain* chain2 = new mvcc_chain();
    chain2->init( num_records, partition_0, chain );

    EXPECT_FALSE( chain2->is_chain_full() );
    EXPECT_EQ( partition_1, chain->get_partition_id() );
    EXPECT_EQ( partition_0, chain2->get_partition_id() );

    check_found( chain2, {false, false, true, true, true, true, true}, snapshots,
                 expected_versions );

#if 0
    // try a GC here: nothing should ever be GC'd
    mvcc_chain* gc_chain = nullptr;
    for( auto& snapshot : snapshots ) {
        gc_chain = chain2->garbage_collect_chain( snapshot );
        EXPECT_EQ( nullptr, gc_chain );
    }

    check_found( chain2, {false, false, true, true, true, true, true},
                 snapshots, expected_versions );

    // chain2: empty, empty, --> chain: 5, 2
    EXPECT_FALSE( chain2->is_chain_full() );
    write_record = chain2->create_new_row_record( &txn_state );
    EXPECT_NE( nullptr, write_record );

    // try a GC here: nothing should ever be GC'd
    for( auto& snapshot : snapshots ) {
        gc_chain = chain2->garbage_collect_chain( snapshot );
        EXPECT_EQ( nullptr, gc_chain );
    }

    check_found( chain2, {false, false, true, true, true, true, true},
                 snapshots, expected_versions );

    txn_state.set_committing();

    // try a GC here: nothing should ever be GC'd
    for( auto& snapshot : snapshots ) {
        gc_chain = chain2->garbage_collect_chain( snapshot );
        EXPECT_EQ( nullptr, gc_chain );
    }

    write_into_record( write_record, 7);
    write_record->set_version( 7 );
    // chain2: emtpy, 7, --> chain: 5, 2
    //
    check_found( chain2, {false, false, true, true, true, true, true, true},
                 snapshots, expected_versions );

    txn_state.set_not_committed();

    row_record* latest_record = chain2->get_latest_row_record();
    EXPECT_NE( nullptr, latest_record );
    EXPECT_EQ( 7, latest_record->get_record().get_as_int() );

    mvcc_chain* next_chain = chain2->get_next_link_in_chain();
    EXPECT_EQ( next_chain, chain );

    mvcc_chain* chain3 = new mvcc_chain();
    chain3->init( num_records, partition_1, chain2 );

    EXPECT_FALSE( chain3->is_chain_full() );

    check_found( chain3, {false, false, true, true, true, true, true},
                 snapshots, expected_versions );

    // chain3: empty, 8 --> chain2: empty, 7, --> chain: 5, 2
    EXPECT_FALSE( chain3->is_chain_full() );
    write_record = chain3->create_new_row_record( &txn_state );
    EXPECT_NE( nullptr, write_record );
    write_into_record( write_record, 8);
    write_record->set_version( 8 );
    //
    // 7, 5, 0 GC shouldn't be able to GC anything
    gc_chain = chain3->garbage_collect_chain( snapshots.at( 7 ) );
    EXPECT_EQ( nullptr, gc_chain );

    client_version_vector low_water_mark = {8, 6, 0};
    gc_chain = chain3->garbage_collect_chain( low_water_mark );

    // Can GC chain 2's children
    EXPECT_EQ( chain, gc_chain );

    delete chain;

    gc_chain = chain3->garbage_collect_chain( low_water_mark );
    EXPECT_EQ( nullptr, gc_chain );

    // chain is
    // chain3: empty, in commit, --> chain2: empty, 7, --> null

    write_into_record( write_record, 8);
    write_record->set_version( 8 );
    txn_state.set_not_committed();
    //
    check_found( chain3,
                 {false, false, false, false, false, false, false, true},
                 snapshots, expected_versions );

    // should recursively delete chain2
    delete chain3;
#endif
}

TEST_F( mvcc_record_test, concurrent_mvcc_chain_test ) {
    // TODO test the waiting for version spin
    mvcc_chain*       chain = new mvcc_chain();
    transaction_state txn_state;

    int32_t  num_records = 10;
    uint32_t partition_0 = 0;
    chain->init( num_records, partition_0, nullptr );
    std::vector<std::vector<uint64_t>> versions = {{0}, {1}, {2}, {3},
                                                   {4}, {5}, {6}};
    std::vector<snapshot_vector> snapshots;
    for( const auto& version : versions ) {
        snapshot_vector snapshot;
        for( uint64_t pos = 0; pos < version.size(); pos++ ) {
            snapshot[pos] = version[pos];
        }
        snapshots.push_back( snapshot );
    }

    std::vector<int64_t> expected = {0, 0, 2, 2, 2, 5, 5};

    EXPECT_FALSE( chain->is_chain_full() );
    row_record* write_record = chain->create_new_row_record( &txn_state );
    EXPECT_NE( nullptr, write_record );

    // this guy should not find the record
    snapshot_vector snapshot;
    snapshot[0] = 2;
    std::thread empty_thread( [chain, snapshot]() {
        row_record* r = chain->find_row_record( snapshot );
        EXPECT_EQ( nullptr, r );
    } );
    join_thread( empty_thread );

    write_into_record( write_record, 2);
    txn_state.set_committing();

    // this guy should find the record (after spinning) because it is in commit.
    std::thread spin_thread( [chain, snapshot]() {
        row_record* r = chain->find_row_record( snapshot );
        EXPECT_NE( nullptr, r );
        check_record( r, 2 );
    } );

    int32_t sleep_time = 50;
    std::this_thread::sleep_for( std::chrono::milliseconds( sleep_time ) );
    txn_state.set_committed( 2 );
    join_thread( spin_thread );

    write_record->set_version( 2 );
    txn_state.set_not_committed();

    // shouldn't see v5
    check_found( chain, {false, false, true, true, true}, snapshots, expected );
    EXPECT_FALSE( chain->is_chain_full() );
    write_record = chain->create_new_row_record( &txn_state );
    EXPECT_NE( nullptr, write_record );
    write_into_record( write_record, 5);
    txn_state.set_committing();

    // now should see v5 even if this means you have to spin
    std::thread spin_5_thread( [chain, &snapshots, &expected]() {
        check_found( chain, {false, false, true, true, true, true}, snapshots,
                     expected );
    } );

    std::this_thread::sleep_for( std::chrono::milliseconds( sleep_time ) );
    txn_state.set_committed( 5 );
    join_thread( spin_5_thread );

    // now all these should be seen without any problem
    check_found( chain, {false, false, true, true, true, true}, snapshots,
                 expected );
    delete chain;
}
