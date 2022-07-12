#define GTEST_HAS_TR1_TUPLE 0

#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/data-site/db/versioned_row_record.h"

class versioned_row_record_test : public ::testing::Test {};

TEST_F( versioned_row_record_test, sanity_row_record_test ) {
    versioned_row_record vr;

    int64_t  key = 123;
    uint64_t partition_0 = 0;
    int32_t  num_row_records_in_chain = 2;
    vr.init( key, partition_0, num_row_records_in_chain );

    EXPECT_EQ( key, vr.get_key() );

    row_record*       read_row_record;
    row_record*       write_row_record;
    transaction_state txn_state;

    snapshot_vector most_recent = {};
    snapshot_vector gc_lwm = {};
    for( uint64_t partition_id = 0; partition_id < 3; partition_id++ ) {
        most_recent[partition_id] = 1000;
        gc_lwm[partition_id] = 0;
    }

    read_row_record = vr.read_record( key * 2, most_recent );
    EXPECT_EQ( nullptr, read_row_record );

    write_row_record = vr.write_record( key * 2, &txn_state,
                                        num_row_records_in_chain, gc_lwm );
    EXPECT_EQ( nullptr, write_row_record );
}

TEST_F( versioned_row_record_test, row_record_test ) {
    versioned_row_record vr;

    int64_t  key = 123;
    uint64_t partition_0 = 0;
    uint64_t partition_1 = 1;
    int32_t  num_row_records_in_chain = 2;
    vr.init( key, k_unassigned_partition, num_row_records_in_chain );

    EXPECT_EQ( key, vr.get_key() );

    row_record*           read_row_record;
    row_record*           write_row_record;
    transaction_state txn_state;

    snapshot_vector most_recent;
    snapshot_vector gc_lwm;
    for( uint64_t partition_id = 0; partition_id < 3; partition_id++ ) {
        most_recent[partition_id] = 1000;
        gc_lwm[partition_id] = 0;
    }

    // no keys
    read_row_record = vr.read_record( key, most_recent );
    EXPECT_EQ( nullptr, read_row_record );

    // insert
    write_row_record = vr.insert_record( key, &txn_state, partition_0,
                                         num_row_records_in_chain );
    EXPECT_NE( nullptr, write_row_record );

    EXPECT_EQ( 0, write_row_record->get_num_columns() );
    EXPECT_EQ( nullptr, write_row_record->get_row_data() );
    write_row_record->init_num_columns( 1, false );
    packed_cell_data* packed_cells = write_row_record->get_row_data();
    EXPECT_NE( nullptr, packed_cells );

    packed_cell_data& pc = packed_cells[0];
    pc.set_int64_data( 2 );
    // versions should be (-,2) --> null
    write_row_record->set_version( 2 );

    read_row_record = vr.read_record( key, most_recent );
    EXPECT_NE( nullptr, read_row_record );
    EXPECT_EQ( 1, read_row_record->get_num_columns() );
    EXPECT_EQ( 2, read_row_record->get_row_data()[0].get_int64_data() );

    // repartition
    std::vector<row_record*> repartition_recs;
    bool is_okay = vr.repartition( repartition_recs, key, partition_1,
                                   &txn_state, num_row_records_in_chain, gc_lwm );
    EXPECT_TRUE( is_okay );
    EXPECT_EQ( 1, repartition_recs.size() );
    row_record* repartition_rec = repartition_recs.at( 0 );
    EXPECT_NE( nullptr, repartition_rec );
    repartition_rec->set_version( 3 );
    // versions should be ([repartition],2) --> null

    // read
    read_row_record = vr.read_record( key, most_recent );
    EXPECT_NE( nullptr, read_row_record );
    EXPECT_EQ( 1, read_row_record->get_num_columns() );
    EXPECT_EQ( 2, read_row_record->get_row_data()[0].get_int64_data() );

    // writes should trigger new chains which we can later gc
    // versions should be (,4) --> (repartition,2) --> null
    write_row_record =
        vr.write_record( key, &txn_state, num_row_records_in_chain, gc_lwm );
    EXPECT_NE( nullptr, write_row_record );
    EXPECT_EQ( 0, write_row_record->get_num_columns() );
    EXPECT_EQ( nullptr, write_row_record->get_row_data() );
    write_row_record->init_num_columns( 1, false );
    write_row_record->get_row_data()[0].set_int64_data( 4 );
    write_row_record->set_version( 4 );

    // versions should be (5,4) --> (repartition,2) --> null
    write_row_record =
        vr.write_record( key, &txn_state, num_row_records_in_chain, gc_lwm );
    EXPECT_NE( nullptr, write_row_record );
    write_row_record->init_num_columns( 1, false );
    write_row_record->get_row_data()[0].set_int64_data( 5 );

    // not committed yet
    read_row_record = vr.read_latest_record( key );
    EXPECT_NE( nullptr, read_row_record );
    EXPECT_EQ( 1, read_row_record->get_num_columns() );
    EXPECT_EQ( 4, read_row_record->get_row_data()[0].get_int64_data() );
    // now commit
    write_row_record->set_version( 5 );

    // versions should be (-,6) --> (5,4) --> (repartition,2) --> null
    write_row_record =
        vr.write_record( key, &txn_state, num_row_records_in_chain, gc_lwm );
    EXPECT_NE( nullptr, write_row_record );
    write_row_record->init_num_columns( 1, false );
    write_row_record->get_row_data()[0].set_int64_data( 6 );
    write_row_record->set_version( 6 );

    snapshot_vector read_snapshot;
    read_snapshot[0] = 2;
    read_snapshot[1] = 6;

    read_row_record = vr.read_record( key, read_snapshot ); // { 2, 6 }
    EXPECT_NE( nullptr, read_row_record );
    EXPECT_EQ( 1, read_row_record->get_num_columns() );
    EXPECT_EQ( 6, read_row_record->get_row_data()[0].get_int64_data() );

    read_snapshot[0] = 2;
    read_snapshot[1] = 5;
    read_row_record = vr.read_record( key, read_snapshot );  // { 2, 5 }
    EXPECT_NE( nullptr, read_row_record );
    EXPECT_EQ( 1, read_row_record->get_num_columns() );
    EXPECT_EQ( 5, read_row_record->get_row_data()[0].get_int64_data() );


    //GC at version 4
    //Can only safely GC last chain, which has no tail ptrs to GC.
    snapshot_vector gc_snapshot = {};
    gc_snapshot[0] = 4;
    gc_snapshot[1] = 5;

    vr.gc_record( gc_snapshot );  // { 4, 5 }
    // versions should be (-,6) --> (5,4) --> (repartition(3), 2) --> null

    // 5 should still be there
    read_snapshot[0] = 2;
    read_snapshot[1] = 5;
    read_row_record = vr.read_record( key, read_snapshot );  // { 2, 5 }
    EXPECT_NE( nullptr, read_row_record );
    EXPECT_EQ( 1, read_row_record->get_num_columns() );
    EXPECT_EQ( 5, read_row_record->get_row_data()[0].get_int64_data() );


    read_snapshot[0] = 2;
    read_snapshot[1] = 0;
    read_row_record = vr.read_record( key, read_snapshot );  //  { 2, 0 }
    LOG( INFO ) << "read row_record:" << read_row_record;
    EXPECT_NE( nullptr, read_row_record );
    EXPECT_EQ( 1, read_row_record->get_num_columns() );
    EXPECT_EQ( 2, read_row_record->get_row_data()[0].get_int64_data() );

    // write should trigger gc
    gc_lwm[0] = 4;
    gc_lwm[1] = 6;
    // versions should be (7,6) --> (5,4) --> (repartition,2) --> null
    write_row_record = vr.write_record( key, &txn_state, num_row_records_in_chain,
                                    gc_lwm );  // { 4, 6}
    EXPECT_NE( nullptr, write_row_record );
    write_row_record->init_num_columns( 1, false );
    write_row_record->get_row_data()[0].set_int64_data( 7 );
    write_row_record->set_version( 7 );

    // versions should be (7,6) --> (5, 4) --> null
    // 5 should be there
    read_snapshot[0] = 2;
    read_snapshot[1] = 4;
    read_row_record = vr.read_record( key, read_snapshot );  // { 2, 4 }
    EXPECT_NE( nullptr, read_row_record );
    EXPECT_EQ( 1, read_row_record->get_num_columns() );
    EXPECT_EQ( 4, read_row_record->get_row_data()[0].get_int64_data() );

#if 0
    //2 should be gone
    read_snapshot[0] = 2;
    read_snapshot[1] = 0;
    read_row_record = vr.read_record( key, read_snapshot );  //  2, 0 }
    EXPECT_EQ( nullptr, read_row_record );
#endif
}
