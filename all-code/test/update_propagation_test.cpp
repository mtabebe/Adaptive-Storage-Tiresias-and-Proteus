#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

#include "../src/common/thread_utils.h"
#include "../src/data-site/db/partition.h"
#include "../src/data-site/db/partition_metadata.h"
#include "../src/data-site/db/partition_util.h"
#include "../src/data-site/db/tables.h"
#include "../src/data-site/db/transaction_partition_holder.h"
#include "../src/data-site/update-propagation/no_op_update_destination.h"
#include "../src/data-site/update-propagation/update_enqueuer.h"
#include "../src/data-site/update-propagation/update_queue.h"
#include "../src/data-site/update-propagation/vector_update_destination.h"
#include "../src/data-site/update-propagation/vector_update_source.h"
#include "../src/data-site/update-propagation/write_buffer_serialization.h"

class update_propagation_test : public ::testing::Test {};

serialized_update create_serialized_update(
    const partition_column_identifier& pid, uint64_t version,
    const cell_identifier& cid, const char* buffer, uint32_t len,
    const partition_type::type& new_type ) {

    uint64_t        pid_hash = partition_column_identifier_key_hasher{}( pid );
    snapshot_vector commit_vv = {};
    set_snapshot_version( commit_vv, pid, version );

    write_buffer* insert_buf = new write_buffer();

    row_record* r1 = new row_record();
    packed_cell_data* pcd = new packed_cell_data();

    switch( new_type ) {
        case partition_type::type::ROW: {
            versioned_row_record_identifier vri_1;

            EXPECT_FALSE( r1->is_present() );
            r1->init_num_columns( 1, false );
            EXPECT_TRUE( r1->is_present() );

            packed_cell_data* packed_cells = r1->get_row_data();
            EXPECT_NE( nullptr, packed_cells );

            packed_cell_data& pc = packed_cells[0];

            EXPECT_FALSE( pc.is_present() );
            pc.set_string_data( std::string( buffer, len ) );
            EXPECT_TRUE( pc.is_present() );
            EXPECT_TRUE( r1->is_present() );

            vri_1.add_db_op( cid, r1, K_WRITE_OP );
            insert_buf->add_to_record_buffer( std::move( vri_1 ) );

            break;
        }
        // fall through explicitly
        case partition_type::type::SORTED_COLUMN:
        case partition_type::type::SORTED_MULTI_COLUMN:
        case partition_type::type::MULTI_COLUMN:
        case partition_type::type::COLUMN: {
            versioned_cell_data_identifier vri_1;

            EXPECT_FALSE( pcd->is_present() );
            pcd->set_string_data( std::string( buffer, len ) );
            EXPECT_TRUE( pcd->is_present() );

            vri_1.add_db_op( cid, pcd, K_WRITE_OP );
            insert_buf->add_to_cell_buffer( std::move( vri_1 ) );

            break;
        }
    }

    insert_buf->store_propagation_information( commit_vv, pid, pid_hash,
                                               version );

    serialized_update update = serialize_write_buffer( insert_buf );
    EXPECT_GE( update.length_, get_serialized_update_min_length() );

    delete insert_buf;
    delete r1;
    delete pcd;

    return std::move( update );
}

snapshot_vector check_values_same(
    std::shared_ptr<partition> part, const snapshot_vector& begin_timestamp,
    const partition_column_identifier& pid,
    const std::vector<std::string>&    expected_vals ) {
    transaction_partition_holder txn_holder;
    txn_holder.add_read_partition( pid, part );
    txn_holder.begin_transaction( begin_timestamp );

    cell_identifier cid;
    cid.table_id_ = pid.table_id;
    cid.col_id_ = pid.column_start;
    cid.key_ = 0;

    for( uint64_t id = pid.partition_start; id <= (uint64_t) pid.partition_end;
         id++ ) {
        cid.key_ = id;
        std::tuple<bool, std::string> got_data =
            txn_holder.get_string_data( cid );
        EXPECT_TRUE( std::get<0>( got_data ) );
        EXPECT_EQ( expected_vals.at( id ), std::get<1>( got_data ) );
    }

    snapshot_vector committed_timestamp = txn_holder.commit_transaction();
    return committed_timestamp;
}

void add_to_insert_buf( write_buffer*                   insert_buf,
                        std::vector<row_record*>&       row_recs,
                        std::vector<packed_cell_data*>& packed_cells,
                        const cell_identifier& cid, uint64_t id,
                        const partition_type::type& p_type ) {

    switch( p_type ) {
        case partition_type::type::ROW: {
            row_record*                     r1 = new row_record();
            versioned_row_record_identifier vri_1;

            EXPECT_FALSE( r1->is_present() );
            r1->init_num_columns( 1, false );
            EXPECT_TRUE( r1->is_present() );

            packed_cell_data* packed_cells = r1->get_row_data();
            EXPECT_NE( nullptr, packed_cells );

            packed_cell_data& pc = packed_cells[0];

            EXPECT_FALSE( pc.is_present() );
            pc.set_uint64_data( id );
            EXPECT_TRUE( pc.is_present() );
            EXPECT_TRUE( r1->is_present() );

            vri_1.add_db_op( cid, r1, K_INSERT_OP );
            insert_buf->add_to_record_buffer( std::move( vri_1 ) );

            row_recs.push_back( r1 );

            break;
        }
        // fall through explicitly
        case partition_type::type::MULTI_COLUMN:
        case partition_type::type::SORTED_MULTI_COLUMN:
        case partition_type::type::SORTED_COLUMN:
        case partition_type::type::COLUMN: {
            packed_cell_data*              pcd = new packed_cell_data();
            versioned_cell_data_identifier vri_1;

            EXPECT_FALSE( pcd->is_present() );
            pcd->set_uint64_data( id );
            EXPECT_TRUE( pcd->is_present() );

            vri_1.add_db_op( cid, pcd, K_INSERT_OP );
            insert_buf->add_to_cell_buffer( std::move( vri_1 ) );

            packed_cells.push_back( pcd );

            break;
        }
    }
}
void clear_vectors( std::vector<row_record*>&       row_recs,
                    std::vector<packed_cell_data*>& packed_cells ) {
    for( uint32_t pos = 0 ; pos < row_recs.size(); pos++) {
        delete row_recs.at( pos );
    }
    row_recs.clear();
    for( uint32_t pos = 0 ; pos < packed_cells.size(); pos++) {
        delete packed_cells.at( pos );
    }
    packed_cells.clear();
}


void update_queue_test( const partition_type::type p_type ) {
    uint32_t                    col_start = 0;
    uint32_t                    col_end = 0;
    uint32_t                    order_table_id = 0;
    partition_column_identifier pid = create_partition_column_identifier(
        order_table_id, 0, 10, col_start, col_end );

    const char* r1_val = "foo";
    const char* r2_val = "abcdefgh";
    const char* r3_val = "horizondb";
    const char* r4_val = "drp";
    const char* r5_val = "drp++";

    cell_identifier cid;
    cid.table_id_ = order_table_id;
    cid.col_id_ = col_start;

    cid.key_ = 1;
    serialized_update v1_update = std::move(
        create_serialized_update( pid, 1, cid, r1_val, 3, p_type ) );
    stashed_update stashed_1 = std::move( create_stashed_update( v1_update ) );
    EXPECT_EQ( 1, stashed_1.commit_version_ );
    cid.key_ = 2;
    serialized_update v2_update =
        std::move( create_serialized_update( pid, 2, cid, r2_val, 8, p_type ) );
    stashed_update stashed_2 = std::move( create_stashed_update( v2_update ) );
    EXPECT_EQ( 2, stashed_2.commit_version_ );
    cid.key_ = 3;
    serialized_update v3_update =
        std::move( create_serialized_update( pid, 3, cid, r3_val, 9, p_type ) );
    stashed_update stashed_3 = std::move( create_stashed_update( v3_update ) );
    EXPECT_EQ( 3, stashed_3.commit_version_ );
    cid.key_ = 4;
    serialized_update v4_update =
        std::move( create_serialized_update( pid, 4, cid, r4_val, 3, p_type ) );
    stashed_update stashed_4 = std::move( create_stashed_update( v4_update ) );
    EXPECT_EQ( 4, stashed_4.commit_version_ );
    cid.key_ = 5;
    serialized_update v5_update =
        std::move( create_serialized_update( pid, 5, cid, r5_val, 5, p_type ) );
    stashed_update stashed_5 = std::move( create_stashed_update( v5_update ) );
    EXPECT_EQ( 5, stashed_5.commit_version_ );

    update_queue up_q;

    stashed_update read_update;
    read_update = up_q.get_next_update( false );
    EXPECT_EQ( K_NOT_COMMITTED, read_update.commit_version_ );

    uint32_t num_updates = up_q.add_update( std::move( stashed_1 ) );
    EXPECT_EQ( 1, num_updates );
    read_update = up_q.get_next_update( false );
    EXPECT_EQ( 1, read_update.commit_version_ );
    destroy_stashed_update( read_update );

    num_updates = up_q.add_update( std::move( stashed_2 ) );
    EXPECT_EQ( 1, num_updates );
    read_update = up_q.get_next_update( true );
    EXPECT_EQ( 2, read_update.commit_version_ );
    destroy_stashed_update( read_update );

    std::thread t( [&up_q, &read_update]() {
        read_update = up_q.get_next_update( true );
        EXPECT_EQ( 3, read_update.commit_version_ );
        destroy_stashed_update( read_update );

        read_update = up_q.get_next_update( false );
        EXPECT_EQ( 4, read_update.commit_version_ );
        destroy_stashed_update( read_update );
    } );

    std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );
    num_updates = up_q.add_update( std::move( stashed_4 ) );
    EXPECT_EQ( 1, num_updates );
    num_updates = up_q.add_update( std::move( stashed_3 ) );
    EXPECT_EQ( 2, num_updates );
    join_thread( t );

    num_updates = up_q.add_update( std::move( stashed_5 ) );
    EXPECT_EQ( 1, num_updates );
    read_update = up_q.get_next_update( true );
    EXPECT_EQ( 5, read_update.commit_version_ );
    destroy_stashed_update( read_update );

    read_update = up_q.get_next_update( true, 5 );
    EXPECT_EQ( K_NOT_COMMITTED, read_update.commit_version_ );
    destroy_stashed_update( read_update );
}

TEST_F( update_propagation_test, update_queue_test_row ) {
    update_queue_test( partition_type::type::ROW );
}
TEST_F( update_propagation_test, update_queue_test_col ) {
    update_queue_test( partition_type::type::COLUMN );
}
TEST_F( update_propagation_test, update_queue_test_sorted_col ) {
    update_queue_test( partition_type::type::SORTED_COLUMN );
}
TEST_F( update_propagation_test, update_queue_test_multi_col ) {
    update_queue_test( partition_type::type::MULTI_COLUMN );
}
TEST_F( update_propagation_test, update_queue_test_sorted_multi_col ) {
    update_queue_test( partition_type::type::SORTED_MULTI_COLUMN );
}

void partition_apply_test( const partition_type::type p_type ) {
    uint32_t           order_table_id = 0;
    uint32_t           num_records_in_chain = 5;
    uint32_t           num_records_in_snapshot_chain = 5;
    uint64_t           p_start = 0;
    uint64_t           p_end = 10;
    uint32_t           col_start = 0;
    uint32_t           col_end = 0;
    partition_metadata p_metadata = create_partition_metadata(
        order_table_id, p_start, p_end, col_start, col_end,
        num_records_in_chain, num_records_in_snapshot_chain, 0 );

    partition_column_identifier pid = p_metadata.partition_id_;

    std::vector<std::string> default_vals = {"0", "1", "2", "3", "4", "5",
                                             "6", "7", "8", "9", "10"};

    std::vector<std::string> final_vals = {"0",    "1",    "two", "three",
                                           "four", "five", "six", "seven",
                                           "8",    "9",    "10"};

    cell_identifier cid;
    cid.table_id_ = pid.table_id;
    cid.col_id_ = pid.column_start;
    cid.key_ = 0;

    cid.key_ = 2;
    serialized_update v2_update = std::move( create_serialized_update(
        pid, 2, cid, final_vals[2].c_str(), final_vals[2].size(), p_type ) );
    stashed_update stashed_2 = std::move( create_stashed_update( v2_update ) );
    EXPECT_EQ( 2, stashed_2.commit_version_ );
    cid.key_ = 3;
    serialized_update v3_update = std::move( create_serialized_update(
        pid, 3, cid, final_vals[3].c_str(), final_vals[3].size(), p_type ) );
    stashed_update stashed_3 = std::move( create_stashed_update( v3_update ) );
    EXPECT_EQ( 3, stashed_3.commit_version_ );
    cid.key_ = 4;
    serialized_update v4_update = std::move( create_serialized_update(
        pid, 4, cid, final_vals[4].c_str(), final_vals[4].size(), p_type ) );
    stashed_update stashed_4 = std::move( create_stashed_update( v4_update ) );
    EXPECT_EQ( 4, stashed_4.commit_version_ );
    cid.key_ = 5;
    serialized_update v5_update = std::move( create_serialized_update(
        pid, 5, cid, final_vals[5].c_str(), final_vals[5].size(), p_type ) );
    stashed_update stashed_5 = std::move( create_stashed_update( v5_update ) );
    EXPECT_EQ( 5, stashed_5.commit_version_ );
    cid.key_ = 6;
    serialized_update v6_update = std::move( create_serialized_update(
        pid, 6, cid, final_vals[6].c_str(), final_vals[6].size(), p_type ) );
    stashed_update stashed_6 = std::move( create_stashed_update( v6_update ) );
    EXPECT_EQ( 6, stashed_6.commit_version_ );

    std::shared_ptr<partition> part = create_partition( p_type );
    (void) part;

    auto part_version_holder = std::make_shared<partition_column_version_holder>(
        p_metadata.partition_id_, 0, 0 );
    std::vector<cell_data_type> col_types = {cell_data_type::STRING};
    part->init( p_metadata, std::make_shared<no_op_update_destination>( 0 ),
                part_version_holder, col_types, (void*) nullptr, 0 );

    part->init_records();

    snapshot_vector c_snap = {};
    set_snapshot_version( c_snap, pid, 0 );

    transaction_partition_holder txn_holder;
    txn_holder.add_write_partition( pid, part );
    txn_holder.begin_transaction( c_snap );

    for( uint64_t id = p_start; id <= p_end; id++) {
        cid.key_ = id;
        bool insert_ok = txn_holder.insert_string_data( cid, default_vals[id] );
        EXPECT_TRUE( insert_ok );
    }

    c_snap = txn_holder.commit_transaction();
    EXPECT_EQ( 1, get_snapshot_version( c_snap, pid ) );


    // apply the update
    part->execute_deserialized_partition_update( stashed_2.deserialized_ );
    destroy_stashed_update( stashed_2 );

    // now read
    set_snapshot_version( c_snap, pid, 2 );
    std::vector<std::string> expected_vals = default_vals;
    expected_vals[2] = final_vals[2];
    c_snap = check_values_same( part, c_snap, pid, expected_vals );
    EXPECT_EQ( 2, get_snapshot_version( c_snap, pid ) );


    // apply the update
    part->execute_deserialized_partition_update( stashed_3.deserialized_ );
    destroy_stashed_update( stashed_3 );

    // now read
    set_snapshot_version( c_snap, pid, 3 );
    expected_vals[3] = final_vals[3];
    c_snap = check_values_same( part, c_snap, pid, expected_vals );
    EXPECT_EQ( 3, get_snapshot_version( c_snap, pid ) );

    std::vector<std::string> expected_vals_4 = expected_vals;
    expected_vals_4[4] = final_vals[4];
    std::vector<std::string> expected_vals_5 = expected_vals_4;
    expected_vals_5[5] = final_vals[5];

    std::thread t_5( [part, pid, expected_vals_5]() {
        snapshot_vector c_snap_5;
        set_snapshot_version( c_snap_5, pid, 5 );
        c_snap_5 = check_values_same( part, c_snap_5, pid, expected_vals_5 );
        EXPECT_EQ( 5, get_snapshot_version( c_snap_5, pid ) );
    } );

    std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );

    std::thread t_4( [part, pid, expected_vals_4]() {
        snapshot_vector c_snap_4;
        set_snapshot_version( c_snap_4, pid, 4 );
        c_snap_4 = check_values_same( part, c_snap_4, pid, expected_vals_4 );
        EXPECT_EQ( 4, get_snapshot_version( c_snap_4, pid ) );
    } );

    std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );
    part->set_update_queue_expected_version( 4 );
    uint32_t num_updates = part->add_stashed_update( std::move( stashed_4 ) );
    EXPECT_EQ( 1, num_updates );

    join_thread( t_4 );

    num_updates = part->add_stashed_update( std::move( stashed_6 ) );
    EXPECT_EQ( 1, num_updates );
    num_updates = part->add_stashed_update( std::move( stashed_5 ) );
    EXPECT_EQ( 2, num_updates );

    join_thread( t_5 );

    expected_vals = expected_vals_5;
    set_snapshot_version( c_snap, pid, 5 );
    c_snap = check_values_same( part, c_snap, pid, expected_vals );
    EXPECT_EQ( 5, get_snapshot_version( c_snap, pid ) );

    // drain queue
    uint32_t num_updates_applied = part->apply_k_updates_or_empty( 2 );
    EXPECT_EQ( 1, num_updates_applied );

    expected_vals[6] = final_vals[6];
    set_snapshot_version( c_snap, pid, 6);
    c_snap = check_values_same( part, c_snap, pid, expected_vals );
    EXPECT_EQ( 6, get_snapshot_version( c_snap, pid ) );

    std::thread t_6( [part, pid, order_table_id, p_start] {
        snapshot_vector b_snap;
        set_snapshot_version( b_snap, pid, 8 );

        transaction_partition_holder txn_holder;
        txn_holder.add_write_partition( pid, part );
        std::vector<partition_column_identifier> inflight_pids = {pid};
        txn_holder.add_do_not_apply_last( inflight_pids );

        txn_holder.begin_transaction( b_snap );

        cell_identifier loc_cid;
        loc_cid.table_id_ = order_table_id;
        loc_cid.col_id_ = 0;
        loc_cid.key_ = p_start;
        bool write_ok = txn_holder.update_string_data( loc_cid, "9-nine" );
        EXPECT_TRUE( write_ok );

        snapshot_vector w_snap = txn_holder.commit_transaction();
        EXPECT_EQ( 9, get_snapshot_version( w_snap, pid ) );
    } );
    std::thread t_7( [part, pid, order_table_id, p_start] {
        std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );
        snapshot_vector b_snap;
        set_snapshot_version( b_snap, pid, 7 );

        transaction_partition_holder txn_holder;
        txn_holder.add_write_partition( pid, part );
        txn_holder.begin_transaction( b_snap );

        cell_identifier loc_cid;
        loc_cid.table_id_ = order_table_id;
        loc_cid.col_id_ = 0;
        loc_cid.key_ = p_start;
        bool write_ok = txn_holder.update_string_data( loc_cid, "8-eight" );
        EXPECT_TRUE( write_ok );

        snapshot_vector w_snap = txn_holder.commit_transaction();
        EXPECT_EQ( 8, get_snapshot_version( w_snap, pid ) );
    } );

    cid.key_ = 7;
    serialized_update v7_update = std::move( create_serialized_update(
        pid, 7, cid, final_vals[7].c_str(), final_vals[7].size(), p_type ) );
    stashed_update stashed_7 = std::move( create_stashed_update( v7_update ) );
    EXPECT_EQ( 7, stashed_7.commit_version_ );

    part->add_stashed_update( std::move( stashed_7 ) );

    join_thread( t_6 );
    join_thread( t_7 );
}

TEST_F( update_propagation_test, partition_apply_test_row ) {
    partition_apply_test( partition_type::type::ROW );
}
TEST_F( update_propagation_test, partition_apply_test_col ) {
    partition_apply_test( partition_type::type::COLUMN );
}
TEST_F( update_propagation_test, partition_apply_test_sorted_col ) {
    partition_apply_test( partition_type::type::SORTED_COLUMN );
}
TEST_F( update_propagation_test, partition_apply_test_multi_col ) {
    partition_apply_test( partition_type::type::MULTI_COLUMN );
}
TEST_F( update_propagation_test, partition_apply_test_sorted_multi_col ) {
    partition_apply_test( partition_type::type::SORTED_MULTI_COLUMN );
}


void update_source_test( const partition_type::type& p_type ) {
    uint32_t    order_table_id = 0;
    std::string table_name = "orders";
    uint32_t    num_records_in_chain = 5;
    uint32_t    num_records_in_snapshot_chain = 5;
    uint32_t    col_start = 0;
    uint32_t    col_end = 0;
    uint64_t    p1_start = 0;
    uint64_t    p1_end = 10;
    uint64_t    p2_start = 11;
    uint64_t    p2_end = 20;

    std::vector<cell_data_type> col_types = {cell_data_type::STRING};

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;

    table_metadata t_metadata = create_table_metadata(
        table_name, order_table_id, ( col_end - col_start ) + 1, col_types,
        num_records_in_chain, num_records_in_snapshot_chain,
        k_unassigned_master, 10 /* partition size*/,
        ( col_end - col_start ) + 1, 10 /* partition tracking size*/,
        ( col_end - col_start ) + 1, p_type, s_type,
        false /* enable sec storage */, "/tmp/" );
    tables_metadata ts_metadata = create_tables_metadata(
        1, k_unassigned_master, 10 /*num clients */, 10 /*gc sleep time*/,
        false /* enable sec storage */, "/tmp" );

    partition_column_identifier p1_id = create_partition_column_identifier(
        order_table_id, p1_start, p1_end, col_start, col_end );
    partition_column_identifier p2_id = create_partition_column_identifier(
        order_table_id, p2_start, p2_end, col_start, col_end );

    tables db_tables;
    db_tables.init( ts_metadata, make_no_op_update_destination_generator(),
                    make_update_enqueuers() );

    uint32_t found_tid = db_tables.create_table( t_metadata );
    EXPECT_EQ( found_tid, t_metadata.table_id_ );

    std::shared_ptr<partition> p1 =
        db_tables.add_partition( p1_id, p_type, s_type );
    EXPECT_NE( nullptr, p1 );
    p1->unlock_partition();
    std::shared_ptr<partition> p2 =
        db_tables.add_partition( p2_id, p_type, s_type );
    EXPECT_NE( nullptr, p2 );
    p2->unlock_partition();

    std::shared_ptr<vector_update_source> source =
        std::make_shared<vector_update_source>();

    std::vector<std::string> default_vals = {
        "0",  "1",  "2",  "3",  "4",  "5",  "6",  "7",  "8",  "9", "10",
        "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"};
    std::vector<std::string> final_vals = {
        "0",        "1",  "two", "three", "4",  "5",      "6",
        "7",        "8",  "9",   "10",    "11", "twelve", "thirteen",
        "fourteen", "15", "16",  "17",    "18", "19",     "20"};

    cell_identifier cid;
    cid.table_id_ = order_table_id;
    cid.col_id_ = col_start;
    cid.key_ = 0;

    cid.key_ = 2;
    serialized_update v_p1_2_update = std::move( create_serialized_update(
        p1_id, 2, cid, final_vals[2].c_str(), final_vals[2].size(), p_type ) );
    cid.key_ = 3;
    serialized_update v_p1_3_update = std::move( create_serialized_update(
        p1_id, 3, cid, final_vals[3].c_str(), final_vals[3].size(), p_type ) );
    cid.key_ = 12;
    serialized_update v_p2_2_update = std::move(
        create_serialized_update( p2_id, 2, cid, final_vals[12].c_str(),
                                  final_vals[12].size(), p_type ) );
    cid.key_ = 13;
    serialized_update v_p2_3_update = std::move(
        create_serialized_update( p2_id, 3, cid, final_vals[13].c_str(),
                                  final_vals[13].size(), p_type ) );

    // insert
    for( auto part : {p1, p2} ) {
        snapshot_vector c_snap = {};
        const auto&     pid = part->get_metadata().partition_id_;
        set_snapshot_version( c_snap, pid, 0 );

        transaction_partition_holder txn_holder;
        txn_holder.add_write_partition( pid, part );
        txn_holder.begin_transaction( c_snap );
        for( uint64_t id = pid.partition_start;
             id <= (uint64_t) pid.partition_end; id++ ) {
            cid.key_ = id;
            bool insert_ok =
                txn_holder.insert_string_data( cid, default_vals[id] );
            EXPECT_TRUE( insert_ok );
        }
        c_snap = txn_holder.commit_transaction();
        EXPECT_EQ( 1, get_snapshot_version( c_snap, pid ) );
    }


    // sleep for 10 ms, if more than 2 updates, then apply 1 update
    update_enqueuer_configs enqueue_configs =
        construct_update_enqueuer_configs( 10, 2, 1 );

    update_enqueuer                                   enqueuer;
    partition_subscription_bounds                     pid_bounds;
    update_enqueuer_subscription_information          enqueue_sub_info;

    pid_bounds.set_expected_number_of_tables(
        ts_metadata.expected_num_tables_ );
    enqueue_sub_info.set_expected_number_of_tables(
        ts_metadata.expected_num_tables_ );
    pid_bounds.create_table( t_metadata );
    enqueue_sub_info.create_table( t_metadata );

    enqueuer.set_update_consumption_source( source, enqueue_configs );
    enqueuer.set_state( &db_tables, &pid_bounds, &enqueue_sub_info );

    // check value
    std::vector<std::string> expected_vals = default_vals;
    snapshot_vector          expected_version = {};
    set_snapshot_version( expected_version, p1_id, 1 );
    set_snapshot_version( expected_version, p2_id, 1 );
    snapshot_vector begin_version = expected_version;
    for( auto part : {p1, p2} ) {
        const auto&     pid = part->get_metadata().partition_id_;
        snapshot_vector c_snap =
            check_values_same( part, begin_version, pid, expected_vals );
        EXPECT_EQ( get_snapshot_version( expected_version, pid ),
                   get_snapshot_version( c_snap, pid ) );
    }

    propagation_configuration prop_config;
    prop_config.type = propagation_type::VECTOR;
    prop_config.partition = 0;

    subscription_bound bound( K_COMMITTED, 0 );
    EXPECT_TRUE( pid_bounds.set_upper_bound( p1_id, bound ) );
    EXPECT_TRUE( pid_bounds.set_upper_bound( p2_id, bound ) );
    pid_bounds.insert_lower_bound( p1_id, 0 );
    pid_bounds.insert_lower_bound( p2_id, 0 );

    enqueuer.add_source( prop_config, p1_id, true /*do seek*/,
                         k_add_replica_cause_string );
    enqueuer.add_source( prop_config, p2_id, true /* do seek*/,
                         k_add_replica_cause_string );

    // this can't be applied yet
    source->add_update( v_p2_3_update );
    source->add_update( v_p1_2_update );


    enqueuer.start_enqueuing();

    //none of these changes should exist, even if we sleep
    std::this_thread::sleep_for( std::chrono::milliseconds( 20 ) );
    for( auto part : {p1, p2} ) {
        const auto&     pid = part->get_metadata().partition_id_;
        snapshot_vector c_snap = {};
        c_snap = check_values_same( part, begin_version, pid, expected_vals );
        EXPECT_EQ( get_snapshot_version( expected_version, pid ),
                   get_snapshot_version( c_snap, pid ) );
    }

    p1->set_update_queue_expected_version( 2 );
    p2->set_update_queue_expected_version( 2 );


    expected_vals[2] = final_vals[2];
    set_snapshot_version( expected_version, p1_id, 2 );
    set_snapshot_version( begin_version, p1_id,
                          2 );  // this should force the update to be applied
    for( auto part : {p1, p2} ) {
        const auto&     pid = part->get_metadata().partition_id_;
        snapshot_vector c_snap = {};
        c_snap = check_values_same( part, begin_version, pid, expected_vals );
        EXPECT_EQ( get_snapshot_version( expected_version, pid ),
                   get_snapshot_version( c_snap, pid ) );
    }

    // this should automatically be applied, if we sleep for 20 ms (2 updates,
    // and 1 is the threshold)
    source->add_update( v_p2_2_update );
    std::this_thread::sleep_for( std::chrono::milliseconds( 20 ) );
    expected_vals[12] = final_vals[12];
    set_snapshot_version( expected_version, p2_id, 2 );
    set_snapshot_version( begin_version, p2_id, 2 );
    for( auto part : {p1, p2} ) {
        const auto&     pid = part->get_metadata().partition_id_;
        snapshot_vector c_snap = {};
        c_snap = check_values_same( part, begin_version, pid, expected_vals );
        EXPECT_EQ( get_snapshot_version( expected_version, pid ),
                   get_snapshot_version( c_snap, pid ) );
    }

    // but if we set the begin timestamp as 3 then it should apply the update
    expected_vals[13] = final_vals[13];
    set_snapshot_version( begin_version, p2_id, 3 );
    set_snapshot_version( expected_version, p2_id, 3 );
    for( auto part : {p1, p2} ) {
        const auto&     pid = part->get_metadata().partition_id_;
        snapshot_vector c_snap = {};
        c_snap = check_values_same( part, begin_version, pid, expected_vals );
        EXPECT_EQ( get_snapshot_version( expected_version, pid ),
                   get_snapshot_version( c_snap, pid ) );
    }

    source->add_update( v_p1_3_update );
    // this should block until the enqueuer enques the update, then we should
    // apply it
    set_snapshot_version( begin_version, p1_id, 3 );
    expected_vals[3] = final_vals[3];
    set_snapshot_version( expected_version, p1_id, 3 );
    for( auto part : {p1, p2} ) {
        const auto&     pid = part->get_metadata().partition_id_;
        snapshot_vector c_snap = {};
        c_snap = check_values_same( part, begin_version, pid, expected_vals );
        EXPECT_EQ( get_snapshot_version( expected_version, pid ),
                   get_snapshot_version( c_snap, pid ) );
    }

    enqueuer.stop_enqueuing();
}

TEST_F( update_propagation_test, update_source_test_row ) {
    update_source_test( partition_type::type::ROW );
}
TEST_F( update_propagation_test, update_source_test_col ) {
    update_source_test( partition_type::type::COLUMN );
}
TEST_F( update_propagation_test, update_source_test_sorted_col ) {
    update_source_test( partition_type::type::SORTED_COLUMN );
}
TEST_F( update_propagation_test, update_source_test_multi_col ) {
    update_source_test( partition_type::type::MULTI_COLUMN );
}
TEST_F( update_propagation_test, update_source_test_sorted_mulit_col ) {
    update_source_test( partition_type::type::SORTED_MULTI_COLUMN );
}


void update_partition_test( const partition_type::type& p_type ) {
    uint32_t           order_table_id = 0;
    std::string        table_name = "orders";
    uint32_t           num_records_in_chain = 5;
    uint32_t           num_records_in_snapshot_chain = 5;
    uint32_t           col_start = 0;
    uint32_t           col_end = 0;
    uint64_t           p1_start = 0;
    uint64_t           p1_end = 10;
    uint64_t           p2_start = 11;
    uint64_t           p2_end = 20;
    uint64_t           p1_split = 6;
    uint64_t           p2_split = 16;

    std::vector<cell_data_type> col_types = {cell_data_type::STRING};
    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;

    table_metadata t_metadata = create_table_metadata(
        table_name, order_table_id, ( col_end - col_start ) + 1, col_types,
        num_records_in_chain, num_records_in_snapshot_chain,
        k_unassigned_master, 10 /* partition size*/,
        ( col_end - col_start ) + 1, 10 /* partition trackign size*/,
        ( col_end - col_start ) + 1, p_type, s_type,
        false /* enable sec storage */, "/tmp/" );

    tables_metadata ts_metadata = create_tables_metadata(
        1, k_unassigned_master, 10 /*num clients */, 10 /*gc sleep time*/,
        false /* enable sec storage */, "/tmp" );

    partition_column_identifier p1_id = create_partition_column_identifier(
        order_table_id, p1_start, p1_end, col_start, col_end );
    partition_column_identifier p2_id = create_partition_column_identifier(
        order_table_id, p2_start, p2_end, col_start, col_end );
    partition_column_identifier p1_0_5_id = create_partition_column_identifier(
        order_table_id, p1_start, p1_split - 1, col_start, col_end );
    partition_column_identifier p1_6_10_id = create_partition_column_identifier(
        order_table_id, p1_split, p1_end, col_start, col_end );
    partition_column_identifier p2_11_15_id =
        create_partition_column_identifier( order_table_id, p2_start,
                                            p2_split - 1, col_start, col_end );
    partition_column_identifier p2_16_20_id =
        create_partition_column_identifier( order_table_id, p2_split, p2_end,
                                            col_start, col_end );

    auto enqueuers = make_update_enqueuers();

    tables db_tables;
    db_tables.init( ts_metadata, make_no_op_update_destination_generator(),
                    enqueuers );

    uint32_t found_tid = db_tables.create_table( t_metadata );
    EXPECT_EQ( found_tid, t_metadata.table_id_ );

    {
        std::shared_ptr<partition> p1 =
            db_tables.add_partition( p1_id, p_type, s_type );
        EXPECT_NE( nullptr, p1 );
        p1->unlock_partition();
        std::shared_ptr<partition> p2 =
            db_tables.add_partition( p2_id, p_type, s_type );
        EXPECT_NE( nullptr, p2 );
        p2->unlock_partition();
    }

    cell_identifier cid;
    cid.table_id_ = order_table_id;
    cid.col_id_ = col_start;
    cid.key_ = 0;

    std::vector<std::string> default_vals = {
        "0",  "1",  "2",  "3",  "4",  "5",  "6",  "7",  "8",  "9", "10",
        "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"};
    // insert
    for( const auto& pid : {p1_id, p2_id} ) {
        snapshot_vector c_snap = {};
        auto            part = db_tables.get_partition( pid );
        set_snapshot_version( c_snap, pid, 0 );

        transaction_partition_holder txn_holder;
        txn_holder.add_write_partition( pid, part );
        txn_holder.begin_transaction( c_snap );

        for( uint64_t id = pid.partition_start;
             id <= (uint64_t) pid.partition_end; id++ ) {
            cid.key_ = id;
            bool insert_ok =
                txn_holder.insert_string_data( cid, default_vals[id] );
            EXPECT_TRUE( insert_ok );
        }
        c_snap = txn_holder.commit_transaction();
        EXPECT_EQ( 1, get_snapshot_version( c_snap, pid ) );

        part->set_update_queue_expected_version( 2 );
    }


    // sleep for 10 ms, if more than 2 updates, then apply 1 update
    update_enqueuer_configs enqueue_configs =
        construct_update_enqueuer_configs( 10, 2, 1 );
    std::shared_ptr<vector_update_source> source =
        std::make_shared<vector_update_source>();

    propagation_configuration prop_configs;
    prop_configs.type = propagation_type::VECTOR;
    prop_configs.partition = 0;

    auto enqueuer = std::make_shared<update_enqueuer>();
    enqueuer->set_update_consumption_source( source, enqueue_configs );
    enqueuers->add_enqueuer( enqueuer );

    enqueuers->set_tables( ts_metadata, (void*) &db_tables );

    snapshot_vector snap_add_source;
    enqueuers->add_source( prop_configs, p1_id, snap_add_source,
                           true /* do seek*/, k_add_replica_cause_string );
    enqueuers->add_source( prop_configs, p2_id, snap_add_source,
                           true /* do seek */, k_add_replica_cause_string );

    write_buffer* split_buf = new write_buffer();

    partition_column_operation_identifier poi_split;
    poi_split.add_partition_op( p1_id, p1_split, k_unassigned_col, K_SPLIT_OP );
    split_buf->add_to_partition_buffer( std::move( poi_split ) );
    snapshot_vector split_vv;
    set_snapshot_version( split_vv, p1_id, 2 );
    set_snapshot_version( split_vv, p1_0_5_id, 2 );
    set_snapshot_version( split_vv, p1_6_10_id, 2 );
    split_buf->store_propagation_information(
        split_vv, p1_id, partition_column_identifier_key_hasher{}( p1_id ), 2 );

    update_propagation_information p1_info;
    p1_info.propagation_config_ = prop_configs;
    p1_info.identifier_ = p1_id;
    p1_info.do_seek_ = false;
    update_propagation_information p1_0_5_info = p1_info;
    p1_0_5_info.identifier_ = p1_0_5_id;
    p1_0_5_info.do_seek_ = should_seek_topic( p1_0_5_info, p1_info );
    update_propagation_information p1_6_10_info = p1_info;
    p1_6_10_info.identifier_ = p1_6_10_id;
    p1_6_10_info.do_seek_ = should_seek_topic( p1_6_10_info, p1_info );

    split_buf->add_to_subscription_buffer( p1_info, false );
    split_buf->add_to_subscription_buffer( p1_0_5_info, true );
    split_buf->add_to_subscription_buffer( p1_6_10_info, true );

    serialized_update split_update = serialize_write_buffer( split_buf );
    stashed_update    stashed_split = create_stashed_update( split_update );

    free( split_update.buffer_ );
    delete( split_buf );

    snapshot_vector          begin_version = split_vv;
    std::vector<std::string> expected_vals = default_vals;
    snapshot_vector          expected_version = split_vv;

    {
        // hold a reference to p1 before this is enqueued
        auto p1 = db_tables.get_partition( p1_id );

        enqueuer->enqueue_stashed_update( std::move( stashed_split ) );
        // this should let the update be applied, without the client doing
        // anything
        std::this_thread::sleep_for( std::chrono::milliseconds(
            50 ) );

        snapshot_vector c_snap =
            check_values_same( p1, begin_version, p1_id, expected_vals );
        EXPECT_EQ( get_snapshot_version( expected_version, p1_id ),
                   get_snapshot_version( c_snap, p1_id ) );
    }

    // p1 should no longer exist here because it got removed in the split
    EXPECT_EQ( nullptr, db_tables.get_partition( p1_id ) );
    for( const auto& pid : {p1_0_5_id, p1_6_10_id} ) {
        auto            part = db_tables.get_partition( pid );
        EXPECT_NE( nullptr, part );
        snapshot_vector c_snap =
            check_values_same( part, begin_version, pid, expected_vals );
        EXPECT_EQ( get_snapshot_version( expected_version, pid ),
                   get_snapshot_version( c_snap, pid ) );
    }

    snapshot_vector merge_vv;
    set_snapshot_version( merge_vv, p1_id, 3 );
    set_snapshot_version( merge_vv, p1_0_5_id, 3 );
    set_snapshot_version( merge_vv, p1_6_10_id, 3 );

    for( const auto& pid : {p1_0_5_id, p1_6_10_id} ) {
        write_buffer*                  merge_buf = new write_buffer();
        partition_column_operation_identifier poi_merge;
        poi_merge.add_partition_op( p1_id, p1_split, k_unassigned_col,
                                    K_MERGE_OP );
        merge_buf->add_to_partition_buffer( std::move( poi_merge ) );
        merge_buf->store_propagation_information(
            merge_vv, pid, partition_column_identifier_key_hasher{}( pid ), 2 );


        update_propagation_information ori_info = p1_info;
        ori_info.identifier_ = pid;
        ori_info.do_seek_ = false;

        p1_info.do_seek_ = should_seek_topic( p1_info, ori_info );

        merge_buf->add_to_subscription_buffer( p1_info, true );
        merge_buf->add_to_subscription_buffer( ori_info, false );

        serialized_update merge_update = serialize_write_buffer( merge_buf );
        stashed_update    stashed_merge = create_stashed_update( merge_update );

        free( merge_update.buffer_ );
        delete( merge_buf );

        enqueuer->enqueue_stashed_update( std::move( stashed_merge ) );
    }
    write_buffer*                  mergee_buf = new write_buffer();
    partition_column_operation_identifier poi_mergee;
    poi_mergee.add_partition_op( p1_id, p1_split, k_unassigned_col,
                                 K_INSERT_OP );
    mergee_buf->add_to_partition_buffer( std::move( poi_mergee ) );
    mergee_buf->store_propagation_information(
        merge_vv, p1_id, partition_column_identifier_key_hasher{}( p1_id ), 3 );

    serialized_update mergee_update = serialize_write_buffer( mergee_buf );
    stashed_update    stashed_mergee = create_stashed_update( mergee_update );

    free( mergee_update.buffer_ );
    delete( mergee_buf );

    enqueuer->enqueue_stashed_update( std::move( stashed_mergee ) );

    begin_version = merge_vv;
    expected_vals = default_vals;
    expected_version = merge_vv;
    {
        auto            p1 = db_tables.get_partition( p1_id );
        EXPECT_NE( nullptr, p1 );
        snapshot_vector c_snap =
            check_values_same( p1, begin_version, p1_id, expected_vals );
        EXPECT_EQ( get_snapshot_version( expected_version, p1_id ),
                   get_snapshot_version( c_snap, p1_id ) );

        DLOG( INFO ) << "==== Test ==== Getting partition:" << p1_0_5_id;
        EXPECT_EQ( nullptr, db_tables.get_partition_if_active( p1_0_5_id ) );
        DLOG( INFO ) << "==== Test ==== Getting partition:" << p1_6_10_id;
        EXPECT_EQ( nullptr, db_tables.get_partition_if_active( p1_6_10_id ) );
    }

    write_buffer* split2_buf = new write_buffer();

    partition_column_operation_identifier poi_split2;
    poi_split2.add_partition_op( p1_id, p1_split, k_unassigned_col,
                                 K_SPLIT_OP );
    split2_buf->add_to_partition_buffer( std::move( poi_split2 ) );
    snapshot_vector split2_vv;
    set_snapshot_version( split2_vv, p1_id, 4 );
    set_snapshot_version( split2_vv, p1_0_5_id, 4 );
    set_snapshot_version( split2_vv, p1_6_10_id, 4 );
    split2_buf->store_propagation_information(
        split2_vv, p1_id, partition_column_identifier_key_hasher{}( p1_id ), 4 );

    p1_info.do_seek_ = false;
    p1_0_5_info.do_seek_ = should_seek_topic( p1_0_5_info, p1_info );
    p1_6_10_info.do_seek_ = should_seek_topic( p1_6_10_info, p1_info );

    split2_buf->add_to_subscription_buffer( p1_info, false );
    split2_buf->add_to_subscription_buffer( p1_0_5_info, true );
    split2_buf->add_to_subscription_buffer( p1_6_10_info, true );

    serialized_update split2_update = serialize_write_buffer( split2_buf );
    stashed_update    stashed_split2 = create_stashed_update( split2_update );

    free( split2_update.buffer_ );
    delete( split2_buf );

    begin_version = split2_vv;
    expected_vals = default_vals;
    expected_version = split2_vv;

    {

        auto        p1 = db_tables.get_partition( p1_id );
        std::thread t_split2( [p1, p1_id, begin_version, expected_vals,
                               expected_version]() {
            snapshot_vector c_snap =
                check_values_same( p1, begin_version, p1_id, expected_vals );
            EXPECT_EQ( get_snapshot_version( expected_version, p1_id ),
                       get_snapshot_version( c_snap, p1_id ) );

        } );

        std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );

        // sleep and then enqueue, which means the other thread should have the
        // lock
        enqueuer->enqueue_stashed_update( std::move( stashed_split2 ) );
        join_thread( t_split2 );
    }
    // p1 should no longer exist here because it got removed in the split
    DLOG( INFO ) << "===Get partition:" << p1_id;
    EXPECT_EQ( nullptr, db_tables.get_partition( p1_id ) );
    for( const auto& pid : {p1_0_5_id, p1_6_10_id} ) {
        auto            part = db_tables.get_partition( pid );
        EXPECT_NE( nullptr, part );
        snapshot_vector c_snap =
            check_values_same( part, begin_version, pid, expected_vals );
        EXPECT_EQ( get_snapshot_version( expected_version, pid ),
                   get_snapshot_version( c_snap, pid ) );
    }

    // now test when the other partition whens the race
    write_buffer* split3_buf = new write_buffer();

    partition_column_operation_identifier poi_split3;
    poi_split3.add_partition_op( p2_id, p2_split, k_unassigned_col,
                                 K_SPLIT_OP );
    split3_buf->add_to_partition_buffer( std::move( poi_split3 ) );
    snapshot_vector split3_vv;
    set_snapshot_version( split3_vv, p2_id, 2 );
    set_snapshot_version( split3_vv, p2_11_15_id, 2 );
    set_snapshot_version( split3_vv, p2_16_20_id, 2 );
    split3_buf->store_propagation_information(
        split3_vv, p2_id, partition_column_identifier_key_hasher{}( p2_id ), 2 );


    update_propagation_information p2_info;
    p2_info.identifier_ = p2_id;
    p2_info.propagation_config_ = prop_configs;
    update_propagation_information p2_11_15_info = p2_info;
    p2_11_15_info.identifier_ = p2_11_15_id;
    update_propagation_information p2_16_20_info = p2_info;
    p2_16_20_info.identifier_ = p2_16_20_id;

    p2_info.do_seek_ = false;
    p2_11_15_info.do_seek_ = should_seek_topic( p2_11_15_info, p2_info );
    p2_16_20_info.do_seek_ = should_seek_topic( p2_16_20_info, p2_info );

    split3_buf->add_to_subscription_buffer( p2_info, false );
    split3_buf->add_to_subscription_buffer( p2_11_15_info, true );
    split3_buf->add_to_subscription_buffer( p2_16_20_info, true );

    serialized_update split3_update = serialize_write_buffer( split3_buf );
    stashed_update    stashed_split3 = create_stashed_update( split3_update );

    free( split3_update.buffer_ );
    delete( split3_buf );

    begin_version = split3_vv;
    expected_vals = default_vals;
    expected_version = split3_vv;

    {

        auto        p2_11 = db_tables.get_partition( p2_11_15_id );
        EXPECT_EQ( nullptr, p2_11 );
        p2_11= db_tables.get_or_create_partition( p2_11_15_id );
        EXPECT_NE( nullptr, p2_11 );

        std::thread t_split3( [p2_11, p2_11_15_id, begin_version, expected_vals,
                               expected_version]() {
            snapshot_vector c_snap = check_values_same(
                p2_11, begin_version, p2_11_15_id, expected_vals );
            EXPECT_EQ( get_snapshot_version( expected_version, p2_11_15_id ),
                       get_snapshot_version( c_snap, p2_11_15_id ) );

        } );

        std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );

        // sleep and then enqueue, which means the other thread should have the
        // lock
        enqueuer->enqueue_stashed_update( std::move( stashed_split3 ) );
        join_thread( t_split3 );
    }
    for( ;; ) {
        auto found_part = db_tables.get_partition( p2_id );
        if( found_part == nullptr ) {
            break;
        }
    }
    EXPECT_EQ( nullptr, db_tables.get_partition( p2_id ) );

    for( const auto& pid : {p2_11_15_id, p2_16_20_id} ) {
        auto part = db_tables.get_partition( pid );
        EXPECT_NE( nullptr, part );
        snapshot_vector c_snap =
            check_values_same( part, begin_version, pid, expected_vals );
        EXPECT_EQ( get_snapshot_version( expected_version, pid ),
                   get_snapshot_version( c_snap, pid ) );
    }

    enqueuers->stop_enqueuers();
}

TEST_F( update_propagation_test, update_partition_test_row ) {
    update_partition_test( partition_type::type::ROW );
}

TEST_F( update_propagation_test, update_partition_test_col ) {
    update_partition_test( partition_type::type::COLUMN );
}
TEST_F( update_propagation_test, update_partition_test_sorted_col ) {
    update_partition_test( partition_type::type::SORTED_COLUMN );
}
TEST_F( update_propagation_test, update_partition_test_multi_col ) {
    update_partition_test( partition_type::type::MULTI_COLUMN );
}
TEST_F( update_propagation_test, update_partition_test_sorted_multi_col ) {
    update_partition_test( partition_type::type::SORTED_MULTI_COLUMN );
}

void partition_sends_update_test( const partition_type::type& p_type ) {

    std::shared_ptr<vector_update_destination> update_dest =
        std::make_shared<vector_update_destination>( 1 );
    std::vector<serialized_update> expected_serialized_updates;
    bool do_match = update_dest->do_stored_and_expected_match(
        expected_serialized_updates );
    EXPECT_TRUE( do_match );

    uint32_t             order_table_id = 0;
    uint32_t             col_start = 0;
    uint32_t             col_end = 0;
    uint32_t             p_start = 0;
    uint32_t             p_end = 10;
    uint32_t             num_records_in_chain = 5;
    uint32_t             num_records_in_snapshot_chain = 5;
    uint32_t             master_location = 1;

    partition_metadata p_metadata = create_partition_metadata(
        order_table_id, p_start, p_end, col_start, col_end,
        num_records_in_chain, num_records_in_snapshot_chain, master_location );
    partition_column_identifier pid = p_metadata.partition_id_;
    uint64_t             pid_hash = p_metadata.partition_id_hash_;

    std::shared_ptr<partition> part = create_partition( p_type );
    auto part_version_holder = std::make_shared<partition_column_version_holder>(
        p_metadata.partition_id_, 0, 0 );

    std::vector<cell_data_type> col_types = {cell_data_type::UINT64};
    part->init( p_metadata, update_dest, part_version_holder, col_types,
                (void*) nullptr, 0 );
    part->init_records();
    part->set_master_location( master_location );

    // store the keys
    write_buffer*   insert_buf = new write_buffer();
    snapshot_vector begin_snap = {};
    snapshot_vector expected_commit_snap = {};
    set_snapshot_version( expected_commit_snap, pid_hash, 1 );

    transaction_partition_holder txn_holder;
    txn_holder.add_write_partition( pid, part );
    txn_holder.begin_transaction( begin_snap );
    cell_identifier cid;
    cid.table_id_ = order_table_id;
    cid.col_id_ = 0;
    cid.key_ = 0;

    std::vector<row_record*> row_recs;
    std::vector<packed_cell_data*> packed_cells;

    for( uint64_t id = pid.partition_start; id <= (uint64_t) pid.partition_end;
         id++ ) {
        cid.key_ = id;
        bool insert_ok = txn_holder.insert_uint64_data( cid, id );
        EXPECT_TRUE( insert_ok );

        add_to_insert_buf( insert_buf, row_recs, packed_cells, cid, id,
                           p_type );
    }
    snapshot_vector commit_vv = txn_holder.commit_transaction();
    EXPECT_EQ( expected_commit_snap, commit_vv );
    DVLOG( 40 ) << "Done commit";
    insert_buf->store_propagation_information( commit_vv, pid, pid_hash, 1 );

    DVLOG( 40 ) << "Serializing write buffer for test";
    serialized_update insert_update = serialize_write_buffer( insert_buf );
    expected_serialized_updates.push_back( std::move( insert_update ) );
    delete( insert_buf );

    do_match = update_dest->do_stored_and_expected_match(
        expected_serialized_updates );
    EXPECT_TRUE( do_match );

    clear_vectors( row_recs, packed_cells );


    uint32_t new_master_location = 0;
    begin_snap = commit_vv;
    expected_commit_snap = commit_vv;
    set_snapshot_version( expected_commit_snap, pid, 2 );
    txn_holder.add_write_partition( pid, part );
    txn_holder.begin_transaction( begin_snap );
    txn_holder.remaster_partitions( new_master_location );
    commit_vv = txn_holder.commit_transaction();
    EXPECT_EQ( expected_commit_snap, commit_vv );

    write_buffer*   remaster_buf = new write_buffer();
    remaster_buf = new write_buffer();
    partition_column_operation_identifier remaster_poi;
    remaster_poi.add_partition_op( pid, 0 /*64*/, new_master_location,
                                   K_REMASTER_OP );
    remaster_buf->add_to_partition_buffer( std::move( remaster_poi ) );
    remaster_buf->store_propagation_information( commit_vv, pid, pid_hash, 1 );
    serialized_update remaster_update = serialize_write_buffer( remaster_buf );
    expected_serialized_updates.push_back( std::move( remaster_update ) );
    delete( remaster_buf );

    do_match = update_dest->do_stored_and_expected_match(
        expected_serialized_updates );
    EXPECT_TRUE( do_match );

    clear_serialized_updates( expected_serialized_updates );
}

TEST_F( update_propagation_test, partition_sends_update_test_row ) {
    partition_sends_update_test( partition_type::type::ROW );
}
TEST_F( update_propagation_test, partition_sends_update_test_col ) {
    partition_sends_update_test( partition_type::type::COLUMN );
}
TEST_F( update_propagation_test, partition_sends_update_test_sorted_col ) {
    partition_sends_update_test( partition_type::type::SORTED_COLUMN );
}
TEST_F( update_propagation_test, partition_sends_update_test_multi_col ) {
    partition_sends_update_test( partition_type::type::MULTI_COLUMN );
}
TEST_F( update_propagation_test,
        partition_sends_update_test_sorted_multi_col ) {
    partition_sends_update_test( partition_type::type::SORTED_MULTI_COLUMN );
}
void sequential_split_merge_test( const partition_type::type& p_type ) {
    uint32_t           order_table_id = 0;
    std::string        table_name = "orders";
    uint32_t           num_records_in_chain = 5;
    uint32_t           num_records_in_snapshot_chain = 5;
    uint64_t           p1_start = 0;
    uint64_t           p1_end = 10;
    uint64_t           p1_split = 6;
    uint32_t           col_start = 0;
    uint32_t           col_end = 0;

    std::vector<cell_data_type> col_types = {cell_data_type::STRING};
    storage_tier_type::type     s_type = storage_tier_type::type::MEMORY;

    table_metadata t_metadata = create_table_metadata(
        table_name, order_table_id, ( col_end - col_start ) + 1, col_types,
        num_records_in_chain, num_records_in_snapshot_chain,
        k_unassigned_master, 10 /* partition size*/,
        ( col_end - col_start ) + 1, 10 /* partition tracking size*/,
        ( col_end - col_start ) + 1, p_type, s_type,
        false /* enable sec storage */, "/tmp/" );
    tables_metadata ts_metadata = create_tables_metadata(
        1, k_unassigned_master, 10 /*num clients */, 10 /*gc sleep time*/,
        false /* enable sec storage */, "/tmp" );

    partition_column_identifier p1_id = create_partition_column_identifier(
        order_table_id, p1_start, p1_end, col_start, col_end );
    partition_column_identifier p1_0_5_id =
        create_partition_column_identifier( order_table_id, p1_start, p1_split - 1 , col_start, col_end);
    partition_column_identifier p1_6_10_id =
        create_partition_column_identifier( order_table_id, p1_split, p1_end , col_start, col_end);

    auto enqueuers = make_update_enqueuers();

    tables db_tables;
    db_tables.init( ts_metadata, make_no_op_update_destination_generator(),
                    enqueuers );

    uint32_t found_tid = db_tables.create_table( t_metadata );
    EXPECT_EQ( found_tid, t_metadata.table_id_ );

    {
        std::shared_ptr<partition> p1 =
            db_tables.add_partition( p1_0_5_id, p_type, s_type );
        EXPECT_NE( nullptr, p1 );
        p1->unlock_partition();
        std::shared_ptr<partition> p2 =
            db_tables.add_partition( p1_6_10_id, p_type, s_type );
        EXPECT_NE( nullptr, p2 );
        p2->unlock_partition();
    }

    std::vector<std::string> default_vals = {"0", "1", "2", "3", "4", "5",
                                             "6", "7", "8", "9", "10"};

    cell_identifier cid;
    cid.table_id_ = order_table_id;
    cid.col_id_ = col_start;
    cid.key_ = 0;
    // insert
    for( const auto& pid : {p1_0_5_id, p1_6_10_id} ) {
        snapshot_vector c_snap = {};
        auto            part = db_tables.get_partition( pid );
        set_snapshot_version( c_snap, pid, 0 );

        transaction_partition_holder txn_holder;
        txn_holder.add_write_partition( pid, part );
        txn_holder.begin_transaction( c_snap );
        for( uint64_t id = pid.partition_start;
             id <= (uint64_t) pid.partition_end; id++ ) {
            cid.key_ = id;
            bool insert_ok =
                txn_holder.insert_string_data( cid, default_vals[id] );
            EXPECT_TRUE( insert_ok );
        }
        c_snap = txn_holder.commit_transaction();
        EXPECT_EQ( 1, get_snapshot_version( c_snap, pid ) );

        part->set_update_queue_expected_version( 2 );
    }

    // sleep for 10 ms, if more than 5 updates, then apply 0 updates
    update_enqueuer_configs enqueue_configs =
        construct_update_enqueuer_configs( 10, 5, 0 );
    std::shared_ptr<vector_update_source> source =
        std::make_shared<vector_update_source>();

    propagation_configuration prop_configs;
    prop_configs.type = propagation_type::VECTOR;
    prop_configs.partition = 0;

    auto enqueuer = std::make_shared<update_enqueuer>();
    enqueuer->set_update_consumption_source( source, enqueue_configs );
    enqueuers->add_enqueuer( enqueuer );

    enqueuers->set_tables( ts_metadata, (void*) &db_tables );


    snapshot_vector snap_add_source;
    enqueuers->add_source( prop_configs, p1_0_5_id, snap_add_source,
                           true /* do seek*/, k_split_cause_string );
    enqueuers->add_source( prop_configs, p1_6_10_id, snap_add_source,
                           true /*do seek*/, k_split_cause_string );

    write_buffer* split_buf = new write_buffer();

    snapshot_vector merge_vv;
    set_snapshot_version( merge_vv, p1_id, 2 );
    set_snapshot_version( merge_vv, p1_0_5_id, 2 );
    set_snapshot_version( merge_vv, p1_6_10_id, 2 );

    update_propagation_information p1_info;
    p1_info.propagation_config_ = prop_configs;
    p1_info.identifier_ = p1_id;

    for( const auto& pid : {p1_0_5_id, p1_6_10_id} ) {
        write_buffer*                  merge_buf = new write_buffer();
        partition_column_operation_identifier poi_merge;
        poi_merge.add_partition_op( p1_id, p1_split, k_unassigned_col,
                                    K_MERGE_OP );
        merge_buf->add_to_partition_buffer( std::move( poi_merge ) );
        merge_buf->store_propagation_information(
            merge_vv, pid, partition_column_identifier_key_hasher{}( pid ), 2 );

        update_propagation_information ori_info = p1_info;
        ori_info.identifier_ = pid;
        p1_info.do_seek_ = should_seek_topic( p1_info, ori_info );
        ori_info.do_seek_ = false;

        merge_buf->add_to_subscription_buffer( p1_info, true );
        merge_buf->add_to_subscription_buffer( ori_info, false );

        serialized_update merge_update = serialize_write_buffer( merge_buf );
        stashed_update    stashed_merge = create_stashed_update( merge_update );

        free( merge_update.buffer_ );
        delete( merge_buf );

        DLOG( INFO ) << "adding stashed merge for pid:" << pid;
        enqueuer->enqueue_stashed_update( std::move( stashed_merge ) );
    }
    write_buffer*                  mergee_buf = new write_buffer();
    partition_column_operation_identifier poi_mergee;
    poi_mergee.add_partition_op( p1_id, p1_split, k_unassigned_col,
                                 K_INSERT_OP );
    mergee_buf->add_to_partition_buffer( std::move( poi_mergee ) );
    mergee_buf->store_propagation_information(
        merge_vv, p1_id, partition_column_identifier_key_hasher{}( p1_id ), 3 );

    serialized_update mergee_update = serialize_write_buffer( mergee_buf );
    source->add_update(mergee_update);


    partition_column_operation_identifier poi_split;
    poi_split.add_partition_op( p1_id, p1_split, k_unassigned_col, K_SPLIT_OP );
    split_buf->add_to_partition_buffer( std::move( poi_split ) );
    snapshot_vector split_vv;
    set_snapshot_version( split_vv, p1_id, 3 );
    set_snapshot_version( split_vv, p1_0_5_id, 3 );
    set_snapshot_version( split_vv, p1_6_10_id, 3 );
    split_buf->store_propagation_information(
        split_vv, p1_id, partition_column_identifier_key_hasher{}( p1_id ), 2 );

    update_propagation_information p1_0_5_info = p1_info;
    p1_0_5_info.identifier_ = p1_0_5_id;
    update_propagation_information p1_6_10_info = p1_info;
    p1_6_10_info.identifier_ = p1_6_10_id;

    p1_info.do_seek_ = false;
    p1_0_5_info.do_seek_ = should_seek_topic( p1_0_5_info, p1_info );
    p1_6_10_info.do_seek_ = should_seek_topic( p1_6_10_info, p1_info );

    split_buf->add_to_subscription_buffer( p1_info, false );
    split_buf->add_to_subscription_buffer( p1_0_5_info, true );
    split_buf->add_to_subscription_buffer( p1_6_10_info, true );

    serialized_update split_update = serialize_write_buffer( split_buf );
    DLOG( INFO ) << "==== TEST adding split update";
    source->add_update( split_update );

    partition_column_identifier_set      write_set;
    partition_column_identifier_set      read_set = {p1_0_5_id, p1_6_10_id};
    transaction_partition_holder* holder = db_tables.get_partition_holder(
        write_set, read_set, partition_lookup_operation::GET );
    EXPECT_NE( nullptr, holder );

    // force the enqueing of these ops
    source->enqueue_updates( (void*) enqueuer.get() );
    source->enqueue_updates( (void*) enqueuer.get() );

    snapshot_vector begin_timestamp = split_vv;
    //    std::this_thread::sleep_for( std::chrono::milliseconds( 1 ) );
    DLOG( INFO ) << "Begin transaction";
    holder->begin_transaction( begin_timestamp );

    for ( uint64_t i = 0; i < default_vals.size(); i++) {
        cid.key_ = i;
        std::tuple<bool, std::string> got_data = holder->get_string_data( cid );
        EXPECT_TRUE( std::get<0>( got_data ) );
        EXPECT_EQ( default_vals.at( i ), std::get<1>( got_data ) );
    }

    holder->commit_transaction();
    delete holder;

    // should terminate the splitter
#if 0
    for( ;; ) {
        int gc_ret = enqueuers->gc_split_threads();
        if( gc_ret > 0 ) {
            break;
        }
    }
#endif
    enqueuer->stop_enqueuing();
}


TEST_F( update_propagation_test, sequential_split_merge_test_row ) {
    sequential_split_merge_test( partition_type::type::ROW );
}
TEST_F( update_propagation_test, sequential_split_merge_test_col ) {
    sequential_split_merge_test( partition_type::type::COLUMN );
}
TEST_F( update_propagation_test, sequential_split_merge_test_sorted_col ) {
    sequential_split_merge_test( partition_type::type::SORTED_COLUMN );
}
TEST_F( update_propagation_test, sequential_split_merge_test_multi_col ) {
    sequential_split_merge_test( partition_type::type::MULTI_COLUMN );
}
TEST_F( update_propagation_test,
        sequential_split_merge_test_sorted_multi_col ) {
    sequential_split_merge_test( partition_type::type::SORTED_MULTI_COLUMN );
}
