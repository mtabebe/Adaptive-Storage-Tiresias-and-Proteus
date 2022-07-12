#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/common/string_conversion.h"
#include "../src/data-site/db/db.h"
#include "../src/data-site/update-propagation/update_enqueuer.h"
#include "../src/data-site/update-propagation/vector_update_source.h"

class db_test : public ::testing::Test {};

void db_test_add_to_insert_buf( write_buffer*                   insert_buf,
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

void db_test_add_to_insert_buf_str(
    write_buffer* insert_buf, std::vector<row_record*>& row_recs,
    std::vector<packed_cell_data*>& packed_cells, const cell_identifier& cid,
    const std::string& str, uint32_t op_code,
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
            pc.set_string_data( str );
            EXPECT_TRUE( pc.is_present() );
            EXPECT_TRUE( r1->is_present() );

            vri_1.add_db_op( cid, r1, op_code );
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
            pcd->set_string_data( str );
            EXPECT_TRUE( pcd->is_present() );

            vri_1.add_db_op( cid, pcd, op_code );
            insert_buf->add_to_cell_buffer( std::move( vri_1 ) );

            packed_cells.push_back( pcd );

            break;
        }
    }
}


void db_test_clear_vectors( std::vector<row_record*>&       row_recs,
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

void table_creation_test( const partition_type::type& p_type ) {
    uint32_t site_loc = 1;
    uint32_t num_tables = 5;
    uint32_t num_clients = 10;
    uint32_t gc_sleep_time = 10;

    db db;
    db.init( make_no_op_update_destination_generator(), make_update_enqueuers(),
             create_tables_metadata(
                 num_tables, site_loc, num_clients, gc_sleep_time,
                 false /* enable sec storage */, "/tmp/" ) );

    EXPECT_EQ( site_loc, db.get_site_location() );
    table* lookup = db.get_tables()->get_table( "orders" );
    EXPECT_EQ( nullptr, lookup );

    int32_t  num_records_in_chain = k_num_records_in_chain;
    int32_t  num_records_in_snapshot_chain = k_num_records_in_snapshot_chain;
    uint32_t col_end = 0;
    std::vector<cell_data_type> col_types = {cell_data_type::INT64};

    table_metadata order_meta = create_table_metadata(
        "orders", 0, col_end + 1, col_types, num_records_in_chain,
        num_records_in_snapshot_chain, 0 /*site*/,
        10 /*default partition size*/, col_end + 1,
        10 /*default tracking partition size*/, col_end + 1, p_type,
        storage_tier_type::type::MEMORY, false /* enable sec storage */,
        "/tmp/" );

    uint32_t order_table_id = db.get_tables()->create_table( order_meta );
    EXPECT_EQ( 0, order_table_id );

    // should get the old table back
    uint32_t order_table_id_1 = db.get_tables()->create_table( order_meta );
    EXPECT_EQ( order_table_id, order_table_id_1 );

    table_metadata shopping_meta = create_table_metadata(
        "shopping_cart", 1, col_end + 1, col_types, num_records_in_chain,
        num_records_in_snapshot_chain, 0 /*site*/,
        10 /*default partition size*/, col_end + 1,
        10 /*default partition trackign size*/, col_end + 1, p_type,
        storage_tier_type::type::MEMORY, false /* enable sec storage */,
        "/tmp/" );

    uint32_t shopping_cart_table_id =
        db.get_tables()->create_table( shopping_meta );
    EXPECT_EQ( 1, shopping_cart_table_id );
    lookup = db.get_tables()->get_table( "shopping_cart" );
    EXPECT_NE( nullptr, lookup );
    EXPECT_EQ( shopping_cart_table_id, lookup->get_metadata().table_id_ );
}

TEST_F( db_test, table_creation_test_row ) {
    table_creation_test( partition_type::type::ROW );
}
TEST_F( db_test, table_creation_test_col ) {
    table_creation_test( partition_type::type::COLUMN );
}
TEST_F( db_test, table_creation_test_sorted_col ) {
    table_creation_test( partition_type::type::SORTED_COLUMN );
}
TEST_F( db_test, table_creation_test_multi_col ) {
    table_creation_test( partition_type::type::MULTI_COLUMN );
}
TEST_F( db_test, table_creation_test_sorted_multi_col ) {
    table_creation_test( partition_type::type::SORTED_MULTI_COLUMN );
}

void db_txn_ops( const partition_type::type& p_type ) {
    uint32_t site_loc = 1;
    uint32_t num_tables = 5;
    uint32_t num_clients = 10;
    int32_t  num_records_in_chain = k_num_records_in_chain;
    int32_t  num_records_in_snapshot_chain = k_num_records_in_snapshot_chain;
    uint32_t gc_sleep_time = 10;

    uint32_t c1 = 1;
    uint32_t c2 = 2;

    snapshot_vector empty_version;

    db db;
    db.init( make_no_op_update_destination_generator(), make_update_enqueuers(),
             create_tables_metadata(
                 num_tables, site_loc, num_clients, gc_sleep_time,
                 false /* enable sec storage */, "/tmp/" ) );

    uint32_t                    col_start = 0;
    uint32_t                    col_end = 0;
    std::vector<cell_data_type> col_types = {cell_data_type::INT64};

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;

    table_metadata order_meta = create_table_metadata(
        "orders", 0, col_end + 1, col_types, num_records_in_chain,
        num_records_in_snapshot_chain, 0 /*site*/,
        10 /*default partition size*/, col_end + 1,
        10 /*default trackign partition size*/, col_end + 1, p_type, s_type,
        false /* enable sec storage */, "/tmp/" );
    uint32_t order_table_id = db.get_tables()->create_table( order_meta );
    EXPECT_EQ( 0, order_table_id );

    partition_column_identifier p_0_10 = create_partition_column_identifier(
        order_table_id, 0, 10, col_start, col_end );
    partition_column_identifier p_11_15 = create_partition_column_identifier(
        order_table_id, 11, 15, col_start, col_end );
    partition_column_identifier p_16_20 = create_partition_column_identifier(
        order_table_id, 16, 20, col_start, col_end );

    // this should not work because the partition is not there
    snapshot_vector                 c1_state = empty_version;
    partition_column_identifier_set c1_read_set = {p_0_10};
    partition_column_identifier_set c1_write_set = {};
    partition_column_identifier_set c1_inflight_set = {};
    transaction_partition_holder*   c1_holder = db.get_partitions_with_begin(
        c1, c1_state, c1_write_set, c1_read_set, c1_inflight_set,
        partition_lookup_operation::GET );
    EXPECT_EQ( c1_holder, nullptr );

    // now add some partitions
    db.add_partition( c1, p_0_10, site_loc, p_type, s_type );
    db.add_partition( c2, p_11_15, site_loc, p_type, s_type );
    db.add_partition( c1, p_16_20, site_loc, p_type, s_type );

    // even though it's already there this should be okay
    db.add_partition( c1, p_0_10, site_loc, p_type, s_type );

    // read only that won't see anything
    // begin
    c1_state = empty_version;
    c1_read_set = {p_0_10};
    c1_write_set = {};
    c1_holder = db.get_partitions_with_begin( c1, c1_state, c1_write_set,
                                              c1_read_set, c1_inflight_set,
                                              partition_lookup_operation::GET );
    // this should have worked
    EXPECT_NE( c1_holder, nullptr );

    cell_identifier cid;
    cid.table_id_ = order_table_id;
    cid.col_id_ = col_start;
    cid.key_ = 0;

    cid.key_ = 1;
    auto read_r = c1_holder->get_int64_data( cid );
    EXPECT_FALSE( std::get<0>( read_r ) );
    // commit
    c1_state = c1_holder->commit_transaction();
    delete c1_holder;
    EXPECT_EQ( 0, get_snapshot_version( c1_state, p_0_10 ) );

    // write transaction that aborts
    snapshot_vector                 c2_state = empty_version;
    partition_column_identifier_set c2_read_set = {};
    partition_column_identifier_set c2_write_set = {p_0_10, p_16_20};
    partition_column_identifier_set c2_inflight_set = {};
    transaction_partition_holder*   c2_holder = db.get_partitions_with_begin(
        c2, c2_state, c2_write_set, c2_read_set, c2_inflight_set,
        partition_lookup_operation::GET );
    EXPECT_NE( c2_holder, nullptr );

    cid.key_ = 10;
    bool write_ok = c2_holder->insert_int64_data( cid, 0 );
    EXPECT_TRUE( write_ok );
    c2_state = c2_holder->abort_transaction();
    delete c2_holder;
    EXPECT_EQ( empty_version, c2_state );

    // write transaction that does something
    c2_holder = db.get_partitions_with_begin( c2, c2_state, c2_write_set,
                                              c2_read_set, c2_inflight_set,
                                              partition_lookup_operation::GET );
    EXPECT_NE( c2_holder, nullptr );

    cid.key_ = 1;
    write_ok = c2_holder->insert_int64_data( cid, 1 );
    EXPECT_TRUE( write_ok );
    cid.key_ = 20;
    write_ok = c2_holder->insert_int64_data( cid, 20 );
    EXPECT_TRUE( write_ok );
    // commit
    c2_state = c2_holder->commit_transaction();
    delete c2_holder;

    snapshot_vector commit_1_version;
    set_snapshot_version( commit_1_version, p_0_10, 1 );
    set_snapshot_version( commit_1_version, p_16_20, 1 );
    EXPECT_EQ( commit_1_version, c2_state );

    // read should see change
    c1_read_set = {p_0_10};
    c1_holder = db.get_partitions_with_begin( c1, c2_state, c1_write_set,
                                              c1_read_set, c1_inflight_set,
                                              partition_lookup_operation::GET );
    EXPECT_NE( c2_holder, nullptr );

    cid.key_ = 1;
    read_r = c1_holder->get_int64_data( cid );
    EXPECT_TRUE( std::get<0>( read_r ) );
    EXPECT_EQ( 1, std::get<1>( read_r ) );
    // commit
    c1_state = c1_holder->commit_transaction();
    delete c1_holder;
    EXPECT_EQ( commit_1_version, c1_state );
}
TEST_F( db_test, db_txn_ops_row ) { db_txn_ops( partition_type::type::ROW ); }
TEST_F( db_test, db_txn_ops_col ) {
    db_txn_ops( partition_type::type::COLUMN );
}
TEST_F( db_test, db_txn_ops_sorted_col ) {
    db_txn_ops( partition_type::type::SORTED_COLUMN );
}
TEST_F( db_test, db_txn_ops_multi_col ) {
    db_txn_ops( partition_type::type::MULTI_COLUMN );
}
TEST_F( db_test, db_txn_ops_sorted_multi_col ) {
    db_txn_ops( partition_type::type::SORTED_MULTI_COLUMN );
}


void db_change_partition( const partition_type::type& p_type ) {
    uint32_t site_loc = 1;
    uint32_t num_tables = 5;
    uint32_t num_clients = 10;
    int32_t num_records_in_chain = k_num_records_in_chain;
    int32_t  num_records_in_snapshot_chain = k_num_records_in_snapshot_chain;
    uint32_t gc_sleep_time = 10;

    uint32_t c1 = 1;
    uint32_t c2 = 2;

    snapshot_vector       empty_version;

    auto update_dest_generator = make_no_op_update_destination_generator();
    auto prop_config =
        update_dest_generator->get_propagation_configurations().at( 0 );

    db db;
    db.init( update_dest_generator, make_update_enqueuers(),
             create_tables_metadata(
                 num_tables, site_loc, num_clients, gc_sleep_time,
                 false /* enable sec storage */, "/tmp/" ) );

    uint32_t                    col_start = 0;
    uint32_t                    col_end = 0;
    std::vector<cell_data_type> col_types = {cell_data_type::INT64};

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;

    table_metadata order_meta = create_table_metadata(
        "orders", 0, col_end + 1, col_types, num_records_in_chain,
        num_records_in_snapshot_chain, 0 /*site*/,
        10 /*default partition size*/, col_end + 1,
        10 /*default partition trackign size*/, col_end + 1, p_type, s_type,
        false /* enable sec storage */, "/tmp/" );

    uint32_t order_table_id = db.get_tables()->create_table( order_meta );
    EXPECT_EQ( 0, order_table_id );

    // add some partitions
    partition_column_identifier p_0_10 = create_partition_column_identifier(
        order_table_id, 0, 10, col_start, col_end );
    partition_column_identifier p_11_15 = create_partition_column_identifier(
        order_table_id, 11, 15, col_start, col_end );

    db.add_partition( c1, p_0_10, site_loc, p_type, s_type );
    db.add_partition( c2, p_11_15, site_loc, p_type, s_type );

    // insert transaction
    snapshot_vector               c2_state = empty_version;
    partition_column_identifier_set      c2_read_set = {};
    partition_column_identifier_set      c2_write_set = {p_0_10, p_11_15};
    partition_column_identifier_set      c2_inflight_set = {};
    transaction_partition_holder* c2_holder = db.get_partitions_with_begin(
        c2, c2_state, c2_write_set, c2_read_set, c2_inflight_set,
        partition_lookup_operation::GET );
    EXPECT_NE( c2_holder, nullptr );

    cell_identifier cid;
    cid.table_id_ = order_table_id;
    cid.col_id_ = col_start;
    cid.key_ = 0;

    for (uint64_t rec = 0; rec <= 15; rec++) {
        cid.key_ = rec;
        bool insert_ok = c2_holder->insert_int64_data( cid, rec );
        EXPECT_TRUE( insert_ok );
    }
    // commit
    c2_state = c2_holder->commit_transaction();
    delete c2_holder;

    snapshot_vector commit_1_version;
    set_snapshot_version( commit_1_version, p_0_10, 1 );
    set_snapshot_version( commit_1_version, p_11_15, 1 );
    EXPECT_EQ( commit_1_version, c2_state );

    // split partition
    partition_column_identifier p_0_5 = create_partition_column_identifier(
        order_table_id, 0, 5, col_start, col_end );
    partition_column_identifier p_6_10 = create_partition_column_identifier(
        order_table_id, 6, 10, col_start, col_end );

    std::vector<propagation_configuration> split_configs = {prop_config,
                                                            prop_config};
    auto split_result =
        db.split_partition( c2, c2_state, p_0_10, 6, k_unassigned_col, p_type,
                            p_type, s_type, s_type, split_configs );
    EXPECT_TRUE( std::get<0>( split_result ) );
    snapshot_vector split_vector = std::get<1>( split_result );
    snapshot_vector expected_split_vector = c2_state;
    set_snapshot_version( expected_split_vector, p_0_10, 2 );
    set_snapshot_version( expected_split_vector, p_0_5, 2 );
    set_snapshot_version( expected_split_vector, p_6_10, 2 );
    EXPECT_EQ( expected_split_vector, split_vector );

    c2_read_set = {p_0_5, p_6_10};
    c2_write_set = {};
    c2_state = split_vector;
    c2_holder = db.get_partitions_with_begin( c2, c2_state, c2_write_set,
                                              c2_read_set, c2_inflight_set,
                                              partition_lookup_operation::GET );
    EXPECT_NE( c2_holder, nullptr );

    for( uint64_t rec = 0; rec < 10; rec++ ) {
        cid.key_ = rec;
        auto read_r = c2_holder->get_int64_data( cid );
        EXPECT_TRUE( std::get<0>( read_r ) );
        EXPECT_EQ( rec, std::get<1>( read_r ) );
    }
    c2_state = c2_holder->commit_transaction();
    delete c2_holder;
    EXPECT_EQ( expected_split_vector, c2_state );

    // merge partition
    partition_column_identifier p_6_15 = create_partition_column_identifier(
        order_table_id, 6, 15, col_start, col_end );
    auto merge_result = db.merge_partition( c2, c2_state, p_6_10, p_11_15,
                                            p_type, s_type, prop_config );
    EXPECT_TRUE( std::get<0>( merge_result ) );
    snapshot_vector merge_vector = std::get<1>( merge_result );
    snapshot_vector expected_merge_vector = expected_split_vector;
    set_snapshot_version( expected_merge_vector, p_6_10, 3 );
    set_snapshot_version( expected_merge_vector, p_11_15, 2 );
    set_snapshot_version( expected_merge_vector, p_6_15, 3 );
    EXPECT_EQ( expected_merge_vector, merge_vector );

    c2_state = merge_vector;
    c2_read_set = {p_6_15};
    c2_write_set = {};
    c2_holder = db.get_partitions_with_begin( c2, c2_state, c2_write_set,
                                              c2_read_set, c2_inflight_set,
                                              partition_lookup_operation::GET );
    EXPECT_NE( c2_holder, nullptr );

    for( uint64_t rec = 6; rec < 15; rec++ ) {
        cid.key_ = rec;
        auto read_r = c2_holder->get_int64_data( cid );
        EXPECT_TRUE( std::get<0>( read_r ) );
        EXPECT_EQ( rec, std::get<1>( read_r ) );
    }
    c2_state = c2_holder->commit_transaction();
    delete c2_holder;
    EXPECT_EQ( expected_merge_vector, c2_state );

    // now we can remaster
    auto remaster_ret = db.remaster_partitions(
        c2, c2_state, {p_0_5}, {}, 2, false, partition_lookup_operation::GET );
    EXPECT_TRUE( std::get<0>( remaster_ret ) );
    snapshot_vector remaster_version = std::get<1>( remaster_ret );

    snapshot_vector expected_remaster_version = c2_state;
    set_snapshot_version( expected_remaster_version, p_0_5, 3 );
    EXPECT_EQ( remaster_version, expected_remaster_version );

    c2_write_set = {p_0_5};
    c2_read_set = {};
    c2_holder = db.get_partitions_with_begin( c2, c2_state, c2_write_set,
                                              c2_read_set, c2_inflight_set,
                                              partition_lookup_operation::GET );
    EXPECT_NE( c2_holder, nullptr );

    EXPECT_FALSE(
        c2_holder->does_site_master_write_set( db.get_site_location() ) );
    c2_state = c2_holder->abort_transaction();
    delete c2_holder;
}
TEST_F( db_test, db_change_partition_row ) {
    db_change_partition( partition_type::type::ROW );
}
TEST_F( db_test, db_change_partition_col ) {
    db_change_partition( partition_type::type::COLUMN );
}
TEST_F( db_test, db_change_partition_sorted_col ) {
    db_change_partition( partition_type::type::SORTED_COLUMN );
}
TEST_F( db_test, db_change_partition_multi_col ) {
    db_change_partition( partition_type::type::MULTI_COLUMN );
}
TEST_F( db_test, db_change_partition_sorted_multi_col ) {
    db_change_partition( partition_type::type::SORTED_MULTI_COLUMN );
}

void db_change_partition_type( const partition_type::type& p_type,
                               const partition_type::type& new_type ) {
    uint32_t site_loc = 1;
    uint32_t num_tables = 5;
    uint32_t num_clients = 10;
    int32_t  num_records_in_chain = k_num_records_in_chain;
    int32_t  num_records_in_snapshot_chain = k_num_records_in_snapshot_chain;
    uint32_t gc_sleep_time = 10;

    uint32_t c1 = 1;
    uint32_t c2 = 2;

    snapshot_vector empty_version;

    auto update_dest_generator = make_no_op_update_destination_generator();
    auto prop_config =
        update_dest_generator->get_propagation_configurations().at( 0 );

    db db;
    db.init( update_dest_generator, make_update_enqueuers(),
             create_tables_metadata(
                 num_tables, site_loc, num_clients, gc_sleep_time,
                 false /* enable sec storage */, "/tmp/" ) );

    uint32_t                    col_start = 0;
    uint32_t                    col_end = 0;
    std::vector<cell_data_type> col_types = {cell_data_type::INT64};

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;

    table_metadata order_meta = create_table_metadata(
        "orders", 0, col_end + 1, col_types, num_records_in_chain,
        num_records_in_snapshot_chain, 0 /*site*/,
        10 /*default partition size*/, col_end + 1,
        10 /*default partition trackign size*/, col_end + 1, p_type, s_type,
        false /* enable sec storage */, "/tmp/" );

    uint32_t order_table_id = db.get_tables()->create_table( order_meta );
    EXPECT_EQ( 0, order_table_id );

    // add some partitions
    partition_column_identifier p_0_10 = create_partition_column_identifier(
        order_table_id, 0, 10, col_start, col_end );
    partition_column_identifier p_11_15 = create_partition_column_identifier(
        order_table_id, 11, 15, col_start, col_end );

    db.add_partition( c1, p_0_10, site_loc, p_type, s_type );
    db.add_partition( c2, p_11_15, site_loc, p_type, s_type );

    // insert transaction
    snapshot_vector                 c2_state = empty_version;
    partition_column_identifier_set c2_read_set = {};
    partition_column_identifier_set c2_write_set = {p_0_10, p_11_15};
    partition_column_identifier_set c2_inflight_set = {};
    transaction_partition_holder*   c2_holder = db.get_partitions_with_begin(
        c2, c2_state, c2_write_set, c2_read_set, c2_inflight_set,
        partition_lookup_operation::GET );
    EXPECT_NE( c2_holder, nullptr );

    cell_identifier cid;
    cid.table_id_ = order_table_id;
    cid.col_id_ = col_start;
    cid.key_ = 0;

    for( uint64_t rec = 0; rec <= 15; rec++ ) {
        cid.key_ = rec;
        bool insert_ok = c2_holder->insert_int64_data( cid, rec );
        EXPECT_TRUE( insert_ok );
    }
    // commit
    c2_state = c2_holder->commit_transaction();
    delete c2_holder;

    snapshot_vector commit_1_version;
    set_snapshot_version( commit_1_version, p_0_10, 1 );
    set_snapshot_version( commit_1_version, p_11_15, 1 );
    EXPECT_EQ( commit_1_version, c2_state );

    std::vector<partition_column_identifier> change_pids = {p_0_10};
    std::vector<partition_type::type>        change_types_vec = {new_type};
    std::vector<storage_tier_type::type> storage_change_types_vec = {s_type};

    snapshot_vector change_version;
    set_snapshot_version( change_version, p_0_10, 1 );

    // change type
    auto change_types = db.change_partition_types(
        c2, change_pids, change_types_vec, storage_change_types_vec );
    EXPECT_TRUE( std::get<0>( change_types ) );
    auto snap = std::get<1>( change_types );
    EXPECT_EQ( change_version, snap );

    c2_read_set = {p_0_10, p_11_15};
    c2_write_set = {};
    c2_holder = db.get_partitions_with_begin( c2, c2_state, c2_write_set,
                                              c2_read_set, c2_inflight_set,
                                              partition_lookup_operation::GET );
    EXPECT_NE( c2_holder, nullptr );

    for( uint64_t rec = 0; rec < 15; rec++ ) {
        cid.key_ = rec;
        auto read_r = c2_holder->get_int64_data( cid );
        EXPECT_TRUE( std::get<0>( read_r ) );
        EXPECT_EQ( rec, std::get<1>( read_r ) );
    }
    c2_state = c2_holder->commit_transaction();
    delete c2_holder;

    c2_write_set = {p_0_10, p_11_15};
    c2_read_set = {p_0_10, p_11_15};
    c2_holder = db.get_partitions_with_begin( c2, c2_state, c2_write_set,
                                              c2_read_set, c2_inflight_set,
                                              partition_lookup_operation::GET );
    EXPECT_NE( c2_holder, nullptr );

    for( uint64_t rec = 0; rec < 15; rec++ ) {
        cid.key_ = rec;
        auto read_r = c2_holder->get_latest_int64_data( cid );
        EXPECT_TRUE( std::get<0>( read_r ) );
        EXPECT_EQ( rec, std::get<1>( read_r ) );

        bool updated = c2_holder->update_uint64_data( cid, rec * 2 );
        EXPECT_TRUE( updated );
    }

    c2_state = c2_holder->commit_transaction();
    delete c2_holder;

    c2_write_set = {};
    c2_read_set = {p_0_10, p_11_15};
    c2_holder = db.get_partitions_with_begin( c2, c2_state, c2_write_set,
                                              c2_read_set, c2_inflight_set,
                                              partition_lookup_operation::GET );
    EXPECT_NE( c2_holder, nullptr );

    for( uint64_t rec = 0; rec < 15; rec++ ) {
        cid.key_ = rec;
        auto read_r = c2_holder->get_int64_data( cid );
        EXPECT_TRUE( std::get<0>( read_r ) );
        EXPECT_EQ( rec * 2, std::get<1>( read_r ) );
    }

    c2_state = c2_holder->commit_transaction();
    delete c2_holder;
}

TEST_F( db_test, db_change_partition_type_row_row ) {
    db_change_partition_type( partition_type::type::ROW,
                              partition_type::type::ROW );
}
TEST_F( db_test, db_change_partition_type_row_col ) {
    db_change_partition_type( partition_type::type::ROW,
                              partition_type::type::COLUMN );
}
TEST_F( db_test, db_change_partition_type_row_multi_col ) {
    db_change_partition_type( partition_type::type::ROW,
                              partition_type::type::MULTI_COLUMN );
}
TEST_F( db_test, db_change_partition_type_row_sorted_col ) {
    db_change_partition_type( partition_type::type::ROW,
                              partition_type::type::SORTED_COLUMN );
}
TEST_F( db_test, db_change_partition_type_row_sorted_multi_col ) {
    db_change_partition_type( partition_type::type::ROW,
                              partition_type::type::SORTED_MULTI_COLUMN );
}
TEST_F( db_test, db_change_partition_type_col_row ) {
    db_change_partition_type( partition_type::type::COLUMN,
                              partition_type::type::ROW );
}
TEST_F( db_test, db_change_partition_type_col_col ) {
    db_change_partition_type( partition_type::type::COLUMN,
                              partition_type::type::COLUMN );
}
TEST_F( db_test, db_change_partition_type_col_multi_col ) {
    db_change_partition_type( partition_type::type::COLUMN,
                              partition_type::type::MULTI_COLUMN );
}
TEST_F( db_test, db_change_partition_type_col_sorted_col ) {
    db_change_partition_type( partition_type::type::COLUMN,
                              partition_type::type::SORTED_COLUMN );
}
TEST_F( db_test, db_change_partition_type_col_sorted_multi_col ) {
    db_change_partition_type( partition_type::type::COLUMN,
                              partition_type::type::SORTED_MULTI_COLUMN );
}
TEST_F( db_test, db_change_partition_type_sorted_col_row ) {
    db_change_partition_type( partition_type::type::SORTED_COLUMN,
                              partition_type::type::ROW );
}
TEST_F( db_test, db_change_partition_type_sorted_col_col ) {
    db_change_partition_type( partition_type::type::SORTED_COLUMN,
                              partition_type::type::COLUMN );
}
TEST_F( db_test, db_change_partition_type_sorted_col_multi_col ) {
    db_change_partition_type( partition_type::type::SORTED_COLUMN,
                              partition_type::type::MULTI_COLUMN );
}
TEST_F( db_test, db_change_partition_type_sorted_col_sorted_col ) {
    db_change_partition_type( partition_type::type::SORTED_COLUMN,
                              partition_type::type::SORTED_COLUMN );
}
TEST_F( db_test, db_change_partition_type_sorted_col_sorted_multi_col ) {
    db_change_partition_type( partition_type::type::SORTED_COLUMN,
                              partition_type::type::SORTED_MULTI_COLUMN );
}

TEST_F( db_test, db_change_partition_type_multi_col_row ) {
    db_change_partition_type( partition_type::type::MULTI_COLUMN,
                              partition_type::type::ROW );
}
TEST_F( db_test, db_change_partition_type_multi_col_col ) {
    db_change_partition_type( partition_type::type::MULTI_COLUMN,
                              partition_type::type::COLUMN );
}
TEST_F( db_test, db_change_partition_type_multi_col_multi_col ) {
    db_change_partition_type( partition_type::type::MULTI_COLUMN,
                              partition_type::type::MULTI_COLUMN );
}
TEST_F( db_test, db_change_partition_type_multi_col_sorted_col ) {
    db_change_partition_type( partition_type::type::MULTI_COLUMN,
                              partition_type::type::SORTED_COLUMN );
}
TEST_F( db_test, db_change_partition_type_multi_col_sorted_multi_col ) {
    db_change_partition_type( partition_type::type::MULTI_COLUMN,
                              partition_type::type::SORTED_MULTI_COLUMN );
}
TEST_F( db_test, db_change_partition_type_sorted_multi_col_row ) {
    db_change_partition_type( partition_type::type::SORTED_MULTI_COLUMN,
                              partition_type::type::ROW );
}
TEST_F( db_test, db_change_partition_type_sorted_multi_col_col ) {
    db_change_partition_type( partition_type::type::SORTED_MULTI_COLUMN,
                              partition_type::type::COLUMN );
}
TEST_F( db_test, db_change_partition_type_sorted_multi_col_multi_col ) {
    db_change_partition_type( partition_type::type::SORTED_MULTI_COLUMN,
                              partition_type::type::MULTI_COLUMN );
}
TEST_F( db_test, db_change_partition_type_sorted_multi_col_sorted_col ) {
    db_change_partition_type( partition_type::type::SORTED_MULTI_COLUMN,
                              partition_type::type::SORTED_COLUMN );
}
TEST_F( db_test, db_change_partition_type_sorted_multi_col_sorted_multi_col ) {
    db_change_partition_type( partition_type::type::SORTED_MULTI_COLUMN,
                              partition_type::type::SORTED_MULTI_COLUMN );
}


#if 0  // MTODO
lock_l void db_gc( table_records_type records_type ) {
    uint32_t site_loc = 0;
    uint32_t num_tables = 1;
    uint32_t num_clients = 1;
    uint32_t num_sites = 1;
    int32_t num_records_in_chain = k_num_records_in_chain;

    uint32_t c0 = 0;

    client_version_vector session = {0};

    db<L, no_op_propagator, no_op_applier<L>> db;
    db.init( site_loc, num_tables, num_clients, num_sites );
    add_no_op_propagators<L>( &db );

    uint32_t order_table_id = db.get_tables()->create_table(
        "orders", 5, 5, num_records_in_chain, records_type );
    EXPECT_EQ( 0, order_table_id );

    EXPECT_EQ( 5, db.get_tables()->get_total_num_records() );
    // rounding
    EXPECT_EQ( 3, db.compute_gc_sleep_time( 10000 /* tps*/,
                                            10 /*num records in chain*/,
                                            3 /*gc threshold*/ ) );
    // more space means more time between GC
    EXPECT_EQ( 9, db.compute_gc_sleep_time( 10000 /* tps*/,
                                            10 /*num records in chain*/,
                                            9 /*gc threshold*/ ) );
    // lower TPS means less GC
    EXPECT_EQ( 30, db.compute_gc_sleep_time( 1000 /* tps*/,
                                             10 /*num records in chain*/,
                                             3 /*gc threshold*/ ) );

    db.init_garbage_collector( 0 /* never sleep*/, 0 /* priority*/ );

    // fill up the chain
    record_identifier rid = {order_table_id, 1};
    db.begin_write_transaction( c0, session, {rid} );
    record* r = db.insert_record( c0, rid );
    EXPECT_NE( r, nullptr );
    // store the version (1) in the record
    r->get_record().set_as_int( 1 );
    session = db.commit_transaction( c0 );
    for( int32_t pos = 2; pos <= num_records_in_chain; pos++ ) {
        db.begin_write_transaction( c0, session, {rid} );
        r = db.read_latest_record( c0, rid );
        EXPECT_NE( r, nullptr );
        EXPECT_EQ( pos - 1, r->get_record().get_as_int() );
        r = db.write_record( c0, rid );
        EXPECT_NE( r, nullptr );
        r->get_record().set_as_int( pos );
        session = db.commit_transaction( c0 );

        // now outside of the session perform reads on the table
        for( int32_t read_pos = 1; read_pos <= pos; read_pos++ ) {
            r = db.get_tables()->read_record( rid, {(uint64_t) read_pos} );
            EXPECT_NE( r, nullptr );
            EXPECT_EQ( read_pos, r->get_record().get_as_int() );
        }
    }

    // The next write will put us in a new chain.
    // Write an entire chain
    for( int32_t pos = 1; pos <= num_records_in_chain; pos++ ) {
        db.begin_write_transaction( c0, session, {rid} );
        r = db.read_latest_record( c0, rid );
        EXPECT_NE( r, nullptr );
        r = db.write_record( c0, rid );
        EXPECT_NE( r, nullptr );
        r->get_record().set_as_int( pos );
        session = db.commit_transaction( c0 );
    }

    // This write goes into another chain, which should allow the chain 2 chains
    // back to be GC'd.
    db.begin_write_transaction( c0, session, {rid} );
    r = db.write_record( c0, rid );
    EXPECT_NE( r, nullptr );
    r->get_record().set_as_int( k_num_records_in_chain );
    session = db.commit_transaction( c0 );

    // we don't know when the GC will update to the lwm will occur so all we can
    // do is sleep for a bit, and hope that the right thing happens
    std::this_thread::sleep_for( std::chrono::milliseconds( 50 ) );
    // this should trigger the actual gc

    db.begin_write_transaction( c0, session, {rid} );
    r = db.write_record( c0, rid );
    EXPECT_NE( r, nullptr );
    r->get_record().set_as_int( num_records_in_chain + 1);
    session = db.commit_transaction( c0 );

    r = db.get_tables()->read_record( rid,
                                      {(uint64_t) num_records_in_chain / 2} );
    EXPECT_EQ( nullptr, r );
}
#endif

void db_update_propagation( const partition_type::type& p_type ) {
    uint32_t site_loc_1 = 1;
    uint32_t site_loc = 0;
    uint32_t num_tables = 1;
    uint32_t num_clients = 2;
    int32_t  num_records_in_chain = k_num_records_in_chain;
    int32_t  num_records_in_snapshot_chain = k_num_records_in_snapshot_chain;
    uint32_t gc_sleep_time = 10;

    uint32_t c1 = 1;

    std::shared_ptr<vector_update_destination> update_dest =
        std::make_shared<vector_update_destination>( 0 );
    auto prop_config = update_dest->get_propagation_configuration();

    std::shared_ptr<update_destination_generator> generator =
        std::make_shared<update_destination_generator>(
            construct_update_destination_generator_configs( 1 ) );
    generator->add_update_destination( update_dest );

    db db;
    db.init( generator, make_update_enqueuers(),
             create_tables_metadata(
                 num_tables, site_loc, num_clients, gc_sleep_time,
                 false /* enable sec storage */, "/tmp/" ) );

    uint32_t                    col_start = 0;
    uint32_t                    col_end = 0;
    std::vector<cell_data_type> col_types = {cell_data_type::INT64};

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;

    table_metadata order_meta = create_table_metadata(
        "orders", 0, col_end + 1, col_types, num_records_in_chain,
        num_records_in_snapshot_chain, 0 /*site*/,
        10 /*default partition size*/, col_end + 1,
        10 /*default partition trackign size*/, col_end + 1, p_type, s_type,
        false /* enable sec storage */, "/tmp/" );
    uint32_t order_table_id = db.get_tables()->create_table( order_meta );
    EXPECT_EQ( 0, order_table_id );

    // add some partitions
    partition_column_identifier p_0_10 = create_partition_column_identifier(
        order_table_id, 0, 10, col_start, col_end );
    partition_column_identifier p_11_15 = create_partition_column_identifier(
        order_table_id, 11, 15, col_start, col_end );

    db.add_partition( c1, p_0_10, site_loc, p_type, s_type );
    db.add_partition( c1, p_11_15, site_loc, p_type, s_type );

    // insert transaction
    snapshot_vector               c1_state = {};
    partition_column_identifier_set      c1_read_set = {};
    partition_column_identifier_set      c1_write_set = {p_0_10};
    partition_column_identifier_set      c1_inflight_set = {};
    transaction_partition_holder* c1_holder = db.get_partitions_with_begin(
        c1, c1_state, c1_write_set, c1_read_set, c1_inflight_set,
        partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    write_buffer* insert_buf = new write_buffer();
    cell_identifier cid;
    cid.table_id_ =order_table_id;
    cid.col_id_ = col_start;
    cid.key_ = 0;

    std::vector<row_record*> row_recs;
    std::vector<packed_cell_data*> packed_cells;

    for( uint64_t id = p_0_10.partition_start;
         id <= (uint64_t) p_0_10.partition_end; id++ ) {
        cid.key_ = id;
        bool insert_ok = c1_holder->insert_int64_data( cid, id );
        EXPECT_TRUE( insert_ok );
        db_test_add_to_insert_buf( insert_buf, row_recs, packed_cells, cid, id,
                                   p_type );
    }
    c1_state = c1_holder->commit_transaction();
    delete c1_holder;
    snapshot_vector expected_commit_snap;
    uint64_t        p_0_10_hash = partition_column_identifier_key_hasher{}( p_0_10 );
    set_snapshot_version( expected_commit_snap, p_0_10, 1 );
    EXPECT_EQ( expected_commit_snap, c1_state );
    insert_buf->store_propagation_information( c1_state, p_0_10, p_0_10_hash,
                                               1 );
    serialized_update insert_update = serialize_write_buffer( insert_buf );

    db_test_clear_vectors( row_recs, packed_cells );

    std::vector<serialized_update> expected_serialized_updates;
    expected_serialized_updates.push_back( std::move( insert_update ) );
    delete( insert_buf );

    bool do_match = update_dest->do_stored_and_expected_match(
        expected_serialized_updates );
    EXPECT_TRUE( do_match );

    auto remaster_ret =
        db.remaster_partitions( c1, c1_state, {p_0_10}, {}, site_loc_1, false,
                                partition_lookup_operation::GET );
    EXPECT_TRUE( std::get<0>( remaster_ret ) );
    c1_state = std::get<1>( remaster_ret );

    set_snapshot_version( expected_commit_snap, p_0_10, 2 );
    EXPECT_EQ( expected_commit_snap, c1_state );

    write_buffer*                  remaster_buf = new write_buffer();
    partition_column_operation_identifier remaster_poi;
    remaster_poi.add_partition_op( p_0_10, 0 /*64*/, site_loc_1,
                                   K_REMASTER_OP );
    remaster_buf->add_to_partition_buffer( std::move( remaster_poi ) );
    remaster_buf->store_propagation_information( c1_state, p_0_10, p_0_10_hash,
                                                 1 );
    serialized_update remaster_update = serialize_write_buffer( remaster_buf );
    expected_serialized_updates.push_back( std::move( remaster_update ) );
    delete( remaster_buf );

    do_match = update_dest->do_stored_and_expected_match(
        expected_serialized_updates );
    EXPECT_TRUE( do_match );

    c1_holder = db.get_partitions_with_begin( c1, c1_state, c1_write_set,
                                              c1_read_set, c1_inflight_set,
                                              partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    EXPECT_FALSE( c1_holder->does_site_master_write_set( site_loc ) );
    c1_state = c1_holder->abort_transaction();
    delete c1_holder;

    auto p_0_5 = create_partition_column_identifier( order_table_id, 0, 5,
                                                     col_start, col_end );
    auto p_6_10 = create_partition_column_identifier( order_table_id, 6, 10,
                                                      col_start, col_end );

    set_snapshot_version( expected_commit_snap, p_0_10, 3 );
    set_snapshot_version( expected_commit_snap, p_0_5, 3 );
    set_snapshot_version( expected_commit_snap, p_6_10, 3 );

    // split
    auto split_result = db.split_partition(
        c1, c1_state, p_0_10, 6, k_unassigned_col, p_type, p_type, s_type,
        s_type, {prop_config, prop_config} );
    EXPECT_TRUE( std::get<0>( split_result ) );
    c1_state = std::get<1>( split_result );

    EXPECT_EQ( expected_commit_snap, c1_state );

    // nothign there because of remaster
    do_match = update_dest->do_stored_and_expected_match(
        expected_serialized_updates );
    EXPECT_TRUE( do_match );


    // the next split
    uint64_t p_11_15_hash = partition_column_identifier_key_hasher{}( p_11_15 );
    auto p_11_12 = create_partition_column_identifier( order_table_id, 11, 12,
                                                       col_start, col_end );
    uint64_t p_11_12_hash = partition_column_identifier_key_hasher{}( p_11_12 );
    auto p_13_15 = create_partition_column_identifier( order_table_id, 13, 15,
                                                       col_start, col_end );
    uint64_t p_13_15_hash = partition_column_identifier_key_hasher{}( p_13_15 );

    expected_commit_snap.clear();
    set_snapshot_version( expected_commit_snap, p_11_15, 1 );
    set_snapshot_version( expected_commit_snap, p_11_12, 1 );
    set_snapshot_version( expected_commit_snap, p_13_15, 1 );

    write_buffer* split_buf = new write_buffer();
    partition_column_operation_identifier split_poi;
    split_poi.add_partition_op( p_11_15, 13, k_unassigned_col, K_SPLIT_OP );
    split_buf->add_to_partition_buffer( std::move( split_poi ) );
    split_buf->store_propagation_information( expected_commit_snap, p_11_15,
                                              p_11_15_hash, 1 );

    propagation_configuration prop_1_configs;
    prop_1_configs.type = propagation_type::VECTOR;
    prop_1_configs.partition = 0;
    prop_1_configs.offset = 1;
    update_propagation_information stop_info;
    stop_info.propagation_config_ = prop_1_configs;
    stop_info.identifier_ = p_11_15;
    stop_info.do_seek_ = false;
    split_buf->add_to_subscription_buffer( stop_info, false );
    update_propagation_information start_info;
    prop_1_configs.offset = 2;
    start_info.propagation_config_ = prop_1_configs;
    start_info.identifier_ = p_11_12;
    start_info.do_seek_ = should_seek_topic( start_info, stop_info );
    split_buf->add_to_subscription_buffer( start_info, true );
    start_info.identifier_ = p_13_15;
    start_info.do_seek_ = should_seek_topic( start_info, stop_info );
    split_buf->add_to_subscription_buffer(
        start_info, true );

    serialized_update split_update = serialize_write_buffer( split_buf );
    expected_serialized_updates.push_back( std::move( split_update ) );
    delete( split_buf );

    // split
    snapshot_vector c2_state;
    uint32_t        c2 = 2;
    split_result = db.split_partition(
        c2, c2_state, p_11_15, 13, k_unassigned_col, p_type, p_type, s_type,
        s_type, {prop_1_configs, prop_1_configs} );
    EXPECT_TRUE( std::get<0>( split_result ) );
    c2_state = std::get<1>( split_result );
    EXPECT_EQ( expected_commit_snap, c2_state );

    // should be there because of split
    do_match = update_dest->do_stored_and_expected_match(
        expected_serialized_updates );
    EXPECT_TRUE( do_match );

    // assert partition is there
    c1_read_set = {p_11_12, p_13_15};
    c1_write_set = {};
    c1_holder = db.get_partitions_with_begin( c1, c1_state, c1_write_set,
                                              c1_read_set, c1_inflight_set,
                                              partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );
    c1_holder->abort_transaction();
    delete c1_holder;

    // now do a merge
    set_snapshot_version( expected_commit_snap, p_11_15, 2 );
    set_snapshot_version( expected_commit_snap, p_11_12, 2 );
    set_snapshot_version( expected_commit_snap, p_13_15, 2 );

    // 11_12, 11_15, 13_15
    write_buffer*                  merger_buf_11 = new write_buffer();
    write_buffer*                  merger_buf_13 = new write_buffer();
    write_buffer*                  mergee_buf = new write_buffer();

    start_info.identifier_ = p_11_15;
    stop_info.identifier_ = p_11_12;
    stop_info.propagation_config_.offset = 2;
    stop_info.do_seek_ = false;
    start_info.propagation_config_.offset = 3;
    start_info.do_seek_ = should_seek_topic( start_info, stop_info );

    partition_column_operation_identifier merger_poi_11;
    merger_poi_11.add_partition_op( p_11_15, 13, k_unassigned_col, K_MERGE_OP );
    merger_buf_11->add_to_partition_buffer( std::move( merger_poi_11 ) );
    merger_buf_11->add_to_subscription_buffer( start_info, true );
    merger_buf_11->add_to_subscription_buffer( stop_info, false );
    merger_buf_11->store_propagation_information( expected_commit_snap, p_11_12,
                                                  p_11_12_hash, 2 );
    partition_column_operation_identifier merger_poi_13;
    merger_poi_13.add_partition_op( p_11_15, 13, k_unassigned_col, K_MERGE_OP );
    merger_buf_13->add_to_partition_buffer( std::move( merger_poi_13 ) );
    merger_buf_13->add_to_subscription_buffer( start_info, true );
    stop_info.identifier_ = p_13_15;
    stop_info.do_seek_ = false;
    merger_buf_13->add_to_subscription_buffer( stop_info, false );
    merger_buf_13->store_propagation_information( expected_commit_snap, p_13_15,
                                                  p_13_15_hash, 2 );

    partition_column_operation_identifier mergee_poi;
    mergee_poi.add_partition_op( p_11_15, 13, k_unassigned_col, K_INSERT_OP );
    mergee_buf->add_to_partition_buffer( std::move( mergee_poi ) );
    mergee_buf->store_propagation_information( expected_commit_snap, p_11_15,
                                               p_11_15_hash, 2 );

    serialized_update merger_update_11 =
        serialize_write_buffer( merger_buf_11 );
    expected_serialized_updates.push_back( std::move( merger_update_11 ) );
    delete( merger_buf_11 );

    serialized_update mergee_update = serialize_write_buffer( mergee_buf );
    expected_serialized_updates.push_back( std::move( mergee_update ) );
    delete( mergee_buf );

    serialized_update merger_update_13 =
        serialize_write_buffer( merger_buf_13 );
    expected_serialized_updates.push_back( std::move( merger_update_13 ) );
    delete( merger_buf_13 );

    auto merge_result =
        db.merge_partition( c2, c2_state, p_11_12, p_13_15, p_type, s_type,
                            start_info.propagation_config_ );
    EXPECT_TRUE( std::get<0>( merge_result ) );
    c2_state = std::get<1>( merge_result );

    EXPECT_EQ( expected_commit_snap, c2_state );

    do_match = update_dest->do_stored_and_expected_match(
        expected_serialized_updates );
    EXPECT_TRUE( do_match );
    (void) site_loc_1;
}

TEST_F( db_test, db_update_propagation_row ) {
    db_update_propagation( partition_type::type::ROW );
}
TEST_F( db_test, db_update_propagation_col ) {
    db_update_propagation( partition_type::type::COLUMN );
}
TEST_F( db_test, db_update_propagation_sorted_col ) {
    db_update_propagation( partition_type::type::SORTED_COLUMN );
}
TEST_F( db_test, db_update_propagation_multi_col ) {
    db_update_propagation( partition_type::type::MULTI_COLUMN );
}
TEST_F( db_test, db_update_propagation_sorted_multi_col ) {
    db_update_propagation( partition_type::type::SORTED_MULTI_COLUMN );
}

void db_update_application( const partition_type::type& p_type ) {
    uint32_t site_loc_1 = 1;
    uint32_t site_loc_2 = 2;
    uint32_t site_loc = 0;
    uint32_t num_tables = 1;
    uint32_t num_clients = 2;
    int32_t  num_records_in_chain = k_num_records_in_chain;
    int32_t  num_records_in_snapshot_chain = k_num_records_in_snapshot_chain;
    uint32_t gc_sleep_time = 10;

    uint32_t c1 = 1;

    std::shared_ptr<vector_update_destination> update_dest =
        std::make_shared<vector_update_destination>( 2 );

    std::shared_ptr<update_destination_generator> generator =
        std::make_shared<update_destination_generator>(
            construct_update_destination_generator_configs( 1 ) );
    generator->add_update_destination( update_dest );

    // sleep for 10 ms, if more than 2 updates, then apply 1 update
    update_enqueuer_configs enqueue_configs =
        construct_update_enqueuer_configs( 10, 2, 1 );
    std::shared_ptr<vector_update_source> source_1 =
        std::make_shared<vector_update_source>();
    std::shared_ptr<vector_update_source> source_2 =
        std::make_shared<vector_update_source>();

    auto enqueuer_1 = std::make_shared<update_enqueuer>();
    enqueuer_1->set_update_consumption_source( source_1, enqueue_configs );
    auto enqueuer_2 = std::make_shared<update_enqueuer>();
    enqueuer_2->set_update_consumption_source( source_2, enqueue_configs );

    auto enqueuers = make_update_enqueuers();
    enqueuers->add_enqueuer( enqueuer_1 );
    enqueuers->add_enqueuer( enqueuer_2 );

    propagation_configuration prop_1_configs;
    prop_1_configs.type = propagation_type::VECTOR;
    prop_1_configs.partition = 0;
    propagation_configuration prop_2_configs = prop_1_configs;
    prop_2_configs.partition = 1;

    uint32_t order_table_id = 0;
    uint32_t col_start = 0;
    uint32_t col_end = 0;

    partition_column_identifier p_0_5 = create_partition_column_identifier(
        order_table_id, 0, 5, col_start, col_end );
    uint64_t p_0_5_hash = partition_column_identifier_key_hasher{}( p_0_5 );
    partition_column_identifier p_6_10 = create_partition_column_identifier(
        order_table_id, 6, 10, col_start, col_end );
    uint64_t p_6_10_hash = partition_column_identifier_key_hasher{}( p_6_10 );

    db db;
    db.init( generator, enqueuers,
             create_tables_metadata(
                 num_tables, site_loc, num_clients, gc_sleep_time,
                 false /* enable sec storage */, "/tmp/" ) );

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;

    std::vector<cell_data_type> col_types = {cell_data_type::STRING};
    table_metadata              order_meta = create_table_metadata(
        "orders", 0, col_end + 1, col_types, num_records_in_chain,
        num_records_in_snapshot_chain, 0 /*site*/,
        10 /*default partition size*/, col_end + 1,
        10 /*default partition tracking size*/, col_end + 1, p_type, s_type,
        false /* enable sec storage */, "/tmp/" );
    uint32_t o_table_id = db.get_tables()->create_table( order_meta );
    EXPECT_EQ( 0, o_table_id );

    // only, add the original two sources, the second source should implicitly
    // be added, by the remaster operation
    snapshot_vector snap_add_source;
    enqueuers->add_source( prop_1_configs, p_0_5, snap_add_source,
                           true /* do seek*/, k_add_replica_cause_string );
    enqueuers->add_source( prop_2_configs, p_6_10, snap_add_source,
                           true /*do seek*/, k_add_replica_cause_string );

    db.add_partition( c1, p_0_5, site_loc_1, p_type, s_type );
    db.add_partition( c1, p_6_10, site_loc_2, p_type, s_type );

    // updates:
    // s1: insert 1,2 (0, 1, 0) (s1_insert_buf)
    // s2: insert 6, 7 (0, 0, 1) (s2_insert_buf)
    // s2: remaster 6, 7 (0, 0, 2) (remaster_buf)
    // s1: update 6, 7 (0, 2, 2) (s1_update_buf)
    // s1: update 6, 7 (0, 3, 2) (s1_update_buf_order)

    std::vector<row_record*>       row_recs;
    std::vector<packed_cell_data*> packed_cells;
    // store all the updates we would like

    const char* r_s1_1_val = "foo";
    const char* r_s1_2_val = "abcdefgh";

    write_buffer* s1_insert_buf = new write_buffer();

    cell_identifier cid;
    cid.table_id_ = order_table_id;
    cid.col_id_ = col_start;
    cid.key_ = 0;

    cid.key_ = 1;
    db_test_add_to_insert_buf_str( s1_insert_buf, row_recs, packed_cells, cid,
                                   std::string( r_s1_1_val, 3 ), K_INSERT_OP,
                                   p_type );
    cid.key_ = 2;
    db_test_add_to_insert_buf_str( s1_insert_buf, row_recs, packed_cells, cid,
                                   std::string( r_s1_2_val, 8 ), K_INSERT_OP,
                                   p_type );

    snapshot_vector s1_insert_vv = {};
    set_snapshot_version( s1_insert_vv, p_0_5, 1 );
    s1_insert_buf->store_propagation_information( s1_insert_vv, p_0_5,
                                                  p_0_5_hash, 1 );
    serialized_update s1_insert_update =
        serialize_write_buffer( s1_insert_buf );
    EXPECT_GE( s1_insert_update.length_, get_serialized_update_min_length() );

    db_test_clear_vectors( row_recs, packed_cells );

    source_1->add_update( s1_insert_update );

    const char* r_s2_6_val = "bar";
    const char* r_s2_7_val = "horizondb";

    write_buffer* s2_insert_buf = new write_buffer();

    cid.key_ = 6;
    db_test_add_to_insert_buf_str( s2_insert_buf, row_recs, packed_cells, cid,
                                   std::string( r_s2_6_val, 3 ), K_INSERT_OP,
                                   p_type );
    cid.key_ = 7;
    db_test_add_to_insert_buf_str( s2_insert_buf, row_recs, packed_cells, cid,
                                   std::string( r_s2_7_val, 9 ), K_INSERT_OP,
                                   p_type );

    snapshot_vector s2_insert_vv = {};
    set_snapshot_version( s2_insert_vv, p_6_10, 1 );
    s2_insert_buf->store_propagation_information( s2_insert_vv, p_6_10,
                                                  p_6_10_hash, 1 );
    serialized_update s2_insert_update =
        serialize_write_buffer( s2_insert_buf );
    EXPECT_GE( s2_insert_update.length_, get_serialized_update_min_length() );
    db_test_clear_vectors( row_recs, packed_cells );

    source_2->add_update( s2_insert_update );

    write_buffer* remaster_buf = new write_buffer();

    partition_column_operation_identifier poi_s2_re;
    poi_s2_re.add_partition_op( p_6_10, 0 /*64*/, site_loc_1, K_REMASTER_OP );
    // can nullptr the versioned_records
    remaster_buf->add_to_partition_buffer( std::move( poi_s2_re ) );
    snapshot_vector s2_remaster_vv = {};
    set_snapshot_version( s2_remaster_vv, p_6_10, 2 );
    remaster_buf->store_propagation_information( s2_remaster_vv, p_6_10,
                                                 p_6_10_hash, 2 );

    update_propagation_information old_info;
    old_info.propagation_config_ = prop_2_configs;
    old_info.identifier_ = p_6_10;
    old_info.do_seek_ = false;
    update_propagation_information new_info = old_info;
    new_info.propagation_config_ = prop_1_configs;
    new_info.do_seek_ = should_seek_topic( old_info, new_info );

    remaster_buf->add_to_subscription_switch_buffer( old_info, new_info );

    serialized_update remaster_update = serialize_write_buffer( remaster_buf );
    EXPECT_GE( remaster_update.length_, get_serialized_update_min_length() );
    source_2->add_update( remaster_update );

    // this update is out of order
    const char* r_s1_6_val_order = "bob";
    const char* r_s1_7_val_order = "mast-dyna";

    write_buffer* s1_update_buf_order = new write_buffer();

    cid.key_ = 6;
    db_test_add_to_insert_buf_str( s1_update_buf_order, row_recs, packed_cells,
                                   cid, std::string( r_s1_6_val_order, 3 ),
                                   K_WRITE_OP, p_type );
    cid.key_ = 7;
    db_test_add_to_insert_buf_str( s1_update_buf_order, row_recs, packed_cells,
                                   cid, std::string( r_s1_7_val_order, 9 ),
                                   K_WRITE_OP, p_type );

    snapshot_vector s1_update_buf_order_vv = {};
    set_snapshot_version( s1_update_buf_order_vv, p_6_10, 4 );
    set_snapshot_version( s1_update_buf_order_vv, p_0_5, 1 );

    s1_update_buf_order->store_propagation_information(
        s1_update_buf_order_vv, p_6_10, p_6_10_hash, 4 );
    serialized_update s1_update_order =
        serialize_write_buffer( s1_update_buf_order );
    EXPECT_GE( s1_update_order.length_, get_serialized_update_min_length() );
    source_1->add_update( s1_update_order );

    db_test_clear_vectors( row_recs, packed_cells );

    // finally this update must have a dependency
    const char* r_s1_6_val = "baz";
    const char* r_s1_7_val = "dyna-mast";

    write_buffer* s1_update_buf = new write_buffer();

    cid.key_ = 6;
    db_test_add_to_insert_buf_str( s1_update_buf, row_recs, packed_cells, cid,
                                   std::string( r_s1_6_val, 3 ), K_WRITE_OP,
                                   p_type );
    cid.key_ = 7;
    db_test_add_to_insert_buf_str( s1_update_buf, row_recs, packed_cells, cid,
                                   std::string( r_s1_7_val, 9 ), K_WRITE_OP,
                                   p_type );

    snapshot_vector s1_update_buf_vv = {};
    set_snapshot_version( s1_update_buf_vv, p_6_10, 3 );
    set_snapshot_version( s1_update_buf_vv, p_0_5, 1 );

    s1_update_buf->store_propagation_information( s1_update_buf_vv, p_6_10,
                                                  p_6_10_hash, 3 );
    serialized_update s1_update = serialize_write_buffer( s1_update_buf );
    EXPECT_GE( s1_update.length_, get_serialized_update_min_length() );
    source_1->add_update( s1_update );

    db_test_clear_vectors( row_recs, packed_cells );

    /*
    enqueuer_1.start_enqueuing();
    enqueuer_2.start_enqueuing();
    */

    snapshot_vector c1_state = s1_update_buf_order_vv;
    // set_snapshot_version( c1_state, p_0_5, 0);
    partition_column_identifier_set c1_read_set = {p_0_5, p_6_10};
    partition_column_identifier_set c1_write_set = {p_0_5, p_6_10};
    partition_column_identifier_set c1_inflight_set = {};
    transaction_partition_holder*   c1_holder = db.get_partitions_with_begin(
        c1, c1_state, c1_write_set, c1_read_set, c1_inflight_set,
        partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    cid.key_ = 6;
    auto read_r = c1_holder->get_string_data( cid );
    EXPECT_TRUE( std::get<0>( read_r ) );
    EXPECT_EQ( "bob", std::get<1>( read_r ) );
    cid.key_ = 1;
    read_r = c1_holder->get_string_data( cid );
    EXPECT_TRUE( std::get<0>( read_r ) );
    EXPECT_EQ( "foo", std::get<1>( read_r ) );
    // commit
    c1_state = c1_holder->commit_transaction();
    delete c1_holder;

    std::vector<serialized_update> expected_serialized_updates;
    bool do_match = update_dest->do_stored_and_expected_match(
        expected_serialized_updates );
    EXPECT_TRUE( do_match );

    /*
    enqueuer_1.stop_enqueuing();
    enqueuer_2.stop_enqueuing();
    */

    delete( s1_update_buf_order );
    delete( s1_insert_buf );
    delete( s1_update_buf );
    delete( remaster_buf );
    delete( s2_insert_buf );
}

TEST_F( db_test, db_update_application_row ) {
    db_update_application( partition_type::type::ROW );
}
TEST_F( db_test, db_update_application_col ) {
    db_update_application( partition_type::type::COLUMN );
}
TEST_F( db_test, db_update_application_sorted_col ) {
    db_update_application( partition_type::type::SORTED_COLUMN );
}
TEST_F( db_test, db_update_application_multi_col ) {
    db_update_application( partition_type::type::MULTI_COLUMN );
}
TEST_F( db_test, db_update_application_sorted_multi_col ) {
    db_update_application( partition_type::type::SORTED_MULTI_COLUMN );
}


void db_snapshot(const partition_type::type& p_type ) {
    uint32_t site_loc_1 = 1;
    uint32_t site_loc_2 = 2;
    uint32_t num_tables = 1;
    uint32_t num_clients = 2;
    int32_t  num_records_in_chain = k_num_records_in_chain;
    int32_t  num_records_in_snapshot_chain = k_num_records_in_snapshot_chain;
    uint32_t gc_sleep_time = 10;

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;

    uint32_t c1 = 1;

    std::shared_ptr<vector_update_destination> update_dest_1 =
        std::make_shared<vector_update_destination>( 0 );
    std::shared_ptr<vector_update_destination> update_dest_2 =
        std::make_shared<vector_update_destination>( 0 );

    std::shared_ptr<update_destination_generator> generator_1 =
        std::make_shared<update_destination_generator>(
            construct_update_destination_generator_configs( 1 ) );
    generator_1->add_update_destination( update_dest_1 );
    std::shared_ptr<update_destination_generator> generator_2 =
        std::make_shared<update_destination_generator>(
            construct_update_destination_generator_configs( 1 ) );
    generator_2->add_update_destination( update_dest_2 );

    // sleep for 10 ms, if more than 2 updates, then apply 1 update
    update_enqueuer_configs enqueue_configs =
        construct_update_enqueuer_configs( 10, 2, 1 );
    std::shared_ptr<vector_update_source> source_1 =
        std::make_shared<vector_update_source>();
    std::shared_ptr<vector_update_source> source_2 =
        std::make_shared<vector_update_source>();

    auto enqueuer_1 = std::make_shared<update_enqueuer>();
    enqueuer_1->set_update_consumption_source( source_1, enqueue_configs );
    auto enqueuer_2 = std::make_shared<update_enqueuer>();
    enqueuer_2->set_update_consumption_source( source_2, enqueue_configs );

    auto enqueuers_1 = make_update_enqueuers();
    auto enqueuers_2 = make_update_enqueuers();
    enqueuers_1->add_enqueuer( enqueuer_1 );
    enqueuers_2->add_enqueuer( enqueuer_2 );

    propagation_configuration prop_1_configs;
    prop_1_configs.type = propagation_type::VECTOR;
    prop_1_configs.partition = 0;
    propagation_configuration prop_2_configs = prop_1_configs;
    prop_2_configs.partition = 1;

    uint32_t order_table_id = 0;
    uint32_t col_start =0;
    uint32_t col_end =0;

    partition_column_identifier p_0_5 = create_partition_column_identifier(
        order_table_id, 0, 5, col_start, col_end );
    uint64_t p_0_5_hash = partition_column_identifier_key_hasher{}( p_0_5 );

    db db_1;
    db_1.init( generator_1, enqueuers_1,
               create_tables_metadata(
                   num_tables, site_loc_1, num_clients, gc_sleep_time,
                   false /* enable sec storage */, "/tmp/" ) );
    db db_2;
    db_2.init( generator_2, enqueuers_2,
               create_tables_metadata(
                   num_tables, site_loc_2, num_clients, gc_sleep_time,
                   false /* enable sec storage */, "/tmp/" ) );

    std::vector<cell_data_type> col_types = {cell_data_type::STRING};
    table_metadata              order_meta = create_table_metadata(
        "orders", 0, col_end + 1, col_types, num_records_in_chain,
        num_records_in_snapshot_chain, 0 /*site*/,
        10 /*default partition size*/, col_end + 1,
        10 /*default partition trackign size*/, col_end + 1, p_type, s_type,
        false /* enable sec storage */, "/tmp/" );

    order_table_id = db_1.get_tables()->create_table( order_meta );
    EXPECT_EQ( 0, order_table_id );
    order_table_id = db_2.get_tables()->create_table( order_meta );
    EXPECT_EQ( 0, order_table_id );

    // only, add the original two sources, the second source should implicitly
    // be added, by the remaster operation
    snapshot_vector snap_add_source;
    enqueuers_2->add_source( prop_1_configs, p_0_5, snap_add_source,
                             false /* no need to seek*/,
                             k_add_replica_cause_string );

    db_1.add_partition( c1, p_0_5, site_loc_1, p_type, s_type );

    // insert transaction
    snapshot_vector               c1_state = {};
    partition_column_identifier_set      c1_read_set = {};
    partition_column_identifier_set      c1_write_set = {p_0_5};
    partition_column_identifier_set      c1_inflight_set = {};
    transaction_partition_holder* c1_holder = db_1.get_partitions_with_begin(
        c1, c1_state, c1_write_set, c1_read_set, c1_inflight_set,
        partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );


    std::vector<row_record*> row_recs;
    std::vector<packed_cell_data*> packed_cells;

    write_buffer*     insert_buf = new write_buffer();

    cell_identifier cid;
    cid.table_id_ = order_table_id;
    cid.col_id_ = col_start;
    cid.key_ = 0;

    for( uint64_t id = p_0_5.partition_start;
         id <= (uint64_t) p_0_5.partition_end; id++ ) {
        cid.key_ = id;
        bool insert_ok =
            c1_holder->insert_string_data( cid, uint64_to_string( id ) );
        EXPECT_TRUE( insert_ok );

        db_test_add_to_insert_buf_str( insert_buf, row_recs, packed_cells, cid,
                                       uint64_to_string( id ), K_INSERT_OP,
                                       p_type );
    }
    c1_state = c1_holder->commit_transaction();
    delete c1_holder;

    snapshot_vector expected_commit_snap;
    set_snapshot_version( expected_commit_snap, p_0_5, 1 );
    EXPECT_EQ( expected_commit_snap, c1_state );
    insert_buf->store_propagation_information( c1_state, p_0_5, p_0_5_hash, 1 );
    serialized_update insert_update = serialize_write_buffer( insert_buf );
    db_test_clear_vectors( row_recs, packed_cells );

    std::vector<serialized_update> expected_serialized_updates;
    expected_serialized_updates.push_back( std::move( insert_update ) );
    delete( insert_buf );

    bool do_match = update_dest_1->do_stored_and_expected_match(
        expected_serialized_updates );
    EXPECT_TRUE( do_match );
    clear_serialized_updates( expected_serialized_updates );

    auto snap_ret = db_1.snapshot_partition( c1, p_0_5 );
    EXPECT_TRUE( std::get<0>( snap_ret ) );
    snapshot_partition_column_state snap_state = std::get<1>( snap_ret );

    EXPECT_EQ( snap_state.pcid, p_0_5 );
    EXPECT_EQ( snap_state.session_version_vector, expected_commit_snap );
    EXPECT_EQ( snap_state.master_location, site_loc_1 );
    EXPECT_EQ( 1, snap_state.columns.size() );
    EXPECT_EQ( 0, snap_state.columns.at( 0 ).col_id );
    EXPECT_EQ( data_type::type::STRING, snap_state.columns.at( 0 ).type );
    EXPECT_EQ( 6, snap_state.columns.at( 0 ).keys.size() );
    EXPECT_EQ( 6, snap_state.columns.at( 0 ).data.size() );
    EXPECT_EQ( 1, snap_state.prop_config.offset );


    snap_state.prop_config.offset = 0;
    std::vector<snapshot_partition_column_state> snapshots = {snap_state};
    std::vector<partition_type::type>            p_types = {p_type};
    std::vector<storage_tier_type::type>         s_types = {s_type};

    db_2.add_replica_partitions( c1, snapshots, p_types, s_types );

    c1_read_set = {p_0_5};
    c1_write_set = {};
    c1_holder = db_2.get_partitions_with_begin(
        c1, c1_state, c1_write_set, c1_read_set, c1_inflight_set,
        partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    for( uint64_t id = p_0_5.partition_start;
         id <= (uint64_t) p_0_5.partition_end; id++ ) {
        cid.key_ = id;
        auto read_r = c1_holder->get_string_data( cid );
        EXPECT_TRUE( std::get<0>( read_r ) );
        EXPECT_EQ( id, string_to_uint64( std::get<1>( read_r ) ) );
    }
    c1_state = c1_holder->commit_transaction();
    delete c1_holder;
    EXPECT_EQ( c1_state, expected_commit_snap );

    const char* r_s1_0_val_order = "bob";
    write_buffer* s1_update_buf_order = new write_buffer();

    cid.key_ = 0;
    db_test_add_to_insert_buf_str( s1_update_buf_order, row_recs, packed_cells,
                                   cid, std::string( r_s1_0_val_order, 3 ),
                                   K_WRITE_OP, p_type );

    snapshot_vector s1_update_buf_order_vv = {};
    set_snapshot_version( s1_update_buf_order_vv, p_0_5, 2 );

    s1_update_buf_order->store_propagation_information(
        s1_update_buf_order_vv, p_0_5, p_0_5_hash, 2 );
    serialized_update s1_update_order =
        serialize_write_buffer( s1_update_buf_order );
    EXPECT_GE( s1_update_order.length_, get_serialized_update_min_length() );
    source_2->add_update( s1_update_order );
    delete s1_update_buf_order;

    db_test_clear_vectors( row_recs, packed_cells );

    c1_holder = db_2.get_partitions_with_begin(
        c1, s1_update_buf_order_vv, c1_write_set, c1_read_set, c1_inflight_set,
        partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    cid.key_ = 0;
    auto read_r = c1_holder->get_string_data( cid );
    EXPECT_TRUE( std::get<0>( read_r ) );
    EXPECT_EQ( "bob", std::get<1>( read_r ) );
    c1_state = c1_holder->commit_transaction();
    delete c1_holder;
    EXPECT_EQ( c1_state, s1_update_buf_order_vv );
}
TEST_F( db_test, db_snapshot_row ) { db_snapshot( partition_type::type::ROW ); }
TEST_F( db_test, db_snapshot_col ) {
    db_snapshot( partition_type::type::COLUMN );
}
TEST_F( db_test, db_snapshot_sorted_col ) {
    db_snapshot( partition_type::type::SORTED_COLUMN );
}
TEST_F( db_test, db_snapshot_multi_col ) {
    db_snapshot( partition_type::type::MULTI_COLUMN );
}
TEST_F( db_test, db_snapshot_sorted_multi_col ) {
    db_snapshot( partition_type::type::SORTED_MULTI_COLUMN );
}


void add_to_nfl( std::vector<int64_t>& numbers, std::vector<std::string>& names,
                 std::vector<std::string>& cats, int64_t number,
                 const std::string& name, const std::string& cat ) {
    numbers.emplace_back( number );
    names.emplace_back( name );
    cats.emplace_back( cat );
}
void db_test_clear_predicate( predicate_chain& predicate ) {
    predicate.and_predicates.clear();
    predicate.or_predicates.clear();
}
bool check_scan( const std::vector<result_tuple>&    result_tuples,
                 const std::unordered_set<uint64_t>& expected_keys,
                 const std::vector<uint32_t>&        expected_cols,
                 const std::vector<int64_t>&         numbers,
                 const std::vector<std::string>&     names,
                 const std::vector<std::string>&     cats ) {
    std::unordered_set<std::tuple<uint64_t, uint32_t>> expected_tuples;
    for( uint64_t key : expected_keys ) {
        for( uint64_t col : expected_cols ) {
            expected_tuples.emplace( std::make_tuple<>( key, col ) );
        }
    }

    for( const auto& res : result_tuples ) {
        uint64_t row_id = res.row_id;
        EXPECT_LT( row_id, names.size() );
        if( row_id >= names.size() ) {
            DVLOG( 40 ) << "Ret false";
            return false;
        }
        for( const auto res_cell : res.cells ) {
            uint32_t col_id = res_cell.col_id;
            std::tuple<uint64_t, uint32_t> res_tuple =
                std::make_tuple<>( row_id, col_id );
            const auto found = expected_tuples.find( res_tuple );
            EXPECT_NE( found, expected_tuples.end() );
            if( found == expected_tuples.end() ) {
                DVLOG( 40 ) << "Ret false: row:" << row_id
                            << ", col_id:" << col_id << ", not found";
                return false;
            }
            expected_tuples.erase( found );

            EXPECT_TRUE( res_cell.present );
            if( !res_cell.present ) {
                DVLOG( 40 ) << "Ret false";
                return false;
            }

            if( col_id == 0 ) {
                EXPECT_EQ( res_cell.type, data_type::type::INT64 );
                if( res_cell.type != data_type::type::INT64 ) {
                    DVLOG( 40 ) << "Ret false";
                    return false;
                }
                int64_t data = string_to_int64( res_cell.data );
                EXPECT_EQ( data, numbers.at( row_id ) );
                if( data != numbers.at( row_id ) ) {
                    DVLOG( 40 ) << "Ret false";
                    return false;
                }
            } else if( col_id == 1 ) {
                EXPECT_EQ( res_cell.type, data_type::type::STRING );
                if( res_cell.type != data_type::type::STRING ) {
                    DVLOG( 40 ) << "Ret false";
                    return false;
                }
                EXPECT_EQ( res_cell.data, names.at( row_id ) );
                if( res_cell.data != names.at( row_id ) ) {
                    DVLOG( 40 ) << "Ret false";
                    return false;
                }

            } else if( col_id == 2 ) {
                EXPECT_EQ( res_cell.type, data_type::type::STRING );
                if( res_cell.type != data_type::type::STRING ) {
                    DVLOG( 40 ) << "Ret false";
                    return false;
                }
                EXPECT_EQ( res_cell.data, cats.at( row_id ) );
                if( res_cell.data != cats.at( row_id ) ) {
                    DVLOG( 40 ) << "Ret false";
                    return false;
                }

            } else {
                EXPECT_GE( col_id, 0 );
                EXPECT_LE( col_id, 2 );
                DVLOG( 40 ) << "Ret false:" << col_id;
                return false;
            }
        }
    }

    EXPECT_TRUE( expected_tuples.empty() );
    if( !expected_tuples.empty() ) {
        DVLOG( 40 ) << "Ret false, empty tuples, tuples left are:";
        for( const auto& tuple : expected_tuples ) {
            DVLOG( 40 ) << "Tuple left ( " << std::get<0>( tuple ) << ", "
                        << std::get<1>( tuple ) << " )";
        }
        DVLOG( 40 ) << "Ret false, empty tuples";
        return false;
    }

    DVLOG( 40 ) << "Ret true";
    return true;
}
bool check_storage_tier_changes(
    const partition_column_identifier_map_t<storage_tier_type::type>& changes,
    const std::vector<partition_column_identifier>&                   pids,
    const std::vector<storage_tier_type::type>&                       types ) {
    EXPECT_EQ( pids.size(), types.size() );
    if( pids.size() != types.size() ) {
        return false;
    }
    EXPECT_EQ( pids.size(), changes.size() );
    if( changes.size() != pids.size() ) {
        return false;
    }

    for( uint32_t pos = 0; pos < pids.size(); pos++ ) {
        auto found = changes.find( pids.at( pos ) );
        EXPECT_NE( found, changes.end() );
        if( found == changes.end() ) {
            return false;
        }
        EXPECT_EQ( found->second, types.at( pos ) );
        if( found->second != types.at( pos ) ) {
            return false;
        }
    }

    return true;
}

std::tuple<bool, int64_t, uint64_t> simple_number_mapper(
    const result_tuple& res_tuple, const uint64_t& probe ) {
    bool     found = false;
    int64_t  res = 0;
    uint64_t count = 0;

    for( const auto& res_cell : res_tuple.cells ) {
        if( ( res_cell.type == data_type::type::INT64 ) and res_cell.present and
            ( res_cell.col_id == 0 ) ) {
            found = true;
            int64_t data = string_to_int64( res_cell.data );
            res = data;
            count += 1;
        }
    }

    return std::make_tuple<>( found, res, count );
}
uint64_t simple_number_reducer( const std::vector<uint64_t>& in,
                                const uint64_t&              probe ) {
    uint64_t sum = 0;
    for( const auto& i : in ) {
        sum += i;
    }
    return sum;
}

void number_mapper( const std::vector<result_tuple>& res_tuples,
                    std::unordered_map<int64_t, uint64_t, std::hash<int64_t>,
                                       std::equal_to<int64_t>>& res,
                    const uint64_t&                             probe ) {
    for( const auto& res_tuple : res_tuples ) {
        auto entry = simple_number_mapper( res_tuple, probe );
        if( !std::get<0>( entry ) ) {
            continue;
        }
        res[std::get<1>( entry )] += std::get<2>( entry );
    }
}

void number_reducer(
    const std::unordered_map<int64_t, std::vector<uint64_t>, std::hash<int64_t>,
                             std::equal_to<int64_t>>& input,
    std::unordered_map<int64_t, uint64_t, std::hash<int64_t>,
                       std::equal_to<int64_t>>& res,
    const uint64_t&                             probe ) {
    for( const auto& entry : input ) {
        uint64_t red = simple_number_reducer( entry.second, probe );
        res[entry.first] = red;
    }
}

void db_single_col_scan_test( const partition_type::type& p_type ) {
    uint32_t site_loc = 1;
    uint32_t num_tables = 5;
    uint32_t num_clients = 10;
    int32_t  num_records_in_chain = k_num_records_in_chain;
    int32_t  num_records_in_snapshot_chain = k_num_records_in_snapshot_chain;
    uint32_t gc_sleep_time = 10;

    uint32_t c1 = 1;
    uint32_t c2 = 2;

    snapshot_vector empty_version;

    db db;
    db.init( make_no_op_update_destination_generator(), make_update_enqueuers(),
             create_tables_metadata(
                 num_tables, site_loc, num_clients, gc_sleep_time,
                 false /* enable sec storage */, "/tmp/" ) );

    uint32_t                    col_start = 0;
    uint32_t                    col_end = 0;
    std::vector<cell_data_type> col_types = {cell_data_type::INT64};

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;

    table_metadata order_meta = create_table_metadata(
        "orders", 0, col_end + 1, col_types, num_records_in_chain,
        num_records_in_snapshot_chain, 0 /*site*/,
        10 /*default partition size*/, col_end + 1,
        10 /*default partition trackign size*/, col_end + 1, p_type, s_type,
        false /* enable sec storage */, "/tmp/" );
    uint32_t order_table_id = db.get_tables()->create_table( order_meta );
    EXPECT_EQ( 0, order_table_id );

    partition_column_identifier p_0_10 = create_partition_column_identifier(
        order_table_id, 0, 10, col_start, col_end );
    partition_column_identifier p_11_15 = create_partition_column_identifier(
        order_table_id, 11, 15, col_start, col_end );
    partition_column_identifier p_16_20 = create_partition_column_identifier(
        order_table_id, 16, 20, col_start, col_end );

    snapshot_vector                 c1_state = empty_version;
    partition_column_identifier_set c1_read_set = {p_0_10};
    partition_column_identifier_set c1_write_set = {};
    partition_column_identifier_set c1_inflight_set = {};
    transaction_partition_holder*   c1_holder = db.get_partitions_with_begin(
        c1, c1_state, c1_write_set, c1_read_set, c1_inflight_set,
        partition_lookup_operation::GET );
    EXPECT_EQ( c1_holder, nullptr );
    c1_read_set.clear();

    // now add some partitions
    db.add_partition( c1, p_0_10, site_loc, p_type, s_type );
    db.add_partition( c2, p_11_15, site_loc, p_type, s_type );
    db.add_partition( c1, p_16_20, site_loc, p_type, s_type );

    c1_write_set = {p_0_10, p_11_15, p_16_20};
    c1_holder = db.get_partitions_with_begin( c1, c1_state, c1_write_set,
                                              c1_read_set, c1_inflight_set,
                                              partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    std::vector<int64_t>     numbers;
    std::vector<std::string> names;
    std::vector<std::string> cats;

    std::string qb_str = "QB";
    std::string rb_wr_str = "RB_WR";
    std::string def_str = "DEF";

    add_to_nfl( numbers, names, cats, 3, "wilson", qb_str );    // 0
    add_to_nfl( numbers, names, cats, 1, "newton", qb_str );    // 1
    add_to_nfl( numbers, names, cats, 16, "goff", qb_str );     // 2
    add_to_nfl( numbers, names, cats, 12, "brady", qb_str );    // 3
    add_to_nfl( numbers, names, cats, 12, "rodgers", qb_str );  // 4
    add_to_nfl( numbers, names, cats, 4, "watson", qb_str );    // 5
    add_to_nfl( numbers, names, cats, 4, "prescott", qb_str );  // 6
    add_to_nfl( numbers, names, cats, 1, "murray", qb_str );    // 7
    add_to_nfl( numbers, names, cats, 9, "brees", qb_str );     // 8
    add_to_nfl( numbers, names, cats, 9, "stafford", qb_str );  // 9
    add_to_nfl( numbers, names, cats, 4, "carr", qb_str );      // 10

    add_to_nfl( numbers, names, cats, 16, "lockett", rb_wr_str );     // 11
    add_to_nfl( numbers, names, cats, 10, "hill", rb_wr_str );        // 12
    add_to_nfl( numbers, names, cats, 87, "kelce", rb_wr_str );       // 13
    add_to_nfl( numbers, names, cats, 87, "gronkowski", rb_wr_str );  // 14
    add_to_nfl( numbers, names, cats, 22, "henry", rb_wr_str );       // 15

    add_to_nfl( numbers, names, cats, 99, "watt", def_str );    // 16
    add_to_nfl( numbers, names, cats, 90, "watt", def_str );    // 17
    add_to_nfl( numbers, names, cats, 99, "donald", def_str );  // 18
    add_to_nfl( numbers, names, cats, 97, "bosa", def_str );    // 19
    add_to_nfl( numbers, names, cats, 97, "bosa", def_str );    // 20

    EXPECT_EQ( names.size(), numbers.size() );
    EXPECT_EQ( names.size(), cats.size() );

    cell_identifier cid;
    cid.table_id_ = order_table_id;
    cid.col_id_ = col_start;
    cid.key_ = 0;

    for( uint64_t rec = 0; rec <= 20; rec++ ) {
        cid.key_ = rec;
        cid.col_id_ = 0;

        EXPECT_LT( rec, numbers.size() );

        bool insert_ok = c1_holder->insert_int64_data( cid, numbers.at( rec ) );
        EXPECT_TRUE( insert_ok );
    }

    c1_state = c1_holder->commit_transaction();
    delete c1_holder;
    c1_write_set.clear();

    // check that data is there
    c1_read_set = {p_0_10, p_11_15, p_16_20};

    c1_holder = db.get_partitions_with_begin( c1, c1_state, c1_write_set,
                                              c1_read_set, c1_inflight_set,
                                              partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    for( uint64_t rec = 0; rec <= 20; rec++ ) {
        cid.key_ = rec;
        cid.col_id_ = 0;

        EXPECT_LT( rec, numbers.size() );

        auto read_int = c1_holder->get_int64_data( cid );
        EXPECT_TRUE( std::get<0>( read_int ) );
        EXPECT_EQ( std::get<1>( read_int ), numbers.at( rec ) );
    }

    bool scan_ok = false;

    // empty predicate (0.. 20)
    std::vector<uint32_t> proj_cols = {0};
    predicate_chain       pred;
    cell_predicate        c_pred;

    std::vector<result_tuple>    scan_res;
    std::unordered_set<uint64_t> expected_keys;

    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );

    DVLOG( 40 ) << "Scan_res:" << scan_res;

    expected_keys = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10,
                     11, 12, 13, 14, 15, 16, 17, 18, 19, 20};

    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // empty predicate but restrict search (5..15);
    expected_keys = {5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    scan_res = c1_holder->scan( order_table_id, 5, 15, proj_cols, pred );

    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // find players numbered 16 (2, 11)
    db_test_clear_predicate( pred );
    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 16 );
    c_pred.predicate = predicate_type::type::EQUALITY;

    pred.and_predicates.emplace_back( c_pred );

    expected_keys = {2, 11};
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // find players numbers 99 or 90 (16, 17, 18)
    db_test_clear_predicate( pred );
    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 99 );
    c_pred.predicate = predicate_type::type::EQUALITY;
    pred.and_predicates.emplace_back( c_pred );

    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 90 );
    c_pred.predicate = predicate_type::type::EQUALITY;
    pred.or_predicates.emplace_back( c_pred );

    pred.is_and_join_predicates = false;

    expected_keys = {16, 17, 18};
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // check it another way
    pred.or_predicates.emplace_back( pred.and_predicates.at( 0 ) );
    pred.and_predicates.clear();
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // find players numbered less than 0 ()
    db_test_clear_predicate( pred );
    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 0 );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    pred.and_predicates.emplace_back( c_pred );
    pred.is_and_join_predicates = false;

    expected_keys = {};
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // find players numbered greater than 99 ()
    db_test_clear_predicate( pred );
    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 99 );
    c_pred.predicate = predicate_type::type::GREATER_THAN;
    pred.and_predicates.emplace_back( c_pred );
    pred.is_and_join_predicates = false;

    expected_keys = {};
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // find players numbered less than 10 and greater than or equal to 3 (0, 5,
    // 6, 8, 9, 10)
    db_test_clear_predicate( pred );
    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 10 );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    pred.and_predicates.emplace_back( c_pred );

    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 3 );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    pred.and_predicates.emplace_back( c_pred );

    expected_keys = {0, 5, 6, 8, 9, 10};
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // #if 0
    // find players numbered greater than or equal to 3  and less than 10 (0, 5,
    // 6, 8, 9, 10)
    db_test_clear_predicate( pred );
    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 10 );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    pred.and_predicates.emplace_back( c_pred );

    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 3 );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    pred.and_predicates.emplace_back( c_pred );

    expected_keys = {0, 5, 6, 8, 9, 10};
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // find player names  numbered greater than or equal to 3  and less than 10
    // (0, 5, 6, 8, 9, 10)
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // these are all unmatched keys, either within range, or outside
    expected_keys = {};

    // find player numbered 54 (0)
    db_test_clear_predicate( pred );
    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 54 );
    c_pred.predicate = predicate_type::type::EQUALITY;
    pred.and_predicates.emplace_back( c_pred );

    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // find player numbered 100
    c_pred.data = int64_to_string( 100 );
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );

    // find player numbered 0
    c_pred.data = int64_to_string( 0 );
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

#if 0
#endif

    db_test_clear_predicate( pred );

    std::vector<cell_key_ranges> scan_ckrs = {
        create_cell_key_ranges( order_table_id, 0, 10, col_start, col_end ),
        create_cell_key_ranges( order_table_id, 11, 20, col_start, col_end )};

    std::unordered_map<int64_t, uint64_t> scan_mr_expected = {
        {1, 2},  {3, 1},  {4, 3},  {9, 2},  {10, 1}, {12, 2},
        {16, 2}, {22, 1}, {87, 2}, {90, 1}, {97, 2}, {99, 2},
    };

    // count the numbers
    // 1 (2), 3, 4 (3), 9 (2), 10, 12 (2), 16 (2), 22, 87 (2), 90, 97 (2) ,99 (2)
    std::unordered_map<int64_t, uint64_t> scan_mr_res =
        c1_holder->scan_mr<int64_t, uint64_t, std::hash<int64_t>,
                           std::equal_to<int64_t>, uint64_t /* probe */,
                           number_mapper, number_reducer>(
            order_table_id, scan_ckrs, proj_cols, pred, 0 /* probe */ );

    auto scan_simple_mr_res =
        c1_holder->scan_simple_mr<int64_t, uint64_t, std::hash<int64_t>,
                                  std::equal_to<int64_t>, uint64_t /* probe */,
                                  simple_number_mapper, simple_number_reducer>(
            order_table_id, scan_ckrs, proj_cols, pred, 0 /* probe*/ );

    EXPECT_EQ( scan_mr_expected.size(), scan_mr_res.size() );
    EXPECT_EQ( scan_mr_expected.size(), scan_simple_mr_res.size() );

    for( const auto& expected_entry : scan_mr_expected ) {
        auto mr_found = scan_mr_res.find( expected_entry.first );
        EXPECT_TRUE( mr_found != scan_mr_res.end() );
        if( mr_found != scan_mr_res.end() ) {
            EXPECT_EQ( expected_entry.second, mr_found->second );
            if( expected_entry.second != mr_found->second ) {
                DVLOG( 1 ) << "Expected entry:" << expected_entry.first << "="
                           << expected_entry.second
                           << ", mr_found:" << mr_found->second;
            }
        }
        auto simple_mr_found = scan_simple_mr_res.find( expected_entry.first );
        EXPECT_TRUE( simple_mr_found != scan_simple_mr_res.end() );
        if( simple_mr_found != scan_simple_mr_res.end() ) {
            EXPECT_EQ( expected_entry.second, simple_mr_found->second );
            if( expected_entry.second != simple_mr_found->second ) {
                DVLOG( 1 ) << "Expected entry:" << expected_entry.first << "="
                           << expected_entry.second
                           << ", simple_mr_found:" << simple_mr_found->second;
            }
        }
    }

    // now do the scan
    c1_state = c1_holder->commit_transaction();
    delete c1_holder;
}

TEST_F( db_test, db_single_col_scan_test_row ) {
    db_single_col_scan_test( partition_type::type::ROW );
}
TEST_F( db_test, db_single_col_scan_test_col ) {
    db_single_col_scan_test( partition_type::type::COLUMN );
}
TEST_F( db_test, db_single_col_scan_test_sorted_col ) {
    db_single_col_scan_test( partition_type::type::SORTED_COLUMN );
}
TEST_F( db_test, db_single_col_scan_test_multi_col ) {
    db_single_col_scan_test( partition_type::type::MULTI_COLUMN );
}
TEST_F( db_test, db_single_col_scan_test_sorted_multi_col ) {
    db_single_col_scan_test( partition_type::type::SORTED_MULTI_COLUMN );
}


void db_multi_col_scan_test( const partition_type::type& p_type ) {
    uint32_t site_loc = 1;
    uint32_t num_tables = 5;
    uint32_t num_clients = 10;
    int32_t  num_records_in_chain = k_num_records_in_chain;
    int32_t  num_records_in_snapshot_chain = k_num_records_in_snapshot_chain;
    uint32_t gc_sleep_time = 10;

    uint32_t c1 = 1;
    uint32_t c2 = 2;

    snapshot_vector empty_version;

    db db;
    db.init( make_no_op_update_destination_generator(), make_update_enqueuers(),
             create_tables_metadata(
                 num_tables, site_loc, num_clients, gc_sleep_time,
                 false /* enable sec storage */, "/tmp/" ) );

    uint32_t                    col_start = 0;
    uint32_t                    col_end = 2;
    std::vector<cell_data_type> col_types = {
        cell_data_type::INT64, cell_data_type::STRING, cell_data_type::STRING};

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;
    table_metadata          order_meta = create_table_metadata(
        "orders", 0, col_end + 1, col_types, num_records_in_chain,
        num_records_in_snapshot_chain, 0 /*site*/,
        10 /*default partition size*/, col_end + 1,
        10 /*default partition tracking size*/, col_end + 1, p_type, s_type,
        false /* enable sec storage */, "/tmp/" );
    uint32_t order_table_id = db.get_tables()->create_table( order_meta );
    EXPECT_EQ( 0, order_table_id );

    partition_column_identifier p_0_10 = create_partition_column_identifier(
        order_table_id, 0, 10, col_start, col_end );
    partition_column_identifier p_11_15 = create_partition_column_identifier(
        order_table_id, 11, 15, col_start, col_end );
    partition_column_identifier p_16_20 = create_partition_column_identifier(
        order_table_id, 16, 20, col_start, col_end );

    snapshot_vector                 c1_state = empty_version;
    partition_column_identifier_set c1_read_set = {p_0_10};
    partition_column_identifier_set c1_write_set = {};
    partition_column_identifier_set c1_inflight_set = {};
    transaction_partition_holder*   c1_holder = db.get_partitions_with_begin(
        c1, c1_state, c1_write_set, c1_read_set, c1_inflight_set,
        partition_lookup_operation::GET );
    EXPECT_EQ( c1_holder, nullptr );
    c1_read_set.clear();

    // now add some partitions
    db.add_partition( c1, p_0_10, site_loc, p_type, s_type );
    db.add_partition( c2, p_11_15, site_loc, p_type, s_type );
    db.add_partition( c1, p_16_20, site_loc, p_type, s_type );

    c1_write_set = {p_0_10, p_11_15, p_16_20};
    c1_holder = db.get_partitions_with_begin( c1, c1_state, c1_write_set,
                                              c1_read_set, c1_inflight_set,
                                              partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    uint32_t num_qbs = 11;
    uint32_t num_rbs_wrs = 5;
    uint32_t num_defs = 5;

    std::string qb_str = "QB";
    std::string rb_wr_str = "RB_WR";
    std::string def_str = "DEF";

    std::string lb_str = "LB";
    std::string cb_str = "CB";
    std::string saf_str = "SAF";

    std::vector<int64_t>     numbers;
    std::vector<std::string> names;
    std::vector<std::string> cats;

    add_to_nfl( numbers, names, cats, 3, "wilson", qb_str );    // 0
    add_to_nfl( numbers, names, cats, 1, "newton", qb_str );    // 1
    add_to_nfl( numbers, names, cats, 16, "goff", qb_str );     // 2
    add_to_nfl( numbers, names, cats, 12, "brady", qb_str );    // 3
    add_to_nfl( numbers, names, cats, 12, "rodgers", qb_str );  // 4
    add_to_nfl( numbers, names, cats, 4, "watson", qb_str );    // 5
    add_to_nfl( numbers, names, cats, 4, "prescott", qb_str );  // 6
    add_to_nfl( numbers, names, cats, 1, "murray", qb_str );    // 7
    add_to_nfl( numbers, names, cats, 9, "brees", qb_str );     // 8
    add_to_nfl( numbers, names, cats, 9, "stafford", qb_str );  // 9
    add_to_nfl( numbers, names, cats, 4, "carr", qb_str );      // 10

    add_to_nfl( numbers, names, cats, 16, "lockett", rb_wr_str );     // 11
    add_to_nfl( numbers, names, cats, 10, "hill", rb_wr_str );        // 12
    add_to_nfl( numbers, names, cats, 87, "kelce", rb_wr_str );       // 13
    add_to_nfl( numbers, names, cats, 87, "gronkowski", rb_wr_str );  // 14
    add_to_nfl( numbers, names, cats, 22, "henry", rb_wr_str );       // 15

    add_to_nfl( numbers, names, cats, 99, "watt", def_str );    // 16
    add_to_nfl( numbers, names, cats, 90, "watt", def_str );    // 17
    add_to_nfl( numbers, names, cats, 99, "donald", def_str );  // 18
    add_to_nfl( numbers, names, cats, 97, "bosa", def_str );    // 19
    add_to_nfl( numbers, names, cats, 97, "bosa", def_str );    // 20

    EXPECT_EQ( names.size(), numbers.size() );
    EXPECT_EQ( names.size(), cats.size() );

    cell_identifier cid;
    cid.table_id_ = order_table_id;
    cid.col_id_ = col_start;
    cid.key_ = 0;

    for( uint64_t rec = 0; rec <= 20; rec++ ) {
        cid.key_ = rec;
        cid.col_id_ = 0;

        EXPECT_LT( rec, names.size() );
        EXPECT_LT( rec, numbers.size() );
        EXPECT_LT( rec, cats.size() );

        bool insert_ok = c1_holder->insert_int64_data( cid, numbers.at( rec ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 1;
        insert_ok = c1_holder->insert_string_data( cid, names.at( rec ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 2;
        insert_ok = c1_holder->insert_string_data( cid, cats.at( rec ) );
        EXPECT_TRUE( insert_ok );
    }

    c1_state = c1_holder->commit_transaction();
    delete c1_holder;
    c1_write_set.clear();

    // check that data is there
    c1_read_set = {p_0_10, p_11_15, p_16_20};

    c1_holder = db.get_partitions_with_begin( c1, c1_state, c1_write_set,
                                              c1_read_set, c1_inflight_set,
                                              partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    for( uint64_t rec = 0; rec <= 20; rec++ ) {
        cid.key_ = rec;
        cid.col_id_ = 0;

        EXPECT_LT( rec, names.size() );
        EXPECT_LT( rec, numbers.size() );

        auto read_int = c1_holder->get_int64_data( cid );
        EXPECT_TRUE( std::get<0>( read_int ) );
        EXPECT_EQ( std::get<1>( read_int ), numbers.at( rec ) );

        cid.col_id_ = 1;
        auto read_str = c1_holder->get_string_data( cid );
        EXPECT_TRUE( std::get<0>( read_str ) );
        EXPECT_EQ( std::get<1>( read_str ), names.at( rec ) );

        cid.col_id_ = 2;
        read_str = c1_holder->get_string_data( cid );
        EXPECT_TRUE( std::get<0>( read_str ) );
        EXPECT_EQ( std::get<1>( read_str ), cats.at( rec ) );
    }

    bool scan_ok = false;

    // empty predicate (0.. 20)
    std::vector<uint32_t> proj_cols = {0, 1, 2};
    predicate_chain       pred;
    cell_predicate        c_pred;

    std::vector<result_tuple>    scan_res;
    std::unordered_set<uint64_t> expected_keys;

    // #if 0
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );

    DVLOG( 40 ) << "Scan_res:" << scan_res;

    expected_keys = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10,
                     11, 12, 13, 14, 15, 16, 17, 18, 19, 20};

    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // empty predicate but restrict search (5..15);
    expected_keys = {5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    scan_res = c1_holder->scan( order_table_id, 5, 15, proj_cols, pred );

    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // find QBs // 0 - 20 and 0 - 10 (0..10)
    c_pred.table_id = 0;
    c_pred.col_id = 2;
    c_pred.type = data_type::type::STRING;
    c_pred.data = qb_str;
    c_pred.predicate = predicate_type::type::EQUALITY;

    pred.and_predicates.emplace_back( c_pred );

    DVLOG( 40 ) << "SCAN";

    expected_keys = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );

    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    scan_res = c1_holder->scan( order_table_id, 0, 10, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // don't restrict the projection to just 0, 1
    proj_cols = {0, 1, 2};
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // find players numbered 16 (2, 11)
    db_test_clear_predicate( pred );
    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 16 );
    c_pred.predicate = predicate_type::type::EQUALITY;

    pred.and_predicates.emplace_back( c_pred );
    expected_keys = {2, 11};
    proj_cols = {0, 1, 2};
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // find players numbered 16 and QB (2)
    db_test_clear_predicate( pred );
    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 16 );
    c_pred.predicate = predicate_type::type::EQUALITY;
    pred.and_predicates.emplace_back( c_pred );

    c_pred.table_id = 0;
    c_pred.col_id = 2;
    c_pred.type = data_type::type::STRING;
    c_pred.data = qb_str;
    c_pred.predicate = predicate_type::type::EQUALITY;
    pred.and_predicates.emplace_back( c_pred );

    expected_keys = {2};
    proj_cols = {0, 1, 2};
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // find players named "watt" (16, 17)
    db_test_clear_predicate( pred );
    c_pred.table_id = 0;
    c_pred.col_id = 1;
    c_pred.type = data_type::type::STRING;
    c_pred.data = "watt";
    c_pred.predicate = predicate_type::type::EQUALITY;
    pred.and_predicates.emplace_back( c_pred );

    expected_keys = {16, 17};
    proj_cols = {0, 1, 2};
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // #endif
    // find players named "watt" and 99 (16)
    db_test_clear_predicate( pred );
    c_pred.table_id = 0;
    c_pred.col_id = 1;
    c_pred.type = data_type::type::STRING;
    c_pred.data = "watt";
    c_pred.predicate = predicate_type::type::EQUALITY;
    pred.and_predicates.emplace_back( c_pred );

    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 99 );
    c_pred.predicate = predicate_type::type::EQUALITY;
    pred.or_predicates.emplace_back( c_pred );
    pred.is_and_join_predicates = true;

    expected_keys = {16};
    proj_cols = {0, 1, 2};
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // check it another way
    pred.and_predicates.emplace_back( c_pred );
    pred.or_predicates.clear();
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // find players numbers 99 or named "watt" (16, 17, 18)
    db_test_clear_predicate( pred );
    c_pred.table_id = 0;
    c_pred.col_id = 1;
    c_pred.type = data_type::type::STRING;
    c_pred.data = "watt";
    c_pred.predicate = predicate_type::type::EQUALITY;
    pred.and_predicates.emplace_back( c_pred );

    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 99 );
    c_pred.predicate = predicate_type::type::EQUALITY;
    pred.or_predicates.emplace_back( c_pred );
    pred.is_and_join_predicates = false;

    expected_keys = {16, 17, 18};
    proj_cols = {0, 1, 2};
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );


    // check it another way
    pred.or_predicates.emplace_back( pred.and_predicates.at( 0 ) );
    pred.and_predicates.clear();
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // find players numbered less than 0 ()
    db_test_clear_predicate( pred );
    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 0 );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    pred.and_predicates.emplace_back( c_pred );
    pred.is_and_join_predicates = false;

    expected_keys = {};
    proj_cols = {0, 1, 2};
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );


    // find players numbered greater than 99 ()
    db_test_clear_predicate( pred );
    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 99 );
    c_pred.predicate = predicate_type::type::GREATER_THAN;
    pred.and_predicates.emplace_back( c_pred );
    pred.is_and_join_predicates = false;

    expected_keys = {};
    proj_cols = {0, 1, 2};
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // find players numbered less than 10 and greater than or equal to 3 (0, 5,
    // 6, 8, 9, 10)
    db_test_clear_predicate( pred );
    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 10 );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    pred.and_predicates.emplace_back( c_pred );

    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 3 );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    pred.and_predicates.emplace_back( c_pred );

    expected_keys = {0, 5, 6, 8, 9, 10};
    proj_cols = {0, 1, 2};
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // #if 0
    // find players numbered greater than or equal to 3  and less than 10 (0, 5,
    // 6, 8, 9, 10)
    db_test_clear_predicate( pred );
    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 10 );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    pred.and_predicates.emplace_back( c_pred );

    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 3 );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    pred.and_predicates.emplace_back( c_pred );

    expected_keys = {0, 5, 6, 8, 9, 10};
    proj_cols = {0, 1, 2};
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // find player names  numbered greater than or equal to 3  and less than 10
    // (0, 5,
    // 6, 8, 9, 10)
    proj_cols = {0};
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // these are all unmatched keys, either within range, or outside
    expected_keys = {};

    proj_cols = {2};
    // find pos CB
    db_test_clear_predicate( pred );
    c_pred.table_id = 0;
    c_pred.col_id = 2;
    c_pred.type = data_type::type::STRING;
    c_pred.data = cb_str;
    c_pred.predicate = predicate_type::type::EQUALITY;
    pred.and_predicates.emplace_back( c_pred );

    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    // find pos LB
    c_pred.data = lb_str;
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );

    // find pos SAF
    c_pred.data = saf_str;
    scan_res = c1_holder->scan( order_table_id, 0, 20, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

#if 0
#endif
    db_test_clear_predicate( pred );

    proj_cols = {0, 1, 2};

    std::vector<cell_key_ranges> scan_ckrs = {
        create_cell_key_ranges( order_table_id, 0, 10, col_start, col_end ),
        create_cell_key_ranges( order_table_id, 11, 20, col_start, col_end )};

    std::unordered_map<int64_t, uint64_t> scan_mr_expected = {
        {1, 2},  {3, 1},  {4, 3},  {9, 2},  {10, 1}, {12, 2},
        {16, 2}, {22, 1}, {87, 2}, {90, 1}, {97, 2}, {99, 2},
    };

    // count the numbers
    // 1 (2), 3, 4 (3), 9 (2), 10, 12 (2), 16 (2), 22, 87 (2), 90, 97 (2) ,99 (2)
    std::unordered_map<int64_t, uint64_t> scan_mr_res =
        c1_holder->scan_mr<int64_t, uint64_t, std::hash<int64_t>,
                           std::equal_to<int64_t>, uint64_t /* probe */,
                           number_mapper, number_reducer>(
            order_table_id, scan_ckrs, proj_cols, pred, 0 /* probe */ );

    auto scan_simple_mr_res =
        c1_holder->scan_simple_mr<int64_t, uint64_t, std::hash<int64_t>,
                                  std::equal_to<int64_t>, uint64_t /* probe */,
                                  simple_number_mapper, simple_number_reducer>(
            order_table_id, scan_ckrs, proj_cols, pred, 0 /* probe */ );

    EXPECT_EQ( scan_mr_expected.size(), scan_mr_res.size() );
    EXPECT_EQ( scan_mr_expected.size(), scan_simple_mr_res.size() );

    for( const auto& expected_entry : scan_mr_expected ) {
        auto mr_found = scan_mr_res.find( expected_entry.first );
        EXPECT_TRUE( mr_found != scan_mr_res.end() );
        if( mr_found != scan_mr_res.end() ) {
            EXPECT_EQ( expected_entry.second, mr_found->second );
            if( expected_entry.second != mr_found->second ) {
                DVLOG( 1 ) << "Expected entry:" << expected_entry.first << "="
                           << expected_entry.second
                           << ", mr_found:" << mr_found->second;
            }
        }
        auto simple_mr_found = scan_simple_mr_res.find( expected_entry.first );
        EXPECT_TRUE( simple_mr_found != scan_simple_mr_res.end() );
        if( simple_mr_found != scan_simple_mr_res.end() ) {
            EXPECT_EQ( expected_entry.second, simple_mr_found->second );
            if( expected_entry.second != simple_mr_found->second ) {
                DVLOG( 1 ) << "Expected entry:" << expected_entry.first << "="
                           << expected_entry.second
                           << ", simple_mr_found:" << simple_mr_found->second;
            }
        }
    }



    // now do the scan
    c1_state = c1_holder->commit_transaction();
    delete c1_holder;

    (void) num_defs;
    (void) num_qbs;
    (void) num_rbs_wrs;
}

TEST_F( db_test, db_multi_col_scan_test_row ) {
    db_multi_col_scan_test( partition_type::type::ROW );
}
TEST_F( db_test, db_multi_col_scan_test_col ) {
    db_multi_col_scan_test( partition_type::type::COLUMN );
}
TEST_F( db_test, db_multi_col_scan_test_sorted_col ) {
    db_multi_col_scan_test( partition_type::type::SORTED_COLUMN );
}
TEST_F( db_test, db_multi_col_scan_test_multi_col ) {
    db_multi_col_scan_test( partition_type::type::MULTI_COLUMN );
}
TEST_F( db_test, db_multi_col_scan_test_sorted_multi_col ) {
    db_multi_col_scan_test( partition_type::type::SORTED_MULTI_COLUMN );
}

void db_tiered_storage( const partition_type::type& p_type ) {
    uint32_t site_loc = 1;
    uint32_t num_tables = 5;
    uint32_t num_clients = 10;
    int32_t  num_records_in_chain = k_num_records_in_chain;
    int32_t  num_records_in_snapshot_chain = k_num_records_in_snapshot_chain;
    uint32_t gc_sleep_time = 10;

    uint32_t c1 = 1;
    uint32_t c2 = 2;

    snapshot_vector empty_version;

    std::string sec_storage_dir =
        "/tmp/" + std::to_string( (uint64_t) std::time( nullptr ) ) + "-" +
        std::to_string( rand() % 1000 ) + "-sec-storage";

    db db;
    db.init( make_no_op_update_destination_generator(), make_update_enqueuers(),
             create_tables_metadata(
                 num_tables, site_loc, num_clients, gc_sleep_time,
                 true /* enable sec storage */, sec_storage_dir ) );

    uint32_t                    col_start = 0;
    uint32_t                    col_end = 2;
    std::vector<cell_data_type> col_types = {
        cell_data_type::INT64, cell_data_type::STRING, cell_data_type::STRING};

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;

    table_metadata order_meta = create_table_metadata(
        "orders", 0, col_end + 1, col_types, num_records_in_chain,
        num_records_in_snapshot_chain, 0 /*site*/,
        10 /*default partition size*/, col_end + 1,
        10 /*default partition trackign size*/, col_end + 1, p_type, s_type,
        true, sec_storage_dir );
    uint32_t order_table_id = db.get_tables()->create_table( order_meta );
    EXPECT_EQ( 0, order_table_id );

    partition_column_identifier p_0_10 = create_partition_column_identifier(
        order_table_id, 0, 10, col_start, col_end );
    partition_column_identifier p_11_15 = create_partition_column_identifier(
        order_table_id, 11, 15, col_start, col_end );
    partition_column_identifier p_16_22 = create_partition_column_identifier(
        order_table_id, 16, 22, col_start, col_end );
    partition_column_identifier p_23_25 = create_partition_column_identifier(
        order_table_id, 23, 25, col_start, col_end );

    snapshot_vector                 c1_state = empty_version;
    partition_column_identifier_set c1_read_set = {p_0_10};
    partition_column_identifier_set c1_write_set = {};
    partition_column_identifier_set c1_inflight_set = {};
    transaction_partition_holder*   c1_holder = db.get_partitions_with_begin(
        c1, c1_state, c1_write_set, c1_read_set, c1_inflight_set,
        partition_lookup_operation::GET );
    EXPECT_EQ( c1_holder, nullptr );
    c1_read_set.clear();

    // now add some partitions
    db.add_partition( c1, p_0_10, site_loc, p_type, s_type );
    db.add_partition( c2, p_11_15, site_loc, p_type, s_type );
    db.add_partition( c1, p_16_22, site_loc, p_type, s_type );
    db.add_partition( c1, p_23_25, site_loc, p_type, s_type );

    c1_write_set = {p_0_10, p_11_15, p_16_22, p_23_25};
    c1_holder = db.get_partitions_with_begin( c1, c1_state, c1_write_set,
                                              c1_read_set, c1_inflight_set,
                                              partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    uint32_t num_qbs = 11;
    uint32_t num_rbs_wrs = 5;
    uint32_t num_defs = 5;

    std::string qb_str = "QB";
    std::string rb_wr_str = "RB_WR";
    std::string def_str = "DEF";

    std::string lb_str = "LB";
    std::string cb_str = "CB";
    std::string saf_str = "SAF";

    std::vector<int64_t>     numbers;
    std::vector<std::string> names;
    std::vector<std::string> cats;

    add_to_nfl( numbers, names, cats, 3, "wilson", qb_str );    // 0
    add_to_nfl( numbers, names, cats, 1, "newton", qb_str );    // 1
    add_to_nfl( numbers, names, cats, 16, "goff", qb_str );     // 2
    add_to_nfl( numbers, names, cats, 12, "brady", qb_str );    // 3
    add_to_nfl( numbers, names, cats, 12, "rodgers", qb_str );  // 4
    add_to_nfl( numbers, names, cats, 4, "watson", qb_str );    // 5
    add_to_nfl( numbers, names, cats, 4, "prescott", qb_str );  // 6
    add_to_nfl( numbers, names, cats, 1, "murray", qb_str );    // 7
    add_to_nfl( numbers, names, cats, 9, "brees", qb_str );     // 8
    add_to_nfl( numbers, names, cats, 9, "stafford", qb_str );  // 9
    add_to_nfl( numbers, names, cats, 4, "carr", qb_str );      // 10

    add_to_nfl( numbers, names, cats, 16, "lockett", rb_wr_str );     // 11
    add_to_nfl( numbers, names, cats, 10, "hill", rb_wr_str );        // 12
    add_to_nfl( numbers, names, cats, 87, "kelce", rb_wr_str );       // 13
    add_to_nfl( numbers, names, cats, 87, "gronkowski", rb_wr_str );  // 14
    add_to_nfl( numbers, names, cats, 22, "henry", rb_wr_str );       // 15

    add_to_nfl( numbers, names, cats, 99, "watt", def_str );    // 16
    add_to_nfl( numbers, names, cats, 90, "watt", def_str );    // 17
    add_to_nfl( numbers, names, cats, 99, "donald", def_str );  // 18
    add_to_nfl( numbers, names, cats, 97, "bosa", def_str );    // 19
    add_to_nfl( numbers, names, cats, 97, "bosa", def_str );    // 20


    EXPECT_EQ( names.size(), numbers.size() );
    EXPECT_EQ( names.size(), cats.size() );

    cell_identifier cid;
    cid.table_id_ = order_table_id;
    cid.col_id_ = col_start;
    cid.key_ = 0;

    uint64_t null_rec_start = 21;

    for( uint64_t rec = 0; rec <= 20; rec++ ) {
        cid.key_ = rec;
        cid.col_id_ = 0;

        EXPECT_LT( rec, names.size() );
        EXPECT_LT( rec, numbers.size() );
        EXPECT_LT( rec, cats.size() );

        bool insert_ok = c1_holder->insert_int64_data( cid, numbers.at( rec ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 1;
        insert_ok = c1_holder->insert_string_data( cid, names.at( rec ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 2;
        insert_ok = c1_holder->insert_string_data( cid, cats.at( rec ) );
        EXPECT_TRUE( insert_ok );
    }

    c1_state = c1_holder->commit_transaction();
    delete c1_holder;
    c1_write_set.clear();

    // check that data is there
    c1_read_set = {p_0_10, p_11_15, p_16_22, p_23_25};

    c1_holder = db.get_partitions_with_begin( c1, c1_state, c1_write_set,
                                              c1_read_set, c1_inflight_set,
                                              partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    for( uint64_t rec = 0; rec <= 25; rec++ ) {
        cid.key_ = rec;
        cid.col_id_ = 0;

        if( rec < null_rec_start ) {
            EXPECT_LT( rec, names.size() );
            EXPECT_LT( rec, numbers.size() );
        }

        auto read_int = c1_holder->get_int64_data( cid );
        if( rec >= null_rec_start ) {
            EXPECT_FALSE( std::get<0>( read_int ) );
        } else {
            EXPECT_TRUE( std::get<0>( read_int ) );
            EXPECT_EQ( std::get<1>( read_int ), numbers.at( rec ) );
        }

        cid.col_id_ = 1;
        auto read_str = c1_holder->get_string_data( cid );
        if( rec >= null_rec_start ) {
            EXPECT_FALSE( std::get<0>( read_str ) );
        } else {
            EXPECT_TRUE( std::get<0>( read_str ) );
            EXPECT_EQ( std::get<1>( read_str ), names.at( rec ) );
        }

        cid.col_id_ = 2;
        read_str = c1_holder->get_string_data( cid );

        if( rec >= null_rec_start ) {
            EXPECT_FALSE( std::get<0>( read_str ) );
        } else {
            EXPECT_TRUE( std::get<0>( read_str ) );
            EXPECT_EQ( std::get<1>( read_str ), cats.at( rec ) );
        }
    }
    c1_state = c1_holder->commit_transaction();
    delete c1_holder;

    std::vector<partition_column_identifier> change_pids = {p_0_10, p_11_15,
                                                            p_16_22, p_23_25};
    std::vector<partition_type::type> change_partition_types = {p_type, p_type,
                                                                p_type, p_type};
    std::vector<storage_tier_type::type> change_types = {
        storage_tier_type::type::DISK, storage_tier_type::type::DISK,
        storage_tier_type::type::DISK, storage_tier_type::type::DISK};

    auto change_ret = db.change_partition_types(
        c1, change_pids, change_partition_types, change_types );
    EXPECT_TRUE( std::get<0>( change_ret ) );

    c1_holder = db.get_partitions_with_begin( c1, c1_state, c1_write_set,
                                              c1_read_set, c1_inflight_set,
                                              partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    // lets do the Reads on disk!
    for( uint64_t rec = 0; rec <= 25; rec++ ) {
        cid.key_ = rec;
        cid.col_id_ = 0;

        if( rec < null_rec_start ) {
            EXPECT_LT( rec, names.size() );
            EXPECT_LT( rec, numbers.size() );
        }

        auto read_int = c1_holder->get_int64_data( cid );
        if( rec >= null_rec_start ) {
            EXPECT_FALSE( std::get<0>( read_int ) );
        } else {
            EXPECT_TRUE( std::get<0>( read_int ) );
            EXPECT_EQ( std::get<1>( read_int ), numbers.at( rec ) );
        }

        cid.col_id_ = 1;
        auto read_str = c1_holder->get_string_data( cid );
        if( rec >= null_rec_start ) {
            EXPECT_FALSE( std::get<0>( read_str ) );
        } else {
            EXPECT_TRUE( std::get<0>( read_str ) );
            EXPECT_EQ( std::get<1>( read_str ), names.at( rec ) );
        }

        cid.col_id_ = 2;
        read_str = c1_holder->get_string_data( cid );

        if( rec >= null_rec_start ) {
            EXPECT_FALSE( std::get<0>( read_str ) );
        } else {
            EXPECT_TRUE( std::get<0>( read_str ) );
            EXPECT_EQ( std::get<1>( read_str ), cats.at( rec ) );
        }
    }

    c1_state = c1_holder->commit_transaction();
    delete c1_holder;

    bool change_check_ok =
        check_storage_tier_changes( db.get_storage_tier_changes(), {}, {} );
    EXPECT_TRUE( change_check_ok );

    change_types = {
        storage_tier_type::type::MEMORY, storage_tier_type::type::MEMORY,
        storage_tier_type::type::MEMORY, storage_tier_type::type::MEMORY};

    change_ret = db.change_partition_types(
        c1, change_pids, change_partition_types, change_types );
    EXPECT_TRUE( std::get<0>( change_ret ) );

    c1_read_set = {p_0_10, p_11_15, p_16_22, p_23_25};

    c1_holder = db.get_partitions_with_begin( c1, c1_state, c1_write_set,
                                              c1_read_set, c1_inflight_set,
                                              partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    for( uint64_t rec = 0; rec <= 25; rec++ ) {
        cid.key_ = rec;
        cid.col_id_ = 0;

        if( rec < null_rec_start ) {
            EXPECT_LT( rec, names.size() );
            EXPECT_LT( rec, numbers.size() );
        }

        auto read_int = c1_holder->get_int64_data( cid );
        if( rec >= null_rec_start ) {
            EXPECT_FALSE( std::get<0>( read_int ) );
        } else {
            EXPECT_TRUE( std::get<0>( read_int ) );
            EXPECT_EQ( std::get<1>( read_int ), numbers.at( rec ) );
        }

        cid.col_id_ = 1;
        auto read_str = c1_holder->get_string_data( cid );
        if( rec >= null_rec_start ) {
            EXPECT_FALSE( std::get<0>( read_str ) );
        } else {
            EXPECT_TRUE( std::get<0>( read_str ) );
            EXPECT_EQ( std::get<1>( read_str ), names.at( rec ) );
        }

        cid.col_id_ = 2;
        read_str = c1_holder->get_string_data( cid );

        if( rec >= null_rec_start ) {
            EXPECT_FALSE( std::get<0>( read_str ) );
        } else {
            EXPECT_TRUE( std::get<0>( read_str ) );
            EXPECT_EQ( std::get<1>( read_str ), cats.at( rec ) );
        }
    }

    c1_state = c1_holder->commit_transaction();
    delete c1_holder;

    /// put it back on disk and do an update that should force going back to
    /// memory
    change_types = {
        storage_tier_type::type::DISK, storage_tier_type::type::DISK,
        storage_tier_type::type::DISK, storage_tier_type::type::DISK};

    change_ret = db.change_partition_types(
        c1, change_pids, change_partition_types, change_types );
    EXPECT_TRUE( std::get<0>( change_ret ) );

    c1_read_set = {p_0_10, p_11_15, p_16_22, p_23_25};
    c1_write_set = {p_0_10, p_11_15, p_16_22, p_23_25};

    c1_holder = db.get_partitions_with_begin( c1, c1_state, c1_write_set,
                                              c1_read_set, c1_inflight_set,
                                              partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    std::vector<storage_tier_type::type> expected_change_types = {
        storage_tier_type::type::MEMORY, storage_tier_type::type::MEMORY,
        storage_tier_type::type::MEMORY, storage_tier_type::type::MEMORY};
    change_check_ok = check_storage_tier_changes(
        db.get_storage_tier_changes(), change_pids, expected_change_types );
    EXPECT_TRUE( change_check_ok );

    for( uint64_t rec = 0; rec <= 25; rec++ ) {
        cid.key_ = rec;
        cid.col_id_ = 0;

        auto read_int = c1_holder->get_int64_data( cid );
        if( rec >= null_rec_start ) {
            EXPECT_FALSE( std::get<0>( read_int ) );
        } else {
            EXPECT_TRUE( std::get<0>( read_int ) );
            EXPECT_EQ( std::get<1>( read_int ), numbers.at( rec ) );
            bool write_ok =
                c1_holder->update_int64_data( cid, -std::get<1>( read_int ) );
            numbers.at( rec ) = -numbers.at( rec );
            EXPECT_TRUE( write_ok );
        }
    }

    c1_state = c1_holder->commit_transaction();
    delete c1_holder;

    change_ret = db.change_partition_types(
        c1, change_pids, change_partition_types, change_types );
    EXPECT_TRUE( std::get<0>( change_ret ) );

    c1_read_set = {p_0_10, p_11_15, p_16_22, p_23_25};
    c1_write_set = {};

    c1_holder = db.get_partitions_with_begin( c1, c1_state, c1_write_set,
                                              c1_read_set, c1_inflight_set,
                                              partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    bool scan_ok = false;
    std::vector<uint32_t> proj_cols = {0, 1, 2};
    predicate_chain       pred;
    cell_predicate        c_pred;

    std::vector<result_tuple>    scan_res;
    std::unordered_set<uint64_t> expected_keys;


    // find players numbered 16 (none);
    db_test_clear_predicate( pred );
    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( 16 );
    c_pred.predicate = predicate_type::type::EQUALITY;

    pred.and_predicates.emplace_back( c_pred );

    expected_keys = {};
    scan_res = c1_holder->scan( order_table_id, 0, 25, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );
    scan_res.clear();

    change_check_ok =
        check_storage_tier_changes( db.get_storage_tier_changes(), {}, {} );
    EXPECT_TRUE( change_check_ok );

    // find players numbered -16 (2, 11)
    db_test_clear_predicate( pred );
    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( -16 );
    c_pred.predicate = predicate_type::type::EQUALITY;

    pred.and_predicates.emplace_back( c_pred );

    DVLOG( 40 ) << " SCANNING -16";

    expected_keys = {2, 11};
    scan_res = c1_holder->scan( order_table_id, 0, 25, proj_cols, pred );
    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );
    scan_res.clear();

    std::vector<partition_column_identifier> expected_change_pids = {p_0_10,
                                                                     p_11_15};
    expected_change_types = {
        storage_tier_type::type::MEMORY, storage_tier_type::type::MEMORY,
    };

    change_check_ok = check_storage_tier_changes( db.get_storage_tier_changes(),
                                                  expected_change_pids,
                                                  expected_change_types );
    EXPECT_TRUE( change_check_ok );
    DVLOG( 40 ) << " SCANNING -16 DONE";

    DVLOG( 40 ) << " SCANNING EMPTY";
    // empty predicate (0.. 20)
    db_test_clear_predicate( pred );

    scan_res = c1_holder->scan( order_table_id, 0, 25, proj_cols, pred );

    DVLOG( 40 ) << "Scan_res:" << scan_res;

    expected_keys = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10,
                     11, 12, 13, 14, 15, 16, 17, 18, 19, 20};

    scan_ok =
        check_scan( scan_res, expected_keys, proj_cols, numbers, names, cats );
    EXPECT_TRUE( scan_ok );

    expected_change_pids = {p_16_22};
    expected_change_types = {storage_tier_type::type::MEMORY};
    change_check_ok = check_storage_tier_changes( db.get_storage_tier_changes(),
                                                  expected_change_pids,
                                                  expected_change_types );
    EXPECT_TRUE( change_check_ok );


    DVLOG( 40 ) << " SCANNING EMPTY DONE";

    c1_state = c1_holder->commit_transaction();
    delete c1_holder;

    (void) num_qbs;
    (void) num_rbs_wrs;
    (void) num_defs;
}

TEST_F( db_test, db_tiered_storage_row ) {
    db_tiered_storage( partition_type::type::ROW );
}
TEST_F( db_test, db_tiered_storage_col ) {
    db_tiered_storage( partition_type::type::COLUMN );
}
TEST_F( db_test, db_tiered_storage_sorted_col ) {
    db_tiered_storage( partition_type::type::SORTED_COLUMN );
}
TEST_F( db_test, db_tiered_storage_multi_col ) {
    db_tiered_storage( partition_type::type::MULTI_COLUMN );
}
TEST_F( db_test, db_tiered_storage_sorted_multi_col ) {
    db_tiered_storage( partition_type::type::SORTED_MULTI_COLUMN );
}


