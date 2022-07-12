#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/data-site/single-site-db/db_abstraction.h"
#include "../src/data-site/single-site-db/single_site_db.h"

class single_site_db_test : public ::testing::Test {};

#define enable_db_templ template <bool enable_db>
enable_db_templ void                   single_site_table_creation_test() {
    single_site_db<enable_db> db;
    uint32_t                  site_loc = 1;
    uint32_t                  num_tables = 5;
    uint32_t                  num_clients = 10;
    uint32_t                  gc_sleep_time = 10;

    uint32_t default_partition_size = 10;
    uint32_t default_column_size = 2;
    uint32_t col_start = 0;
    uint32_t col_end = 4;

    std::vector<cell_data_type> col_types = {
        cell_data_type::INT64, cell_data_type::INT64, cell_data_type::INT64,
        cell_data_type::INT64, cell_data_type::INT64};

    db.init( make_no_op_update_destination_generator(), make_update_enqueuers(),
             create_tables_metadata(
                 num_tables, site_loc, num_clients, gc_sleep_time,
                 false /* enable sec storage */, "/tmp/" ) );

    int32_t num_records_in_chain = k_num_records_in_chain;
    int32_t num_records_in_snapshot_chain = k_num_records_in_snapshot_chain;

    // t0 = orders,
    auto order_meta = create_table_metadata(
        "orders", 0, ( col_end - col_start ) + 1, col_types,
        num_records_in_chain, num_records_in_snapshot_chain, site_loc,
        default_partition_size, default_column_size,
        default_partition_size, default_column_size,
        partition_type::type::COLUMN, storage_tier_type::type::MEMORY,
        false /* enable sec storage */, "/tmp/" );
    uint32_t order_table_id = db.create_table( order_meta );
    EXPECT_EQ( 0, order_table_id );

    // should get the old table back
    uint32_t order_table_id_1 = db.create_table( order_meta );
    EXPECT_EQ( order_table_id, order_table_id_1 );

    auto shopping_meta = create_table_metadata(
        "shopping_cart", 1, ( col_end - col_start ) + 1, col_types,
        num_records_in_chain, num_records_in_snapshot_chain, site_loc,
        default_partition_size, default_column_size, default_partition_size,
        default_column_size, partition_type::type::ROW,
        storage_tier_type::type::MEMORY, false /* enable sec storage */,
        "/tmp/" );

    uint32_t shopping_cart_table_id = db.create_table( shopping_meta );
    EXPECT_EQ( 1, shopping_cart_table_id );
}

enable_db_templ void single_site_db_txn_ops(
    const partition_type::type& p_type ) {
    uint32_t site_loc = 1;
    uint32_t num_tables = 5;
    uint32_t num_clients = 10;
    uint32_t gc_sleep_time = 10;

    uint32_t default_partition_size = 100;
    uint32_t default_column_size = 2;
    uint32_t col_start = 0;
    uint32_t col_end = 4;

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;

    std::vector<cell_data_type> col_types = {
        cell_data_type::INT64, cell_data_type::INT64, cell_data_type::INT64,
        cell_data_type::INT64, cell_data_type::INT64};

    single_site_db<enable_db> db;
    db.init( make_no_op_update_destination_generator(), make_update_enqueuers(),
             create_tables_metadata(
                 num_tables, site_loc, num_clients, gc_sleep_time,
                 false /* enable sec storage */, "/tmp/" ) );

    int32_t num_records_in_chain = k_num_records_in_chain;
    int32_t num_records_in_snapshot_chain = k_num_records_in_snapshot_chain;

    // t0 = orders,
    auto order_meta = create_table_metadata(
        "orders", 0, ( col_end - col_start ) + 1, col_types,
        num_records_in_chain, num_records_in_snapshot_chain, site_loc,
        default_partition_size, default_column_size, default_partition_size,
        default_column_size, p_type, storage_tier_type::type::MEMORY,
        false /* enable sec storage */, "/tmp/" );
    uint32_t order_table_id = db.create_table( order_meta );
    EXPECT_EQ( 0, order_table_id );

    // should get the old table back
    uint32_t order_table_id_1 = db.create_table( order_meta );
    EXPECT_EQ( order_table_id, order_table_id_1 );

    uint32_t c1 = 1;
    uint32_t c2 = 2;

    snapshot_vector empty_version;
    // add some partitions
    partition_column_identifier p_0_10 =
        create_partition_column_identifier( order_table_id, 0, 10, 0, 1 );
    partition_column_identifier p_11_15 =
        create_partition_column_identifier( order_table_id, 11, 15, 0, 1 );
    partition_column_identifier p_16_20 =
        create_partition_column_identifier( order_table_id, 16, 20, 0, 1 );
    db.add_partition( c1, p_0_10, site_loc, p_type, s_type );
    db.add_partition( c2, p_11_15, site_loc, p_type, s_type );
    db.add_partition( c1, p_16_20, site_loc, p_type, s_type );

    // read only that won't see anything
    // begin
    std::vector<cell_key_ranges> c1_read_set;
    std::vector<cell_key_ranges> c1_write_set;
    cell_key_ranges              ckr;
    ckr.table_id = order_table_id;
    ckr.col_id_start = 0;
    ckr.col_id_end = 1;
    ckr.row_id_start = 0;
    ckr.row_id_end = 1;

    cell_key ck;
    ck.table_id = order_table_id;
    ck.col_id = 0;
    ck.row_id = 0;

    c1_read_set.emplace_back( ckr );

    snapshot_vector               c1_state = empty_version;
    transaction_partition_holder* c1_holder = db.get_partitions_with_begin(
        c1, c1_state, c1_write_set, c1_read_set, false /* allow missing */ );
    EXPECT_NE( c1_holder, nullptr );

    if( enable_db ) {
        auto read_r = c1_holder->get_int64_data( ck );
        EXPECT_FALSE( std::get<0>( read_r ) );
        // commit
        c1_state = c1_holder->commit_transaction();
        EXPECT_EQ( 0, get_snapshot_version( c1_state, p_0_10 ) );
    }
    delete c1_holder;

    // write transaction that aborts
    snapshot_vector              c2_state = empty_version;
    std::vector<cell_key_ranges> c2_read_set = {};
    std::vector<cell_key_ranges> c2_write_set;
    ckr.row_id_start = 10;
    ckr.row_id_end = 15;
    c2_write_set.push_back( ckr );

    ckr.row_id_start = 17;
    ckr.row_id_end = 19;
    c2_write_set.push_back( ckr );

    transaction_partition_holder* c2_holder = db.get_partitions_with_begin(
        c2, c2_state, c2_write_set, c2_read_set, false /* allow missing */ );
    EXPECT_NE( c2_holder, nullptr );

    if( enable_db ) {
        ck.row_id = 10;
        bool write_ok = c2_holder->insert_int64_data( ck, 0 );
        EXPECT_TRUE( write_ok );
        c2_state = c2_holder->abort_transaction();
        EXPECT_EQ( empty_version, c2_state );
    }
    delete c2_holder;

    c2_read_set.clear();
    c2_write_set.clear();
    ckr.row_id_start = 1;
    ckr.row_id_end = 9;
    c2_write_set.push_back( ckr );
    ckr.row_id_start = 16;
    ckr.row_id_end = 20;
    c2_write_set.push_back( ckr );

    c2_holder = db.get_partitions_with_begin(
        c2, c2_state, c2_write_set, c2_read_set, false /* allow missing */ );
    EXPECT_NE( c2_holder, nullptr );
    snapshot_vector commit_1_version;
    if( enable_db ) {
        ck.row_id = 1;
        bool write_ok = c2_holder->insert_int64_data( ck, 1 );
        EXPECT_TRUE( write_ok );
        ck.row_id = 20;
        write_ok = c2_holder->insert_int64_data( ck, 20 );
        EXPECT_TRUE( write_ok );
        // commit
        c2_state = c2_holder->commit_transaction();

        set_snapshot_version( commit_1_version, p_0_10, 1 );
        set_snapshot_version( commit_1_version, p_16_20, 1 );
        EXPECT_EQ( commit_1_version, c2_state );
    }
    delete c2_holder;

    // read should see change
    c1_state = c2_state;
    c1_read_set.clear();
    c1_write_set.clear();
    ckr.row_id_start = 0;
    ckr.row_id_end = 1;
    c1_read_set.push_back( ckr );
    c1_holder = db.get_partitions_with_begin(
        c1, c1_state, c1_write_set, c1_read_set, false /* allow missing */ );
    EXPECT_NE( c1_holder, nullptr );

    if( enable_db ) {
        ck.row_id = 1;
        auto read_r = c1_holder->get_int64_data( ck );
        EXPECT_TRUE( std::get<0>( read_r ) );
        EXPECT_EQ( std::get<1>( read_r ), 1 );
        // commit
        c1_state = c1_holder->commit_transaction();
        EXPECT_EQ( commit_1_version, c1_state );
    }
    delete c1_holder;

    c1_state = c2_state;
    c1_read_set.clear();
    c1_write_set.clear();
    ckr.row_id_start = 500;
    ckr.row_id_end = 510;
    c1_read_set.push_back( ckr );
    c1_holder = db.get_partitions_with_begin(
        c1, c1_state, c1_write_set, c1_read_set, true /* allow missing */ );
    EXPECT_NE( c1_holder, nullptr );

    if( enable_db ) {
        ck.row_id = 500;
        auto read_r = c1_holder->get_int64_data( ck );
        EXPECT_FALSE( std::get<0>( read_r ) );
        c1_state = c1_holder->commit_transaction();
        EXPECT_EQ( commit_1_version, c1_state );
    }
    delete c1_holder;
}

enable_db_templ void single_site_db_change_partition(
    const partition_type::type& p_type ) {
    uint32_t site_loc = 1;
    uint32_t num_tables = 5;
    uint32_t num_clients = 10;
    uint32_t gc_sleep_time = 10;

    uint32_t default_partition_size = 10;
    uint32_t default_column_size = 2;
    uint32_t col_start = 0;
    uint32_t col_end = 4;

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;

    std::vector<cell_data_type> col_types = {
        cell_data_type::INT64, cell_data_type::INT64, cell_data_type::INT64,
        cell_data_type::INT64, cell_data_type::INT64};

    single_site_db<enable_db> db;
    db.init( make_no_op_update_destination_generator(), make_update_enqueuers(),
             create_tables_metadata(
                 num_tables, site_loc, num_clients, gc_sleep_time,
                 false /* enable sec storage */, "/tmp/" ) );

    int32_t num_records_in_chain = k_num_records_in_chain;
    int32_t num_records_in_snapshot_chain = k_num_records_in_snapshot_chain;

    // t0 = orders,
    auto order_meta = create_table_metadata(
        "orders", 0, ( col_end - col_start ) + 1, col_types,
        num_records_in_chain, num_records_in_snapshot_chain, site_loc,
        default_partition_size, default_column_size, default_partition_size,
        default_column_size, p_type, storage_tier_type::type::MEMORY,
        false /* enable sec storage */, "/tmp/" );
    uint32_t order_table_id = db.create_table( order_meta );
    EXPECT_EQ( 0, order_table_id );

    uint32_t c1 = 1;
    uint32_t c2 = 2;

    snapshot_vector empty_version;

    // add some partitions
    partition_column_identifier p_0_10 =
        create_partition_column_identifier( order_table_id, 0, 10, 0, 1 );
    partition_column_identifier p_11_15 =
        create_partition_column_identifier( order_table_id, 11, 15, 0, 1 );

    db.add_partition( c1, p_0_10, site_loc, p_type, s_type );
    db.add_partition( c2, p_11_15, site_loc, p_type, s_type );

    // insert transaction
    cell_key_ranges ckr;
    ckr.table_id = order_table_id;
    ckr.col_id_start = 0;
    ckr.col_id_end = 1;
    ckr.row_id_start = 0;
    ckr.row_id_end = 15;

    cell_key ck;
    ck.table_id = order_table_id;
    ck.col_id = 0;
    ck.row_id = 0;

    snapshot_vector              c2_state = empty_version;
    std::vector<cell_key_ranges> c2_read_set = {};
    std::vector<cell_key_ranges> c2_write_set = {ckr};

    transaction_partition_holder* c2_holder = db.get_partitions_with_begin(
        c2, c2_state, c2_write_set, c2_read_set, false /* allow missing */ );
    EXPECT_NE( c2_holder, nullptr );

    snapshot_vector commit_1_version;
    if( enable_db ) {
        for( uint64_t rec = 0; rec <= 15; rec++ ) {
            ck.row_id = rec;
            bool insert_ok = c2_holder->insert_int64_data( ck, (int64_t) rec );
            EXPECT_TRUE( insert_ok );
        }
        // commit
        c2_state = c2_holder->commit_transaction();

        set_snapshot_version( commit_1_version, p_0_10, 1 );
        set_snapshot_version( commit_1_version, p_11_15, 1 );
        EXPECT_EQ( commit_1_version, c2_state );
    }
    delete c2_holder;

    ckr.row_id_start = 6;
    ckr.row_id_end = 6;

    c2_read_set.clear();
    c2_write_set.clear();
    c2_read_set = {ckr};

    auto group_info = db.get_partitions_for_operations(
        c2, c2_write_set, c2_read_set, partition_lock_mode::lock,
        false /* allow missing */ );
    std::vector<std::shared_ptr<partition_payload>> found_payloads =
        group_info.payloads_;
    EXPECT_EQ( 1, found_payloads.size() );
    auto p_0_10_payload = found_payloads.at( 0 );
    EXPECT_EQ( p_0_10, p_0_10_payload->identifier_ );

    // split partition
    partition_column_identifier p_0_5 =
        create_partition_column_identifier( order_table_id, 0, 5, 0, 1 );
    partition_column_identifier p_6_10 =
        create_partition_column_identifier( order_table_id, 6, 10, 0, 1 );

    snapshot_vector split_vector =
        db.split_partition( c2, c2_state, p_0_10_payload, 6, k_unassigned_col,
                            p_type, p_type, s_type, s_type );
    snapshot_vector expected_split_vector = c2_state;
    set_snapshot_version( expected_split_vector, p_0_10, 2 );
    set_snapshot_version( expected_split_vector, p_0_5, 2 );
    set_snapshot_version( expected_split_vector, p_6_10, 2 );

    if( enable_db ) {
        EXPECT_EQ( expected_split_vector, split_vector );
    }
    c2_state = split_vector;

    c2_read_set.clear();
    c2_write_set.clear();
    ckr.row_id_start = 0;
    ckr.row_id_end = 9;
    c2_read_set.emplace_back( ckr );

    c2_holder = db.get_partitions_with_begin(
        c2, c2_state, c2_write_set, c2_read_set, false /* allow missing */ );
    EXPECT_NE( c2_holder, nullptr );
    if( enable_db ) {
        for( uint64_t rec = 0; rec < 10; rec++ ) {
            ck.row_id = rec;
            auto read_r = c2_holder->get_int64_data( ck );
            EXPECT_TRUE( std::get<0>( read_r ) );
            EXPECT_EQ( rec, std::get<1>( read_r ) );
        }
        c2_state = c2_holder->commit_transaction();
        EXPECT_EQ( expected_split_vector, c2_state );
    }
    delete c2_holder;

    // merge partition
    partition_column_identifier p_6_15 =
        create_partition_column_identifier( order_table_id, 6, 15, 0, 1 );

    c2_read_set.clear();
    c2_write_set.clear();
    ckr.row_id_start = 10;
    ckr.row_id_end = 11;

    c2_read_set.emplace_back( ckr );

    group_info = db.get_partitions_for_operations(
        c2, c2_write_set, c2_read_set, partition_lock_mode::lock,
        false /* allow missing */ );
    found_payloads = group_info.payloads_;
    EXPECT_EQ( 2, found_payloads.size() );
    auto p_6_10_payload = found_payloads.at( 0 );
    EXPECT_EQ( p_6_10, p_6_10_payload->identifier_ );
    auto p_11_15_payload = found_payloads.at( 1 );
    EXPECT_EQ( p_11_15, p_11_15_payload->identifier_ );

    DVLOG( 20 ) << "Calling merge partition in test";

    snapshot_vector merge_vector = db.merge_partition(
        c2, c2_state, p_6_10_payload, p_11_15_payload, p_type, s_type );
    snapshot_vector expected_merge_vector = expected_split_vector;
    set_snapshot_version( expected_merge_vector, p_6_10, 3 );
    set_snapshot_version( expected_merge_vector, p_11_15, 2 );
    set_snapshot_version( expected_merge_vector, p_6_15, 3 );

    if( enable_db ) {
        EXPECT_EQ( expected_merge_vector, merge_vector );
    }

    c2_state = merge_vector;

    c2_read_set.clear();
    c2_write_set.clear();
    ckr.row_id_start = 6;
    ckr.row_id_end = 15;
    c2_read_set.emplace_back( ckr );

    c2_holder = db.get_partitions_with_begin(
        c2, c2_state, c2_write_set, c2_read_set, false /* allow missing */ );
    EXPECT_NE( c2_holder, nullptr );
    if( enable_db ) {
        for( uint64_t rec = 6; rec < 15; rec++ ) {
            ck.row_id = rec;
            auto read_r = c2_holder->get_int64_data( ck );
            EXPECT_TRUE( std::get<0>( read_r ) );
            EXPECT_EQ( rec, std::get<1>( read_r ) );
        }
        c2_state = c2_holder->commit_transaction();
        EXPECT_EQ( expected_merge_vector, c2_state );
    }
    delete c2_holder;

    c2_read_set.clear();
    c2_write_set.clear();
    ckr.row_id_start = 0;
    ckr.row_id_end = 5;
    c2_write_set.emplace_back( ckr );

    group_info = db.get_partitions_for_operations(
        c2, c2_write_set, c2_read_set, partition_lock_mode::lock,
        false /* allow missing */ );
    found_payloads = group_info.payloads_;
    EXPECT_EQ( 1, found_payloads.size() );
    EXPECT_EQ( p_0_5, found_payloads.at( 0 )->identifier_ );
    snapshot_vector remaster_version =
        db.remaster_partitions( c2, c2_state, found_payloads, 2 );
    if( enable_db ) {
        snapshot_vector expected_remaster_version = c2_state;
        set_snapshot_version( expected_remaster_version, p_0_5, 2 );
    }

    c2_write_set.clear();
    c2_read_set.clear();
    c2_write_set.push_back( ckr );
    c2_holder = db.get_partitions_with_begin(
        c2, c2_state, c2_write_set, c2_read_set, false /* allow missing */ );
    EXPECT_NE( c2_holder, nullptr );

    if( enable_db ) {
        EXPECT_FALSE( c2_holder->does_site_master_write_set(
            db.get_database()->get_site_location() ) );
    }
    c2_state = c2_holder->abort_transaction();
    delete c2_holder;
}

TEST_F( single_site_db_test, enable_db_table_creation_test ) {
    single_site_table_creation_test<true>();
}
TEST_F( single_site_db_test, disable_db_table_creation_test ) {
    single_site_table_creation_test<false>();
}

TEST_F( single_site_db_test, enable_db_txn_ops_row ) {
    single_site_db_txn_ops<true>( partition_type::type::ROW );
}
TEST_F( single_site_db_test, disable_db_txn_ops_row ) {
    single_site_db_txn_ops<false>( partition_type::type::ROW );
}
TEST_F( single_site_db_test, enable_db_txn_ops_col ) {
    single_site_db_txn_ops<true>( partition_type::type::COLUMN );
}
TEST_F( single_site_db_test, disable_db_txn_ops_col ) {
    single_site_db_txn_ops<false>( partition_type::type::COLUMN );
}
TEST_F( single_site_db_test, enable_db_txn_ops_sorted_col ) {
    single_site_db_txn_ops<true>( partition_type::type::SORTED_COLUMN );
}
TEST_F( single_site_db_test, disable_db_txn_ops_sorted_col ) {
    single_site_db_txn_ops<false>( partition_type::type::SORTED_COLUMN );
}
TEST_F( single_site_db_test, enable_db_txn_ops_multi_col ) {
    single_site_db_txn_ops<true>( partition_type::type::MULTI_COLUMN );
}
TEST_F( single_site_db_test, disable_db_txn_ops_multi_col ) {
    single_site_db_txn_ops<false>( partition_type::type::MULTI_COLUMN );
}
TEST_F( single_site_db_test, enable_db_txn_ops_sorted_multi_col ) {
    single_site_db_txn_ops<true>( partition_type::type::SORTED_MULTI_COLUMN );
}
TEST_F( single_site_db_test, disable_db_txn_ops_sorted_multi_col ) {
    single_site_db_txn_ops<false>( partition_type::type::SORTED_MULTI_COLUMN );
}


TEST_F( single_site_db_test, enable_db_change_partition_row ) {
    single_site_db_change_partition<true>( partition_type::type::ROW );
}
TEST_F( single_site_db_test, disable_db_change_partition_row ) {
    single_site_db_change_partition<false>( partition_type::type::ROW );
}
TEST_F( single_site_db_test, enable_db_change_partition_col ) {
    single_site_db_change_partition<true>( partition_type::type::COLUMN );
}
TEST_F( single_site_db_test, disable_db_change_partition_col ) {
    single_site_db_change_partition<false>( partition_type::type::COLUMN );
}
TEST_F( single_site_db_test, enable_db_change_partition_sorted_col ) {
    single_site_db_change_partition<true>(
        partition_type::type::SORTED_COLUMN );
}
TEST_F( single_site_db_test, disable_db_change_partition_sorted_col ) {
    single_site_db_change_partition<false>(
        partition_type::type::SORTED_COLUMN );
}
TEST_F( single_site_db_test, enable_db_change_partition_multi_col ) {
    single_site_db_change_partition<true>( partition_type::type::MULTI_COLUMN );
}
TEST_F( single_site_db_test, disable_db_change_partition_multi_col ) {
    single_site_db_change_partition<false>( partition_type::type::MULTI_COLUMN );
}
TEST_F( single_site_db_test, enable_db_change_partition_sorted_multi_col ) {
    single_site_db_change_partition<true>(
        partition_type::type::SORTED_MULTI_COLUMN );
}
TEST_F( single_site_db_test, disable_db_change_partition_sorted_multi_col ) {
    single_site_db_change_partition<false>(
        partition_type::type::SORTED_MULTI_COLUMN );
}


void db_abstraction_table_creation( const db_abstraction_type&  db_type,
                                    const partition_type::type& p_type ) {
    uint32_t site_loc = 1;
    uint32_t num_tables = 5;
    uint32_t num_clients = 10;
    uint32_t gc_sleep_time = 10;

    uint32_t default_partition_size = 10;
    uint32_t default_column_size = 2;
    uint32_t col_start = 0;
    uint32_t col_end = 4;

    std::vector<cell_data_type> col_types = {
        cell_data_type::INT64, cell_data_type::INT64, cell_data_type::INT64,
        cell_data_type::INT64, cell_data_type::INT64};

    db_abstraction* db =
        create_db_abstraction( construct_db_abstraction_configs( db_type ) );

    db->init(
        make_no_op_update_destination_generator(), make_update_enqueuers(),
        create_tables_metadata( num_tables, site_loc, num_clients,
                                gc_sleep_time, false /* enable sec storage */,
                                "/tmp/" ) );

    int32_t num_records_in_chain = k_num_records_in_chain;
    int32_t num_records_in_snapshot_chain = k_num_records_in_snapshot_chain;

    // t0 = orders,
    auto order_meta = create_table_metadata(
        "orders", 0, ( col_end - col_start ) + 1, col_types,
        num_records_in_chain, num_records_in_snapshot_chain, site_loc,
        default_partition_size, default_column_size, default_partition_size,
        default_column_size, p_type, storage_tier_type::type::MEMORY,
        false /* enable sec storage */, "/tmp/" );
    auto shopping_meta = create_table_metadata(
        "shopping_cart", 1, ( col_end - col_start ) + 1, col_types,
        num_records_in_chain, num_records_in_snapshot_chain, site_loc,
        default_partition_size, default_column_size,
        default_partition_size, default_column_size,
        p_type,
        storage_tier_type::type::MEMORY, false /* enable sec storage */,
        "/tmp/" );

    uint32_t order_table_id = db->create_table( order_meta );
    EXPECT_EQ( 0, order_table_id );

    // should get the old table back
    uint32_t order_table_id_1 = db->create_table( order_meta );
    EXPECT_EQ( order_table_id, order_table_id_1 );

    uint32_t shopping_cart_table_id = db->create_table( shopping_meta );
    EXPECT_EQ( 1, shopping_cart_table_id );

    delete db;
}

void db_abstraction_db_txn_ops( const db_abstraction_type& db_type,
                                const partition_type::type&      p_type ) {
    uint32_t site_loc = 1;
    uint32_t num_tables = 5;
    uint32_t num_clients = 10;
    uint32_t gc_sleep_time = 10;

    uint32_t default_partition_size = 10;
    uint32_t default_column_size = 2;
    uint32_t col_start = 0;
    uint32_t col_end = 4;

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;

    std::vector<cell_data_type> col_types = {
        cell_data_type::INT64, cell_data_type::INT64, cell_data_type::INT64,
        cell_data_type::INT64, cell_data_type::INT64};

    db_abstraction* db =
        create_db_abstraction( construct_db_abstraction_configs( db_type ) );

    db->init(
        make_no_op_update_destination_generator(), make_update_enqueuers(),
        create_tables_metadata( num_tables, site_loc, num_clients,
                                gc_sleep_time, false /* enable sec storage */,
                                "/tmp/" ) );

    int32_t num_records_in_chain = k_num_records_in_chain;
    int32_t num_records_in_snapshot_chain = k_num_records_in_snapshot_chain;

    // t0 = orders,
    auto order_meta = create_table_metadata(
        "orders", 0, ( col_end - col_start ) + 1, col_types,
        num_records_in_chain, num_records_in_snapshot_chain, site_loc,
        default_partition_size, default_column_size, default_partition_size,
        default_column_size, p_type, storage_tier_type::type::MEMORY,
        false /* enable sec storage */, "/tmp/" );

    uint32_t order_table_id = db->create_table( order_meta );
    EXPECT_EQ( 0, order_table_id );

    uint32_t c1 = 1;
    uint32_t c2 = 2;

    snapshot_vector       empty_version;
    // add some partitions
    partition_column_identifier p_0_10 =
        create_partition_column_identifier( order_table_id, 0, 10, 0, 1 );
    partition_column_identifier p_11_15 =
        create_partition_column_identifier( order_table_id, 11, 15, 0, 1 );
    partition_column_identifier p_16_20 =
        create_partition_column_identifier( order_table_id, 16, 20, 0, 1 );
    db->add_partition( c1, p_0_10, site_loc, p_type, s_type );
    db->add_partition( c2, p_11_15, site_loc, p_type, s_type );
    db->add_partition( c1, p_16_20, site_loc, p_type, s_type );

    // read only that won't see anything
    // begin
    cell_key_ranges ckr;
    ckr.table_id = order_table_id;
    ckr.col_id_start = 0;
    ckr.col_id_end = 1;
    ckr.row_id_start = 0;
    ckr.row_id_end = 1;

    cell_key ck;
    ck.table_id = order_table_id;
    ck.col_id = 0;
    ck.row_id = 1;

    snapshot_vector                  c1_state = empty_version;
    std::vector<cell_key_ranges>     c1_read_set = {ckr};
    std::vector<cell_key_ranges>     c1_write_set = {};
    transaction_partition_holder*    c1_holder = db->get_partitions_with_begin(
        c1, c1_state, c1_write_set, c1_read_set, false /* allow missing */ );
    EXPECT_NE( c1_holder, nullptr );

    if( db_type != db_abstraction_type::SS_DB ) {
        ck.row_id = 1;
        auto read_r = c1_holder->get_int64_data( ck );
        EXPECT_FALSE( std::get<0>( read_r ) );
        // commit
        c1_state = c1_holder->commit_transaction();
        EXPECT_EQ( 0, get_snapshot_version( c1_state, p_0_10 ) );
    }
    delete c1_holder;

    // write transaction that aborts
    snapshot_vector              c2_state = empty_version;
    std::vector<cell_key_ranges>     c2_read_set = {};
    std::vector<cell_key_ranges>     c2_write_set;
    ckr.row_id_start = 10;
    ckr.row_id_end = 12;
    c2_write_set.push_back( ckr );
    ckr.row_id_start = 16;
    ckr.row_id_end = 18;
    c2_write_set.push_back( ckr );

    transaction_partition_holder* c2_holder = db->get_partitions_with_begin(
        c2, c2_state, c2_write_set, c2_read_set, false /* allow missing */ );

    if( db_type != db_abstraction_type::SS_DB ) {
      ck.row_id = 10;
      bool write_ok = c2_holder->insert_int64_data( ck, 0 );
      EXPECT_TRUE( write_ok );
      c2_state = c2_holder->abort_transaction();
      EXPECT_EQ( empty_version, c2_state );
    }
    delete c2_holder;

    snapshot_vector commit_1_version;

    c2_read_set.clear();
    c2_write_set.clear();
    ckr.row_id_start = 0;
    ckr.row_id_end = 2;
    c2_write_set.push_back( ckr );
    ckr.row_id_start = 16;
    ckr.row_id_end = 20;
    c2_write_set.push_back( ckr );
    // write transaction that does something
    c2_holder = db->get_partitions_with_begin(
        c2, c2_state, c2_write_set, c2_read_set, false /* allow missing */ );
    if( db_type != db_abstraction_type::SS_DB ) {
        ck.row_id = 1;
        bool write_ok = c2_holder->insert_int64_data( ck, 1 );
        EXPECT_TRUE( write_ok );
        ck.row_id = 20;
        write_ok = c2_holder->insert_int64_data( ck, 20 );
        EXPECT_TRUE( write_ok );
        // commit
        c2_state = c2_holder->commit_transaction();

        set_snapshot_version( commit_1_version, p_0_10, 1 );
        set_snapshot_version( commit_1_version, p_16_20, 1 );
        if( db_type != db_abstraction_type::PLAIN_DB ) {
            EXPECT_EQ( commit_1_version, c2_state );
        }
    }
    delete c2_holder;

    c1_state = c2_state;

    // read should see change
    c1_read_set.clear();
    c1_write_set.clear();
    ckr.row_id_start = 0;
    ckr.row_id_end = 2;
    c1_read_set.push_back( ckr );
    c1_holder = db->get_partitions_with_begin(
        c1, c1_state, c1_write_set, c1_read_set, false /* allow missing */ );

    if( db_type != db_abstraction_type::SS_DB ) {
        ck.row_id = 1;
        auto read_r = c1_holder->get_int64_data( ck );
        EXPECT_TRUE( std::get<0>( read_r ) );
        EXPECT_EQ( 1, std::get<1>( read_r ) );
        // commit
        c1_state = c1_holder->commit_transaction();
        if( db_type != db_abstraction_type::PLAIN_DB ) {
            EXPECT_EQ( commit_1_version, c1_state );
        }
    }
    delete c1_holder;
    delete db;
}

void db_abstraction_db_change_partition( const db_abstraction_type&  db_type,
                                         const partition_type::type& p_type ) {
    uint32_t site_loc = 1;
    uint32_t num_tables = 5;
    uint32_t num_clients = 10;
    uint32_t gc_sleep_time = 10;

    uint32_t default_partition_size = 10;
    uint32_t default_column_size = 2;
    uint32_t col_start = 0;
    uint32_t col_end = 4;

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;

    std::vector<cell_data_type> col_types = {
        cell_data_type::INT64, cell_data_type::INT64, cell_data_type::INT64,
        cell_data_type::INT64, cell_data_type::INT64};

    db_abstraction* db =
        create_db_abstraction( construct_db_abstraction_configs( db_type ) );

    db->init(
        make_no_op_update_destination_generator(), make_update_enqueuers(),
        create_tables_metadata( num_tables, site_loc, num_clients,
                                gc_sleep_time, false /* enable sec storage */,
                                "/tmp/" ) );

    int32_t num_records_in_chain = k_num_records_in_chain;
    int32_t num_records_in_snapshot_chain = k_num_records_in_snapshot_chain;

    // t0 = orders,
    auto order_meta = create_table_metadata(
        "orders", 0, ( col_end - col_start ) + 1, col_types,
        num_records_in_chain, num_records_in_snapshot_chain, site_loc,
        default_partition_size, default_column_size, default_partition_size,
        default_column_size, p_type, storage_tier_type::type::MEMORY,
        false /* enable sec storage */, "/tmp/" );

    uint32_t order_table_id = db->create_table( order_meta );
    EXPECT_EQ( 0, order_table_id );


    uint32_t c1 = 1;
    uint32_t c2 = 2;

    snapshot_vector       empty_version;

    // add some partitions
    partition_column_identifier p_0_10 =
        create_partition_column_identifier( order_table_id, 0, 10, 0, 1 );
    partition_column_identifier p_11_15 =
        create_partition_column_identifier( order_table_id, 11, 15, 0, 1 );

    db->add_partition( c1, p_0_10, site_loc, p_type, s_type );
    db->add_partition( c2, p_11_15, site_loc, p_type, s_type );

    // insert transaction
    cell_key_ranges ckr;
    cell_key ck;

    ckr.table_id = order_table_id;
    ckr.row_id_start = 0;
    ckr.row_id_end = 15;
    ckr.col_id_start = 0;
    ckr.col_id_end = 1;

    ck.table_id = order_table_id;
    ck.row_id = 0;
    ck.col_id = 0;

    snapshot_vector              c2_state = empty_version;
    std::vector<cell_key_ranges>     c2_read_set = {};
    std::vector<cell_key_ranges>     c2_write_set = {};

    c2_write_set.emplace_back( ckr );

    transaction_partition_holder* c2_holder = db->get_partitions_with_begin(
        c2, c2_state, c2_write_set, c2_read_set, false /* allow missing */ );

    if (db_type != db_abstraction_type::SS_DB) {
        for( uint64_t rec = 0; rec <= 15; rec++ ) {
          ck.row_id = rec;
          bool    write_ok = c2_holder->insert_int64_data( ck, rec );
          EXPECT_TRUE( write_ok );
        }
        // commit
        c2_state = c2_holder->commit_transaction();
    }
    delete c2_holder;

    if (db_type == db_abstraction_type::PLAIN_DB) {
        return;
    }

    snapshot_vector commit_1_version;
    set_snapshot_version( commit_1_version, p_0_10, 1 );
    set_snapshot_version( commit_1_version, p_11_15, 1 );

    if ( db_type != db_abstraction_type::SS_DB) {
        EXPECT_EQ( commit_1_version, c2_state );
    }

    ck.row_id = 6;
    ck.col_id = 0;

    // split partition
    partition_column_identifier p_0_5 =
        create_partition_column_identifier( order_table_id, 0, 5, 0, 1 );
    partition_column_identifier p_6_10 =
        create_partition_column_identifier( order_table_id, 6, 10, 0, 1 );

    snapshot_vector split_vector =
        db->split_partition( c2, c2_state, ck, false /*split horizontally*/ );
    snapshot_vector expected_split_vector = c2_state;
    set_snapshot_version( expected_split_vector, p_0_10, 2 );
    set_snapshot_version( expected_split_vector, p_0_5, 2 );
    set_snapshot_version( expected_split_vector, p_6_10, 2 );
    if (db_type != db_abstraction_type::SS_DB) {
        EXPECT_EQ( expected_split_vector, split_vector );
    }


    c2_state = split_vector;

    ckr.row_id_start = 0;
    ckr.row_id_end = 9;

    c2_read_set.clear();
    c2_write_set.clear();
    c2_read_set.emplace_back( ckr );

    c2_holder = db->get_partitions_with_begin(
        c2, c2_state, c2_write_set, c2_read_set, false /* allow missing */ );
    if( db_type != db_abstraction_type::SS_DB ) {
        for( uint64_t rec = 0; rec < 10; rec++ ) {
            ck.row_id = rec;
            auto read_r = c2_holder->get_int64_data( ck );
            EXPECT_TRUE( std::get<0>( read_r ) );
            EXPECT_EQ( rec, std::get<1>( read_r ) );
        }
        c2_state = c2_holder->commit_transaction();
        EXPECT_EQ( expected_split_vector, c2_state );
    }
    delete c2_holder;

    // merge partition
    partition_column_identifier p_6_15 =
        create_partition_column_identifier( order_table_id, 6, 15, 0, 1 );

    ck.row_id = 11;
    ck.col_id = 0;

    snapshot_vector merge_vector =
        db->merge_partition( c2, c2_state, ck, false /*split horizontally*/ );
    snapshot_vector expected_merge_vector = expected_split_vector;
    set_snapshot_version( expected_merge_vector, p_6_10, 3 );
    set_snapshot_version( expected_merge_vector, p_11_15, 2 );
    set_snapshot_version( expected_merge_vector, p_6_15, 3 );
    if (db_type != db_abstraction_type::SS_DB) {
        EXPECT_EQ( expected_merge_vector, merge_vector );
    }

    c2_read_set.clear();
    c2_write_set.clear();

    ckr.row_id_start  = 6;
    ckr.row_id_end  = 14;
    c2_read_set.push_back( ckr );

    c2_state = merge_vector;

    c2_holder = db->get_partitions_with_begin(
        c2, c2_state, c2_write_set, c2_read_set, false /* allow missing */ );
    if( db_type != db_abstraction_type::SS_DB ) {
        for( uint64_t rec = 6; rec < 15; rec++ ) {
          ck.row_id = rec;
          auto read_r = c2_holder->get_int64_data( ck );
          EXPECT_TRUE( std::get<0>( read_r ) );
          EXPECT_EQ( rec, std::get<1>( read_r ) );
        }
        c2_state = c2_holder->commit_transaction();
        EXPECT_EQ( expected_merge_vector, c2_state );
    }
    delete c2_holder;

    ckr.row_id_start = 0;
    ckr.row_id_end = 5;
    std::vector<cell_key_ranges> remaster_ckrs = {ckr};
    // now we can remaster
    snapshot_vector remaster_version =
        db->remaster_partitions( c2, c2_state, remaster_ckrs, 2 );
    snapshot_vector expected_remaster_version = c2_state;
    set_snapshot_version( expected_remaster_version, p_0_5, 2 );

    c2_write_set.clear();
    c2_read_set.clear();
    ckr.row_id_start = 0;
    ckr.row_id_end =0 ;
    c2_write_set.push_back( ckr );
    c2_holder = db->get_partitions_with_begin(
        c2, c2_state, c2_write_set, c2_read_set, false /* allow missing */ );
    if( db_type != db_abstraction_type::SS_DB ) {
        EXPECT_FALSE( c2_holder->does_site_master_write_set( site_loc ) );
        c2_state = c2_holder->abort_transaction();
    }
    delete c2_holder;

    delete db;
#if 0
#endif
}

TEST_F( single_site_db_test, single_site_db_abstraction_test_row ) {
    db_abstraction_table_creation( db_abstraction_type::SINGLE_SITE_DB,
                                   partition_type::type::ROW );
    db_abstraction_db_txn_ops( db_abstraction_type::SINGLE_SITE_DB,
                               partition_type::type::ROW );
    db_abstraction_db_change_partition( db_abstraction_type::SINGLE_SITE_DB,
                                        partition_type::type::ROW );
}
TEST_F( single_site_db_test, ss_db_abstraction_test_row ) {
    db_abstraction_table_creation( db_abstraction_type::SS_DB,
                                   partition_type::type::ROW );
    db_abstraction_db_txn_ops( db_abstraction_type::SS_DB,
                               partition_type::type::ROW );
    db_abstraction_db_change_partition( db_abstraction_type::SS_DB,
                                        partition_type::type::ROW );
}
TEST_F( single_site_db_test, plain_db_abstraction_test_row ) {
    db_abstraction_table_creation( db_abstraction_type::PLAIN_DB,
                                   partition_type::type::ROW );
    db_abstraction_db_txn_ops( db_abstraction_type::PLAIN_DB,
                               partition_type::type::ROW );
    db_abstraction_db_change_partition( db_abstraction_type::PLAIN_DB,
                                        partition_type::type::ROW );
}

TEST_F( single_site_db_test, single_site_db_abstraction_test_col ) {
    db_abstraction_table_creation( db_abstraction_type::SINGLE_SITE_DB,
                                   partition_type::type::COLUMN );
    db_abstraction_db_txn_ops( db_abstraction_type::SINGLE_SITE_DB,
                               partition_type::type::COLUMN );
    db_abstraction_db_change_partition( db_abstraction_type::SINGLE_SITE_DB,
                                        partition_type::type::COLUMN );
}
TEST_F( single_site_db_test, ss_db_abstraction_test_col ) {
    db_abstraction_table_creation( db_abstraction_type::SS_DB,
                                   partition_type::type::COLUMN );
    db_abstraction_db_txn_ops( db_abstraction_type::SS_DB,
                               partition_type::type::COLUMN );
    db_abstraction_db_change_partition( db_abstraction_type::SS_DB,
                                        partition_type::type::COLUMN );
}
TEST_F( single_site_db_test, plain_db_abstraction_test_col ) {
    db_abstraction_table_creation( db_abstraction_type::PLAIN_DB,
                                   partition_type::type::COLUMN );
    db_abstraction_db_txn_ops( db_abstraction_type::PLAIN_DB,
                               partition_type::type::COLUMN );
    db_abstraction_db_change_partition( db_abstraction_type::PLAIN_DB,
                                        partition_type::type::COLUMN );
}

TEST_F( single_site_db_test, single_site_db_abstraction_test_sorted_col ) {
    db_abstraction_table_creation( db_abstraction_type::SINGLE_SITE_DB,
                                   partition_type::type::SORTED_COLUMN );
    db_abstraction_db_txn_ops( db_abstraction_type::SINGLE_SITE_DB,
                               partition_type::type::SORTED_COLUMN );
    db_abstraction_db_change_partition( db_abstraction_type::SINGLE_SITE_DB,
                                        partition_type::type::SORTED_COLUMN );
}
TEST_F( single_site_db_test, ss_db_abstraction_test_sorted_col ) {
    db_abstraction_table_creation( db_abstraction_type::SS_DB,
                                   partition_type::type::SORTED_COLUMN );
    db_abstraction_db_txn_ops( db_abstraction_type::SS_DB,
                               partition_type::type::SORTED_COLUMN );
    db_abstraction_db_change_partition( db_abstraction_type::SS_DB,
                                        partition_type::type::SORTED_COLUMN );
}
TEST_F( single_site_db_test, plain_db_abstraction_test_sorted_col ) {
    db_abstraction_table_creation( db_abstraction_type::PLAIN_DB,
                                   partition_type::type::SORTED_COLUMN );
    db_abstraction_db_txn_ops( db_abstraction_type::PLAIN_DB,
                               partition_type::type::SORTED_COLUMN );
    db_abstraction_db_change_partition( db_abstraction_type::PLAIN_DB,
                                        partition_type::type::SORTED_COLUMN );
}

TEST_F( single_site_db_test, single_site_db_abstraction_test_multi_col ) {
    db_abstraction_table_creation( db_abstraction_type::SINGLE_SITE_DB,
                                   partition_type::type::MULTI_COLUMN );
    db_abstraction_db_txn_ops( db_abstraction_type::SINGLE_SITE_DB,
                               partition_type::type::MULTI_COLUMN );
    db_abstraction_db_change_partition( db_abstraction_type::SINGLE_SITE_DB,
                                        partition_type::type::MULTI_COLUMN );
}
TEST_F( single_site_db_test, ss_db_abstraction_test_multi_col ) {
    db_abstraction_table_creation( db_abstraction_type::SS_DB,
                                   partition_type::type::MULTI_COLUMN );
    db_abstraction_db_txn_ops( db_abstraction_type::SS_DB,
                               partition_type::type::MULTI_COLUMN );
    db_abstraction_db_change_partition( db_abstraction_type::SS_DB,
                                        partition_type::type::MULTI_COLUMN );
}
TEST_F( single_site_db_test, plain_db_abstraction_test_multi_col ) {
    db_abstraction_table_creation( db_abstraction_type::PLAIN_DB,
                                   partition_type::type::MULTI_COLUMN );
    db_abstraction_db_txn_ops( db_abstraction_type::PLAIN_DB,
                               partition_type::type::MULTI_COLUMN );
    db_abstraction_db_change_partition( db_abstraction_type::PLAIN_DB,
                                        partition_type::type::MULTI_COLUMN );
}

TEST_F( single_site_db_test, single_site_db_abstraction_test_sorted_multi_col ) {
    db_abstraction_table_creation( db_abstraction_type::SINGLE_SITE_DB,
                                   partition_type::type::SORTED_MULTI_COLUMN );
    db_abstraction_db_txn_ops( db_abstraction_type::SINGLE_SITE_DB,
                               partition_type::type::SORTED_MULTI_COLUMN );
    db_abstraction_db_change_partition( db_abstraction_type::SINGLE_SITE_DB,
                                        partition_type::type::SORTED_MULTI_COLUMN );
}
TEST_F( single_site_db_test, ss_db_abstraction_test_sorted_multi_col ) {
    db_abstraction_table_creation( db_abstraction_type::SS_DB,
                                   partition_type::type::SORTED_MULTI_COLUMN );
    db_abstraction_db_txn_ops( db_abstraction_type::SS_DB,
                               partition_type::type::SORTED_MULTI_COLUMN );
    db_abstraction_db_change_partition( db_abstraction_type::SS_DB,
                                        partition_type::type::SORTED_MULTI_COLUMN );
}
TEST_F( single_site_db_test, plain_db_abstraction_test_sorted_multi_col ) {
    db_abstraction_table_creation( db_abstraction_type::PLAIN_DB,
                                   partition_type::type::SORTED_MULTI_COLUMN );
    db_abstraction_db_txn_ops( db_abstraction_type::PLAIN_DB,
                               partition_type::type::SORTED_MULTI_COLUMN );
    db_abstraction_db_change_partition( db_abstraction_type::PLAIN_DB,
                                        partition_type::type::SORTED_MULTI_COLUMN );
}

