#define GTEST_HAS_TR1_TUPLE 0

#include <stdlib.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/data-site/db/db.h"
#include "../src/persistence/data_site_persistence_manager.h"
#include "../src/persistence/persistence_editor.h"
#include "../src/persistence/persistence_utils.h"
#include "../src/persistence/site_selector_persistence_manager.h"

class db_persistence_test : public ::testing::Test {};

class test_data_location_rewriter : public data_location_rewriter_interface {
   public:
    std::vector<table_metadata> tables_metadata_;
    int                         num_sites_;

    std::vector<std::vector<propagation_configuration>> prop_configs_;
    std::shared_ptr<update_destination_generator>       update_generator_;
    std::shared_ptr<update_enqueuers>                   enqueuers_;

    partition_column_identifier_map_t<int> rewrite_masters_;
    partition_column_identifier_map_t<std::unordered_set<int>>
        rewrite_replicas_;
    partition_column_identifier_map_t<partition_type::type>
        rewrite_master_types_;
    partition_column_identifier_map_t<storage_tier_type::type>
        rewrite_master_storage_types_;

    std::unordered_map<int, partition_type::type>    replica_types_;
    std::unordered_map<int, storage_tier_type::type> storage_types_;

    test_data_location_rewriter(
        const std::vector<table_metadata>& tables_metadata, int num_sites,
        const std::vector<std::vector<propagation_configuration>>& prop_configs,
        const persistence_configs&                                 per_configs,
        std::shared_ptr<update_destination_generator> update_generator,
        std::shared_ptr<update_enqueuers>             enqueuers )
        : data_location_rewriter_interface( tables_metadata, num_sites,
                                            prop_configs, per_configs,
                                            update_generator, enqueuers ) {
        rewrite_masters_.clear();
        rewrite_replicas_.clear();
        rewrite_master_types_.clear();
        rewrite_master_storage_types_.clear();
        replica_types_.clear();
        storage_types_.clear();
    }

    void rewrite_partition_locations(
        partition_locations_map& partition_locations ) {
        for( auto& table_entry : partition_locations ) {
            for( auto& part_entry : table_entry.second ) {
                auto pid = part_entry.first;
                auto master_found = rewrite_masters_.find( pid );
                EXPECT_NE( master_found, rewrite_masters_.end() );
                auto replicas_found = rewrite_replicas_.find( pid );
                EXPECT_NE( replicas_found, rewrite_replicas_.end() );
                auto type_found = rewrite_master_types_.find( pid );
                EXPECT_NE( type_found, rewrite_master_types_.end() );
                auto storage_type_found =
                    rewrite_master_storage_types_.find( pid );
                EXPECT_NE( storage_type_found,
                           rewrite_master_storage_types_.end() );

                auto& part_loc = part_entry.second;

                part_loc.replica_locations_.clear();
                part_loc.partition_types_.clear();
                part_loc.storage_types_.clear();

                part_loc.master_location_ = master_found->second;
                part_loc.partition_types_[master_found->second] =
                    type_found->second;
                part_loc.storage_types_[master_found->second] =
                    storage_type_found->second;

                for( int r : replicas_found->second ) {
                    auto replica_type_found = replica_types_.find( r );
                    EXPECT_NE( replica_type_found, replica_types_.end() );

                    auto storage_type_found = storage_types_.find( r );
                    EXPECT_NE( storage_type_found, storage_types_.end() );

                    part_loc.replica_locations_.emplace( r );
                    part_loc.partition_types_.emplace(
                        r, replica_type_found->second );
                    part_loc.storage_types_.emplace(
                        r, storage_type_found->second );
                }
                EXPECT_EQ( part_loc.partition_types_.size(),
                           part_loc.replica_locations_.size() + 1 );
                EXPECT_EQ( part_loc.storage_types_.size(),
                           part_loc.replica_locations_.size() + 1 );
            }
        }
    }
};

void persist_data_site( const persistence_configs&  per_configs,
                        const partition_type::type& part_type ) {
    std::string base_str = std::to_string( (uint64_t) std::time( nullptr ) ) +
                           "-" + std::to_string( rand() % 1000 );

    std::string output_dir_1 = "/tmp/" + base_str + "-ds1";
    std::string output_dir_2 = "/tmp/" + base_str + "-ds2";
    std::string part_output_dir = "/tmp/" + base_str + "-part";

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;

    uint32_t site_loc = 0;
    uint32_t replica_site_loc = 1;
    uint32_t table_id = 0;
    uint32_t num_tables = 2;
    uint32_t default_partition_size = 10;
    uint32_t default_column_size = 2;
    uint32_t col_start = 0;
    uint32_t col_end = 4;
    uint32_t gc_sleep_time = 10;
    uint32_t num_clients = 10;
    int32_t  num_records_in_chain = 5;
    int32_t  num_records_in_snapshot_chain = 5;
    uint32_t c1 = 1;

    std::vector<cell_data_type> col_types = {
        cell_data_type::INT64, cell_data_type::STRING, cell_data_type::DOUBLE,
        cell_data_type::INT64, cell_data_type::INT64};

    std::shared_ptr<vector_update_destination> update_dest_1 =
        std::make_shared<vector_update_destination>( 0 );
    std::shared_ptr<vector_update_destination> update_dest_2 =
        std::make_shared<vector_update_destination>( 0 );

    auto prop_config_1 = update_dest_1->get_propagation_configuration();
    auto prop_config_2 = update_dest_2->get_propagation_configuration();

    std::shared_ptr<update_destination_generator> generator_1 =
        std::make_shared<update_destination_generator>(
            construct_update_destination_generator_configs( 1 ) );
    generator_1->add_update_destination( update_dest_1 );

    std::shared_ptr<update_destination_generator> generator_2 =
        std::make_shared<update_destination_generator>(
            construct_update_destination_generator_configs( 1 ) );
    generator_2->add_update_destination( update_dest_2 );

    std::vector<std::vector<propagation_configuration>>
        site_propagation_configs;
    site_propagation_configs.push_back( {prop_config_1} );
    site_propagation_configs.push_back( {prop_config_2} );

    db ori_db_1;
    ori_db_1.init( generator_1, make_update_enqueuers(),
                   create_tables_metadata(
                       num_tables, site_loc, num_clients, gc_sleep_time,
                       false /* enable sec storage */, "/tmp/" ) );
    db ori_db_2;
    ori_db_2.init( generator_2, make_update_enqueuers(),
                   create_tables_metadata(
                       num_tables, replica_site_loc, num_clients, gc_sleep_time,
                       false /* enable sec storage */, "/tmp/" ) );

    auto t_metadata = create_table_metadata(
        "t", table_id, ( col_end - col_start ) + 1, col_types,
        num_records_in_chain, num_records_in_snapshot_chain, site_loc,
        default_partition_size, default_column_size, default_partition_size,
        default_column_size, part_type, s_type, false /* enable sec storage */,
        "/tmp/" );

    uint32_t order_table_id =
        ori_db_1.get_tables()->create_table( t_metadata );
    EXPECT_EQ( 0, order_table_id );

    order_table_id = ori_db_2.get_tables()->create_table( t_metadata );
    EXPECT_EQ( 0, order_table_id );

    partition_column_identifier p_0_10 =
        create_partition_column_identifier( order_table_id, 0, 10, 0, 2 );
    partition_column_identifier p_11_15 =
        create_partition_column_identifier( order_table_id, 11, 15, 0, 4 );
    partition_column_identifier p_16_20 =
        create_partition_column_identifier( order_table_id, 16, 20, 0, 4 );

    ori_db_1.add_partition( c1, p_0_10, site_loc, part_type, s_type );
    ori_db_1.add_partition( c1, p_11_15, site_loc, part_type, s_type );
    ori_db_1.add_partition( c1, p_16_20, replica_site_loc, part_type, s_type );

    ori_db_2.add_partition( c1, p_16_20, replica_site_loc, part_type, s_type );

    std::vector<int64_t> cols_0 = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10,
                                   11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
    std::vector<std::string> cols_1 = {
        "zero",     "one",      "two",      "three",   "four",    "five",
        "six",      "seven",    "eight",    "nine",    "ten",     "eleven",
        "twelve",   "thirteen", "fourteen", "fifteen", "sixteen", "seventeen",
        "eighteen", "nineteen", "twenty"};
    std::vector<double> cols_2 = {0.0,  0.01, 0.02, 0.03, 0.04, 0.05, 0.06,
                                  0.07, 0.08, 0.09, 0.1,  0.11, 0.12, 0.13,
                                  0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.2};

    std::vector<int64_t>     cols_3;
    std::vector<int64_t>     cols_4;
    for( uint32_t pos = 0; pos < cols_0.size(); pos++ ) {
        cols_3.emplace_back( cols_0.at( pos ) * 3 );
        cols_4.emplace_back( cols_0.at( pos ) * 4 );
    }

    snapshot_vector              c1_state;
    partition_column_identifier_set     c1_read_set = {};
    partition_column_identifier_set     c1_inflight_set = {};
    partition_column_identifier_set     c1_write_set = {p_0_10, p_11_15};

    transaction_partition_holder* c1_holder =
        ori_db_1.get_partitions_with_begin( c1, c1_state, c1_write_set,
                                            c1_read_set, c1_inflight_set,
                                            partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    cell_identifier cid;
    cid.table_id_ = order_table_id;
    cid.col_id_ = 0;
    cid.key_ = 0;

    /*
        partition_column_identifier p_0_10 =
            create_partition_column_identifier( order_table_id, 0, 10, 0, 2 );
        partition_column_identifier p_11_15 =
            create_partition_column_identifier( order_table_id, 11, 15, 0, 4 );
        partition_column_identifier p_16_20 =
            create_partition_column_identifier( order_table_id, 16, 20, 0, 4 );

        std::vector<cell_data_type> col_types = {
            cell_data_type::INT64, cell_data_type::STRING,
       cell_data_type::DOUBLE,
            cell_data_type::INT64, cell_data_type::INT64};
    */

    // 0-10 rows, 0-2 cols
    for ( uint64_t row = 0; row <= 10; row++) {
        cid.key_ = row;

        cid.col_id_ = 0;
        bool insert_ok = c1_holder->insert_int64_data( cid, cols_0.at( row ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 1;
        insert_ok = c1_holder->insert_string_data( cid, cols_1.at( row ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 2;
        insert_ok = c1_holder->insert_double_data( cid, cols_2.at( row ) );
        EXPECT_TRUE( insert_ok );
    }

    // 11-15 rows, 0-4 cols
    for ( uint64_t row = 11; row <= 15; row++) {
        cid.key_ = row;

        cid.col_id_ = 0;
        bool insert_ok = c1_holder->insert_int64_data( cid, cols_0.at( row ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 1;
        insert_ok = c1_holder->insert_string_data( cid, cols_1.at( row ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 2;
        insert_ok = c1_holder->insert_double_data( cid, cols_2.at( row ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 3;
        insert_ok = c1_holder->insert_int64_data( cid, cols_3.at( row ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 4;
        insert_ok = c1_holder->insert_int64_data( cid, cols_4.at( row ) );
        EXPECT_TRUE( insert_ok );
    }

    // commit
    c1_state = c1_holder->commit_transaction();
    delete c1_holder;

    c1_write_set = {p_16_20};
    c1_holder = ori_db_2.get_partitions_with_begin(
        c1, c1_state, c1_write_set, c1_read_set, c1_inflight_set,
        partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    // 16-20 rows, 0-4 cols
    for ( uint64_t row = 16; row <= 20; row++) {
        cid.key_ = row;

        cid.col_id_ = 0;
        bool insert_ok = c1_holder->insert_int64_data( cid, cols_0.at( row ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 1;
        insert_ok = c1_holder->insert_string_data( cid, cols_1.at( row ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 2;
        insert_ok = c1_holder->insert_double_data( cid, cols_2.at( row ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 3;
        insert_ok = c1_holder->insert_int64_data( cid, cols_3.at( row ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 4;
        insert_ok = c1_holder->insert_int64_data( cid, cols_4.at( row ) );
        EXPECT_TRUE( insert_ok );
    }

    // commit
    c1_state = c1_holder->commit_transaction();
    delete c1_holder;

    data_site_persistence_manager persister_1(
        output_dir_1, part_output_dir, ori_db_1.get_tables(), generator_1,
        site_propagation_configs, per_configs, site_loc );
    persister_1.persist();

    data_site_persistence_manager persister_2(
        output_dir_2, part_output_dir, ori_db_2.get_tables(), generator_2,
        site_propagation_configs, per_configs, replica_site_loc );
    persister_2.persist();

    std::shared_ptr<vector_update_destination> restored_update_dest =
        std::make_shared<vector_update_destination>( 0 );
    auto restored_prop_config =
        restored_update_dest->get_propagation_configuration();

    std::shared_ptr<update_destination_generator> restored_generator =
        std::make_shared<update_destination_generator>(
            construct_update_destination_generator_configs( 1 ) );
    restored_generator->add_update_destination( restored_update_dest );

    db restored_db;
    restored_db.init( restored_generator, make_update_enqueuers(),
                      create_tables_metadata(
                          num_tables, site_loc, num_clients, gc_sleep_time,
                          false /* enable sec storage */, "/tmp/" ) );
    order_table_id = restored_db.get_tables()->create_table( t_metadata );
    EXPECT_EQ( 0, order_table_id );

    c1_write_set = {};
    c1_read_set = {p_0_10, p_11_15, p_16_20};

    c1_holder = restored_db.get_partitions_with_begin(
        c1, c1_state, c1_write_set, c1_read_set, c1_inflight_set,
        partition_lookup_operation::GET );
    EXPECT_EQ( c1_holder, nullptr );

    data_site_persistence_manager restorer(
        output_dir_1, part_output_dir, restored_db.get_tables(),
        restored_generator, site_propagation_configs, per_configs, site_loc );
    restorer.load();

    c1_holder = restored_db.get_partitions_with_begin(
        c1, c1_state, c1_write_set, c1_read_set, c1_inflight_set,
        partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    // 0-10 rows, 0-2 cols
    for( uint64_t row = 0; row <= 10; row++ ) {
        cid.key_ = row;

        cid.col_id_ = 0;
        auto read_col_0 = c1_holder->get_int64_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_0 ) );
        EXPECT_EQ( std::get<1>( read_col_0 ), cols_0.at( row ) );

        cid.col_id_ = 1;
        auto read_col_1 = c1_holder->get_string_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_1 ) );
        EXPECT_EQ( std::get<1>( read_col_1 ), cols_1.at( row ) );

        cid.col_id_ = 2;
        auto read_col_2 = c1_holder->get_double_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_2 ) );
        EXPECT_DOUBLE_EQ( std::get<1>( read_col_2 ), cols_2.at( row ) );
    }

    // 11-15 rows, 0-4 cols
    for( uint64_t row = 11; row <= 15; row++ ) {
        cid.key_ = row;

        cid.col_id_ = 0;
        auto read_col_0 = c1_holder->get_int64_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_0 ) );
        EXPECT_EQ( std::get<1>( read_col_0 ), cols_0.at( row ) );

        cid.col_id_ = 1;
        auto read_col_1 = c1_holder->get_string_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_1 ) );
        EXPECT_EQ( std::get<1>( read_col_1 ), cols_1.at( row ) );

        cid.col_id_ = 2;
        auto read_col_2 = c1_holder->get_double_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_2 ) );
        EXPECT_DOUBLE_EQ( std::get<1>( read_col_2 ), cols_2.at( row ) );

        cid.col_id_ = 3;
        auto read_col_3 = c1_holder->get_int64_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_3 ) );
        EXPECT_EQ( std::get<1>( read_col_3 ), cols_3.at( row ) );

        cid.col_id_ = 4;
        auto read_col_4 = c1_holder->get_int64_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_4 ) );
        EXPECT_EQ( std::get<1>( read_col_4 ), cols_4.at( row ) );
    }

    // 16-20 rows, 0-4 cols
    for( uint64_t row = 16; row <= 20; row++ ) {
        cid.key_ = row;

        cid.col_id_ = 0;
        auto read_col_0 = c1_holder->get_int64_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_0 ) );
        EXPECT_EQ( std::get<1>( read_col_0 ), cols_0.at( row ) );

        cid.col_id_ = 1;
        auto read_col_1 = c1_holder->get_string_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_1 ) );
        EXPECT_EQ( std::get<1>( read_col_1 ), cols_1.at( row ) );

        cid.col_id_ = 2;
        auto read_col_2 = c1_holder->get_double_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_2 ) );
        EXPECT_DOUBLE_EQ( std::get<1>( read_col_2 ), cols_2.at( row ) );

        cid.col_id_ = 3;
        auto read_col_3 = c1_holder->get_int64_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_3 ) );
        EXPECT_EQ( std::get<1>( read_col_3 ), cols_3.at( row ) );

        cid.col_id_ = 4;
        auto read_col_4 = c1_holder->get_int64_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_4 ) );
        EXPECT_EQ( std::get<1>( read_col_4 ), cols_4.at( row ) );
    }

    // commit
    c1_state = c1_holder->commit_transaction();
    delete c1_holder;
}

void persist_site_selector( const persistence_configs& per_configs ) {
    std::string base_str = std::to_string( (uint64_t) std::time( nullptr ) ) +
                           "-" + std::to_string( rand() % 1000 );

    std::string output_dir = "/tmp/" + base_str + "-ss";

    uint32_t site_loc = 0;
    uint32_t replica_site_loc = 1;
    uint32_t table_id = 0;
    uint32_t default_partition_size = 10;
    uint32_t default_column_size = 2;
    uint32_t col_start = 0;
    uint32_t col_end = 4;
    int32_t  num_records_in_chain = 5;
    int32_t  num_records_in_snapshot_chain = 5;

    std::vector<cell_data_type> col_types = {
        cell_data_type::INT64, cell_data_type::STRING, cell_data_type::DOUBLE,
        cell_data_type::INT64, cell_data_type::INT64};

    std::shared_ptr<vector_update_destination> update_dest_1 =
        std::make_shared<vector_update_destination>( 0 );
    std::shared_ptr<vector_update_destination> update_dest_2 =
        std::make_shared<vector_update_destination>( 0 );

    auto prop_config_1 = update_dest_1->get_propagation_configuration();
    auto prop_config_2 = update_dest_2->get_propagation_configuration();

    std::vector<std::vector<propagation_configuration>>
        site_propagation_configs;
    site_propagation_configs.push_back( {prop_config_1} );
    site_propagation_configs.push_back( {prop_config_2} );

    multi_version_partition_data_location_table ori_loc_table;
    auto t_metadata = create_table_metadata(
        "t", table_id, ( col_end - col_start ) + 1, col_types,
        num_records_in_chain, num_records_in_snapshot_chain, site_loc,
        default_partition_size, default_column_size, default_partition_size,
        default_column_size, partition_type::type::COLUMN,
        storage_tier_type::type::MEMORY, false /* enable sec storage */,
        "/tmp/" );

    auto created = ori_loc_table.create_table( t_metadata );
    EXPECT_EQ( created, table_id );

    partition_lock_mode lock_mode = partition_lock_mode::no_lock;

    auto pid_0_9 = create_partition_column_identifier( table_id, 0, 9, 0, 2 );
    auto pid_10_19 =
        create_partition_column_identifier( table_id, 10, 19, 0, 4 );

    std::vector<partition_column_identifier> pids = {pid_0_9, pid_10_19};
    for( const auto& pid : pids ) {
        auto part = std::make_shared<partition_payload>( pid );
        auto part_loc = std::make_shared<partition_location_information>();
        part_loc->master_location_ = site_loc;
        part_loc->replica_locations_.emplace( replica_site_loc );

        part_loc->partition_types_[site_loc] = partition_type::type::ROW;
        part_loc->partition_types_[replica_site_loc] =
            partition_type::type::SORTED_COLUMN;
        part_loc->storage_types_[site_loc] = storage_tier_type::type::MEMORY;
        part_loc->storage_types_[replica_site_loc] =
            storage_tier_type::type::DISK;

        part_loc->update_destination_slot_ = 0;
        part_loc->version_ = 1;
        part->set_location_information( part_loc );

        auto ret = ori_loc_table.insert_partition( part, lock_mode );
        EXPECT_NE( ret, nullptr );
        EXPECT_EQ( ret->identifier_, pid );
    }

    site_selector_persistence_manager persister(
        output_dir, &ori_loc_table, site_propagation_configs, per_configs );
    persister.persist();

    multi_version_partition_data_location_table restored_loc_table;
    created = restored_loc_table.create_table( t_metadata );
    EXPECT_EQ( created, table_id );
    site_selector_persistence_manager restorer( output_dir, &restored_loc_table,
                                                site_propagation_configs,
                                                per_configs );
    restorer.load();

    for( const auto& pid : pids ) {
        auto read_part = restored_loc_table.get_partition( pid, lock_mode );
        EXPECT_NE( read_part, nullptr );
        EXPECT_EQ( pid, read_part->identifier_ );
        auto part_loc = read_part->get_location_information();
        EXPECT_NE( part_loc, nullptr );
        EXPECT_EQ( part_loc->master_location_, site_loc );
        EXPECT_EQ( part_loc->replica_locations_.size(), 1 );
        EXPECT_EQ( part_loc->replica_locations_.count( replica_site_loc ), 1 );
        EXPECT_EQ( part_loc->partition_types_.at( site_loc ),
                   partition_type::type::ROW );
        EXPECT_EQ( part_loc->partition_types_.at( replica_site_loc ),
                   partition_type::type::SORTED_COLUMN );
        EXPECT_EQ( part_loc->storage_types_.at( site_loc ),
                   storage_tier_type::type::MEMORY );
        EXPECT_EQ( part_loc->storage_types_.at( replica_site_loc ),
                   storage_tier_type::type::DISK );

        EXPECT_EQ( part_loc->update_destination_slot_, 0 );
    }
}

void persist_editor( const persistence_configs&  per_configs,
                     const partition_type::type& part_type ) {

    auto og_k_enable_secondary_storage = k_enable_secondary_storage;
    k_enable_secondary_storage = false;

    std::string base_str = std::to_string( (uint64_t) std::time( nullptr ) ) +
                           "-" + std::to_string( rand() % 1000 );

    std::string out_ds_dir = "/tmp/" + base_str + "-ds";
    create_directory( out_ds_dir );
    std::string output_dir_1 = out_ds_dir + "/0";
    std::string output_dir_2 = out_ds_dir + "/1";
    std::string part_output_dir = "/tmp/" + base_str + "-part";
    std::string ss_output_dir = "/tmp/" + base_str + "-ss";

    std::string edit_out_ds_dir = "/tmp/" + base_str + "-ds-e";
    create_directory( edit_out_ds_dir );
    std::string edit_output_dir_1 = edit_out_ds_dir + "/0";
    std::string edit_output_dir_2 = edit_out_ds_dir + "/1";
    std::string edit_part_output_dir = "/tmp/" + base_str + "-part-e";
    std::string edit_ss_output_dir = "/tmp/" + base_str + "-ss-e";

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;

    uint32_t site_loc = 0;
    uint32_t replica_site_loc = 1;
    uint32_t table_id = 0;
    uint32_t num_tables = 2;
    uint32_t default_partition_size = 10;
    uint32_t default_column_size = 2;
    uint32_t col_start = 0;
    uint32_t col_end = 4;
    uint32_t gc_sleep_time = 10;
    uint32_t num_clients = 10;
    int32_t  num_records_in_chain = 5;
    int32_t  num_records_in_snapshot_chain = 5;
    uint32_t c1 = 1;

    std::vector<cell_data_type> col_types = {
        cell_data_type::INT64, cell_data_type::STRING, cell_data_type::DOUBLE,
        cell_data_type::INT64, cell_data_type::INT64};

    std::shared_ptr<vector_update_destination> update_dest_1 =
        std::make_shared<vector_update_destination>( 0 );
    std::shared_ptr<vector_update_destination> update_dest_2 =
        std::make_shared<vector_update_destination>( 0 );

    auto prop_config_1 = update_dest_1->get_propagation_configuration();
    auto prop_config_2 = update_dest_2->get_propagation_configuration();

    std::shared_ptr<update_destination_generator> generator_1 =
        std::make_shared<update_destination_generator>(
            construct_update_destination_generator_configs( 1 ) );
    generator_1->add_update_destination( update_dest_1 );

    std::shared_ptr<update_destination_generator> generator_2 =
        std::make_shared<update_destination_generator>(
            construct_update_destination_generator_configs( 1 ) );
    generator_2->add_update_destination( update_dest_2 );

    std::vector<std::vector<propagation_configuration>>
        site_propagation_configs;
    site_propagation_configs.push_back( {prop_config_1} );
    site_propagation_configs.push_back( {prop_config_2} );

    db ori_db_1;
    ori_db_1.init( generator_1, make_update_enqueuers(),
                   create_tables_metadata(
                       num_tables, site_loc, num_clients, gc_sleep_time,
                       false /* enable sec storage */, "/tmp/" ) );
    db ori_db_2;
    ori_db_2.init( generator_2, make_update_enqueuers(),
                   create_tables_metadata(
                       num_tables, replica_site_loc, num_clients, gc_sleep_time,
                       false /* enable sec storage */, "/tmp/" ) );

    auto t_metadata = create_table_metadata(
        "t", table_id, ( col_end - col_start ) + 1, col_types,
        num_records_in_chain, num_records_in_snapshot_chain, site_loc,
        default_partition_size, default_column_size, default_partition_size,
        default_column_size, part_type, s_type, false /* enable sec storage */,
        "/tmp/" );

    uint32_t order_table_id = ori_db_1.get_tables()->create_table( t_metadata );
    EXPECT_EQ( 0, order_table_id );

    order_table_id = ori_db_2.get_tables()->create_table( t_metadata );
    EXPECT_EQ( 0, order_table_id );

    partition_column_identifier p_0_10 =
        create_partition_column_identifier( order_table_id, 0, 10, 0, 2 );
    partition_column_identifier p_11_15 =
        create_partition_column_identifier( order_table_id, 11, 15, 0, 4 );
    partition_column_identifier p_16_20 =
        create_partition_column_identifier( order_table_id, 16, 20, 0, 4 );

    ori_db_1.add_partition( c1, p_0_10, site_loc, part_type, s_type );
    ori_db_1.add_partition( c1, p_11_15, site_loc, part_type, s_type );
    ori_db_1.add_partition( c1, p_16_20, replica_site_loc, part_type, s_type );

    ori_db_2.add_partition( c1, p_16_20, replica_site_loc, part_type, s_type );

    std::vector<int64_t> cols_0 = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10,
                                   11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
    std::vector<std::string> cols_1 = {
        "zero",     "one",      "two",      "three",   "four",    "five",
        "six",      "seven",    "eight",    "nine",    "ten",     "eleven",
        "twelve",   "thirteen", "fourteen", "fifteen", "sixteen", "seventeen",
        "eighteen", "nineteen", "twenty"};
    std::vector<double> cols_2 = {0.0,  0.01, 0.02, 0.03, 0.04, 0.05, 0.06,
                                  0.07, 0.08, 0.09, 0.1,  0.11, 0.12, 0.13,
                                  0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.2};

    std::vector<int64_t> cols_3;
    std::vector<int64_t> cols_4;
    for( uint32_t pos = 0; pos < cols_0.size(); pos++ ) {
        cols_3.emplace_back( cols_0.at( pos ) * 3 );
        cols_4.emplace_back( cols_0.at( pos ) * 4 );
    }

    snapshot_vector              c1_state;
    partition_column_identifier_set     c1_read_set = {};
    partition_column_identifier_set     c1_inflight_set = {};
    partition_column_identifier_set     c1_write_set = {p_0_10, p_11_15};

    transaction_partition_holder* c1_holder =
        ori_db_1.get_partitions_with_begin( c1, c1_state, c1_write_set,
                                            c1_read_set, c1_inflight_set,
                                            partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    cell_identifier cid;
    cid.table_id_ = order_table_id;
    cid.col_id_ = 0;
    cid.key_ = 0;

    // 0-10 rows, 0-2 cols
    for ( uint64_t row = 0; row <= 10; row++) {
        cid.key_ = row;

        cid.col_id_ = 0;
        bool insert_ok = c1_holder->insert_int64_data( cid, cols_0.at( row ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 1;
        insert_ok = c1_holder->insert_string_data( cid, cols_1.at( row ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 2;
        insert_ok = c1_holder->insert_double_data( cid, cols_2.at( row ) );
        EXPECT_TRUE( insert_ok );
    }

    // 11-15 rows, 0-4 cols
    for ( uint64_t row = 11; row <= 15; row++) {
        cid.key_ = row;

        cid.col_id_ = 0;
        bool insert_ok = c1_holder->insert_int64_data( cid, cols_0.at( row ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 1;
        insert_ok = c1_holder->insert_string_data( cid, cols_1.at( row ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 2;
        insert_ok = c1_holder->insert_double_data( cid, cols_2.at( row ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 3;
        insert_ok = c1_holder->insert_int64_data( cid, cols_3.at( row ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 4;
        insert_ok = c1_holder->insert_int64_data( cid, cols_4.at( row ) );
        EXPECT_TRUE( insert_ok );
    }

    // commit
    c1_state = c1_holder->commit_transaction();
    delete c1_holder;

    c1_write_set = {p_16_20};
    c1_holder = ori_db_2.get_partitions_with_begin(
        c1, c1_state, c1_write_set, c1_read_set, c1_inflight_set,
        partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    // 16-20 rows, 0-4 cols
    for ( uint64_t row = 16; row <= 20; row++) {
        cid.key_ = row;

        cid.col_id_ = 0;
        bool insert_ok = c1_holder->insert_int64_data( cid, cols_0.at( row ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 1;
        insert_ok = c1_holder->insert_string_data( cid, cols_1.at( row ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 2;
        insert_ok = c1_holder->insert_double_data( cid, cols_2.at( row ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 3;
        insert_ok = c1_holder->insert_int64_data( cid, cols_3.at( row ) );
        EXPECT_TRUE( insert_ok );

        cid.col_id_ = 4;
        insert_ok = c1_holder->insert_int64_data( cid, cols_4.at( row ) );
        EXPECT_TRUE( insert_ok );
    }

    // commit
    c1_state = c1_holder->commit_transaction();
    delete c1_holder;

    multi_version_partition_data_location_table ori_loc_table;
    auto created = ori_loc_table.create_table( t_metadata );
    EXPECT_EQ( created, table_id );

    partition_lock_mode lock_mode = partition_lock_mode::no_lock;

    std::vector<partition_column_identifier> pids = {p_0_10, p_11_15, p_16_20};
    for( const auto& pid : pids ) {
        auto part = std::make_shared<partition_payload>( pid );
        auto part_loc = std::make_shared<partition_location_information>();
        part_loc->master_location_ = site_loc;
        part_loc->partition_types_[site_loc] = part_type;
        part_loc->storage_types_[site_loc] = s_type;
        if( pid == p_16_20 ) {
            part_loc->replica_locations_.emplace( site_loc );
            part_loc->master_location_ = replica_site_loc;
            part_loc->partition_types_.emplace( replica_site_loc,
                                                partition_type::COLUMN );
            part_loc->storage_types_.emplace( replica_site_loc,
                                              storage_tier_type::type::DISK );
        }
        EXPECT_EQ( part_loc->partition_types_.size(),
                   part_loc->replica_locations_.size() + 1 );
        EXPECT_EQ( part_loc->storage_types_.size(),
                   part_loc->replica_locations_.size() + 1 );

        part_loc->update_destination_slot_ = 0;
        part_loc->version_ = 1;
        part->set_location_information( part_loc );

        auto ret = ori_loc_table.insert_partition( part, lock_mode );
        EXPECT_NE( ret, nullptr );
        EXPECT_EQ( ret->identifier_, pid );
    }

    data_site_persistence_manager persister_1(
        output_dir_1, part_output_dir, ori_db_1.get_tables(), generator_1,
        site_propagation_configs, per_configs, site_loc );
    persister_1.persist();

    data_site_persistence_manager persister_2(
        output_dir_2, part_output_dir, ori_db_2.get_tables(), generator_2,
        site_propagation_configs, per_configs, replica_site_loc );
    persister_2.persist();

    site_selector_persistence_manager ss_persister(
        ss_output_dir, &ori_loc_table, site_propagation_configs, per_configs );
    ss_persister.persist();

    std::shared_ptr<vector_update_destination> restored_update_dest =
        std::make_shared<vector_update_destination>( 0 );
    auto restored_prop_config =
        restored_update_dest->get_propagation_configuration();

    std::shared_ptr<update_destination_generator> restored_generator =
        std::make_shared<update_destination_generator>(
            construct_update_destination_generator_configs( 1 ) );
    restored_generator->add_update_destination( restored_update_dest );

    std::vector<table_metadata> ts_metadata = {t_metadata};

    test_data_location_rewriter rewriter(
        ts_metadata, 2, site_propagation_configs, per_configs,
        restored_generator, make_update_enqueuers() );
    rewriter.rewrite_masters_[p_0_10] = 1;
    rewriter.rewrite_masters_[p_11_15] = 1;
    rewriter.rewrite_masters_[p_16_20] = 1;
    rewriter.rewrite_master_types_[p_0_10] = partition_type::ROW;
    rewriter.rewrite_master_types_[p_11_15] = partition_type::COLUMN;
    rewriter.rewrite_master_types_[p_16_20] = partition_type::SORTED_COLUMN;

    rewriter.rewrite_master_storage_types_[p_0_10] =
        storage_tier_type::type::MEMORY;
    rewriter.rewrite_master_storage_types_[p_11_15] =
        storage_tier_type::type::MEMORY;
    rewriter.rewrite_master_storage_types_[p_16_20] =
        storage_tier_type::type::DISK;

    rewriter.rewrite_replicas_[p_0_10] = {0};
    rewriter.rewrite_replicas_[p_11_15] = {};
    rewriter.rewrite_replicas_[p_16_20] = {0};

    rewriter.replica_types_[0] = part_type;
    rewriter.storage_types_[0] = s_type;

    persistence_editor editor( &rewriter, ss_output_dir, out_ds_dir,
                               part_output_dir, edit_ss_output_dir,
                               edit_out_ds_dir, edit_part_output_dir );
    editor.rewrite();

    db restored_db;
    restored_db.init( restored_generator, make_update_enqueuers(),
                      create_tables_metadata(
                          num_tables, site_loc, num_clients, gc_sleep_time,
                          false /* enable sec storage */, "/tmp/" ) );
    order_table_id = restored_db.get_tables()->create_table( t_metadata );
    EXPECT_EQ( 0, order_table_id );

    c1_write_set = {};
    c1_read_set = {p_0_10, p_11_15, p_16_20};

    c1_holder = restored_db.get_partitions_with_begin(
        c1, c1_state, c1_write_set, c1_read_set, c1_inflight_set,
        partition_lookup_operation::GET );
    EXPECT_EQ( c1_holder, nullptr );

    data_site_persistence_manager restorer(
        edit_output_dir_1, edit_part_output_dir, restored_db.get_tables(),
        restored_generator, site_propagation_configs, per_configs, site_loc );
    restorer.load();

    multi_version_partition_data_location_table restored_loc_table;
    created = restored_loc_table.create_table( t_metadata );
    EXPECT_EQ( created, table_id );
    site_selector_persistence_manager ss_restorer(
        edit_ss_output_dir, &restored_loc_table, site_propagation_configs,
        per_configs );
    ss_restorer.load();

    for( const auto& pid : pids ) {
        auto read_part = restored_loc_table.get_partition( pid, lock_mode );
        EXPECT_NE( read_part, nullptr );
        EXPECT_EQ( pid, read_part->identifier_ );
        auto part_loc = read_part->get_location_information();
        EXPECT_NE( part_loc, nullptr );
        EXPECT_EQ( part_loc->master_location_, 1 );

        if( pid == p_0_10 ) {
            EXPECT_EQ( part_loc->partition_types_.at( 1 ),
                       partition_type::ROW );
            EXPECT_EQ( part_loc->storage_types_.at( 1 ),
                       storage_tier_type::type::MEMORY );
        } else if( pid == p_11_15 ) {
            EXPECT_EQ( part_loc->partition_types_.at( 1 ),
                       partition_type::COLUMN );
            EXPECT_EQ( part_loc->storage_types_.at( 1 ),
                       storage_tier_type::type::MEMORY );
        } else if( pid == p_16_20 ) {
            EXPECT_EQ( part_loc->partition_types_.at( 1 ),
                       partition_type::SORTED_COLUMN );
            EXPECT_EQ( part_loc->storage_types_.at( 1 ),
                       storage_tier_type::type::DISK );
        }

        if ( pid != p_11_15) {
            EXPECT_EQ( part_loc->replica_locations_.size(), 1 );
            EXPECT_EQ( part_loc->replica_locations_.count( 0 ),
                       1 );
            EXPECT_EQ( part_type, part_loc->partition_types_.at( 0 ) );
            EXPECT_EQ( s_type, part_loc->storage_types_.at( 0 ) );

        } else {
            EXPECT_EQ( part_loc->replica_locations_.size(), 0 );
        }
        EXPECT_EQ( part_loc->update_destination_slot_, 0 );
    }

    c1_holder = restored_db.get_partitions_with_begin(
        c1, c1_state, c1_write_set, c1_read_set, c1_inflight_set,
        partition_lookup_operation::GET );
    EXPECT_EQ( c1_holder, nullptr );


    c1_read_set = {p_0_10, p_16_20};
    c1_holder = restored_db.get_partitions_with_begin(
        c1, c1_state, c1_write_set, c1_read_set, c1_inflight_set,
        partition_lookup_operation::GET );
    EXPECT_NE( c1_holder, nullptr );

    // 0-10 rows, 0-2 cols
    for( uint64_t row = 0; row <= 10; row++ ) {
        cid.key_ = row;

        cid.col_id_ = 0;
        auto read_col_0 = c1_holder->get_int64_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_0 ) );
        EXPECT_EQ( std::get<1>( read_col_0 ), cols_0.at( row ) );

        cid.col_id_ = 1;
        auto read_col_1 = c1_holder->get_string_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_1 ) );
        EXPECT_EQ( std::get<1>( read_col_1 ), cols_1.at( row ) );

        cid.col_id_ = 2;
        auto read_col_2 = c1_holder->get_double_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_2 ) );
        EXPECT_DOUBLE_EQ( std::get<1>( read_col_2 ), cols_2.at( row ) );
    }

    // 16-20 rows, 0-4 cols
    for( uint64_t row = 16; row <= 20; row++ ) {
        cid.key_ = row;

        cid.col_id_ = 0;
        auto read_col_0 = c1_holder->get_int64_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_0 ) );
        EXPECT_EQ( std::get<1>( read_col_0 ), cols_0.at( row ) );

        cid.col_id_ = 1;
        auto read_col_1 = c1_holder->get_string_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_1 ) );
        EXPECT_EQ( std::get<1>( read_col_1 ), cols_1.at( row ) );

        cid.col_id_ = 2;
        auto read_col_2 = c1_holder->get_double_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_2 ) );
        EXPECT_DOUBLE_EQ( std::get<1>( read_col_2 ), cols_2.at( row ) );

        cid.col_id_ = 3;
        auto read_col_3 = c1_holder->get_int64_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_3 ) );
        EXPECT_EQ( std::get<1>( read_col_3 ), cols_3.at( row ) );

        cid.col_id_ = 4;
        auto read_col_4 = c1_holder->get_int64_data( cid );
        EXPECT_TRUE( std::get<0>( read_col_4 ) );
        EXPECT_EQ( std::get<1>( read_col_4 ), cols_4.at( row ) );
    }

    // commit
    c1_state = c1_holder->commit_transaction();
    delete c1_holder;
#if 0
#endif

    k_enable_secondary_storage = og_k_enable_secondary_storage;
}

TEST_F( db_persistence_test, persist_data_site_no_chunk_row ) {
    persistence_configs c = construct_persistence_configs( false, false, 1 );
    persist_data_site( c, partition_type::type::ROW );
}
TEST_F( db_persistence_test, persist_data_site_chunk_row ) {
    persistence_configs c = construct_persistence_configs( true, true, 1 );
    persist_data_site( c, partition_type::type::ROW );
}
TEST_F( db_persistence_test, persist_data_site_no_chunk_column ) {
    persistence_configs c = construct_persistence_configs( false, false, 1 );
    persist_data_site( c, partition_type::type::COLUMN );
}
TEST_F( db_persistence_test, persist_data_site_chunk_column ) {
    persistence_configs c = construct_persistence_configs( true, true, 1 );
    persist_data_site( c, partition_type::type::COLUMN );
}
TEST_F( db_persistence_test, persist_data_site_no_chunk_sorted_column ) {
    persistence_configs c = construct_persistence_configs( false, false, 1 );
    persist_data_site( c, partition_type::type::SORTED_COLUMN );
}
TEST_F( db_persistence_test, persist_data_site_chunk_sorted_column ) {
    persistence_configs c = construct_persistence_configs( true, true, 1 );
    persist_data_site( c, partition_type::type::SORTED_COLUMN );
}
TEST_F( db_persistence_test, persist_data_site_no_chunk_multi_column ) {
    persistence_configs c = construct_persistence_configs( false, false, 1 );
    persist_data_site( c, partition_type::type::MULTI_COLUMN );
}
TEST_F( db_persistence_test, persist_data_site_chunk_multi_column ) {
    persistence_configs c = construct_persistence_configs( true, true, 1 );
    persist_data_site( c, partition_type::type::MULTI_COLUMN );
}
TEST_F( db_persistence_test, persist_data_site_no_chunk_sorted_multi_column ) {
    persistence_configs c = construct_persistence_configs( false, false, 1 );
    persist_data_site( c, partition_type::type::SORTED_MULTI_COLUMN );
}
TEST_F( db_persistence_test, persist_data_site_chunk_sorted_multi_column ) {
    persistence_configs c = construct_persistence_configs( true, true, 1 );
    persist_data_site( c, partition_type::type::SORTED_MULTI_COLUMN );
}

TEST_F( db_persistence_test, persist_site_selector_no_chunk ) {
    persistence_configs c = construct_persistence_configs( false, false, 1 );
    persist_site_selector( c );
}
TEST_F( db_persistence_test, persist_site_selector_chunk ) {
    persistence_configs c = construct_persistence_configs( true, true, 1 );
    persist_site_selector( c );
}

TEST_F( db_persistence_test, persist_editor_no_chunk_row ) {
    persistence_configs c = construct_persistence_configs( false, false, 1 );
    persist_editor( c, partition_type::type::ROW );
}
TEST_F( db_persistence_test, persist_editor_no_chunk_column ) {
    persistence_configs c = construct_persistence_configs( false, false, 1 );
    persist_editor( c, partition_type::type::COLUMN );
}
TEST_F( db_persistence_test, persist_editor_no_chunk_sorted_column ) {
    persistence_configs c = construct_persistence_configs( false, false, 1 );
    persist_editor( c, partition_type::type::SORTED_COLUMN );
}
TEST_F( db_persistence_test, persist_editor_no_chunk_multi_column ) {
    persistence_configs c = construct_persistence_configs( false, false, 1 );
    persist_editor( c, partition_type::type::MULTI_COLUMN );
}
TEST_F( db_persistence_test, persist_editor_no_chunk_sorted_multi_column ) {
    persistence_configs c = construct_persistence_configs( false, false, 1 );
    persist_editor( c, partition_type::type::SORTED_MULTI_COLUMN );
}


TEST_F( db_persistence_test, persist_editor_chunk_row ) {
    persistence_configs c = construct_persistence_configs( true, true, 1 );
    persist_editor( c, partition_type::type::ROW );
}
TEST_F( db_persistence_test, persist_editor_chunk_column ) {
    persistence_configs c = construct_persistence_configs( true, true, 1 );
    persist_editor( c, partition_type::type::COLUMN );
}
TEST_F( db_persistence_test, persist_editor_chunk_sorted_column ) {
    persistence_configs c = construct_persistence_configs( true, true, 1 );
    persist_editor( c, partition_type::type::SORTED_COLUMN );
}
TEST_F( db_persistence_test, persist_editor_chunk_multi_column ) {
    persistence_configs c = construct_persistence_configs( true, true, 1 );
    persist_editor( c, partition_type::type::MULTI_COLUMN );
}
TEST_F( db_persistence_test, persist_editor_chunk_sorted_multi_column ) {
    persistence_configs c = construct_persistence_configs( true, true, 1 );
    persist_editor( c, partition_type::type::SORTED_MULTI_COLUMN );
}

TEST_F( db_persistence_test, persist_and_seek_test ) {
    std::string base_str = std::to_string( (uint64_t) std::time( nullptr ) ) +
                           "-" + std::to_string( rand() % 1000 );

    std::string file_name = "/tmp/" + base_str + "-seek.txt";

    data_persister persister( file_name );

    // let's write [0, 1, 2, 3, 4, 5, 6]
    persister.open();
    persister.write_i32( 0 );
    persister.write_i32( 1 );
    persister.write_i32( 2 );
    uint32_t pos_3 = persister.get_position();
    persister.write_i32( 3 );
    persister.write_i32( 4 );
    persister.write_i32( 5 );
    persister.write_i32( 6 );
    uint32_t pos_7 = persister.get_position();

    // lets change the 300 to a 6 and then append 7
    persister.seek_to_position( pos_3 );
    persister.write_i32( 300 );
    persister.seek_to_position( pos_7 );
    persister.write_i32( 7 );

    // so data is [0, 1, 2, 300, 4, 5, 6, 7]
    persister.close();

    data_reader reader( file_name );
    reader.open();

    uint32_t read_data = 0;
    reader.read_i32( &read_data );
    EXPECT_EQ( 0, read_data );
    reader.read_i32( &read_data );
    EXPECT_EQ( 1, read_data );
    reader.read_i32( &read_data );
    EXPECT_EQ( 2, read_data );

    uint32_t read_pos = reader.get_position();

    reader.read_i32( &read_data );
    EXPECT_EQ( 300, read_data );

    reader.read_i32( &read_data );
    EXPECT_EQ( 4, read_data );
    reader.read_i32( &read_data );
    EXPECT_EQ( 5, read_data );
    reader.read_i32( &read_data );
    EXPECT_EQ( 6, read_data );
    uint32_t read_7_pos = reader.get_position();

    reader.seek_to_position( read_pos );

    reader.read_i32( &read_data );
    EXPECT_EQ( 300, read_data );

    reader.seek_to_position( read_7_pos );

    reader.read_i32( &read_data );
    EXPECT_EQ( 7, read_data );


    reader.close();
}
