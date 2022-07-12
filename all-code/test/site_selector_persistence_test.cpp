#define GTEST_HAS_TR1_TUPLE 0

#include <stdlib.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/data-site/update-propagation/vector_update_destination.h"
#include "../src/persistence/site_selector_persistence_manager.h"

class site_selector_persistence_test : public ::testing::Test {};

std::shared_ptr<partition_payload> create_partition_payload(
    uint32_t table_id, uint64_t p_start, uint64_t p_end, uint32_t col_start,
    uint32_t col_end, uint32_t master_location,
    const partition_type::type&    master_type,
    const storage_tier_type::type& storage_type,
    const std::unordered_map<
        uint32_t, std::tuple<partition_type::type, storage_tier_type::type>>&
        replicas ) {
    auto p_id = create_partition_column_identifier( table_id, p_start, p_end,
                                                    col_start, col_end );
    auto payload = std::make_shared<partition_payload>( p_id );
    auto p_location = std::make_shared<partition_location_information>();
    p_location->master_location_ = master_location;
    p_location->partition_types_[ master_location ] = master_type;
    p_location->storage_types_[ master_location ] = storage_type;
    for( const auto& replica : replicas ) {
        p_location->replica_locations_.emplace( replica.first );
        p_location->partition_types_.emplace( replica.first,
                                              std::get<0>( replica.second ) );
        p_location->storage_types_.emplace( replica.first,
                                            std::get<1>( replica.second ) );
    }
    p_location->version_ = 1;
    bool swapped =
        payload->compare_and_swap_location_information( p_location, nullptr );
    EXPECT_TRUE( swapped );

    return payload;
}

void check_partitions_match( std::shared_ptr<partition_payload> expected,
                             std::shared_ptr<partition_payload> actual ) {
    EXPECT_NE( nullptr, actual );
    EXPECT_EQ( expected->identifier_, actual->identifier_ );

    auto expected_loc_info = expected->get_location_information();
    auto actual_loc_info = expected->get_location_information();

    EXPECT_NE( nullptr, actual_loc_info );
    EXPECT_EQ( 1, actual_loc_info->version_ );
    EXPECT_EQ( expected_loc_info->master_location_,
               actual_loc_info->master_location_ );
    EXPECT_EQ( expected_loc_info->replica_locations_.size(),
               actual_loc_info->replica_locations_.size() );

    for (const auto replica_entry : actual_loc_info->replica_locations_) {
        EXPECT_EQ(
            1, expected_loc_info->replica_locations_.count( replica_entry ) );
    }

    EXPECT_EQ( expected_loc_info->partition_types_.size(),
               actual_loc_info->partition_types_.size() );

    for( const auto replica_entry : actual_loc_info->partition_types_ ) {
        EXPECT_EQ( 1, expected_loc_info->partition_types_.count(
                          replica_entry.first ) );
        EXPECT_EQ( replica_entry.second, expected_loc_info->partition_types_.at(
                                             replica_entry.first ) );
    }

    EXPECT_EQ( expected_loc_info->storage_types_.size(),
               actual_loc_info->storage_types_.size() );

    for( const auto storage_entry : actual_loc_info->storage_types_ ) {
        EXPECT_EQ(
            1, expected_loc_info->storage_types_.count( storage_entry.first ) );
        EXPECT_EQ( storage_entry.second, expected_loc_info->storage_types_.at(
                                             storage_entry.first ) );
    }
}

void test_persistence( const persistence_configs& per_configs ) {
    std::string output_dir = "/tmp/" +
                             std::to_string( (uint64_t) std::time( nullptr ) ) +
                             "-" + std::to_string( rand() % 1000 ) + "-ss";

    uint32_t table_id = 0;

    uint32_t default_partition_size = 10;
    uint32_t default_column_size = 2;
    uint32_t col_start = 0;
    uint32_t col_end = 4;

    std::vector<cell_data_type> col_types = {
        cell_data_type::INT64, cell_data_type::INT64, cell_data_type::INT64,
        cell_data_type::INT64, cell_data_type::INT64};

    auto table_metadata = create_table_metadata(
        "t", table_id, ( col_end - col_start ) + 1, col_types,
        5 /*num_records_in_chain*/, 5 /*num_records_in_snapshot_chain*/,
        0 /*site_location*/, default_partition_size, default_column_size,
        default_partition_size, default_column_size,
        partition_type::type::COLUMN, storage_tier_type::type::MEMORY,
        false /* enable sec storage */, "/tmp/" );

    multi_version_partition_data_location_table persist_table;
    auto created = persist_table.create_table( table_metadata );
    EXPECT_EQ( created, table_id );

    partition_lock_mode lock_mode = partition_lock_mode::no_lock;

    auto p_0_9_payload = create_partition_payload(
        table_id, 0, 9, 0, 2, 1, partition_type::type::ROW,
        storage_tier_type::type::MEMORY, {} );
    auto inserted = persist_table.insert_partition( p_0_9_payload, lock_mode );
    inserted->unlock( lock_mode );

    auto p_10_19_payload = create_partition_payload(
        table_id, 10, 19, 0, 2, 0, partition_type::COLUMN,
        storage_tier_type::type::DISK,
        {{1, std::make_tuple<>( partition_type::type::ROW,
                                storage_tier_type::type::MEMORY )}} );
    inserted = persist_table.insert_partition( p_10_19_payload, lock_mode );
    inserted->unlock( lock_mode );

    auto p_20_25_payload = create_partition_payload(
        table_id, 20, 25, 1, 2, 1, partition_type::SORTED_COLUMN,
        storage_tier_type::type::DISK,
        {{0, std::make_tuple<>( partition_type::ROW,
                                storage_tier_type::type::MEMORY )},
         {2, std::make_tuple<>( partition_type::COLUMN,
                                storage_tier_type::type::DISK )}} );
    inserted = persist_table.insert_partition( p_20_25_payload, lock_mode );
    inserted->unlock( lock_mode );

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

    site_selector_persistence_manager persister(
        output_dir, &persist_table, site_propagation_configs, per_configs );
    persister.persist();

    multi_version_partition_data_location_table restore_table;
    created = restore_table.create_table( table_metadata );
    EXPECT_EQ( created, table_id );

    site_selector_persistence_manager restorer(
        output_dir, &restore_table, site_propagation_configs, per_configs );
    restorer.load();

    auto found_p_0_9 =
        restore_table.get_partition( p_0_9_payload->identifier_, lock_mode );
    check_partitions_match( p_0_9_payload, found_p_0_9 );
    found_p_0_9->unlock( lock_mode );

    auto found_p_10_19 =
        restore_table.get_partition( p_10_19_payload->identifier_, lock_mode );
    check_partitions_match( p_10_19_payload, found_p_10_19 );
    found_p_10_19->unlock( lock_mode );

    auto found_p_20_25 =
        restore_table.get_partition( p_20_25_payload->identifier_, lock_mode );
    check_partitions_match( p_20_25_payload, found_p_20_25 );
    found_p_20_25->unlock( lock_mode );
}

TEST_F( site_selector_persistence_test, persist_site_selctor ) {
    persistence_configs c = construct_persistence_configs( false, false, 1 );
    test_persistence( c );
}
TEST_F( site_selector_persistence_test, persist_site_selctor_chunked ) {
    persistence_configs c = construct_persistence_configs( true, true, 1 );
    test_persistence( c );
}
