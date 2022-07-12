#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/data-site/db/partition_metadata.h"
#include "../src/data-site/update-propagation/update_enqueuer_subscription_information.h"

class update_enqueuer_subscription_information_test : public ::testing::Test {};

TEST_F( update_enqueuer_subscription_information_test,
        partition_subscription_information_test ) {

    int                                    num_configs = 10;
    std::vector<propagation_configuration> prop_configs;
    for(int part = 0; part < num_configs; part++) {
        propagation_configuration pc;
        pc.type = propagation_type::VECTOR;
        pc.partition = part;
        pc.offset = part * part;

        prop_configs.push_back( pc );
    }

    uint32_t col_start = 0;
    uint32_t col_end = 10;

    partition_column_identifier pid =
        create_partition_column_identifier( 0, 0, 10, col_start, col_end );

    std::unordered_map<propagation_configuration, int64_t /*offset*/,
                       propagation_configuration_ignore_offset_hasher,
                       propagation_configuration_ignore_offset_equal_functor>
        begin_configs;
    begin_configs[prop_configs.at( 0 )] = 7;
    begin_configs[prop_configs.at( 1 )] = 3;
    begin_configs[prop_configs.at( 2 )] = 4;

    update_enqueuer_partition_subscription_information p_info( pid,
                                                               begin_configs );

    // state: 7, 3, 4

    EXPECT_EQ( 7, p_info.get_subscription_offset( prop_configs.at( 0 ) ) );
    EXPECT_EQ( 3, p_info.get_subscription_offset( prop_configs.at( 1 ) ) );
    EXPECT_EQ( 4, p_info.get_subscription_offset( prop_configs.at( 2 ) ) );
    EXPECT_EQ( K_NOT_COMMITTED,
               p_info.get_subscription_offset( prop_configs.at( 3 ) ) );

    // state: 7, 3, 4
    // less shouldn't have an effect, for add_new_source_information
    p_info.add_new_source_information( prop_configs.at( 0 ), 6 );
    EXPECT_EQ( 7, p_info.get_subscription_offset( prop_configs.at( 0 ) ) );
    // but it should for add_in_info_subscription_offset_information
    p_info.add_in_subscription_offset_information( prop_configs.at( 0 ),
                                                          6 );
    EXPECT_EQ( 6, p_info.get_subscription_offset( prop_configs.at( 0 ) ) );
    // more should not have an effect for add_in_info_subscription_offset_information
    // state: 6, 3, 4
    p_info.add_in_subscription_offset_information( prop_configs.at( 0 ),
                                                          7 );
    EXPECT_EQ( 6, p_info.get_subscription_offset( prop_configs.at( 0 ) ) );
    // more should have an affect for add_new_source
    // state: 6, 3, 4
    p_info.add_new_source_information( prop_configs.at( 0 ), 7 );
    EXPECT_EQ( 7, p_info.get_subscription_offset( prop_configs.at( 0 ) ) );

    // state: 7, 3, 4
    // more should
    p_info.add_new_source_information( prop_configs.at( 0 ), 9 );
    EXPECT_EQ( 9, p_info.get_subscription_offset( prop_configs.at( 0 ) ) );

    // state: 9, 3, 4
    p_info.add_new_source_information( prop_configs.at( 3 ), 21 );
    // state: 9, 3, 4, 21
    EXPECT_EQ( 21, p_info.get_subscription_offset( prop_configs.at( 3 ) ) );

    // less shouldn't have an effect
    // state: 9, 3, 4, 19
    p_info.remove_subscription_offset_information( prop_configs.at( 3 ), 19 );
    EXPECT_EQ( 21, p_info.get_subscription_offset( prop_configs.at( 3 ) ) );

    // state: 9, 3, 4, 19
    p_info.remove_subscription_offset_information( prop_configs.at( 3 ), 22 );
    // state: 9, 3, 4
    EXPECT_EQ( K_NOT_COMMITTED,
               p_info.get_subscription_offset( prop_configs.at( 3 ) ) );
}

TEST_F( update_enqueuer_subscription_information_test,
        subscription_information_test ) {

    int                                    num_configs = 10;
    std::vector<propagation_configuration> prop_configs;
    for(int part = 0; part < num_configs; part++) {
        propagation_configuration pc;
        pc.type = propagation_type::VECTOR;
        pc.partition = part;
        pc.offset = part * part;

        prop_configs.push_back( pc );
    }

    uint32_t col_start = 0;
    uint32_t col_end = 5;
    std::vector<cell_data_type> col_types;
    for( uint32_t col_id = col_start; col_id <= col_end; col_id++ ) {
        col_types.emplace_back( cell_data_type::INT64 );
    }

    int                                    num_pids = 10;
    std::vector<partition_column_identifier> pids;
    for( int part = 0; part < num_pids; part++ ) {
        uint64_t             start = num_pids * part;
        partition_column_identifier pid = create_partition_column_identifier(
            0, start, start + num_pids - 1, col_start, col_end );

        pids.push_back( pid );
    }

    update_enqueuer_subscription_information info;
    info.set_expected_number_of_tables( 1 );
    info.create_table( create_table_metadata(
        "orders", 0, col_end + 1, col_types, 1 /*num_records_in_chain*/,
        1 /*num_records_in_snapshot_chain*/ ) );

    // state:
    info.add_source( prop_configs.at( 0 ), 5 );
    EXPECT_EQ( K_NOT_COMMITTED, info.get_offset_of_partition_for_config(
                                    pids.at( 0 ), prop_configs.at( 0 ) ) );

    std::unordered_map<propagation_configuration, int64_t /*offset*/,
                       propagation_configuration_ignore_offset_hasher,
                       propagation_configuration_ignore_offset_equal_functor>
        begin_configs;
    begin_configs[prop_configs.at( 0 )] = 7;

    // state:
    info.add_partition_subscription_information( pids.at( 0 ), begin_configs );
    // state:  [0: 7]
    EXPECT_EQ( 7, info.get_offset_of_partition_for_config(
                      pids.at( 0 ), prop_configs.at( 0 ) ) );

    // state:  [0: 7]
    info.add_source( prop_configs.at( 1 ), 11 );
    // state:  [0: 7, 11]
    EXPECT_EQ( 11, info.get_offset_of_partition_for_config(
                       pids.at( 0 ), prop_configs.at( 1 ) ) );

    begin_configs[prop_configs.at( 0 )] = 9;
    begin_configs[prop_configs.at( 1 )] = 11;

    // state:  [0: 7, 11]
    info.add_partition_subscription_information( pids.at( 1 ), begin_configs );
    // state:  [0: 7, 11], [1: 9, 11]
    EXPECT_EQ( 9, info.get_offset_of_partition_for_config(
                      pids.at( 1 ), prop_configs.at( 0 ) ) );
    EXPECT_EQ( 11, info.get_offset_of_partition_for_config(
                       pids.at( 1 ), prop_configs.at( 1 ) ) );

    std::vector<partition_column_identifier> read_pids;
    read_pids.push_back( pids.at( 0 ) );
    read_pids.push_back( pids.at( 1 ) );

    // state:  [0: 7, 11], [1: 9, 11]
    EXPECT_EQ( 11, info.get_max_offset_of_partitions_for_config(
                       read_pids, prop_configs.at( 1 ) ) );

    // state:  [0: 7, 11], [1: 9, 11]
    read_pids.push_back( pids.at( 2 ) );
    EXPECT_EQ( K_NOT_COMMITTED, info.get_max_offset_of_partitions_for_config(
                                    read_pids, prop_configs.at( 1 ) ) );

    // state:  [0: 7, 11], [1: 9, 11]
    begin_configs[prop_configs.at( 0 )] = 11;
    begin_configs[prop_configs.at( 1 )] = 7;
    begin_configs[prop_configs.at( 2 )] = 13;
    info.add_partition_subscription_information( pids.at( 1 ), begin_configs );
    // state:  [0: 7, 11], [1: 9, 7, 13]

    EXPECT_EQ( 9, info.get_offset_of_partition_for_config(
                       pids.at( 1 ), prop_configs.at( 0 ) ) );
    EXPECT_EQ( 7, info.get_offset_of_partition_for_config(
                       pids.at( 1 ), prop_configs.at( 1 ) ) );
    EXPECT_EQ( 13, info.get_offset_of_partition_for_config(
                       pids.at( 1 ), prop_configs.at( 2 ) ) );

    // state:  [0: 7, 11], [1: 9, 7, 13]
    info.add_source( prop_configs.at( 2 ), 15 );
    // state:  [0: 7, 11, 15], [1: 9, 7, 15]

    EXPECT_EQ( 15, info.get_offset_of_partition_for_config(
                       pids.at( 0 ), prop_configs.at( 2 ) ) );
    EXPECT_EQ( 15, info.get_offset_of_partition_for_config(
                       pids.at( 1 ), prop_configs.at( 2 ) ) );

    // state:  [0: 7, 11, 15], [1: 9, 7, 15]
    info.remove_source( prop_configs.at( 2 ), 13 );
    EXPECT_EQ( 15, info.get_offset_of_partition_for_config(
                       pids.at( 0 ), prop_configs.at( 2 ) ) );
    EXPECT_EQ( 15, info.get_offset_of_partition_for_config(
                       pids.at( 1 ), prop_configs.at( 2 ) ) );

    // state:  [0: 7, 11, 15], [1: 9, 7, 15]
    info.remove_source( prop_configs.at( 2 ), 17 );
    // state:  [0: 7, 11], [1: 9, 7]
    EXPECT_EQ( K_NOT_COMMITTED, info.get_offset_of_partition_for_config(
                       pids.at( 0 ), prop_configs.at( 2 ) ) );
    EXPECT_EQ( K_NOT_COMMITTED, info.get_offset_of_partition_for_config(
                                    pids.at( 1 ), prop_configs.at( 2 ) ) );

    // state:  [0: 7, 11], [1: 9, 7]
    info.remove_partition_subscription_information( pids.at( 1 ) );
    // state:  [0: 7, 11]
    EXPECT_EQ( K_NOT_COMMITTED, info.get_offset_of_partition_for_config(
                       pids.at( 1 ), prop_configs.at( 0 ) ) );
    EXPECT_EQ( K_NOT_COMMITTED, info.get_offset_of_partition_for_config(
                       pids.at( 1 ), prop_configs.at( 1 ) ) );
    EXPECT_EQ( K_NOT_COMMITTED, info.get_offset_of_partition_for_config(
                       pids.at( 1 ), prop_configs.at( 2 ) ) );


    // state:  [0: 7, 11]
    EXPECT_EQ( 7, info.get_offset_of_partition_for_config(
                       pids.at( 0 ), prop_configs.at( 0 ) ) );
    EXPECT_EQ( 11, info.get_offset_of_partition_for_config(
                       pids.at( 0 ), prop_configs.at( 1 ) ) );
    EXPECT_EQ( K_NOT_COMMITTED, info.get_offset_of_partition_for_config(
                       pids.at( 0 ), prop_configs.at( 2 ) ) );

    info.add_source( prop_configs.at( 0 ), 2 );
    // state:  [0: 7, 11]
    EXPECT_EQ( 7, info.get_offset_of_partition_for_config(
                       pids.at( 0 ), prop_configs.at( 0 ) ) );
    EXPECT_EQ( 11, info.get_offset_of_partition_for_config(
                       pids.at( 0 ), prop_configs.at( 1 ) ) );

    info.add_source( prop_configs.at( 0 ), 15 );
    // state:  [0: 15, 11]
    EXPECT_EQ( 15, info.get_offset_of_partition_for_config(
                       pids.at( 0 ), prop_configs.at( 0 ) ) );
    EXPECT_EQ( 11, info.get_offset_of_partition_for_config(
                       pids.at( 0 ), prop_configs.at( 1 ) ) );
}

