#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/data-site/db/partition_metadata.h"
#include "../src/site-selector/periodic_site_selector_operations.h"
#include "cost_modeller_types_test.h"
#include "mock_sm_client_conn.h"

using ::testing::_;
using ::testing::Return;
using namespace ::testing;

class periodic_site_selector_operations_test : public ::testing::Test {};

TEST_F( periodic_site_selector_operations_test, poll_sites ) {
    uint32_t num_poller_threads = 2;
    uint32_t num_sites = 3;
    uint32_t sleep_delay = 100;
    uint32_t num_clients = 1;
    auto     poll_configs = construct_periodic_site_selector_operations_configs(
        num_poller_threads, sleep_delay );

    partition_column_identifier pid =
        create_partition_column_identifier( 0, 0, 10, 0, 1 );
    std::vector<partition_column_identifier> pids = {pid};

    std::vector<uint32_t> site_to_thread_assignments = {0, 0, 1};

    std::unique_ptr<std::vector<client_conn_pool>> pool(
        new std::vector<client_conn_pool>() );

    for( uint32_t tid = 0; tid < num_poller_threads; tid++ ) {
        std::unique_ptr<std::vector<sm_client_ptr>> clients(
            new std::vector<sm_client_ptr>() );
        for( uint32_t site = 0; site < num_sites; site++ ) {
            mock_sm_client_conn *cli_conn = new mock_sm_client_conn();

            site_statistics_results first_result;
            site_statistics_results future_result;

            first_result.status = exec_status_type::COMMAND_OK;
            future_result.status = exec_status_type::COMMAND_OK;

            propagation_configuration p0_0;
            p0_0.type = propagation_type::VECTOR;
            p0_0.partition = ( site * 2 );
            p0_0.offset = ( site * 2 ) + 1;
            std::vector<propagation_configuration> first_configs = {p0_0};
            std::vector<int64_t>                   first_counts = {p0_0.offset};

            std::vector<polled_partition_column_version_information>
                first_polled_versions;
            std::vector<polled_partition_column_version_information>
                future_polled_versions;

            polled_partition_column_version_information first_version;
            first_version.pcid = pid;
            first_version.version = site;

            polled_partition_column_version_information future_version =
                first_version;
            future_version.version = ( site * 2 ) + 1;

            first_polled_versions.push_back( first_version );
            future_polled_versions.push_back( future_version );

            propagation_configuration p1_0 = p0_0;
            p1_0.offset = ( site * 3 ) + 1;
            std::vector<propagation_configuration> future_configs = {p1_0};
            std::vector<int64_t> future_counts = {p1_0.offset};

            first_result.prop_configs = first_configs;
            first_result.prop_counts = first_counts;
            first_result.partition_col_versions = first_polled_versions;
            future_result.prop_configs = future_configs;
            future_result.prop_counts = future_counts;
            future_result.partition_col_versions = future_polled_versions;

            if( site_to_thread_assignments.at( site ) == tid ) {
              if ( tid == 0) {
                  EXPECT_CALL( *cli_conn, rpc_get_site_statistics( _ ) )
                      .WillOnce( testing::SetArgReferee<0>( first_result ) )
                      .WillRepeatedly( testing::SetArgReferee<0>( future_result ) );
              } else {
                  EXPECT_CALL( *cli_conn, rpc_get_site_statistics( _ ) )
                      .WillRepeatedly(
                          testing::SetArgReferee<0>( future_result ) );
              }
            } else if( tid == 0 ) {
                EXPECT_CALL( *cli_conn, rpc_get_site_statistics( _ ) )
                    .WillOnce( testing::SetArgReferee<0>( first_result ) );
            }

            sm_client_ptr client( std::move( cli_conn ) );
            clients->push_back( std::move( client ) );
        }

        pool->push_back( std::move( clients ) );
    }
    std::shared_ptr<client_conn_pools> conn_pool( new client_conn_pools(
        std::move( pool ), num_clients, num_poller_threads ) );

    auto static_configs = k_cost_model_test_configs;

    auto cost_model2 = std::make_shared<cost_modeller2>( static_configs );
    auto site_partition_version_info =
        std::make_shared<sites_partition_version_information>( cost_model2 );
    site_partition_version_info->init_partition_states( num_sites,
                                                        1 /*num tables */ );

    auto data_loc_tab = std::make_shared<partition_data_location_table>(
        construct_partition_data_location_table_configs( num_sites ) );

    auto query_predictor = std::make_shared<query_arrival_predictor>(
        construct_query_arrival_predictor_configs(
            false /*use query arrival predictor*/,
            query_arrival_predictor_type::SIMPLE_QUERY_PREDICTOR,
            0.5 /* threshold */, 30 /* time bucket */, 10 /* count bucket */,
            1.0 /* score scalar */, 0.001 /* default score */ ) );

    auto tier_tracking = std::make_shared<partition_tier_tracking>(
        data_loc_tab, cost_model2, query_predictor );

    std::vector<std::atomic<int64_t>> site_read_count( num_sites );
    std::vector<std::atomic<int64_t>> site_write_count( num_sites );

    uint32_t default_partition_size = 10;
    uint32_t default_column_size = 2;
    uint32_t col_start = 0;
    uint32_t col_end = 4;
    std::vector<cell_data_type> col_types = {
        cell_data_type::INT64, cell_data_type::STRING, cell_data_type::STRING,
        cell_data_type::INT64, cell_data_type::INT64};

    auto table_metadata_0 = create_table_metadata(
        "0", 0, ( col_end - col_start ) + 1, col_types,
        5 /*num_records_in_chain*/, 5 /*num_records_in_snapshot_chain*/,
        0 /*site_location*/, default_partition_size, default_column_size,
        default_partition_size, default_column_size,
        partition_type::type::COLUMN, storage_tier_type::type::DISK,
        false /* enable sec storage */, "/tmp/" );

    std::unique_ptr<site_selector_query_stats> query_stats =
        std::make_unique<site_selector_query_stats>();
    query_stats->create_table( table_metadata_0 );
    query_predictor->add_table_metadata( table_metadata_0 );

    periodic_site_selector_operations poller(
        conn_pool, cost_model2, site_partition_version_info, data_loc_tab,
        query_stats.get(), tier_tracking, query_predictor, &site_read_count,
        &site_write_count, num_sites, num_clients - 1, poll_configs );

    poller.start_polling();

    auto versions =
        site_partition_version_info->get_version_of_partition( pid );
    EXPECT_EQ( num_sites, versions.size() );
    for( uint32_t site = 0; site < num_sites; site++ ) {
        auto locations =
            poller.get_site_propagation_configurations( site, pids );
        EXPECT_EQ( 1, locations.size() );
        EXPECT_EQ( propagation_type::VECTOR, locations.at( 0 ).type );
        EXPECT_EQ( site * 2, locations.at( 0 ).partition );
        uint64_t offset = locations.at( 0 ).offset;
        EXPECT_TRUE( ( offset == ( site * 2 ) + 1 ) or
                     ( offset == ( site * 3 ) + 1 ) );
        uint64_t version =
            site_partition_version_info->get_version_of_partition( site, pid );
        EXPECT_TRUE( ( version == site ) or ( version == ( site * 2 ) + 1 ) );
        version = versions.at( site );
        EXPECT_TRUE( ( version == site ) or ( version == ( site * 2 ) + 1 ) );
    }

    std::chrono::duration<int, std::milli> sleep_time( sleep_delay * 2 );

    std::this_thread::sleep_for( sleep_time );

    poller.stop_polling();

    versions = site_partition_version_info->get_version_of_partition( pid );
    EXPECT_EQ( num_sites, versions.size() );
    for( uint32_t site = 0; site < num_sites; site++ ) {
        auto locations =
            poller.get_site_propagation_configurations( site, pids );
        EXPECT_EQ( 1, locations.size() );
        EXPECT_EQ( propagation_type::VECTOR, locations.at( 0 ).type );
        EXPECT_EQ( site * 2, locations.at( 0 ).partition );
        uint64_t offset = locations.at( 0 ).offset;
        EXPECT_TRUE( ( offset == ( site * 2 ) + 1 ) or
                     ( offset == ( site * 3 ) + 1 ) );
        uint64_t version =
            site_partition_version_info->get_version_of_partition( site, pid );
        EXPECT_TRUE( ( version == site ) or ( version == ( site * 2 ) + 1 ) );
        version = versions.at( site );
        EXPECT_TRUE( ( version == site ) or ( version == ( site * 2 ) + 1 ) );

    }

    // if the topic has no updates then our estimate is 0
    auto payload = std::make_shared<partition_payload>( pid );
    auto p_location_information = std::make_shared<partition_location_information>();
    p_location_information->master_location_ = 0;
    p_location_information->replica_locations_ = {
      1, 2, 3 };
    p_location_information->partition_types_ = {
        {0, partition_type::type::ROW},
        {1, partition_type::type::COLUMN},
        {2, partition_type::type::ROW},
        {3, partition_type::type::SORTED_COLUMN}};
    p_location_information->version_ = 1;
    p_location_information->update_destination_slot_ = 0;
    bool compared_and_swapped = payload->compare_and_swap_location_information(
        p_location_information, nullptr );
    EXPECT_TRUE( compared_and_swapped );
    payload->read_accesses_ = 2;
    payload->write_accesses_ = 10;
    payload->sample_based_read_accesses_ = 1;
    payload->sample_based_write_accesses_ = 5;


    site_partition_version_info->set_topic_update_count( 0, 0 );

    // no updates to topic
    double num_updates_to_wait_for =
        site_partition_version_info
            ->estimate_number_of_updates_need_to_wait_for(
                payload, p_location_information, 1, 6 );
    EXPECT_DOUBLE_EQ( 0, num_updates_to_wait_for );

    site_partition_version_info->set_topic_update_count( 0, 20 );
    site_partition_version_info->set_version_of_partition( 1, pid, 3 );

    // session is behind version
    num_updates_to_wait_for = site_partition_version_info
                                  ->estimate_number_of_updates_need_to_wait_for(
                                      payload, p_location_information, 1, 2 );
    EXPECT_DOUBLE_EQ( 0, num_updates_to_wait_for );

    // no need to wait at master
    num_updates_to_wait_for = site_partition_version_info
                                  ->estimate_number_of_updates_need_to_wait_for(
                                      payload, p_location_information, 0, 2 );
    EXPECT_DOUBLE_EQ( 0, num_updates_to_wait_for );

    // normalized write count = (10 / normalization)*weight + bias
    // normalization = 10, weight=1, bias = 0 ==> 1
    // we need to wait for 3 updates * 20 / 1 (updates to topic / normalized write count)
    num_updates_to_wait_for = site_partition_version_info
                                  ->estimate_number_of_updates_need_to_wait_for(
                                      payload, p_location_information, 1, 6 );
    EXPECT_DOUBLE_EQ( 60, num_updates_to_wait_for );
}

