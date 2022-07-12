#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/site-selector/data_site_storage_stats.h"
#include "../src/site-selector/site_selector_metadata.h"
#include "../src/site-selector/site_selector_query_stats.h"

class site_selector_stats_test : public ::testing::Test {};

TEST_F( site_selector_stats_test, query_stats_test ) {
    std::string table_name = "ycsb";
    uint32_t num_records_in_chain = 5;
    uint32_t num_records_in_snapshot_chain = 5;
    uint32_t table_id = 0;
    uint64_t p1_start = 5;
    uint64_t p1_end = 10;
    uint32_t    p1_col_end = 1;

    std::vector<cell_data_type> col_types = {cell_data_type::INT64,
                                             cell_data_type::STRING};

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;
    partition_type::type    default_type = partition_type::type::ROW;

    table_metadata t_metadata = create_table_metadata(
        table_name, table_id, p1_col_end + 1, col_types, num_records_in_chain,
        num_records_in_snapshot_chain, 0, 10 /*default partition size*/,
        p1_col_end + 1, 10 /*default partition tracking size*/, p1_col_end + 1,
        default_type, s_type, false /* enable sec storage */, "/tmp/" );

    site_selector_query_stats stats;

    stats.create_table( t_metadata );

    auto pid = create_partition_column_identifier( 0, p1_start, p1_end, 0,
                                                   p1_col_end );
    auto widths = stats.get_cell_widths( pid );
    EXPECT_EQ( 2, widths.size() );
    EXPECT_DOUBLE_EQ( 0, widths.at( 0 ) );
    EXPECT_DOUBLE_EQ( 0, widths.at( 0 ) );

    stats.set_cell_width( 0, 0, 4 );
    stats.set_cell_width( 0, 1, 12 );
    widths = stats.get_cell_widths( pid );
    EXPECT_EQ( 2, widths.size() );
    EXPECT_DOUBLE_EQ( 4, widths.at( 0 ) );
    EXPECT_DOUBLE_EQ( 12, widths.at( 1 ) );

    EXPECT_DOUBLE_EQ( 0, stats.get_average_scan_selectivity( pid ) );

    stats.set_column_selectivity( 0, 0, 0.5 );
    stats.set_column_selectivity( 0, 1, 0.25 );

    EXPECT_DOUBLE_EQ( 0.125, stats.get_average_scan_selectivity( pid ) );
}

TEST_F( site_selector_stats_test, data_site_storage_test ) {
    std::string table_name = "ycsb";
    uint32_t    num_records_in_chain = 5;
    uint32_t    num_records_in_snapshot_chain = 5;
    uint32_t    table_id = 0;
    uint64_t    p1_start = 5;
    uint64_t    p1_end = 10;
    uint32_t    p1_col_end = 1;

    std::vector<cell_data_type> col_types = {cell_data_type::INT64,
                                             cell_data_type::STRING};

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;
    partition_type::type    default_type = partition_type::type::ROW;

    table_metadata t_metadata = create_table_metadata(
        table_name, table_id, p1_col_end + 1, col_types, num_records_in_chain,
        num_records_in_snapshot_chain, 0, 10 /*default partition size*/,
        p1_col_end + 1, 10 /*default partition tracking size*/, p1_col_end + 1,
        default_type, s_type, false /* enable sec storage */, "/tmp/" );

    uint32_t num_sites = 2;
    std::vector<storage_tier_type::type> acceptable_storage_types = {
        storage_tier_type::type::MEMORY, storage_tier_type::type::DISK};

    auto configs = construct_partition_data_location_table_configs(
        num_sites, acceptable_storage_types, 100 /* memory_tier_limit */,
        500 /* disk_tier_limit  */ );

    data_site_storage_stats stats( configs.num_sites_,
                                   configs.storage_tier_limits_ );
    stats.create_table( t_metadata );

    site_selector_query_stats q_stats;
    q_stats.create_table( t_metadata );

    q_stats.set_cell_width( 0, 0, 4 );
    q_stats.set_cell_width( 0, 1, 12 );

    auto pid = create_partition_column_identifier( 0, p1_start, p1_end, 0,
                                                   p1_col_end );
    auto pid_1 = create_partition_column_identifier( 0, p1_end + 1,
                                                     ( p1_end * 2 ), 0, 0 );

    EXPECT_DOUBLE_EQ(
        0, stats.get_storage_tier_size( 0, storage_tier_type::type::MEMORY ) );
    EXPECT_DOUBLE_EQ( 0, stats.get_partition_size( pid ) );

    stats.add_partition_to_tier( 0, storage_tier_type::type::MEMORY, pid );
    stats.add_partition_to_tier( 1, storage_tier_type::type::DISK, pid );
    stats.add_partition_to_tier( 0, storage_tier_type::type::MEMORY, pid_1 );

    EXPECT_DOUBLE_EQ( 0, stats.get_partition_size( pid ) );

    stats.update_table_widths_from_stats( q_stats);

    EXPECT_DOUBLE_EQ( 96, stats.get_partition_size( pid ) );
    EXPECT_DOUBLE_EQ( 40, stats.get_partition_size( pid_1 ) );

    EXPECT_DOUBLE_EQ( 136, stats.get_storage_tier_size(
                               0, storage_tier_type::type::MEMORY ) );
    EXPECT_DOUBLE_EQ(
        0, stats.get_storage_tier_size( 0, storage_tier_type::type::DISK ) );
    EXPECT_DOUBLE_EQ(
        0, stats.get_storage_tier_size( 1, storage_tier_type::type::MEMORY ) );
    EXPECT_DOUBLE_EQ(
        96, stats.get_storage_tier_size( 1, storage_tier_type::type::DISK ) );

    EXPECT_DOUBLE_EQ(
        100, stats.get_storage_limit( 0, storage_tier_type::type::MEMORY ) );
    EXPECT_DOUBLE_EQ(
        500, stats.get_storage_limit( 0, storage_tier_type::type::DISK ) );

    EXPECT_DOUBLE_EQ(
        1.36, stats.get_storage_ratio( 0, storage_tier_type::type::MEMORY ) );
    EXPECT_DOUBLE_EQ(
        0, stats.get_storage_ratio( 0, storage_tier_type::type::DISK ) );
    EXPECT_DOUBLE_EQ(
        0.192, stats.get_storage_ratio( 1, storage_tier_type::type::DISK ) );

    stats.change_partition_tier( 0, storage_tier_type::type::MEMORY,
                                 storage_tier_type::type::DISK, pid_1 );

    EXPECT_DOUBLE_EQ(
        96, stats.get_storage_tier_size( 0, storage_tier_type::type::MEMORY ) );
    EXPECT_DOUBLE_EQ(
        40, stats.get_storage_tier_size( 0, storage_tier_type::type::DISK ) );
    EXPECT_DOUBLE_EQ(
        0, stats.get_storage_tier_size( 1, storage_tier_type::type::MEMORY ) );
    EXPECT_DOUBLE_EQ(
        96, stats.get_storage_tier_size( 1, storage_tier_type::type::DISK ) );

    auto storage_ratios = stats.get_storage_ratios();

    EXPECT_EQ( 2, storage_ratios.size() );
    EXPECT_EQ( 1, storage_ratios.count( storage_tier_type::type::DISK ) );
    EXPECT_EQ( 1, storage_ratios.count( storage_tier_type::type::MEMORY ) );

    EXPECT_EQ( 2, storage_ratios.at( storage_tier_type::type::MEMORY ).size() );
    EXPECT_DOUBLE_EQ(
        0.96, storage_ratios.at( storage_tier_type::type::MEMORY ).at( 0 ) );
    EXPECT_DOUBLE_EQ(
        0, storage_ratios.at( storage_tier_type::type::MEMORY ).at( 1 ) );

    EXPECT_EQ( 2, storage_ratios.at( storage_tier_type::type::DISK ).size() );
    EXPECT_DOUBLE_EQ(
        0.08, storage_ratios.at( storage_tier_type::type::DISK ).at( 0 ) );
    EXPECT_DOUBLE_EQ(
        0.192, storage_ratios.at( storage_tier_type::type::DISK ).at( 1 ) );
}
