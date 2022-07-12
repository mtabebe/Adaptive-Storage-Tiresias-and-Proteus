#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/site-selector/cost_modeller_types.h"
#include "../src/site-selector/stat_tracked_enumerator_holder.h"

class stat_tracked_enumerator_test : public ::testing::Test {};

TEST_F( stat_tracked_enumerator_test, bucketize_test ) {
    EXPECT_DOUBLE_EQ( 30, bucketize_double( 27, 10 ) );
    EXPECT_DOUBLE_EQ( 27, bucketize_double( 27, 0.1 ) );
    EXPECT_DOUBLE_EQ( 0, bucketize_double( 0.27, 10 ) );
    EXPECT_DOUBLE_EQ( 0.3, bucketize_double( 0.27, 0.1 ) );


    EXPECT_EQ( 20, bucketize_int( 27, 10 ) );
    EXPECT_EQ( 27, bucketize_int( 27, 1 ) );
    EXPECT_EQ( 0, bucketize_int( 2, 10 ) );
    EXPECT_EQ( 2, bucketize_int( 2, 1 ) );

    EXPECT_DOUBLE_EQ( 0, bucketize_double( 0, 1 ) );
    EXPECT_EQ( 0, bucketize_int( 0, 1 ) );
}

TEST_F( stat_tracked_enumerator_test, enumerator_holder_no_tracking_test ) {
    std::vector<partition_type::type> acceptable_new_partition_types = {
        partition_type::type::ROW,
        partition_type::type::COLUMN,
        partition_type::type::SORTED_COLUMN,
        partition_type::type::MULTI_COLUMN,
        partition_type::type::SORTED_MULTI_COLUMN
    };
    std::vector<partition_type::type> acceptable_partition_types =
        acceptable_new_partition_types;

    std::vector<storage_tier_type::type> acceptable_new_storage_types = {
        storage_tier_type::type::MEMORY, storage_tier_type::type::DISK};
    std::vector<storage_tier_type::type> acceptable_storage_types =
        acceptable_new_storage_types;

    stat_tracked_enumerator_configs configs =
        construct_stat_tracked_enumerator_configs(
            false /* track_stats */, 0.0 /* prob_of_returning_default */,
            0.25 /* min_threshold */, 10 /* min count threshold */,
            acceptable_new_partition_types, acceptable_partition_types,
            acceptable_new_storage_types, acceptable_storage_types,
            0.1 /* contention_bucket */, 10 /* cell_width_bucket */,
            10 /* num_entries_bucket */, 10 /* num_updates_needed_bucket */,
            0.1 /* scan_selectivity_bucket */, 10 /* num_point_reads_bucket */,
            10 /*num_point_updates_bucket */ );

    std::vector<double> cell_widths = {5.0, 10.0};

    auto enumerator_holder =
        std::make_shared<stat_tracked_enumerator_holder>( configs );

    // expect it to always return the default
    add_replica_stats add_stats_high_cont(
        partition_type::type::ROW, storage_tier_type::type::MEMORY,
        partition_type::type::ROW, storage_tier_type::type::MEMORY, 0.5,
        cell_widths, 10 );
    add_replica_stats add_stats_low_cont(
        partition_type::type::ROW, storage_tier_type::type::MEMORY,
        partition_type::type::ROW, storage_tier_type::type::MEMORY, 0.05,
        cell_widths, 10 );

    uint32_t num_iters = 100;

    std::vector<std::tuple<partition_type::type, storage_tier_type::type>>
        acceptable_types;
    for( const auto& part_type : acceptable_partition_types ) {
        for( const auto& storage_type : acceptable_storage_types ) {
            acceptable_types.emplace_back(
                std::make_tuple<>( part_type, storage_type ) );
        }
    }

    for( uint32_t i = 0; i < num_iters; i++ ) {
        EXPECT_EQ( acceptable_types, enumerator_holder->get_add_replica_types(
                                         add_stats_high_cont ) );
        EXPECT_EQ( acceptable_types, enumerator_holder->get_add_replica_types(
                                         add_stats_low_cont ) );

        enumerator_holder->add_replica_types_decision(
            add_stats_high_cont,
            std::make_tuple<>( partition_type::type::COLUMN,
                               storage_tier_type::type::DISK ) );
        enumerator_holder->add_replica_types_decision(
            add_stats_low_cont,
            std::make_tuple<>( partition_type::type::SORTED_COLUMN,
                               storage_tier_type::type::MEMORY ) );
    }
}
TEST_F( stat_tracked_enumerator_test, enumerator_holder_test ) {
    std::vector<partition_type::type> acceptable_new_partition_types = {
        partition_type::type::ROW, partition_type::type::COLUMN,
        partition_type::type::SORTED_COLUMN, partition_type::type::MULTI_COLUMN,
        partition_type::type::SORTED_MULTI_COLUMN};
    std::vector<partition_type::type> acceptable_partition_types =
        acceptable_new_partition_types;

    std::vector<storage_tier_type::type> acceptable_new_storage_types = {
        storage_tier_type::type::MEMORY, storage_tier_type::type::DISK};
    std::vector<storage_tier_type::type> acceptable_storage_types =
        acceptable_new_storage_types;

    stat_tracked_enumerator_configs configs =
        construct_stat_tracked_enumerator_configs(
            true /* track_stats */, 0.0 /* prob_of_returning_default */,
            0.25 /* min_threshold */, 10 /* min count threshold */,
            acceptable_new_partition_types, acceptable_partition_types,
            acceptable_new_storage_types, acceptable_storage_types,
            0.1 /* contention_bucket */, 10 /* cell_width_bucket */,
            10 /* num_entries_bucket */, 10 /* num_updates_needed_bucket */,
            0.1 /* scan_selectivity_bucket */, 10 /* num_point_reads_bucket */,
            10 /*num_point_updates_bucket */ );
    auto enumerator_holder =
        std::make_shared<stat_tracked_enumerator_holder>( configs );

    std::vector<double> cell_widths = {5.0, 10.0};

    add_replica_stats add_stats_high_cont(
        partition_type::type::ROW, storage_tier_type::type::MEMORY,
        partition_type::type::ROW, storage_tier_type::type::MEMORY, 0.5,
        cell_widths, 10 );
    add_replica_stats add_stats_low_cont(
        partition_type::type::ROW, storage_tier_type::type::MEMORY,
        partition_type::type::ROW, storage_tier_type::type::MEMORY, 0.05,
        cell_widths, 10 );

    std::vector<std::tuple<partition_type::type, storage_tier_type::type>>
        acceptable_types;
    for( const auto& part_type : acceptable_partition_types ) {
        for( const auto& storage_type : acceptable_storage_types ) {
            acceptable_types.emplace_back(
                std::make_tuple<>( part_type, storage_type ) );
        }
    }

    for( uint32_t i = 0; i < configs.min_count_threshold_; i++ ) {
        EXPECT_EQ(
            acceptable_types,
            enumerator_holder->get_add_replica_types( add_stats_high_cont ) );
        EXPECT_EQ( acceptable_types, enumerator_holder->get_add_replica_types(
                                         add_stats_low_cont ) );

        enumerator_holder->add_replica_types_decision(
            add_stats_high_cont,
            std::make_tuple<>( partition_type::type::COLUMN,
                               storage_tier_type::type::DISK ) );
        enumerator_holder->add_replica_types_decision(
            add_stats_low_cont,
            std::make_tuple<>( partition_type::type::SORTED_COLUMN,
                               storage_tier_type::type::MEMORY ) );
    }
    uint32_t num_iters = 100;
    std::vector<std::tuple<partition_type::type, storage_tier_type::type>>
        high_cont_partition_types = {std::make_tuple<>(
            partition_type::type::COLUMN, storage_tier_type::type::DISK )};
    std::vector<std::tuple<partition_type::type, storage_tier_type::type>>
        low_cont_partition_types = {
            std::make_tuple<>( partition_type::type::SORTED_COLUMN,
                               storage_tier_type::type::MEMORY )};

    for( uint32_t i = 0; i < num_iters; i++ ) {
        EXPECT_EQ(
            high_cont_partition_types,
            enumerator_holder->get_add_replica_types( add_stats_high_cont ) );
        EXPECT_EQ(
            low_cont_partition_types,
            enumerator_holder->get_add_replica_types( add_stats_low_cont ) );

        enumerator_holder->add_replica_types_decision(
            add_stats_high_cont,
            std::make_tuple<>( partition_type::type::COLUMN,
                               storage_tier_type::type::DISK ) );
        enumerator_holder->add_replica_types_decision(
            add_stats_low_cont,
            std::make_tuple<>( partition_type::type::SORTED_COLUMN,
                               storage_tier_type::type::MEMORY ) );
    }
}

TEST_F( stat_tracked_enumerator_test,
        decision_tracker_enumerator_holder_no_tracking_test ) {
    std::vector<partition_type::type> acceptable_new_partition_types = {
        partition_type::type::ROW, partition_type::type::COLUMN,
        partition_type::type::SORTED_COLUMN, partition_type::type::MULTI_COLUMN,
        partition_type::type::SORTED_MULTI_COLUMN};
    std::vector<partition_type::type> acceptable_partition_types =
        acceptable_new_partition_types;

    std::vector<storage_tier_type::type> acceptable_new_storage_types = {
        storage_tier_type::type::MEMORY, storage_tier_type::type::DISK};
    std::vector<storage_tier_type::type> acceptable_storage_types =
        acceptable_new_storage_types;

    stat_tracked_enumerator_configs configs =
        construct_stat_tracked_enumerator_configs(
            false /* track_stats */, 0.0 /* prob_of_returning_default */,
            0.25 /* min_threshold */, 10 /* min count threshold */,
            acceptable_new_partition_types, acceptable_partition_types,
            acceptable_new_storage_types, acceptable_storage_types,
            0.1 /* contention_bucket */, 10 /* cell_width_bucket */,
            10 /* num_entries_bucket */, 10 /* num_updates_needed_bucket */,
            0.1 /* scan_selectivity_bucket */, 10 /* num_point_reads_bucket */,
            10 /*num_point_updates_bucket */ );

    std::vector<double> cell_widths = {5.0, 10.0};

    auto enumerator_holder =
        std::make_shared<stat_tracked_enumerator_holder>( configs );

    auto decision_holder = enumerator_holder->construct_decision_holder();

    // expect it to always return the default
    add_replica_stats add_stats_high_cont(
        partition_type::type::ROW, storage_tier_type::type::MEMORY,
        partition_type::type::ROW, storage_tier_type::type::MEMORY, 0.5,
        cell_widths, 10 );
    add_replica_stats add_stats_low_cont(
        partition_type::type::ROW, storage_tier_type::type::MEMORY,
        partition_type::type::ROW, storage_tier_type::type::MEMORY, 0.05,
        cell_widths, 10 );

    uint32_t num_iters = 10;
    uint32_t num_inner_iters = 10;

    std::vector<std::tuple<partition_type::type, storage_tier_type::type>>
        acceptable_types;
    for( const auto& part_type : acceptable_partition_types ) {
        for( const auto& storage_type : acceptable_storage_types ) {
            acceptable_types.emplace_back(
                std::make_tuple<>( part_type, storage_type ) );
        }
    }

    for( uint32_t i = 0; i < num_iters; i++ ) {
        for( uint32_t j = 0; j < num_inner_iters; j++ ) {
            EXPECT_EQ( acceptable_types,
                       enumerator_holder->get_add_replica_types(
                           add_stats_high_cont ) );
            EXPECT_EQ( acceptable_types,
                       enumerator_holder->get_add_replica_types(
                           add_stats_low_cont ) );

            decision_holder.add_replica_types_decision(
                add_stats_high_cont,
                std::make_tuple<>( partition_type::type::COLUMN,
                                   storage_tier_type::type::DISK ) );
            decision_holder.add_replica_types_decision(
                add_stats_low_cont,
                std::make_tuple<>( partition_type::type::SORTED_COLUMN,
                                   storage_tier_type::type::MEMORY ) );
        }
        enumerator_holder->add_decisions( decision_holder );
        decision_holder.clear_decisions();
    }
}
TEST_F( stat_tracked_enumerator_test,
        decision_tracker_enumerator_holder_test ) {
    std::vector<partition_type::type> acceptable_new_partition_types = {
        partition_type::type::ROW, partition_type::type::COLUMN,
        partition_type::type::SORTED_COLUMN, partition_type::type::MULTI_COLUMN,
        partition_type::type::SORTED_MULTI_COLUMN};
    std::vector<partition_type::type> acceptable_partition_types =
        acceptable_new_partition_types;

    std::vector<storage_tier_type::type> acceptable_new_storage_types = {
        storage_tier_type::type::MEMORY, storage_tier_type::type::DISK};
    std::vector<storage_tier_type::type> acceptable_storage_types =
        acceptable_new_storage_types;

    stat_tracked_enumerator_configs configs =
        construct_stat_tracked_enumerator_configs(
            true /* track_stats */, 0.0 /* prob_of_returning_default */,
            0.25 /* min_threshold */, 10 /* min count threshold */,
            acceptable_new_partition_types, acceptable_partition_types,
            acceptable_new_storage_types, acceptable_storage_types,
            0.1 /* contention_bucket */, 10 /* cell_width_bucket */,
            10 /* num_entries_bucket */, 10 /* num_updates_needed_bucket */,
            0.1 /* scan_selectivity_bucket */, 10 /* num_point_reads_bucket */,
            10 /*num_point_updates_bucket */ );
    auto enumerator_holder =
        std::make_shared<stat_tracked_enumerator_holder>( configs );

    auto decision_holder = enumerator_holder->construct_decision_holder();

    std::vector<double> cell_widths = {5.0, 10.0};

    add_replica_stats add_stats_high_cont(
        partition_type::type::ROW, storage_tier_type::type::MEMORY,
        partition_type::type::ROW, storage_tier_type::type::MEMORY, 0.5,
        cell_widths, 10 );
    add_replica_stats add_stats_low_cont(
        partition_type::type::ROW, storage_tier_type::type::MEMORY,
        partition_type::type::ROW, storage_tier_type::type::MEMORY, 0.05,
        cell_widths, 10 );

    std::vector<std::tuple<partition_type::type, storage_tier_type::type>>
        acceptable_types;
    for( const auto& part_type : acceptable_partition_types ) {
        for( const auto& storage_type : acceptable_storage_types ) {
            acceptable_types.emplace_back(
                std::make_tuple<>( part_type, storage_type ) );
        }
    }

    for( uint32_t i = 0; i < configs.min_count_threshold_; i++ ) {
        EXPECT_EQ( acceptable_types, enumerator_holder->get_add_replica_types(
                                         add_stats_high_cont ) );
        EXPECT_EQ( acceptable_types, enumerator_holder->get_add_replica_types(
                                         add_stats_low_cont ) );

        decision_holder.add_replica_types_decision(
            add_stats_high_cont,
            std::make_tuple<>( partition_type::type::COLUMN,
                               storage_tier_type::type::DISK ) );
        decision_holder.add_replica_types_decision(
            add_stats_low_cont,
            std::make_tuple<>( partition_type::type::SORTED_COLUMN,
                               storage_tier_type::type::MEMORY ) );
    }
    enumerator_holder->add_decisions( decision_holder );
    decision_holder.clear_decisions();

    uint32_t num_iters = 10;
    uint32_t num_inner_iters = 10;

    std::vector<std::tuple<partition_type::type, storage_tier_type::type>>
        high_cont_partition_types = {std::make_tuple<>(
            partition_type::type::COLUMN, storage_tier_type::type::DISK )};
    std::vector<std::tuple<partition_type::type, storage_tier_type::type>>
        low_cont_partition_types = {
            std::make_tuple<>( partition_type::type::SORTED_COLUMN,
                               storage_tier_type::type::MEMORY )};

    for( uint32_t i = 0; i < num_iters; i++ ) {
        for( uint32_t j = 0; j < num_inner_iters; j++ ) {
            EXPECT_EQ( high_cont_partition_types,
                       enumerator_holder->get_add_replica_types(
                           add_stats_high_cont ) );
            EXPECT_EQ( low_cont_partition_types,
                       enumerator_holder->get_add_replica_types(
                           add_stats_low_cont ) );

            decision_holder.add_replica_types_decision(
                add_stats_high_cont,
                std::make_tuple<>( partition_type::type::COLUMN,
                                   storage_tier_type::type::DISK ) );
            decision_holder.add_replica_types_decision(
                add_stats_low_cont,
                std::make_tuple<>( partition_type::type::SORTED_COLUMN,
                                   storage_tier_type::type::MEMORY ) );
        }
        enumerator_holder->add_decisions( decision_holder );
        decision_holder.clear_decisions();
    }

    for( uint32_t j = 0; j < num_inner_iters; j++ ) {
        EXPECT_EQ(
            high_cont_partition_types,
            enumerator_holder->get_add_replica_types( add_stats_high_cont ) );
        EXPECT_EQ(
            low_cont_partition_types,
            enumerator_holder->get_add_replica_types( add_stats_low_cont ) );

        decision_holder.add_replica_types_decision(
            add_stats_high_cont,
            std::make_tuple<>( partition_type::type::COLUMN,
                               storage_tier_type::type::DISK ) );
        decision_holder.add_replica_types_decision(
            add_stats_high_cont,
            std::make_tuple<>( partition_type::type::SORTED_COLUMN,
                               storage_tier_type::type::MEMORY ) );

        decision_holder.add_replica_types_decision(
            add_stats_low_cont,
            std::make_tuple<>( partition_type::type::SORTED_COLUMN,
                               storage_tier_type::type::MEMORY ) );
        decision_holder.add_replica_types_decision(
            add_stats_low_cont,
            std::make_tuple<>( partition_type::type::COLUMN,
                               storage_tier_type::type::DISK ) );
    }
    enumerator_holder->add_decisions( decision_holder );
    decision_holder.clear_decisions();
}
