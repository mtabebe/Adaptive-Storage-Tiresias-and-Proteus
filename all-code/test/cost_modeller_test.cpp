#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/site-selector/cost_modeller.h"
#include "cost_modeller_types_test.h"

class cost_modeller_test : public ::testing::Test {};

TEST_F( cost_modeller_test, sanity_of_model) {

    auto static_configs = k_cost_model_test_configs;

    cost_modeller2 cost_model( static_configs );

    double   contention = 7;
    uint32_t partition_size = 10;
    double   site_load = 10;
    double   update_count = 200;
    uint32_t num_ops = 5;
    std::vector<double> widths = {5, 7};
    partition_type::type part_type = partition_type::type::ROW;
    storage_tier_type::type storage_type = storage_tier_type::type::MEMORY;

    std::vector<transaction_prediction_stats> txn_stats;
    txn_stats.emplace_back( transaction_prediction_stats(
        part_type, storage_type, contention, update_count, widths,
        partition_size, false /* is scan*/, 0, true, num_ops, true, num_ops ) );

    cost_model_prediction_holder pred_holder =
        cost_model.predict_single_site_transaction_execution_time( site_load,
                                                                   txn_stats );
    EXPECT_LT( 0, pred_holder.get_prediction() );

    std::vector<remaster_stats> remasters;
    remasters.emplace_back(
        remaster_stats( part_type, part_type, update_count, contention ) );

    pred_holder = cost_model.predict_remaster_execution_time(
        site_load, site_load, remasters );
    EXPECT_LT( 0, pred_holder.get_prediction() );

    split_stats h_split( part_type, partition_type::type::COLUMN,
                         partition_type::type::SORTED_COLUMN, storage_type,
                         storage_tier_type::type::DISK,
                         storage_tier_type::type::DISK, contention, widths,
                         partition_size, partition_size / 2 );

    pred_holder = cost_model.predict_horizontal_split_execution_time(
        site_load, h_split );
    EXPECT_LT( 0, pred_holder.get_prediction() );

    split_stats v_split( part_type, partition_type::type::COLUMN,
                         partition_type::type::SORTED_COLUMN, storage_type,
                         storage_tier_type::type::DISK,
                         storage_tier_type::type::DISK, contention, widths,
                         partition_size, widths.size() / 2 );

    pred_holder =
        cost_model.predict_vertical_split_execution_time( site_load, v_split );
    EXPECT_LT( 0, pred_holder.get_prediction() );

    merge_stats h_merge( part_type, partition_type::type::COLUMN,
                         partition_type::type::SORTED_COLUMN, storage_type,
                         storage_tier_type::MEMORY, storage_tier_type::DISK,
                         contention, contention, widths, widths, partition_size,
                         partition_size );

    pred_holder = cost_model.predict_horizontal_merge_execution_time( site_load,
                                                                      h_merge );
    EXPECT_LT( 0, pred_holder.get_prediction() );

    pred_holder =
        cost_model.predict_vertical_merge_execution_time( site_load, h_merge );
    EXPECT_LT( 0, pred_holder.get_prediction() );

    std::vector<add_replica_stats> add_stats;
    add_stats.emplace_back(
        add_replica_stats( part_type, storage_type, part_type, storage_type,
                           contention, widths, partition_size ) );

    pred_holder = cost_model.predict_add_replica_execution_time(
        site_load, site_load, add_stats );
    EXPECT_LT( 0, pred_holder.get_prediction() );


    EXPECT_LT( 0, cost_model.predict_site_load( 10 /*write*/, 3 /*read*/,
                                                2 /*update*/ ) );
}

TEST_F( cost_modeller_test, model_adjusts ) {
    auto static_configs = k_cost_model_test_configs;
    static_configs.is_static_model_ = true;
    auto dynamic_configs = static_configs;
    dynamic_configs.is_static_model_ = false;

    cost_modeller2 static_model( static_configs );
    cost_modeller2 dynamic_model( dynamic_configs );

    double   contention = 7;
    uint32_t partition_size = 10;
    double   site_load = 10;
    double   update_count = 100;
    uint32_t num_ops = 5;
    std::vector<double> widths = {5, 7};
    partition_type::type part_type = partition_type::type::ROW;
    storage_tier_type::type storage_type = storage_tier_type::type::MEMORY;

    std::vector<transaction_prediction_stats> txn_stats;
    txn_stats.emplace_back( transaction_prediction_stats(
        part_type, storage_type, contention, update_count, widths,
        partition_size, false /* is scan*/, 0, true, num_ops, true, num_ops ) );

    cost_model_prediction_holder static_pred_holder =
        static_model.predict_single_site_transaction_execution_time(
            site_load, txn_stats );

    cost_model_prediction_holder dynamic_pred_holder =
        dynamic_model.predict_single_site_transaction_execution_time(
            site_load, txn_stats );

    EXPECT_DOUBLE_EQ( static_pred_holder.get_prediction(),
                      dynamic_pred_holder.get_prediction() );
    EXPECT_LT( 0, dynamic_pred_holder.get_prediction() );

    context_timer read_timer;
    read_timer.counter_id = ROW_READ_RECORD_TIMER_ID;
    read_timer.counter_seen_count = num_ops * widths.size();
    read_timer.timer_us_average = ( static_pred_holder.get_prediction() ) + 17;

    context_timer write_timer;
    write_timer.counter_id = ROW_WRITE_RECORD_TIMER_ID;
    write_timer.counter_seen_count = num_ops * widths.size();
    write_timer.timer_us_average = ( static_pred_holder.get_prediction() ) + 23;

    context_timer contention_timer;
    contention_timer.counter_id =
        ROW_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID;
    contention_timer.counter_seen_count = 1;
    contention_timer.timer_us_average =
        ( static_pred_holder.get_prediction() ) + 29;

    context_timer build_commit_timer;
    build_commit_timer.counter_id = PARTITION_BUILD_COMMIT_SNAPSHOT_TIMER_ID;
    build_commit_timer.counter_seen_count = txn_stats.size();
    build_commit_timer.timer_us_average =
        ( static_pred_holder.get_prediction() ) + 19;

    context_timer serialize_commit_timer;
    serialize_commit_timer.counter_id = SERIALIZE_WRITE_BUFFER_TIMER_ID;
    serialize_commit_timer.counter_seen_count = txn_stats.size();
    serialize_commit_timer.timer_us_average =
        ( static_pred_holder.get_prediction() ) + 17;

    context_timer wait_for_service_timer;
    wait_for_service_timer.counter_id = RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID;
    wait_for_service_timer.counter_seen_count = 1;
    wait_for_service_timer.timer_us_average =
        static_pred_holder.get_prediction() + 29;

    context_timer wait_for_session_version_timer;
    wait_for_session_version_timer.counter_id =
        ROW_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID;
    wait_for_session_version_timer.counter_seen_count = 1;
    wait_for_session_version_timer.timer_us_average =
        static_pred_holder.get_prediction() + 37;

    context_timer wait_for_session_snapshot_timer;
    wait_for_session_snapshot_timer.counter_id =
        ROW_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID;
    wait_for_session_snapshot_timer.counter_seen_count = 1;
    wait_for_session_snapshot_timer.timer_us_average =
        static_pred_holder.get_prediction() + 37;

    std::vector<context_timer> actual_timers;
    actual_timers.push_back( read_timer );
    actual_timers.push_back( write_timer );
    actual_timers.push_back( contention_timer );
    actual_timers.push_back( build_commit_timer );
    actual_timers.push_back( serialize_commit_timer );
    actual_timers.push_back( wait_for_session_version_timer );
    actual_timers.push_back( wait_for_session_snapshot_timer );

    dynamic_pred_holder.add_real_timers( actual_timers );
    static_pred_holder.add_real_timers( actual_timers );

    static_model.add_results( static_pred_holder );
    dynamic_model.add_results( dynamic_pred_holder );

    static_model.update_model();
    dynamic_model.update_model();

    // now the prediction should be bigger than the last one
    cost_model_prediction_holder new_static_pred_holder =
        static_model.predict_single_site_transaction_execution_time(
            site_load, txn_stats );

    cost_model_prediction_holder new_dynamic_pred_holder =
        dynamic_model.predict_single_site_transaction_execution_time(
            site_load, txn_stats );

    EXPECT_DOUBLE_EQ( static_pred_holder.get_prediction(),
                      new_static_pred_holder.get_prediction() );
    // they shouldn't be the same anymore
    EXPECT_NE( static_pred_holder.get_prediction(),
               new_dynamic_pred_holder.get_prediction() );
    // but it should be bigger than old one
    EXPECT_LT( dynamic_pred_holder.get_prediction(),
               new_dynamic_pred_holder.get_prediction() );

    context_timer random_timer;
    random_timer.counter_id = ARS_PUT_NEW_SAMPLE_TIMER_ID;  // random timer
    random_timer.counter_seen_count = 5;
    random_timer.timer_us_average = 7;

    actual_timers.clear();
    actual_timers.push_back( random_timer );

    new_dynamic_pred_holder.add_real_timers( actual_timers );

    // if we don't have any results, or we have results that are
    // other things,
    // then it shouldn't crash
    static_model.add_results( new_static_pred_holder );
    dynamic_model.add_results( new_dynamic_pred_holder );

    static_model.update_model();
    dynamic_model.update_model();
}
