#define GTEST_HAS_TR1_TUPLE 0

#include <chrono>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thread>

#include "../src/common/constants.h"
#include "../src/common/perf_tracking.h"
#include "../src/common/thread_utils.h"

class test_in_memory_tracing : public ::testing::Test {};

TEST( test_in_memory_tracing, test_macros_not_horribly_broken ) {
    init_global_perf_counters( END_TIMER_COUNTERS + 1, 1,
                               NUMBER_OF_MODEL_TIMERS + 1, 1, 1 );

    // braces to scope timers
    // god help you if you put two of the same timers in the same scope
    {
        start_timer( BEGIN_FOR_WRITE_TIMER_ID );
        (void) 0;
        stop_timer( BEGIN_FOR_WRITE_TIMER_ID );
    }

    {
        start_timer( BEGIN_FOR_WRITE_TIMER_ID );
        (void) 0;
        stop_timer( BEGIN_FOR_WRITE_TIMER_ID );
    }

    std::unordered_map<uint64_t, performance_counter_accumulator> agg_ctr =
        std::move( aggregate_thread_counters() );

    // One thread, so nothing to aggregate
    EXPECT_EQ( agg_ctr.size(), 1 );
    auto search = agg_ctr.find( BEGIN_FOR_WRITE_TIMER_ID );
    EXPECT_NE( search, agg_ctr.end() );
    const performance_counter_accumulator &pc = search->second;
    EXPECT_GT( pc.counter_min_, 0 );
    EXPECT_GT( pc.counter_max_, 0 );
    EXPECT_GT( pc.counter_average_, 0 );
    EXPECT_EQ( pc.counter_seen_count_, 2 );

    {
        start_timer( END_TIMER_COUNTERS );
        (void) 0;
        stop_timer( END_TIMER_COUNTERS );
    }

    // Destroy unique_ptr
    global_performance_counters = nullptr;
    per_context_performance_counters = nullptr;
}

TEST( test_in_memory_tracing, one_thread_correct_average ) {

    init_global_perf_counters( END_TIMER_COUNTERS + 1, 1,
                               NUMBER_OF_MODEL_TIMERS + 1, 1, 1 );

    int32_t thr_id = get_thread_id();

    global_performance_counters->insert_new_data_point(
        thr_id, BEGIN_FOR_WRITE_TIMER_ID, 1.0 );
    global_performance_counters->insert_new_data_point(
        thr_id, BEGIN_FOR_WRITE_TIMER_ID, 2.0 );
    global_performance_counters->insert_new_data_point(
        thr_id, BEGIN_FOR_WRITE_TIMER_ID, 3.0 );
    global_performance_counters->insert_new_data_point(
        thr_id, BEGIN_FOR_WRITE_TIMER_ID, 4.0 );
    global_performance_counters->insert_new_data_point(
        thr_id, BEGIN_FOR_WRITE_TIMER_ID, 5.0 );

    std::unordered_map<uint64_t, performance_counter_accumulator> agg_ctr =
        std::move( aggregate_thread_counters() );

    // One thread, so nothing to aggregate
    EXPECT_EQ( agg_ctr.size(), 1 );
    auto search = agg_ctr.find( BEGIN_FOR_WRITE_TIMER_ID );
    EXPECT_NE( search, agg_ctr.end() );
    const performance_counter_accumulator &pc = search->second;
    EXPECT_DOUBLE_EQ( pc.counter_min_, 1.0 );
    EXPECT_DOUBLE_EQ( pc.counter_max_, 5.0 );
    EXPECT_DOUBLE_EQ( pc.counter_average_, 3.0 );
    EXPECT_EQ( pc.counter_seen_count_, 5 );

    // Destroy unique_ptr
    global_performance_counters = nullptr;
    per_context_performance_counters = nullptr;
}

TEST( test_in_memory_tracing, one_thread_multi_average ) {

    init_global_perf_counters( END_TIMER_COUNTERS + 1, 1,
                               NUMBER_OF_MODEL_TIMERS + 1, 1, 1 );

    int32_t thr_id = get_thread_id();

    global_performance_counters->insert_new_data_point(
        thr_id, BEGIN_FOR_WRITE_TIMER_ID, 1.0 );
    global_performance_counters->insert_new_data_point(
        thr_id, BEGIN_FOR_WRITE_TIMER_ID, 2.0 );
    global_performance_counters->insert_new_data_point(
        thr_id, BEGIN_FOR_WRITE_TIMER_ID, 3.0 );
    global_performance_counters->insert_new_data_point(
        thr_id, BEGIN_FOR_WRITE_TIMER_ID, 4.0 );
    global_performance_counters->insert_new_data_point(
        thr_id, BEGIN_FOR_WRITE_TIMER_ID, 5.0 );

    global_performance_counters->insert_new_data_point(
        thr_id, UPDATE_SVV_FROM_RESULT_SET_TIMER_ID, 2.0 );
    global_performance_counters->insert_new_data_point(
        thr_id, UPDATE_SVV_FROM_RESULT_SET_TIMER_ID, 4.0 );
    global_performance_counters->insert_new_data_point(
        thr_id, UPDATE_SVV_FROM_RESULT_SET_TIMER_ID, 6.0 );
    global_performance_counters->insert_new_data_point(
        thr_id, UPDATE_SVV_FROM_RESULT_SET_TIMER_ID, 8.0 );
    global_performance_counters->insert_new_data_point(
        thr_id, UPDATE_SVV_FROM_RESULT_SET_TIMER_ID, 10.0 );

    std::unordered_map<uint64_t, performance_counter_accumulator> agg_ctr =
        std::move( aggregate_thread_counters() );

    // One thread, so nothing to aggregate
    EXPECT_EQ( agg_ctr.size(), 2 );
    auto search = agg_ctr.find( BEGIN_FOR_WRITE_TIMER_ID );
    EXPECT_NE( search, agg_ctr.end() );
    const performance_counter_accumulator &pc = search->second;
    EXPECT_DOUBLE_EQ( pc.counter_min_, 1.0 );
    EXPECT_DOUBLE_EQ( pc.counter_max_, 5.0 );
    EXPECT_DOUBLE_EQ( pc.counter_average_, 3.0 );
    EXPECT_EQ( pc.counter_seen_count_, 5 );

    search = agg_ctr.find( UPDATE_SVV_FROM_RESULT_SET_TIMER_ID );
    EXPECT_NE( search, agg_ctr.end() );
    const performance_counter_accumulator &pc2 = search->second;
    EXPECT_DOUBLE_EQ( pc2.counter_min_, 2.0 );
    EXPECT_DOUBLE_EQ( pc2.counter_max_, 10.0 );
    EXPECT_DOUBLE_EQ( pc2.counter_average_, 6.0 );
    EXPECT_EQ( pc2.counter_seen_count_, 5 );

    // Destroy unique_ptr
    global_performance_counters = nullptr;
    per_context_performance_counters = nullptr;
}

TEST( test_in_memory_tracing, context_tracking_is_correct ) {
    for( int id = 0; id <= END_TIMER_COUNTERS; id++ ) {
        if( model_timer_positions[id] >= 0 ) {
            // we can only have number_of_model_timers positions
            EXPECT_LT( model_timer_positions[id], NUMBER_OF_MODEL_TIMERS );
            // the id, should match the id in the positions array
            EXPECT_EQ(
                id,
                model_timer_positions_to_timer_ids[model_timer_positions[id]] );
        }
    }
    // RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID should be at the end
    EXPECT_EQ( RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID, END_TIMER_COUNTERS );
    EXPECT_EQ(
        model_timer_positions[RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID] + 1,
        NUMBER_OF_MODEL_TIMERS );
    EXPECT_EQ(
        model_timer_positions_to_timer_ids
            [model_timer_positions[RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID]],
        RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID );

}

TEST( test_in_memory_tracing, context_tracing ) {
    init_global_perf_counters( END_TIMER_COUNTERS + 1, 1,
                               NUMBER_OF_MODEL_TIMERS + 1, 1, 1 );

    EXPECT_LE( 0, model_timer_positions
                      [COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID] );
    EXPECT_EQ( COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID,
               model_timer_positions_to_timer_ids
                   [model_timer_positions
                        [COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID]] );

    // unused
    EXPECT_EQ( -1, model_timer_positions[ARS_PUT_NEW_SAMPLE_TIMER_ID] );

    LOG( INFO ) << "Get context timers, expect empty";
    std::vector<context_timer> context_timers = get_context_timers();
    EXPECT_EQ( 0, context_timers.size() );

    LOG( INFO ) << "Reset context timers";
    reset_context_timers();

    uint32_t num_iter = 7;

    LOG( INFO ) << "start timers";
    for( uint32_t count = 0; count < num_iter; count++ ) {
        start_timer( COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID );
        std::this_thread::sleep_for( std::chrono::milliseconds( count ) );
        stop_timer( COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID );

        start_timer( ARS_PUT_NEW_SAMPLE_TIMER_ID );
        std::this_thread::sleep_for( std::chrono::milliseconds( count ) );
        stop_timer( ARS_PUT_NEW_SAMPLE_TIMER_ID );
    }
    LOG( INFO ) << "get context timers expect 1";
    context_timers = get_context_timers();
    EXPECT_EQ( 1, context_timers.size() );
    context_timer c_timer = context_timers.at( 0 );

    EXPECT_EQ( COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID,
               c_timer.counter_id );
    EXPECT_EQ( num_iter, c_timer.counter_seen_count );
    // 21 ms of sleep so it is safe to assue it is at least one ms
    EXPECT_GT( c_timer.timer_us_average, 1000 );

    std::unordered_set<uint64_t> filter_ids = {
        COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID};
    context_timers = get_aggregated_global_timers( filter_ids );
    EXPECT_EQ( 1, context_timers.size() );
    context_timer g_timer = context_timers.at( 0 );

    EXPECT_EQ( COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID,
               g_timer.counter_id );
    EXPECT_EQ( num_iter, g_timer.counter_seen_count );
    // these should be the same
    EXPECT_DOUBLE_EQ( g_timer.timer_us_average, c_timer.timer_us_average );

    reset_context_timers();

    context_timers = get_context_timers();
    EXPECT_EQ( 0, context_timers.size() );

    // but they should exist globally
    filter_ids = {COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID,
                  ARS_PUT_NEW_SAMPLE_TIMER_ID};
    context_timers = get_aggregated_global_timers( filter_ids );

    EXPECT_EQ( 2, context_timers.size() );
    context_timer g1_timer = context_timers.at( 0 );
    context_timer g2_timer = context_timers.at( 1 );

    if( g1_timer.counter_id !=
        COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID ) {
        std::swap( g1_timer, g2_timer );
    }

    EXPECT_EQ( COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID,
               g1_timer.counter_id );
    EXPECT_EQ( num_iter, g1_timer.counter_seen_count );
    // these should be the same
    EXPECT_DOUBLE_EQ( g1_timer.timer_us_average, c_timer.timer_us_average );

    EXPECT_EQ( ARS_PUT_NEW_SAMPLE_TIMER_ID, g2_timer.counter_id );
    EXPECT_EQ( num_iter, g2_timer.counter_seen_count );
    // these should be the same
    EXPECT_GT( g2_timer.timer_us_average, 1000 );

    // Destroy unique_ptr
    global_performance_counters = nullptr;
    per_context_performance_counters = nullptr;
}

void do_perf_tracking( unsigned int thr_id_int ) {
    int32_t thr_id = get_thread_id();

    global_performance_counters->insert_new_data_point(
        thr_id, BEGIN_FOR_WRITE_TIMER_ID, 1.0 * thr_id_int );
    global_performance_counters->insert_new_data_point(
        thr_id, BEGIN_FOR_WRITE_TIMER_ID, 2.0 * thr_id_int );
    global_performance_counters->insert_new_data_point(
        thr_id, BEGIN_FOR_WRITE_TIMER_ID, 3.0 * thr_id_int );
    global_performance_counters->insert_new_data_point(
        thr_id, BEGIN_FOR_WRITE_TIMER_ID, 4.0 * thr_id_int );
    global_performance_counters->insert_new_data_point(
        thr_id, BEGIN_FOR_WRITE_TIMER_ID, 5.0 * thr_id_int );

    global_performance_counters->insert_new_data_point(
        thr_id, UPDATE_SVV_FROM_RESULT_SET_TIMER_ID, 2.0 * thr_id_int );
    global_performance_counters->insert_new_data_point(
        thr_id, UPDATE_SVV_FROM_RESULT_SET_TIMER_ID, 4.0 * thr_id_int );
    global_performance_counters->insert_new_data_point(
        thr_id, UPDATE_SVV_FROM_RESULT_SET_TIMER_ID, 6.0 * thr_id_int );
    global_performance_counters->insert_new_data_point(
        thr_id, UPDATE_SVV_FROM_RESULT_SET_TIMER_ID, 8.0 * thr_id_int );
    global_performance_counters->insert_new_data_point(
        thr_id, UPDATE_SVV_FROM_RESULT_SET_TIMER_ID, 10.0 * thr_id_int );
}

TEST( test_in_memory_tracing, multi_thread_multi_average ) {

    unsigned int num_threads = 4;

    // 2 because we are doing 2 counters
    init_global_perf_counters( END_TIMER_COUNTERS + 1, num_threads,
                               NUMBER_OF_MODEL_TIMERS + 1, num_threads, 1 );

    std::vector<std::thread> threads_to_track;
    for( unsigned int i = 1; i < num_threads + 1; i++ ) {
        std::thread thr = std::thread( do_perf_tracking, i );
        threads_to_track.push_back( std::move( thr ) );
    }

    EXPECT_EQ( threads_to_track.size(), 4 );

    join_threads( threads_to_track );

    std::unordered_map<uint64_t, performance_counter_accumulator> agg_ctr =
        std::move( aggregate_thread_counters() );

    // One thread, so nothing to aggregate
    EXPECT_EQ( agg_ctr.size(), 2 );
    auto search = agg_ctr.find( BEGIN_FOR_WRITE_TIMER_ID );
    EXPECT_NE( search, agg_ctr.end() );
    const performance_counter_accumulator &pc = search->second;
    EXPECT_DOUBLE_EQ( pc.counter_min_, 1.0 );
    EXPECT_DOUBLE_EQ( pc.counter_max_, 20.0 );
    EXPECT_DOUBLE_EQ( pc.counter_average_, 7.5 );
    EXPECT_EQ( pc.counter_seen_count_, 20 );

    search = agg_ctr.find( UPDATE_SVV_FROM_RESULT_SET_TIMER_ID );
    EXPECT_NE( search, agg_ctr.end() );
    const performance_counter_accumulator &pc2 = search->second;
    EXPECT_DOUBLE_EQ( pc2.counter_min_, 2.0 );
    EXPECT_DOUBLE_EQ( pc2.counter_max_, 40.0 );
    EXPECT_DOUBLE_EQ( pc2.counter_average_, 15.0 );
    EXPECT_EQ( pc2.counter_seen_count_, 20 );

    // Destroy unique_ptr
    global_performance_counters = nullptr;
    per_context_performance_counters = nullptr;

    // Put back so tests can use this...
    // HACK
    init_global_perf_counters(
        END_TIMER_COUNTERS + 1, k_bench_num_clients, NUMBER_OF_MODEL_TIMERS + 1,
        k_bench_num_clients,
        k_perf_counter_client_multiplier *
            std::max( (int32_t) k_num_site_selector_worker_threads_per_client,
                      (int32_t) k_ss_num_poller_threads ) );
}
