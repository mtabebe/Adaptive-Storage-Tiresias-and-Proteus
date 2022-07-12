#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <thread>

#include "../src/common/hw.h"
#include "../src/common/thread_utils.h"
#include "../src/common/system_stats.h"

class system_stats_test : public ::testing::Test {};

void calculate_running_pow_sum_worker( int* arr, int start_pos, int end_pos,
                                       int num_pows ) {
    int pos = 0;
    // store the data
    for( int i = start_pos; i < end_pos; i++ ) {
        arr[i] = pos;
        pos += 1;
        pos = pos % 5;
    }
    // init the pows
    int* pows = new int[num_pows];
    for( int i = 0; i < num_pows; i++ ) {
        pows[i] = 0;
    }

    // now do the pow
    for( int i = start_pos; i < end_pos; i++ ) {
        int val = 1;
        for ( int j = 0; j < num_pows; j++) {
            val = val * arr[i];
            pows[j] = val;
        }
    }
    // now do the sum
    int total = 0;
    for (int i = 0; i < num_pows; i++) {
        total += pows[i];
    }
    delete[] pows;

}

TEST_F( system_stats_test, check_stats ) {
    system_stats       s;
    machine_statistics ori_stats = s.get_machine_statistics();
    EXPECT_GE( ori_stats.average_cpu_load, 0 );
    EXPECT_GE( ori_stats.average_interval, 0 );
    EXPECT_GE( ori_stats.average_overall_load, 0 );
    EXPECT_GE( ori_stats.memory_usage, 0 );

    // allocate some stuff and iterate over it for a second;
    int num_threads = 5;
    int num_pows = 10;
    int num_pos_per_thread = 1000;

    int* arr = new int[num_threads * num_pos_per_thread];

    std::vector<std::thread> workers;
    workers.reserve( num_threads );

    for( int worker = 0; worker < num_threads; worker++ ) {
        int start = worker * num_pos_per_thread;
        std::thread t( calculate_running_pow_sum_worker, arr, start,
                       start + num_pos_per_thread - 1, num_pows );
        workers.push_back( std::move( t ) );
    }

    join_threads( workers );

    machine_statistics new_stats = s.get_machine_statistics();
    // EXPECT_GT( new_stats.average_cpu_load, ori_stats.average_cpu_load );
    // since beggining of time vs loop
    // EXPECT_LT( new_stats.average_interval, ori_stats.average_interval );
    // EXPECT_GE( new_stats.average_overall_load, ori_stats.average_overall_load );
    // we allocated memory
    // EXPECT_GE( new_stats.memory_usage, ori_stats.memory_usage );

    EXPECT_GT( new_stats.average_cpu_load, 0 );
    // since beggining of time vs loop
    EXPECT_GT( new_stats.average_interval, 0 );
    EXPECT_GT( new_stats.average_overall_load, 0 );
    // we allocated memory
    EXPECT_GT( new_stats.memory_usage, 0 );
    delete[] arr;
}

