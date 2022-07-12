#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <thread>

#include "../src/common/hw.h"
#include "../src/common/thread_utils.h"
#include "../src/concurrency/counter_lock.h"

class counter_lock_test : public ::testing::Test {};

TEST_F( counter_lock_test, check_alignment ) {

    std::vector<counter_lock> cl_arr( 4 );
    uint64_t                  prev = ( uint64_t ) & ( cl_arr[0] );

    for( unsigned int i = 1; i < cl_arr.size(); i++ ) {
        uint64_t addr = ( uint64_t ) & ( cl_arr[i] );
        // Aligned  only guarantees that things are alignment size apart
        // https://github.com/bglasber/horizondb/pull/13#issuecomment-354333812
        EXPECT_GE( addr - prev, CACHELINE_SIZE );
        prev = addr;
    }
}

TEST_F( counter_lock_test, inc_read_value ) {
    counter_lock cl;
    EXPECT_EQ( cl.get_counter(), 0 );
    EXPECT_EQ( cl.incr_counter(), 1 );
    EXPECT_EQ( cl.get_counter(), 1 );
}

TEST_F( counter_lock_test, concurrent_increment ) {
    counter_lock cl;
    uint32_t     num_ops_per_thread = 1000;
    uint32_t     num_threads = 4;

    std::vector<std::thread> threads;
    for( unsigned int tid = 0; tid < num_threads; tid++ ) {
        std::thread t( [&num_ops_per_thread, &cl, tid]() {
            for( unsigned int i = 0; i < num_ops_per_thread; i++ ) {
                cl.incr_counter();
                cl.get_counter();
            }
        } );
        threads.push_back( std::move( t ) );
    }
    join_threads( threads );
    uint64_t cl_val = cl.get_counter();
    EXPECT_EQ( num_ops_per_thread * num_threads, cl_val );
}

TEST_F( counter_lock_test, wait_until_value_reached ) {
    counter_lock cl;
    uint64_t     desired_value = 1000;

    std::vector<std::thread> threads;
    std::thread              wait_t(
        [&desired_value, &cl]() { cl.wait_until( desired_value ); } );
    threads.push_back( std::move( wait_t ) );
    std::thread incr_t( [&desired_value, &cl]() {
        for( unsigned int i = 0; i < desired_value; i++ ) {
            cl.incr_counter();
        }
    } );
    threads.push_back( std::move( incr_t ) );
    join_threads( threads );
    EXPECT_EQ( cl.get_counter(), 1000 );
}
