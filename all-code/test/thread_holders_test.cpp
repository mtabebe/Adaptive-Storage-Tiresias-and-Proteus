#define GTEST_HAS_TR1_TUPLE 0

#include <atomic>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/common/gdcheck.h"
#include "../src/concurrency/thread_holders.h"

class thread_holders_test : public ::testing::Test {};

void run_thread_holder_test_thread( thread_holder* t_holder, uint32_t id,
                                    std::atomic<uint32_t>* accumulator ) {
    GASSERT_GT( id, 0 );
    std::this_thread::sleep_for( std::chrono::milliseconds( id - 1 ) );
    auto ret = accumulator->fetch_add( id );
    DVLOG( 40 ) << "id:" << id << ", ret:" << ret
                << ", current:" << accumulator->load();
    t_holder->mark_as_done();
}

TEST_F( thread_holders_test, thread_holders ) {
    thread_holders holders;
    std::atomic<uint32_t> accumulator;
    accumulator.store( 0 );

    uint32_t running_sum = 0;
    for ( uint32_t i = 1; i <= 10; i++) {
        thread_holder* t_holder = new thread_holder();
        t_holder->set_thread( std::move( std::unique_ptr<std::thread>(
            new std::thread( run_thread_holder_test_thread, t_holder, i,
                             &accumulator ) ) ) );
        holders.add_holder( t_holder );

        running_sum += i;
    }
    for( ;; ) {
        std::this_thread::yield();
        holders.gc_inactive_threads();
        if( accumulator.load() > 0 ) {
            break;
        }
    }
    GASSERT_GT( accumulator.load(), 0 );

    thread_holder* long_t_holder = new thread_holder();
    long_t_holder->set_thread( std::move( std::unique_ptr<std::thread>(
        new std::thread( run_thread_holder_test_thread, long_t_holder, 100,
                         &accumulator ) ) ) );
    holders.add_holder( long_t_holder );
    running_sum += 100;

    holders.wait_for_all_to_complete();

    EXPECT_EQ( running_sum, accumulator.load() );
}

