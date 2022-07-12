#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/common/thread_utils.h"
#include "../src/concurrency/counter_lock.h"
#include "../src/concurrency/mutex_lock.h"
#include "../src/concurrency/semaphore.h"
#include "../src/concurrency/spinlock.h"

class lock_test : public ::testing::Test {};

#define lock_l template <typename L>

const std::vector<int32_t> k_spin_num_threads = {1, 2, 4, 8, 12};
// possible run with more threads
const std::vector<int32_t> k_thread_num_threads = {1, 2, 4, 8, 12 /*, 24, 48*/};

int32_t fib( int32_t n ) {
    int32_t ret = 0;
    if( n == 0 ) {
        return 0;
    } else if( n == 1 ) {
        return 1;
    }

    int32_t low = 0;
    int32_t high = 1;

    for( int32_t i = 2; i <= n; i++ ) {
        ret = low + high;
        low = high;
        high = ret;
    }
    return ret;
}

lock_l void set_and_get( int32_t num_threads ) {
    int64_t op_count = 0;
    L       lock;

    int32_t num_ops_per_thread = 1000;

    std::vector<std::thread> threads;
    for( int tid = 0; tid < num_threads; tid++ ) {
        std::thread t( [&op_count, &lock, &num_ops_per_thread, tid]() {
            for( int i = 0; i < num_ops_per_thread; i++ ) {
                int64_t count;
                bool    r = lock.lock();
                EXPECT_TRUE( r );
                count = op_count;
                op_count += 1;
                r = lock.unlock();
                EXPECT_TRUE( r );
                (void) count;
            }
        } );
        threads.push_back( std::move( t ) );
    }

    join_threads( threads );

    EXPECT_EQ( num_threads * num_ops_per_thread, op_count );
}

lock_l void long_work_in_crit( int32_t num_threads ) {
    int64_t op_count = 0;
    L       lock;

    int32_t num_ops_per_thread = 1000;

    std::vector<std::thread> threads;
    for( int tid = 0; tid < num_threads; tid++ ) {
        std::thread t( [&op_count, &lock, &num_ops_per_thread, tid]() {
            for( int i = 0; i < num_ops_per_thread; i++ ) {
                int64_t count;
                bool    r = lock.lock();
                EXPECT_TRUE( r );
                count = op_count;
                op_count += 1;
                fib( count );
                r = lock.unlock();
                EXPECT_TRUE( r );
            }
        } );
        threads.push_back( std::move( t ) );
    }

    join_threads( threads );

    EXPECT_EQ( num_threads * num_ops_per_thread, op_count );
}

lock_l void long_work_outside_crit( int32_t num_threads ) {
    int64_t op_count = 0;
    L       lock;

    int32_t num_ops_per_thread = 1000;

    std::vector<std::thread> threads;
    for( int tid = 0; tid < num_threads; tid++ ) {
        std::thread t( [&op_count, &lock, &num_ops_per_thread, tid]() {
            for( int i = 0; i < num_ops_per_thread; i++ ) {
                int64_t count;
                bool    r = lock.lock();
                EXPECT_TRUE( r );
                count = op_count;
                op_count += 1;
                r = lock.unlock();
                EXPECT_TRUE( r );
                fib( count );
            }
        } );
        threads.push_back( std::move( t ) );
    }

    join_threads( threads );

    EXPECT_EQ( num_threads * num_ops_per_thread, op_count );
}

TEST_F( lock_test, set_and_get_spinlock ) {
    for( int32_t count : k_spin_num_threads ) {
        set_and_get<spinlock>( count );
    }
}
TEST_F( lock_test, set_and_get_lock ) {
    for( int32_t count : k_thread_num_threads ) {
        set_and_get<mutex_lock>( count );
    }
}
TEST_F( lock_test, set_and_get_semaphore ) {
    for( int32_t count : k_thread_num_threads ) {
        set_and_get<semaphore>( count );
    }
}

TEST_F( lock_test, long_work_in_crit_spinlock ) {
    for( int32_t count : k_spin_num_threads ) {
        long_work_in_crit<spinlock>( count );
    }
}
TEST_F( lock_test, long_work_in_crit_lock ) {
    for( int32_t count : k_thread_num_threads ) {
        long_work_in_crit<mutex_lock>( count );
    }
}
TEST_F( lock_test, long_work_in_crit_semaphore ) {
    for( int32_t count : k_thread_num_threads ) {
        long_work_in_crit<semaphore>( count );
    }
}

TEST_F( lock_test, long_work_outside_crit_spinlock ) {
    for( int32_t count : k_spin_num_threads ) {
        long_work_outside_crit<spinlock>( count );
    }
}
TEST_F( lock_test, long_work_outside_crit_lock ) {
    for( int32_t count : k_thread_num_threads ) {
        long_work_outside_crit<mutex_lock>( count );
    }
}
TEST_F( lock_test, long_work_outside_crit_semaphore ) {
    for( int32_t count : k_thread_num_threads ) {
        long_work_outside_crit<semaphore>( count );
    }
}
