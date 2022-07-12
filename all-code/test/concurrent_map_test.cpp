#define GTEST_HAS_TR1_TUPLE 0

#include "gtest/gtest.h"

#include <chrono>
#include <thread>

#include "../src/common/thread_utils.h"
#include "../src/concurrency/concurrent_map.h"
#include "../src/concurrency/mutex_lock.h"
#include "../src/concurrency/semaphore.h"
#include "../src/concurrency/spinlock.h"

class concurrent_map_test : public ::testing::Test {};

#define lock_l template <typename L>

lock_l void simple_operations() {
    concurrent_map<int, char, std::hash<char>, L> map;
    std::pair<bool, char> found;
    std::pair<bool, char> previous;

    found = map.find( 1 );
    EXPECT_FALSE( found.first );

    previous = map.insert( 1, 'a' );
    EXPECT_FALSE( previous.first );

    found = map.find( 1 );
    EXPECT_TRUE( found.first );
    EXPECT_EQ( 'a', found.second );

    // won't overwrite
    previous = map.insert( 1, 'b' );
    EXPECT_TRUE( previous.first );
    EXPECT_EQ( 'a', previous.second );
    found = map.find( 1 );
    EXPECT_TRUE( found.first );
    EXPECT_EQ( 'a', found.second );

    // overwrite
    previous = map.insert( 1, 'b', true );
    EXPECT_TRUE( previous.first );
    EXPECT_EQ( 'a', previous.second );
    found = map.find( 1 );
    EXPECT_TRUE( found.first );
    EXPECT_EQ( 'b', found.second );

    previous = map.erase( 1 );
    EXPECT_TRUE( previous.first );
    EXPECT_EQ( 'b', previous.second );
    found = map.find( 1 );
    EXPECT_FALSE( found.first );

    previous = map.insert( 1, 'c' );
    EXPECT_FALSE( previous.first );
    found = map.find( 1 );
    EXPECT_TRUE( found.first );
    EXPECT_EQ( 'c', found.second );
}

TEST_F( concurrent_map_test, simple_operations_spinlock ) {
    simple_operations<spinlock>();
}
TEST_F( concurrent_map_test, simple_operations_lock ) {
    simple_operations<mutex_lock>();
}
TEST_F( concurrent_map_test, simple_operations_semaphore ) {
    simple_operations<semaphore>();
}

lock_l void multi_thread_operations() {
    concurrent_map<int, char, std::hash<char>, L> map;

    std::vector<int>  read_set = {1, 2};
    std::vector<char> write_set = {'a', 'b'};
    EXPECT_EQ( read_set.size(), write_set.size() );

    std::vector<std::thread> readers;

    for( unsigned int partition = 0; partition < write_set.size();
         partition++ ) {
        std::thread r = std::thread( [partition, &read_set, &write_set,
                                      &map]() {
            std::pair<bool, char> found = map.find( read_set[partition] );
            while( !found.first ) {
                std::this_thread::sleep_for( std::chrono::milliseconds( 1 ) );
                found = map.find( read_set[partition] );
            }
            // check for reads
            EXPECT_EQ( found.second, write_set[partition] );
        } );
        readers.push_back( std::move( r ) );
    }

    std::vector<std::thread> writers;

    for( unsigned int partition = 0; partition < write_set.size();
         partition++ ) {
        std::thread w =
            std::thread( [partition, &read_set, &write_set, &map]() {
                std::this_thread::sleep_for( std::chrono::milliseconds( 5 ) );
                std::pair<bool, char> previous =
                    map.insert( read_set[partition], write_set[partition] );
                EXPECT_FALSE( previous.first );
            } );
        writers.push_back( std::move( w ) );
    }

    join_threads( readers );
    join_threads( writers );
}

TEST_F( concurrent_map_test, multi_thread_operations_spinlock ) {
    multi_thread_operations<spinlock>();
}
TEST_F( concurrent_map_test, multi_thread_operations_lock ) {
    multi_thread_operations<mutex_lock>();
}
TEST_F( concurrent_map_test, multi_thread_operations_semaphore ) {
    multi_thread_operations<semaphore>();
}
