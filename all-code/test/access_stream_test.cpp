#define GTEST_HAS_TR1_TUPLE 0

#include "../src/site-selector/access_stream.h"
#include "../src/common/gdcheck.h"
#include "../src/site-selector/adaptive_reservoir_sampler.h"
#include "../src/site-selector/partition_access.h"
#include "../src/site-selector/partition_data_location_table.h"
#include "../src/site-selector/site_selector_executor.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

class access_stream_test : public ::testing::Test {};

class no_replace_sampler : public adaptive_reservoir_sampler {
   public:
    using adaptive_reservoir_sampler::adaptive_reservoir_sampler;

    void put_new_sample( tracking_summary &&sample ) override {
        add_counts_for_sample( sample );
        remove_counts_for_sample( reservoir_[replacement_index] );
        reservoir_[replacement_index] = std::move( sample );
        replacement_index++;
    }

    void set_replacement_index( int val ) { replacement_index = val; }

    int replacement_index;
};
static std::shared_ptr<partition_data_location_table>
    create_test_location_table() {

    uint32_t tid_0 = 0;
    uint32_t tid_1 = 1;
    uint32_t default_partition_size = 10;

    uint32_t t1_num_cols = 2;
    uint32_t t2_num_cols = 3;

    uint32_t t1_col_part_size = 2;
    uint32_t t2_col_part_size = 2;

    partition_type::type t1_p_type = partition_type::ROW;
    partition_type::type t2_p_type = partition_type::COLUMN;

    storage_tier_type::type t1_s_type = storage_tier_type::MEMORY;
    storage_tier_type::type t2_s_type = storage_tier_type::DISK;

    std::vector<cell_data_type> t1_cols = {cell_data_type::STRING,
                                           cell_data_type::UINT64};
    std::vector<cell_data_type> t2_cols = {
        cell_data_type::INT64, cell_data_type::STRING, cell_data_type::UINT64};

    partition_lock_mode lock_mode = partition_lock_mode::no_lock;

    std::shared_ptr<partition_data_location_table> data_location =
        make_partition_data_location_table(
            construct_partition_data_location_table_configs( 1 ) );

    uint32_t created_tid = data_location->create_table( create_table_metadata(
        "0", tid_0, t1_num_cols, t1_cols, 5 /*num_records_in_chain*/,
        5 /*num_records_in_snapshot_chain*/, 0 /*site_location*/,
        default_partition_size, t1_col_part_size, default_partition_size,
        t1_col_part_size, t1_p_type, t1_s_type, false /* enable sec storage */,
        "/tmp/" ) );
    GASSERT_EQ( tid_0, created_tid );
    created_tid = data_location->create_table( create_table_metadata(
        "1", tid_1, t2_num_cols, t2_cols, 5 /*num_records_in_chain*/,
        5 /*num_records_in_snapshot_chain*/, 0 /*site_location*/,
        default_partition_size, t2_col_part_size, default_partition_size,
        t2_col_part_size, t2_p_type, t2_s_type, false /* enable sec storage */,
        "/tmp/" ) );
    GASSERT_EQ( tid_1, created_tid );

    auto p_0_0_9_pid = create_partition_column_identifier( tid_0, 0, 9, 0, 1 );
    auto p_0_0_9_payload = std::make_shared<partition_payload>(
        create_partition_column_identifier( tid_0, 0, 9, 0, 1 ) );

    auto p_1_0_9_pid = create_partition_column_identifier( tid_1, 0, 9, 0, 1 );
    auto p_1_0_9_payload = std::make_shared<partition_payload>(
        create_partition_column_identifier( tid_1, 0, 9, 0, 1 ) );

    auto insert_ret =
        data_location->insert_partition( p_0_0_9_payload, lock_mode );
    EXPECT_NE( nullptr, insert_ret );
    EXPECT_EQ( insert_ret->identifier_, p_0_0_9_payload->identifier_ );
    insert_ret->unlock( lock_mode );

    insert_ret = data_location->insert_partition( p_1_0_9_payload, lock_mode );
    EXPECT_NE( nullptr, insert_ret );
    EXPECT_EQ( insert_ret->identifier_, p_1_0_9_payload->identifier_ );
    insert_ret->unlock( lock_mode );

    return data_location;
}

TEST_F( access_stream_test, test_record_one_entry ) {

    std::shared_ptr<partition_data_location_table> data_location =
        create_test_location_table();
    partition_lock_mode lock_mode = partition_lock_mode::no_lock;

    std::unique_ptr<adaptive_reservoir_sampler> ars =
        std::make_unique<adaptive_reservoir_sampler>( 1, 1.0, 1.0, 1.0,
                                                      data_location );

    access_stream astream( 10, ars.get(), std::chrono::milliseconds( 0 ),
                           std::chrono::milliseconds( 0 ) );

    // Record a {0,0-9}{1,0-9} txn with no subsequent transitions
    auto p_0_0_9_pid = create_partition_column_identifier( 0, 0, 9, 0, 1 );
    auto p_1_0_9_pid = create_partition_column_identifier( 1, 0, 9, 0, 1 );
    partition_access part_access( p_0_0_9_pid, true, 0 );
    partition_access part_access2( p_1_0_9_pid, true, 0 );
    transaction_partition_accesses txn_partition_accesses;
    txn_partition_accesses.partition_accesses_.push_back( part_access );
    txn_partition_accesses.partition_accesses_.push_back( part_access2 );

    astream.write_access_to_stream( txn_partition_accesses,
                                    std::chrono::high_resolution_clock::now(),
                                    true );

    std::this_thread::sleep_for( std::chrono::milliseconds( 200 ) );

    std::shared_ptr<partition_payload> partition =
        data_location->get_partition( p_0_0_9_pid, lock_mode );

    // 1 within transition, no across
    {
        std::lock_guard<std::mutex> lk(
            partition->within_ww_txn_statistics_.mutex_ );
        auto &transitions = partition->within_ww_txn_statistics_.transitions_;
        GASSERT_EQ( transitions.size(), 1 );
        for( const auto &n : transitions ) {
            GASSERT_EQ( n.first, p_1_0_9_pid );
            GASSERT_EQ( n.second, 1 );
        }
        GASSERT_EQ( partition->across_ww_txn_statistics_.transitions_.size(),
                    0 );
    }

    partition->unlock( lock_mode );
}

TEST_F( access_stream_test, test_drop_stale_entry ) {

    std::shared_ptr<partition_data_location_table> data_location =
        create_test_location_table();
    partition_lock_mode lock_mode = partition_lock_mode::no_lock;

    std::unique_ptr<adaptive_reservoir_sampler> ars =
        std::make_unique<adaptive_reservoir_sampler>( 1, 1.0, 1.0, 1.0,
                                                      data_location );

    access_stream astream( 10, ars.get(), std::chrono::milliseconds( 0 ),
                           std::chrono::milliseconds( 0 ) );

    // Record a {0,0-9}, {1,0-20} transition. 0-20 does not exist
    auto p_0_0_9_pid = create_partition_column_identifier( 0, 0, 9, 0, 1 );
    auto p_1_0_20_pid = create_partition_column_identifier( 1, 0, 20, 0, 1 );
    partition_access part_access( p_0_0_9_pid, true, 0 );
    partition_access part_access2( p_1_0_20_pid, true, 0 );
    transaction_partition_accesses txn_partition_accesses;
    txn_partition_accesses.partition_accesses_.push_back( part_access );
    txn_partition_accesses.partition_accesses_.push_back( part_access2 );

    astream.write_access_to_stream( txn_partition_accesses,
                                    std::chrono::high_resolution_clock::now(),
                                    true );

    std::this_thread::sleep_for( std::chrono::milliseconds( 200 ) );

    std::shared_ptr<partition_payload> partition =
        data_location->get_partition( p_0_0_9_pid, lock_mode );

    // 1 within transition, no across
    //
    {
        std::lock_guard<std::mutex> lk(
            partition->within_ww_txn_statistics_.mutex_ );
        auto &transitions = partition->within_ww_txn_statistics_.transitions_;
        GASSERT_EQ( transitions.size(), 1 );
        for( const auto &n : transitions ) {
            // N.B. this is stale but will be cleaned up by the optimizer
            GASSERT_EQ( n.first, p_1_0_20_pid );
            GASSERT_EQ( n.second, 1 );
        }

        GASSERT_EQ( partition->across_ww_txn_statistics_.transitions_.size(),
                   0 );
    }

    partition->unlock( lock_mode );

    partition = data_location->get_partition( p_1_0_20_pid, lock_mode );
    GASSERT_TRUE( partition == nullptr );
}

TEST_F( access_stream_test, test_pick_up_across_transition ) {

    std::shared_ptr<partition_data_location_table> data_location =
        create_test_location_table();
    partition_lock_mode lock_mode = partition_lock_mode::no_lock;

    std::unique_ptr<adaptive_reservoir_sampler> ars =
        std::make_unique<adaptive_reservoir_sampler>( 100, 1.0, 1.0, 1.0,
                                                      data_location );

    access_stream astream( 10, ars.get(), std::chrono::milliseconds( 0 ),
                           std::chrono::milliseconds( 100 ) );

    // Record a {0,0-9}{1,0-9} txn with itself as a subsequent txn
    auto p_0_0_9_pid = create_partition_column_identifier( 0, 0, 9, 0, 1 );
    auto p_1_0_9_pid = create_partition_column_identifier( 1, 0, 9, 0, 1 );
    partition_access part_access( p_0_0_9_pid, true, 0 );
    partition_access part_access2( p_1_0_9_pid, true, 0 );
    transaction_partition_accesses txn_partition_accesses;
    txn_partition_accesses.partition_accesses_.push_back( part_access );
    txn_partition_accesses.partition_accesses_.push_back( part_access2 );

    astream.write_access_to_stream( txn_partition_accesses,
                                    std::chrono::high_resolution_clock::now(),
                                    true );
    astream.write_access_to_stream( txn_partition_accesses,
                                    std::chrono::high_resolution_clock::now(),
                                    true );

    std::this_thread::sleep_for( std::chrono::milliseconds( 300 ) );

    std::shared_ptr<partition_payload> partition =
        data_location->get_partition( p_0_0_9_pid, lock_mode );

    {
        // 1 within transition
        std::lock_guard<std::mutex> lk(
            partition->within_ww_txn_statistics_.mutex_ );
        auto &transitions = partition->within_ww_txn_statistics_.transitions_;
        GASSERT_EQ( transitions.size(), 1 );
        for( const auto &n : transitions ) {
            GASSERT_EQ( n.first, p_1_0_9_pid );
            // Count is likely 2, but could be 1 if we replaced our only
            // non-placeholder sample
            GASSERT_GE( n.second, 1 );
        }
    }

    {
        std::lock_guard<std::mutex> lk(
            partition->across_ww_txn_statistics_.mutex_ );
        // We will have a transition to both of our partition accesses (ourself)
        GASSERT_EQ( partition->across_ww_txn_statistics_.transitions_.size(),
                   2 );
        for( const auto &n :
             partition->across_ww_txn_statistics_.transitions_ ) {
            // Count is likely 1, but we could have replaced our own entry
            GASSERT_GE( n.second, 0 );
        }
    }

    partition->unlock( lock_mode );
}

TEST_F( access_stream_test, test_respect_tracking_interval ) {

    std::shared_ptr<partition_data_location_table> data_location =
        create_test_location_table();
    partition_lock_mode lock_mode = partition_lock_mode::no_lock;

    std::unique_ptr<no_replace_sampler> no_replace_ars =
        std::make_unique<no_replace_sampler>( 100, 1.0, 1.0, 1.0,
                                              data_location );
    no_replace_ars->set_replacement_index( 0 );

    std::unique_ptr<adaptive_reservoir_sampler> ars =
        std::move( no_replace_ars );

    access_stream astream( 10, ars.get(), std::chrono::milliseconds( 0 ),
                           std::chrono::milliseconds( 100 ) );

    // Record a {0,0-9}{1,0-9} txn with {0,0-9} as a subsequent txn
    //{1,0-9} txn should be out of range
    auto p_0_0_9_pid = create_partition_column_identifier( 0, 0, 9, 0, 1 );
    auto p_1_0_9_pid = create_partition_column_identifier( 1, 0, 9, 0, 1 );
    partition_access part_access( p_0_0_9_pid, true, 0 );
    partition_access part_access2( p_1_0_9_pid, true, 0 );

    transaction_partition_accesses txn_partition_accesses;
    txn_partition_accesses.partition_accesses_.push_back( part_access );
    txn_partition_accesses.partition_accesses_.push_back( part_access2 );

    transaction_partition_accesses sub_txn_accesses1;
    sub_txn_accesses1.partition_accesses_.push_back( part_access );

    transaction_partition_accesses sub_txn_accesses2;
    sub_txn_accesses2.partition_accesses_.push_back( part_access2 );

    astream.write_access_to_stream( txn_partition_accesses,
                                    std::chrono::high_resolution_clock::now(),
                                    true );
    astream.write_access_to_stream(
        sub_txn_accesses1, std::chrono::high_resolution_clock::now(), true );

    std::this_thread::sleep_for( std::chrono::milliseconds( 200 ) );

    astream.write_access_to_stream(
        sub_txn_accesses2, std::chrono::high_resolution_clock::now(), true );

    std::this_thread::sleep_for( std::chrono::milliseconds( 300 ) );

    std::shared_ptr<partition_payload> partition =
        data_location->get_partition( p_0_0_9_pid, lock_mode );

    {
        std::lock_guard<std::mutex> lk(
            partition->within_ww_txn_statistics_.mutex_ );
        // We will have all three samples b/c we used a no_replace_sampler
        auto &transitions = partition->within_ww_txn_statistics_.transitions_;
        GASSERT_EQ( transitions.size(), 1 );
        for( const auto &n : transitions ) {
            GASSERT_EQ( n.first, p_1_0_9_pid );
            GASSERT_EQ( n.second, 1 );
        }
    }

    {
        std::lock_guard<std::mutex> lk(
            partition->across_ww_txn_statistics_.mutex_ );
        // We will have a transition to only the partition that fell within the
        // tracking interval
        GASSERT_EQ( partition->across_ww_txn_statistics_.transitions_.size(),
                    1 );
        for( const auto &n :
             partition->across_ww_txn_statistics_.transitions_ ) {
            GASSERT_EQ( n.second, 1 );
        }
    }

    partition->unlock( lock_mode );
}

