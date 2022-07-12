#define GTEST_HAS_TR1_TUPLE 0

#include "../src/site-selector/adaptive_reservoir_sampler.h"
#include "../src/common/gdcheck.h"
#include "../src/site-selector/partition_access.h"
#include "../src/site-selector/partition_data_location_table.h"
#include "../src/site-selector/site_selector_executor.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <thread>

class adaptive_reservoir_sampler_test : public ::testing::Test {};

class ars_tester : public adaptive_reservoir_sampler {

   public:
    using adaptive_reservoir_sampler::adaptive_reservoir_sampler;

    std::vector<tracking_summary> &get_summaries_in_reservoir() {
        return reservoir_;
    }

    float get_current_weight() { return adaptive_.get_current_weight(); }
};

TEST_F( adaptive_reservoir_sampler_test, reservoir_starts_with_placeholders ) {
    std::shared_ptr<partition_data_location_table> data_location_table =
        make_partition_data_location_table(
            construct_partition_data_location_table_configs( 1 ) );
    ars_tester ars( 1000, 1.0, 1.0, 1.0, data_location_table );
    std::vector<tracking_summary> &reservoir = ars.get_summaries_in_reservoir();
    EXPECT_EQ( reservoir.size(), 1000 );

    for( const auto &summary : reservoir ) {
        EXPECT_EQ( summary.is_placeholder_sample(), true );
    }
}

TEST_F( adaptive_reservoir_sampler_test, sample_and_decay_test ) {
    std::shared_ptr<partition_data_location_table> data_location_table =
        make_partition_data_location_table(
            construct_partition_data_location_table_configs( 1 ) );
    float      weight_incr = 100.0;
    float      decay_rate = 0.01;
    ars_tester ars( 1000, 1.0, weight_incr, decay_rate, data_location_table );

    // Guaranteed based on 1000 with 1.0 orig mult
    EXPECT_TRUE( ars.should_sample() );

    int times_to_sample = 1000;
    for( int i = 0; i < times_to_sample; i++ ) {
        // No longer guaranteed, so can't test
        ars.should_sample();
    }

    // Ensure that the weights went up substantially
    float exp_current_value = ( times_to_sample + 1 ) * weight_incr + 1000;
    EXPECT_EQ( ars.get_current_weight(), exp_current_value );

    ars.decay();
    EXPECT_EQ( ars.get_current_weight(), exp_current_value * decay_rate );

    ars.decay();
    EXPECT_EQ( ars.get_current_weight(),
               exp_current_value * decay_rate * decay_rate );
}

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

TEST_F( adaptive_reservoir_sampler_test, test_record_single_access_sample ) {

    std::shared_ptr<partition_data_location_table> data_location =
        create_test_location_table();
    partition_lock_mode lock_mode = partition_lock_mode::no_lock;

    // Record a sample that contains a partition access for {0, 0-9}. No within
    // correlations,
    // no across correlations.

    auto p_0_0_9_pid = create_partition_column_identifier( 0, 0, 9, 0, 1 );
    partition_access single_part_access( p_0_0_9_pid, true, 0 );
    ::cell_key_ranges ckr_0_3_5 = create_cell_key_ranges( 0, 3, 5, 0, 0 );

    std::vector<partition_access> part_accesses;
    part_accesses.push_back( single_part_access );

    transaction_partition_accesses txn_part_accesses( part_accesses,
                                                      {ckr_0_3_5}, {} );

    std::vector<transaction_partition_accesses> subsequent_accesses;
    across_transaction_access_correlations      across_txn_corrs(
        txn_part_accesses, subsequent_accesses );

    tracking_summary summary( txn_part_accesses, across_txn_corrs );

    ars_tester ars( 1, 1.0, 1.0, 1.0, data_location );

    // Verify that our reservoir is of size 1 (the placeholder sample)
    std::vector<tracking_summary> &reservoir = ars.get_summaries_in_reservoir();
    GASSERT_EQ( reservoir.size(), 1 );
    EXPECT_EQ( reservoir.at( 0 ).is_placeholder_sample(), true );

    // Store this sample, verify it is in the reservoir
    ars.put_new_sample( std::move( summary ) );
    GASSERT_EQ( reservoir.size(), 1 );
    EXPECT_EQ( reservoir.at( 0 ).is_placeholder_sample(), false );

    // Check the correlations. Since we have no correlations, none should show
    // up.
    std::shared_ptr<partition_payload> partition =
        data_location->get_partition( p_0_0_9_pid, lock_mode );

    transition_statistics &within_stats = partition->within_ww_txn_statistics_;
    EXPECT_EQ( within_stats.transitions_.size(), 0 );

    transition_statistics &across_stats = partition->across_ww_txn_statistics_;
    EXPECT_EQ( across_stats.transitions_.size(), 0 );

    partition->unlock( lock_mode );
}

TEST_F( adaptive_reservoir_sampler_test,
        test_record_multi_within_no_across_sample ) {

    std::shared_ptr<partition_data_location_table> data_location =
        create_test_location_table();
    partition_lock_mode lock_mode = partition_lock_mode::no_lock;

    // Create a transaction that accesses {0, 0-9} and {1, 0-9} with no across
    // transitions
    auto p_0_0_9_pid = create_partition_column_identifier( 0, 0, 9, 0, 1 );
    partition_access first_part_access( p_0_0_9_pid, true, 0 );
    ::cell_key_ranges ckr_0_3_5 = create_cell_key_ranges( 0, 3, 5, 0, 0 );

    auto p_1_0_9_pid = create_partition_column_identifier( 1, 0, 9, 0, 1 );
    partition_access second_part_access( p_1_0_9_pid, false, 0 );
    ::cell_key_ranges ckr_1_3_5 = create_cell_key_ranges( 1, 3, 5, 0, 0 );

    std::vector<partition_access> part_accesses;
    part_accesses.push_back( first_part_access );
    part_accesses.push_back( second_part_access );

    transaction_partition_accesses txn_part_accesses(
        part_accesses, {ckr_0_3_5}, {ckr_1_3_5} );

    std::vector<transaction_partition_accesses> subsequent_accesses;
    across_transaction_access_correlations      across_txn_corrs(
        txn_part_accesses, subsequent_accesses );

    tracking_summary summary( txn_part_accesses, across_txn_corrs );

    // Record the sample. Sample verification of reservoir as above.
    ars_tester ars( 1, 1.0, 1.0, 1.0, data_location );

    std::vector<tracking_summary> &reservoir = ars.get_summaries_in_reservoir();
    GASSERT_EQ( reservoir.size(), 1 );
    EXPECT_EQ( reservoir.at( 0 ).is_placeholder_sample(), true );
    ars.put_new_sample( std::move( summary ) );
    GASSERT_EQ( reservoir.size(), 1 );
    EXPECT_EQ( reservoir.at( 0 ).is_placeholder_sample(), false );

    // Get partition {0, 0-9}
    std::shared_ptr<partition_payload> partition =
        data_location->get_partition( p_0_0_9_pid, lock_mode );

    // We should have a within transition to {1, 0-9}
    transition_statistics &within_stats = partition->within_wr_txn_statistics_;
    GASSERT_EQ( within_stats.transitions_.size(), 1 );
    for( const auto &n : within_stats.transitions_ ) {
        EXPECT_EQ( n.first, p_1_0_9_pid );
        EXPECT_EQ( n.second, 1 );
    }

    // No across transitions
    transition_statistics &across_stats = partition->across_wr_txn_statistics_;
    EXPECT_EQ( across_stats.transitions_.size(), 0 );

    partition->unlock( lock_mode );

    // Get a partition {1, 0-9}
    partition = data_location->get_partition( p_1_0_9_pid, lock_mode );

    // We should have a within transition to {0, 0-9}
    transition_statistics &within_stats2 = partition->within_rw_txn_statistics_;
    GASSERT_EQ( within_stats2.transitions_.size(), 1 );
    for( const auto &n : within_stats2.transitions_ ) {
        EXPECT_EQ( n.first, p_0_0_9_pid );
        EXPECT_EQ( n.second, 1 );
    }

    // No across transitions
    transition_statistics &across_stats2 = partition->across_rw_txn_statistics_;
    EXPECT_EQ( across_stats2.transitions_.size(), 0 );
}

TEST_F( adaptive_reservoir_sampler_test, test_record_within_across_sample ) {

    std::shared_ptr<partition_data_location_table> data_location =
        create_test_location_table();
    partition_lock_mode lock_mode = partition_lock_mode::no_lock;

    auto p_0_0_9_pid = create_partition_column_identifier( 0, 0, 9, 0, 1 );
    partition_access first_part_access( p_0_0_9_pid, true, 0 );
    ::cell_key_ranges    ckr_0_3_5 = create_cell_key_ranges( 0, 3, 5, 0, 0 );

    auto p_1_0_9_pid = create_partition_column_identifier( 1, 0, 9, 0, 1 );
    partition_access second_part_access( p_1_0_9_pid, false, 0 );
    ::cell_key_ranges ckr_1_3_5 = create_cell_key_ranges( 1, 3, 5, 0, 0 );

    // Set up a {0, 0-9}{1, 0-9} txn with an across transition to itself
    std::vector<partition_access> part_accesses;
    part_accesses.push_back( first_part_access );
    part_accesses.push_back( second_part_access );

    transaction_partition_accesses txn_part_accesses(
        part_accesses, {ckr_0_3_5}, {ckr_1_3_5} );

    std::vector<transaction_partition_accesses> subsequent_accesses;
    subsequent_accesses.push_back( txn_part_accesses );
    across_transaction_access_correlations across_txn_corrs(
        txn_part_accesses, subsequent_accesses );

    tracking_summary summary( txn_part_accesses, across_txn_corrs );

    // Assert we replace the placeholder
    ars_tester                     ars( 1, 1.0, 1.0, 1.0, data_location );
    std::vector<tracking_summary> &reservoir = ars.get_summaries_in_reservoir();
    GASSERT_EQ( reservoir.size(), 1 );
    EXPECT_EQ( reservoir.at( 0 ).is_placeholder_sample(), true );
    ars.put_new_sample( std::move( summary ) );
    GASSERT_EQ( reservoir.size(), 1 );
    EXPECT_EQ( reservoir.at( 0 ).is_placeholder_sample(), false );

    // Get {0,0-9} partition
    std::shared_ptr<partition_payload> partition =
        data_location->get_partition( p_0_0_9_pid, lock_mode );

    // We have a transition to {1, 0-9}
    transition_statistics &within_stats = partition->within_wr_txn_statistics_;
    GASSERT_EQ( within_stats.transitions_.size(), 1 );

    // Transition to {0, 0-9} and {1, 0-9}
    transition_statistics &across_stats = partition->across_wr_txn_statistics_;
    GASSERT_EQ( across_stats.transitions_.size(), 1 );

    transition_statistics &across_stats_ww =
        partition->across_ww_txn_statistics_;
    GASSERT_EQ( across_stats_ww.transitions_.size(), 1 );

    partition->unlock( lock_mode );

    // Get {1,0-9} partition and do same verification
    partition = data_location->get_partition( p_1_0_9_pid, lock_mode );

    transition_statistics &within_stats2 = partition->within_rw_txn_statistics_;
    GASSERT_EQ( within_stats2.transitions_.size(), 1 );

    transition_statistics &across_stats2 = partition->across_rw_txn_statistics_;
    GASSERT_EQ( across_stats2.transitions_.size(), 1 );

    transition_statistics &across_stats2_rr =
        partition->across_rr_txn_statistics_;
    GASSERT_EQ( across_stats2_rr.transitions_.size(), 1 );
}

TEST_F( adaptive_reservoir_sampler_test, test_replace_sample ) {

    std::shared_ptr<partition_data_location_table> data_location =
        create_test_location_table();
    partition_lock_mode lock_mode = partition_lock_mode::no_lock;

    auto p_0_0_9_pid = create_partition_column_identifier( 0, 0, 9, 0, 1 );
    partition_access first_part_access( p_0_0_9_pid, true, 0 );
    ::cell_key_ranges ckr_0_3_5 = create_cell_key_ranges( 0, 3, 5, 0, 1 );

    auto p_1_0_9_pid = create_partition_column_identifier( 1, 0, 9, 0, 1 );
    partition_access second_part_access( p_1_0_9_pid, false, 0 );
    ::cell_key_ranges ckr_1_3_5 = create_cell_key_ranges( 1, 3, 5, 0, 1 );

    // Set up a {0, 0-9}{1, 0-9} txn with an across transition to itself
    std::vector<partition_access> part_accesses;
    part_accesses.push_back( first_part_access );
    part_accesses.push_back( second_part_access );

    transaction_partition_accesses txn_part_accesses(
        part_accesses, {ckr_0_3_5}, {ckr_1_3_5} );

    std::vector<transaction_partition_accesses> subsequent_accesses;
    subsequent_accesses.push_back( txn_part_accesses );
    across_transaction_access_correlations across_txn_corrs(
        txn_part_accesses, subsequent_accesses );

    tracking_summary summary( txn_part_accesses, across_txn_corrs );

    // Verify we replace the reservoir
    ars_tester                     ars( 1, 1.0, 1.0, 1.0, data_location );
    std::vector<tracking_summary> &reservoir = ars.get_summaries_in_reservoir();
    GASSERT_EQ( reservoir.size(), 1 );
    EXPECT_EQ( reservoir.at( 0 ).is_placeholder_sample(), true );
    ars.put_new_sample( std::move( summary ) );
    GASSERT_EQ( reservoir.size(), 1 );
    EXPECT_EQ( reservoir.at( 0 ).is_placeholder_sample(), false );

    // Same assertion as above: make sure everything is in place
    std::shared_ptr<partition_payload> partition =
        data_location->get_partition( p_0_0_9_pid, lock_mode );

    transition_statistics &within_stats = partition->within_wr_txn_statistics_;
    GASSERT_EQ( within_stats.transitions_.size(), 1 );

    transition_statistics &across_stats = partition->across_wr_txn_statistics_;
    GASSERT_EQ( across_stats.transitions_.size(), 1 );

    transition_statistics &across_stats_ww =
        partition->across_ww_txn_statistics_;
    GASSERT_EQ( across_stats_ww.transitions_.size(), 1 );

    partition->unlock( lock_mode );

    partition = data_location->get_partition( p_1_0_9_pid, lock_mode );

    transition_statistics &within_stats2 = partition->within_rw_txn_statistics_;
    GASSERT_EQ( within_stats2.transitions_.size(), 1 );

    transition_statistics &across_stats2 = partition->across_rw_txn_statistics_;
    GASSERT_EQ( across_stats2.transitions_.size(), 1 );

    transition_statistics &across_stats2_rr =
        partition->across_rr_txn_statistics_;
    GASSERT_EQ( across_stats2_rr.transitions_.size(), 1 );

    // Replace the above sample with a {0,0-9} -> {0,0-9} txn
    std::vector<partition_access> part_accesses2;
    part_accesses2.push_back( first_part_access );

    transaction_partition_accesses txn_part_accesses2( part_accesses2,
                                                       {ckr_0_3_5}, {} );

    std::vector<transaction_partition_accesses> subsequent_accesses2;
    subsequent_accesses2.push_back( txn_part_accesses2 );
    across_transaction_access_correlations across_txn_corrs2(
        txn_part_accesses2, subsequent_accesses2 );

    tracking_summary summary2( txn_part_accesses2, across_txn_corrs2 );
    ars.put_new_sample( std::move( summary2 ) );

    // Get {0,0-9} partition
    partition = data_location->get_partition( p_0_0_9_pid, lock_mode );

    // Should be no within transitions (note that the entry still exists, but
    // count is 0)
    transition_statistics &within_stats3 = partition->within_wr_txn_statistics_;
    GASSERT_EQ( within_stats3.transitions_.size(), 1 );
    for( const auto &n : within_stats3.transitions_ ) {
        EXPECT_EQ( n.second, 0 );
    }

    // Should only have a transition to ourselves
    transition_statistics &across_stats3 = partition->across_wr_txn_statistics_;
    GASSERT_EQ( across_stats3.transitions_.size(), 1 );
    for( const auto &n : across_stats3.transitions_ ) {
        EXPECT_EQ( n.second, 0 );
    }
    transition_statistics &across_stats4 = partition->across_ww_txn_statistics_;
    GASSERT_EQ( across_stats4.transitions_.size(), 1 );
    for( const auto &n : across_stats4.transitions_ ) {
        EXPECT_EQ( n.second, 1 );
    }

    partition->unlock( lock_mode );

    partition = data_location->get_partition( p_1_0_9_pid, lock_mode );

    partition->unlock( lock_mode );
}

void put_sample_thread(
    std::shared_ptr<partition_data_location_table> data_location,
    transaction_partition_accesses &               txn_part_accesses,
    across_transaction_access_correlations &       across_txn_corrs,
    transaction_partition_accesses &               txn_part_accesses2,
    across_transaction_access_correlations &       across_txn_corrs2 ) {
    ars_tester ars( 1, 1.0, 1.0, 1.0, data_location );

    for( int i = 0; i < 100; i++ ) {
        tracking_summary summary( txn_part_accesses, across_txn_corrs );
        ars.put_new_sample( std::move( summary ) );

        tracking_summary summary2( txn_part_accesses2, across_txn_corrs2 );
        ars.put_new_sample( std::move( summary2 ) );
    }
}

TEST_F( adaptive_reservoir_sampler_test, test_concurrency ) {

    std::shared_ptr<partition_data_location_table> data_location =
        create_test_location_table();
    partition_lock_mode lock_mode = partition_lock_mode::no_lock;

    auto p_0_0_9_pid = create_partition_column_identifier( 0, 0, 9, 0, 1 );
    partition_access first_part_access( p_0_0_9_pid, true, 0 );
    ::cell_key_ranges ckr_0_3_5 = create_cell_key_ranges( 0, 3, 5, 0, 1 );

    auto p_1_0_9_pid = create_partition_column_identifier( 1, 0, 9, 0, 1 );
    partition_access second_part_access( p_1_0_9_pid, false, 0 );
    ::cell_key_ranges ckr_1_3_5 = create_cell_key_ranges( 1, 3, 5, 0, 1 );

    // Set up a {0, 0-9}{1, 0-9} txn with an across transition to itself
    std::vector<partition_access> part_accesses;
    part_accesses.push_back( first_part_access );
    part_accesses.push_back( second_part_access );

    transaction_partition_accesses txn_part_accesses(
        part_accesses, {ckr_0_3_5}, {ckr_1_3_5} );

    std::vector<transaction_partition_accesses> subsequent_accesses;
    subsequent_accesses.push_back( txn_part_accesses );
    across_transaction_access_correlations across_txn_corrs(
        txn_part_accesses, subsequent_accesses );

    // Set up a {0, 0-9} txn with an across transition to itself
    std::vector<partition_access> part_accesses2;
    part_accesses2.push_back( first_part_access );

    transaction_partition_accesses txn_part_accesses2( part_accesses2,
                                                       {ckr_0_3_5}, {} );

    std::vector<transaction_partition_accesses> subsequent_accesses2;
    subsequent_accesses2.push_back( txn_part_accesses2 );
    across_transaction_access_correlations across_txn_corrs2(
        txn_part_accesses2, subsequent_accesses2 );

    std::vector<std::unique_ptr<std::thread>> threads;
    for( int i = 0; i < 20; i++ ) {
        std::unique_ptr<std::thread> t = std::make_unique<std::thread>(
            put_sample_thread, data_location, std::ref( txn_part_accesses ),
            std::ref( across_txn_corrs ), std::ref( txn_part_accesses2 ),
            std::ref( across_txn_corrs2 ) );
        threads.push_back( std::move( t ) );
    }

    for( int i = 0; i < 20; i++ ) {
        threads.at( i )->join();
    }

    // no need for transition locks, because there is no multi-threading past
    // this point

    // Only last transition should stick
    std::shared_ptr<partition_payload> partition =
        data_location->get_partition( p_0_0_9_pid, lock_mode );

    transition_statistics &within_stats = partition->within_wr_txn_statistics_;
    GASSERT_EQ( within_stats.transitions_.size(), 1 );
    for( const auto &n : within_stats.transitions_ ) {
        EXPECT_EQ( n.first, p_1_0_9_pid );
        EXPECT_EQ( n.second, 0 );
    }

    transition_statistics &across_stats = partition->across_wr_txn_statistics_;
    GASSERT_EQ( across_stats.transitions_.size(), 1 );
    for( const auto &n : across_stats.transitions_ ) {
        EXPECT_EQ( n.second, 0 );
    }

    transition_statistics &across_stats_ww =
        partition->across_ww_txn_statistics_;
    GASSERT_EQ( across_stats_ww.transitions_.size(), 1 );
    for( const auto &n : across_stats_ww.transitions_ ) {
        // Because there are 20 threads
        EXPECT_EQ( n.second, 20 );
    }

    partition->unlock( lock_mode );

    partition = data_location->get_partition( p_1_0_9_pid, lock_mode );

    transition_statistics &within_stats2 = partition->within_rw_txn_statistics_;
    GASSERT_EQ( within_stats2.transitions_.size(), 1 );
    for( const auto &n : within_stats2.transitions_ ) {
        EXPECT_EQ( n.first, p_0_0_9_pid );
        EXPECT_EQ( n.second, 0 );
    }

    transition_statistics &across_stats2 = partition->across_rw_txn_statistics_;
    GASSERT_EQ( across_stats2.transitions_.size(), 1 );
    for( const auto &n : across_stats2.transitions_ ) {
        EXPECT_EQ( n.first, p_0_0_9_pid );
        EXPECT_EQ( n.second, 0 );
    }

    transition_statistics &across_stats2_rr =
        partition->across_rr_txn_statistics_;
    GASSERT_EQ( across_stats2_rr.transitions_.size(), 1 );
    for( const auto &n : across_stats2_rr.transitions_ ) {
        EXPECT_EQ( n.first, p_1_0_9_pid );
        EXPECT_EQ( n.second, 0 );
    }

    partition->unlock( lock_mode );
}

