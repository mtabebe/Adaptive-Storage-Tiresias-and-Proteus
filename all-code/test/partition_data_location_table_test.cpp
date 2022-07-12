#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

#include "../src/data-site/db/cell_identifier.h"
#include "../src/data-site/db/partition_metadata.h"
#include "../src/site-selector/partition_data_location_table.h"
#include "../src/site-selector/site_selector_executor.h"

class partition_data_location_table_test : public ::testing::Test {};

TEST_F( partition_data_location_table_test, data_location_table_test ) {
    uint32_t tid_0 = 0;
    uint32_t tid_1 = 1;
    uint32_t loc = 0;

    partition_lock_mode lock_mode = partition_lock_mode::no_lock;

    std::shared_ptr<partition_data_location_table> data_location =
        make_partition_data_location_table(
            construct_partition_data_location_table_configs( 1 ) );

    uint32_t default_partition_size = 10;
    uint32_t default_column_size = 2;
    uint32_t col_start = 0;
    uint32_t col_end = 4;

    std::vector<cell_data_type> col_types = {
        cell_data_type::INT64, cell_data_type::INT64, cell_data_type::INT64,
        cell_data_type::INT64, cell_data_type::INT64};

    auto table_metadata_0 = create_table_metadata(
        "0", tid_0, ( col_end - col_start ) + 1, col_types,
        5 /*num_records_in_chain*/, 5 /*num_records_in_snapshot_chain*/,
        0 /*site_location*/, default_partition_size, default_column_size,
        default_partition_size, default_column_size,
        partition_type::type::COLUMN, storage_tier_type::type::DISK,
        false /* enable sec storage */, "/tmp/" );

    auto table_metadata_1 = create_table_metadata(
        "1", tid_1, col_end + 1, col_types, 5 /*num_records_in_chain*/,
        5 /*num_records_in_snapshot_chain*/, 0 /*site_location*/,
        default_partition_size, default_column_size, default_partition_size,
        default_column_size, partition_type::type::COLUMN,
        storage_tier_type::type::DISK, false /* enable sec storage */,
        "/tmp/" );

    auto created_table_id = data_location->create_table( table_metadata_0 );
    EXPECT_EQ( created_table_id, tid_0 );
    created_table_id = data_location->create_table( table_metadata_1 );
    EXPECT_EQ( created_table_id, tid_1 );

    auto p_0_0_9_payload = std::make_shared<partition_payload>(
        create_partition_column_identifier( tid_0, 0, 9, 0, 1 ) );
    auto transition_dest =
        create_partition_column_identifier( tid_1, 0, 9, 0, 1 );
    p_0_0_9_payload->read_accesses_ = 22;
    p_0_0_9_payload->write_accesses_ = 20;
    p_0_0_9_payload->sample_based_read_accesses_ = 11;
    p_0_0_9_payload->sample_based_write_accesses_ = 10;

    auto p_0_0_9_record = std::make_shared<partition_location_information>();
    p_0_0_9_record->master_location_ = loc;
    p_0_0_9_record->partition_types_[loc] = partition_type::type::ROW;
    p_0_0_9_record->version_ = 1;
    bool compare_and_swapped =
        p_0_0_9_payload->compare_and_swap_location_information( p_0_0_9_record,
                                                                nullptr );
    EXPECT_TRUE( compare_and_swapped );

    auto transition_dest_copy = transition_dest;
    p_0_0_9_payload->within_rr_txn_statistics_.transitions_.insert(
        std::make_pair<partition_column_identifier, int64_t>(
            create_partition_column_identifier( tid_1, 0, 9, 0, 1 ), 3 ) );
    p_0_0_9_payload->within_rw_txn_statistics_.transitions_.insert(
        std::make_pair<partition_column_identifier, int64_t>(
            create_partition_column_identifier( 2, 0, 9, 0, 1 ), 4 ) );
    p_0_0_9_payload->within_ww_txn_statistics_.transitions_.insert(
        std::make_pair<partition_column_identifier, int64_t>(
            create_partition_column_identifier( 2, 0, 9, 0, 1 ), 3 ) );
    p_0_0_9_payload->across_rr_txn_statistics_.transitions_.insert(
        std::make_pair<partition_column_identifier, int64_t>(
            create_partition_column_identifier( tid_0, 0, 9, 0, 1 ), 3 ) );
    p_0_0_9_payload->across_rw_txn_statistics_.transitions_.insert(
        std::make_pair<partition_column_identifier, int64_t>(
            create_partition_column_identifier( tid_0, 0, 9, 0, 1 ), 3 ) );
    p_0_0_9_payload->across_rr_txn_statistics_.transitions_.insert(
        std::make_pair<partition_column_identifier, int64_t>(
            create_partition_column_identifier( tid_1, 0, 9, 0, 1 ), 2 ) );

    EXPECT_EQ( 0, data_location->get_approx_total_number_of_partitions() );

    auto insert_ret =
        data_location->insert_partition( p_0_0_9_payload, lock_mode );
    EXPECT_NE( nullptr, insert_ret );
    EXPECT_EQ( insert_ret->identifier_, p_0_0_9_payload->identifier_ );
    insert_ret->unlock( lock_mode );
    EXPECT_EQ( 1, data_location->get_approx_total_number_of_partitions() );

    auto split_partitions = data_location->split_partition(
        insert_ret, 5, k_unassigned_col, partition_type::type::ROW,
        partition_type::type::COLUMN, storage_tier_type::type::MEMORY,
        storage_tier_type::type::DISK, 2, 3, lock_mode );
    EXPECT_EQ( 2, split_partitions.size() );
    EXPECT_NE( nullptr, split_partitions.at( 0 ) );
    auto p_0_0_4 = create_partition_column_identifier( tid_0, 0, 4, 0, 1 );
    EXPECT_EQ( p_0_0_4, split_partitions.at( 0 )->identifier_ );
    EXPECT_EQ( split_partitions.at( 0 )->read_accesses_, 11 );
    EXPECT_EQ( split_partitions.at( 0 )->write_accesses_, 10 );
    EXPECT_EQ( split_partitions.at( 0 )->sample_based_read_accesses_, 5 );
    EXPECT_EQ( split_partitions.at( 0 )->sample_based_write_accesses_, 5 );

    auto table_1_transition =
        create_partition_column_identifier( tid_1, 0, 9, 0, 1 );
    auto table_2_transition =
        create_partition_column_identifier( 2, 0, 9, 0, 1 );
    auto low_part_id = create_partition_column_identifier( tid_0, 0, 4, 0, 1 );
    auto high_part_id = create_partition_column_identifier( tid_0, 5, 9, 0, 1 );
    EXPECT_EQ( 2, data_location->get_approx_total_number_of_partitions() );

    EXPECT_EQ( split_partitions.at( 0 )
                   ->get_location_information( lock_mode )
                   ->update_destination_slot_,
               2 );
    EXPECT_EQ( split_partitions.at( 1 )
                   ->get_location_information( lock_mode )
                   ->update_destination_slot_,
               3 );

    // Ensure we get the same transition as before the split
    EXPECT_EQ(
        split_partitions.at( 0 )->within_rr_txn_statistics_.transitions_.size(),
        1 );
    auto search =
        split_partitions.at( 0 )->within_rr_txn_statistics_.transitions_.find(
            table_1_transition );
    EXPECT_NE( search, split_partitions.at( 0 )
                           ->within_rr_txn_statistics_.transitions_.end() );
    EXPECT_EQ( search->second, 1 );
    EXPECT_EQ(
        split_partitions.at( 0 )->within_rw_txn_statistics_.transitions_.size(),
        1 );
    search =
        split_partitions.at( 0 )->within_rw_txn_statistics_.transitions_.find(
            table_2_transition );
    EXPECT_NE( search, split_partitions.at( 0 )
                           ->within_rw_txn_statistics_.transitions_.end() );
    EXPECT_EQ( search->second, 2 );
    EXPECT_EQ(
        split_partitions.at( 0 )->within_ww_txn_statistics_.transitions_.size(),
        1 );
    search =
        split_partitions.at( 0 )->within_ww_txn_statistics_.transitions_.find(
            table_2_transition );
    EXPECT_NE( search, split_partitions.at( 0 )
                           ->within_ww_txn_statistics_.transitions_.end() );
    EXPECT_EQ( search->second, 1 );

    // Ensure table 1 across transition is still there
    search =
        split_partitions.at( 0 )->across_rr_txn_statistics_.transitions_.find(
            table_1_transition );
    EXPECT_NE( search, split_partitions.at( 0 )
                           ->across_rr_txn_statistics_.transitions_.end() );
    EXPECT_EQ( search->second, 1 );

    // Ensure our self transition has been broken up, since we are a splitting
    // partition
    search =
        split_partitions.at( 0 )->across_rr_txn_statistics_.transitions_.find(
            create_partition_column_identifier( tid_0, 0, 9, 0, 1 ) );
    EXPECT_EQ( search, split_partitions.at( 0 )
                           ->across_rr_txn_statistics_.transitions_.end() );
    search =
        split_partitions.at( 0 )->across_rw_txn_statistics_.transitions_.find(
            create_partition_column_identifier( tid_0, 0, 9, 0, 1 ) );
    EXPECT_EQ( search, split_partitions.at( 0 )
                           ->across_rw_txn_statistics_.transitions_.end() );
    search =
        split_partitions.at( 0 )->across_rr_txn_statistics_.transitions_.find(
            low_part_id );
    EXPECT_NE( search, split_partitions.at( 0 )
                           ->across_rr_txn_statistics_.transitions_.end() );
    EXPECT_EQ( search->second, 1 );
    search =
        split_partitions.at( 0 )->across_rw_txn_statistics_.transitions_.find(
            low_part_id );
    EXPECT_NE( search, split_partitions.at( 0 )
                           ->across_rw_txn_statistics_.transitions_.end() );
    EXPECT_EQ( search->second, 1 );
    search =
        split_partitions.at( 0 )->across_rr_txn_statistics_.transitions_.find(
            high_part_id );
    EXPECT_NE( search, split_partitions.at( 0 )
                           ->across_rr_txn_statistics_.transitions_.end() );
    EXPECT_EQ( search->second, 1 );
    search =
        split_partitions.at( 0 )->across_rw_txn_statistics_.transitions_.find(
            high_part_id );
    EXPECT_NE( search, split_partitions.at( 0 )
                           ->across_rw_txn_statistics_.transitions_.end() );
    EXPECT_EQ( search->second, 1 );

    // Ensure that we get half the access counts as the lower partition id
    EXPECT_EQ( split_partitions.at( 0 )->read_accesses_, 11 );
    EXPECT_EQ( split_partitions.at( 0 )->write_accesses_, 10 );
    EXPECT_EQ( split_partitions.at( 0 )->sample_based_read_accesses_, 5 );
    EXPECT_EQ( split_partitions.at( 0 )->sample_based_write_accesses_, 5 );

    EXPECT_NE( nullptr, split_partitions.at( 1 ) );
    auto p_0_5_9 = create_partition_column_identifier( tid_0, 5, 9, 0, 1 );
    EXPECT_EQ( p_0_5_9, split_partitions.at( 1 )->identifier_ );

    // Ensure we get half the access counts as the upper partition id
    EXPECT_EQ( split_partitions.at( 1 )->read_accesses_, 11 );
    EXPECT_EQ( split_partitions.at( 1 )->write_accesses_, 10 );
    EXPECT_EQ( split_partitions.at( 1 )->sample_based_read_accesses_, 6 );
    EXPECT_EQ( split_partitions.at( 1 )->sample_based_write_accesses_, 5 );

    // Ensure we get the transition to table 1
    search =
        split_partitions.at( 1 )->within_rr_txn_statistics_.transitions_.find(
            table_1_transition );
    EXPECT_NE( search, split_partitions.at( 1 )
                           ->within_rr_txn_statistics_.transitions_.end() );
    EXPECT_EQ( search->second,
               2 );  // We get the +1 b/c 3 doesn't divide evenly
    search =
        split_partitions.at( 1 )->within_rw_txn_statistics_.transitions_.find(
            table_2_transition );
    EXPECT_NE( search, split_partitions.at( 1 )
                           ->within_rw_txn_statistics_.transitions_.end() );
    EXPECT_EQ( search->second, 2 );
    search =
        split_partitions.at( 1 )->within_ww_txn_statistics_.transitions_.find(
            table_2_transition );
    EXPECT_NE( search, split_partitions.at( 1 )
                           ->within_ww_txn_statistics_.transitions_.end() );
    EXPECT_EQ( search->second, 2 );

    // Ensure table 1 across transition is still there
    search =
        split_partitions.at( 1 )->across_rr_txn_statistics_.transitions_.find(
            table_1_transition );
    EXPECT_NE( search, split_partitions.at( 1 )
                           ->across_rr_txn_statistics_.transitions_.end() );
    EXPECT_EQ( search->second, 1 );

    // Ensure our self transition has been broken up, since we are a splitting
    // partition
    search =
        split_partitions.at( 1 )->across_rr_txn_statistics_.transitions_.find(
            create_partition_column_identifier( tid_0, 0, 9, 0, 1 ) );
    EXPECT_EQ( search, split_partitions.at( 1 )
                           ->across_rr_txn_statistics_.transitions_.end() );
    search =
        split_partitions.at( 1 )->across_rr_txn_statistics_.transitions_.find(
            low_part_id );
    EXPECT_NE( search, split_partitions.at( 1 )
                           ->across_rr_txn_statistics_.transitions_.end() );
    EXPECT_EQ( search->second, 2 );
    search =
        split_partitions.at( 1 )->across_rr_txn_statistics_.transitions_.find(
            high_part_id );
    EXPECT_NE( search, split_partitions.at( 1 )
                           ->across_rr_txn_statistics_.transitions_.end() );
    EXPECT_EQ( search->second, 2 );
    search =
        split_partitions.at( 1 )->across_rw_txn_statistics_.transitions_.find(
            create_partition_column_identifier( tid_0, 0, 9, 0, 1 ) );
    EXPECT_EQ( search, split_partitions.at( 1 )
                           ->across_rw_txn_statistics_.transitions_.end() );
    search =
        split_partitions.at( 1 )->across_rw_txn_statistics_.transitions_.find(
            low_part_id );
    EXPECT_NE( search, split_partitions.at( 1 )
                           ->across_rw_txn_statistics_.transitions_.end() );
    EXPECT_EQ( search->second, 2 );
    search =
        split_partitions.at( 1 )->across_rw_txn_statistics_.transitions_.find(
            high_part_id );
    EXPECT_NE( search, split_partitions.at( 1 )
                           ->across_rw_txn_statistics_.transitions_.end() );
    EXPECT_EQ( search->second, 2 );

    unlock_payloads( split_partitions, lock_mode );

    std::vector<cell_key_ranges> ckrs;
    cell_key_ranges              ckr;
    ckr.table_id = tid_0;
    ckr.col_id_start = 0;
    ckr.col_id_end = 0;
    ckr.row_id_start = 6;
    ckr.row_id_end = 8;
    cell_key_ranges ckr_0_6_8 = ckr;
    ckrs.emplace_back( ckr_0_6_8 );

    ckr.row_id_start = 14;
    ckr.row_id_end = 18;
    cell_key_ranges ckr_0_14_18 = ckr;
    ckrs.emplace_back( ckr_0_14_18 );

    auto found_payloads =
        data_location->get_or_create_partitions( ckrs, lock_mode );
    EXPECT_EQ( 3, data_location->get_approx_total_number_of_partitions() );

    EXPECT_EQ( 2, found_payloads.size() );
    //
    EXPECT_NE( nullptr, found_payloads.at( 0 ) );
    EXPECT_EQ( p_0_5_9, found_payloads.at( 0 )->identifier_ );
    //
    EXPECT_NE( nullptr, found_payloads.at( 1 ) );
    auto p_0_10_19 = create_partition_column_identifier( tid_0, 10, 19, 0, 1 );
    EXPECT_EQ( p_0_10_19, found_payloads.at( 1 )->identifier_ );

    std::vector<std::shared_ptr<partition_payload>> inserted_payloads;
    for( auto& payload : found_payloads ) {
        auto existing_record = payload->get_location_information();
        if( !existing_record ) {
            auto record = std::make_shared<partition_location_information>();
            record->master_location_ = loc;
            record->partition_types_[loc] = partition_type::type::COLUMN;
            record->version_ = 1;
            compare_and_swapped =
                payload->compare_and_swap_location_information( record,
                                                                nullptr );
            EXPECT_TRUE( compare_and_swapped );
            inserted_payloads.emplace_back( payload );
        }
    }
    EXPECT_EQ( 1, inserted_payloads.size() );
    EXPECT_NE( nullptr, inserted_payloads.at( 0 ) );
    EXPECT_EQ( p_0_10_19, inserted_payloads.at( 0 )->identifier_ );

    unlock_payloads( found_payloads, lock_mode );

    ckrs.clear();

    ckr.row_id_start = 30;
    ckr.row_id_end = 33;
    cell_key_ranges ckr_0_30_33 = ckr;
    ckrs.emplace_back( ckr_0_30_33 );

    ckrs.emplace_back( ckr_0_14_18 );

    ckr.table_id = 1;
    ckr.row_id_start = 17;
    ckr.row_id_end = 19;
    cell_key_ranges ckr_1_17_19 = ckr;
    ckrs.emplace_back( ckr_1_17_19 );

    ckr.row_id_start = 5;
    ckr.row_id_end = 7;
    cell_key_ranges ckr_1_5_7 = ckr;
    ckrs.emplace_back( ckr_1_5_7 );

    found_payloads = data_location->get_or_create_partitions( ckrs, lock_mode );
    EXPECT_EQ( 6, data_location->get_approx_total_number_of_partitions() );

    EXPECT_EQ( 4, found_payloads.size() );
    //
    EXPECT_NE( nullptr, found_payloads.at( 0 ) );
    EXPECT_EQ( p_0_10_19, found_payloads.at( 0 )->identifier_ );
    //
    EXPECT_NE( nullptr, found_payloads.at( 1 ) );
    auto p_0_30_39 = create_partition_column_identifier( tid_0, 30, 39, 0, 1 );
    EXPECT_EQ( p_0_30_39, found_payloads.at( 1 )->identifier_ );
    //
    EXPECT_NE( nullptr, found_payloads.at( 2 ) );
    auto p_1_0_9 = create_partition_column_identifier( tid_1, 0, 9, 0, 1 );
    EXPECT_EQ( p_1_0_9, found_payloads.at( 2 )->identifier_ );
    //
    EXPECT_NE( nullptr, found_payloads.at( 3 ) );
    auto p_1_10_19 = create_partition_column_identifier( tid_1, 10, 19, 0, 1 );
    EXPECT_EQ( p_1_10_19, found_payloads.at( 3 )->identifier_ );

    inserted_payloads.clear();
    for( auto& payload : found_payloads ) {
        auto existing_record = payload->get_location_information();
        if( !existing_record ) {
            auto record = std::make_shared<partition_location_information>();
            record->master_location_ = loc;
            record->partition_types_[loc] = partition_type::type::COLUMN;
            record->version_ = 1;
            compare_and_swapped =
                payload->compare_and_swap_location_information( record,
                                                                nullptr );
            EXPECT_TRUE( compare_and_swapped );
            inserted_payloads.emplace_back( payload );
        }
    }

    EXPECT_EQ( 3, inserted_payloads.size() );
    EXPECT_NE( nullptr, inserted_payloads.at( 0 ) );
    EXPECT_EQ( p_0_30_39, inserted_payloads.at( 0 )->identifier_ );
    EXPECT_NE( nullptr, inserted_payloads.at( 1 ) );
    EXPECT_EQ( p_1_0_9, inserted_payloads.at( 1 )->identifier_ );
    EXPECT_NE( nullptr, inserted_payloads.at( 2 ) );
    EXPECT_EQ( p_1_10_19, inserted_payloads.at( 2 )->identifier_ );

    found_payloads.at( 0 )->unlock( lock_mode );
    found_payloads.at( 1 )->unlock( lock_mode );

    found_payloads.at( 2 )->write_accesses_ = 10;
    found_payloads.at( 2 )->read_accesses_ = 11;
    found_payloads.at( 3 )->write_accesses_ = 10;
    found_payloads.at( 3 )->read_accesses_ = 11;

    found_payloads.at( 2 )->sample_based_write_accesses_ = 5;
    found_payloads.at( 2 )->sample_based_read_accesses_ = 5;
    found_payloads.at( 3 )->sample_based_write_accesses_ = 5;
    found_payloads.at( 3 )->sample_based_read_accesses_ = 6;

    auto p1_id = found_payloads.at( 1 )->identifier_;
    auto p2_id = found_payloads.at( 2 )->identifier_;
    auto p3_id = found_payloads.at( 3 )->identifier_;

    auto p1_id_copy = p1_id;
    auto p2_id_copy = p2_id;
    auto p3_id_copy = p3_id;

    auto p1_id_copy2 = p1_id;
    auto p2_id_copy2 = p2_id;
    auto p3_id_copy2 = p3_id;

    auto p1_id_copy3 = p1_id;
    auto p2_id_copy3 = p2_id;
    auto p3_id_copy3 = p3_id;

    auto p1_id_copy4 = p1_id;
    auto p2_id_copy4 = p2_id;
    auto p3_id_copy4 = p3_id;

    auto p1_id_copy5 = p1_id;
    auto p2_id_copy5 = p2_id;
    auto p3_id_copy5 = p3_id;

    found_payloads.at( 2 )->within_rr_txn_statistics_.transitions_.insert(
        std::make_pair<partition_column_identifier, int64_t>(
            std::move( p3_id_copy ), 5 ) );
    found_payloads.at( 2 )->within_rr_txn_statistics_.transitions_.insert(
        std::make_pair<partition_column_identifier, int64_t>(
            std::move( p1_id_copy ), 1 ) );
    found_payloads.at( 2 )->within_rw_txn_statistics_.transitions_.insert(
        std::make_pair<partition_column_identifier, int64_t>(
            std::move( p1_id_copy4 ), 1 ) );
    // P2 ID should be converted to p_1_0_19

    found_payloads.at( 2 )->within_wr_txn_statistics_.transitions_.insert(
        std::make_pair<partition_column_identifier, int64_t>(
            std::move( p2_id_copy4 ), 1 ) );
    found_payloads.at( 2 )->across_rr_txn_statistics_.transitions_.insert(
        std::make_pair<partition_column_identifier, int64_t>(
            std::move( p1_id_copy2 ), 3 ) );
    found_payloads.at( 2 )->across_rr_txn_statistics_.transitions_.insert(
        std::make_pair<partition_column_identifier, int64_t>(
            std::move( p3_id_copy2 ), 3 ) );
    found_payloads.at( 2 )->across_ww_txn_statistics_.transitions_.insert(
        std::make_pair<partition_column_identifier, int64_t>(
            std::move( p3_id_copy4 ), 3 ) );

    found_payloads.at( 3 )->within_rr_txn_statistics_.transitions_.insert(
        std::make_pair<partition_column_identifier, int64_t>(
            std::move( p2_id_copy ), 5 ) );
    found_payloads.at( 3 )->within_wr_txn_statistics_.transitions_.insert(
        std::make_pair<partition_column_identifier, int64_t>(
            std::move( p2_id_copy5 ), 5 ) );
    found_payloads.at( 3 )->across_rr_txn_statistics_.transitions_.insert(
        std::make_pair<partition_column_identifier, int64_t>(
            std::move( p2_id_copy2 ), 7 ) );
    found_payloads.at( 3 )->across_rr_txn_statistics_.transitions_.insert(
        std::make_pair<partition_column_identifier, int64_t>(
            std::move( p1_id_copy3 ), 7 ) );
    found_payloads.at( 3 )->across_ww_txn_statistics_.transitions_.insert(
        std::make_pair<partition_column_identifier, int64_t>(
            std::move( p3_id_copy5 ), 3 ) );

    auto merged_partition = data_location->merge_partition(
        found_payloads.at( 2 ), found_payloads.at( 3 ),
        partition_type::type::ROW, storage_tier_type::type::MEMORY, 0,
        lock_mode );
    EXPECT_NE( nullptr, merged_partition );
    auto p_1_0_19 = create_partition_column_identifier( tid_1, 0, 19, 0, 1 );
    EXPECT_EQ( p_1_0_19, merged_partition->identifier_ );
    EXPECT_EQ( 5, data_location->get_approx_total_number_of_partitions() );

    EXPECT_EQ( merged_partition->read_accesses_, 22 );
    EXPECT_EQ( merged_partition->write_accesses_, 20 );
    EXPECT_EQ( merged_partition->sample_based_read_accesses_, 11 );
    EXPECT_EQ( merged_partition->sample_based_write_accesses_, 10 );

    auto merged_partition_search =
        merged_partition->within_rr_txn_statistics_.transitions_.find(
            p_1_0_19 );
    EXPECT_NE( merged_partition_search,
               merged_partition->within_rr_txn_statistics_.transitions_.end() );
    EXPECT_EQ( merged_partition_search->second, 10 );

    merged_partition_search =
        merged_partition->within_rr_txn_statistics_.transitions_.find( p1_id );
    EXPECT_NE( merged_partition_search,
               merged_partition->within_rr_txn_statistics_.transitions_.end() );
    EXPECT_EQ( merged_partition_search->second, 1 );

    merged_partition_search =
        merged_partition->within_rw_txn_statistics_.transitions_.find( p1_id );
    EXPECT_NE( merged_partition_search,
               merged_partition->within_rw_txn_statistics_.transitions_.end() );
    EXPECT_EQ( merged_partition_search->second, 1 );
    EXPECT_EQ( merged_partition->within_rw_txn_statistics_.transitions_.size(),
               1 );

    merged_partition_search =
        merged_partition->within_wr_txn_statistics_.transitions_.find(
            p_1_0_19 );
    EXPECT_NE( merged_partition_search,
               merged_partition->within_wr_txn_statistics_.transitions_.end() );
    EXPECT_EQ( merged_partition_search->second, 6 );
    merged_partition_search =
        merged_partition->within_wr_txn_statistics_.transitions_.find(
            p_1_0_9 );
    EXPECT_EQ( merged_partition_search,
               merged_partition->within_wr_txn_statistics_.transitions_.end() );
    merged_partition_search =
        merged_partition->within_wr_txn_statistics_.transitions_.find(
            p_1_10_19 );
    EXPECT_EQ( merged_partition_search,
               merged_partition->within_wr_txn_statistics_.transitions_.end() );

    merged_partition_search =
        merged_partition->within_rr_txn_statistics_.transitions_.find( p2_id );
    EXPECT_EQ( merged_partition_search,
               merged_partition->within_rr_txn_statistics_.transitions_.end() );
    merged_partition_search =
        merged_partition->within_rr_txn_statistics_.transitions_.find( p3_id );
    EXPECT_EQ( merged_partition_search,
               merged_partition->within_rr_txn_statistics_.transitions_.end() );

    // Same key, should merge to 10
    merged_partition_search =
        merged_partition->across_rr_txn_statistics_.transitions_.find( p1_id );
    EXPECT_NE( merged_partition_search,
               merged_partition->across_rr_txn_statistics_.transitions_.end() );
    EXPECT_EQ( merged_partition_search->second, 10 );

    // transitions are regarded as the same because being merged, should merge
    // to 10
    merged_partition_search =
        merged_partition->across_rr_txn_statistics_.transitions_.find(
            p_1_0_19 );
    EXPECT_NE( merged_partition_search,
               merged_partition->across_rr_txn_statistics_.transitions_.end() );
    EXPECT_EQ( merged_partition_search->second, 10 );

    merged_partition_search =
        merged_partition->across_ww_txn_statistics_.transitions_.find(
            p_1_0_19 );
    EXPECT_NE( merged_partition_search,
               merged_partition->across_ww_txn_statistics_.transitions_.end() );
    EXPECT_EQ( merged_partition_search->second, 6 );

    merged_partition->unlock( lock_mode );
}
