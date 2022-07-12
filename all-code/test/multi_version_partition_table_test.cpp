#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

#include "../src/common/gdcheck.h"
#include "../src/data-site/db/cell_identifier.h"
#include "../src/data-site/db/partition_metadata.h"
#include "../src/site-selector/multi_version_partition_table.h"

class multi_version_partition_table_test : public ::testing::Test {};

void check_contains( multi_version_partition_data_location_table*     table,
                     const cell_key_ranges&                           ckr,
                     const partition_column_identifier_unordered_set& pid_set,
                     const partition_lock_mode& lock_mode ) {
    auto found_payloads = table->get_partition( ckr, lock_mode );
    EXPECT_EQ( found_payloads.size(), pid_set.size() );
    partition_column_identifier_unordered_set already_seen;
    for( auto payload : found_payloads ) {
        EXPECT_NE( payload, nullptr );
        EXPECT_EQ( 1, pid_set.count( payload->identifier_ ) );
        already_seen.insert( payload->identifier_ );
    }
    EXPECT_EQ( pid_set.size(), already_seen.size() );
}

void check_partitions_match(
    const std::vector<partition_column_identifier>&               expected_pids,
    const std::vector<std::shared_ptr<partition_payload>>& actual_payloads ) {
    EXPECT_EQ( expected_pids.size(), actual_payloads.size() );
    for( uint32_t pos = 0; pos < expected_pids.size(); pos++ ) {
        EXPECT_NE( nullptr, actual_payloads.at( pos ) );
        EXPECT_EQ( expected_pids.at( pos ),
                   actual_payloads.at( pos )->identifier_ );
    }
}
void mv_table_check_present( multi_version_partition_data_location_table* table,
                             const cell_key&                              ck,
                             const partition_column_identifier&           pid,
                             uint32_t                   inserted_count,
                             const partition_lock_mode& lock_mode ) {
    auto found_payload = table->get_partition( ck, lock_mode );
    if( inserted_count ) {
        EXPECT_NE( nullptr, found_payload );
        EXPECT_EQ( pid, found_payload->identifier_ );
        found_payload->unlock( lock_mode );
    } else {
        EXPECT_EQ( nullptr, found_payload );
    }
    found_payload = table->get_partition( pid, lock_mode );
    if( inserted_count ) {
        EXPECT_NE( nullptr, found_payload );
        EXPECT_EQ( pid, found_payload->identifier_ );
        found_payload->unlock( lock_mode );
    } else {
        EXPECT_EQ( nullptr, found_payload );
    }
    cell_key_ranges ckr;
    ckr.table_id = pid.table_id;
    ckr.col_id_start = pid.column_start;
    ckr.col_id_end = pid.column_end;
    ckr.row_id_start = pid.partition_start;
    ckr.row_id_end = pid.partition_end;

    auto found_payloads = table->get_partition( ckr, lock_mode );
    if( inserted_count ) {
        EXPECT_EQ( 1, found_payloads.size() );
        found_payload = found_payloads.at( 0 );
        EXPECT_NE( nullptr, found_payload );

        EXPECT_EQ( pid, found_payload->identifier_ );
        found_payload->unlock( lock_mode );
    } else {
        EXPECT_EQ( 0, found_payloads.size() );
    }
}

void insert_and_check(
    multi_version_partition_data_location_table*                  table,
    std::vector<std::vector<partition_column_identifier>>&        pids,
    std::vector<std::vector<std::shared_ptr<partition_payload>>>& payloads,
    const partition_column_identifier_unordered_set&              to_insert,
    const partition_column_identifier_unordered_set&              inserted,
    const partition_lock_mode&                                    lock_mode ) {

    partition_column_identifier_unordered_set already_inserted;

    EXPECT_EQ( pids.size(), payloads.size() );
    cell_key ck;
    for( uint64_t pos = 0; pos < pids.size(); pos++ ) {
        ck.row_id = pos;
        EXPECT_EQ( pids.at( pos ).size(), payloads.at( pos ).size() );
        for( uint64_t col = 0; col < pids.at( pos ).size(); col++ ) {
            ck.col_id = col;
            const auto& pid = pids.at( pos ).at( col );
            auto        payload = payloads.at( pos ).at( col );

            ck.table_id = pid.table_id;
            uint32_t count = to_insert.count( pid );
            if( count ) {
                if( already_inserted.count( pid ) == 0 ) {
                    auto inserted =
                        table->insert_partition( payload, lock_mode );
                    EXPECT_NE( nullptr, inserted );
                    inserted->unlock( lock_mode );
                    already_inserted.insert( pid );
                }
            }
            uint32_t inserted_count = inserted.count( pid );
            mv_table_check_present( table, ck, pid, inserted_count, lock_mode );
        }
    }
}

void delete_and_check(
    multi_version_partition_data_location_table*                  table,
    std::vector<std::vector<partition_column_identifier>>&        pids,
    std::vector<std::vector<std::shared_ptr<partition_payload>>>& payloads,
    const partition_column_identifier_unordered_set&              to_delete,
    const partition_column_identifier_unordered_set&              inserted,
    const partition_lock_mode&                                    lock_mode ) {

    partition_column_identifier_unordered_set already_deleted;

    EXPECT_EQ( pids.size(), payloads.size() );
    cell_key ck;
    for( uint64_t pos = 0; pos < pids.size(); pos++ ) {
        ck.row_id = pos;
        EXPECT_EQ( pids.at( pos ).size(), payloads.at( pos ).size() );
        for( uint64_t col = 0; col < pids.at( pos ).size(); col++ ) {
            ck.col_id = col;
            const auto& pid = pids.at( pos ).at( col );
            auto        payload = payloads.at( pos ).at( col );

            ck.table_id = pid.table_id;
            uint32_t count = to_delete.count( pid );
            if( count ) {
                if( already_deleted.count( pid ) == 0 ) {
                    auto del_payload = table->remove_partition( payload );
                    EXPECT_NE( del_payload, nullptr );
                    del_payload->unlock( lock_mode );
                    already_deleted.insert( pid );
                }
            }

            uint32_t inserted_count = inserted.count( pid );
            mv_table_check_present( table, ck, pid, inserted_count, lock_mode );
        }
    }
}

TEST_F( multi_version_partition_table_test, simple_tree_operations ) {
    uint32_t table_id = 0;
    uint32_t default_partition_size = 10;
    uint32_t default_column_size = 2;
    uint32_t col_start = 0;
    uint32_t col_end = 4;

    std::vector<cell_data_type> col_types = {
        cell_data_type::INT64, cell_data_type::INT64, cell_data_type::INT64,
        cell_data_type::INT64, cell_data_type::INT64};

    auto table_metadata = create_table_metadata(
        "t", table_id, col_end + 1, col_types, 5 /*num_records_in_chain*/,
        5 /*num_records_in_snapshot_chain*/, 0 /*site_location*/,
        default_partition_size, default_column_size,
        default_partition_size, default_column_size,
        partition_type::type::COLUMN, storage_tier_type::type::MEMORY );
    multi_version_partition_data_location_table table;
    auto created = table.create_table( table_metadata );
    EXPECT_EQ( created, table_id );

    partition_lock_mode lock_mode = partition_lock_mode::no_lock;

    std::vector<std::vector<partition_column_identifier>>        pids;
    std::vector<std::vector<std::shared_ptr<partition_payload>>> payloads;
    uint64_t                                                     max_key = 26;

    cell_key ck;
    ck.table_id = table_id;

    cell_key_ranges ckr;
    ckr.table_id = table_id;

    partition_column_identifier_unordered_set all_pids;

    for( uint64_t base = 0; base < max_key; base++ ) {
        uint64_t lower =
            ( base / default_partition_size ) * default_partition_size;
        uint64_t upper = lower + ( default_partition_size - 1 );

        std::vector<partition_column_identifier>        loc_pids;
        std::vector<std::shared_ptr<partition_payload>> loc_payloads;

        ck.row_id = base;

        for( uint32_t col = col_start; col <= col_end; col++ ) {
            ck.col_id = col;

            uint32_t low_col =
                ( col / default_column_size ) * default_column_size;
            uint32_t upper_col =
                std::min( low_col + ( default_column_size - 1 ), col_end );
            auto pid = create_partition_column_identifier(
                table_id, lower, upper, low_col, upper_col );
            loc_payloads.push_back(
                std::make_shared<partition_payload>( pid ) );
            loc_pids.push_back( pid );

            all_pids.insert( pid );

            EXPECT_EQ( nullptr, table.get_partition( pid, lock_mode ) );
            EXPECT_EQ( nullptr, table.get_partition( ck, lock_mode ) );

            ckr.col_id_start = col;
            ckr.col_id_end = col;
            ckr.row_id_start = base;
            ckr.row_id_end = base;
            auto found_payloads = table.get_partition( ckr, lock_mode );
            EXPECT_TRUE( found_payloads.empty() );

            ckr.col_id_start = low_col;
            ckr.col_id_end = upper_col;
            ckr.row_id_start = lower;
            ckr.row_id_end = upper;
            found_payloads = table.get_partition( ckr, lock_mode );
            EXPECT_TRUE( found_payloads.empty() );

            ckr.col_id_start = col_start;
            ckr.col_id_end = col_end;
            ckr.row_id_start = 0;
            ckr.row_id_end = max_key - 1;
            found_payloads = table.get_partition( ckr, lock_mode );
            EXPECT_TRUE( found_payloads.empty() );
        }
        pids.emplace_back( loc_pids );
        payloads.emplace_back( loc_payloads );
    }
    // initial insert
    partition_column_identifier_unordered_set to_insert = {
        pids.at( 0 ).at( 0 ), pids.at( 0 ).at( 1 ),  pids.at( 4 ).at( 2 ),
        pids.at( 9 ).at( 0 ), pids.at( 16 ).at( 1 ), pids.at( 25 ).at( 2 ),
        pids.at( 25 ).at( 1 )};
    partition_column_identifier_unordered_set inserted = to_insert;
    //    DVLOG( 40 ) << "Initial insert:" << to_insert;
    insert_and_check( &table, pids, payloads, to_insert, inserted, lock_mode );

    auto pid_25_5 =
        create_partition_column_identifier( table_id, 255, 259, 0, 5 );
    EXPECT_EQ( nullptr, table.get_partition( pid_25_5, lock_mode ) );

    to_insert = {pids.at( 20 ).at( 0 ), pids.at( 13 ).at( 0 ),
                 pids.at( 15 ).at( 2 ), pids.at( 0 ).at( 4 ),
                 pids.at( 10 ).at( 4 ), pids.at( 20 ).at( 4 )};
    inserted.insert( to_insert.begin(), to_insert.end() );
    //    DVLOG( 40 ) << "Insert:" << to_insert;
    insert_and_check( &table, pids, payloads, to_insert, inserted, lock_mode );

    partition_column_identifier_unordered_set all_pids_copy = all_pids;
    for( const auto& pid : inserted ) {
        EXPECT_EQ( 1, all_pids_copy.count( pid ) );
        all_pids_copy.erase( pid );
    }

    EXPECT_TRUE( all_pids_copy.empty() );

    ckr.table_id = 0;
    ckr.col_id_start = col_start;
    ckr.col_id_end = col_end;
    ckr.row_id_start = 0;
    ckr.row_id_end = max_key - 1;
    check_contains( &table, ckr, all_pids, lock_mode );

    ckr.col_id_end = col_end + 1;
    partition_column_identifier_unordered_set found_pids;
    check_contains( &table, ckr, found_pids, lock_mode );

    ckr.col_id_end = col_end;
    ckr.row_id_end = 9;
    found_pids = {pids.at( 0 ).at( 0 ), pids.at( 0 ).at( 2 ),
                  pids.at( 0 ).at( 4 )};
    check_contains( &table, ckr, found_pids, lock_mode );

    ckr.col_id_end = 1;
    ckr.row_id_end = 2;
    found_pids = {pids.at( 0 ).at( 0 )};
    check_contains( &table, ckr, found_pids, lock_mode );

    partition_column_identifier_unordered_set to_delete = {
        pids.at( 9 ).at( 0 ), pids.at( 13 ).at( 1 )};
    for( const auto& d : to_delete ) {
        inserted.erase( d );
    }
    DVLOG( 40 ) << "Delete:" << to_delete;
    delete_and_check( &table, pids, payloads, to_delete, inserted, lock_mode );

    to_delete = {pids.at( 4 ).at( 2 ), pids.at( 1 ).at( 2 ),
                 pids.at( 10 ).at( 3 )};
    for( const auto& d : to_delete ) {
        inserted.erase( d );
    }
    DVLOG( 40 ) << "Delete:" << to_delete;
    delete_and_check( &table, pids, payloads, to_delete, inserted, lock_mode );

    EXPECT_EQ( nullptr, table.get_partition( pid_25_5, lock_mode ) );

    found_pids = {};
    check_contains( &table, ckr, found_pids, lock_mode );
}

TEST_F( multi_version_partition_table_test, split_merge_tree_operations ) {
    uint32_t table_id = 0;
    uint32_t default_partition_size = 10;
    uint32_t default_column_size = 2;
    uint32_t col_start = 0;
    uint32_t col_end = 2;
    partition_type::type p_type = partition_type::type::COLUMN;
    storage_tier_type::type s_type = storage_tier_type::type::DISK;

    std::vector<cell_data_type> col_types = {
        cell_data_type::INT64, cell_data_type::INT64, cell_data_type::INT64};

    auto table_metadata = create_table_metadata(
        "t", table_id, ( col_end - col_start ) + 1, col_types,
        5 /*num_records_in_chain*/, 5 /*num_records_in_snapshot_chain*/,
        0 /*site_location*/, default_partition_size, default_column_size,
        default_partition_size, default_column_size, p_type );
    multi_version_partition_data_location_table table;
    auto created = table.create_table( table_metadata );
    EXPECT_EQ( created, table_id );

    partition_lock_mode lock_mode = partition_lock_mode::no_lock;

    partition_column_identifier_map_t<std::shared_ptr<partition_payload>> payloads;
    auto p_10_19 = create_partition_column_identifier( table_id, 10, 19, 0, 1 );
    payloads[p_10_19] = std::make_shared<partition_payload>( p_10_19 );
    auto inserted =
        table.insert_partition( payloads[p_10_19], lock_mode );
    inserted->unlock( lock_mode );

    auto p_40_49 = create_partition_column_identifier( table_id, 40, 49, 0, 1 );
    payloads[p_40_49] = std::make_shared<partition_payload>( p_40_49 );
    inserted = table.insert_partition( payloads[p_40_49], lock_mode );
    inserted->unlock( lock_mode );

    auto p_80_89 = create_partition_column_identifier( table_id, 80, 89, 0, 1 );
    payloads[p_80_89] = std::make_shared<partition_payload>( p_80_89 );
    inserted = table.insert_partition( payloads[p_80_89], lock_mode );
    inserted->unlock( lock_mode );

    auto p_160_169 =
        create_partition_column_identifier( table_id, 160, 169, 0, 1 );
    payloads[p_160_169] = std::make_shared<partition_payload>( p_160_169 );
    inserted = table.insert_partition( payloads[p_160_169], lock_mode );
    inserted->unlock( lock_mode );

    auto p_70_79 = create_partition_column_identifier( table_id, 70, 79, 0, 1 );
    payloads[p_70_79] = std::make_shared<partition_payload>( p_70_79 );
    inserted = table.insert_partition( payloads[p_70_79], lock_mode );
    inserted->unlock( lock_mode );

    auto merged = table.merge_partition( payloads[p_70_79], payloads[p_80_89],
                                         p_type, s_type, 0, lock_mode );
    EXPECT_NE( nullptr, merged );
    auto p_70_89 = create_partition_column_identifier( table_id, 70, 89, 0, 1 );
    EXPECT_EQ( p_70_89, merged->identifier_ );
    payloads[p_70_89] = merged;
    merged->unlock( lock_mode );

    auto found_payload = table.get_partition( p_70_89, lock_mode );
    EXPECT_NE( nullptr, found_payload );
    EXPECT_EQ( p_70_89, found_payload->identifier_ );

    found_payload = table.get_partition( p_70_79, lock_mode );
    EXPECT_EQ( nullptr, found_payload );

    auto p_30_39 = create_partition_column_identifier( table_id, 30, 39, 0, 1 );
    payloads[p_30_39] = std::make_shared<partition_payload>( p_30_39 );
    inserted = table.insert_partition( payloads[p_30_39], lock_mode );
    inserted->unlock( lock_mode );

    merged = table.merge_partition( payloads[p_30_39], payloads[p_40_49],
                                    p_type, s_type, 0, lock_mode );
    EXPECT_NE( nullptr, merged );
    auto p_30_49 = create_partition_column_identifier( table_id, 30, 49, 0, 1 );
    EXPECT_EQ( p_30_49, merged->identifier_ );
    merged->unlock( lock_mode );
    payloads[p_30_49] = merged;

    found_payload = table.get_partition( p_30_49, lock_mode );
    EXPECT_NE( nullptr, found_payload );
    EXPECT_EQ( p_30_49, found_payload->identifier_ );

    auto p_50_59_low = create_partition_column_identifier( table_id, 50, 59, 0 , 0 );
    payloads[p_50_59_low] = std::make_shared<partition_payload>( p_50_59_low );
    inserted = table.insert_partition( payloads[p_50_59_low], lock_mode );
    inserted->unlock( lock_mode );

    auto p_50_59_high =
        create_partition_column_identifier( table_id, 50, 59, 1, 1 );
    payloads[p_50_59_high] =
        std::make_shared<partition_payload>( p_50_59_high );
    inserted = table.insert_partition( payloads[p_50_59_high], lock_mode );
    inserted->unlock( lock_mode );

    merged =
        table.merge_partition( payloads[p_50_59_low], payloads[p_50_59_high],
                               p_type, s_type, 2, lock_mode );
    auto p_50_59 = create_partition_column_identifier( table_id, 50, 59, 0, 1 );
    EXPECT_NE( nullptr, merged );
    EXPECT_EQ( p_50_59, merged->identifier_ );
    payloads[p_50_59] = merged;
    found_payload = table.get_partition( p_50_59, lock_mode );
    EXPECT_NE( nullptr, found_payload );
    EXPECT_EQ( p_50_59, found_payload->identifier_ );

    merged = table.merge_partition( payloads[p_30_49], payloads[p_50_59],
                                    p_type, s_type, 2, lock_mode );
    auto p_30_59 = create_partition_column_identifier( table_id, 30, 59, 0, 1 );
    EXPECT_NE( nullptr, merged );
    EXPECT_EQ( p_30_59, merged->identifier_ );
    payloads[p_30_59] = merged;
    found_payload = table.get_partition( p_30_59, lock_mode );
    EXPECT_NE( nullptr, found_payload );
    EXPECT_EQ( p_30_59, found_payload->identifier_ );

    auto split_partitions =
        table.split_partition( payloads[p_70_89], 80, k_unassigned_col, p_type,
                               p_type, s_type, s_type, 0, 1, lock_mode );
    EXPECT_EQ( 2, split_partitions.size() );
    EXPECT_NE( nullptr, split_partitions.at( 0 ) );
    EXPECT_EQ( p_70_79, split_partitions.at( 0 )->identifier_ );
    EXPECT_NE( nullptr, split_partitions.at( 1 ) );
    EXPECT_EQ( p_80_89, split_partitions.at( 1 )->identifier_ );
    payloads[p_70_79] = split_partitions.at( 0 );
    payloads[p_80_89] = split_partitions.at( 1 );
    unlock_payloads( split_partitions, lock_mode );

    found_payload = table.get_partition( p_70_89, lock_mode );
    EXPECT_EQ( nullptr, found_payload );

    found_payload = table.get_partition( p_70_79, lock_mode );
    EXPECT_NE( nullptr, found_payload );
    EXPECT_EQ( p_70_79, found_payload->identifier_ );

    found_payload = table.get_partition( p_80_89, lock_mode );
    EXPECT_NE( nullptr, found_payload );
    EXPECT_EQ( p_80_89, found_payload->identifier_ );

    split_partitions =
        table.split_partition( payloads[p_80_89], k_unassigned_key, 1, p_type,
                               p_type, s_type, s_type, 1, 2, lock_mode );
    EXPECT_EQ( 2, split_partitions.size() );
    EXPECT_NE( nullptr, split_partitions.at( 0 ) );
    auto p_80_89_low =
        create_partition_column_identifier( table_id, 80, 89, 0, 0 );
    EXPECT_EQ( p_80_89_low, split_partitions.at( 0 )->identifier_ );
    EXPECT_NE( nullptr, split_partitions.at( 1 ) );
    auto p_80_89_high =
        create_partition_column_identifier( table_id, 80, 89, 1, 1 );
    EXPECT_EQ( p_80_89_high, split_partitions.at( 1 )->identifier_ );
    payloads[p_80_89_low] = split_partitions.at( 0 );
    payloads[p_80_89_high] = split_partitions.at( 1 );
    unlock_payloads( split_partitions, lock_mode );

    found_payload = table.get_partition( p_80_89, lock_mode );
    EXPECT_EQ( nullptr, found_payload );

    found_payload = table.get_partition( p_80_89_low, lock_mode );
    EXPECT_NE( nullptr, found_payload );
    EXPECT_EQ( p_80_89_low, found_payload->identifier_ );

    found_payload = table.get_partition( p_80_89_high, lock_mode );
    EXPECT_NE( nullptr, found_payload );
    EXPECT_EQ( p_80_89_high, found_payload->identifier_ );

    split_partitions =
        table.split_partition( payloads[p_30_59], 40, k_unassigned_col, p_type,
                               p_type, s_type, s_type, 0, 2, lock_mode );
    EXPECT_EQ( 2, split_partitions.size() );
    EXPECT_NE( nullptr, split_partitions.at( 0 ) );
    EXPECT_EQ( p_30_39, split_partitions.at( 0 )->identifier_ );
    EXPECT_NE( nullptr, split_partitions.at( 1 ) );
    auto p_40_59 = create_partition_column_identifier( table_id, 40, 59, 0, 1 );
    EXPECT_EQ( p_40_59, split_partitions.at( 1 )->identifier_ );
    payloads[p_30_39] = split_partitions.at( 0 );
    payloads[p_40_59] = split_partitions.at( 1 );
    unlock_payloads( split_partitions, lock_mode );

    found_payload = table.get_partition( p_30_59, lock_mode );
    EXPECT_EQ( nullptr, found_payload );

    found_payload = table.get_partition( p_30_39, lock_mode );
    EXPECT_NE( nullptr, found_payload );
    EXPECT_EQ( p_30_39, found_payload->identifier_ );

    found_payload = table.get_partition( p_40_59, lock_mode );
    EXPECT_NE( nullptr, found_payload );
    EXPECT_EQ( p_40_59, found_payload->identifier_ );
}

TEST_F( multi_version_partition_table_test, complex_tree_operations ) {
    uint32_t table_id = 0;
    uint32_t default_partition_size = 10;
    uint32_t default_column_size = 2;
    uint32_t col_start = 0;
    uint32_t col_end = 2;

    std::vector<cell_data_type> col_types = {
        cell_data_type::INT64, cell_data_type::INT64, cell_data_type::INT64};

    auto table_metadata = create_table_metadata(
        "t", table_id, ( col_end - col_start ) + 1, col_types,
        5 /*num_records_in_chain*/, 5 /*num_records_in_snapshot_chain*/,
        0 /*site_location*/, default_partition_size, default_column_size,
        default_partition_size, default_column_size,
        partition_type::type::COLUMN );

    multi_version_partition_data_location_table table;
    auto created = table.create_table( table_metadata );
    EXPECT_EQ( created, table_id );

    partition_lock_mode lock_mode = partition_lock_mode::no_lock;

    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
         payloads;
    auto p_10_15 = create_partition_column_identifier( table_id, 10, 15, 0, 1 );
    payloads[p_10_15] = std::make_shared<partition_payload>( p_10_15 );
    auto inserted = table.insert_partition( payloads[p_10_15], lock_mode );
    inserted->unlock( lock_mode );

    auto p_30_59 = create_partition_column_identifier( table_id, 30, 59, 0, 1 );
    payloads[p_30_59] = std::make_shared<partition_payload>( p_30_59 );
    inserted = table.insert_partition( payloads[p_30_59], lock_mode );
    inserted->unlock( lock_mode );

    auto p_80_89 = create_partition_column_identifier( table_id, 80, 89, 0, 1 );
    payloads[p_80_89] = std::make_shared<partition_payload>( p_80_89 );
    inserted = table.insert_partition( payloads[p_80_89], lock_mode );
    inserted->unlock( lock_mode );

    auto p_160_169 =
        create_partition_column_identifier( table_id, 160, 169, 0, 1 );
    payloads[p_160_169] = std::make_shared<partition_payload>( p_160_169 );
    inserted = table.insert_partition( payloads[p_160_169], lock_mode );
    inserted->unlock( lock_mode );

    cell_key ck;
    ck.table_id = table_id;
    ck.row_id = 10;
    ck.col_id = 0;

    auto found_payload = table.get_partition( ck, lock_mode );
    EXPECT_NE( nullptr, found_payload );
    EXPECT_EQ( p_10_15, found_payload->identifier_ );

    ck.row_id = 14;
    found_payload = table.get_partition( ck, lock_mode );
    EXPECT_NE( nullptr, found_payload );
    EXPECT_EQ( p_10_15, found_payload->identifier_ );

    ck.row_id = 91;
    found_payload = table.get_partition( ck, lock_mode );
    EXPECT_EQ( nullptr, found_payload );

    cell_key ck_31 = ck;
    cell_key ck_34 = ck;
    cell_key ck_41 = ck;
    cell_key ck_52 = ck;
    cell_key ck_61 = ck;
    ck_31.row_id = 31;
    ck_34.row_id = 34;
    ck_41.row_id = 41;
    ck_52.row_id = 52;
    ck_61.row_id = 61;

    cell_key_ranges ckr;
    ckr.table_id = ck.table_id;
    ckr.col_id_start = 0;
    ckr.col_id_end = 1;
    ckr.row_id_start = 31;
    ckr.row_id_end = 34;

    std::vector<cell_key_ranges> ckrs;
    ckrs.push_back( ckr );

    ckr.row_id_start = 41;
    ckr.row_id_end = 52;
    ckrs.push_back( ckr );

    p_30_59 = create_partition_column_identifier( table_id, 30, 59, 0, 1 );
    auto p_30_59_payload =
        table.get_or_create_partition( ck_31, p_30_59, lock_mode );
    EXPECT_NE( nullptr, p_30_59_payload );
    EXPECT_EQ( p_30_59, p_30_59_payload->identifier_ );
    p_30_59_payload->unlock( lock_mode );
    payloads[p_30_59] = p_30_59_payload;

    auto found_payloads = table.get_partitions( ckrs, lock_mode );
    GASSERT_EQ( 1, found_payloads.size() );
    EXPECT_NE( nullptr, found_payloads.at( 0 ) );
    EXPECT_EQ( p_30_59, found_payloads.at( 0 )->identifier_ );
    unlock_payloads( found_payloads, lock_mode );

    auto p_55_64 = create_partition_column_identifier( table_id, 55, 64, 0, 1 );
    auto p_60_64 = create_partition_column_identifier( table_id, 60, 64, 0, 1 );

    auto p_60_64_payload =
        table.get_or_create_partition( ck_61, p_55_64, lock_mode );
    EXPECT_NE( nullptr, p_60_64_payload );
    EXPECT_EQ( p_60_64, p_60_64_payload->identifier_ );
    p_60_64_payload->unlock( lock_mode );
    payloads[p_60_64] = p_60_64_payload;

    ckr.row_id_start = 50;
    ckr.row_id_end = 61;
    ckrs.push_back( ckr );

    found_payloads = table.get_partitions( ckrs, lock_mode );
    GASSERT_EQ( 2, found_payloads.size() );
    EXPECT_NE( nullptr, found_payloads.at( 0 ) );
    EXPECT_EQ( p_30_59, found_payloads.at( 0 )->identifier_ );
    EXPECT_NE( nullptr, found_payloads.at( 1 ) );
    EXPECT_EQ( p_60_64, found_payloads.at( 1 )->identifier_ );
    unlock_payloads( found_payloads, lock_mode );

    ckr.row_id_start = 45;
    ckr.row_id_end = 45;
    found_payloads = table.get_partitions( {ckr}, lock_mode );
    EXPECT_EQ( 1, found_payloads.size() );
    EXPECT_NE( nullptr, found_payloads.at( 0 ) );
    EXPECT_EQ( p_30_59, found_payloads.at( 0 )->identifier_ );
    unlock_payloads( found_payloads, lock_mode );

    ck.row_id = 23;
    auto p_20_25 = create_partition_column_identifier( table_id, 20, 25, 0, 1 );
    found_payload = table.get_or_create_partition( ck, p_20_25, lock_mode );
    EXPECT_NE( nullptr, found_payload );
    EXPECT_EQ( p_20_25, found_payload->identifier_ );
    found_payload->unlock( lock_mode );

    ckr.row_id_start = 26;
    ckr.row_id_end = 26;

    found_payloads = table.get_partition( ckr, lock_mode );
    EXPECT_EQ( 0, found_payloads.size() );

    found_payloads = table.get_or_create_partition( ckr, lock_mode );
    EXPECT_EQ( 1, found_payloads.size() );
    EXPECT_NE( nullptr, found_payloads.at( 0 ) );
    auto p_26_29 = create_partition_column_identifier( table_id, 26, 29, 0, 1 );
    auto p_26_29_payload = found_payloads.at( 0 );
    EXPECT_EQ( p_26_29, p_26_29_payload->identifier_ );

    ckr.row_id_start = 4;
    ckr.row_id_end = 11;
    ckrs.clear();
    ckrs.emplace_back( ckr );

    ckr.row_id_start = 11;
    ckr.row_id_end = 17;
    ckrs.emplace_back( ckr );

    ckr.row_id_start = 19;
    ckr.row_id_end = 22;
    ckrs.emplace_back( ckr );

    found_payloads = table.get_or_create_partitions( ckrs, lock_mode );

    EXPECT_EQ( 4, found_payloads.size() );
    EXPECT_NE( nullptr, found_payloads.at( 0 ) );
    auto p_0_9 = create_partition_column_identifier( table_id, 0, 9, 0, 1 );
    EXPECT_EQ( p_0_9, found_payloads.at( 0 )->identifier_ );
    EXPECT_NE( nullptr, found_payloads.at( 1 ) );
    EXPECT_EQ( p_10_15, found_payloads.at( 1 )->identifier_ );
    EXPECT_NE( nullptr, found_payloads.at( 2 ) );
    auto p_16_19 = create_partition_column_identifier( table_id, 16, 19, 0, 1 );
    EXPECT_EQ( p_16_19, found_payloads.at( 2 )->identifier_ );
    EXPECT_NE( nullptr, found_payloads.at( 3 ) );
    EXPECT_EQ( p_20_25, found_payloads.at( 3 )->identifier_ );

    // remove a bunch of these new nodes
    // p_0_9
    table.remove_partition( found_payloads.at( 0 ) );
    found_payloads.at( 0 )->unlock( lock_mode );
    // p_16_19
    table.remove_partition( found_payloads.at( 2 ) );
    found_payloads.at( 2 )->unlock( lock_mode );
    // p_20_25
    table.remove_partition( found_payloads.at( 3 ) );
    found_payloads.at( 3 )->unlock( lock_mode );
    // p_26_29
    table.remove_partition( p_26_29_payload );
    p_26_29_payload->unlock( lock_mode );

    DVLOG( 40 ) << "Deleted a bunch of nodes";

    DVLOG( 40 ) << "Look up 59";
    ck.row_id = 59;
    found_payload = table.get_partition( ck, lock_mode );
    EXPECT_NE( nullptr, found_payload );
    EXPECT_EQ( p_30_59, found_payload->identifier_ );
    DVLOG( 40 ) << "Done look up 59";

    ckrs.clear();
    ckr.row_id_start = 26;
    ckr.row_id_end = 34;
    ckrs.push_back( ckr );

    ckr.row_id_start = 67;
    ckr.row_id_end = 67;
    ckrs.push_back( ckr );

    lock_mode = partition_lock_mode::lock;

    found_payloads = table.get_or_create_partitions( ckrs, lock_mode );

    EXPECT_EQ( 3, found_payloads.size() );
    EXPECT_NE( nullptr, found_payloads.at( 0 ) );
    auto p_20_29 = create_partition_column_identifier( table_id, 20, 29, 0, 1 );
    EXPECT_EQ( p_20_29, found_payloads.at( 0 )->identifier_ );
    EXPECT_NE( nullptr, found_payloads.at( 1 ) );
    EXPECT_EQ( p_30_59, found_payloads.at( 1 )->identifier_ );
    EXPECT_NE( nullptr, found_payloads.at( 2 ) );
    auto p_65_69 = create_partition_column_identifier( table_id, 65, 69, 0, 1 );
    EXPECT_EQ( p_65_69, found_payloads.at( 2 )->identifier_ );

    found_payloads.at( 0 )->unlock( lock_mode );

    // it isn't unlocked, so should fail
    lock_mode = partition_lock_mode::try_lock;
    auto unlocked_found_payloads = table.get_partitions( ckrs, lock_mode );
    EXPECT_EQ( 0, unlocked_found_payloads.size() );

    found_payloads.at( 1 )->unlock( lock_mode );
    found_payloads.at( 2 )->unlock( lock_mode );
}

TEST_F( multi_version_partition_table_test, grouped_info ) {
    uint32_t table_id = 0;
    uint32_t default_partition_size = 10;
    uint32_t default_column_size = 2;
    uint32_t col_start = 0;
    uint32_t col_end = 2;

    std::vector<cell_data_type> col_types = {
        cell_data_type::INT64, cell_data_type::INT64, cell_data_type::INT64};

    auto table_metadata = create_table_metadata(
        "t", table_id, ( col_end - col_start ) + 1, col_types,
        5 /*num_records_in_chain*/, 5 /*num_records_in_snapshot_chain*/,
        0 /*site_location*/, default_partition_size, default_column_size,
        default_partition_size, default_column_size,
        partition_type::type::COLUMN );

    multi_version_partition_data_location_table table;
    auto created = table.create_table( table_metadata );
    EXPECT_EQ( created, table_id );

    partition_lock_mode lock_mode = partition_lock_mode::no_lock;

    // add some stuff here
    // pk_5 is at 1, pk_15, is at 0, pk_25 is at 0 and 1, pk_35 doesn't exist

    auto p_0_9 = create_partition_column_identifier( table_id, 0, 9, 0, 1 );
    auto p_0_9_payload = std::make_shared<partition_payload>( p_0_9 );
    auto p_0_9_location = std::make_shared<partition_location_information>();
    p_0_9_location->master_location_ = 1;
    p_0_9_location->partition_types_[1] = partition_type::type::SORTED_COLUMN;
    p_0_9_location->version_ = 1;
    bool swapped = p_0_9_payload->compare_and_swap_location_information(
        p_0_9_location, nullptr );
    EXPECT_TRUE( swapped );

    auto inserted = table.insert_partition( p_0_9_payload, lock_mode );
    inserted->unlock( lock_mode );

    auto p_10_19 = create_partition_column_identifier( table_id, 10, 19, 0, 1 );
    auto p_10_19_payload = std::make_shared<partition_payload>( p_10_19 );
    auto p_10_19_location = std::make_shared<partition_location_information>();
    p_10_19_location->master_location_ = 0;
    p_10_19_location->partition_types_[0] = partition_type::type::ROW;
    p_10_19_location->version_ = 1;
    swapped = p_10_19_payload->compare_and_swap_location_information(
        p_10_19_location, nullptr );
    EXPECT_TRUE( swapped );

    inserted = table.insert_partition( p_10_19_payload, lock_mode );
    inserted->unlock( lock_mode );

    auto p_20_29 = create_partition_column_identifier( table_id, 20, 29,0, 1 );
    auto p_20_29_payload = std::make_shared<partition_payload>( p_20_29 );
    auto p_20_29_location = std::make_shared<partition_location_information>();
    p_20_29_location->master_location_ = 1;
    p_20_29_location->partition_types_[1] = partition_type::type::COLUMN;
    p_20_29_location->replica_locations_.emplace( 0 );
    p_20_29_location->partition_types_.emplace( 0, partition_type::type::ROW );
    p_20_29_location->version_ = 1;
    swapped = p_20_29_payload->compare_and_swap_location_information(
        p_20_29_location, nullptr );
    EXPECT_TRUE( swapped );

    inserted = table.insert_partition( p_20_29_payload, lock_mode );
    inserted->unlock( lock_mode );

    std::vector<cell_key_ranges> write_ckrs;
    std::vector<cell_key_ranges> read_ckrs;

    cell_key_ranges ckr;
    ckr.table_id = table_id;
    ckr.col_id_start = col_start;
    ckr.col_id_end = col_start + 1;

    ckr.row_id_start  = 5;
    ckr.row_id_end  = 5;

    cell_key_ranges ckr_5_5 = ckr;
    write_ckrs.emplace_back( ckr_5_5 );

    ckr.row_id_start = 10;
    ckr.row_id_end = 20;
    cell_key_ranges ckr_10_20 = ckr;
    write_ckrs.emplace_back( ckr_10_20 );

    ckr.row_id_start = 20;
    ckr.row_id_end = 30;
    cell_key_ranges ckr_20_30 = ckr;
    read_ckrs.emplace_back( ckr_20_30 );

    ckr.row_id_start = 35;
    ckr.row_id_end = 37;
    cell_key_ranges ckr_35_37 = ckr;
    read_ckrs.emplace_back( ckr_35_37 );

    grouped_partition_information group_info =
        table.get_or_create_partitions_and_group( write_ckrs, read_ckrs,
                                                  lock_mode, 3 /*num sites*/ );

    auto p_30_39 = create_partition_column_identifier( table_id, 30, 39, 0, 1 );
    std::vector<partition_column_identifier> expected_pids = {p_0_9, p_10_19, p_20_29,
                                                       p_30_39};
    check_partitions_match( expected_pids, group_info.payloads_ );

    expected_pids = {p_30_39};
    check_partitions_match( expected_pids, group_info.new_partitions_ );

    expected_pids = {p_0_9, p_10_19, p_20_29};
    check_partitions_match( expected_pids,
                            group_info.existing_write_partitions_ );

    expected_pids = { p_20_29};
    check_partitions_match( expected_pids,
                            group_info.existing_read_partitions_ );

    expected_pids = { };
    check_partitions_match( expected_pids, group_info.new_write_partitions_ );

    expected_pids = {p_30_39};
    check_partitions_match( expected_pids, group_info.new_read_partitions_ );

    // sizes are okay, to lazy to check all of them
    EXPECT_EQ( 3, group_info.write_pids_to_shaped_ckrs_.size() );
    EXPECT_EQ( 2, group_info.read_pids_to_shaped_ckrs_.size() );
    EXPECT_EQ( 4, group_info.partition_location_informations_.size() );

    // things are not mastered at the same sites
    auto no_change_destinations = group_info.get_no_change_destinations();
    EXPECT_TRUE( no_change_destinations.empty() );
    unlock_payloads( group_info.payloads_, lock_mode );

    cell_key_ranges ckr_10_19 = ckr_10_20;
    ckr_10_19.row_id_end = 19;

    cell_key_ranges ckr_20_29 = ckr_20_30;
    ckr_20_29.row_id_end = 29;


    write_ckrs.clear();
    read_ckrs.clear();

    write_ckrs.push_back( ckr_10_19 );
    read_ckrs.push_back( ckr_20_29 );

    // now this should give one option
    group_info = table.get_or_create_partitions_and_group(
        write_ckrs, read_ckrs, lock_mode, 3 /*num sites*/ );
    // things are not mastered at the same sites, but there is a replica
    no_change_destinations = group_info.get_no_change_destinations();
    EXPECT_EQ( 1, no_change_destinations.size() );
    EXPECT_EQ( 0, no_change_destinations.at( 0 ) );

    unlock_payloads( group_info.payloads_, lock_mode );

    write_ckrs.clear();
    read_ckrs.clear();

    write_ckrs.push_back( ckr_10_19 );
    read_ckrs.push_back( ckr_10_19 );
    group_info = table.get_partitions_and_group(
        write_ckrs, read_ckrs, lock_mode, 3 /* num sites*/,
        false /* don't allow missing*/ );

    EXPECT_EQ( 1, group_info.payloads_.size() );
    EXPECT_EQ( p_10_19, group_info.payloads_.at( 0 )->identifier_ );

    EXPECT_EQ( 1, group_info.partitions_set_.size() );
    EXPECT_EQ( 1, group_info.partitions_set_.count( p_10_19 ) );

    EXPECT_EQ( 1, group_info.write_pids_to_shaped_ckrs_.size() );
    EXPECT_EQ( 1, group_info.write_pids_to_shaped_ckrs_.count( p_10_19 ) );
    EXPECT_EQ( 1, group_info.write_pids_to_shaped_ckrs_.at( p_10_19 ).size() );
    EXPECT_EQ( ckr_10_19,
               group_info.write_pids_to_shaped_ckrs_.at( p_10_19 ).at( 0 ) );

    EXPECT_EQ( 1, group_info.read_pids_to_shaped_ckrs_.size() );
    EXPECT_EQ( 1, group_info.read_pids_to_shaped_ckrs_.count( p_10_19 ) );
    EXPECT_EQ( 1, group_info.read_pids_to_shaped_ckrs_.at( p_10_19 ).size() );
    EXPECT_EQ( ckr_10_19,
               group_info.read_pids_to_shaped_ckrs_.at( p_10_19 ).at( 0 ) );

    EXPECT_EQ( 1, group_info.partition_location_informations_.size() );
    EXPECT_EQ( 1,
               group_info.partition_location_informations_.count( p_10_19 ) );

    EXPECT_EQ( 0, group_info.new_partitions_.size() );
    EXPECT_EQ( 1, group_info.existing_write_partitions_.size() );
    EXPECT_EQ( p_10_19,
               group_info.existing_write_partitions_.at( 0 )->identifier_ );
    EXPECT_EQ( 1, group_info.existing_read_partitions_.size() );
    EXPECT_EQ( p_10_19,
               group_info.existing_write_partitions_.at( 0 )->identifier_ );

    EXPECT_EQ( 0, group_info.new_write_partitions_.size() );
    EXPECT_EQ( 0, group_info.new_read_partitions_.size() );

    EXPECT_EQ( 0, group_info.new_partitions_set_.size() );
    EXPECT_EQ( 1, group_info.existing_write_partitions_set_.size() );
    EXPECT_EQ( 1, group_info.existing_write_partitions_set_.count( p_10_19 ) );
    EXPECT_EQ( 1, group_info.existing_read_partitions_set_.size() );
    EXPECT_EQ( 1, group_info.existing_read_partitions_set_.count( p_10_19 ) );

    EXPECT_EQ( 0, group_info.new_write_partitions_set_.size() );
    EXPECT_EQ( 0, group_info.new_read_partitions_set_.size() );
}
