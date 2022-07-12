#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/data-site/db/table.h"
#include "../src/data-site/db/tables.h"

class table_test : public ::testing::Test {};

#define check_polled_versions( _actual, _expected )        \
    EXPECT_EQ( _actual.size(), _expected.size() );         \
    for( const auto& act : _actual ) {                     \
        const auto& pid = act.pcid;                        \
        int64_t     act_version = act.version;             \
        auto        search = _expected.find( pid );        \
        EXPECT_NE( search, _expected.end() );              \
        if( search != _expected.end() ) {                  \
            EXPECT_EQ( search->second, act_version );      \
        } else {                                           \
            DLOG( WARNING ) << "PID version:" << pid       \
                            << ", version:" << act_version \
                            << ", not found in expected";  \
        }                                                  \
    }

bool check_data_present( tables*                                db_tables,
                         const partition_column_identifier_set& read_pids,
                         const snapshot_vector& snapshot, uint32_t table_id,
                         uint64_t key_start, uint64_t key_end,
                         uint32_t col_start, uint32_t col_end ) {

    DVLOG( 10 ) << "Check data present:" << read_pids;

    bool ret = true;

    partition_column_identifier_set write_pids = {};

    transaction_partition_holder* t_holder =
        db_tables->get_partitions( write_pids, read_pids );
    // these partitions t exist
    EXPECT_NE( t_holder, nullptr );
    ret = ret and ( t_holder != nullptr );
    if( t_holder == nullptr ) {
        LOG( WARNING ) << "Unable to check data present, as transaction holder "
                          "is null, read_pids:"
                       << read_pids;
        return ret;
    }

    t_holder->begin_transaction( snapshot );

    cell_identifier cid;
    cid.table_id_ = table_id;
    cid.col_id_ = 0;
    cid.key_ = 0;

    for( uint64_t key = key_start; key <= key_end; key++ ) {
        for( uint32_t col = col_start; col <= col_end; col++ ) {
            cid.col_id_ = col;
            cid.key_ = key;
            int64_t expected_val = ( col + 1 ) * key;
            auto    read_ok = t_holder->get_int64_data( cid );
            EXPECT_TRUE( std::get<0>( read_ok ) );

            if( std::get<0>( read_ok ) ) {
                EXPECT_EQ( std::get<1>( read_ok ), expected_val );
                ret = ret and ( std::get<1>( read_ok ) == expected_val );
                if ( std::get<1>( read_ok ) !=  expected_val) {
                    DVLOG( 5 ) << "CID:" << cid
                               << ", do not match, expected:" << expected_val
                               << ", got:" << std::get<1>( read_ok );
                }
            } else {
                ret = false;
            }
        }
    }

    t_holder->commit_transaction();

    return ret;
}

void table_creation_test( const partition_type::type& default_type,
                          const partition_type::type& new_type,
                          bool                        check_data_records ) {
    std::string table_name = "ycsb";
    uint32_t num_records_in_chain = 5;
    uint32_t num_records_in_snapshot_chain = 5;
    uint32_t table_id = 0;
    uint64_t p1_start = 5;
    uint64_t p1_end = 10;
    uint32_t p1_col_start = 0;
    uint32_t p1_col_end = 1;

    uint64_t p2_start = 11;
    uint64_t p2_end = 20;
    uint32_t p2_col_start = p1_col_start;
    uint32_t p2_col_end = p1_col_end;

    uint64_t p2_row_split = 15;
    uint32_t p2_col_split = 1;

    uint32_t site_location = 0;

    auto no_op_generator = make_no_op_update_destination_generator();
    auto no_op_dest = no_op_generator->get_update_destination_by_position( 0 );

    partition_metadata p1_metadata = create_partition_metadata(
        table_id, p1_start, p1_end, p1_col_start, p1_col_end,
        num_records_in_chain, num_records_in_snapshot_chain, site_location );
    partition_metadata p2_metadata = create_partition_metadata(
        table_id, p2_start, p2_end, p2_col_start, p2_col_end,
        num_records_in_snapshot_chain, site_location );

    partition_metadata p2_row_low_metadata = create_partition_metadata(
        table_id, p2_start, p2_row_split - 1, p2_col_start, p2_col_end,
        num_records_in_chain, num_records_in_snapshot_chain, site_location );
    partition_metadata p2_row_high_metadata = create_partition_metadata(
        table_id, p2_row_split, p2_end, p2_col_start, p2_col_end,
        num_records_in_chain, num_records_in_snapshot_chain, site_location );

    partition_metadata p2_low_metadata = create_partition_metadata(
        table_id, p2_start, p2_row_split - 1, p2_col_start, p2_col_split - 1,
        num_records_in_chain, num_records_in_snapshot_chain, site_location );
    partition_metadata p2_high_metadata = create_partition_metadata(
        table_id, p2_start, p2_row_split - 1, p2_col_split, p2_col_end,
        num_records_in_chain, num_records_in_snapshot_chain, site_location );

    partition_metadata p1_low_metadata = create_partition_metadata(
        table_id, p1_start, p1_end, p1_col_start, p2_col_split - 1,
        num_records_in_chain, num_records_in_snapshot_chain, site_location );
    partition_metadata p1_high_metadata = create_partition_metadata(
        table_id, p1_start, p1_end, p2_col_split, p2_col_end,
        num_records_in_chain, num_records_in_snapshot_chain, site_location );

    partition_metadata p1_p2_low_metadata = create_partition_metadata(
        table_id, p1_start, p2_row_split - 1, p2_col_start, p2_col_split - 1,
        num_records_in_chain, num_records_in_snapshot_chain, site_location );

    partition_metadata p1_p2_high_metadata = create_partition_metadata(
        table_id, p1_start, p2_row_split - 1, p2_col_split, p2_col_end,
        num_records_in_chain, num_records_in_snapshot_chain, site_location );

    partition_metadata p1_p2_metadata = create_partition_metadata(
        table_id, p1_start, p2_row_split - 1, p2_col_start, p2_col_end,
        num_records_in_chain, num_records_in_snapshot_chain, site_location );

    std::vector<cell_data_type> col_types = {cell_data_type::INT64,
                                             cell_data_type::INT64};

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;

    table_metadata t_metadata = create_table_metadata(
        table_name, table_id, p1_col_end + 1, col_types, num_records_in_chain,
        num_records_in_snapshot_chain, site_location,
        10 /*default partition size*/, p1_col_end + 1,
        10 /*default tracking partition size*/, p1_col_end + 1, default_type,
        s_type, false /* enable sec storage */, "/tmp/" );

    tables_metadata ts_metadata = create_tables_metadata(
        1, site_location, 10 /*num clients */, 10 /*gc sleep time*/,
        false /* enable sec storage */, "/tmp" );

    tables db_tables;
    db_tables.init( ts_metadata, no_op_generator, make_update_enqueuers() );

    uint32_t found_tid = db_tables.create_table( t_metadata );
    EXPECT_EQ( found_tid, t_metadata.table_id_ );

    snapshot_vector snapshot;
    snapshot_vector expected_snapshot;

    partition_column_identifier_map_t<int64_t> polled_versions_expected;
    std::vector<polled_partition_column_version_information>
        polled_versions_actual;

    // check parts don't exist
    {
        partition_column_identifier_set write_pids = {
            p1_metadata.partition_id_};
        partition_column_identifier_set read_pids = {p2_metadata.partition_id_};

        transaction_partition_holder* t_holder =
            db_tables.get_partitions( write_pids, read_pids );
        // these partitions don't exist
        EXPECT_EQ( t_holder, nullptr );
    }
    // create parts (p1, p2)
    {
        // implictly locks
        std::shared_ptr<partition> p1 = db_tables.add_partition(
            p1_metadata.partition_id_, new_type, s_type );
        EXPECT_NE( nullptr, p1 );
        std::shared_ptr<partition> p2 = db_tables.add_partition(
            p2_metadata.partition_id_, new_type, s_type );
        EXPECT_NE( nullptr, p2 );
        // unlock these bad boys
        p1->unlock_partition();
        p2->unlock_partition();

        std::shared_ptr<partition> p1_copy = db_tables.add_partition(
            p1_metadata.partition_id_, new_type, s_type );
        EXPECT_NE( nullptr, p1_copy );

        EXPECT_EQ( p1.get(), p1_copy.get() );

        p1_copy->unlock_partition();

        polled_versions_expected = {
            {p1_metadata.partition_id_, 0}, {p2_metadata.partition_id_, 0},
        };

        db_tables.get_update_partition_states( polled_versions_actual );
        check_polled_versions( polled_versions_actual,
                               polled_versions_expected );
        polled_versions_actual.clear();

    }

    // insert records
    if( check_data_records ) {
        DVLOG( 10 ) << "Insert data";

        // do insert
        partition_column_identifier_set write_pids = {
            p1_metadata.partition_id_, p2_metadata.partition_id_};
        partition_column_identifier_set read_pids = {};

        DVLOG(1) << "Get partitions";
        transaction_partition_holder* t_holder =
            db_tables.get_partitions( write_pids, read_pids );
        // these partitions t exist
        EXPECT_NE( t_holder, nullptr );
        DVLOG( 1 ) << "Begin transaction:" << expected_snapshot;
        t_holder->begin_transaction( expected_snapshot );

        cell_identifier cid;
        cid.table_id_ = table_id;
        cid.col_id_ = 0;
        cid.key_ = 0;

        for( uint64_t key = p1_start; key <= p2_end; key++ ) {
            for( uint32_t col = p1_col_start; col <= p1_col_end; col++ ) {
                cid.col_id_ = col;
                cid.key_ = key;
                bool insert_ok =
                    t_holder->insert_int64_data( cid, ( col + 1 ) * key );
                EXPECT_TRUE( insert_ok );
            }
        }

        snapshot = t_holder->commit_transaction();

        expected_snapshot[p1_metadata.partition_id_hash_] =
            (int) check_data_records;
        expected_snapshot[p2_metadata.partition_id_hash_] =
            (int) check_data_records;
        EXPECT_EQ( expected_snapshot, snapshot );

        polled_versions_expected = {
            {p1_metadata.partition_id_, (int) check_data_records},
            {p2_metadata.partition_id_, (int) check_data_records},
        };

        db_tables.get_update_partition_states( polled_versions_actual );
        check_polled_versions( polled_versions_actual,
                               polled_versions_expected );
        polled_versions_actual.clear();
    }

    // split p2 horizontally (creates p2_row_low, p2_row_high)
    {
        partition_column_identifier_set write_pids = {
            p1_metadata.partition_id_};
        partition_column_identifier_set read_pids = {p2_metadata.partition_id_};

        transaction_partition_holder* t_holder =
            db_tables.get_partitions( write_pids, read_pids );

        std::shared_ptr<partition> p1 =
            t_holder->get_partition( p1_metadata.partition_id_ );
        std::shared_ptr<partition> p2 =
            t_holder->get_partition( p2_metadata.partition_id_ );
        split_partition_result split_result = db_tables.split_partition(
            snapshot, p2_metadata.partition_id_, p2_row_split, k_unassigned_col,
            new_type, new_type, s_type, s_type, no_op_dest, no_op_dest );
        EXPECT_EQ( p2.get(), split_result.partition_cover_.get() );
        snapshot = split_result.snapshot_;

        auto low = split_result.partition_low_;
        auto high = split_result.partition_high_;
        auto ori = split_result.partition_cover_;

        EXPECT_EQ( p2_metadata.partition_id_,
                   ori->get_metadata().partition_id_ );
        EXPECT_EQ( p2_row_low_metadata.partition_id_,
                   low->get_metadata().partition_id_ );
        EXPECT_EQ( p2_row_high_metadata.partition_id_,
                   high->get_metadata().partition_id_ );

        expected_snapshot[low->get_metadata().partition_id_hash_] =
            1 + (int) check_data_records;
        expected_snapshot[high->get_metadata().partition_id_hash_] =
            1 + (int) check_data_records;
        expected_snapshot[ori->get_metadata().partition_id_hash_] =
            1 + (int) check_data_records;
        EXPECT_EQ( expected_snapshot, snapshot );

        transaction_partition_holder* t2_holder =
            db_tables.get_partitions( {}, read_pids );
        // p2 doesn't exist any more
        EXPECT_EQ( nullptr, t2_holder );
        delete t_holder;

        polled_versions_expected = {
            {p2_row_high_metadata.partition_id_, 1 + (int) check_data_records},
            {p2_row_low_metadata.partition_id_, 1 + (int) check_data_records},
            {p2_metadata.partition_id_, -1},
        };

        db_tables.get_update_partition_states( polled_versions_actual );
        check_polled_versions( polled_versions_actual,
                               polled_versions_expected );
        polled_versions_actual.clear();
    }

    if( check_data_records ) {
        partition_column_identifier_set read_pids = {
            p1_metadata.partition_id_, p2_row_low_metadata.partition_id_,
            p2_row_high_metadata.partition_id_};

        bool present =
            check_data_present( &db_tables, read_pids, snapshot, table_id,
                                p1_start, p2_end, p1_col_start, p1_col_end );
        EXPECT_TRUE( present );
    }

    // split p2_row_low vertically (creates p2_low, p2_high)
    {
        partition_column_identifier_set write_pids = {
            p1_metadata.partition_id_};
        partition_column_identifier_set read_pids = {
            p2_row_low_metadata.partition_id_};

        transaction_partition_holder* t_holder =
            db_tables.get_partitions( write_pids, read_pids );

        std::shared_ptr<partition> p1 =
            t_holder->get_partition( p1_metadata.partition_id_ );
        std::shared_ptr<partition> p2_row_low =
            t_holder->get_partition( p2_row_low_metadata.partition_id_ );
        split_partition_result split_result = db_tables.split_partition(
            snapshot, p2_row_low_metadata.partition_id_, k_unassigned_key,
            p2_col_split, new_type, new_type, s_type, s_type, no_op_dest,
            no_op_dest );
        EXPECT_EQ( p2_row_low.get(), split_result.partition_cover_.get() );
        snapshot = split_result.snapshot_;

        auto low = split_result.partition_low_;
        auto high = split_result.partition_high_;
        auto ori = split_result.partition_cover_;

        EXPECT_EQ( p2_row_low_metadata.partition_id_,
                   ori->get_metadata().partition_id_ );
        EXPECT_EQ( p2_low_metadata.partition_id_,
                   low->get_metadata().partition_id_ );
        EXPECT_EQ( p2_high_metadata.partition_id_,
                   high->get_metadata().partition_id_ );

        expected_snapshot[low->get_metadata().partition_id_hash_] =
            2 + (int) check_data_records;
        expected_snapshot[high->get_metadata().partition_id_hash_] =
            2 + (int) check_data_records;
        expected_snapshot[ori->get_metadata().partition_id_hash_] =
            2 + (int) check_data_records;
        EXPECT_EQ( expected_snapshot, snapshot );

        transaction_partition_holder* t2_holder =
            db_tables.get_partitions( {}, read_pids );
        // p2 doesn't exist any more
        EXPECT_EQ( nullptr, t2_holder );
        delete t_holder;

        polled_versions_expected = {
            {p2_high_metadata.partition_id_, 2 + (int) check_data_records},
            {p2_low_metadata.partition_id_, 2 + (int) check_data_records},
            {p2_row_low_metadata.partition_id_, -1},
        };

        db_tables.get_update_partition_states( polled_versions_actual );
        check_polled_versions( polled_versions_actual,
                               polled_versions_expected );
        polled_versions_actual.clear();
    }

    if( check_data_records ) {
        partition_column_identifier_set read_pids = {
            p1_metadata.partition_id_, p2_low_metadata.partition_id_,
            p2_high_metadata.partition_id_, p2_row_high_metadata.partition_id_};

        bool present =
            check_data_present( &db_tables, read_pids, snapshot, table_id,
                                p1_start, p2_end, p1_col_start, p1_col_end );
        EXPECT_TRUE( present );
    }

    // split p1 vertically (creates p1_low, p1 high)
    {
        partition_column_identifier_set write_pids = {
            p1_metadata.partition_id_};
        partition_column_identifier_set read_pids = {};
        transaction_partition_holder*   t_holder =
            db_tables.get_partitions( write_pids, read_pids );

        EXPECT_NE( t_holder, nullptr );

        std::shared_ptr<partition> p1 =
            t_holder->get_partition( p1_metadata.partition_id_ );
        split_partition_result split_result = db_tables.split_partition(
            snapshot, p1_metadata.partition_id_, k_unassigned_key, p2_col_split,
            new_type, new_type, s_type, s_type, no_op_dest, no_op_dest );
        EXPECT_EQ( p1.get(), split_result.partition_cover_.get() );
        snapshot = split_result.snapshot_;

        auto low = split_result.partition_low_;
        auto high = split_result.partition_high_;
        auto ori = split_result.partition_cover_;

        EXPECT_EQ( p1_metadata.partition_id_,
                   ori->get_metadata().partition_id_ );
        EXPECT_EQ( p1_low_metadata.partition_id_,
                   low->get_metadata().partition_id_ );
        EXPECT_EQ( p1_high_metadata.partition_id_,
                   high->get_metadata().partition_id_ );

        expected_snapshot[low->get_metadata().partition_id_hash_] =
            1 + (int) check_data_records;
        expected_snapshot[high->get_metadata().partition_id_hash_] =
            1 + (int) check_data_records;
        expected_snapshot[ori->get_metadata().partition_id_hash_] =
            1 + (int) check_data_records;
        EXPECT_EQ( expected_snapshot, snapshot );

        read_pids = {p1_metadata.partition_id_};

        transaction_partition_holder* t2_holder =
            db_tables.get_partitions( {}, read_pids );
        // p1 doesn't exist any more
        EXPECT_EQ( nullptr, t2_holder );
        delete t_holder;

        polled_versions_expected = {
            {p1_high_metadata.partition_id_, 1 + (int) check_data_records},
            {p1_low_metadata.partition_id_, 1 + (int) check_data_records},
            {p1_metadata.partition_id_, -1},
        };

        db_tables.get_update_partition_states( polled_versions_actual );
        check_polled_versions( polled_versions_actual,
                               polled_versions_expected );
        polled_versions_actual.clear();
    }

    if( check_data_records ) {
        partition_column_identifier_set read_pids = {
            p1_low_metadata.partition_id_, p1_high_metadata.partition_id_,
            p2_low_metadata.partition_id_, p2_high_metadata.partition_id_,
            p2_row_high_metadata.partition_id_};

        bool present =
            check_data_present( &db_tables, read_pids, snapshot, table_id,
                                p1_start, p2_end, p1_col_start, p1_col_end );
        EXPECT_TRUE( present );
    }

    // merge horizontally p1_low p2_low ( creates p1_p2_low)
    {
        partition_column_identifier_set read_pids = {
            p1_low_metadata.partition_id_, p2_low_metadata.partition_id_};

        transaction_partition_holder* t_holder =
            db_tables.get_partitions( {}, read_pids );

        EXPECT_NE( t_holder, nullptr );

        std::shared_ptr<partition> p1_low =
            t_holder->get_partition( p1_low_metadata.partition_id_ );
        std::shared_ptr<partition> p2_low =
            t_holder->get_partition( p2_low_metadata.partition_id_ );

        split_partition_result merge_result = db_tables.merge_partition(
            snapshot, p1_low_metadata.partition_id_,
            p2_low_metadata.partition_id_, new_type, s_type, no_op_dest );

        snapshot = merge_result.snapshot_;

        auto low = merge_result.partition_low_;
        auto high = merge_result.partition_high_;
        auto merged = merge_result.partition_cover_;

        EXPECT_EQ( low.get(), p1_low.get() );
        EXPECT_EQ( high.get(), p2_low.get() );

        EXPECT_EQ( p2_low_metadata.partition_id_,
                   high->get_metadata().partition_id_ );
        EXPECT_EQ( p1_low_metadata.partition_id_,
                   low->get_metadata().partition_id_ );
        EXPECT_EQ( p1_p2_low_metadata.partition_id_,
                   merged->get_metadata().partition_id_ );

        expected_snapshot[p1_p2_low_metadata.partition_id_hash_] =
            3 + (int) check_data_records;
        expected_snapshot[p1_low_metadata.partition_id_hash_] =
            2 + (int) check_data_records;
        expected_snapshot[p2_low_metadata.partition_id_hash_] =
            3 + (int) check_data_records;  // part of another hash
        EXPECT_EQ( expected_snapshot, snapshot );

        delete t_holder;

        polled_versions_expected = {
            {p1_p2_low_metadata.partition_id_, 3 + (int) check_data_records},
            {p2_low_metadata.partition_id_, -1},
            {p1_low_metadata.partition_id_, -1},
        };

        db_tables.get_update_partition_states( polled_versions_actual );
        check_polled_versions( polled_versions_actual,
                               polled_versions_expected );
        polled_versions_actual.clear();
    }

    if( check_data_records ) {
        partition_column_identifier_set read_pids = {
            p1_p2_low_metadata.partition_id_, p1_high_metadata.partition_id_,
            p2_high_metadata.partition_id_, p2_row_high_metadata.partition_id_};

        bool present =
            check_data_present( &db_tables, read_pids, snapshot, table_id,
                                p1_start, p2_end, p1_col_start, p1_col_end );
        EXPECT_TRUE( present );
    }

    // merge horizontally p1_high, p2_high (create p1_p2_high)
    {
        partition_column_identifier_set read_pids = {
            p1_high_metadata.partition_id_, p2_high_metadata.partition_id_};

        transaction_partition_holder* t_holder =
            db_tables.get_partitions( {}, read_pids );

        EXPECT_NE( t_holder, nullptr );

        std::shared_ptr<partition> p1_high =
            t_holder->get_partition( p1_high_metadata.partition_id_ );
        std::shared_ptr<partition> p2_high =
            t_holder->get_partition( p2_high_metadata.partition_id_ );

        split_partition_result merge_result = db_tables.merge_partition(
            snapshot, p1_high_metadata.partition_id_,
            p2_high_metadata.partition_id_, new_type, s_type, no_op_dest );

        snapshot = merge_result.snapshot_;

        auto low = merge_result.partition_low_;
        auto high = merge_result.partition_high_;
        auto merged = merge_result.partition_cover_;

        EXPECT_EQ( low.get(), p1_high.get() );
        EXPECT_EQ( high.get(), p2_high.get() );

        EXPECT_EQ( p2_high_metadata.partition_id_,
                   high->get_metadata().partition_id_ );
        EXPECT_EQ( p1_high_metadata.partition_id_,
                   low->get_metadata().partition_id_ );
        EXPECT_EQ( p1_p2_high_metadata.partition_id_,
                   merged->get_metadata().partition_id_ );

        expected_snapshot[p1_p2_high_metadata.partition_id_hash_] =
            3 + (int) check_data_records;
        expected_snapshot[p1_high_metadata.partition_id_hash_] =
            2 + (int) check_data_records;
        expected_snapshot[p2_high_metadata.partition_id_hash_] =
            3 + (int) check_data_records;  // part of another hash
        EXPECT_EQ( expected_snapshot, snapshot );

        delete t_holder;

        polled_versions_expected = {
            {p1_p2_high_metadata.partition_id_, 3 + (int) check_data_records},
            {p2_high_metadata.partition_id_, -1},
            {p1_high_metadata.partition_id_, -1},
        };

        db_tables.get_update_partition_states( polled_versions_actual );
        check_polled_versions( polled_versions_actual,
                               polled_versions_expected );
        polled_versions_actual.clear();
    }
    if( check_data_records ) {
        partition_column_identifier_set read_pids = {
            p1_p2_low_metadata.partition_id_, p1_p2_high_metadata.partition_id_,
            p2_row_high_metadata.partition_id_};

        bool present =
            check_data_present( &db_tables, read_pids, snapshot, table_id,
                                p1_start, p2_end, p1_col_start, p1_col_end );
        EXPECT_TRUE( present );
    }

    //merge vertically p1_p2_low, p1_p2_high (creates p1_p2)
    {
        partition_column_identifier_set read_pids = {
            p1_p2_low_metadata.partition_id_,
            p1_p2_high_metadata.partition_id_};

        transaction_partition_holder* t_holder =
            db_tables.get_partitions( {}, read_pids );

        EXPECT_NE( t_holder, nullptr );

        std::shared_ptr<partition> p1_p2_low =
            t_holder->get_partition( p1_p2_low_metadata.partition_id_ );
        std::shared_ptr<partition> p1_p2_high =
            t_holder->get_partition( p1_p2_high_metadata.partition_id_ );

        split_partition_result merge_result = db_tables.merge_partition(
            snapshot, p1_p2_low_metadata.partition_id_,
            p1_p2_high_metadata.partition_id_, new_type, s_type, no_op_dest );

        snapshot = merge_result.snapshot_;

        auto low = merge_result.partition_low_;
        auto high = merge_result.partition_high_;
        auto merged = merge_result.partition_cover_;

        EXPECT_EQ( low.get(), p1_p2_low.get() );
        EXPECT_EQ( high.get(), p1_p2_high.get() );

        EXPECT_EQ( p1_p2_high_metadata.partition_id_,
                   high->get_metadata().partition_id_ );
        EXPECT_EQ( p1_p2_low_metadata.partition_id_,
                   low->get_metadata().partition_id_ );
        EXPECT_EQ( p1_p2_metadata.partition_id_,
                   merged->get_metadata().partition_id_ );

        expected_snapshot[p1_p2_metadata.partition_id_hash_] =
            4 + (int) check_data_records;
        expected_snapshot[p1_p2_low_metadata.partition_id_hash_] =
            4 + (int) check_data_records;
        expected_snapshot[p1_p2_high_metadata.partition_id_hash_] =
            4 + (int) check_data_records;  // part of another hash
        EXPECT_EQ( expected_snapshot, snapshot );

        delete t_holder;

        polled_versions_expected = {
            {p1_p2_metadata.partition_id_, 4 + (int) check_data_records},
            {p1_p2_low_metadata.partition_id_, -1},
            {p1_p2_high_metadata.partition_id_, -1},
        };

        db_tables.get_update_partition_states( polled_versions_actual );
        check_polled_versions( polled_versions_actual,
                               polled_versions_expected );
        polled_versions_actual.clear();
    }

    if( check_data_records ) {
        partition_column_identifier_set read_pids = {
            p1_p2_metadata.partition_id_, p2_row_high_metadata.partition_id_};

        bool present =
            check_data_present( &db_tables, read_pids, snapshot, table_id,
                                p1_start, p2_end, p1_col_start, p1_col_end );
        EXPECT_TRUE( present );
    }
    // assert that partitions are not present, and create them
    {
        partition_column_identifier_set pids = {p1_metadata.partition_id_};

        // these read pids don't exist anymore
        transaction_partition_holder* t_holder =
            db_tables.get_partitions( {}, {pids} );
        EXPECT_EQ( nullptr, t_holder );

        // but we shouldn't be able to create them if they are part of our
        // write set
        t_holder = db_tables.get_partition_holder(
            pids, {}, partition_lookup_operation::GET_WRITE_MAY_CREATE_READS );
        EXPECT_EQ( nullptr, t_holder );

        // but we should be able to create them if they are part of our read set
        t_holder = db_tables.get_partition_holder(
            {}, pids, partition_lookup_operation::GET_WRITE_MAY_CREATE_READS );
        EXPECT_NE( nullptr, t_holder );
        std::shared_ptr<partition> p1 =
            t_holder->get_partition( p1_metadata.partition_id_ );
        EXPECT_NE( nullptr, p1 );
        delete t_holder;

        polled_versions_expected = {
            {p1_metadata.partition_id_, 0},
        };

        db_tables.get_update_partition_states( polled_versions_actual );
        check_polled_versions( polled_versions_actual,
                               polled_versions_expected );
        polled_versions_actual.clear();

    }

    // GC
    {

        auto p1 = db_tables.get_partition( p1_metadata.partition_id_ );
        EXPECT_NE( nullptr, p1 );

        // pin the partition
        p1->pin_partition();
        db_tables.remove_partition( p1_metadata.partition_id_ );
    }
    {
        auto p1 =
            db_tables.get_partition_if_active( p1_metadata.partition_id_ );
        EXPECT_EQ( nullptr, p1 );

        p1 = db_tables.get_partition( p1_metadata.partition_id_ );
        EXPECT_NE( nullptr, p1 );
        p1->unpin_partition();

        db_tables.gc_inactive_partitions();

        polled_versions_expected = {
            {p1_metadata.partition_id_, -1},
        };

        db_tables.get_update_partition_states( polled_versions_actual );
        check_polled_versions( polled_versions_actual,
                               polled_versions_expected );
        polled_versions_actual.clear();

    }
    {
        auto p1 =
            db_tables.get_partition_if_active( p1_metadata.partition_id_ );
        EXPECT_EQ( nullptr, p1 );

        p1 = db_tables.get_partition( p1_metadata.partition_id_ );
        EXPECT_EQ( nullptr, p1 );
    }
}

TEST_F( table_test, table_creation_test_row_row ) {
    table_creation_test( partition_type::type::ROW, partition_type::type::ROW,
                         false );
}
TEST_F( table_test, table_creation_test_row_col ) {
    table_creation_test( partition_type::type::ROW,
                         partition_type::type::COLUMN, false );
}
TEST_F( table_test, table_creation_test_row_sorted_col ) {
    table_creation_test( partition_type::type::ROW,
                         partition_type::type::SORTED_COLUMN, false );
}
TEST_F( table_test, table_creation_test_row_multi_col ) {
    table_creation_test( partition_type::type::ROW,
                         partition_type::type::MULTI_COLUMN, false );
}
TEST_F( table_test, table_creation_test_row_sorted_multi_col ) {
    table_creation_test( partition_type::type::ROW,
                         partition_type::type::SORTED_MULTI_COLUMN, false );
}

TEST_F( table_test, table_creation_test_col_col ) {
    table_creation_test( partition_type::type::COLUMN,
                         partition_type::type::COLUMN, false );
}
TEST_F( table_test, table_creation_test_col_row ) {
    table_creation_test( partition_type::type::COLUMN,
                         partition_type::type::ROW, false );
}
TEST_F( table_test, table_creation_test_col_sorted_col ) {
    table_creation_test( partition_type::type::COLUMN,
                         partition_type::type::SORTED_COLUMN, false );
}
TEST_F( table_test, table_creation_test_col_multi_col ) {
    table_creation_test( partition_type::type::COLUMN,
                         partition_type::type::MULTI_COLUMN, false );
}
TEST_F( table_test, table_creation_test_col_sorted_multi_col ) {
    table_creation_test( partition_type::type::COLUMN,
                         partition_type::type::SORTED_MULTI_COLUMN, false );
}

TEST_F( table_test, table_creation_test_sorted_col_sorted_col ) {
    table_creation_test( partition_type::type::SORTED_COLUMN,
                         partition_type::type::SORTED_COLUMN, false );
}
TEST_F( table_test, table_creation_test_sorted_col_row ) {
    table_creation_test( partition_type::type::SORTED_COLUMN,
                         partition_type::type::ROW, false );
}
TEST_F( table_test, table_creation_test_sorted_col_col ) {
    table_creation_test( partition_type::type::SORTED_COLUMN,
                         partition_type::type::COLUMN, false );
}
TEST_F( table_test, table_creation_test_sorted_col_sorted_multi_col ) {
    table_creation_test( partition_type::type::SORTED_COLUMN,
                         partition_type::type::SORTED_MULTI_COLUMN, false );
}
TEST_F( table_test, table_creation_test_sorted_col_multi_col ) {
    table_creation_test( partition_type::type::SORTED_COLUMN,
                         partition_type::type::MULTI_COLUMN, false );
}

TEST_F(table_test, table_creation_test_multi_col_col) {
  table_creation_test(partition_type::type::MULTI_COLUMN,
                      partition_type::type::COLUMN, false);
}
TEST_F(table_test, table_creation_test_multi_col_row) {
  table_creation_test(partition_type::type::MULTI_COLUMN,
                      partition_type::type::ROW, false);
}
TEST_F(table_test, table_creation_test_multi_col_sorted_col) {
  table_creation_test(partition_type::type::MULTI_COLUMN,
                      partition_type::type::SORTED_COLUMN, false);
}
TEST_F(table_test, table_creation_test_multi_col_multi_col) {
  table_creation_test(partition_type::type::MULTI_COLUMN,
                      partition_type::type::MULTI_COLUMN, false);
}
TEST_F(table_test, table_creation_test_multi_col_sorted_multi_col) {
  table_creation_test(partition_type::type::MULTI_COLUMN,
                      partition_type::type::SORTED_MULTI_COLUMN, false);
}

TEST_F(table_test, table_creation_test_multi_sorted_col_sorted_col) {
  table_creation_test(partition_type::type::SORTED_MULTI_COLUMN,
                      partition_type::type::SORTED_COLUMN, false);
}
TEST_F(table_test, table_creation_test_multi_sorted_col_row) {
  table_creation_test(partition_type::type::SORTED_MULTI_COLUMN,
                      partition_type::type::ROW, false);
}
TEST_F(table_test, table_creation_test_multi_sorted_col_col) {
  table_creation_test(partition_type::type::SORTED_MULTI_COLUMN,
                      partition_type::type::COLUMN, false);
}
TEST_F(table_test, table_creation_test_multi_sorted_col_sorted_multi_col) {
  table_creation_test(partition_type::type::SORTED_MULTI_COLUMN,
                      partition_type::type::SORTED_MULTI_COLUMN, false);
}
TEST_F(table_test, table_creation_test_multi_sorted_col_multi_col) {
  table_creation_test(partition_type::type::SORTED_MULTI_COLUMN,
                      partition_type::type::MULTI_COLUMN, false);
}


TEST_F( table_test, table_creation_test_row_row_data ) {
    table_creation_test( partition_type::type::ROW, partition_type::type::ROW,
                         true );
}
TEST_F( table_test, table_creation_test_row_col_data ) {
    table_creation_test( partition_type::type::ROW,
                         partition_type::type::COLUMN, true );
}
TEST_F( table_test, table_creation_test_row_sorted_col_data ) {
    table_creation_test( partition_type::type::ROW,
                         partition_type::type::SORTED_COLUMN, true );
}
TEST_F( table_test, table_creation_test_row_multi_col_data ) {
    table_creation_test( partition_type::type::ROW,
                         partition_type::type::MULTI_COLUMN, true );
}
TEST_F( table_test, table_creation_test_row_sorted_multi_col_data ) {
    table_creation_test( partition_type::type::ROW,
                         partition_type::type::SORTED_MULTI_COLUMN, true );
}

TEST_F( table_test, table_creation_test_col_col_data ) {
    table_creation_test( partition_type::type::COLUMN,
                         partition_type::type::COLUMN, true );
}
TEST_F( table_test, table_creation_test_col_row_data ) {
    table_creation_test( partition_type::type::COLUMN,
                         partition_type::type::ROW, true );
}
TEST_F( table_test, table_creation_test_col_sorted_col_data ) {
    table_creation_test( partition_type::type::COLUMN,
                         partition_type::type::SORTED_COLUMN, true );
}
TEST_F( table_test, table_creation_test_col_multi_col_data ) {
    table_creation_test( partition_type::type::COLUMN,
                         partition_type::type::MULTI_COLUMN, true );
}
TEST_F( table_test, table_creation_test_col_sorted_multi_col_data ) {
    table_creation_test( partition_type::type::COLUMN,
                         partition_type::type::SORTED_MULTI_COLUMN, true );
}

TEST_F( table_test, table_creation_test_sorted_col_sorted_col_data ) {
    table_creation_test( partition_type::type::SORTED_COLUMN,
                         partition_type::type::SORTED_COLUMN, true );
}
TEST_F( table_test, table_creation_test_sorted_col_row_data ) {
    table_creation_test( partition_type::type::SORTED_COLUMN,
                         partition_type::type::ROW, true );
}
TEST_F( table_test, table_creation_test_sorted_col_col_data ) {
    table_creation_test( partition_type::type::SORTED_COLUMN,
                         partition_type::type::COLUMN, true );
}
TEST_F( table_test, table_creation_test_sorted_col_sorted_multi_col_data ) {
    table_creation_test( partition_type::type::SORTED_COLUMN,
                         partition_type::type::SORTED_MULTI_COLUMN, true );
}
TEST_F( table_test, table_creation_test_sorted_col_multi_col_data ) {
    table_creation_test( partition_type::type::SORTED_COLUMN,
                         partition_type::type::MULTI_COLUMN, true );
}

TEST_F(table_test, table_creation_test_multi_col_col_data) {
  table_creation_test(partition_type::type::MULTI_COLUMN,
                      partition_type::type::COLUMN, true);
}
TEST_F(table_test, table_creation_test_multi_col_row_data) {
  table_creation_test(partition_type::type::COLUMN, partition_type::type::ROW,
                      true);
}
TEST_F(table_test, table_creation_test_multi_col_sorted_col_data) {
  table_creation_test(partition_type::type::MULTI_COLUMN,
                      partition_type::type::SORTED_COLUMN, true);
}
TEST_F(table_test, table_creation_test_multi_col_multi_col_data) {
  table_creation_test(partition_type::type::MULTI_COLUMN,
                      partition_type::type::MULTI_COLUMN, true);
}
TEST_F(table_test, table_creation_test_multi_col_sorted_multi_col_data) {
  table_creation_test(partition_type::type::MULTI_COLUMN,
                      partition_type::type::SORTED_MULTI_COLUMN, true);
}

TEST_F(table_test, table_creation_test_multi_sorted_col_sorted_col_data) {
  table_creation_test(partition_type::type::SORTED_MULTI_COLUMN,
                      partition_type::type::SORTED_COLUMN, true);
}
TEST_F(table_test, table_creation_test_multi_sorted_col_row_data) {
  table_creation_test(partition_type::type::SORTED_MULTI_COLUMN,
                      partition_type::type::ROW, true);
}
TEST_F(table_test, table_creation_test_multi_sorted_col_col_data) {
  table_creation_test(partition_type::type::SORTED_MULTI_COLUMN,
                      partition_type::type::COLUMN, true);
}
TEST_F(table_test, table_creation_test_multi_sorted_col_sorted_multi_col_data) {
  table_creation_test(partition_type::type::SORTED_MULTI_COLUMN,
                      partition_type::type::SORTED_MULTI_COLUMN, true);
}
TEST_F(table_test, table_creation_test_multi_sorted_col_multi_col_data) {
  table_creation_test(partition_type::type::SORTED_MULTI_COLUMN,
                      partition_type::type::MULTI_COLUMN, true);
}
