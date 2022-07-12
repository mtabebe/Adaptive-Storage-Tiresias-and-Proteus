#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

#include "../src/data-site/db/col_partition.h"
#include "../src/data-site/db/partition.h"
#include "../src/data-site/db/partition_util.h"
#include "../src/data-site/db/row_partition.h"
#include "../src/data-site/update-propagation/no_op_update_destination.h"

class partition_test : public ::testing::Test {};

TEST_F( partition_test, row_partition_creation_test ) {
    uint32_t num_records_in_chain = 5;
    uint32_t num_records_in_snapshot_chain = 5;
    uint32_t table_id = 0;
    uint64_t partition_start = 5;
    uint64_t partition_end = 20;
    uint32_t site_location = 0;
    uint32_t col_start = 0;
    uint32_t col_end = 1;

    partition_metadata p_metadata = create_partition_metadata(
        table_id, partition_start, partition_end, col_start, col_end,
        num_records_in_chain, num_records_in_snapshot_chain, site_location );

    auto part_version_holder =
        std::make_shared<partition_column_version_holder>(
            p_metadata.partition_id_, 0, 0 );

    partition* part = create_partition_ptr( partition_type::type::ROW );
    std::vector<cell_data_type> col_types = {cell_data_type::UINT64,
                                             cell_data_type::UINT64};
    part->init( p_metadata, std::make_shared<no_op_update_destination>( 0 ),
                part_version_holder, col_types, (void*) nullptr /*table */, 0 );

    cell_identifier cid;
    cid.table_id_ = 0;
    cid.col_id_ = col_start;
    cid.key_ = 5;

    snapshot_vector snapshot;

    auto read_res = part->get_uint64_data( cid, snapshot );
    EXPECT_FALSE( std::get<0>( read_res ) );

    part->init_records();

    EXPECT_TRUE( part->begin_write( snapshot, true /*acquire locks*/ ) );

    for( uint64_t key = partition_start; key <= partition_end; key++ ) {
        for( uint32_t col_id = col_start; col_id <= col_end; col_id++ ) {
            cid.key_ = key;
            cid.col_id_ = col_id;
            bool write_ok =
                part->update_uint64_data( cid, key * col_id, snapshot );
            EXPECT_TRUE( write_ok );
        }
    }
    snapshot_vector commit_snapshot;
    part->build_commit_snapshot( commit_snapshot );

    uint64_t version =
        part->commit_write_transaction( commit_snapshot, 1, true );
    EXPECT_EQ( 1, version );
    uint64_t part_v;
    uint64_t poll_epoch;
    part->get_partition_column_version_holder()->get_version_and_epoch(
        p_metadata.partition_id_, &part_v, &poll_epoch );
    EXPECT_EQ( 1, part_v );
    EXPECT_EQ( 1, poll_epoch );

    snapshot_vector read_snapshot = commit_snapshot;

    EXPECT_TRUE( part->begin_read_wait_and_build_snapshot(
        read_snapshot, false /* apply last */ ) );
    EXPECT_TRUE( part->begin_read_wait_for_version( read_snapshot,
                                                    false /* apply last */ ) );
    for( uint64_t key = partition_start; key <= partition_end; key++ ) {
        for( uint32_t col_id = col_start; col_id <= col_end; col_id++ ) {
            cid.key_ = key;
            cid.col_id_ = col_id;
            read_res = part->get_uint64_data( cid, read_snapshot );
            EXPECT_TRUE( std::get<0>( read_res ) );
            EXPECT_EQ( key * col_id, std::get<1>( read_res ) );
        }
    }

    // no writes
    snapshot = commit_snapshot;
    EXPECT_TRUE( part->begin_write( snapshot, true /* acquire locks*/ ) );
    part->build_commit_snapshot( commit_snapshot );
    version = part->commit_write_transaction( commit_snapshot, 1, true );
    EXPECT_EQ( 1, version );  // no increment
}

void single_col_partition_creation( bool                        is_sorted,
                                    const partition_type::type& part_type ) {
    uint32_t num_records_in_chain = 5;
    uint32_t num_records_in_snapshot_chain = 5;
    uint32_t table_id = 0;
    uint64_t partition_start = 5;
    uint64_t partition_end = 20;
    uint32_t site_location = 0;
    uint32_t col_start = 1;
    uint32_t col_end = 1;

    partition_metadata p_metadata = create_partition_metadata(
        table_id, partition_start, partition_end, col_start, col_end,
        num_records_in_chain, num_records_in_snapshot_chain, site_location );

    auto part_version_holder =
        std::make_shared<partition_column_version_holder>(
            p_metadata.partition_id_, 0, 0 );

    partition*                  part = create_partition_ptr( part_type );
    std::vector<cell_data_type> col_types = {cell_data_type::UINT64};
    part->init( p_metadata, std::make_shared<no_op_update_destination>( 0 ),
                part_version_holder, col_types, (void*) nullptr /*table */, 0 );

    cell_identifier cid;
    cid.table_id_ = 0;
    cid.col_id_ = col_start;
    cid.key_ = 5;

    snapshot_vector snapshot;

    auto read_res = part->get_uint64_data( cid, snapshot );
    EXPECT_FALSE( std::get<0>( read_res ) );

    part->init_records();

    EXPECT_TRUE( part->begin_write( snapshot, true /*acquire locks*/ ) );

    for( uint64_t key = partition_start; key <= partition_end; key++ ) {
        for( uint32_t col_id = col_start; col_id <= col_end; col_id++ ) {
            cid.key_ = key;
            cid.col_id_ = col_id;
            bool write_ok =
                part->update_uint64_data( cid, key * col_id, snapshot );
            EXPECT_TRUE( write_ok );
        }
    }
    snapshot_vector commit_snapshot;
    part->build_commit_snapshot( commit_snapshot );

    uint64_t version =
        part->commit_write_transaction( commit_snapshot, 1, true );
    EXPECT_EQ( 1, version );
    uint64_t part_v;
    uint64_t poll_epoch;
    part->get_partition_column_version_holder()->get_version_and_epoch(
        p_metadata.partition_id_, &part_v, &poll_epoch );
    EXPECT_EQ( 1, part_v );
    EXPECT_EQ( 1, poll_epoch );

    snapshot_vector read_snapshot = commit_snapshot;

    EXPECT_TRUE( part->begin_read_wait_and_build_snapshot(
        read_snapshot, false /* apply last */ ) );
    EXPECT_TRUE( part->begin_read_wait_for_version( read_snapshot,
                                                    false /* apply last */ ) );
    for( uint64_t key = partition_start; key <= partition_end; key++ ) {
        for( uint32_t col_id = col_start; col_id <= col_end; col_id++ ) {
            cid.key_ = key;
            cid.col_id_ = col_id;
            read_res = part->get_uint64_data( cid, read_snapshot );
            EXPECT_TRUE( std::get<0>( read_res ) );
            EXPECT_EQ( key * col_id, std::get<1>( read_res ) );
        }
    }


    // no writes
    snapshot = commit_snapshot;
    EXPECT_TRUE( part->begin_write( snapshot, true /* acquire locks*/ ) );
    // no writes
    part->build_commit_snapshot( commit_snapshot );
    version = part->commit_write_transaction( commit_snapshot, 1, true );
    EXPECT_EQ( 1, version );  // no increment
}

TEST_F( partition_test, single_col_partition_creation_test ) {
    single_col_partition_creation( false, partition_type::type::COLUMN );
}
TEST_F( partition_test, single_sorted_col_partition_creation_test ) {
    single_col_partition_creation( true, partition_type::type::SORTED_COLUMN );
}
TEST_F( partition_test, single_multi_col_partition_creation_test ) {
    single_col_partition_creation( false, partition_type::type::MULTI_COLUMN );
}
TEST_F( partition_test, single_sorted_multi_col_partition_creation_test ) {
    single_col_partition_creation( true,
                                   partition_type::type::SORTED_MULTI_COLUMN );
}

void multi_col_partition_creation( bool                        is_sorted,
                                   const partition_type::type& part_type ) {
    uint32_t num_records_in_chain = 5;
    uint32_t num_records_in_snapshot_chain = 5;
    uint32_t table_id = 0;
    uint64_t partition_start = 5;
    uint64_t partition_end = 20;
    uint32_t site_location = 0;
    uint32_t col_start = 0;
    uint32_t col_end = 1;

    partition_metadata p_metadata = create_partition_metadata(
        table_id, partition_start, partition_end, col_start, col_end,
        num_records_in_chain, num_records_in_snapshot_chain, site_location );

    auto part_version_holder =
        std::make_shared<partition_column_version_holder>(
            p_metadata.partition_id_, 0, 0 );

    partition* part = create_partition_ptr( part_type );

    std::vector<cell_data_type> col_types = {cell_data_type::UINT64,
                                             cell_data_type::UINT64};
    part->init( p_metadata, std::make_shared<no_op_update_destination>( 0 ),
                part_version_holder, col_types, (void*) nullptr /*table */, 0 );

    cell_identifier cid;
    cid.table_id_ = 0;
    cid.col_id_ = col_start;
    cid.key_ = 5;

    snapshot_vector snapshot;

    auto read_res = part->get_uint64_data( cid, snapshot );
    EXPECT_FALSE( std::get<0>( read_res ) );

    part->init_records();

    EXPECT_TRUE( part->begin_write( snapshot, true /*acquire locks*/ ) );

    for( uint64_t key = partition_start; key <= partition_end; key++ ) {
        for( uint32_t col_id = col_start; col_id <= col_end; col_id++ ) {
            cid.key_ = key;
            cid.col_id_ = col_id;
            bool write_ok =
                part->update_uint64_data( cid, key * col_id, snapshot );
            EXPECT_TRUE( write_ok );
        }
    }
    snapshot_vector commit_snapshot;
    part->build_commit_snapshot( commit_snapshot );

    uint64_t version =
        part->commit_write_transaction( commit_snapshot, 1, true );
    EXPECT_EQ( 1, version );
    uint64_t part_v;
    uint64_t poll_epoch;
    part->get_partition_column_version_holder()->get_version_and_epoch(
        p_metadata.partition_id_, &part_v, &poll_epoch );
    EXPECT_EQ( 1, part_v );
    EXPECT_EQ( 1, poll_epoch );

    snapshot_vector read_snapshot = commit_snapshot;

    EXPECT_TRUE( part->begin_read_wait_and_build_snapshot(
        read_snapshot, false /* apply last */ ) );
    EXPECT_TRUE( part->begin_read_wait_for_version( read_snapshot,
                                                    false /* apply last */ ) );
    for( uint64_t key = partition_start; key <= partition_end; key++ ) {
        for( uint32_t col_id = col_start; col_id <= col_end; col_id++ ) {
            cid.key_ = key;
            cid.col_id_ = col_id;
            read_res = part->get_uint64_data( cid, read_snapshot );
            EXPECT_TRUE( std::get<0>( read_res ) );
            EXPECT_EQ( key * col_id, std::get<1>( read_res ) );
        }
    }


    // no writes
    snapshot = commit_snapshot;
    EXPECT_TRUE( part->begin_write( snapshot, true /* acquire locks*/ ) );
    // no writes
    part->build_commit_snapshot( commit_snapshot );
    version = part->commit_write_transaction( commit_snapshot, 1, true );
    EXPECT_EQ( 1, version );  // no increment

    read_snapshot.clear();
    set_snapshot_version( read_snapshot, p_metadata.partition_id_, 0 );

    EXPECT_TRUE( part->begin_read_wait_and_build_snapshot(
        read_snapshot, false /* apply last */ ) );
    EXPECT_TRUE( part->begin_read_wait_for_version( read_snapshot,
                                                    false /* apply last */ ) );
    for( uint64_t key = partition_start; key <= partition_end; key++ ) {
        for( uint32_t col_id = col_start; col_id <= col_end; col_id++ ) {
            cid.key_ = key;
            cid.col_id_ = col_id;
            read_res = part->get_uint64_data( cid, read_snapshot );
            EXPECT_FALSE( std::get<0>( read_res ) );
        }
    }
}

TEST_F( partition_test, multi_col_partition_creation_test ) {
    multi_col_partition_creation( false, partition_type::type::COLUMN );
}
TEST_F( partition_test, multi_sorted_col_partition_creation_test ) {
    multi_col_partition_creation( true, partition_type::type::SORTED_COLUMN );
}
TEST_F( partition_test, multi_multi_col_partition_creation_test ) {
    multi_col_partition_creation( false, partition_type::type::MULTI_COLUMN );
}
TEST_F( partition_test, multi_sorted_multi_col_partition_creation_test ) {
    multi_col_partition_creation( true,
                                  partition_type::type::SORTED_MULTI_COLUMN );
}


void partition_interface_test( partition* part ) {
    uint32_t num_records_in_chain = 5;
    uint32_t num_records_in_snapshot_chain = 5;
    uint32_t table_id = 0;
    uint64_t partition_start = 5;
    uint64_t partition_end = 20;
    uint32_t site_location = 0;
    uint32_t col_start = 0;
    uint32_t col_end = 1;

    partition_metadata p_metadata = create_partition_metadata(
        table_id, partition_start, partition_end, col_start, col_end,
        num_records_in_chain, num_records_in_snapshot_chain, site_location );

    auto part_version_holder =
        std::make_shared<partition_column_version_holder>(
            p_metadata.partition_id_, 0, 0 );

    std::vector<cell_data_type> col_types = {cell_data_type::UINT64,
                                             cell_data_type::STRING};
    part->init( p_metadata, std::make_shared<no_op_update_destination>( 0 ),
                part_version_holder, col_types, (void*) nullptr /*table */, 0 );

    std::vector<std::string> strings = {
        "five",      "six",      "seven",    "eight",    "nine",    "ten",
        "eleven",    "twelve",   "thirteen", "fourteen", "fifteen", "sixteen",
        "seventeen", "eighteen", "nineteen", "twenty",
    };

    cell_identifier cid;
    cid.table_id_ = 0;
    cid.col_id_ = col_start;
    cid.key_ = 5;

    snapshot_vector snapshot;

    auto int_read_res = part->get_uint64_data( cid, snapshot );
    EXPECT_FALSE( std::get<0>( int_read_res ) );

    part->init_records();

    EXPECT_TRUE( part->begin_write( snapshot, true /*acquire locks*/ ) );

    for( uint64_t key = partition_start; key <= partition_end; key++ ) {
            cid.key_ = key;
            cid.col_id_ = 0;
            bool write_ok = part->update_uint64_data( cid, key, snapshot );
            EXPECT_TRUE( write_ok );

            cid.col_id_ = 1;
            write_ok = part->update_string_data(
                cid, strings.at( key - partition_start ), snapshot );
            EXPECT_TRUE( write_ok );

    }
    snapshot_vector commit_snapshot;
    part->build_commit_snapshot( commit_snapshot );

    uint64_t version =
        part->commit_write_transaction( commit_snapshot, 1, true );
    EXPECT_EQ( 1, version );
    uint64_t part_v;
    uint64_t poll_epoch;
    part->get_partition_column_version_holder()->get_version_and_epoch(
        p_metadata.partition_id_, &part_v, &poll_epoch );
    EXPECT_EQ( 1, part_v );
    EXPECT_EQ( 1, poll_epoch );

    snapshot_vector read_snapshot = commit_snapshot;

    EXPECT_TRUE( part->begin_read_wait_and_build_snapshot(
        read_snapshot, false /* apply last */ ) );
    EXPECT_TRUE( part->begin_read_wait_for_version( read_snapshot,
                                                    false /* apply last */ ) );
    for( uint64_t key = partition_start; key <= partition_end; key++ ) {
        cid.key_ = key;
        cid.col_id_ = 0;
        int_read_res = part->get_uint64_data( cid, read_snapshot );
        EXPECT_TRUE( std::get<0>( int_read_res ) );
        EXPECT_EQ( key, std::get<1>( int_read_res ) );

        cid.col_id_ = 1;
        auto str_read_res = part->get_string_data( cid, read_snapshot );
        EXPECT_TRUE( std::get<0>( str_read_res ) );
        EXPECT_EQ( strings.at( key - partition_start ),
                   std::get<1>( str_read_res ) );
    }

    // no writes
    snapshot = commit_snapshot;
    EXPECT_TRUE( part->begin_write( snapshot, true /* acquire locks*/ ) );
    // no writes
    part->build_commit_snapshot( commit_snapshot );
    version = part->commit_write_transaction( commit_snapshot, 1, true );
    EXPECT_EQ( 1, version );  // no increment

    EXPECT_TRUE( part->begin_write( snapshot, true /*acquire locks*/ ) );

    for( uint64_t key = partition_start; key <= partition_end; key++ ) {
            cid.key_ = key;
            cid.col_id_ = 0;
            bool write_ok = part->update_uint64_data( cid, key * 2, snapshot );
            EXPECT_TRUE( write_ok );

            cid.col_id_ = 1;
            write_ok = part->update_string_data(
                cid, strings.at( key - partition_start ), snapshot );
            EXPECT_TRUE( write_ok );

    }
    part->build_commit_snapshot( commit_snapshot );

    version = part->commit_write_transaction( commit_snapshot, 2, true );
    EXPECT_EQ( 2, version );

    read_snapshot = commit_snapshot;

    EXPECT_TRUE( part->begin_read_wait_and_build_snapshot(
        read_snapshot, false /* apply last */ ) );
    EXPECT_TRUE( part->begin_read_wait_for_version( read_snapshot,
                                                    false /* apply last */ ) );
    for( uint64_t key = partition_start; key <= partition_end; key++ ) {
        cid.key_ = key;
        cid.col_id_ = 0;
        int_read_res = part->get_uint64_data( cid, read_snapshot );
        EXPECT_TRUE( std::get<0>( int_read_res ) );
        EXPECT_EQ( key * 2, std::get<1>( int_read_res ) );

        cid.col_id_ = 1;
        auto str_read_res = part->get_string_data( cid, read_snapshot );
        EXPECT_TRUE( std::get<0>( str_read_res ) );
        EXPECT_EQ( strings.at( key - partition_start ),
                   std::get<1>( str_read_res ) );
    }

    EXPECT_TRUE( part->begin_write( snapshot, true /*acquire locks*/ ) );

    for( uint64_t key = partition_start; key <= partition_end; key++ ) {
        cid.key_ = key;
        cid.col_id_ = 0;
        bool write_ok = part->remove_data( cid, snapshot );
        EXPECT_TRUE( write_ok );

        if( key % 2 == 0 ) {
            cid.col_id_ = 1;

            write_ok = part->remove_data( cid, snapshot );
            EXPECT_TRUE( write_ok );
        }
    }
    part->build_commit_snapshot( commit_snapshot );

    version = part->commit_write_transaction( commit_snapshot, 2, true );
    EXPECT_EQ( 3, version );

    read_snapshot = commit_snapshot;

    EXPECT_TRUE( part->begin_read_wait_and_build_snapshot(
        read_snapshot, false /* apply last */ ) );
    EXPECT_TRUE( part->begin_read_wait_for_version( read_snapshot,
                                                    false /* apply last */ ) );
    for( uint64_t key = partition_start; key <= partition_end; key++ ) {
        cid.key_ = key;
        cid.col_id_ = 0;
        int_read_res = part->get_uint64_data( cid, read_snapshot );
        EXPECT_FALSE( std::get<0>( int_read_res ) );

        cid.col_id_ = 1;
        auto str_read_res = part->get_string_data( cid, read_snapshot );
        if( key % 2 == 0 ) {
            EXPECT_FALSE( std::get<0>( str_read_res ) );
        } else {
            EXPECT_TRUE( std::get<0>( str_read_res ) );
            EXPECT_EQ( strings.at( key - partition_start ),
                       std::get<1>( str_read_res ) );
        }
    }
}

TEST_F( partition_test, col_partition_interface_test ) {
    partition* p = create_partition_ptr( partition_type::type::COLUMN );
    partition_interface_test( p );
    delete p;
}
TEST_F( partition_test, sorted_col_partition_interface_test ) {
    partition* p = create_partition_ptr( partition_type::type::SORTED_COLUMN );
    partition_interface_test( p );
    delete p;
}
TEST_F( partition_test, multi_col_partition_interface_test ) {
    partition* p = create_partition_ptr( partition_type::type::MULTI_COLUMN );
    partition_interface_test( p );
    delete p;
}
TEST_F( partition_test, sorted_multi_col_partition_interface_test ) {
    partition* p =
        create_partition_ptr( partition_type::type::SORTED_MULTI_COLUMN );
    partition_interface_test( p );
    delete p;
}


TEST_F( partition_test, row_partition_interface_test ) {
    partition* p = create_partition_ptr( partition_type::type::ROW );
    partition_interface_test( p );
    delete p;
}

TEST_F( partition_test, partition_dependency_test ) {
    uint64_t             p_id = 5;
    int32_t              num_records_in_chain = 2;
    partition_dependency p_dep;

    snapshot_vector gc_lwm;

    uint64_t base_version = 3;
    snapshot_vector base_snapshot;
    base_snapshot[p_id] = base_version;
    base_snapshot[1] = 7;

    p_dep.init( p_id, base_version, base_snapshot, num_records_in_chain );

    EXPECT_EQ( base_version, p_dep.get_version() );
    EXPECT_EQ( base_version, p_dep.wait_until( 2 ) );

    snapshot_vector snapshot_read;
    snapshot_read[p_id] = base_version;

    p_dep.get_dependency( snapshot_read );
    EXPECT_EQ( snapshot_read, base_snapshot );

    snapshot_vector snap_write_4 = snapshot_read;
    snap_write_4[p_id] = 4;
    snap_write_4[2] = 6;

    p_dep.store_dependency( snap_write_4, num_records_in_chain, gc_lwm );

    snapshot_read.clear();
    snapshot_read[p_id] = 3;
    p_dep.get_dependency( snapshot_read );
    EXPECT_EQ( snapshot_read, base_snapshot );

    snapshot_read.clear();
    snapshot_read[p_id] = 4;
    p_dep.get_dependency( snapshot_read );
    EXPECT_EQ( snapshot_read, snap_write_4 );

    snapshot_vector snap_write_5 = snap_write_4;
    snap_write_5[p_id] = 5;
    snap_write_5[2] = 7;
    snap_write_5[7] = 6;

    p_dep.store_dependency( snap_write_5, num_records_in_chain, gc_lwm );

    snapshot_read.clear();
    snapshot_read[p_id] = 5;
    snapshot_read[7] = 8;  // should not be updated
    p_dep.get_dependency( snapshot_read );

    EXPECT_EQ( 4, snapshot_read.size() );

    EXPECT_EQ( 7, get_snapshot_version( snapshot_read, 1 ) );
    EXPECT_EQ( 5, get_snapshot_version( snapshot_read, p_id ) );
    EXPECT_EQ( 7, get_snapshot_version( snapshot_read, 2 ) );
    EXPECT_EQ( 8, get_snapshot_version( snapshot_read, 7 ) );

    // should write a new version
    snapshot_vector snap_write_6 = snapshot_read;
    snap_write_6[p_id] = 6; // should be a small merge
    p_dep.store_dependency( snap_write_6, num_records_in_chain, gc_lwm );

    snapshot_read[p_id] = 6;
    p_dep.get_dependency( snapshot_read );
    EXPECT_EQ( snapshot_read, snap_write_6 );
}

