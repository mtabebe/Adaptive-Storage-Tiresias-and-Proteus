#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/data-site/db/partition_metadata.h"
#include "../src/data-site/update-propagation/write_buffer_serialization.h"

class write_buffer_serialization_test : public ::testing::Test {};

void check_c_str( const char* expected_buffer, uint32_t expected_len,
                  const char* actual_buffer, uint32_t actual_len ) {
    EXPECT_EQ( expected_len, actual_len );
    for( uint32_t pos = 0; pos < expected_len; pos++ ) {
        EXPECT_EQ( expected_buffer[pos], actual_buffer[pos] );
    }
}

void set_row_record( row_record* r, const char* val, uint32_t len ) {

    r->init_num_columns( 1, false );

    packed_cell_data* packed_cells = r->get_row_data();
    EXPECT_NE( nullptr, packed_cells );

    packed_cell_data& pc = packed_cells[0];

    EXPECT_FALSE( pc.is_present() );
    pc.set_string_data( std::string( val, len ) );
}
void set_packed_cell_data( packed_cell_data* p, const char* val,
                           uint32_t len ) {
    EXPECT_FALSE( p->is_present() );
    p->set_string_data( std::string( val, len ) );
}


TEST_F( write_buffer_serialization_test, insert_row_test ) {
    uint32_t             order_table_id = 0;

    uint32_t col_start = 0;
    uint32_t col_end = 0;

    partition_column_identifier p_0_10 = create_partition_column_identifier(
        order_table_id, 0, 10, col_start, col_end );
    uint64_t p_0_10_id = partition_column_identifier_key_hasher{}( p_0_10 );
    partition_column_identifier p_11_20 = create_partition_column_identifier(
        order_table_id, 11, 20, col_start, col_end );

    snapshot_vector commit_vv = {};
    set_snapshot_version( commit_vv, p_0_10, 1 );
    set_snapshot_version( commit_vv, p_11_20, 1 );

    versioned_row_record_identifier vri_1;
    versioned_row_record_identifier vri_2;
    versioned_row_record_identifier vri_3;

    row_record r1;
    row_record r2;
    row_record r3;

    const char* r1_val = "foo";
    const char* r2_val = "abcdefgh";
    const char* r3_val = "horizondb";

    set_row_record( &r1, r1_val, 3 );
    set_row_record( &r2, r2_val, 8 );
    set_row_record( &r3, r3_val, 9 );

    write_buffer* insert_buf = new write_buffer();

    cell_identifier cid;
    cid.table_id_ = 0;
    cid.col_id_ = 0;

    cid.key_ = 1;
    vri_1.add_db_op( cid, &r1, K_INSERT_OP );
    insert_buf->add_to_record_buffer( std::move( vri_1 ) );
    cid.key_ = 2;
    vri_2.add_db_op( cid, &r2, K_INSERT_OP );
    insert_buf->add_to_record_buffer( std::move( vri_2 ) );
    cid.key_ = 3;
    vri_3.add_db_op( cid, &r3, K_INSERT_OP );
    insert_buf->add_to_record_buffer( std::move( vri_3 ) );

    partition_column_operation_identifier poi;
    poi.add_partition_op( p_0_10, 0, 0, K_INSERT_OP );
    insert_buf->add_to_partition_buffer( std::move( poi ) );

    insert_buf->store_propagation_information( commit_vv, p_0_10, p_0_10_id,
                                               1 );

    serialized_update update = serialize_write_buffer( insert_buf );
    EXPECT_GE( update.length_, get_serialized_update_min_length() );

    deserialized_update* deserialized_ptr = deserialize_update( update );
    EXPECT_NE( deserialized_ptr, nullptr );

    EXPECT_EQ( p_0_10, deserialized_ptr->pcid_ );
    EXPECT_EQ( 1, deserialized_ptr->is_new_partition_ );

    EXPECT_EQ( commit_vv, deserialized_ptr->commit_vv_ );

    EXPECT_EQ( 1, deserialized_ptr->partition_updates_.size() );
    const partition_column_operation_identifier poi_1 =
        deserialized_ptr->partition_updates_.at( 0 );
    EXPECT_EQ( p_0_10, poi_1.identifier_ );
    EXPECT_EQ( 0, poi_1.data_32_ );
    EXPECT_EQ( K_INSERT_OP, poi_1.op_code_ );

    EXPECT_EQ( 3, deserialized_ptr->cell_updates_.size() );
    const deserialized_cell_op dur_1 =
        deserialized_ptr->cell_updates_.at( 0 );
    const deserialized_cell_op dur_2 =
        deserialized_ptr->cell_updates_.at( 1 );
    const deserialized_cell_op dur_3 =
        deserialized_ptr->cell_updates_.at( 2 );

    EXPECT_EQ( K_INSERT_OP, dur_1.get_op() );
    EXPECT_EQ( K_INSERT_OP, dur_2.get_op() );
    EXPECT_EQ( K_INSERT_OP, dur_3.get_op() );

    EXPECT_EQ( 0, dur_1.cid_.table_id_ );
    EXPECT_EQ( 0, dur_2.cid_.table_id_ );
    EXPECT_EQ( 0, dur_3.cid_.table_id_ );
    EXPECT_EQ( 0, dur_1.cid_.col_id_ );
    EXPECT_EQ( 0, dur_2.cid_.col_id_ );
    EXPECT_EQ( 0, dur_3.cid_.col_id_ );
    EXPECT_EQ( 1, dur_1.cid_.key_ );
    EXPECT_EQ( 2, dur_2.cid_.key_ );
    EXPECT_EQ( 3, dur_3.cid_.key_ );

    check_c_str( r1_val, 3, dur_1.get_update_buffer(),
                 dur_1.get_update_buffer_length() );
    check_c_str( r2_val, 8, dur_2.get_update_buffer(),
                 dur_2.get_update_buffer_length() );
    check_c_str( r3_val, 9, dur_3.get_update_buffer(),
                 dur_3.get_update_buffer_length() );

    delete deserialized_ptr;
    delete insert_buf;

    free( update.buffer_ );
}

TEST_F( write_buffer_serialization_test, insert_col_test ) {
    uint32_t             order_table_id = 0;

    uint32_t col_start = 0;
    uint32_t col_end = 0;

    partition_column_identifier p_0_10 = create_partition_column_identifier(
        order_table_id, 0, 10, col_start, col_end );
    uint64_t p_0_10_id = partition_column_identifier_key_hasher{}( p_0_10 );
    partition_column_identifier p_11_20 = create_partition_column_identifier(
        order_table_id, 11, 20, col_start, col_end );

    snapshot_vector commit_vv = {};
    set_snapshot_version( commit_vv, p_0_10, 1 );
    set_snapshot_version( commit_vv, p_11_20, 1 );

    versioned_cell_data_identifier vri_1;
    versioned_cell_data_identifier vri_2;
    versioned_cell_data_identifier vri_3;

    packed_cell_data r1;
    packed_cell_data r2;
    packed_cell_data r3;

    const char* r1_val = "foo";
    const char* r2_val = "abcdefgh";
    const char* r3_val = "horizondb";

    set_packed_cell_data( &r1, r1_val, 3 );
    set_packed_cell_data( &r2, r2_val, 8 );
    set_packed_cell_data( &r3, r3_val, 9 );

    write_buffer* insert_buf = new write_buffer();

    cell_identifier cid;
    cid.table_id_ = 0;
    cid.col_id_ = 0;

    cid.key_ = 1;
    vri_1.add_db_op( cid, &r1, K_INSERT_OP );
    insert_buf->add_to_cell_buffer( std::move( vri_1 ) );
    cid.key_ = 2;
    vri_2.add_db_op( cid, &r2, K_INSERT_OP );
    insert_buf->add_to_cell_buffer( std::move( vri_2 ) );
    cid.key_ = 3;
    vri_3.add_db_op( cid, &r3, K_INSERT_OP );
    insert_buf->add_to_cell_buffer( std::move( vri_3 ) );

    partition_column_operation_identifier poi;
    poi.add_partition_op( p_0_10, 0, 0, K_INSERT_OP );
    insert_buf->add_to_partition_buffer( std::move( poi ) );

    insert_buf->store_propagation_information( commit_vv, p_0_10, p_0_10_id,
                                               1 );

    serialized_update update = serialize_write_buffer( insert_buf );
    EXPECT_GE( update.length_, get_serialized_update_min_length() );

    deserialized_update* deserialized_ptr = deserialize_update( update );
    EXPECT_NE( deserialized_ptr, nullptr );

    EXPECT_EQ( p_0_10, deserialized_ptr->pcid_ );
    EXPECT_EQ( 1, deserialized_ptr->is_new_partition_ );

    EXPECT_EQ( commit_vv, deserialized_ptr->commit_vv_ );

    EXPECT_EQ( 1, deserialized_ptr->partition_updates_.size() );
    const partition_column_operation_identifier poi_1 =
        deserialized_ptr->partition_updates_.at( 0 );
    EXPECT_EQ( p_0_10, poi_1.identifier_ );
    EXPECT_EQ( 0, poi_1.data_32_ );
    EXPECT_EQ( K_INSERT_OP, poi_1.op_code_ );

    EXPECT_EQ( 3, deserialized_ptr->cell_updates_.size() );
    const deserialized_cell_op dur_1 =
        deserialized_ptr->cell_updates_.at( 0 );
    const deserialized_cell_op dur_2 =
        deserialized_ptr->cell_updates_.at( 1 );
    const deserialized_cell_op dur_3 =
        deserialized_ptr->cell_updates_.at( 2 );

    EXPECT_EQ( K_INSERT_OP, dur_1.get_op() );
    EXPECT_EQ( K_INSERT_OP, dur_2.get_op() );
    EXPECT_EQ( K_INSERT_OP, dur_3.get_op() );

    EXPECT_EQ( 0, dur_1.cid_.table_id_ );
    EXPECT_EQ( 0, dur_2.cid_.table_id_ );
    EXPECT_EQ( 0, dur_3.cid_.table_id_ );
    EXPECT_EQ( 0, dur_1.cid_.col_id_ );
    EXPECT_EQ( 0, dur_2.cid_.col_id_ );
    EXPECT_EQ( 0, dur_3.cid_.col_id_ );
    EXPECT_EQ( 1, dur_1.cid_.key_ );
    EXPECT_EQ( 2, dur_2.cid_.key_ );
    EXPECT_EQ( 3, dur_3.cid_.key_ );

    check_c_str( r1_val, 3, dur_1.get_update_buffer(),
                 dur_1.get_update_buffer_length() );
    check_c_str( r2_val, 8, dur_2.get_update_buffer(),
                 dur_2.get_update_buffer_length() );
    check_c_str( r3_val, 9, dur_3.get_update_buffer(),
                 dur_3.get_update_buffer_length() );

    delete deserialized_ptr;
    delete insert_buf;

    free( update.buffer_ );
}

TEST_F( write_buffer_serialization_test, split_test ) {
    uint32_t             order_table_id = 0;

    uint32_t col_start = 0;
    uint32_t col_end = 0;

    partition_column_identifier p_0_10 = create_partition_column_identifier(
        order_table_id, 0, 10, col_start, col_end );
    uint64_t p_0_10_id = partition_column_identifier_key_hasher{}( p_0_10 );
    partition_column_identifier p_0_5 = create_partition_column_identifier(
        order_table_id, 0, 5, col_start, col_end );
    partition_column_identifier p_6_10 = create_partition_column_identifier(
        order_table_id, 6, 10, col_start, col_end );

    snapshot_vector commit_vv = {};
    set_snapshot_version( commit_vv, p_0_10, 7 );
    set_snapshot_version( commit_vv, p_0_5, 1 );
    set_snapshot_version( commit_vv, p_6_10, 1 );

    write_buffer* insert_buf = new write_buffer();

    partition_column_operation_identifier poi_0;
    poi_0.add_partition_op( p_0_10, 6, k_unassigned_col, K_SPLIT_OP );
    insert_buf->add_to_partition_buffer( std::move( poi_0 ) );

    insert_buf->store_propagation_information( commit_vv, p_0_10, p_0_10_id,
                                               7 );

    serialized_update update = serialize_write_buffer( insert_buf );
    EXPECT_GE( update.length_, get_serialized_update_min_length() );

    deserialized_update* deserialized_ptr = deserialize_update( update );
    EXPECT_NE( deserialized_ptr, nullptr );

    EXPECT_EQ( p_0_10, deserialized_ptr->pcid_ );
    EXPECT_EQ( 0, deserialized_ptr->is_new_partition_ );

    EXPECT_EQ( commit_vv, deserialized_ptr->commit_vv_ );

    EXPECT_EQ( 1, deserialized_ptr->partition_updates_.size() );
    const partition_column_operation_identifier poi_1 =
        deserialized_ptr->partition_updates_.at( 0 );
    EXPECT_EQ( p_0_10, poi_1.identifier_ );
    EXPECT_EQ( 6, poi_1.data_64_ );
    EXPECT_EQ( k_unassigned_col, poi_1.data_32_ );
    EXPECT_EQ( K_SPLIT_OP, poi_1.op_code_ );

    EXPECT_EQ( 0, deserialized_ptr->cell_updates_.size() );

    delete deserialized_ptr;
    delete insert_buf;

    free( update.buffer_ );
}

TEST_F( write_buffer_serialization_test, remaster_test ) {
    uint32_t             order_table_id = 0;

    uint32_t col_start = 0;
    uint32_t col_end = 0;

    partition_column_identifier p_0_10 = create_partition_column_identifier(
        order_table_id, 0, 10, col_start, col_end );
    uint64_t p_0_10_id = partition_column_identifier_key_hasher{}( p_0_10 );

    uint32_t new_site = 2;

    snapshot_vector commit_vv = {};
    set_snapshot_version( commit_vv, p_0_10, 7 );

    write_buffer* insert_buf = new write_buffer();

    partition_column_operation_identifier poi_0;
    poi_0.add_partition_op( p_0_10, 0 /*64*/, new_site /*32*/, K_REMASTER_OP );
    insert_buf->add_to_partition_buffer( std::move( poi_0 ) );

    insert_buf->store_propagation_information( commit_vv, p_0_10, p_0_10_id,
                                               7 );

    serialized_update update = serialize_write_buffer( insert_buf );
    EXPECT_GE( update.length_, get_serialized_update_min_length() );

    deserialized_update* deserialized_ptr = deserialize_update( update );
    EXPECT_NE( deserialized_ptr, nullptr );

    EXPECT_EQ( p_0_10, deserialized_ptr->pcid_ );
    EXPECT_EQ( 0, deserialized_ptr->is_new_partition_ );

    EXPECT_EQ( commit_vv, deserialized_ptr->commit_vv_ );

    EXPECT_EQ( 1, deserialized_ptr->partition_updates_.size() );
    const partition_column_operation_identifier poi_1 =
        deserialized_ptr->partition_updates_.at( 0 );
    EXPECT_EQ( p_0_10, poi_1.identifier_ );
    EXPECT_EQ( new_site, poi_1.data_32_ );
    EXPECT_EQ( 0, poi_1.data_64_ );
    EXPECT_EQ( K_REMASTER_OP, poi_1.op_code_ );

    delete deserialized_ptr;
    free( update.buffer_ );
    delete insert_buf;
}

TEST_F( write_buffer_serialization_test, update_test_row ) {
    uint32_t             order_table_id = 0;

    uint32_t col_start = 0;
    uint32_t col_end = 0;

    partition_column_identifier p_0_10 = create_partition_column_identifier(
        order_table_id, 0, 10, col_start, col_end );
    uint64_t p_0_10_id = partition_column_identifier_key_hasher{}( p_0_10 );
    partition_column_identifier p_11_20 = create_partition_column_identifier(
        order_table_id, 11, 20, col_start, col_end );

    snapshot_vector commit_vv = {};
    set_snapshot_version( commit_vv, p_0_10, 10 );
    set_snapshot_version( commit_vv, p_11_20, 1 );


    versioned_row_record_identifier vri_1;
    versioned_row_record_identifier vri_2;
    versioned_row_record_identifier vri_3;

    row_record r1;
    row_record r2;
    row_record r3;

    const char* r1_val = "bar";
    const char* r2_val = "hgfedcba";
    const char* r3_val = "dynamast++";

    set_row_record( &r1, r1_val, 3 );
    set_row_record( &r2, r2_val, 8 );
    set_row_record( &r3, r3_val, 10 );

    write_buffer* insert_buf = new write_buffer();

    cell_identifier cid;
    cid.table_id_ = 0;
    cid.col_id_ = 0;

    cid.key_ = 1;
    vri_1.add_db_op( cid, &r1, K_WRITE_OP );
    insert_buf->add_to_record_buffer( std::move( vri_1 ) );
    cid.key_ = 2;
    vri_2.add_db_op( cid, &r2, K_WRITE_OP );
    insert_buf->add_to_record_buffer( std::move( vri_2 ) );
    cid.key_ = 3;
    vri_3.add_db_op( cid, &r3, K_WRITE_OP );
    insert_buf->add_to_record_buffer( std::move( vri_3 ) );

    insert_buf->store_propagation_information( commit_vv, p_0_10, p_0_10_id,
                                               10 );

    serialized_update update = serialize_write_buffer( insert_buf );
    EXPECT_GE( update.length_, get_serialized_update_min_length() );

    deserialized_update* deserialized_ptr = deserialize_update( update );
    EXPECT_NE( deserialized_ptr, nullptr );

    EXPECT_EQ( p_0_10, deserialized_ptr->pcid_ );
    EXPECT_EQ( 0, deserialized_ptr->is_new_partition_ );

    EXPECT_EQ( commit_vv, deserialized_ptr->commit_vv_ );

    EXPECT_EQ( 0, deserialized_ptr->partition_updates_.size() );

    EXPECT_EQ( 3, deserialized_ptr->cell_updates_.size() );
    const deserialized_cell_op dur_1 =
        deserialized_ptr->cell_updates_.at( 0 );
    const deserialized_cell_op dur_2 =
        deserialized_ptr->cell_updates_.at( 1 );
    const deserialized_cell_op dur_3 =
        deserialized_ptr->cell_updates_.at( 2 );

    EXPECT_EQ( K_WRITE_OP, dur_1.get_op() );
    EXPECT_EQ( K_WRITE_OP, dur_2.get_op() );
    EXPECT_EQ( K_WRITE_OP, dur_3.get_op() );

    EXPECT_EQ( 0, dur_1.cid_.table_id_ );
    EXPECT_EQ( 0, dur_2.cid_.table_id_ );
    EXPECT_EQ( 0, dur_3.cid_.table_id_ );
    EXPECT_EQ( 0, dur_1.cid_.col_id_ );
    EXPECT_EQ( 0, dur_2.cid_.col_id_ );
    EXPECT_EQ( 0, dur_3.cid_.col_id_ );
    EXPECT_EQ( 1, dur_1.cid_.key_ );
    EXPECT_EQ( 2, dur_2.cid_.key_ );
    EXPECT_EQ( 3, dur_3.cid_.key_ );

    check_c_str( r1_val, 3, dur_1.get_update_buffer(),
                 dur_1.get_update_buffer_length() );
    check_c_str( r2_val, 8, dur_2.get_update_buffer(),
                 dur_2.get_update_buffer_length() );
    check_c_str( r3_val, 10, dur_3.get_update_buffer(),
                 dur_3.get_update_buffer_length() );

    delete deserialized_ptr;
    free( update.buffer_ );
    delete( insert_buf );
}
TEST_F( write_buffer_serialization_test, update_test_col ) {
    uint32_t             order_table_id = 0;

    uint32_t col_start = 0;
    uint32_t col_end = 0;

    partition_column_identifier p_0_10 = create_partition_column_identifier(
        order_table_id, 0, 10, col_start, col_end );
    uint64_t p_0_10_id = partition_column_identifier_key_hasher{}( p_0_10 );
    partition_column_identifier p_11_20 = create_partition_column_identifier(
        order_table_id, 11, 20, col_start, col_end );

    snapshot_vector commit_vv = {};
    set_snapshot_version( commit_vv, p_0_10, 10 );
    set_snapshot_version( commit_vv, p_11_20, 1 );


    versioned_cell_data_identifier vri_1;
    versioned_cell_data_identifier vri_2;
    versioned_cell_data_identifier vri_3;

    packed_cell_data r1;
    packed_cell_data r2;
    packed_cell_data r3;

    const char* r1_val = "bar";
    const char* r2_val = "hgfedcba";
    const char* r3_val = "dynamast++";

    set_packed_cell_data( &r1, r1_val, 3 );
    set_packed_cell_data( &r2, r2_val, 8 );
    set_packed_cell_data( &r3, r3_val, 10 );

    write_buffer* insert_buf = new write_buffer();

    cell_identifier cid;
    cid.table_id_ = 0;
    cid.col_id_ = 0;

    cid.key_ = 1;
    vri_1.add_db_op( cid, &r1, K_WRITE_OP );
    insert_buf->add_to_cell_buffer( std::move( vri_1 ) );
    cid.key_ = 2;
    vri_2.add_db_op( cid, &r2, K_WRITE_OP );
    insert_buf->add_to_cell_buffer( std::move( vri_2 ) );
    cid.key_ = 3;
    vri_3.add_db_op( cid, &r3, K_WRITE_OP );
    insert_buf->add_to_cell_buffer( std::move( vri_3 ) );

    insert_buf->store_propagation_information( commit_vv, p_0_10, p_0_10_id,
                                               10 );

    serialized_update update = serialize_write_buffer( insert_buf );
    EXPECT_GE( update.length_, get_serialized_update_min_length() );

    deserialized_update* deserialized_ptr = deserialize_update( update );
    EXPECT_NE( deserialized_ptr, nullptr );

    EXPECT_EQ( p_0_10, deserialized_ptr->pcid_ );
    EXPECT_EQ( 0, deserialized_ptr->is_new_partition_ );

    EXPECT_EQ( commit_vv, deserialized_ptr->commit_vv_ );

    EXPECT_EQ( 0, deserialized_ptr->partition_updates_.size() );

    EXPECT_EQ( 3, deserialized_ptr->cell_updates_.size() );
    const deserialized_cell_op dur_1 =
        deserialized_ptr->cell_updates_.at( 0 );
    const deserialized_cell_op dur_2 =
        deserialized_ptr->cell_updates_.at( 1 );
    const deserialized_cell_op dur_3 =
        deserialized_ptr->cell_updates_.at( 2 );

    EXPECT_EQ( K_WRITE_OP, dur_1.get_op() );
    EXPECT_EQ( K_WRITE_OP, dur_2.get_op() );
    EXPECT_EQ( K_WRITE_OP, dur_3.get_op() );

    EXPECT_EQ( 0, dur_1.cid_.table_id_ );
    EXPECT_EQ( 0, dur_2.cid_.table_id_ );
    EXPECT_EQ( 0, dur_3.cid_.table_id_ );
    EXPECT_EQ( 0, dur_1.cid_.col_id_ );
    EXPECT_EQ( 0, dur_2.cid_.col_id_ );
    EXPECT_EQ( 0, dur_3.cid_.col_id_ );
    EXPECT_EQ( 1, dur_1.cid_.key_ );
    EXPECT_EQ( 2, dur_2.cid_.key_ );
    EXPECT_EQ( 3, dur_3.cid_.key_ );

    check_c_str( r1_val, 3, dur_1.get_update_buffer(),
                 dur_1.get_update_buffer_length() );
    check_c_str( r2_val, 8, dur_2.get_update_buffer(),
                 dur_2.get_update_buffer_length() );
    check_c_str( r3_val, 10, dur_3.get_update_buffer(),
                 dur_3.get_update_buffer_length() );

    delete deserialized_ptr;
    free( update.buffer_ );
    delete( insert_buf );
}

TEST_F( write_buffer_serialization_test, delete_test_row ) {
    uint32_t order_table_id = 0;

    uint32_t col_start = 0;
    uint32_t col_end = 0;

    partition_column_identifier p_0_10 = create_partition_column_identifier(
        order_table_id, 0, 10, col_start, col_end );
    uint64_t p_0_10_id = partition_column_identifier_key_hasher{}( p_0_10 );
    partition_column_identifier p_11_20 = create_partition_column_identifier(
        order_table_id, 11, 20, col_start, col_end );

    snapshot_vector commit_vv = {};
    set_snapshot_version( commit_vv, p_0_10, 7 );
    set_snapshot_version( commit_vv, p_11_20, 1 );

    versioned_row_record_identifier vri_1;
    versioned_row_record_identifier vri_2;
    versioned_row_record_identifier vri_3;

    row_record r1;
    row_record r2;
    row_record r3;

    r2.set_as_deleted();
    r3.init_num_columns( 1, false );
    packed_cell_data* packed_cells = r3.get_row_data();
    EXPECT_NE( nullptr, packed_cells );

    write_buffer* insert_buf = new write_buffer();

    cell_identifier cid;
    cid.table_id_ = 0;
    cid.col_id_ = 0;

    cid.key_ = 1;
    vri_1.add_db_op( cid, &r1, K_INSERT_OP );  // should be a delete
    insert_buf->add_to_record_buffer( std::move( vri_1 ) );
    cid.key_ = 2;
    vri_2.add_db_op( cid, &r2, K_DELETE_OP );
    insert_buf->add_to_record_buffer( std::move( vri_2 ) );
    cid.key_ = 3;
    vri_3.add_db_op( cid, &r3, K_DELETE_OP );
    insert_buf->add_to_record_buffer( std::move( vri_3 ) );

    insert_buf->store_propagation_information( commit_vv, p_0_10, p_0_10_id,
                                               7 );

    serialized_update update = serialize_write_buffer( insert_buf );
    EXPECT_GE( update.length_, get_serialized_update_min_length() );

    deserialized_update* deserialized_ptr = deserialize_update( update );
    EXPECT_NE( deserialized_ptr, nullptr );

    EXPECT_EQ( p_0_10, deserialized_ptr->pcid_ );
    EXPECT_EQ( 0, deserialized_ptr->is_new_partition_ );

    EXPECT_EQ( commit_vv, deserialized_ptr->commit_vv_ );

    EXPECT_EQ( 0, deserialized_ptr->partition_updates_.size() );

    EXPECT_EQ( 3, deserialized_ptr->cell_updates_.size() );
    const deserialized_cell_op dur_1 = deserialized_ptr->cell_updates_.at( 0 );
    const deserialized_cell_op dur_2 = deserialized_ptr->cell_updates_.at( 1 );
    const deserialized_cell_op dur_3 = deserialized_ptr->cell_updates_.at( 2 );

    EXPECT_EQ( K_DELETE_OP, dur_1.get_op() );
    EXPECT_EQ( K_DELETE_OP, dur_2.get_op() );
    EXPECT_EQ( K_DELETE_OP, dur_3.get_op() );

    EXPECT_EQ( 0, dur_1.cid_.table_id_ );
    EXPECT_EQ( 0, dur_2.cid_.table_id_ );
    EXPECT_EQ( 0, dur_3.cid_.table_id_ );
    EXPECT_EQ( 0, dur_1.cid_.col_id_ );
    EXPECT_EQ( 0, dur_2.cid_.col_id_ );
    EXPECT_EQ( 0, dur_3.cid_.col_id_ );
    EXPECT_EQ( 1, dur_1.cid_.key_ );
    EXPECT_EQ( 2, dur_2.cid_.key_ );
    EXPECT_EQ( 3, dur_3.cid_.key_ );

    delete deserialized_ptr;
    free( update.buffer_ );
    delete insert_buf;
}

TEST_F( write_buffer_serialization_test, delete_test_col ) {
    uint32_t order_table_id = 0;

    uint32_t col_start = 0;
    uint32_t col_end = 0;

    partition_column_identifier p_0_10 = create_partition_column_identifier(
        order_table_id, 0, 10, col_start, col_end );
    uint64_t p_0_10_id = partition_column_identifier_key_hasher{}( p_0_10 );
    partition_column_identifier p_11_20 = create_partition_column_identifier(
        order_table_id, 11, 20, col_start, col_end );

    snapshot_vector commit_vv = {};
    set_snapshot_version( commit_vv, p_0_10, 7 );
    set_snapshot_version( commit_vv, p_11_20, 1 );

    versioned_cell_data_identifier vri_1;
    versioned_cell_data_identifier vri_2;
    versioned_cell_data_identifier vri_3;

    packed_cell_data r1;
    packed_cell_data r2;
    packed_cell_data r3;

    r2.set_as_deleted();

    write_buffer* insert_buf = new write_buffer();

    cell_identifier cid;
    cid.table_id_ = 0;
    cid.col_id_ = 0;

    cid.key_ = 1;
    vri_1.add_db_op( cid, &r1, K_INSERT_OP );  // should be a delete
    insert_buf->add_to_cell_buffer( std::move( vri_1 ) );
    cid.key_ = 2;
    vri_2.add_db_op( cid, &r2, K_DELETE_OP );
    insert_buf->add_to_cell_buffer( std::move( vri_2 ) );
    cid.key_ = 3;
    vri_3.add_db_op( cid, &r3, K_DELETE_OP );
    insert_buf->add_to_cell_buffer( std::move( vri_3 ) );

    insert_buf->store_propagation_information( commit_vv, p_0_10, p_0_10_id,
                                               7 );

    serialized_update update = serialize_write_buffer( insert_buf );
    EXPECT_GE( update.length_, get_serialized_update_min_length() );

    deserialized_update* deserialized_ptr = deserialize_update( update );
    EXPECT_NE( deserialized_ptr, nullptr );

    EXPECT_EQ( p_0_10, deserialized_ptr->pcid_ );
    EXPECT_EQ( 0, deserialized_ptr->is_new_partition_ );

    EXPECT_EQ( commit_vv, deserialized_ptr->commit_vv_ );

    EXPECT_EQ( 0, deserialized_ptr->partition_updates_.size() );

    EXPECT_EQ( 3, deserialized_ptr->cell_updates_.size() );
    const deserialized_cell_op dur_1 = deserialized_ptr->cell_updates_.at( 0 );
    const deserialized_cell_op dur_2 = deserialized_ptr->cell_updates_.at( 1 );
    const deserialized_cell_op dur_3 = deserialized_ptr->cell_updates_.at( 2 );

    EXPECT_EQ( K_DELETE_OP, dur_1.get_op() );
    EXPECT_EQ( K_DELETE_OP, dur_2.get_op() );
    EXPECT_EQ( K_DELETE_OP, dur_3.get_op() );

    EXPECT_EQ( 0, dur_1.cid_.table_id_ );
    EXPECT_EQ( 0, dur_2.cid_.table_id_ );
    EXPECT_EQ( 0, dur_3.cid_.table_id_ );
    EXPECT_EQ( 0, dur_1.cid_.col_id_ );
    EXPECT_EQ( 0, dur_2.cid_.col_id_ );
    EXPECT_EQ( 0, dur_3.cid_.col_id_ );
    EXPECT_EQ( 1, dur_1.cid_.key_ );
    EXPECT_EQ( 2, dur_2.cid_.key_ );
    EXPECT_EQ( 3, dur_3.cid_.key_ );

    delete deserialized_ptr;
    free( update.buffer_ );
    delete insert_buf;
}
