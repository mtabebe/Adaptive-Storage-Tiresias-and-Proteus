#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/common/packed_pointer.h"
#include "../src/common/string_utils.h"
#include "../src/data-site/db/packed_cell_data.h"

#include "../src/benchmark/tpcc/record-types/tpcc_record_types.h"

class cell_data_test : public ::testing::Test {};

TEST_F( cell_data_test, packed_pointer_test ) {
    int*     i_ptr_1 = new int( 100 );
    int*     i_ptr_2 = new int( 200 );
    uint64_t i = 300;

    int16_t f1 = 8;
    int16_t f2 = 24;

    void*    found_ptr;
    int16_t  found_flag_bits;
    uint64_t found_int;

    packed_pointer pp = 0;

    EXPECT_EQ( nullptr, packed_pointer_ops::get_pointer( pp ) );
    EXPECT_EQ( 0, packed_pointer_ops::get_flag_bits( pp ) );

    pp = packed_pointer_ops::set_packed_pointer_ptr( (void*) i_ptr_1, f1 );

    found_ptr = packed_pointer_ops::get_pointer( pp );
    found_flag_bits = packed_pointer_ops::get_flag_bits( pp );

    EXPECT_EQ( i_ptr_1, (int*) found_ptr );
    EXPECT_EQ( 100, *(int*) found_ptr );
    EXPECT_EQ( f1, found_flag_bits );

    pp = packed_pointer_ops::set_flag_bits( pp, f2 );
    found_ptr = packed_pointer_ops::get_pointer( pp );
    found_flag_bits = packed_pointer_ops::get_flag_bits( pp );

    EXPECT_EQ( i_ptr_1, (int*) found_ptr );
    EXPECT_EQ( 100, *(int*) found_ptr );
    EXPECT_EQ( f2, found_flag_bits );

    pp = packed_pointer_ops::set_packed_pointer_int( i, f1 );
    found_int = packed_pointer_ops::get_int( pp );
    found_flag_bits = packed_pointer_ops::get_flag_bits( pp );

    EXPECT_EQ( i, found_int );
    EXPECT_EQ( f1, found_flag_bits );

    delete i_ptr_1;
    delete i_ptr_2;
}

TEST_F( cell_data_test, packed_cell_data_test ) {
    packed_cell_data i_pc;
    EXPECT_FALSE( i_pc.is_present() );
    i_pc.set_uint64_data( 100 );
    EXPECT_TRUE( i_pc.is_present() );
    EXPECT_EQ( K_INT_RECORD_TYPE, i_pc.get_type() );
    EXPECT_EQ( 100, i_pc.get_uint64_data() );

    i_pc.set_as_deleted();
    EXPECT_FALSE( i_pc.is_present() );

    std::string   small_str = "abc";
    packed_cell_data ss_pc;
    EXPECT_FALSE( ss_pc.is_present() );
    ss_pc.set_string_data( small_str );
    EXPECT_TRUE( ss_pc.is_present() );
    EXPECT_EQ( K_STRING_RECORD_TYPE, ss_pc.get_type() );
    EXPECT_EQ( small_str, ss_pc.get_string_data() );

    ss_pc.set_as_deleted();
    EXPECT_FALSE( ss_pc.is_present() );

    std::string   med_str = "abcdefgh";
    packed_cell_data ms_pc;
    EXPECT_FALSE( ms_pc.is_present() );
    ms_pc.set_string_data( med_str );
    EXPECT_TRUE( ms_pc.is_present() );
    EXPECT_EQ( K_STRING_RECORD_TYPE, ms_pc.get_type() );
    EXPECT_EQ( med_str, ms_pc.get_string_data() );

    ms_pc.set_as_deleted();
    EXPECT_FALSE( ms_pc.is_present() );

    std::string   big_str = "foo bar baz lorem ipsum";
    packed_cell_data bs_pc;
    EXPECT_FALSE( bs_pc.is_present() );
    bs_pc.set_string_data( big_str );
    EXPECT_TRUE( bs_pc.is_present() );
    EXPECT_EQ( K_STRING_RECORD_TYPE, bs_pc.get_type() );
    EXPECT_EQ( big_str, bs_pc.get_string_data() );

    bs_pc.set_as_deleted();
    EXPECT_FALSE( bs_pc.is_present() );

    // test reuse
    std::string alt_str = "The quick, brown fox jumps over a lazy dog.";
    bs_pc.set_string_data( alt_str );
    EXPECT_TRUE( bs_pc.is_present() );
    EXPECT_EQ( K_STRING_RECORD_TYPE, bs_pc.get_type() );
    EXPECT_EQ( alt_str, bs_pc.get_string_data() );

    // test buffer
    packed_cell_data cs_pc;
    const char*   c_str_short = "foo";
    const char*   c_str_long = "horizondb";
    cs_pc.set_buffer_data( c_str_short, 3 );
    char* found_str = cs_pc.get_buffer_data();
    EXPECT_TRUE(
        c_strings_equal( 3, c_str_short, cs_pc.get_length(), found_str ) );
    cs_pc.set_as_deleted();
    EXPECT_FALSE( cs_pc.is_present() );

    cs_pc.set_buffer_data( c_str_long, 9 );
    found_str = cs_pc.get_buffer_data();
    EXPECT_TRUE(
        c_strings_equal( 9, c_str_long, cs_pc.get_length(), found_str ) );

    cs_pc.set_as_deleted();

    snapshot_vector* snap = new snapshot_vector();
    set_snapshot_version( *snap, 1, 17 );
    set_snapshot_version( *snap, 13, 11 );
    set_snapshot_version( *snap, 27, 15 );

    // map test
    packed_cell_data snap_pc;
    snap_pc.set_pointer_data( (void*) snap, K_SNAPSHOT_VECTOR_RECORD_TYPE );
    snapshot_vector* new_snap = (snapshot_vector*) snap_pc.get_pointer_data();
    EXPECT_EQ( new_snap, snap );

    EXPECT_EQ( 17, get_snapshot_version( *new_snap, 1 ) );
    EXPECT_EQ( 11, get_snapshot_version( *new_snap, 13 ) );
    EXPECT_EQ( 15, get_snapshot_version( *new_snap, 27 ) );

    packed_cell_data snap_deep_copy;
    snap_deep_copy.deep_copy( snap_pc );
    snapshot_vector* snap_copy =
        (snapshot_vector*) snap_deep_copy.get_pointer_data();
    EXPECT_NE( snap_copy, new_snap );
    EXPECT_EQ( 17, get_snapshot_version( *snap_copy, 1 ) );
    EXPECT_EQ( 11, get_snapshot_version( *snap_copy, 13 ) );
    EXPECT_EQ( 15, get_snapshot_version( *snap_copy, 27 ) );
}

class simple_item {
  public:
   int32_t i_id;
   int32_t i_im_id;
   float   i_price;
};

class nested_struct {
  public:
    int32_t s_id;
    float s_price;

    simple_item it;
};

TEST_F( cell_data_test, simple_struct_to_cell_data_test ) {
    packed_cell_data i_pc;
    EXPECT_FALSE( i_pc.is_present() );

    simple_item i;
    i.i_id = 5;
    i.i_im_id = 3;
    i.i_price = 7.3;

    i_pc.set_buffer_data( struct_to_buffer<simple_item>( &i ),
                          sizeof( simple_item ) );

    simple_item* i_from_buf = buffer_to_struct<simple_item>(
        i_pc.get_buffer_data(), i_pc.get_length() );

    EXPECT_EQ( i.i_id, i_from_buf->i_id );
    EXPECT_EQ( i.i_im_id, i_from_buf->i_im_id );
    EXPECT_DOUBLE_EQ( i.i_im_id, i_from_buf->i_im_id );
}

TEST_F( cell_data_test, nested_struct_to_cell_data_test ) {
    packed_cell_data o_pc;
    EXPECT_FALSE( o_pc.is_present() );

    nested_struct o;
    o.s_id = 5;
    o.s_price = 50.0;

    o.it.i_id = 5;
    o.it.i_im_id = 3;
    o.it.i_price = 7.3;

    o_pc.set_buffer_data( struct_to_buffer<nested_struct>( &o ),
                          sizeof( nested_struct ) );

    nested_struct* o_from_buf = buffer_to_struct<nested_struct>( o_pc.get_buffer_data(),
                                                         o_pc.get_length() );

    EXPECT_EQ( o.s_id, o_from_buf->s_id );
    EXPECT_DOUBLE_EQ( o.s_price, o_from_buf->s_price );
    EXPECT_EQ( o.it.i_id, o_from_buf->it.i_id );
    EXPECT_EQ( o.it.i_im_id, o_from_buf->it.i_im_id );
    EXPECT_DOUBLE_EQ( o.it.i_price, o_from_buf->it.i_price );
}

