#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <vector>

#include "../src/common/unions.h"
#include "../src/data-site/site-manager/serialize.h"

#include "../src/data-site/site-manager/sproc_lookup_table.h"

class serialize_test : public ::testing::Test {};

TEST_F( serialize_test, serialize_int ) {
    char *       buff = NULL;
    unsigned int i = 5;

    struct arg_code ac;
    ac.array_flag = false;
    ac.code = UNSIGNED;

    std::vector<arg_code> arg_codes;
    arg_codes.push_back( ac );

    std::vector<void *> arg_ptrs;
    arg_ptrs.push_back( (void *) &i );

    EXPECT_EQ( serialize( arg_codes, arg_ptrs, &buff ), 8 );
    EXPECT_TRUE( buff != NULL );
    EXPECT_EQ( ntohl( *( (unsigned *) buff ) ), *(unsigned *) &ac );
    EXPECT_EQ( ntohl( *( (unsigned *) ( buff + sizeof( arg_code ) ) ) ), i );

    std::vector<void *>   deserialized_ptrs;
    std::vector<arg_code> deserialization_types;

    EXPECT_EQ( deserialize( buff, 8, deserialization_types, deserialized_ptrs ),
               true );
    EXPECT_EQ( deserialized_ptrs.size(), 1 );
    EXPECT_EQ( *(unsigned *) &( deserialization_types[0] ), *(unsigned *) &ac );
    EXPECT_EQ( *(unsigned *) deserialized_ptrs[0], 5 );
    free_args( deserialized_ptrs );
    free( buff );
}

TEST_F( serialize_test, serialize_float ) {
    char *buff = NULL;
    float f = 3.14;

    struct arg_code ac;
    ac.array_flag = false;
    ac.code = FLOAT;

    std::vector<arg_code> arg_codes;
    arg_codes.push_back( ac );

    std::vector<void *> arg_ptrs;
    arg_ptrs.push_back( (void *) &f );

    size_t len = serialize( arg_codes, arg_ptrs, &buff );
    EXPECT_EQ( len, 8 );
    EXPECT_TRUE( buff != NULL );

    EXPECT_EQ( ntohl( *( (unsigned *) buff ) ), *(unsigned *) &ac );
    _4_bytes float_bits;
    float_bits.as_uint =
        ntohl( *( (unsigned *) ( buff + sizeof( arg_code ) ) ) );
    ;
    EXPECT_EQ( float_bits.as_float, f );

    std::vector<void *>   deserialized_ptrs;
    std::vector<arg_code> deserialization_types;

    EXPECT_EQ(
        deserialize( buff, len, deserialization_types, deserialized_ptrs ),
        true );

    EXPECT_EQ( *(unsigned *) &( deserialization_types[0] ), *(unsigned *) &ac );
    EXPECT_EQ( *(float *) deserialized_ptrs[0], f );
    free_args( deserialized_ptrs );
    free( buff );
}

TEST_F( serialize_test, serialize_dbl ) {
    char * buff = NULL;
    double dbl = 3.14;

    struct arg_code ac;
    ac.array_flag = false;
    ac.code = DOUBLE;

    std::vector<arg_code> arg_codes;
    arg_codes.push_back( ac );

    std::vector<void *> arg_ptrs;
    arg_ptrs.push_back( (void *) &dbl );

    size_t len = serialize( arg_codes, arg_ptrs, &buff );
    EXPECT_EQ( len, 12 );
    EXPECT_TRUE( buff != NULL );

    _8_bytes dbl_bits;
    EXPECT_EQ( ntohl( *( (unsigned *) buff ) ), *(unsigned *) &ac );
    dbl_bits.as_uint64 =
        ntohll( *( (uint64_t *) ( buff + sizeof( arg_code ) ) ) );
    EXPECT_EQ( dbl_bits.as_double, dbl );

    std::vector<void *>   deserialized_ptrs;
    std::vector<arg_code> deserialization_types;

    EXPECT_EQ(
        deserialize( buff, len, deserialization_types, deserialized_ptrs ),
        true );
    EXPECT_EQ( *(unsigned *) &( deserialization_types[0] ), *(unsigned *) &ac );
    EXPECT_EQ( *(double *) deserialized_ptrs[0], dbl );

    free_args( deserialized_ptrs );
    free( buff );
}

TEST_F( serialize_test, serialize_bigint ) {
    char *  buff = NULL;
    int64_t bigint = 93424;

    struct arg_code ac;
    ac.array_flag = false;
    ac.code = BIGINT;

    std::vector<arg_code> arg_codes;
    arg_codes.push_back( ac );

    std::vector<void *> arg_ptrs;
    arg_ptrs.push_back( (void *) &bigint );

    size_t len = serialize( arg_codes, arg_ptrs, &buff );
    EXPECT_EQ( len, 12 );
    EXPECT_TRUE( buff != NULL );
    EXPECT_EQ( ntohl( *( (unsigned *) buff ) ), *(unsigned *) &ac );
    int64_t int_bits =
        ntohll( *( (uint64_t *) ( buff + sizeof( arg_code ) ) ) );
    EXPECT_EQ( *( (int64_t *) &int_bits ), bigint );

    std::vector<void *>   deserialized_ptrs;
    std::vector<arg_code> deserialization_types;

    EXPECT_EQ(
        deserialize( buff, len, deserialization_types, deserialized_ptrs ),
        true );
    EXPECT_EQ( *(unsigned *) &( deserialization_types[0] ), *(unsigned *) &ac );
    EXPECT_EQ( *(int64_t *) deserialized_ptrs[0], bigint );

    free_args( deserialized_ptrs );
    free( buff );
}

TEST_F( serialize_test, serialize_char ) {
    char *buff = NULL;
    char  c = 'c';

    struct arg_code ac;
    ac.array_flag = false;
    ac.code = CHAR;

    std::vector<arg_code> arg_codes;
    arg_codes.push_back( ac );

    std::vector<void *> arg_ptrs;
    arg_ptrs.push_back( (void *) &c );

    size_t len = serialize( arg_codes, arg_ptrs, &buff );
    EXPECT_EQ( len, 5 );
    EXPECT_TRUE( buff != NULL );

    EXPECT_EQ( ntohl( *( (unsigned *) buff ) ), *(unsigned *) &ac );
    char read = *( (char *) ( buff + sizeof( arg_code ) ) );
    EXPECT_EQ( read, c );

    std::vector<void *>   deserialized_ptrs;
    std::vector<arg_code> deserialization_types;

    EXPECT_EQ(
        deserialize( buff, len, deserialization_types, deserialized_ptrs ),
        true );
    EXPECT_EQ( *(unsigned *) &( deserialization_types[0] ), *(unsigned *) &ac );
    EXPECT_EQ( *(char *) deserialized_ptrs[0], c );

    free_args( deserialized_ptrs );
    free( buff );
}

TEST_F( serialize_test, serialize_bool ) {
    char *buff = NULL;
    bool  b = true;

    struct arg_code ac;
    ac.array_flag = false;
    ac.code = BOOL;

    std::vector<arg_code> arg_codes;
    arg_codes.push_back( ac );

    std::vector<void *> arg_ptrs;
    arg_ptrs.push_back( (void *) &b );

    size_t len = serialize( arg_codes, arg_ptrs, &buff );
    EXPECT_EQ( len, 5 );
    EXPECT_TRUE( buff != NULL );

    EXPECT_EQ( ntohl( *( (unsigned *) buff ) ), *(unsigned *) &ac );
    bool read = *( (bool *) ( buff + sizeof( arg_code ) ) );
    EXPECT_EQ( read, b );

    std::vector<void *>   deserialized_ptrs;
    std::vector<arg_code> deserialization_types;

    EXPECT_EQ(
        deserialize( buff, len, deserialization_types, deserialized_ptrs ),
        true );
    EXPECT_EQ( *(unsigned *) &( deserialization_types[0] ), *(unsigned *) &ac );
    EXPECT_EQ( *(bool *) deserialized_ptrs[0], b );

    free_args( deserialized_ptrs );
    free( buff );
}

TEST_F( serialize_test, serialize_char_array ) {
    char *      buff = NULL;
    std::string data = "test";

    struct arg_code ac;
    ac.array_flag = true;
    ac.array_length = 5;
    ac.code = CHAR;

    std::vector<arg_code> arg_codes;
    arg_codes.push_back( ac );

    std::vector<void *> arg_ptrs;
    arg_ptrs.push_back( (void *) data.c_str() );

    size_t len = serialize( arg_codes, arg_ptrs, &buff );
    EXPECT_EQ( len, 9 );
    EXPECT_TRUE( buff != NULL );
    EXPECT_EQ( ntohl( *( (unsigned *) buff ) ), *(unsigned *) &ac );
    EXPECT_EQ( strncmp( ( buff + sizeof( arg_code ) ), data.c_str(), 5 ), 0 );

    std::vector<void *>   deserialized_ptrs;
    std::vector<arg_code> deserialization_types;

    EXPECT_EQ(
        deserialize( buff, len, deserialization_types, deserialized_ptrs ),
        true );
    EXPECT_EQ( *(unsigned *) &( deserialization_types[0] ), *(unsigned *) &ac );
    EXPECT_EQ( strncmp( (char *) deserialized_ptrs[0], data.c_str(), 5 ), 0 );

    free_args( deserialized_ptrs );
    free( buff );
}

TEST_F( serialize_test, serialize_int_array ) {
    char *buff = NULL;
    int   a[3] = {1, 2, 3};

    struct arg_code ac;
    ac.array_flag = true;
    ac.array_length = 3;
    ac.code = UNSIGNED;

    std::vector<arg_code> arg_codes;
    arg_codes.push_back( ac );

    std::vector<void *> arg_ptrs;
    arg_ptrs.push_back( (void *) a );

    size_t len = serialize( arg_codes, arg_ptrs, &buff );
    EXPECT_EQ( len, 16 );
    EXPECT_TRUE( buff != NULL );
    EXPECT_EQ( ntohl( *( (unsigned *) buff ) ), *(unsigned *) &ac );

    for( unsigned i = 0; i < 3; i++ ) {
        int temp =
            ntohl( *( ( (unsigned *) ( buff + sizeof( arg_code ) ) ) + i ) );
        EXPECT_EQ( temp, a[i] );
    }

    std::vector<void *>   deserialized_ptrs;
    std::vector<arg_code> deserialization_types;

    EXPECT_EQ(
        deserialize( buff, len, deserialization_types, deserialized_ptrs ),
        true );
    EXPECT_EQ( *(unsigned *) &( deserialization_types[0] ), *(unsigned *) &ac );
    int *d_arr = (int *) deserialized_ptrs[0];
    for( unsigned i = 0; i < 3; i++ ) {
        EXPECT_EQ( d_arr[i], a[i] );
    }

    free_args( deserialized_ptrs );
    free( buff );
}

TEST_F( serialize_test, serialize_double_array ) {
    char * buff = NULL;
    double a[5] = {1.0, 2.2, 3.14, 4.13, -5};

    struct arg_code ac;
    ac.array_flag = true;
    ac.array_length = 5;
    ac.code = DOUBLE;

    std::vector<arg_code> arg_codes;
    arg_codes.push_back( ac );

    std::vector<void *> arg_ptrs;
    arg_ptrs.push_back( (void *) a );

    size_t len = serialize( arg_codes, arg_ptrs, &buff );
    EXPECT_EQ( len, 44 );
    EXPECT_TRUE( buff != NULL );

    EXPECT_EQ( ntohl( *( (unsigned *) buff ) ), *(unsigned *) &ac );
    for( unsigned i = 0; i < 5; i++ ) {
        _8_bytes temp;
        temp.as_uint64 =
            ntohll( *( ( (uint64_t *) ( buff + sizeof( arg_code ) ) ) + i ) );
        EXPECT_DOUBLE_EQ( temp.as_double, a[i] );
    }

    std::vector<void *>   deserialized_ptrs;
    std::vector<arg_code> deserialization_types;

    EXPECT_EQ(
        deserialize( buff, len, deserialization_types, deserialized_ptrs ),
        true );
    EXPECT_EQ( *(unsigned *) &( deserialization_types[0] ), *(unsigned *) &ac );
    double *d_arr = (double *) deserialized_ptrs[0];
    for( unsigned i = 0; i < 5; i++ ) {
        EXPECT_EQ( d_arr[i], a[i] );
    }

    free_args( deserialized_ptrs );
    free( buff );
}

TEST_F( serialize_test, serialize_multiple ) {
    char *buff = NULL;

    int a[2] = {1, 2};

    arg_code ac1;
    ac1.array_flag = true;
    ac1.array_length = 2;
    ac1.code = UNSIGNED;

    double   d = 3.14;
    arg_code ac2;
    ac2.array_flag = false;
    ac2.code = DOUBLE;

    std::vector<arg_code> arg_codes;
    arg_codes.push_back( ac1 );
    arg_codes.push_back( ac2 );

    std::vector<void *> arg_ptrs;
    arg_ptrs.push_back( (void *) a );
    arg_ptrs.push_back( (void *) &d );

    size_t len = serialize( arg_codes, arg_ptrs, &buff );
    EXPECT_EQ( len, 24 );
    EXPECT_TRUE( buff != NULL );

    EXPECT_EQ( ntohl( *( (unsigned *) buff ) ), *(unsigned *) &ac1 );
    for( unsigned i = 0; i < 2; i++ ) {
        int temp = ntohl( *( ( (int *) ( buff + sizeof( arg_code ) ) ) + i ) );
        EXPECT_EQ( temp, a[i] );
    }

    EXPECT_EQ( ntohl( *(unsigned *) ( buff + 12 ) ), *(unsigned *) &ac2 );

    _8_bytes dbl_bits;
    dbl_bits.as_uint64 = ntohll( *(uint64_t *) ( buff + 16 ) );
    EXPECT_DOUBLE_EQ( dbl_bits.as_double, d );

    std::vector<void *>   deserialized_ptrs;
    std::vector<arg_code> deserialization_types;

    EXPECT_EQ(
        deserialize( buff, len, deserialization_types, deserialized_ptrs ),
        true );
    EXPECT_EQ( *(unsigned *) &( deserialization_types[0] ),
               *(unsigned *) &ac1 );
    EXPECT_EQ( *(unsigned *) &( deserialization_types[1] ),
               *(unsigned *) &ac2 );
    int *d_arr = (int *) deserialized_ptrs[0];
    for( unsigned i = 0; i < 2; i++ ) {
        EXPECT_EQ( d_arr[i], a[i] );
    }
    double *d_d = (double *) deserialized_ptrs[1];
    EXPECT_EQ( *d_d, d );

    free_args( deserialized_ptrs );
    free( buff );
}

sproc_result test_func( transaction_partition_holder *holder, const clientid id,
                        const std::vector<cell_key_ranges> &write_ckrs,
                        const std::vector<cell_key_ranges> &read_ckrs,
                        std::vector<arg_code> &             codes,
                        std::vector<void *> &values, void *opaque_sproc_ptr ) {
    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;
    return res;
}

sproc_result test_func2( transaction_partition_holder *      holder,
                         const clientid                      id,
                         const std::vector<cell_key_ranges> &write_ckrs,
                         const std::vector<cell_key_ranges> &read_ckrs,
                         std::vector<arg_code> &             codes,
                         std::vector<void *> &values, void *opaque_sproc_ptr ) {
    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;
    return res;
}

TEST_F( serialize_test, lookup_function_simple ) {
    sproc_lookup_table sproc_tab( 3, 1 );

    std::vector<arg_code> arg_codes;
    std::string           name = "func";

    function_identifier id( name, arg_codes );
    EXPECT_EQ( sproc_tab.lookup_function( id ), nullptr );

    sproc_tab.register_function( id, test_func );

    EXPECT_EQ( sproc_tab.lookup_function( id ), test_func );
}

TEST_F( serialize_test, lookup_function_match_args ) {
    sproc_lookup_table sproc_tab( 3, 1 );

    std::vector<arg_code> arg_codes;
    arg_code              ac;
    ac.code = INTEGER;
    ac.array_flag = false;
    arg_codes.push_back( ac );
    std::string name = "func";

    std::vector<arg_code> no_args;

    function_identifier id( name, arg_codes );
    function_identifier bad_id( name, no_args );

    sproc_tab.register_function( id, test_func );

    EXPECT_EQ( sproc_tab.lookup_function( id ), test_func );
    EXPECT_EQ( sproc_tab.lookup_function( bad_id ), nullptr );
}

TEST_F( serialize_test, lookup_function_handle_multiple ) {
    sproc_lookup_table sproc_tab( 3, 1 );

    std::vector<arg_code> arg_codes;
    arg_code              ac;
    ac.code = INTEGER;
    ac.array_flag = false;
    arg_codes.push_back( ac );

    std::vector<arg_code> arg_codes2;
    arg_code              ac2;
    ac2.code = DOUBLE;
    ac2.array_flag = false;
    arg_codes2.push_back( ac2 );

    std::string name = "func";

    function_identifier id( name, arg_codes );
    function_identifier id2( name, arg_codes2 );

    sproc_tab.register_function( id, test_func );
    sproc_tab.register_function( id2, test_func2 );

    EXPECT_EQ( sproc_tab.lookup_function( id ), test_func );
    EXPECT_EQ( sproc_tab.lookup_function( id2 ), test_func2 );
}
