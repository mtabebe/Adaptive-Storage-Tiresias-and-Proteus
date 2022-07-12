#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/common/string_conversion.h"
#include "../src/data-site/db/predicate.h"

class predicate_test : public ::testing::Test {};

TEST_F( predicate_test, uint64_test ) {
    packed_cell_data pcd;
    cell_predicate   c_pred;
    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::UINT64;

    uint64_t data = 5;
    // true value (equal) ==> (less than or equal), (greater than or equal)
    uint64_t pred = 5;

    pcd.set_uint64_data( data );
    c_pred.data = uint64_to_string( pred );

    EXPECT_TRUE( evaluate_predicate_on_uint64_data(
        data, pred, predicate_type::type::EQUALITY ) );
    EXPECT_TRUE( evaluate_predicate_on_uint64_data(
        data, pred, predicate_type::type::LESS_THAN_OR_EQUAL ) );
    EXPECT_TRUE( evaluate_predicate_on_uint64_data(
        data, pred, predicate_type::type::GREATER_THAN_OR_EQUAL ) );


    EXPECT_FALSE( evaluate_predicate_on_uint64_data(
        data, pred, predicate_type::type::INEQUALITY ) );
    EXPECT_FALSE( evaluate_predicate_on_uint64_data(
        data, pred, predicate_type::type::LESS_THAN ) );
    EXPECT_FALSE( evaluate_predicate_on_uint64_data(
        data, pred, predicate_type::type::GREATER_THAN ) );

    c_pred.predicate = predicate_type::type::EQUALITY;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );

    c_pred.predicate = predicate_type::type::INEQUALITY;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );


    // true value (inequal) (less than)  ==> (less than or equal)
    pred = 10;
    c_pred.data = uint64_to_string( pred );
    EXPECT_TRUE( evaluate_predicate_on_uint64_data(
        data, pred, predicate_type::type::INEQUALITY ) );
    EXPECT_TRUE( evaluate_predicate_on_uint64_data(
        data, pred, predicate_type::type::LESS_THAN ) );
    EXPECT_TRUE( evaluate_predicate_on_uint64_data(
        data, pred, predicate_type::type::LESS_THAN_OR_EQUAL ) );

    EXPECT_FALSE( evaluate_predicate_on_uint64_data(
        data, pred, predicate_type::type::EQUALITY ) );
    EXPECT_FALSE( evaluate_predicate_on_uint64_data(
        data, pred, predicate_type::type::GREATER_THAN_OR_EQUAL ) );
    EXPECT_FALSE( evaluate_predicate_on_uint64_data(
        data, pred, predicate_type::type::GREATER_THAN ) );

    c_pred.predicate = predicate_type::type::INEQUALITY;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );

    c_pred.predicate = predicate_type::type::EQUALITY;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );

    // true value (inequal) (greater than) ==> ( greater_than_or_equal)
    pred = 1;
    c_pred.data = uint64_to_string( pred );
    EXPECT_TRUE( evaluate_predicate_on_uint64_data(
        data, pred, predicate_type::type::INEQUALITY ) );
    EXPECT_TRUE( evaluate_predicate_on_uint64_data(
        data, pred, predicate_type::type::GREATER_THAN ) );
    EXPECT_TRUE( evaluate_predicate_on_uint64_data(
        data, pred, predicate_type::type::GREATER_THAN_OR_EQUAL ) );

    EXPECT_FALSE( evaluate_predicate_on_uint64_data(
        data, pred, predicate_type::type::EQUALITY ) );
    EXPECT_FALSE( evaluate_predicate_on_uint64_data(
        data, pred, predicate_type::type::LESS_THAN_OR_EQUAL ) );
    EXPECT_FALSE( evaluate_predicate_on_uint64_data(
        data, pred, predicate_type::type::LESS_THAN ) );

    c_pred.predicate = predicate_type::type::INEQUALITY;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );

    c_pred.predicate = predicate_type::type::EQUALITY;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );

}

TEST_F( predicate_test, int64_test ) {
    packed_cell_data pcd;
    cell_predicate   c_pred;
    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::INT64;

    int64_t data = 5;
    // true value (equal) ==> (less than or equal), (greater than or equal)
    int64_t pred = 5;

    pcd.set_int64_data( data );
    c_pred.data = int64_to_string( pred );

    EXPECT_TRUE( evaluate_predicate_on_int64_data(
        data, pred, predicate_type::type::EQUALITY ) );
    EXPECT_TRUE( evaluate_predicate_on_int64_data(
        data, pred, predicate_type::type::LESS_THAN_OR_EQUAL ) );
    EXPECT_TRUE( evaluate_predicate_on_int64_data(
        data, pred, predicate_type::type::GREATER_THAN_OR_EQUAL ) );

    EXPECT_FALSE( evaluate_predicate_on_int64_data(
        data, pred, predicate_type::type::INEQUALITY ) );
    EXPECT_FALSE( evaluate_predicate_on_int64_data(
        data, pred, predicate_type::type::LESS_THAN ) );
    EXPECT_FALSE( evaluate_predicate_on_int64_data(
        data, pred, predicate_type::type::GREATER_THAN ) );

    c_pred.predicate = predicate_type::type::EQUALITY;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );

    c_pred.predicate = predicate_type::type::INEQUALITY;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );


    // true value (inequal) (less than)  ==> (less than or equal)
    pred = 10;
    c_pred.data = int64_to_string( pred );

    EXPECT_TRUE( evaluate_predicate_on_int64_data(
        data, pred, predicate_type::type::INEQUALITY ) );
    EXPECT_TRUE( evaluate_predicate_on_int64_data(
        data, pred, predicate_type::type::LESS_THAN ) );
    EXPECT_TRUE( evaluate_predicate_on_int64_data(
        data, pred, predicate_type::type::LESS_THAN_OR_EQUAL ) );

    EXPECT_FALSE( evaluate_predicate_on_int64_data(
        data, pred, predicate_type::type::EQUALITY ) );
    EXPECT_FALSE( evaluate_predicate_on_int64_data(
        data, pred, predicate_type::type::GREATER_THAN_OR_EQUAL ) );
    EXPECT_FALSE( evaluate_predicate_on_int64_data(
        data, pred, predicate_type::type::GREATER_THAN ) );

    c_pred.predicate = predicate_type::type::INEQUALITY;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );

    c_pred.predicate = predicate_type::type::EQUALITY;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );

    // true value (inequal) (greater than) ==> ( greater_than_or_equal)
    pred = -1;
    c_pred.data = int64_to_string( pred );

    EXPECT_TRUE( evaluate_predicate_on_int64_data(
        data, pred, predicate_type::type::INEQUALITY ) );
    EXPECT_TRUE( evaluate_predicate_on_int64_data(
        data, pred, predicate_type::type::GREATER_THAN ) );
    EXPECT_TRUE( evaluate_predicate_on_int64_data(
        data, pred, predicate_type::type::GREATER_THAN_OR_EQUAL ) );

    EXPECT_FALSE( evaluate_predicate_on_int64_data(
        data, pred, predicate_type::type::EQUALITY ) );
    EXPECT_FALSE( evaluate_predicate_on_int64_data(
        data, pred, predicate_type::type::LESS_THAN_OR_EQUAL ) );
    EXPECT_FALSE( evaluate_predicate_on_int64_data(
        data, pred, predicate_type::type::LESS_THAN ) );

    c_pred.predicate = predicate_type::type::INEQUALITY;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );

    c_pred.predicate = predicate_type::type::EQUALITY;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
}

TEST_F( predicate_test, double_test ) {
    packed_cell_data pcd;
    cell_predicate   c_pred;
    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::DOUBLE;

    double data = 5;
    // true value (equal) ==> (less than or equal), (greater than or equal)
    double pred = 5;

    char* array = (char*)&pred;
    double conv = *(double*) array;

    std::string array_str( array, sizeof( double ) );
    DVLOG(40) << "Array str:";
    for ( char c :  array_str) {
        uint32_t u = ( uint32_t )( (uint8_t) c );
        DVLOG( 40 ) << u << ", " << c;
    }
    DVLOG( 40 ) << "Array str done";

    double array_dbl = *(double*) array_str.data();

    EXPECT_DOUBLE_EQ( conv, pred );
    EXPECT_DOUBLE_EQ( array_dbl, pred );

    EXPECT_DOUBLE_EQ( pred, data );
    EXPECT_DOUBLE_EQ( pred, string_to_double( array_str ) );
    EXPECT_DOUBLE_EQ( pred, string_to_double( double_to_string( pred) ));


    pcd.set_double_data( data );
    c_pred.data = double_to_string( pred );
    EXPECT_DOUBLE_EQ( pred, string_to_double( c_pred.data ) );
    DVLOG( 40 ) << "C_pred data:" << c_pred.data
                << ",size:" << c_pred.data.size();

    EXPECT_TRUE( evaluate_predicate_on_double_data(
        data, pred, predicate_type::type::EQUALITY ) );
    EXPECT_TRUE( evaluate_predicate_on_double_data(
        data, pred, predicate_type::type::LESS_THAN_OR_EQUAL ) );
    EXPECT_TRUE( evaluate_predicate_on_double_data(
        data, pred, predicate_type::type::GREATER_THAN_OR_EQUAL ) );

    EXPECT_FALSE( evaluate_predicate_on_double_data(
        data, pred, predicate_type::type::INEQUALITY ) );
    EXPECT_FALSE( evaluate_predicate_on_double_data(
        data, pred, predicate_type::type::LESS_THAN ) );
    EXPECT_FALSE( evaluate_predicate_on_double_data(
        data, pred, predicate_type::type::GREATER_THAN ) );

    c_pred.predicate = predicate_type::type::EQUALITY;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );

    c_pred.predicate = predicate_type::type::INEQUALITY;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );

    // true value (inequal) (less than)  ==> (less than or equal)
    pred = 10;
    c_pred.data = double_to_string( pred );

    EXPECT_TRUE( evaluate_predicate_on_double_data(
        data, pred, predicate_type::type::INEQUALITY ) );
    EXPECT_TRUE( evaluate_predicate_on_double_data(
        data, pred, predicate_type::type::LESS_THAN ) );
    EXPECT_TRUE( evaluate_predicate_on_double_data(
        data, pred, predicate_type::type::LESS_THAN_OR_EQUAL ) );

    EXPECT_FALSE( evaluate_predicate_on_double_data(
        data, pred, predicate_type::type::EQUALITY ) );
    EXPECT_FALSE( evaluate_predicate_on_double_data(
        data, pred, predicate_type::type::GREATER_THAN_OR_EQUAL ) );
    EXPECT_FALSE( evaluate_predicate_on_double_data(
        data, pred, predicate_type::type::GREATER_THAN ) );

    c_pred.predicate = predicate_type::type::INEQUALITY;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );

    c_pred.predicate = predicate_type::type::EQUALITY;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );

    // true value (inequal) (greater than) ==> ( greater_than_or_equal)
    pred = -1;
    c_pred.data = double_to_string( pred );

    EXPECT_TRUE( evaluate_predicate_on_double_data(
        data, pred, predicate_type::type::INEQUALITY ) );
    EXPECT_TRUE( evaluate_predicate_on_double_data(
        data, pred, predicate_type::type::GREATER_THAN ) );
    EXPECT_TRUE( evaluate_predicate_on_double_data(
        data, pred, predicate_type::type::GREATER_THAN_OR_EQUAL ) );

    EXPECT_FALSE( evaluate_predicate_on_double_data(
        data, pred, predicate_type::type::EQUALITY ) );
    EXPECT_FALSE( evaluate_predicate_on_double_data(
        data, pred, predicate_type::type::LESS_THAN_OR_EQUAL ) );
    EXPECT_FALSE( evaluate_predicate_on_double_data(
        data, pred, predicate_type::type::LESS_THAN ) );

    c_pred.predicate = predicate_type::type::INEQUALITY;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );

    c_pred.predicate = predicate_type::type::EQUALITY;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
}

TEST_F( predicate_test, string_test ) {
    packed_cell_data pcd;
    cell_predicate   c_pred;
    c_pred.table_id = 0;
    c_pred.col_id = 0;
    c_pred.type = data_type::type::STRING;

    std::string data = "morphosys";
    // true value (equal) ==> (less than or equal), (greater than or equal)
    std::string pred = "morphosys";

    pcd.set_string_data( data );
    c_pred.data = pred;

    EXPECT_TRUE( evaluate_predicate_on_string_data(
        data, pred, predicate_type::type::EQUALITY ) );
    EXPECT_TRUE( evaluate_predicate_on_string_data(
        data, pred, predicate_type::type::LESS_THAN_OR_EQUAL ) );
    EXPECT_TRUE( evaluate_predicate_on_string_data(
        data, pred, predicate_type::type::GREATER_THAN_OR_EQUAL ) );

    EXPECT_FALSE( evaluate_predicate_on_string_data(
        data, pred, predicate_type::type::INEQUALITY ) );
    EXPECT_FALSE( evaluate_predicate_on_string_data(
        data, pred, predicate_type::type::LESS_THAN ) );
    EXPECT_FALSE( evaluate_predicate_on_string_data(
        data, pred, predicate_type::type::GREATER_THAN ) );

    c_pred.predicate = predicate_type::type::EQUALITY;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );

    c_pred.predicate = predicate_type::type::INEQUALITY;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );

    // true value (inequal) (less than)  ==> (less than or equal)
    pred = "proteus";
    c_pred.data = pred;

    EXPECT_TRUE( evaluate_predicate_on_string_data(
        data, pred, predicate_type::type::INEQUALITY ) );
    EXPECT_TRUE( evaluate_predicate_on_string_data(
        data, pred, predicate_type::type::LESS_THAN ) );
    EXPECT_TRUE( evaluate_predicate_on_string_data(
        data, pred, predicate_type::type::LESS_THAN_OR_EQUAL ) );

    EXPECT_FALSE( evaluate_predicate_on_string_data(
        data, pred, predicate_type::type::EQUALITY ) );
    EXPECT_FALSE( evaluate_predicate_on_string_data(
        data, pred, predicate_type::type::GREATER_THAN_OR_EQUAL ) );
    EXPECT_FALSE( evaluate_predicate_on_string_data(
        data, pred, predicate_type::type::GREATER_THAN ) );

    c_pred.predicate = predicate_type::type::INEQUALITY;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );

    c_pred.predicate = predicate_type::type::EQUALITY;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );

    // true value (inequal) (greater than) ==> ( greater_than_or_equal)
    pred = "dynamast";
    c_pred.data = pred;

    EXPECT_TRUE( evaluate_predicate_on_string_data(
        data, pred, predicate_type::type::INEQUALITY ) );
    EXPECT_TRUE( evaluate_predicate_on_string_data(
        data, pred, predicate_type::type::GREATER_THAN ) );
    EXPECT_TRUE( evaluate_predicate_on_string_data(
        data, pred, predicate_type::type::GREATER_THAN_OR_EQUAL ) );

    EXPECT_FALSE( evaluate_predicate_on_string_data(
        data, pred, predicate_type::type::EQUALITY ) );
    EXPECT_FALSE( evaluate_predicate_on_string_data(
        data, pred, predicate_type::type::LESS_THAN_OR_EQUAL ) );
    EXPECT_FALSE( evaluate_predicate_on_string_data(
        data, pred, predicate_type::type::LESS_THAN ) );

    c_pred.predicate = predicate_type::type::INEQUALITY;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    EXPECT_TRUE( evaluate_predicate_on_cell_data( pcd, c_pred ) );

    c_pred.predicate = predicate_type::type::EQUALITY;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    EXPECT_FALSE( evaluate_predicate_on_cell_data( pcd, c_pred ) );
}
