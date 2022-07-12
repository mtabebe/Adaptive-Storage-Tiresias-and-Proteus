#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/common/predictor/simple_early_query_predictor.h"

class simple_early_query_predictor_test_basic : public ::testing::Test {
   protected:
    simple_early_query_predictor eqp;

    void SetUp() override {
        const int query_a = 1, query_b = 2, query_c = 3;
        eqp.add_observation( query_a, 30 );
        eqp.add_observation( query_a, 40 );
        eqp.add_query_schedule( query_b, 2 );
        eqp.add_observation( query_b, 34 );

        std::vector<std::pair<query_id, epoch_time>> observed_queries;
        observed_queries.emplace_back( query_c, 1000 );
        observed_queries.emplace_back( query_c, 2000 );
        eqp.add_observations( observed_queries );
    }
};

class simple_early_query_predictor_test_empty : public ::testing::Test {
   protected:
    simple_early_query_predictor eqp;
};

TEST_F( simple_early_query_predictor_test_empty, predicted_arrival_empty ) {
    epoch_time predicted_arrival = eqp.get_predicted_arrival( 123, 32.3 );
    EXPECT_EQ( predicted_arrival, std::numeric_limits<epoch_time>::max() );
}

TEST_F( simple_early_query_predictor_test_basic, basic_test ) {
    const int query_a = 1, query_b = 2, query_c = 3;

    const auto &resp_arrivals = eqp.get_predicted_queries( 0, 1000, 0.001 );
    EXPECT_EQ( resp_arrivals.size(), 2 );
    EXPECT_NE( resp_arrivals.find( query_a ), resp_arrivals.end() );
    EXPECT_NE( resp_arrivals.find( query_b ), resp_arrivals.end() );
    EXPECT_EQ( resp_arrivals.find( query_c ), resp_arrivals.end() );

    const auto &resp_arrival_a = eqp.get_predicted_arrival( query_a, 0.001 );
    EXPECT_EQ( resp_arrival_a, 50 );

    const auto &resp_arrival_b = eqp.get_predicted_arrival( query_b, 0.001 );
    EXPECT_EQ( resp_arrival_b, 36 );
}

TEST_F( simple_early_query_predictor_test_basic, get_queries_inclusive_range ) {
    const int query_a = 1, query_b = 2;
    auto      resp_arrivals = eqp.get_predicted_queries( 36, 50, 0.001 );

    EXPECT_EQ( resp_arrivals.size(), 2 );
    EXPECT_NE( resp_arrivals.find( query_a ), resp_arrivals.end() );
    EXPECT_NE( resp_arrivals.find( query_b ), resp_arrivals.end() );

    resp_arrivals = eqp.get_predicted_queries( 37, 49, 0.001 );
    EXPECT_EQ( resp_arrivals.size(), 0 );

    resp_arrivals = eqp.get_predicted_queries( 36, 49, 0.001 );
    EXPECT_EQ( resp_arrivals.size(), 1 );
    EXPECT_EQ( resp_arrivals.find( query_a ), resp_arrivals.end() );
    EXPECT_NE( resp_arrivals.find( query_b ), resp_arrivals.end() );

    resp_arrivals = eqp.get_predicted_queries( 37, 50, 0.001 );
    EXPECT_EQ( resp_arrivals.size(), 1 );
    EXPECT_NE( resp_arrivals.find( query_a ), resp_arrivals.end() );
    EXPECT_EQ( resp_arrivals.find( query_b ), resp_arrivals.end() );
}

TEST_F( simple_early_query_predictor_test_basic,
        get_queries_bulk_observations ) {
    const int query_c = 3;
    auto      resp_arrivals = eqp.get_predicted_queries( 3000, 3000, 0.001 );

    EXPECT_EQ( resp_arrivals.size(), 1 );
    EXPECT_NE( resp_arrivals.find( query_c ), resp_arrivals.end() );
}

TEST_F( simple_early_query_predictor_test_basic, get_queries_updating_range ) {
    const int query_a = 1, query_b = 2;
    eqp.add_observation( query_a, 50 );
    eqp.add_observation( query_a, 70 );

    auto resp_arrivals = eqp.get_predicted_queries( 60, 90, 0.001 );
    EXPECT_EQ( resp_arrivals.size(), 1 );
    EXPECT_NE( resp_arrivals.find( query_a ), resp_arrivals.end() );

    eqp.add_query_schedule( query_b, 10 );
    eqp.add_observation( query_b, 100 );

    resp_arrivals = eqp.get_predicted_queries( 100, 120, 0.001 );
    EXPECT_EQ( resp_arrivals.size(), 1 );
    EXPECT_NE( resp_arrivals.find( query_b ), resp_arrivals.end() );
}

TEST_F( simple_early_query_predictor_test_basic, get_queries_out_of_order ) {
    const int query_a = 1;
    eqp.add_observation( query_a, 47 );
    eqp.add_observation( query_a, 50 );
    eqp.add_observation( query_a, 49 );

    // Strangely, the correct answer is 50 because the delta is confirmed 1, and
    // last record was at 49
    auto resp_arrivals = eqp.get_predicted_queries( 50, 50, 0.001 );
    EXPECT_EQ( resp_arrivals.size(), 1 );
    EXPECT_NE( resp_arrivals.find( query_a ), resp_arrivals.end() );
}
