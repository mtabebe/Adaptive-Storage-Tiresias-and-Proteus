#include <chrono>
#include <cmath>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iostream>
#include <stdlib.h>

#include "../src/common/constants.h"
#include "../src/common/predictor/ensemble/ensemble_early_query_predictor_configs.h"
#include "../src/common/predictor/ensemble/ensemble_rnn_predictor.h"

using ::testing::_;
using ::testing::Return;

// Macro used to verify that the predicted rnn_predictor.get_estimated_query
// result is within err_range (where an assertion is established), or log_range
// (where a LOG(INFO) message is recorded). This handles flaky tests due to
// non-determinism from LibTorch.

#define check_rnn_prediction( predict_time, curr_time, query_id, expected, \
                              log_range, err_range )                       \
    do {                                                                   \
        int counts =                                                       \
            static_cast<int>( rnn_predictor.get_estimated_query_count(     \
                query_id, predict_time, curr_time ) );                     \
        EXPECT_LE( abs( counts - expected ), err_range );                  \
        if( abs( counts - expected ) > log_range ) {                       \
            LOG( INFO ) << "Unexpected count for prediction: expected "    \
                        << expected << " but found " << counts;            \
        }                                                                  \
    } while( 0 )

class ensemble_rnn_predictor_test_basic : public ::testing::Test {
   protected:
    ensemble_early_query_predictor_configs ensemble_configs_ =
        construct_ensemble_early_query_predictor_configs(
            2,    /* slot size */
            2,    /* search window */
            8,    /* linear training interval (not relevant) */
            120,  /* rnn training interval */
            0.01, /* krls learning rate (not relevant)  */
            0.01  /* rnn learning rate */
            );

    const size_t LRU_CACHE_SIZE = 1000;
    folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>
        query_observations_map_{LRU_CACHE_SIZE};

    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>>
        query_observations_{std::move( query_observations_map_ )};

    void SetUp() override {
        ensemble_configs_.rnn_layer_count_ = 5;
        ensemble_configs_.rnn_epoch_count_ = 100;
        const uint32_t query_a = 123;
        auto           p = query_observations_.wlock();

        for( int x = 10; x <= 300; x++ ) {
            p->insert(
                x,
                std::make_shared<std::unordered_map<query_id, query_count>>() );

            auto map = p->get( x );
            ( *map )[query_a] = 20 * abs( sin( 3.14 * x / 17.0 ) );
        }
    }

   public:
    ensemble_rnn_predictor_test_basic() {}
};

TEST_F( ensemble_rnn_predictor_test_basic, basic_rnn_pattern ) {

    if( k_skip_slow_tests ) {
        // DLOG( WARNING ) << "Skipping test";
        return;
    }

    const int query_a = 123;

    ensemble_rnn_predictor rnn_predictor( query_observations_,
                                          ensemble_configs_, 420 );
    rnn_predictor.train( query_a, 300 );

    // The historical amount is 3.76704, which rounds down to 3
    EXPECT_LE( rnn_predictor.get_estimated_query_count( query_a, 50, 100 ), 3 );

    // The expected prediction values for f(x) = 20|sin(3.14 * x / 17)| are:
    //
    //   x  |   y
    // ------------
    //  300 | 18.1467
    //  302 | 13.8866
    //  304 | 7.75298

    check_rnn_prediction( 301, 300, query_a, 18, 2, 5 );
    check_rnn_prediction( 303, 300, query_a, 13, 2, 5 );
    check_rnn_prediction( 304, 300, query_a, 7, 2, 5 );
    check_rnn_prediction( 305, 300, query_a, 7, 2, 5 );

    {
        auto p = query_observations_.wlock();
        for( int x = 300; x <= 600; x++ ) {
            p->insert(
                x,
                std::make_shared<std::unordered_map<query_id, query_count>>() );

            auto map = p->get( x );
            ( *map )[query_a] = abs( 20 * sin( x * 3.14 / 17 ) );
        }
    }

    rnn_predictor.train( query_a, 600 );

    // The expected prediction values for f(x) = 20|sin(3.14 * x / 17)| are:
    //
    //   x  |   y
    // ------------
    //  600 | 15.258
    //  602 | 18.8974
    //  604 | 19.9872
    //  606 | 18.3803
    //  608 | 14.2935
    //  610 | 8.27824
    //  612 | 1.14608

    check_rnn_prediction( 600, 600, query_a, 15, 1, 3 );
    check_rnn_prediction( 602, 600, query_a, 18, 1, 3 );
    check_rnn_prediction( 604, 600, query_a, 19, 1, 3 );
    check_rnn_prediction( 606, 600, query_a, 18, 2, 3 );
    check_rnn_prediction( 608, 600, query_a, 14, 2, 7 );
    check_rnn_prediction( 610, 600, query_a, 8, 3, 7 );
    check_rnn_prediction( 612, 600, query_a, 1, 3, 7 );
}

TEST_F( ensemble_rnn_predictor_test_basic, basic_rnn_regression_empty ) {
    const int query_b = 124;

    ensemble_rnn_predictor rnn_predictor( query_observations_,
                                          ensemble_configs_, 50 );
    rnn_predictor.train( query_b, 20 );
    EXPECT_EQ( rnn_predictor.get_estimated_query_count( query_b, 30 ), 0 );
}

TEST_F( ensemble_rnn_predictor_test_basic, basic_rnn_regression_large ) {

    if( k_skip_slow_tests ) {
        // DLOG( WARNING ) << "Skipping test";
        return;
    }

    const int query_b = 124;
    {
        auto query_data = query_observations_.wlock();
        for( int x = 0; x <= 1000; x++ ) {
            if( query_data->find( x ) == query_data->end() ) {
                query_data->insert(
                    x, std::make_shared<
                           std::unordered_map<query_id, query_count>>() );
            }

            // Apply simple noise function from [-1, 1] using sinx
            auto map = query_data->get( x );
            ( *map )[query_b] = abs( 30 * sin( x * 3.14 / 11 ) ) + sin( x );
        }
    }

    ensemble_configs_.rnn_training_interval_ = 200;
    ensemble_rnn_predictor rnn_predictor( query_observations_,
                                          ensemble_configs_, 2000 );
    rnn_predictor.train( query_b, 1000 );

    // The expected prediction values for f(x) = 30|sin(3.14 * x / 11| are:
    //
    //   x  |   y
    // ------------
    //  1000 | 13.3436
    //  1002 | 4.3691
    //  1004 | 18.6215
    //  1006 | 29.3973
    //  1008 | 29.2473
    //  1010 | 18.7238
    //  1014 | 13.023
    //  1020 | 26.1674

    check_rnn_prediction( 1000, 1000, query_b, 13, 2, 5 );
    check_rnn_prediction( 1002, 1000, query_b, 4, 2, 7 );
    check_rnn_prediction( 1004, 1000, query_b, 18, 2, 7 );
    check_rnn_prediction( 1006, 1000, query_b, 29, 2, 7 );
    check_rnn_prediction( 1008, 1000, query_b, 29, 3, 7 );
    check_rnn_prediction( 1010, 1000, query_b, 18, 3, 9 );
    check_rnn_prediction( 1014, 1000, query_b, 13, 3, 12 );
    check_rnn_prediction( 1020, 1000, query_b, 26, 3, 15 );
}
