#ifdef krls_predictor_test
#include <chrono>
#include <cmath>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iostream>
#include <stdlib.h>

#include "../src/common/predictor/ensemble/ensemble_early_query_predictor_configs.h"
#include "../src/common/predictor/ensemble/ensemble_krls_predictor.h"

using ::testing::_;
using ::testing::Return;

class ensemble_krls_predictor_test_basic : public ::testing::Test {
   protected:
    ensemble_early_query_predictor_configs ensemble_configs_ =
        construct_ensemble_early_query_predictor_configs(
            10,    /* slot size */
            2,     /* search window */
            0,     /* training window size (linear) */
            0,     /* training window size (rnn) */
            500,   /* training window size (krls) */
            0.001, /* learning rate (rnn) */
            0.005  /* learning rate (krls) */
            );

    const uint32_t LRU_CACHE_SIZE_FACTOR = 1.25;
    folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>
        query_observations_map_{LRU_CACHE_SIZE_FACTOR};

    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>>
        query_observations_{std::move( query_observations_map_ )};

    void SetUp() override {
        const uint32_t query_a = 123;
        auto           p = query_observations_.wlock();
        p->insert(
            query_a,
            std::make_shared<std::unordered_map<query_id, query_count>>() );

        auto map = p->get( query_a );
        for( int x = 10; x <= 10000; x++ ) ( *map )[x] = x + 10;
    }

   public:
    ensemble_krls_predictor_test_basic() {}
};

TEST_F( ensemble_krls_predictor_test_basic, basic_krls_regression ) {
    const int query_a = 123;

    ensemble_krls_predictor krls_predictor( query_observations_,
                                            ensemble_configs_, 20200 );
    krls_predictor.train( query_a, 5000 );
    krls_predictor.train( query_a, 10000 );

    double mse = 0;
    for( int x = 2000; x < 10010; x += 10 ) {
        mse +=
            abs( (int) krls_predictor.get_estimated_query_count( query_a, x ) -
                 ( x + 10 ) );
    }
    EXPECT_LT( ( mse / ( 10010 - 2000 ) / 10 ), 50 );

    // Training again flushes already learned data, and relearns based on the
    // window
    // computed by the train function in ensemble_krls_predictor.
    //
    // Hence, there is a new period where the shift function is x + 20, and not
    // x + 10, and that should be picked up by the model.

    {
        auto p = query_observations_.wlock();
        auto map = p->get( query_a );
        for( int x = 10000; x <= 20000; x++ ) ( *map )[x] = x + 20;
    }

    krls_predictor.train( query_a, 20000 );
    EXPECT_LT(
        abs( (int) krls_predictor.get_estimated_query_count( query_a, 20010 ) -
             20030 ),
        50 );
}

// TODO: tweak hyper-parameters to converge better
TEST_F( ensemble_krls_predictor_test_basic, basic_krls_regression_empty ) {
    const int query_b = 124;

    ensemble_krls_predictor krls_predictor( query_observations_,
                                            ensemble_configs_, 50 );
    krls_predictor.train( query_b, 20 );
    EXPECT_EQ( krls_predictor.get_estimated_query_count( query_b, 30 ), 0 );
}

TEST_F( ensemble_krls_predictor_test_basic, basic_krls_regression_large ) {
    const int query_b = 124;
    {
        auto query_data = query_observations_.wlock();
        query_data->insert(
            query_b,
            std::make_shared<std::unordered_map<query_id, query_count>>() );
        auto map = query_data->get( query_b );

        // Apply simple noise function from [-1, 1] using sinx
        for( int x = 0; x <= 5000; x += 10 ) {
            ( *map )[x] = ( x / 2 ) + 10 * sin( x );
        }
    }

    ensemble_configs_.krls_training_interval_ = 75;
    ensemble_krls_predictor krls_predictor( query_observations_,
                                            ensemble_configs_, 6000 );
    krls_predictor.train( query_b, 5000 );
    EXPECT_LT(
        abs( (int) krls_predictor.get_estimated_query_count( query_b, 5010 ) -
             2500 ),
        30 );
}
#endif
