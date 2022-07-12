#include <chrono>
#include <cmath>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iostream>
#include <stdlib.h>

#include "../src/common/predictor/ensemble/ensemble_early_query_predictor_configs.h"
#include "../src/common/predictor/ensemble/ensemble_linear_predictor.h"

using ::testing::_;
using ::testing::Return;

class ensemble_linear_predictor_test_basic : public ::testing::Test {
   protected:
    ensemble_early_query_predictor_configs ensemble_configs_ =
        construct_ensemble_early_query_predictor_configs(
            10, /* slot size */
            2,  /* search window */
            8   /* training window */
            );

    const uint32_t LRU_CACHE_SIZE =
        ensemble_configs_.slot_ * ensemble_configs_.linear_training_interval_ *
        1.25;
    folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>
        query_observations_map_{LRU_CACHE_SIZE};

    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>>
        query_observations_{std::move( query_observations_map_ )};

    void SetUp() override {
        const uint32_t query_a = 123;
        auto           p = query_observations_.wlock();
        for( int x = 10; x <= 100; x++ ) {
            p->insert( x, std::make_shared<
                              std::unordered_map<epoch_time, query_count>>() );
            auto map = p->get( x );
            ( *map )[query_a] = x + 10;
        }
    }

   public:
    ensemble_linear_predictor_test_basic() {}
};

TEST_F( ensemble_linear_predictor_test_basic, basic_linear_regression ) {
    const int query_a = 123;

    ensemble_linear_predictor linear_predictor( query_observations_,
                                                ensemble_configs_, 120 );
    linear_predictor.train( query_a, 100 );
    EXPECT_EQ( linear_predictor.get_estimated_query_count( query_a, 50 ), 60 );

    // Training again flushes already learned data, and relearns based on the
    // window
    // computed by the train function in ensemble_linear_predictor.
    //
    // Hence, there is a new period where the shift function is x + 20, and not
    // x + 10, and that should be picked up by the model.

    {
        auto p = query_observations_.wlock();
        for( int x = 100; x <= 200; x++ ) {
            p->insert(
                x,
                std::make_shared<std::unordered_map<query_id, query_count>>() );

            auto map = p->get( x );
            ( *map )[query_a] = x + 20;
        }
    }

    linear_predictor.train( query_a, 200 );
    EXPECT_EQ( linear_predictor.get_estimated_query_count( query_a, 210 ),
               230 );
}

TEST_F( ensemble_linear_predictor_test_basic, basic_linear_regression_empty ) {
    const int query_b = 124;

    ensemble_linear_predictor linear_predictor( query_observations_,
                                                ensemble_configs_, 50 );
    linear_predictor.train( query_b, 20 );
    EXPECT_EQ( linear_predictor.get_estimated_query_count( query_b, 30 ), 0 );
}

TEST_F( ensemble_linear_predictor_test_basic, basic_linear_regression_large ) {
    const int query_b = 124;
    {
        auto query_data = query_observations_.wlock();
        for( int x = 0; x <= 1000; x += 10 ) {
            query_data->insert(
                x,
                std::make_shared<std::unordered_map<query_id, query_count>>() );

            auto map = query_data->get( x );

            // Apply simple noise function from [-1, 1] using sinx
            ( *map )[query_b] = ( x / 2 ) + 10 * sin( x );
        }
    }

    ensemble_configs_.linear_training_interval_ = 75;
    ensemble_linear_predictor linear_predictor( query_observations_,
                                                ensemble_configs_, 2000 );
    linear_predictor.train( query_b, 1000 );
    EXPECT_LT(
        abs( (int) linear_predictor.get_estimated_query_count( query_b, 1010 ) -
             505 ),
        20 );
}
