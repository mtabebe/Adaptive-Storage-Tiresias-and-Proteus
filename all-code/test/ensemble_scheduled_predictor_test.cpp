#include "folly/Synchronized.h"
#include "folly/container/EvictingCacheMap.h"
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <sstream>
#include <stdlib.h>

#include "../src/common/predictor/ensemble/ensemble_scheduled_predictor.h"
#include "../src/common/predictor/ensemble/gaussian_scheduled_distribution.h"

using ::testing::_;
using ::testing::Return;

class ensemble_scheduled_predictor_test : public ::testing::Test {
    folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>
        query_observations_map_{1 << 16};

    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>>
        query_observations_{std::move( query_observations_map_ )};

   public:
    ensemble_scheduled_predictor esp_;

    ensemble_scheduled_predictor_test() : esp_{query_observations_, 1 << 16} {
        std::stringstream ss;
        ss << "gaussian" << std::endl;
        ss << "100 0 50" << std::endl;
        ss << "100 0 0 1" << std::endl;

        std::istringstream ssi( ss.str() );
        auto               p = scheduled_distribution::get_distribution( ssi );
        esp_.add_query_distribution( p );
    }
};

TEST_F( ensemble_scheduled_predictor_test, basic_tests_initializer ) {
    const query_id query_a = 123;
    int testSum = 60;  // populate sum of predictors to calculate average

    EXPECT_EQ( esp_.get_estimated_query_count( query_a, 100 ), 100 );
    EXPECT_EQ( esp_.get_estimated_query_count( query_a, 101 ), testSum );
    EXPECT_EQ( esp_.get_estimated_query_count( query_a, 104 ), 0 );

    auto new_dist = std::make_shared<gaussian_scheduled_distribution>(
        100 /* base_time */, 200 /* period_time */, 50 /* time_window */,
        100 /* y_max */, 0 /* y_min */, 0 /* mean */, 10 /* std_deviation */
        );

    testSum += new_dist->get_estimated_query_count( 101 );

    // Two of the same distributions, but we take the average anyway
    esp_.add_query_distribution( new_dist );
    EXPECT_EQ( esp_.get_estimated_query_count( query_a, 100 ), 100 );
    EXPECT_EQ( esp_.get_estimated_query_count( query_a, 101 ), 79 );
    EXPECT_EQ( esp_.get_estimated_query_count( query_a, 104 ), 46 );

    // Create a new distribution that will be computed when factoring in
    new_dist = std::make_shared<gaussian_scheduled_distribution>(
        101 /* base_time */, 200 /* period_time */, 50 /* time_window */,
        200 /* y_max */, 0 /* y_min */, 0 /* mean */, 10 /* std_deviation */
        );
    testSum += new_dist->get_estimated_query_count( 101 );

    esp_.add_query_distribution( new_dist );
    EXPECT_EQ( esp_.get_estimated_query_count( query_a, 101 ),
               ( testSum ) / 3 );
}
