#include <chrono>
#include <cmath>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdlib.h>

#include "../src/common/predictor/ensemble/ensemble_early_query_predictor.h"
#include "../src/common/predictor/ensemble/ensemble_early_query_predictor_configs.h"
#include "ensemble_linear_predictor_mock.h"
#include "ensemble_rnn_predictor_mock.h"
#include "ensemble_scheduled_predictor_mock.h"

using ::testing::_;
using ::testing::Return;

class ensemble_early_query_predictor_test_empty : public ::testing::Test {
   protected:
    const uint32_t                         LRU_CACHE_MAX_SIZE = 100;
    ensemble_early_query_predictor_configs ensemble_configs_ =
        construct_ensemble_early_query_predictor_configs(
            10,   /* slot */
            2,    /* search interval */
            3,    /* linear training interval */
            15,   /* rnn training interval */
            4,    /* scheduled training interval */
            0.01, /* rnn learning rate */
            0.01, /* krls learning rate */
            1.5   /* scheduled outlier threshold */
            );

    folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>
        query_observation_map_{LRU_CACHE_MAX_SIZE};

    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>>
        query_observations_{std::move( query_observation_map_ )};

    ensemble_early_query_predictor ensemble_early_query_predictor_;
    std::shared_ptr<ensemble_linear_predictor_mock>
        ensemble_linear_predictor_mock_;
    std::shared_ptr<ensemble_scheduled_predictor_mock>
                                                 ensemble_scheduled_predictor_mock_;
    std::shared_ptr<ensemble_rnn_predictor_mock> ensemble_rnn_predictor_mock_;

   public:
    ensemble_early_query_predictor_test_empty()
        : ensemble_early_query_predictor_( ensemble_configs_ ),
          ensemble_linear_predictor_mock_(
              std::make_shared<ensemble_linear_predictor_mock>(
                  query_observations_, ensemble_configs_ ) ),
          ensemble_scheduled_predictor_mock_(
              std::make_shared<ensemble_scheduled_predictor_mock>(
                  query_observations_, ensemble_configs_ ) ),
          ensemble_rnn_predictor_mock_(
              std::make_shared<ensemble_rnn_predictor_mock>(
                  query_observations_, ensemble_configs_ ) ) {}
};

TEST_F( ensemble_early_query_predictor_test_empty, predicted_arrival_empty ) {
    const int query_a = 123;
    ensemble_early_query_predictor_.add_linear_query_predictor(
        query_a, ensemble_linear_predictor_mock_ );
    ensemble_early_query_predictor_.add_scheduled_query_predictor(
        query_a, ensemble_scheduled_predictor_mock_ );

    EXPECT_CALL( *ensemble_linear_predictor_mock_,
                 get_estimated_query_count( _, _ ) )
        .Times( 0 );
    EXPECT_CALL( *ensemble_scheduled_predictor_mock_,
                 get_estimated_query_count( _, _ ) )
        .Times( 0 );
    EXPECT_CALL( *ensemble_linear_predictor_mock_,
                 get_estimated_query_count( query_a, 0 ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_linear_predictor_mock_,
                 get_estimated_query_count( query_a, 10 ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_linear_predictor_mock_,
                 get_estimated_query_count( query_a, 20 ) )
        .Times( 1 );

    EXPECT_CALL( *ensemble_scheduled_predictor_mock_,
                 get_estimated_query_count( _, _ ) )
        .Times( 0 );
    EXPECT_CALL( *ensemble_scheduled_predictor_mock_,
                 get_estimated_query_count( query_a, 0 ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_scheduled_predictor_mock_,
                 get_estimated_query_count( query_a, 10 ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_scheduled_predictor_mock_,
                 get_estimated_query_count( query_a, 20 ) )
        .Times( 1 );

    // Query schedules are not supported for the ensemble model, and should do
    // nothing in this case
    ensemble_early_query_predictor_.add_query_schedule( query_a, 2 );

    epoch_time predicted_arrival =
        ensemble_early_query_predictor_.get_predicted_arrival( query_a, 1, 1 );
    EXPECT_EQ( predicted_arrival, std::numeric_limits<epoch_time>::max() );
}

TEST_F( ensemble_early_query_predictor_test_empty,
        predicted_arrival_training_no_query ) {
    /**
     * If there are no queries observed, then there should be no invocations to
     * the train function
     */

    EXPECT_CALL( *ensemble_linear_predictor_mock_, train( _, _ ) ).Times( 0 );
    EXPECT_CALL( *ensemble_scheduled_predictor_mock_, train( _, _ ) )
        .Times( 0 );
    ensemble_early_query_predictor_.train();
}

TEST_F( ensemble_early_query_predictor_test_empty, training_update ) {
    const int32_t query_a = 1, query_b = 2;

    ensemble_early_query_predictor_.add_linear_query_predictor(
        query_a, ensemble_linear_predictor_mock_ );
    ensemble_early_query_predictor_.add_scheduled_query_predictor(
        query_a, ensemble_scheduled_predictor_mock_ );
    ensemble_early_query_predictor_.add_rnn_query_predictor(
        query_a, ensemble_rnn_predictor_mock_ );

    // Irregular use, but leverage the same predictor for two query ids
    ensemble_early_query_predictor_.add_linear_query_predictor(
        query_b, ensemble_linear_predictor_mock_ );
    ensemble_early_query_predictor_.add_scheduled_query_predictor(
        query_b, ensemble_scheduled_predictor_mock_ );
    ensemble_early_query_predictor_.add_rnn_query_predictor(
        query_b, ensemble_rnn_predictor_mock_ );

    EXPECT_CALL( *ensemble_linear_predictor_mock_, add_observation( _, _ ) )
        .Times( 0 );
    EXPECT_CALL( *ensemble_scheduled_predictor_mock_, add_observation( _, _ ) )
        .Times( 0 );

    // These should capture the query_b hits (going from 60 to 40)
    EXPECT_CALL( *ensemble_linear_predictor_mock_, train( query_b, 60 ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_rnn_predictor_mock_, train( query_b, 60 ) )
        .Times( 1 );

    // These should capture the query_a hits (going from 100 to 80)
    EXPECT_CALL( *ensemble_linear_predictor_mock_, train( query_a, 100 ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_rnn_predictor_mock_, train( query_a, 100 ) )
        .Times( 1 );

    ensemble_early_query_predictor_.add_observation( query_a, 10 );
    ensemble_early_query_predictor_.add_observation( query_a, 11 );
    ensemble_early_query_predictor_.train( 100 );

    ensemble_early_query_predictor_.add_observation( query_b, 11 );
    ensemble_early_query_predictor_.train( 60 );
}

TEST_F( ensemble_early_query_predictor_test_empty, predicted_queries_basic ) {
    const int32_t query_a = 1, query_b = 2;

    //  We consider a begin_time of 3, end_time of 49 and threshold of 10.
    //  Since the granularity is 10, we need an inclusive range of
    //  [10, 40] with correct rounding. Hence, we should have for each observed
    //  query id four invocations at the rounded values.

    ensemble_early_query_predictor_.add_linear_query_predictor(
        query_a, ensemble_linear_predictor_mock_ );
    ensemble_early_query_predictor_.add_scheduled_query_predictor(
        query_a, ensemble_scheduled_predictor_mock_ );
    ensemble_early_query_predictor_.add_rnn_query_predictor(
        query_a, ensemble_rnn_predictor_mock_ );

    // Irregular use, but leverage the same predictor for two query ids
    ensemble_early_query_predictor_.add_linear_query_predictor(
        query_b, ensemble_linear_predictor_mock_ );
    ensemble_early_query_predictor_.add_scheduled_query_predictor(
        query_b, ensemble_scheduled_predictor_mock_ );
    ensemble_early_query_predictor_.add_rnn_query_predictor(
        query_b, ensemble_rnn_predictor_mock_ );

    // Expect to train both query predictors
    EXPECT_CALL( *ensemble_linear_predictor_mock_, train( query_a, 100 ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_linear_predictor_mock_, train( query_b, 100 ) )
        .Times( 1 );

    EXPECT_CALL( *ensemble_rnn_predictor_mock_, train( query_a, 100 ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_rnn_predictor_mock_, train( query_b, 100 ) )
        .Times( 1 );

    EXPECT_CALL( *ensemble_scheduled_predictor_mock_, train( _, _ ) )
        .Times( 0 );

    EXPECT_CALL( *ensemble_linear_predictor_mock_, add_observation( _, _ ) )
        .Times( 0 );

    EXPECT_CALL( *ensemble_linear_predictor_mock_,
                 get_estimated_query_count( _, _ ) )
        .Times( 0 );

    EXPECT_CALL( *ensemble_rnn_predictor_mock_, add_observation( _, _ ) )
        .Times( 0 );
    EXPECT_CALL( *ensemble_rnn_predictor_mock_,
                 get_estimated_query_count( _, _ ) )
        .Times( 0 );

    // Linear predictor
    EXPECT_CALL( *ensemble_linear_predictor_mock_,
                 get_estimated_query_count( query_a, 10 ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_linear_predictor_mock_,
                 get_estimated_query_count( query_a, 20 ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_linear_predictor_mock_,
                 get_estimated_query_count( query_a, 30 ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_linear_predictor_mock_,
                 get_estimated_query_count( query_a, 40 ) )
        .Times( 1 );

    EXPECT_CALL( *ensemble_linear_predictor_mock_,
                 get_estimated_query_count( query_b, 10 ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_linear_predictor_mock_,
                 get_estimated_query_count( query_b, 20 ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_linear_predictor_mock_,
                 get_estimated_query_count( query_b, 30 ) )
        .Times( 1 );

    EXPECT_CALL( *ensemble_linear_predictor_mock_,
                 get_estimated_query_count( query_b, 40 ) )
        .Times( 1 );

    // rnn predictor
    EXPECT_CALL( *ensemble_rnn_predictor_mock_,
                 get_estimated_query_count( query_a, 10, _ ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_rnn_predictor_mock_,
                 get_estimated_query_count( query_a, 20, _ ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_rnn_predictor_mock_,
                 get_estimated_query_count( query_a, 30, _ ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_rnn_predictor_mock_,
                 get_estimated_query_count( query_a, 40, _ ) )
        .Times( 1 );

    EXPECT_CALL( *ensemble_rnn_predictor_mock_,
                 get_estimated_query_count( query_b, 10, _ ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_rnn_predictor_mock_,
                 get_estimated_query_count( query_b, 20, _ ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_rnn_predictor_mock_,
                 get_estimated_query_count( query_b, 30, _ ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_rnn_predictor_mock_,
                 get_estimated_query_count( query_b, 40, _ ) )
        .Times( 1 );

    // scheduled predictor
    EXPECT_CALL( *ensemble_scheduled_predictor_mock_,
                 get_estimated_query_count( query_a, 10 ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_scheduled_predictor_mock_,
                 get_estimated_query_count( query_a, 20 ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_scheduled_predictor_mock_,
                 get_estimated_query_count( query_a, 30 ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_scheduled_predictor_mock_,
                 get_estimated_query_count( query_a, 40 ) )
        .Times( 1 );

    EXPECT_CALL( *ensemble_scheduled_predictor_mock_,
                 get_estimated_query_count( query_b, 10 ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_scheduled_predictor_mock_,
                 get_estimated_query_count( query_b, 20 ) )
        .Times( 1 );
    EXPECT_CALL( *ensemble_scheduled_predictor_mock_,
                 get_estimated_query_count( query_b, 30 ) )
        .Times( 1 );

    EXPECT_CALL( *ensemble_scheduled_predictor_mock_,
                 get_estimated_query_count( query_b, 40 ) )
        .Times( 1 );

    ensemble_early_query_predictor_.add_observation( query_a, 10 );
    ensemble_early_query_predictor_.add_observation( query_a, 11 );
    ensemble_early_query_predictor_.add_observation( query_b, 12 );

    // Training should have no effect on the query ids verified
    ensemble_early_query_predictor_.train( 100 );

    ensemble_early_query_predictor_.add_observation( query_a, 12 );
    const auto resp =
        ensemble_early_query_predictor_.get_predicted_queries( 3, 49, 10 );
    EXPECT_EQ( resp.size(), 0 );
}

TEST_F( ensemble_early_query_predictor_test_empty,
        estimated_query_count_calculation ) {
    const int32_t query_a = 1, query_b = 2;

    ensemble_early_query_predictor_.add_linear_query_predictor(
        query_a, ensemble_linear_predictor_mock_ );
    ensemble_early_query_predictor_.add_scheduled_query_predictor(
        query_a, ensemble_scheduled_predictor_mock_ );
    ensemble_early_query_predictor_.add_rnn_query_predictor(
        query_a, ensemble_rnn_predictor_mock_ );

    ensemble_early_query_predictor_.add_linear_query_predictor(
        query_b, ensemble_linear_predictor_mock_ );
    ensemble_early_query_predictor_.add_scheduled_query_predictor(
        query_b, ensemble_scheduled_predictor_mock_ );
    ensemble_early_query_predictor_.add_rnn_query_predictor(
        query_b, ensemble_rnn_predictor_mock_ );

    ensemble_early_query_predictor_.add_observation( query_a, 10 );
    ensemble_early_query_predictor_.add_observation( query_b, 11 );

    EXPECT_CALL( *ensemble_scheduled_predictor_mock_,
                 get_estimated_query_count( _, _ ) )
        .Times( 32 );

    EXPECT_CALL( *ensemble_rnn_predictor_mock_,
                 get_estimated_query_count( _, _, _ ) )
        .Times( 32 );

    EXPECT_CALL( *ensemble_linear_predictor_mock_,
                 get_estimated_query_count( _, _ ) )
        .Times( 32 );

    ON_CALL( *ensemble_scheduled_predictor_mock_,
             get_estimated_query_count( _, _ ) )
        .WillByDefault( Return( 0 ) );
    ON_CALL( *ensemble_linear_predictor_mock_,
             get_estimated_query_count( _, _ ) )
        .WillByDefault( Return( 0 ) );
    ON_CALL( *ensemble_rnn_predictor_mock_,
             get_estimated_query_count( _, _, _ ) )
        .WillByDefault( Return( 0 ) );

    ON_CALL( *ensemble_scheduled_predictor_mock_,
             get_estimated_query_count( query_b, 40 ) )
        .WillByDefault( Return( 0 ) );

    ON_CALL( *ensemble_linear_predictor_mock_,
             get_estimated_query_count( query_b, 40 ) )
        .WillByDefault( Return( 0 ) );

    ON_CALL( *ensemble_rnn_predictor_mock_,
             get_estimated_query_count( query_b, 40, _ ) )
        .WillByDefault( Return( 0 ) );

    ON_CALL( *ensemble_scheduled_predictor_mock_,
             get_estimated_query_count( query_a, 40 ) )
        .WillByDefault( Return( 5 ) );

    ON_CALL( *ensemble_linear_predictor_mock_,
             get_estimated_query_count( query_a, 40 ) )
        .WillByDefault( Return( 5 ) );

    ON_CALL( *ensemble_rnn_predictor_mock_,
             get_estimated_query_count( query_a, 40, _ ) )
        .WillByDefault( Return( 5 ) );

    // Case #1: The scheduled result is clearly an outlier (5 vs 0), and 5 >= 5
    auto resp =
        ensemble_early_query_predictor_.get_predicted_queries( 5, 49, 5 );
    EXPECT_EQ( resp.size(), 1 );

    // Case #2: Verify threshold conditional, as it is too large because 5 < 6
    resp = ensemble_early_query_predictor_.get_predicted_queries( 6, 49, 6 );
    EXPECT_EQ( resp.size(), 0 );

    // Case #3: Hybrid prediction should take precedence because 1.5*4 = 6 > 5
    // hence the kernel result is not considered an "outlier" despite being
    // larger

    ON_CALL( *ensemble_scheduled_predictor_mock_,
             get_estimated_query_count( query_a, 40 ) )
        .WillByDefault( Return( 6 ) );

    // We average (4 + 6) / 2 = 5
    ON_CALL( *ensemble_linear_predictor_mock_,
             get_estimated_query_count( query_a, 40 ) )
        .WillByDefault( Return( 4 ) );
    ON_CALL( *ensemble_rnn_predictor_mock_,
             get_estimated_query_count( query_a, 40, _ ) )
        .WillByDefault( Return( 6 ) );

    // Do not observe the query returned from the scheduled predictor
    resp = ensemble_early_query_predictor_.get_predicted_queries( 6, 49, 6 );
    EXPECT_EQ( resp.size(), 0 );

    // However, we do observe the hybrid model result which is 5
    resp = ensemble_early_query_predictor_.get_predicted_queries( 6, 49, 5 );
    EXPECT_EQ( resp.size(), 1 );
}

TEST_F( ensemble_early_query_predictor_test_empty,
        predicted_arrival_calculation ) {
    const int32_t query_a = 1;

    ensemble_early_query_predictor_.add_linear_query_predictor(
        query_a, ensemble_linear_predictor_mock_ );
    ensemble_early_query_predictor_.add_scheduled_query_predictor(
        query_a, ensemble_scheduled_predictor_mock_ );
    ensemble_early_query_predictor_.add_rnn_query_predictor(
        query_a, ensemble_rnn_predictor_mock_ );

    EXPECT_CALL( *ensemble_scheduled_predictor_mock_,
                 get_estimated_query_count( query_a, _ ) )
        .Times( 6 );

    EXPECT_CALL( *ensemble_linear_predictor_mock_,
                 get_estimated_query_count( query_a, _ ) )
        .Times( 6 );

    EXPECT_CALL( *ensemble_rnn_predictor_mock_,
                 get_estimated_query_count( query_a, _, _ ) )
        .Times( 8 );

    ON_CALL( *ensemble_scheduled_predictor_mock_,
             get_estimated_query_count( _, _ ) )
        .WillByDefault( Return( 0 ) );

    ON_CALL( *ensemble_scheduled_predictor_mock_,
             get_estimated_query_count( query_a, 40 ) )
        .WillByDefault( Return( 5 ) );

    // Case #1: The scheduled result is clearly an outlier (5 vs 0), and 5 >= 5,
    // but since the search interval is only two it won't be found at t = 0
    auto resp =
        ensemble_early_query_predictor_.get_predicted_arrival( query_a, 5, 0 );
    EXPECT_EQ( resp, std::numeric_limits<epoch_time>::max() );

    // Case #2: The scheduled result occurs at time 40, hence by starting
    // at time 20 we will be able to notice it as: 40 <= 20 + 2 (search window)
    // * 10 (slot size)
    resp =
        ensemble_early_query_predictor_.get_predicted_arrival( query_a, 5, 20 );
    EXPECT_EQ( resp, 40 );

    // Case #3: We observe multiple predicted query arrivals, and we need
    // to pick the earliest one.
    EXPECT_CALL( *ensemble_scheduled_predictor_mock_,
                 get_estimated_query_count( query_a, _ ) )
        .Times( 2 );

    EXPECT_CALL( *ensemble_linear_predictor_mock_,
                 get_estimated_query_count( query_a, _ ) )
        .Times( 2 );

    ON_CALL( *ensemble_scheduled_predictor_mock_,
             get_estimated_query_count( query_a, 40 ) )
        .WillByDefault( Return( 6 ) );

    ON_CALL( *ensemble_linear_predictor_mock_,
             get_estimated_query_count( query_a, 40 ) )
        .WillByDefault( Return( 5 ) );

    ON_CALL( *ensemble_linear_predictor_mock_,
             get_estimated_query_count( query_a, 30 ) )
        .WillByDefault( Return( 4 ) );

    ON_CALL( *ensemble_rnn_predictor_mock_,
             get_estimated_query_count( query_a, 40, _ ) )
        .WillByDefault( Return( 5 ) );

    ON_CALL( *ensemble_rnn_predictor_mock_,
             get_estimated_query_count( query_a, 30, _ ) )
        .WillByDefault( Return( 4 ) );

    resp =
        ensemble_early_query_predictor_.get_predicted_arrival( query_a, 4, 20 );
    EXPECT_EQ( resp, 30 );
}
