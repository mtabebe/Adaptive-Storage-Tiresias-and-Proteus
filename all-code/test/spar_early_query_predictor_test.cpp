#include <chrono>
#include <cmath>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdlib.h>

#include "../src/common/constants.h"
#include "../src/common/predictor/spar_early_query_predictor.h"
#include "spar_predictor_mock.h"

using ::testing::_;
using ::testing::Return;

class spar_early_query_predictor_test_empty : public ::testing::Test {
   protected:
    std::shared_ptr<spar_predictor_mock> spar_predictor_mock_;
    spar_predictor_configs               spar_configs_ =
        construct_spar_predictor_configs( 10,  /* width */
                                          10,  /* slot */
                                          200, /* training interval */
                                          10,  /* temporal granularity */
                                          20,  /* periodic granularity */
                                          2    /* arrival window */
                                          );
    spar_early_query_predictor spar_early_query_predictor_;

   public:
    spar_early_query_predictor_test_empty()
        : spar_predictor_mock_( std::make_shared<spar_predictor_mock>() ),
          spar_early_query_predictor_( spar_configs_, spar_predictor_mock_ ) {}
};

TEST_F( spar_early_query_predictor_test_empty, predicted_arrival_empty ) {
    const int query_a = 123;

    /**
     * The search window will be 10 * 2 = 20, and will be invoked
     * in temporal increments of 10. Hence, we expect 3 invocations.
     *
     * NOTE: By default gmock will traverse up the expected calls,
     * hence we simply state that there were no other calls by expecting 0.
     */
    EXPECT_CALL( *spar_predictor_mock_, get_estimated_query_count( _, _ ) )
        .Times( 0 );
    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_a, 0 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_a, 10 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_a, 20 ) )
        .Times( 1 );

    // Query schedules are not supported for the SPAR model, and should do
    // nothing in this case
    spar_early_query_predictor_.add_query_schedule( query_a, 2 );

    epoch_time predicted_arrival =
        spar_early_query_predictor_.get_predicted_arrival( query_a, 1, 1 );
    EXPECT_EQ( predicted_arrival, std::numeric_limits<epoch_time>::max() );
}

TEST_F( spar_early_query_predictor_test_empty,
        predicted_arrival_training_no_query ) {
    /**
     * If there are no queries observed, then there should be no invocations to
     * the train function
     */

    EXPECT_CALL( *spar_predictor_mock_, train( _, _ ) ).Times( 0 );
    spar_early_query_predictor_.train();
}

TEST_F( spar_early_query_predictor_test_empty, training_update ) {
    const int32_t query_a = 1, query_b = 2, query_c = 3;

    EXPECT_CALL( *spar_predictor_mock_, train( query_a, _ ) ).Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_, train( query_b, _ ) ).Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_, train( query_c, _ ) ).Times( 1 );

    EXPECT_CALL( *spar_predictor_mock_, add_observation( query_a, 10 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_, add_observation( query_a, 11 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_, add_observation( query_b, 12 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_, add_observation( query_c, 12 ) )
        .Times( 1 );

    spar_early_query_predictor_.add_observation( query_a, 10 );
    spar_early_query_predictor_.add_observation( query_a, 11 );
    spar_early_query_predictor_.add_observation( query_b, 12 );
    spar_early_query_predictor_.train();

    spar_early_query_predictor_.add_observation( query_c, 12 );
    spar_early_query_predictor_.train();
}

TEST_F( spar_early_query_predictor_test_empty, training_update_bulk ) {
    const int32_t query_a = 1, query_b = 2, query_c = 3;

    EXPECT_CALL( *spar_predictor_mock_, train( query_a, _ ) ).Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_, train( query_b, _ ) ).Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_, train( query_c, _ ) ).Times( 1 );

    EXPECT_CALL( *spar_predictor_mock_, add_observation( query_a, 1 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_, add_observation( query_a, 2 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_, add_observation( query_b, 3 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_, add_observation( query_b, 4 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_, add_observation( query_c, 12 ) )
        .Times( 1 );

    std::vector<std::pair<query_id, epoch_time>> observations;
    observations.emplace_back( query_a, 1 );
    observations.emplace_back( query_a, 2 );
    observations.emplace_back( query_b, 3 );
    observations.emplace_back( query_b, 4 );

    spar_early_query_predictor_.add_observations( observations );
    spar_early_query_predictor_.train();

    spar_early_query_predictor_.add_observation( query_c, 12 );
    spar_early_query_predictor_.train();
}

TEST_F( spar_early_query_predictor_test_empty, predicted_queries_basic ) {
    const int32_t query_a = 1, query_b = 2, query_c = 3;

    /**
     * We consider a begin_time of 3, end_time of 49 and threshold of 10.
     * Since the temporal granularity is 10, we need an inclusive range of
     * [10, 40] with correct rounding. Hence, we should have for each observed
     * query id four invocations at the rounded values.
     */
    EXPECT_CALL( *spar_predictor_mock_, train( query_a, _ ) ).Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_, train( query_b, _ ) ).Times( 1 );

    EXPECT_CALL( *spar_predictor_mock_, add_observation( query_a, 10 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_, add_observation( query_a, 11 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_, add_observation( query_b, 12 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_, add_observation( query_c, 12 ) )
        .Times( 1 );

    EXPECT_CALL( *spar_predictor_mock_, get_estimated_query_count( _, _ ) )
        .Times( 0 );

    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_a, 10 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_a, 20 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_a, 30 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_a, 40 ) )
        .Times( 1 );

    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_b, 10 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_b, 20 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_b, 30 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_b, 40 ) )
        .Times( 1 );

    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_c, 10 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_c, 20 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_c, 30 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_c, 40 ) )
        .Times( 1 );

    spar_early_query_predictor_.add_observation( query_a, 10 );
    spar_early_query_predictor_.add_observation( query_a, 11 );
    spar_early_query_predictor_.add_observation( query_b, 12 );

    // Training should have no effect on the query ids verified
    spar_early_query_predictor_.train();

    spar_early_query_predictor_.add_observation( query_c, 12 );
    const auto resp =
        spar_early_query_predictor_.get_predicted_queries( 3, 49, 10 );
    EXPECT_EQ( resp.size(), 0 );
}

TEST_F( spar_early_query_predictor_test_empty, predicted_queries_boundary ) {
    const int32_t query_a = 1;

    /**
     * We consider a begin_time of 10, end_time of 50 and threshold of 10.
     * Since the temporal granularity is 10, we obtain an inclusive range of
     * [10, 50] with correct rounding. Hence, we should have for each observed
     * query id five invocations at the rounded values.
     */

    EXPECT_CALL( *spar_predictor_mock_, train( query_a, _ ) ).Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_, add_observation( query_a, 10 ) )
        .Times( 1 );

    EXPECT_CALL( *spar_predictor_mock_, get_estimated_query_count( _, _ ) )
        .Times( 0 );

    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_a, 10 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_a, 20 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_a, 30 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_a, 40 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_a, 50 ) )
        .Times( 1 );

    spar_early_query_predictor_.add_observation( query_a, 10 );

    // Training should have no effect on the query ids verified
    spar_early_query_predictor_.train();

    const auto resp =
        spar_early_query_predictor_.get_predicted_queries( 3, 50, 10 );
    EXPECT_EQ( resp.size(), 0 );
}

TEST_F( spar_early_query_predictor_test_empty, predicted_queries_with_return ) {
    const int32_t query_a = 1, query_b = 2;

    EXPECT_CALL( *spar_predictor_mock_, add_observation( _, _ ) ).Times( 0 );

    EXPECT_CALL( *spar_predictor_mock_, add_observation( query_a, 10 ) )
        .Times( 1 );

    EXPECT_CALL( *spar_predictor_mock_, add_observation( query_b, 10 ) )
        .Times( 1 );

    EXPECT_CALL( *spar_predictor_mock_, get_estimated_query_count( _, _ ) )
        .Times( 0 );

    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_a, 0 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_b, 0 ) )
        .Times( 1 );

    ON_CALL( *spar_predictor_mock_, get_estimated_query_count( _, _ ) )
        .WillByDefault( Return( 10 ) );

    spar_early_query_predictor_.add_observation( query_a, 10 );
    spar_early_query_predictor_.add_observation( query_b, 10 );

    const auto resp =
        spar_early_query_predictor_.get_predicted_queries( 0, 3, 10 );
    EXPECT_EQ( resp.size(), 2 );

    EXPECT_NE( resp.find( query_a ), resp.end() );
    EXPECT_NE( resp.find( query_b ), resp.end() );
}

TEST_F( spar_early_query_predictor_test_empty, predicted_arrival_with_return ) {
    const int query_a = 123;

    /**
     * The search window will be 10 * 2 = 20, and will be invoked
     * in temporal increments of 10. Hence, we expect 3 invocations.
     *
     * However, the second invocation at time 10 should return 1,
     * terminating the loop and returning the expected candidate time.
     */
    EXPECT_CALL( *spar_predictor_mock_, get_estimated_query_count( _, _ ) )
        .Times( 0 );
    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_a, 10 ) )
        .Times( 1 );
    EXPECT_CALL( *spar_predictor_mock_,
                 get_estimated_query_count( query_a, 0 ) )
        .Times( 1 );

    ON_CALL( *spar_predictor_mock_, get_estimated_query_count( query_a, 0 ) )
        .WillByDefault( Return( 0 ) );

    ON_CALL( *spar_predictor_mock_, get_estimated_query_count( query_a, 10 ) )
        .WillByDefault( Return( 10 ) );

    epoch_time predicted_arrival =
        spar_early_query_predictor_.get_predicted_arrival( query_a, 10, 1 );
    EXPECT_EQ( predicted_arrival, 10 );
}

class spar_early_query_predictor_test_e2e : public ::testing::Test {
   protected:
    static constexpr long variate_count =
        SPAR_PERIODIC_QUANTITY + SPAR_TEMPORAL_QUANTITY;

    spar_predictor_configs spar_configs_ = construct_spar_predictor_configs(
        1000,                    /* width */
        100,                     /* slot */
        2000,                    /* training interval */
        10,                      /* temporal granularity */
        20,                      /* periodic granularity */
        10,                      /* arrival window */
        25000,                   /* normalization parameter */
        0.07,                    /* gamma */
        0.1 /* learning rate */  // .01 0.0000001
        );
    std::shared_ptr<spar_predictor<variate_count>> spar_predictor_;
    spar_early_query_predictor                     spar_early_query_predictor_;

   public:
    spar_early_query_predictor_test_e2e()
        : spar_predictor_( std::make_shared<spar_predictor<variate_count>>(
              spar_configs_ ) ),
          spar_early_query_predictor_( spar_configs_, spar_predictor_ ) {}
};

int apply_noise( int query_count ) {
    srand( time( nullptr ) );
    int noise = ( rand() % 10 ) - 5;
    return query_count - noise < 0 ? 0 : query_count - noise;
}

query_count sin_function( epoch_time time ) {
    time %= 100 * 1000;
    return (query_count) abs(
        1000 * sin( ( 1.0 / ( 100 * 1000 ) ) * dlib::pi * (double) time ) );
}

TEST_F( spar_early_query_predictor_test_e2e, end_to_end_test_sinx ) {

    if( k_skip_slow_tests ) {
        DLOG( WARNING ) << "Skipping test";
        return;
    }

    const int query_a = 123;
    /**
     * Performs an end-to-end test with a sinx function.
     * The sinx function will have a period of 1 "day"
     * and we will provide a simple noise function of a ~10 queries.
     *
     * The trough will be 0 queries, and the peak will be 1,000 queries.
     * Since the width is 100, then there are 100 slots in a day.
     * Each slot is 1000ms = 1 second, which means that we have
     * 100 seconds in this day.
     *
     * The training interval is 200, meaning 2 days.
     * Hence, we obtain the following function:
     * f(x) = abs(10000 * sin(1/(100*100) * pi * x))
     */

    epoch_time current_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch() )
            .count();

    for( epoch_time x = current_time - 100 * 1000 * 9; x <= current_time;
         x += 10 ) {
        query_count count = sin_function( x );
        count = apply_noise( count );
        for( epoch_time y = 0; y < count; y++ ) {
            spar_early_query_predictor_.add_observation( query_a, x );
        }
    }

    spar_early_query_predictor_.train( current_time );
    double mae = 0;
    int    sum_predicted = 0;

    for( epoch_time x = current_time; x <= current_time + 100 * 10; x += 10 ) {
        query_count count = sin_function( x ) * 10;
        long estimated = spar_early_query_predictor_.get_estimated_query_count(
            query_a, x, current_time );
        mae += abs( (long) ( count - estimated ) );
        sum_predicted += estimated;
    }

    mae /= ( 100 * 10 ) / 10;
    EXPECT_LT( mae, 5000 );
    DLOG_IF( WARNING, mae > 150 )
        << "Warning: Expected SPAR sinx e2e test MAE to be <= 150, found"
        << mae;
}

query_count linear_function( epoch_time time, epoch_time base_time ) {
    return (query_count) 200 *
           ( (double) ( time - base_time ) / ( 100 * 1000 ) );
}

TEST_F( spar_early_query_predictor_test_e2e, end_to_end_test_linear_x ) {

    if( k_skip_slow_tests ) {
        DLOG( WARNING ) << "Skipping test";
        return;
    }

    const int query_a = 123;
    /**
     * Performs an end-to-end test with a linear function.
     * The linear function will increase by 200 every "day" uniformly
     * and will be provided a simple noise function of a ~10 queries.
     *
     * The trough will be 0 queries, and it will increase linearly.
     * Since the width is 100, then there are 100 slots in a day.
     * Each slot is 1000ms = 1 second, which means that we have
     * 100 seconds in this day.
     *
     * Since the base unit of time is epochs, we establish a base time
     * which is defined as (current_time - (100 * 100 * 9)). At this time,
     * the linear function returns 0, as this is the earliest time the
     * algorithm will query in this example.
     *
     * The training interval is 200, meaning 2 days.
     *
     * Hence, we obtain the following function:
     * f(x) = 200 * ((x-base_time) / (100*1000))
     */

    epoch_time current_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch() )
            .count();
    epoch_time base_time = current_time - 100 * 1000 * 9;

    for( epoch_time x = base_time; x <= current_time; x += 10 ) {
        query_count count = linear_function( x, base_time );
        count = apply_noise( count );
        for( epoch_time y = 0; y < count; y++ ) {
            spar_early_query_predictor_.add_observation( query_a, x );
        }
    }

    spar_early_query_predictor_.train( current_time );
    double mae = 0;
    int    sum_predicted = 0;

    for( epoch_time x = current_time; x <= current_time + 100 * 10; x += 100 ) {
        query_count count = linear_function( x, base_time ) * 10;
        long estimated = spar_early_query_predictor_.get_estimated_query_count(
            query_a, x, current_time );
        mae += abs( (long) ( count - estimated ) );
        sum_predicted += estimated;
    }

    mae /= ( 100 * 10 ) / 100;
    EXPECT_LT( mae, 500 );

    DLOG_IF( WARNING, mae > 150 )
        << "Warning: Expected SPAR linear e2e test MAE to be <= 150, found"
        << mae;
}

query_count sinx_linear_function( epoch_time time, epoch_time base_time ) {
    return (query_count) 200 *
               ( (double) ( time - base_time ) / ( 100 * 1000 ) ) +
           abs( 500 * sin( ( 1.0 / 100 * 1000 ) * dlib::pi * time ) );
}

TEST_F( spar_early_query_predictor_test_e2e, end_to_end_test_sinx_linear_x ) {
    if( k_skip_slow_tests ) {
        DLOG( WARNING ) << "Skipping test";
        return;
    }

    const int query_a = 123;
    /**
     * Performs an end-to-end test with a periodic function and a linear
     * adder. The periodic function will increase by 1000 every "day" uniformly
     * and will be provided a simple noise function of a ~10 queries.
     *
     * On the other hand, the linear function will be increasing by 200
     * every "day".
     *
     * Since the base unit of time is epochs, we establish a base time
     * which is defined as (current_time - (100 * 100 * 9)). At this time,
     * the linear function returns 0, as this is the earliest time the
     * algorithm will query in this example.
     *
     * The training interval is 200, meaning 2 days.
     *
     * Therefore, we obtain the following formula:
     * f(x) = 100 * (time - base_time) / (100 * 1000) + abs(1000 *
     * sin(1/100*100 * Pi * x))
     */

    epoch_time current_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch() )
            .count();
    epoch_time base_time = current_time - 100 * 1000 * 9;

    for( epoch_time x = base_time; x <= current_time; x += 10 ) {
        query_count count = sinx_linear_function( x, base_time );
        count = apply_noise( count );
        for( epoch_time y = 0; y < count; y++ ) {
            spar_early_query_predictor_.add_observation( query_a, x );
        }
    }

    spar_early_query_predictor_.train( current_time );
    double mae = 0;
    int    sum_predicted = 0;

    for( epoch_time x = current_time; x <= current_time + 100 * 10; x += 100 ) {
        query_count count = sinx_linear_function( x, base_time ) * 10;
        long estimated = spar_early_query_predictor_.get_estimated_query_count(
            query_a, x, current_time );
        mae += abs( (long) ( count - estimated ) );
        sum_predicted += estimated;
    }

    mae /= ( 100 * 10 ) / 100;
    DVLOG( 2) << "SPAR E2E SINX LINX  MAE:" << mae;
    EXPECT_LT( mae, 500 );
    DLOG_IF( WARNING, mae > 150 )
        << "Warning: Expected SPAR linear sinx e2e test MAE to be <= 150, found"
        << mae;
}

class spar_early_query_predictor_test_e2e_random : public ::testing::Test {
   protected:
    static constexpr long variate_count =
        SPAR_PERIODIC_QUANTITY + SPAR_TEMPORAL_QUANTITY;

    spar_predictor_configs spar_configs_ =
        construct_spar_predictor_configs( 1000,  /* width */
                                          100,   /* slot */
                                          2000,  /* training interval */
                                          10,    /* temporal granularity */
                                          20,    /* periodic granularity */
                                          10,    /* arrival window */
                                          25000, /* normalization parameter */
                                          0.07,  /* gamma */
                                          0.1    /* learning rate */
                                          );
    std::shared_ptr<spar_predictor<variate_count>> spar_predictor_;
    spar_early_query_predictor                     spar_early_query_predictor_;

   public:
    spar_early_query_predictor_test_e2e_random()
        : spar_predictor_( std::make_shared<spar_predictor<variate_count>>(
              spar_configs_ ) ),
          spar_early_query_predictor_( spar_configs_, spar_predictor_ ) {}
};

query_count temporal_function( epoch_time time, epoch_time base_time ) {
    time %= ( 100 * 1000 );
    query_count res = abs(
        1000 * sin( ( 1.0 / ( 100 * 1000 ) ) * dlib::pi * (double) time ) );

    /**
     * In order to verify if temporal results are weighed, we introduce
     * random spikes throughout the day. The intent is that small spikes
     * will appear randomly, and the model would learn that in the past
     * days this event did not occur hence it should not depend only on
     * historical periodic data, but instead adjust those values and apply
     * an adjustment increasing the estimated query count.
     *
     * The "randomness" is achieved by multiplying the query count
     * by a factor of 100 for epoch times where
     * */

    uint32_t day_number = ( time - base_time ) / 100 * 1000;
    if( ( time / 1000 ) % 10 == 1 && ( day_number % 3 ) == 0 ) {
        res *= 10;
    }
    return res;
}

TEST_F( spar_early_query_predictor_test_e2e_random,
        end_to_end_test_sinx_random ) {

    if( k_skip_slow_tests ) {
        DLOG( WARNING ) << "Skipping test";
        return;
    }

    const int query_a = 123;

    /**
     * Performs an end-to-end test with a periodic and linear function.
     * The linear function will increase by 200 every "day" uniformly
     * and will be provided a simple noise function of a ~10 queries.
     *
     * Moreover, it will experience spikes where the function return
     * values are multiplied by 100.
     *
     * The training interval is 200, meaning 2 days.
     */
    epoch_time current_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch() )
            .count();

    epoch_time base_time = current_time - 100 * 1000 * 9;
    for( epoch_time x = base_time; x <= current_time; x += 10 ) {
        query_count count = temporal_function( x, base_time );
        count = apply_noise( count );
        for( epoch_time y = 0; y < count; y++ ) {
            spar_early_query_predictor_.add_observation( query_a, x );
        }
    }

    spar_early_query_predictor_.train( current_time );
    double mae = 0;
    int    sum_predicted = 0;

    for( epoch_time x = current_time; x <= current_time + 100 * 10; x += 100 ) {
        query_count count = temporal_function( x, base_time ) * 10;
        long estimated = spar_early_query_predictor_.get_estimated_query_count(
            query_a, x, current_time );
        mae += abs( (long) ( count - estimated ) );
        sum_predicted += estimated;
    }

    mae /= ( 100 * 10 ) / 100;

    EXPECT_LT( mae, 2000 );
    DLOG_IF( WARNING, mae > 1500 )
        << "Warning: Expected SPAR random e2e test MAE to be <= 1500, found"
        << mae;
}
