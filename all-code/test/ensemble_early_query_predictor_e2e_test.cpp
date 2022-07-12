#include <chrono>
#include <cmath>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdlib.h>
#include <unistd.h>

#include "../src/common/constants.h"
#include "../src/common/predictor/ensemble/ensemble_early_query_predictor.h"
#include "../src/common/predictor/ensemble/ensemble_early_query_predictor_configs.h"
#include "../src/common/predictor/ensemble/gaussian_scheduled_distribution.h"

using ::testing::_;
using ::testing::Return;

class ensemble_early_query_predictor_e2e_test_empty : public ::testing::Test {
   protected:
    ensemble_early_query_predictor_configs ensemble_configs_ =
        construct_ensemble_early_query_predictor_configs(
            1000, /* slot */
            10,   /* search interval */
            75,   /* linear training interval */
            50,   /* rnn training interval */
            4,    /* scheduled training interval */
            0.05, /* rnn learning rate */
            0.1,  /* krls learning rate */
            1.5,  /* outlier threshold */
            1.0,  /* krls gamma*/
            75,   /* rnn epoch count*/
            12,   /* rnn sequence count*/
            5     /* rnn layer count*/
            );

    ensemble_early_query_predictor ensemble_early_query_predictor_;

   public:
    ensemble_early_query_predictor_e2e_test_empty()
        : ensemble_early_query_predictor_( ensemble_configs_ ) {}
};

std::pair<query_count, query_count> get_query_frequency(
    epoch_time current_time ) {
    query_count f = static_cast<query_count>(
        abs( 100 * abs( sin( current_time * 3.14 / 17777 ) ) +
             5 * sin( current_time ) ) );
    query_count g = static_cast<query_count>(
        abs( 75 * abs( cos( current_time * 3.14 / 20033 ) ) +
             2 * sin( current_time ) ) );

    return std::pair<query_count, query_count>( f, g );
}

void apply_hybrid_cycle_queries( ensemble_early_query_predictor& predictor,
                                 int query_a, int query_b ) {
    for( int x = 0; x < 100; x++ ) {
        epoch_time current_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch() )
                .count();
        query_count f, g;
        std::tie( f, g ) = get_query_frequency( current_time );

        for( query_count y = 0; y < f; y++ ) {
            predictor.add_observation( query_a, current_time );
        }

        for( query_count y = 0; y < g; y++ ) {
            predictor.add_observation( query_b, current_time );
        }

        // For more accurate results, keep track of time spent adding
        // observations
        epoch_time loaded_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch() )
                .count();

        // Sleep for 1000ms
        usleep( 1000000 - ( loaded_time - current_time ) );
    }
}

TEST_F( ensemble_early_query_predictor_e2e_test_empty,
        predicted_sinx_noise_hybrid ) {

    if( k_skip_slow_tests ) {
        // DLOG( WARNING ) << "Skipping test";
        return;
    }

    /**
     * We simulate 1000ms slots with a predefined number of queries occuring
     * in each such slot. The expectation is that the hybrid model will
     * pickup the underlying model reasonably well.
     *
     * Function:
     *   f(x) = 100 * abs(sin(x * 3.14 / 17777)) + 5 * sin(x)
     *   g(x) = 75 * abs(cos(x * 3.14 / 20033)) + 2 * sin(x)
     *
     * We verify for ~20% accuracy (in this case +- 20 points of the expected
     * results).
     */

    const int query_a = 123, query_b = 124;

    epoch_time predicted_arrival =
        ensemble_early_query_predictor_.get_predicted_arrival( query_a, 1, 1 );
    size_t expected_arrival_count =
        ensemble_early_query_predictor_.get_predicted_queries( 0, 100, 1 )
            .size();
    query_count estimated_query_count =
        ensemble_early_query_predictor_.get_estimated_query_count( query_a,
                                                                   123 );

    // Case #1: Trivial case where no query arrival is expected
    EXPECT_EQ( predicted_arrival, std::numeric_limits<epoch_time>::max() );
    EXPECT_EQ( expected_arrival_count, 0 );
    EXPECT_EQ( estimated_query_count, 0 );

    // Case #2: Complete end-to-end test with hybrid model
    apply_hybrid_cycle_queries( ensemble_early_query_predictor_, query_a,
                                query_b );

    // Training the model and testing.
    //
    // Note: Realistically, we continue to see queries incoming, hence we
    // provide a sample of queries in order to not create a gap of historical
    // data. This is required because
    // otherwise we cannot correctly stitch the current_time and future time
    // sequences properly in the RNN model. We continue receiving queries even
    // during the 10 test queries as well for the same reason.

    ensemble_early_query_predictor_.train();
    apply_hybrid_cycle_queries( ensemble_early_query_predictor_, query_a,
                                query_b );

    double mae_f = 0, mae_g = 0;

    // Check results 10 times, predicting the next slot
    for( int x = 0; x < 10; x++ ) {
        epoch_time current_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch() )
                .count();

        // Predict the future
        query_count f2, g2;
        std::tie( f2, g2 ) = get_query_frequency( current_time + 1000 );

        query_count estimated_query_count_f =
            ensemble_early_query_predictor_.get_estimated_query_count(
                query_a, current_time + 1000 );
        query_count estimated_query_count_g =
            ensemble_early_query_predictor_.get_estimated_query_count(
                query_b, current_time + 1000 );

        mae_f += pow( (int) estimated_query_count_f - (int) f2, 2 );
        mae_g += pow( (int) estimated_query_count_g - (int) g2, 2 );

        // Add concurrent queries
        query_count f, g;
        std::tie( f, g ) = get_query_frequency( current_time );

        for( query_count y = 0; y < f; y++ ) {
            ensemble_early_query_predictor_.add_observation( query_a,
                                                             current_time );
        }

        for( query_count y = 0; y < g; y++ ) {
            ensemble_early_query_predictor_.add_observation( query_b,
                                                             current_time );
        }

        // For more accurate results, keep track of time spent adding
        // observations
        epoch_time loaded_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch() )
                .count();

        // Sleep for 1000ms
        usleep( 1000000 - ( loaded_time - current_time ) );
    }

    mae_f /= 10;
    mae_g /= 10;

    mae_f = pow( mae_f, 0.5 );
    mae_g = pow( mae_g, 0.5 );

    EXPECT_LT( mae_f, 45 );
    EXPECT_LT( mae_g, 45 );

    DVLOG( 2) << "Ensemble E2E SINX LINX  MAE:" << mae_f << ", " << mae_g;

    /*
    DLOG_IF( WARNING, mae_f > 20 )
        << "Ensemble early query predictor e2e f(x) exceeds acceptable limit: "
        << mae_f << " vs expected <= 20";
    DLOG_IF( WARNING, mae_g > 20 )
        << "Ensemble early query predictor e2e g(x) exceeds acceptable limit: "
        << mae_g << " vs expected <= 20";
        */
}

void apply_hybrid_linear_queries( ensemble_early_query_predictor& predictor,
                                  int                             query_a ) {
    for( int x = 0; x < 100; x++ ) {
        epoch_time current_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch() )
                .count();

        query_count f = 200 + sin( x );
        for( query_count y = 0; y < f; y++ ) {
            predictor.add_observation( query_a, current_time );
        }

        epoch_time loaded_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch() )
                .count();

        // Sleep for 1000ms
        usleep( 1000000 - ( loaded_time - current_time ) );
    }
}

TEST_F( ensemble_early_query_predictor_e2e_test_empty,
        predicted_linear_outlier ) {

    if( k_skip_slow_tests ) {
//        DLOG( WARNING ) << "Skipping test";
        return;
    }

    /**
     * We simulate 1000ms slots with a predefined number of queries occuring
     * in each such slot. The expectation is that the hybrid model will
     * pickup the underlying model reasonably well.
     *
     * Function:
     *   f(x) = 200 + sin(x)
     *
     * Note however, we introduce several tight "spikes" that recurr often.
     * This is to ensure that we override when appropriate and use scheduled
     * gaussian curves. This test is weak in the sense that it doesn't look for
     * preciseness, but instead ensure that "at some time" the gaussian
     * distribution is factored in.
     */

    const int query_a = 123;
    apply_hybrid_linear_queries( ensemble_early_query_predictor_, query_a );

    // Training the model and testing.
    ensemble_early_query_predictor_.train();
    apply_hybrid_linear_queries( ensemble_early_query_predictor_, query_a );

    auto gsd = std::make_shared<gaussian_scheduled_distribution>(
        10000,  // base_time
        9000,   // period_time
        4500,   // time_window
        500,    // y_max
        100,    // y_min
        0,      // mean
        2000    // std_deviation
        );
    ensemble_early_query_predictor_.add_query_distribution( query_a, gsd );

    double mae_f = 0;

    // Check results 10 times, predicting the next slot
    for( int x = 0; x < 10; x++ ) {
        epoch_time current_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch() )
                .count();

        epoch_time rounded_current_time =
            current_time - ( current_time % ensemble_configs_.slot_ );

        // Predict the future
        query_count scheduled_count =
            gsd->get_estimated_query_count( current_time + 1000 );
        query_count formulated_count = 200 + sin( rounded_current_time + 1000 );
        query_count f2 = formulated_count;
        if( scheduled_count > (query_count) formulated_count *
                                  ensemble_configs_.outlier_threshold_ ) {
            f2 = scheduled_count;
        }

        query_count estimated_query_count_f =
            ensemble_early_query_predictor_.get_estimated_query_count(
                query_a, current_time + 1000 );

        mae_f += pow( (int) estimated_query_count_f - (int) f2, 2 );

        // Add concurrent queries
        query_count f = 200 + sin( rounded_current_time );

        for( query_count y = 0; y < f; y++ ) {
            ensemble_early_query_predictor_.add_observation( query_a,
                                                             current_time );
        }

        // For more accurate results, keep track of time spent adding
        // observations
        epoch_time loaded_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch() )
                .count();

        // Sleep for 1000ms
        usleep( 1000000 - ( loaded_time - current_time ) );
    }

    mae_f /= 10;
    mae_f = pow( mae_f, 0.5 );

    EXPECT_LT( mae_f, 52 );
    /*
    DLOG_IF( WARNING, mae_f > 20 )
        << "Ensemble early query predictor e2e f(x) exceeds acceptable limit: "
        << "expected <= 20 but found " << mae_f;
        */
}
