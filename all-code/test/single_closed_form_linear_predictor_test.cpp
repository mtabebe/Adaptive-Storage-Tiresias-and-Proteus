#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/common/predictor/single_closed_form_linear_predictor.h"
#include "../src/distributions/distributions.h"

class single_closed_form_linear_predictor_test : public ::testing::Test {};

double make_actual_y( double x, double actual_weight, double actual_bias,
                      double noise_range, distributions& dist );

void check_single_model( double x_min, double x_max, double actual_weight,
                         double actual_bias, double noise, double init_weight,
                         double init_bias, double max_input, int num_samples,
                         double acceptable_rmse, bool is_static ) {

    single_closed_form_linear_predictor p( init_weight, init_bias, max_input,
                                           is_static );

    distributions dist( nullptr );

    uint64_t model_v;

    model_v = p.model_version();

    for( int sample = 0; sample < num_samples; sample++ ) {
        double x = dist.get_uniform_double_in_range( x_min, x_max );
        double y = make_actual_y( x, actual_weight, actual_bias, noise, dist );
        p.add_observation( x, y );

        // model version shouldn't change
        EXPECT_EQ( model_v, p.model_version() );
    }

    p.update_model();

    if( p.is_static() == false ) {
        // model version should be incremented
        EXPECT_EQ( model_v + 1, p.model_version() );
        model_v += 1;
    }

    double sse = 0;
    double sae = 0;
    for( int sample = 0; sample < num_samples; sample++ ) {
        double x = dist.get_uniform_double_in_range( x_min, x_max );
        double y = make_actual_y( x, actual_weight, actual_bias, noise, dist );

        double err = p.make_prediction( x ) - y;

        sse += ( err * err );
        sae += abs( err );

        // model version shouldn't change
        EXPECT_EQ( model_v, p.model_version() );
    }

    double rmse = sqrt( sse / num_samples );
    EXPECT_LT( rmse, acceptable_rmse );

    double mae = sae / num_samples;

    DLOG( INFO ) << "Actual Model : "
                 << "[ weight:" << actual_weight << ", bias:" << actual_bias
                 << " ]";
    DLOG( INFO ) << "Learned Model: "
                 << "[ weight:" << p.weight() << ", bias:" << p.bias() << " ]";
    DLOG( INFO ) << "RMSE:" << rmse << ", MAE:" << mae << ", Noise:" << noise;
}

TEST_F( single_closed_form_linear_predictor_test, static_model ) {
    check_single_model( 0 /*x_min*/, 100 /*x_max*/, 5 /*actual_weight*/,
                        7 /*actual_bias*/, 1 /*noise*/, 5 /*init_weight*/,
                        7 /*init_bias*/, 100 /*max_input*/, 15 /*num samples*/,
                        3 /* acceptable MSE */, true /*is_static*/ );
}

TEST_F( single_closed_form_linear_predictor_test, non_static_smaller_samples ) {
    check_single_model( 0 /*x_min*/, 100 /*x_max*/, 5 /*actual_weight*/,
                        7 /*actual_bias*/, 1 /*noise*/, 0 /*init_weight*/,
                        0 /*init_bias*/, 10 /*max input*/, 25 /*num samples*/,
                        3 /* acceptable MSE */, false /*is_static*/ );
}

TEST_F( single_closed_form_linear_predictor_test, non_static_larger_samples ) {
    check_single_model( 0 /*x_min*/, 10 /*x_max*/, 5 /*actual_weight*/,
                        7 /*actual_bias*/, 1 /*noise*/, 0 /*init_weight*/,
                        0 /*init_bias*/, 10 /*max_input*/, 50 /*num samples*/,
                        3 /* acceptable MSE */, false /*is_static*/ );
}
