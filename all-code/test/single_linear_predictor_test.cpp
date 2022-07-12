#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/common/predictor/single_linear_predictor.h"
#include "../src/distributions/distributions.h"

class single_linear_predictor_test : public ::testing::Test {};

double make_actual_y( double x, double actual_weight, double actual_bias,
                      double noise_range, distributions& dist ) {
    double noise =
        dist.get_uniform_double_in_range( 0, noise_range * 2 ) - noise_range;
    double actual = ( x * actual_weight ) + actual_bias + noise;

    return actual;
}

void check_single_model( double x_min, double x_max, double actual_weight,
                         double actual_bias, double noise, double init_weight,
                         double init_bias, double learning_rate,
                         double regularization, double bias_regularization,
                         double max_input, double max_pred, int num_iters,
                         int num_samples, double acceptable_rmse,
                         bool is_static ) {

    single_linear_predictor p( init_weight, init_bias, regularization,
                               bias_regularization, learning_rate, max_input,
                               -max_pred, max_pred, is_static );

    distributions dist( nullptr );

    uint64_t model_v;

    for( int iter = 0; iter < num_iters; iter++ ) {
        model_v = p.model_version();

        for( int sample = 0; sample < num_samples; sample++ ) {
            double x = dist.get_uniform_double_in_range( x_min, x_max );
            double y =
                make_actual_y( x, actual_weight, actual_bias, noise, dist );
            p.add_observation( x, y );

            // model version shouldn't change
            EXPECT_EQ( model_v, p.model_version() );
        }

        p.update_model();
        DVLOG( 40 ) << "training iteration " << iter << " finished";

        if( p.is_static() == false ) {
            // model version should be incremented
            EXPECT_EQ( model_v + 1, p.model_version() );
            model_v += 1;
        }
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

TEST_F( single_linear_predictor_test, static_model ) {
    check_single_model(
        0 /*x_min*/, 100 /*x_max*/, 5 /*actual_weight*/, 7 /*actual_bias*/,
        1 /*noise*/, 5 /*init_weight*/, 7 /*init_bias*/, 0.01 /*learning rate*/,
        0.01 /*regularization*/, 0.001 /*bias regularization*/,
        100 /*max_input*/, 4294967296 /* max pred */, 100 /*num iters*/,
        10 /*num samples*/, 1 /* acceptable MSE */, true /*is_static*/ );
}

TEST_F( single_linear_predictor_test, non_static_but_init_accurate_model ) {
    check_single_model( 0 /*x_min*/, 10 /*x_max*/, 5 /*actual_weight*/,
                        7 /*actual_bias*/, 1 /*noise*/, 5 /*init_weight*/,
                        7 /*init_bias*/, 0.01 /*learning rate*/,
                        0.01 /*regularization*/, 0.001 /*bias regularization*/,
                        10 /*max input*/,
                        4294967296 /* max pred */,
                        100 /*num iters*/, 10 /*num samples*/,
                        1 /* acceptable MSE*/, false /*is_static*/ );
}

TEST_F( single_linear_predictor_test, non_static_but_zero_init_model ) {
    check_single_model(
        0 /*x_min*/, 10 /*x_max*/, 5 /*actual_weight*/, 7 /*actual_bias*/,
        1 /*noise*/, 0 /*init_weight*/, 0 /*init_bias*/, 0.01 /*learning rate*/,
        0.01 /*regularization*/, 0.001 /*bias regularization*/,
        10 /*max input*/, 4294967296 /* max pred */, 500 /*num iters*/,
        10 /*num samples*/, 1 /* acceptable MSE */, false /*is_static*/ );
}

TEST_F( single_linear_predictor_test, non_static_but_one_init_model ) {
    check_single_model(
        0 /*x_min*/, 10 /*x_max*/, 5 /*actual_weight*/, 7 /*actual_bias*/,
        1 /*noise*/, 1 /*init_weight*/, 0 /*init_bias*/, 0.01 /*learning rate*/,
        0.01 /*regularization*/, 0.001 /*bias regularization*/,
        10 /*max_input*/, 4294967296 /* max pred */, 500 /*num iters*/,
        10 /*num samples*/, 1 /* acceptable MSE */, false /*is_static*/ );
}

TEST_F( single_linear_predictor_test,
        non_static_but_zero_init_model_fewer_iters ) {
    check_single_model(
        0 /*x_min*/, 10 /*x_max*/, 5 /*actual_weight*/, 7 /*actual_bias*/,
        1 /*noise*/, 0 /*init_weight*/, 0 /*init_bias*/, 0.01 /*learning rate*/,
        0.01 /*regularization*/, 0.001 /*bias regularization*/,
        10 /*max_inputs*/, 4294967296 /* max pred */, 100 /*num iters*/,
        10 /*num samples*/, 3 /* acceptable MSE */, false /*is_static*/ );
}

TEST_F( single_linear_predictor_test,
        non_static_but_zero_init_model_fewer_iters_larger_samples ) {
    check_single_model(
        0 /*x_min*/, 10 /*x_max*/, 5 /*actual_weight*/, 7 /*actual_bias*/,
        1 /*noise*/, 0 /*init_weight*/, 0 /*init_bias*/, 0.01 /*learning rate*/,
        0.01 /*regularization*/, 0.001 /*bias regularization*/,
        10 /*max_input*/, 4294967296 /* max pred */, 100 /*num iters*/,
        50 /*num samples*/, 3 /* acceptable MSE */, false /*is_static*/ );
}

TEST_F( single_linear_predictor_test, non_static_but_zero_weight ) {
    check_single_model(
        0 /*x_min*/, 10 /*x_max*/, 0 /*actual_weight*/, 7 /*actual_bias*/,
        1 /*noise*/, 0 /*init_weight*/, 0 /*init_bias*/, 0.01 /*learning rate*/,
        0.01 /*regularization*/, 0.001 /*bias regularization*/,
        10 /*max_input*/, 4294967296 /* max pred */, 500 /*num iters*/,
        10 /*num samples*/, 1 /* acceptable MSE */, false /*is_static*/ );
}

TEST_F( single_linear_predictor_test, non_static_but_zero_bias ) {
    check_single_model(
        0 /*x_min*/, 10 /*x_max*/, 5 /*actual_weight*/, 0 /*actual_bias*/,
        1 /*noise*/, 0 /*init_weight*/, 0 /*init_bias*/, 0.01 /*learning rate*/,
        0.01 /*regularization*/, 0.001 /*bias regularization*/,
        10 /*max_input*/, 4294967296 /* max pred */, 500 /*num iters*/,
        10 /*num samples*/, 1 /* acceptable MSE */, false /*is_static*/ );
}
