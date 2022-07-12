#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/common/predictor/multi_linear_predictor.h"
#include "../src/distributions/distributions.h"

class multi_linear_predictor_test : public ::testing::Test {};

double make_actual_multi_y( const std::vector<double>& x,
                            const std::vector<double>& actual_weight,
                            double actual_bias, double noise_range,
                            distributions& dist ) {
    EXPECT_EQ( x.size(), actual_weight.size() );

    double noise =
        dist.get_uniform_double_in_range( 0, noise_range * 2 ) - noise_range;
    double actual = actual_bias + noise;

    for( uint32_t pos = 0; pos < x.size(); pos++ ) {
         actual = actual + ( x.at( pos ) * actual_weight.at( pos ) );
    }

    // auto p = x * actual_weight;
    // actual += p.sum();

    return actual;
}

std::vector<double> get_multi_x( distributions&             dist,
                                 const std::vector<double>& x_min,
                                 const std::vector<double>& x_max ) {
    EXPECT_EQ( x_min.size(), x_max.size() );

    std::vector<double> x( x_min.size() );
    for( uint32_t pos = 0; pos < x.size(); pos++ ) {
        x[pos] = dist.get_uniform_double_in_range( x_min[pos], x_max[pos] );
    }
    return x;
}

void check_multi_model(
    const std::vector<double>& x_min, const std::vector<double>& x_max,
    const std::vector<double>& actual_weight, double actual_bias, double noise,
    const std::vector<double>& init_weight, double init_bias,
    double learning_rate, double regularization, double bias_regularization,
    const std::vector<double>& max_input, double max_pred, int num_iters,
    int num_samples, double acceptable_rmse, bool is_static ) {

    multi_linear_predictor p( init_weight, init_bias, regularization,
                              bias_regularization, learning_rate, max_input,
                              -max_pred, max_pred, is_static );

    EXPECT_EQ( x_min.size(), x_max.size() );
    EXPECT_EQ( x_min.size(), actual_weight.size() );
    EXPECT_EQ( x_min.size(), init_weight.size() );

    distributions dist( nullptr );

    uint64_t model_v;

    for( int iter = 0; iter < num_iters; iter++ ) {
        model_v = p.model_version();

        for( int sample = 0; sample < num_samples; sample++ ) {
            std::vector<double> x = get_multi_x( dist, x_min, x_max );
            double y = make_actual_multi_y( x, actual_weight, actual_bias,
                                            noise, dist );
            p.add_observation( x, y );
            DVLOG( 40 ) << "training iteration " << iter << " finished";

            // model version shouldn't change
            EXPECT_EQ( model_v, p.model_version() );
        }

        p.update_model();

        if (p.is_static() == false) {
            // model version should be incremented
            EXPECT_EQ( model_v + 1, p.model_version() );
            model_v += 1;
        }
    }

    double sse = 0;
    double sae = 0;
    for( int sample = 0; sample < num_samples; sample++ ) {
        std::vector<double> x = get_multi_x( dist, x_min, x_max );
        double              y =
            make_actual_multi_y( x, actual_weight, actual_bias, noise, dist );

        double pred = p.make_prediction( x );
        double err = pred - y;

        sse += ( err * err );
        sae += abs( err );

        // model version shouldn't change
        EXPECT_EQ( model_v, p.model_version() );
    }

    double rmse = sqrt( sse / num_samples );
    EXPECT_LT( rmse, acceptable_rmse );

    double mae = sae / num_samples;

    DLOG( INFO ) << "Actual Model : "
                 << "[ weight:" << actual_weight << ", bias:" << actual_bias << " ]";
    DLOG( INFO ) << "Learned Model: "
                 << "[ weight:" << p.weight() << ", bias:" << p.bias() << " ]";
    DLOG( INFO ) << "RMSE:" << rmse << ", MAE:" << mae << ", Noise:" << noise;
}

TEST_F( multi_linear_predictor_test, static_model ) {
    check_multi_model(
        {0, 0} /*x_min*/, {100, 100} /*x_max*/, {5, 5} /*actual_weight*/,
        7 /*actual_bias*/, 1 /*noise*/, {5, 5} /*init_weight*/, 7 /*init_bias*/,
        0.01 /*learning rate*/, 0.01 /*regularization*/,
        0.001 /*bias regularization*/, {100, 100} /*max inputs*/,
        4294967296 /* max pred */, 100 /*num iters*/, 10 /*num samples*/,
        1 /* acceptable MSE */, true /*is_static*/ );
}

TEST_F( multi_linear_predictor_test, non_static_but_init_accurate_model ) {
    check_multi_model(
        {0, 0} /*x_min*/, {10, 10} /*x_max*/, {5, 5} /*actual_weight*/,
        7 /*actual_bias*/, 1 /*noise*/, {5, 5} /*init_weight*/, 7 /*init_bias*/,
        0.01 /*learning rate*/, 0.01 /*regularization*/,
        0.001 /*bias regularization*/, {10, 10} /*max_inputs*/,
        4294967296 /* max pred */, 100 /*num iters*/, 10 /*num samples*/,
        1 /* acceptable MSE*/, false /*is_static*/ );
}

TEST_F( multi_linear_predictor_test, non_static_but_zero_init_model ) {
    check_multi_model(
        {0, 0} /*x_min*/, {10, 10} /*x_max*/, {5, 5} /*actual_weight*/,
        7 /*actual_bias*/, 1 /*noise*/, {0, 0} /*init_weight*/, 0 /*init_bias*/,
        0.01 /*learning rate*/, 0.01 /*regularization*/,
        0.001 /*bias regularization*/, {10, 10} /*max_inputs*/,
        4294967296 /* max pred */, 500 /*num iters*/, 10 /*num samples*/,
        1 /* acceptable MSE */, false /*is_static*/ );
}

TEST_F( multi_linear_predictor_test, non_static_but_one_init_model ) {
    check_multi_model(
        {0, 0} /*x_min*/, {10, 10} /*x_max*/, {5, 5} /*actual_weight*/,
        7 /*actual_bias*/, 1 /*noise*/, {1, 1} /*init_weight*/, 0 /*init_bias*/,
        0.01 /*learning rate*/, 0.01 /*regularization*/,
        0.001 /*bias regularization*/, {10, 10} /*max_inputs*/,
        4294967296 /* max pred */, 500 /*num iters*/, 10 /*num samples*/,
        1 /* acceptable MSE */, false /*is_static*/ );
}

TEST_F( multi_linear_predictor_test,
        non_static_but_zero_init_model_fewer_iters ) {
    check_multi_model(
        {0, 0} /*x_min*/, {10, 10} /*x_max*/, {5, 5} /*actual_weight*/,
        7 /*actual_bias*/, 1 /*noise*/, {0, 0} /*init_weight*/, 0 /*init_bias*/,
        0.01 /*learning rate*/, 0.01 /*regularization*/,
        0.001 /*bias regularization*/, {10, 10} /*max_inputs*/,
        4294967296 /* max pred */, 100 /*num iters*/, 10 /*num samples*/,
        3 /* acceptable MSE */, false /*is_static*/ );
}

TEST_F( multi_linear_predictor_test,
        non_static_but_zero_init_model_fewer_iters_larger_samples ) {
    check_multi_model(
        {0, 0} /*x_min*/, {10, 10} /*x_max*/, {5, 5} /*actual_weight*/,
        7 /*actual_bias*/, 1 /*noise*/, {0, 0} /*init_weight*/, 0 /*init_bias*/,
        0.01 /*learning rate*/, 0.01 /*regularization*/,
        0.001 /*bias regularization*/, {10, 10} /*max_inputs*/,
        4294967296 /* max pred */, 100 /*num iters*/, 50 /*num samples*/,
        3 /* acceptable MSE */, false /*is_static*/ );
}

TEST_F( multi_linear_predictor_test, non_static_but_zero_weight ) {
    check_multi_model(
        {0, 0} /*x_min*/, {10, 10} /*x_max*/, {0, 0} /*actual_weight*/,
        7 /*actual_bias*/, 1 /*noise*/, {0, 0} /*init_weight*/, 0 /*init_bias*/,
        0.01 /*learning rate*/, 0.01 /*regularization*/,
        0.001 /*bias regularization*/, {10, 10} /*max_inputs*/,
        4294967296 /* max pred */, 500 /*num iters*/, 10 /*num samples*/,
        1 /* acceptable MSE */, false /*is_static*/ );
}

TEST_F( multi_linear_predictor_test, non_static_but_zero_bias ) {
    check_multi_model(
        {0, 0} /*x_min*/, {10, 10} /*x_max*/, {5, 5} /*actual_weight*/,
        0 /*actual_bias*/, 1 /*noise*/, {0, 0} /*init_weight*/, 0 /*init_bias*/,
        0.01 /*learning rate*/, 0.01 /*regularization*/,
        0.001 /*bias regularization*/, {10, 10} /*max_input*/,
        4294967296 /* max pred */, 500 /*num iters*/, 10 /*num samples*/,
        1 /* acceptable MSE */, false /*is_static*/ );
}
