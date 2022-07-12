#define GTEST_HAS_TR1_TUPLE 0

#include <dlib/svm.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/common/predictor/mlp_predictor.h"
#include "predictor_test_utils.h"

class mlp_predictor_test : public ::testing::Test {};

#if 0
TEST_F( mlp_predictor_test, dlib_sample_classification_test ) {
    mlp_predictor<2> p( false /* is_static */, {1000, 1000} /* max_input */,
                        5 /* layer_1_nodes */, 0 /* layer_2_nodes */,
                        0.01 /* learning_rate */, 0.8 /* momentum */ );

    dvector<2> input;

    for( int i = 0; i < 1000; ++i ) {
        for( int r = -20; r <= 20; ++r ) {
            for( int c = -20; c <= 20; ++c ) {
                input( 0 ) = r;
                input( 1 ) = c;

                // if this point is less than 10 from the origin
                if( sqrt( (double) r * r + c * c ) <= 10 )
                    p.add_observation( input, 1 );
                else
                    p.add_observation( input, 0 );
            }
        }
        p.update_model();
    }

    input( 0 ) = 3.123;
    input( 1 ) = 4;
    std::cout << "This sample should be close to 1 and it is classified as a "
              << p.make_prediction( input ) << std::endl;

    input( 0 ) = 0;
    input( 1 ) = 1;
    std::cout << "This sample should be close to 1 and it is classified as a "
              << p.make_prediction( input ) << std::endl;

    input( 0 ) = 13.123;
    input( 1 ) = 9.3545;
    std::cout << "This sample should be close to 0 and it is classified as a "
              << p.make_prediction( input ) << std::endl;

    input( 0 ) = 13.123;
    input( 1 ) = 0;
    std::cout << "This sample should be close to 0 and it is classified as a "
              << p.make_prediction( input ) << std::endl;
}
#endif

template <long N>
void check_mlp_model( const dvector<N>& x_min, const dvector<N>&    x_max,
                      double ( *func )( const dvector<N>& ), double noise,
                      int num_iters, int num_samples,
                      const dvector<N>& max_input, double max_pred,
                      bool                       is_static,
                      const std::vector<long>&   layer_1_nodes_configs,
                      const std::vector<long>&   layer_2_nodes_configs,
                      const std::vector<double>& learning_rate_configs,
                      const std::vector<double>& momentum_configs,
                      double                     acceptable_rmse ) {

    double least_rmse = INFINITY;
    long   least_rmse_layer_1_nodes_configs;
    long   least_rmse_layer_2_nodes_configs;
    double least_rmse_learning_rate_configs;
    double least_rmse_momentum_configs;

    for( long layer_1_nodes : layer_1_nodes_configs ) {
        for( long layer_2_nodes : layer_2_nodes_configs ) {
            for( double learning_rate : learning_rate_configs ) {
                for( double momentum : momentum_configs ) {
                    mlp_predictor<N> p( is_static, max_input, layer_1_nodes,
                                        layer_2_nodes, learning_rate, momentum,
                                        -max_pred, max_pred );

                    double rmse = test_dlib_model( p, x_min, x_max, func, noise,
                                                   num_iters, num_samples );

                    DVLOG( 1 ) << "layer_1_nodes: " << layer_1_nodes
                               << ", layer_2_nodes: " << layer_2_nodes
                               << ", learning_rate: " << learning_rate
                               << ", momentum: " << momentum
                               << ", RMSE: " << rmse;

                    if( rmse < least_rmse ) {
                        least_rmse = rmse;
                        least_rmse_layer_1_nodes_configs = layer_1_nodes;
                        least_rmse_layer_2_nodes_configs = layer_2_nodes;
                        least_rmse_learning_rate_configs = learning_rate;
                        least_rmse_momentum_configs = momentum;
                    }
                }
            }
        }
    }

    DLOG( INFO ) << "least RMSE: " << least_rmse
                 << ", least_rmse_layer_1_nodes_configs: "
                 << least_rmse_layer_1_nodes_configs
                 << ", least_rmse_layer_2_nodes_configs: "
                 << least_rmse_layer_2_nodes_configs
                 << ", least_rmse_learning_rate_configs: "
                 << least_rmse_learning_rate_configs
                 << ", least_rmse_momentum_configs: "
                 << least_rmse_momentum_configs;
    EXPECT_LE( least_rmse, acceptable_rmse );
}

TEST_F( mlp_predictor_test, single_sigmoid_test ) {
    check_mlp_model(
        {-100} /* x_min */, {100} /* x_max */, sigmoid /* func */,
        0.05 /* noise */, 100 /* num_iters */, 100 /* num_samples */,
        {100} /* max_input */, 4294967296 /* max pred */, false /* is_static */,
#if 0
        {1, 2, 3, 4, 5, 6, 7, 8, 9, 10} /* layer_1_nodes_configs */,
        {0, 1, 2, 3} /* layer_2_nodes_configs */,
        {0.001, 0.01, 0.1} /* learning_rate_configs */,
        {0.4, 0.8} /* momentum_configs */,
#endif
        {1, 2, 3} /* layer_1_nodes_configs */,
        {2, 3} /* layer_2_nodes_configs */, {0.1} /* learning_rate_configs */,
        {0.8} /* momentum_configs */,

        0.1 /* acceptable_rmse */ );
}

TEST_F( mlp_predictor_test, double_mean_squared_test ) {
    check_mlp_model(
        {-0.9, -0.9} /* x_min */, {0.9, 0.9} /* x_max */,
        mean_squared /* func */, 0.05 /* noise */, 100 /* num_iters */,
        100 /* num_samples */, {1, 1} /* max_input */,
        4294967296 /* max pred */, false /* is_static */,
#if 0
        {1, 2, 3, 4, 5, 6, 7, 8, 9, 10} /* layer_1_nodes_configs */,
        {0, 1, 2, 3} /* layer_2_nodes_configs */,
        {0.001, 0.01, 0.1} /* learning_rate_configs */,
        {0.4, 0.8} /* momentum_configs */,
#endif
        {1, 5, 10} /* layer_1_nodes_configs */,
        {0, 5} /* layer_2_nodes_configs */, {0.1} /* learning_rate_configs */,
        {0.8} /* momentum_configs */, 0.2 /* acceptable_rmse */ );
}

TEST_F( mlp_predictor_test, triple_mean_abs_sinc_test ) {
    check_mlp_model(
        {1, 1, 1} /* x_min */, {100, 100, 100} /* x_max */,
        mean_abs_sinc /* func */, 0.05 /* noise */, 100 /* num_iters */,
        100 /* num_samples */, {100, 100, 100} /* max_input */,
        4294967296 /* max pred */, false /* is_static */,
#if 0
        {1, 2, 3, 4, 5, 6, 7, 8, 9, 10} /* layer_1_nodes_configs */,
        {0, 1, 2, 3} /* layer_2_nodes_configs */,
        {0.001, 0.01, 0.1} /* learning_rate_configs */,
        {0.4, 0.8} /* momentum_configs */,
#endif
        {1, 5, 10} /* layer_1_nodes_configs */,
        {0, 3} /* layer_2_nodes_configs */, {0.01} /* learning_rate_configs */,
        {0.8} /* momentum_configs */, 0.2 /* acceptable_rmse */ );
}

TEST_F( mlp_predictor_test, quadruple_weight_test ) {
    check_mlp_model(
        {-1, -1, -1, -1} /* x_min */, {1, 1, 1, 1} /* x_max */,
        abs_weight /* func */, 0.05 /* noise */, 100 /* num_iters */,
        100 /* num_samples */, {1, 1, 1, 1} /* max_input */,
        4294967296 /* max pred */, false /* is_static */,
#if 0
        {1, 2, 3, 4, 5, 6, 7, 8, 9, 10} /* layer_1_nodes_configs */,
        {0, 1, 2, 3} /* layer_2_nodes_configs */,
        {0.001, 0.01, 0.1} /* learning_rate_configs */,
        {0.4, 0.8} /* momentum_configs */,
#endif
        {1, 4, 10} /* layer_1_nodes_configs */,
        {0, 3} /* layer_2_nodes_configs */, {0.1} /* learning_rate_configs */,
        {0.8} /* momentum_configs */, 0.2 /* acceptable_rmse */ );
}
