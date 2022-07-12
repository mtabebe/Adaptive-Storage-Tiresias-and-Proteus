#define GTEST_HAS_TR1_TUPLE 0

#include <dlib/svm.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/common/predictor/krls_predictor2.h"
#include "predictor_test_utils2.h"

class krls_predictor2_test : public ::testing::Test {};

void check_radial_basis_krls2_model(
    const std::vector<double>& x_min, const std::vector<double>& x_max,
    double ( *func )( const std::vector<double>& ), double noise, int num_iters,
    int num_samples, double learning_rate, const std::vector<double>& max_input,
    double max_pred_range, bool is_static, uint32_t max_internel_model_size,
    const std::vector<double>& kernel_gammas, double acceptable_rmse ) {

    typedef dlib::radial_basis_kernel<d_var_vector> kernel_t;

    double least_rmse = INFINITY;
    double least_rmse_gamma = INFINITY;

    for( double gamma : kernel_gammas ) {
        kernel_t test_kernel( gamma );

        DVLOG( 1 ) << "testing for gamma=" << gamma;

        krls_predictor2<kernel_t> p( learning_rate, max_input, is_static,
                                     test_kernel, max_internel_model_size,
                                     -max_pred_range, max_pred_range );

        double rmse = test_dlib2_model( p, x_min, x_max, func, noise, num_iters,
                                        num_samples );

        DVLOG( 1 ) << "testing for gamma=" << gamma << ", rmse:" << rmse;

        if( rmse < least_rmse ) {
            least_rmse = rmse;
            least_rmse_gamma = gamma;
        }
    }

    DVLOG( 1 ) << "gamma value with least RMSE is: " << least_rmse_gamma
               << ", RMSE=" << least_rmse;
    EXPECT_LE( least_rmse, acceptable_rmse );
}

TEST_F( krls_predictor2_test, single_sigmoid_test ) {
#if 0 // SIMPLIFY
    std::vector<double> kernel_gammas{0.0001, 0.0005, 0.001, 0.005,
                                      0.006,  0.007,  0.008, 0.009,
                                      0.01,   0.05,   0.1,   0.5};
#endif
    std::vector<double> kernel_gammas{0.0001, 0.007, 0.01};

    check_radial_basis_krls2_model(
        {-100} /* x_min */, {100} /* x_max */, sigmoid /* func */,
        0.05 /* noise */, 10 /* num_iters */, 100 /* num_samples */,
        0.001 /* learning_rate */, {100} /* max_input */,
        4294967296 /* max pred */, false /* is_static */,
        100 /* max_internel_model_size */, kernel_gammas,
        0.1 /* acceptable_rmse */ );
}

TEST_F( krls_predictor2_test, double_mean_squared_test ) {
#if 0 // SIMPLIFY
    std::vector<double> kernel_gammas{
        0.0001, 0.0003, 0.0005, 0.0008, 0.001, 0.003, 0.005, 0.008, 0.01,
        0.05,   0.1,    0.5,    1,      2,     5,     10,    20};
#endif
    std::vector<double> kernel_gammas{0.5, 5, 20};

    check_radial_basis_krls2_model(
        {-0.9, -0.9} /* x_min */, {0.9, 0.9} /* x_max */,
        mean_squared /* func */, 0.05 /* noise */, 10 /* num_iters */,
        100 /* num_samples */, 0.001 /* learning_rate */,
        {1, 1} /* max_input */, 4294967296 /* max pred */,
        false /* is_static */, 500 /* max_internel_model_size */, kernel_gammas,
        0.1 /* acceptable_rmse */ );
}

TEST_F( krls_predictor2_test, triple_mean_abs_sinc_test ) {
#if 0
    std::vector<double> kernel_gammas{0.0001, 0.0003, 0.0005, 0.0008,
                                      0.001,  0.003,  0.005,  0.008,
                                      0.01,   0.05,   0.1,    0.5};
#endif
    std::vector<double> kernel_gammas{0.0001, 0.0003, 0.0005};

    check_radial_basis_krls2_model(
        {1, 1, 1} /* x_min */, {100, 100, 100} /* x_max */,
        mean_abs_sinc /* func */, 0.05 /* noise */, 10 /* num_iters */,
        100 /* num_samples */, 0.001 /* learning_rate */,
        {100, 100, 100} /* max_input */, 4294967296 /* max pred */,
        false /* is_static */, 500 /* max_internel_model_size */, kernel_gammas,
        0.1 /* acceptable_rmse */ );
}

TEST_F( krls_predictor2_test, quadruple_weight_test ) {
#if 0  // SIMPLIFY
      std::vector<double> kernel_gammas{
          0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005,
          0.001,    0.005,    0.01,    0.05,    0.1,    0.5,
          1,        2,        5,       10,      20};
#endif

    std::vector<double> kernel_gammas{0.000001, 0.5};

    check_radial_basis_krls2_model(
        {-1, -1, -1, -1} /* x_min */, {1, 1, 1, 1} /* x_max */,
        abs_weight /* func */, 0.05 /* noise */, 10 /* num_iters */,
        100 /* num_samples */, 0.001 /* learning_rate */,
        {1, 1, 1, 1} /* max_input */, 4294967296 /* max pred */,
        false /* is_static */, 500 /* max_internel_model_size */, kernel_gammas,
        10 /* acceptable_rmse */ );
}
