#include <gflags/gflags.h>
#include <glog/logging.h>
#include <glog/stl_logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <iostream>
#include <string>
#include <valarray>
#include <vector>

#include "../src/common/constants.h"
#include "../src/common/predictor/krls_predictor2.h"
#include "../src/common/predictor/mlp_predictor2.h"
#include "../src/common/predictor/multi_linear_predictor.h"
#include "../src/common/predictor/vector_util.h"
#include "../src/distributions/distributions.h"

class external_data_predictor_test : public ::testing::Test {};

struct external_data {
    std::string              timestamp_colname;
    std::vector<std::string> exp_var_colnames;
    std::string              resp_var_colname;

    std::vector<double>              timestamps;
    std::vector<std::vector<double>> raw_exp_vars;
    std::vector<double>              resp_vars;

    uint64_t size() const {
        uint64_t ret = timestamps.size();
        // ASSERT_EQ( ret, raw_exp_vars.size() );
        // ASSERT_EQ( ret, resp_vars.size() );
        return ret;
    }
};

void parse_external_data_from_csv( std::string filename, external_data* edat,
                                   uint32_t exp_var_size ) {

    std::ifstream fs( filename );
    if( fs.fail() ) {
        DLOG( ERROR ) << "file failed at path " << filename;
        FAIL();
    }

    std::string curr_line, curr_token;

    // get colnames
    std::getline( fs, curr_line );
    if( !fs ) {
        DLOG( ERROR ) << "fs is empty";
        FAIL();
    }

    std::istringstream colnames_iss( curr_line );

    std::getline( colnames_iss, curr_token, ',' );
    if( !colnames_iss ) {
        DLOG( ERROR ) << "cannot extract timestamp colname";
        FAIL();
    }
    edat->timestamp_colname = curr_token;

    for( long i = 0; i < exp_var_size; ++i ) {
        std::getline( colnames_iss, curr_token, ',' );
        if( !colnames_iss ) {
            DLOG( ERROR ) << "cannot extract expr_var colname at " << i;
            FAIL();
        }
        edat->exp_var_colnames.push_back( curr_token );
    }

    std::getline( colnames_iss, curr_token, ',' );
    if( !colnames_iss ) {
        DLOG( ERROR ) << "cannot extract resp_var colname";
        FAIL();
    }
    edat->resp_var_colname = curr_token;

    if( !colnames_iss.eof() ) {
        DLOG( ERROR ) << "colname line did not reach eof after parsing";
        FAIL();
    }

    for( long l = 1; std::getline( fs, curr_line ); ++l ) {
        std::istringstream line_iss( curr_line );

        std::getline( line_iss, curr_token, ',' );
        if( !line_iss ) {
            DLOG( ERROR ) << "cannot extract timestamp value at line " << l;
            FAIL();
        }
        edat->timestamps.push_back( std::stod( curr_token ) );

        std::vector<double> raw_exp_var;
        raw_exp_var.reserve( exp_var_size );
        for( long i = 0; i < exp_var_size; ++i ) {
            std::getline( line_iss, curr_token, ',' );
            if( !line_iss ) {
                DLOG( ERROR ) << "cannot extract expr_var value " << i
                              << " at line " << l;
                FAIL();
            }
            raw_exp_var.push_back( std::stod( curr_token ) );
        }
        edat->raw_exp_vars.push_back( std::move( raw_exp_var ) );

        std::getline( line_iss, curr_token, ',' );
        if( !line_iss ) {
            DLOG( ERROR ) << "cannot extract resp_var value at line " << l;
            FAIL();
        }
        edat->resp_vars.push_back( std::stod( curr_token ) );

        if( !line_iss.eof() ) {
            DLOG( ERROR ) << "line did not reach eof after parsing line " << l;
            FAIL();
        }
    }

    if( !fs.eof() ) {
        DLOG( ERROR ) << "file did not reach eof after parsing";
    }
}

TEST_F( external_data_predictor_test, parse_csv ) {
    external_data edat_dvec;
    uint32_t edat_num_cols = 3;
    parse_external_data_from_csv(
        "/hdd1/dyna-mast/Adapt-HTAP/analysis/data/cpu_util/cpu_util_site_0.csv",
        &edat_dvec, edat_num_cols );

    DLOG( INFO ) << "parsing data to dvec";
    DLOG( INFO ) << "\ntimestamp:\n\tcolname:" << edat_dvec.timestamp_colname
                 << "\n\t[0]:" << edat_dvec.timestamps[0]
                 << "\n\t[1]:" << edat_dvec.timestamps[1]
                 << "\n\t[2]:" << edat_dvec.timestamps[2]
                 << "\n\tsize:" << edat_dvec.timestamps.size();

    DLOG( INFO ) << "\nexp_var\n\tcolname:" << edat_dvec.exp_var_colnames
                 << "\n\t[0]:" << edat_dvec.raw_exp_vars[0]
                 << "\n\t[1]:" << edat_dvec.raw_exp_vars[1]
                 << "\n\t[2]:" << edat_dvec.raw_exp_vars[2]
                 << "\n\tsize:" << edat_dvec.raw_exp_vars.size();

    DLOG( INFO ) << "\nresp_var\n\tcolname:" << edat_dvec.resp_var_colname
                 << "\n\t[0]:" << edat_dvec.resp_vars[0]
                 << "\n\t[1]:" << edat_dvec.resp_vars[1]
                 << "\n\t[2]:" << edat_dvec.resp_vars[2]
                 << "\n\tsize:" << edat_dvec.resp_vars.size();

    external_data edat_valarray;
    parse_external_data_from_csv(
        "/hdd1/dyna-mast/Adapt-HTAP/analysis/data/cpu_util/cpu_util_site_0.csv",
        &edat_valarray, edat_num_cols );

    DLOG( INFO ) << "parsing data to vararray";
    DLOG( INFO ) << "\ntimestamp\n\tcolname:" << edat_valarray.timestamp_colname
                 << "\n\t[0]:" << edat_valarray.timestamps[0]
                 << "\n\t[1]:" << edat_valarray.timestamps[1]
                 << "\n\t[2]:" << edat_valarray.timestamps[2]
                 << "\n\tsize:" << edat_valarray.timestamps.size();

    DLOG( INFO ) << "\nexp_var\n\tcolname:" << edat_valarray.exp_var_colnames
                 << "\n\t[0]:" << edat_valarray.raw_exp_vars[0]
                 << "\n\t[1]:" << edat_valarray.raw_exp_vars[1]
                 << "\n\t[2]:" << edat_valarray.raw_exp_vars[2]
                 << "\n\tsize:" << edat_valarray.raw_exp_vars.size();

    DLOG( INFO ) << "\nresp_var\n\tcolname:" << edat_valarray.resp_var_colname
                 << "\n\t[0]:" << edat_valarray.resp_vars[0]
                 << "\n\t[1]:" << edat_valarray.resp_vars[1]
                 << "\n\t[2]:" << edat_valarray.resp_vars[2]
                 << "\n\tsize:" << edat_valarray.resp_vars.size();
}

double scale_resp_val( double resp ) { return ( resp / 100 ); }
double scale_pred_val( double pred ) { return ( 100 * pred ); }

double rmse_with_external_data( const external_data& edat,
                                const std::string&   label,
                                predictor<std::vector<double>, double>& p,
                                double   training_portion,
                                uint64_t update_model_freq,
                                uint32_t num_iters ) {

    distributions dist( nullptr );

    double   sse = 0;
    double   sae = 0;
    uint64_t num_validated = 0;
    uint64_t num_samples = edat.size();
    uint64_t num_train = 0;

    double total_train_time = 0;
    double total_infer_time = 0;

    for( uint32_t iter = 0; iter < num_iters; iter++ ) {
        for( uint64_t i = 0; i < num_samples; ++i ) {
            const std::vector<double>& exp_vals = edat.raw_exp_vars[i];
            const double               resp_val = edat.resp_vars[i];

            const double scaled_resp_val = scale_resp_val( resp_val );

            double v_rand = dist.get_uniform_double();
            if( v_rand > training_portion || i == num_samples - 1 ) {
                // validate - at least one observation is validated
                //
                std::chrono::high_resolution_clock::time_point start_time =
                    std::chrono::high_resolution_clock::now();

                double pred = scale_pred_val( p.make_prediction( exp_vals ) );

                std::chrono::high_resolution_clock::time_point end_time =
                    std::chrono::high_resolution_clock::now();

                std::chrono::duration<double, std::nano> elapsed =
                    end_time - start_time;
                double loc_infer_time = elapsed.count();

                DVLOG( 2 ) << "TIMED_PRED:" << label
                           << ", INFER:" << loc_infer_time;

                double err = pred - resp_val;

                sse += ( err * err );
                sae += abs( err );

                num_validated += 1;
                total_infer_time += loc_infer_time;

                DVLOG( 40 ) << "observation " << i
                            << " validated. exp_vals=" << exp_vals
                            << ", pred=" << pred << ", resp_val=" << resp_val
                            << ", err=" << err;

                DVLOG( 2 ) << "PRED_ERR:" << label << ", COUNT:" << i
                           << ", ERR:" << err << ", PRED:" << pred
                           << ", OBS:" << resp_val << ", input:" << exp_vals;
            }

            // train
            p.add_observation( exp_vals, scaled_resp_val );

            if( i % update_model_freq == ( update_model_freq - 1 ) ) {
                std::chrono::high_resolution_clock::time_point start_time =
                    std::chrono::high_resolution_clock::now();

                p.update_model();

                std::chrono::high_resolution_clock::time_point end_time =
                    std::chrono::high_resolution_clock::now();

                std::chrono::duration<double, std::nano> elapsed =
                    end_time - start_time;
                double loc_train_time = elapsed.count();

                DVLOG( 2 ) << "TIMED_PRED:" << label
                           << ", TRAIN:" << loc_train_time;

                total_train_time += loc_train_time;
                num_train += 1;
            }
        }
    }

    double rmse = sqrt( sse / num_validated );
    double mae = sae / num_validated;

    double train_time = total_train_time /  num_train;
    double infer_time = total_infer_time /  num_validated;

    DVLOG( 20 ) << "RMSE:" << rmse << ", MAE:" << mae;

    DLOG( INFO ) << "PREDICTOR:" << label << ", RMSE:" << rmse
                 << ", MAE:" << mae << ", TRAIN_TIME:" << train_time
                 << ", INFER_TIME:" << infer_time
                 << ", NUM_INFER:" << num_validated
                 << ", NUM_TRAIN:" << num_train;

    return rmse;
}

void check_radial_basis_krls_model_with_external_data(
    const external_data& edat, double learning_rate,
    const std::vector<double>& max_input, bool is_static,
    uint64_t max_internel_model_size, const std::vector<double>& kernel_gammas,
    double training_portion, uint64_t update_model_freq,
    double acceptable_rmse ) {

    double min_prediction = 1;
    double max_prediction = 5000;

    typedef dlib::radial_basis_kernel<d_var_vector> kernel_t;

    double least_rmse = INFINITY;
    double least_rmse_gamma = INFINITY;

    for( double gamma : kernel_gammas ) {
        kernel_t test_kernel( gamma );

        DVLOG( 20 ) << "testing for gamma=" << gamma << std::endl;

        krls_predictor2<kernel_t> p( learning_rate, max_input, is_static,
                                     test_kernel, max_internel_model_size,
                                     min_prediction, max_prediction );

        double rmse = rmse_with_external_data(
            edat, "KRLS", p, training_portion, update_model_freq, 1 );

        if( rmse < least_rmse ) {
            least_rmse = rmse;
            least_rmse_gamma = gamma;
        }
    }

    DLOG( INFO ) << "gamma value with least RMSE is: " << least_rmse_gamma
                 << ", RMSE=" << least_rmse;
    EXPECT_LE( least_rmse, acceptable_rmse );
}

TEST_F( external_data_predictor_test, krls_cpu_util_site_0 ) {
    constexpr long N = 3;

    external_data edat;
    parse_external_data_from_csv(
        "/hdd1/dyna-mast/Adapt-HTAP/analysis/data/cpu_util/cpu_util_site_0.csv",
        &edat, N );

    std::vector<double> kernel_gammas{
        0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005,
        0.01,     0.05,     0.1,     0.5,     1,      5,      10};

    check_radial_basis_krls_model_with_external_data(
        edat /* edat */, 0.001 /* learning_rate */,
        {1000, 1000, 1000} /* max_input */, false /* is_static */,
        500 /* max_internel_model_size */, kernel_gammas,
        0.6 /* training_portion */, 15 /* update_model_freq */,
        100 /* acceptable_rmse */ );
}

TEST_F( external_data_predictor_test, krls_cpu_util_site_1 ) {
    constexpr long N = 3;

    external_data edat;
    parse_external_data_from_csv(
        "/hdd1/dyna-mast/Adapt-HTAP/analysis/data/cpu_util/cpu_util_site_1.csv",
        &edat, N );

    std::vector<double> kernel_gammas{
        0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005,
        0.01,     0.05,     0.1,     0.5,     1,      5,      10};

    check_radial_basis_krls_model_with_external_data(
        edat /* edat */, 0.001 /* learning_rate */,
        {1000, 1000, 1000} /* max_input */, false /* is_static */,
        500 /* max_internel_model_size */, kernel_gammas,
        0.6 /* training_portion */, 15 /* update_model_freq */,
        100 /* acceptable_rmse */ );
}

TEST_F( external_data_predictor_test, krls_cpu_util_site_2 ) {
    constexpr long N = 3;

    external_data edat;
    parse_external_data_from_csv(
        "/hdd1/dyna-mast/Adapt-HTAP/analysis/data/cpu_util/cpu_util_site_2.csv",
        &edat, N );

    std::vector<double> kernel_gammas{
        0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005,
        0.01,     0.05,     0.1,     0.5,     1,      5,      10};

    check_radial_basis_krls_model_with_external_data(
        edat /* edat */, 0.001 /* learning_rate */,
        {1000, 1000, 1000} /* max_input */, false /* is_static */,
        500 /* max_internel_model_size */, kernel_gammas,
        0.6 /* training_portion */, 15 /* update_model_freq */,
        100 /* acceptable_rmse */ );
}

TEST_F( external_data_predictor_test, krls_cpu_util_site_3 ) {
    constexpr long N = 3;

    external_data edat;
    parse_external_data_from_csv(
        "/hdd1/dyna-mast/Adapt-HTAP/analysis/data/cpu_util/cpu_util_site_3.csv",
        &edat, N );

    std::vector<double> kernel_gammas{
        0.000001, 0.000005, 0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005,
        0.01,     0.05,     0.1,     0.5,     1,      5,      10};

    check_radial_basis_krls_model_with_external_data(
        edat /* edat */, 0.001 /* learning_rate */,
        {1000, 1000, 1000} /* max_input */, false /* is_static */,
        500 /* max_internel_model_size */, kernel_gammas,
        0.6 /* training_portion */, 15 /* update_model_freq */,
        100 /* acceptable_rmse */ );
}

TEST_F( external_data_predictor_test, multi_linear_cpu_util_site_3 ) {
    constexpr long N = 3;

    double min_prediction = 1;
    double max_prediction = 5000;

    external_data edat;
    parse_external_data_from_csv(
        "/hdd1/dyna-mast/Adapt-HTAP/analysis/data/cpu_util/cpu_util_site_3.csv",
        &edat, N );

    multi_linear_predictor p(
        {0, 0, 0} /* init_weight */, 0 /* init_bias */,
        0.01 /* regularization */, 0.001 /* bias regularization */,
        0.0001 /* learning rate */, {1000, 1000, 1000} /* max_inputs */,
        min_prediction, max_prediction, false /* is_static */ );

    double rmse =
        rmse_with_external_data( edat, "LP", p, 0.6 /* training_portion */,
                                 15 /* update_model_freq */, 1 );

    DLOG( INFO ) << "Learned Model: "
                 << "[ weight:" << p.weight() << ", bias:" << p.bias() << " ]";
    DLOG( INFO ) << "RMSE:" << rmse;
}

TEST_F( external_data_predictor_test, multi_linear_cpu_util_site_3_1 ) {
    constexpr long N = 3;

    double min_prediction = 1;
    double max_prediction = 5000;

    external_data edat;
    parse_external_data_from_csv(
        "/hdd1/dyna-mast/Adapt-HTAP/analysis/data/cpu_util/cpu_util_site_3.csv",
        &edat, N );

    multi_linear_predictor p(
        {0, 0, 0} /* init_weight */, 0 /* init_bias */,
        0.01 /* regularization */, 0.001 /* bias regularization */,
        0.00001 /* learning rate */, {1000, 1000, 1000} /* max_inputs */,
        min_prediction, max_prediction, false /* is_static */ );

    double rmse =
        rmse_with_external_data( edat, "LP", p, 0.6 /* training_portion */,
                                 15 /* update_model_freq */, 1 );

    DLOG( INFO ) << "Learned Model: "
                 << "[ weight:" << p.weight() << ", bias:" << p.bias() << " ]";
    DLOG( INFO ) << "RMSE:" << rmse;
}

TEST_F( external_data_predictor_test, mlp_cpu_util_site_3 ) {
    constexpr long N = 3;

    double min_prediction = 1;
    double max_prediction = 5000;

    external_data edat;
    parse_external_data_from_csv(
        "/hdd1/dyna-mast/Adapt-HTAP/analysis/data/cpu_util/cpu_util_site_3.csv",
        &edat, N );

    mlp_predictor2 p( false /* is_static */, {1000, 1000, 1000} /* max_input */,
                      3 /* layer_1_nodes */, 2 /* layer_2_nodes */,
                      0.001 /* learning_rate */, 0.8 /* momentum */,
                      min_prediction, max_prediction );

    double rmse =
        rmse_with_external_data( edat, "MLP", p, 0.6 /* training_portion */,
                                 15 /* update_model_freq */, 1 );

    DLOG( INFO ) << "Learned Model: ";
    DLOG( INFO ) << "RMSE:" << rmse;
}

void test_all_external_data( const std::string& csv ) {

    if ( k_skip_slow_tests ) {
        DLOG( WARNING ) << "Skipping test";
        return;
    }

    constexpr long N = 3;

    double   min_prediction = 1;
    double   max_prediction = 5000;

    double   training_portion = 0.6;
    uint64_t update_model_freq = 15;

    std::vector<double> max_inputs = {1000, 1000, 1000};
    double              learning_rate = 0.001;
    double              momentum = 0.8;

    double regularization = 0.01;
    double bias_regularization = 0.001;

    bool is_static = false;
    (void) is_static;

    DVLOG( 2 ) << "TEST RMSE, training_portion:" << training_portion
               << ", update_model_freq:" << update_model_freq
               << ", max_inputs:" << max_inputs
               << ", min_prediction:" << min_prediction
               << ", max_prediction:" << max_prediction
               << ", learning_rate:" << learning_rate
               << ", momentum:" << momentum
               << ", regularization:" << regularization
               << ", bias_regularization" << bias_regularization
               << ", CSV:" << csv;

    external_data edat;
    parse_external_data_from_csv( csv, &edat, N );

// #if 0
    // LP
    std::vector<std::vector<double>> init_weights = {{0, 0, 0}, {1, 1, 1}};
    std::vector<double>             init_biases = {0, 1};
    for( const auto& init_weight : init_weights ) {
        for( const auto& init_bias : init_biases ) {
            multi_linear_predictor p( init_weight, init_bias, regularization,
                                      bias_regularization, learning_rate,
                                      max_inputs, min_prediction,
                                      max_prediction, is_static );
            DVLOG( 2 ) << "LP: Init weight:" << init_weight
                       << ", init_bias:" << init_bias;
            double rmse = rmse_with_external_data(
                edat, "LP", p, training_portion, update_model_freq, 100 );
            DVLOG( 2 ) << "LP: Init weight:" << init_weight
                       << ", init_bias:" << init_bias << ", RMSE:" << rmse;
        }
    }
// #endif

// #if 0
    // MLP
    std::vector<uint64_t> layer_1_nodes = {3, 5, 10};
    std::vector<uint64_t> layer_2_nodes = {1, 3, 5};
    for( const auto& layer_1_node : layer_1_nodes ) {
        for( const auto& layer_2_node : layer_2_nodes ) {
            mlp_predictor2 p( is_static, max_inputs,
                              layer_1_node /* layer_1_nodes */,
                              layer_2_node /* layer_2_nodes */, learning_rate,
                              momentum, min_prediction, max_prediction );
            DVLOG( 2 ) << "MLP: Layer 1:" << layer_1_node
                       << ", Layer 2:" << layer_2_node;
            double rmse = rmse_with_external_data(
                edat, "MLP", p, training_portion, update_model_freq, 100 );
            DVLOG( 2 ) << "MLP: Layer 1:" << layer_1_node
                       << ", Layer 2:" << layer_2_node << ", RMSE:" << rmse;
        }
    }
//#endif

// #if 0

    // KRLS
    typedef dlib::radial_basis_kernel<d_var_vector> kernel_t;
    std::vector<double> kernel_gammas{0.00001, 0.0001, 0.001 };
    std::vector<uint64_t> max_internal_model_sizes = {5, 50, 500};

    for( const auto& gamma : kernel_gammas ) {
        for( const auto& max_internal_model_size : max_internal_model_sizes ) {
            kernel_t test_kernel( gamma );

            krls_predictor2<kernel_t> p( learning_rate, max_inputs, is_static,
                                         test_kernel, max_internal_model_size,
                                         min_prediction, max_prediction );

            DVLOG( 2 ) << "KRLS: Kernel Gamma:" << gamma
                       << ", max_internal_model_size:"
                       << max_internal_model_size;
            double rmse = rmse_with_external_data(
                edat, "KRLS", p, training_portion, update_model_freq, 100 );
            DVLOG( 2 ) << "KRLS: Kernel Gamma:" << gamma
                       << ", max_internal_model_size:"
                       << max_internal_model_size << ", RMSE:" << rmse;
        }
    }
// #endif

}

TEST_F( external_data_predictor_test, test_all_cpu_util_3 ) {
    test_all_external_data(
        "/hdd1/dyna-mast/Adapt-HTAP/analysis/data/cpu_util/cpu_util_site_3.csv" );
}
TEST_F( external_data_predictor_test, test_all_cpu_util_all ) {
    test_all_external_data(
        "/hdd1/dyna-mast/Adapt-HTAP/analysis/data/cpu_util/cpu_util_all_sites.csv" );
}
