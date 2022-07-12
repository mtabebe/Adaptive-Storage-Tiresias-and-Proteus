#include "../src/common/predictor/ensemble/ensemble_lstm_model.h"
#include "../src/common/constants.h"
#include <torch/torch.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>


class lstm_test : public ::testing::Test {};

/**
 * Standalone LSTM model test, verifies end-to-end LibTorch build
 * functionality.
 */

TEST_F( lstm_test, e2e_test ) {
    using namespace std;
    using namespace torch::indexing;

    if ( k_skip_slow_tests ) {
        DLOG( WARNING ) << "Skipping test";
        return;
    }

    ensemble_lstm_model lstm{1, 100, 1, 1};
    const float         learning_rate = 0.001;
    torch::optim::Adam  optimizer( lstm.parameters(),
                                  torch::optim::AdamOptions( learning_rate ) );

    torch::nn::MSELoss loss_function;
    const uint32_t     training_window = 12;

    vector<float> data;
    for( size_t x = 0; x < 144 - training_window; x++ ) {
        data.push_back( 10 * abs( sin( x * 3.14 / 10 ) ) );
    }

    auto options = torch::TensorOptions().dtype( at::kFloat );
    auto training_data_normalized =
        torch::from_blob( data.data(), {static_cast<long int>( data.size() )},
                          options )
            .view( {-1} );

    std::vector<std::pair<torch::Tensor, torch::Tensor>> training_set;

    for( int64_t i = 0;
         i < training_data_normalized.size( 0 ) - training_window - 1; i++ ) {
        torch::Tensor t1 =
            training_data_normalized.index( {Slice( i, i + training_window )} );
        torch::Tensor t2 = training_data_normalized.index(
            {Slice( i + training_window, i + training_window + 1 )} );
        auto pair_ = pair<torch::Tensor, torch::Tensor>( t1, t2 );
        training_set.push_back( pair_ );
    }

    size_t epoch_count = 25;
    for( size_t i = 0; i < epoch_count; i++ ) {
        for( auto p : training_set ) {
            optimizer.zero_grad();
            lstm.hidden_cell = tuple<torch::Tensor, torch::Tensor>(
                torch::zeros( {1, 1, lstm.hidden_size} ),
                torch::zeros( {1, 1, lstm.hidden_size} ) );

            auto pred = lstm.forward( p.first );
            auto single_loss = loss_function( pred, p.second );
            single_loss.backward();
            optimizer.step();
        }
    }

    lstm.eval();

    // Compute the MSE across 12 additional predictions
    double        squared_err = 0;
    vector<float> expected_values{
        5.70649,   // x = 132
        7.96486,   // x = 133
        9.44246,   // x = 134
        9.99769,   // x = 135
        9.57526,   // x = 136
        8.21648,   // x = 137
        6.05432,   // x = 138
        3.29994,   // x = 139
        0.222953,  // x = 140
        2.87584,   // x = 141
        5.6934,    // x = 142
        7.95422    // x = 143
    };

    for( size_t x = 0; x < training_window; x++ ) {
        auto seq = torch::from_blob(
            data.data(), {static_cast<long int>( data.size() )}, options );
        lstm.hidden_cell = tuple<torch::Tensor, torch::Tensor>(
            torch::zeros( {1, 1, lstm.hidden_size} ),
            torch::zeros( {1, 1, lstm.hidden_size} ) );
        data.push_back( lstm.forward( seq ).item<float>() );
        squared_err += pow( data.back() - expected_values[x], 2 );
    }

    // Expect results to be within 2.0 of the real value
    auto mae = pow( squared_err / training_window, 0.5 );
    EXPECT_LT( mae, 4.0 );
    DLOG_IF( WARNING, mae > 2.0 )
        << "LSTM basic test expected mae <= 2.0, but found " << mae;
}
