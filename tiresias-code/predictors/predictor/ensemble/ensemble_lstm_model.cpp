#include "ensemble_lstm_model.h"

using namespace std;
using namespace torch::indexing;

ensemble_lstm_model::ensemble_lstm_model( long input_size, long hidden_size,
                                          long num_layers, long output_size )
    : lstm( torch::nn::LSTMOptions( input_size, hidden_size )
                .num_layers( num_layers )
                .batch_first( false ) ),
      fc( hidden_size, output_size ),
      input_size( input_size ),
      hidden_size( hidden_size ),
      output_size( output_size ),
      hidden_cell( make_tuple<torch::Tensor, torch::Tensor>(
          torch::zeros( {1, 1, hidden_size} ),
          torch::zeros( {1, 1, hidden_size} ) ) ) {
    register_module( "lstm", lstm );
    register_module( "fc", fc );
}

torch::Tensor ensemble_lstm_model::forward( torch::Tensor input ) {
    torch::Tensor output;
    std::tie( output, hidden_cell ) =
        lstm->forward( input.view( {input.size( 0 ), 1, -1} ), hidden_cell );
    auto prediction = fc->forward( output.view( {input.size( 0 ), -1} ) );
    return prediction.index( {prediction.size( 0 ) - 1} );
}
