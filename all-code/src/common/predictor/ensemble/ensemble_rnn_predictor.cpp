#include "ensemble_rnn_predictor.h"
#include "ensemble_lstm_model.h"

ensemble_rnn_predictor::ensemble_rnn_predictor(
    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time,
        std::shared_ptr<std::unordered_map<query_id, query_count>>>>&
                                                 query_observations,
    const ensemble_early_query_predictor_configs configs,
    query_count                                  max_query_count )
    : ensemble_predictor( query_observations, configs, max_query_count ),
      raw_lstm_model_( std::make_unique<ensemble_lstm_model>(
          1.0, configs_.rnn_layer_count_, 1.0, 1.0 ) ),
      lstm_model_{std::move( raw_lstm_model_ )} {}

ensemble_rnn_predictor::ensemble_rnn_predictor(
    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time,
        std::shared_ptr<std::unordered_map<query_id, query_count>>>>&
                query_observations,
    query_count max_query_count )
    : ensemble_predictor( query_observations, max_query_count ),
      raw_lstm_model_( std::make_unique<ensemble_lstm_model>(
          1.0, configs_.rnn_layer_count_, 1.0, 1.0 ) ),
      lstm_model_( std::move( raw_lstm_model_ ) ) {}

query_count ensemble_rnn_predictor::get_estimated_query_count(
    query_id q_id, epoch_time time ) {
    epoch_time current_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch() )
            .count();

    return get_estimated_query_count( q_id, time, current_time );
}

query_count ensemble_rnn_predictor::get_estimated_query_count(
    query_id q_id, epoch_time time, epoch_time current_time ) {
    using namespace torch::indexing;
    LOG( INFO ) << "Obtaining prediction for: " << time << " at time "
                << current_time;

    auto rounding_factor = configs_.slot_;
    time -= time % rounding_factor;
    current_time -= current_time % rounding_factor;

    // Trivial look-up in map since it's historical
    if( time < current_time ) {
        return this->get_historic_load( q_id, time );
    }

    // Since we need to provide a sequence to the LSTM model, we may
    // have a gap of known "query counts" between the time requested and
    // the current time. As a result, we need to interpolate the values
    // between current_time and the requested time.
    //
    // |-------------------|----------------|????????|
    //                     ^ (1) training   ^        ^ (3) requested time
    //                       interval       (2) current time
    //
    // Hence in this case we would make a sequence of size between (1) and (2)
    // and then keep expanding it until it reaches (3).
    //
    // Note: Potential room for optimization by using double-ended deque and
    // re-using the buffer for the sequences

    auto& lstm_model = ( *lstm_model_.wlock() );

    const uint32_t rnn_time_range =
        configs_.slot_ * configs_.rnn_training_interval_;
    auto input_data_start = current_time - rnn_time_range;

    if( current_time < rnn_time_range ) {
        LOG( INFO ) << "Cannot have the current time of  " << current_time
                    << " be less than the rnn training time range - there is "
                       "unsufficient training samples.";
    }

    // Step #1: Construct the input sequence to the LSTM model. We assume
    // that we can retrieve data with query counts into the buffer, and then
    // incrementally increase the buffer; corresponds to (1) to (2) in the
    // diagram.

    std::vector<float> query_count_buffer;
    for( epoch_time t = input_data_start; t < current_time;
         t += configs_.slot_ ) {
        query_count_buffer.push_back( this->get_historic_load( q_id, t ) );
        LOG( INFO ) << "Prediction added: " << query_count_buffer.back()
                    << " for " << t;
    }

    auto options = torch::TensorOptions().dtype( at::kFloat );

    // Step #2: Incrementally bridge the gap between the current time and the
    // requested time; corresponds to going from  (2) to (3) in the diagram.
    for( epoch_time t = current_time; t <= time; t += configs_.slot_ ) {
        auto seq = torch::from_blob(
            query_count_buffer.data(),
            {static_cast<long int>( query_count_buffer.size() )}, options );

        lstm_model->hidden_cell = std::tuple<torch::Tensor, torch::Tensor>(
            torch::zeros( {1, 1, lstm_model->hidden_size} ),
            torch::zeros( {1, 1, lstm_model->hidden_size} ) );
        query_count_buffer.push_back(
            lstm_model->forward( seq ).item<float>() );
        LOG( INFO ) << "Prediction created: " << query_count_buffer.back();
    }

    return query_count_buffer.back();
}

void ensemble_rnn_predictor::train( query_id q_id, epoch_time current_time ) {
    using namespace torch::indexing;

    auto&          lstm_model = ( *lstm_model_.wlock() );
    const uint32_t rounding_factor = configs_.slot_;
    const uint32_t rnn_time_range =
        rounding_factor * configs_.rnn_training_interval_;

    const uint32_t sequence_size = configs_.rnn_sequence_size_;
    const double   learning_rate = configs_.rnn_learning_rate_;
    current_time -= ( current_time % rounding_factor );

    std::vector<float> raw_data;
    std::vector<std::pair<torch::Tensor, torch::Tensor>> training_sets;

    for( epoch_time t = current_time - rnn_time_range; t < current_time;
         t += rounding_factor ) {
        raw_data.push_back( this->get_historic_load( q_id, t ) );
    }

    auto options = torch::TensorOptions().dtype( at::kFloat );

    // Training data contains a vector of data-points, while training sets
    // contains
    // tensors with sequences and resulting expected output
    auto training_data =
        torch::from_blob( raw_data.data(),
                          {static_cast<long int>( raw_data.size() )}, options )
            .view( {-1} );

    for( size_t x = 0; x < raw_data.size() - sequence_size - 1; x++ ) {
        if( raw_data.size() < sequence_size + 1 ) {
            LOG( INFO ) << "Insufficient data-points to train RNN: expected "
                        << sequence_size + 1 << " and found "
                        << raw_data.size();
            break;
        }

        torch::Tensor t1 =
            training_data.index( {Slice( x, x + sequence_size )} );
        torch::Tensor t2 = training_data.index(
            {Slice( x + sequence_size, x + sequence_size + 1 )} );
        auto pair_ = std::pair<torch::Tensor, torch::Tensor>( t1, t2 );
        training_sets.push_back( pair_ );
    }

    torch::optim::Adam optimizer( lstm_model->parameters(),
                                  torch::optim::AdamOptions( learning_rate ) );
    torch::nn::MSELoss loss_function;
    lstm_model->train();

    size_t epoch_count = configs_.rnn_epoch_count_;
    for( size_t i = 0; i < epoch_count; i++ ) {
        for( auto p : training_sets ) {
            optimizer.zero_grad();
            lstm_model->hidden_cell = std::tuple<torch::Tensor, torch::Tensor>(
                torch::zeros( {1, 1, lstm_model->hidden_size} ),
                torch::zeros( {1, 1, lstm_model->hidden_size} ) );
            auto pred = lstm_model->forward( p.first );
            auto single_loss = loss_function( pred, p.second );
            single_loss.backward();
            optimizer.step();
        }
    }

    lstm_model->eval();
}

void ensemble_rnn_predictor::add_observation( query_id   observed_query_id,
                                              epoch_time time ) {
    /* Do nothing, handled by ensemble_early_query_predictor */
}
