#pragma once

#include "predictor.h"

#include <glog/logging.h>

template <typename input_t, typename result_t>
predictor<input_t, result_t>::observation::observation( const input_t& input,
                                                        const result_t& result )
    : input_( input ), result_( result ) {}

template <typename input_t, typename result_t>
predictor<input_t, result_t>::predictor( double learning_rate,
                                         const input_t& max_input,
                                         result_t       min_prediction_range,
                                         result_t       max_prediction_range,
                                         bool           is_static )
    : is_static_( is_static ),
      learning_rate_( learning_rate ),
      max_input_( max_input ),
      min_prediction_range_( min_prediction_range ),
      max_prediction_range_( max_prediction_range ),
      observations_(),
      observations_mutex_() {}

template <typename input_t, typename result_t>
predictor<input_t, result_t>::~predictor() {}

template <typename input_t, typename result_t>
bool predictor<input_t, result_t>::is_static() const {
    return is_static_;
}

template <typename input_t, typename result_t>
double predictor<input_t, result_t>::learning_rate() const {
    return learning_rate_;
}

template <typename input_t, typename result_t>
input_t predictor<input_t, result_t>::max_input() const {
    return max_input_;
}

template <typename input_t, typename result_t>
void predictor<input_t, result_t>::add_observation( const input_t& input,
                                                    const result_t& res ) {
    if( is_static_ ) {
        return;
    }
    const std::lock_guard<std::mutex> lock( observations_mutex_ );
    observations_.emplace_back( input, res );
}

template <typename input_t, typename result_t>
void predictor<input_t, result_t>::update_model() {
    if( is_static_ ) {
        return;
    }
    std::vector<observation> obs;
    {
        const std::lock_guard<std::mutex> lock( observations_mutex_ );
        std::swap( obs, observations_ );
    }
    add_observations_to_model(obs);
}

template <typename input_t, typename result_t>
void predictor<input_t, result_t>::set_static( bool is_static ) {
    is_static_ = is_static;
}

template <typename input_t, typename result_t>
void predictor<input_t, result_t>::write( std::ostream& os ) const {
    os << "[ is_static_:" << is_static_ << ", learning_rate_:" << learning_rate_
       << ", max_input_:" << max_input_
       << ", min_prediction_range_:" << min_prediction_range_
       << ", max_prediction_range_:" << max_prediction_range_ << " ]";
}

template <typename I, typename R>
std::ostream& operator<<( std::ostream& os, const predictor<I, R>& p ) {
    p.write( os );
    return os;
}
