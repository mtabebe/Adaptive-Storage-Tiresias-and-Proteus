#pragma once

#include "linear_predictor.h"

template <typename input_t>
linear_predictor<input_t>::internal_model::internal_model(
    const input_t& weight, double bias, uint64_t model_version )
    : weight_( weight ), bias_( bias ), model_version_( model_version ) {}

template <typename input_t>
linear_predictor<input_t>::linear_predictor(
    const input_t& init_weight, double init_bias, double regularization,
    double bias_regularization, double learning_rate, const input_t& max_input,
    double min_prediction_range, double max_prediction_range, bool is_static )
    : predictor<input_t, double>( learning_rate, max_input,
                                  min_prediction_range, max_prediction_range,
                                  is_static ),
      internal_model_( nullptr ),
      regularization_( regularization ),
      bias_regularization_( bias_regularization ) {
    internal_model_.store(
        std::make_shared<internal_model>( init_weight, init_bias, 0 ) );
}

template <typename input_t>
linear_predictor<input_t>::~linear_predictor() {}

template <typename input_t>
uint64_t linear_predictor<input_t>::model_version() const {
    return internal_model_.load()->model_version_;
}

template <typename input_t>
input_t linear_predictor<input_t>::weight() const {
    return internal_model_.load()->weight_;
}

template <typename input_t>
double linear_predictor<input_t>::bias() const {
    return internal_model_.load()->bias_;
}

template <typename input_t>
double linear_predictor<input_t>::regularization() const {
    return regularization_;
}

template <typename input_t>
double linear_predictor<input_t>::bias_regularization() const {
    return bias_regularization_;
}

template <typename input_t>
double linear_predictor<input_t>::make_prediction( const input_t& input ) const {
    std::shared_ptr<internal_model> model = internal_model_.load();
    DVLOG( 20 ) << "make prediction:" << input;
    return predict( model->weight_, model->bias_, input );
}

template <typename input_t>
void linear_predictor<input_t>::write( std::ostream& os ) const {
    std::shared_ptr<internal_model> model = internal_model_.load();
    os << "[ model_version_:" << model->model_version_
       << ", weight_:" << model->weight_ << ", bias_:" << model->bias_
       << ", regularization_:" << regularization_
       << ", bias_regularization_:" << bias_regularization_ << ", parent:";
    predictor<input_t, double>::write( os );
    os << " ]";
}
