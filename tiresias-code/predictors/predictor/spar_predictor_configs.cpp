#include "spar_predictor_configs.h"

#include <glog/logging.h>

spar_predictor_configs construct_spar_predictor_configs(
    uint32_t width, uint32_t slot, uint32_t training_interval,
    uint32_t rounding_factor_temporal, uint32_t rounding_factor_periodic,
    uint32_t predicted_arrival_window, double normalization_value, double gamma,
    double learning_rate, double init_weight, double init_bias,
    double regularization, double bias_regularization, uint32_t batch_size ) {

    spar_predictor_configs configs;
    configs.width_ = width;
    configs.slot_ = slot;
    configs.training_interval_ = training_interval;
    configs.rounding_factor_temporal_ = rounding_factor_temporal;
    configs.rounding_factor_periodic_ = rounding_factor_periodic;
    configs.predicted_arrival_window_ = predicted_arrival_window;
    configs.normalization_value_ = normalization_value;
    configs.gamma_ = gamma;
    configs.learning_rate_ = learning_rate;

    configs.init_weight_ = init_weight;
    configs.init_bias_ = init_bias;
    configs.regularization_ = regularization;
    configs.bias_regularization_ = bias_regularization;
    configs.batch_size_ = batch_size;

    DVLOG( 1 ) << "Created: " << configs;

    return configs;
}

std::ostream& operator<<( std::ostream&                 os,
                          const spar_predictor_configs& config ) {
    os << "SPAR Predictor Config: [Spar width: " << config.width_
       << ", Number of ms in a slot: " << config.slot_
       << ", Number of slots in training window: " << config.training_interval_
       << ", Granularity of time window for temporal (ms): "
       << config.rounding_factor_temporal_
       << ", Granularity of time window for periodic (ms): "
       << config.rounding_factor_periodic_
       << ", Predicted arrival window (slots): "
       << config.predicted_arrival_window_
       << ", Normalization value: " << config.normalization_value_
       << ", Gamma KRLS hyper-parameter: " << config.gamma_
       << ", KRLS learning rate: " << config.learning_rate_
       << ", Initial weight: " << config.init_weight_
       << ", Initial bias: " << config.init_bias_
       << ", Regularization: " << config.regularization_
       << ", Bias regularization: " << config.bias_regularization_
       << ", Batch size: " << config.batch_size_ << " ]";
    return os;
}
