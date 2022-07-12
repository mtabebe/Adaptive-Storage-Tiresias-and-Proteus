#include "ensemble_early_query_predictor_configs.h"

#include <glog/logging.h>

ensemble_early_query_predictor_configs
    construct_ensemble_early_query_predictor_configs(
        uint32_t slot, uint32_t search_window,
        uint32_t linear_training_interval, uint32_t rnn_training_interval,
        uint32_t krls_training_interval, double rnn_learning_rate,
        double krls_learning_rate, double outlier_threshold, double krls_gamma,
        uint32_t rnn_epoch_count, uint32_t rnn_sequence_size,
        uint32_t rnn_layer_count, bool apply_scheduled_outlier_model ) {

    ensemble_early_query_predictor_configs configs;
    configs.slot_ = slot;
    configs.search_window_ = search_window;
    configs.linear_training_interval_ = linear_training_interval;
    configs.rnn_training_interval_ = rnn_training_interval;
    configs.krls_training_interval_ = krls_training_interval;
    configs.rnn_learning_rate_ = rnn_learning_rate;
    configs.krls_learning_rate_ = krls_learning_rate;
    configs.outlier_threshold_ = outlier_threshold;
    configs.krls_gamma_ = krls_gamma;
    configs.rnn_epoch_count_ = rnn_epoch_count;
    configs.rnn_sequence_size_ = rnn_sequence_size;
    configs.rnn_layer_count_ = rnn_layer_count;
    configs.apply_scheduled_outlier_model_ = apply_scheduled_outlier_model;

    DVLOG( 1 ) << "Created: " << configs;

    return configs;
}

std::ostream& operator<<(
    std::ostream& os, const ensemble_early_query_predictor_configs& config ) {
    os << "Ensemble Predictor Config: [Slot size: " << config.slot_
       << ", Search Window: " << config.search_window_
       << ", Linear training interval: " << config.linear_training_interval_
       << ", RNN training interval: " << config.rnn_training_interval_
       << ", KRLS training interval: : " << config.krls_training_interval_
       << ", RNN learning rate: " << config.rnn_learning_rate_
       << ", KRLS learning rate: " << config.krls_learning_rate_
       << ", KRLS outlier threshold: " << config.outlier_threshold_
       << ", KRLS gamma: " << config.krls_gamma_
       << ", RNN epoch count: " << config.rnn_epoch_count_
       << ", RNN sequence size: " << config.rnn_sequence_size_
       << ", RNN sequence size: " << config.rnn_layer_count_
       << ", Apply scheduled outlier model: "
       << config.apply_scheduled_outlier_model_ << " ]";
    return os;
}
