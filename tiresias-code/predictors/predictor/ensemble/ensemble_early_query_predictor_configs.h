#pragma once
#include <iostream>

#include "../../constants.h"

class ensemble_early_query_predictor_configs {
   public:
    uint32_t slot_;
    uint32_t search_window_;
    uint32_t linear_training_interval_;
    uint32_t rnn_training_interval_;
    uint32_t krls_training_interval_;
    double   rnn_learning_rate_;
    double   krls_learning_rate_;
    double   outlier_threshold_;
    double   krls_gamma_;
    uint32_t rnn_epoch_count_;
    uint32_t rnn_sequence_size_;
    uint32_t rnn_layer_count_;
    bool     apply_scheduled_outlier_model_;
};

ensemble_early_query_predictor_configs
    construct_ensemble_early_query_predictor_configs(
        uint32_t slot_ = k_ensemble_slot,
        uint32_t search_window_ = k_ensemble_search_window,
        uint32_t linear_training_interval_ =
            k_ensemble_linear_training_interval,
        uint32_t rnn_training_interval_ = k_ensemble_rnn_training_interval,
        uint32_t krls_training_interval_ = k_ensemble_krls_training_interval,
        double   rnn_learning_rate_ = k_ensemble_rnn_learning_rate,
        double   krls_learning_rate_ = k_ensemble_krls_learning_rate,
        double   outlier_threshold_ = k_ensemble_outlier_threshold,
        double   krls_gamma_ = k_ensemble_krls_gamma,
        uint32_t rnn_epoch_count_ = k_ensemble_rnn_epoch_count,
        uint32_t rnn_sequence_size_ = k_ensemble_rnn_sequence_size,
        uint32_t rnn_layer_count_ = k_ensemble_rnn_layer_count,
        bool     apply_scheduled_outlier_model_ =
            k_ensemble_apply_scheduled_outlier_model );

std::ostream& operator<<(
    std::ostream& os, const ensemble_early_query_predictor_configs& configs );
