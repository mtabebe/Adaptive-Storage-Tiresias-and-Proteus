#pragma once
#include <iostream>

#include "../constants.h"

class spar_predictor_configs {
   public:
    uint32_t width_;
    uint32_t slot_;
    uint32_t training_interval_;
    uint32_t rounding_factor_temporal_;
    uint32_t rounding_factor_periodic_;
    uint32_t predicted_arrival_window_;
    double   normalization_value_;
    double   gamma_;
    double   learning_rate_;
    double   init_weight_;
    double   init_bias_;
    double   regularization_;
    double   bias_regularization_;
    uint32_t batch_size_;
};

spar_predictor_configs construct_spar_predictor_configs(
    uint32_t width_ = k_spar_width, uint32_t slot_ = k_spar_slot,
    uint32_t training_interval_ = k_spar_training_interval,
    uint32_t rounding_factor_temporal_ = k_spar_rounding_factor_temporal,
    uint32_t rounding_factor_periodic_ = k_spar_rounding_factor_periodic,
    uint32_t predicted_arrival_window_ = k_spar_predicted_arrival_window,
    double   normalization_value_ = k_spar_normalization_value,
    double gamma_ = k_spar_gamma, double learning_rate_ = k_spar_learning_rate,
    double   init_weight_ = k_spar_init_weight,
    double   init_bias_ = k_spar_init_bias,
    double   regularization_ = k_spar_regularization,
    double   bias_regularization_ = k_spar_bias_regularization,
    uint32_t batch_size_ = k_spar_batch_size );

std::ostream& operator<<( std::ostream&                 os,
                          const spar_predictor_configs& configs );
