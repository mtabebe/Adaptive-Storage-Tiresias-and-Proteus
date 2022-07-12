#pragma once

#include "linear_predictor.h"

class single_linear_predictor : public linear_predictor<double> {
   public:
    single_linear_predictor( double init_weight, double init_bias,
                             double regularization, double bias_regularization,
                             double learning_rate, double max_input,
                             double min_prediction_range,
                             double max_prediction_range, bool is_static );
    virtual ~single_linear_predictor();

   protected:
    void add_observations_to_model(
        const std::vector<observation>& obs ) override;

    double predict( const double& w, double b, const double& i ) const override;
};

