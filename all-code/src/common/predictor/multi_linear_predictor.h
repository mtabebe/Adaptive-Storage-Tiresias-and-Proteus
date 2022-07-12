#pragma once

#include "linear_predictor.h"
#include <vector>

class multi_linear_predictor : public linear_predictor<std::vector<double>> {
   public:
    multi_linear_predictor( const std::vector<double>& init_weight,
                            double init_bias, double regularization,
                            double bias_regularization, double learning_rate,
                            const std::vector<double>& max_input,
                            double                     min_prediction_range,
                            double max_prediction_range, bool is_static );
    virtual ~multi_linear_predictor();

    uint32_t size() const;

   protected:
    double predict( const std::vector<double>& w, double b,
                    const std::vector<double>& i ) const override;
    void add_observations_to_model(
        const std::vector<observation>& obs ) override;

   private:
    uint32_t size_;
};
