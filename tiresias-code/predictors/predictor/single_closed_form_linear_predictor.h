#pragma once
#include "predictor.h"

typedef uint64_t query_count;

class single_closed_form_linear_predictor
    : public predictor<query_count, query_count> {
    double weight_ = 0, bias_ = 0, model_version_ = 0;

   public:
    single_closed_form_linear_predictor( double init_weight, double init_bias,
                                         query_count max_input,
                                         bool        is_static );
    ~single_closed_form_linear_predictor();
    query_count make_prediction( const query_count& input ) const override;

    uint64_t model_version() const override { return model_version_; };

    double weight() const { return weight_; }
    double bias() const { return bias_; }

   protected:
    void add_observations_to_model(
        const std::vector<observation>& obs ) override;
};
