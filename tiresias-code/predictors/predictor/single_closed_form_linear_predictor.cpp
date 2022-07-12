#include "single_closed_form_linear_predictor.h"

#include <stdint.h>

single_closed_form_linear_predictor::single_closed_form_linear_predictor(
    double init_weight, double init_bias, query_count max_input,
    bool is_static )
    : predictor( 0.0 /* learning rate */, max_input, 0, UINT64_MAX,
                 is_static ),
      weight_( init_weight ),
      bias_( init_bias ) {}

inline query_count single_closed_form_linear_predictor::make_prediction(
    const query_count& input ) const {
    if( weight_ * input + bias_ < 0 ) {
        return 0;
    }
    return weight_ * input + bias_;
}

single_closed_form_linear_predictor::~single_closed_form_linear_predictor() {}

void single_closed_form_linear_predictor::add_observations_to_model(
    const std::vector<observation>& obs ) {

    if( obs.size() == 0 ) {
        DVLOG( 40 ) << "update_model: no observations";
        return;
    }

    /**
     * Since we are solving linear regression with a single variate,
     * we can leverage a closed-form solution using normal equations.
     *
     * The technique leverages maximum likelihood estimates, and can
     * explicitly solve for the slope and bias.
     *
     * More concretely, we compute (where xb = x bar):
     *   Sxx: sum i = 1, ..., n of (xi - xb)^2
     *   Sxy: sum i = 1, ..., n of (xi - xb)(yi - yb)
     *
     *   w = Sxy / Sxx
     *   b = yb - wxb
     *
     * The resulting equation is: y = wi + b
     */

    double xb = 0, yb = 0;
    for( const observation& o : obs ) {
        xb += o.input_;
        yb += o.result_;
    }

    xb /= obs.size();
    yb /= obs.size();

    double sxx = 0, sxy = 0;

    for( const observation& o : obs ) {
        sxx += pow( ( o.input_ - xb ), 2 );
        sxy += ( o.input_ - xb ) * ( o.result_ - yb );
    }

    weight_ = sxy / sxx;
    bias_ = yb - weight_ * xb;
    model_version_++;

    DVLOG( 40 ) << "update_model: "
                << "total " << obs.size() << " samples observed"
                << ", new_weight:" << weight_ << ", new_bias:" << bias_
                << ", new_model_version:" << model_version_;
}
