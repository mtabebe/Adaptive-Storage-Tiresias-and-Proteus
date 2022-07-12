#include "single_linear_predictor.h"

#include "predictor_util.h"

single_linear_predictor::single_linear_predictor(
    double init_weight, double init_bias, double learning_rate,
    double regularization, double bias_regularization, double max_input,
    double min_prediction_range, double max_prediction_range, bool is_static )
    : linear_predictor( init_weight, init_bias, regularization,
                        bias_regularization, learning_rate, max_input,
                        min_prediction_range, max_prediction_range,
                        is_static ) {}

single_linear_predictor::~single_linear_predictor() {}

inline double single_linear_predictor::predict( const double& w, double b,
                                                const double& i ) const {
  DVLOG(20) << "SLP predict";
    return w * i + b;
}

#if 0  // MTODO
void single_linear_predictor::update_model() {
    if( is_static_ ) {
        return;
    }

    std::vector<observation> obs;
    {
        const std::lock_guard<std::mutex> lock(observations_mutex_);
        std::swap(obs, observations_);
    }

    auto old_model = internal_model_.load();
}
#endif
void single_linear_predictor::add_observations_to_model(
    const std::vector<observation>& obs ) {

    if( is_static_ ) {
        return;
    }

    auto   old_model = internal_model_.load();
    double old_weight = old_model->weight_;
    double old_bias = old_model->bias_;

    uint64_t num_observed = 0;
    double   weight_deriv = 0;
    double   bias_deriv = 0;

    for( const observation& o : obs ) {
        double prediction = predict( old_weight, old_bias, o.input_ );
        double rescaled = scale_prediction_to_range(
            prediction, min_prediction_range_, max_prediction_range_ );
        double err = o.result_ - rescaled;

        weight_deriv += -2 * std::min( o.input_, max_input_ ) * err;
        bias_deriv += -2 * err;
        num_observed += 1;
    }

    if( num_observed == 0 ) {
        DVLOG( 40 ) << "update_model: no observations";
        return;
    }

    uint64_t new_model_version = old_model->model_version_ + 1;

    double regularized_weight_deriv =
        weight_deriv + ( regularization_ * old_weight );
    double new_weight =
        old_weight -
        ( regularized_weight_deriv / num_observed ) * learning_rate_;

    double regularized_bias_deriv =
        bias_deriv + ( bias_regularization_ * old_bias );
    double new_bias =
        old_bias - ( regularized_bias_deriv / num_observed ) * learning_rate_;

    DVLOG( 40 ) << "update_model: "
                << "total " << num_observed << " samples observed"
                << ", weight_deriv:" << weight_deriv
                << ", bias_deriv:" << bias_deriv
                << ", new_weight:" << new_weight << ", new_bias:" << new_bias
                << ", new_model_version:" << new_model_version;

    internal_model_.store( std::make_shared<internal_model>(
        new_weight, new_bias, new_model_version ) );
}
