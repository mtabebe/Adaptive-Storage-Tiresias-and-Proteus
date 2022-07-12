#include "multi_linear_predictor.h"

#include "predictor_util.h"

multi_linear_predictor::multi_linear_predictor(
    const std::vector<double>& init_weight, double init_bias,
    double regularization, double bias_regularization, double learning_rate,
    const std::vector<double>& max_input, double min_prediction_range,
    double max_prediction_range, bool is_static )
    : linear_predictor( init_weight, init_bias, regularization,
                        bias_regularization, learning_rate, max_input,
                        min_prediction_range, max_prediction_range, is_static ),
      size_( init_weight.size() ) {
    DCHECK_EQ( max_input.size(), size_ );
}

inline double multi_linear_predictor::predict(
    const std::vector<double>& w, double b,
    const std::vector<double>& i ) const {
    DVLOG( 20 ) << "MLP predict";

    DCHECK_EQ( w.size(), i.size() );
    double w_times_i = 0;
    for( uint32_t pos = 0; pos < w.size(); pos++) {
        w_times_i = w_times_i + ( w.at( pos ) * i.at( pos ) );
    }
    double predict = w_times_i + b;
    return predict;
}

multi_linear_predictor::~multi_linear_predictor() {}

void multi_linear_predictor::add_observations_to_model(
    const std::vector<observation>& obs ) {

#if 0
    std::vector<observation> obs;
    {
        const std::lock_guard<std::mutex> lock(observations_mutex_);
        std::swap(obs, observations_);
    }
#endif

    auto old_model = internal_model_.load();

    std::vector<double> old_weight = old_model->weight_;
    double                old_bias = old_model->bias_;

    uint64_t              num_observed = 0;
    std::vector<double>   weight_deriv( size_, 0.0 );
    double                bias_deriv = 0;

    DCHECK_EQ( old_weight.size(), weight_deriv.size() );

    for( const observation& o : obs ) {
        double prediction = predict( old_weight, old_bias, o.input_ );
        double rescaled = scale_prediction_to_range(
            prediction, min_prediction_range_, max_prediction_range_ );
        double err = o.result_ - rescaled;

        std::vector<double> min_input = o.input_;
        for( uint32_t i = 0; i < min_input.size(); ++i ) {
            min_input[i] = std::min( min_input[i], max_input_[i] );
            weight_deriv[i] += (double) -2 * min_input[i] * err;
        }

        bias_deriv += -2 * err;
        num_observed += 1;
    }

    if( num_observed == 0 ) {
        DVLOG( 40 ) << "update_model: no observations";
        return;
    }

    uint64_t new_model_version = old_model->model_version_ + 1;

    std::vector<double> new_weight = old_weight;
    DCHECK_EQ( weight_deriv.size(), old_weight.size() );

    for (uint32_t pos = 0; pos < new_weight.size(); pos++) {
        double regularized_weight_deriv =
            weight_deriv.at( pos ) + ( regularization_ * old_weight.at( pos ) );
        new_weight.at( pos ) =
            old_weight.at( pos ) -
            ( regularized_weight_deriv / (double) num_observed ) *
                learning_rate_;

#if 0
    std::vector<double> regularized_weight_deriv =
        weight_deriv + ( regularization_ * old_weight );
    std::vector<double> new_weight =
        old_weight -
        ( regularized_weight_deriv / (double) num_observed ) * learning_rate_;
#endif
    }

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
