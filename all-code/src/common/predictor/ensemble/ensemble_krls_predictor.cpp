#include "ensemble_krls_predictor.h"

#include <float.h>

ensemble_krls_predictor::ensemble_krls_predictor(
    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time,
        std::shared_ptr<std::unordered_map<query_id, query_count>>>>&
                                                 query_observations,
    const ensemble_early_query_predictor_configs configs,
    query_count                                  max_query_count )
    : ensemble_predictor( query_observations, configs, max_query_count ),
      raw_krls_predictor_( std::make_unique<krls_predictor<1>>(
          configs_.krls_learning_rate_, max_input_, false, kernel_,
          k_predictor_max_internal_model_size, -DBL_MAX, DBL_MAX ) ),
      krls_predictor_( std::move( raw_krls_predictor_ ) ) {}

ensemble_krls_predictor::ensemble_krls_predictor(
    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time,
        std::shared_ptr<std::unordered_map<query_id, query_count>>>>&
                query_observations,
    query_count max_query_count )
    : ensemble_predictor( query_observations, max_query_count ),
      raw_krls_predictor_( std::make_unique<krls_predictor<1>>(
          configs_.krls_learning_rate_, max_input_, false, kernel_,
          k_predictor_max_internal_model_size, -DBL_MAX, DBL_MAX ) ),
      krls_predictor_( std::move( raw_krls_predictor_ ) ) {}

query_count ensemble_krls_predictor::get_estimated_query_count(
    query_id q_id, epoch_time time ) {
    auto krls_predictor = krls_predictor_.wlock();
    auto rounding_factor = configs_.slot_;

    time -= time % rounding_factor;
    return static_cast<query_count>(
        ( *krls_predictor )->make_prediction( {static_cast<double>( time )} ) *
        ( max_query_count_ ) );
}

void ensemble_krls_predictor::train( query_id q_id, epoch_time current_time ) {
    auto           krls_predictor = krls_predictor_.wlock();
    const uint32_t rounding_factor = configs_.slot_;

    const uint32_t krls_time_range =
        configs_.slot_ * configs_.krls_training_interval_;

    current_time -= ( current_time % rounding_factor );

    for( epoch_time offset = 0; offset <= krls_time_range;
         offset += rounding_factor ) {
        const epoch_time t = ( current_time - offset );
        ( *krls_predictor )
            ->add_observation(
                {static_cast<double>( t )},
                static_cast<double>( this->get_historic_load( q_id, t ) ) /
                    ( max_query_count_ ) );
    }

    ( *krls_predictor )->update_model();
}

void ensemble_krls_predictor::add_observation( query_id   observed_query_id,
                                               epoch_time time ) {
    /* Do nothing, handled by ensemble_early_query_predictor */
}
