#include "ensemble_linear_predictor.h"

ensemble_linear_predictor::ensemble_linear_predictor(
    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time,
        std::shared_ptr<std::unordered_map<query_id, query_count>>>>&
                                                 query_observations,
    const ensemble_early_query_predictor_configs configs,
    query_count                                  max_query_count )
    : ensemble_predictor( query_observations, configs, max_query_count ),
      single_linear_predictor_(
          std::make_unique<single_closed_form_linear_predictor>(
              0.0, 0.0, max_query_count, false ) ),
      linear_predictor_{std::move( single_linear_predictor_ )} {}

ensemble_linear_predictor::ensemble_linear_predictor(
    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time,
        std::shared_ptr<std::unordered_map<query_id, query_count>>>>&
                query_observations,
    query_count max_query_count )
    : ensemble_predictor( query_observations, max_query_count ),
      single_linear_predictor_(
          std::make_unique<single_closed_form_linear_predictor>(
              0.0, 0.0, max_query_count, false ) ),
      linear_predictor_( std::move( single_linear_predictor_ ) ) {}

query_count ensemble_linear_predictor::get_estimated_query_count(
    query_id q_id, epoch_time time ) {
    auto linear_predictor = linear_predictor_.wlock();
    auto rounding_factor = configs_.slot_;

    time -= time % rounding_factor;
    return ( *linear_predictor )->make_prediction( time - training_offset_ );
}

void ensemble_linear_predictor::train( query_id   q_id,
                                       epoch_time current_time ) {
    auto           linear_predictor = linear_predictor_.wlock();
    const uint32_t rounding_factor = configs_.slot_;

    const uint32_t linear_time_range =
        configs_.slot_ * configs_.linear_training_interval_;

    current_time -= ( current_time % rounding_factor );
    training_offset_ = current_time - linear_time_range;

    for( epoch_time offset = 0; offset < linear_time_range;
         offset += rounding_factor ) {
        const epoch_time t = ( current_time - offset );
        ( *linear_predictor )
            ->add_observation( t - training_offset_,
                               this->get_historic_load( q_id, t ) );
    }

    ( *linear_predictor )->update_model();
}

void ensemble_linear_predictor::add_observation( query_id   observed_query_id,
                                                 epoch_time time ) {
    /* Do nothing, handled by ensemble_early_query_predictor */
}
