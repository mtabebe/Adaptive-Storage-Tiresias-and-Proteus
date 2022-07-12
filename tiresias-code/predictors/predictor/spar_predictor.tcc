#pragma once
#include <dlib/matrix.h>

#include "../constants.h"
#include "folly/Synchronized.h"
#include "spar_predictor.h"

template <long N>
spar_predictor<N>::spar_predictor( const spar_predictor_configs spar_config )
    : spar_config_( spar_config ) {
    initialize_predictor();
}

template <long N>
void           spar_predictor<N>::initialize_predictor() {
    const auto max_values = max_values_.wlock();
    *max_values = std::vector<double>( N, 0.0f );
    for( unsigned int i = 0; i < N; i++ ) {
        ( *max_values )[i] = 1 << 16;
    }

    set_custom_prune_hooks( key_eviction_logger );
}

template <long N>
spar_predictor<N>::spar_predictor() {
    initialize_predictor();
}

template <long N>
void           spar_predictor<N>::key_eviction_logger(
    epoch_time time,
    std::shared_ptr<std::unordered_map<query_id, query_count>> &&occurances ) {
    DVLOG( 40 ) << "evicting at time " << time << " with entries for "
               << occurances->size() << "query ids";
}

template <long N>
query_count    spar_predictor<N>::get_historic_load(
    query_id q_id, epoch_time time,
    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>>
        &      query_observations_,
    epoch_time rounding_factor ) {

    const epoch_time rounded_time = time - time % rounding_factor;
    auto             query_observations = query_observations_.rlock();
    if( !query_observations->exists( rounded_time ) ) {
        return 0;
    }

    const auto &query_counts =
        query_observations->getWithoutPromotion( rounded_time );
    if( query_counts->find( q_id ) != query_counts->end() ) {

        /**
         * Edge-case: Normalization needs to occur because when retrieving
         * values that are rounded by a larger rounding_factor, naturally
         * we collect more observations in that bucket. Hence, return
         * a normalized value in terms of rounding with respect to the size
         * of a slot.
         *
         * For example, consider a rounding_factor of every 10 seconds, while
         * we define slots as 1 second. Each "bucket" in the LRU cache would
         * span 10 seconds, we could expect the historical count inside the
         * cache to contain ~10x that amount expected in 1 second (slot size).
         *
         * Therefore, the semantics for get_historical_load are to retrieve the
         * amount of queries per slot (which is consistent with the ensemble
         * approach).
         */
        return static_cast<query_count>( ( (double) ( std::round(
            query_counts->at( q_id ) *
            ( (double) spar_config_.slot_ / (double) rounding_factor ) ) ) ) );
    }
    return 0;
}

template <long N>
void spar_predictor<N>::train( query_id q_id, epoch_time current_time ) {
    train( q_id, spar_config_.slot_ * spar_config_.training_interval_,
           current_time );
}

template <long N>
void spar_predictor<N>::train( query_id q_id, epoch_time time_range,
                               epoch_time current_time ) {
    const auto   max_values = max_values_.rlock();
    const auto   predictors = predictors_.wlock();
    const double learning_rate = spar_config_.learning_rate_;

    std::vector<double>   init_weight( (size_t) N, spar_config_.init_weight_ );
    const double          init_bias = spar_config_.init_bias_;
    const double          regularization = spar_config_.regularization_;
    const double bias_regularization = spar_config_.bias_regularization_;

    /**
     * Trains using two days of previous data by default, as optionally
     * specified
     * by the time_range parameter.
     */
    std::vector<query_count> values;
    const int32_t rounding_factor = spar_config_.rounding_factor_temporal_;

    // Note: Since epoch_time is unsigned, it would underflow when subtracting
    for( epoch_time offset = time_range; offset - rounding_factor < offset;
         offset -= rounding_factor * 10 ) {
        // retrieve former parameter value of y(t)
        epoch_time t = ( current_time - offset );
        t -= t % spar_config_.rounding_factor_temporal_;

        auto input_coefficients =
            get_input_coefficients( q_id, t, current_time );
        normalize_coefficients( input_coefficients );

        if( predictors->find( q_id ) == predictors->end() ) {
            predictors->insert(
                {q_id, std::make_shared<multi_linear_predictor>(
                           init_weight, init_bias, regularization,
                           bias_regularization, learning_rate, *max_values,
                           -1000000 /* MTODO-PRED */, 10000000 /*MTODO-PRED*/,
                           false )} );
        }
        query_count historic_load =
            get_historic_load( q_id, t, query_observations_temporal_,
                               spar_config_.rounding_factor_temporal_ );

        predictors->at( q_id )->add_observation( input_coefficients,
                                                 historic_load );

        // Note: There can be improved model performance if training is batched
        if( ( offset % ( rounding_factor * 10 * spar_config_.batch_size_ ) ) ==
            0 ) {

            predictors->at( q_id )->update_model();
        }
    }
    predictors->at( q_id )->update_model();
}

template <long        N>
std::vector<double> spar_predictor<N>::get_input_coefficients(
    query_id q_id, epoch_time time, epoch_time current_time ) {

    std::vector<double>   input_coefficients( N, 0.0f );
    unsigned int          counter = 0;

    time -= time % spar_config_.rounding_factor_temporal_;
    current_time -= current_time % spar_config_.rounding_factor_temporal_;

    /**
     * Follows the logic from the p-store paper. First, the input is
     * determined with respect to a_k and b_j, and they are populated (in that
     * order) inside a darray<>, which denotes the a_1, ..., a_n, b_1, ..., b_n
     * input coefficients.
     *
     * By using multiple regression, the actual values of a_k and b_j are
     * determined
     * with the least-squares approach, and hence a future value of y(t+T) can
     * be found.
     */

    // Part 1: Periodic signals
    for( uint32_t k = 1; k <= (uint32_t) SPAR_PERIODIC_QUANTITY; k++ ) {
        input_coefficients[counter++] =
            get_historic_load( q_id, time - ( k * spar_config_.width_ ),
                               query_observations_periodic_,
                               spar_config_.rounding_factor_periodic_ );
    }

    // Part 2: Temporal signals
    for( uint32_t j = 1; j <= (uint32_t) SPAR_TEMPORAL_QUANTITY; j++ ) {
        // Represents the summation of k = 1 .. n loop for delta y
        double historical_periodic_average = 0;

        // Computes the inner delta y based on averaged periodic data
        for( uint32_t k = 1; k <= (uint32_t) SPAR_PERIODIC_QUANTITY; k++ ) {
            historical_periodic_average += get_historic_load(
                q_id, ( current_time - ( j * spar_config_.slot_ ) ) -
                          ( k * spar_config_.width_ ),
                query_observations_periodic_,
                spar_config_.rounding_factor_periodic_ );
        }
        historical_periodic_average /= SPAR_PERIODIC_QUANTITY;

        query_count recent_temporal_count =
            get_historic_load( q_id, current_time - ( j * spar_config_.slot_ ),
                               query_observations_temporal_,
                               spar_config_.rounding_factor_temporal_ );
        /**
         * Since we are computing delta y, we are interested in the difference
         * between the averaged historical (periodic) values and local
         * temporal values
         */
        recent_temporal_count++;
        input_coefficients[counter++] =
            recent_temporal_count - historical_periodic_average;
    }

    return input_coefficients;
}

template <long N>
query_count    spar_predictor<N>::get_estimated_query_count(
    query_id q_id, epoch_time time, epoch_time current_time ) {
    auto input_coefficients =
        get_input_coefficients( q_id, time, current_time );
    normalize_coefficients( input_coefficients );
    const auto &predictors = predictors_.wlock();

    if( predictors->find( q_id ) == predictors->end() ) {
        return 0;
    }

    if( time > current_time && time - current_time > spar_config_.width_) {
      LOG (WARNING) << "Potentially inaccurate query: too far ahead when predicting time " << time << " from time " << current_time;
    }

    return predictors->at( q_id )->make_prediction( input_coefficients );
}

template <long N>
query_count spar_predictor<N>::get_estimated_query_count( query_id   q_id,
                                                          epoch_time time ) {
    epoch_time current_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch() )
            .count();
    return get_estimated_query_count( q_id, time, current_time );
}

template <long N>
void spar_predictor<N>::add_observation_by_type(
    query_id observed_query_id, epoch_time time, query_count count,
    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>>
        &      query_observations_,
    epoch_time rounding_factor ) {

    const epoch_time rounded_time = time - time % rounding_factor;

    auto query_observations = query_observations_.wlock();
    if( !query_observations->exists( rounded_time ) ) {
        query_observations->insert(
            rounded_time,
            std::make_shared<std::unordered_map<query_id, query_count>>() );
    }

    auto &observation_map = query_observations->get( rounded_time );
    if( observation_map->find( observed_query_id ) == observation_map->end() ) {
        observation_map->insert( {observed_query_id, 0} );
    }
    observation_map->at( observed_query_id ) += count;
}

template <long N>
void spar_predictor<N>::add_observation( query_id   observed_query_id,
                                         epoch_time time, query_count count ) {
    add_observation_by_type( observed_query_id, time, count,
                             query_observations_periodic_,
                             spar_config_.rounding_factor_periodic_ );
    add_observation_by_type( observed_query_id, time, count,
                             query_observations_temporal_,
                             spar_config_.rounding_factor_temporal_ );
}

template <long N>
void           spar_predictor<N>::add_custom_predictor(
    query_id                                q_id,
    std::shared_ptr<multi_linear_predictor> predictor_instance ) {
    auto predictors = predictors_.wlock();
    ( *predictors )[q_id] = predictor_instance;
}

template <long N>
void           spar_predictor<N>::set_custom_prune_hooks( void ( *function )(
    epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>
                    &&occurances ) ) {
    const auto query_observations_temporal =
        query_observations_temporal_.wlock();
    const auto query_observations_periodic =
        query_observations_periodic_.wlock();

    query_observations_temporal->setPruneHook( function );
    query_observations_periodic->setPruneHook( function );
}

template <long N>
void spar_predictor<N>::normalize_coefficients( std::vector<double> &arr ) {
    if( spar_config_.normalization_value_ != 0 ) {
        for( size_t x = 0; x < arr.size(); x++ ) {
            arr[x] /= spar_config_.normalization_value_;
        }
    }
}
