#pragma once
#include <dlib/matrix.h>
#include <valarray>

#include "../constants.h"
#include "early_query_predictor.h"
#include "folly/Synchronized.h"
#include "folly/container/EvictingCacheMap.h"
#include "multi_linear_predictor.h"
#include "predictor.h"
#include "spar_predictor_configs.h"

typedef uint64_t query_count;

template <long N>
class spar_predictor {
    friend class spar_predictor_test;

   private:
    folly::Synchronized<std::vector<double>> max_values_;
    folly::Synchronized<
        std::unordered_map<query_id, std::shared_ptr<multi_linear_predictor>>>
                           predictors_;
    spar_predictor_configs spar_config_ = construct_spar_predictor_configs();

    /**
     * The sizes of the LRU caches can be determined mathematically. Two types
     * of caches are used depending on temporal (more recent data) or periodic
     * data. This is more memory efficient, as we don't need very granular data
     * for periodic computation.
     *
     * For temporal data, we only look at m recent slots of time. However, when
     * training we may look beyond that point, as we consider different points
     * in time as our current time, hence shifting our search window.
     *
     * Therefore, the formula becomes: (m * (milliseconds in a timeslot
     * / granularity of temporal data) + training window size / granularity of
     * temporal data.
     *
     * This can be visualized as a window of potential current times, in
     * addition to a loop that iterates on the left-most (oldest time)
     * boundary and expands it.
     *
     * Meanwhile, for periodic, we only have to consider m samplings, each up
     * to mT distance away from the current time, in addition to the training
     * current time offset.
     *
     * Hence, the maximum size possible is:
     * (m * t) / periodic rounding + training window size / granularity of
     * periodic data.
     *
     * NOTE: An LRU_CACHE_SIZE_FACTOR is used as a margin of error (ex: clock
     * skew).
     *
     * */
    const uint32_t LRU_CACHE_SIZE_FACTOR = 10;  // 1.2;
    const uint32_t TEMPORAL_LRU_CACHE_SIZE =
        ( ( SPAR_TEMPORAL_QUANTITY * spar_config_.slot_ /
            spar_config_.rounding_factor_temporal_ ) +
          ( spar_config_.training_interval_ * spar_config_.slot_ /
            spar_config_.rounding_factor_temporal_ ) ) *
        LRU_CACHE_SIZE_FACTOR;
    const uint32_t PERIODIC_LRU_CACHE_SIZE =
        ( ( SPAR_PERIODIC_QUANTITY * spar_config_.training_interval_ *
            spar_config_.slot_ / spar_config_.rounding_factor_periodic_ ) ) *

        LRU_CACHE_SIZE_FACTOR;

    folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>
        query_observations_temporal_map_{TEMPORAL_LRU_CACHE_SIZE};

    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>>
        query_observations_temporal_{
            std::move( query_observations_temporal_map_ )};

    folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>
        query_observations_periodic_map_{PERIODIC_LRU_CACHE_SIZE};

    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>>
        query_observations_periodic_{
            std::move( query_observations_periodic_map_ )};

    virtual void add_observation_by_type(
        query_id observed_query_id, epoch_time time, query_count count,
        folly::Synchronized<folly::EvictingCacheMap<
            epoch_time,
            std::shared_ptr<std::unordered_map<query_id, query_count>>>>
            &      query_observations,
        epoch_time round_factor );

    query_count get_historic_load(
        query_id q_id, epoch_time time,
        folly::Synchronized<folly::EvictingCacheMap<
            epoch_time,
            std::shared_ptr<std::unordered_map<query_id, query_count>>>>
            &      query_observations,
        epoch_time round_factor );
    std::vector<double> get_input_coefficients( query_id   q_id,
                                                  epoch_time time,
                                                  epoch_time current_time );

    void initialize_predictor();
    void normalize_coefficients( std::vector<double> &arr );

    static void key_eviction_logger(
        epoch_time time,
        std::shared_ptr<std::unordered_map<query_id, query_count>>
            &&occurances );

   public:
    spar_predictor( const spar_predictor_configs spc );
    spar_predictor();

    virtual query_count get_estimated_query_count( query_id   q_id,
                                                   epoch_time time );

    virtual query_count get_estimated_query_count( query_id   q_id,
                                                   epoch_time time,
                                                   epoch_time current_time );

    virtual void train( query_id q_id, epoch_time time_range,
                        epoch_time current_time );
    virtual void train( query_id q_id, epoch_time current_time );
    virtual void add_observation( query_id observed_query_id, epoch_time time,
                                  query_count count );

    // NOTE: These are for testing purposes only
    void add_custom_predictor(
        query_id                                q_id,
        std::shared_ptr<multi_linear_predictor> predictor_instance );
    void set_custom_prune_hooks( void ( *function )(
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>
                        &&occurances ) );
};

#include "spar_predictor.tcc"
