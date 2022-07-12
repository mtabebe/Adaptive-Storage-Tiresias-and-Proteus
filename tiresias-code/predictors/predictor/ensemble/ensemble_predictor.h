#pragma once
#include "folly/Synchronized.h"
#include "folly/container/EvictingCacheMap.h"

#include "ensemble_early_query_predictor_configs.h"

typedef uint64_t query_count;
typedef uint64_t query_id;
typedef uint64_t epoch_time;

class ensemble_predictor {
   protected:
    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>>
        &query_observations_;

    ensemble_early_query_predictor_configs configs_ =
        construct_ensemble_early_query_predictor_configs();

    query_count get_historic_load( query_id q_id, epoch_time time );
    const query_count max_query_count_;

   public:
    virtual ~ensemble_predictor();

    ensemble_predictor(
        folly::Synchronized<folly::EvictingCacheMap<
            epoch_time,
            std::shared_ptr<std::unordered_map<query_id, query_count>>>>
            &       query_observations,
        query_count max_query_count );

    ensemble_predictor(
        folly::Synchronized<folly::EvictingCacheMap<
            epoch_time,
            std::shared_ptr<std::unordered_map<query_id, query_count>>>>
            &                                        query_observations,
        const ensemble_early_query_predictor_configs configs,
        query_count                                  max_query_count );

    virtual query_count get_estimated_query_count( query_id   q_id,
                                                   epoch_time time ) = 0;

    virtual void train( query_id q_id, epoch_time current_time ) = 0;

    virtual void add_observation( query_id   observed_query_id,
                                  epoch_time time ) = 0;
};
