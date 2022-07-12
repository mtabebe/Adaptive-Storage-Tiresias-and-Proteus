#pragma once

#include "ensemble_predictor.h"
#include "folly/Synchronized.h"
#include "scheduled_distribution.h"

class ensemble_scheduled_predictor : public ensemble_predictor {
    folly::Synchronized<std::vector<std::shared_ptr<scheduled_distribution>>>
        scheduled_distributions_;

   public:
    ~ensemble_scheduled_predictor() {}

    ensemble_scheduled_predictor(
        folly::Synchronized<folly::EvictingCacheMap<
            epoch_time,
            std::shared_ptr<std::unordered_map<query_id, query_count>>>>&
                    query_observations,
        query_count max_query_count );

    ensemble_scheduled_predictor(
        folly::Synchronized<folly::EvictingCacheMap<
            epoch_time,
            std::shared_ptr<std::unordered_map<query_id, query_count>>>>&
                                               query_observations,
        ensemble_early_query_predictor_configs configs,
        query_count                            max_query_count );

    virtual query_count get_estimated_query_count( query_id   q_id,
                                                   epoch_time time );
    void add_query_distribution( std::shared_ptr<scheduled_distribution> dist );

    /* No implementation */
    void train( query_id q_id, epoch_time curren_time ) {}
    void add_observation( query_id observed_query_id, epoch_time time ) {}
};
