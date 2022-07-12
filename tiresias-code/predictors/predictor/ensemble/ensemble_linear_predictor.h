#pragma once

#include "../single_closed_form_linear_predictor.h"
#include "ensemble_early_query_predictor_configs.h"
#include "ensemble_predictor.h"
#include <chrono>

typedef uint64_t query_id;
typedef uint64_t query_count;

class ensemble_linear_predictor : public ensemble_predictor {
    const uint32_t LRU_CACHE_SIZE_FACTOR = 1.25;
    epoch_time     training_offset_ = 0;

    std::unique_ptr<single_closed_form_linear_predictor>
        single_linear_predictor_;
    folly::Synchronized<std::unique_ptr<single_closed_form_linear_predictor>>
        linear_predictor_;

   public:
    ~ensemble_linear_predictor() {}
    ensemble_linear_predictor(
        folly::Synchronized<folly::EvictingCacheMap<
            epoch_time,
            std::shared_ptr<std::unordered_map<query_id, query_count>>>>&
                                                     query_observations,
        const ensemble_early_query_predictor_configs configs,
        query_count                                  max_query_count );

    ensemble_linear_predictor(
        folly::Synchronized<folly::EvictingCacheMap<
            epoch_time,
            std::shared_ptr<std::unordered_map<query_id, query_count>>>>&
                    query_observations,
        query_count max_query_count );

    query_count get_estimated_query_count( query_id q_id, epoch_time time );

    void train( query_id q_id, epoch_time current_time );
    void add_observation( query_id observed_query_id, epoch_time time );
};
