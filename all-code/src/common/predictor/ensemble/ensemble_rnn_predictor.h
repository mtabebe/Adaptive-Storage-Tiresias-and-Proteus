#pragma once

#include "ensemble_early_query_predictor_configs.h"
#include "ensemble_lstm_model.h"
#include "ensemble_predictor.h"
#include <chrono>

typedef uint64_t query_id;
typedef uint64_t query_count;

class ensemble_rnn_predictor : public ensemble_predictor {
    std::unique_ptr<ensemble_lstm_model>                      raw_lstm_model_;
    folly::Synchronized<std::unique_ptr<ensemble_lstm_model>> lstm_model_;

   public:
    ~ensemble_rnn_predictor() {}
    ensemble_rnn_predictor(
        folly::Synchronized<folly::EvictingCacheMap<
            epoch_time,
            std::shared_ptr<std::unordered_map<query_id, query_count>>>>&
                                                     query_observations,
        const ensemble_early_query_predictor_configs configs,
        query_count                                  max_query_count );

    ensemble_rnn_predictor(
        folly::Synchronized<folly::EvictingCacheMap<
            epoch_time,
            std::shared_ptr<std::unordered_map<query_id, query_count>>>>&
                    query_observations,
        query_count max_query_count );

    virtual query_count get_estimated_query_count( query_id   q_id,
                                                   epoch_time time );
    virtual query_count get_estimated_query_count( query_id   q_id,
                                                   epoch_time time,
                                                   epoch_time current_time );

    virtual void train( query_id q_id, epoch_time current_time );
    void add_observation( query_id observed_query_id, epoch_time time );
};
