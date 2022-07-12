#pragma once

#include "folly/Synchronized.h"
#include "folly/container/EvictingCacheMap.h"
#include <dlib/matrix.h>
#include <valarray>

#include "../../constants.h"
#include "../early_query_predictor.h"
#include "ensemble_early_query_predictor_configs.h"
#include "ensemble_krls_predictor.h"
#include "ensemble_linear_predictor.h"
#include "ensemble_rnn_predictor.h"
#include "ensemble_scheduled_predictor.h"

typedef uint64_t query_count;
typedef uint64_t query_id;

class ensemble_early_query_predictor : public early_query_predictor {
    const ensemble_early_query_predictor_configs ensemble_config_;

    const uint32_t LRU_CACHE_SIZE_FACTOR_ = 1.25;
    const uint32_t LRU_CACHE_MAX_SIZE_ =
        std::max( ensemble_config_.linear_training_interval_,
                  std::max( ensemble_config_.krls_training_interval_,
                            ensemble_config_.rnn_training_interval_ ) );

    folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>
        query_observations_map_{
            LRU_CACHE_SIZE_FACTOR_ *
            ( ensemble_config_.slot_ * LRU_CACHE_MAX_SIZE_ )};

    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>>
        query_observations_{std::move( query_observations_map_ )};

   public:
    ensemble_early_query_predictor(
        const ensemble_early_query_predictor_configs &esemble_config );

    std::unordered_set<query_id> get_predicted_queries( epoch_time begin_time,
                                                        epoch_time end_time,
                                                        double     theshold );

    epoch_time get_predicted_arrival( query_id search_query_id,
                                      double   threshold );

    epoch_time get_predicted_arrival( query_id   search_query_id,
                                      double     threshold,
                                      epoch_time current_time );
    void       add_observation( query_id observed_query_id, epoch_time time,
                                query_count count = 1 );
    void add_observations(
        const std::vector<std::pair<query_id, epoch_time>> &observed_queries );
    void add_observations( const std::vector<query_id> &observed_queries,
                           epoch_time                   time );
    void add_query_schedule( query_id   scheduled_query_id,
                             epoch_time period_length );
    void add_query_schedule( const std::vector<query_id> &scheduled_query_ids,
                             epoch_time                   period_length );

    /* RNN model leverages current_time to extrapolate intermediate points
       for sequencing. */
    query_count get_estimated_query_count( query_id   search_query_id,
                                           epoch_time candidate_time,
                                           epoch_time current_time );
    query_count get_estimated_query_count( query_id   search_query_id,
                                           epoch_time candidate_time );

    void add_query_distribution( query_id                                q_id,
                                 std::shared_ptr<scheduled_distribution> dist );

    using early_query_predictor::train;
    void train( epoch_time current_time );

   private:
    folly::Synchronized<std::unordered_map<
        query_id, std::shared_ptr<ensemble_linear_predictor>>>
        ensemble_linear_predictors_;
    folly::Synchronized<
        std::unordered_map<query_id, std::shared_ptr<ensemble_krls_predictor>>>
        ensemble_krls_predictors_;
    folly::Synchronized<std::unordered_map<
        query_id, std::shared_ptr<ensemble_scheduled_predictor>>>
        ensemble_scheduled_predictors_;
    folly::Synchronized<
        std::unordered_map<query_id, std::shared_ptr<ensemble_rnn_predictor>>>
        ensemble_rnn_predictors_;

    /* Denotes query id's observed since the model was last trained */
    folly::Synchronized<std::unordered_set<query_id>> query_ids_refreshed_;

    /* Denotes query id's observed over time (including trained ones) */
    folly::Synchronized<std::unordered_set<query_id>> query_ids_observed_;

   public:
    /* Note: Only used for testing purposes */
    void add_linear_query_predictor(
        query_id q_id, std::shared_ptr<ensemble_linear_predictor> predictor );
    void add_krls_query_predictor(
        query_id q_id, std::shared_ptr<ensemble_krls_predictor> predictor );
    void add_scheduled_query_predictor(
        query_id                                      q_id,
        std::shared_ptr<ensemble_scheduled_predictor> predictor );
    void add_rnn_query_predictor(
        query_id q_id, std::shared_ptr<ensemble_rnn_predictor> predictor );
};
