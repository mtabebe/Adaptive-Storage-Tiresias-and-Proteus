#pragma once
#include <unordered_map>
#include <unordered_set>

#include "../constants.h"
#include "./early_query_predictor.h"
#include "folly/Synchronized.h"
#include "folly/container/EvictingCacheMap.h"
#include "spar_predictor.h"
#include "spar_predictor_configs.h"

class spar_early_query_predictor : public early_query_predictor {
    const spar_predictor_configs spar_config_;

   public:
    static constexpr long variate_count =
        SPAR_PERIODIC_QUANTITY + SPAR_TEMPORAL_QUANTITY;

    spar_early_query_predictor(
        const spar_predictor_configs &                 spar_config,
        std::shared_ptr<spar_predictor<variate_count>> spar_pred );

    virtual std::unordered_set<query_id> get_predicted_queries(
        epoch_time begin_time, epoch_time end_time, double theshold );

    virtual epoch_time get_predicted_arrival( query_id search_query_id,
                                              double   threshold );
    virtual epoch_time get_predicted_arrival( query_id   search_query_id,
                                              double     threshold,
                                              epoch_time current_time );

    virtual void add_observation( query_id observed_query_id, epoch_time time,
                                  query_count count = 1 );
    virtual void add_observations(
        const std::vector<std::pair<query_id, epoch_time>> &observed_queries );
    virtual void add_observations(
        const std::vector<query_id> &observed_queries, epoch_time time );

    virtual void add_query_schedule( query_id   scheduled_query_id,
                                     epoch_time period_length );
    virtual void add_query_schedule(
        const std::vector<query_id> &scheduled_query_ids,
        epoch_time                   period_length );

    virtual query_count get_estimated_query_count( query_id   search_query_id,
                                                   epoch_time candidate_time );
    virtual query_count get_estimated_query_count( query_id   search_query_id,
                                                   epoch_time candidate_time,
                                                   epoch_time current_time );

    void train( epoch_time current_time );
    void train();

   private:
    folly::Synchronized<std::shared_ptr<spar_predictor<variate_count>>>
        spar_predictor_;

    /* Denotes query id's observed since the model was last trained */
    folly::Synchronized<std::unordered_set<query_id>> query_ids_refreshed_;

    /* Denotes query id's observed over time (including trained ones) */
    folly::Synchronized<std::unordered_set<query_id>> query_ids_observed_;
};
