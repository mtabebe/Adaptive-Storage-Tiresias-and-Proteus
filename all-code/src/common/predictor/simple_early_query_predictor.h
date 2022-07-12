#pragma once
#include <unordered_map>
#include <unordered_set>

#include "./early_query_predictor.h"
#include "folly/Synchronized.h"

class simple_early_query_predictor : public early_query_predictor {
   public:
    virtual std::unordered_set<query_id> get_predicted_queries(
        epoch_time begin_time, epoch_time end_time, double theshold );
    virtual epoch_time get_predicted_arrival( query_id search_query_id,
                                              double   threshold );
    virtual query_count get_estimated_query_count( query_id   search_query_id,
                                                   epoch_time candidate_time );
    virtual query_count get_estimated_query_count( query_id   search_query_id,
                                                   epoch_time candidate_time,
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

    using early_query_predictor::train;
    virtual void train( epoch_time current_time );

   private:
    /* Denotes the timestamp for the last query observation */
    folly::Synchronized<std::unordered_map<query_id, epoch_time>>
        query_observations_;
    /* Denotes the computed period length based off of observations */
    folly::Synchronized<std::unordered_map<query_id, epoch_time>>
        query_period_length_;
    /* Denotes the scheduled periodicity of a query */
    folly::Synchronized<std::unordered_map<query_id, epoch_time>>
        query_schedule_;
};
