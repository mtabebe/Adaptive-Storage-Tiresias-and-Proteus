#pragma once
#include <iostream>
#include <istream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

typedef uint64_t query_id;
typedef uint64_t epoch_time;
typedef uint64_t query_count;

class early_query_predictor {
    const query_count DEFAULT_MAX_QUERY_COUNT_ = ( 1 << 16 );
    std::unordered_map<query_id, query_count> max_query_count_;

   public:
    virtual ~early_query_predictor();
    virtual std::unordered_set<query_id> get_predicted_queries(
        epoch_time begin_time, epoch_time end_time, double theshold ) = 0;
    virtual epoch_time get_predicted_arrival( query_id search_query_id,
                                              double   threshold ) = 0;

    virtual query_count get_estimated_query_count( query_id   q_id,
                                                   epoch_time time ) = 0;
    virtual query_count
        get_estimated_query_count( query_id q_id, epoch_time time,
                                   epoch_time current_time /* =
       std::chrono::duration_cast<std::chrono::milliseconds>(
       std::chrono::system_clock::now().time_since_epoch() )
       .count() */ ) = 0;

    virtual void add_observation( query_id observed_query_id, epoch_time time,
                                  query_count count = 1 ) = 0;
    virtual void add_observations(
        const std::vector<std::pair<query_id, epoch_time>>
            &observed_queries ) = 0;
    virtual void add_observations(
        const std::vector<query_id> &observed_queries, epoch_time time ) = 0;

    virtual void add_query_schedule( query_id   scheduled_query_id,
                                     epoch_time period_length ) = 0;
    virtual void add_query_schedule(
        const std::vector<query_id> &scheduled_query_ids,
        epoch_time                   period_length ) = 0;
    virtual void train( epoch_time current_time ) = 0;
    void train();
    void populate_max_query_count( std::istream &in );

   protected:
    query_count get_max_query_count( query_id q_id );
};
