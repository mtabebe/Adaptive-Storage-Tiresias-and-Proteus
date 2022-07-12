#include "simple_early_query_predictor.h"

std::unordered_set<query_id>
    simple_early_query_predictor::get_predicted_queries( epoch_time begin_time,
                                                         epoch_time end_time,
                                                         double     theshold ) {
    // Note: Currently this implementation is a dummy implementation that uses
    // brute-force
    const auto query_observations = query_observations_.rlock();
    const auto query_period_length = query_period_length_.rlock();
    const auto query_schedule = query_schedule_.rlock();

    std::unordered_set<query_id> resp;
    for( const auto &period_pair : *query_period_length ) {
        epoch_time expected_arrival_time =
            query_observations->at( period_pair.first ) + period_pair.second;
        if( expected_arrival_time >= begin_time &&
            expected_arrival_time <= end_time ) {
            resp.insert( period_pair.first );
        }
    }

    for( const auto &period_pair : *query_schedule ) {
        epoch_time expected_arrival_time =
            query_observations->at( period_pair.first ) + period_pair.second;
        if( expected_arrival_time >= begin_time &&
            expected_arrival_time <= end_time ) {
            resp.insert( period_pair.first );
        }
    }
    return resp;
}

epoch_time simple_early_query_predictor::get_predicted_arrival(
    query_id search_query_id, double threshold ) {
    // Note: Currently this implementation is a dummy implementation that relies
    // on previous info
    const auto query_observations = query_observations_.rlock();
    const auto query_schedule = query_schedule_.rlock();
    const auto query_period_length = query_period_length_.rlock();
    if( query_schedule->find( search_query_id ) != query_schedule->end() ) {
        if( query_observations->find( search_query_id ) !=
            query_observations->end() ) {
            return query_observations->at( search_query_id ) +
                   query_schedule->at( search_query_id );
        }
    }
    if( query_period_length->find( search_query_id ) !=
        query_period_length->end() ) {
        return query_period_length->at( search_query_id ) +
               query_observations->at( search_query_id );
    }

    return std::numeric_limits<epoch_time>::max();
}

query_count simple_early_query_predictor::get_estimated_query_count(
    query_id search_query_id, epoch_time candidate_time,
    epoch_time current_time ) {
    return get_estimated_query_count( search_query_id, candidate_time );
}

query_count simple_early_query_predictor::get_estimated_query_count(
    query_id search_query_id, epoch_time candidate_time ) {
    // Note: Currently this implementation is a dummy implementation that relies
    // on previous info
    const auto query_observations = query_observations_.rlock();
    const auto query_schedule = query_schedule_.rlock();
    const auto query_period_length = query_period_length_.rlock();
    if( query_schedule->find( search_query_id ) != query_schedule->end() ) {
        if( query_observations->find( search_query_id ) !=
            query_observations->end() ) {
            int obsTime = query_observations->at( search_query_id );
            int scheduleTime = query_schedule->at( search_query_id );
            int deltaTime = ( candidate_time - obsTime );
            if ( deltaTime % scheduleTime == 0 ) {
              return 1;
            }
        }
    }
    if( query_period_length->find( search_query_id ) !=
        query_period_length->end() ) {
        int obsTime = query_observations->at( search_query_id );
        int scheduleTime = query_period_length->at( search_query_id );
        int deltaTime = ( candidate_time - obsTime );
        if( deltaTime % scheduleTime == 0 ) {
            return 1;
        }
    }

    return 0;
}

void simple_early_query_predictor::add_observation( query_id observed_query_id,
                                                    epoch_time  time,
                                                    query_count count ) {
    const auto query_observations = query_observations_.wlock();
    const auto query_period_length = query_period_length_.wlock();

    if( query_observations->find( observed_query_id ) !=
        query_observations->end() ) {
        epoch_time prev_time = query_observations->at( observed_query_id );

        // It may be possible that the client returns observations out of order
        int64_t period_length = std::abs( int64_t( time - prev_time ) );
        ( *query_period_length )[observed_query_id] = period_length;
    }
    ( *query_observations )[observed_query_id] = time;
}

void simple_early_query_predictor::add_observations(
    const std::vector<std::pair<query_id, epoch_time>> &observed_queries ) {
    for( auto &query : observed_queries ) {
        simple_early_query_predictor::add_observation( query.first,
                                                       query.second );
    }
}

void simple_early_query_predictor::add_observations(
    const std::vector<query_id> &observed_queries, epoch_time time ) {
    for( auto &query : observed_queries ) {
        simple_early_query_predictor::add_observation( query, time );
    }
}

void simple_early_query_predictor::add_query_schedule(
    query_id scheduled_query_id, epoch_time period_length ) {
    const auto query_schedule = query_schedule_.wlock();
    ( *query_schedule )[scheduled_query_id] = period_length;
}

void simple_early_query_predictor::add_query_schedule(
    const std::vector<query_id> &scheduled_query_ids,
    epoch_time                   period_length ) {
    const auto query_schedule = query_schedule_.wlock();
    for( const auto &scheduled_query_id : scheduled_query_ids ) {
        ( *query_schedule )[scheduled_query_id] = period_length;
    }
}
void simple_early_query_predictor::train( epoch_time current_time ) {}
