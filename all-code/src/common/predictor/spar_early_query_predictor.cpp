#include "glog/logging.h"
#include <chrono>

#include "../constants.h"
#include "spar_early_query_predictor.h"

spar_early_query_predictor::spar_early_query_predictor(
    const spar_predictor_configs &                 spar_config,
    std::shared_ptr<spar_predictor<variate_count>> spar_pred )
    : spar_config_( spar_config ), spar_predictor_( spar_pred ) {}

std::unordered_set<query_id> spar_early_query_predictor::get_predicted_queries(
    epoch_time begin_time, epoch_time end_time, double threshold ) {
    const auto query_ids_observed = query_ids_observed_.rlock();
    auto       spar_predictor = *( spar_predictor_.wlock() );
    std::unordered_set<query_id> resp;

    /**
     * Round inclusively, such that all results appear within the requested
     * window (ex: the observed query time for any
     * query is not earlier than begin_time or later than end_time)
     * */
    const int32_t rounding_factor = spar_config_.rounding_factor_temporal_;
    begin_time += ( rounding_factor - ( begin_time % rounding_factor ) ) %
                  rounding_factor;
    end_time -= end_time % rounding_factor;

    for( epoch_time candidate_time = begin_time; candidate_time <= end_time;
         candidate_time += rounding_factor ) {
        for( auto &q_id : *query_ids_observed ) {
            if( resp.find( q_id ) == resp.end() ) {
                if( spar_predictor->get_estimated_query_count(
                        q_id, candidate_time ) >= threshold ) {
                    resp.insert( q_id );
                }
            }
        }
    }

    return resp;
}

query_count spar_early_query_predictor::get_estimated_query_count(
    query_id search_query_id, epoch_time candidate_time ) {
    auto spar_predictor = *( spar_predictor_.wlock() );
    return spar_predictor->get_estimated_query_count( search_query_id,
                                                      candidate_time );
}

query_count spar_early_query_predictor::get_estimated_query_count(
    query_id search_query_id, epoch_time candidate_time,
    epoch_time current_time ) {
    auto spar_predictor = *( spar_predictor_.wlock() );
    return spar_predictor->get_estimated_query_count(
        search_query_id, candidate_time, current_time );
}

epoch_time spar_early_query_predictor::get_predicted_arrival(
    query_id search_query_id, double threshold ) {
    epoch_time current_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch() )
            .count();
    return get_predicted_arrival( search_query_id, threshold, current_time );
}

epoch_time spar_early_query_predictor::get_predicted_arrival(
    query_id search_query_id, double threshold, epoch_time current_time ) {
    const auto    spar_predictor = *( spar_predictor_.wlock() );
    const int32_t rounding_factor = spar_config_.rounding_factor_temporal_;
    current_time = current_time - current_time % rounding_factor;
    const int search_window =
        spar_config_.width_ * spar_config_.predicted_arrival_window_;

    for( epoch_time candidate_time = current_time;
         candidate_time <= current_time + search_window;
         candidate_time += rounding_factor ) {
        if( spar_predictor->get_estimated_query_count(
                search_query_id, candidate_time ) >= threshold ) {
            return candidate_time;
        }
    }

    return std::numeric_limits<epoch_time>::max();
}

void spar_early_query_predictor::train() {
    epoch_time current_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch() )
            .count();
    train( current_time );
}

void spar_early_query_predictor::train( epoch_time current_time ) {
    auto query_ids_refreshed = query_ids_refreshed_.wlock();
    auto spar_predictor = *( spar_predictor_.wlock() );

    for( auto query_id : *query_ids_refreshed ) {
        DVLOG( 40 ) << "Training SPAR model for " << query_id;
        spar_predictor->train( query_id, current_time );
    }
    query_ids_refreshed->clear();
    DVLOG( 40 ) << "Finished training SPAR models";
}

void spar_early_query_predictor::add_observation( query_id    observed_query_id,
                                                  epoch_time  time,
                                                  query_count count ) {
    auto query_ids_refreshed = query_ids_refreshed_.wlock();
    auto query_ids_observed = query_ids_observed_.wlock();
    auto spar_predictor = *( spar_predictor_.wlock() );

    query_ids_refreshed->insert( observed_query_id );
    query_ids_observed->insert( observed_query_id );
    spar_predictor->add_observation( observed_query_id, time, count );
}

void spar_early_query_predictor::add_observations(
    const std::vector<std::pair<query_id, epoch_time>> &observed_queries ) {
    for( const auto &observed_query : observed_queries ) {
        spar_early_query_predictor::add_observation( observed_query.first,
                                                     observed_query.second );
    }
}

void spar_early_query_predictor::add_observations(
    const std::vector<query_id> &observed_queries, epoch_time time ) {

    for( const auto &observed_query : observed_queries ) {
        spar_early_query_predictor::add_observation( observed_query, time );
    }
}

void spar_early_query_predictor::add_query_schedule(
    query_id scheduled_query_id, epoch_time period_length ) {}
void spar_early_query_predictor::add_query_schedule(
    const std::vector<query_id> &scheduled_query_ids,
    epoch_time                   period_length ) {
    for( const auto &scheduled_query_id : scheduled_query_ids ) {
        spar_early_query_predictor::add_query_schedule( scheduled_query_id,
                                                            period_length );
    }
}
