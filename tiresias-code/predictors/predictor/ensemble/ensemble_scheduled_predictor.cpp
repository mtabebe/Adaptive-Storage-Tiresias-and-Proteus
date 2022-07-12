#include "ensemble_scheduled_predictor.h"
#include <math.h>

ensemble_scheduled_predictor::ensemble_scheduled_predictor(
    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>>
        &       query_observations,
    query_count max_query_count )
    : ensemble_predictor( query_observations, max_query_count ) {}

ensemble_scheduled_predictor::ensemble_scheduled_predictor(
    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>>
        &                                        query_observations,
    const ensemble_early_query_predictor_configs configs,
    query_count                                  max_query_count )
    : ensemble_predictor( query_observations, configs, max_query_count ) {}

query_count ensemble_scheduled_predictor::get_estimated_query_count(
    query_id q_id, epoch_time time ) {
    auto &scheduled_distributions = ( *scheduled_distributions_.rlock() );

    // Iteratively compute the average to avoid overflow:
    // http://www.heikohoffmann.de/htmlthesis/node134.html
    query_count avg = 0, total = 1;
    for( auto &distribution : scheduled_distributions ) {
        if( distribution->get_estimated_query_count( time ) >= 0 ) {
            avg += ( distribution->get_estimated_query_count( time ) - avg ) /
                   total;
            total++;
        }
    }
    return avg;
}

void ensemble_scheduled_predictor::add_query_distribution(
    std::shared_ptr<scheduled_distribution> dist ) {
    auto scheduled_distributions = scheduled_distributions_.wlock();
    scheduled_distributions->push_back( dist );
}
