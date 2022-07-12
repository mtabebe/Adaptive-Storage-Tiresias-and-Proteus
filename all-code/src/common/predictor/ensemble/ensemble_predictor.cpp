#include "ensemble_predictor.h"

query_count ensemble_predictor::get_historic_load( query_id   q_id,
                                                   epoch_time time ) {
    auto query_observations = query_observations_.wlock();
    time -= ( time % configs_.slot_ );

    if( !query_observations->exists( time ) ) {
        return 0;
    }

    const auto &map = query_observations->getWithoutPromotion( time );
    return ( *map )[q_id];
}

ensemble_predictor::ensemble_predictor(
    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>>
        &                                        query_observations,
    const ensemble_early_query_predictor_configs configs,
    query_count                                  max_query_count )
    : query_observations_( query_observations ),
      configs_( configs ),
      max_query_count_( max_query_count ) {}

ensemble_predictor::ensemble_predictor(
    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>>
        &       query_observations,
    query_count max_query_count )
    : query_observations_( query_observations ),
      max_query_count_( max_query_count ) {}

ensemble_predictor::~ensemble_predictor() {}
