#pragma once
#include <gmock/gmock.h>

#include "../src/common/constants.h"
#include "../src/common/predictor/ensemble/ensemble_rnn_predictor.h"

class ensemble_rnn_predictor_mock : public ensemble_rnn_predictor {
   public:
    ensemble_rnn_predictor_mock(
        folly::Synchronized<folly::EvictingCacheMap<
            epoch_time,
            std::shared_ptr<std::unordered_map<query_id, query_count>>>>&
                                                     query_observations,
        const ensemble_early_query_predictor_configs configs )
        : ensemble_rnn_predictor( query_observations, configs, ( 1 << 16 ) ) {}

    MOCK_METHOD2( get_estimated_query_count,
                  query_count( query_id q_id, epoch_time time ) );
    MOCK_METHOD3( get_estimated_query_count,
                  query_count( query_id q_id, epoch_time time,
                               epoch_time current_time ) );
    MOCK_METHOD2( train, void( query_id q_id, epoch_time current_time ) );

    MOCK_METHOD2( add_observation,
                  void( query_id observed_query_id, epoch_time time ) );
};
