#pragma once
#include <gmock/gmock.h>

#include "../src/common/constants.h"
#include "../src/common/predictor/spar_predictor.h"

constexpr long variate_count = SPAR_PERIODIC_QUANTITY + SPAR_TEMPORAL_QUANTITY;

class spar_predictor_mock : public spar_predictor<variate_count> {
   public:
    MOCK_METHOD2( get_estimated_query_count,
                  query_count( query_id q_id, epoch_time time ) );

    MOCK_METHOD3( train, void( query_id q_id, epoch_time time_range,
                               epoch_time current_time ) );

    MOCK_METHOD2( train, void( query_id q_id, epoch_time current_time ) );

    MOCK_METHOD2( add_observation,
                  void( query_id observed_query_id, epoch_time time ) );
};
