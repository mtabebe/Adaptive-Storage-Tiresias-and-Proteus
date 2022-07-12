#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/common/constants.h"
#include "../src/common/predictor/spar_predictor.h"
#include "multi_linear_predictor_mock.h"

using ::testing::_;
using ::testing::Return;

constexpr long variate_count = SPAR_PERIODIC_QUANTITY + SPAR_TEMPORAL_QUANTITY;

class spar_predictor_test : public ::testing::Test {
    const double init_bias = 0.01;
    const double regularization = 0.01;
    const double bias_regularization = 0.01;

   public:
    static uint32_t eviction_count;
    static void     mock_eviction_logger(
        epoch_time time,
        std::shared_ptr<std::unordered_map<query_id, query_count>>
            &&occurrances ) {
        eviction_count++;
    }

   protected:
    std::shared_ptr<multi_linear_predictor_mock> multi_linear_predictor_mock_;
    spar_predictor_configs                       spar_configs_ =
        construct_spar_predictor_configs( 200, /* width */
                                          10,  /* slot */
                                          400, /* training interval */
                                          10,  /* temporal granularity */
                                          20,  /* periodic granularity */
                                          2    /* arrival window */
                                          );
    spar_predictor<variate_count> spar_predictor_{spar_configs_};

    void SetUp() override {
        spar_predictor_.set_custom_prune_hooks( &mock_eviction_logger );
        eviction_count = 0;
    }

    static uint32_t get_eviction_counter() { return eviction_count; }

   public:
    std::vector<double> get_max_values() {
        std::vector<double> max_values( (size_t) variate_count, 0.0 );
        for( unsigned int i = 0; i < variate_count; i++ ) {
            max_values[i] = 1 << 16;
        }
        return max_values;
    }

    std::vector<double> get_init_values() {
        std::vector<double> initial_values( (size_t) variate_count, 0.0 );
        for( unsigned int i = 0; i < variate_count; i++ ) {
            initial_values[i] = 0;
        }
        return initial_values;
    }

    spar_predictor_test()
        : multi_linear_predictor_mock_(
              std::make_shared<multi_linear_predictor_mock>(
                  get_init_values(), init_bias, regularization,
                  bias_regularization, spar_configs_.learning_rate_,
                  get_max_values(), -1000000 /* MTODO-PRED */,
                  10000000 /*MTODO-PRED*/, false ) ) {}

    /**
     * Below are accessor methods to the private members of spar_predictor.
     *
     * NOTE: This is due to googletest not allowing accessing directly from
     * the tests (i.e., each sub-test is another sub-class).
         */
    query_count get_historic_load(
        query_id q_id, epoch_time time,
        folly::Synchronized<folly::EvictingCacheMap<
            epoch_time,
            std::shared_ptr<std::unordered_map<query_id, query_count>>>>
            &      query_observations_,
        epoch_time rounding_factor ) {
        return spar_predictor_.get_historic_load(
            q_id, time, query_observations_, rounding_factor );
    }

    void add_observation_by_type(
        query_id observed_query_id, epoch_time time, query_count count,
        folly::Synchronized<folly::EvictingCacheMap<
            epoch_time,
            std::shared_ptr<std::unordered_map<query_id, query_count>>>>
            &      query_observations,
        epoch_time round_factor ) {
        return spar_predictor_.add_observation_by_type(
            observed_query_id, time, count, query_observations, round_factor );
    }

    std::vector<double> get_input_coefficients( query_id q_id, epoch_time time,
                                                epoch_time current_time ) {
        return spar_predictor_.get_input_coefficients( q_id, time,
                                                       current_time );
    }

    /* Accessors for member variables and constants */
    folly::Synchronized<folly::EvictingCacheMap<
        epoch_time, std::shared_ptr<std::unordered_map<query_id, query_count>>>>
        &get_query_observations_map( bool is_periodic ) {
        if( is_periodic ) {
            return spar_predictor_.query_observations_periodic_;
        }
        return spar_predictor_.query_observations_temporal_;
    }

    uint32_t get_cache_size( bool is_periodic ) {
        if( is_periodic ) {
            return spar_predictor_.PERIODIC_LRU_CACHE_SIZE;
        }
        return spar_predictor_.TEMPORAL_LRU_CACHE_SIZE;
    }

    folly::Synchronized<
        std::unordered_map<query_id, std::shared_ptr<multi_linear_predictor>>>
        &get_predictors() {
        return spar_predictor_.predictors_;
    }
};

uint32_t spar_predictor_test::eviction_count = 0;

TEST_F( spar_predictor_test, historic_load_test_empty ) {
    const query_id   query_a = 1;
    const epoch_time rounding_a = 2;

    {
        auto temporal_map = get_query_observations_map( false ).wlock();
        temporal_map->clear();
    }
    auto &      temporal_map_synchronized = get_query_observations_map( false );
    query_count no_results =
        get_historic_load( query_a, 0, temporal_map_synchronized, rounding_a );

    EXPECT_EQ( no_results, 0 );
}

TEST_F( spar_predictor_test, historic_load_test_rounding ) {
    const query_id   query_a = 1, query_b = 2;
    const epoch_time rounding_factor = 10, early_time = 0, late_time = 100;

    {
        auto temporal_map = get_query_observations_map( false ).wlock();
        temporal_map->clear();

        auto early_entries =
            std::make_shared<std::unordered_map<query_id, query_count>>();
        early_entries->insert( {query_a, 1} );
        early_entries->insert( {query_b, 2} );

        auto late_entries =
            std::make_shared<std::unordered_map<query_id, query_count>>();
        late_entries->insert( {query_a, 1} );

        temporal_map->insert( early_time, early_entries );
        temporal_map->insert( late_time, late_entries );
    }

    auto &temporal_map_synchronized = get_query_observations_map( false );

    // Verify behavior for the earlier time
    query_count results = get_historic_load(
        query_a, 0, temporal_map_synchronized, rounding_factor );
    EXPECT_EQ( results, 1 );

    results = get_historic_load( query_b, 1, temporal_map_synchronized,
                                 rounding_factor );
    EXPECT_EQ( results, 2 );

    results = get_historic_load( query_b, 7, temporal_map_synchronized,
                                 rounding_factor );
    EXPECT_EQ( results, 2 );

    results = get_historic_load( query_b, 10, temporal_map_synchronized,
                                 rounding_factor );
    EXPECT_EQ( results, 0 );

    // Verify behavior for the late time
    results = get_historic_load( query_a, 101, temporal_map_synchronized,
                                 rounding_factor );
    EXPECT_EQ( results, 1 );

    results = get_historic_load( query_b, 101, temporal_map_synchronized,
                                 rounding_factor );
    EXPECT_EQ( results, 0 );
}

TEST_F( spar_predictor_test, estimated_query_count_no_predictor_test ) {
    const query_id query_a = 1;
    {
        auto predictors = get_predictors().wlock();
        predictors->clear();
    }

    query_count result_queries =
        spar_predictor_.get_estimated_query_count( query_a, 0 );
    EXPECT_EQ( result_queries, 0 );
}

TEST_F( spar_predictor_test, estimated_query_count_queried_test ) {
    const query_id query_a = 1, query_b = 2;

    EXPECT_CALL( *multi_linear_predictor_mock_, make_prediction( _ ) )
        .Times( 1 );

    ON_CALL( *multi_linear_predictor_mock_, make_prediction( _ ) )
        .WillByDefault( Return( 37 ) );

    {
        auto predictors = get_predictors().wlock();
        predictors->clear();

        predictors->insert( {query_a, multi_linear_predictor_mock_} );
        predictors->insert( {query_b, multi_linear_predictor_mock_} );
    }

    query_count result =
        spar_predictor_.get_estimated_query_count( query_a, 0, 0 );
    EXPECT_EQ( result, 37 );
}

TEST_F( spar_predictor_test, add_observation_by_type_test ) {
    const query_id   query_a = 1, query_b = 2, query_c = 3;
    const epoch_time rounding_factor = 10, early_time = 0, late_time = 100;

    {
        auto temporal_map = get_query_observations_map( false ).wlock();
        temporal_map->clear();

        auto early_entries =
            std::make_shared<std::unordered_map<query_id, query_count>>();
        early_entries->insert( {query_a, 1} );
        early_entries->insert( {query_b, 2} );

        auto late_entries =
            std::make_shared<std::unordered_map<query_id, query_count>>();
        late_entries->insert( {query_a, 1} );

        temporal_map->insert( early_time, early_entries );
        temporal_map->insert( late_time, late_entries );
    }

    // First ensure that we can add new queries to the observation map
    auto &temporal_map_synchronized = get_query_observations_map( false );
    add_observation_by_type( query_c, early_time + 3, 1,
                             temporal_map_synchronized, rounding_factor );

    {
        auto temporal_map = temporal_map_synchronized.wlock();

        const auto early_slot = temporal_map->get( early_time );
        EXPECT_EQ( early_slot->at( query_a ), 1 );
        EXPECT_EQ( early_slot->at( query_b ), 2 );
        EXPECT_EQ( early_slot->at( query_c ), 1 );

        const auto late_slot = temporal_map->get( late_time );
        EXPECT_EQ( late_slot->at( query_a ), 1 );
        EXPECT_EQ( late_slot->find( query_b ), late_slot->end() );
        EXPECT_EQ( late_slot->find( query_c ), late_slot->end() );
    }

    // Add another query for query_c, ensure that incrementing is functional
    add_observation_by_type( query_c, early_time + 4, 1,
                             temporal_map_synchronized, rounding_factor );
    {
        auto temporal_map = temporal_map_synchronized.wlock();

        auto early_slot = temporal_map->get( early_time );
        EXPECT_EQ( early_slot->at( query_a ), 1 );
        EXPECT_EQ( early_slot->at( query_b ), 2 );
        EXPECT_EQ( early_slot->at( query_c ), 2 );

        auto late_slot = temporal_map->get( late_time );
        EXPECT_EQ( late_slot->at( query_a ), 1 );
        EXPECT_EQ( late_slot->find( query_b ), late_slot->end() );
        EXPECT_EQ( late_slot->find( query_c ), late_slot->end() );
    }
}

TEST_F( spar_predictor_test, add_observation_test ) {
    const query_id   query_a = 1, query_b = 2, query_c = 3;
    const epoch_time early_time = 0;

    // Recall that temporal rounding is 10, periodic is 20
    {
        auto temporal_map = get_query_observations_map( false ).wlock();
        temporal_map->clear();
        auto periodic_map = get_query_observations_map( true ).wlock();
        periodic_map->clear();

        auto early_entries_temporal =
            std::make_shared<std::unordered_map<query_id, query_count>>();
        early_entries_temporal->insert( {query_a, 1} );
        early_entries_temporal->insert( {query_b, 2} );

        auto early_entries_periodic =
            std::make_shared<std::unordered_map<query_id, query_count>>();
        early_entries_periodic->insert( {query_a, 1} );
        early_entries_periodic->insert( {query_b, 2} );

        temporal_map->insert( early_time, early_entries_temporal );
        periodic_map->insert( early_time, early_entries_periodic );
    }

    // First ensure that we can add new queries to the observation map,
    // as for query time of 15, it should be an entry at 10 for temporal
    // and query of 0 for periodic
    spar_predictor_.add_observation( query_c, 15, 1 );
    spar_predictor_.add_observation( query_a, 0, 1 );
    spar_predictor_.add_observation( query_a, 15, 1 );

    auto temporal_map = get_query_observations_map( false ).wlock();
    auto periodic_map = get_query_observations_map( true ).wlock();

    auto early_slot = temporal_map->get( early_time );
    EXPECT_EQ( early_slot->at( query_a ), 2 );
    EXPECT_EQ( early_slot->at( query_b ), 2 );
    EXPECT_EQ( early_slot->find( query_c ), early_slot->end() );

    early_slot = periodic_map->get( early_time );
    EXPECT_EQ( early_slot->at( query_a ), 3 );
    EXPECT_EQ( early_slot->at( query_b ), 2 );
    EXPECT_EQ( early_slot->at( query_c ), 1 );

    // Delayed timeslot (epoch time of 15)
    early_slot = temporal_map->get( early_time + 10 );
    EXPECT_EQ( early_slot->at( query_a ), 1 );
    EXPECT_EQ( early_slot->find( query_b ), early_slot->end() );
    EXPECT_EQ( early_slot->at( query_c ), 1 );

    // No such time entry should exist for periodic map
    EXPECT_EQ( periodic_map->find( early_time + 10 ), periodic_map->end() );
}

TEST_F( spar_predictor_test, train_test ) {
    const query_id query_a = 123;
    EXPECT_CALL( *multi_linear_predictor_mock_, add_observation( _, _ ) )
        .Times( 40 );

    std::cerr << "Value: " << spar_configs_ << std::endl;

    {
        auto predictors = get_predictors().wlock();
        predictors->insert( {query_a, multi_linear_predictor_mock_} );
    }

    spar_predictor_.train( query_a, 0.0 );
}

TEST_F( spar_predictor_test, get_input_coefficients_test ) {
    /*
     * The approach for verifying correctness is to create an  identity map for
     * the temporal and periodic observations the will facilitate easier
     * calculations. That is, at time t, there were t observed queries.
     *
     * The following values are assumed:
     * 200 = width
     * 10 = slot
     * 10 = temporal granularity
     * 20 = periodic granularity
     * 2 =  arrival window
     * 30 = temporal search space
     * 7 = periodic search space
     */

    const query_id query_a = 1;
    {
        auto temporal_map = get_query_observations_map( false ).wlock();
        temporal_map->clear();
        auto periodic_map = get_query_observations_map( true ).wlock();
        periodic_map->clear();

        for( int i = 0; i <= 2000;
             i += spar_configs_.rounding_factor_temporal_ ) {
            auto map =
                std::make_shared<std::unordered_map<query_id, query_count>>();
            map->insert( {query_a, i} );
            temporal_map->insert( i, map );
        }

        for( int i = 0; i <= 2000;
             i += spar_configs_.rounding_factor_periodic_ ) {
            auto map =
                std::make_shared<std::unordered_map<query_id, query_count>>();
            map->insert(
                {query_a, i * 2} );  // multiply by 2 because of normalization
            periodic_map->insert( i, map );
        }
    }

    auto ret = get_input_coefficients( query_a, 2002, 1909 );

    /**
     * Part 1: Periodic Signals
     *
     * These are computed using the following relation:
     * ret[i) = 2000 - 200 * i, where i = 1 .. 7
     *
     * However, the normalization will result in division by two because
     * the periodic granularity is 20 and slots are 10.
     */

    EXPECT_EQ( ret[0], 1800 );
    EXPECT_EQ( ret[1], 1600 );
    EXPECT_EQ( ret[2], 1400 );
    EXPECT_EQ( ret[3], 1200 );
    EXPECT_EQ( ret[4], 1000 );
    EXPECT_EQ( ret[5], 800 );
    EXPECT_EQ( ret[6], 600 );

    /**
     * Part 2: Temporal Signals
     *
     * These are computed using an inner and outer loop. The outer loop
     * iterates
     * through values j = 1, ..., m for all temporal historical queries.
     *
     * However, the inner loop is used to compute the delta value. To compute
     * the delta value, another index i = 1, ..., n for all periodic values are
     * used.
     *
     * For example, for j = 1, we would perform:
     *   [1900 - 1 * 10 - 1 * 200 + ...  + 1900 - 1 * 10 - 7 * 200]
     *    ^      ^   ^    ^   ^
     * curr_time j   slot i   width
     *
     * After, the average is taken of all those values, and compared with
     * the current temporal difference, that is curr_time - j * temp.
     *
     * NOTE: We may add "1" because the historical lookup tends to round-up
     * query counts due to potential floating point inprecision it may report
     * 1 higher.
     */

    EXPECT_EQ( ret[7], 811.0 );

    /*
     * Calculation:
     * 1900 - 2 * 10 +
     *  + (1/7) * (1900 - 2 * 10 - 1 * 200 + ... + 1900 - 1 * 20 - 7 * 20)
     *    ^        ^      ^   ^    ^    ^                          ^
     *    |        |      |   |    |    |                          |
     *    1/n   curr_time i  slot  j   width                       m
     */

    EXPECT_EQ( ret[8], 801.0 );
    EXPECT_EQ( ret[9], 811.0 );
    EXPECT_EQ( ret[10], 801.0 );
    EXPECT_EQ( ret[11], 811.0 );
    EXPECT_EQ( ret[12], 801.0 );
    EXPECT_EQ( ret[13], 811.0 );
    EXPECT_EQ( ret[14], 801.0 );
    EXPECT_EQ( ret[15], 811.0 );
    EXPECT_EQ( ret[16], 801.0 );
    EXPECT_EQ( ret[17], 811.0 );
    EXPECT_EQ( ret[18], 801.0 );
    EXPECT_EQ( ret[19], 811.0 );
    EXPECT_EQ( ret[20], 801.0 );
    EXPECT_EQ( ret[21], 811.0 );
    EXPECT_EQ( ret[22], 801.0 );
    EXPECT_EQ( ret[23], 811.0 );
    EXPECT_EQ( ret[24], 801.0 );
    EXPECT_EQ( ret[25], 811.0 );
    EXPECT_EQ( ret[26], 801.0 );
    EXPECT_EQ( ret[27], 811.0 );
    EXPECT_EQ( ret[28], 801.0 );
    EXPECT_EQ( ret[29], 811.0 );
    EXPECT_EQ( ret[30], 801.0 );
    EXPECT_EQ( ret[31], 811.0 );
    EXPECT_EQ( ret[32], 801.0 );
    EXPECT_EQ( ret[33], 811.0 );
    EXPECT_EQ( ret[34], 801.0 );
    EXPECT_EQ( ret[35], 811.0 );
    EXPECT_EQ( ret[36], 801.0 );
}
