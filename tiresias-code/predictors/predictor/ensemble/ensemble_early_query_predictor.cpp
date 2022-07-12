#include "glog/logging.h"
#include <chrono>

#include "../../constants.h"
#include "ensemble_early_query_predictor.h"

ensemble_early_query_predictor::ensemble_early_query_predictor(
    const ensemble_early_query_predictor_configs &ensemble_config )
    : ensemble_config_( ensemble_config ) {}

std::unordered_set<query_id>
    ensemble_early_query_predictor::get_predicted_queries(
        epoch_time begin_time, epoch_time end_time, double threshold ) {
    auto       query_observations = query_observations_.wlock();
    const auto query_ids_observed = query_ids_observed_.rlock();

    std::unordered_set<query_id> resp;

    /**
     * Round inclusively, such that all results appear within the requested
     * window (ex: the observed query time for any
     * query is not earlier than begin_time or later than end_time)
     * */

    const int32_t rounding_factor = ensemble_config_.slot_;
    begin_time += ( rounding_factor - ( begin_time % rounding_factor ) ) %
                  rounding_factor;
    end_time -= end_time % rounding_factor;

    for( epoch_time candidate_time = begin_time; candidate_time <= end_time;
         candidate_time += rounding_factor ) {
        for( auto &q_id : *query_ids_observed ) {
            if( resp.find( q_id ) == resp.end() &&
                get_estimated_query_count( q_id, candidate_time ) >=
                    threshold ) {
                resp.insert( q_id );
            }
        }
    }
    return resp;
}

query_count ensemble_early_query_predictor::get_estimated_query_count(
    query_id search_query_id, epoch_time candidate_time ) {
    epoch_time current_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch() )
            .count();
    return get_estimated_query_count( search_query_id, candidate_time,
                                      current_time );
}

query_count ensemble_early_query_predictor::get_estimated_query_count(
    query_id search_query_id, epoch_time candidate_time,
    epoch_time curr_time ) {

    query_count linear_count = 0, rnn_count = 0, outlier_count = 0;

    auto &linear_predictors = *( ensemble_linear_predictors_.wlock() );
    if( linear_predictors.find( search_query_id ) != linear_predictors.end() ) {
        auto &linear_predictor = linear_predictors[search_query_id];
        linear_count = linear_predictor->get_estimated_query_count(
            search_query_id, candidate_time );
    }

    auto &rnn_predictors = *( ensemble_rnn_predictors_.wlock() );
    if( rnn_predictors.find( search_query_id ) != rnn_predictors.end() ) {
        auto &rnn_predictor = rnn_predictors[search_query_id];
        rnn_count = rnn_predictor->get_estimated_query_count(
            search_query_id, candidate_time, curr_time );
    }

    if( ensemble_config_.apply_scheduled_outlier_model_ ) {
        auto &scheduled_predictors =
            *( ensemble_scheduled_predictors_.wlock() );
        if( scheduled_predictors.find( search_query_id ) !=
            scheduled_predictors.end() ) {
            auto &scheduled_predictor = scheduled_predictors[search_query_id];
            outlier_count = scheduled_predictor->get_estimated_query_count(
                search_query_id, candidate_time );
        }
    } else {
        auto &krls_predictors = *( ensemble_krls_predictors_.wlock() );
        if( krls_predictors.find( search_query_id ) != krls_predictors.end() ) {
            auto &krls_predictor = krls_predictors[search_query_id];
            outlier_count = krls_predictor->get_estimated_query_count(
                search_query_id, candidate_time );
        }
    }

    // If the krls count is an outlier, we pick that result, otherwise return
    // hybrid between the linear model and RNN (both weighed as 0.5)
    query_count hybrid_model = ( linear_count + rnn_count ) / 2;
    if( outlier_count > ensemble_config_.outlier_threshold_ * hybrid_model ) {
        return outlier_count;
    }

    return hybrid_model;
}

void ensemble_early_query_predictor::add_query_distribution(
    query_id q_id, std::shared_ptr<scheduled_distribution> dist ) {
    auto &ensemble_scheduled_predictors =
        ( *ensemble_scheduled_predictors_.wlock() );
    if( ensemble_scheduled_predictors.find( q_id ) ==
        ensemble_scheduled_predictors.end() ) {
        ensemble_scheduled_predictors[q_id] =
            std::make_shared<ensemble_scheduled_predictor>(
                query_observations_, ensemble_config_,
                get_max_query_count( q_id ) );
    }
    ensemble_scheduled_predictors[q_id]->add_query_distribution( dist );
}

query_count ensemble_early_query_predictor::get_predicted_arrival(
    query_id search_query_id, double threshold ) {
    epoch_time current_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch() )
            .count();

    return get_predicted_arrival( search_query_id, threshold, current_time );
}

epoch_time ensemble_early_query_predictor::get_predicted_arrival(
    query_id search_query_id, double threshold, epoch_time current_time ) {
    const int32_t rounding_factor = ensemble_config_.slot_;
    const int     search_window =
        ensemble_config_.search_window_ * ensemble_config_.slot_;

    current_time = current_time - current_time % rounding_factor;

    for( epoch_time candidate_time = current_time;
         candidate_time <= current_time + search_window;
         candidate_time += rounding_factor ) {
        if( get_estimated_query_count( search_query_id, candidate_time ) >=
            threshold ) {
            return candidate_time;
        }
    }

    return std::numeric_limits<epoch_time>::max();
}

void ensemble_early_query_predictor::train( epoch_time current_time ) {
    auto query_ids_refreshed = query_ids_refreshed_.wlock();
    auto linear_predictors = *( ensemble_linear_predictors_.wlock() );
    auto krls_predictors = *( ensemble_krls_predictors_.wlock() );
    auto rnn_predictors = *( ensemble_rnn_predictors_.wlock() );

    for( auto query_id : *query_ids_refreshed ) {
        // DVLOG( 40 ) << "Training linear model for " << query_id;

        if( linear_predictors.find( query_id ) != linear_predictors.end() ) {
            linear_predictors[query_id]->train( query_id, current_time );
        }

        if( rnn_predictors.find( query_id ) != rnn_predictors.end() ) {
            rnn_predictors[query_id]->train( query_id, current_time );
        }

        if( !ensemble_config_.apply_scheduled_outlier_model_ ) {
            // DVLOG( 40 ) << "Training krls model for " << query_id;
            if( krls_predictors.find( query_id ) != krls_predictors.end() ) {
                krls_predictors[query_id]->train( query_id, current_time );
            }
        }
    }

    query_ids_refreshed->clear();
    // DVLOG( 40 ) << "Finished training ensemble models";
}

void ensemble_early_query_predictor::add_observation(
    query_id observed_query_id, epoch_time time, query_count count ) {
    auto query_ids_refreshed = query_ids_refreshed_.wlock();
    auto query_ids_observed = query_ids_observed_.wlock();
    auto query_observations = query_observations_.wlock();

    query_ids_refreshed->insert( observed_query_id );
    query_ids_observed->insert( observed_query_id );

    time -= time % ensemble_config_.slot_;

    if( !query_observations->exists( time ) ) {
        query_observations->insert(
            time,
            std::make_shared<std::unordered_map<query_id, query_count>>() );
    }
    auto &query_observation_map = query_observations->get( time );
    if( query_observation_map->find( observed_query_id ) ==
        query_observation_map->end() ) {
        query_observation_map->insert( {observed_query_id, 0} );
    }
    query_observation_map->at( observed_query_id ) += count;

    auto &ensemble_linear_predictors = *( ensemble_linear_predictors_.wlock() );
    auto &ensemble_krls_predictors = *( ensemble_krls_predictors_.wlock() );
    auto &ensemble_rnn_predictors = *( ensemble_rnn_predictors_.wlock() );
    auto &ensemble_scheduled_predictors =
        *( ensemble_scheduled_predictors_.wlock() );

    if( ensemble_linear_predictors.find( observed_query_id ) ==
        ensemble_linear_predictors.end() ) {
        ensemble_linear_predictors[observed_query_id] =
            std::make_shared<ensemble_linear_predictor>(
                query_observations_, ensemble_config_,
                get_max_query_count( observed_query_id ) );
    }

    if( ensemble_config_.apply_scheduled_outlier_model_ ) {
        if( ensemble_scheduled_predictors.find( observed_query_id ) ==
            ensemble_scheduled_predictors.end() ) {
            ensemble_scheduled_predictors[observed_query_id] =
                std::make_shared<ensemble_scheduled_predictor>(
                    query_observations_, ensemble_config_,
                    get_max_query_count( observed_query_id ) );
        }
    } else {
        if( ensemble_krls_predictors.find( observed_query_id ) ==
            ensemble_krls_predictors.end() ) {
            ensemble_krls_predictors[observed_query_id] =
                std::make_shared<ensemble_krls_predictor>(
                    query_observations_, ensemble_config_,
                    get_max_query_count( observed_query_id ) );
        }
    }

    if( ensemble_rnn_predictors.find( observed_query_id ) ==
        ensemble_rnn_predictors.end() ) {
        ensemble_rnn_predictors[observed_query_id] =
            std::make_shared<ensemble_rnn_predictor>(
                query_observations_, ensemble_config_,
                get_max_query_count( observed_query_id ) );
    }
}

void ensemble_early_query_predictor::add_observations(
    const std::vector<std::pair<query_id, epoch_time>> &observed_queries ) {
    for( const auto &observed_query : observed_queries ) {
        ensemble_early_query_predictor::add_observation(
            observed_query.first, observed_query.second );
    }
}

void ensemble_early_query_predictor::add_observations(
    const std::vector<query_id> &observed_queries, epoch_time time ) {
    for( const auto &observed_query : observed_queries ) {
        ensemble_early_query_predictor::add_observation( observed_query, time );
    }
}

void ensemble_early_query_predictor::add_query_schedule(
    query_id scheduled_query_id, epoch_time period_length ) {}

void ensemble_early_query_predictor::add_query_schedule(
    const std::vector<query_id> &scheduled_query_ids,
    epoch_time                   period_length ) {
    for( const auto &scheduled_query_id : scheduled_query_ids ) {
        ensemble_early_query_predictor::add_query_schedule( scheduled_query_id,
                                                            period_length );
    }
}

void ensemble_early_query_predictor::add_linear_query_predictor(
    query_id q_id, std::shared_ptr<ensemble_linear_predictor> predictor ) {
    auto linear_predictors_map = ensemble_linear_predictors_.wlock();
    ( *linear_predictors_map )[q_id] = predictor;
}

void ensemble_early_query_predictor::add_krls_query_predictor(
    query_id q_id, std::shared_ptr<ensemble_krls_predictor> predictor ) {
    if( ensemble_config_.apply_scheduled_outlier_model_ ) {
        LOG( FATAL ) << "Unexpected scheduled predictor while scheduled "
                        "outlier model prediction is enabled.";
    }

    auto krls_predictors_map = ensemble_krls_predictors_.wlock();
    ( *krls_predictors_map )[q_id] = predictor;
}

void ensemble_early_query_predictor::add_scheduled_query_predictor(
    query_id q_id, std::shared_ptr<ensemble_scheduled_predictor> predictor ) {
    if( !ensemble_config_.apply_scheduled_outlier_model_ ) {
        LOG( FATAL ) << "Unexpected scheduled predictor while scheduled "
                        "outlier model prediction is disabled.";
    }

    auto scheduled_predictors_map = ensemble_scheduled_predictors_.wlock();
    ( *scheduled_predictors_map )[q_id] = predictor;
}

void ensemble_early_query_predictor::add_rnn_query_predictor(
    query_id q_id, std::shared_ptr<ensemble_rnn_predictor> predictor ) {
    auto rnn_predictors_map = ensemble_rnn_predictors_.wlock();
    ( *rnn_predictors_map )[q_id] = predictor;
}
