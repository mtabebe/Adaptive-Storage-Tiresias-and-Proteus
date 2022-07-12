#pragma once

#include <climits>
#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "../../common/perf_tracking.h"
#include "../../common/scan_results.h"
#include "../../common/string_conversion.h"
#include "../../common/thread_utils.h"
#include "record-types/twitter_primary_key_generation.h"
#include "record-types/twitter_record_types.h"
#include "twitter_db_operators.h"
#include "twitter_mr.h"

twitter_benchmark_worker_types
    twitter_benchmark_worker_templ::twitter_benchmark_worker(
        uint32_t client_id, db_abstraction* db, zipf_distribution_cdf* z_cdf,
        const workload_operation_selector& op_selector,
        const twitter_configs&           configs,
        const db_abstraction_configs&      abstraction_configs )
    : db_operators_( client_id, db, abstraction_configs,
                     configs.bench_configs_.limit_update_propagation_ , false /* store global state */),
      generator_( z_cdf, op_selector, client_id, configs ),
      statistics_(),
      worker_( nullptr ),
      done_( false ) {
    statistics_.init( k_twitter_workload_operations );
}

twitter_benchmark_worker_types
    twitter_benchmark_worker_templ::~twitter_benchmark_worker() {}

twitter_benchmark_worker_types void
    twitter_benchmark_worker_templ::start_timed_workload() {
    worker_ = std::unique_ptr<std::thread>(
        new std::thread( &twitter_benchmark_worker_templ::run_workload, this ) );
}

twitter_benchmark_worker_types void
    twitter_benchmark_worker_templ::stop_timed_workload() {
    done_ = true;
    DCHECK( worker_ );
    join_thread( *worker_ );
    worker_ = nullptr;
}

twitter_benchmark_worker_types void twitter_benchmark_worker_templ::run_workload() {
    // time me boys
    DVLOG( 10 ) << db_operators_.client_id_ << " starting workload";
    std::chrono::high_resolution_clock::time_point s =
        std::chrono::high_resolution_clock::now();

    // this will only every be true once so it's likely to not be done

    while( likely( !done_ ) ) {
        // we don't want to check this very often so we do a bunch of operations
        // in a loop
        for( uint32_t op_count = 0;
             op_count < generator_.get_num_ops_before_timer_check();
             op_count++ ) {
            do_workload_operation();
        }
    }
    std::chrono::high_resolution_clock::time_point e =
        std::chrono::high_resolution_clock::now();
    std::chrono::duration<int, std::nano> elapsed = e - s;
    DVLOG( 10 ) << db_operators_.client_id_ << " ran for:" << elapsed.count()
                << " ns";
    statistics_.store_running_time( elapsed.count() );
}

twitter_benchmark_worker_types void
    twitter_benchmark_worker_templ::do_workload_operation() {

    double                  lat;

    workload_operation_enum op = generator_.get_operation();
    DCHECK_GE( op, TWITTER_GET_TWEET );
    DCHECK_LE( op, TWITTER_UPDATE_FOLLOWERS );

    DVLOG( 10 ) << db_operators_.client_id_ << " performing "
                << workload_operation_string( op );
    start_timer( TWITTER_WORKLOAD_OP_TIMER_ID );
    workload_operation_outcome_enum status = perform_workload_operation( op );
    stop_and_store_timer( TWITTER_WORKLOAD_OP_TIMER_ID, lat );

    statistics_.add_outcome( op, status, lat );

    DVLOG( 10 ) << db_operators_.client_id_ << " performing "
                << k_workload_operations_to_strings.at( op )
                << " status:" << status;
}

twitter_benchmark_worker_types workload_operation_outcome_enum
    twitter_benchmark_worker_templ::perform_workload_operation(
        const workload_operation_enum& op ) {
    switch( op ) {
        case TWITTER_GET_TWEET:
            return do_get_tweet();
        case TWITTER_GET_TWEETS_FROM_FOLLOWING:
            return do_get_tweets_from_following();
        case TWITTER_GET_FOLLOWERS:
            return do_get_followers();
        case TWITTER_GET_USER_TWEETS:
            return do_get_user_tweets();
        case TWITTER_INSERT_TWEET:
            return do_insert_tweet();
        case TWITTER_GET_RECENT_TWEETS:
            return do_get_recent_tweets();
        case TWITTER_GET_TWEETS_FROM_FOLLOWERS:
            return do_get_tweets_from_followers();
        case TWITTER_GET_TWEETS_LIKE:
            return do_get_tweets_like();
        case TWITTER_UPDATE_FOLLOWERS:
            return do_update_followers();
    }
    // should be unreachable
    return false;
}

twitter_benchmark_worker_types inline benchmark_statistics
    twitter_benchmark_worker_templ::get_statistics() const {
    return statistics_;
}

twitter_benchmark_worker_types workload_operation_enum
    twitter_benchmark_worker_templ::do_update_followers() {

    return perform_update_followers( generator_.get_user(),
                                     generator_.get_user() );
}

twitter_benchmark_worker_types workload_operation_enum
    twitter_benchmark_worker_templ::perform_update_followers(
        int32_t u_id, int32_t follower_u_id ) {
    if( u_id == follower_u_id ) {
        return WORKLOAD_OP_SUCCESS;
    }

    DVLOG( 20 ) << "UpdateFollowers( user_id:" << u_id
                << ", follower_u_id:" << follower_u_id << " )";

    auto follow_nums = perform_get_follower_counts( u_id, follower_u_id );
    int32_t num_follows = std::get<0>( follow_nums );
    int32_t num_following = std::get<1>( follow_nums );
    if( num_follows <= 0 ) {
        return WORKLOAD_OP_SUCCESS;
    }
    else if( num_following <= 0 ) {
        return WORKLOAD_OP_SUCCESS;
    }

    uint32_t follower_id = generator_.get_follower_id( num_follows );
    uint32_t follows_id = generator_.get_follower_id( num_following );

    workload_operation_enum ret =
        do_update_follower_and_follows( u_id, follower_u_id, follower_id, follows_id );

    DVLOG( 20 ) << "UpdateFollowers( user_id:" << u_id
                << ", follower_u_id:" << follower_u_id << " ), okay!";

    return ret;
}

twitter_benchmark_worker_types std::tuple<int32_t, int32_t>
                               twitter_benchmark_worker_templ::perform_get_follower_counts(
        int32_t u_id, int32_t follower_u_id ) {

    DVLOG( 20 ) << "GetFollowersCount( user_id:" << u_id
                << ", follower_u_id:" << follower_u_id << " )";

    auto u_ckr =
        create_cell_key_ranges( k_twitter_user_profiles_table_id, u_id, u_id,
                                twitter_user_profile_cols::num_followers,
                                twitter_user_profile_cols::num_followers );
    auto f_ckr = create_cell_key_ranges(
        k_twitter_user_profiles_table_id, follower_u_id, follower_u_id,
        twitter_user_profile_cols::num_following,
        twitter_user_profile_cols::num_following );

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> ckrs = { u_ckr, f_ckr };
        std::vector<cell_key_ranges> empty_ckrs;
        db_operators_.begin_transaction( empty_ckrs, ckrs, "UpdateFollowers" );
    }

    RETURN_AND_COMMIT_IF_SS_DB( std::make_tuple<>( 1, 1 ) );

    std::vector<int32_t> followers;
    std::vector<int32_t> following;
    do_get_num_followers( u_ckr, followers );
    do_get_num_followers( f_ckr, following );

    int32_t num_followers = -1;
    int32_t num_follows = -1;

    if( followers.size() == 1 ) {
        num_followers = followers.at( 0 );
    }
    if( following.size() == 1 ) {
        num_follows = following.at( 0 );
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "GetFollowersCount( user_id:" << u_id
                << ", follower_u_id:" << follower_u_id << " ), returning ("
                << num_followers << ", " << num_follows << ")!";

    return std::make_tuple<>( num_followers, num_follows );
}

twitter_benchmark_worker_types void
    twitter_benchmark_worker_templ::do_get_num_followers(
        const cell_key_ranges& ckr, std::vector<int32_t>& num_followers ) {
    twitter_user_profile prof;
    for( int64_t u_id = ckr.row_id_start; u_id <= ckr.row_id_end; u_id++ ) {
        bool read = lookup_twitter_user_profile( &db_operators_, &prof, u_id,
                                                 ckr, false /* is latest */,
                                                 true /* allow nullable */ );

        if( read ) {
            num_followers.emplace_back( prof.num_followers );
        } else {
            num_followers.emplace_back( -1 );
        }
    }
}
twitter_benchmark_worker_types void
    twitter_benchmark_worker_templ::do_get_num_following(
        const cell_key_ranges& ckr, std::vector<int32_t>& num_following ) {
    twitter_user_profile prof;
    for( int64_t u_id = ckr.row_id_start; u_id <= ckr.row_id_end; u_id++ ) {
        bool read = lookup_twitter_user_profile( &db_operators_, &prof, u_id,
                                                 ckr, false /* is latest */,
                                                 true /* allow nullable */ );

        if( read ) {
            num_following.emplace_back( prof.num_following );
        } else {
            num_following.emplace_back( -1 );
        }
    }
}

twitter_benchmark_worker_types workload_operation_enum
    twitter_benchmark_worker_templ::do_update_follower_and_follows(
        int32_t u_id, int32_t follower_u_id, int32_t follower_id,
        int32_t follows_id ) {

    DVLOG( 20 ) << "UpdateFollowerAndFollows( user_id:" << u_id
                << ", follower_u_id:" << follower_u_id
                << ", follower_id:" << follower_id
                << ", follows_id:" << follows_id << " )";

    twitter_followers followers;
    followers.u_id = u_id;
    followers.f_id = follower_id;
    followers.follower_u_id = follower_u_id;

    twitter_follows follows;
    follows.u_id = follower_u_id;
    follows.f_id = follows_id;
    follows.following_u_id = u_id;

    auto u_ckr = create_cell_key_ranges(
        k_twitter_followers_table_id, make_followers_key( followers ),
        make_followers_key( followers ), twitter_followers_cols::follower_u_id,
        twitter_followers_cols::follower_u_id );
    auto f_ckr = create_cell_key_ranges(
        k_twitter_follows_table_id, make_follows_key( follows ),
        make_follows_key( follows ), twitter_follows_cols::following_u_id,
        twitter_follows_cols::following_u_id );
    std::vector<cell_key_ranges> ckrs = { u_ckr, f_ckr };

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> empty_ckrs;
        db_operators_.begin_transaction( ckrs, empty_ckrs, "UpdateFollowers" );
    }
    RETURN_AND_COMMIT_IF_SS_DB( WORKLOAD_OP_SUCCESS );

    do_update_follower( u_id, follower_u_id, follower_id, u_ckr );
    do_update_follows( u_id, follower_u_id, follows_id, f_ckr );

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "UpdateFollowerAndFollows( user_id:" << u_id
                << ", follower_u_id:" << follower_u_id
                << ", follower_id:" << follower_id
                << ", follows_id:" << follows_id << " ), okay!";

    return WORKLOAD_OP_SUCCESS;
}

twitter_benchmark_worker_types void
    twitter_benchmark_worker_templ::do_update_follower(
        int32_t u_id, int32_t follower_u_id, int32_t follower_id,
        const cell_key_ranges& ckr ) {
    twitter_followers followers;
    followers.u_id = u_id;
    followers.f_id = follower_id;
    followers.follower_u_id = follower_u_id;

    update_twitter_followers( &db_operators_, &followers,
                              make_followers_key( followers ), ckr,
                              true /* do propagate */ );
}
twitter_benchmark_worker_types void
    twitter_benchmark_worker_templ::do_update_follows(
        int32_t u_id, int32_t follower_u_id, int32_t follows_id,
        const cell_key_ranges& ckr ) {
    twitter_follows follows;
    follows.u_id = follower_u_id;
    follows.f_id = follows_id;
    follows.following_u_id = u_id;

    update_twitter_follows( &db_operators_, &follows,
                            make_follows_key( follows ), ckr,
                            true /* do propagate */ );
}

twitter_benchmark_worker_types workload_operation_outcome_enum
    twitter_benchmark_worker_templ::do_get_recent_tweets() {

    uint32_t user = generator_.get_user();
    uint64_t cur_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch() )
            .count();
    return perform_get_recent_tweets(
        std::max( (uint32_t) 0, user / 2 ),
        std::min( ( uint32_t )( generator_.configs_.num_users_ - 1 ),
                  ( uint32_t )( user * 2 ) ),
        0, generator_.configs_.limit_tweets_, cur_time - ( 60 * 1000 ),
        cur_time - ( 5 * 1000 ) );
}

twitter_benchmark_worker_types workload_operation_outcome_enum
    twitter_benchmark_worker_templ::do_get_tweets_from_followers() {
    uint32_t user = generator_.get_user();
    return perform_get_tweets_from_followers(
        user, 0, generator_.configs_.limit_followers_,
        std::max( (uint32_t) 0,
                  user - ( generator_.configs_.limit_followers_ * 2 ) ),
        std::min( ( uint32_t )( generator_.configs_.num_users_ - 1 ),
                  ( uint32_t )(
                      user + ( generator_.configs_.limit_followers_ * 2 ) ) ),
        0, generator_.configs_.limit_tweets_ );
}

twitter_benchmark_worker_types workload_operation_outcome_enum
    twitter_benchmark_worker_templ::do_get_tweets_like() {
    auto string_range = generator_.get_tweet_string_range();

    uint32_t user = generator_.get_user();
    return perform_get_tweets_like(
        std::max( (uint32_t) 0, user / 2 ),
        std::min( ( uint32_t )( generator_.configs_.num_users_ - 1 ),
                  ( uint32_t )( user * 2 ) ),
        0, generator_.configs_.limit_tweets_, std::get<0>( string_range ),
        std::get<1>( string_range ) );

    return WORKLOAD_OP_SUCCESS;
}

twitter_benchmark_worker_types workload_operation_outcome_enum
                               twitter_benchmark_worker_templ::do_get_tweet() {
    return perform_get_tweet( generator_.get_user(), generator_.get_tweet() );
}

twitter_benchmark_worker_types workload_operation_outcome_enum
    twitter_benchmark_worker_templ::perform_get_tweets_from_followers(
        uint32_t u_id, uint32_t followers_start, uint32_t followers_end,
        uint32_t follower_id_start, uint32_t follower_id_end,
        uint32_t tweet_start, uint32_t tweet_end ) {

    DVLOG( 20 ) << "GetTweetsFromFollowers( user_id:" << u_id
                << ", followers_start:" << followers_start
                << ", followers_end:" << followers_end
                << ", follower_id_start:" << follower_id_start
                << ", follower_id_end:" << follower_id_end
                << ", tweet_start:" << tweet_start
                << ", tweet_end:" << tweet_end << " )";

    std::vector<scan_arguments> scan_args =
        generate_get_tweets_from_followers_scan_args(
            u_id, followers_start, followers_end, follower_id_start,
            follower_id_end, tweet_start, tweet_end );

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs,
                                              "GetTweetsFromFollowers" );
    }

    workload_operation_outcome_enum outcome = WORKLOAD_OP_SUCCESS;
    RETURN_AND_COMMIT_IF_SS_DB( outcome );

    scan_result scan_res;
    uint32_t    num_tweets = get_tweets_from_followers_by_scan(
        map_scan_args( scan_args ), scan_res );

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "GetTweetsFromFollowers( user_id:" << u_id
                << ", followers_stars:" << followers_start
                << ", followers_end:" << followers_end
                << ", follower_id_start:" << follower_id_start
                << ", follower_id_end:" << follower_id_end
                << ", tweet_start:" << tweet_start
                << ", tweet_end:" << tweet_end
                << " ), num_tweets:" << num_tweets;

    return WORKLOAD_OP_SUCCESS;
}

twitter_benchmark_worker_types std::vector<scan_arguments>
                               twitter_benchmark_worker_templ::
        generate_get_tweets_from_followers_scan_args(
            uint32_t u_id, uint32_t followers_start, uint32_t followers_end,
            uint32_t follower_id_start, uint32_t follower_id_end,
            uint32_t tweet_start, uint32_t tweet_end ) const {

    twitter_followers followers;
    followers.u_id = u_id;
    followers.f_id = followers_start;

    uint64_t low_key = make_followers_key( followers );
    followers.f_id = followers_end;
    uint64_t high_key = make_followers_key( followers );

    cell_key_ranges ckr =
        create_cell_key_ranges( k_twitter_followers_table_id, low_key, high_key,
                                twitter_followers_cols::follower_u_id,
                                twitter_followers_cols::follower_u_id );

    scan_arguments followers_scan_arg;
    followers_scan_arg.label = 0;
    followers_scan_arg.read_ckrs = { ckr };

    cell_predicate c_pred;
    c_pred.table_id = k_twitter_followers_table_id;
    c_pred.col_id = twitter_followers_cols::follower_u_id;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( follower_id_start );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;

    followers_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = uint64_to_string( follower_id_end );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    followers_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    twitter_tweets tweet;
    for( uint32_t u_id = follower_id_start; u_id <= follower_id_end; u_id++ ) {
        tweet.u_id = u_id;
        tweet.tweet_id = tweet_start;
        low_key = make_tweets_key( tweet );

        tweet.tweet_id = tweet_end;
        high_key = make_tweets_key( tweet );

        ckr = create_cell_key_ranges( k_twitter_tweets_table_id, low_key,
                                      high_key, twitter_tweets_cols::u_id,
                                      twitter_tweets_cols::createdate );
    }

    scan_arguments tweets_scan_arg;
    tweets_scan_arg.label = 1;
    tweets_scan_arg.read_ckrs = { ckr };

    c_pred.table_id = k_twitter_tweets_table_id;
    c_pred.col_id = twitter_tweets_cols::tweet_id;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( tweet_start );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;

    tweets_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = uint64_to_string( tweet_end );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    tweets_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.col_id = twitter_tweets_cols::u_id;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( follower_id_start );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;

    tweets_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = uint64_to_string( follower_id_end );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    tweets_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    std::vector<scan_arguments> scan_args = { followers_scan_arg,
                                              tweets_scan_arg };
    return scan_args;
}

twitter_benchmark_worker_types uint32_t
    twitter_benchmark_worker_templ::get_tweets_from_followers_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result&                                   scan_res ) {
    std::vector<uint32_t> followers_project_cols = {
        twitter_followers_cols::follower_u_id };
    std::unordered_map<get_followers_scan_kv_types> followers_map_res;

    if( scan_args.count( 0 ) == 1 ) {
        followers_map_res = db_operators_.scan_mr<get_followers_scan_types>(
            k_twitter_followers_table_id, scan_args.at( 0 ).read_ckrs,
            followers_project_cols, scan_args.at( 0 ).predicate,
            (EmptyProbe) emptyProbeVal );
    }

    std::vector<uint32_t> tweets_project_cols = {
        twitter_tweets_cols::u_id,
        twitter_tweets_cols::tweet_id,
        twitter_tweets_cols::text,
        twitter_tweets_cols::createdate,
    };
    std::unordered_map<get_tweets_from_following_scan_kv_types> tweets_map_res;

    if( scan_args.count( 1 ) == 1 ) {
        tweets_map_res =
            db_operators_.scan_mr<get_tweets_from_following_scan_types>(
                k_twitter_tweets_table_id, scan_args.at( 1 ).read_ckrs,
                tweets_project_cols, scan_args.at( 1 ).predicate,
                followers_map_res );
    }

    uint32_t observed = 0;
    for( const auto& entry : tweets_map_res ) {
        observed += entry.second;
    }
    make_result_count( observed, k_twitter_tweets_table_id, scan_res );
    return observed;
}

twitter_benchmark_worker_types workload_operation_outcome_enum
    twitter_benchmark_worker_templ::perform_get_recent_tweets(
        uint32_t min_user, uint32_t max_user, uint32_t min_tweet,
        uint32_t max_tweet, uint64_t min_tweet_time, uint64_t max_tweet_time ) {

    DVLOG( 20 ) << "GetRecentTweets( min_user:" << min_user
                << ", max_user:" << max_user << ", min_tweet:" << min_tweet
                << ", max_tweet:" << max_tweet
                << ", min_tweet_time:" << min_tweet_time
                << ", max_tweet_time:" << max_tweet_time << " )";

    std::vector<scan_arguments> scan_args = generate_get_recent_tweet_scan_args(
        min_user, max_user, min_tweet, max_tweet, min_tweet_time,
        max_tweet_time );

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs, "GetRecentTweets" );
    }

    workload_operation_outcome_enum outcome = WORKLOAD_OP_SUCCESS;
    RETURN_AND_COMMIT_IF_SS_DB( outcome );

    scan_result scan_res;
    uint32_t    num_tweets =
        get_tweets_by_scan( map_scan_args( scan_args ), scan_res );

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "GetRecentTweets( min_user:" << min_user
                << ", max_user:" << max_user << ", min_tweet:" << min_tweet
                << ", max_tweet:" << max_tweet
                << ", min_tweet_time:" << min_tweet_time
                << ", max_tweet_time:" << max_tweet_time
                << " ), got tweet:" << num_tweets;

    return outcome;
}

twitter_benchmark_worker_types std::vector<scan_arguments>
                               twitter_benchmark_worker_templ::generate_get_recent_tweet_scan_args(
        uint32_t min_user, uint32_t max_user, uint32_t min_tweet,
        uint32_t max_tweet, uint64_t min_tweet_time,
        uint64_t max_tweet_time ) const {

    twitter_tweets tweet;

    scan_arguments scan_arg;
    scan_arg.label = 0;

    for( uint32_t u_id = min_user; u_id <= max_user; u_id++ ) {
        tweet.u_id = u_id;
        tweet.tweet_id = min_tweet;
        uint64_t low_key = make_tweets_key( tweet );

        tweet.tweet_id = max_tweet;
        uint64_t high_key = make_tweets_key( tweet );

        scan_arg.read_ckrs.emplace_back( create_cell_key_ranges(
            k_twitter_tweets_table_id, low_key, high_key,
            twitter_tweets_cols::text, twitter_tweets_cols::createdate ) );
    }

    cell_predicate c_pred;
    c_pred.table_id = k_twitter_tweets_table_id;
    c_pred.col_id = twitter_tweets_cols::createdate;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( min_tweet_time );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;

    scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = uint64_to_string( max_tweet_time );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    scan_arg.predicate.and_predicates.emplace_back( c_pred );

    std::vector<scan_arguments> scan_args = { scan_arg };
    return scan_args;
}

twitter_benchmark_worker_types workload_operation_outcome_enum
    twitter_benchmark_worker_templ::perform_get_tweets_like(
        uint32_t min_user, uint32_t max_user, uint32_t min_tweet,
        uint32_t max_tweet, const std::string& min_tweet_str,
        const std::string& max_tweet_str ) {

    DVLOG( 20 ) << "GetTweetsLike( min_user:" << min_user
                << ", max_user:" << max_user << ", min_tweet:" << min_tweet
                << ", max_tweet:" << max_tweet
                << ", min_tweet_str:" << min_tweet_str
                << ", max_tweet_str:" << max_tweet_str << " )";

    std::vector<scan_arguments> scan_args = generate_get_tweets_like_scan_args(
        min_user, max_user, min_tweet, max_tweet, min_tweet_str,
        max_tweet_str );

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs, "GetTweetsLike" );
    }

    workload_operation_outcome_enum outcome = WORKLOAD_OP_SUCCESS;
    RETURN_AND_COMMIT_IF_SS_DB( outcome );

    scan_result scan_res;
    uint32_t    num_tweets =
        get_tweets_by_scan( map_scan_args( scan_args ), scan_res );

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "GetTweetsLike( min_user:" << min_user
                << ", max_user:" << max_user << ", min_tweet:" << min_tweet
                << ", max_tweet:" << max_tweet
                << ", min_tweet_str:" << min_tweet_str
                << ", max_tweet_str:" << max_tweet_str
                << " ), got tweet:" << num_tweets;

    return outcome;
}

twitter_benchmark_worker_types std::vector<scan_arguments>
                               twitter_benchmark_worker_templ::generate_get_tweets_like_scan_args(
        uint32_t min_user, uint32_t max_user, uint32_t min_tweet,
        uint32_t max_tweet, const std::string& min_tweet_str,
        const std::string& max_tweet_str ) const {

    twitter_tweets tweet;

    scan_arguments scan_arg;
    scan_arg.label = 0;

    for( uint32_t u_id = min_user; u_id <= max_user; u_id++ ) {
        tweet.u_id = u_id;
        tweet.tweet_id = min_tweet;
        uint64_t low_key = make_tweets_key( tweet );

        tweet.tweet_id = max_tweet;
        uint64_t high_key = make_tweets_key( tweet );

        scan_arg.read_ckrs.emplace_back( create_cell_key_ranges(
            k_twitter_tweets_table_id, low_key, high_key,
            twitter_tweets_cols::text, twitter_tweets_cols::createdate ) );
    }

    cell_predicate c_pred;
    c_pred.table_id = k_twitter_tweets_table_id;
    c_pred.col_id = twitter_tweets_cols::text;
    c_pred.type = data_type::type::STRING;
    c_pred.data = min_tweet_str;
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;

    scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = max_tweet_str;
    c_pred.predicate = predicate_type::type::LESS_THAN;
    scan_arg.predicate.and_predicates.emplace_back( c_pred );

    std::vector<scan_arguments> scan_args = { scan_arg };
    return scan_args;
}

twitter_benchmark_worker_types workload_operation_outcome_enum
                               twitter_benchmark_worker_templ::do_get_tweets_from_following() {
    uint32_t user = generator_.get_user();
    return perform_get_tweets_from_following(
        user, 0, generator_.configs_.limit_followers_,
        std::max( (uint32_t) 0,
                  user - ( generator_.configs_.limit_followers_ * 2 ) ),
        std::min( ( uint32_t )( generator_.configs_.num_users_ - 1 ),
                  ( uint32_t )(
                      user + ( generator_.configs_.limit_followers_ * 2 ) ) ),
        0, generator_.configs_.limit_tweets_ );
}
twitter_benchmark_worker_types workload_operation_outcome_enum
    twitter_benchmark_worker_templ::do_get_followers() {
    uint32_t user = generator_.get_user();
    return perform_get_followers(
        user, 0, generator_.configs_.limit_followers_,
        std::max( (uint32_t) 0,
                  user - ( generator_.configs_.limit_followers_ * 2 ) ),
        std::min( ( uint32_t )( generator_.configs_.num_users_ - 1 ),
                  ( uint32_t )(
                      user + ( generator_.configs_.limit_followers_ * 2 ) ) ) );
}
twitter_benchmark_worker_types workload_operation_outcome_enum
                               twitter_benchmark_worker_templ::do_get_user_tweets() {
    return perform_get_user_tweets( generator_.get_user(), 0,
                                    generator_.configs_.limit_tweets_for_uid_ );
}
twitter_benchmark_worker_types workload_operation_outcome_enum
                               twitter_benchmark_worker_templ::do_insert_tweet() {
    return perform_insert_tweet( generator_.get_user() );
}

twitter_benchmark_worker_types workload_operation_outcome_enum
    twitter_benchmark_worker_templ::perform_get_tweet( uint32_t u_id,
                                                       uint32_t tweet_id ) {

    DVLOG( 20 ) << "GetTweet( user_id:" << u_id << ", tweet:" << tweet_id
                << ")";

    std::vector<scan_arguments> scan_args =
        generate_get_tweet_scan_args( u_id, tweet_id );

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs, "GetTweet" );
    }

    workload_operation_outcome_enum outcome = WORKLOAD_OP_SUCCESS;
    RETURN_AND_COMMIT_IF_SS_DB( outcome );

    scan_result scan_res;
    uint32_t    num_tweets =
        get_tweets_by_scan( map_scan_args( scan_args ), scan_res );

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "GetTweet( user_id:" << u_id << ", tweet:" << tweet_id
                << "), got tweet:" << num_tweets;

    return outcome;
}
twitter_benchmark_worker_types std::vector<scan_arguments>
                               twitter_benchmark_worker_templ::generate_get_tweet_scan_args(
        uint32_t u_id, uint32_t tweet_id ) const {
    twitter_tweets tweet;
    tweet.u_id = u_id;
    tweet.tweet_id = tweet_id;

    uint64_t key = make_tweets_key( tweet );

    cell_key_ranges ckr = create_cell_key_ranges(
        k_twitter_tweets_table_id, key, key, twitter_tweets_cols::text,
        twitter_tweets_cols::createdate );

    scan_arguments scan_arg;
    scan_arg.label = 0;
    scan_arg.read_ckrs = {ckr};

    std::vector<scan_arguments> scan_args = {scan_arg};
    return scan_args;
}

twitter_benchmark_worker_types workload_operation_outcome_enum
    twitter_benchmark_worker_templ::perform_get_tweets_from_following(
        uint32_t u_id, uint32_t following_start, uint32_t following_end,
        uint32_t follower_id_start, uint32_t follower_id_end,
        uint32_t tweet_start, uint32_t tweet_end ) {

    DVLOG( 20 ) << "GetTweetsFromFollowing( user_id:" << u_id
                << ", following_start:" << following_start
                << ", following_end:" << following_end
                << ", follower_id_start:" << follower_id_start
                << ", follower_id_end:" << follower_id_end
                << ", tweet_start:" << tweet_start
                << ", tweet_end:" << tweet_end << " )";

    std::vector<scan_arguments> scan_args =
        generate_get_tweets_from_following_scan_args(
            u_id, following_start, following_end, follower_id_start,
            follower_id_end, tweet_start, tweet_end );

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs, "GetTweetsFromFollowing" );
    }

    workload_operation_outcome_enum outcome = WORKLOAD_OP_SUCCESS;
    RETURN_AND_COMMIT_IF_SS_DB( outcome );

    scan_result scan_res;
    uint32_t    num_tweets = get_tweets_from_following_by_scan(
        map_scan_args( scan_args ), scan_res );

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "GetTweetsFromFollowing( user_id:" << u_id
                << ", following_start:" << following_start
                << ", following_end:" << following_end
                << ", follower_id_start:" << follower_id_start
                << ", follower_id_end:" << follower_id_end
                << ", tweet_start:" << tweet_start
                << ", tweet_end:" << tweet_end
                << " ), num_tweets:" << num_tweets;

    return WORKLOAD_OP_SUCCESS;
}

twitter_benchmark_worker_types std::vector<scan_arguments>
                               twitter_benchmark_worker_templ::
        generate_get_tweets_from_following_scan_args(
            uint32_t u_id, uint32_t following_start, uint32_t following_end,
            uint32_t follower_id_start, uint32_t follower_id_end,
            uint32_t tweet_start, uint32_t tweet_end ) const {

    twitter_follows follows;
    follows.u_id = u_id;
    follows.f_id = following_start;

    uint64_t low_key = make_follows_key( follows );
    follows.f_id = following_end;
    uint64_t high_key = make_follows_key( follows );

    cell_key_ranges ckr =
        create_cell_key_ranges( k_twitter_follows_table_id, low_key, high_key,
                                twitter_follows_cols::following_u_id,
                                twitter_follows_cols::following_u_id );

    scan_arguments follows_scan_arg;
    follows_scan_arg.label = 0;
    follows_scan_arg.read_ckrs = { ckr };

    cell_predicate c_pred;
    c_pred.table_id = k_twitter_follows_table_id;
    c_pred.col_id = twitter_follows_cols::following_u_id;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( follower_id_start );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;

    follows_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = uint64_to_string( follower_id_end );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    follows_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    twitter_tweets tweet;
    for( uint32_t u_id = follower_id_start; u_id <= follower_id_end; u_id++ ) {
        tweet.u_id = u_id;
        tweet.tweet_id = tweet_start;
        low_key = make_tweets_key( tweet );

        tweet.tweet_id = tweet_end;
        high_key = make_tweets_key( tweet );

        ckr = create_cell_key_ranges( k_twitter_tweets_table_id, low_key,
                                      high_key, twitter_tweets_cols::u_id,
                                      twitter_tweets_cols::createdate );
    }

    scan_arguments tweets_scan_arg;
    tweets_scan_arg.label = 1;
    tweets_scan_arg.read_ckrs = { ckr };

    c_pred.table_id = k_twitter_tweets_table_id;
    c_pred.col_id = twitter_tweets_cols::tweet_id;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( tweet_start );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;

    tweets_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = uint64_to_string( tweet_end );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    tweets_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.col_id = twitter_tweets_cols::u_id;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( follower_id_start );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;

    tweets_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = uint64_to_string( follower_id_end );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    tweets_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    std::vector<scan_arguments> scan_args = { follows_scan_arg,
                                              tweets_scan_arg };
    return scan_args;
}

twitter_benchmark_worker_types workload_operation_outcome_enum
    twitter_benchmark_worker_templ::perform_get_followers(
        uint32_t u_id, uint32_t f_id_start, uint32_t f_id_end,
        uint32_t follower_min, uint32_t follower_max ) {
    DVLOG( 20 ) << "GetFollowers( user_id:" << u_id
                << ", f_id_start:" << f_id_start << ", f_id_end:" << f_id_end
                << ", follower_min:" << follower_min
                << ", follower_max:" << follower_max << " )";

    std::vector<scan_arguments> scan_args = generate_get_followers_scan_args(
        u_id, f_id_start, f_id_end, follower_min, follower_max );

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs, "GetFollowers" );
    }

    workload_operation_outcome_enum outcome = WORKLOAD_OP_SUCCESS;
    RETURN_AND_COMMIT_IF_SS_DB( outcome );

    scan_result scan_res;
    uint32_t    num_followers =
        get_followers_by_scan( map_scan_args( scan_args ), scan_res );

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "GetFollowers( user_id:" << u_id
                << ", f_id_start:" << f_id_start << ", f_id_end:" << f_id_end
                << ", follower_min:" << follower_min
                << ", follower_max:" << follower_max
                << " ), num_followers:" << num_followers << ", okay!";

    return outcome;
}

twitter_benchmark_worker_types std::vector<scan_arguments>
                               twitter_benchmark_worker_templ::generate_get_followers_scan_args(
        uint32_t u_id, uint32_t f_id_start, uint32_t f_id_end,
        uint32_t follower_min, uint32_t follower_max ) const {

    cell_key_ranges ckr = create_cell_key_ranges(
        k_twitter_followers_table_id, combine_keys( u_id, f_id_start ),
        combine_keys( u_id, f_id_end ), twitter_follows_cols::following_u_id,
        twitter_follows_cols::following_u_id );

    scan_arguments followers_scan_arg;
    followers_scan_arg.label = 0;
    followers_scan_arg.read_ckrs = { ckr };

    cell_predicate c_pred;
    c_pred.table_id = k_twitter_followers_table_id;
    c_pred.col_id = twitter_follows_cols::following_u_id;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( follower_min );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;

    followers_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = uint64_to_string( follower_max );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;

    followers_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    ckr = create_cell_key_ranges(
        k_twitter_user_profiles_table_id, follower_min, follower_max,
        twitter_user_profile_cols::u_id, twitter_user_profile_cols::name );

    scan_arguments users_scan_arg;
    users_scan_arg.label = 1;
    users_scan_arg.read_ckrs = { ckr };

    std::vector<scan_arguments> scan_args = { followers_scan_arg,
                                              users_scan_arg };
    return scan_args;
}

twitter_benchmark_worker_types uint32_t
    twitter_benchmark_worker_templ::get_followers_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result&                                   scan_res ) {
    std::vector<uint32_t> followers_project_cols = {
        twitter_followers_cols::follower_u_id };
    std::unordered_map<get_followers_scan_kv_types> follower_map_res;

    if( scan_args.count( 0 ) == 1 ) {
        follower_map_res = db_operators_.scan_mr<get_followers_scan_types>(
            k_twitter_followers_table_id, scan_args.at( 0 ).read_ckrs,
            followers_project_cols, scan_args.at( 0 ).predicate,
            (EmptyProbe) emptyProbeVal );
    }

    std::vector<uint32_t> users_project_cols = {
        twitter_user_profile_cols::u_id, twitter_user_profile_cols::name };
    std::unordered_map<get_users_from_followers_scan_kv_types> users_map_res;

    if( scan_args.count( 1 ) == 1 ) {
        users_map_res =
            db_operators_.scan_mr<get_users_from_followers_scan_types>(
                k_twitter_user_profiles_table_id, scan_args.at( 1 ).read_ckrs,
                users_project_cols, scan_args.at( 1 ).predicate,
                follower_map_res );
    }

    uint32_t                  observed = users_map_res.size();
    make_result_count( observed, k_twitter_user_profiles_table_id, scan_res );
    return observed;
}

twitter_benchmark_worker_types void
    twitter_benchmark_worker_templ::make_result_count( uint32_t     observed,
                                                       uint32_t     table_id,
                                                       scan_result& scan_res ) {
    result_tuple rt;
    rt.table_id = table_id;
    rt.row_id = 0;

    result_cell cell;
    cell.col_id = 0;
    cell.present = true;
    cell.data = uint64_to_string( observed );
    cell.type = data_type::type::UINT64;
    rt.cells.emplace_back( cell );

    std::vector<result_tuple> results;
    results.emplace_back( rt );
    scan_res.res_tuples[0] = results;
}


twitter_benchmark_worker_types std::vector<scan_arguments>
                               twitter_benchmark_worker_templ::generate_get_user_tweets_scan_args(
        uint32_t u_id, uint32_t tweet_id_start, uint32_t tweet_id_end ) const {
    twitter_tweets tweet;
    tweet.u_id = u_id;
    tweet.tweet_id = tweet_id_start;
    uint64_t low_key = make_tweets_key( tweet );

    tweet.tweet_id = tweet_id_end;
    uint64_t high_key = make_tweets_key( tweet );

    cell_key_ranges ckr = create_cell_key_ranges(
        k_twitter_tweets_table_id, low_key, high_key, twitter_tweets_cols::text,
        twitter_tweets_cols::createdate );

    scan_arguments scan_arg;
    scan_arg.label = 0;
    scan_arg.read_ckrs = { ckr };

    std::vector<scan_arguments> scan_args = { scan_arg };
    return scan_args;
}

twitter_benchmark_worker_types uint32_t
    twitter_benchmark_worker_templ::perform_get_user_tweets(
        uint32_t u_id, uint32_t tweet_id_start, uint32_t tweet_id_end ) {
    DVLOG( 20 ) << "GetUserTweets( user_id:" << u_id
                << ", tweet_id_start:" << tweet_id_start
                << ", tweet_id_end:" << tweet_id_end << " )";

    DCHECK_LE( tweet_id_start, tweet_id_end );

    std::vector<scan_arguments> scan_args = generate_get_user_tweets_scan_args(
        u_id, tweet_id_start, tweet_id_end );

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs, "GetUserTweets" );
    }

    workload_operation_outcome_enum outcome = WORKLOAD_OP_SUCCESS;
    RETURN_AND_COMMIT_IF_SS_DB( outcome );

    scan_result scan_res;
    uint32_t    num_tweets =
        get_tweets_by_scan( map_scan_args( scan_args ), scan_res );

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "GetUserTweets( user_id:" << u_id
                << ", tweet_id_start:" << tweet_id_start
                << ", limit:" << tweet_id_end
                << " ), num_tweets:" << num_tweets;
    return outcome;
}


twitter_benchmark_worker_types uint32_t
                               twitter_benchmark_worker_templ::get_tweets_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res ) {

    std::vector<uint32_t> project_cols = {twitter_tweets_cols::text,
                                          twitter_tweets_cols::createdate};
    std::unordered_map<tweet_scan_kv_types> map_res;
    uint32_t observed = 0;

    if( scan_args.count( 0 ) == 1 ) {
        map_res = db_operators_.scan_mr<tweet_scan_types>(
            k_twitter_tweets_table_id, scan_args.at( 0 ).read_ckrs,
            project_cols, scan_args.at( 0 ).predicate,
            (EmptyProbe) emptyProbeVal );

        observed = map_res[0];
    }

    make_result_count( observed, k_twitter_tweets_table_id, scan_res );
    return observed;
}

twitter_benchmark_worker_types uint32_t
    twitter_benchmark_worker_templ::get_tweets_from_following_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result&                                   scan_res ) {
    std::vector<uint32_t> following_project_cols = {
        twitter_follows_cols::following_u_id };
    std::unordered_map<get_following_scan_kv_types> following_map_res;

    if( scan_args.count( 0 ) == 1 ) {
        following_map_res = db_operators_.scan_mr<get_following_scan_types>(
            k_twitter_follows_table_id, scan_args.at( 0 ).read_ckrs,
            following_project_cols, scan_args.at( 0 ).predicate,
            (EmptyProbe) emptyProbeVal );
    }

    std::vector<uint32_t> tweets_project_cols = {
        twitter_tweets_cols::u_id,
        twitter_tweets_cols::tweet_id,
        twitter_tweets_cols::text,
        twitter_tweets_cols::createdate,
    };
    std::unordered_map<get_tweets_from_following_scan_kv_types> tweets_map_res;

    if( scan_args.count( 1 ) == 1 ) {
        tweets_map_res =
            db_operators_.scan_mr<get_tweets_from_following_scan_types>(
                k_twitter_tweets_table_id, scan_args.at( 1 ).read_ckrs,
                tweets_project_cols, scan_args.at( 1 ).predicate,
                following_map_res );
    }

    uint32_t                  observed = 0;
    for( const auto& entry : tweets_map_res ) {
        observed += entry.second;
    }
    make_result_count( observed, k_twitter_tweets_table_id, scan_res );
    return observed;
}

twitter_benchmark_worker_types workload_operation_outcome_enum
    twitter_benchmark_worker_templ::perform_insert_tweet( uint32_t u_id ) {
    DVLOG( 20 ) << "InsertTweet( user_id:" << u_id << ")";

    workload_operation_outcome_enum outcome = WORKLOAD_OP_SUCCESS;

    cell_identifier cid = create_cell_identifier(
        k_twitter_user_profiles_table_id,
        twitter_user_profile_cols::next_tweet_id, u_id );
    auto ckr = cell_key_ranges_from_cell_identifier( cid );

    std::vector<int32_t> tweet_ids;
    fetch_and_set_next_tweet_id( ckr, tweet_ids );
    DCHECK_EQ( 1, tweet_ids.size() );
    int32_t tweet_id = tweet_ids.at( 0 );
    if( tweet_id < 0 ) {
        DVLOG( 20 ) << "InsertTweet( user_id:" << u_id << "), failed";
        return WORKLOAD_OP_FAILURE;
    }
    uint64_t tweet_key = combine_keys( u_id, tweet_id );
    auto     tweet_ckr =
        create_cell_key_ranges( k_twitter_tweets_table_id, tweet_key, tweet_key,
                                0, k_twitter_tweets_num_columns - 1 );
    uint32_t num_tweets = insert_tweet( tweet_ckr );
    if( num_tweets < 1 ) {
        outcome = WORKLOAD_OP_FAILURE;
    }

    DVLOG( 20 ) << "InsertTweet( user_id:" << u_id
                << "), num_tweets:" << num_tweets;

    return outcome;
}

twitter_benchmark_worker_types uint32_t
                               twitter_benchmark_worker_templ::fetch_and_set_next_tweet_id(
        const cell_key_ranges& ckr, std::vector<int32_t>& tweet_ids ) {
    DVLOG( 20 ) << "FetchAndSetNextTweetId( ckr:" << ckr << ")";

    uint32_t num_tweets = ( ckr.row_id_end - ckr.row_id_start ) + 1;

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> ckrs = {ckr};
        db_operators_.begin_transaction( ckrs, ckrs, "InsertTweet" );
    }

    DCHECK_GE( twitter_user_profile_cols::next_tweet_id, ckr.col_id_start );
    DCHECK_LE( twitter_user_profile_cols::next_tweet_id, ckr.col_id_end );

    for( int64_t u_id = ckr.row_id_start; u_id <= ckr.row_id_end; u_id++ ) {
        twitter_user_profile user;
        int32_t next_tweet_id = 0;
        if( db_operators_.abstraction_configs_.db_type_ !=
            db_abstraction_type::SS_DB ) {
            next_tweet_id = -1;
            bool read_ok = lookup_twitter_user_profile(
                &db_operators_, &user, u_id, ckr, true /* latest */,
                true /* allow nullable */ );
            if( read_ok ) {
                next_tweet_id = user.next_tweet_id;
                user.next_tweet_id = ( user.next_tweet_id + 1 ) %
                                     generator_.configs_.max_tweets_per_user_;
                update_twitter_user_profile( &db_operators_, &user, u_id, ckr,
                                             true /* prop*/ );
            }
        }
        tweet_ids.emplace_back( next_tweet_id );
    }
    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "FetchAndSetNextTweetId (ckr:" << ckr
                << " ), tweet_id:" << tweet_ids;
    return num_tweets;
}
twitter_benchmark_worker_types uint32_t
    twitter_benchmark_worker_templ::insert_tweet( const cell_key_ranges& ckr ) {
    DVLOG( 20 ) << "InsertTweet( ckr:" << ckr << ")";

    uint32_t num_tweets = 1 + ( ckr.row_id_end - ckr.row_id_start );

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> ckrs = {ckr};
        std::vector<cell_key_ranges> empty_ckrs;
        db_operators_.begin_transaction( ckrs, empty_ckrs, "InsertTweet");
    }
    RETURN_AND_COMMIT_IF_SS_DB( num_tweets );

    twitter_tweets tweet;
    tweet.createdate =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch() )
            .count();

    for( int64_t row_id = ckr.row_id_start; row_id <= ckr.row_id_end;
         row_id++ ) {
        tweet.u_id = get_user_from_key( ckr.table_id, row_id );
        tweet.tweet_id = get_tweet_from_key( ckr.table_id, row_id );

        tweet.text = generator_.dist_.write_uniform_str(
            twitter_tweets::MIN_TWEET_LEN, twitter_tweets::MAX_TWEET_LEN );

        update_twitter_tweets( &db_operators_, &tweet, row_id, ckr,
                               false /* prop*/ );
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "InsertTweet( ckr:" << ckr << "), num_tweets:" << num_tweets;

    return num_tweets;
}

twitter_benchmark_worker_types void
    twitter_benchmark_worker_templ::set_transaction_partition_holder(
        transaction_partition_holder* holder ) {
    db_operators_.set_transaction_partition_holder( holder );
}

