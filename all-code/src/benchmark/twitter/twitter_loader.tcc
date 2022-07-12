#pragma once

#include <chrono>

#include <glog/logging.h>

#include "../../common/string_utils.h"
#include "../benchmark_interface.h"
#include "record-types/twitter_primary_key_generation.h"
#include "record-types/twitter_record_types.h"
#include "twitter_db_operators.h"
#include "twitter_table_ids.h"

twitter_loader_types twitter_loader_templ::twitter_loader(
    db_abstraction* db, const twitter_configs& configs,
    const db_abstraction_configs& abstraction_configs, uint32_t client_id )
    : db_operators_( client_id, db, abstraction_configs,
                     configs.bench_configs_.limit_update_propagation_,
                     true /* store global state */ ),
      configs_( configs ),
      generator_( nullptr /*no need for zipf*/, client_id, configs ),
      dist_( nullptr /* no need for zipf*/ ) {}

twitter_loader_types twitter_loader_templ::~twitter_loader() {}

twitter_loader_types void twitter_loader_templ::insert_user(
    uint32_t u_id, uint32_t num_followers, uint32_t num_following,
    uint32_t num_tweets, const cell_key_ranges& ckr ) {

    DVLOG( 20 ) << "Load user:" << u_id << ", ckr:" << ckr;

    twitter_user_profile user;
    user.u_id = u_id;

    user.name = generator_.dist_.write_uniform_str(
        twitter_user_profile::MIN_NAME_LEN,
        twitter_user_profile::MAX_NAME_LEN );

    user.num_followers = num_followers;
    user.num_following = num_following;
    user.num_tweets = num_tweets;
    user.next_tweet_id =
        user.num_tweets % generator_.configs_.max_tweets_per_user_;

    insert_twitter_user_profile( &db_operators_, &user,
                                 make_user_profile_key( user ), ckr );
    DVLOG( 20 ) << "Load user:" << u_id << ", ckr:" << ckr << ", okay!";
}

twitter_loader_types void twitter_loader_templ::insert_tweet(
    uint32_t u_id, uint32_t tweet_id, const cell_key_ranges& ckr ) {
    DVLOG( 20 ) << "Load tweet:" << u_id << ", " << tweet_id << ", ckr:" << ckr;

    twitter_tweets tweet;
    tweet.u_id = u_id;
    tweet.tweet_id = tweet_id;

    tweet.text = generator_.dist_.write_uniform_str(
        twitter_tweets::MIN_TWEET_LEN, twitter_tweets::MAX_TWEET_LEN );

    tweet.createdate =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch() )
            .count();

    insert_twitter_tweets( &db_operators_, &tweet, make_tweets_key( tweet ),
                           ckr );
    DVLOG( 20 ) << "Load tweet:" << u_id << ", " << tweet_id << ", ckr:" << ckr
                << ", okay!";
}
twitter_loader_types void twitter_loader_templ::insert_follower(
    uint32_t u_id, uint32_t follower_u_id, uint32_t follow_id,
    const cell_key_ranges& ckr ) {
    DVLOG( 20 ) << "Load follower:" << u_id
                << ", follower_u_id:" << follower_u_id
                << ", follow_id:" << follow_id << ", ckr:" << ckr;

    twitter_followers follower;
    follower.u_id = u_id;
    follower.f_id = follow_id;
    follower.follower_u_id = follower_u_id;

    insert_twitter_followers( &db_operators_, &follower,
                              make_followers_key( follower ), ckr );

    DVLOG( 20 ) << "Load follower:" << u_id
                << ", follower_u_id:" << follower_u_id
                << ", follow_id:" << follow_id << ", ckr:" << ckr << ", okay!";
}
twitter_loader_types void twitter_loader_templ::insert_follows(
    uint32_t u_id, uint32_t following_u_id, uint32_t following_id,
    const cell_key_ranges& ckr ) {
    DVLOG( 20 ) << "Load follows:" << u_id
                << ", following_u_id:" << following_u_id
                << ", following_id:" << following_id << ", ckr:" << ckr;

    twitter_follows follows;
    follows.u_id = u_id;
    follows.f_id = following_id;
    follows.following_u_id = following_u_id;

    insert_twitter_follows( &db_operators_, &follows,
                            make_follows_key( follows ), ckr );

    DVLOG( 20 ) << "Load follows:" << u_id
                << ", following_u_id:" << following_u_id
                << ", following_id:" << following_id << ", ckr:" << ckr
                << ", okay!";
}

twitter_loader_types void twitter_loader_templ::generate_range_load_users_ckrs(
    std::vector<cell_key_ranges>& ckrs, uint32_t u_id_start,
    uint32_t u_id_end ) {

    cell_key_ranges ckr = create_cell_key_ranges(
        k_twitter_user_profiles_table_id, u_id_start, u_id_end, 0,
        k_twitter_user_profile_num_columns - 1 );

    ckrs.emplace_back( ckr );
}
twitter_loader_types void twitter_loader_templ::generate_range_load_users_ckrs(
    std::vector<cell_key_ranges>& ckrs, const uint32_t* u_ids,
    uint32_t num_users ) {

    cell_key_ranges ckr =
        create_cell_key_ranges( k_twitter_user_profiles_table_id, 0, 0, 0,
                                k_twitter_user_profile_num_columns - 1 );
    for ( uint32_t pos = 0; pos < num_users; pos++ ) {
        ckr.row_id_start = u_ids[pos];
        ckr.row_id_end = u_ids[pos];
        ckrs.emplace_back( ckr );
    }
}

twitter_loader_types void twitter_loader_templ::do_range_load_users(
    const uint32_t* u_ids, const uint32_t* num_followers,
    const uint32_t* num_following, const uint32_t* num_tweets,
    uint32_t num_users ) {
    std::vector<cell_key_ranges> ckrs;
    generate_range_load_users_ckrs( ckrs, u_ids, num_users );
    for( uint32_t pos = 0; pos < num_users; pos++ ) {
        return do_range_load_users( num_followers[pos], num_following[pos],
                                    num_tweets[pos], 1, ckrs[pos] );
    }
}
twitter_loader_types void twitter_loader_templ::do_range_load_users(
    const uint32_t* num_followers, const uint32_t* num_following,
    const uint32_t* num_tweets, uint32_t num_users,
    const cell_key_ranges& ckr ) {
    DVLOG( 20 ) << "Loading users:" << num_users << ", ckr:" << ckr;

    if( num_users == 0 ) {
        return;
    }

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> ckrs = {ckr};
        std::vector<cell_key_ranges> empty_ckrs;
        db_operators_.begin_transaction( ckrs, empty_ckrs );
    }

    RETURN_VOID_AND_COMMIT_IF_SS_DB();

    DCHECK_LE( ( ckr.row_id_end - ckr.row_id_start ) + 1, num_users );
    for( int64_t pos = ckr.row_id_start; pos <= ckr.row_id_end; pos++ ) {
        insert_user( pos, num_followers[pos - ckr.row_id_start],
                     num_following[pos - ckr.row_id_start],
                     num_tweets[pos - ckr.row_id_start], ckr );
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "Loading users:" << num_users << ", ckr:" << ckr << " okay!";
}

twitter_loader_types void twitter_loader_templ::generate_backfill_user_ckrs(
    std::vector<cell_key_ranges>& ckrs, uint32_t u_id, uint32_t max_num_tweets,
    uint32_t max_num_follows ) {
    uint32_t num_ckrs =
        ( max_num_tweets + 1 ) + ( ( max_num_follows + 1 ) * 2 );
    ckrs.reserve( num_ckrs );

    cell_key_ranges ckr = create_cell_key_ranges(
        k_twitter_tweets_table_id, 0, 0, 0, k_twitter_tweets_num_columns - 1 );

    for( uint32_t tweet_id = 0; tweet_id <= max_num_tweets; tweet_id++ ) {
        ckr.row_id_start = combine_keys( u_id, tweet_id );
        ckr.row_id_end = ckr.row_id_start;
        ckrs.emplace_back( ckr );
    }
    cell_key_ranges follower_ckr =
        create_cell_key_ranges( k_twitter_followers_table_id, 0, 0, 0,
                                k_twitter_followers_num_columns - 1 );
    cell_key_ranges following_ckr =
        create_cell_key_ranges( k_twitter_follows_table_id, 0, 0, 0,
                                k_twitter_follows_num_columns - 1 );

    for( uint32_t f_id = 0; f_id <= max_num_follows; f_id++ ) {
        following_ckr.row_id_start = combine_keys( u_id, f_id );
        following_ckr.row_id_end = following_ckr.row_id_start;

        follower_ckr.row_id_start = combine_keys( u_id, f_id );
        follower_ckr.row_id_end = follower_ckr.row_id_start;

        ckrs.emplace_back( following_ckr );
        ckrs.emplace_back( follower_ckr );
    }
}

twitter_loader_types void twitter_loader_templ::backfill_user(
    uint32_t u_id, uint32_t max_num_tweets, uint32_t max_num_follows ) {
    DVLOG( 20 ) << "Backfill user:" << u_id
                << ", max_num_tweets:" << max_num_tweets
                << ", max_num_follows:" << max_num_follows;
    std::vector<cell_key_ranges> ckrs;
    generate_backfill_user_ckrs( ckrs, u_id, max_num_tweets, max_num_follows );

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> empty_ckrs;
        db_operators_.begin_transaction( ckrs, empty_ckrs );
    }

    RETURN_VOID_AND_COMMIT_IF_SS_DB();

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "Backfill user:" << u_id
                << ", max_num_tweets:" << max_num_tweets
                << ", max_num_follows:" << max_num_follows << ", okay!";
}

twitter_loader_types void twitter_loader_templ::generate_load_user_follows_ckrs(
    std::vector<cell_key_ranges>& ckrs, uint32_t u_id, uint32_t num_follows ) {

    cell_key_ranges ckr = create_cell_key_ranges(
        k_twitter_follows_table_id, combine_keys( u_id, 0 ),
        combine_keys( u_id, num_follows - 1 ), 0,
        k_twitter_follows_num_columns - 1 );

    ckrs.emplace_back( ckr );
}

twitter_loader_types void twitter_loader_templ::generate_load_user_follows_ckrs(
    std::vector<cell_key_ranges>& ckrs, uint32_t u_id,
    const uint32_t* follows_u_ids, const uint32_t* f_ids,
    uint32_t num_follows ) {
    cell_key_ranges ckr =
        create_cell_key_ranges( k_twitter_follows_table_id, 0, 0, 0,
                                k_twitter_follows_num_columns - 1 );

    for( uint32_t pos = 0; pos < num_follows; pos++ ) {
        ckr.row_id_start = combine_keys( u_id, f_ids[pos] );
        ckr.row_id_end = ckr.row_id_start;
        ckrs.emplace_back( ckr );
    }
}

twitter_loader_types void twitter_loader_templ::do_load_user_follows(
    uint32_t u_id, const uint32_t* follows_u_ids, const uint32_t* f_ids,
    uint32_t num_follows ) {

    std::vector<cell_key_ranges> ckrs;
    generate_load_user_follows_ckrs( ckrs, u_id, follows_u_ids, f_ids,
                                     num_follows );
    for( uint32_t pos = 0; pos < num_follows; pos++ ) {
        do_load_user_follows( u_id, follows_u_ids[pos], 1, ckrs[pos] );
    }
}
twitter_loader_types void twitter_loader_templ::do_load_user_follows(
    uint32_t u_id, const uint32_t* follows_u_ids, uint32_t num_follows,
    const cell_key_ranges& ckr ) {
    DVLOG( 20 ) << "Loading user follows:" << u_id
                << ", follows_u_ids:" << follows_u_ids << ", "
                << ", num_follows:" << num_follows << ", ckr:" << ckr;

    if( num_follows == 0 ) {
        return;
    }

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> ckrs = {ckr};
        std::vector<cell_key_ranges> empty_ckrs;
        db_operators_.begin_transaction( ckrs, empty_ckrs );
    }

    RETURN_VOID_AND_COMMIT_IF_SS_DB();

    uint32_t f_id_start =
        get_follows_id_from_key( ckr.table_id, ckr.row_id_start );
    uint32_t f_id_end = get_follows_id_from_key( ckr.table_id, ckr.row_id_end );
    DCHECK_LE( ( f_id_end - f_id_start ) + 1, num_follows );
    for( uint32_t f_id = f_id_start; f_id <= f_id_end; f_id++ ) {
        insert_follows( u_id, follows_u_ids[f_id - f_id_start], f_id, ckr );
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "Loading user follows:" << u_id
                << ", follows_u_ids:" << follows_u_ids << ", "
                << ", num_follows:" << num_follows << ", ckr:" << ckr
                << ", okay!";
}

twitter_loader_types void
    twitter_loader_templ::generate_load_user_followers_ckrs(
        std::vector<cell_key_ranges>& ckrs, uint32_t u_id,
        uint32_t num_followers ) {

    cell_key_ranges ckr = create_cell_key_ranges(
        k_twitter_followers_table_id, combine_keys( u_id, 0 ),
        combine_keys( u_id, num_followers - 1 ), 0,
        k_twitter_followers_num_columns - 1 );

    ckrs.emplace_back( ckr );
}

twitter_loader_types void
    twitter_loader_templ::generate_load_user_followers_ckrs(
        std::vector<cell_key_ranges>& ckrs, uint32_t u_id,
        const uint32_t* followers_u_ids, const uint32_t* f_ids,
        uint32_t num_followers ) {

    cell_key_ranges ckr =
        create_cell_key_ranges( k_twitter_followers_table_id, 0, 0, 0,
                                k_twitter_followers_num_columns - 1 );
    for ( uint32_t pos = 0; pos < num_followers; pos++) {
        ckr.row_id_start = combine_keys( u_id, f_ids[pos] );
        ckr.row_id_end = ckr.row_id_start;
        ckrs.emplace_back( ckr );
    }
}

twitter_loader_types void twitter_loader_templ::do_load_user_followers(
    uint32_t u_id, const uint32_t* followers_u_ids, const uint32_t* f_ids,
    uint32_t num_followers ) {
    std::vector<cell_key_ranges> ckrs;
    generate_load_user_followers_ckrs( ckrs, u_id, followers_u_ids, f_ids,
                                       num_followers );
    for( uint32_t pos = 0; pos < num_followers; pos++ ) {
        do_load_user_followers( u_id, followers_u_ids[pos], 1, ckrs[pos] );
    }
}

twitter_loader_types void twitter_loader_templ::do_load_user_followers(
    uint32_t u_id, const uint32_t* followers_u_ids, uint32_t num_followers,
    const cell_key_ranges& ckr ) {

    DVLOG( 20 ) << "Loading user followers:" << u_id
                << ", followers_u_ids:" << followers_u_ids
                << ", num_followers:" << num_followers << ", ckr:" << ckr;

    if( num_followers == 0 ) {
        return;
    }

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> ckrs = {ckr};
        std::vector<cell_key_ranges> empty_ckrs;
        db_operators_.begin_transaction( ckrs, empty_ckrs );
    }

    RETURN_VOID_AND_COMMIT_IF_SS_DB();

    uint32_t f_id_start =
        get_follows_id_from_key( ckr.table_id, ckr.row_id_start );
    uint32_t f_id_end = get_follows_id_from_key( ckr.table_id, ckr.row_id_end );

    DCHECK_LE( ( f_id_end - f_id_start ) + 1, num_followers );

    for( uint32_t f_id = f_id_start; f_id <= f_id_end; f_id++ ) {
        insert_follower( u_id, followers_u_ids[f_id - f_id_start ], f_id, ckr );
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "Loading user followers:" << u_id
                << ", followers_u_ids:" << followers_u_ids << ", "
                << ", num_followers:" << num_followers << ", ckr:" << ckr
                << ", okay!";
}

twitter_loader_types void twitter_loader_templ::generate_load_user_tweets_ckrs(
    std::vector<cell_key_ranges>& ckrs, uint32_t u_id, uint32_t num_tweets ) {
    cell_key_ranges ckr = create_cell_key_ranges(
        k_twitter_tweets_table_id, combine_keys( u_id, 0 ),
        combine_keys( u_id, num_tweets - 1 ), 0,
        k_twitter_tweets_num_columns - 1 );

    ckrs.emplace_back( ckr );
}

twitter_loader_types void twitter_loader_templ::generate_load_user_tweets_ckrs(
    std::vector<cell_key_ranges>& ckrs, uint32_t u_id,
    const uint32_t* tweet_ids, uint32_t num_tweets ) {
    cell_key_ranges ckr = create_cell_key_ranges(
        k_twitter_tweets_table_id, 0, 0, 0, k_twitter_tweets_num_columns - 1 );

    for( uint32_t pos = 0; pos < num_tweets; pos++ ) {
        ckr.row_id_start = combine_keys( u_id, tweet_ids[pos] );
        ckr.row_id_end = ckr.row_id_start;
        ckrs.emplace_back( ckr );
    }
}

twitter_loader_types void twitter_loader_templ::do_load_user_tweets(
    uint32_t u_id, const uint32_t* tweet_ids, uint32_t num_tweets ) {
    std::vector<cell_key_ranges> ckrs;
    generate_load_user_tweets_ckrs( ckrs, u_id, tweet_ids, num_tweets );

    for( const auto& ckr : ckrs ) {
        return do_load_user_tweets( u_id, ckr );
    }
}

twitter_loader_types void twitter_loader_templ::do_load_user_tweets(
    uint32_t u_id, const cell_key_ranges& ckr ) {
    DVLOG( 20 ) << "Loading user tweets:" << u_id << ", ckr:" << ckr;

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> ckrs = {ckr};
        std::vector<cell_key_ranges> empty_ckrs;
        db_operators_.begin_transaction( ckrs, empty_ckrs );
    }

    RETURN_VOID_AND_COMMIT_IF_SS_DB();

    uint32_t tweet_id_start =
        get_tweet_from_key( ckr.table_id, ckr.row_id_start );
    uint32_t tweet_id_end = get_tweet_from_key( ckr.table_id, ckr.row_id_end );
    for( uint32_t tweet_id = tweet_id_start; tweet_id <= tweet_id_end;
         tweet_id++ ) {
        insert_tweet( u_id, tweet_id, ckr );
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "Loading user tweets:" << u_id << ", ckr:" << ckr
                << ", okay!";
}

twitter_loader_types void
    twitter_loader_templ::set_transaction_partition_holder(
        transaction_partition_holder* holder ) {
    db_operators_.set_transaction_partition_holder( holder );
}

twitter_loader_types void twitter_loader_templ::load_range_of_clients(
    uint32_t cur_start, uint32_t cur_end,
    const std::vector<std::vector<uint32_t>>& follow_data,
    const std::vector<std::vector<uint32_t>>& follower_data,
    const std::vector<uint32_t>&              tweet_data ) {
    std::vector<uint32_t> uids;
    std::vector<uint32_t> num_followers;
    std::vector<uint32_t> num_following;
    std::vector<uint32_t> num_tweets;

    cell_key_ranges tweet_ckr = create_cell_key_ranges(
        k_twitter_tweets_table_id, 0, 0, 0, k_twitter_tweets_num_columns - 1 );
    cell_key_ranges following_ckr =
        create_cell_key_ranges( k_twitter_follows_table_id, 0, 0, 0,
                                k_twitter_follows_num_columns - 1 );
    cell_key_ranges follower_ckr =
        create_cell_key_ranges( k_twitter_followers_table_id, 0, 0, 0,
                                k_twitter_followers_num_columns - 1 );
    cell_key_ranges user_ckr = create_cell_key_ranges(
        k_twitter_user_profiles_table_id, cur_start, cur_end, 0,
        k_twitter_user_profile_num_columns - 1 );

    for( uint32_t id = cur_start; id <= cur_end; id++ ) {
        uids.push_back( id );

        uint32_t tweet_cnt = tweet_data.at( id );
        num_tweets.push_back( tweet_cnt );

        uint32_t follower_cnt = follower_data.at( id ).size();
        num_followers.push_back( follower_cnt );

        uint32_t following_cnt = follow_data.at( id ).size();
        num_following.push_back( following_cnt );

        if( tweet_cnt > 0 ) {
            tweet_ckr.row_id_start = combine_keys( id, 0 );
            tweet_ckr.row_id_end = combine_keys( id, tweet_cnt - 1 );

            do_load_user_tweets( id, tweet_ckr );
        }

        if( following_cnt > 0 ) {
            following_ckr.row_id_start = combine_keys( id, 0 );
            following_ckr.row_id_end = combine_keys( id, following_cnt - 1 );

            do_load_user_follows( id, follow_data.at( id ).data(),
                                  following_cnt, following_ckr );
        }
        if( follower_cnt > 0 ) {
            follower_ckr.row_id_start = combine_keys( id, 0 );
            follower_ckr.row_id_end = combine_keys( id, follower_cnt - 1 );

            do_load_user_followers( id, follower_data.at( id ).data(),
                                    follower_cnt, follower_ckr );
        }
    }

    do_range_load_users( num_followers.data(), num_following.data(),
                         num_tweets.data(), uids.size(), user_ckr );
    for( uint32_t id = cur_start; id <= cur_end; id++ ) {
        backfill_user( id, configs_.insert_num_tweets_,
                       configs_.insert_num_follows_ );
    }
}

twitter_loader_types std::vector<cell_key_ranges>
                     twitter_loader_templ::generate_insert_user_write_set_as_ckrs(
        uint32_t cur_start, uint32_t cur_end,
        const std::vector<std::vector<uint32_t>>& follow_data,
        const std::vector<std::vector<uint32_t>>& follower_data,
        const std::vector<uint32_t>& tweet_data, const twitter_configs& cfg ) {
    std::vector<cell_key_ranges> ckrs;

    for( uint32_t id = cur_start; id <= cur_end; id++ ) {
        uint32_t tweet_cnt = tweet_data.at( id );
        uint32_t follower_cnt = follower_data.at( id ).size();
        uint32_t following_cnt = follow_data.at( id ).size();

        if( tweet_cnt > 0 ) {
            generate_load_user_tweets_ckrs( ckrs, id, tweet_cnt );
        }

        if( following_cnt > 0 ) {
            generate_load_user_follows_ckrs( ckrs, id, following_cnt );
        }
        if( follower_cnt > 0 ) {
            generate_load_user_followers_ckrs( ckrs, id, follower_cnt );
        }
    }

    generate_range_load_users_ckrs( ckrs, cur_start, cur_end );
    for( uint32_t id = cur_start; id <= cur_end; id++ ) {
        generate_backfill_user_ckrs( ckrs, id, cfg.insert_num_tweets_,
                                     cfg.insert_num_follows_ );
    }

    return ckrs;
}

