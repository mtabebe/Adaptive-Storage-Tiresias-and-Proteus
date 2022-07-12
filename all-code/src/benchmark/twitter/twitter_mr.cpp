#include "twitter_mr.h"

#include "../../common/string_conversion.h"
#include "twitter_db_operators.h"

std::tuple<bool, uint64_t, uint32_t> tweet_map( const result_tuple& res ) {
    twitter_tweets tweet;

    auto read_cols = read_from_scan_twitter_tweets( res, &tweet );

    bool found = ( read_cols.count( twitter_tweets_cols::text ) == 1 ) and
                 ( read_cols.count( twitter_tweets_cols::createdate ) == 1 );
    uint64_t key = res.row_id;

    return std::make_tuple<>( found, key, 1 );
}

void tweet_scan_mapper( const std::vector<result_tuple>&         res_tuples,
                        std::unordered_map<tweet_scan_kv_types>& res,
                        const EmptyProbe&                        p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint64_t /* tweet id */,
                   uint32_t /* count */>
            mapped_res = tweet_map( res_tuple );
        DVLOG( 40 ) << "Tweet map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            }
        }
    }
}

void tweet_scan_reducer(
    const std::unordered_map<tweet_scan_kv_vec_types>& input,
    std::unordered_map<tweet_scan_kv_types>& res, const EmptyProbe& p ) {
    res[0] = input.size();
    DVLOG( 40 ) << "Tweet reduce:" << input.size();
}

std::tuple<bool, uint32_t, uint32_t> get_following_map(
    const result_tuple& res ) {
    twitter_follows follows;
    follows.following_u_id = 0;

    auto read_cols = read_from_scan_twitter_follows( res, &follows );

    bool found =
        ( read_cols.count( twitter_follows_cols::following_u_id ) == 1 );

    return std::make_tuple<>( found, follows.following_u_id,
                              follows.following_u_id );
}

void get_following_mapper( const std::vector<result_tuple>& res_tuples,
                           std::unordered_map<get_following_scan_kv_types>& res,
                           const EmptyProbe& p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint32_t /* tweet id */,
                   uint32_t /* tweet id */>
            mapped_res = get_following_map( res_tuple );
        DVLOG( 40 ) << "GetFollowing map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            }
        }
    }
}

void get_following_reducer(
    const std::unordered_map<get_following_scan_kv_vec_types>& input,
    std::unordered_map<get_following_scan_kv_types>&           res,
    const EmptyProbe&                                          p ) {
    for( const auto& entry : input ) {
        DVLOG( 40 ) << "Following reduce:" << entry.first;
        res[entry.first] = entry.first;
    }
}

std::tuple<bool /* found */, uint32_t /* user id */, uint64_t /* tweet id */>
    get_tweet_from_following_map(
        const result_tuple&                                    res,
        const std::unordered_map<get_following_scan_kv_types>& p ) {
    twitter_tweets tweet;

    auto read_cols = read_from_scan_twitter_tweets( res, &tweet );

    bool found = ( read_cols.count( twitter_tweets_cols::u_id ) == 1 ) and
                 ( read_cols.count( twitter_tweets_cols::text ) == 1 ) and
                 ( read_cols.count( twitter_tweets_cols::createdate ) == 1 );

    if( found ) {
        found = ( p.count( tweet.u_id ) == 1 );
    }

    return std::make_tuple<>( found, tweet.u_id, 1 );
}

void get_tweets_from_following_mapper(
    const std::vector<result_tuple>&                             res_tuples,
    std::unordered_map<get_tweets_from_following_scan_kv_types>& res,
    const std::unordered_map<get_following_scan_kv_types>&       p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint32_t /* user id */,
                   uint64_t /* tweet id */>
            mapped_res = get_tweet_from_following_map( res_tuple, p );
        DVLOG( 40 ) << "GetTweetFromFollowing map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            } else {
                found->second += std::get<2>( mapped_res );
            }
        }
    }
}

void get_tweets_from_following_reducer(
    const std::unordered_map<get_tweets_from_following_scan_kv_vec_types>&
                                                                 input,
    std::unordered_map<get_tweets_from_following_scan_kv_types>& res,
    const std::unordered_map<get_following_scan_kv_types>&       p ) {
    for( const auto& entry : input ) {
        DVLOG( 40 ) << "GetTweets from Following reduce:" << entry;
        uint64_t count = 0;
        for( const auto& val : entry.second ) {
            count += val;
        }
        res[entry.first] = count;
    }
}

std::tuple<bool, uint32_t, uint32_t> get_followers_map(
    const result_tuple& res ) {
    twitter_followers followers;
    followers.follower_u_id = 0;

    auto read_cols = read_from_scan_twitter_followers( res, &followers );

    bool found =
        ( read_cols.count( twitter_followers_cols::follower_u_id ) == 1 );

    return std::make_tuple<>( found, followers.follower_u_id,
                              followers.follower_u_id );
}

void get_followers_mapper( const std::vector<result_tuple>& res_tuples,
                           std::unordered_map<get_followers_scan_kv_types>& res,
                           const EmptyProbe&                                p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint32_t /* follower u id */,
                   uint32_t /* follower u id */>
            mapped_res = get_followers_map( res_tuple );
        DVLOG( 40 ) << "GetFollowers map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            }
        }
    }
}

void get_followers_reducer(
    const std::unordered_map<get_followers_scan_kv_vec_types>& input,
    std::unordered_map<get_followers_scan_kv_types>&           res,
    const EmptyProbe&                                          p ) {
    for( const auto& entry : input ) {
        DVLOG( 40 ) << "Followers reduce:" << entry.first;
        res[entry.first] = entry.first;
    }
}

std::tuple<bool /* found */, uint32_t /* user id */, std::string /* name */>
    get_users_from_followers_map(
        const result_tuple&                                    res,
        const std::unordered_map<get_followers_scan_kv_types>& p ) {
    twitter_user_profile user;

    auto read_cols = read_from_scan_twitter_user_profile( res, &user );

    bool found = ( read_cols.count( twitter_user_profile_cols::name ) == 1 );

    if( found ) {
        found = ( p.count( (uint32_t) res.row_id ) == 1 );
    }

    return std::make_tuple<>( found, (uint32_t) res.row_id, user.name );
}

void get_users_from_followers_mapper(
    const std::vector<result_tuple>&                            res_tuples,
    std::unordered_map<get_users_from_followers_scan_kv_types>& res,
    const std::unordered_map<get_followers_scan_kv_types>&      p ) {
    for( const auto& res_tuple : res_tuples ) {
        std::tuple<bool /* found */, uint32_t /* follower u id */,
                   std::string /* name */>
            mapped_res = get_users_from_followers_map( res_tuple, p );
        DVLOG( 40 ) << "GetUsersFollowers map:" << res_tuple.row_id
                    << ", found:" << std::get<0>( mapped_res );

        if( std::get<0>( mapped_res ) ) {
            auto found = res.find( std::get<1>( mapped_res ) );
            if( found == res.end() ) {
                res.emplace( std::get<1>( mapped_res ),
                             std::get<2>( mapped_res ) );
            }
        }
    }
}

void get_users_from_followers_reducer(
    const std::unordered_map<get_users_from_followers_scan_kv_vec_types>& input,
    std::unordered_map<get_users_from_followers_scan_kv_types>&           res,
    const std::unordered_map<get_followers_scan_kv_types>&                p ) {
    for( const auto& entry : input ) {
        if( entry.second.size() > 0 ) {
            DVLOG( 40 ) << "Follower users reduce:" << entry.first;
            res[entry.first] = entry.second.at( 0 );
        }
    }
}
