#pragma once

#include <unordered_map>
#include <vector>

#include "record-types/twitter_primary_key_generation.h"
#include "record-types/twitter_record_types.h"
#include "twitter_configs.h"
#include "twitter_table_ids.h"

#define EmptyProbe uint64_t
#define emptyProbeVal 0

#define tweet_scan_kv_types                                      \
    uint64_t /* Key */, uint32_t /* Val */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>
#define tweet_scan_kv_vec_types                                               \
    uint64_t /* Key */, std::vector<uint32_t> /* Val */, std::hash<uint64_t>, \
        std::equal_to<uint64_t>

void tweet_scan_mapper( const std::vector<result_tuple>&         res_tuples,
                        std::unordered_map<tweet_scan_kv_types>& res,
                        const EmptyProbe&                        p );

void tweet_scan_reducer(
    const std::unordered_map<tweet_scan_kv_vec_types>& input,
    std::unordered_map<tweet_scan_kv_types>& res, const EmptyProbe& p );

#define tweet_scan_types \
    tweet_scan_kv_types, EmptyProbe, tweet_scan_mapper, tweet_scan_reducer

#define get_following_scan_kv_types                              \
    uint32_t /* key */, uint32_t /* Val */, std::hash<uint32_t>, \
        std::equal_to<uint32_t>
#define get_following_scan_kv_vec_types                                       \
    uint32_t /* Key */, std::vector<uint32_t> /* Val */, std::hash<uint32_t>, \
        std::equal_to<uint32_t>

void get_following_mapper( const std::vector<result_tuple>& res_tuples,
                           std::unordered_map<get_following_scan_kv_types>& res,
                           const EmptyProbe&                                p );

void get_following_reducer(
    const std::unordered_map<get_following_scan_kv_vec_types>& input,
    std::unordered_map<get_following_scan_kv_types>& res, const EmptyProbe& p );

#define get_following_scan_types                                   \
    get_following_scan_kv_types, EmptyProbe, get_following_mapper, \
        get_following_reducer

#define get_tweets_from_following_scan_kv_types                              \
    uint32_t /* user id */, uint64_t /* tweet count */, std::hash<uint32_t>, \
        std::equal_to<uint32_t>
#define get_tweets_from_following_scan_kv_vec_types                           \
    uint32_t /* Key */, std::vector<uint64_t> /* Val */, std::hash<uint32_t>, \
        std::equal_to<uint32_t>

void get_tweets_from_following_mapper(
    const std::vector<result_tuple>&                             res_tuples,
    std::unordered_map<get_tweets_from_following_scan_kv_types>& res,
    const std::unordered_map<get_following_scan_kv_types>&       p );

void get_tweets_from_following_reducer(
    const std::unordered_map<get_tweets_from_following_scan_kv_vec_types>&
                                                                 input,
    std::unordered_map<get_tweets_from_following_scan_kv_types>& res,
    const std::unordered_map<get_following_scan_kv_types>&       p );

#define get_tweets_from_following_scan_types             \
    get_tweets_from_following_scan_kv_types,             \
        std::unordered_map<get_following_scan_kv_types>, \
        get_tweets_from_following_mapper, get_tweets_from_following_reducer

#define get_followers_scan_kv_types get_following_scan_kv_types
#define get_followers_scan_kv_vec_types get_following_scan_kv_vec_types

void get_followers_mapper( const std::vector<result_tuple>& res_tuples,
                           std::unordered_map<get_followers_scan_kv_types>& res,
                           const EmptyProbe&                                p );

void get_followers_reducer(
    const std::unordered_map<get_followers_scan_kv_vec_types>& input,
    std::unordered_map<get_followers_scan_kv_types>& res, const EmptyProbe& p );

#define get_followers_scan_types                                   \
    get_followers_scan_kv_types, EmptyProbe, get_followers_mapper, \
        get_followers_reducer

#define get_users_from_followers_scan_kv_types                           \
    uint32_t /* user id */, std::string /* user name */, std::hash<uint32_t>, \
        std::equal_to<uint32_t>
#define get_users_from_followers_scan_kv_vec_types           \
    uint32_t /* Key */, std::vector<std::string> /* name */, \
        std::hash<uint32_t>, std::equal_to<uint32_t>

void get_users_from_followers_mapper(
    const std::vector<result_tuple>&                            res_tuples,
    std::unordered_map<get_users_from_followers_scan_kv_types>& res,
    const std::unordered_map<get_followers_scan_kv_types>&      p );

void get_users_from_followers_reducer(
    const std::unordered_map<get_users_from_followers_scan_kv_vec_types>& input,
    std::unordered_map<get_users_from_followers_scan_kv_types>&           res,
    const std::unordered_map<get_followers_scan_kv_types>&                p );

#define get_users_from_followers_scan_types              \
    get_users_from_followers_scan_kv_types,              \
        std::unordered_map<get_followers_scan_kv_types>, \
        get_users_from_followers_mapper, get_users_from_followers_reducer
