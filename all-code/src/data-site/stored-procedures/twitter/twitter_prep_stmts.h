#pragma once

#include "../../../benchmark/twitter/twitter_db_operators.h"
#include "../../../benchmark/twitter/twitter_loader.h"
#include "../../../templates/twitter_benchmark_types.h"
#include "../../site-manager/serialize.h"
#include "../../site-manager/sproc_lookup_table.h"
#include "twitter_sproc_helper_holder.h"

static const std::string k_twitter_insert_entire_user_sproc_name =
    "twitter_insert_entire_user";
static const std::vector<arg_code> k_twitter_insert_entire_user_arg_codes = {
    INTEGER_CODE /*u_id*/, INTEGER_CODE /*num_tweets*/,
    INTEGER_ARRAY_CODE /* following ids*/,
    INTEGER_ARRAY_CODE /* follower ids*/};

static const std::string k_twitter_insert_user_sproc_name = "twitter_insert_user";
static const std::vector<arg_code> k_twitter_insert_user_arg_codes = {
    INTEGER_CODE /*u_id*/, INTEGER_CODE /*num_followers*/,
    INTEGER_CODE /* num_following*/, INTEGER_CODE /*num_tweets*/};

static const std::string k_twitter_insert_user_follows_sproc_name =
    "twitter_insert_user_follows";
static const std::vector<arg_code> k_twitter_insert_user_follows_arg_codes = {
    INTEGER_CODE /*u_id*/, INTEGER_ARRAY_CODE /*follows_uids*/};

static const std::string k_twitter_insert_user_followers_sproc_name =
    "twitter_insert_user_followers";
static const std::vector<arg_code> k_twitter_insert_user_followers_arg_codes = {
    INTEGER_CODE /*u_id*/, INTEGER_ARRAY_CODE /*followers_uids*/};

static const std::string k_twitter_insert_user_tweets_sproc_name =
    "twitter_insert_user_tweets";
static const std::vector<arg_code> k_twitter_insert_user_tweets_arg_codes = {
    INTEGER_CODE /*u_id*/};

static const std::string k_twitter_get_tweet_sproc_name = "twitter_get_tweet";
static const std::vector<arg_code> k_twitter_get_tweet_arg_codes = {};

static const std::string k_twitter_fetch_and_set_next_tweet_id_sproc_name =
    "twitter_fetch_and_set_next_tweet_id";
static const std::vector<arg_code>
    k_twitter_fetch_and_set_next_tweet_id_arg_codes = {};
static const std::vector<arg_code>
    k_twitter_fetch_and_set_next_tweet_id_return_arg_codes = {
        INTEGER_ARRAY_CODE /*o_id*/};

static const std::string k_twitter_insert_tweet_sproc_name =
    "twitter_insert_tweet";
static const std::vector<arg_code> k_twitter_insert_tweet_arg_codes = {
};

static const std::string k_twitter_get_tweets_from_following_sproc_name =
    "twitter_get_tweets_from_following";
static const std::vector<arg_code>
    k_twitter_get_tweets_from_following_arg_codes = {};

static const std::string k_twitter_get_tweets_from_followers_sproc_name =
    "twitter_get_tweets_from_followers";
static const std::vector<arg_code>
    k_twitter_get_tweets_from_followers_arg_codes = {};

static const std::string k_twitter_get_followers_sproc_name =
    "twitter_get_followers";
static const std::vector<arg_code> k_twitter_get_followers_arg_codes = {};

static const std::string k_twitter_get_follower_counts_sproc_name =
    "twitter_get_follower_counts";
static const std::vector<arg_code> k_twitter_get_follower_counts_arg_codes = {};
static const std::vector<arg_code>
    k_twitter_get_follower_counts_return_arg_codes = {
        INTEGER_ARRAY_CODE /*num_followers*/,
        INTEGER_ARRAY_CODE /* num_follows */ };

static const std::string k_twitter_update_follower_and_follows_sproc_name =
    "twitter_update_follower_and_follows";
static const std::vector<arg_code>
    k_twitter_update_follower_and_follows_arg_codes = {
        INTEGER_CODE /* u_id */, INTEGER_CODE /* follower_u_id */,
        INTEGER_CODE /* follower_id */, INTEGER_CODE /* follows_id */
};

sproc_result twitter_create_database(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );

sproc_result twitter_insert_entire_user(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs,
    std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );

sproc_result twitter_insert_user(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );

sproc_result twitter_insert_user_follows(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );

sproc_result twitter_insert_user_followers(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );

sproc_result twitter_insert_user_tweets(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );

void twitter_get_tweet( transaction_partition_holder *     partition_holder,
                        const clientid                     id,
                        const std::vector<scan_arguments> &scan_args,
                        std::vector<arg_code> &            codes,
                        std::vector<void *> &values, void *sproc_opaque,
                        scan_result &ret_val );

void twitter_get_tweets_from_following(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<scan_arguments> &scan_args, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque, scan_result &ret_val );

void twitter_get_tweets_from_followers(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<scan_arguments> &scan_args, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque, scan_result &ret_val );


void twitter_get_followers( transaction_partition_holder *     partition_holder,
                            const clientid                     id,
                            const std::vector<scan_arguments> &scan_args,
                            std::vector<arg_code> &            codes,
                            std::vector<void *> &values, void *sproc_opaque,
                            scan_result &ret_val );

sproc_result twitter_fetch_and_set_next_tweet_id(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
sproc_result twitter_insert_tweet(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );

sproc_result twitter_update_follower_and_follows(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );
sproc_result twitter_get_follower_counts(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );

