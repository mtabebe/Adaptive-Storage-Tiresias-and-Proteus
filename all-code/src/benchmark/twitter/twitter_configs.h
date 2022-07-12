#pragma once

#include <iostream>

#include "../../common/constants.h"
#include "../../common/hw.h"
#include "../../data-site/db/partition_metadata.h"
#include "../benchmark_configs.h"
#include "record-types/twitter_record_types.h"

class twitter_layout_configs {
   public:
    uint32_t                user_profile_column_partition_size_;
    partition_type::type    user_profile_part_type_;
    storage_tier_type::type user_profile_storage_type_;

    uint32_t                followers_column_partition_size_;
    partition_type::type    followers_part_type_;
    storage_tier_type::type followers_storage_type_;

    uint32_t                follows_column_partition_size_;
    partition_type::type    follows_part_type_;
    storage_tier_type::type follows_storage_type_;

    uint32_t                tweets_column_partition_size_;
    partition_type::type    tweets_part_type_;
    storage_tier_type::type tweets_storage_type_;
};

class twitter_configs {
  public:
   uint64_t num_users_;
   uint64_t num_tweets_;

   double tweet_skew_;
   double follow_skew_;

   uint32_t account_partition_size_;
   uint32_t max_follow_per_user_;
   uint32_t max_tweets_per_user_;

   uint32_t limit_tweets_;
   uint32_t limit_tweets_for_uid_;
   uint32_t limit_followers_;

   uint32_t insert_num_tweets_;
   uint32_t insert_num_follows_;

   twitter_layout_configs layouts_;

   benchmark_configs     bench_configs_;
   std::vector<uint32_t> workload_probs_;
};

twitter_layout_configs construct_twitter_layout_configs(
    uint32_t user_profile_column_partition_size =
        k_twitter_user_profile_column_partition_size,
    const partition_type::type &user_profile_part_type =
        k_twitter_user_profile_part_type,
    const storage_tier_type::type &user_profile_storage_type =
        k_twitter_user_profile_storage_type,
    uint32_t followers_column_partition_size =
        k_twitter_followers_column_partition_size,
    const partition_type::type &followers_part_type =
        k_twitter_followers_part_type,
    const storage_tier_type::type &followers_storage_type =
        k_twitter_followers_storage_type,
    uint32_t follows_column_partition_size =
        k_twitter_follows_column_partition_size,
    const partition_type::type &follows_part_type = k_twitter_follows_part_type,
    const storage_tier_type::type &follows_storage_type =
        k_twitter_follows_storage_type,
    uint32_t tweets_column_partition_size =
        k_twitter_tweets_column_partition_size,
    const partition_type::type &tweets_part_type = k_twitter_tweets_part_type,
    const storage_tier_type::type &tweets_storage_type =
        k_twitter_tweets_storage_type );

twitter_configs construct_twitter_configs(
    uint64_t num_users = k_twitter_num_users,
    uint64_t num_tweets = k_twitter_num_tweets,
    double   tweet_skew = k_twitter_tweet_skew,
    double   follow_skew = k_twitter_follow_skew,
    uint32_t account_partition_size = k_twitter_account_partition_size,
    uint32_t max_follow_per_user = k_twitter_max_follow_per_user,
    uint32_t max_tweets_per_user = k_twitter_max_tweets_per_user,
    uint32_t limit_tweets = k_twitter_limit_tweets,
    uint32_t limit_tweets_for_uid = k_twitter_limit_tweets_for_uid,
    uint32_t limit_followers = k_twitter_limit_followers,
    const benchmark_configs &bench_configs = construct_benchmark_configs(),
    uint32_t                 get_tweet_prob = k_twitter_get_tweet_prob,
    uint32_t                 get_tweets_from_following_prob =
        k_twitter_get_tweets_from_following_prob,
    uint32_t get_followers_prob = k_twitter_get_followers_prob,
    uint32_t get_user_tweets_prob = k_twitter_get_user_tweets_prob,
    uint32_t insert_tweet_prob = k_twitter_insert_tweet_prob,
    uint32_t get_recent_tweets_prob = k_twitter_get_recent_tweets_prob,
    uint32_t get_tweets_from_followers_prob =
        k_twitter_get_tweets_from_followers_prob,
    uint32_t get_tweets_like_prob = k_twitter_get_tweets_like_prob,
    uint32_t update_followers_prob = k_twitter_update_followers_prob,
    const twitter_layout_configs &layouts =
        construct_twitter_layout_configs() );

std::ostream &operator<<( std::ostream &                os,
                          const twitter_layout_configs &configs );
std::ostream &operator<<( std::ostream &os, const twitter_configs &configs );

std::vector<table_metadata> create_twitter_table_metadata(
    const twitter_configs &configs = construct_twitter_configs(),
    int32_t                  site = k_site_location_identifier );

