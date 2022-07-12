#include "twitter_configs.h"

#include <glog/logging.h>

#include "twitter_table_sizes.h"

twitter_layout_configs construct_twitter_layout_configs(
    uint32_t                       user_profile_column_partition_size,
    const partition_type::type&    user_profile_part_type,
    const storage_tier_type::type& user_profile_storage_type,
    uint32_t                       followers_column_partition_size,
    const partition_type::type&    followers_part_type,
    const storage_tier_type::type& followers_storage_type,
    uint32_t                       follows_column_partition_size,
    const partition_type::type&    follows_part_type,
    const storage_tier_type::type& follows_storage_type,
    uint32_t                       tweets_column_partition_size,
    const partition_type::type&    tweets_part_type,
    const storage_tier_type::type& tweets_storage_type ) {

    twitter_layout_configs configs;

    configs.user_profile_column_partition_size_ =
        user_profile_column_partition_size;
    configs.user_profile_part_type_ = user_profile_part_type;
    configs.user_profile_storage_type_ = user_profile_storage_type;

    configs.followers_column_partition_size_ = followers_column_partition_size;
    configs.followers_part_type_ = followers_part_type;
    configs.followers_storage_type_ = followers_storage_type;

    configs.follows_column_partition_size_ = follows_column_partition_size;
    configs.follows_part_type_ = follows_part_type;
    configs.follows_storage_type_ = follows_storage_type;

    configs.tweets_column_partition_size_ = tweets_column_partition_size;
    configs.tweets_part_type_ = tweets_part_type;
    configs.tweets_storage_type_ = tweets_storage_type;

    return configs;
}

twitter_configs construct_twitter_configs(
    uint64_t num_users, uint64_t num_tweets, double tweet_skew,
    double follow_skew, uint32_t account_partition_size,
    uint32_t max_follow_per_user, uint32_t limit_tweets,
    uint32_t limit_tweets_for_uid, uint32_t limit_followers,
    uint32_t max_tweets_per_user, const benchmark_configs& bench_configs,
    uint32_t get_tweet_prob, uint32_t get_tweets_from_following_prob,
    uint32_t get_followers_prob, uint32_t get_user_tweets_prob,
    uint32_t insert_tweet_prob, uint32_t get_recent_tweets_prob,
    uint32_t get_tweets_from_followers_prob, uint32_t get_tweets_like_prob,
    uint32_t update_followers_prob,
    const twitter_layout_configs& layouts ) {

    twitter_configs configs;

    configs.num_users_ = num_users;
    configs.num_tweets_ = num_tweets;

    configs.tweet_skew_ = tweet_skew;
    configs.follow_skew_ = follow_skew;

    configs.account_partition_size_ = account_partition_size;
    configs.max_follow_per_user_ = max_follow_per_user;
    configs.max_tweets_per_user_ = max_tweets_per_user;

    configs.limit_tweets_ = limit_tweets;
    configs.limit_tweets_for_uid_ = limit_tweets_for_uid;
    configs.limit_followers_ = limit_followers;

    configs.insert_num_tweets_ = std::max(
        std::max(
            std::max( configs.limit_tweets_, configs.limit_tweets_for_uid_ ),
            configs.limit_followers_ ),
        configs.max_tweets_per_user_ );
    configs.insert_num_follows_ =
        std::max( configs.max_follow_per_user_, configs.limit_followers_ );

    configs.bench_configs_ = bench_configs;

    configs.layouts_ = layouts;

    configs.workload_probs_ = { get_tweet_prob,
                                get_tweets_from_following_prob,
                                get_followers_prob,
                                get_user_tweets_prob,
                                insert_tweet_prob,
                                get_recent_tweets_prob,
                                get_tweets_from_followers_prob,
                                get_tweets_like_prob,
                                update_followers_prob };
    uint32_t cum_workload_prob = 0;
    for( uint32_t prob : configs.workload_probs_ ) {
        DCHECK_GE( prob, 0 );
        cum_workload_prob += prob;
    }

    DCHECK_EQ( 100, cum_workload_prob );

    DVLOG( 1 ) << "Created: " << configs;

    return configs;
}

std::ostream& operator<<( std::ostream&                 os,
                          const twitter_layout_configs& configs ) {
    os << "Twitter Layout Config: [ "
       << " user_profile_column_partition_size_: "
       << configs.user_profile_column_partition_size_
       << ", user_profile_part_type_:" << configs.user_profile_part_type_
       << ", user_profile_storage_type_:" << configs.user_profile_storage_type_
       << ", followers_column_partition_size_: "
       << configs.followers_column_partition_size_
       << ", followers_part_type_:" << configs.followers_part_type_
       << ", followers_storage_type_:" << configs.followers_storage_type_
       << ", follows_column_partition_size_: "
       << configs.follows_column_partition_size_
       << ", follows_part_type_:" << configs.follows_part_type_
       << ", follows_storage_type_:" << configs.follows_storage_type_
       << ", tweets_column_partition_size_: "
       << configs.tweets_column_partition_size_
       << ", tweets_part_type_:" << configs.tweets_part_type_
       << ", tweets_storage_type_:" << configs.tweets_storage_type_ << " ]";
    return os;
}

std::ostream& operator<<( std::ostream& os, const twitter_configs& config ) {
    os << "Twitter Config: [ "
       << " Num Users:" << config.num_users_ << ","
       << " Num Tweets:" << config.num_tweets_ << ","
       << " Tweet Skew:" << config.tweet_skew_ << ","
       << " Follow Skew:" << config.follow_skew_ << ","
       << " Account Partition Size:" << config.account_partition_size_ << ","
       << " Max Follow Per User:" << config.max_follow_per_user_ << ","
       << " Max Tweets Per User:" << config.max_tweets_per_user_ << ","
       << " Limit Tweets:" << config.limit_tweets_ << ","
       << " Limit Tweets for UID:" << config.limit_tweets_for_uid_ << ","
       << " Limit Followers:" << config.limit_followers_ << ","
       << ", Insert Num Tweets:" << config.insert_num_tweets_
       << ", Insert Num Follows:" << config.insert_num_tweets_ << ", "
       << config.layouts_ << ", " << config.bench_configs_ << " ]";
    return os;
}

std::vector<table_metadata> create_twitter_table_metadata(
    const twitter_configs& configs, int32_t site ) {

    std::vector<table_metadata> table_metas;

    std::vector<table_partition_information> table_info =
        get_twitter_data_sizes( configs );

    int32_t  chain_size = k_num_records_in_chain;
    int32_t  chain_size_for_snapshot = k_num_records_in_snapshot_chain;
    uint32_t site_location = site;

    std::vector<std::string> names = {"twitter_user_profiles", "twitter_followers",
                                      "twitter_follows", "twitter_tweets"};
    std::vector<uint32_t> table_ids = {
        k_twitter_user_profiles_table_id, k_twitter_followers_table_id,
        k_twitter_follows_table_id, k_twitter_tweets_table_id};
    DCHECK_EQ( names.size(), table_info.size() );
    DCHECK_EQ( names.size(), table_ids.size() );

    for( uint32_t table_id : table_ids ) {
        auto t_info = table_info.at( table_id );

        DCHECK_EQ( table_id, std::get<0>( t_info ) );
        // create tables
        table_metas.push_back( create_table_metadata(
            names.at( table_id ), table_id,
            std::get<4>( t_info ) /* num columns*/,
            std::get<5>( t_info ) /* col types*/, chain_size,
            chain_size_for_snapshot, site_location,
            /* partition size*/ std::get<2>( t_info ), /* col size*/
            std::get<3>( t_info ),
            /* partition tracking size*/ std::get<2>(
                t_info ), /* col tracking size*/
            std::get<3>( t_info ),
            /* part type*/ std::get<6>( t_info ),
            /* storage type*/ std::get<7>( t_info ) ) );
    }

    return table_metas;
}

