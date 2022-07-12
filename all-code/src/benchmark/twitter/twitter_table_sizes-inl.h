#pragma once

inline std::vector<table_partition_information> get_twitter_data_sizes(
    const twitter_configs& configs ) {

    std::vector<table_partition_information> sizes;

    sizes.emplace_back( k_twitter_user_profiles_table_id, configs.num_users_,
                        configs.account_partition_size_,
                        configs.layouts_.user_profile_column_partition_size_,
                        k_twitter_user_profile_num_columns,
                        k_twitter_user_profile_col_types,
                        configs.layouts_.user_profile_part_type_,
                        configs.layouts_.user_profile_storage_type_ );
    sizes.emplace_back( k_twitter_followers_table_id,
                        configs.num_users_ * configs.max_follow_per_user_,
                        configs.limit_followers_,
                        configs.layouts_.followers_column_partition_size_,
                        k_twitter_followers_num_columns,
                        k_twitter_followers_col_types,
                        configs.layouts_.followers_part_type_,
                        configs.layouts_.followers_storage_type_ );
    sizes.emplace_back( k_twitter_follows_table_id,
                        configs.num_users_ * configs.max_follow_per_user_,
                        configs.limit_followers_,
                        configs.layouts_.follows_column_partition_size_,
                        k_twitter_follows_num_columns,
                        k_twitter_follows_col_types,
                        configs.layouts_.follows_part_type_,
                        configs.layouts_.follows_storage_type_ );
    sizes.emplace_back(
        k_twitter_tweets_table_id, configs.num_users_ * configs.num_tweets_,
        configs.limit_tweets_, configs.layouts_.tweets_column_partition_size_,
        k_twitter_tweets_num_columns, k_twitter_tweets_col_types,
        configs.layouts_.tweets_part_type_,
        configs.layouts_.tweets_storage_type_ );

    return sizes;
}
