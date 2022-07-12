#pragma once

#include "../../common/hw.h"
#include "../../distributions/distributions.h"
#include "../../distributions/non_uniform_distribution.h"
#include "../workload_operation_selector.h"
#include "twitter_configs.h"

class generated_twitter_data {
  public:
    generated_twitter_data();

    std::vector<std::vector<uint32_t>> follow_data_;
    std::vector<std::vector<uint32_t>> follower_data_;

    std::vector<uint32_t>             tweet_data_;

    std::vector<uint32_t>             follow_ids_;
    std::vector<uint32_t>             follower_ids_;
};

class twitter_workload_generator {
   public:
    twitter_workload_generator( zipf_distribution_cdf*             z_cdf,
                             const workload_operation_selector& op_selector,
                             int32_t client_id, const twitter_configs& configs );

    twitter_workload_generator( zipf_distribution_cdf* z_cdf,
                                  int32_t                  client_id,
                                  const twitter_configs& configs );

    ~twitter_workload_generator();

    ALWAYS_INLINE workload_operation_enum get_operation();
    ALWAYS_INLINE uint64_t get_num_ops_before_timer_check() const;

    uint32_t get_user();
    uint32_t get_tweet();

    uint32_t get_follower_id( uint32_t num_follows );

    std::tuple<std::string, std::string> get_tweet_string_range();

    static void generate_num_tweets( std::vector<uint32_t>& tweet_data,
                                     const twitter_configs& configs,
                                     zipf_distribution_cdf* z_cdf );
    static void generate_follow_data(
        std::vector<std::vector<uint32_t>>& follow_data,
        std::vector<std::vector<uint32_t>>& follower_data,
        std::vector<uint32_t>& follow_ids, std::vector<uint32_t>& follower_ids,
        const twitter_configs& configs, zipf_distribution_cdf* follow_z_cdf,
        zipf_distribution_cdf* followee_z_cdf );

    static generated_twitter_data generate_data(
        const twitter_configs& configs, zipf_distribution_cdf* tweet_z_cdf,
        zipf_distribution_cdf* follows_z_cdf,
        zipf_distribution_cdf* followee_z_cdf );

    distributions            dist_;

    twitter_configs configs_;

   private:
    workload_operation_selector op_selector_;
    int32_t                     client_id_;

};

#include "twitter_workload_generator-inl.h"
