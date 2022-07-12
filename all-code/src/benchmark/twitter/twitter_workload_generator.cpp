#include "twitter_workload_generator.h"

generated_twitter_data::generated_twitter_data()
    : follow_data_(),
      follower_data_(),
      tweet_data_(),
      follow_ids_(),
      follower_ids_() {}

twitter_workload_generator::twitter_workload_generator(
    zipf_distribution_cdf*             z_cdf,
    const workload_operation_selector& op_selector, int32_t client_id,
    const twitter_configs& configs )
    : dist_( z_cdf ),
      configs_( configs ),
      op_selector_( op_selector ),
      client_id_( client_id ) {}

twitter_workload_generator::twitter_workload_generator(
    zipf_distribution_cdf* z_cdf, int32_t client_id,
    const twitter_configs& configs )
    : dist_( z_cdf ),
      configs_( configs ),
      op_selector_(),
      client_id_( client_id ) {}

twitter_workload_generator::~twitter_workload_generator() {}

uint32_t twitter_workload_generator::get_user() {
    return dist_.get_uniform_int( 0, configs_.num_users_ );
}

uint32_t twitter_workload_generator::get_follower_id( uint32_t num_follows ) {
    return dist_.get_uniform_int( 0, num_follows );
}
uint32_t twitter_workload_generator::get_tweet() {
    uint32_t avg_tweet_cnt = configs_.num_tweets_ / configs_.num_users_;
    return dist_.get_uniform_int( 0, avg_tweet_cnt );
}

std::tuple<std::string, std::string>
    twitter_workload_generator::get_tweet_string_range() {
    int32_t pos = dist_.get_uniform_int( 0, k_all_chars.size() - 2 );
    return std::make_tuple<>( std::string( 1, k_all_chars.at( pos ) ),
                              std::string( 1, k_all_chars.at( pos + 1 ) ) );
}

void twitter_workload_generator::generate_num_tweets(
    std::vector<uint32_t>& tweet_data, const twitter_configs& configs,
    zipf_distribution_cdf* z_cdf ) {
    tweet_data.assign( configs.num_users_ + 1, 0 );

    distributions dist( z_cdf );

    for( uint32_t tweet = 0; tweet <= configs.num_tweets_; tweet++ ) {
        uint32_t user = dist.get_scrambled_zipf_value() % configs.num_users_;
        tweet_data.at( user ) = tweet_data.at( user ) + 1;
    }
}
void twitter_workload_generator::generate_follow_data(
    std::vector<std::vector<uint32_t>>& follow_data,
    std::vector<std::vector<uint32_t>>& follower_data,
    std::vector<uint32_t>& follow_ids, std::vector<uint32_t>& follower_ids,
    const twitter_configs& configs, zipf_distribution_cdf* follows_z_cdf,
    zipf_distribution_cdf* followee_z_cdf ) {
    std::vector<uint32_t> empty_vec;
    follow_data.assign( configs.num_users_ + 1, empty_vec );
    follower_data.assign( configs.num_users_ + 1, empty_vec );

    std::unordered_set<uint32_t>              empty_set;
    std::vector<std::unordered_set<uint32_t>> already_following;
    already_following.assign( configs.num_users_ + 1, empty_set );

    distributions followee_dist( followee_z_cdf );
    distributions follows_dist( follows_z_cdf );

    for( uint32_t follower = 0; follower < configs.num_users_; follower++ ) {
        int num_followers = follows_dist.get_zipf_value();
        if( num_followers == 0 ) {
            num_followers = 1;
        }
        for( int f = 0; f < num_followers; f++ ) {
            int following = followee_dist.get_zipf_value();
            if( already_following.at( follower ).count( following ) == 0 ) {
                already_following.at( follower ).emplace( following );
                follow_data.at( follower ).push_back( following );
                follower_data.at( following ).push_back( follower );

                if( follow_ids.size() < follow_data.at( follower ).size() ) {
                    follow_ids.emplace_back( follow_ids.size() );
                    DCHECK_EQ( follow_ids.size(),
                               follow_data.at( follower ).size() );
                }
                if( follower_ids.size() <
                    follower_data.at( following ).size() ) {
                    follower_ids.emplace_back( follower_ids.size() );
                    DCHECK_EQ( follower_ids.size(),
                               follower_data.at( following ).size() );
                }
            }
        }
    }
}

generated_twitter_data twitter_workload_generator::generate_data(
    const twitter_configs& configs, zipf_distribution_cdf* tweet_z_cdf,
    zipf_distribution_cdf* follows_z_cdf,
    zipf_distribution_cdf* followee_z_cdf ) {

    generated_twitter_data gen_data;

    generate_num_tweets( gen_data.tweet_data_, configs, tweet_z_cdf );
    generate_follow_data( gen_data.follow_data_, gen_data.follower_data_,
                          gen_data.follow_ids_, gen_data.follower_ids_, configs,
                          follows_z_cdf, followee_z_cdf );

    return gen_data;
}

