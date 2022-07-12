#pragma once

#include <memory>
#include <thread>
#include <unordered_map>

#include "../../common/hw.h"
#include "../../data-site/single-site-db/db_abstraction.h"
#include "../../templates/twitter_benchmark_types.h"
#include "../benchmark_interface.h"
#include "../benchmark_statistics.h"
#include "record-types/twitter_record_types.h"
#include "twitter_configs.h"
#include "twitter_db_operators.h"
#include "twitter_table_ids.h"
#include "twitter_workload_generator.h"

twitter_benchmark_worker_types class twitter_benchmark_worker {
   public:
    twitter_benchmark_worker(
        uint32_t client_id, db_abstraction* db, zipf_distribution_cdf* z_cdf,
        const workload_operation_selector& op_selector,
        const twitter_configs&           configs,
        const db_abstraction_configs&      abstraction_configs );
    ~twitter_benchmark_worker();

    void start_timed_workload();
    void stop_timed_workload();

    ALWAYS_INLINE benchmark_statistics get_statistics() const;

    workload_operation_outcome_enum do_update_followers();
    workload_operation_outcome_enum do_get_tweet();
    workload_operation_outcome_enum do_get_tweets_from_following();
    workload_operation_outcome_enum do_get_followers();
    workload_operation_outcome_enum do_get_user_tweets();
    workload_operation_outcome_enum do_insert_tweet();

    workload_operation_outcome_enum do_get_recent_tweets();
    workload_operation_outcome_enum do_get_tweets_from_followers();
    workload_operation_outcome_enum do_get_tweets_like();

    workload_operation_outcome_enum perform_update_followers(
        int32_t u_id, int32_t follower_u_id );
    std::tuple<int32_t, int32_t> perform_get_follower_counts(
        int32_t u_id, int32_t follower_u_id );
    void do_get_num_followers( const cell_key_ranges& ckr,
                               std::vector<int32_t>&  num_followers );
    void do_get_num_following( const cell_key_ranges& ckr,
                                    std::vector<int32_t>&  num_following );
    workload_operation_outcome_enum do_update_follower_and_follows(
        int32_t u_id, int32_t follower_u_id, int32_t follower_id,
        int32_t follows_id );
    void do_update_follower( int32_t u_id, int32_t follower_u_id,
                             int32_t follower_id, const cell_key_ranges& ckr );
    void do_update_follows( int32_t u_id, int32_t follower_u_id,
                            int32_t follows_id, const cell_key_ranges& ckr );

    workload_operation_enum perform_get_recent_tweets(
        uint32_t min_user, uint32_t max_user, uint32_t min_tweet,
        uint32_t max_tweet, uint64_t min_tweet_time, uint64_t max_tweet_time );
    std::vector<scan_arguments> generate_get_recent_tweet_scan_args(
        uint32_t min_user, uint32_t max_user, uint32_t min_tweet,
        uint32_t max_tweet, uint64_t min_tweet_time,
        uint64_t max_tweet_time ) const;

    workload_operation_outcome_enum perform_get_tweets_like(
        uint32_t min_user, uint32_t max_user, uint32_t min_tweet,
        uint32_t max_tweet, const std::string& min_tweet_str,
        const std::string& max_tweet_str );
    std::vector<scan_arguments> generate_get_tweets_like_scan_args(
        uint32_t min_user, uint32_t max_user, uint32_t min_tweet,
        uint32_t max_tweet, const std::string& min_tweet_str,
        const std::string& max_tweet_str ) const;

    uint32_t perform_get_tweet( uint32_t u_id, uint32_t tweet_id );
    std::vector<scan_arguments> generate_get_tweet_scan_args(
        uint32_t u_id, uint32_t tweet_id ) const;

    workload_operation_outcome_enum perform_get_tweets_from_followers(
        uint32_t u_id, uint32_t followers_start, uint32_t followers_end,
        uint32_t follower_id_start, uint32_t follower_id_end,
        uint32_t tweet_start, uint32_t tweet_end );
    std::vector<scan_arguments> generate_get_tweets_from_followers_scan_args(
        uint32_t u_id, uint32_t followers_start, uint32_t followers_end,
        uint32_t follower_id_start, uint32_t follower_id_end,
        uint32_t tweet_start, uint32_t tweet_end ) const;

    workload_operation_outcome_enum perform_get_tweets_from_following(
        uint32_t u_id, uint32_t following_start, uint32_t following_end,
        uint32_t follower_id_start, uint32_t follower_id_end,
        uint32_t tweet_start, uint32_t tweet_end );
    std::vector<scan_arguments> generate_get_tweets_from_following_scan_args(
        uint32_t u_id, uint32_t following_start, uint32_t following_end,
        uint32_t follower_id_start, uint32_t follower_id_end,
        uint32_t tweet_start, uint32_t tweet_end ) const;

    workload_operation_outcome_enum perform_get_followers(
        uint32_t u_id, uint32_t start, uint32_t limit, uint32_t follower_min,
        uint32_t follower_max );
    std::vector<scan_arguments> generate_get_followers_scan_args(
        uint32_t u_id, uint32_t start, uint32_t limit, uint32_t follower_min,
        uint32_t follower_max ) const;

    uint32_t get_followers_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result&                                   scan_res );

    uint32_t perform_get_user_tweets( uint32_t u_id, uint32_t tweet_id_start,
                                      uint32_t tweet_id_end );
    std::vector<scan_arguments> generate_get_user_tweets_scan_args(
        uint32_t u_id, uint32_t tweet_id_start, uint32_t tweet_id_end ) const;

    workload_operation_outcome_enum perform_insert_tweet( uint32_t u_id );


    std::vector<scan_arguments> generate_get_following_scan_args(
        uint32_t u_id ) const;

    uint32_t get_tweets_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result&                                   scan_res );
    uint32_t get_tweets_from_following_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result&                                   scan_res );
    uint32_t get_tweets_from_followers_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result&                                   scan_res );

    uint32_t fetch_and_set_next_tweet_id( const cell_key_ranges& ckr,
                                          std::vector<int32_t>&  tweet_ids );
    uint32_t insert_tweet( const cell_key_ranges& ckr );

    void set_transaction_partition_holder(
        transaction_partition_holder* holder );

   private:
    void                            run_workload();
    void                            do_workload_operation();
    workload_operation_outcome_enum perform_workload_operation(
        const workload_operation_enum& op );

    void make_result_count( uint32_t observed, uint32_t table_id,
                            scan_result& scan_res );

    benchmark_db_operators db_operators_;

    twitter_workload_generator generator_;
    benchmark_statistics    statistics_;

    std::unique_ptr<std::thread> worker_;
    volatile bool                done_;
};


#include "twitter_benchmark_worker.tcc"
