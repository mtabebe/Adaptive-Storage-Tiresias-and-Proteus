#pragma once

#include "../../data-site/single-site-db/db_abstraction.h"
#include "../../distributions/distributions.h"
#include "../../distributions/non_uniform_distribution.h"
#include "../../templates/twitter_benchmark_types.h"
#include "record-types/twitter_record_types.h"
#include "twitter_configs.h"

twitter_loader_types class twitter_loader {
   public:
    twitter_loader( db_abstraction* db, const twitter_configs& configs,
                    const db_abstraction_configs& abstraction_configs,
                    uint32_t                      client_id );
    ~twitter_loader();

    void insert_user( uint32_t u_id, uint32_t num_followers,
                      uint32_t num_following, uint32_t num_tweets,
                      const cell_key_ranges& ckr );
    void insert_tweet( uint32_t u_id, uint32_t tweet_id,
                       const cell_key_ranges& ckr );
    void insert_follower( uint32_t u_id, uint32_t follower_u_id,
                          uint32_t follow_id, const cell_key_ranges& ckr );
    void insert_follows( uint32_t u_id, uint32_t following_u_id,
                         uint32_t following_id, const cell_key_ranges& ckr );

    void backfill_user( uint32_t u_id, uint32_t max_num_tweets,
                        uint32_t max_num_follows );

    static void generate_backfill_user_ckrs( std::vector<cell_key_ranges>& ckrs,
                                             uint32_t                      u_id,
                                             uint32_t max_num_tweets,
                                             uint32_t max_num_follows );

    static std::vector<cell_key_ranges> generate_insert_user_write_set_as_ckrs(
        uint32_t cur_start, uint32_t cur_end,
        const std::vector<std::vector<uint32_t>>& follow_data,
        const std::vector<std::vector<uint32_t>>& follower_data,
        const std::vector<uint32_t>& tweet_data, const twitter_configs& cfg );

    void load_range_of_clients(
        uint32_t cur_start, uint32_t cur_end,
        const std::vector<std::vector<uint32_t>>& follow_data,
        const std::vector<std::vector<uint32_t>>& follower_data,
        const std::vector<uint32_t>&              tweet_data );

    void do_range_load_users( const uint32_t* u_ids,
                              const uint32_t* num_followers,
                              const uint32_t* num_following,
                              const uint32_t* num_tweets, uint32_t num_users );
    void do_range_load_users( const uint32_t* num_followers,
                              const uint32_t* num_following,
                              const uint32_t* num_tweets, uint32_t num_users,
                              const cell_key_ranges& ckr );

    void do_load_user_follows( uint32_t u_id, const uint32_t* follows_u_ids,
                               const uint32_t* f_ids, uint32_t num_follows );
    void do_load_user_follows( uint32_t u_id, const uint32_t* follows_u_ids,
                               uint32_t               num_follows,
                               const cell_key_ranges& ckrs );

    void do_load_user_followers( uint32_t u_id, const uint32_t* followers_u_ids,
                                 const uint32_t* f_ids,
                                 uint32_t        num_followers );
    void do_load_user_followers( uint32_t u_id, const uint32_t* followers_u_ids,
                                 uint32_t               num_followers,
                                 const cell_key_ranges& ckrs );

    void do_load_user_tweets( uint32_t u_id, const uint32_t* tweet_ids,
                              uint32_t num_tweets );
    void do_load_user_tweets( uint32_t u_id, const cell_key_ranges& ckr );

    static void generate_range_load_users_ckrs(
        std::vector<cell_key_ranges>& ckrs, uint32_t u_id_start,
        uint32_t u_id_end );
    static void generate_range_load_users_ckrs(
        std::vector<cell_key_ranges>& ckrs, const uint32_t* u_ids,
        uint32_t num_users );

    static void generate_load_user_follows_ckrs(
        std::vector<cell_key_ranges>& ckrs, uint32_t u_id,
        uint32_t num_follows );
    static void generate_load_user_follows_ckrs(
        std::vector<cell_key_ranges>& ckrs, uint32_t u_id,
        const uint32_t* follows_u_ids, const uint32_t* f_ids,
        uint32_t num_follows );

    static void generate_load_user_followers_ckrs(
        std::vector<cell_key_ranges>& ckrs, uint32_t u_id,
        uint32_t num_followers );
    static void generate_load_user_followers_ckrs(
        std::vector<cell_key_ranges>& ckrs, uint32_t u_id,
        const uint32_t* followers_u_ids, const uint32_t* f_ids,
        uint32_t num_followers );

    static void generate_load_user_tweets_ckrs(
        std::vector<cell_key_ranges>& ckrs, uint32_t u_id,
        uint32_t num_tweets );
    static void generate_load_user_tweets_ckrs(
        std::vector<cell_key_ranges>& ckrs, uint32_t u_id,
        const uint32_t* tweet_ids, uint32_t num_tweets );

    void set_transaction_partition_holder(
        transaction_partition_holder* holder );

   private:
    benchmark_db_operators db_operators_;

    twitter_configs        configs_;
    db_abstraction_configs abstraction_configs_;

    twitter_workload_generator generator_;

    distributions dist_;
};

#include "twitter_loader.tcc"
