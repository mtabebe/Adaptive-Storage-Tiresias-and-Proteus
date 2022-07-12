#include "twitter_benchmark.h"

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include <algorithm>

#include "../../common/thread_utils.h"
#include "twitter_table_ids.h"
#include "twitter_table_sizes.h"
#include "twitter_workload_generator.h"

twitter_benchmark::twitter_benchmark(
    const twitter_configs&        configs,
    const db_abstraction_configs& abstraction_configs )
    : db_( nullptr ),
      configs_( configs ),
      abstraction_configs_( abstraction_configs ),
      tweet_z_cdf_( nullptr ),
      follows_z_cdf_( nullptr ),
      followee_z_cdf_( nullptr ),
      op_selector_(),
      statistics_() {}
twitter_benchmark::~twitter_benchmark() {
    if( db_ != nullptr ) {
        delete db_;
        db_ = nullptr;
    }
    if( tweet_z_cdf_ ) {
        delete tweet_z_cdf_;
        tweet_z_cdf_ = nullptr;
    }
    if( follows_z_cdf_ ) {
        delete follows_z_cdf_;
        follows_z_cdf_ = nullptr;
    }
    if( followee_z_cdf_ ) {
        delete followee_z_cdf_;
        followee_z_cdf_ = nullptr;
    }
}

benchmark_statistics twitter_benchmark::get_statistics() {
    return statistics_;
}

void twitter_benchmark::init() {
    db_ = create_db_abstraction( abstraction_configs_ );

    DVLOG( 5 ) << "Initializing twitter_benchmark";
    // assume 1 site for now
    // there is one table in twitter
    statistics_.init( k_twitter_workload_operations );

    tweet_z_cdf_ = new zipf_distribution_cdf( 0, configs_.num_tweets_,
                                              configs_.tweet_skew_ );
    followee_z_cdf_ = new zipf_distribution_cdf( 0, configs_.num_users_,
                                                 configs_.follow_skew_ );
    follows_z_cdf_ = new zipf_distribution_cdf(
        0, configs_.max_follow_per_user_, configs_.follow_skew_ );

    tweet_z_cdf_->init();
    follows_z_cdf_->init();
    followee_z_cdf_->init();

    db_->init( make_no_op_update_destination_generator(),
               make_update_enqueuers(),
               create_tables_metadata(
                   4 /* there are thirteen tables*/, 0 /*single site bench*/,
                   configs_.bench_configs_.num_clients_,
                   configs_.bench_configs_.gc_sleep_time_,
                   configs_.bench_configs_.enable_secondary_storage_,
                   configs_.bench_configs_.secondary_storage_dir_ ) );

    op_selector_.init( k_twitter_workload_operations,
                       configs_.workload_probs_ );
    DVLOG( 5 ) << "Initializing twitter_benchmark okay!";
}

void twitter_benchmark::create_database() {
    DVLOG( 5 ) << "Creating database for twitter_benchmark";

    twitter_create_tables( db_, configs_ );

    // db_.init_garbage_collector( db_.compute_gc_sleep_time() );
    DVLOG( 5 ) << "Creating database for twitter_benchmark okay!";
}

void twitter_benchmark::load_database() {
    DVLOG( 5 ) << "Loading database for twitter_benchmark";

    uint32_t load_clients = configs_.bench_configs_.num_clients_;

    std::vector<uint32_t> tweet_data;
    twitter_workload_generator::generate_num_tweets( tweet_data, configs_,
                                                     tweet_z_cdf_ );

    std::vector<std::vector<uint32_t>> follow_data;
    std::vector<std::vector<uint32_t>> follower_data;
    std::vector<uint32_t> follow_ids;
    std::vector<uint32_t> follower_ids;
    twitter_workload_generator::generate_follow_data(
        follow_data, follower_data, follow_ids, follower_ids, configs_,
        follows_z_cdf_, followee_z_cdf_ );

    if( load_clients == 1 ) {
        load_users( 0, 0, configs_.num_users_, follow_data, follower_data,
                    tweet_data, follow_ids, follower_ids );
    } else {
        std::vector<std::thread> loader_threads;
        uint32_t                 num_users_per_client =
            ( configs_.num_users_ / load_clients ) + 1;

        for( uint32_t client_id = 0; client_id < load_clients; client_id++ ) {
            uint32_t start = ( client_id ) * num_users_per_client;
            uint32_t end = std::min( (uint32_t) configs_.num_users_,
                                     start + num_users_per_client );
            std::thread l( &twitter_benchmark::load_users, this, client_id,
                           start, end, follow_data, follower_data, tweet_data,
                           follow_ids, follower_ids );
            loader_threads.push_back( std::move( l ) );
        }
        join_threads( loader_threads );
    }

    DVLOG( 5 ) << "Loading database for twitter_benchmark okay!";
}

void twitter_benchmark::run_workload() {
    DVLOG( 5 ) << "Running workload for twitter_benchmark";
    std::vector<twitter_benchmark_worker_templ_do_commit*> workers =
        create_workers();

    // start a timer
    std::chrono::high_resolution_clock::time_point s =
        std::chrono::high_resolution_clock::now();
    std::chrono::high_resolution_clock::time_point e =
        s + std::chrono::seconds( configs_.bench_configs_.benchmark_time_sec_ );

    start_workers( workers );

    sleep_until_end_of_benchmark( e );

    stop_workers( workers );

    gather_workers( workers );

    DVLOG( 5 ) << "Running workload for twitter_benchmark okay!";
}

void twitter_benchmark::load_users(
    uint32_t client_id, uint32_t user_start, uint32_t user_end,
    const std::vector<std::vector<uint32_t>>& follow_data,
    const std::vector<std::vector<uint32_t>>& follower_data,
    const std::vector<uint32_t>&              tweet_data,
    const std::vector<uint32_t>&              follow_ids,
    const std::vector<uint32_t>&              follower_ids ) {

    DCHECK_EQ( tweet_data.size(), follow_data.size() );
    DCHECK_EQ( tweet_data.size(), follower_data.size() );

    DCHECK_LT( user_end, tweet_data.size() );

    DVLOG( 10 ) << "Client:" << client_id << " loading users from "
                << user_start << " to " << user_end;

    twitter_loader_templ_do_commit loader( db_, configs_, abstraction_configs_,
                                           client_id );

    uint32_t cur_start = user_start;
    uint32_t cur_end = 0;
    while( cur_start < user_end ) {
        cur_end = std::min( user_end - 1,
                            cur_start + configs_.account_partition_size_ );

        loader.load_range_of_clients( cur_start, cur_end, follower_data,
                                      follower_data, tweet_data );

        cur_start = cur_end + 1;
    }

    DVLOG( 10 ) << "Client:" << client_id << " loading users from "
                << user_start << " to " << user_end << " okay!";
}

