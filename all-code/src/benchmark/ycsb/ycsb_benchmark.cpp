#include "ycsb_benchmark.h"

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include <algorithm>

#include "../../common/thread_utils.h"
#include "../../data-site/update-propagation/no_op_update_destination.h"
#include "../../data-site/update-propagation/update_destination_generator.h"

ycsb_benchmark::ycsb_benchmark(
    const ycsb_configs&           configs,
    const db_abstraction_configs& abstraction_configs )
    : db_( nullptr ),
      configs_( configs ),
      abstraction_configs_( abstraction_configs ),
      table_id_( -1 ),
      z_cdf_( nullptr ),
      op_selector_(),
      statistics_() {}
ycsb_benchmark::~ycsb_benchmark() {
    if( db_ != nullptr ) {
        delete db_;
        db_ = nullptr;
    }
}

void ycsb_benchmark::init() {
    db_ = create_db_abstraction( abstraction_configs_ );
    DVLOG( 5 ) << "Initializing ycsb_benchmark";
    // assume 1 site for now
    // there is one table in ycsb
    statistics_.init( k_ycsb_workload_operations );

    db_->init( make_no_op_update_destination_generator(),
               make_update_enqueuers(),
               create_tables_metadata(
                   1 /* there is one table*/, 0 /*single site bench*/,
                   configs_.bench_configs_.num_clients_,
                   configs_.bench_configs_.gc_sleep_time_,
                   configs_.bench_configs_.enable_secondary_storage_,
                   configs_.bench_configs_.secondary_storage_dir_ ) );
    // create zipf_distribution
    // max_key is never written
    z_cdf_ = new zipf_distribution_cdf( 0, configs_.max_key_ - 1,
                                        configs_.zipf_alpha_ );
    z_cdf_->init();
    op_selector_.init( k_ycsb_workload_operations, configs_.workload_probs_ );
    DVLOG( 5 ) << "Initializing ycsb_benchmark okay!";
}

void ycsb_benchmark::create_database() {
    DVLOG( 5 ) << "Creating database for ycsb_benchmark";
    table_id_ = db_->create_table( create_ycsb_table_metadata( configs_, 0 ) );
    DCHECK_EQ( 0, table_id_ );
    // db_.init_garbage_collector( db_.compute_gc_sleep_time() );
    DVLOG( 5 ) << "Creating database for ycsb_benchmark okay!";
}

void ycsb_benchmark::load_database() {
    DVLOG( 5 ) << "Loading database for ycsb_benchmark";
    // create num client threads each of which loads their region
    uint64_t load_size_per_client =
        ( configs_.max_key_ / configs_.bench_configs_.num_clients_ ) + 1;

    std::vector<std::thread> loaders;

    for( uint32_t client_id = 0;
         client_id < configs_.bench_configs_.num_clients_; client_id++ ) {
        uint64_t start = (client_id) *load_size_per_client;
        uint64_t end =
            std::min( configs_.max_key_, start + load_size_per_client );
        std::thread l( &ycsb_benchmark::load_worker, this, client_id, start,
                       end );
        loaders.push_back( std::move( l ) );
    }
    join_threads( loaders );
    DVLOG( 5 ) << "Loading database for ycsb_benchmark okay!";
}

void ycsb_benchmark::run_workload() {
    DVLOG( 5 ) << "Running workload for ycsb_benchmark";
    std::vector<ycsb_benchmark_worker_templ_do_commit*> workers =
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
    DVLOG( 5 ) << "Running workload for ycsb_benchmark okay!";
}

benchmark_statistics ycsb_benchmark::get_statistics() { return statistics_; }

void ycsb_benchmark::load_worker( uint32_t client_id, uint64_t start,
                                  uint64_t end ) {
    ycsb_benchmark_worker_templ_do_commit worker(
        client_id, table_id_, db_, z_cdf_, op_selector_, configs_,
        abstraction_configs_ );
    DVLOG( 10 ) << client_id << " loading range:" << start << " , " << end;

    worker.add_partitions( start, end );

    uint64_t cur_start = start;
    uint64_t cur_end = 0;
    while( cur_start < end ) {
        cur_end =
            std::min( end - 1, cur_start + configs_.num_ops_per_transaction_ );

        worker.do_range_insert( cur_start, cur_end );

        cur_start = cur_end + 1;
    }

    if( configs_.storage_type_ != storage_tier_type::type::MEMORY ) {
        worker.change_partition_types( start, end, configs_.partition_type_,
                                       configs_.storage_type_ );
    }

    DVLOG( 10 ) << client_id << " loading range:" << start << " , " << end
                << ", OKAY";
}
