#include "smallbank_benchmark.h"

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include <algorithm>

#include "../../common/thread_utils.h"
#include "smallbank_table_ids.h"
#include "smallbank_table_sizes.h"

smallbank_benchmark::smallbank_benchmark(
    const smallbank_configs&      configs,
    const db_abstraction_configs& abstraction_configs )
    : db_( nullptr ),
      configs_( configs ),
      abstraction_configs_( abstraction_configs ),
      z_cdf_( nullptr ),
      op_selector_(),
      statistics_() {}
smallbank_benchmark::~smallbank_benchmark() {
    if( db_ != nullptr ) {
        delete db_;
        db_ = nullptr;
    }
}

benchmark_statistics smallbank_benchmark::get_statistics() {
    return statistics_;
}

void smallbank_benchmark::init() {
    db_ = create_db_abstraction( abstraction_configs_ );

    DVLOG( 5 ) << "Initializing smallbank_benchmark";
    // assume 1 site for now
    // there is one table in smallbank
    statistics_.init( k_smallbank_workload_operations );
    db_->init( make_no_op_update_destination_generator(),
               make_update_enqueuers(),
               create_tables_metadata(
                   3 /* there are three tables*/, 0 /*single site bench*/,
                   configs_.bench_configs_.num_clients_,
                   configs_.bench_configs_.gc_sleep_time_,
                   configs_.bench_configs_.enable_secondary_storage_,
                   configs_.bench_configs_.secondary_storage_dir_ ) );
    op_selector_.init( k_smallbank_workload_operations,
                       configs_.workload_probs_ );
    DVLOG( 5 ) << "Initializing smallbank_benchmark okay!";
}

void smallbank_benchmark::create_database() {
    DVLOG( 5 ) << "Creating database for smallbank_benchmark";

    smallbank_create_tables( db_, configs_ );

    // db_.init_garbage_collector( db_.compute_gc_sleep_time() );
    DVLOG( 5 ) << "Creating database for smallbank_benchmark okay!";
}

void smallbank_benchmark::load_database() {
    DVLOG( 5 ) << "Loading database for smallbank_benchmark";

    uint32_t load_clients = configs_.bench_configs_.num_clients_;
    if( load_clients == 1 ) {
        load_accounts_savings_and_checkings( 0, 0, configs_.num_accounts_ );
    } else {
        std::vector<std::thread> loader_threads;
        uint64_t num_accounts_per_client =
            ( configs_.num_accounts_ / load_clients ) + 1;

        for( uint32_t client_id = 0; client_id < load_clients; client_id++ ) {
            uint64_t start = ( client_id ) * num_accounts_per_client;
            uint64_t end = std::min( configs_.num_accounts_,
                                     start + num_accounts_per_client );
            std::thread l(
                &smallbank_benchmark::load_accounts_savings_and_checkings, this,
                client_id, start, end );
            loader_threads.push_back( std::move( l ) );
        }
        join_threads( loader_threads );
    }

    DVLOG( 5 ) << "Loading database for smallbank_benchmark okay!";
}

void smallbank_benchmark::run_workload() {
    DVLOG( 5 ) << "Running workload for smallbank_benchmark";
    std::vector<smallbank_benchmark_worker_templ_do_commit*> workers =
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

    DVLOG( 5 ) << "Running workload for smallbank_benchmark okay!";
}

void smallbank_benchmark::load_accounts_savings_and_checkings(
    uint32_t client_id, uint64_t account_start, uint64_t account_end ) {

    DVLOG( 10 ) << "Client:" << client_id << " loading accounts from "
                << account_start << " to " << account_end;

    smallbank_loader_templ_do_commit loader( db_, configs_,
                                             abstraction_configs_, client_id );

    uint64_t cur_start = account_start;
    uint64_t cur_end = 0;
    while( cur_start < account_end ) {
        cur_end =
            std::min( account_end - 1, cur_start + configs_.partition_size_ );

        loader.add_partition_ranges( cur_start, cur_end );

        loader.do_range_load_accounts( cur_start, cur_end );
        loader.do_range_load_savings( cur_start, cur_end );
        loader.do_range_load_checkings( cur_start, cur_end );

        cur_start = cur_end + 1;
    }

    DVLOG( 10 ) << "Client:" << client_id << " loading accounts from "
                << account_start << " to " << account_end << " okay!";
}
