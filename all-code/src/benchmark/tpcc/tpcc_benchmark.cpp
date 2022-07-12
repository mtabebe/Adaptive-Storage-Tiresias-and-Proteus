#include "tpcc_benchmark.h"

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include <algorithm>

#include "../../common/thread_utils.h"
#include "tpcc_table_ids.h"
#include "tpcc_table_sizes.h"

tpcc_benchmark::tpcc_benchmark(
    const tpcc_configs&           configs,
    const db_abstraction_configs& abstraction_configs )
    : db_( nullptr ),
      configs_( configs ),
      abstraction_configs_( abstraction_configs ),
      z_cdf_( nullptr ),
      c_op_selector_(),
      h_op_selector_(),
      statistics_(),
      loaded_mutex_(),
      loaded_state_() {}
tpcc_benchmark::~tpcc_benchmark() {
    if( db_ != nullptr ) {
        delete db_;
        db_ = nullptr;
    }
}

benchmark_statistics tpcc_benchmark::get_statistics() { return statistics_; }

void tpcc_benchmark::init() {
    db_ = create_db_abstraction( abstraction_configs_ );
    DVLOG( 5 ) << "Initializing tpcc_benchmark";
    // assume 1 site for now
    // there is one table in tpcc
    statistics_.init( k_tpcch_workload_operations );
    db_->init( make_no_op_update_destination_generator(),
               make_update_enqueuers(),
               create_tables_metadata(
                   13 /* there are thirteen tables*/, 0 /*single site bench*/,
                   configs_.bench_configs_.num_clients_,
                   configs_.bench_configs_.gc_sleep_time_,
                   configs_.bench_configs_.enable_secondary_storage_,
                   configs_.bench_configs_.secondary_storage_dir_ ) );
    /*
     * fun fact, TPCC doesn't use a ZIPF distribution it uses some made up
     * approximate non uniform distribution, so we just leave the z_cdf_ null
     * to pass around
    */
    c_op_selector_.init( k_tpcc_workload_operations, configs_.c_workload_probs_ );
    h_op_selector_.init( k_tpch_workload_operations, configs_.h_workload_probs_ );
    DVLOG( 5 ) << "Initializing tpcc_benchmark okay!";
}

void tpcc_benchmark::create_database() {
    DVLOG( 5 ) << "Creating database for tpcc_benchmark";

    tpcc_create_tables( db_, configs_ );

    // db_.init_garbage_collector( db_.compute_gc_sleep_time() );
    DVLOG( 5 ) << "Creating database for tpcc_benchmark okay!";
}

void tpcc_benchmark::load_database() {
    DVLOG( 1 ) << "Loading database for tpcc_benchmark";

    if( configs_.bench_configs_.num_clients_ == 1 ) {
        load_items_and_nations( 0 );
        load_warehouses( 0, 0, configs_.num_warehouses_ );
    } else {
        std::vector<std::thread> loader_threads;
        std::thread i_loader( &tpcc_benchmark::load_items_and_nations, this, 0 );
        loader_threads.push_back( std::move( i_loader ) );
        uint32_t load_clients =
            std::min( configs_.num_warehouses_,
                      configs_.bench_configs_.num_clients_ - 1 );
        uint32_t load_warehouse_size_per_client =
            ( configs_.num_warehouses_ / load_clients ) + 1;

        for( uint32_t client_id = 1; client_id < load_clients; client_id++ ) {
            uint32_t start = ( client_id - 1 ) * load_warehouse_size_per_client;
            uint32_t end = std::min( configs_.num_warehouses_,
                                     start + load_warehouse_size_per_client );
            std::thread l( &tpcc_benchmark::load_warehouses, this, client_id,
                           start, end );
            loader_threads.push_back( std::move( l ) );
        }
        join_threads( loader_threads );
    }

    DVLOG( 1 ) << "Loading database for tpcc_benchmark okay!";
}

void tpcc_benchmark::run_workload() {
    DVLOG( 1 ) << "Running workload for tpcc_benchmark";
    std::vector<tpcc_benchmark_worker_templ_do_commit*> c_workers =
        create_c_workers();
    std::vector<tpch_benchmark_worker_templ_do_commit*> h_workers =
        create_h_workers();

    // start a timer
    std::chrono::high_resolution_clock::time_point s =
        std::chrono::high_resolution_clock::now();
    std::chrono::high_resolution_clock::time_point e =
        s + std::chrono::seconds( configs_.bench_configs_.benchmark_time_sec_ );

    start_c_workers( c_workers );
    start_h_workers( h_workers );

    sleep_until_end_of_benchmark( e );

    stop_c_workers( c_workers );
    stop_h_workers( h_workers );

    gather_c_workers( c_workers );
    gather_h_workers( h_workers );

    DVLOG( 1 ) << "Running workload for tpcc_benchmark okay!";
}
void tpcc_benchmark::load_items_and_nations( uint32_t client_id ) {
    DVLOG( 10 ) << "Client:" << client_id << " loading items and nations";

    load_items( 0 );
    load_nations( 0 );

    DVLOG( 10 ) << "Client:" << client_id << " loading items and nations okay!";
}

void tpcc_benchmark::load_nations( uint32_t client_id ) {
    DVLOG( 10 ) << "Client:" << client_id << " loading nations";

    tpcc_loader_templ_do_commit loader( db_, configs_, abstraction_configs_,
                                        client_id );

    loader.make_nation_related_partitions();
    loader.make_nation_related_tables();

    merge_in_loaded_state( loader.get_snapshot() );

    DVLOG( 10 ) << "Client:" << client_id << " loading nations okay!";
}

void tpcc_benchmark::load_items( uint32_t client_id ) {
    DVLOG( 10 ) << "Client:" << client_id << " loading items";

    tpcc_loader_templ_do_commit loader( db_, configs_, abstraction_configs_,
                                        client_id );

    loader.make_items_partitions();
    loader.make_items_table();

    merge_in_loaded_state( loader.get_snapshot() );

    DVLOG( 10 ) << "Client:" << client_id << " loading items okay!";
}
void tpcc_benchmark::load_warehouses( uint32_t client_id,
                                      uint32_t warehouse_start,
                                      uint32_t warehouse_end ) {

    DVLOG( 10 ) << "Client:" << client_id << " loading warehouses from "
                << warehouse_start << " to " << warehouse_end;

    tpcc_loader_templ_do_commit loader( db_, configs_, abstraction_configs_,
                                        client_id );

    for( uint32_t w_id = warehouse_start; w_id < warehouse_end; w_id++ ) {
        loader.make_warehouse_related_partitions( w_id );
        loader.make_warehouse_related_tables( w_id );
    }

    merge_in_loaded_state( loader.get_snapshot() );

    DVLOG( 10 ) << "Client:" << client_id << " loading warehouses from "
                << warehouse_start << " to " << warehouse_end << " okay!";
}

void tpcc_benchmark::merge_in_loaded_state( const snapshot_vector& state ) {
    {
        std::lock_guard<std::mutex> guard( loaded_mutex_ );
        merge_snapshot_versions_if_larger( loaded_state_, state );
    }
}

#include "tpcc_benchmark-inl.h"
