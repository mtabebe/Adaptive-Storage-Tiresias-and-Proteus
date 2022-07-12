#pragma once

#include <glog/logging.h>

inline std::vector<twitter_benchmark_worker_templ_do_commit*>
    twitter_benchmark::create_workers() {
    DVLOG( 5 ) << "Creating workers";
    std::vector<twitter_benchmark_worker_templ_do_commit*> workers;
    for( uint32_t client_id = 0;
         client_id < configs_.bench_configs_.num_clients_; client_id++ ) {
        twitter_benchmark_worker_templ_do_commit* worker =
            new twitter_benchmark_worker_templ_do_commit(
                client_id, db_, nullptr, op_selector_, configs_,
                abstraction_configs_ );

        workers.push_back( worker );
    }
    return workers;
}

inline void twitter_benchmark::start_workers(
    std::vector<twitter_benchmark_worker_templ_do_commit*>& workers ) const {
    DVLOG( 5 ) << "Starting workers";
    // start threads
    for( twitter_benchmark_worker_templ_do_commit* worker : workers ) {
        worker->start_timed_workload();
    }
}

inline void twitter_benchmark::stop_workers(
    std::vector<twitter_benchmark_worker_templ_do_commit*>& workers ) const {
    DVLOG( 5 ) << "Stopping workers";
    // stop threads
    for( twitter_benchmark_worker_templ_do_commit* worker : workers ) {
        worker->stop_timed_workload();
    }
}

inline void twitter_benchmark::gather_workers(
    std::vector<twitter_benchmark_worker_templ_do_commit*>& workers ) {
    DVLOG( 5 ) << "Gathering workers";
    // retrieve statistics and merge
    bool merge_safe = true;
    for( twitter_benchmark_worker_templ_do_commit* worker : workers ) {
        merge_safe = statistics_.merge( worker->get_statistics() );
        DCHECK( merge_safe );
        delete worker;
    }
    workers.clear();
}

