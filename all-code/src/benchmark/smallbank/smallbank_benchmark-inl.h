#pragma once

#include <glog/logging.h>

inline std::vector<smallbank_benchmark_worker_templ_do_commit*>
    smallbank_benchmark::create_workers() {
    DVLOG( 5 ) << "Creating workers";
    std::vector<smallbank_benchmark_worker_templ_do_commit*> workers;
    for( uint32_t client_id = 0;
         client_id < configs_.bench_configs_.num_clients_; client_id++ ) {
        smallbank_benchmark_worker_templ_do_commit* worker =
            new smallbank_benchmark_worker_templ_do_commit(
                client_id, db_, z_cdf_, op_selector_, configs_,
                abstraction_configs_ );

        workers.push_back( worker );
    }
    return workers;
}

inline void smallbank_benchmark::start_workers(
    std::vector<smallbank_benchmark_worker_templ_do_commit*>& workers ) const {
    DVLOG( 5 ) << "Starting workers";
    // start threads
    for( smallbank_benchmark_worker_templ_do_commit* worker : workers ) {
        worker->start_timed_workload();
    }
}

inline void smallbank_benchmark::stop_workers(
    std::vector<smallbank_benchmark_worker_templ_do_commit*>& workers ) const {
    DVLOG( 5 ) << "Stopping workers";
    // stop threads
    for( smallbank_benchmark_worker_templ_do_commit* worker : workers ) {
        worker->stop_timed_workload();
    }
}

inline void smallbank_benchmark::gather_workers(
    std::vector<smallbank_benchmark_worker_templ_do_commit*>& workers ) {
    DVLOG( 5 ) << "Gathering workers";
    // retrieve statistics and merge
    bool merge_safe = true;
    for( smallbank_benchmark_worker_templ_do_commit* worker : workers ) {
        merge_safe = statistics_.merge( worker->get_statistics() );
        DCHECK( merge_safe );
        delete worker;
    }
    workers.clear();
}

