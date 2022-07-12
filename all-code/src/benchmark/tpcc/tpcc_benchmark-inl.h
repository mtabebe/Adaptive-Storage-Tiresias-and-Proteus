#pragma once

inline std::vector<tpcc_benchmark_worker_templ_do_commit*>
    tpcc_benchmark::create_c_workers() {
    DVLOG( 5 ) << "Creating c_workers";
    std::vector<tpcc_benchmark_worker_templ_do_commit*> workers;
    for( uint32_t client_id = 0;
         client_id < configs_.c_num_clients_; client_id++ ) {
        tpcc_benchmark_worker_templ_do_commit* worker =
            new tpcc_benchmark_worker_templ_do_commit( client_id, db_, z_cdf_,
                                                       c_op_selector_, configs_,
                                                       abstraction_configs_ );

        workers.push_back( worker );
    }
    return workers;
}

inline void tpcc_benchmark::start_c_workers(
    std::vector<tpcc_benchmark_worker_templ_do_commit*>& workers ) const {
    DVLOG( 5 ) << "Starting c_workers";
    // start threads
    for( tpcc_benchmark_worker_templ_do_commit* worker : workers ) {
        worker->start_timed_workload();
    }
}

inline void tpcc_benchmark::stop_c_workers(
    std::vector<tpcc_benchmark_worker_templ_do_commit*>& workers ) const {
    DVLOG( 5 ) << "Stopping c_workers";
    // stop threads
    for( tpcc_benchmark_worker_templ_do_commit* worker : workers ) {
        worker->stop_timed_workload();
    }
}

inline void tpcc_benchmark::gather_c_workers(
    std::vector<tpcc_benchmark_worker_templ_do_commit*>& workers ) {
    DVLOG( 5 ) << "Gathering c_workers";
    // retrieve statistics and merge
    bool merge_safe = true;
    for( tpcc_benchmark_worker_templ_do_commit* worker : workers ) {
        merge_safe = statistics_.merge( worker->get_statistics() );
        DCHECK( merge_safe );
        delete worker;
    }
    workers.clear();
}

inline std::vector<tpch_benchmark_worker_templ_do_commit*>
    tpcc_benchmark::create_h_workers() {
    DVLOG( 5 ) << "Creating h_workers";
    std::vector<tpch_benchmark_worker_templ_do_commit*> workers;
    for( uint32_t client_id = 0; client_id < configs_.h_num_clients_;
         client_id++ ) {
        tpch_benchmark_worker_templ_do_commit* worker =
            new tpch_benchmark_worker_templ_do_commit(
                client_id + configs_.c_num_clients_, db_, z_cdf_,
                h_op_selector_, configs_, abstraction_configs_, loaded_state_ );

        workers.push_back( worker );
    }
    return workers;
}

inline void tpcc_benchmark::start_h_workers(
    std::vector<tpch_benchmark_worker_templ_do_commit*>& workers ) const {
    DVLOG( 5 ) << "Starting h_workers";
    // start threads
    for( tpch_benchmark_worker_templ_do_commit* worker : workers ) {
        worker->start_timed_workload();
    }
}

inline void tpcc_benchmark::stop_h_workers(
    std::vector<tpch_benchmark_worker_templ_do_commit*>& workers ) const {
    DVLOG( 5 ) << "Stopping h_workers";
    // stop threads
    for( tpch_benchmark_worker_templ_do_commit* worker : workers ) {
        worker->stop_timed_workload();
    }
}

inline void tpcc_benchmark::gather_h_workers(
    std::vector<tpch_benchmark_worker_templ_do_commit*>& workers ) {
    DVLOG( 5 ) << "Gathering h_workers";
    // retrieve statistics and merge
    bool merge_safe = true;
    for( tpch_benchmark_worker_templ_do_commit* worker : workers ) {
        merge_safe = statistics_.merge( worker->get_statistics() );
        DCHECK( merge_safe );
        delete worker;
    }
    workers.clear();
}

