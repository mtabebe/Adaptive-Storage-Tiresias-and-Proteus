#include "tpcc_sproc_helper_holder.h"

#include <glog/logging.h>

tpcc_sproc_helper_holder::tpcc_sproc_helper_holder(
    const tpcc_configs&           configs,
    const db_abstraction_configs& abstraction_configs )
    : configs_( configs ),
      abstraction_configs_( abstraction_configs ),
      loaders_(),
      c_workers_(),
      h_workers_() {}
tpcc_sproc_helper_holder::~tpcc_sproc_helper_holder() {
    for( uint32_t client_id = 0; client_id < loaders_.size(); client_id++ ) {
        delete c_workers_.at( client_id );
        delete h_workers_.at( client_id );
        delete loaders_.at( client_id );
    }
    c_workers_.clear();
    h_workers_.clear();
    loaders_.clear();
}

void tpcc_sproc_helper_holder::init( db* database ) {
    DCHECK_EQ( abstraction_configs_.db_type_, db_abstraction_type::PLAIN_DB );

    plain_db_wrapper* wrapper = new plain_db_wrapper();
    wrapper->init( database );

    init( wrapper );
}

void tpcc_sproc_helper_holder::init( db_abstraction* db ) {
    uint32_t num_clients = configs_.bench_configs_.num_clients_;
    workload_operation_selector c_op_selector;
    workload_operation_selector h_op_selector;

    c_op_selector.init( k_tpcc_workload_operations,
                        configs_.c_workload_probs_ );
    h_op_selector.init( k_tpch_workload_operations,
                        configs_.h_workload_probs_ );

    loaders_.reserve( num_clients );
    c_workers_.reserve( num_clients );
    h_workers_.reserve( num_clients );

    snapshot_vector snap;

    for( uint32_t client_id = 0; client_id < num_clients; client_id++ ) {
        tpcc_benchmark_worker_templ_no_commit* c_worker =
            new tpcc_benchmark_worker_templ_no_commit(
                client_id, db /*db*/, nullptr /*zipf*/, c_op_selector, configs_,
                abstraction_configs_ );
        c_workers_.push_back( c_worker );
        tpch_benchmark_worker_templ_no_commit* h_worker =
            new tpch_benchmark_worker_templ_no_commit(
                client_id, db /*db*/, nullptr /*zipf*/, h_op_selector, configs_,
                abstraction_configs_, snap );
        h_workers_.push_back( h_worker );
        tpcc_loader_templ_no_commit* loader = new tpcc_loader_templ_no_commit(
            db /*db*/, configs_, abstraction_configs_, client_id );
        loaders_.push_back( loader );
    }
}

tpcc_benchmark_worker_templ_no_commit*
    tpcc_sproc_helper_holder::get_c_worker_and_set_holder(
        const clientid id, transaction_partition_holder* txn_holder ) {
    DCHECK_LT( id, c_workers_.size() );
    auto worker = c_workers_.at( id );
    worker->set_transaction_partition_holder( txn_holder );
    return worker;
}
tpch_benchmark_worker_templ_no_commit*
    tpcc_sproc_helper_holder::get_h_worker_and_set_holder(
        const clientid id, transaction_partition_holder* txn_holder ) {
    DCHECK_LT( id, h_workers_.size() );
    auto worker = h_workers_.at( id );
    worker->set_transaction_partition_holder( txn_holder );
    return worker;
}

tpcc_loader_templ_no_commit*
    tpcc_sproc_helper_holder::get_loader_and_set_holder(
        const clientid id, transaction_partition_holder* txn_holder ) {
    DCHECK_LT( id, loaders_.size() );
    auto loader = loaders_.at( id );
    loader->set_transaction_partition_holder( txn_holder );
    return loader;
}
