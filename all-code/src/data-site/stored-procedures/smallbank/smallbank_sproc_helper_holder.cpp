#include "smallbank_sproc_helper_holder.h"

#include <glog/logging.h>

smallbank_sproc_helper_holder::smallbank_sproc_helper_holder(
    const smallbank_configs&      configs,
    const db_abstraction_configs& abstraction_configs )
    : configs_( configs ),
      abstraction_configs_( abstraction_configs ),
      loaders_(),
      workers_() {}
smallbank_sproc_helper_holder::~smallbank_sproc_helper_holder() {
    for( uint32_t client_id = 0; client_id < loaders_.size(); client_id++ ) {
        delete workers_.at( client_id );
        delete loaders_.at( client_id );
    }
    workers_.clear();
    loaders_.clear();
}

void smallbank_sproc_helper_holder::init( db* database ) {
    DCHECK_EQ( abstraction_configs_.db_type_, db_abstraction_type::PLAIN_DB );

    plain_db_wrapper* wrapper = new plain_db_wrapper();
    wrapper->init( database );

    init( wrapper );
}

void smallbank_sproc_helper_holder::init( db_abstraction* db ) {
    uint32_t num_clients = configs_.bench_configs_.num_clients_;
    workload_operation_selector op_selector;
    op_selector.init( k_smallbank_workload_operations,
                      configs_.workload_probs_ );

    loaders_.reserve( num_clients );
    workers_.reserve( num_clients );

    for( uint32_t client_id = 0; client_id < num_clients; client_id++ ) {
        smallbank_benchmark_worker_templ_no_commit* worker =
            new smallbank_benchmark_worker_templ_no_commit(
                client_id, db, nullptr /*zipf*/, op_selector, configs_,
                abstraction_configs_ );
        workers_.push_back( worker );
        smallbank_loader_templ_no_commit* loader =
            new smallbank_loader_templ_no_commit(
                db, configs_, abstraction_configs_, client_id );
        loaders_.push_back( loader );
    }
}

smallbank_benchmark_worker_templ_no_commit*
    smallbank_sproc_helper_holder::get_worker_and_set_holder(
        const clientid id, transaction_partition_holder* txn_holder ) {
    DCHECK_LT( id, workers_.size() );
    auto worker = workers_.at( id );
    worker->set_transaction_partition_holder( txn_holder );
    return worker;
}
smallbank_loader_templ_no_commit*
    smallbank_sproc_helper_holder::get_loader_and_set_holder(
        const clientid id, transaction_partition_holder* txn_holder ) {
    DCHECK_LT( id, loaders_.size() );
    auto loader = loaders_.at( id );
    loader->set_transaction_partition_holder( txn_holder );
    return loader;
}
