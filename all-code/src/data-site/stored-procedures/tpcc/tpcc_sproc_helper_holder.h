#pragma once

#include "../../../benchmark/tpcc/tpcc_benchmark_worker.h"
#include "../../../benchmark/tpcc/tpcc_configs.h"
#include "../../../benchmark/tpcc/tpcc_loader.h"
#include "../../../benchmark/tpcc/tpch_benchmark_worker.h"
#include "../../../templates/tpcc_benchmark_types.h"
#include "../../../templates/tpch_benchmark_types.h"

class tpcc_sproc_helper_holder {
   public:
    tpcc_sproc_helper_holder(
        const tpcc_configs&           configs,
        const db_abstraction_configs& abstraction_configs );
    ~tpcc_sproc_helper_holder();

    void init( db* database );
    void init( db_abstraction* db );

    tpcc_benchmark_worker_templ_no_commit* get_c_worker_and_set_holder(
        const clientid id, transaction_partition_holder* txn_holder );
    tpch_benchmark_worker_templ_no_commit* get_h_worker_and_set_holder(
        const clientid id, transaction_partition_holder* txn_holder );
    tpcc_loader_templ_no_commit* get_loader_and_set_holder(
        const clientid id, transaction_partition_holder* txn_holder );

   private:
    tpcc_configs            configs_;
    db_abstraction_configs  abstraction_configs_;

    std::vector<tpcc_loader_templ_no_commit*>           loaders_;
    std::vector<tpcc_benchmark_worker_templ_no_commit*> c_workers_;
    std::vector<tpch_benchmark_worker_templ_no_commit*> h_workers_;
};

