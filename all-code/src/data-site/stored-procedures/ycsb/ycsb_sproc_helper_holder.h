#pragma once

#include "../../../benchmark/ycsb/ycsb_benchmark_worker.h"
#include "../../../benchmark/ycsb/ycsb_configs.h"
#include "../../../templates/ycsb_benchmark_types.h"

class ycsb_sproc_helper_holder {
   public:
    ycsb_sproc_helper_holder(
        const ycsb_configs&      configs,
        const db_abstraction_configs& abstraction_configs );
    ~ycsb_sproc_helper_holder();

    void init( db* database );
    void init( db_abstraction* db );

    ycsb_benchmark_worker_templ_no_commit* get_worker_and_set_holder(
        const clientid id, transaction_partition_holder* txn_holder );

   private:
    ycsb_configs      configs_;
    db_abstraction_configs abstraction_configs_;

    std::vector<ycsb_benchmark_worker_templ_no_commit*> workers_;
};

