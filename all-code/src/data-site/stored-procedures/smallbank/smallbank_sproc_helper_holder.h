#pragma once

#include "../../../benchmark/smallbank/smallbank_benchmark_worker.h"
#include "../../../benchmark/smallbank/smallbank_configs.h"
#include "../../../benchmark/smallbank/smallbank_loader.h"
#include "../../../templates/smallbank_benchmark_types.h"

class smallbank_sproc_helper_holder {
   public:
    smallbank_sproc_helper_holder(
        const smallbank_configs&      configs,
        const db_abstraction_configs& abstraction_configs );
    ~smallbank_sproc_helper_holder();

    void init( db* database );
    void init( db_abstraction* db );

    smallbank_benchmark_worker_templ_no_commit* get_worker_and_set_holder(
        const clientid id, transaction_partition_holder* txn_holder );
    smallbank_loader_templ_no_commit* get_loader_and_set_holder(
        const clientid id, transaction_partition_holder* txn_holder );

   private:
    smallbank_configs      configs_;
    db_abstraction_configs abstraction_configs_;

    std::vector<smallbank_loader_templ_no_commit*>           loaders_;
    std::vector<smallbank_benchmark_worker_templ_no_commit*> workers_;
};

