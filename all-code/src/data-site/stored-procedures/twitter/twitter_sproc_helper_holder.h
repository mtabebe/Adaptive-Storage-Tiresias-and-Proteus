#pragma once

#include "../../../benchmark/twitter/twitter_benchmark_worker.h"
#include "../../../benchmark/twitter/twitter_configs.h"
#include "../../../benchmark/twitter/twitter_loader.h"
#include "../../../templates/twitter_benchmark_types.h"

class twitter_sproc_helper_holder {
   public:
    twitter_sproc_helper_holder(
        const twitter_configs&      configs,
        const db_abstraction_configs& abstraction_configs );
    ~twitter_sproc_helper_holder();

    void init( db* database );
    void init( db_abstraction* db );

    twitter_benchmark_worker_templ_no_commit* get_worker_and_set_holder(
        const clientid id, transaction_partition_holder* txn_holder );
    twitter_loader_templ_no_commit* get_loader_and_set_holder(
        const clientid id, transaction_partition_holder* txn_holder );

   private:
    twitter_configs      configs_;
    db_abstraction_configs abstraction_configs_;

    std::vector<twitter_loader_templ_no_commit*>           loaders_;
    std::vector<twitter_benchmark_worker_templ_no_commit*> workers_;
};

