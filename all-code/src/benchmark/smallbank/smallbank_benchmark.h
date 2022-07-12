#pragma once

#include "../../data-site/db/db.h"
#include "../../data-site/single-site-db/db_abstraction.h"
#include "../../data-site/single-site-db/db_abstraction_types.h"
#include "../../templates/smallbank_benchmark_types.h"
#include "../benchmark_interface.h"
#include "../workload_operation_selector.h"
#include "smallbank_benchmark_worker.h"
#include "smallbank_configs.h"
#include "smallbank_db_operators.h"
#include "smallbank_loader.h"

class smallbank_benchmark : public benchmark_interface {
   public:
    smallbank_benchmark( const smallbank_configs&      configs,
                         const db_abstraction_configs& abstraction_configs );
    ~smallbank_benchmark();

    void init();

    void                 create_database();
    void                 load_database();
    void                 run_workload();
    benchmark_statistics get_statistics();

   private:
    // helpers for run benchmark worker
    ALWAYS_INLINE
    std::vector<smallbank_benchmark_worker_templ_do_commit*> create_workers();
    ALWAYS_INLINE void                                       start_workers(
        std::vector<smallbank_benchmark_worker_templ_do_commit*>& workers )
        const;
    ALWAYS_INLINE void stop_workers(
        std::vector<smallbank_benchmark_worker_templ_do_commit*>& workers )
        const;
    ALWAYS_INLINE void gather_workers(
        std::vector<smallbank_benchmark_worker_templ_do_commit*>& workers );

    void load_accounts_savings_and_checkings( uint32_t client_id,
                                              uint64_t account_start,
                                              uint64_t account_end );

    db_abstraction*        db_;
    smallbank_configs      configs_;
    db_abstraction_configs abstraction_configs_;

    zipf_distribution_cdf*      z_cdf_;
    workload_operation_selector op_selector_;

    benchmark_statistics statistics_;
};

#include "smallbank_benchmark-inl.h"
