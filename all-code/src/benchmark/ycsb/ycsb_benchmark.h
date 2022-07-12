#pragma once

#include "../benchmark_interface.h"

#include "../../data-site/single-site-db/db_abstraction_types.h"
#include "ycsb_benchmark_worker.h"

class ycsb_benchmark : public benchmark_interface {
   public:
    ycsb_benchmark( const ycsb_configs&           configs,
                    const db_abstraction_configs& abstraction_configs );
    ~ycsb_benchmark();

    void init();

    void                 create_database();
    void                 load_database();
    void                 run_workload();
    benchmark_statistics get_statistics();

   private:
    // loads from start to end (non inclusive of end)
    void load_worker( uint32_t client_id, uint64_t start, uint64_t end );

    // helpers for run benchmark worker
    ALWAYS_INLINE      std::vector<ycsb_benchmark_worker_templ_do_commit*>
                       create_workers();
    ALWAYS_INLINE void start_workers(
        std::vector<ycsb_benchmark_worker_templ_do_commit*>& workers ) const;
    ALWAYS_INLINE void stop_workers(
        std::vector<ycsb_benchmark_worker_templ_do_commit*>& workers ) const;
    ALWAYS_INLINE void gather_workers(
        std::vector<ycsb_benchmark_worker_templ_do_commit*>& workers );

    db_abstraction*        db_;
    ycsb_configs           configs_;
    db_abstraction_configs abstraction_configs_;

    uint32_t table_id_;

    zipf_distribution_cdf*      z_cdf_;
    workload_operation_selector op_selector_;

    benchmark_statistics statistics_;
};

#include "ycsb_benchmark-inl.h"
