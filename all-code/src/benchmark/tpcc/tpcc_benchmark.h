#pragma once

#include <mutex>

#include "../../data-site/db/db.h"
#include "../../data-site/single-site-db/db_abstraction.h"
#include "../../data-site/single-site-db/db_abstraction_types.h"
#include "../../templates/tpcc_benchmark_types.h"
#include "../../templates/tpch_benchmark_types.h"
#include "../benchmark_interface.h"
#include "../workload_operation_selector.h"
#include "tpcc_benchmark_worker.h"
#include "tpcc_configs.h"
#include "tpcc_db_operators.h"
#include "tpcc_loader.h"
#include "tpch_benchmark_worker.h"

class tpcc_benchmark : public benchmark_interface {
   public:
    tpcc_benchmark( const tpcc_configs&           configs,
                    const db_abstraction_configs& abstraction_configs );
    ~tpcc_benchmark();

    void init();

    void                 create_database();
    void                 load_database();
    void                 run_workload();
    benchmark_statistics get_statistics();

   private:
    void load_items_and_nations( uint32_t client_id );
    void load_items( uint32_t client_id );
    void load_nations( uint32_t client_id );
    void load_warehouses( uint32_t client_id, uint32_t warehouse_start,
                          uint32_t warehouse_end );
    // helpers for run benchmark worker
    ALWAYS_INLINE
    std::vector<tpcc_benchmark_worker_templ_do_commit*> create_c_workers();
    ALWAYS_INLINE void                                  start_c_workers(
        std::vector<tpcc_benchmark_worker_templ_do_commit*>& workers ) const;
    ALWAYS_INLINE void stop_c_workers(
        std::vector<tpcc_benchmark_worker_templ_do_commit*>& workers ) const;
    ALWAYS_INLINE void gather_c_workers(
        std::vector<tpcc_benchmark_worker_templ_do_commit*>& workers );

    ALWAYS_INLINE
    std::vector<tpch_benchmark_worker_templ_do_commit*> create_h_workers();
    ALWAYS_INLINE void                                  start_h_workers(
        std::vector<tpch_benchmark_worker_templ_do_commit*>& workers ) const;
    ALWAYS_INLINE void stop_h_workers(
        std::vector<tpch_benchmark_worker_templ_do_commit*>& workers ) const;
    ALWAYS_INLINE void gather_h_workers(
        std::vector<tpch_benchmark_worker_templ_do_commit*>& workers );

    void merge_in_loaded_state( const snapshot_vector& state );

    db_abstraction*        db_;
    tpcc_configs           configs_;
    db_abstraction_configs abstraction_configs_;

    zipf_distribution_cdf*      z_cdf_;
    workload_operation_selector c_op_selector_;
    workload_operation_selector h_op_selector_;

    benchmark_statistics statistics_;

    std::mutex      loaded_mutex_;
    snapshot_vector loaded_state_;
};

