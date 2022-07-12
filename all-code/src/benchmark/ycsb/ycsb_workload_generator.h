#pragma once

#include "../../common/hw.h"
#include "../../common/partition_funcs.h"
#include "../../data-site/db/cell_identifier.h"
#include "../../distributions/distributions.h"
#include "../workload_operation_selector.h"
#include "ycsb_configs.h"

class ycsb_workload_generator {
   public:
    ycsb_workload_generator( zipf_distribution_cdf*             z_cdf,
                             const workload_operation_selector& op_selector,
                             const ycsb_configs& configs, uint32_t table_id );
    ~ycsb_workload_generator();

    ALWAYS_INLINE workload_operation_enum get_operation();
    ALWAYS_INLINE uint64_t get_key();
    ALWAYS_INLINE uint64_t get_end_key( uint64_t start ) const;
    ALWAYS_INLINE uint32_t get_column();
    ALWAYS_INLINE bool     get_bool();

    ALWAYS_INLINE bool     get_rmw_bool();
    ALWAYS_INLINE bool     get_scan_bool();

    ALWAYS_INLINE std::string get_value();
    ALWAYS_INLINE uint32_t get_value_size() const;
    ALWAYS_INLINE uint64_t get_num_ops_before_timer_check() const;

    ALWAYS_INLINE bool store_scan_results() const;

    ALWAYS_INLINE partition_column_identifier
                  generate_partition_column_identifier(
            const cell_identifier& cid ) const;

    ALWAYS_INLINE int64_t get_current_time() const;

    predicate_chain get_scan_predicate( uint32_t col );

   private:
    distributions               dist_;
    workload_operation_selector op_selector_;
    ycsb_configs                configs_;
    uint32_t                    table_id_;
};

#include "ycsb_workload_generator-inl.h"
