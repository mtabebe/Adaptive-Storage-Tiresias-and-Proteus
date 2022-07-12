#pragma once

#include <memory>
#include <thread>

#include "../../common/hw.h"
#include "../../data-site/single-site-db/db_abstraction.h"
#include "../../templates/ycsb_benchmark_types.h"
#include "../benchmark_db_operators.h"
#include "../benchmark_interface.h"
#include "../benchmark_statistics.h"
#include "ycsb_configs.h"
#include "ycsb_workload_generator.h"

ycsb_benchmark_worker_types class ycsb_benchmark_worker {
   public:
    ycsb_benchmark_worker( uint32_t client_id, uint32_t table_id,
                           db_abstraction* db, zipf_distribution_cdf* z_cdf,
                           const workload_operation_selector& op_selector,
                           const ycsb_configs&                configs,
                           const db_abstraction_configs& abstraction_configs );
    ~ycsb_benchmark_worker();

    void set_transaction_partition_holder(
        transaction_partition_holder* holder );

    void start_timed_workload();
    void stop_timed_workload();

    void add_partitions( uint64_t start, uint64_t end );

    ALWAYS_INLINE workload_operation_outcome_enum do_read( uint64_t key );
    workload_operation_outcome_enum do_ckr_read( const cell_key_ranges& ckr );

    ALWAYS_INLINE workload_operation_outcome_enum do_write( uint64_t key );
    workload_operation_outcome_enum do_ckr_write( const cell_key_ranges& ckr );

    ALWAYS_INLINE workload_operation_outcome_enum
        do_read_modify_write( uint64_t key, uint32_t col );

    workload_operation_outcome_enum do_ckr_read_modify_write(
        const std::vector<cell_key_ranges>& writeCKRs,
        const std::vector<cell_key_ranges>& readCKRs, bool do_prop );

    ALWAYS_INLINE workload_operation_outcome_enum do_delete( uint64_t key );
    workload_operation_outcome_enum do_delete_ckr( const cell_key_ranges& ckr );

    ALWAYS_INLINE workload_operation_outcome_enum do_scan( uint64_t start,
                                                           uint32_t col );
    ALWAYS_INLINE workload_operation_outcome_enum
        do_multi_key_read_modify_write( uint64_t start, uint32_t col );

    workload_operation_outcome_enum do_range_insert( uint64_t start,
                                                     uint64_t end );

    workload_operation_outcome_enum do_ckr_insert( const cell_key_ranges& ckr );

    workload_operation_outcome_enum do_db_split( uint64_t key, uint32_t col,
                                                 bool split_vertically );
    workload_operation_outcome_enum do_db_merge( uint64_t key, uint32_t col,
                                                 bool merge_vertically );
    workload_operation_outcome_enum do_db_remaster( uint64_t key,
                                                    uint32_t col );

    workload_operation_enum do_internal_scan( uint64_t start, uint64_t end,
                                              uint32_t               scan_col,
                                              const predicate_chain& pred );
    workload_operation_outcome_enum do_ckr_scan(
        const std::vector<cell_key_ranges>& ckrs, const predicate_chain& pred );
    workload_operation_outcome_enum do_ckr_scan(
        const std::vector<cell_key_ranges>& ckrs, const predicate_chain& pred,
        std::vector<result_tuple>& result );

    workload_operation_enum do_internal_multi_key_read_modify_write(
        const uint64_t* keys, uint32_t num_keys, uint32_t field_col );

    workload_operation_outcome_enum change_partition_types(
        uint64_t start, uint64_t end, const partition_type::type& part_type,
        const storage_tier_type::type& storage_type );

    ALWAYS_INLINE benchmark_statistics get_statistics() const;

    ALWAYS_INLINE                      std::vector<partition_column_identifier>
        generate_partition_set( uint64_t row_start, uint64_t row_end,
                                uint32_t col_start, uint32_t col_end ) const;

   private:
    void                            run_workload();
    void                            do_workload_operation();
    workload_operation_outcome_enum perform_workload_operation(
        const workload_operation_enum& op );

    workload_operation_enum do_internal_read( uint64_t start );
    workload_operation_enum do_internal_multi_key_read_modify_write(
        const std::vector<uint64_t>& keys, uint32_t field_col );

    ALWAYS_INLINE void check_ycsb_ckr( const cell_key_ranges& ckr ) const;

    uint32_t table_id_;

    benchmark_db_operators db_operators_;

    ycsb_workload_generator generator_;
    benchmark_statistics    statistics_;


    std::unique_ptr<std::thread> worker_;
    volatile bool                done_;
};

#include "ycsb_benchmark_worker.tcc"
