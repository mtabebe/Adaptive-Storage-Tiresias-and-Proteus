#pragma once

#include "../../data-site/db/partition_metadata.h"

inline workload_operation_enum ycsb_workload_generator::get_operation() {
    return op_selector_.get_operation( dist_.get_uniform_int( 0, 100 ) );
}
inline uint64_t ycsb_workload_generator::get_key() {
    return dist_.get_zipf_value();
}
inline std::string ycsb_workload_generator::get_value() {
    std::string value( configs_.value_size_, dist_.get_uniform_char() );
    return value;
}

inline uint32_t ycsb_workload_generator::get_value_size() const {
    return configs_.value_size_;
}

inline uint64_t ycsb_workload_generator::get_num_ops_before_timer_check()
    const {
    return configs_.bench_configs_.num_ops_before_timer_check_;
}
inline uint64_t ycsb_workload_generator::get_end_key( uint64_t start ) const {
    return std::min( configs_.max_key_ - 1,
                     start + configs_.num_ops_per_transaction_ );
}

inline uint32_t ycsb_workload_generator::get_column() {
    return dist_.get_uniform_int( 0, k_ycsb_num_columns );
}
inline bool ycsb_workload_generator::get_bool() {
    return (bool) dist_.get_uniform_int( 0, 2 );
}

inline bool ycsb_workload_generator::get_rmw_bool() {
    if( configs_.rmw_restricted_ ) {
        return false;
    }
    return (bool) dist_.get_uniform_int( 0, 2 );
}
inline bool ycsb_workload_generator::get_scan_bool() {
    if( configs_.scan_restricted_ ) {
        return true;
    }
    return (bool) dist_.get_uniform_int( 0, 2 );
}

inline partition_column_identifier
    ycsb_workload_generator::generate_partition_column_identifier(
        const cell_identifier& cid ) const {
    uint64_t partition_start =
        configs_.partition_size_ * ( cid.key_ / configs_.partition_size_ );
    uint64_t partition_end = partition_start + configs_.partition_size_ - 1;
    uint32_t col_start = configs_.column_partition_size_ *
                         ( cid.col_id_ / configs_.column_partition_size_ );
    uint32_t col_end =
        std::min( col_start + configs_.column_partition_size_ - 1,
                  k_ycsb_num_columns - 1 );
    return create_partition_column_identifier(
        table_id_, partition_start, partition_end, col_start, col_end );
}

inline int64_t ycsb_workload_generator::get_current_time() const {
    return dist_.get_current_time();
}

inline bool ycsb_workload_generator::store_scan_results() const {
    return configs_.store_scan_results_;
}
