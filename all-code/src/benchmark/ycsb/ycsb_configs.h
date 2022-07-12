#pragma once

#include <iostream>

#include "../../common/constants.h"
#include "../../common/hw.h"
#include "../../data-site/db/partition_metadata.h"
#include "../benchmark_configs.h"
#include "ycsb_record_types.h"

class ycsb_configs {
   public:
    uint64_t          max_key_;
    uint32_t          value_size_;
    uint32_t          partition_size_;
    uint32_t          num_ops_per_transaction_;
    double            zipf_alpha_;

    bool     limit_number_of_records_propagated_;
    uint64_t number_of_updates_needed_for_propagation_;
    double   scan_selectivity_;

    uint32_t                column_partition_size_;
    partition_type::type    partition_type_;
    storage_tier_type::type storage_type_;

    bool scan_restricted_;
    bool rmw_restricted_;

    bool store_scan_results_;

    benchmark_configs bench_configs_;

    std::vector<uint32_t> workload_probs_;
};

ycsb_configs construct_ycsb_configs(
    uint64_t max_key = k_ycsb_max_key, uint32_t value_size = k_ycsb_value_size,
    uint32_t partition_size = k_ycsb_partition_size,
    uint32_t num_ops_per_transaction = k_ycsb_num_ops_per_transaction,
    double   zipf_alpha = k_ycsb_zipf_alpha,
    bool     limit_number_of_records_propagated =
        k_limit_number_of_records_propagated,
    uint64_t number_of_updates_needed_for_propagation =
        k_number_of_updates_needed_for_propagation,
    double   scan_selectivity = k_ycsb_scan_selectivity,
    uint32_t column_partition_size = k_ycsb_column_partition_size,
    const partition_type::type&    p_type = k_ycsb_partition_type,
    const storage_tier_type::type& s_type = k_ycsb_storage_type,
    bool                           scan_restricted = k_ycsb_scan_restricted,
    bool                           rmw_restricted = k_ycsb_rmw_restricted,
    bool                     store_scan_results = k_ycsb_store_scan_results,
    const benchmark_configs& bench_configs = construct_benchmark_configs(),
    uint32_t                 write_prob = k_ycsb_write_prob,
    uint32_t read_prob = k_ycsb_read_prob, uint32_t rmw_prob = k_ycsb_rmw_prob,
    uint32_t scan_prob = k_ycsb_scan_prob,
    uint32_t multi_rmw_prob = k_ycsb_multi_rmw_prob,
    uint32_t db_split_prob = k_db_split_prob,
    uint32_t db_merge_prob = k_db_merge_prob,
    uint32_t db_remaster_prob = k_db_remaster_prob );

std::ostream& operator<<( std::ostream& os, const ycsb_configs& configs );

table_metadata create_ycsb_table_metadata(
    const ycsb_configs& configs = construct_ycsb_configs(),
    int32_t             site = k_site_location_identifier );

