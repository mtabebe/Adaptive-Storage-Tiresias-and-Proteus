#include "ycsb_configs.h"

#include <glog/logging.h>

ycsb_configs construct_ycsb_configs(
    uint64_t max_key, uint32_t value_size, uint32_t partition_size,
    uint32_t num_ops_per_transaction, double zipf_alpha,
    bool     limit_number_of_records_propagated,
    uint64_t number_of_updates_needed_for_propagation, double scan_selectivity,
    uint32_t column_partition_size, const partition_type::type& p_type,
    const storage_tier_type::type& s_type, bool scan_restricted,
    bool rmw_restricted, bool store_scan_results,
    const benchmark_configs& bench_configs, uint32_t write_prob,
    uint32_t read_prob, uint32_t rmw_prob, uint32_t scan_prob,
    uint32_t multi_rmw_prob, uint32_t db_split_prob, uint32_t db_merge_prob,
    uint32_t db_remaster_prob ) {
    ycsb_configs configs;

    configs.max_key_ = max_key;
    configs.value_size_ = value_size;
    configs.partition_size_ = partition_size;
    configs.num_ops_per_transaction_ = num_ops_per_transaction;
    configs.zipf_alpha_ = zipf_alpha;

    configs.limit_number_of_records_propagated_ =
        limit_number_of_records_propagated;
    configs.number_of_updates_needed_for_propagation_ =
        number_of_updates_needed_for_propagation;
    configs.scan_selectivity_ = scan_selectivity;
    DCHECK_GE( configs.scan_selectivity_, 0.0 );
    DCHECK_LE( configs.scan_selectivity_, 1.0 );

    configs.column_partition_size_ = column_partition_size;
    configs.partition_type_ = p_type;
    configs.storage_type_ = s_type;

    configs.scan_restricted_ = scan_restricted;
    configs.rmw_restricted_ = rmw_restricted;

    configs.store_scan_results_ = store_scan_results;

    configs.bench_configs_ = bench_configs;

    configs.workload_probs_ = {write_prob,    read_prob,       rmw_prob,
                               scan_prob,     multi_rmw_prob,  db_split_prob,
                               db_merge_prob, db_remaster_prob};
    uint32_t cum_workload_prob = 0;
    for( uint32_t prob : configs.workload_probs_ ) {
        DCHECK_GE( prob, 0 );
        cum_workload_prob += prob;
    }

    DCHECK_EQ( 100, cum_workload_prob );

    DVLOG( 1 ) << "Created: " << configs;

    return configs;
}

std::ostream& operator<<( std::ostream& os, const ycsb_configs& config ) {
    os << "YCSB Config: [ Max Key:" << config.max_key_
       << ", Value Size:" << config.value_size_
       << ", Partition Size:" << config.partition_size_
       << ", Num Ops Per Transaction:" << config.num_ops_per_transaction_
       << ", Zipf Alpha:" << config.zipf_alpha_
       << ", Limit Number of Records Propagated:"
       << config.limit_number_of_records_propagated_
       << ", Number of Updates Needed For Propagation:"
       << config.number_of_updates_needed_for_propagation_ << ", "
       << "Scan Selectivity:" << config.scan_selectivity_
       << ", Column Partition Size:" << config.column_partition_size_
       << ", Partition Type:" << config.partition_type_
       << ", Storage Tier Type:" << config.storage_type_
       << ", Scan Restricted :" << config.scan_restricted_
       << ", RMW Restricted :" << config.rmw_restricted_
       << ", Store Scan Results:" << config.store_scan_results_
       << config.bench_configs_ << " ]";
    return os;
}

table_metadata create_ycsb_table_metadata( const ycsb_configs& configs,
                                           int32_t             site ) {
    return create_table_metadata(
        "ycsb_table", 0, k_ycsb_num_columns, k_ycsb_col_types,
        k_num_records_in_chain, k_num_records_in_snapshot_chain, site,
        configs.partition_size_, configs.column_partition_size_,
        configs.partition_size_, configs.column_partition_size_,
        configs.partition_type_, configs.storage_type_ );
}
