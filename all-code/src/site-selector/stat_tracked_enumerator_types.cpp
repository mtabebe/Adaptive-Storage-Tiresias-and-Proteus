#include "stat_tracked_enumerator_types.h"

#include <glog/logging.h>

stat_tracked_enumerator_configs construct_stat_tracked_enumerator_configs(
    bool track_stats, double prob_of_returning_default, double min_threshold,
    uint64_t                                    min_count_threshold,
    const std::vector<partition_type::type>&    acceptable_new_partition_types,
    const std::vector<partition_type::type>&    acceptable_partition_types,
    const std::vector<storage_tier_type::type>& acceptable_new_storage_types,
    const std::vector<storage_tier_type::type>& acceptable_storage_types,
    double contention_bucket, double cell_width_bucket,
    uint32_t num_entries_bucket, double num_updates_needed_bucket,
    double scan_selectivity_bucket, uint32_t num_point_reads_bucket,
    uint32_t num_point_updates_bucket ) {
    stat_tracked_enumerator_configs configs;

    configs.track_stats_ = track_stats;
    configs.prob_of_returning_default_ = prob_of_returning_default;
    configs.min_threshold_ = min_threshold;
    configs.min_count_threshold_ = min_count_threshold;

    configs.acceptable_new_partition_types_ = acceptable_new_partition_types;
    configs.acceptable_partition_types_ = acceptable_partition_types;

    configs.acceptable_new_storage_types_ = acceptable_new_storage_types;
    configs.acceptable_storage_types_ = acceptable_storage_types;

    configs.contention_bucket_ = contention_bucket;
    configs.cell_width_bucket_ = {cell_width_bucket};
    configs.num_entries_bucket_ = num_entries_bucket;

    configs.num_updates_needed_bucket_ = num_updates_needed_bucket;
    configs.scan_selectivity_bucket_ = scan_selectivity_bucket;
    configs.num_point_reads_bucket_ = num_point_reads_bucket;
    configs.num_point_updates_bucket_ = num_point_updates_bucket;

    DVLOG( 1 ) << "Constructed configs:" << configs;

    return configs;
}

std::ostream& operator<<( std::ostream&                          os,
                          const stat_tracked_enumerator_configs& configs ) {
    DCHECK_EQ( 1, configs.cell_width_bucket_.size() );

    os << "Stat Tracked Enumerator Configs: [ "
          ", track_stats_:"
       << configs.track_stats_
       << ", prob_of_returning_default_:" << configs.prob_of_returning_default_
       << ", min_threshold_:" << configs.min_threshold_
       << ", min_count_threshold_:" << configs.min_count_threshold_
       << ", acceptable_new_partition_types_: (";
    for( const auto& t : configs.acceptable_new_partition_types_ ) {
        os << t << ", ";
    }
    os << "), acceptable_partition_types_: (";
    for( const auto& t : configs.acceptable_partition_types_ ) {
        os << t << ", ";
    }
    os << "), acceptable_new_storage_types_: (";
    for( const auto& t : configs.acceptable_new_storage_types_ ) {
        os << t << ", ";
    }
    os << "), acceptable_storage_types_: (";
    for( const auto& t : configs.acceptable_storage_types_ ) {
        os << t << ", ";
    }
    os << "), contention_bucket_:" << configs.contention_bucket_
       << ", cell_width_bucket_:" << configs.cell_width_bucket_.at( 0 )
       << ", num_entries_bucket_:" << configs.num_entries_bucket_
       << ", num_updates_needed_bucket_:" << configs.num_updates_needed_bucket_
       << ", scan_selectivity_bucket_:" << configs.scan_selectivity_bucket_
       << ", num_point_reads_bucket_:" << configs.num_point_reads_bucket_
       << ", num_point_updates_bucket_:" << configs.num_point_updates_bucket_;
    os << "]";
    return os;
}

