#pragma once

#include <iostream>

#include "../common/constants.h"
#include "cost_modeller_types.h"
#include "stat_tracked_enumerator.h"

typedef stat_tracked_enumerator<
    add_replica_stats, partition_type_tuple, add_replica_stats_hasher,
    add_replica_stats_equal_functor, partition_type_tuple_hasher,
    partition_type_tuple_equal_functor>
    add_replica_type_stat_tracked_enumerator;
typedef stat_tracked_enumerator_decision_tracker<
    add_replica_stats, partition_type_tuple, add_replica_stats_hasher,
    add_replica_stats_equal_functor, partition_type_tuple_hasher,
    partition_type_tuple_equal_functor>
    add_replica_type_stat_tracked_enumerator_decision_tracker;

typedef stat_tracked_enumerator<merge_stats, partition_type_tuple,
                                merge_stats_hasher, merge_stats_equal_functor,
                                partition_type_tuple_hasher,
                                partition_type_tuple_equal_functor>
    merge_partition_type_stat_tracked_enumerator;
typedef stat_tracked_enumerator_decision_tracker<
    merge_stats, partition_type_tuple, merge_stats_hasher,
    merge_stats_equal_functor, partition_type_tuple_hasher,
    partition_type_tuple_equal_functor>
    merge_partition_type_stat_tracked_enumerator_decision_tracker;

typedef stat_tracked_enumerator<split_stats, split_partition_types,
                                split_stats_hasher, split_stats_equal_functor,
                                split_partition_types_hasher,
                                split_partition_types_equal_functor>
    split_partition_type_stat_tracked_enumerator;
typedef stat_tracked_enumerator_decision_tracker<split_stats, split_partition_types,
                                split_stats_hasher, split_stats_equal_functor,
                                split_partition_types_hasher,
                                split_partition_types_equal_functor>
    split_partition_type_stat_tracked_enumerator_decision_tracker;

typedef stat_tracked_enumerator<
    transaction_prediction_stats, partition_type_tuple,
    transaction_prediction_stats_hasher,
    transaction_prediction_stats_equal_functor, partition_type_tuple_hasher,
    partition_type_tuple_equal_functor>
    new_partition_type_stat_tracked_enumerator;
typedef stat_tracked_enumerator_decision_tracker<
    transaction_prediction_stats, partition_type_tuple,
    transaction_prediction_stats_hasher,
    transaction_prediction_stats_equal_functor, partition_type_tuple_hasher,
    partition_type_tuple_equal_functor>
    new_partition_type_stat_tracked_enumerator_decision_tracker;

typedef stat_tracked_enumerator<
    change_types_stats, partition_type_tuple, change_types_stats_hasher,
    change_types_stats_equal_functor, partition_type_tuple_hasher,
    partition_type_tuple_equal_functor>
    change_partition_type_stat_tracked_enumerator;
typedef stat_tracked_enumerator_decision_tracker<
    change_types_stats, partition_type_tuple, change_types_stats_hasher,
    change_types_stats_equal_functor, partition_type_tuple_hasher,
    partition_type_tuple_equal_functor>
    change_partition_type_stat_tracked_enumerator_decision_tracker;

class stat_tracked_enumerator_configs {
   public:
    bool     track_stats_;
    double   prob_of_returning_default_;
    double   min_threshold_;
    uint64_t min_count_threshold_;

    std::vector<partition_type::type> acceptable_new_partition_types_;
    std::vector<partition_type::type> acceptable_partition_types_;

    std::vector<storage_tier_type::type> acceptable_new_storage_types_;
    std::vector<storage_tier_type::type> acceptable_storage_types_;

    double              contention_bucket_;
    std::vector<double> cell_width_bucket_;
    uint32_t            num_entries_bucket_;

    double   num_updates_needed_bucket_;
    double   scan_selectivity_bucket_;
    uint32_t num_point_reads_bucket_;
    uint32_t num_point_updates_bucket_;
};

stat_tracked_enumerator_configs construct_stat_tracked_enumerator_configs(
    bool     track_stats = k_ss_enum_enable_track_stats,
    double   prob_of_returning_default = k_ss_enum_prob_of_returning_default,
    double   min_threshold = k_ss_enum_min_threshold,
    uint64_t min_count_threshold = k_ss_enum_min_count_threshold,
    const std::vector<partition_type::type>& acceptable_new_partition_types =
        k_ss_acceptable_partition_types,
    const std::vector<partition_type::type>& acceptable_partition_types =
        k_ss_acceptable_partition_types,
    const std::vector<storage_tier_type::type>& acceptable_new_storage_types =
        k_ss_acceptable_storage_types,
    const std::vector<storage_tier_type::type>& acceptable_storage_types =
        k_ss_acceptable_storage_types,
    double   contention_bucket = k_ss_enum_contention_bucket,
    double   cell_width_bucket = k_ss_enum_cell_width_bucket,
    uint32_t num_entries_bucket = k_ss_enum_num_entries_bucket,
    double   num_updates_needed_bucket = k_ss_enum_num_updates_needed_bucket,
    double   scan_selectivity_bucket = k_ss_enum_scan_selectivity_bucket,
    uint32_t num_point_reads_bucket = k_ss_enum_num_point_reads_bucket,
    uint32_t num_point_updates_bucket = k_ss_enum_num_point_updates_bucket );

std::ostream& operator<<( std::ostream&                          os,
                          const stat_tracked_enumerator_configs& configs );

