#pragma once

#include <folly/Hash.h>
#include <unordered_map>

#include "site_evaluator.h"
#include "site_selector_query_stats.h"
#include "stat_tracked_enumerator_holder.h"

// this is a filthy hack
class split_plan_state {
   public:
    split_plan_state();

    bool                         splittable_;
    cell_key                     split_point_;
    bool                         is_vertical_;
    partition_type::type         low_type_;
    partition_type::type         high_type_;
    storage_tier_type::type      low_storage_type_;
    storage_tier_type::type      high_storage_type_;
    double                       cost_;
    cost_model_prediction_holder upfront_cost_prediction_;

    split_stats split_stats_;
};

class shared_split_plan_state {
   public:
    shared_split_plan_state();

    std::shared_ptr<partition_payload>              partition_;
    std::shared_ptr<partition_location_information> location_information_;

    split_plan_state col_split_;
    split_plan_state row_split_;
};

class shared_merge_plan_state {
   public:
    shared_merge_plan_state();

    bool                         mergeable_;
    cell_key                     other_ck_;
    bool                         is_vertical_;
    partition_column_identifier  other_part_;
    partition_type::type         merge_type_;
    storage_tier_type::type      merge_storage_type_;
    double                       cost_;
    cost_model_prediction_holder upfront_cost_prediction_;

    merge_stats merge_stats_;
};

class shared_left_right_merge_plan_state {
   public:
    shared_left_right_merge_plan_state();

    std::shared_ptr<partition_payload>              partition_;
    std::shared_ptr<partition_location_information> location_information_;

    shared_merge_plan_state merge_left_row_;
    shared_merge_plan_state merge_right_row_;
    shared_merge_plan_state merge_left_col_;
    shared_merge_plan_state merge_right_col_;
};

class partitioning_cost_benefit {
   public:
    partitioning_cost_benefit();

    double get_split_benefit() const;
    double get_merge_benefit() const;

    double combined_benefit_;
    double split_benefit_;
};

class plan_for_site_args {
   public:
    plan_for_site_args(
        const std::vector<std::shared_ptr<partition_payload>> &new_partitions,
        const std::vector<std::shared_ptr<partition_payload>> &write_partitions,
        const std::vector<std::shared_ptr<partition_payload>> &read_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_to_ckrs &write_pids_to_ckrs,
        const partition_column_identifier_to_ckrs &read_pids_to_ckrs,
        const std::vector<site_load_information> & site_load_infos,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                                    storage_sizes,
        const snapshot_vector &                    session );

    const std::vector<std::shared_ptr<partition_payload>> &new_partitions_;
    const std::vector<std::shared_ptr<partition_payload>> &write_partitions_;
    const std::vector<std::shared_ptr<partition_payload>> &read_partitions_;
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &    partition_location_informations_;
    const partition_column_identifier_to_ckrs &write_pids_to_ckrs_;
    const partition_column_identifier_to_ckrs &read_pids_to_ckrs_;
    const std::vector<site_load_information> &site_load_infos_;
    const std::unordered_map<storage_tier_type::type, std::vector<double>>
        &                  site_storage_sizes_;
    const snapshot_vector &session_;

    double average_load_per_site_;
    double site_storage_sizes_cost_;

    double                                          avg_write_count_;
    double                                          std_dev_write_count_;
    std::vector<shared_split_plan_state>            partitions_to_split_;
    std::vector<shared_left_right_merge_plan_state> partitions_to_merge_;
    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
                                              other_write_locked_partitions_;
    partition_column_identifier_unordered_set original_write_partition_set_;
    partition_column_identifier_unordered_set original_read_partition_set_;
    partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>>
        sampled_partition_accesses_index_by_write_partition_;
    partition_column_identifier_map_t<
        std::vector<transaction_partition_accesses>>
        sampled_partition_accesses_index_by_read_partition_;

    cached_ss_stat_holder      cached_stats_;
    query_arrival_cached_stats cached_query_arrival_stats_;
};

enum future_change_state_type {
    BOTH_REQUIRE_CHANGE,
    NEITHER_REQUIRE_CHANGE,
    PREVIOUSLY_REQUIRED_CHANGE,
    NOW_REQUIRE_CHANGE,
};

std::string future_change_state_type_to_string(
    const future_change_state_type &change_type );

class master_destination_dependency_version {
   public:
    master_destination_dependency_version( int master, int destination,
                                           int version );

    int master_;
    int destination_;
    int version_;
};

class partitions_entry {
   public:
    partitions_entry();

    void add( const partition_column_identifier & pid,
              const std::shared_ptr<partition_payload> &payload,
              const std::shared_ptr<partition_location_information>
                  &part_location_information );
    void add( const partition_column_identifier &       pid,
              const std::shared_ptr<partition_payload> &payload,
              const std::shared_ptr<partition_location_information>
                  &                         part_location_information,
              const partition_type::type    part_type,
              const storage_tier_type::type storage_type );

    partition_column_identifier_unordered_set       seen_pids_;

    std::vector<partition_column_identifier>        pids_;
    std::vector<std::shared_ptr<partition_payload>> payloads_;
    std::vector<std::shared_ptr<partition_location_information>>
                                         location_infos_;
    std::vector<partition_type::type>    partition_types_;
    std::vector<storage_tier_type::type> storage_types_;
};

struct master_destination_dependency_version_equal_functor {
    bool operator()(
        const ::master_destination_dependency_version &md1,
        const ::master_destination_dependency_version &md2 ) const {
        return ( ( md1.master_ == md2.master_ ) &&
                 ( md1.destination_ == md2.destination_ ) &&
                 ( md1.version_ == md2.version_ ) );
    }
};

struct master_destination_dependency_version_key_hasher {
    std::size_t operator()(
        const ::master_destination_dependency_version &md ) const {
        std::size_t seed = 0;
        seed = folly::hash::hash_combine( seed, md.master_ );
        seed = folly::hash::hash_combine( seed, md.destination_ );
        seed = folly::hash::hash_combine( seed, md.version_ );
        return seed;
    }
};

typedef std::unordered_map<master_destination_dependency_version,
                           partitions_entry,
                           master_destination_dependency_version_key_hasher,
                           master_destination_dependency_version_equal_functor>
    master_destination_partition_entries;

class change_scan_plan {
  public:
    change_scan_plan();

    double cost_;
    int destination_;

    bool add_replica_;
    bool change_type_;

    partition_type::type part_type_;
    storage_tier_type::type storage_type_;
};
