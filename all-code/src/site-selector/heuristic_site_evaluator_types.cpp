#include "heuristic_site_evaluator_types.h"

#include "../data-site/db/cell_identifier.h"

plan_for_site_args::plan_for_site_args(
    const std::vector<std::shared_ptr<partition_payload>> &new_partitions,
    const std::vector<std::shared_ptr<partition_payload>> &write_partitions,
    const std::vector<std::shared_ptr<partition_payload>> &read_partitions,
    partition_column_identifier_map_t<std::shared_ptr<
        partition_location_information>> &     partition_location_informations,
    const partition_column_identifier_to_ckrs &write_pids_to_ckrs,
    const partition_column_identifier_to_ckrs &read_pids_to_ckrs,
    const std::vector<site_load_information> & site_load_infos,
    const std::unordered_map<storage_tier_type::type, std::vector<double>>
        &                  storage_sizes,
    const snapshot_vector &session )
    : new_partitions_( new_partitions ),
      write_partitions_( write_partitions ),
      read_partitions_( read_partitions ),
      partition_location_informations_( partition_location_informations ),
      write_pids_to_ckrs_( write_pids_to_ckrs ),
      read_pids_to_ckrs_( read_pids_to_ckrs ),
      site_load_infos_( site_load_infos ),
      site_storage_sizes_( storage_sizes ),
      session_( session ),
      average_load_per_site_( 0 ),
      site_storage_sizes_cost_( 0 ),
      avg_write_count_( 0 ),
      std_dev_write_count_( 0 ),
      partitions_to_split_(),
      partitions_to_merge_(),
      other_write_locked_partitions_(),
      original_write_partition_set_(),
      original_read_partition_set_(),
      sampled_partition_accesses_index_by_write_partition_(),
      sampled_partition_accesses_index_by_read_partition_(),
      cached_stats_(),
      cached_query_arrival_stats_() {}

split_plan_state::split_plan_state()
    : splittable_( false ),
      split_point_(),
      is_vertical_( false ),
      low_type_( partition_type::type::ROW ),
      high_type_( partition_type::type::ROW ),
      low_storage_type_( storage_tier_type::type::MEMORY ),
      high_storage_type_( storage_tier_type::type::MEMORY ),
      cost_( 0 ),
      upfront_cost_prediction_(),
      split_stats_() {
    split_point_.table_id = 0;
    split_point_.row_id = k_unassigned_key;
    split_point_.col_id = k_unassigned_col;
}

shared_split_plan_state::shared_split_plan_state()
    : partition_( nullptr ),
      location_information_( nullptr ),
      col_split_(),
      row_split_() {}

shared_merge_plan_state::shared_merge_plan_state()
    : mergeable_( false ),
      other_ck_(),
      is_vertical_( false ),
      other_part_(),
      merge_type_( partition_type::type::ROW ),
      merge_storage_type_( storage_tier_type::type::MEMORY ),
      cost_( 0 ),
      upfront_cost_prediction_(),
      merge_stats_() {
    other_ck_.table_id = 0;
    other_ck_.col_id = k_unassigned_col;
    other_ck_.row_id = k_unassigned_key;
}

shared_left_right_merge_plan_state::shared_left_right_merge_plan_state()
    : partition_( nullptr ),
      location_information_( nullptr ),
      merge_left_row_(),
      merge_right_row_(),
      merge_left_col_(),
      merge_right_col_() {}

partitioning_cost_benefit::partitioning_cost_benefit()
    : combined_benefit_( 0 ), split_benefit_( 0 ) {}
double partitioning_cost_benefit::get_split_benefit() const {
    return ( combined_benefit_ - split_benefit_ );
}
double partitioning_cost_benefit::get_merge_benefit() const {
    return ( split_benefit_ - combined_benefit_ );
}

std::string future_change_state_type_to_string(
    const future_change_state_type &change_type ) {
    switch( change_type ) {
        case BOTH_REQUIRE_CHANGE:
            return "both_require_change";
        case NEITHER_REQUIRE_CHANGE:
            return "neither_require_change";
        case PREVIOUSLY_REQUIRED_CHANGE:
            return "previously_required_change";
        case NOW_REQUIRE_CHANGE:
            return "now_require_change";
    }

    return "";
}

master_destination_dependency_version::master_destination_dependency_version(
    int master, int destination, int version )
    : master_( master ), destination_( destination ), version_( version ) {}

partitions_entry::partitions_entry()
    : seen_pids_(),
      pids_(),
      payloads_(),
      location_infos_(),
      partition_types_(),
      storage_types_() {}

void partitions_entry::add( const partition_column_identifier & pid,
                            const std::shared_ptr<partition_payload> &payload,
                            const std::shared_ptr<partition_location_information>
                                &part_location_information ) {
    if( seen_pids_.count( pid ) == 1 ) {
        return;
    }
    seen_pids_.insert( pid );
    pids_.emplace_back( pid );
    payloads_.emplace_back( payload );
    location_infos_.emplace_back( part_location_information );
}

void partitions_entry::add(
    const partition_column_identifier &              pid,
    const std::shared_ptr<partition_payload> &             payload,
    const std::shared_ptr<partition_location_information> &part_location_information,
    const partition_type::type                       part_type,
    const storage_tier_type::type                    storage_type ) {
    if( seen_pids_.count( pid ) == 1 ) {
        return;
    }
    seen_pids_.insert( pid );
    pids_.emplace_back( pid );
    payloads_.emplace_back( payload );
    location_infos_.emplace_back( part_location_information );
    partition_types_.emplace_back( part_type );
    storage_types_.emplace_back( storage_type );
}

change_scan_plan::change_scan_plan()
    : cost_( 0 ),
      destination_( 0 ),
      add_replica_( false ),
      change_type_( false ),
      part_type_( partition_type::type::ROW ),
      storage_type_( storage_tier_type::type::MEMORY ) {}
