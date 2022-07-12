#pragma once

#include <iostream>

#include "../common/constants.h"
#include "cost_modeller_types.h"
#include "partition_lock_mode.h"

static const int K_DATA_AT_ALL_SITES = INT_MAX;

class periodic_site_selector_operations_configs {
   public:
    uint32_t num_poller_threads_;
    uint32_t update_stats_sleep_delay_ms_;
};

class transaction_preparer_configs {
   public:
    uint32_t num_worker_threads_per_client_;
    uint32_t num_clients_;

    uint32_t astream_mod_;
    uint32_t astream_empty_wait_ms_;
    uint32_t astream_tracking_interval_ms_;
    uint32_t astream_max_queue_size_;

    double max_write_blend_rate_;

    uint32_t background_executor_cid_;
    uint32_t periodic_operations_cid_;

    bool     run_background_workers_;
    uint32_t num_background_work_items_to_execute_;

    ss_mastering_type ss_mastering_type_;

    uint32_t estore_clay_periodic_interval_ms_;
    double   estore_load_epsilon_threshold_;
    double   estore_hot_partition_pct_thresholds_;
    double   adr_multiplier_;

    bool enable_plan_reuse_;
    bool enable_wait_for_plan_reuse_;

    partition_lock_mode optimistic_lock_mode_;
    partition_lock_mode physical_adjustment_lock_mode_;
    partition_lock_mode force_change_lock_mode_;

    periodic_site_selector_operations_configs
        periodic_site_selector_operations_configs_;
};

class heuristic_site_evaluator_configs {
   public:
    int      num_sites_;
    uint32_t num_clients_;

    double   split_partition_ratio_;
    double   merge_partition_ratio_;

    double   upfront_cost_weight_;
    double   scan_upfront_cost_weight_;
    double   horizon_weight_;
    double   default_horizon_benefit_weight_;
    double   repartition_remaster_horizon_weight_;
    double   horizon_storage_weight_;

    double repartition_prob_rescale_;
    double outlier_repartition_std_dev_;
    double split_partition_size_ratio_;

    double plan_site_load_balance_weight_;

    double plan_site_storage_balance_weight_;
    double plan_site_storage_overlimit_weight_;
    double plan_site_storage_variance_weight_;
    std::unordered_map<storage_tier_type::type, double>
        plan_site_storage_overlimit_weights_;
    std::unordered_map<storage_tier_type::type, double>
        plan_site_storage_variance_weights_;

    bool   plan_scan_in_cost_order_;
    bool   plan_scan_limit_changes_;
    double plan_scan_limit_ratio_;

    uint32_t num_update_destinations_per_site_;
    uint32_t num_samples_for_client_;
    uint32_t num_samples_from_other_clients_;
    bool     allow_force_site_selector_changes_;
    size_t   sample_reservoir_size_;
    float    sample_reservoir_initial_multiplier_;
    float    sample_reservoir_weight_increment_;
    float    sample_reservoir_decay_rate_;
    size_t   force_change_reservoir_size_;
    float    force_change_reservoir_initial_multiplier_;
    float    force_change_reservoir_weight_increment_;
    float    force_change_reservoir_decay_rate_;

    double    storage_removal_threshold_;
    uint32_t  num_remove_replicas_iteration_;

    bool allow_vertical_partitioning_;
    bool allow_horizontal_partitioning_;
    bool allow_change_partition_types_;
    bool allow_change_storage_types_;

    std::vector<partition_type::type> acceptable_new_partition_types_;
    std::vector<partition_type::type> acceptable_partition_types_;

    std::vector<storage_tier_type::type> acceptable_new_storage_types_;
    std::vector<storage_tier_type::type> acceptable_storage_types_;

    bool   enable_plan_reuse_;
    double plan_reuse_threshold_;
    bool   enable_wait_for_plan_reuse_;

    bool   use_query_arrival_predictor_;
    double arrival_time_score_scalar_;
    double arrival_time_no_prediction_default_score_;

    uint32_t astream_mod_;

    ss_mastering_type ss_mastering_type_;
};

periodic_site_selector_operations_configs
    construct_periodic_site_selector_operations_configs(
        uint32_t num_poller_threads = k_ss_num_poller_threads,
        uint32_t update_stats_sleep_delay_ms =
            k_ss_update_stats_sleep_delay_ms );

class partition_data_location_table_configs {
   public:
    uint32_t num_sites_;
    std::unordered_map<storage_tier_type::type, double> storage_tier_limits_;
    transaction_prediction_stats partition_tracking_bucket_;
};

partition_data_location_table_configs
    construct_partition_data_location_table_configs(
        uint32_t                                    num_sites,
        const std::vector<storage_tier_type::type>& acceptable_storage_types =
            k_ss_acceptable_storage_types,
        double   memory_tier_limit = k_memory_tier_site_space_limit,
        double   disk_tier_limit = k_disk_tier_site_space_limit,
        double   contention_bucket = k_ss_enum_contention_bucket,
        double   cell_width_bucket = k_ss_enum_cell_width_bucket,
        uint32_t num_entries_bucket = k_ss_enum_num_entries_bucket,
        double num_updates_needed_bucket = k_ss_enum_num_updates_needed_bucket,
        double scan_selectivity_bucket = k_ss_enum_scan_selectivity_bucket,
        uint32_t num_point_reads_bucket = k_ss_enum_num_point_reads_bucket,
        uint32_t num_point_updates_bucket =
            k_ss_enum_num_point_updates_bucket );

transaction_preparer_configs construct_transaction_preparer_configs(
    uint32_t num_worker_threads_per_client =
        k_num_site_selector_worker_threads_per_client,
    uint32_t num_clients = k_bench_num_clients,
    uint32_t astream_mod = k_astream_mod,
    uint32_t astream_empty_wait_ms = k_astream_empty_wait_ms,
    uint32_t astream_max_queue_size = k_astream_max_queue_size,
    uint32_t astream_tracking_interval_ms = k_astream_tracking_interval_ms,
    double   max_write_blend_rate = k_ss_max_write_blend_rate,
    uint32_t background_executor_cid = k_bench_num_clients - 2,
    uint32_t periodic_operations_cid = k_bench_num_clients - 1,
    bool     run_background_workers = k_ss_run_background_workers,
    uint32_t num_background_work_items_to_execute =
        k_ss_num_background_work_items_to_execute,
    const ss_mastering_type& ss_master_type = k_ss_mastering_type,
    uint32_t                 estore_clay_periodic_interval_ms =
        k_estore_clay_periodic_interval_ms,
    double estore_load_epsilon_threshold = k_estore_load_epsilon_threshold,
    double estore_hot_partition_pct_thresholds =
        k_estore_hot_partition_pct_thresholds,
    double adr_multiplier = k_adr_multiplier,
    bool   enable_plan_reuse = k_ss_enable_plan_reuse,
    bool   enable_wait_for_plan_reuse = k_ss_enable_wait_for_plan_reuse,
    const partition_lock_mode& optimistic_lock_mode = k_ss_optimistic_lock_mode,
    const partition_lock_mode& physical_adjustment_lock_mode =
        k_ss_physical_adjustment_lock_mode,
    const partition_lock_mode& force_change_lock_mode =
        k_ss_force_change_lock_mode,
    const periodic_site_selector_operations_configs& poller_configs =
        construct_periodic_site_selector_operations_configs() );

heuristic_site_evaluator_configs construct_heuristic_site_evaluator_configs(
    int num_sites, uint32_t num_clients = k_bench_num_clients,
    double split_partition_ratio = k_ss_split_partition_ratio,
    double merge_partition_ratio = k_ss_merge_partition_ratio,
    double upfront_cost_weight = k_ss_upfront_cost_weight,
    double scan_upfront_cost_weight = k_ss_scan_upfront_cost_weight,
    double horizon_weight = k_ss_horizon_weight,
    double default_horizon_benefit_weight = k_ss_default_horizon_benefit_weight,
    double repartition_remaster_horizon_weight =
        k_ss_repartition_remaster_horizon_weight,
    double horizon_storage_weight = k_ss_horizon_storage_weight,
    double repartition_prob_rescale = k_ss_repartition_prob_rescale,
    double outlier_repartition_std_dev = k_ss_outlier_repartition_std_dev,
    double split_partition_size_ratio = k_ss_split_partition_size_ratio,
    double plan_site_load_balance_weight = k_ss_plan_site_load_balance_weight,
    double plan_site_storage_balance_weight =
        k_ss_plan_site_storage_balance_weight,
    double plan_site_storage_overlimit_weight =
        k_ss_plan_site_storage_overlimit_weight,
    double plan_site_storage_variance_weight =
        k_ss_plan_site_storage_variance_weight,
    double plan_site_memory_overlimit_weight =
        k_ss_plan_site_memory_overlimit_weight,
    double plan_site_disk_overlimit_weight =
        k_ss_plan_site_disk_overlimit_weight,
    double plan_site_memory_variance_weight =
        k_ss_plan_site_memory_variance_weight,
    double plan_site_disk_variance_weight = k_ss_plan_site_disk_variance_weight,
    bool   plan_scan_in_cost_order = k_ss_plan_scan_in_cost_order,
    bool   plan_scan_limit_changes = k_ss_plan_scan_limit_changes,
    double plan_scan_limit_ratio = k_ss_plan_scan_limit_ratio,
    uint32_t num_update_destinations_per_site = k_num_update_destinations,
    uint32_t num_samples_for_client = k_ss_num_sampled_transactions_for_client,
    uint32_t num_samples_from_other_clients =
        k_ss_num_sampled_transactions_from_other_clients,
    bool allow_force_site_selector_changes =
        k_ss_allow_force_site_selector_changes,
    size_t sample_reservoir_size = k_sample_reservoir_size_per_client,
    float  sample_reservoir_initial_multiplier =
        k_sample_reservoir_initial_multiplier,
    float sample_reservoir_weight_increment =
        k_sample_reservoir_weight_increment,
    float  sample_reservoir_decay_rate = k_sample_reservoir_decay_rate,
    size_t force_change_reservoir_size =
        k_force_change_reservoir_size_per_client,
    float force_change_reservoir_initial_multiplier =
        k_force_change_reservoir_initial_multiplier,
    float force_change_reservoir_weight_increment =
        k_force_change_reservoir_weight_increment,
    float force_change_reservoir_decay_rate =
        k_force_change_reservoir_decay_rate,
    double   storage_removal_threshold = k_ss_storage_removal_threshold,
    uint32_t num_remove_replicas_iteration = k_ss_num_remove_replicas_iteration,
    bool     allow_vertical_partitioning = k_ss_allow_vertical_partitioning,
    bool     allow_horizontal_partitioning = k_ss_allow_horizontal_partitioning,
    bool     allow_change_partition_types = k_ss_allow_change_partition_types,
    bool     allow_change_storage_types = k_ss_allow_change_storage_types,
    const std::vector<partition_type::type>& acceptable_new_partition_types =
        k_ss_acceptable_new_partition_types,
    const std::vector<partition_type::type>& acceptable_partition_types =
        k_ss_acceptable_partition_types,
    const std::vector<storage_tier_type::type>& acceptable_new_storage_types =
        k_ss_acceptable_new_storage_types,
    const std::vector<storage_tier_type::type>& acceptable_storage_types =
        k_ss_acceptable_storage_types,
    bool   enable_plan_reuse = k_ss_enable_plan_reuse,
    double plan_reuse_threshold = k_ss_plan_reuse_threshold,
    bool   enable_wait_for_plan_reuse = k_ss_enable_wait_for_plan_reuse,
    bool   use_query_arrival_predictor = k_use_query_arrival_predictor,
    double arrival_time_score_scalar = k_query_arrival_time_score_scalar,
    double arrival_time_no_prediction_default_score =
        k_query_arrival_default_score,
    uint32_t                 astream_mod = k_astream_mod,
    const ss_mastering_type& ss_master_type = k_ss_mastering_type );

std::ostream& operator<<(
    std::ostream&                                    os,
    const periodic_site_selector_operations_configs& configs );

std::ostream& operator<<( std::ostream&                       os,
                          const transaction_preparer_configs& configs );
std::ostream& operator<<( std::ostream&                           os,
                          const heuristic_site_evaluator_configs& configs );
std::ostream& operator<<(
    std::ostream& os, const partition_data_location_table_configs& configs );

