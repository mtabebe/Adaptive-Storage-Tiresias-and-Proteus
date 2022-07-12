#include "site_selector_metadata.h"

#include <glog/logging.h>

periodic_site_selector_operations_configs construct_periodic_site_selector_operations_configs(
    uint32_t num_poller_threads, uint32_t update_stats_sleep_delay_ms ) {
    periodic_site_selector_operations_configs configs;
    configs.num_poller_threads_ = num_poller_threads;
    configs.update_stats_sleep_delay_ms_ = update_stats_sleep_delay_ms;
    return configs;
}

partition_data_location_table_configs
    construct_partition_data_location_table_configs(
        uint32_t                                    num_sites,
        const std::vector<storage_tier_type::type>& acceptable_storage_types,
        double memory_tier_limit, double disk_tier_limit,
        double contention_bucket, double cell_width_bucket,
        uint32_t num_entries_bucket, double num_updates_needed_bucket,
        double scan_selectivity_bucket, uint32_t num_point_reads_bucket,
        uint32_t num_point_updates_bucket ) {
    partition_data_location_table_configs configs;
    configs.num_sites_ = num_sites;

    for( const auto& type : acceptable_storage_types ) {
        double limit = 0;
        switch( type ) {
            case( storage_tier_type::type::DISK ): {
                limit = disk_tier_limit;
                break;
            }
            case( storage_tier_type::type::MEMORY ): {
                limit = memory_tier_limit;
                break;
            }
        }
        configs.storage_tier_limits_.emplace( type, limit );
    }

    configs.partition_tracking_bucket_ = transaction_prediction_stats(
        partition_type::type::ROW, storage_tier_type::type::MEMORY,
        num_updates_needed_bucket, contention_bucket, {cell_width_bucket},
        num_entries_bucket, true /* is scan */, scan_selectivity_bucket,
        true /* is point read */, num_point_reads_bucket,
        true /* is point update */, num_point_updates_bucket );

    return configs;
}

transaction_preparer_configs construct_transaction_preparer_configs(
    uint32_t num_worker_threads_per_client, uint32_t num_clients,
    uint32_t astream_mod, uint32_t astream_empty_wait_ms,
    uint32_t astream_tracking_interval_ms, uint32_t astream_max_queue_size,
    double max_write_blend_rate, uint32_t background_executor_cid,
    uint32_t periodic_operations_cid, bool run_background_workers,
    uint32_t                 num_background_work_items_to_execute,
    const ss_mastering_type& ss_master_type,
    uint32_t                 estore_clay_periodic_interval_ms,
    double                   estore_load_epsilon_threshold,
    double estore_hot_partition_pct_thresholds, double adr_multiplier,
    bool enable_plan_reuse, bool enable_wait_for_plan_reuse,
    const partition_lock_mode& optimistic_lock_mode,
    const partition_lock_mode& physical_adjustment_lock_mode,
    const partition_lock_mode& force_change_lock_mode,
    const periodic_site_selector_operations_configs& poller_configs ) {
    transaction_preparer_configs configs;

    configs.num_worker_threads_per_client_ = num_worker_threads_per_client;
    configs.num_clients_ = num_clients;

    configs.astream_mod_ = astream_mod;
    configs.astream_empty_wait_ms_ = astream_empty_wait_ms;
    configs.astream_tracking_interval_ms_ = astream_tracking_interval_ms;
    configs.astream_max_queue_size_ = astream_max_queue_size;

    configs.max_write_blend_rate_ = max_write_blend_rate;

    configs.background_executor_cid_ = background_executor_cid;
    configs.periodic_operations_cid_ = periodic_operations_cid;

    configs.run_background_workers_ = run_background_workers;
    configs.num_background_work_items_to_execute_ =
        num_background_work_items_to_execute;

    configs.ss_mastering_type_ = ss_master_type;

    configs.estore_clay_periodic_interval_ms_ =
        estore_clay_periodic_interval_ms;
    configs.estore_load_epsilon_threshold_ = estore_load_epsilon_threshold;
    configs.estore_hot_partition_pct_thresholds_ =
        estore_hot_partition_pct_thresholds;
    configs.adr_multiplier_ = adr_multiplier;

    configs.enable_plan_reuse_ = enable_plan_reuse;
    configs.enable_wait_for_plan_reuse_ = enable_wait_for_plan_reuse;

    configs.optimistic_lock_mode_ = optimistic_lock_mode;
    configs.physical_adjustment_lock_mode_ = physical_adjustment_lock_mode;
    configs.force_change_lock_mode_ = force_change_lock_mode;

    DCHECK( ( configs.physical_adjustment_lock_mode_ ==
              partition_lock_mode::try_lock ) or
            ( configs.physical_adjustment_lock_mode_ ==
              partition_lock_mode::lock ) );
    DCHECK(
        ( configs.force_change_lock_mode_ == partition_lock_mode::try_lock ) or
        ( configs.force_change_lock_mode_ == partition_lock_mode::lock ) );

    configs.periodic_site_selector_operations_configs_ = poller_configs;
    return configs;
}

heuristic_site_evaluator_configs construct_heuristic_site_evaluator_configs(
    int num_sites, uint32_t num_clients, double split_partition_ratio,
    double merge_partition_ratio, double upfront_cost_weight,
    double scan_upfront_cost_weight, double horizon_weight,
    double default_horizon_benefit_weight,
    double repartition_remaster_horizon_weight, double horizon_storage_weight,
    double repartition_prob_rescale, double outlier_repartition_std_dev,
    double split_partition_size_ratio, double plan_site_load_balance_weight,
    double plan_site_storage_balance_weight,
    double plan_site_storage_overlimit_weight,
    double plan_site_storage_variance_weight,
    double plan_site_memory_overlimit_weight,
    double plan_site_disk_overlimit_weight,
    double plan_site_memory_variance_weight,
    double plan_site_disk_variance_weight, bool plan_scan_in_cost_order,
    bool plan_scan_limit_changes, double plan_scan_limit_ratio,
    uint32_t num_update_destinations_per_site, uint32_t num_samples_for_client,
    uint32_t num_samples_from_other_clients,
    bool allow_force_site_selector_changes, size_t sample_reservoir_size,
    float sample_reservoir_initial_multiplier,
    float sample_reservoir_weight_increment, float sample_reservoir_decay_rate,
    size_t force_change_reservoir_size,
    float  force_change_reservoir_initial_multiplier,
    float  force_change_reservoir_weight_increment,
    float force_change_reservoir_decay_rate, double storage_removal_threshold,
    uint32_t num_remove_replicas_iteration, bool allow_vertical_partitioning,
    bool allow_horizontal_partitioning, bool allow_change_partition_types,
    bool                                        allow_change_storage_types,
    const std::vector<partition_type::type>&    acceptable_new_partition_types,
    const std::vector<partition_type::type>&    acceptable_partition_types,
    const std::vector<storage_tier_type::type>& acceptable_new_storage_types,
    const std::vector<storage_tier_type::type>& acceptable_storage_types,
    bool enable_plan_reuse, double plan_reuse_threshold,
    bool enable_wait_for_plan_reuse, bool use_query_arrival_predictor,
    double arrival_time_score_scalar,
    double arrival_time_no_prediction_default_score, uint32_t astream_mod,
    const ss_mastering_type& ss_master_type ) {

    heuristic_site_evaluator_configs configs;
    configs.num_sites_ = num_sites;
    configs.num_clients_ = num_clients;

    configs.split_partition_ratio_ = split_partition_ratio;
    configs.merge_partition_ratio_ = merge_partition_ratio;

    configs.upfront_cost_weight_ = upfront_cost_weight;
    configs.scan_upfront_cost_weight_ = scan_upfront_cost_weight;
    configs.horizon_weight_ = horizon_weight;
    configs.default_horizon_benefit_weight_ = default_horizon_benefit_weight;
    configs.repartition_remaster_horizon_weight_ =
        repartition_remaster_horizon_weight;
    configs.horizon_storage_weight_ = horizon_storage_weight;

    configs.repartition_prob_rescale_ = repartition_prob_rescale;
    configs.outlier_repartition_std_dev_ = outlier_repartition_std_dev;
    configs.split_partition_size_ratio_ = split_partition_size_ratio;

    configs.plan_site_load_balance_weight_ = plan_site_load_balance_weight;

    configs.plan_site_storage_balance_weight_ =
        plan_site_storage_balance_weight;
    configs.plan_site_storage_overlimit_weight_ =
        plan_site_storage_overlimit_weight;
    configs.plan_site_storage_variance_weight_ =
        plan_site_storage_variance_weight;

    for( const auto& tier : acceptable_storage_types ) {
        double limit_weight = 0;
        double variance_weight = 0;
        switch( tier ) {
            case( storage_tier_type::type::DISK ): {
                limit_weight = plan_site_disk_overlimit_weight;
                variance_weight = plan_site_disk_variance_weight;
                break;
            }
            case( storage_tier_type::type::MEMORY ): {
                limit_weight = plan_site_memory_overlimit_weight;
                variance_weight = plan_site_memory_variance_weight;
                break;
            }
        }
        configs.plan_site_storage_overlimit_weights_[tier] = limit_weight;
        configs.plan_site_storage_variance_weights_[tier] = variance_weight;
    }

    configs.plan_scan_in_cost_order_ = plan_scan_in_cost_order;
    configs.plan_scan_limit_changes_ = plan_scan_limit_changes;
    configs.plan_scan_limit_ratio_ = plan_scan_limit_ratio;

    configs.num_update_destinations_per_site_ =
        num_update_destinations_per_site;
    configs.num_samples_for_client_ = num_samples_for_client;
    configs.num_samples_from_other_clients_ = num_samples_from_other_clients;

    configs.allow_force_site_selector_changes_ =
        allow_force_site_selector_changes;

    configs.sample_reservoir_size_ = sample_reservoir_size;
    configs.sample_reservoir_initial_multiplier_ =
        sample_reservoir_initial_multiplier;
    configs.sample_reservoir_weight_increment_ =
        sample_reservoir_weight_increment;
    configs.sample_reservoir_decay_rate_ = sample_reservoir_decay_rate;

    configs.force_change_reservoir_size_ = force_change_reservoir_size;
    configs.force_change_reservoir_initial_multiplier_ =
        force_change_reservoir_initial_multiplier;
    configs.force_change_reservoir_weight_increment_ =
        force_change_reservoir_weight_increment;
    configs.force_change_reservoir_decay_rate_ =
        force_change_reservoir_decay_rate;

    configs.storage_removal_threshold_ = storage_removal_threshold;
    configs.num_remove_replicas_iteration_ = num_remove_replicas_iteration;

    configs.allow_vertical_partitioning_ = allow_vertical_partitioning;
    configs.allow_horizontal_partitioning_ = allow_horizontal_partitioning;
    configs.allow_change_partition_types_ = allow_change_partition_types;
    configs.allow_change_storage_types_ = allow_change_storage_types;

    configs.acceptable_new_partition_types_ = acceptable_new_partition_types;
    configs.acceptable_partition_types_ = acceptable_partition_types;
    configs.acceptable_new_storage_types_ = acceptable_new_storage_types;
    configs.acceptable_storage_types_ = acceptable_storage_types;

    configs.enable_plan_reuse_ = enable_plan_reuse;
    configs.plan_reuse_threshold_ = plan_reuse_threshold;
    configs.enable_wait_for_plan_reuse_ = enable_wait_for_plan_reuse;

    configs.use_query_arrival_predictor_ = use_query_arrival_predictor;
    configs.arrival_time_score_scalar_ = arrival_time_score_scalar;
    configs.arrival_time_no_prediction_default_score_ =
        arrival_time_no_prediction_default_score;

    configs.astream_mod_ = astream_mod;

    configs.ss_mastering_type_ = ss_master_type;

    return configs;
}

std::ostream& operator<<( std::ostream&              os,
                          const periodic_site_selector_operations_configs& configs ) {
    os << "Site Poller Configs: [ "
          "num_poller_threads_:"
       << configs.num_poller_threads_ << ", update_stats_sleep_delay_ms_:"
       << configs.update_stats_sleep_delay_ms_ << "]";
    return os;
}

std::ostream& operator<<( std::ostream&                       os,
                          const transaction_preparer_configs& configs ) {
    os << "Transaction Preparer Configs: [ "
          "num_worker_threads_per_client_:"
       << configs.num_worker_threads_per_client_
       << ", num_clients_:" << configs.num_clients_
       << ", astream_mod_:" << configs.astream_mod_
       << ", astream_empty_wait_ms_:" << configs.astream_empty_wait_ms_
       << ", astream_tracking_interval_ms_:"
       << configs.astream_tracking_interval_ms_
       << ", astream_max_queue_size_:" << configs.astream_max_queue_size_
       << ", max_write_blend_rate_:" << configs.max_write_blend_rate_
       << ", background_executor_cid_:" << configs.background_executor_cid_
       << ", periodic_operations_cid_:" << configs.periodic_operations_cid_
       << ", run_background_workers_:" << configs.run_background_workers_
       << ", num_background_work_items_to_execute_:"
       << configs.num_background_work_items_to_execute_
       << ", ss_mastering_type_:"
       << ss_mastering_type_string( configs.ss_mastering_type_ )
       << ", estore_clay_periodic_interval_ms_:"
       << configs.estore_clay_periodic_interval_ms_
       << ", estore_load_epsilon_threshold_:"
       << configs.estore_load_epsilon_threshold_
       << ", estore_hot_partition_pct_thresholds_:"
       << configs.estore_hot_partition_pct_thresholds_
       << ", adr_multiplier_:" << configs.adr_multiplier_
       << ", enable_plan_reuse_:" << configs.enable_plan_reuse_
       << ", enable_wait_for_plan_reuse_:"
       << configs.enable_wait_for_plan_reuse_ << ", optimistic_lock_mode:_"
       << partition_lock_mode_string( configs.optimistic_lock_mode_ )
       << ", physical_adjustment_lock_mode:_"
       << partition_lock_mode_string( configs.physical_adjustment_lock_mode_ )
       << ", force_change_lock_mode:_"
       << partition_lock_mode_string( configs.force_change_lock_mode_ )
       << ", periodic_site_selector_operations_configs_:"
       << configs.periodic_site_selector_operations_configs_ << "]";
    return os;
}

std::ostream& operator<<( std::ostream&                           os,
                          const heuristic_site_evaluator_configs& configs ) {
    os << "Heuristic Site Evaluator Configs: [ num_sites_:"
       << configs.num_sites_ << ", num_clients_:" << configs.num_clients_
       << ", split_partition_ratio_:" << configs.split_partition_ratio_
       << ", merge_partition_ratio_:" << configs.merge_partition_ratio_
       << ", upfront_cost_weight_:" << configs.upfront_cost_weight_
       << ", scan_upfront_cost_weight_:" << configs.scan_upfront_cost_weight_
       << ", horizon_weight_:" << configs.horizon_weight_
       << ", default_horizon_benefit_weight_:"
       << configs.default_horizon_benefit_weight_
       << ", repartition_remaster_horizon_weight_:"
       << configs.repartition_remaster_horizon_weight_
       << ", horizon_storage_weight_:" << configs.horizon_storage_weight_
       << ", repartition_prob_rescale_:" << configs.repartition_prob_rescale_
       << ", outlier_repartition_std_dev_:"
       << configs.outlier_repartition_std_dev_
       << ", split_partition_size_ratio_:"
       << configs.split_partition_size_ratio_
       << ", plan_site_load_balance_weight_:"
       << configs.plan_site_load_balance_weight_
       << ", plan_site_storage_balance_weight_:"
       << configs.plan_site_storage_balance_weight_
       << ", plan_site_storage_balance_weight_:"
       << configs.plan_site_storage_balance_weight_
       << ", plan_site_storage_overlimit_weight_:"
       << configs.plan_site_storage_overlimit_weight_
       << ", plan_site_storage_variance_weight_:"
       << configs.plan_site_storage_variance_weight_;
    os << ", plan_site_storage_overlimit_weights_: {";
    for( const auto& entry : configs.plan_site_storage_overlimit_weights_ ) {
        os << "( " << entry.first << ", " << entry.second << " ),";
    }
    os << "}, plan_site_storage_variance_weights_: {";
    for( const auto& entry : configs.plan_site_storage_variance_weights_ ) {
        os << "( " << entry.first << ", " << entry.second << " ),";
    }
    os << "}, plan_scan_in_cost_order_:" << configs.plan_scan_in_cost_order_
       << ", plan_scan_limit_changes_:" << configs.plan_scan_limit_changes_
       << ", plan_scan_limit_ratio_:" << configs.plan_scan_limit_ratio_
       << "num_update_destinations_per_site_:"
       << configs.num_update_destinations_per_site_
       << ", num_samples_for_client_:" << configs.num_samples_for_client_
       << ", num_samples_from_other_clients_:"
       << configs.num_samples_from_other_clients_
       << ", allow_force_site_selector_changes_:"
       << configs.allow_force_site_selector_changes_
       << ", sample_reservoir_size_:" << configs.sample_reservoir_size_
       << ", sample_reservoir_initial_multiplier_:"
       << configs.sample_reservoir_initial_multiplier_
       << ", sample_reservoir_weight_increment_:"
       << configs.sample_reservoir_weight_increment_
       << ", sample_reservoir_decay_rate_:"
       << configs.sample_reservoir_decay_rate_
       << ", force_change_reservoir_size_:"
       << configs.force_change_reservoir_size_
       << ", force_change_reservoir_initial_multiplier_:"
       << configs.force_change_reservoir_initial_multiplier_
       << ", force_change_reservoir_weight_increment_:"
       << configs.force_change_reservoir_weight_increment_
       << ", force_change_reservoir_decay_rate_:"
       << configs.force_change_reservoir_decay_rate_
       << ", storage_removal_threshold_:" << configs.storage_removal_threshold_
       << ", num_remove_replicas_iteration_:"
       << configs.num_remove_replicas_iteration_
       << ", allow_vertical_partitioning_:"
       << configs.allow_vertical_partitioning_
       << ", allow_horizontal_partitioning_:"
       << configs.allow_horizontal_partitioning_
       << ", allow_change_partition_types_:"
       << configs.allow_change_partition_types_
       << ", allow_change_storage_types_:"
       << configs.allow_change_storage_types_
       << ", acceptable_new_partition_types_: (";
    for( const auto& t : configs.acceptable_new_partition_types_ ) {
        os << t << ", ";
    }
    os << ") , acceptable_partition_types_: (";
    for( const auto& t : configs.acceptable_partition_types_ ) {
        os << t << ", ";
    }
    os << ") , acceptable_new_storage_types_: (";
    for( const auto& t : configs.acceptable_new_storage_types_ ) {
        os << t << ", ";
    }
    os << ") , acceptable_storage_types_: (";
    for( const auto& t : configs.acceptable_storage_types_ ) {
        os << t << ", ";
    }
    os << "), enable_plan_reuse_: " << configs.enable_plan_reuse_
       << ", plan_reuse_threshold_:" << configs.plan_reuse_threshold_
       << ", enable_wait_for_plan_reuse_: "
       << configs.enable_wait_for_plan_reuse_
       << ", use_query_arrival_predictor_:"
       << configs.use_query_arrival_predictor_
       << ", arrival_time_score_scalar_:" << configs.arrival_time_score_scalar_
       << ", arrival_time_no_prediction_default_score_:"
       << configs.arrival_time_no_prediction_default_score_
       << ", astream_mod_:" << configs.astream_mod_ << ", ss_mastering_type_:"
       << ss_mastering_type_string( configs.ss_mastering_type_ ) << "]";
    return os;
}

std::ostream& operator<<(
    std::ostream& os, const partition_data_location_table_configs& configs ) {
    os << "Partition Data Location Table Configs: [ num_sites_:"
       << configs.num_sites_ << ", storage_tier_limits_: {";
    for( const auto& limit : configs.storage_tier_limits_ ) {
        os << "(" << limit.first << ", " << limit.second << "), ";
    }
    os << "},  partition_tracking_bucket_:"
       << configs.partition_tracking_bucket_ << "]";
    return os;
}
