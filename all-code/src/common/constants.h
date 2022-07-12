#pragma once

#include <chrono>
#include <gflags/gflags.h>

#include "../benchmark/workload_type.h"
#include "../concurrency/lock_type.h"
#include "../data-site/single-site-db/db_abstraction_types.h"
#include "../data-site/update-propagation/update_propagation_types.h"
#include "../persistence/rewriter_type.h"
#include "../site-selector/partition_lock_mode.h"
#include "../site-selector/query_arrival_predictor_types.h"
#include "../site-selector/ss_types.h"
#include "compile_time_constants.h"
#include "predictor2.h"

DECLARE_bool( enable_timer_dumps );

DECLARE_bool( skip_slow_tests );

DECLARE_uint32( num_sites );
DECLARE_uint32( site_location_identifier );

DECLARE_uint32( perf_counter_client_multiplier );

DECLARE_int32( num_records_in_chain );
DECLARE_int32( num_records_in_snapshot_chain );
DECLARE_uint32( num_records_in_multi_entry_table );
DECLARE_uint64( spinlock_yield_length );

DECLARE_string( lock_type );

DECLARE_uint64( expected_tps );

DECLARE_uint32( gc_sleep_time );

DECLARE_double( gc_memory_threshold );
DECLARE_int32( gc_thread_priority );
DECLARE_int32( always_gc_on_writes );

DECLARE_double( memory_tier_site_space_limit );
DECLARE_double( disk_tier_site_space_limit);

DECLARE_string( workload_type );
DECLARE_int32( bench_num_clients );
DECLARE_int32( bench_time_sec );
DECLARE_uint64( bench_num_ops_before_timer_check );

DECLARE_string( db_abstraction_type );

DECLARE_int64( ycsb_max_key );
DECLARE_int32( ycsb_value_size );
DECLARE_int32( ycsb_partition_size );
DECLARE_int32( ycsb_column_size );
DECLARE_int32( ycsb_num_ops_per_transaction );
DECLARE_double( ycsb_zipf_alpha );
DECLARE_double( ycsb_scan_selectivity );
DECLARE_int32( ycsb_column_partition_size );
DECLARE_string( ycsb_partition_type );
DECLARE_string( ycsb_storage_type );
DECLARE_bool( ycsb_scan_restricted );
DECLARE_bool( ycsb_rmw_restricted );
DECLARE_bool( ycsb_store_scan_results );

// SPAR config
DECLARE_int32( spar_width );
DECLARE_int32( spar_slot );
DECLARE_int32( spar_training_interval );
DECLARE_int32( spar_rounding_factor_temporal );
DECLARE_int32( spar_rounding_factor_periodic );
DECLARE_int32( spar_predicted_arrival_window );
DECLARE_double( spar_normalization_value );
DECLARE_double( spar_gamma );
DECLARE_double( spar_learning_rate );

DECLARE_double( spar_init_weight );
DECLARE_double( spar_init_bias );
DECLARE_double( spar_regularization );
DECLARE_double( spar_bias_regularization );
DECLARE_int32( spar_batch_size );

// Ensemble config
DECLARE_uint32( ensemble_slot );
DECLARE_uint32( ensemble_search_window );
DECLARE_uint32( ensemble_linear_training_interval );
DECLARE_uint32( ensemble_rnn_training_interval );
DECLARE_uint32( ensemble_krls_training_interval );
DECLARE_double( ensemble_rnn_learning_rate );
DECLARE_double( ensemble_krls_learning_rate );
DECLARE_double( ensemble_krls_gamma );
DECLARE_double( ensemble_outlier_threshold );
DECLARE_uint32( ensemble_rnn_epoch_count );
DECLARE_uint32( ensemble_rnn_sequence_size );
DECLARE_uint32( ensemble_rnn_layer_count );
DECLARE_bool( ensemble_apply_scheduled_outlier_model );

DECLARE_int32( ycsb_write_prob );
DECLARE_int32( ycsb_read_prob );
DECLARE_int32( ycsb_rmw_prob );
DECLARE_int32( ycsb_scan_prob );
DECLARE_int32( ycsb_multi_rmw_prob );

DECLARE_int32( db_split_prob );
DECLARE_int32( db_merge_prob );
DECLARE_int32( db_remaster_prob );

DECLARE_uint32( tpcc_num_warehouses );
DECLARE_uint32( tpcc_num_items );
DECLARE_uint32( tpcc_num_suppliers );
DECLARE_uint32( tpcc_expected_num_orders_per_cust );
DECLARE_uint32( tpcc_item_partition_size );
DECLARE_uint32( tpcc_partition_size );
DECLARE_uint32( tpcc_district_partition_size );
DECLARE_uint32( tpcc_customer_partition_size );
DECLARE_uint32( tpcc_supplier_partition_size );
DECLARE_uint32( tpcc_new_order_within_warehouse_likelihood );
DECLARE_uint32( tpcc_payment_within_warehouse_likelihood );
DECLARE_bool( tpcc_track_and_use_recent_items );

DECLARE_uint32( tpcc_num_clients );
DECLARE_uint32( tpch_num_clients );

// tpcc workload likelihoods
DECLARE_uint32( tpcc_delivery_prob );
DECLARE_uint32( tpcc_new_order_prob );
DECLARE_uint32( tpcc_order_status_prob );
DECLARE_uint32( tpcc_payment_prob );
DECLARE_uint32( tpcc_stock_level_prob );

DECLARE_uint32( tpcc_customer_column_partition_size );
DECLARE_string( tpcc_customer_part_type );
DECLARE_string( tpcc_customer_storage_type );
DECLARE_uint32( tpcc_customer_district_column_partition_size );
DECLARE_string( tpcc_customer_district_part_type );
DECLARE_string( tpcc_customer_district_storage_type );
DECLARE_uint32( tpcc_district_column_partition_size );
DECLARE_string( tpcc_district_part_type );
DECLARE_string( tpcc_district_storage_type );
DECLARE_uint32( tpcc_history_column_partition_size );
DECLARE_string( tpcc_history_part_type );
DECLARE_string( tpcc_history_storage_type );
DECLARE_uint32( tpcc_item_column_partition_size );
DECLARE_string( tpcc_item_part_type );
DECLARE_string( tpcc_item_storage_type );
DECLARE_uint32( tpcc_nation_column_partition_size );
DECLARE_string( tpcc_nation_part_type );
DECLARE_string( tpcc_nation_storage_type );
DECLARE_uint32( tpcc_new_order_column_partition_size );
DECLARE_string( tpcc_new_order_part_type );
DECLARE_string( tpcc_new_order_storage_type );
DECLARE_uint32( tpcc_order_column_partition_size );
DECLARE_string( tpcc_order_part_type );
DECLARE_string( tpcc_order_storage_type );
DECLARE_uint32( tpcc_order_line_column_partition_size );
DECLARE_string( tpcc_order_line_part_type );
DECLARE_string( tpcc_order_line_storage_type );
DECLARE_uint32( tpcc_region_column_partition_size );
DECLARE_string( tpcc_region_part_type );
DECLARE_string( tpcc_region_storage_type );
DECLARE_uint32( tpcc_stock_column_partition_size );
DECLARE_string( tpcc_stock_part_type );
DECLARE_string( tpcc_stock_storage_type );
DECLARE_uint32( tpcc_supplier_column_partition_size );
DECLARE_string( tpcc_supplier_part_type );
DECLARE_string( tpcc_supplier_storage_type );
DECLARE_uint32( tpcc_warehouse_column_partition_size );
DECLARE_string( tpcc_warehouse_part_type );
DECLARE_string( tpcc_warehouse_storage_type );

DECLARE_uint32( tpch_q1_prob );
DECLARE_uint32( tpch_q2_prob );
DECLARE_uint32( tpch_q3_prob );
DECLARE_uint32( tpch_q4_prob );
DECLARE_uint32( tpch_q5_prob );
DECLARE_uint32( tpch_q6_prob );
DECLARE_uint32( tpch_q7_prob );
DECLARE_uint32( tpch_q8_prob );
DECLARE_uint32( tpch_q9_prob );
DECLARE_uint32( tpch_q10_prob );
DECLARE_uint32( tpch_q11_prob );
DECLARE_uint32( tpch_q12_prob );
DECLARE_uint32( tpch_q13_prob );
DECLARE_uint32( tpch_q14_prob );
DECLARE_uint32( tpch_q15_prob );
DECLARE_uint32( tpch_q16_prob );
DECLARE_uint32( tpch_q17_prob );
DECLARE_uint32( tpch_q18_prob );
DECLARE_uint32( tpch_q19_prob );
DECLARE_uint32( tpch_q20_prob );
DECLARE_uint32( tpch_q21_prob );
DECLARE_uint32( tpch_q22_prob );
DECLARE_uint32( tpch_all_prob );

// tpcc spec
DECLARE_uint32( tpcc_spec_num_districts_per_warehouse );
DECLARE_uint32( tpcc_spec_num_customers_per_district );
DECLARE_uint32( tpcc_spec_max_num_order_lines_per_order );
DECLARE_uint32( tpcc_spec_initial_num_customers_per_district );

DECLARE_bool( tpcc_should_fully_replicate_warehouses );
DECLARE_bool( tpcc_should_fully_replicate_districts );

DECLARE_bool( tpcc_use_warehouse );
DECLARE_bool( tpcc_use_district );
DECLARE_bool( tpcc_h_scan_full_tables );

// smallbank
DECLARE_uint64( smallbank_num_accounts );
DECLARE_bool( smallbank_hotspot_use_fixed_size );
DECLARE_double( smallbank_hotspot_percentage );
DECLARE_uint32( smallbank_hotspot_fixed_size );
DECLARE_uint32( smallbank_partition_size );
DECLARE_uint32( smallbank_customer_account_partition_spread );

DECLARE_uint32( smallbank_accounts_col_size );
DECLARE_string( smallbank_accounts_partition_data_type );
DECLARE_string( smallbank_accounts_storage_data_type );
DECLARE_uint32( smallbank_banking_col_size );
DECLARE_string( smallbank_banking_partition_data_type );
DECLARE_string( smallbank_banking_storage_data_type );

// probabilities
DECLARE_uint32( smallbank_amalgamate_prob );
DECLARE_uint32( smallbank_balance_prob );
DECLARE_uint32( smallbank_deposit_checking_prob );
DECLARE_uint32( smallbank_send_payment_prob );
DECLARE_uint32( smallbank_transact_savings_prob );
DECLARE_uint32( smallbank_write_check_prob );

// twitter
DECLARE_uint64( twitter_num_users );
DECLARE_uint64( twitter_num_tweets );
DECLARE_double( twitter_tweet_skew );
DECLARE_double( twitter_follow_skew );
DECLARE_uint32( twitter_account_partition_size );
DECLARE_uint32( twitter_max_follow_per_user );
DECLARE_uint32( twitter_max_tweets_per_user );
DECLARE_uint32( twitter_limit_tweets );
DECLARE_uint32( twitter_limit_tweets_for_uid );
DECLARE_uint32( twitter_limit_followers );

DECLARE_bool( twitter_should_fully_replicate_users );
DECLARE_bool( twitter_should_fully_replicate_follows );
DECLARE_bool( twitter_should_fully_replicate_tweets );

// probabilities
DECLARE_uint32( twitter_get_tweet_prob );
DECLARE_uint32( twitter_get_tweets_from_following_prob );
DECLARE_uint32( twitter_get_followers_prob );
DECLARE_uint32( twitter_get_user_tweets_prob );
DECLARE_uint32( twitter_insert_tweet_prob );
DECLARE_uint32( twitter_get_recent_tweets_prob );
DECLARE_uint32( twitter_get_tweets_from_followers_prob );
DECLARE_uint32( twitter_get_tweets_like_prob );
DECLARE_uint32( twitter_update_followers_prob );

DECLARE_uint32( twitter_followers_column_partition_size );
DECLARE_string( twitter_followers_part_type );
DECLARE_string( twitter_followers_storage_type );
DECLARE_uint32( twitter_follows_column_partition_size );
DECLARE_string( twitter_follows_part_type );
DECLARE_string( twitter_follows_storage_type );
DECLARE_uint32( twitter_tweets_column_partition_size );
DECLARE_string( twitter_tweets_part_type );
DECLARE_string( twitter_tweets_storage_type );
DECLARE_uint32( twitter_user_profile_column_partition_size );
DECLARE_string( twitter_user_profile_part_type );
DECLARE_string( twitter_user_profile_storage_type );


DECLARE_uint64( ss_bucket_size );

DECLARE_int32( astream_tracking_interval_ms );
DECLARE_int32( astream_mod );
DECLARE_int32( astream_empty_wait_ms );
DECLARE_uint64( astream_max_queue_size );

DECLARE_double( ss_max_write_blend_rate );

DECLARE_bool( ss_run_background_workers );
DECLARE_uint32( ss_num_background_work_items_to_execute );

DECLARE_uint32( estore_clay_periodic_interval_ms );
DECLARE_double( estore_load_epsilon_threshold );
DECLARE_double( estore_hot_partition_pct_thresholds );
DECLARE_double( adr_multiplier );

DECLARE_bool( ss_enable_plan_reuse );
DECLARE_double( ss_plan_reuse_threshold );
DECLARE_bool( ss_enable_wait_for_plan_reuse );

DECLARE_string( ss_optimistic_lock_mode );
DECLARE_string( ss_physical_adjustment_lock_mode );
DECLARE_string( ss_force_change_lock_mode );

DECLARE_int32( num_site_selector_worker_threads_per_client );

DECLARE_string( ss_strategy_type );
DECLARE_string( ss_mastering_type );
DECLARE_double( ss_skew_weight );
DECLARE_double( ss_skew_exp_constant );
DECLARE_double( ss_svv_delay_weight );
DECLARE_double( ss_single_site_within_transactions_weight );
DECLARE_double( ss_single_site_across_transactions_weight );
DECLARE_double( ss_wait_time_normalization_constant );
DECLARE_double( ss_split_partition_ratio );
DECLARE_double( ss_merge_partition_ratio );
DECLARE_int32( ss_num_sampled_transactions_for_client );
DECLARE_int32( ss_num_sampled_transactions_from_other_clients );
DECLARE_double( ss_upfront_cost_weight );
DECLARE_double( ss_scan_upfront_cost_weight );
DECLARE_double( ss_horizon_weight );
DECLARE_double( ss_default_horizon_benefit_weight );
DECLARE_double( ss_repartition_remaster_horizon_weight );
DECLARE_double( ss_horizon_storage_weight );
DECLARE_double( ss_repartition_prob_rescale );
DECLARE_double( ss_outlier_repartition_std_dev );
DECLARE_double( ss_split_partition_size_ratio );
DECLARE_double( ss_plan_site_load_balance_weight );

DECLARE_double( ss_plan_site_storage_balance_weight );
DECLARE_double( ss_plan_site_storage_overlimit_weight );
DECLARE_double( ss_plan_site_storage_variance_weight );
DECLARE_double( ss_plan_site_memory_overlimit_weight );
DECLARE_double( ss_plan_site_disk_overlimit_weight );
DECLARE_double( ss_plan_site_memory_variance_weight );
DECLARE_double( ss_plan_site_disk_variance_weight );

DECLARE_bool( ss_allow_force_site_selector_changes );

DECLARE_bool( ss_plan_scan_in_cost_order );
DECLARE_bool( ss_plan_scan_limit_changes );
DECLARE_double( ss_plan_scan_limit_ratio );

DECLARE_string( ss_acceptable_new_partition_types );
DECLARE_string( ss_acceptable_partition_types );
DECLARE_string( ss_acceptable_new_storage_types );
DECLARE_string( ss_acceptable_storage_types );

DECLARE_bool( use_query_arrival_predictor );
DECLARE_string( query_arrival_predictor_type );
DECLARE_double( query_arrival_access_threshold );
DECLARE_uint64( query_arrival_time_bucket );
DECLARE_double( query_arrival_count_bucket );
DECLARE_double( query_arrival_time_score_scalar );
DECLARE_double( query_arrival_default_score );

DECLARE_bool( cost_model_is_static_model );
DECLARE_double( cost_model_learning_rate );
DECLARE_double( cost_model_regularization );
DECLARE_double( cost_model_bias_regularization );

DECLARE_double( cost_model_momentum );
DECLARE_double( cost_model_kernel_gamma );

DECLARE_int32( cost_model_max_internal_model_size_scalar );
DECLARE_int32( cost_model_layer_1_nodes_scalar );
DECLARE_int32( cost_model_layer_2_nodes_scalar );

DECLARE_double( cost_model_num_reads_weight );
DECLARE_double( cost_model_num_reads_normalization );
DECLARE_double( cost_model_num_reads_max_input );
DECLARE_double( cost_model_read_bias );
DECLARE_string( cost_model_read_predictor_type );

DECLARE_double( cost_model_num_reads_disk_weight );
DECLARE_double( cost_model_read_disk_bias );
DECLARE_string( cost_model_read_disk_predictor_type );

DECLARE_double( cost_model_scan_num_rows_weight );
DECLARE_double( cost_model_scan_num_rows_normalization );
DECLARE_double( cost_model_scan_num_rows_max_input );
DECLARE_double( cost_model_scan_selectivity_weight );
DECLARE_double( cost_model_scan_selectivity_normalization );
DECLARE_double( cost_model_scan_selectivity_max_input );
DECLARE_double( cost_model_scan_bias );
DECLARE_string( cost_model_scan_predictor_type );

DECLARE_double( cost_model_scan_disk_num_rows_weight );
DECLARE_double( cost_model_scan_disk_selectivity_weight );
DECLARE_double( cost_model_scan_disk_bias );
DECLARE_string( cost_model_scan_disk_predictor_type );

DECLARE_double( cost_model_num_writes_weight );
DECLARE_double( cost_model_num_writes_normalization );
DECLARE_double( cost_model_num_writes_max_input );
DECLARE_double( cost_model_write_bias );
DECLARE_string( cost_model_write_predictor_type );

DECLARE_double( cost_model_lock_weight );
DECLARE_double( cost_model_lock_normalization );
DECLARE_double( cost_model_lock_max_input );
DECLARE_double( cost_model_lock_bias );
DECLARE_string( cost_model_lock_predictor_type );

DECLARE_double( cost_model_commit_serialize_weight );
DECLARE_double( cost_model_commit_serialize_normalization );
DECLARE_double( cost_model_commit_serialize_max_input );
DECLARE_double( cost_model_commit_serialize_bias );
DECLARE_string( cost_model_commit_serialize_predictor_type );

DECLARE_double( cost_model_commit_build_snapshot_weight );
DECLARE_double( cost_model_commit_build_snapshot_normalization );
DECLARE_double( cost_model_commit_build_snapshot_max_input );
DECLARE_double( cost_model_commit_build_snapshot_bias );
DECLARE_string( cost_model_commit_build_snapshot_predictor_type );

DECLARE_double( cost_model_wait_for_service_weight );
DECLARE_double( cost_model_wait_for_service_normalization );
DECLARE_double( cost_model_wait_for_service_max_input );
DECLARE_double( cost_model_wait_for_service_bias );
DECLARE_string( cost_model_wait_for_service_predictor_type );

DECLARE_double( cost_model_wait_for_session_version_weight );
DECLARE_double( cost_model_wait_for_session_version_normalization );
DECLARE_double( cost_model_wait_for_session_version_max_input );
DECLARE_double( cost_model_wait_for_session_version_bias );
DECLARE_string( cost_model_wait_for_session_version_predictor_type );

DECLARE_double( cost_model_wait_for_session_snapshot_weight );
DECLARE_double( cost_model_wait_for_session_snapshot_normalization );
DECLARE_double( cost_model_wait_for_session_snapshot_max_input );
DECLARE_double( cost_model_wait_for_session_snapshot_bias );
DECLARE_string( cost_model_wait_for_session_snapshot_predictor_type );

DECLARE_double( cost_model_site_load_prediction_write_weight );
DECLARE_double( cost_model_site_load_prediction_write_normalization );
DECLARE_double( cost_model_site_load_prediction_write_max_input );
DECLARE_double( cost_model_site_load_prediction_read_weight );
DECLARE_double( cost_model_site_load_prediction_read_normalization );
DECLARE_double( cost_model_site_load_prediction_read_max_input );
DECLARE_double( cost_model_site_load_prediction_update_weight );
DECLARE_double( cost_model_site_load_prediction_update_normalization );
DECLARE_double( cost_model_site_load_prediction_update_max_input );
DECLARE_double( cost_model_site_load_prediction_bias );
DECLARE_double( cost_model_site_load_prediction_max_scale );
DECLARE_string( cost_model_site_load_predictor_type );

DECLARE_double( cost_model_site_operation_count_weight );
DECLARE_double( cost_model_site_operation_count_normalization );
DECLARE_double( cost_model_site_operation_count_max_input );
DECLARE_double( cost_model_site_operation_count_max_scale );
DECLARE_double( cost_model_site_operation_count_bias );
DECLARE_string( cost_model_site_operation_count_predictor_type );

DECLARE_double( cost_model_distributed_scan_max_weight );
DECLARE_double( cost_model_distributed_scan_min_weight );
DECLARE_double( cost_model_distributed_scan_normalization );
DECLARE_double( cost_model_distributed_scan_max_input );
DECLARE_double( cost_model_distributed_scan_bias );
DECLARE_string( cost_model_distributed_scan_predictor_type );

DECLARE_double( cost_model_memory_num_rows_normalization );
DECLARE_double( cost_model_memory_num_rows_max_input );

DECLARE_double( cost_model_memory_allocation_num_rows_weight );
DECLARE_double( cost_model_memory_allocation_bias );
DECLARE_string( cost_model_memory_allocation_predictor_type );

DECLARE_double( cost_model_memory_deallocation_num_rows_weight );
DECLARE_double( cost_model_memory_deallocation_bias );
DECLARE_string( cost_model_memory_deallocation_predictor_type );

DECLARE_double( cost_model_memory_assignment_num_rows_weight );
DECLARE_double( cost_model_memory_assignment_bias );
DECLARE_string( cost_model_memory_assignment_predictor_type );

DECLARE_double( cost_model_disk_num_rows_normalization );
DECLARE_double( cost_model_disk_num_rows_max_input );

DECLARE_double( cost_model_evict_to_disk_num_rows_weight );
DECLARE_double( cost_model_evict_to_disk_bias );
DECLARE_string( cost_model_evict_to_disk_predictor_type );

DECLARE_double( cost_model_pull_from_disk_num_rows_weight );
DECLARE_double( cost_model_pull_from_disk_bias );
DECLARE_string( cost_model_pull_from_disk_predictor_type );

DECLARE_double( cost_model_widths_weight );
DECLARE_double( cost_model_widths_normalization );
DECLARE_double( cost_model_widths_max_input );

DECLARE_double( cost_model_wait_for_session_remaster_default_pct );

DECLARE_bool( ss_enum_enable_track_stats );
DECLARE_double( ss_enum_prob_of_returning_default );
DECLARE_double( ss_enum_min_threshold );
DECLARE_uint64( ss_enum_min_count_threshold );
DECLARE_double( ss_enum_contention_bucket );
DECLARE_double( ss_enum_cell_width_bucket );
DECLARE_int32( ss_enum_num_entries_bucket );
DECLARE_double( ss_enum_num_updates_needed_bucket );
DECLARE_double( ss_enum_scan_selectivity_bucket );
DECLARE_int32( ss_enum_num_point_reads_bucket );
DECLARE_int32( ss_enum_num_point_updates_bucket );

DECLARE_int32( predictor_max_internal_model_size );

DECLARE_int32( ss_update_stats_sleep_delay_ms );
DECLARE_int32( ss_num_poller_threads );
DECLARE_int32( ss_colocation_keys_sampled );
DECLARE_double( ss_partition_coverage_threshold );

DECLARE_string( update_source_type );
DECLARE_string( update_destination_type );
DECLARE_int64( kafka_buffer_read_backoff );

DECLARE_int32( num_update_sources );
DECLARE_int32( num_update_destinations );
DECLARE_int32( sleep_time_between_update_enqueue_iterations );
DECLARE_int32( num_updates_before_apply_self );
DECLARE_int32( num_updates_to_apply_self );

DECLARE_bool( limit_number_of_records_propagated );
DECLARE_int32( number_of_updates_needed_for_propagation );

DECLARE_int64( kafka_poll_count );
DECLARE_int64( kafka_poll_sleep_ms );
DECLARE_int32( kafka_backoff_sleep_ms );
DECLARE_int32( kafka_poll_timeout );
DECLARE_int32( kafka_flush_wait_ms );
DECLARE_int32( kafka_consume_timeout_ms );
DECLARE_int32( kafka_seek_timeout_ms );

DECLARE_string( kafka_fetch_max_ms );
DECLARE_string( kafka_queue_max_ms );
DECLARE_string( kafka_socket_blocking_max_ms );
DECLARE_string( kafka_consume_max_messages );
DECLARE_string( kafka_produce_max_messages );
DECLARE_string( kafka_required_acks );
DECLARE_string( kafka_send_max_retries );
DECLARE_string( kafka_max_buffered_messages );
DECLARE_string( kafka_max_buffered_messages_size );

DECLARE_string( rewriter_type );
DECLARE_double( prob_range_rewriter_threshold );

DECLARE_bool( load_chunked_tables );
DECLARE_bool( persist_chunked_tables );
DECLARE_int32( chunked_table_size );

DECLARE_bool( enable_secondary_storage );
DECLARE_string( secondary_storage_dir );

DECLARE_int32( sample_reservoir_size_per_client );
DECLARE_double( sample_reservoir_initial_multiplier );
DECLARE_double( sample_reservoir_weight_increment );
DECLARE_double( sample_reservoir_decay_rate );
DECLARE_int32( force_change_reservoir_size_per_client );
DECLARE_double( force_change_reservoir_initial_multiplier );
DECLARE_double( force_change_reservoir_weight_increment );
DECLARE_double( force_change_reservoir_decay_rate );
DECLARE_int32( reservoir_decay_seconds );

DECLARE_double( ss_storage_removal_threshold );
DECLARE_int32( ss_num_remove_replicas_iteration );

DECLARE_bool( ss_allow_vertical_partitioning );
DECLARE_bool( ss_allow_horizontal_partitioning );
DECLARE_bool( ss_allow_change_partition_types );
DECLARE_bool( ss_allow_change_storage_types );

DECLARE_int32( timer_log_level );
DECLARE_int32( slow_timer_error_log_level );
DECLARE_int32( cost_model_log_level );
DECLARE_int32( heuristic_evaluator_log_level );
DECLARE_int32( periodic_cost_model_log_level );
DECLARE_int32( update_propagation_log_level );
DECLARE_int32( significant_update_propagation_log_level );
DECLARE_int32( ss_action_executor_log_level );
DECLARE_int32( rpc_handler_log_level );

DECLARE_double( slow_timer_log_time_threshold );
DECLARE_double( cost_model_log_error_threshold );

extern bool k_enable_timer_dumps;

extern bool k_skip_slow_tests;

extern uint32_t k_num_sites;
extern uint32_t k_site_location_identifier;

extern uint32_t k_perf_counter_client_multiplier;

extern uint64_t  k_spinlock_back_off_duration;
extern int32_t   k_num_records_in_chain;
extern int32_t   k_num_records_in_snapshot_chain;
extern uint32_t  k_num_records_in_multi_entry_table;
extern lock_type k_lock_type;

extern uint64_t k_expected_tps;
extern uint64_t k_max_spin_time_us;

extern uint32_t k_gc_sleep_time;
extern double   k_gc_memory_threshold;
extern int32_t  k_gc_thread_priority;
extern int32_t  k_always_gc_on_writes;

extern double k_memory_tier_site_space_limit;
extern double k_disk_tier_site_space_limit;

// General benchmark configs
extern workload_type k_workload_type;
extern int32_t       k_bench_num_clients;
extern int32_t       k_bench_time_sec;
extern uint64_t      k_bench_num_ops_before_timer_check;

extern db_abstraction_type k_db_abstraction_type;

// YCSB configs
extern int64_t k_ycsb_max_key;
extern int32_t k_ycsb_value_size;
extern int32_t k_ycsb_partition_size;
extern int32_t k_ycsb_column_size;
extern int32_t k_ycsb_num_ops_per_transaction;
extern double  k_ycsb_zipf_alpha;
extern double  k_ycsb_scan_selectivity;
extern int32_t k_ycsb_column_partition_size;

extern partition_type::type    k_ycsb_partition_type;
extern storage_tier_type::type k_ycsb_storage_type;
extern bool                    k_ycsb_scan_restricted;
extern bool                    k_ycsb_rmw_restricted;
extern bool                    k_ycsb_store_scan_results;

// ycsb workload likelihoods
extern int32_t k_ycsb_write_prob;
extern int32_t k_ycsb_read_prob;
extern int32_t k_ycsb_rmw_prob;
extern int32_t k_ycsb_scan_prob;
extern int32_t k_ycsb_multi_rmw_prob;

extern int32_t k_db_split_prob;
extern int32_t k_db_merge_prob;
extern int32_t k_db_remaster_prob;

// TPCC configs
extern uint32_t k_tpcc_num_warehouses;
extern uint32_t k_tpcc_num_items;
extern uint32_t k_tpcc_num_suppliers;
extern uint32_t k_tpcc_expected_num_orders_per_cust;
extern uint32_t k_tpcc_item_partition_size;
extern uint32_t k_tpcc_partition_size;
extern uint32_t k_tpcc_district_partition_size;
extern uint32_t k_tpcc_customer_partition_size;
extern uint32_t k_tpcc_supplier_partition_size;
extern uint32_t k_tpcc_new_order_within_warehouse_likelihood;
extern uint32_t k_tpcc_payment_within_warehouse_likelihood;

extern bool k_tpcc_track_and_use_recent_items;

extern uint32_t k_tpcc_num_clients;
extern uint32_t k_tpch_num_clients;

// tpcc workload likelihoods
extern uint32_t k_tpcc_delivery_prob;
extern uint32_t k_tpcc_new_order_prob;
extern uint32_t k_tpcc_order_status_prob;
extern uint32_t k_tpcc_payment_prob;
extern uint32_t k_tpcc_stock_level_prob;

extern uint32_t                k_tpcc_customer_column_partition_size;
extern partition_type::type    k_tpcc_customer_part_type;
extern storage_tier_type::type k_tpcc_customer_storage_type;
extern uint32_t                k_tpcc_customer_district_column_partition_size;
extern partition_type::type    k_tpcc_customer_district_part_type;
extern storage_tier_type::type k_tpcc_customer_district_storage_type;
extern uint32_t                k_tpcc_district_column_partition_size;
extern partition_type::type    k_tpcc_district_part_type;
extern storage_tier_type::type k_tpcc_district_storage_type;
extern uint32_t                k_tpcc_history_column_partition_size;
extern partition_type::type    k_tpcc_history_part_type;
extern storage_tier_type::type k_tpcc_history_storage_type;
extern uint32_t                k_tpcc_item_column_partition_size;
extern partition_type::type    k_tpcc_item_part_type;
extern storage_tier_type::type k_tpcc_item_storage_type;
extern uint32_t                k_tpcc_nation_column_partition_size;
extern partition_type::type    k_tpcc_nation_part_type;
extern storage_tier_type::type k_tpcc_nation_storage_type;
extern uint32_t                k_tpcc_new_order_column_partition_size;
extern partition_type::type    k_tpcc_new_order_part_type;
extern storage_tier_type::type k_tpcc_new_order_storage_type;
extern uint32_t                k_tpcc_order_column_partition_size;
extern partition_type::type    k_tpcc_order_part_type;
extern storage_tier_type::type k_tpcc_order_storage_type;
extern uint32_t                k_tpcc_order_line_column_partition_size;
extern partition_type::type    k_tpcc_order_line_part_type;
extern storage_tier_type::type k_tpcc_order_line_storage_type;
extern uint32_t                k_tpcc_region_column_partition_size;
extern partition_type::type    k_tpcc_region_part_type;
extern storage_tier_type::type k_tpcc_region_storage_type;
extern uint32_t                k_tpcc_stock_column_partition_size;
extern partition_type::type    k_tpcc_stock_part_type;
extern storage_tier_type::type k_tpcc_stock_storage_type;
extern uint32_t                k_tpcc_supplier_column_partition_size;
extern partition_type::type    k_tpcc_supplier_part_type;
extern storage_tier_type::type k_tpcc_supplier_storage_type;
extern uint32_t                k_tpcc_warehouse_column_partition_size;
extern partition_type::type    k_tpcc_warehouse_part_type;
extern storage_tier_type::type k_tpcc_warehouse_storage_type;

extern uint32_t k_tpch_q1_prob;
extern uint32_t k_tpch_q2_prob;
extern uint32_t k_tpch_q3_prob;
extern uint32_t k_tpch_q4_prob;
extern uint32_t k_tpch_q5_prob;
extern uint32_t k_tpch_q6_prob;
extern uint32_t k_tpch_q7_prob;
extern uint32_t k_tpch_q8_prob;
extern uint32_t k_tpch_q9_prob;
extern uint32_t k_tpch_q10_prob;
extern uint32_t k_tpch_q11_prob;
extern uint32_t k_tpch_q12_prob;
extern uint32_t k_tpch_q13_prob;
extern uint32_t k_tpch_q14_prob;
extern uint32_t k_tpch_q15_prob;
extern uint32_t k_tpch_q16_prob;
extern uint32_t k_tpch_q17_prob;
extern uint32_t k_tpch_q18_prob;
extern uint32_t k_tpch_q19_prob;
extern uint32_t k_tpch_q20_prob;
extern uint32_t k_tpch_q21_prob;
extern uint32_t k_tpch_q22_prob;
extern uint32_t k_tpch_all_prob;

// tpcc spec param
extern uint32_t k_tpcc_spec_num_districts_per_warehouse;
extern uint32_t k_tpcc_spec_num_customers_per_district;
extern uint32_t k_tpcc_spec_max_num_order_lines_per_order;
extern uint32_t k_tpcc_spec_initial_num_customers_per_district;

extern bool k_tpcc_should_fully_replicate_warehouses;
extern bool k_tpcc_should_fully_replicate_districts;

extern bool k_tpcc_use_warehouse;
extern bool k_tpcc_use_district;
extern bool k_tpcc_h_scan_full_tables;

// Smallbank configs
extern uint64_t k_smallbank_num_accounts;
extern bool     k_smallbank_hotspot_use_fixed_size;
extern double   k_smallbank_hotspot_percentage;
extern uint32_t k_smallbank_hotspot_fixed_size;
extern uint32_t k_smallbank_partition_size;
extern uint32_t k_smallbank_customer_account_partition_spread;

extern uint32_t             k_smallbank_accounts_col_size;
extern partition_type::type k_smallbank_accounts_partition_data_type;
extern storage_tier_type::type k_smallbank_accounts_storage_data_type;
extern uint32_t             k_smallbank_banking_col_size;
extern partition_type::type k_smallbank_banking_partition_data_type;
extern storage_tier_type::type k_smallbank_banking_storage_data_type;

// probabilities
extern uint32_t k_smallbank_amalgamate_prob;
extern uint32_t k_smallbank_balance_prob;
extern uint32_t k_smallbank_deposit_checking_prob;
extern uint32_t k_smallbank_send_payment_prob;
extern uint32_t k_smallbank_transact_savings_prob;
extern uint32_t k_smallbank_write_check_prob;

// twitter
extern uint64_t k_twitter_num_users;
extern uint64_t k_twitter_num_tweets;
extern double   k_twitter_tweet_skew;
extern double   k_twitter_follow_skew;
extern uint32_t k_twitter_account_partition_size;
extern uint32_t k_twitter_max_follow_per_user;
extern uint32_t k_twitter_max_tweets_per_user;
extern uint32_t k_twitter_limit_tweets;
extern uint32_t k_twitter_limit_tweets_for_uid;
extern uint32_t k_twitter_limit_followers;

extern uint32_t k_twitter_followers_column_partition_size;
extern partition_type::type k_twitter_followers_part_type;
extern storage_tier_type::type k_twitter_followers_storage_type;
extern uint32_t k_twitter_follows_column_partition_size;
extern partition_type::type k_twitter_follows_part_type;
extern storage_tier_type::type k_twitter_follows_storage_type;
extern uint32_t k_twitter_tweets_column_partition_size;
extern partition_type::type k_twitter_tweets_part_type;
extern storage_tier_type::type k_twitter_tweets_storage_type;
extern uint32_t k_twitter_user_profile_column_partition_size;
extern partition_type::type k_twitter_user_profile_part_type;
extern storage_tier_type::type k_twitter_user_profile_storage_type;

extern bool k_twitter_should_fully_replicate_users;
extern bool k_twitter_should_fully_replicate_follows;
extern bool k_twitter_should_fully_replicate_tweets;

// probabilities
extern uint32_t k_twitter_get_tweet_prob;
extern uint32_t k_twitter_get_tweets_from_following_prob;
extern uint32_t k_twitter_get_followers_prob;
extern uint32_t k_twitter_get_user_tweets_prob;
extern uint32_t k_twitter_insert_tweet_prob;
extern uint32_t k_twitter_get_recent_tweets_prob;
extern uint32_t k_twitter_get_tweets_from_followers_prob;
extern uint32_t k_twitter_get_tweets_like_prob;
extern uint32_t k_twitter_update_followers_prob;

// Ensemble configs
extern uint32_t k_ensemble_slot;
extern uint32_t k_ensemble_search_window;
extern uint32_t k_ensemble_linear_training_interval;
extern uint32_t k_ensemble_rnn_training_interval;
extern uint32_t k_ensemble_krls_training_interval;
extern double   k_ensemble_rnn_learning_rate;
extern double   k_ensemble_krls_learning_rate;
extern double   k_ensemble_outlier_threshold;
extern double   k_ensemble_krls_gamma;
extern uint32_t k_ensemble_rnn_epoch_count;
extern uint32_t k_ensemble_rnn_sequence_size;
extern uint32_t k_ensemble_rnn_layer_count;
extern bool     k_ensemble_apply_scheduled_outlier_model;

// site selector configs
extern uint64_t k_ss_bucket_size;

extern uint64_t k_astream_max_queue_size;
extern int32_t  k_astream_empty_wait_ms;
extern int32_t  k_astream_tracking_interval_ms;
extern int32_t  k_astream_mod;

extern double k_ss_max_write_blend_rate;

extern bool     k_ss_run_background_workers;
extern uint32_t k_ss_num_background_work_items_to_execute;

extern uint32_t k_estore_clay_periodic_interval_ms;
extern double   k_estore_load_epsilon_threshold;
extern double   k_estore_hot_partition_pct_thresholds;
extern double   k_adr_multiplier;

extern bool   k_ss_enable_plan_reuse;
extern double k_ss_plan_reuse_threshold;
extern bool   k_ss_enable_wait_for_plan_reuse;

extern partition_lock_mode k_ss_optimistic_lock_mode;
extern partition_lock_mode k_ss_physical_adjustment_lock_mode;
extern partition_lock_mode k_ss_force_change_lock_mode;

extern uint32_t k_num_site_selector_worker_threads_per_client;

extern ss_strategy_type  k_ss_strategy_type;
extern ss_mastering_type k_ss_mastering_type;

extern double  k_ss_skew_weight;
extern double  k_ss_skew_exp_constant;
extern double  k_ss_svv_delay_weight;
extern double  k_ss_single_site_within_transactions_weight;
extern double  k_ss_single_site_across_transactions_weight;
extern double  k_ss_wait_time_normalization_constant;
extern double  k_ss_split_partition_ratio;
extern double  k_ss_merge_partition_ratio;
extern int32_t k_ss_num_sampled_transactions_for_client;
extern int32_t k_ss_num_sampled_transactions_from_other_clients;
extern double  k_ss_upfront_cost_weight;
extern double  k_ss_scan_upfront_cost_weight;
extern double  k_ss_horizon_weight;
extern double  k_ss_default_horizon_benefit_weight;
extern double  k_ss_repartition_remaster_horizon_weight;
extern double  k_ss_horizon_storage_weight;
extern double  k_ss_repartition_prob_rescale;
extern double  k_ss_outlier_repartition_std_dev;
extern double  k_ss_split_partition_size_ratio;

extern double k_ss_plan_site_load_balance_weight;

extern double k_ss_plan_site_storage_balance_weight;
extern double k_ss_plan_site_storage_overlimit_weight;
extern double k_ss_plan_site_storage_variance_weight;
extern double k_ss_plan_site_memory_overlimit_weight;
extern double k_ss_plan_site_disk_overlimit_weight;
extern double k_ss_plan_site_memory_variance_weight;
extern double k_ss_plan_site_disk_variance_weight;

extern bool    k_ss_allow_force_site_selector_changes;

extern bool   k_ss_plan_scan_in_cost_order;
extern bool   k_ss_plan_scan_limit_changes;
extern double k_ss_plan_scan_limit_ratio;

extern std::vector<partition_type::type>    k_ss_acceptable_new_partition_types;
extern std::vector<partition_type::type>    k_ss_acceptable_partition_types;
extern std::vector<storage_tier_type::type> k_ss_acceptable_new_storage_types;
extern std::vector<storage_tier_type::type> k_ss_acceptable_storage_types;

extern bool                         k_use_query_arrival_predictor;
extern query_arrival_predictor_type k_query_arrival_predictor_type;
extern double                       k_query_arrival_access_threshold;
extern uint64_t                     k_query_arrival_time_bucket;
extern double                       k_query_arrival_count_bucket;
extern double                       k_query_arrival_time_score_scalar;
extern double                       k_query_arrival_default_score;

extern bool   k_cost_model_is_static_model;
extern double k_cost_model_learning_rate;
extern double k_cost_model_regularization;
extern double k_cost_model_bias_regularization;

extern double k_cost_model_momentum;
extern double k_cost_model_kernel_gamma;

extern uint32_t k_cost_model_max_internal_model_size_scalar;
extern uint32_t k_cost_model_layer_1_nodes_scalar;
extern uint32_t k_cost_model_layer_2_nodes_scalar;

extern double         k_cost_model_num_reads_weight;
extern double         k_cost_model_num_reads_normalization;
extern double         k_cost_model_num_reads_max_input;
extern double         k_cost_model_read_bias;
extern predictor_type k_cost_model_read_predictor_type;

extern double         k_cost_model_num_reads_disk_weight;
extern double         k_cost_model_read_disk_bias;
extern predictor_type k_cost_model_read_disk_predictor_type;

extern double         k_cost_model_scan_num_rows_weight;
extern double         k_cost_model_scan_num_rows_normalization;
extern double         k_cost_model_scan_num_rows_max_input;
extern double         k_cost_model_scan_selectivity_weight;
extern double         k_cost_model_scan_selectivity_normalization;
extern double         k_cost_model_scan_selectivity_max_input;
extern double         k_cost_model_scan_bias;
extern predictor_type k_cost_model_scan_predictor_type;

extern double         k_cost_model_scan_disk_num_rows_weight;
extern double         k_cost_model_scan_disk_selectivity_weight;
extern double         k_cost_model_scan_disk_bias;
extern predictor_type k_cost_model_scan_disk_predictor_type;

extern double         k_cost_model_num_writes_weight;
extern double         k_cost_model_num_writes_normalization;
extern double         k_cost_model_num_writes_max_input;
extern double         k_cost_model_write_bias;
extern predictor_type k_cost_model_write_predictor_type;

extern double         k_cost_model_lock_weight;
extern double         k_cost_model_lock_normalization;
extern double         k_cost_model_lock_max_input;
extern double         k_cost_model_lock_bias;
extern predictor_type k_cost_model_lock_predictor_type;

extern double         k_cost_model_commit_serialize_weight;
extern double         k_cost_model_commit_serialize_normalization;
extern double         k_cost_model_commit_serialize_max_input;
extern double         k_cost_model_commit_serialize_bias;
extern predictor_type k_cost_model_commit_serialize_predictor_type;

extern double         k_cost_model_commit_build_snapshot_weight;
extern double         k_cost_model_commit_build_snapshot_normalization;
extern double         k_cost_model_commit_build_snapshot_max_input;
extern double         k_cost_model_commit_build_snapshot_bias;
extern predictor_type k_cost_model_commit_build_snapshot_predictor_type;

extern double         k_cost_model_wait_for_service_weight;
extern double         k_cost_model_wait_for_service_normalization;
extern double         k_cost_model_wait_for_service_max_input;
extern double         k_cost_model_wait_for_service_bias;
extern predictor_type k_cost_model_wait_for_service_predictor_type;

extern double         k_cost_model_wait_for_session_version_weight;
extern double         k_cost_model_wait_for_session_version_normalization;
extern double         k_cost_model_wait_for_session_version_max_input;
extern double         k_cost_model_wait_for_session_version_bias;
extern predictor_type k_cost_model_wait_for_session_version_predictor_type;

extern double         k_cost_model_wait_for_session_snapshot_weight;
extern double         k_cost_model_wait_for_session_snapshot_normalization;
extern double         k_cost_model_wait_for_session_snapshot_max_input;
extern double         k_cost_model_wait_for_session_snapshot_bias;
extern predictor_type k_cost_model_wait_for_session_snapshot_predictor_type;

extern double         k_cost_model_site_load_prediction_write_weight;
extern double         k_cost_model_site_load_prediction_write_normalization;
extern double         k_cost_model_site_load_prediction_write_max_input;
extern double         k_cost_model_site_load_prediction_read_weight;
extern double         k_cost_model_site_load_prediction_read_normalization;
extern double         k_cost_model_site_load_prediction_read_max_input;
extern double         k_cost_model_site_load_prediction_update_weight;
extern double         k_cost_model_site_load_prediction_update_normalization;
extern double         k_cost_model_site_load_prediction_update_max_input;
extern double         k_cost_model_site_load_prediction_bias;
extern double         k_cost_model_site_load_prediction_max_scale;
extern predictor_type k_cost_model_site_load_predictor_type;

extern double         k_cost_model_site_operation_count_weight;
extern double         k_cost_model_site_operation_count_normalization;
extern double         k_cost_model_site_operation_count_max_input;
extern double         k_cost_model_site_operation_count_max_scale;
extern double         k_cost_model_site_operation_count_bias;
extern predictor_type k_cost_model_site_operation_count_predictor_type;

extern double         k_cost_model_distributed_scan_max_weight;
extern double         k_cost_model_distributed_scan_min_weight;
extern double         k_cost_model_distributed_scan_normalization;
extern double         k_cost_model_distributed_scan_max_input;
extern double         k_cost_model_distributed_scan_bias;
extern predictor_type k_cost_model_distributed_scan_predictor_type;

extern double k_cost_model_memory_num_rows_normalization;
extern double k_cost_model_memory_num_rows_max_input;

extern double         k_cost_model_memory_allocation_num_rows_weight;
extern double         k_cost_model_memory_allocation_bias;
extern predictor_type k_cost_model_memory_allocation_predictor_type;

extern double         k_cost_model_memory_deallocation_num_rows_weight;
extern double         k_cost_model_memory_deallocation_bias;
extern predictor_type k_cost_model_memory_deallocation_predictor_type;

extern double         k_cost_model_memory_assignment_num_rows_weight;
extern double         k_cost_model_memory_assignment_bias;
extern predictor_type k_cost_model_memory_assignment_predictor_type;

extern double k_cost_model_disk_num_rows_normalization;
extern double k_cost_model_disk_num_rows_max_input;

extern double k_cost_model_evict_to_disk_num_rows_weight;
extern double k_cost_model_evict_to_disk_bias;
extern predictor_type k_cost_model_evict_to_disk_predictor_type;

extern double k_cost_model_pull_from_disk_num_rows_weight;
extern double k_cost_model_pull_from_disk_bias;
extern predictor_type k_cost_model_pull_from_disk_predictor_type;

extern double k_cost_model_widths_weight;
extern double k_cost_model_widths_normalization;
extern double k_cost_model_widths_max_input;

extern double k_cost_model_wait_for_session_remaster_default_pct;

extern bool     k_ss_enum_enable_track_stats;
extern double   k_ss_enum_prob_of_returning_default;
extern double   k_ss_enum_min_threshold;
extern uint64_t k_ss_enum_min_count_threshold;
extern double   k_ss_enum_contention_bucket;
extern double   k_ss_enum_cell_width_bucket;
extern uint32_t k_ss_enum_num_entries_bucket;
extern double   k_ss_enum_num_updates_needed_bucket;
extern double   k_ss_enum_scan_selectivity_bucket;
extern uint32_t k_ss_enum_num_point_reads_bucket;
extern uint32_t k_ss_enum_num_point_updates_bucket;

extern uint32_t k_predictor_max_internal_model_size;

extern int32_t k_ss_update_stats_sleep_delay_ms;
extern int32_t k_ss_num_poller_threads;
extern int32_t k_ss_colocation_keys_sampled;
extern double  k_ss_partition_coverage_threshold;

extern update_destination_type k_update_destination_type;
extern update_source_type      k_update_source_type;
extern uint64_t                k_kafka_buffer_read_backoff;

extern uint32_t k_num_update_sources;
extern uint32_t k_num_update_destinations;
extern uint32_t k_sleep_time_between_update_enqueue_iterations;
extern uint32_t k_num_updates_before_apply_self;
extern uint32_t k_num_updates_to_apply_self;

extern bool    k_limit_number_of_records_propagated;
extern int32_t k_number_of_updates_needed_for_propagation;

// Early query prediction configs
extern int32_t k_spar_width;
extern int32_t k_spar_slot;
extern int32_t k_spar_training_interval;
extern int32_t k_spar_rounding_factor_temporal;
extern int32_t k_spar_rounding_factor_periodic;
extern int32_t k_spar_predicted_arrival_window;
extern double  k_spar_normalization_value;
extern double  k_spar_gamma;
extern double  k_spar_learning_rate;
extern double  k_spar_init_weight;
extern double  k_spar_init_bias;
extern double  k_spar_regularization;
extern double  k_spar_bias_regularization;
extern int32_t k_spar_batch_size;

// kafka configs
extern uint64_t k_kafka_poll_count;
extern uint64_t k_kafka_poll_sleep_ms;
extern uint32_t k_kafka_backoff_sleep_ms;
extern int32_t  k_kafka_poll_timeout;
extern int32_t  k_kafka_flush_wait_ms;
extern int32_t  k_kafka_consume_timeout_ms;
extern int32_t  k_kafka_seek_timeout_ms;

extern rewriter_type k_rewriter_type;
extern double        k_prob_range_rewriter_threshold;

extern bool    k_load_chunked_tables;
extern bool    k_persist_chunked_tables;
extern int32_t k_chunked_table_size;

extern bool        k_enable_secondary_storage;
extern std::string k_secondary_storage_dir;

extern int32_t k_sample_reservoir_size_per_client;
extern double  k_sample_reservoir_initial_multiplier;
extern double  k_sample_reservoir_weight_increment;
extern double  k_sample_reservoir_decay_rate;
extern int32_t k_force_change_reservoir_size_per_client;
extern double  k_force_change_reservoir_initial_multiplier;
extern double  k_force_change_reservoir_weight_increment;
extern double  k_force_change_reservoir_decay_rate;
extern int32_t k_reservoir_decay_seconds;

extern double   k_ss_storage_removal_threshold;
extern uint32_t k_ss_num_remove_replicas_iteration;

extern bool k_ss_allow_vertical_partitioning;
extern bool k_ss_allow_horizontal_partitioning;
extern bool k_ss_allow_change_partition_types;
extern bool k_ss_allow_change_storage_types;

// rdkafka configs see kafka_helpers for use
extern std::string k_kafka_fetch_max_ms;
extern std::string k_kafka_queue_max_ms;
extern std::string k_kafka_socket_blocking_max_ms;
extern std::string k_kafka_consume_max_messages;
extern std::string k_kafka_produce_max_messages;
extern std::string k_kafka_required_acks;
extern std::string k_kafka_send_max_retries;
extern std::string k_kafka_max_buffered_messages;
extern std::string k_kafka_max_buffered_messages_size;

extern int32_t k_timer_log_level;
extern bool    k_timer_log_level_on;
extern int32_t k_slow_timer_error_log_level;
extern int32_t k_cost_model_log_level;
extern int32_t k_heuristic_evaluator_log_level;
extern int32_t k_periodic_cost_model_log_level;
extern int32_t k_update_propagation_log_level;
extern int32_t k_significant_update_propagation_log_level;
extern int32_t k_ss_action_executor_log_level;
extern int32_t k_rpc_handler_log_level;

extern double k_slow_timer_log_time_threshold;
extern double k_cost_model_log_error_threshold;

extern std::chrono::system_clock::time_point k_process_start_time;

void init_constants();
