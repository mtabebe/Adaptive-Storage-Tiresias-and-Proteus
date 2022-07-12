#pragma once

#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "../gen-cpp/gen-cpp/SiteSelector.h"
#include "access_stream.h"
#include "adaptive_reservoir_sampler.h"
#include "client_conn_pool.h"
#include "cost_modeller.h"
#include "multi_version_partition_table.h"
#include "partition_access.h"
#include "partition_data_location_table.h"
#include "periodic_site_selector_operations.h"
#include "pre_action_executor.h"
#include "pre_transaction_plan.h"
#include "query_arrival_predictor.h"
#include "site_evaluator.h"
#include "site_selector_metadata.h"

typedef std::vector<std::unique_ptr<std::thread>> ThreadPtrs;

enum plan_strategy { ANY, NO_CHANGE, CHANGE_POSSIBLE, FORCE_CHANGE };

typedef std::tuple<int, plan_strategy> execution_result;

// A class that prepares the system for running a client's transactions
// To do so, it monitors system state, decides on the best site for transaction
// execution,
// and moves data to that site to allow for single-site execution
class transaction_preparer {
   public:
    transaction_preparer(
        std::shared_ptr<client_conn_pools>             conn_pool,
        std::shared_ptr<partition_data_location_table> tab,
        std::shared_ptr<cost_modeller2>                cost_model2,
        std::shared_ptr<sites_partition_version_information>
                                                        site_partition_version_info,
        std::unique_ptr<abstract_site_evaluator>        evaluator,
        std::shared_ptr<query_arrival_predictor>        query_predictor,
        std::shared_ptr<stat_tracked_enumerator_holder> stat_enumerator_holder,
        std::shared_ptr<partition_tier_tracking>        tier_tracking,
        const transaction_preparer_configs&             configs );
    ~transaction_preparer();

    // Prepare the system for transaction execution
    // Client id wants to execute a transaction with write set items write_set
    // at the given *start* svv
    int prepare_transaction(
        const ::clientid                      id,
        const std::vector<::cell_key_ranges>& ckr_write_set,
        const std::vector<::cell_key_ranges>& ckr_read_set,
        const ::snapshot_vector&              svv );

    std::unordered_map<int, std::vector<::cell_key_ranges>>
        prepare_multi_site_transaction(
            const ::clientid                      id,
            const std::vector<::cell_key_ranges>& ckr_write_set,
            const std::vector<::cell_key_ranges>& ckr_read_set,
            const std::vector<::cell_key_ranges>& ckr_full_replica_set,
            const ::snapshot_vector&              svv );

    // returns the site of the executed transaction
    std::vector<int> execute_one_shot_scan(
        one_shot_scan_result& _return, const ::clientid id,
        const ::snapshot_vector& svv, const std::string& name,
        const std::vector<scan_arguments>& scan_args,
        const std::string& sproc_args, bool allow_missing_data,
        bool allow_force_change );

    // returns the site of the executed transaction
    int execute_one_shot_sproc(
        one_shot_sproc_result& _return, const ::clientid id,
        const ::snapshot_vector&              svv,
        const std::vector<::cell_key_ranges>& ckr_write_set,
        const std::vector<::cell_key_ranges>& ckr_read_set,
        const std::string& name, const std::string& sproc_args,
        bool allow_force_change );
    int execute_one_shot_sproc_for_multi_site_system(
        one_shot_sproc_result& _return, const ::clientid id,
        const ::snapshot_vector&              client_session_version_vector,
        const std::vector<::cell_key_ranges>& ckr_write_set,
        const std::vector<::cell_key_ranges>& ckr_read_set,
        const std::string& name, const std::string& sproc_args );

    std::vector<tracking_summary>& generate_txn_sample(::clientid id = -1 );
    abstract_site_evaluator*    get_evaluator();
    adaptive_reservoir_sampler* get_sampler(::clientid id );

    bool is_single_site_transaction_system() const;

    void restore_state_from_files( const ::clientid   id,
                                   const std::string& in_ss_name,
                                   const std::string& in_db_name,
                                   const std::string& in_part_name );
    void persist_state_to_files( const ::clientid   id,
                                 const std::string& out_ss_name,
                                 const std::string& out_db_name,
                                 const std::string& out_part_name );

    void add_previous_context_timer_information(
        const ::clientid                          id,
        const previous_context_timer_information& previous_contexts );

   private:
    execution_result prepare_transaction_with_strategy(
        const std::vector<::cell_key_ranges>& sorted_ckr_write_set,
        const std::vector<::cell_key_ranges>& sorted_ckr_read_set,
        const ::clientid id, const ::snapshot_vector& svv,
        const plan_strategy& strategy );
    execution_result execute_one_shot_sproc_with_strategy(
        const std::vector<::cell_key_ranges>& sorted_ckr_write_set,
        const std::vector<::cell_key_ranges>& sorted_ckr_read_set,
        one_shot_sproc_result& _return, const ::clientid id,
        const ::snapshot_vector& svv, const std::string& name,
        const std::string& sproc_args, const plan_strategy& strategy );

    // the true helper
    std::shared_ptr<pre_transaction_plan>
        prepare_transaction_data_items_for_single_site_execution(
            const ::clientid id, ::snapshot_vector& svv,
            const std::vector<::cell_key_ranges>& sorted_ckr_write_set,
            const std::vector<::cell_key_ranges>& sorted_ckr_read_set,
            plan_strategy&                        strategy );
    // wait
    void wait_for_downgrade_to_complete_and_release_partitions(
        const ::clientid id, std::shared_ptr<pre_transaction_plan> plan,
        const plan_strategy& actual_strategy );

    // generate some plans
    std::shared_ptr<pre_transaction_plan> get_pre_transaction_execution_plan(
        const ::clientid id, grouped_partition_information& group_info,
        std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
        ::snapshot_vector& svv, const plan_strategy& strategy );

    std::shared_ptr<pre_transaction_plan>
        generate_no_change_destination_plan_if_possible(
            const grouped_partition_information&     grouped_info,
            std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
            ::snapshot_vector&                       svv );
    std::shared_ptr<pre_transaction_plan>
        generate_no_change_destination_plan_if_possible_or_wait_for_plan_reuse(
            const grouped_partition_information&     grouped_info,
            std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
            ::snapshot_vector&                       svv );

    std::tuple<std::shared_ptr<pre_transaction_plan>, bool>
        generate_optimistic_no_change_plan(
            const std::vector<::cell_key_ranges>&    sorted_ckr_write_set,
            const std::vector<::cell_key_ranges>&    sorted_ckr_read_set,
            std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
            ::snapshot_vector&                       svv );
    std::shared_ptr<pre_transaction_plan>
        generate_plan_with_physical_adjustments(
            const ::clientid                         id,
            const std::vector<::cell_key_ranges>&    sorted_ckr_write_set,
            const std::vector<::cell_key_ranges>&    sorted_ckr_read_set,
            std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
            ::snapshot_vector& svv, const plan_strategy& strategy,
            bool need_new_partitions );

    // execute plan & actions
    action_outcome execute_pre_transaction_plan(
        std::shared_ptr<pre_transaction_plan> plan, const ::clientid id,
        ::snapshot_vector& svv );
    void execute_pre_transaction_plan_actions(
        const ::clientid id, const int32_t tid, const ::clientid translated_id,
        std::shared_ptr<pre_transaction_plan> plan );

    void add_plan_to_multi_query_tracking(
        std::shared_ptr<pre_transaction_plan>& plan ) const;
    void add_scan_plan_to_multi_query_tracking(
        std::shared_ptr<multi_site_pre_transaction_plan>& plan ) const;

    void sample_transaction_accesses(
        ::clientid id, std::shared_ptr<pre_transaction_plan>& plan,
        const std::vector<::cell_key_ranges>& ckr_write_set,
        const std::vector<::cell_key_ranges>& ckr_read_set,
        const high_res_time&                  time_of_exec );
    void sample_transaction_accesses(
        ::clientid id, std::shared_ptr<multi_site_pre_transaction_plan>& plan,
        const std::vector<::cell_key_ranges>& ckr_write_set,
        const std::vector<::cell_key_ranges>& ckr_read_set,
        const high_res_time&                  time_of_exec );

    void increase_tracking_counts(
        std::shared_ptr<pre_transaction_plan>& plan );
    void increase_tracking_counts(
        std::shared_ptr<multi_site_pre_transaction_plan>& plan );
    void increase_tracking_counts(
        int destination, std::shared_ptr<pre_transaction_plan>& plan,
        const std::vector<partition_column_identifier>& read_pids,
        const std::vector<partition_column_identifier>& write_pids );

    transaction_partition_accesses generate_txn_partition_accesses_from_plan(
        std::shared_ptr<pre_transaction_plan>& plan,
        const std::vector<::cell_key_ranges>   write_ckrs,
        const std::vector<::cell_key_ranges>   read_ckrs );
    transaction_partition_accesses generate_txn_partition_accesses_from_plan(
        std::shared_ptr<multi_site_pre_transaction_plan>& plan,
        const std::vector<::cell_key_ranges>              write_ckrs,
        const std::vector<::cell_key_ranges>              read_ckrs );

    void add_tracking_for_multi_site(
        const ::clientid id, const grouped_partition_information& group_info,
        const std::unordered_map<int, per_site_grouped_partition_information>&
            per_site_groupings );
    void sample_transaction_accesses(
        ::clientid id,
        const std::unordered_map<int, per_site_grouped_partition_information>&
                                              per_site_groupings,
        const std::vector<::cell_key_ranges>& ckr_write_set,
        const std::vector<::cell_key_ranges>& ckr_read_set,
        const high_res_time&                  time_of_exec );
    transaction_partition_accesses generate_txn_partition_accesses(
        const std::vector<partition_column_identifier>& write_pids,
        const std::vector<partition_column_identifier>& read_pids,
        int                                             destination,
        const partition_column_identifier_map_t<int>&   destinations,
        const std::vector<::cell_key_ranges>            write_ckrs,
        const std::vector<::cell_key_ranges>            read_ckrs );

    void init_client_stream( int32_t id, high_res_time t,
                             bool add_access_stream );

    void persist_site_selector(
        const ::clientid id, const std::string& dir,
        const std::vector<std::vector<propagation_configuration>>&
            prop_configs );
    void restore_site_selector(
        const ::clientid id, const std::string& dir,
        const std::vector<std::vector<propagation_configuration>>&
            prop_configs );

    std::vector<transaction_prediction_stats> get_previous_requests_stats(
        const ::clientid id, const uint32_t site,
        const std::vector<cell_key_ranges>& write_ckrs,
        const std::vector<int32_t>&         write_ckr_counts,
        const std::vector<cell_key_ranges>& read_ckrs,
        const std::vector<int32_t>&         read_ckr_counts ) const;
    void add_to_stats(
        const ::clientid id, const uint32_t site,
        const std::vector<cell_key_ranges>& ckrs,
        const std::vector<int32_t>& ckr_counts, bool is_write,
        std::vector<transaction_prediction_stats>& txn_stats ) const;

    void add_replica_to_all_sites(
        std::shared_ptr<pre_transaction_plan>& plan, int master,
        const std::vector<std::shared_ptr<partition_payload>>& new_partitions,
        const std::shared_ptr<pre_action>&                     add_action );

    grouped_partition_information
        get_partitions_and_create_if_necessary_for_multi_site_transactions(
            const ::clientid                      id,
            const std::vector<::cell_key_ranges>& ckr_write_set,
            const std::vector<::cell_key_ranges>& ckr_read_set,
            const std::vector<::cell_key_ranges>& ckr_full_replica_set,
            bool is_fully_replicated, snapshot_vector& svv,
            const partition_lock_mode& lock_mode );
    void create_multi_site_partitions(
        const ::clientid                      id,
        const std::vector<::cell_key_ranges>& sorted_ckr_write_set,
        const std::vector<::cell_key_ranges>& sorted_ckr_read_set,
        snapshot_vector& svv, bool is_fully_replicated );
    void group_data_items_for_multi_site_transaction(
        const ::clientid id, const grouped_partition_information& group_info,
        std::unordered_map<int, per_site_grouped_partition_information>&
            per_site_groupings,
        std::unordered_map<int, std::vector<::cell_key_ranges>>& destinations,
        std::unordered_map<int, std::vector<::cell_key_ranges>>&
            write_destinations,
        std::unordered_map<int, std::vector<::cell_key_ranges>>&
                                        read_destinations,
        std::vector<::cell_key_ranges>& full_replica_read_ckrs,
        bool                            is_fully_replicated );

    void generate_per_site_adr_grouping(
        const grouped_partition_information& group_info,
        std::unordered_map<int, per_site_grouped_partition_information>&
            per_site_groupings,
        std::unordered_map<int, std::vector<::cell_key_ranges>>& destinations,
        std::unordered_map<int, std::vector<::cell_key_ranges>>&
            write_destinations_ckr,
        std::unordered_map<int, std::vector<::cell_key_ranges>>&
                                        read_destinations_ckr,
        std::vector<::cell_key_ranges>& full_replica_read_ckrs,
        bool                            is_fully_replicated );
    void add_to_adr_destinations(
        int site, const partition_column_identifier& pid,
        const grouped_partition_information& group_info,
        std::unordered_map<int, per_site_grouped_partition_information>&
            per_site_groupings,
        std::unordered_map<int, std::vector<::cell_key_ranges>>& destinations,
        std::unordered_map<int, std::vector<::cell_key_ranges>>&
            type_destinations,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>&
             pids_to_parts,
        bool is_write );
    void add_to_adr_per_site_group_data_items(
        std::shared_ptr<partition_payload>&  part,
        const grouped_partition_information& group_info,
        std::unordered_map<int, partition_column_identifier_unordered_set>&
            destinations,
        partition_column_identifier_map_t<std::unordered_set<int>>&
                                        part_locations,
        std::vector<::cell_key_ranges>& full_replica_read_ckrs, bool is_write,
        bool is_fully_replicated );

    exec_status_type::type execute_multi_site_begin(
        const ::clientid id, int site,
        const std::vector<partition_column_identifier>& write_pids,
        const std::vector<partition_column_identifier>& read_pids,
        const snapshot_vector&                          transaction_begin_svv,
        const partition_lock_mode&                      lock_mode );
    void add_to_per_site_group_data_items(
        std::shared_ptr<partition_payload>&  part,
        const grouped_partition_information& group_info,
        std::unordered_map<int, per_site_grouped_partition_information>&
            per_site_groupings,
        std::unordered_map<int, std::vector<::cell_key_ranges>>& destinations,
        std::unordered_map<int, std::vector<::cell_key_ranges>>&
                                        type_destinations,
        std::vector<::cell_key_ranges>& full_replica_read_ckrs, bool is_write,
        bool is_fully_replicated );

    int execute_one_shot_sproc_at_site(
        const ::clientid id, int destination, one_shot_sproc_result& _return,
        const snapshot_vector&                          svv,
        const std::vector<partition_column_identifier>& write_pids,
        const std::vector<partition_column_identifier>& read_pids,
        const std::vector<partition_column_identifier>& inflight_pids,
        const std::string& name, const std::vector<cell_key_ranges>& write_ckrs,
        const std::vector<cell_key_ranges>& read_ckrs,
        const std::string& sproc_args, const plan_strategy& actual_strategy );

    std::vector<int> execute_one_shot_scan_at_sites(
        const ::clientid id, one_shot_scan_result& _return,
        const snapshot_vector&                            svv,
        std::shared_ptr<multi_site_pre_transaction_plan>& plan,
        const std::string& name, const std::vector<cell_key_ranges>& read_ckrs,
        const std::vector<scan_arguments>& scan_args,
        const std::string& sproc_args, const plan_strategy& actual_strategy );
    void execute_one_shot_scan_at_site(
        const ::clientid id, int destination, one_shot_scan_result& _return,
        const snapshot_vector&                          svv,
        const std::vector<partition_column_identifier>& read_pids,
        const std::vector<partition_column_identifier>& inflight_pids,
        const std::string& name, const std::vector<scan_arguments>& scan_args,
        const std::string& sproc_args );
    std::shared_ptr<multi_site_pre_transaction_plan>
        prepare_transaction_data_items_for_scan_execution(
            const ::clientid id, ::snapshot_vector& svv,
            const std::vector<::cell_key_ranges>& sorted_ckr_write_set,
            const std::vector<::cell_key_ranges>& sorted_ckr_read_set,
            const std::vector<scan_arguments>& scan_args, bool allow_missing,
            plan_strategy& strategy );
    std::shared_ptr<multi_site_pre_transaction_plan>
        generate_scan_plan_with_physical_adjustments(
            const ::clientid                         id,
            const std::vector<::cell_key_ranges>&    sorted_ckr_write_set,
            const std::vector<::cell_key_ranges>&    sorted_ckr_read_set,
            const std::vector<scan_arguments>&       scan_args,
            std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
            ::snapshot_vector& svv, const plan_strategy& strategy,
            bool need_new_parts, bool allow_missing );
    std::tuple<std::shared_ptr<multi_site_pre_transaction_plan>, bool>
        generate_optimistic_no_change_scan_plan(
            const ::clientid                         id,
            const std::vector<::cell_key_ranges>&    ckr_write_set,
            const std::vector<::cell_key_ranges>&    ckr_read_set,
            const std::vector<scan_arguments>&       scan_args,
            std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
            bool allow_missing, ::snapshot_vector& svv );
    std::shared_ptr<multi_site_pre_transaction_plan>
        generate_no_change_destination_scan_plan_if_possible_or_wait_for_plan_reuse(
            const ::clientid                     id,
            const grouped_partition_information& grouped_info,
            const std::vector<scan_arguments>&   scan_args,
            const std::unordered_map<uint32_t /* label*/,
                                     partition_column_identifier_to_ckrs>&
                labels_to_pids_and_ckrs,
            const partition_column_identifier_map_t<std::unordered_map<
                uint32_t /* label */, std::vector<cell_key_ranges>>>&
                pids_to_labels_and_ckrs,
            const std::unordered_map<uint32_t /* label */, uint32_t /* pos */>&
                label_to_pos,
            std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
            ::snapshot_vector&                       svv );
    std::shared_ptr<multi_site_pre_transaction_plan>
        generate_no_change_destination_scan_plan_if_possible(
            const ::clientid                     id,
            const grouped_partition_information& grouped_info,
            const std::vector<scan_arguments>&   scan_args,
            const std::unordered_map<uint32_t /* label*/,
                                     partition_column_identifier_to_ckrs>&
                labels_to_pids_and_ckrs,
            const partition_column_identifier_map_t<std::unordered_map<
                uint32_t /* label */, std::vector<cell_key_ranges>>>&
                pids_to_labels_and_ckrs,
            const std::unordered_map<uint32_t /* label */, uint32_t /* pos */>&
                                                     label_to_pos,
            std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
            ::snapshot_vector&                       svv );
    std::shared_ptr<multi_site_pre_transaction_plan>
        get_pre_transaction_execution_scan_plan(
            const ::clientid id, grouped_partition_information& grouped_info,
            const std::vector<scan_arguments>& scan_args,
            const std::unordered_map<uint32_t /* label*/,
                                     partition_column_identifier_to_ckrs>&
                labels_to_pids_and_ckrs,
            const partition_column_identifier_map_t<std::unordered_map<
                uint32_t /* label */, std::vector<cell_key_ranges>>>&
                pids_to_labels_and_ckrs,
            const std::unordered_map<uint32_t /* label */, uint32_t /* pos */>&
                                                     label_to_pos,
            std::shared_ptr<multi_query_plan_entry>& mq_plan_entry,
            ::snapshot_vector& svv, const plan_strategy& strategy );
    void build_scan_argument_mappings(
        const std::vector<scan_arguments>&   scan_args,
        const grouped_partition_information& grouped_info,
        std::unordered_map<uint32_t /* label*/,
                           partition_column_identifier_to_ckrs>&
            labels_to_pids_and_ckrs,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t /* label */, std::vector<cell_key_ranges>>>&
            pids_to_labels_and_ckrs,
        std::unordered_map<uint32_t /* label */, uint32_t /* pos */>&
            label_to_pos ) const;

    std::shared_ptr<client_conn_pools>             conn_pool_;
    std::shared_ptr<partition_data_location_table> data_loc_tab_;
    std::shared_ptr<cost_modeller2>                cost_model2_;
    std::shared_ptr<sites_partition_version_information>
                                                    site_partition_version_info_;
    std::unique_ptr<abstract_site_evaluator>        evaluator_;
    std::shared_ptr<query_arrival_predictor>        query_predictor_;
    std::shared_ptr<stat_tracked_enumerator_holder> stat_enumerator_holder_;
    std::shared_ptr<partition_tier_tracking>        tier_tracking_;

    transaction_preparer_configs             configs_;
    mastering_type_configs                   mastering_type_configs_;
    std::vector<std::unique_ptr<adaptive_reservoir_sampler>> samplers_;
    std::vector<std::unique_ptr<access_stream>>              access_streams_;
    std::vector<high_res_time>        last_sampled_txn_start_points_;
    std::chrono::milliseconds         tracking_duration_;
    periodic_site_selector_operations periodic_site_selector_operations_;

    pre_action_executor action_executor_;
};
