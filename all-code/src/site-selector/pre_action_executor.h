#pragma once
#if 0 // MTODO-ESTORE
#include "adr_estore_clay_plan_evaluator.h"
#endif
#include "client_conn_pool.h"
#include "cost_modeller.h"
#include "partition_data_location_table.h"
#include "partition_tier_tracking.h"
#include "periodic_site_selector_operations.h"
#include "pre_transaction_plan.h"

class pre_action_executor {
   public:
    pre_action_executor(
        std::shared_ptr<client_conn_pools>             conn_pool,
        std::shared_ptr<partition_data_location_table> data_loc_tab,
        std::shared_ptr<cost_modeller2>                cost_model2,
        periodic_site_selector_operations* periodic_site_selector_operations,
        std::shared_ptr<sites_partition_version_information>
                                                 site_partition_version_info,
        std::shared_ptr<partition_tier_tracking> tier_tracking,
        const transaction_preparer_configs&      configs );
    ~pre_action_executor();

    void start_background_executor_threads();
    void stop_background_executor_threads();

    void add_background_work_items(
        const std::vector<std::shared_ptr<pre_action>>& actions );
    void execute_background_work_items( uint32_t tid );
    void execute_background_actions(
        uint32_t tid, std::vector<std::shared_ptr<pre_action>>& actions );

#if 0 // MTODO-ESTORE
    void execute_adr_estore_clay_work( uint32_t tid );
#endif

    action_outcome execute_pre_transaction_plan_action(
        std::shared_ptr<pre_transaction_plan> plan,
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id );
    action_outcome background_execute_pre_transaction_plan_action(
        const ::clientid id, const int32_t tid, const ::clientid translated_id,
        const std::vector<site_load_information>& site_load,
        std::shared_ptr<pre_action>               action,
        const partition_lock_mode&                lock_mode );

    // execute specific actions
    std::tuple<action_outcome, std::vector<context_timer>>
        execute_add_partitions_action(
            std::shared_ptr<pre_transaction_plan> plan,
            std::shared_ptr<pre_action> action, const ::clientid id,
            const int32_t tid, const ::clientid translated_id );
    std::tuple<action_outcome, std::vector<context_timer>>
        execute_add_replica_partitions_action(
            std::shared_ptr<pre_transaction_plan> plan,
            std::shared_ptr<pre_action> action, const ::clientid id,
            const int32_t tid, const ::clientid translated_id );
    std::tuple<action_outcome, std::vector<context_timer>>
        execute_remaster_partitions_action(
            std::shared_ptr<pre_transaction_plan> plan,
            std::shared_ptr<pre_action> action, const ::clientid id,
            const int32_t tid, const ::clientid translated_id );
    std::tuple<action_outcome, std::vector<context_timer>>
        execute_split_partition_action(
            std::shared_ptr<pre_transaction_plan> plan,
            std::shared_ptr<pre_action> action, const ::clientid id,
            const int32_t tid, const ::clientid translated_id );
    std::tuple<action_outcome, std::vector<context_timer>>
        execute_merge_partition_action(
            std::shared_ptr<pre_transaction_plan> plan,
            std::shared_ptr<pre_action> action, const ::clientid id,
            const int32_t tid, const ::clientid translated_id );
    std::tuple<action_outcome, std::vector<context_timer>>
        execute_change_partition_output_destination(
            std::shared_ptr<pre_transaction_plan> plan,
            std::shared_ptr<pre_action> action, const ::clientid id,
            const int32_t tid, const ::clientid translated_id );
    std::tuple<action_outcome, std::vector<context_timer>>
        execute_change_partition_type(
            std::shared_ptr<pre_transaction_plan> plan,
            std::shared_ptr<pre_action> action, const ::clientid id,
            const int32_t tid, const ::clientid translated_id );

    std::tuple<action_outcome, std::vector<context_timer>>
        execute_transfer_remaster_partition(
            std::shared_ptr<pre_transaction_plan> plan,
            std::shared_ptr<pre_action> action, const ::clientid id,
            const int32_t tid, const ::clientid translated_id );

   private:
    std::tuple<action_outcome, std::vector<context_timer>>
        add_partitions_to_data_site(
            const std::vector<partition_column_identifier>& pids,
            const std::vector<partition_type::type>&        partition_types,
            const std::vector<storage_tier_type::type>&     storage_types,
            const ::clientid id, const int32_t tid,
            const ::clientid translated_id, int master_site,
            const snapshot_vector& svv, uint32_t update_dest_slot );
    std::tuple<action_outcome, std::vector<context_timer>>
        internal_add_partitions_to_data_site(
            const std::vector<partition_column_identifier>& pids,
            const std::vector<partition_type::type>&        partition_types,
            const std::vector<storage_tier_type::type>&     storage_types,
            const ::clientid id, const int32_t tid,
            const ::clientid translated_id, int master_site,
            const snapshot_vector&           svv,
            const propagation_configuration& prop_config );
    std::tuple<action_outcome, std::vector<context_timer>>
        add_replica_partitions_to_data_sites(
            const std::vector<partition_column_identifier>& pids,
            const std::vector<partition_type::type>&        partition_types,
            const std::vector<storage_tier_type::type>&     storage_types,
            const ::clientid id, const int32_t tid,
            const ::clientid translated_id, int master_site, int destination );
    std::tuple<action_outcome, std::vector<context_timer>>
        remaster_partitions_at_data_sites(
            const std::vector<partition_column_identifier>       pids,
            const std::vector<propagation_configuration>& new_prop_configs,
            const ::clientid id, const int32_t tid,
            const ::clientid translated_id, int source, int destination,
            const snapshot_vector&                           svv,
            std::vector<std::shared_ptr<partition_payload>>& partitions );
    std::tuple<action_outcome, std::vector<context_timer>>
        remove_partitions_from_data_sites(
            const std::vector<partition_column_identifier>& pids, const ::clientid id,
            const int32_t tid, const ::clientid translated_id, int site,
            const snapshot_vector& svv );

    std::tuple<action_outcome, std::vector<context_timer>>
        split_partition_at_data_site(
            const ::clientid id, const int32_t tid,
            const ::clientid                   translated_id,
            const partition_column_identifier& pid, const cell_key& split_point,
            const partition_type::type&    low_type,
            const partition_type::type&    high_type,
            const storage_tier_type::type& low_storage_type,
            const storage_tier_type::type& high_storage_type, int destination,
            const snapshot_vector&                        svv,
            const std::vector<propagation_configuration>& new_prop_configs );
    std::tuple<action_outcome, std::vector<context_timer>>
        merge_partitions_at_data_site(
            const ::clientid id, const int32_t tid,
            const ::clientid                   translated_id,
            const partition_column_identifier& left_pid,
            const partition_column_identifier& right_pid,
            const partition_type::type&        merge_type,
            const storage_tier_type::type& merge_storage_type, int destination,
            const snapshot_vector&           svv,
            const propagation_configuration& prop_config );
    std::tuple<action_outcome, std::vector<context_timer>>
        change_partition_output_destination_at_data_site(
            const ::clientid, const int32_t tid, const ::clientid translated_id,
            const std::vector<partition_column_identifier>& pids, int destination,
            const propagation_configuration& prop_config,
            const snapshot_vector&           svv );

    std::tuple<action_outcome, std::vector<context_timer>>
        internal_execute_change_partition_type_action(
            std::shared_ptr<pre_action> action, const ::clientid id,
            const int32_t tid, const ::clientid translated_id,
            const snapshot_vector&                           svv,
            std::vector<std::shared_ptr<partition_payload>>& partitions );

    std::tuple<action_outcome, std::vector<context_timer>>
        change_partition_types_at_data_sites(
            const std::vector<partition_column_identifier> pids,
            const std::vector<partition_type::type>&       partition_types,
            const std::vector<storage_tier_type::type>&    storage_types,
            const ::clientid id, const int32_t tid,
            const ::clientid translated_id, int destination,
            const snapshot_vector&                           svv,
            std::vector<std::shared_ptr<partition_payload>>& partitions );

    std::tuple<action_outcome, std::vector<context_timer>>
        transfer_remaster_partitions_at_data_sites(
            const std::vector<partition_column_identifier> pids,
            const std::vector<partition_type::type>&       partition_types,
            const std::vector<storage_tier_type::type>&    storage_types,
            const ::clientid id, const int32_t tid,
            const ::clientid translated_id, int source, int destination,
            const snapshot_vector&                           svv,
            std::vector<std::shared_ptr<partition_payload>>& partitions );

    std::tuple<action_outcome, std::vector<context_timer>>
        internal_execute_remaster_partitions_action(
            std::shared_ptr<pre_action> action, const ::clientid id,
            const int32_t tid, const ::clientid translated_id,
            const snapshot_vector&                           svv,
            std::vector<std::shared_ptr<partition_payload>>& partitions );
    std::tuple<action_outcome, std::vector<context_timer>>
        internal_execute_add_replica_partitions_action(
            std::shared_ptr<pre_action> action, const ::clientid id,
            const int32_t tid, const ::clientid translated_id,
            std::vector<std::shared_ptr<partition_payload>>& partitions );
    std::tuple<action_outcome, std::vector<context_timer>>
        internal_execute_split_partition_action(
            std::shared_ptr<pre_action> action, const ::clientid id,
            const int32_t tid, const ::clientid translated_id,
            const snapshot_vector&                           svv,
            std::vector<std::shared_ptr<partition_payload>>& split_payloads );
    std::tuple<action_outcome, std::vector<context_timer>>
        internal_execute_merge_partition_action(
            std::shared_ptr<pre_action> action, const ::clientid id,
            const int32_t tid, const ::clientid translated_id,
            const snapshot_vector&                           svv,
            std::vector<std::shared_ptr<partition_payload>>& merge_payloads );
    std::tuple<action_outcome, std::vector<context_timer>>
        internal_execute_change_partition_output_destination(
            std::shared_ptr<pre_action> action, const ::clientid id,
            const int32_t tid, const ::clientid translated_id,
            const snapshot_vector&                           svv,
            std::vector<std::shared_ptr<partition_payload>>& partitions );
    std::tuple<action_outcome, std::vector<context_timer>>
        internal_execute_remove_partitions_action(
            std::shared_ptr<pre_action> action, const ::clientid id,
            const int32_t tid, const ::clientid translated_id,
            const snapshot_vector&                           svv,
            std::vector<std::shared_ptr<partition_payload>>& partitions );
    std::tuple<action_outcome, std::vector<context_timer>>
        internal_execute_transfer_remaster_partitions_action(
            std::shared_ptr<pre_action> action, const ::clientid id,
            const int32_t tid, const ::clientid translated_id,
            const snapshot_vector&                           svv,
            std::vector<std::shared_ptr<partition_payload>>& partitions );

    // execute specific actions in background
    action_outcome background_execute_add_replica_partitions_action(
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id,
        const std::vector<site_load_information>& site_load,
        const partition_lock_mode&                lock_mode );
    action_outcome background_execute_remaster_partitions_action(
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id,
        const std::vector<site_load_information>& site_load,
        const partition_lock_mode&                lock_mode );
    action_outcome background_execute_split_partition_action(
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id,
        const std::vector<site_load_information>& site_load,
        const partition_lock_mode&                lock_mode );
    action_outcome background_execute_merge_partition_action(
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id,
        const std::vector<site_load_information>& site_loads,
        const partition_lock_mode                 lock_mode );
    action_outcome background_execute_change_partition_output_destination(
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id,
        const std::vector<site_load_information>& site_load,
        const partition_lock_mode&                lock_mode );
    action_outcome background_execute_remove_partitions_action(
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id,
        const std::vector<site_load_information>& site_load,
        const partition_lock_mode&                lock_mode );
    action_outcome background_execute_transfer_remaster(
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id,
        const std::vector<site_load_information>& site_loads,
        const partition_lock_mode&                lock_mode );
    action_outcome background_execute_change_partition_type(
        std::shared_ptr<pre_action> action, const ::clientid id,
        const int32_t tid, const ::clientid translated_id,
        const std::vector<site_load_information>& site_loads,
        const partition_lock_mode&                lock_mode );

    void run_background_executor( uint32_t tid );

    void try_to_get_payloads(
        std::vector<partition_column_identifier>&               pids,
        std::vector<std::shared_ptr<partition_payload>>& found_payloads,
        const partition_lock_mode&                       lock_mode );

    void add_result_timers( std::shared_ptr<pre_action>&      action,
                            const std::vector<context_timer>& timer );

#if 0 // MTODO-ESTORE
    void execute_background_adr_or_estore_or_clay_worker( uint32_t tid );
#endif

    std::shared_ptr<client_conn_pools>             conn_pool_;
    std::shared_ptr<partition_data_location_table> data_loc_tab_;
    data_site_storage_stats*                       storage_stats_;
    std::shared_ptr<cost_modeller2>                cost_model2_;
    periodic_site_selector_operations* periodic_site_selector_operations_;
    std::shared_ptr<sites_partition_version_information>
                                 site_partition_version_info_;
    std::shared_ptr<partition_tier_tracking> tier_tracking_;
    transaction_preparer_configs configs_;

    std::priority_queue<std::shared_ptr<pre_action>,
                        std::vector<std::shared_ptr<pre_action>>,
                        pre_action_shared_ptr_comparator>
                             background_actions_;
    std::mutex               background_lock_;
    std::condition_variable  background_cv_;
    bool                     shutdown_;
    std::vector<std::thread> background_worker_threads_;

    partition_column_identifier_sort_functor pid_sorter_;
};
