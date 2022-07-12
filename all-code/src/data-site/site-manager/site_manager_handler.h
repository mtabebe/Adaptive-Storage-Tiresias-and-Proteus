#pragma once

#include <glog/logging.h>

#include "../../common/system_stats.h"
#include "../../gen-cpp/gen-cpp/site_manager.h"
#include "../db/db.h"
#include "../stored-procedures/stored_procedures_executor.h"
#include "../update-propagation/update_enqueuers.h"
#include "client_state_information.h"
#include "serialize.h"
#include "sproc_lookup_table.h"

class site_manager_handler : virtual public site_managerIf {

   public:
    site_manager_handler( std::unique_ptr<db>                 db,
                          std::unique_ptr<sproc_lookup_table> sprocs,
                          void *                              sproc_opaque );
    ~site_manager_handler();

    void init( uint32_t num_clients, uint32_t num_client_threads,
               void *create_args = nullptr );

    void rpc_begin_transaction(
        begin_result &_return, const clientid id,
        const ::snapshot_vector &client_session_version_vector,
        const std::vector<partition_column_identifier> &write_set = {},
        const std::vector<partition_column_identifier> &read_set = {},
        const std::vector<partition_column_identifier> &inflight_set = {} );

    void rpc_prepare_transaction( commit_result &_return, const ::clientid id );

    void rpc_stored_procedure( sproc_result &_return, const ::clientid id,
                               const std::string &name,
                               const std::vector<cell_key_ranges>& write_ckrs,
                               const std::vector<cell_key_ranges>& read_ckrs,
                               const std::string &sproc_args );
    void rpc_scan( scan_result &_return, const ::clientid id,
                   const std::string &                name,
                   const std::vector<scan_arguments> &scan_args,
                   const std::string &                sproc_args );

    void rpc_select( query_result &_return, const ::clientid id,
                     const std::vector<::cell_key> &read_keys );
    void rpc_insert( query_result &_return, const ::clientid id,
                     const std::vector<::cell_key> & write_keys,
                     const std::vector<std::string> &write_vals );
    void rpc_delete( query_result &_return, const ::clientid id,
                     const std::vector<::cell_key> &delete_keys );
    void rpc_update( query_result &_return, const ::clientid id,
                     const std::vector<::cell_key> & write_keys,
                     const std::vector<std::string> &write_vals );

    void rpc_one_shot_sproc(
        one_shot_sproc_result &_return, const ::clientid id,
        const ::snapshot_vector &client_session_version_vector,
        const std::vector<::partition_column_identifier> &write_set,
        const std::vector<::partition_column_identifier> &read_set,
        const std::vector<::partition_column_identifier> &inflight_set,
        const std::string &name, const std::vector<cell_key_ranges> &write_ckrs,
        const std::vector<cell_key_ranges> &read_ckrs,
        const std::string &                 sproc_args );
    void rpc_one_shot_scan(
        one_shot_scan_result &_return, const ::clientid id,
        const ::snapshot_vector &client_session_version_vector,
        const std::vector<::partition_column_identifier> &read_set,
        const std::vector<::partition_column_identifier> &inflight_set,
        const std::string &name, const std::vector<scan_arguments> &scan_args,
        const std::string &sproc_args );

    void rpc_commit_transaction( commit_result &_return, const clientid id );
    void rpc_abort_transaction( abort_result &_return, const clientid id );

    /*
     * Grant mastership to a set of data items
     * Do this by first waiting until the session is satisfied and then
     * incrementing
     * the site svv to indicate that you have taken ownership
     */
    void rpc_grant_mastership(
        grant_result &_grant_res, const clientid id,
        const std::vector<partition_column_identifier> &pids,
        const std::vector<propagation_configuration> &new_prop_configs,
        const ::snapshot_vector &                session_version_vector );
    // this just wraps grant by converting bucket_keys to cell_keys
    /*
     * Release mastership of a set of data items
     * Do this by first waiting until all active transactions whose write sets
     * intersect have completed, then incrementing the site svv to indicate that
     * all active transactions have completed. The resulting svv is passed to
     * future grant ownerships as an indication that all updates have been
     * captured
     */
    void rpc_release_mastership(
        release_result &_release_res, const clientid id,
        const std::vector<partition_column_identifier> &pids, const int destination,
        const std::vector<propagation_configuration> &new_prop_configs,
        const ::snapshot_vector &                     session_version_vector );
    // this just wraps grant by converting bucket_keys to cell_keys

    void rpc_release_transfer_mastership(
        snapshot_partition_columns_results &_release_res, const clientid id,
        const std::vector<partition_column_identifier> &pids,
        const int                                       destination,
        const ::snapshot_vector &session_version_vector );
    void rpc_grant_transfer_mastership(
        grant_result &_grant_res, const clientid id,
        const std::vector<snapshot_partition_column_state> &snapshots,
        const std::vector<partition_type::type> &           p_types,
        const std::vector<storage_tier_type::type> &        s_types );

    void rpc_get_site_statistics( site_statistics_results &_site_stats_res );

    void rpc_persist_db_to_file(
        persistence_result &res, const clientid id, const std::string &out_name,
        const std::string &out_part_name,
        const std::vector<std::vector<propagation_configuration>>
            &site_propagation_configs );
    void rpc_restore_db_from_file(
        persistence_result &res, const clientid id, const std::string &in_name,
        const std::string &in_part_name,
        const std::vector<std::vector<propagation_configuration>>
            &site_propagation_configs );

    void rpc_add_partitions(
        commit_result &_return, const ::clientid id,
        const ::snapshot_vector &client_session_version_vector,
        const std::vector<::partition_column_identifier> &pids_to_add,
        const int32_t                                     master_location,
        const std::vector<partition_type::type> &         p_types,
        const std::vector<storage_tier_type::type> &      s_types,
        const ::propagation_configuration &               prop_config );
    void rpc_add_replica_partitions(
        commit_result &_return, const ::clientid id,
        const std::vector<snapshot_partition_column_state> &snapshots,
        const std::vector<partition_type::type> &           p_types,
        const std::vector<storage_tier_type::type> &        s_types );

    void rpc_snapshot_partitions(
        snapshot_partition_columns_results &_return, const ::clientid id,
        const std::vector<::partition_column_identifier> &pids );

    void rpc_remove_partitions(
        commit_result &_return, const ::clientid id,
        const ::snapshot_vector &client_session_version_vector,
        const std::vector<::partition_column_identifier> &pids_to_remove );
    void rpc_merge_partition(
        commit_result &_return, const ::clientid id,
        const ::snapshot_vector &            client_session_version_vector,
        const ::partition_column_identifier &lower_partition,
        const ::partition_column_identifier &higher_partition,
        const partition_type::type           merge_type,
        const storage_tier_type::type        merge_storage_type,
        const ::propagation_configuration &  prop_config );
    void rpc_split_partition(
        commit_result &_return, const ::clientid id,
        const ::snapshot_vector &            client_session_version_vector,
        const ::partition_column_identifier &partition,
        const ::cell_key &split_point, const partition_type::type low_type,
        const partition_type::type                      high_type,
        const storage_tier_type::type                   low_storage_type,
        const storage_tier_type::type                   high_storage_type,
        const std::vector<::propagation_configuration> &prop_configs );

    void rpc_change_partition_output_destination(
        commit_result &_return, const ::clientid id,
        const ::snapshot_vector &client_session_version_vector,
        const std::vector<::partition_column_identifier> &pids,
        const ::propagation_configuration &        new_prop_config );

    void rpc_change_partition_type(
        commit_result &_return, const ::clientid id,
        const std::vector<::partition_column_identifier> &pids,
        const std::vector<partition_type::type> &         part_types,
        const std::vector<storage_tier_type::type> &      storage_types );

   private:
    void rpc_stored_procedure_helper(
        sproc_result &_return, const ::clientid id, const std::string &name,
        const std::vector<cell_key_ranges> &write_ckrs,
        const std::vector<cell_key_ranges> &read_ckrs,
        const std::string &                 sproc_args );
    void rpc_scan_helper( scan_result &_return, const ::clientid id,
                          const std::string &                name,
                          const std::vector<scan_arguments> &scan_args,
                          const std::string &                sproc_args );

    template <class T_ret>
    void remaster(
        T_ret &_return, const clientid id,
        const std::vector<partition_column_identifier> &pids, uint32_t master_location,
        const std::vector<propagation_configuration> &new_prop_configs,
        const ::snapshot_vector &session_version_vector, bool do_master_check,
        const partition_lookup_operation &lookup_operation );

    void internal_rpc_begin_transaction(
        begin_result &_return, const clientid id,
        const ::snapshot_vector &client_session_version_vector,
        const std::vector<partition_column_identifier> &write_set = {},
        const std::vector<partition_column_identifier> &read_set = {},
        const std::vector<partition_column_identifier> &inflight_set = {},
        bool                                            allow_missing = false );

    void internal_rpc_prepare_transaction( commit_result &  _return,
                                           const ::clientid id );

    void internal_rpc_stored_procedure(
        sproc_result &_return, const ::clientid id, const std::string &name,
        const std::vector<cell_key_ranges> &write_ckrs,
        const std::vector<cell_key_ranges> &read_ckrs,
        const std::string &                 sproc_args );
    void internal_rpc_scan( scan_result &_return, const ::clientid id,
                            const std::string &                name,
                            const std::vector<scan_arguments> &scan_args,
                            const std::string &                sproc_args );

    void internal_rpc_select( query_result &_return, const ::clientid id,
                              const std::vector<::cell_key> &read_keys );
    void internal_rpc_insert( query_result &_return, const ::clientid id,
                              const std::vector<::cell_key> &write_keys,
                              const std::vector<std::string> &  write_vals );
    void internal_rpc_delete( query_result &_return, const ::clientid id,
                              const std::vector<::cell_key> &delete_keys );
    void internal_rpc_update( query_result &_return, const ::clientid id,
                              const std::vector<::cell_key> &write_keys,
                              const std::vector<std::string> &  write_vals );

    void internal_rpc_one_shot_sproc(
        one_shot_sproc_result &_return, const ::clientid id,
        const ::snapshot_vector &client_session_version_vector,
        const std::vector<::partition_column_identifier> &write_set,
        const std::vector<::partition_column_identifier> &read_set,
        const std::vector<partition_column_identifier> &  inflight_set,
        const std::string &name, const std::vector<cell_key_ranges> &write_ckrs,
        const std::vector<cell_key_ranges> &read_ckrs,
        const std::string &sproc_args );
    void internal_rpc_one_shot_scan(
        one_shot_scan_result &_return, const ::clientid id,
        const ::snapshot_vector &client_session_version_vector,
        const std::vector<::partition_column_identifier> &read_set,
        const std::vector<::partition_column_identifier> &inflight_set,
        const std::string &name, const std::vector<scan_arguments> &scan_args,
        const std::string &sproc_args );

    void internal_rpc_commit_transaction( commit_result &_return, const clientid id );
    void internal_rpc_abort_transaction( abort_result & _return,
                                         const clientid id );

    void internal_rpc_grant_mastership(
        grant_result &_grant_res, const clientid id,
        const std::vector<partition_column_identifier> &     pids,
        const std::vector<propagation_configuration> &new_prop_configs,
        const ::snapshot_vector &                     session_version_vector );
    void internal_rpc_release_mastership(
        release_result &_release_res, const clientid id,
        const std::vector<partition_column_identifier> &pids, const int destination,
        const std::vector<propagation_configuration> &new_prop_configs,
        const ::snapshot_vector &                     session_version_vector );

    void internal_rpc_release_transfer_mastership(
        snapshot_partition_columns_results &_release_res, const clientid id,
        const std::vector<partition_column_identifier> &pids, const int destination,
        const ::snapshot_vector &session_version_vector );
    void internal_rpc_grant_transfer_mastership(
        grant_result &_grant_res, const clientid id,
        const std::vector<snapshot_partition_column_state> &snapshots,
        const std::vector<partition_type::type> &           p_types,
        const std::vector<storage_tier_type::type> &        s_types );

    void internal_rpc_add_partitions(
        commit_result &_return, const ::clientid id,
        const ::snapshot_vector &client_session_version_vector,
        const std::vector<::partition_column_identifier> &pids_to_add,
        const int32_t                                     master_location,
        const std::vector<partition_type::type> &         p_types,
        const std::vector<storage_tier_type::type> &      s_types,
        const ::propagation_configuration &               prop_config );
    void internal_rpc_add_replica_partitions(
        commit_result &_return, const ::clientid id,
        const std::vector<snapshot_partition_column_state> &snapshots,
        const std::vector<partition_type::type> &           p_types,
        const std::vector<storage_tier_type::type> &        s_types );

    void internal_rpc_snapshot_partitions(
        snapshot_partition_columns_results &_return, const ::clientid id,
        const std::vector<::partition_column_identifier> &pids );

    void internal_rpc_remove_partitions(
        commit_result &_return, const ::clientid id,
        const ::snapshot_vector &client_session_version_vector,
        const std::vector<::partition_column_identifier> &pids_to_remove );
    void internal_rpc_merge_partition(
        commit_result &_return, const ::clientid id,
        const ::snapshot_vector &            client_session_version_vector,
        const ::partition_column_identifier &lower_partition,
        const ::partition_column_identifier &higher_partition,
        const partition_type::type &         merge_type,
        const storage_tier_type::type &      merge_storage_type,
        const ::propagation_configuration &  prop_config );
    void internal_rpc_split_partition(
        commit_result &_return, const ::clientid id,
        const ::snapshot_vector &            client_session_version_vector,
        const ::partition_column_identifier &partition,
        const ::cell_key &split_point, const partition_type::type &low_type,
        const partition_type::type &                    high_type,
        const storage_tier_type::type &                 low_storage_type,
        const storage_tier_type::type &                 high_storage_type,
        const std::vector<::propagation_configuration> &prop_configs );

    void internal_rpc_change_partition_output_destination(
        commit_result &_return, const ::clientid id,
        const ::snapshot_vector &client_session_version_vector,
        const std::vector<::partition_column_identifier> &pids,
        const ::propagation_configuration &        new_prop_config );

    void internal_rpc_change_partition_type(
        commit_result &_return, const ::clientid id,
        const std::vector<::partition_column_identifier> &pids,
        const std::vector<partition_type::type> &         part_types,
        const std::vector<storage_tier_type::type> &      storage_types );

    std::unique_ptr<db>                 db_;
    std::unique_ptr<sproc_lookup_table> sprocs_;
    client_state_information            cli_state_;
    system_stats                        mach_stats_;
    void *                              sproc_opaque_;
};
