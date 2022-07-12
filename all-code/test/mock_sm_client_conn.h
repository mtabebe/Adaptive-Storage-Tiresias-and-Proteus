#define GTEST_HAS_TR1_TUPLE 0

#include <gmock/gmock.h>

#include "../src/gen-cpp/gen-cpp/site_manager.h"

class mock_sm_client_conn : public site_managerIf {
   public:
    MOCK_METHOD6(
        rpc_begin_transaction,
        void(
            begin_result& _return, const ::clientid id,
            const ::snapshot_vector& client_session_version_vector,
            const std::vector<::partition_column_identifier>& write_set,
            const std::vector<::partition_column_identifier>& read_set,
            const std::vector<::partition_column_identifier>& inflight_set ) );
    MOCK_METHOD3( rpc_select,
                  void( query_result& _return, const ::clientid id,
                        const std::vector<::cell_key>& read_keys ) );
    MOCK_METHOD4( rpc_insert,
                  void( query_result& _return, const ::clientid id,
                        const std::vector<::cell_key>&  write_keys,
                        const std::vector<std::string>& write_vals ) );
    MOCK_METHOD3( rpc_delete,
                  void( query_result& _return, const ::clientid id,
                        const std::vector<::cell_key>& delete_keys ) );
    MOCK_METHOD4( rpc_update,
                  void( query_result& _return, const ::clientid id,
                        const std::vector<::cell_key>&  write_keys,
                        const std::vector<std::string>& write_vals ) );
    MOCK_METHOD6( rpc_stored_procedure,
                  void( sproc_result& _return, const ::clientid id,
                        const std::string&                    name,
                        const std::vector<::cell_key_ranges>& write_ckrs,
                        const std::vector<::cell_key_ranges>& read_ckrs,
                        const std::string&                    sproc_args ) );
    MOCK_METHOD5( rpc_scan, void( scan_result& _return, const ::clientid id,
                                  const std::string&                 name,
                                  const std::vector<scan_arguments>& scan_args,
                                  const std::string& sproc_args ) );

    MOCK_METHOD2( rpc_prepare_transaction,
                  void( commit_result& _return, const ::clientid id ) );
    MOCK_METHOD2( rpc_commit_transaction,
                  void( commit_result& _return, const ::clientid id ) );
    MOCK_METHOD2( rpc_abort_transaction,
                  void( abort_result& _return, const ::clientid id ) );
    MOCK_METHOD5( rpc_grant_mastership,
                  void( grant_result& _return, const ::clientid id,
                        const std::vector<::partition_column_identifier>& keys,
                        const std::vector<::propagation_configuration>&
                                                 new_propagation_configs,
                        const ::snapshot_vector& session_version_vector ) );
    MOCK_METHOD6( rpc_release_mastership,
                  void( release_result& _return, const ::clientid id,
                        const std::vector<::partition_column_identifier>& keys,
                        const int32_t destination,
                        const std::vector<propagation_configuration>&
                                                 new_propagation_configs,
                        const ::snapshot_vector& session_version_vector ) );

    MOCK_METHOD5( rpc_release_transfer_mastership,
                  void( snapshot_partition_columns_results& _release_res,
                        const clientid                      id,
                        const std::vector<partition_column_identifier>& pids,
                        const int                destination,
                        const ::snapshot_vector& session_version_vector ) );
    MOCK_METHOD5(
        rpc_grant_transfer_mastership,
        void( grant_result& _grant_res, const clientid id,
              const std::vector<snapshot_partition_column_state>& snapshots,
              const std::vector<partition_type::type>&            p_types,
              const std::vector<storage_tier_type::type>&         s_types ) );

    MOCK_METHOD10(
        rpc_one_shot_sproc,
        void( one_shot_sproc_result& _return, const ::clientid id,
              const ::snapshot_vector& client_session_version_vector,
              const std::vector<::partition_column_identifier>& write_set,
              const std::vector<::partition_column_identifier>& read_set,
              const std::vector<::partition_column_identifier>& inflight_set,
              const std::string&                                name,
              const std::vector<cell_key_ranges>&               write_ckrs,
              const std::vector<cell_key_ranges>&               read_ckrs,
              const std::string&                                sproc_args ) );
    MOCK_METHOD8(
        rpc_one_shot_scan,
        void( one_shot_scan_result& _return, const ::clientid id,
              const ::snapshot_vector& client_session_version_vector,
              const std::vector<::partition_column_identifier>& read_set,
              const std::vector<::partition_column_identifier>& inflight_set,
              const std::string&                                name,
              const std::vector<scan_arguments>&                scan_args,
              const std::string&                                sproc_args ) );

    MOCK_METHOD8(
        rpc_add_partitions,
        void( commit_result& _return, const ::clientid id,
              const ::snapshot_vector& client_session_version_vector,
              const std::vector<::partition_column_identifier>& pids_to_add,
              const int32_t                                     master_location,
              const std::vector<partition_type::type>&          p_types,
              const std::vector<storage_tier_type::type>&       s_types,
              const ::propagation_configuration&                prop_config ) );
    MOCK_METHOD5(
        rpc_add_replica_partitions,
        void( commit_result& _return, const ::clientid id,
              const std::vector<snapshot_partition_column_state>& snapshots,
              const std::vector<partition_type::type>&            p_types,
              const std::vector<storage_tier_type::type>&         s_types ) );
    MOCK_METHOD4( rpc_remove_partitions,
                  void( commit_result& _return, const ::clientid id,
                        const ::snapshot_vector& client_session_version_vector,
                        const std::vector<::partition_column_identifier>&
                            pids_to_remove ) );
    MOCK_METHOD8( rpc_merge_partition,
                  void( commit_result& _return, const ::clientid id,
                        const ::snapshot_vector& client_session_version_vector,
                        const ::partition_column_identifier& lower_partition,
                        const ::partition_column_identifier& higher_partition,
                        const ::partition_type::type         merge_type,
                        const ::storage_tier_type::type      merge_storage_type,
                        const propagation_configuration&     prop_config ) );
    MOCK_METHOD10(
        rpc_split_partition,
        void( commit_result& _return, const ::clientid id,
              const ::snapshot_vector& client_session_version_vector,
              const ::partition_column_identifier&          partition,
              const ::cell_key&                             split_point,
              const ::partition_type::type                  low_type,
              const ::partition_type::type                  high_type,
              const ::storage_tier_type::type               low_storage_type,
              const ::storage_tier_type::type               high_storage_type,
              const std::vector<propagation_configuration>& prop_configs ) );
    MOCK_METHOD3(
        rpc_snapshot_partitions,
        void( snapshot_partition_columns_results& _return, const ::clientid id,
              const std::vector<::partition_column_identifier>& pids ) );
    MOCK_METHOD1( rpc_get_site_statistics,
                  void( site_statistics_results& _return ) );
    MOCK_METHOD5(
        rpc_persist_db_to_file,
        void( persistence_result& _return, const ::clientid id,
              const std::string& out_name, const std::string& out_part_name,
              const std::vector<std::vector<propagation_configuration>>&
                  prop_configs ) );
    MOCK_METHOD5(
        rpc_restore_db_from_file,
        void( persistence_result& _return, const ::clientid id,
              const std::string& in_name, const std::string& in_part_name,
              const std::vector<std::vector<propagation_configuration>>&
                  prop_configs ) );

    MOCK_METHOD5( rpc_change_partition_output_destination,
                  void( commit_result& _return, const ::clientid id,
                        const ::snapshot_vector& client_session_version_vector,
                        const std::vector<::partition_column_identifier>& pids,
                        const ::propagation_configuration& new_prop_config ) );
    MOCK_METHOD5(
        rpc_change_partition_type,
        void( commit_result& _return, const ::clientid id,
              const std::vector<::partition_column_identifier>& pids,
              const std::vector<partition_type::type>&          part_types,
              const std::vector<storage_tier_type::type>& storage_types ) );
};
