#pragma once

#include "../../common/hw.h"
#include "../../gen-cpp/gen-cpp/proto_types.h"
#include "../update-propagation/update_destination_generator.h"
#include "../update-propagation/update_enqueuers.h"
#include "tables.h"


class db {
   public:
    db();
    ~db();

    void init( std::shared_ptr<update_destination_generator> update_gen,
               std::shared_ptr<update_enqueuers>             enqueuers,
               const tables_metadata&                        t_metadata );

    transaction_partition_holder* get_partition_holder(
        uint32_t client_id, const partition_column_identifier_set& write_pids,
        const partition_column_identifier_set&   read_pids,
        const partition_column_identifier_set&   do_not_apply_last_pids,
        const partition_lookup_operation& lookup_operation );
    transaction_partition_holder* get_partitions_with_begin(
        uint32_t client_id, const snapshot_vector& begin_timestamp,
        const partition_column_identifier_set&   write_pids,
        const partition_column_identifier_set&   read_pids,
        const partition_column_identifier_set&   do_not_apply_last_pids,
        const partition_lookup_operation& lookup_operation );

    void add_partition( uint32_t                           client_id,
                        const partition_column_identifier& pid,
                        uint32_t                           master_location,
                        const partition_type::type&        p_type,
                        const storage_tier_type::type&     s_type );
    void add_partition( uint32_t                           client_id,
                        const partition_column_identifier& pid,
                        uint32_t                           master_location,
                        const partition_type::type&        p_type,
                        const storage_tier_type::type&     s_type,
                        const propagation_configuration&   prop_config );

    void add_replica_partitions(
        uint32_t                                            cid,
        const std::vector<snapshot_partition_column_state>& snapshots,
        const std::vector<partition_type::type>&            p_type,
        const std::vector<storage_tier_type::type>&         s_type );

    void remove_partition( uint32_t                    client_id,
                           const partition_column_identifier& pid );
    std::tuple<bool, snapshot_vector> split_partition(
        uint32_t client_id, const snapshot_vector& snapshot,
        const partition_column_identifier& old_pid, uint64_t row_split_point,
        uint32_t col_split_point, const partition_type::type& low_type,
        const partition_type::type&                   high_type,
        const storage_tier_type::type&                low_storage_type,
        const storage_tier_type::type&                high_storage_type,
        const std::vector<propagation_configuration>& prop_configs );
    std::tuple<bool, snapshot_vector> merge_partition(
        uint32_t client_id, const snapshot_vector& snapshot,
        const partition_column_identifier& low_pid,
        const partition_column_identifier& high_pid,
        const partition_type::type&        merge_type,
        const storage_tier_type::type&     merge_storage_type,
        const propagation_configuration&   prop_config );

    std::tuple<bool, snapshot_vector> remaster_partitions(
        uint32_t client_id, const snapshot_vector& snapshot,
        const std::vector<partition_column_identifier>&      pids,
        const std::vector<propagation_configuration>& new_prop_configs,
        uint32_t new_master, bool is_release,
        const partition_lookup_operation& lookup_operation );

    std::tuple<bool, snapshot_partition_column_state> snapshot_partition(
        uint32_t client_id, const partition_column_identifier& pid );

    std::tuple<bool, snapshot_vector> change_partition_output_destination(
        uint32_t client_id, const ::snapshot_vector& snapshot,
        const std::vector<::partition_column_identifier>& pids,
        const ::propagation_configuration&         new_prop_config );
    std::tuple<bool, snapshot_vector> change_partition_types(
        uint32_t                                          client_id,
        const std::vector<::partition_column_identifier>& pids,
        const std::vector<partition_type::type>&          part_types,
        const std::vector<storage_tier_type::type>&       storage_types );

    std::vector<propagation_configuration> get_propagation_configurations();
    std::vector<int64_t>                   get_propagation_counts();

    std::vector<std::vector<cell_widths_stats>> get_column_widths() const;
    std::vector<std::vector<selectivity_stats>> get_selectivities() const;
    partition_column_identifier_map_t<storage_tier_type::type>
        get_storage_tier_changes() const;
    std::vector<::storage_tier_change> get_changes_to_storage_tiers() const;

    tables* get_tables();
    uint32_t get_site_location() const;
    std::shared_ptr<update_destination_generator>
        get_update_destination_generator();

    void get_update_partition_states(
        std::vector<polled_partition_column_version_information>& polled_versions );

    bool snapshot_and_remove_partitions(
        uint32_t client_id, const partition_column_identifier_set& pids,
        uint32_t                               new_master_location,
        std::vector<snapshot_partition_column_state>& snapshot_state );

    std::tuple<bool, snapshot_vector> add_partitions_from_snapshots(
        uint32_t                                            client_id,
        const std::vector<snapshot_partition_column_state>& snapshot_states,
        const std::vector<partition_type::type>&            p_types,
        const std::vector<storage_tier_type::type>&         s_types,
        uint32_t                                            master_location );

    cell_data_type get_cell_data_type( const cell_identifier& cid ) const;

   private:
    void start_gc_worker();
    void stop_gc_worker();
    void run_gc_worker_thread();
    void do_gc_work();

    std::tuple<bool, std::shared_ptr<partition>> add_replica_partition(
        uint32_t client_id, const snapshot_vector& snapshot,
        const partition_column_identifier& pid, uint32_t master_location,
        const std::vector<snapshot_column_state>& columns,
        const partition_type::type&               p_type,
        const storage_tier_type::type&            s_type );

    void add_partition(
        uint32_t client_id, const partition_column_identifier& pid,
        uint32_t master_location, const partition_type::type& p_type,
        const storage_tier_type::type&                s_type,
        std::shared_ptr<update_destination_interface> update_destination );

    std::tuple<bool, std::shared_ptr<partition>, uint64_t>
        change_partition_type( uint32_t                           client_id,
                               const partition_column_identifier& pid,
                               const partition_type::type&        part_type,
                               const storage_tier_type::type&     tier_type );

    tables tables_;

    std::shared_ptr<update_destination_generator> update_gen_;
    std::shared_ptr<update_enqueuers> enqueuers_;

    tables_metadata tables_metadata_;

    std::unique_ptr<std::thread> gc_worker_thread_;
    std::mutex                   worker_mutex_;
    std::condition_variable      worker_cv_;
    bool                         done_;
};
