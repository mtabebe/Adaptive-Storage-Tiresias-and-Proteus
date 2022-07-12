#pragma once

#include "../../site-selector/partition_data_location_table.h"
#include "../../site-selector/site_selector_executor.h"

#include "../../templates/single_site_db_types.h"
#include "../db/db.h"
#include "../db/partition.h"

single_site_db_types class single_site_db {
   public:
    single_site_db();
    ~single_site_db();

    void init( std::shared_ptr<update_destination_generator> update_gen,
               std::shared_ptr<update_enqueuers>             enqueuers,
               const tables_metadata&                        t_meta );

    db* get_database();

    std::shared_ptr<partition_data_location_table> get_data_location_table();

    uint32_t create_table( const table_metadata& metadata );

    transaction_partition_holder* get_partitions_with_begin(
        uint32_t client_id, const snapshot_vector& begin_timestamp,
        const std::vector<cell_key_ranges>& write_keys,
        const std::vector<cell_key_ranges>& read_keys, bool allow_missing );
    transaction_partition_holder* get_partitions_with_begin(
        uint32_t client_id, const snapshot_vector& begin_timestamp,
        const partition_column_identifier_set& write_pids,
        const partition_column_identifier_set& read_pids,
        const partition_column_identifier_set& inflight_pids,
        bool                                   allow_missing );

    void change_partition_types(
        uint32_t                                          client_id,
        const std::vector<::partition_column_identifier>& pids,
        const std::vector<partition_type::type>&          part_types,
        const std::vector<storage_tier_type::type>&       storage_types );

    void add_partition( uint32_t                           client_id,
                        const partition_column_identifier& pid,
                        uint32_t                           master_location,
                        const partition_type::type&        p_type,
                        const storage_tier_type::type&     s_type );
    void remove_partition( uint32_t                           client_id,
                           std::shared_ptr<partition_payload> partition );

    snapshot_vector split_partition(
        uint32_t client_id, const snapshot_vector& snapshot,
        std::shared_ptr<partition_payload> partition, uint64_t row_split_point,
        uint32_t col_split_point, const partition_type::type& low_type,
        const partition_type::type&    high_type,
        const storage_tier_type::type& low_storage_type,
        const storage_tier_type::type& high_storage_type );

    snapshot_vector merge_partition(
        uint32_t client_id, const snapshot_vector& snapshot,
        std::shared_ptr<partition_payload> low_partition,
        std::shared_ptr<partition_payload> high_partition,
        const partition_type::type&        merge_type,
        const storage_tier_type::type&     merge_storage_type );

    snapshot_vector remaster_partitions(
        uint32_t client_id, const snapshot_vector& snapshot,
        std::vector<std::shared_ptr<partition_payload>>& partitions,
        uint32_t                                         new_master );

    grouped_partition_information get_partitions_for_operations(
        uint32_t client_id, const std::vector<cell_key_ranges>& write_cks,
        const std::vector<cell_key_ranges>& read_cks,
        const partition_lock_mode& mode, bool allow_missing );

    grouped_partition_information get_partitions_for_operations(
        uint32_t client_id, const std::vector<cell_key>& write_cks,
        const std::vector<cell_key>& read_cks, const partition_lock_mode& mode,
        bool allow_missing );

    partition_type::type get_default_partition_type( uint32_t table_id );
    storage_tier_type::type get_default_storage_type( uint32_t table_id );

   private:
    transaction_partition_holder* get_transaction_holder(
        uint32_t client_id, const partition_column_identifier_set& write_pids,
        const partition_column_identifier_set& read_pids,
        const partition_column_identifier_set& inflight_pids,
        bool                                   allow_missing );

    db db_;

    std::shared_ptr<partition_data_location_table> loc_table_;

    propagation_configuration prop_config_;
};

partition_column_identifier_set build_pid_set(
    const std::vector<std::shared_ptr<partition_payload>>& payloads );

#include "single_site_db.tcc"
