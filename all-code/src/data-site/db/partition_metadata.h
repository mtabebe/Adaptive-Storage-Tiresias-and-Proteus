#pragma once

#include <iostream>

#include "../../common/constants.h"
#include "../../common/partition_funcs.h"
#include "mvcc_chain.h"

class partition_metadata {
   public:
    partition_column_identifier partition_id_;
    uint32_t                    num_records_in_chain_;
    uint32_t                    num_records_in_snapshot_chain_;
    uint64_t                    partition_id_hash_;
    uint32_t                    site_location_;
};

class tables_metadata {
   public:
    uint32_t expected_num_tables_;
    uint32_t site_location_;

    uint64_t num_clients_;
    uint32_t gc_sleep_time_;

    bool        enable_secondary_storage_;
    std::string secondary_storage_dir_;
};

class table_metadata {
   public:
    std::string                 table_name_;
    uint32_t                    table_id_;
    uint32_t                    num_columns_;
    std::vector<cell_data_type> col_types_;
    uint32_t                    num_records_in_chain_;
    uint32_t                    num_records_in_snapshot_chain_;
    uint32_t                    site_location_;
    uint64_t                    default_partition_size_;
    uint32_t                    default_column_size_;

    uint64_t default_tracking_partition_size_;
    uint32_t default_tracking_column_size_;

    partition_type::type        default_partition_type_;
    storage_tier_type::type     default_storage_type_;

    bool                        enable_secondary_storage_;
    std::string                 secondary_storage_dir_;
};

partition_metadata create_partition_metadata(
    uint32_t table_id, uint64_t partition_start, uint64_t partition_end,
    uint32_t col_start, uint32_t col_end,
    uint32_t num_records_in_chain = k_num_records_in_chain,
    uint32_t num_records_in_snapshot_chain = k_num_records_in_snapshot_chain,
    uint32_t site_location = k_unassigned_master );

tables_metadata create_tables_metadata(
    uint32_t expected_num_tables, uint32_t site_location = k_unassigned_master,
    uint64_t           num_clients = k_bench_num_clients,
    uint32_t           gc_sleep_time = k_gc_sleep_time,
    bool               enable_secondary_storage = k_enable_secondary_storage,
    const std::string& secondary_storage_dir = k_secondary_storage_dir );

table_metadata create_table_metadata(
    const std::string& table_name, uint32_t table_id, uint32_t num_columns,
    const std::vector<cell_data_type>& col_types, uint32_t num_records_in_chain,
    uint32_t num_records_in_snapshot_chain,
    uint32_t site_location = k_unassigned_master,
    uint64_t default_partition_size = k_ycsb_partition_size,
    uint32_t default_column_size = k_ycsb_column_size,
    uint64_t default_tracking_partition_size = k_ycsb_partition_size,
    uint32_t default_tracking_column_size = k_ycsb_column_size,
    const partition_type::type&    p_type = partition_type::type::ROW,
    const storage_tier_type::type& s_type = storage_tier_type::type::MEMORY,
    bool               enable_secondary_storage = k_enable_secondary_storage,
    const std::string& secondary_storage_dir = k_secondary_storage_dir );

std::ostream& operator<<( std::ostream&             os,
                          const partition_metadata& p_metadata );
std::ostream& operator<<( std::ostream& os, const tables_metadata& t_metadata );
std::ostream& operator<<( std::ostream& os, const table_metadata& t_metadata );

