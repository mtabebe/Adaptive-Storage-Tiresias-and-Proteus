#include "partition_metadata.h"

partition_metadata create_partition_metadata(
    uint32_t table_id, uint64_t partition_start, uint64_t partition_end,
    uint32_t col_start, uint32_t col_end, uint32_t num_records_in_chain,
    uint32_t num_records_in_snapshot_chain, uint32_t site_location ) {
    partition_metadata p_meta;
    p_meta.partition_id_ = create_partition_column_identifier(
        table_id, partition_start, partition_end, col_start, col_end );
    p_meta.site_location_ = site_location;
    p_meta.num_records_in_chain_ = num_records_in_chain;
    p_meta.num_records_in_snapshot_chain_ = num_records_in_snapshot_chain;
    p_meta.partition_id_hash_ =
        partition_column_identifier_key_hasher{}( p_meta.partition_id_ );
    return p_meta;
}
tables_metadata create_tables_metadata(
    uint32_t expected_num_tables, uint32_t site_location, uint64_t num_clients,
    uint32_t gc_sleep_time, bool enable_secondary_storage,
    const std::string& secondary_storage_dir ) {
    tables_metadata t_meta;

    t_meta.expected_num_tables_ = expected_num_tables;
    t_meta.site_location_ = site_location;

    t_meta.num_clients_ = num_clients;
    t_meta.gc_sleep_time_ = gc_sleep_time;

    t_meta.enable_secondary_storage_ = enable_secondary_storage;
    t_meta.secondary_storage_dir_ = secondary_storage_dir;

    return t_meta;
}
table_metadata create_table_metadata(
    const std::string& table_name, uint32_t table_id, uint32_t num_columns,
    const std::vector<cell_data_type>& col_types, uint32_t num_records_in_chain,
    uint32_t num_records_in_snapshot_chain, uint32_t site_location,
    uint64_t default_partition_size, uint32_t default_column_size,
    uint64_t                       default_tracking_partition_size,
    uint32_t                       default_tracking_column_size,
    const partition_type::type&    default_partition_type,
    const storage_tier_type::type& default_storage_type,
    bool enable_secondary_storage, const std::string& secondary_storage_dir ) {
    table_metadata t_meta;
    t_meta.table_name_ = table_name;
    t_meta.table_id_ = table_id;
    t_meta.num_columns_ = num_columns;
    t_meta.col_types_ = col_types;
    t_meta.num_records_in_chain_ = num_records_in_chain;
    t_meta.num_records_in_snapshot_chain_ = num_records_in_snapshot_chain;
    t_meta.site_location_ = site_location;
    t_meta.default_partition_size_ = default_partition_size;
    t_meta.default_column_size_ = default_column_size;
    t_meta.default_tracking_partition_size_ = default_tracking_partition_size;
    t_meta.default_tracking_column_size_ = default_tracking_column_size;
    t_meta.default_partition_type_ = default_partition_type;
    t_meta.default_storage_type_ = default_storage_type;
    t_meta.enable_secondary_storage_ = enable_secondary_storage;
    t_meta.secondary_storage_dir_ = secondary_storage_dir;
    return t_meta;
}

std::ostream& operator<<( std::ostream&             os,
                          const partition_metadata& p_metadata ) {
    os << "partition_metadata: [ partition_id_:" << p_metadata.partition_id_
       << ", num_records_in_chain_:" << p_metadata.num_records_in_chain_
       << ", num_records_in_snapshot_chain_:"
       << p_metadata.num_records_in_snapshot_chain_
       << ", partition_id_hash_:" << p_metadata.partition_id_hash_
       << ", site_location_:" << p_metadata.site_location_ << " ]";
    return os;
}

std::ostream& operator<<( std::ostream&          os,
                          const tables_metadata& t_metadata ) {
    os << "tables_metadata: [ expected_num_tables_:"
       << t_metadata.expected_num_tables_
       << ", site_location_:" << t_metadata.site_location_
       << ", num_clients_:" << t_metadata.num_clients_
       << ", gc_sleep_time_:" << t_metadata.gc_sleep_time_
       << ", enable_secondary_storage_:" << t_metadata.enable_secondary_storage_
       << ", secondary_storage_dir_:" << t_metadata.secondary_storage_dir_
       << " ]";
    return os;
}

std::ostream& operator<<( std::ostream& os, const table_metadata& t_metadata ) {
    os << "table_metadata: [ table_name_:" << t_metadata.table_name_
       << ", table_id_:" << t_metadata.table_id_
       << ", num_columns_:" << t_metadata.num_columns_
       << ", col_types_:" << t_metadata.col_types_
       << ", num_records_in_chain_:" << t_metadata.num_records_in_chain_
       << ", num_records_in_snapshot_chain_:"
       << t_metadata.num_records_in_snapshot_chain_
       << ", site_location_:" << t_metadata.site_location_
       << ", default_partition_size_:" << t_metadata.default_partition_size_
       << ", default_column_size_:" << t_metadata.default_column_size_
       << ", default_partition_type_:" << t_metadata.default_partition_type_
       << ", default_storage_type_:" << t_metadata.default_storage_type_
       << ", enable_secondary_storage_:" << t_metadata.enable_secondary_storage_
       << ", secondary_storage_dir_:" << t_metadata.secondary_storage_dir_
       << " ]";
    return os;
}

