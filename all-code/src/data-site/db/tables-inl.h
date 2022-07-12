#pragma once

inline table* tables::get_table( uint32_t table_id ) const {
    DCHECK_LT( table_id, tables_.size() );
    return tables_.at( table_id );
}

#define execute_table_operation( method, table_id, args... ) \
    get_table( table_id )->method( args )

inline std::shared_ptr<partition> tables::add_partition(
    const partition_column_identifier& pid, const partition_type::type& p_type,
    const storage_tier_type::type& s_type ) {
    return execute_table_operation( add_partition, pid.table_id, pid, p_type,
                                    s_type );
}
inline std::shared_ptr<partition> tables::add_partition(
    const partition_column_identifier& pid, const partition_type::type& p_type,
    const storage_tier_type::type& s_type, bool do_pin, uint64_t version,
    bool                                          do_lock,
    std::shared_ptr<update_destination_interface> update_destination ) {
    return execute_table_operation( add_partition, pid.table_id, pid, p_type,
                                    s_type, do_pin, version, do_lock,
                                    update_destination );
}

inline bool tables::insert_partition( const partition_column_identifier& pid,
                                      std::shared_ptr<partition> part ) {
    return execute_table_operation( insert_partition, pid.table_id, pid, part );
}
inline bool tables::set_partition( const partition_column_identifier& pid,
                                   std::shared_ptr<partition>         part ) {
    return execute_table_operation( set_partition, pid.table_id, pid, part );
}

inline void tables::remove_partition( const partition_column_identifier& pid ) {
    execute_table_operation( remove_partition, pid.table_id, pid );
}

inline partition* tables::create_partition_without_adding_to_table(
    const partition_column_identifier& pid, const partition_type::type& p_type,
    const storage_tier_type::type& s_type, bool do_pin, uint64_t version ) {
    return execute_table_operation( create_partition_without_adding_to_table,
                                    pid.table_id, pid, p_type, s_type, do_pin,
                                    version );
}
inline partition* tables::create_partition_without_adding_to_table(
    const partition_column_identifier& pid, const partition_type::type& p_type,
    const storage_tier_type::type& s_type, bool do_pin, uint64_t version,
    std::shared_ptr<update_destination_interface> update_destination ) {
    /*
    return get_table( pid.table_id )
        ->create_partition_without_adding_to_table( pid, version,
                                                    update_destination );
                                      */
    return execute_table_operation( create_partition_without_adding_to_table,
                                    pid.table_id, pid, p_type, s_type, do_pin,
                                    version, update_destination );
}

inline split_partition_result tables::split_partition(
    const snapshot_vector& snapshot, const partition_column_identifier& old_pid,
    uint64_t split_row_point, uint32_t split_col_point,
    const partition_type::type& low_type, const partition_type::type& high_type,
    const storage_tier_type::type&                low_storage_type,
    const storage_tier_type::type&                high_storage_type,
    std::shared_ptr<update_destination_interface> low_update_destination,
    std::shared_ptr<update_destination_interface> high_update_destination ) {
    return execute_table_operation(
        split_partition, old_pid.table_id, snapshot, old_pid, split_row_point,
        split_col_point, low_type, high_type, low_storage_type,
        high_storage_type, low_update_destination, high_update_destination );
}

inline split_partition_result tables::merge_partition(
    const snapshot_vector& snapshot, const partition_column_identifier& low_pid,
    const partition_column_identifier&            high_pid,
    const partition_type::type&                   merge_type,
    const storage_tier_type::type&                merge_storage_type,
    std::shared_ptr<update_destination_interface> update_destination ) {
    return execute_table_operation( merge_partition, low_pid.table_id, snapshot,
                                    low_pid, high_pid, merge_type,
                                    merge_storage_type, update_destination );
}

inline transaction_partition_holder* tables::get_partitions(
    const partition_column_identifier_set& write_pids,
    const partition_column_identifier_set& read_pids ) {
    return get_partition_holder( write_pids, read_pids,
                                 partition_lookup_operation::GET );
}

inline transaction_partition_holder* tables::get_or_create_partitions(
    const partition_column_identifier_set& write_pids,
    const partition_column_identifier_set& read_pids ) {
    auto ret = get_partition_holder(
        write_pids, read_pids, partition_lookup_operation::GET_OR_CREATE );
    DCHECK( ret );
    return ret;
}

inline uint64_t tables::get_approx_total_number_of_partitions(
    uint32_t table_id ) {
    return execute_table_operation( get_approx_total_number_of_partitions,
                                    table_id );
}


inline
    cell_data_type tables::get_cell_data_type(const cell_identifier& cid) const {
    return execute_table_operation( get_cell_data_type, cid.table_id_, cid );
}

