#pragma once

#include <glog/logging.h>

single_site_db_wrapper_t_types
    single_site_db_wrapper_t_templ::single_site_db_wrapper_t()
    : db_() {}

single_site_db_wrapper_t_types void single_site_db_wrapper_t_templ::init(
    std::shared_ptr<update_destination_generator> update_gen,
    std::shared_ptr<update_enqueuers>             enqueuers,
    const tables_metadata&                        t_meta ) {
    db_.init( update_gen, enqueuers, t_meta );
}

single_site_db_wrapper_t_types uint32_t
                               single_site_db_wrapper_t_templ::create_table(
        const table_metadata& metadata ) {
    return db_.create_table( metadata );
}

single_site_db_wrapper_t_types transaction_partition_holder*
                               single_site_db_wrapper_t_templ::get_partitions_with_begin(
        uint32_t client_id, const snapshot_vector& begin_timestamp,
        std::vector<cell_key_ranges>& write_keys,
        std::vector<cell_key_ranges>& read_keys, bool allow_missing ) {
    return db_.get_partitions_with_begin(
        client_id, begin_timestamp, write_keys, read_keys, allow_missing );
}

single_site_db_wrapper_t_types transaction_partition_holder*
                               single_site_db_wrapper_t_templ::get_partitions_with_begin(
        uint32_t client_id, const snapshot_vector& begin_timestamp,
        const partition_column_identifier_set& write_pids,
        const partition_column_identifier_set& read_pids,
        const partition_column_identifier_set& inflight_pids,
        bool                                   allow_missing ) {
    return db_.get_partitions_with_begin( client_id, begin_timestamp,
                                          write_pids, read_pids, inflight_pids,
                                          allow_missing );
}

single_site_db_wrapper_t_types
void single_site_db_wrapper_t_templ::change_partition_types(
        uint32_t                                          client_id,
        const std::vector<::partition_column_identifier>& pids,
        const std::vector<partition_type::type>&          part_types,
        const std::vector<storage_tier_type::type>&       storage_types ) {
    db_.change_partition_types( client_id, pids, part_types, storage_types );
}

single_site_db_wrapper_t_types void
    single_site_db_wrapper_t_templ::add_partition(
        uint32_t client_id, const partition_column_identifier& pid,
        uint32_t master_location, const partition_type::type& p_type,
        const storage_tier_type::type& s_type ) {
    return db_.add_partition( client_id, pid, master_location, p_type, s_type );
}

single_site_db_wrapper_t_types void
    single_site_db_wrapper_t_templ::remove_partition( uint32_t        client_id,
                                                      const cell_key& ck ) {
    partition_lock_mode lock_mode = partition_lock_mode::lock;

    auto group_info = db_.get_partitions_for_operations(
        client_id, {ck}, {}, lock_mode, false /* allow missing */ );
    auto partitions = group_info.payloads_;
    if( partitions.size() != 1 ) {
        return;
    }

    db_.remove_partition( client_id, partitions.at( 0 ) );
}

single_site_db_wrapper_t_types snapshot_vector
                               single_site_db_wrapper_t_templ::merge_partition(
        uint32_t client_id, const snapshot_vector& snapshot,
        const cell_key& merge_point, bool merge_vertically ) {

    partition_lock_mode lock_mode = partition_lock_mode::lock;
    auto                low_ck = merge_point;

    if( merge_vertically ) {
        DCHECK_GT( low_ck.col_id, 0 );
        low_ck.col_id = low_ck.col_id - 1;
    } else {
        DCHECK_GT( low_ck.row_id, 0 );
        low_ck.row_id = low_ck.row_id - 1;
    }

    auto group_info = db_.get_partitions_for_operations(
        client_id, {merge_point, low_ck}, {}, lock_mode,
        false /* allow missing */ );
    auto partitions = group_info.payloads_;
    if( partitions.size() != 2 ) {
        unlock_payloads( partitions, lock_mode );
        return {};
    }

    bool merge_ok = false;
    if( merge_vertically ) {
        merge_ok = ( partitions.at( 0 )->identifier_.partition_start ==
                     partitions.at( 1 )->identifier_.partition_start ) and
                   ( partitions.at( 0 )->identifier_.partition_end ==
                     partitions.at( 1 )->identifier_.partition_end );
    } else {
        merge_ok = ( partitions.at( 0 )->identifier_.column_start ==
                     partitions.at( 1 )->identifier_.column_start ) and
                   ( partitions.at( 0 )->identifier_.column_end ==
                     partitions.at( 1 )->identifier_.column_end );
    }

    if( !merge_ok ) {
        unlock_payloads( partitions, lock_mode );
        return {};
    }

    auto part_type = db_.get_default_partition_type( merge_point.table_id );
    auto storage_type = db_.get_default_storage_type( merge_point.table_id );

    return db_.merge_partition( client_id, snapshot, partitions.at( 0 ),
                                partitions.at( 1 ), part_type, storage_type );
}

single_site_db_wrapper_t_types snapshot_vector
                               single_site_db_wrapper_t_templ::split_partition(
        uint32_t client_id, const snapshot_vector& snapshot,
        const cell_key& split_point, bool split_vertically ) {

    partition_lock_mode lock_mode = partition_lock_mode::lock;

    auto group_info = db_.get_partitions_for_operations(
        client_id, {split_point}, {}, lock_mode, false /* allow missing */ );
    auto partitions = group_info.payloads_;
    if( partitions.size() != 1 ) {
        unlock_payloads( partitions, lock_mode );
        return {};
    }
    auto part = partitions.at( 0 );
    auto pid = part->identifier_;

    uint64_t range = pid.partition_end - pid.partition_start;
    if( split_vertically ) {
        range = pid.column_end - pid.column_start;
    }

    if( range == 0 ) {
        unlock_payloads( partitions, lock_mode );
        return {};
    }

    uint64_t row_split_point = split_point.row_id;
    uint32_t col_split_point = split_point.col_id;
    if( split_vertically ) {
        row_split_point = k_unassigned_key;
    } else {
        col_split_point = k_unassigned_col;
    }

    auto part_type = db_.get_default_partition_type( split_point.table_id );
    auto storage_type = db_.get_default_storage_type( split_point.table_id );

    return db_.split_partition( client_id, snapshot, part, row_split_point,
                                col_split_point, part_type, part_type,
                                storage_type, storage_type );
}

single_site_db_wrapper_t_types snapshot_vector
                               single_site_db_wrapper_t_templ::remaster_partitions(
        uint32_t client_id, const snapshot_vector& snapshot,
        std::vector<cell_key_ranges>& ckrs, uint32_t new_master ) {
    partition_lock_mode lock_mode = partition_lock_mode::lock;

    auto group_info = db_.get_partitions_for_operations(
        client_id, ckrs, {}, lock_mode, false /* allow missing */ );
    auto partitions = group_info.payloads_;
    return db_.remaster_partitions( client_id, snapshot, partitions,
                                    new_master );
}

single_site_db_wrapper_t_types uint32_t
                               single_site_db_wrapper_t_templ::get_site_location() {
    return db_.get_database()->get_site_location();
}

single_site_db_wrapper_t_types partition_type::type
                               single_site_db_wrapper_t_templ::get_default_partition_type(
        uint32_t table_id ) {
    return db_.get_default_partition_type( table_id );
}
single_site_db_wrapper_t_types storage_tier_type::type
                               single_site_db_wrapper_t_templ::get_default_storage_type(
        uint32_t table_id ) {
    return db_.get_default_storage_type( table_id );
}
