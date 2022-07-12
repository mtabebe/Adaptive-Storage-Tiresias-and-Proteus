#include "multi_version_partition_table.h"

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "../common/perf_tracking.h"
#include "../data-site/db/partition.h"
#include "partition_payload.h"

internal_hash_multi_version_partition_payload_table::
    internal_hash_multi_version_partition_payload_table(
        const table_metadata& metadata )
    : metadata_( metadata ), rows_to_partitions_(), partition_map_() {}
internal_hash_multi_version_partition_payload_table::
    ~internal_hash_multi_version_partition_payload_table() {}

std::shared_ptr<partition_payload>
    internal_hash_multi_version_partition_payload_table::get_partition(
        const cell_key& ck, const partition_lock_mode& lock_mode,
        partition_column_identifier_unordered_set& already_locked_set ) const {
    start_timer( SS_INTERNAL_DATA_LOCATION_GET_PARTITION_TIMER_ID );
    DVLOG( 40 ) << "Get partition:" << ck;
    auto found_payload = lookup_by_key( ck );
    if( found_payload ) {
        DVLOG( 40 ) << "Get partition:" << ck << ", found, trying to lock";
        auto ret =
            try_lock_from_found( found_payload, lock_mode, already_locked_set );
        stop_timer( SS_INTERNAL_DATA_LOCATION_GET_PARTITION_TIMER_ID );
        return ret;
    }
    DVLOG( 40 ) << "Get partition:" << ck << ", not found!";
    stop_timer( SS_INTERNAL_DATA_LOCATION_GET_PARTITION_TIMER_ID );
    return nullptr;
}
std::shared_ptr<partition_payload>
    internal_hash_multi_version_partition_payload_table::get_partition(
        const partition_column_identifier&         pid,
        const partition_lock_mode&                 lock_mode,
        partition_column_identifier_unordered_set& already_locked_set ) const {
    start_timer( SS_INTERNAL_DATA_LOCATION_GET_PARTITION_TIMER_ID );
    DVLOG( 40 ) << "Get partition:" << pid;
    // look up the partition and lock it
    auto found = partition_map_.find( pid );
    if( found == partition_map_.cend() ) {
        DVLOG( 40 ) << "Get partition:" << pid << ", not found!";
        stop_timer( SS_INTERNAL_DATA_LOCATION_GET_PARTITION_TIMER_ID );
        return nullptr;
    }
    DVLOG( 40 ) << "Get partition:" << pid << ", found, trying to lock";
    auto ret =
        try_lock_from_found( found->second, lock_mode, already_locked_set );
    stop_timer( SS_INTERNAL_DATA_LOCATION_GET_PARTITION_TIMER_ID );
    return ret;
}
std::vector<std::shared_ptr<partition_payload>>
    internal_hash_multi_version_partition_payload_table::get_partition(
        const cell_key_ranges& ckr, const partition_lock_mode& lock_mode,
        partition_column_identifier_unordered_set& already_locked_set ) const {

    start_timer( SS_INTERNAL_DATA_LOCATION_GET_PARTITION_TIMER_ID );

    std::vector<std::shared_ptr<partition_payload>> payloads;

    DVLOG( 40 ) << "Get:" << ckr;

    cell_key ck;
    ck.table_id = ckr.table_id;
    ck.row_id = ckr.row_id_start;
    ck.col_id = ckr.col_id_start;

    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        found_payloads;

    partition_column_identifier pid;
    while( ck.row_id <= ckr.row_id_end ) {
        while( ck.col_id <= ckr.col_id_end ) {
            auto found_part =
                get_partition( ck, lock_mode, already_locked_set );
            if( found_part ) {
                pid = found_part->identifier_;
                if( found_payloads.count( pid ) == 0 ) {
                    found_payloads.emplace( pid, found_part );
                    payloads.emplace_back( found_part );
                }
                ck.col_id = pid.column_end + 1;
            } else {
                unlock_payloads( payloads, lock_mode );
                payloads.clear();
                stop_timer( SS_INTERNAL_DATA_LOCATION_GET_PARTITION_TIMER_ID );
                return payloads;
            }
        }
        ck.row_id = pid.partition_end + 1;
        ck.col_id = ckr.col_id_start;
    }

    stop_timer( SS_INTERNAL_DATA_LOCATION_GET_PARTITION_TIMER_ID );

    return payloads;
}

std::shared_ptr<partition_payload>
    internal_hash_multi_version_partition_payload_table::get_or_create(
        const cell_key& ck, const partition_lock_mode& lock_mode,
        partition_column_identifier_unordered_set& already_locked_set ) {
    partition_column_identifier default_pid =
        map_cell_key_to_partition_column_identifier( ck );
    return get_or_create( ck, default_pid, lock_mode, already_locked_set );
}

std::shared_ptr<partition_payload_row_holder>
    internal_hash_multi_version_partition_payload_table::get_or_insert_row(
        int64_t row ) {
      DVLOG(40) << "Get or insert row:" << row;
    auto found = rows_to_partitions_.find( row );
    if( found != rows_to_partitions_.end() ) {
      DVLOG(40) << "Get or insert row:" << row << ", found:" << found->second;
        return found->second;
    }
    auto created = std::make_shared<partition_payload_row_holder>( row );
    auto inserted_in_row_map = rows_to_partitions_.insert( row, created );

    DVLOG( 40 ) << "Get or insert row:" << row
                << ", inserted:" << inserted_in_row_map.first->second;
    return inserted_in_row_map.first->second;
}

std::tuple<bool, std::shared_ptr<partition_payload>>
    internal_hash_multi_version_partition_payload_table::get_or_insert_into_row(
        const cell_key&                     ck,
        std::shared_ptr<partition_payload>& created_payload ) {
    auto row = get_or_insert_row( ck.row_id );
    auto found_part = row->get_or_create_payload( ck, created_payload );
    bool inserted = false;
    DCHECK( found_part );
    if( found_part.get() == created_payload.get() ) {
        inserted = true;
    }
    return std::make_tuple<>( inserted, found_part );
}

std::tuple<bool, std::shared_ptr<partition_payload>>
    internal_hash_multi_version_partition_payload_table::
        get_or_insert_into_row_and_expand(
            const cell_key&                     ck,
            std::shared_ptr<partition_payload>& created_payload ) {
    auto row = get_or_insert_row( ck.row_id );
    auto found_part =
        row->get_or_create_payload_and_expand( ck, created_payload );
    bool inserted = false;
    DCHECK( found_part );
    if( found_part.get() == created_payload.get() ) {
        inserted = true;
    }
    return std::make_tuple<>( inserted, found_part );
}

std::shared_ptr<partition_payload>
    internal_hash_multi_version_partition_payload_table::get_or_create(
        const cell_key& ck, const partition_column_identifier& default_pid,
        const partition_lock_mode&                 lock_mode,
        partition_column_identifier_unordered_set& already_locked_set ) {

    start_timer( SS_INTERNAL_DATA_LOCATION_GET_OR_CREATE_PARTITION_TIMER_ID );

    DVLOG( 40 ) << "Get or create:" << ck << ", default_pid:" << default_pid;

    DCHECK_EQ( ck.table_id, default_pid.table_id );

    // first do a lookup, if it already exists
    auto found_by_payload_by_lookup = lookup_by_key( ck );
    if( found_by_payload_by_lookup ) {
        DVLOG( 40 ) << "Get or create:" << ck << ", default_pid:" << default_pid
                    << " already exists:"
                    << found_by_payload_by_lookup->identifier_
                    << ", trying to lock";

        auto ret = try_lock_from_found( found_by_payload_by_lookup, lock_mode,
                                        already_locked_set );
        stop_timer(
            SS_INTERNAL_DATA_LOCATION_GET_OR_CREATE_PARTITION_TIMER_ID );

        return ret;
    }

    auto created_payload = std::make_shared<partition_payload>();
    created_payload->lock( lock_mode );
    created_payload->identifier_.table_id = ck.table_id;
    created_payload->identifier_.partition_start = ck.row_id;
    created_payload->identifier_.partition_end = ck.row_id;
    created_payload->identifier_.column_start = default_pid.column_start;
    created_payload->identifier_.column_end = default_pid.column_end;

    DVLOG( 40 ) << "Insert: " << ck;

    // CAS, on it being nullptr
    auto inserted_in_row_map =
        get_or_insert_into_row_and_expand( ck, created_payload );
    bool assigned = std::get<0>( inserted_in_row_map );
    // already exists?
    if( !assigned ) {
        DVLOG( 40 )
            << "Insert: " << ck
            << ", failed, unlocking and trying to get the partition directly";

        created_payload->unlock( lock_mode );
        auto ret = std::get<1>( inserted_in_row_map );
        if( ret ) {
            ret = try_lock_from_found( ret, lock_mode, already_locked_set );
        }

        stop_timer(
            SS_INTERNAL_DATA_LOCATION_GET_OR_CREATE_PARTITION_TIMER_ID );
        return ret;
    }
    DVLOG( 40 ) << "Insert: " << ck
                << ", as pid:" << created_payload->identifier_
                << ", okay, trying to assign to rows";

    partition_column_identifier new_default_pid = default_pid;

    new_default_pid.column_start = created_payload->identifier_.column_start;
    new_default_pid.column_end = created_payload->identifier_.column_end;

    // now iterate over the rows and try to assign
    cell_key new_ck = ck;
    new_ck.table_id = new_default_pid.table_id;
    new_ck.col_id = new_default_pid.column_start;
    if( ck.row_id > 0 ) {
        for( int64_t row_id = ck.row_id - 1;
             row_id >= new_default_pid.partition_start; row_id-- ) {
            DVLOG( 40 ) << "Insert to grow partition: " << new_default_pid
                        << ", row:" << row_id;
            new_ck.row_id = row_id;
            created_payload->identifier_.partition_start = row_id;
            auto row_assigned =
                get_or_insert_into_row( new_ck, created_payload );
            // if it already exists, then break out
            if( !std::get<0>( row_assigned ) ) {
                created_payload->identifier_.partition_start = row_id + 1;
                DVLOG( 10 ) << "Insert to grow partition: " << new_default_pid
                            << ", row:" << row_id << ", failed!";
                break;
            }
        }
    }
    if( ck.row_id < INT64_MAX ) {
        for( int64_t row_id = ck.row_id + 1;
             row_id <= new_default_pid.partition_end; row_id++ ) {
            DVLOG( 40 ) << "Insert to grow partition: " << new_default_pid
                        << ", row:" << row_id;
            new_ck.row_id = row_id;
            created_payload->identifier_.partition_end = row_id;

            auto row_assigned =
                get_or_insert_into_row( new_ck, created_payload );
            // if it already exists, then break out
            if( !std::get<0>( row_assigned ) ) {
                created_payload->identifier_.partition_end = row_id - 1;
                DVLOG( 10 ) << "Insert to grow partition: " << new_default_pid
                            << ", row:" << row_id << ", failed!";

                break;
            }
        }
    }

    DVLOG( 40 ) << "Reshaping original pid:" << new_default_pid
                << ", to:" << created_payload->identifier_;

    already_locked_set.insert( created_payload->identifier_ );

    // actually make the partition in the map, and create or make the partition
    DVLOG( 40 ) << "Inserting payload:" << *created_payload
                << ", in partition_map_";

    auto inserted_in_partition_map =
        partition_map_.insert( created_payload->identifier_, created_payload );

    // we should be the only one to do this insert
    DCHECK( inserted_in_partition_map.second );

    stop_timer( SS_INTERNAL_DATA_LOCATION_GET_OR_CREATE_PARTITION_TIMER_ID );
    return created_payload;
}

std::vector<std::shared_ptr<partition_payload>>
    internal_hash_multi_version_partition_payload_table::get_or_create(
        const cell_key_ranges& ckr, const partition_lock_mode& lock_mode,
        partition_column_identifier_unordered_set& already_locked_set ) {

    start_timer( SS_INTERNAL_DATA_LOCATION_GET_OR_CREATE_PARTITION_TIMER_ID );

    std::vector<std::shared_ptr<partition_payload>> payloads;

    DVLOG( 40 ) << "Get or create:" << ckr;

    cell_key ck;
    ck.table_id = ckr.table_id;
    ck.row_id = ckr.row_id_start;
    ck.col_id = ckr.col_id_start;

    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        found_payloads;

    while( ck.row_id <= ckr.row_id_end ) {
        auto pid = map_cell_key_to_partition_column_identifier( ck );
        while( ck.col_id <= ckr.col_id_end ) {
            pid = map_cell_key_to_partition_column_identifier( ck );
            auto found_part =
                get_or_create( ck, pid, lock_mode, already_locked_set );
            if( found_part ) {
                auto pid = found_part->identifier_;
                if( found_payloads.count( pid ) == 0 ) {
                    found_payloads.emplace( pid, found_part );
                    payloads.emplace_back( found_part );
                }
                ck.col_id = pid.column_end + 1;
            } else {
                unlock_payloads( payloads, lock_mode );
                payloads.clear();
                stop_timer(
                    SS_INTERNAL_DATA_LOCATION_GET_OR_CREATE_PARTITION_TIMER_ID );
                return payloads;
            }
        }
        ck.row_id = pid.partition_end + 1;
        ck.col_id = ckr.col_id_start;
    }

    stop_timer( SS_INTERNAL_DATA_LOCATION_GET_OR_CREATE_PARTITION_TIMER_ID );

    return payloads;
}

std::shared_ptr<partition_payload>
    internal_hash_multi_version_partition_payload_table::remove_partition(
        std::shared_ptr<partition_payload> payload ) {
    DVLOG( 2 ) << "Removing PID:" << payload->identifier_;
    auto erased = partition_map_.erase( payload->identifier_ );
    if( erased == 0 ) {
        DVLOG( 40 ) << "Removing PID:" << payload->identifier_
                    << ", not in partition map";
        return nullptr;
    }
    DCHECK_EQ( 1, erased );
    cell_key ck;
    ck.table_id = payload->identifier_.table_id;
    ck.row_id = payload->identifier_.partition_start;
    ck.col_id = payload->identifier_.column_start;
    for( int64_t row_id = payload->identifier_.partition_start;
         row_id <= payload->identifier_.partition_end; row_id++ ) {
        ck.row_id = row_id;
        DVLOG( 40 ) << "Removing row:" << row_id;
        auto found = rows_to_partitions_.find( ck.row_id );
        if( found != rows_to_partitions_.cend() ) {
            found->second->remove_payload( payload );
        }
    }
    return payload;
}

std::shared_ptr<partition_payload>
    internal_hash_multi_version_partition_payload_table::insert_partition(
        std::shared_ptr<partition_payload> payload,
        const partition_lock_mode&         lock_mode ) {
    DVLOG( 2 ) << "Inserting PID:" << payload->identifier_
               << ", payload:" << *payload;

    partition_column_identifier_unordered_set already_locked_set;

    payload->lock( lock_mode );

    auto inserted_in_partition_map =
        partition_map_.insert( payload->identifier_, payload );
    bool ori_partition = inserted_in_partition_map.second;

    if( !ori_partition ) {
        DVLOG( 40 ) << "Inserting PID:" << payload->identifier_
                    << ", already exists, trying to lock it";
        payload->unlock( lock_mode );

        return try_lock_from_found( inserted_in_partition_map.first->second,
                                    lock_mode, already_locked_set );
    }

    already_locked_set.insert( payload->identifier_ );

    cell_key ck;
    ck.table_id = payload->identifier_.table_id;
    ck.col_id = payload->identifier_.column_start;
    // now go and insert stuff
    for( int64_t row_id = payload->identifier_.partition_start;
         row_id <= payload->identifier_.partition_end; row_id++ ) {
        DVLOG( 40 ) << "Inserting PID:" << payload->identifier_
                    << ", row:" << row_id;
        ck.row_id = row_id;
        auto row_assigned = get_or_insert_into_row( ck, payload );
        // should not already exist
        DCHECK( std::get<0>( row_assigned ) );
    }
    return payload;
}

std::shared_ptr<partition_payload>
    internal_hash_multi_version_partition_payload_table::merge_partition(
        std::shared_ptr<partition_payload> low_payload,
        std::shared_ptr<partition_payload> high_payload,
        const partition_type::type&        merge_type,
        const storage_tier_type::type&     merge_storage_type,
        uint32_t                           update_destination_slot,
        const partition_lock_mode&         lock_mode ) {
    DVLOG( 2 ) << "Merging partitions:" << low_payload->identifier_ << ", "
               << high_payload->identifier_;
    // partitions are locked, create a new partition in the partition map
    // then go and update all the keys to point to new ones
    auto merged_partition = merge_partition_payloads(
        low_payload, high_payload, merge_type, merge_storage_type,
        update_destination_slot, lock_mode );

    DVLOG( 40 ) << "Inserting or assigning merged partition:"
                << merged_partition->identifier_;

#if 0 // HDB-INSERT_OR_ASSIGN
    auto insert_ret = partition_map_.insert_or_assign(
        merged_partition->identifier_, merged_partition );
#endif
    auto inserted_in_partition_map = partition_map_.insert(
        merged_partition->identifier_, merged_partition );
    DCHECK( inserted_in_partition_map.second );

    DVLOG( 40 ) << "Erasing low partition:" << low_payload->identifier_;
    auto erased_low = partition_map_.erase( low_payload->identifier_ );
    DCHECK_EQ( 1, erased_low );
    DVLOG( 40 ) << "Erasing high partition:" << high_payload->identifier_;
    auto erased_high = partition_map_.erase( high_payload->identifier_ );
    DCHECK_EQ( 1, erased_high );

    // iterate and merge
    for( int64_t row_id = merged_partition->identifier_.partition_start;
         row_id <= merged_partition->identifier_.partition_end; row_id++ ) {
        auto found = rows_to_partitions_.find( row_id );
        if ( found != rows_to_partitions_.end()) {
            found->second->merge_payload( low_payload, high_payload,
                                          merged_partition );
        }
    }

    return merged_partition;
}

std::vector<std::shared_ptr<partition_payload>>
    internal_hash_multi_version_partition_payload_table::split_partition(
        std::shared_ptr<partition_payload> ori_payload,
        uint64_t row_split_point, uint32_t col_split_point,
        const partition_type::type&    low_type,
        const partition_type::type&    high_type,
        const storage_tier_type::type& low_storage_type,
        const storage_tier_type::type& high_storage_type,
        uint32_t                       low_update_destination_slot,
        uint32_t                       high_update_destination_slot,
        const partition_lock_mode&     lock_mode ) {
    DVLOG( 2 ) << "Splitting partition:" << ori_payload->identifier_
               << ", at row:" << row_split_point << ", col:" << col_split_point;

    // partitions are locked, create the new partitions in the map
    // then go and update all the keys to point to new ones

    auto split_partitions = split_partition_payload(
        ori_payload, row_split_point, col_split_point, low_type, high_type,
        low_storage_type, high_storage_type, low_update_destination_slot,
        high_update_destination_slot, lock_mode );

    auto low_partition = std::get<0>( split_partitions );
    auto high_partition = std::get<1>( split_partitions );

    DVLOG( 40 ) << "Inserting or assigning split low partition:"
                << low_partition->identifier_;
#if 0 // HDB-INSERT_OR_ASSIGN
    partition_map_.insert_or_assign( low_partition->identifier_,
                                     low_partition );
#endif
    auto inserted_in_partition_map = partition_map_.insert(
        low_partition->identifier_, low_partition );
    DCHECK( inserted_in_partition_map.second );

    DVLOG( 40 ) << "Inserting or assigning split high partition:"
                << high_partition->identifier_;
#if 0 // HDB-INSERT_OR_ASSIGN
    partition_map_.insert_or_assign( high_partition->identifier_,
                                     high_partition );
#endif
    inserted_in_partition_map =
        partition_map_.insert( high_partition->identifier_, high_partition );
    DCHECK( inserted_in_partition_map.second );

    DVLOG( 40 ) << "Erasing original partition:" << ori_payload->identifier_;
    auto erased = partition_map_.erase( ori_payload->identifier_ );
    DCHECK_EQ( 1, erased );

    // iterate and split
    for( int64_t row_id = ori_payload->identifier_.partition_start;
         row_id <= ori_payload->identifier_.partition_end; row_id++ ) {
        auto found = rows_to_partitions_.find( row_id );
        if ( found != rows_to_partitions_.end()) {
            found->second->split_payload( ori_payload, low_partition,
                                          high_partition );
        }
    }

    return {low_partition, high_partition};
}

uint64_t internal_hash_multi_version_partition_payload_table::
    get_approx_total_number_of_partitions() const {
    return partition_map_.size();
}

std::shared_ptr<partition_payload>
    internal_hash_multi_version_partition_payload_table::lookup_by_key(
        const cell_key& ck ) const {
    DVLOG( 40 ) << "Lookup by key:" << ck;
    auto found = rows_to_partitions_.find( ck.row_id );
    if( found == rows_to_partitions_.cend() ) {
        DVLOG( 40 ) << "Lookup by key:" << ck
                    << ", not found in rows_to_partitions_";
        return nullptr;
    }
    DVLOG( 40 ) << "Lookup by key:" << ck
                << ", in payload holder:" << found->second;
    auto ret = found->second->get_payload( ck );
    DVLOG( 40 ) << "Lookup by key:" << ck << ", found in rows_to_partitions_"
                << ", found_payload:" << ret;

    return ret;
}

std::shared_ptr<partition_payload>
    internal_hash_multi_version_partition_payload_table::try_lock_from_found(
        std::shared_ptr<partition_payload>         found_payload,
        const partition_lock_mode&                 lock_mode,
        partition_column_identifier_unordered_set& already_locked_set ) const {
    if( already_locked_set.count( found_payload->identifier_ ) == 1 ) {
        return found_payload;
    }
    bool locked = found_payload->lock( lock_mode );
    if( !locked ) {
        return nullptr;
    }
    if ( lock_mode != partition_lock_mode::no_lock) {
        // redo the lookup, it should be the payload we got, if not, then it got
        // switched from under us
        auto found = partition_map_.find( found_payload->identifier_ );
        if( found == partition_map_.end() ) {
            LOG( WARNING ) << "Looking up:" << found_payload->identifier_
                           << ", after lock, not in table anymore, unlocking "
                              "and trying again";
            found_payload->unlock( lock_mode );
            return nullptr;
        } else if( found->second.get() != found_payload.get() ) {
            LOG( WARNING )
                << "Looking up:" << found_payload->identifier_
                << ", after lock, changed in table underneath us, unlocking "
                   "and trying again";

            found_payload->unlock( lock_mode );
            return nullptr;
        }
    }
    already_locked_set.insert( found_payload->identifier_ );

    return found_payload;
}

partition_column_identifier
    internal_hash_multi_version_partition_payload_table::
        map_cell_key_to_partition_column_identifier( const cell_key& ck ) const {
    partition_column_identifier pid;
    pid.table_id = ck.table_id;

    int64_t table_default_size = metadata_.default_partition_size_;
    int32_t table_col_size = metadata_.default_column_size_;

    pid.partition_start =
        ( ck.row_id / table_default_size ) * table_default_size;
    pid.partition_end = pid.partition_start + ( table_default_size - 1 );

    pid.column_start = ( ck.col_id / table_col_size ) * table_col_size;
    pid.column_end = std::min( pid.column_start + ( table_col_size - 1 ),
                               (int32_t) metadata_.num_columns_ - 1 );

    DVLOG( 40 ) << "PK:" << ck << " maps to:" << pid;

    return pid;
}

table_metadata
    internal_hash_multi_version_partition_payload_table::get_table_metadata()
        const {
    return metadata_;
}

void internal_hash_multi_version_partition_payload_table::
    persist_table_location( site_selector_table_persister* persister ) {

    persister->persist_table_info( metadata_ );

    for( auto& entry : partition_map_ ) {
        persister->persist_partition_payload( entry.second );
    }
}

void internal_hash_multi_version_partition_payload_table::
    build_partition_access_frequencies(
        partition_access_entry_priority_queue& sorted_partition_access_counts,
        partition_column_identifier_unordered_set&    partition_accesses ) const {

    for( const auto& part_entry : partition_map_ ) {
        const auto& pid = part_entry.first;
        auto part = part_entry.second;

        if( partition_accesses.count( pid ) == 1 ) {
            continue;
        }

        partition_access_entry access_entry =
            build_partition_access_entry( part );
        if( access_entry.site_ != -1 ) {
            sorted_partition_access_counts.push( std::move( access_entry ) );
            partition_accesses.insert( pid );
        }
    }
}

multi_version_partition_data_location_table::
    multi_version_partition_data_location_table()
    : tables_() {}
multi_version_partition_data_location_table::
    ~multi_version_partition_data_location_table() {
    for( uint32_t pos = 0; pos < tables_.size(); pos++ ) {
        internal_hash_multi_version_partition_payload_table* table =
            tables_.at( pos );
        delete table;
        tables_.at( pos ) = nullptr;
    }
    tables_.clear();
}

// not thread safe
uint32_t multi_version_partition_data_location_table::create_table(
    const table_metadata& metadata ) {
    DVLOG( 20 ) << "Create table:" << metadata;

    uint32_t table_id = metadata.table_id_;
    if( table_id < tables_.size() ) {
        return table_id;
    }
    internal_hash_multi_version_partition_payload_table* t =
        new internal_hash_multi_version_partition_payload_table( metadata );
    tables_.emplace_back( t );
    DCHECK_EQ( table_id, tables_.size() - 1 );

    return table_id;
}

internal_hash_multi_version_partition_payload_table*
    multi_version_partition_data_location_table::get_table(
        uint32_t table_id ) const {
    DCHECK_LT( table_id, tables_.size() );
    return tables_.at( table_id );
}

uint64_t internal_hash_multi_version_partition_payload_table::
    get_default_partition_size_for_table() const {
    return metadata_.default_partition_size_;
}
uint32_t internal_hash_multi_version_partition_payload_table::
    get_default_column_size_for_table() const {
    return metadata_.default_column_size_;
}

#define execute_multi_version_partition_table_operation( method, table_id, \
                                                         args... )         \
    get_table( table_id )->method( args )

#define execute_multi_version_ckrs_operation( method, cks, payloads,          \
                                              lock_mode, tolerate_missing )   \
    std::vector<cell_key_ranges> sorted_cks = cks;                            \
    std::sort( sorted_cks.begin(), sorted_cks.end() );                        \
    DVLOG( 40 ) << "Execute multi CKRs:" << sorted_cks << ", ori:" << cks;    \
    std::shared_ptr<partition_payload>        last_payload = nullptr;         \
    partition_column_identifier_set           seen_parts;                     \
    cell_key_ranges                           ck;                             \
    partition_column_identifier_unordered_set already_locked;                 \
    for( uint32_t pos = 0; pos < sorted_cks.size(); pos++ ) {                 \
        ck = sorted_cks.at( pos );                                            \
        DVLOG( 40 ) << "Considering CKR:" << ck;                              \
        uint32_t table_id = ck.table_id;                                      \
        if( last_payload and                                                  \
            is_ckr_fully_within_pcid( ck, last_payload->identifier_ ) ) {     \
            DVLOG( 40 ) << "CKR:" << ck                                       \
                        << " subsumed by:" << last_payload->identifier_;      \
        } else {                                                              \
            auto last_payloads =                                              \
                get_table( table_id )                                         \
                    ->method( ck, lock_mode, already_locked );                \
            if( !tolerate_missing and last_payloads.empty() ) {               \
                unlock_payloads( payloads, lock_mode );                       \
                payloads.clear();                                             \
                break;                                                        \
            }                                                                 \
            for( auto& part : last_payloads ) {                               \
                if( seen_parts.count( part->identifier_ ) == 0 ) {            \
                    payloads.push_back( part );                               \
                    last_payload = part;                                      \
                    seen_parts.insert( part->identifier_ );                   \
                    DVLOG( 40 ) << "CKR:" << ck                               \
                                << ", maps to:" << last_payload->identifier_; \
                }                                                             \
            }                                                                 \
        }                                                                     \
    }

std::vector<std::shared_ptr<partition_payload>>
    multi_version_partition_data_location_table::get_partitions(
        const std::vector<cell_key_ranges>& cks,
        const partition_lock_mode&          lock_mode ) const {
    start_timer( SS_DATA_LOCATION_GET_PARTITIONS_TIMER_ID );

    DVLOG( 20 ) << "Get partitions:" << cks;

    std::vector<std::shared_ptr<partition_payload>> payloads;

    execute_multi_version_ckrs_operation( get_partition, cks, payloads,
                                          lock_mode, false );

    stop_timer( SS_DATA_LOCATION_GET_PARTITIONS_TIMER_ID );

    return payloads;
}

// return a ck, and is_write
// side effect is advance the positions
bool get_next_ck_and_type_from_read_and_write_sets(
    uint32_t* write_pos, const std::vector<cell_key_ranges>& sorted_write_cks,
    uint32_t* read_pos, const std::vector<cell_key_ranges>& sorted_read_cks,
    cell_key_ranges& next_ck ) {
    bool      is_write = false;
    uint32_t* pointer_to_advance = nullptr;

    DVLOG( 40 ) << "Consider next ck and type from write_pos:" << *write_pos
                << ", write_cks size:" << sorted_write_cks.size()
                << ", read_pos:" << *read_pos
                << ", read_cks size:" << sorted_read_cks.size();

    if( *write_pos == sorted_write_cks.size() ) {
        DCHECK_LT( *read_pos, sorted_read_cks.size() );
        next_ck = sorted_read_cks.at( *read_pos );
        pointer_to_advance = read_pos;
        is_write = false;
    } else if( *read_pos == sorted_read_cks.size() ) {
        DCHECK_LT( *write_pos, sorted_write_cks.size() );
        next_ck = sorted_write_cks.at( *write_pos );
        pointer_to_advance = write_pos;
        is_write = true;
    } else {
        DCHECK_LT( *read_pos, sorted_read_cks.size() );
        DCHECK_LT( *write_pos, sorted_write_cks.size() );

        if( sorted_read_cks.at( *read_pos ) <
            sorted_write_cks.at( *write_pos ) ) {
            is_write = false;
            next_ck = sorted_read_cks.at( *read_pos );
            pointer_to_advance = read_pos;
        } else {
            is_write = true;
            next_ck = sorted_write_cks.at( *write_pos );
            pointer_to_advance = write_pos;
        }
    }

    *pointer_to_advance = *pointer_to_advance + 1;

    return is_write;
}

#define execute_multi_version_ckrs_operation_and_group(                       \
    _method, _sorted_write_cks, _sorted_read_cks, _group_info, _lock_mode,    \
    _tolerate_missing )                                                       \
    DCHECK( std::is_sorted( _sorted_write_cks.begin(),                        \
                            _sorted_write_cks.end() ) );                      \
    DCHECK(                                                                   \
        std::is_sorted( _sorted_read_cks.begin(), _sorted_read_cks.end() ) ); \
    uint32_t                                  read_pos = 0;                   \
    uint32_t                                  write_pos = 0;                  \
    std::shared_ptr<partition_payload>        last_payload = nullptr;         \
    cell_key_ranges                           _next_ck;                       \
    bool                                      _next_ck_is_write = false;      \
    partition_column_identifier_unordered_set already_locked_set;             \
    /* iterate over the sorted cks */                                         \
    DVLOG( 40 ) << "Considering read_cks size:" << _sorted_read_cks.size()    \
                << ", write_cks size:" << _sorted_write_cks.size();           \
    while( ( read_pos < _sorted_read_cks.size() ) or                          \
           ( write_pos < _sorted_write_cks.size() ) ) {                       \
        /* get the next ck from the sorted list */                            \
        _next_ck_is_write = get_next_ck_and_type_from_read_and_write_sets(    \
            &write_pos, _sorted_write_cks, &read_pos, _sorted_read_cks,       \
            _next_ck );                                                       \
        DVLOG( 40 ) << "Considering ck:" << _next_ck                          \
                    << ", is_write:" << _next_ck_is_write;                    \
        uint32_t table_id = _next_ck.table_id;                                \
        std::vector<std::shared_ptr<partition_payload>> payloads;             \
        if( last_payload and is_ckr_fully_within_pcid(                        \
                                 _next_ck, last_payload->identifier_ ) ) {    \
            payloads.emplace_back( last_payload );                            \
            /* if it is subsumed then don't do anything special */            \
            DVLOG( 40 ) << "PK:" << _next_ck                                  \
                        << " subsumed by:" << last_payload->identifier_;      \
        } else {                                                              \
            payloads =                                                        \
                get_table( table_id )                                         \
                    ->_method( _next_ck, _lock_mode, already_locked_set );    \
            if( !_tolerate_missing and payloads.empty() ) {                   \
                unlock_payloads( _group_info.payloads_, _lock_mode );         \
                _group_info.payloads_.clear();                                \
                break;                                                        \
            }                                                                 \
        }                                                                     \
        /* add the current ck information to the current payload */           \
        if( !payloads.empty() ) {                                             \
            last_payload = payloads.at( payloads.size() - 1 );                \
        }                                                                     \
        _group_info.add_partition_information(                                \
            payloads, _next_ck, _next_ck_is_write, _lock_mode );              \
    }

grouped_partition_information
    multi_version_partition_data_location_table::get_partitions_and_group(
        const std::vector<cell_key_ranges>& sorted_write_cks,
        const std::vector<cell_key_ranges>& sorted_read_cks,
        const partition_lock_mode& lock_mode, int num_sites,
        bool allow_missing ) const {

    start_timer( SS_DATA_LOCATION_GET_PARTITIONS_AND_GROUP_TIMER_ID );

    grouped_partition_information group_info( num_sites );

    execute_multi_version_ckrs_operation_and_group(
        get_partition, sorted_write_cks, sorted_read_cks, group_info, lock_mode,
        allow_missing );

    stop_timer( SS_DATA_LOCATION_GET_PARTITIONS_AND_GROUP_TIMER_ID );
    return group_info;
}

grouped_partition_information multi_version_partition_data_location_table::
    get_or_create_partitions_and_group(
        const std::vector<cell_key_ranges>& sorted_write_cks,
        const std::vector<cell_key_ranges>& sorted_read_cks,
        const partition_lock_mode& lock_mode, int num_sites ) {

    start_timer( SS_DATA_LOCATION_GET_OR_CREATE_PARTITIONS_AND_GROUP_TIMER_ID );

    grouped_partition_information group_info( num_sites );

    execute_multi_version_ckrs_operation_and_group(
        get_or_create, sorted_write_cks, sorted_read_cks, group_info,
        lock_mode, false );

    stop_timer( SS_DATA_LOCATION_GET_OR_CREATE_PARTITIONS_AND_GROUP_TIMER_ID );
    return group_info;
}

std::shared_ptr<partition_payload>
    multi_version_partition_data_location_table::get_partition(
        const cell_key& ck, const partition_lock_mode& lock_mode ) const {
    start_timer( SS_DATA_LOCATION_GET_PARTITION_TIMER_ID );
    DVLOG( 20 ) << "Get partition:" << ck;
    partition_column_identifier_unordered_set already_locked;
    auto ret = execute_multi_version_partition_table_operation(
        get_partition, ck.table_id, ck, lock_mode, already_locked );
    stop_timer( SS_DATA_LOCATION_GET_PARTITION_TIMER_ID );

    return ret;
}

std::shared_ptr<partition_payload>
    multi_version_partition_data_location_table::get_partition(
        const partition_column_identifier& pid,
        const partition_lock_mode&         lock_mode ) const {
    start_timer( SS_DATA_LOCATION_GET_PARTITION_TIMER_ID );

    DVLOG( 20 ) << "Get partition:" << pid;
    partition_column_identifier_unordered_set already_locked;
    auto ret = execute_multi_version_partition_table_operation(
        get_partition, pid.table_id, pid, lock_mode, already_locked );

    stop_timer( SS_DATA_LOCATION_GET_PARTITION_TIMER_ID );

    return ret;
}

std::vector<std::shared_ptr<partition_payload>>
    multi_version_partition_data_location_table::get_partition(
        const cell_key_ranges&     ckr,
        const partition_lock_mode& lock_mode ) const {
    start_timer( SS_DATA_LOCATION_GET_PARTITION_TIMER_ID );

    DVLOG( 20 ) << "Get partition:" << ckr;
    partition_column_identifier_unordered_set already_locked;
    auto ret = execute_multi_version_partition_table_operation(
        get_partition, ckr.table_id, ckr, lock_mode, already_locked );

    stop_timer( SS_DATA_LOCATION_GET_PARTITION_TIMER_ID );

    return ret;
}

std::vector<std::shared_ptr<partition_payload>>
    multi_version_partition_data_location_table::get_or_create_partitions(
        const std::vector<cell_key_ranges>& cks,
        const partition_lock_mode&          lock_mode ) {
    start_timer( SS_DATA_LOCATION_GET_OR_CREATE_PARTITIONS_TIMER_ID );

    DVLOG( 20 ) << "Get or create partitions:" << cks;

    std::vector<std::shared_ptr<partition_payload>> payloads;
    execute_multi_version_ckrs_operation( get_or_create, cks, payloads,
                                          lock_mode, false );

    stop_timer( SS_DATA_LOCATION_GET_OR_CREATE_PARTITIONS_TIMER_ID );

    return payloads;
}

std::shared_ptr<partition_payload>
    multi_version_partition_data_location_table::get_or_create_partition(
        const cell_key& ck, const partition_column_identifier& default_pid,
        const partition_lock_mode& lock_mode ) {

    start_timer( SS_DATA_LOCATION_GET_OR_CREATE_PARTITION_TIMER_ID );

    DVLOG( 20 ) << "Get or create partition:" << ck
                << ", default pid:" << default_pid;
    partition_column_identifier_unordered_set already_locked;
    auto ret = execute_multi_version_partition_table_operation(
        get_or_create, ck.table_id, ck, default_pid, lock_mode,
        already_locked );

    stop_timer( SS_DATA_LOCATION_GET_OR_CREATE_PARTITION_TIMER_ID );

    return ret;

}

std::vector<std::shared_ptr<partition_payload>>
    multi_version_partition_data_location_table::get_or_create_partition(
        const cell_key_ranges& ckr, const partition_lock_mode& lock_mode ) {

    start_timer( SS_DATA_LOCATION_GET_OR_CREATE_PARTITION_TIMER_ID );

    DVLOG( 20 ) << "Get or create partition:" << ckr;
    partition_column_identifier_unordered_set already_locked;
    auto ret = execute_multi_version_partition_table_operation(
        get_or_create, ckr.table_id, ckr, lock_mode, already_locked );

    stop_timer( SS_DATA_LOCATION_GET_OR_CREATE_PARTITION_TIMER_ID );

    return ret;
}

std::shared_ptr<partition_payload>
    multi_version_partition_data_location_table::remove_partition(
        std::shared_ptr<partition_payload> payload ) {
    DVLOG( 20 ) << "Remove partition:" << *payload;

    start_timer( SS_DATA_LOCATION_REMOVE_PARTITION_TIMER_ID );

    auto ret = execute_multi_version_partition_table_operation(
        remove_partition, payload->identifier_.table_id, payload );

    stop_timer( SS_DATA_LOCATION_REMOVE_PARTITION_TIMER_ID );

    return ret;
}
std::shared_ptr<partition_payload>
    multi_version_partition_data_location_table::insert_partition(
        std::shared_ptr<partition_payload> payload,
        const partition_lock_mode&         lock_mode ) {
    DVLOG( 20 ) << "Insert partition:" << *payload;


    start_timer( SS_DATA_LOCATION_INSERT_PARTITION_TIMER_ID );

    auto ret = execute_multi_version_partition_table_operation(
        insert_partition, payload->identifier_.table_id, payload, lock_mode );

    stop_timer( SS_DATA_LOCATION_INSERT_PARTITION_TIMER_ID );

    return ret;
}

std::vector<std::shared_ptr<partition_payload>>
    multi_version_partition_data_location_table::split_partition(
        std::shared_ptr<partition_payload> partition, uint64_t row_split_point,
        uint32_t col_split_point, const partition_type::type& low_type,
        const partition_type::type&    high_type,
        const storage_tier_type::type& low_storage_type,
        const storage_tier_type::type& high_storage_type,
        uint32_t                       low_update_destination_slot,
        uint32_t                       high_update_destination_slot,
        const partition_lock_mode&     lock_mode ) {

    start_timer( SS_DATA_LOCATION_SPLIT_PARTITION_TIMER_ID );

    DVLOG( 20 ) << "Split Partition:" << *partition
                << ", @ row:" << row_split_point << ", col:" << col_split_point;

    auto pid = partition->identifier_;

    if ( row_split_point != k_unassigned_key) {
        DCHECK_EQ( col_split_point, k_unassigned_col );
        DCHECK_LT( pid.partition_start, row_split_point );
        DCHECK_LE( row_split_point, pid.partition_end );
    }
    if( col_split_point != k_unassigned_col ) {
        DCHECK_EQ( row_split_point, k_unassigned_key );
        DCHECK_LT( pid.column_start, col_split_point );
        DCHECK_LE( col_split_point, pid.column_end );
    }

    auto ret = execute_multi_version_partition_table_operation(
        split_partition, pid.table_id, partition, row_split_point,
        col_split_point, low_type, high_type, low_storage_type,
        high_storage_type, low_update_destination_slot,
        high_update_destination_slot, lock_mode );

    stop_timer( SS_DATA_LOCATION_SPLIT_PARTITION_TIMER_ID );

    return ret;
}
std::shared_ptr<partition_payload>
    multi_version_partition_data_location_table::merge_partition(
        std::shared_ptr<partition_payload> low_partition,
        std::shared_ptr<partition_payload> high_partition,
        const partition_type::type&        merge_type,
        const storage_tier_type::type&     merge_storage_type,
        uint32_t                           update_destination_slot,
        const partition_lock_mode&         lock_mode ) {

    start_timer( SS_DATA_LOCATION_MERGE_PARTITION_TIMER_ID );

    DVLOG( 20 ) << "Merge Partition:" << *low_partition << ", "
                << *high_partition;

    auto low_pid = low_partition->identifier_;
    auto high_pid = high_partition->identifier_;

    DCHECK_EQ( low_pid.table_id, high_pid.table_id );
    if( low_pid.partition_start != high_pid.partition_start ) {
        DCHECK_EQ( low_pid.column_start, high_pid.column_start );
        DCHECK_EQ( low_pid.column_end, high_pid.column_end );
        DCHECK_EQ( low_pid.partition_end + 1, high_pid.partition_start );
    }
    if( low_pid.column_start != high_pid.column_start ) {
        DCHECK_EQ( low_pid.partition_start, high_pid.partition_start );
        DCHECK_EQ( low_pid.partition_end, high_pid.partition_end );
        DCHECK_EQ( low_pid.column_end + 1, high_pid.column_start );
    }

    auto ret = execute_multi_version_partition_table_operation(
        merge_partition, low_pid.table_id, low_partition, high_partition,
        merge_type, merge_storage_type, update_destination_slot, lock_mode );

    stop_timer( SS_DATA_LOCATION_MERGE_PARTITION_TIMER_ID );

    return ret;
}

uint64_t multi_version_partition_data_location_table::
    get_approx_total_number_of_partitions() const {
    uint64_t total = 0;
    for (auto table : tables_ ) {
        total += table->get_approx_total_number_of_partitions();
    }
    return total;
}

table_metadata multi_version_partition_data_location_table::get_table_metadata(
    uint32_t table_id ) const {
    auto ret = execute_multi_version_partition_table_operation(
        get_table_metadata, table_id );
    return ret;
}

uint32_t multi_version_partition_data_location_table::get_num_tables() const {
    return tables_.size();
}

void multi_version_partition_data_location_table::persist_table_location(
    uint32_t table_id, site_selector_table_persister* persister ) {
    execute_multi_version_partition_table_operation( persist_table_location,
                                                     table_id, persister );
}

uint64_t multi_version_partition_data_location_table::
    get_default_partition_size_for_table( uint32_t table_id ) const {
    auto ret = execute_multi_version_partition_table_operation(
        get_default_partition_size_for_table, table_id );
    return ret;
}
uint32_t multi_version_partition_data_location_table::
    get_default_column_size_for_table( uint32_t table_id ) const {
    auto ret = execute_multi_version_partition_table_operation(
        get_default_column_size_for_table, table_id );
    return ret;
}

void multi_version_partition_data_location_table::
    build_partition_access_frequencies(
        partition_access_entry_priority_queue& sorted_partition_access_counts,
        partition_column_identifier_unordered_set&    partition_accesses ) const {
    for( const auto t : tables_ ) {
        t->build_partition_access_frequencies( sorted_partition_access_counts,
                                               partition_accesses );
    }
}
