#pragma once

inline transaction_partition_holder::transaction_partition_holder()
    : partitions_(),
      records_to_partitions_(),
      write_partition_ids_(),
      read_partition_ids_(),
      do_not_apply_last_partition_ids_(),
      snapshot_(),
      write_snapshot_(),
      prev_part_( nullptr ),
      table_to_poll_epoch_() {}
inline transaction_partition_holder::~transaction_partition_holder() {}

inline std::shared_ptr<partition> transaction_partition_holder::get_partition(
    const partition_column_identifier& pid ) const {
    auto found = partitions_.find( pid );
    if( found == partitions_.end() ) {
        return nullptr;
    }
    return found->second;
}

inline void transaction_partition_holder::add_do_not_apply_last(
    const partition_column_identifier_set& pids ) {
    do_not_apply_last_partition_ids_.insert( pids.begin(), pids.end() );
}
inline void transaction_partition_holder::add_do_not_apply_last(
    const std::vector<partition_column_identifier>& pids ) {
    do_not_apply_last_partition_ids_.insert( pids.begin(), pids.end() );
}

inline void transaction_partition_holder::add_partition(
    const partition_column_identifier& pid, std::shared_ptr<partition> part,
    partition_column_identifier_set& parts ) {
    partitions_.emplace( pid, std::move( part ) );
    parts.emplace( pid );
    table_to_poll_epoch_[pid.table_id] = 0;
}

inline void transaction_partition_holder::add_write_partition(
    const partition_column_identifier& pid, std::shared_ptr<partition> part ) {
    add_partition( pid, part, write_partition_ids_ );
}

inline void transaction_partition_holder::add_read_partition(
    const partition_column_identifier& pid, std::shared_ptr<partition> part ) {
    add_partition( pid, part, read_partition_ids_ );
}

inline void transaction_partition_holder::assert_readable_partition(
    const partition_column_identifier& pid ) const {
    DCHECK( ( 1 == read_partition_ids_.count( pid ) ) );
}

inline void transaction_partition_holder::assert_writeable_partition(
    const partition_column_identifier& pid ) const {
    DCHECK_EQ( 1, write_partition_ids_.count( pid ) );
}

