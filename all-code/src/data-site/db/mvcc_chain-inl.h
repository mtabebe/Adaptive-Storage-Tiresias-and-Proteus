#pragma once

#include <glog/logging.h>
#include <atomic>

inline bool mvcc_chain::is_chain_full() const {
    bool is_full = ( next_row_record_pos_ < 0 );
    return is_full;
}

inline bool mvcc_chain::is_chain_empty() const {
    bool is_empty = ( next_row_record_pos_ == row_record_len_ - 1 );
    return is_empty;
}

inline uint64_t mvcc_chain::get_partition_id() const {
    return partition_id_.load( std::memory_order_acquire );
}

inline void mvcc_chain::set_partition_id( uint64_t partition_id ) {
    partition_id_.store( partition_id, std::memory_order_release );
}

inline mvcc_chain* mvcc_chain::get_next_link_in_chain() const {
    return next_chain_;
}

inline row_record* mvcc_chain::find_row_record(
    const snapshot_vector& snapshot ) const {
    return internal_find_row_record<false /*no tracking*/>( snapshot, nullptr );
}

inline row_record* mvcc_chain::get_latest_row_record( bool recurse ) const {
    uint64_t partition_id = 0;
    return internal_get_latest_row_record( recurse, partition_id );
}
