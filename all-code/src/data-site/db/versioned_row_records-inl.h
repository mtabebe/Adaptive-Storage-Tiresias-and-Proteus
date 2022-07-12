#pragma once

#include <glog/logging.h>

inline uint32_t versioned_row_records::get_record_pos( uint64_t key ) const {
    DCHECK_GE( key, metadata_.partition_id_.partition_start );
    DCHECK_LE( key, metadata_.partition_id_.partition_end );

    uint32_t offset =
        ( uint32_t )( key - metadata_.partition_id_.partition_start );
    return offset;
}

inline void versioned_row_records::set_record(
    uint64_t key, std::shared_ptr<versioned_row_record> record ) {
    uint32_t pos = get_record_pos( key );
    DVLOG( 40 ) << "Set record (key):" << key << ", pos:" << pos;
    records_.at( pos ) = record;
    DVLOG( 40 ) << "Set record (key):" << key << ", pos:" << pos
                << ", record:" << records_.at( pos )
                << ", use count:" << records_.at( pos ).use_count();
}

inline std::shared_ptr<versioned_row_record> versioned_row_records::get_record(
    uint64_t key ) const {
    uint32_t pos = get_record_pos( key );
    DVLOG( 40 ) << "Get record (key):" << key << ", pos:" << pos;
    std::shared_ptr<versioned_row_record> rec = records_.at( pos );
    DVLOG( 40 ) << "Got record (key):" << key << ", pos:" << pos
                << ", record:" << rec << ", use count:" << rec.use_count();
    return rec;
}

