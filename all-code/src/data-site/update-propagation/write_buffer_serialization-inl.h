#pragma once

#include <glog/logging.h>

#include "../site-manager/serialize.h"

inline uint64_t get_serialized_update_len( const serialized_update& update ) {
    DCHECK_GE( update.length_, get_serialized_update_min_length() );

    uint64_t* as_int64_t = (uint64_t*) ( update.buffer_ );
    uint64_t  len = htonll( *as_int64_t );

    return len;
}
inline uint64_t get_serialized_update_message_id(
    const serialized_update& update ) {
    DCHECK_GE( update.length_, get_serialized_update_min_length() );

    uint64_t* as_int64_t = (uint64_t*) ( update.buffer_ + sizeof( int64_t ) );
    uint64_t  id = htonll( *as_int64_t );

    return id;
}
inline uint32_t get_serialized_update_version(
    const serialized_update& update ) {
    DCHECK_GE( update.length_, get_serialized_update_min_length() );

    uint32_t* as_int32_t =
        (uint32_t*) ( update.buffer_ + ( sizeof( int64_t ) * 2 ) );
    uint32_t version = htonl( *as_int32_t );

    return version;
}
#if defined( RECORD_COMMIT_TS )
inline uint64_t get_serialized_update_txn_timestamp_offset() {
    return sizeof( uint64_t ) * 2 + sizeof( int32_t );
}

inline uint64_t get_serialized_update_txn_timestamp(
    const serialized_update& update ) {
    DCHECK_GE( update.length_, get_serialized_update_min_length() );

    uint64_t* as_int64_t =
        (uint64_t*) ( update.buffer_ + ( get_serialized_update_txn_timestamp_offset() )  );
    uint64_t ts = htonll( *as_int64_t );

    return ts;
}
#endif

inline uint64_t get_serialized_update_footer(
    const serialized_update& update ) {
    DCHECK_GE( update.length_, get_serialized_update_min_length() );
    uint64_t* as_int64_array =
        (uint64_t*) ( ( (char*) update.buffer_ ) + update.length_ -
                      sizeof( int64_t ) );
    uint64_t footer = htonll( *as_int64_array );
    return footer;
}

inline uint64_t get_serialized_update_min_length() {
    return ( sizeof( int64_t ) * 3 ) + sizeof( int32_t );
}

inline bool is_serialized_update_footer_ok( const serialized_update& update ) {
    uint64_t expected_footer = get_serialized_update_len( update ) *
                               get_serialized_update_message_id( update ) *
                               get_serialized_update_version( update );
    uint64_t actual_footer = get_serialized_update_footer( update );
    /*

    DVLOG( 20 ) << "expected_footer: " << expected_footer
                << ", actual_footer:" << actual_footer;
    DVLOG( 20 ) << "len:" << get_serialized_update_len( update )
                << ", message_id:" << get_serialized_update_message_id( update )
                << ", version:" << get_serialized_update_version( update );
                */
    return ( expected_footer == actual_footer );

}
