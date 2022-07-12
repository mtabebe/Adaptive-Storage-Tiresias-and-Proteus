#include "record_identifier.h"

static_assert( sizeof( record_identifier ) == 16,
               "record_identifier should be 16 bytes because padding" );

record_identifier::record_identifier() : table_id_( 0 ), key_( 0 ) {}
record_identifier::record_identifier( uint32_t table_id, uint64_t key )
    : table_id_( table_id ), key_( key ) {}
record_identifier::record_identifier( const ::primary_key& pk )
    : table_id_( pk.table_id ), key_( pk.row_id ) {}

bool is_rid_within_pid( const partition_identifier& pid,
                        const record_identifier&    rid ) {
    if( (uint64_t) pid.table_id != rid.table_id_ ) {
        return false;
    }
    if( ( (uint64_t) pid.partition_start <= rid.key_ ) &&
        ( (uint64_t) pid.partition_end >= rid.key_ ) ) {
        return true;
    }
    return false;
}

std::ostream& operator<<( std::ostream& os, const record_identifier& rid ) {
    os << "Record: [ Table:" << rid.table_id_ << ", Key:" << rid.key_ << "]";
    return os;
}
