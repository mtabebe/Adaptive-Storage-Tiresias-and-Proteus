#include "cell_identifier.h"

static_assert( sizeof( cell_identifier ) == 16,
               "cell_identifier should be 16 bytes" );

cell_identifier::cell_identifier() : table_id_( 0 ), col_id_( 0 ), key_( 0 ) {}
cell_identifier::cell_identifier( uint32_t table_id, uint64_t key,
                                  uint32_t col_id )
    : table_id_( table_id ), col_id_( col_id ), key_( key ) {}
cell_identifier::cell_identifier( const ::cell_key& ck )
    : table_id_( ck.table_id ), col_id_( ck.col_id ), key_( ck.row_id ) {}

bool is_cid_within_pcid( const partition_column_identifier& pcid,
                         const cell_identifier&             cid ) {
    if( (uint64_t) pcid.table_id != cid.table_id_ ) {
        return false;
    }
    if( ( (uint64_t) pcid.partition_start > cid.key_ ) or
        ( (uint64_t) pcid.partition_end < cid.key_ ) ) {
        return false;
    }
    if( ( (uint64_t) pcid.column_start > cid.col_id_ ) or
        ( (uint64_t) pcid.column_end < cid.col_id_ ) ) {
        return false;
    }
    return true;
}

std::ostream& operator<<( std::ostream& os, const cell_identifier& cid ) {
    os << "Record: [ Table:" << cid.table_id_ << ", Key:" << cid.key_
       << ", ColumnID:" << cid.col_id_ << "]";
    return os;
}

cell_identifier create_cell_identifier( uint32_t table_id, uint32_t col_id,
                                        uint64_t row_id ) {
    cell_identifier cid( table_id, row_id, col_id );
    return cid;
}
cell_key_ranges cell_key_ranges_from_cell_identifier(
    const cell_identifier& cid ) {
    cell_key_ranges ckr;
    ckr.table_id = cid.table_id_;
    ckr.col_id_start = cid.col_id_;
    ckr.col_id_end = cid.col_id_;
    ckr.row_id_start = cid.key_;
    ckr.row_id_end = cid.key_;
    return ckr;
}

