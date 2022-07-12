#include "write_buffer_serialization.h"

#include <cstring>

#include "../../common/partition_funcs.h"
#include "../../common/perf_tracking.h"
#include "../db/op_codes.h"
#include "../site-manager/serialize.h"

serialized_update serialize_write_buffer( write_buffer* write_buf ) {
    start_timer( SERIALIZE_WRITE_BUFFER_TIMER_ID );

    serialized_update update;
    update.length_ = 0;
    update.buffer_ = NULL;

    DVLOG( 20 ) << "Serializing write buffer";

    update.length_ = compute_serialization_length_2( write_buf );
    DVLOG( 20 ) << "Serializing write buffer has length:" << update.length_;
    void* allocated_buffer = malloc( (size_t) update.length_ );
    update.buffer_ = (char*) allocated_buffer;
    write_into_serialized_update_2( update, write_buf );

    DCHECK_EQ( update.length_, get_serialized_update_len( update ) );
    DCHECK_EQ( 2, get_serialized_update_version( update ) );

    stop_timer( SERIALIZE_WRITE_BUFFER_TIMER_ID );

    return update;
}

int64_t compute_serialization_length_2( write_buffer* write_buf ) {
    int64_t size = 0;
    size += sizeof( int64_t );  // total length field;
    size += sizeof( int64_t );  // message id
    size += sizeof( int32_t );  // magic_version
#if defined( RECORD_COMMIT_TS )
    size += sizeof( uint64_t );  // timestamp
#endif
    int64_t pcid_size = ( sizeof( int64_t ) * 2 ) + ( sizeof( int32_t ) * 3 );
    size += pcid_size;           // ( partition identifier)
    size += sizeof( int32_t );   // is_new_partition_
    size += sizeof( int32_t );  // num sites
    size += sizeof( int64_t ) * 2 *
            write_buf->commit_vv_.size();  // the version vector;
    size += sizeof( int32_t );             // num_partition_ops
    int64_t partition_op_size =
        sizeof( int32_t ) + sizeof( int32_t ) + sizeof( int64_t ) + pcid_size;
    size += write_buf->partition_buffer_.size() * partition_op_size;

    size += sizeof( int32_t );             // num_cells

    uint32_t num_records_to_propagate =
        write_buf->record_buffer_.size() + write_buf->cell_buffer_.size();

    // things that are the same for everyone
    int64_t base_record_size = 0;
    base_record_size += sizeof( uint32_t );  // table
    base_record_size += sizeof( uint32_t );  // col_id
    base_record_size += sizeof( uint64_t );  // key
    base_record_size += sizeof( uint64_t );  // op code

    size += ( base_record_size * num_records_to_propagate );

    uint64_t op = K_NO_OP;
    for( uint32_t record_pos = 0; record_pos < write_buf->record_buffer_.size();
         record_pos++ ) {
        const versioned_row_record_identifier& vri =
            write_buf->record_buffer_[record_pos];
        op = vri.get_op();

        if( ( ( op == K_INSERT_OP ) or ( op == K_WRITE_OP ) ) and
            !vri.is_delete_op() ) {
            size += sizeof( int32_t );  // length of buffer
                                        // plus the actual buf length
            const packed_cell_data& pcd =
                get_cell_data_from_vri( write_buf->pcid_, vri );
            size += pcd.get_length();
        }
        // otherwise no size
    }
    op = K_NO_OP;
    for( uint32_t record_pos = 0; record_pos < write_buf->cell_buffer_.size();
         record_pos++ ) {
        const versioned_cell_data_identifier& vci =
            write_buf->cell_buffer_[record_pos];
        op = vci.get_op();

        if( ( ( op == K_INSERT_OP ) or ( op == K_WRITE_OP ) ) and
            !vci.is_delete_op() ) {
            size += sizeof( int32_t );  // length of buffer
                                        // plus the actual buf length
            size += vci.get_data()->get_length();
        }
        // otherwise no size
    }


    size += sizeof( int32_t );             // start_subscribing size
    int64_t do_seek_size = sizeof( int8_t );
    for( const update_propagation_information& info :
         write_buf->start_subscribing_ ) {
        size += pcid_size;  // pcid
        size += compute_serialization_length_of_propagation_config(
            info.propagation_config_ );
        size += do_seek_size;  // do_seek
    }

    size += sizeof( int32_t );             // stop subscribing size
    for( const update_propagation_information& info :
         write_buf->stop_subscribing_ ) {
        size += pcid_size;  // pcid
        size += compute_serialization_length_of_propagation_config(
            info.propagation_config_ );
        size += do_seek_size;  // do_seek
    }

    size += sizeof( int32_t );             // switch subscription size
    DCHECK_EQ( 0, write_buf->switch_subscription_.size() % 2 );
    for( const update_propagation_information& info :
         write_buf->switch_subscription_ ) {
        size += pcid_size;  // pcid
        size += compute_serialization_length_of_propagation_config(
            info.propagation_config_ );
        size += do_seek_size;  // do_seek
    }

    // footer
    size += sizeof( int64_t );

    return size;
}

int64_t compute_serialization_length_of_propagation_config(
    const propagation_configuration& config ) {
    int64_t size = sizeof( int32_t );

    if( config.type != propagation_type::NO_OP ) {
        size += sizeof( int32_t );  // partition
        size += sizeof( int64_t );  // offset

        if( config.type == propagation_type::KAFKA ) {
            size += sizeof( int32_t );
            size += config.topic.size();
        }
    }
    return size;
}


void write_into_serialized_update_2( serialized_update& update,
                                     write_buffer*      write_buf ) {
    DVLOG( 20 ) << "Writing into serialized update";
    uint64_t pos = 0;

    uint32_t* buffer_as_int32;
    uint64_t* buffer_as_int64;
    // write the length
    DVLOG( 30 ) << "Writing into serialized update: length:" << update.length_
                << ", pos:" << pos;
    buffer_as_int64 = (uint64_t*) ( update.buffer_ + pos );
    *buffer_as_int64 = htonll( update.length_ );
    pos += sizeof( int64_t );
    DCHECK_LT( pos, update.length_ );

    uint64_t message_id =
        get_snapshot_version( write_buf->commit_vv_, write_buf->partition_id_ );
    DVLOG( 30 ) << "Writing into serialized update: message_id:" << message_id
                << ", pos:" << pos;
    buffer_as_int64 = (uint64_t*) ( update.buffer_ + pos );
    *buffer_as_int64 = htonll( message_id );
    pos += sizeof( int64_t );
    DCHECK_LT( pos, update.length_ );

    // write the version
    uint32_t version = 2;
    DVLOG( 30 ) << "Writing into serialized update: version:" << version
                << ", pos:" << pos;
    buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
    *buffer_as_int32 = htonl( version );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );

#if defined( RECORD_COMMIT_TS )
    uint64_t ts = write_buf->txn_ts_;
    DVLOG( 30 ) << "Writing into serialized update: commit ts: " << ts
                << ", pos:" << pos;
    buffer_as_int64 = (uint64_t *) ( update.buffer_ + pos );
    *buffer_as_int64 = htonll( ts );
    pos += sizeof( uint64_t );
    DCHECK_LT( pos, update.length_ );
#endif

    // partition identifier
    DVLOG( 30 ) << "Writing into serialized update: pcid: " << write_buf->pcid_
                << ", pos:" << pos;
    pos = serialize_partition_id_2( write_buf->pcid_, update, pos );
    DCHECK_LT( pos, update.length_ );

    DVLOG( 30 ) << "Writing into serialized update: new_partition: "
                << write_buf->is_new_partition_ << ", pos:" << pos;
    uint32_t new_partition = write_buf->is_new_partition_;  // MTODO
    buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
    *buffer_as_int32 = htonl( new_partition );
    pos += sizeof( uint32_t );
    DCHECK_LT( pos, update.length_ );

    uint32_t num_sites = write_buf->commit_vv_.size();
    buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
    *buffer_as_int32 = htonl( num_sites );
    pos += sizeof( uint32_t );
    DCHECK_LT( pos, update.length_ );

    for( const auto& snap_version : write_buf->commit_vv_ ) {
        uint64_t pcid = snap_version.first;
        uint64_t version = snap_version.second;

        buffer_as_int64 = (uint64_t*) ( update.buffer_ + pos );
        *buffer_as_int64 = htonll( pcid );
        pos += sizeof( uint64_t );

        buffer_as_int64 = (uint64_t*) ( update.buffer_ + pos );
        *buffer_as_int64 = htonll( version );
        pos += sizeof( uint64_t );
        DCHECK_LT( pos, update.length_ );
    }

    uint32_t num_partition_ops = write_buf->partition_buffer_.size();
    buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
    *buffer_as_int32 = htonl( num_partition_ops );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );

    // serialize records
    for( uint32_t record_pos = 0; record_pos < num_partition_ops;
         record_pos++ ) {
        const partition_column_operation_identifier& poi =
            write_buf->partition_buffer_[record_pos];
        pos = serialize_partition_2( poi, update, pos );
        DCHECK_LT( pos, update.length_ );
    }

    // number of records
    uint32_t num_records_to_propagate =
        write_buf->record_buffer_.size() + write_buf->cell_buffer_.size();

    DVLOG( 30 ) << "Writing into serialized update: num_records_to_propagate_:"
                << num_records_to_propagate << ", pos:" << pos;

    buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
    *buffer_as_int32 = htonl( num_records_to_propagate );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );

    // serialize records
    for( uint32_t record_pos = 0; record_pos < write_buf->record_buffer_.size();
         record_pos++ ) {
        const versioned_row_record_identifier& vri =
            write_buf->record_buffer_[record_pos];
        pos = serialize_record_2( write_buf->pcid_, vri, update, pos );
        DCHECK_LT( pos, update.length_ );
    }
    for( uint32_t record_pos = 0; record_pos < write_buf->cell_buffer_.size();
         record_pos++ ) {
        const versioned_cell_data_identifier& vci =
            write_buf->cell_buffer_[record_pos];
        pos = serialize_cell_2( write_buf->pcid_, vci, update, pos );
        DCHECK_LT( pos, update.length_ );
    }

    //start subscribing
    DVLOG( 30 ) << "Writing into serialized update: num start subscribing:"
                << write_buf->start_subscribing_.size() << ", pos:" << pos;

    buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
    *buffer_as_int32 = htonl( write_buf->start_subscribing_.size() );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );

    for( const update_propagation_information& info :
         write_buf->start_subscribing_ ) {
        pos = serialize_update_propagation_information_2( info, update, pos );
        DCHECK_LT( pos, update.length_ );
    }

    // stop subscribing
    DVLOG( 30 ) << "Writing into serialized update: num stop subscribing:"
                << write_buf->stop_subscribing_.size() << ", pos:" << pos;
    buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
    *buffer_as_int32 = htonl( write_buf->stop_subscribing_.size() );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );

    for( const update_propagation_information& info :
         write_buf->stop_subscribing_ ) {
        pos = serialize_update_propagation_information_2( info, update, pos );
        DCHECK_LT( pos, update.length_ );
    }

    // switch subscription
    DVLOG( 30 ) << "Writing into serialized update: num switch subscription:"
                << write_buf->switch_subscription_.size() << ", pos:" << pos;
    DCHECK_EQ( 0, write_buf->switch_subscription_.size() % 2 );
    buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
    *buffer_as_int32 = htonl( write_buf->switch_subscription_.size() );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );
    for( const update_propagation_information& info :
         write_buf->switch_subscription_ ) {
        pos = serialize_update_propagation_information_2( info, update, pos );
        DCHECK_LT( pos, update.length_ );
    }

    uint64_t footer = update.length_ * 1 * message_id * version;  // version = 2
    DVLOG( 30 ) << "Writing into serialized update: footer:" << footer
                << ", pos:" << pos;

    buffer_as_int64 = (uint64_t*) ( update.buffer_ + pos );
    *buffer_as_int64 = htonll( footer );

    pos += sizeof( int64_t );
    DCHECK_EQ( pos, update.length_ );
}

uint64_t serialize_record_2( const partition_column_identifier&     pcid,
                             const versioned_row_record_identifier& vri,
                             serialized_update& update, uint64_t pos ) {

    uint32_t* buffer_as_int32;
    uint64_t* buffer_as_int64;
    char*     buffer_as_char;

    const cell_identifier& cid = vri.identifier_;
    uint64_t op = vri.get_op();

    DVLOG( 40 ) << "Serializing record:" << cid << ", op:" << op;

    if( vri.is_delete_op() ) {
        // it's actually a delete in sheeps clothing
        DVLOG( 40 ) << "Changing op type to delete:" << op << ", cid:" << cid;
        op = K_DELETE_OP;
    }

    // table id
    DVLOG( 30 ) << "Writing into serialized update: table_id:" << cid.table_id_
                << ", pos:" << pos;

    buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
    *buffer_as_int32 = htonl( cid.table_id_ );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );

    DVLOG( 30 ) << "Writing into serialized update: column_id:" << cid.col_id_
                << ", pos:" << pos;

    buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
    *buffer_as_int32 = htonl( cid.col_id_ );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );


    // key
    DVLOG( 30 ) << "Writing into serialized update: key:" << cid.key_
                << ", pos:" << pos;
    buffer_as_int64 = (uint64_t*) ( update.buffer_ + pos );
    *buffer_as_int64 = htonll( cid.key_ );
    pos += sizeof( int64_t );
    DCHECK_LT( pos, update.length_ );

    // op
    DVLOG( 30 ) << "Writing into serialized update: op:" << op
                << ", pos:" << pos;
    buffer_as_int64 = (uint64_t*) ( update.buffer_ + pos );
    *buffer_as_int64 = htonll( op );
    pos += sizeof( int64_t );
    DCHECK_LT( pos, update.length_ );


    if( ( op == K_INSERT_OP ) or ( op == K_WRITE_OP ) ) {
        // length
        const packed_cell_data& pcd = get_cell_data_from_vri( pcid, vri );
        uint32_t                len = pcd.get_length();
        DVLOG( 30 ) << "Writing into serialized update: len:" << len
                    << ", pos:" << pos;

        buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
        *buffer_as_int32 = htonl( len );
        pos += sizeof( int32_t );
        DCHECK_LT( pos, update.length_ );

        // record data
        char* record_buf = pcd.get_buffer_data();
        DVLOG( 30 ) << "Writing into serialized update: record_buf:"
                    << std::string( record_buf, len ) << ", pos:" << pos;
        buffer_as_char = (char*) ( update.buffer_ + pos );
        std::memcpy( (void*) buffer_as_char, (void*) record_buf, len );
        pos += len;
    }

    DCHECK_LT( pos, update.length_ );

    return pos;
}

uint64_t serialize_cell_2( const partition_column_identifier&    pcid,
                           const versioned_cell_data_identifier& vci,
                           serialized_update& update, uint64_t pos ) {
    uint32_t* buffer_as_int32;
    uint64_t* buffer_as_int64;
    char*     buffer_as_char;

    uint64_t op = vci.get_op();
    const cell_identifier& cid = vci.identifier_;
    DVLOG( 40 ) << "Serializing cell:" << cid << ", op:" << op;

    if( vci.is_delete_op() ) {
        // it's actually a delete in sheeps clothing
        DVLOG( 40 ) << "Changing op type to delete:" << op << ", cid:" << cid;
        op = K_DELETE_OP;
    }

    // table id
    DVLOG( 30 ) << "Writing into serialized update: table_id:" << cid.table_id_
                << ", pos:" << pos;

    buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
    *buffer_as_int32 = htonl( cid.table_id_ );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );

    DVLOG( 30 ) << "Writing into serialized update: column_id:" << cid.col_id_
                << ", pos:" << pos;

    buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
    *buffer_as_int32 = htonl( cid.col_id_ );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );


    // key
    DVLOG( 30 ) << "Writing into serialized update: key:" << cid.key_
                << ", pos:" << pos;
    buffer_as_int64 = (uint64_t*) ( update.buffer_ + pos );
    *buffer_as_int64 = htonll( cid.key_ );
    pos += sizeof( int64_t );
    DCHECK_LT( pos, update.length_ );

    // op
    DVLOG( 30 ) << "Writing into serialized update: op:" << op
                << ", pos:" << pos;
    buffer_as_int64 = (uint64_t*) ( update.buffer_ + pos );
    *buffer_as_int64 = htonll( op );
    pos += sizeof( int64_t );
    DCHECK_LT( pos, update.length_ );


    if( ( op == K_INSERT_OP ) or ( op == K_WRITE_OP ) ) {
        // length
        packed_cell_data*       pcd = vci.get_data();
        uint32_t                len = pcd->get_length();
        DVLOG( 30 ) << "Writing into serialized update: len:" << len
                    << ", pos:" << pos;

        buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
        *buffer_as_int32 = htonl( len );
        pos += sizeof( int32_t );
        DCHECK_LT( pos, update.length_ );

        // record data
        char* record_buf = pcd->get_buffer_data();
        DVLOG( 30 ) << "Writing into serialized update: record_buf:"
                    << std::string( record_buf, len ) << ", pos:" << pos;
        buffer_as_char = (char*) ( update.buffer_ + pos );
        std::memcpy( (void*) buffer_as_char, (void*) record_buf, len );
        pos += len;
    }

    DCHECK_LT( pos, update.length_ );

    return pos;

}

uint64_t serialize_partition_2( const partition_column_operation_identifier& poi,
                                serialized_update& update, uint64_t pos ) {
    DVLOG( 30 ) << "Writing into serialized_update: poi: pcid:"
                << poi.identifier_ << ", data_64_:" << poi.data_64_
                << ", data_32_:" << poi.data_32_ << ", op:" << poi.op_code_
                << ", pos:" << pos;

    pos = serialize_partition_id_2( poi.identifier_, update, pos );

    uint64_t* buffer_as_int64;
    uint32_t* buffer_as_int32;

    buffer_as_int64 = (uint64_t*) ( update.buffer_ + pos );
    *buffer_as_int64 = htonll( poi.data_64_ );
    pos += sizeof( int64_t );
    DCHECK_LT( pos, update.length_ );

    buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
    *buffer_as_int32 = htonl( poi.data_32_ );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );


    buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
    *buffer_as_int32 = htonl( poi.op_code_ );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );

    if( poi.op_code_ == K_SPLIT_OP ) {
        DVLOG( 10 )
            << "Serializing partition_column_operation_identifier: identifier_:"
            << poi.identifier_ << ", data_64:" << poi.data_64_
            << ", data_32:" << poi.data_32_
            << ", split op_code:" << poi.op_code_ << ", pos" << pos;
    }


    return pos;
}

uint64_t serialize_partition_id_2( const partition_column_identifier& pcid,
                                   serialized_update& update, uint64_t pos ) {
    uint32_t* buffer_as_int32;
    uint64_t* buffer_as_int64;
    DVLOG( 40 ) << "Writing into serialized update: pcid:" << pcid;

    DVLOG( 40 ) << "Writing into serialized update: table_id:" << pcid.table_id
                << ", pos:" << pos;

    buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
    *buffer_as_int32 = htonl( pcid.table_id );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );

    DVLOG( 40 ) << "Writing into serialized update: partition_start:"
                << pcid.partition_start << ", pos:" << pos;

    buffer_as_int64 = (uint64_t*) ( update.buffer_ + pos );
    *buffer_as_int64 = htonll( pcid.partition_start );
    pos += sizeof( int64_t );
    DCHECK_LT( pos, update.length_ );

    DVLOG( 40 ) << "Writing into serialized update: partition_end:"
                << pcid.partition_end << ", pos:" << pos;

    buffer_as_int64 = (uint64_t*) ( update.buffer_ + pos );
    *buffer_as_int64 = htonll( pcid.partition_end );
    pos += sizeof( int64_t );
    DCHECK_LT( pos, update.length_ );

    DVLOG( 40 ) << "Writing into serialized update: column_start:"
                << pcid.column_start << ", pos:" << pos;

    buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
    *buffer_as_int32 = htonl( pcid.column_start );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );

    DVLOG( 40 ) << "Writing into serialized update: column_end:"
                << pcid.column_end << ", pos:" << pos;

    buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
    *buffer_as_int32 = htonl( pcid.column_end );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );


    return pos;
}

uint64_t serialize_update_propagation_information_2(
    const update_propagation_information& info, serialized_update& update,
    uint64_t pos ) {
    // -- partition id size
    // -- propagation_configuration size
    // --- type (i32)
    // ---  topic string
    // ---- len (i32)
    // ---- buffer (length size)
    // --- partition i32
    // --- offset  i64
    // do seek i8

    uint32_t* buffer_as_int32;
    uint64_t* buffer_as_int64;
    uint8_t* buffer_as_int8;
    char*    buffer_as_char;

    DVLOG(40) << "Writing into serialized update: prop info";
    DVLOG( 40 ) << "Writing into serialized update: prop info type:"
                << info.propagation_config_.type << ", pos:" << pos;

    buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
    *buffer_as_int32 = htonl( (uint32_t) info.propagation_config_.type );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );

    if( info.propagation_config_.type != propagation_type::NO_OP ) {

        DVLOG( 40 ) << "Writing into serialized update: prop info partition:"
                    << info.propagation_config_.partition << ", pos:" << pos;

        buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
        *buffer_as_int32 = htonl( info.propagation_config_.partition );
        pos += sizeof( int32_t );
        DCHECK_LT( pos, update.length_ );

        DVLOG( 40 ) << "Writing into serialized update: prop info offset:"
                    << info.propagation_config_.offset << ", pos:" << pos;

        buffer_as_int64 = (uint64_t*) ( update.buffer_ + pos );
        *buffer_as_int64 = htonll( info.propagation_config_.offset );
        pos += sizeof( int64_t );
        DCHECK_LT( pos, update.length_ );

        if( info.propagation_config_.type == propagation_type::KAFKA ) {
            DVLOG( 40 )
                << "Writing into serialized update: prop info topic (as str):"
                << info.propagation_config_.topic << ", pos:" << pos;

            buffer_as_int32 = (uint32_t*) ( update.buffer_ + pos );
            *buffer_as_int32 = htonl( info.propagation_config_.topic.size() );
            pos += sizeof( int32_t );
            DCHECK_LT( pos, update.length_ );

            buffer_as_char = (char*) ( update.buffer_ + pos );
            std::memcpy( (void*) buffer_as_char,
                         (void*) info.propagation_config_.topic.c_str(),
                         info.propagation_config_.topic.size() );
            pos += info.propagation_config_.topic.size();
            DCHECK_LT( pos, update.length_ );
        }
    }

    pos = serialize_partition_id_2( info.identifier_, update, pos );

    DVLOG( 40 ) << "Writing into serialized update: prop info do seek:"
                << info.do_seek_ << ", pos:" << pos;
    buffer_as_int8 = (uint8_t*) ( update.buffer_ + pos );
    *buffer_as_int8 = (uint8_t) info.do_seek_;
    pos += sizeof( uint8_t );

    DCHECK_LT( pos, update.length_ );

    return pos;
}

deserialized_update* deserialize_update( const serialized_update& update ) {
    DVLOG( 20 ) << "Deserializing update";

    uint64_t len = get_serialized_update_len( update );
    uint64_t message_id = get_serialized_update_message_id( update );
    DVLOG( 30 ) << "Deserializing update: len:" << len
                << ", message id:" << message_id;
    DCHECK_EQ( len, update.length_ );

    uint32_t version = get_serialized_update_version( update );
    DVLOG( 30 ) << "Deserializing update: version:" << version;
    DCHECK_GT( version, 0 );

#if defined( RECORD_COMMIT_TS )
    uint64_t txn_ts = get_serialized_update_txn_timestamp( update );
    DVLOG( 30 ) << "Deserializing update: ts:" << txn_ts;
    DCHECK_GT( txn_ts, 0 );
#endif

    // check footer
    uint64_t footer = get_serialized_update_footer( update );
    DVLOG( 30 ) << "Deserializing update: footer:" << footer;
    DCHECK( is_serialized_update_footer_ok( update ) );

    if( version == 2 ) {
        return deserialize_update_2( update );
    }

    return nullptr;
}

deserialized_update* deserialize_update_2( const serialized_update& update ) {

    DVLOG( 30 ) << "Deserializing update: version 2";
    uint64_t pos = 0;

    int64_t* as_int64_array;
    int32_t* as_int32_array;

    deserialized_update* deserialized = new deserialized_update();
    deserialized->table_ = nullptr;

    // jump forward total length, id, magic_version
    pos += sizeof( int64_t ) + sizeof( int64_t ) + sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );

#if defined( RECORD_COMMIT_TS )
    as_int64_array = (int64_t*) ( update.buffer_ + pos );
    deserialized->txn_ts_ = ntohll( (uint64_t) *as_int64_array );
    pos += sizeof( int64_t );
    DCHECK_LT( pos, update.length_ );
    DVLOG( 30 ) << "Deserializing update: txn_ts:" << deserialized->txn_ts_;
#endif

    pos = deserialize_partition_id_2( update, &deserialized->pcid_, pos );

    as_int32_array = (int32_t*) ( update.buffer_ + pos );
    deserialized->is_new_partition_ = ntohl( *as_int32_array );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );

    as_int32_array = (int32_t*) ( update.buffer_ + pos );
    uint32_t num_pcids = ntohl( *as_int32_array );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );

    for (uint32_t iter = 0; iter < num_pcids; iter++) {
        as_int64_array = (int64_t*) ( update.buffer_ + pos );
        uint64_t part_id = ntohll( *as_int64_array );
        pos += sizeof( int64_t );
        DCHECK_LT( pos, update.length_ );

        as_int64_array = (int64_t*) ( update.buffer_ + pos );
        uint64_t version = ntohll( *as_int64_array );
        pos += sizeof( int64_t );
        DCHECK_LT( pos, update.length_ );

        set_snapshot_version( deserialized->commit_vv_, part_id, version );
    }

    as_int32_array = (int32_t*) ( update.buffer_ + pos );
    uint32_t num_partition_ops = ntohl( *as_int32_array );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );
    DVLOG( 30 ) << "Deserializing update: num_partitions:" << num_partition_ops;
    deserialized->cell_updates_.reserve( num_partition_ops );

    for( uint32_t partition_pos = 0; partition_pos < num_partition_ops;
         partition_pos++ ) {
        pos = deserialize_partition_2( update, deserialized, pos );
    }


    as_int32_array = (int32_t*) ( update.buffer_ + pos );
    int32_t num_records = ntohl( *as_int32_array );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );
    DVLOG( 30 ) << "Deserializing update: num_record:" << num_records;
    deserialized->cell_updates_.reserve( num_records );
    for( int32_t record_pos = 0; record_pos < num_records; record_pos++ ) {
        pos = deserialize_cell_2( update, deserialized, pos );
        DCHECK_LT( pos, update.length_ );
    }

    // start subscribing
    as_int32_array = (int32_t*) ( update.buffer_ + pos );
    int32_t num_start_subscriptions = ntohl( *as_int32_array );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );
    DVLOG( 30 ) << "Deserializing start subscription: num_start_subscriptions:"
                << num_start_subscriptions;
    deserialized->start_subscribing_.reserve( num_start_subscriptions );
    for( int32_t subscription_pos = 0;
         subscription_pos < num_start_subscriptions; subscription_pos++ ) {
        pos = deserialize_update_propagation_information_2(
            update, &( deserialized->start_subscribing_ ), pos );
        DCHECK_LT( pos, update.length_ );
    }

    // stop subscribing
    as_int32_array = (int32_t*) ( update.buffer_ + pos );
    int32_t num_stop_subscriptions = ntohl( *as_int32_array );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );
    DVLOG( 30 ) << "Deserializing stop subscription: num_stop_subscriptions:"
                << num_stop_subscriptions;
    deserialized->stop_subscribing_.reserve( num_stop_subscriptions );
    for( int32_t subscription_pos = 0;
         subscription_pos < num_stop_subscriptions; subscription_pos++ ) {
        pos = deserialize_update_propagation_information_2(
            update, &( deserialized->stop_subscribing_ ), pos );
        DCHECK_LT( pos, update.length_ );
    }

    // switch subscription
    as_int32_array = (int32_t*) ( update.buffer_ + pos );
    int32_t num_switch_subscriptions = ntohl( *as_int32_array );
    DCHECK_EQ( 0, num_switch_subscriptions % 2 );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );
    DVLOG( 30 )
        << "Deserializing switch subscription: num_switch_subscriptions:"
        << num_switch_subscriptions;
    deserialized->switch_subscription_.reserve( num_switch_subscriptions );
    for( int32_t subscription_pos = 0;
         subscription_pos < num_switch_subscriptions; subscription_pos++ ) {
        pos = deserialize_update_propagation_information_2(
            update, &( deserialized->switch_subscription_ ), pos );
        DCHECK_LT( pos, update.length_ );
    }

    // footer no need to read it, we've already checked this
    DCHECK_EQ( pos, update.length_ - sizeof( int64_t ) );

    return deserialized;
}

uint64_t deserialize_partition_2( const serialized_update& update,
                                  deserialized_update*     deserialized,
                                  uint64_t                 pos ) {
    int32_t* as_int32_array;
    int64_t* as_int64_array;

    partition_column_operation_identifier poi;

    pos = deserialize_partition_id_2( update, &poi.identifier_, pos );

    as_int64_array = (int64_t*) ( update.buffer_ + pos );
    poi.data_64_ = ntohll( *as_int64_array );
    pos += sizeof( int64_t );
    DCHECK_LT( pos, update.length_ );

    as_int32_array = (int32_t*) ( update.buffer_ + pos );
    poi.data_32_ = ntohl( *as_int32_array );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );


    as_int32_array = (int32_t*) ( update.buffer_ + pos );
    poi.op_code_ = ntohl( *as_int32_array );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );

    DVLOG( 30 )
        << "Deserializing partition_column_operation_identifier: identifier_:"
        << poi.identifier_ << ", data_64_:" << poi.data_64_
        << ", data_32_:" << poi.data_32_ << ", op_code:" << poi.op_code_;

    if( poi.op_code_ == K_SPLIT_OP ) {
        DVLOG( 10 ) << "Deserializing partition_column_operation_identifier: "
                       "identifier_:"
                    << poi.identifier_ << ", data_64_:" << poi.data_64_
                    << ", data_32_:" << poi.data_32_
                    << ", split op_code:" << poi.op_code_ << ", pos:" << pos;
    }

    deserialized->partition_updates_.push_back( poi );

    return pos;
}

uint64_t deserialize_partition_id_2( const serialized_update& update,
                                     partition_column_identifier* pcid, uint64_t pos ) {
    int64_t* as_int64_array;
    int32_t* as_int32_array;

    as_int32_array = (int32_t*) ( update.buffer_ + pos );
    pcid->table_id = ntohl( *as_int32_array );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );

    as_int64_array = (int64_t*) ( update.buffer_ + pos );
    pcid->partition_start = ntohll( *as_int64_array );
    pos += sizeof( int64_t );
    DCHECK_LT( pos, update.length_ );

    as_int64_array = (int64_t*) ( update.buffer_ + pos );
    pcid->partition_end = ntohll( *as_int64_array );
    pos += sizeof( int64_t );
    DCHECK_LT( pos, update.length_ );

    as_int32_array = (int32_t*) ( update.buffer_ + pos );
    pcid->column_start = ntohl( *as_int32_array );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );

    as_int32_array = (int32_t*) ( update.buffer_ + pos );
    pcid->column_end = ntohl( *as_int32_array );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );



    DVLOG( 40 ) << "Deserialize partition id:" << *pcid;

    return pos;
}

uint64_t deserialize_cell_2( const serialized_update& update,
                             deserialized_update* deserialized, uint64_t pos ) {
    char*    buffer_as_char;
    int64_t* as_int64_array;
    int32_t* as_int32_array;

    deserialized_cell_op deserialized_cell;

    as_int32_array = (int32_t*) ( update.buffer_ + pos );
    uint32_t table_id = ntohl( *as_int32_array );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );
    DVLOG( 30 ) << "Deserializing update: table_id:" << table_id;

    as_int32_array = (int32_t*) ( update.buffer_ + pos );
    uint32_t col_id = ntohl( *as_int32_array );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );
    DVLOG( 30 ) << "Deserializing update: col_id:" << col_id;

    as_int64_array = (int64_t*) ( update.buffer_ + pos );
    uint64_t key = ntohll( *as_int64_array );
    pos += sizeof( int64_t );
    DCHECK_LT( pos, update.length_ );
    DVLOG( 30 ) << "Deserializing update: key:" << key;

    deserialized_cell.set_cell_identifier( table_id, col_id, key );

    as_int64_array = (int64_t*) ( update.buffer_ + pos );
    uint64_t op = ntohll( *as_int64_array );
    pos += sizeof( int64_t );
    DCHECK_LT( pos, update.length_ );

    DVLOG( 30 ) << "Deserializing update: op:" << op;

    if( ( op == K_INSERT_OP ) or ( op == K_WRITE_OP ) ) {
        as_int32_array = (int32_t*) ( update.buffer_ + pos );
        uint32_t buffer_len = ntohl( *as_int32_array );
        pos += sizeof( int32_t );
        DCHECK_LT( pos, update.length_ );
        DVLOG( 30 ) << "Deserializing update: buffer_len:" << buffer_len;

        buffer_as_char = (char*) ( update.buffer_ + pos );
        pos += buffer_len;
        DVLOG( 30 ) << "Deserializing update: buffer:"
                    << std::string( buffer_as_char, buffer_len );

        deserialized_cell.add_update( op, buffer_as_char, buffer_len );

    } else if( op == K_DELETE_OP ) {
        deserialized_cell.add_delete();
    } else {
        LOG( WARNING ) << "Deserialized record unknown op:" << op;
    }

    DCHECK_LT( pos, update.length_ );
    deserialized->cell_updates_.push_back( deserialized_cell );

    return pos;
}

uint64_t deserialize_update_propagation_information_2(
    const serialized_update&                     update,
    std::vector<update_propagation_information>* deserialized, uint64_t pos ) {

    update_propagation_information info;

    char*    as_buffer_array;
    int64_t* as_int64_array;
    int32_t* as_int32_array;
    int8_t*  as_int8_array;

    as_int32_array = (int32_t*) ( update.buffer_ + pos );
    info.propagation_config_.type =
        (propagation_type::type) ntohl( *as_int32_array );
    pos += sizeof( int32_t );
    DCHECK_LT( pos, update.length_ );

    if( info.propagation_config_.type != propagation_type::NO_OP ) {

        as_int32_array = (int32_t*) ( update.buffer_ + pos );
        info.propagation_config_.partition = ntohl( *as_int32_array );
        pos += sizeof( int32_t );
        DCHECK_LT( pos, update.length_ );

        as_int64_array = (int64_t*) ( update.buffer_ + pos );
        info.propagation_config_.offset = ntohll( *as_int64_array );
        pos += sizeof( int64_t );
        DCHECK_LT( pos, update.length_ );

        if( info.propagation_config_.type == propagation_type::KAFKA ) {
            as_int32_array = (int32_t*) ( update.buffer_ + pos );
            int topic_len = ntohl( *as_int32_array );
            pos += sizeof( int32_t );
            DCHECK_LT( pos, update.length_ );

            as_buffer_array = (char*) ( update.buffer_ + pos );
            info.propagation_config_.topic =
                std::string( as_buffer_array, topic_len );

            pos += topic_len;
            DCHECK_LT( pos, update.length_ );
        }
    }

    pos = deserialize_partition_id_2( update, &info.identifier_, pos );

    as_int8_array = (int8_t*) ( update.buffer_ + pos );
    info.do_seek_ = (bool) ( *as_int8_array );
    pos += sizeof( int8_t );

    DCHECK_LT( pos, update.length_ );

    DVLOG( 40 ) << "Deserialized update propagation info: { config:"
                << info.propagation_config_ << ", ID:" << info.identifier_
                << ", do_seek_:" << info.do_seek_ << " }";

    deserialized->push_back( info);
    return pos;
}

void clear_serialized_updates( std::vector<serialized_update>& updates ) {
    for( serialized_update& update : updates ) {
        if( update.buffer_ != nullptr ) {
            free( update.buffer_ );
        }
        update.buffer_ = nullptr;
        update.length_ = 0;
    }
    updates.clear();
}
const packed_cell_data& get_cell_data_from_vri(
    const partition_column_identifier&     pcid,
    const versioned_row_record_identifier& vri ) {
    uint32_t translated_col_id =
        normalize_column_id( vri.identifier_.col_id_, pcid );
    row_record* row_rec = vri.get_data();
    DCHECK( row_rec );
    DCHECK_LT( translated_col_id, row_rec->get_num_columns() );
    const packed_cell_data& pcd = row_rec->get_row_data()[translated_col_id];
    return pcd;
}
