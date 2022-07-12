#pragma once

#include <glog/logging.h>

#include "../data-site/site-manager/serialize.h"

inline data_persister::data_persister( const std::string& output_file_name )
    : output_file_name_( output_file_name ), o_stream_() {}

inline void data_persister::open() {
    // open the file as output only, as binary, and truncate mode (overwrite)
    o_stream_.open( output_file_name_, std::ofstream::out |
                                           std::ofstream::binary |
                                           std::ofstream::trunc );
}

inline void data_persister::flush() { o_stream_.flush(); }
// flush
inline void data_persister::close() {
    // mark this as the end
    flush();
    o_stream_.close();
}

inline void data_persister::check_failure() {
#ifndef NDEBUG
    if( !o_stream_.good() ) {
        LOG( WARNING ) << "I/O error when writing to file:"
                       << output_file_name_;
        o_stream_.clear();
    }
#endif
}

inline void data_persister::write_i64( uint64_t i64 ) {
    uint64_t no_i64 = htonll( i64 );
    o_stream_.write( (const char*) ( &no_i64 ), sizeof( uint64_t ) );
    check_failure();
}
inline void data_persister::write_i32( uint32_t i32 ) {
    uint32_t no_i32 = htonl( i32 );
    o_stream_.write( (const char*) ( &no_i32 ), sizeof( uint32_t ) );
    check_failure();
}
inline void data_persister::write_i8( uint8_t i8 ) {

    o_stream_.write( (const char*) ( &i8 ), sizeof( uint8_t ) );
    check_failure();
}
inline void data_persister::write_dbl( double dbl ) {

    o_stream_.write( (const char*) ( &dbl ), sizeof( double ) );
    check_failure();
}

inline void data_persister::write_chars( const char* buffer, uint32_t len ) {
    write_i32( len );
    o_stream_.write( buffer, len );
    check_failure();
}

inline void data_persister::write_str( const std::string& str ) {
    write_chars( str.c_str(), str.size() );
}

inline uint32_t data_persister::get_position() {
    uint32_t pos = o_stream_.tellp();
    check_failure();
    return pos;
}
inline void data_persister::seek_to_position( uint32_t position ) {
    o_stream_.seekp( position, o_stream_.beg );
    check_failure();
}

