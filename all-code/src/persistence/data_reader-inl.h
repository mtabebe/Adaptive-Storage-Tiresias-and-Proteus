#pragma once

#include <glog/logging.h>

#include "../data-site/site-manager/serialize.h"

inline data_reader::data_reader( const std::string& input_file_name )
    : input_file_name_( input_file_name ), i_stream_() {}

inline void data_reader::open() {
    i_stream_.open( input_file_name_,
                    std::ifstream::in | std::ofstream::binary );
}
inline void data_reader::close() { i_stream_.close(); }

inline void data_reader::check_failure() {
#ifndef NDEBUG
    if( !i_stream_.good() ) {
        LOG( FATAL ) << "I/O error when reading from file:" << input_file_name_
                     << ", read only:" << i_stream_.gcount();
        i_stream_.clear();
    }
#endif
}

inline void data_reader::read_i64( uint64_t* i64 ) {
    uint64_t no_i64;
    i_stream_.read( (char*) &no_i64, sizeof( uint64_t ) );
    *i64 = ntohll( no_i64 );
    check_failure();
}
inline void data_reader::read_i32( uint32_t* i32 ) {
    uint32_t no_i32;
    i_stream_.read( (char*) &no_i32, sizeof( uint32_t ) );
    *i32 = ntohl( no_i32 );
    check_failure();
}
inline void data_reader::read_i8( uint8_t* i8 ) {

    i_stream_.read( (char*) i8, sizeof( uint8_t ) );
    check_failure();
}
inline void data_reader::read_dbl( double* dbl ) {

    i_stream_.read( (char*) dbl, sizeof( double ) );
    check_failure();
}

inline void data_reader::read_chars( char** buffer, uint32_t* len ) {
    read_i32( len );
    // it would be sweet if we could just zero copy the value from the file
    // string in, but we can't we have to alloc and then free it out

    *buffer = new char[*len];

    i_stream_.read( *buffer, *len );
    check_failure();
}

inline void data_reader::read_str( std::string* str ) {
    char*    c_buf = nullptr;
    uint32_t len = 0;

    read_chars( &c_buf, &len );
    str->assign( c_buf, len );

    delete[] c_buf;
}

inline uint32_t data_reader::get_position() {
    uint32_t pos = i_stream_.tellg();
    check_failure();
    return pos;
}

inline void data_reader::seek_to_position( uint32_t amount ) {
    i_stream_.seekg( amount, i_stream_.beg );
    check_failure();
}
