#pragma once

#include <iostream>

#include <glog/logging.h>


inline bool c_strings_equal( int64_t expected_len, const char* expected_buf,
                             int64_t actual_len, const char* actual_buf ) {
    if( expected_len != actual_len ) {
        DVLOG( 5 ) << "Uneven lengths, expected:" << expected_len
                   << ", actual:" << actual_len;
        return false;
    }
    for( int64_t char_pos = 0; char_pos < expected_len; char_pos++ ) {
        if( actual_buf[char_pos] != expected_buf[char_pos] ) {
            DVLOG( 5 ) << " Buffers do not match in pos:" << char_pos
                       << ", expected:" << expected_buf[char_pos]
                       << ", actual:" << actual_buf[char_pos];
            return false;
        }
    }
    return true;
}

inline void set_c_string( uint32_t src_len, const char* src_str,
                          uint32_t* dest_len, char* dest_str ) {
    *dest_len = src_len;
    std::memcpy( (void*) dest_str, (void*) src_str, src_len );
}

inline std::vector<std::string> split_string( const std::string& input,
                                              char               delim ) {

    std::stringstream        ss( input );
    std::vector<std::string> words;
    std::string              tmp;

    while( getline( ss, tmp, delim ) ) {
        words.push_back( tmp );
    }

    return words;
}
