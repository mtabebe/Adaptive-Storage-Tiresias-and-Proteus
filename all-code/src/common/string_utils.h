#pragma once

#include <cstdint>
#include <cstring>

#include "hw.h"

ALWAYS_INLINE bool c_strings_equal( int64_t     expected_len,
                                    const char* expected_buf,
                                    int64_t     actual_len,
                                    const char* actual_buf );

ALWAYS_INLINE void set_c_string( uint32_t src_len, const char* src_str,
                                 uint32_t* dest_len, char* dest_str );

ALWAYS_INLINE std::vector<std::string> split_string( const std::string& input,
                                                     char               delim );

#include "string_utils-inl.h"
