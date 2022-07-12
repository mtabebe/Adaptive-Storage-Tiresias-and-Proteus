#pragma once

#include <fstream>  // std::ifstream

#include "../common/hw.h"

class data_reader {
   public:
    ALWAYS_INLINE data_reader( const std::string& input_file_name );

    ALWAYS_INLINE void open();
    ALWAYS_INLINE void close();

    ALWAYS_INLINE void read_str( std::string* str );
    ALWAYS_INLINE void read_chars( char** buffer, uint32_t* len );
    ALWAYS_INLINE void read_i64( uint64_t* i64 );
    ALWAYS_INLINE void read_i32( uint32_t* i32 );
    ALWAYS_INLINE void read_i8( uint8_t* i8 );
    ALWAYS_INLINE void read_dbl( double* dbl );

    ALWAYS_INLINE void seek_to_position( uint32_t position );
    ALWAYS_INLINE uint32_t get_position();

   private:
    ALWAYS_INLINE void check_failure();

    std::string   input_file_name_;
    std::ifstream i_stream_;
};

#include "data_reader-inl.h"
