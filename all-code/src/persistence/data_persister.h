#pragma once

#include <fstream>  // std::ofstream
#include <string>

#include "../common/hw.h"

class data_persister {
   public:
    ALWAYS_INLINE data_persister( const std::string& output_file_name );

    // open
    ALWAYS_INLINE void open();
    // close and flush
    ALWAYS_INLINE void close();
    // explicit flush
    ALWAYS_INLINE void flush();

    ALWAYS_INLINE void write_str( const std::string& str );
    ALWAYS_INLINE void write_chars( const char* buffer, uint32_t len );
    ALWAYS_INLINE void write_i64( uint64_t i64 );
    ALWAYS_INLINE void write_i32( uint32_t i32 );
    ALWAYS_INLINE void write_i8( uint8_t i8 );
    ALWAYS_INLINE void write_dbl( double dbl );

    ALWAYS_INLINE uint32_t get_position();
    ALWAYS_INLINE void seek_to_position( uint32_t position );

   private:
    ALWAYS_INLINE void check_failure();

    std::string           output_file_name_;

    std::ofstream o_stream_;
};

#include "data_persister-inl.h"
