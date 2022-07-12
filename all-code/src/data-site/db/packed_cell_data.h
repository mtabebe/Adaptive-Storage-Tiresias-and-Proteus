#pragma once

#include <cstdint>
#include <string>

#include "../../common/cell_data_type.h"
#include "../../common/hw.h"

class packed_cell_data {
   public:
    packed_cell_data();
    ~packed_cell_data();

    // explicitly disable copy constructor
    packed_cell_data( const packed_cell_data& ) = delete;

    void deep_copy( const packed_cell_data& other );

    void set_as_deleted();

    ALWAYS_INLINE bool is_present() const;
    ALWAYS_INLINE uint32_t get_length() const;
    ALWAYS_INLINE int16_t get_type() const;

    uint64_t get_uint64_data() const;
    void set_uint64_data( uint64_t u );

    int64_t get_int64_data() const;
    void set_int64_data( int64_t i );

    double get_double_data() const;
    void set_double_data( double d );

    std::string get_string_data() const;
    void set_string_data( const std::string& s );

    void* get_pointer_data();
    void set_pointer_data( void* v, int16_t record_type );

    ALWAYS_INLINE char* get_buffer_data() const;
    void set_buffer_data( const char* s, uint32_t len );

    uint32_t get_data_size() const;

    friend std::ostream& operator<<( std::ostream&           os,
                                     const packed_cell_data& pcd );

   private:
    // 8 bytes
    int8_t is_pointer_;
    int8_t is_deleted_;
    // some thing if we wanted to have a typed database
    int16_t   record_type_;
    uint32_t data_size_;

    // 8 bytes
    void* data_;
};

#include "packed_cell_data-inl.h"
