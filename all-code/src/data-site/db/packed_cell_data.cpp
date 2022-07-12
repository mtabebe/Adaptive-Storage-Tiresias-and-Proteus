#include "packed_cell_data.h"

#include <cstring>
#include <glog/logging.h>

const static uint64_t k_pointer_size = 8;

static_assert( sizeof( packed_cell_data ) == 16,
               "packed_cell_data must be 16 bytes" );
static_assert( sizeof( int64_t ) == k_pointer_size, "int64_t must be size 8 " );
static_assert( sizeof( uint64_t ) == k_pointer_size, "uint64_t must be size 8 " );
static_assert( sizeof( double ) == k_pointer_size,
               "double must be equal to size 8 " );
static_assert( sizeof( void* ) == k_pointer_size, "void* must be size 8 " );

packed_cell_data::packed_cell_data()
    : is_pointer_( 0 ),
      is_deleted_( 1 ),
      record_type_( 0 ),
      data_size_( 0 ),
      data_( nullptr ) {}

packed_cell_data::~packed_cell_data() { set_as_deleted(); }

void packed_cell_data::deep_copy( const packed_cell_data& other ) {
    is_deleted_ = other.is_deleted_;

    if( is_deleted_ == 0 ) {
        if( other.record_type_ == K_SNAPSHOT_VECTOR_RECORD_TYPE ) {
            snapshot_vector* o_snap = (snapshot_vector*) other.data_;
            snapshot_vector* new_snap = nullptr;
            if( o_snap ) {
                new_snap = new snapshot_vector( *o_snap );
            }

            set_pointer_data( new_snap, K_SNAPSHOT_VECTOR_RECORD_TYPE );
        } else {
            // copy the raw data
            set_buffer_data( other.get_buffer_data(), other.data_size_ );
        }
    }

    // then reset the type
    is_pointer_ = other.is_pointer_;
    record_type_ = other.record_type_;
}

void packed_cell_data::set_as_deleted() {
    DVLOG( 50 ) << "Setting record:" << this << ", as deleted";

    if( !is_deleted_ and is_pointer_ and data_ != nullptr ) {
        if( record_type_ == K_SNAPSHOT_VECTOR_RECORD_TYPE ) {
            snapshot_vector* snapshot = (snapshot_vector*) data_;
            delete snapshot;
        } else {
            char* data_as_str = (char*) data_;
            DVLOG( 50 ) << "Deleting data_as_str:" << data_as_str
                        << ", data_:" << data_ << ", this:" << this;
            delete[] data_as_str;
        }
    }

    data_ = nullptr;
    is_deleted_ = 1;
    is_pointer_ = 0;
    data_size_ = 0;
}

#define SET_AS_X( _type, _val )                             \
    DVLOG( 20 ) << "Setting record as " #_type ":" << _val; \
    set_as_deleted();                                       \
    is_deleted_ = 0;                                        \
    is_pointer_ = 0;                                        \
    record_type_ = K_INT_RECORD_TYPE;                       \
    data_size_ = k_pointer_size;                            \
    std::memcpy( &data_, &_val, sizeof( _type ) );

#define GET_AS_X( _type )                                \
    DVLOG( 20 ) << "Getting  as " #_type;                \
    _type t;                                             \
    std::memcpy( &t, &data_, sizeof( _type ) );          \
    DVLOG( 20 ) << "Getting record as " #_type ":" << t; \
    return t;

uint64_t packed_cell_data::get_uint64_data() const { GET_AS_X( uint64_t ); }
void packed_cell_data::set_uint64_data( uint64_t u ) {
    SET_AS_X( uint64_t, u );
}

int64_t packed_cell_data::get_int64_data() const { GET_AS_X( int64_t ); }
void packed_cell_data::set_int64_data( int64_t i ) { SET_AS_X( int64_t, i ); }

double packed_cell_data::get_double_data() const { GET_AS_X( double ); }
void packed_cell_data::set_double_data( double d ) { SET_AS_X( double, d ); }

std::string packed_cell_data::get_string_data() const {
    DVLOG( 20 ) << "Getting record as string";
    char* data_as_str = get_buffer_data();

    std::string s( data_as_str, data_size_ );
    DVLOG( 20 ) << "Getting record as str:" << s;

    return s;
}
void packed_cell_data::set_string_data( const std::string& s ) {
    DVLOG( 20 ) << "Setting record as string:" << s;

    set_as_deleted();

    is_deleted_ = 0;
    record_type_ = K_STRING_RECORD_TYPE;
    data_size_ = s.size();

    if( s.size() <= k_pointer_size ) {
        is_pointer_ = 0;
        std::memcpy( &data_, (void*) s.c_str(), s.size() );
    } else {
        is_pointer_ = 1;
        DVLOG( 50 ) << "Allocating data:" << data_ << ", this:" << this;
        data_ = (void*) new char[s.size()];
        DVLOG( 50 ) << "Allocated data:" << data_ << ", this:" << this;
        std::memcpy( data_, (void*) s.c_str(), s.size() );
        DVLOG( 50 ) << "memcpy'd data:" << data_ << ", this:" << this;
    }
}

void packed_cell_data::set_buffer_data( const char* s, uint32_t len ) {
    DVLOG( 20 ) << "Setting record as buffer:" << std::string( s, len );

    set_as_deleted();

    is_deleted_ = 0;
    record_type_ = K_BUFFER_RECORD_TYPE;
    data_size_ = len;

    if( len <= k_pointer_size ) {
        is_pointer_ = 0;
        std::memcpy( &data_, (void*) s, len );
    } else {
        is_pointer_ = 1;
        data_ = (void*) new char[len];
        std::memcpy( data_, (void*) s, len );
    }
}

void* packed_cell_data::get_pointer_data() { return data_; }
void packed_cell_data::set_pointer_data( void* v, int16_t record_type ) {
    set_as_deleted();
    is_deleted_ = 0;
    record_type_ = record_type;
    data_size_ = sizeof( void* );
    is_pointer_ = 1;
    data_ = v;
}

uint32_t packed_cell_data::get_data_size() const { return data_size_; }

std::ostream& operator<<( std::ostream& os, const packed_cell_data& pcd ) {
    os << "PCD: [ is_pointer_:" << (bool) pcd.is_pointer_
       << ", is_deleted_:" << (bool) pcd.is_deleted_
       << ", record_type_:" << pcd.record_type_
       << ", data_size_:" << pcd.data_size_;
    if( !pcd.is_deleted_ ) {
        os << ", data_as_buffer_:" << pcd.get_buffer_data()
        << ", as int:" << pcd.get_int64_data();
    }
    os << " ]";
    return os;
}
