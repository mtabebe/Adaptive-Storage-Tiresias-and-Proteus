#include "serialize.h"

#include <cstdlib>
#include <vector>

#include <glog/logging.h>

uint64_t htonll( uint64_t value ) {
    int num = 1;
    if( *( (char *) &num ) ) {
        // Little endian
        const uint32_t hi = htonl( value >> 32 );
        const uint32_t lo = htonl( value & 0xFFFFFFFFLL );
        return ( static_cast<uint64_t>( lo ) << 32 ) | hi;
    }
    // Big endian
    return value;
}

uint64_t ntohll( uint64_t value ) {
    int num = 1;
    if( *( (char *) &num ) ) {
        // Little endian
        const uint32_t hi = ntohl( value >> 32 );
        const uint32_t lo = ntohl( value & 0xFFFFFFFFLL );
        return ( static_cast<uint64_t>( lo ) << 32 ) | hi;
    }
    // Big endian
    return value;
}

// Forward declare
static bool decode_and_advance( const char *buffer, unsigned int &position,
                                arg_code *             code,
                                std::vector<arg_code> &arg_codes,
                                std::vector<void *> &  arg_ptrs );

static bool decode_array_and_advance( const char *  buffer,
                                      unsigned int &position, arg_code *code,
                                      std::vector<arg_code> &arg_codes,
                                      std::vector<void *> &  arg_ptrs );

static bool decode_single_and_advance_alloc_and_record(
    const char *buffer, unsigned int &position, arg_code *code,
    std::vector<arg_code> &arg_codes, std::vector<void *> &arg_ptrs );

static bool decode_single_and_advance( const char *  buffer,
                                       unsigned int &position, arg_code *code,
                                       void *write_loc );

size_t get_size( int arg_type_code ) {
    switch( arg_type_code ) {
        case UNSIGNED:
        case INTEGER:
        case FLOAT:
            return sizeof( unsigned );
        case BIGINT:
        case DOUBLE:
            return sizeof( uint64_t );
        case CHAR:
        case BOOL:
            return sizeof( char );
        default:
            return 0;
    }
}
static bool serialize_single( const arg_code code, const void *arg_ptr,
                              char *buff, unsigned int &position ) {
    switch( code.code ) {
        case UNSIGNED:
        case INTEGER:
        case FLOAT: {
            unsigned *ptr = (unsigned *) arg_ptr;
            unsigned *write_ptr = (unsigned *) ( buff + position );
            *write_ptr = htonl( *ptr );
            position += get_size( code.code );
            DVLOG( 40 ) << "Serialize 4 bytes:" << *ptr;
        }
            return true;
        case BIGINT:
        case DOUBLE: {
            uint64_t *ptr = (uint64_t *) arg_ptr;
            uint64_t *write_ptr = (uint64_t *) ( buff + position );
            *write_ptr = htonll( *ptr );
            position += get_size( code.code );
            DVLOG( 40 ) << "Serialize 8 bytes:" << *ptr;
        }
            return true;
        case CHAR:
        case BOOL: {
            char *ptr = (char *) arg_ptr;
            char *write_ptr = ( buff + position );
            *write_ptr = *ptr;
            position += get_size( code.code );
            DVLOG( 40 ) << "Serialize 1 byte:" << *ptr;
        }
            return true;
        default:
            return false;
    }
}

static bool serialize_array( const arg_code code, size_t sz,
                             const void *arg_ptr, char *buff,
                             unsigned int &position ) {
    const char *arr_bytes = (const char *) arg_ptr;
    size_t      prim_size = get_size( code.code );

    DVLOG( 40 ) << "Serializing array of len:" << sz;

    for( unsigned i = 0; i < sz; i++ ) {
        if( !serialize_single( code,
                               (const void *) ( arr_bytes + prim_size * i ),
                               buff, position ) ) {
            return false;
        }
    }
    return true;
}

static bool serialize( const arg_code code, const void *arg_ptr, char *buff,
                       unsigned int &position ) {
    DVLOG( 50 ) << "Serialize code: as bytes:" << code.as_bytes
               << ",: code:" << code.code
               << ", code length:" << code.array_length
               << ", array flag:" << code.array_flag;

    // Write code first
    unsigned *write_ptr = (unsigned *) ( buff + position );
    *write_ptr = htonl( code.as_bytes );
    position += sizeof( arg_code );
    if( code.array_flag ) {
        return serialize_array( code, code.array_length, arg_ptr, buff,
                                position );
    } else {
        return serialize_single( code, arg_ptr, buff, position );
    }
}

size_t serialize_into_buff( const std::vector<arg_code> &arg_codes,
                            const std::vector<void *> &arg_ptrs, char *buff ) {
    unsigned int position = 0;
    for( unsigned int i = 0; i < arg_codes.size(); i++ ) {
        if( !serialize( arg_codes[i], arg_ptrs[i], buff, position ) ) {
            free( buff );
            return 0;
        }
    }
    return position;
}

size_t get_size( const std::vector<arg_code> &arg_codes,
                 const std::vector<void *> &  arg_ptrs ) {
    size_t sz = 0;
    for( const auto &code : arg_codes ) {
        size_t prim_size = get_size( code.code );
        if( code.array_flag ) {
            prim_size *= code.array_length;
        }
        sz += prim_size + sizeof( arg_code );
    }
    return sz;
}

size_t serialize( const std::vector<arg_code> &arg_codes,
                  const std::vector<void *> &arg_ptrs, char **out ) {

    // Determine buffer size
    size_t sz = get_size( arg_codes, arg_ptrs );
    char * buff = (char *) malloc( sz );
    *out = buff;

    return serialize_into_buff( arg_codes, arg_ptrs, buff );
}

size_t serialize_for_sproc( const std::vector<arg_code> &arg_codes,
                            const std::vector<void *> &arg_ptrs, char **out ) {

    size_t sz_int = sizeof( int );
    // Determine buffer size
    size_t sz = get_size( arg_codes, arg_ptrs );
    char * buff = (char *) malloc( sz + sz_int );
    *out = buff;

    DVLOG( 50 ) << "Serialize for sproc len:" << sz;

    int *buf_as_in = (int *) buff;
    *buf_as_in = htonl( sz );
    buff += sz_int;

    size_t serialize_ret = serialize_into_buff( arg_codes, arg_ptrs, buff );
    return ( serialize_ret + sz_int );
}

bool deserialize( const char *buffer, size_t len,
                  std::vector<arg_code> &arg_codes,
                  std::vector<void *> &  arg_ptrs ) {

    unsigned int position = 0;
    while( position < len ) {
        DVLOG( 50 ) << "Deserialize position:" << position << ", len:" << len;

        // Read arg_code from this position in the string, move forward, begin
        // decoding
        unsigned  code_temp = ntohl( *(unsigned *) &( buffer[position] ) );
        arg_code *code = (arg_code *) &code_temp;
        position += sizeof( arg_code );

        if( !decode_and_advance( buffer, position, code, arg_codes,
                                 arg_ptrs ) ) {
            return false;
        }
    }
    return position == len;
}

void deserialize_result( sproc_result &res, std::vector<void *> &arg_results,
                         std::vector<arg_code> &arg_codes ) {
    DCHECK_GE( res.res_args.size(), 4 );

    char* buffer = (char*) res.res_args.c_str();

    // Read the length of stuff to deserialize
    int len = *(int*) buffer;
    len = ntohl( len );
    buffer += sizeof( int );

    DVLOG( 50 ) << "Deserialize: len:" << len;

    auto des_res = deserialize( buffer, len, arg_codes, arg_results );
    DCHECK( des_res );
    DVLOG( 50 ) << "Deserialize arg codes size:" << arg_codes.size();
}

static bool decode_and_advance( const char *buffer, unsigned int &position,
                                arg_code *             code,
                                std::vector<arg_code> &arg_codes,
                                std::vector<void *> &  arg_ptrs ) {
    DVLOG( 50 ) << "Decode and advance code: as_bytes" << code->as_bytes
               << ",: code:" << code->code
               << ", code length:" << code->array_length
               << ", array flag:" << code->array_flag;
    if( code->array_flag ) {
        return decode_array_and_advance( buffer, position, code, arg_codes,
                                         arg_ptrs );
    }
    return decode_single_and_advance_alloc_and_record( buffer, position, code,
                                                       arg_codes, arg_ptrs );
}

void *allocate_array_memory( int arg_type_code, unsigned array_length ) {
    return malloc( get_size( arg_type_code ) * array_length );
}

void free_args( std::vector<void *> &arg_ptrs ) {
    for( unsigned int i = 0; i < arg_ptrs.size(); i++ ) {
        free( arg_ptrs.at( i ) );
    }
    arg_ptrs.clear();
}

static bool decode_array_and_advance( const char *  buffer,
                                      unsigned int &position, arg_code *code,
                                      std::vector<arg_code> &arg_codes,
                                      std::vector<void *> &  arg_ptrs ) {
    void *ptr = allocate_array_memory( code->code, code->array_length );
    if( !ptr ) {
        return false;
    }
    int    offset = 0;
    size_t sz = get_size( code->code );
    if( !sz ) {
        return false;
    }
    DVLOG( 50 ) << "Deserialize array of len:" << code->array_length;
    for( unsigned int i = 0; i < code->array_length; i++ ) {
        bool ok = decode_single_and_advance( buffer, position, code,
                                             ( (char *) ptr + offset ) );
        if( !ok ) {
            return ok;
        }
        offset += sz;
    }

    arg_codes.push_back( *code );
    arg_ptrs.push_back( ptr );
    return true;
}

static bool decode_single_and_advance_alloc_and_record(
    const char *buffer, unsigned int &position, arg_code *code,
    std::vector<arg_code> &arg_codes, std::vector<void *> &arg_ptrs ) {
    void *ptr = allocate_array_memory( code->code, 1 );
    bool  ok = decode_single_and_advance( buffer, position, code, ptr );
    arg_codes.push_back( *code );
    arg_ptrs.push_back( ptr );
    return ok;
}

static bool decode_single_and_advance( const char *  buffer,
                                       unsigned int &position, arg_code *code,
                                       void *write_loc ) {
    switch( code->code ) {
        case UNSIGNED:
        case INTEGER:
        case FLOAT: {
            unsigned *ptr = (unsigned *) write_loc;
            uint32_t *str_uint = (uint32_t *) &buffer[position];
            *ptr = ntohl( *str_uint );
            position += get_size( code->code );
            DVLOG( 40 ) << "Deserialize 4 bytes:" << *ptr;
        }
            return true;
        case DOUBLE:
        case BIGINT: {
            int64_t *ptr = (int64_t *) write_loc;
            int64_t *str_dbl = (int64_t *) &buffer[position];
            *ptr = ntohll( *str_dbl );
            position += get_size( code->code );
            DVLOG( 40 ) << "Deserialize 8 bytes:" << *ptr;
        }
            return true;
        case CHAR:
        case BOOL: {
            char *ptr = (char *) write_loc;
            *ptr = buffer[position];
            position += get_size( code->code );
            DVLOG( 40 ) << "Deserialize 1 bytes:" << *ptr;
        }
            return true;
        default:
            return false;
    }
}
