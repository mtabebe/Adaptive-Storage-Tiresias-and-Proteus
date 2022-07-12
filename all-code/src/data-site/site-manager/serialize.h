#pragma once

#include <arpa/inet.h>
#include <cstdlib>
#include <folly/Hash.h>
#include <glog/logging.h>
#include <inttypes.h>
#include <vector>

#include "../../gen-cpp/gen-cpp/proto_types.h"

struct arg_code {
    union {
        struct {
            int      code : 8;
            unsigned array_length : 16;
            bool     array_flag : 1;
            int      pad : 7;  // align on word boundary
        };
        uint32_t as_bytes;
    };

    // Since we use this for hashing, make sure all fields are zeroed
    arg_code( int code, unsigned array_length, bool array_flag )
        : code( code ),
          array_length( array_length ),
          array_flag( array_flag ),
          pad( 0 ) {}

    arg_code() { memset( this, 0, sizeof( arg_code ) ); }

    arg_code( const arg_code &other ) {
        memcpy( this, &other, sizeof( arg_code ) );
    }
};

// NOTE THIS IGNORES THE ARRAY_LENGTH
struct arg_code_hasher {
    size_t operator()( const arg_code &code ) const {
        size_t seed = 0;
        DVLOG( 20 ) << "Hashing arg_code:" << code.code
                    << ", array_flag:" << code.array_flag;
        seed = folly::hash::hash_combine( seed, code.code );
        seed = folly::hash::hash_combine( seed, code.array_flag );
        // seed = folly::hash::hash_combine( seed, code.array_length );
        return seed;
    }
};

size_t get_size( int arg_type_code );
void *allocate_array_memory( int arg_type_code, unsigned array_length );
void free_args( std::vector<void *> &arg_ptrs );

static_assert( sizeof( arg_code ) == sizeof( int ),
               "arg_code/integer size mismatch" );
static_assert( sizeof( float ) == sizeof( int32_t ),
               "float/integer size mismatch" );
static_assert( sizeof( double ) == sizeof( int64_t ),
               "double/uint64_t size mismatch" );

// The C standard does not define sizeof( bool ) = 1, because some
// architectures (e.g. SPARC) are garbage at reading single bytes (the minimum
// addressable unit is 32 bits). Therefore, the below assertion will throw on
// any such platform.
// Since we are testing only on x86, I'll leave this in, but use it as a warning
// if others try
// to port
static_assert( sizeof( bool ) == sizeof( char ), "bool/char size mismatch" );

enum arg_type {
    UNSIGNED = 0,
    INTEGER = 1,
    BIGINT = 2,
    FLOAT = 4,
    DOUBLE = 8,
    CHAR = 16,
    BOOL = 32,
    PAST_END = 64
};

uint64_t htonll( uint64_t value );
uint64_t ntohll( uint64_t value );

size_t serialize( const std::vector<arg_code> &arg_codes,
                  const std::vector<void *> &arg_ptrs, char **out );
size_t serialize_for_sproc( const std::vector<arg_code> &arg_codes,
                            const std::vector<void *> &arg_ptrs, char **out );
bool deserialize( const char *buffer, size_t len,
                  std::vector<arg_code> &arg_codes,
                  std::vector<void *> &  arg_ptrs );

void deserialize_result( sproc_result &res, std::vector<void *> &arg_results,
                         std::vector<arg_code> &arg_codes );

static const arg_code INTEGER_CODE = {INTEGER, 0, false};
static const arg_code INTEGER_ARRAY_CODE = {INTEGER, 0, true};
static const arg_code BIGINT_CODE = {BIGINT, 0, false};
static const arg_code BIGINT_ARRAY_CODE = {BIGINT, 0, true};
static const arg_code STRING_CODE = {CHAR, 0, true};
static const arg_code BOOL_CODE = {BOOL, 0, false};
static const arg_code FLOAT_CODE = {FLOAT, 0, false};
static const arg_code FLOAT_ARRAY_CODE = {FLOAT, 0, true};
