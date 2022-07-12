#include "string_conversion.h"

#include <glog/logging.h>

uint64_t string_to_uint64( const std::string& s ) {
    return chars_to_uint64( s.data(), s.length() );
}
int64_t string_to_int64( const std::string& s ) {
    return chars_to_int64( s.data(), s.length() );
}
double string_to_double( const std::string& s ) {
    return chars_to_double( s.data(), s.length() );
}
std::string string_to_string( const std::string& s ) { return s; }

#define CHARS_TO_X( _s, _len, _type )   \
    DCHECK( _s );                       \
    DCHECK_EQ( _len, sizeof( _type ) ); \
    _type _data = *( (_type*) _s );     \
    return _data;

uint64_t chars_to_uint64( const char* s, uint32_t len ) {
    CHARS_TO_X( s, len, uint64_t );
}
int64_t chars_to_int64( const char* s, uint32_t len ) {
    CHARS_TO_X( s, len, int64_t );
}
double chars_to_double( const char* s, uint32_t len ) {
    CHARS_TO_X( s, len, double );
}
std::string chars_to_string( const char* s, uint32_t len ) {
    std::string str_data = "";
    if( s != nullptr ) {
        str_data.assign( s, len );
    }
    return s;
}

#define X_TO_STRING( _data, _type )            \
    char*       _bytes = (char*) &_data;       \
    std::string _s( _bytes, sizeof( _type ) ); \
    return _s;

std::string uint64_to_string( uint64_t data ) { X_TO_STRING( data, uint64_t ); }
std::string int64_to_string( int64_t data ) { X_TO_STRING( data, int64_t ); }
std::string double_to_string( double data ) { X_TO_STRING( data, double ); }

