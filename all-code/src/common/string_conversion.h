#pragma once

#include <cstdint>
#include <cstring>
#include <string>

uint64_t string_to_uint64( const std::string& s );
int64_t string_to_int64( const std::string& s );
double string_to_double( const std::string& s );
std::string string_to_string( const std::string& s );

uint64_t chars_to_uint64( const char* s, uint32_t len );
int64_t chars_to_int64( const char* s, uint32_t len );
double chars_to_double( const char* s, uint32_t len );
std::string chars_to_string( const char* s, uint32_t len );

std::string uint64_to_string( uint64_t data );
std::string int64_to_string( int64_t data );
std::string double_to_string( double data );

