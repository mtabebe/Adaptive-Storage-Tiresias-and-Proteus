#pragma once

#include <string>

enum db_abstraction_type {
    UNKNOWN_DB,
    PLAIN_DB,
    SINGLE_SITE_DB,
    SS_DB,
};

std::string db_abstraction_type_string( const db_abstraction_type& db_type );
db_abstraction_type string_to_db_abstraction_type( const std::string& s );


