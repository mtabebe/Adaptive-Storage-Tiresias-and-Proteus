#include "db_abstraction_types.h"

#include <algorithm>
#include <glog/logging.h>

std::string db_abstraction_type_string( const db_abstraction_type& db_type ) {
    switch( db_type ) {
        case SINGLE_SITE_DB:
            return "SINGLE_SITE_DB";
        case PLAIN_DB:
            return "PLAIN_DB";
        case SS_DB:
            return "SS_DB";
        case UNKNOWN_DB:
            return "UNKNOWN_DB";
    }
    return "UNKNOWN_DB";
}

db_abstraction_type string_to_db_abstraction_type( const std::string& s ) {
    std::string lower = s;
    std::transform( lower.begin(), lower.end(), lower.begin(), ::tolower );
    if( lower.compare( "single_site_db" ) == 0 ) {
        return db_abstraction_type::SINGLE_SITE_DB;
    } else if( lower.compare( "plain_db" ) == 0 ) {
        return db_abstraction_type::PLAIN_DB;
    } else if( lower.compare( "ss_db" ) == 0 ) {
        return db_abstraction_type::SS_DB;
    }

    return db_abstraction_type::UNKNOWN_DB;
}


