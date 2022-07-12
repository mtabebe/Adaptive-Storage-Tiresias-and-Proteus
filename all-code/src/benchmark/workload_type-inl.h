#pragma once
#include <algorithm>

inline std::string workload_type_string( const workload_type& w_type ) {
    switch( w_type ) {
        case YCSB:
            return "YCSB";
        case TPCC:
            return "TPCC";
        case SMALLBANK:
            return "SMALLBANK";
        case TWITTER:
            return "TWITTER";
        case UNKNOWN_WORKLOAD:
            return "UNKNOWN_WORKLOAD";
    }
    return "UNKNOWN_WORKLOAD";
}
inline workload_type string_to_workload_type( const std::string& s ) {
    std::string lower = s;
    std::transform( lower.begin(), lower.end(), lower.begin(), ::tolower );
    if( lower.compare( "ycsb" ) == 0 ) {
        return workload_type::YCSB;
    } else if( lower.compare( "tpcc" ) == 0 ) {
        return workload_type::TPCC;
    } else if( lower.compare( "smallbank" ) == 0 ) {
      return workload_type::SMALLBANK;
    } else if( lower.compare( "twitter" ) == 0 ) {
        return workload_type::TWITTER;
    }
    return workload_type::UNKNOWN_WORKLOAD;
}
