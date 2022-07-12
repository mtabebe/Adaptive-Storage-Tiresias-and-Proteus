#pragma once
#include <algorithm>

inline std::string ss_strategy_type_string(
    const ss_strategy_type& strategy_type ) {
    switch( strategy_type ) {
        case HEURISTIC:
            return "HEURISTIC";
        case NAIVE:
            return "NAIVE";
        case UNKNOWN_STRATEGY:
            return "UNKNOWN_STRATEGY";
    }
    return "UNKNOWN_STRATEGY";
}
inline ss_strategy_type string_to_ss_strategy_type( const std::string& s ) {
    std::string lower = s;
    std::transform( lower.begin(), lower.end(), lower.begin(), ::tolower );
    if( lower.compare( "heuristic" ) == 0 ) {
        return ss_strategy_type::HEURISTIC;
    } else if( lower.compare( "naive" ) == 0 ) {
        return ss_strategy_type::NAIVE;
    }

    return ss_strategy_type::UNKNOWN_STRATEGY;
}

inline std::string ss_mastering_type_string(
    const ss_mastering_type& mastering_type ) {
    switch( mastering_type ) {
        case SINGLE_MASTER_MULTI_SLAVE:
            return "SINGLE_MASTER_MULTI_SLAVE";
        case TWO_PC:
            return "TWO_PC";
        case DYNAMIC_MASTERING:
            return "DYNAMIC_MASTERING";
        case PARTITIONED_STORE:
            return "PARTITIONED_STORE";
        case DRP:
            return "DRP";
        case E_STORE:
            return "E_STORE";
        case CLAY:
            return "CLAY";
        case ADR:
            return "ADR";
        case ADAPT:
            return "ADAPT";
        case UNKNOWN_MASTERING:
            return "UNKNOWN_MASTERING";
    }
    return "UNKNOWN_MASTERING";
}
inline ss_mastering_type string_to_ss_mastering_type( const std::string& s ) {
    std::string lower = s;
    std::transform( lower.begin(), lower.end(), lower.begin(), ::tolower );
    if( lower.compare( "single_master_multi_slave" ) == 0 ) {
        return ss_mastering_type::SINGLE_MASTER_MULTI_SLAVE;
    } else if( lower.compare( "two_pc" ) == 0 ) {
        return ss_mastering_type::TWO_PC;
    } else if( lower.compare( "dynamic_mastering" ) == 0 ) {
        return ss_mastering_type::DYNAMIC_MASTERING;
    } else if( lower.compare( "partitioned_store" ) == 0 ) {
        return ss_mastering_type::PARTITIONED_STORE;
    } else if( lower.compare( "drp" ) == 0 ) {
        return ss_mastering_type::DRP;
    } else if( lower.compare( "e_store" ) == 0 ) {
        return ss_mastering_type::E_STORE;
    } else if( lower.compare( "clay" ) == 0 ) {
        return ss_mastering_type::CLAY;
    } else if( lower.compare( "adr" ) == 0 ) {
        return ss_mastering_type::ADR;
    } else if( lower.compare( "adapt" ) == 0 ) {
        return ss_mastering_type::ADAPT;
    }


    return ss_mastering_type::UNKNOWN_MASTERING;
}

