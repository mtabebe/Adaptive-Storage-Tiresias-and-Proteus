#pragma once

#include <algorithm>

inline std::string update_destination_type_string(
    const update_destination_type& up_type ) {
    switch( up_type ) {
        case NO_OP_DESTINATION:
            return "NO_OP_DESTINATION";
        case VECTOR_DESTINATION:
            return "VECTOR_DESTINATION";
        case KAFKA_DESTINATION:
            return "KAFKA_DESTINATION";
        case UNKNOWN_UPDATE_DESTINATION:
            return "UNKNOWN_UPDATE_DESTINATION";
    }
    return "UNKNOWN_UPDATE_DESTINATION";
}

inline update_destination_type string_to_update_destination_type(
    const std::string& s ) {
    std::string lower = s;
    std::transform( lower.begin(), lower.end(), lower.begin(), ::tolower );
    if( lower.compare( "no_op_destination" ) == 0 ) {
        return update_destination_type::NO_OP_DESTINATION;
    } else if( lower.compare( "vector_destination" ) == 0 ) {
        return update_destination_type::VECTOR_DESTINATION;
    } else if( lower.compare( "kafka_destination" ) == 0 ) {
        return update_destination_type::KAFKA_DESTINATION;
    }
    return update_destination_type::UNKNOWN_UPDATE_DESTINATION;
}

inline std::string update_source_type_string(
    const update_source_type& up_type ) {
    switch( up_type ) {
        case NO_OP_SOURCE:
            return "NO_OP_SOURCE";
        case VECTOR_SOURCE:
            return "VECTOR_SOURCE";
        case KAFKA_SOURCE:
            return "KAFKA_SOURCE";
        case UNKNOWN_UPDATE_SOURCE:
            return "UNKNOWN_UPDATE_SOURCE";
    }
    return "UNKNOWN_UPDATE_SOURCE";
}

inline update_source_type string_to_update_source_type(
    const std::string& s ) {
    std::string lower = s;
    std::transform( lower.begin(), lower.end(), lower.begin(), ::tolower );
    if( lower.compare( "no_op_source" ) == 0 ) {
        return update_source_type::NO_OP_SOURCE;
    } else if( lower.compare( "vector_source" ) == 0 ) {
        return update_source_type::VECTOR_SOURCE;
    } else if( lower.compare( "kafka_source" ) == 0 ) {
        return update_source_type::KAFKA_SOURCE;
    }
    return update_source_type::UNKNOWN_UPDATE_SOURCE;
}
