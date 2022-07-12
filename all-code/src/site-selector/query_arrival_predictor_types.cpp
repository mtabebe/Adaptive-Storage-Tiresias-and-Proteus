#include "query_arrival_predictor_types.h"

#include <algorithm>

std::string query_arrival_predictor_type_string(
    const query_arrival_predictor_type& qap_type ) {
    switch( qap_type ) {
        case query_arrival_predictor_type::SIMPLE_QUERY_PREDICTOR:
            return "simple_query_predictor";
    }
    return "simple_query_predictor";
}
query_arrival_predictor_type string_to_query_arrival_predictor_type(
    const std::string& s ) {
    std::string lower = s;
    std::transform( lower.begin(), lower.end(), lower.begin(), ::tolower );
    if( lower.compare( "simple_query_predictor" ) == 0 ) {
        return query_arrival_predictor_type::SIMPLE_QUERY_PREDICTOR;
    }

    return query_arrival_predictor_type::SIMPLE_QUERY_PREDICTOR;
}

