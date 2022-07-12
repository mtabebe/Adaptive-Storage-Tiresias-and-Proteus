#pragma once
#include <algorithm>

inline std::string rewriter_type_string( const rewriter_type& w_type ) {
    switch( w_type ) {
        case RANGE_REWRITER:
            return "RANGE_REWRITER";
        case HASH_REWRITER:
            return "HASH_REWRITER";
        case SINGLE_MASTER_REWRITER:
            return "SINGLE_MASTER_REWRITER";
        case WAREHOUSE_REWRITER:
            return "WAREHOUSE_REWRITER";
        case TWITTER_REWRITER:
            return "TWITTER_REWRITER";
        case PROBABILITY_RANGE_REWRITER:
            return "PROBABILITY_RANGE_REWRITER";
        case SINGLE_NODE_REWRITER:
            return "SINGLE_NODE_REWRITER";
        case NO_OP_REWRITER:
            return "NO_OP_REWRITER";
        case UNKNOWN_REWRITER:
            return "UNKNOWN_REWRITER";
    }
    return "UNKNOWN_REWRITER";
}
inline rewriter_type string_to_rewriter_type( const std::string& s ) {
    std::string lower = s;
    std::transform( lower.begin(), lower.end(), lower.begin(), ::tolower );
    if( lower.compare( "range_rewriter" ) == 0 ) {
        return rewriter_type::RANGE_REWRITER;
    } else if( lower.compare( "hash_rewriter" ) == 0 ) {
        return rewriter_type::HASH_REWRITER;
    } else if( lower.compare( "single_master_rewriter" ) == 0 ) {
        return rewriter_type::SINGLE_MASTER_REWRITER;
    } else if( lower.compare( "warehouse_rewriter" ) == 0 ) {
        return rewriter_type::WAREHOUSE_REWRITER;
    } else if( lower.compare( "twitter_rewriter" ) == 0 ) {
        return rewriter_type::TWITTER_REWRITER;
    } else if( lower.compare( "probability_range_rewriter" ) == 0 ) {
        return rewriter_type::PROBABILITY_RANGE_REWRITER;
    } else if( lower.compare( "single_node_rewriter" ) == 0 ) {
        return rewriter_type::SINGLE_NODE_REWRITER;
    } else if( lower.compare( "no_op_rewriter" ) == 0 ) {
        return rewriter_type::NO_OP_REWRITER;
    }

    return rewriter_type::UNKNOWN_REWRITER;
}
