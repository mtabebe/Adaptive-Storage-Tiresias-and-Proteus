#pragma once

#include <string>

#include "../common/hw.h"

enum rewriter_type {
    UNKNOWN_REWRITER,
    RANGE_REWRITER,
    HASH_REWRITER,
    SINGLE_MASTER_REWRITER,
    WAREHOUSE_REWRITER,
    TWITTER_REWRITER,
    PROBABILITY_RANGE_REWRITER,
    SINGLE_NODE_REWRITER,
    NO_OP_REWRITER,
};

ALWAYS_INLINE std::string rewriter_type_string( const rewriter_type& w_type );
ALWAYS_INLINE rewriter_type string_to_rewriter_type( const std::string& s );

#include "rewriter_type-inl.h"
