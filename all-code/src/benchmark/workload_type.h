#pragma once

#include <string>

#include "../common/hw.h"

enum workload_type {
    UNKNOWN_WORKLOAD,
    YCSB,
    TPCC,
    SMALLBANK,
    TWITTER,
};

ALWAYS_INLINE std::string workload_type_string( const workload_type& w_type );
ALWAYS_INLINE workload_type string_to_workload_type( const std::string& s );

#include "workload_type-inl.h"
