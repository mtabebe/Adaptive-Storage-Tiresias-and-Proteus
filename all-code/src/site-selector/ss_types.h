#pragma once

#include <string>

#include "../common/hw.h"

enum ss_strategy_type {
    UNKNOWN_STRATEGY,
    NAIVE,
    HEURISTIC,
};

ALWAYS_INLINE std::string ss_strategy_type_string(
    const ss_strategy_type& strategy_type );
ALWAYS_INLINE ss_strategy_type
    string_to_ss_strategy_type( const std::string& s );

enum ss_mastering_type {
    UNKNOWN_MASTERING,
    SINGLE_MASTER_MULTI_SLAVE,
    TWO_PC,
    DYNAMIC_MASTERING,
    PARTITIONED_STORE,
    DRP,
    E_STORE,
    CLAY,
    ADR,
    ADAPT,
};

class mastering_type_configs {
   public:
    mastering_type_configs( const ss_mastering_type& master_type );

    ss_mastering_type master_type_;
    bool              is_mastering_type_fully_replicated_;
    bool              is_single_site_transaction_system_;
};

ALWAYS_INLINE std::string ss_mastering_type_string(
    const ss_mastering_type& mastering_type );
ALWAYS_INLINE ss_mastering_type
    string_to_ss_mastering_type( const std::string& s );

#include "ss_types-inl.h"
