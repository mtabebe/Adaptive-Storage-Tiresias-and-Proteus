#pragma once

#include <glog/logging.h>

inline workload_operation_enum workload_operation_selector::get_operation(
    uint32_t prob ) const {
    DCHECK_GE( prob, 0 );
    DCHECK_LE( prob, 99 );
    return workload_probabilities_.at( prob );
}
