#pragma once

#include <glog/logging.h>

inline void benchmark_statistics::add_outcome(
    const workload_operation_enum&         operation,
    const workload_operation_outcome_enum& outcome, const double lat,
    const uint64_t count ) {
    uint32_t pos = operation - shift_;
    DCHECK_GE( pos, 0 );
    DCHECK_LT( pos, outcome_counters_.size() );
    DCHECK_LT( pos, outcome_lats_.size() );
    uint32_t outcome_pos = (uint32_t) outcome;
    outcome_counters_.at( pos ).at( outcome_pos ) += count;
    outcome_lats_.at( pos ).at( outcome_pos ) += lat;
}
inline void benchmark_statistics::store_running_time( double running_time_ns ) {
    running_time_ns_ = running_time_ns;
}

inline double benchmark_statistics::get_running_time() const {
    return running_time_ns_;
}
inline uint64_t benchmark_statistics::get_outcome(
    const workload_operation_enum&         op,
    const workload_operation_outcome_enum& outcome ) const {
    uint32_t pos = op - shift_;
    DCHECK_LT( pos, outcome_counters_.size() );
    uint32_t outcome_pos = (uint32_t) outcome;
    return outcome_counters_.at( pos ).at( outcome_pos );
}

inline double benchmark_statistics::get_latency(
    const workload_operation_enum&         op,
    const workload_operation_outcome_enum& outcome ) const {
    uint32_t pos = op - shift_;
    DCHECK_LT( pos, outcome_lats_.size() );
    uint32_t outcome_pos = (uint32_t) outcome;
    return outcome_lats_.at( pos ).at( outcome_pos );
}
