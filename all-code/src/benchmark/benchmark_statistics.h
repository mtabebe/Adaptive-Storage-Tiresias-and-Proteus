#pragma once

#include <chrono>
#include <vector>

#include "workload_operation_enum.h"

class benchmark_statistics {
   public:
    benchmark_statistics();
    ~benchmark_statistics();

    void init( const std::vector<workload_operation_enum>& operations );
    ALWAYS_INLINE void add_outcome(
        const workload_operation_enum&         operation,
        const workload_operation_outcome_enum& outcome, const double lat,
        const uint64_t count = 1 );
    ALWAYS_INLINE void store_running_time( double running_time_ns );

    void log_statistics() const;

    ALWAYS_INLINE double get_running_time() const;
    ALWAYS_INLINE        uint64_t
        get_outcome( const workload_operation_enum&         op,
                     const workload_operation_outcome_enum& outcome ) const;
    ALWAYS_INLINE double get_latency(
        const workload_operation_enum&         op,
        const workload_operation_outcome_enum& outcome ) const;

    bool merge( const benchmark_statistics& other );

   private:
    uint32_t                           shift_;
    std::vector<std::vector<uint64_t>> outcome_counters_;
    std::vector<std::vector<double>>   outcome_lats_;
    double                             running_time_ns_;
};

#include "benchmark_statistics-inl.h"
