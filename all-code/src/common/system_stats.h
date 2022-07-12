#pragma once

#include <chrono>
#include <sys/resource.h>
#include <sys/time.h>

#include "../gen-cpp/gen-cpp/proto_types.h"

class system_stats {
   public:
    system_stats();
    ~system_stats();

    machine_statistics get_machine_statistics();

   private:
    int64_t get_cpu_usage_in_us( const struct rusage& usage );

    // the time of construction
    std::chrono::system_clock::time_point start_time_;
    // the time of the last rusage call
    std::chrono::system_clock::time_point prev_time_;
    struct rusage prev_usage_;
};
