#include "system_stats.h"

#include <glog/logging.h>

#include "constants.h"

#define US_TO_S 1000000  // there are 1 million us in a second

system_stats::system_stats()
    : start_time_( k_process_start_time ),
      prev_time_( start_time_ ),
      prev_usage_() {
    getrusage( RUSAGE_SELF, &prev_usage_ );
}
system_stats::~system_stats() {}

int64_t system_stats::get_cpu_usage_in_us( const struct rusage& usage ) {
    // time is ( tv_sec * 1,000,000 ) +  tv_usec
    // user time:
    // ru_utime
    // system time:
    // ru_stime

    int64_t u_t = ( usage.ru_utime.tv_sec * US_TO_S ) + usage.ru_utime.tv_usec;
    int64_t s_t = ( usage.ru_stime.tv_sec * US_TO_S ) + usage.ru_stime.tv_usec;

    return u_t + s_t;
}

machine_statistics system_stats::get_machine_statistics() {
    std::chrono::system_clock::time_point cur_time =
        std::chrono::system_clock::now();

    struct rusage usage;
    getrusage( RUSAGE_SELF, &usage );

    int64_t time_since_last_us =
        std::chrono::duration_cast<std::chrono::microseconds>( cur_time -
                                                               prev_time_ )
            .count();
    int64_t time_since_begin_us =
        std::chrono::duration_cast<std::chrono::microseconds>( cur_time -
                                                               start_time_ )
            .count();

    machine_statistics ret;

    int64_t cur_cpu = get_cpu_usage_in_us( usage );
    int64_t prev_cpu = get_cpu_usage_in_us( prev_usage_ );

    DVLOG( 20 ) << "cur cpu:" << cur_cpu << ", prev cpu:" << prev_cpu
                << ", time_since_last_us:" << time_since_last_us
                << ", time_since_begin_us:" << time_since_begin_us;

    ret.average_cpu_load =
        ( (double) ( cur_cpu - prev_cpu ) ) / (double) time_since_last_us;
    ret.average_interval = time_since_last_us;
    ret.average_overall_load =
        ( (double) cur_cpu ) / (double) time_since_begin_us;
    ret.memory_usage = usage.ru_maxrss;

    DVLOG( 10 ) << "System Stats:" << ret;

    prev_time_ = cur_time;
    prev_usage_ = usage;

    return ret;
}

