#pragma once
#include "../common/constants.h"
#include <memory>
#include <thread>

static const double   k_update_time_usec = (double) 1000000 / k_expected_tps;
static const uint64_t k_exp_updates_per_spin_time =
    ( uint64_t )( k_max_spin_time_us * k_update_time_usec );
// A memory read is about 100 ns...
// 1 us -> 10 of 100ns reads
static const uint64_t k_tot_loops = ( k_max_spin_time_us * 10 );

inline bool should_yield( const uint64_t &num_updates_required ) {
    return num_updates_required > k_exp_updates_per_spin_time;
}

inline uint64_t get_num_loops_before_backoff() { return k_tot_loops; }
