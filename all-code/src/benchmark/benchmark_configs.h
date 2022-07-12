#pragma once

#include <iostream>

#include "../common/constants.h"
#include "../common/hw.h"

class benchmark_configs {
   public:
    uint32_t num_clients_;
    uint32_t gc_sleep_time_;
    uint32_t benchmark_time_sec_;
    uint64_t num_ops_before_timer_check_;
    bool     limit_update_propagation_;

    bool        enable_secondary_storage_;
    std::string secondary_storage_dir_;
};

benchmark_configs construct_benchmark_configs(
    uint32_t num_clients = k_bench_num_clients,
    uint32_t gc_sleep_time = k_gc_sleep_time,
    uint32_t benchmark_time_sec = k_bench_time_sec,
    uint64_t num_ops_before_timer_check = k_bench_num_ops_before_timer_check,
    bool     limit_update_propagation = k_limit_number_of_records_propagated,
    bool     enable_secondary_storage = k_enable_secondary_storage,
    const std::string& secondary_storage_dir = k_secondary_storage_dir );

std::ostream& operator<<( std::ostream& os, const benchmark_configs& configs );
