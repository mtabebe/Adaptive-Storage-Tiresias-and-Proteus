#include "benchmark_configs.h"

#include <glog/logging.h>

benchmark_configs construct_benchmark_configs(
    uint32_t num_clients, uint32_t gc_sleep_time, uint32_t benchmark_time_sec,
    uint64_t num_ops_before_timer_check, bool limit_update_propagation,
    bool enable_secondary_storage, const std::string& secondary_storage_dir ) {
    benchmark_configs configs;

    configs.num_clients_ = num_clients;
    configs.gc_sleep_time_ = gc_sleep_time;
    configs.benchmark_time_sec_ = benchmark_time_sec;
    configs.num_ops_before_timer_check_ = num_ops_before_timer_check;
    configs.limit_update_propagation_ = limit_update_propagation;
    configs.enable_secondary_storage_ = enable_secondary_storage;
    configs.secondary_storage_dir_ = secondary_storage_dir;

    DVLOG( 1 ) << "Created: " << configs;

    return configs;
}

std::ostream& operator<<( std::ostream& os, const benchmark_configs& config ) {
    os << "Benchmark Config: [ Num Clients:" << config.num_clients_
       << "< GC Sleep Time:" << config.gc_sleep_time_
       << ", Benchmark Time (Sec):" << config.benchmark_time_sec_
       << ", Number of Operations Before Timer Check:"
       << config.num_ops_before_timer_check_
       << ", Limit Update Propagation:" << config.limit_update_propagation_
       << ", enable_secondary_storage_:" << config.enable_secondary_storage_
       << ", secondary_storage_dir_:" << config.secondary_storage_dir_

       << " ]";
    return os;
}
