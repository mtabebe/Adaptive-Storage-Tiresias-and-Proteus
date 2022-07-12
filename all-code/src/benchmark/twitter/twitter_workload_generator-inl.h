#pragma once
inline workload_operation_enum twitter_workload_generator::get_operation() {
    return op_selector_.get_operation( dist_.get_uniform_int( 0, 100 ) );
}

inline uint64_t twitter_workload_generator::get_num_ops_before_timer_check()
    const {
    return configs_.bench_configs_.num_ops_before_timer_check_;
}

