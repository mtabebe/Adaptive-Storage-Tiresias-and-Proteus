#pragma once
inline workload_operation_enum smallbank_workload_generator::get_operation() {
    return op_selector_.get_operation( dist_.get_uniform_int( 0, 100 ) );
}

inline uint64_t smallbank_workload_generator::get_num_ops_before_timer_check()
    const {
    return configs_.bench_configs_.num_ops_before_timer_check_;
}

inline uint64_t smallbank_workload_generator::generate_second_customer_id(
    uint64_t ori_id ) {
    uint64_t range_size = ( configs_.partition_size_ *
                            configs_.customer_account_partition_spread_ ) /
                          2;
    uint64_t min_bound = 0;
    uint64_t max_bound = configs_.num_accounts_ - 1;
    if( range_size < ori_id ) {
        min_bound = ori_id - range_size;
    }
    if( ( configs_.num_accounts_ - 1 ) - range_size > ori_id ) {
        max_bound = ori_id + range_size;
    }
    return dist_.get_uniform_int( min_bound, max_bound );
}

inline void smallbank_workload_generator::generate_customer_ids(
    bool need_two_accounts ) {
    customer_ids_.at( 0 ) =
        dist_.get_uniform_int( 0, configs_.num_accounts_ - 1 );

    if( !need_two_accounts or ( configs_.num_accounts_ <= 1 ) ) {
        return;
    }

    customer_ids_.at( 1 ) =
        generate_second_customer_id( customer_ids_.at( 0 ) );

    while( customer_ids_.at( 1 ) == customer_ids_.at( 0 ) ) {
        customer_ids_.at( 1 ) =
            generate_second_customer_id( customer_ids_.at( 0 ) );
    }
}

