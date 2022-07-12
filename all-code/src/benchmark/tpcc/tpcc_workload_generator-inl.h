#pragma once
inline workload_operation_enum tpcc_workload_generator::get_operation() {
    return op_selector_.get_operation( dist_.get_uniform_int( 0, 100 ) );
}

inline uint64_t tpcc_workload_generator::get_num_ops_before_timer_check()
    const {
    return configs_.bench_configs_.num_ops_before_timer_check_;
}

inline int32_t tpcc_workload_generator::generate_warehouse_id() {
    return client_w_id_;
}

inline int32_t tpcc_workload_generator::generate_district_id() {
    return dist_.get_uniform_int( lower_terminal_id_, upper_terminal_id_ );
}

inline int32_t tpcc_workload_generator::generate_rand_warehouse_id() {
    return dist_.get_uniform_int( 0, configs_.num_warehouses_ );
}
inline int32_t tpcc_workload_generator::generate_rand_district_id() {
    return dist_.get_uniform_int( 0,
                                  configs_.num_districts_per_warehouse_ - 1 );
}
inline int32_t tpcc_workload_generator::generate_rand_region_id() {
    return dist_.get_uniform_int( 0, region::NUM_REGIONS - 1 );
}
inline int32_t tpcc_workload_generator::generate_rand_nation_id() {
    return dist_.get_uniform_int( 0, nation::NUM_NATIONS - 1 );
}


inline int32_t tpcc_workload_generator::generate_customer_id() {
    return nu_dist_.nu_rand_c_last( 1,
                                    configs_.num_customers_per_district_ - 1 );
}

inline int32_t tpcc_workload_generator::generate_number_order_lines() {
    return dist_.get_uniform_int( order::MIN_OL_CNT,
                                  configs_.max_num_order_lines_per_order_ );
}

inline int32_t tpcc_workload_generator::generate_item_id() {
    return nu_dist_.nu_rand_ol_i_id( 0, configs_.num_items_ - 1 );
}
inline int32_t tpcc_workload_generator::generate_supplier_id() {
    return dist_.get_uniform_int( 0, configs_.num_suppliers_ - 1 );
}

inline int32_t tpcc_workload_generator::generate_warehouse_excluding(
    int32_t exclude_w_id ) {
    if( configs_.num_warehouses_ == 1 ) {
        return client_w_id_;
    }
    int32_t w_id = dist_.get_uniform_int( 0, configs_.num_warehouses_ );
    while( w_id == exclude_w_id ) {
        w_id = dist_.get_uniform_int( 0, configs_.num_warehouses_ );
    }
    return w_id;
}

inline bool tpcc_workload_generator::is_distributed_new_order() {
    return ( op_selector_.get_operation( dist_.get_uniform_int( 0, 100 ) ) <
             configs_.new_order_within_warehouse_likelihood_ );
}

inline bool tpcc_workload_generator::is_distributed_payment() {
    return ( op_selector_.get_operation( dist_.get_uniform_int( 0, 100 ) ) <
             configs_.payment_within_warehouse_likelihood_ );
}

inline float tpcc_workload_generator::generate_payment_amount() {
    return dist_.fixed_point( 2, history::MIN_PAYMENT_AMOUNT,
                              history::MAX_PAYMENT_AMOUNT );
}

inline std::tuple<int32_t, int32_t>
    tpcc_workload_generator::generate_scan_warehouse_id() {
    int32_t low = 0;
    int32_t high = configs_.num_warehouses_ - 1;

    if( !configs_.h_scans_full_tables_ ) {
        int32_t w1 = generate_rand_warehouse_id();
        int32_t w2 = generate_rand_warehouse_id();

        low = std::min( w1, w2 );
        high = std::max( w1, w2 );
    }

    return std::make_tuple<>( low, high );
}
inline std::tuple<int32_t, int32_t>
    tpcc_workload_generator::generate_scan_district_id() {
    int32_t low = 0;
    int32_t high = configs_.num_districts_per_warehouse_ - 1;

    if( !configs_.h_scans_full_tables_ ) {
        int32_t d1 = generate_rand_district_id();
        int32_t d2 = generate_rand_district_id();

        low = std::min( d1, d2 );
        high = std::max( d1, d2 );
    }

    return std::make_tuple<>( low, high );
}
inline std::tuple<int32_t, int32_t>
    tpcc_workload_generator::generate_scan_item_id() {
    int32_t low = 0;
    int32_t high = configs_.num_items_ - 1;

    if( !configs_.h_scans_full_tables_ ) {
        int32_t i1 = generate_item_id();
        int32_t i2 = generate_item_id();

        low = std::min( i1, i2 );
        high = std::max( i1, i2 );
    }

    return std::make_tuple<>( low, high );
}
inline std::tuple<int32_t, int32_t>
    tpcc_workload_generator::generate_scan_supplier_id() {
    int32_t low = 0;
    int32_t high = configs_.num_suppliers_ - 1;

    if( !configs_.h_scans_full_tables_ ) {
        int32_t s1 = generate_supplier_id();
        int32_t s2 = generate_supplier_id();

        low = std::min( s1, s2 );
        high = std::max( s1, s2 );
    }
    return std::make_tuple<>( low, high );
}


