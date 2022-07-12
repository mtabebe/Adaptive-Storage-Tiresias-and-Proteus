#pragma once

#include "../../common/perf_tracking.h"
#include "../../common/scan_results.h"
#include "../../common/string_conversion.h"
#include "../../common/thread_utils.h"
#include "tpcc_db_operators.h"
#include <glog/logging.h>
#include <glog/stl_logging.h>

tpch_benchmark_worker_types tpch_benchmark_worker_templ::tpch_benchmark_worker(
    uint32_t client_id, db_abstraction* db, zipf_distribution_cdf* z_cdf,
    const workload_operation_selector& op_selector, const tpcc_configs& configs,
    const db_abstraction_configs& abstraction_configs,
    const snapshot_vector&        init_state )
    : db_operators_( client_id, db, abstraction_configs,
                     configs.bench_configs_.limit_update_propagation_,
                     false /* store glboal state */ ),
      loader_( db, configs, abstraction_configs, client_id ),
      generator_( z_cdf, op_selector, client_id, configs ),
      data_sizes_( get_tpcc_data_sizes( configs ) ),
      statistics_(),
      worker_( nullptr ),
      done_( false ) {

    db_operators_.state_ = init_state;
    statistics_.init( k_tpcch_workload_operations );
}

tpch_benchmark_worker_types
    tpch_benchmark_worker_templ::~tpch_benchmark_worker() {}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::start_timed_workload() {
    worker_ = std::unique_ptr<std::thread>(
        new std::thread( &tpch_benchmark_worker_templ::run_workload, this ) );
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::stop_timed_workload() {
    done_ = true;
    DCHECK( worker_ );
    join_thread( *worker_ );
    worker_ = nullptr;
}

tpch_benchmark_worker_types void tpch_benchmark_worker_templ::run_workload() {
    // time me boys
    DVLOG( 10 ) << db_operators_.client_id_ << " starting workload";
    std::chrono::high_resolution_clock::time_point s =
        std::chrono::high_resolution_clock::now();

    // this will only every be true once so it's likely to not be done

    while( likely( !done_ ) ) {
        // we don't want to check this very often so we do a bunch of operations
        // in a loop
        do_workload_operation();
    }
    std::chrono::high_resolution_clock::time_point e =
        std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::nano> elapsed = e - s;
    DVLOG( 10 ) << db_operators_.client_id_ << " ran for:" << elapsed.count()
                << " ns";
    statistics_.store_running_time( elapsed.count() );
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_workload_operation() {

    workload_operation_enum op = generator_.get_operation();
    DCHECK_GE( op, TPCH_Q1 );
    DCHECK_LE( op, TPCH_ALL );

    do_workload_operation_work( op );
}
tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_workload_operation_work(
        const workload_operation_enum& op ) {

    double lat;

    DVLOG( 30 ) << db_operators_.client_id_ << " performing "
                << workload_operation_string( op );
    start_timer( TPCH_WORKLOAD_OP_TIMER_ID );
    workload_operation_outcome_enum status = perform_workload_operation( op );
    stop_and_store_timer( TPCH_WORKLOAD_OP_TIMER_ID, lat );
    statistics_.add_outcome( op, status, lat );

    DVLOG( 30 ) << db_operators_.client_id_ << " performing "
                << k_workload_operations_to_strings.at( op )
                << " status:" << status;
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::perform_workload_operation(
        const workload_operation_enum& op ) {
    switch( op ) {
        case TPCH_Q1:
            return do_q1();
        case TPCH_Q2:
            return do_q2();
        case TPCH_Q3:
            return do_q3();
        case TPCH_Q4:
            return do_q4();
        case TPCH_Q5:
            return do_q5();
        case TPCH_Q6:
            return do_q6();
        case TPCH_Q7:
            return do_q7();
        case TPCH_Q8:
            return do_q8();
        case TPCH_Q9:
            return do_q9();
        case TPCH_Q10:
            return do_q10();
        case TPCH_Q11:
            return do_q11();
        case TPCH_Q12:
            return do_q12();
        case TPCH_Q13:
            return do_q13();
        case TPCH_Q14:
            return do_q14();
        case TPCH_Q15:
            return do_q15();
        case TPCH_Q16:
            return do_q16();
        case TPCH_Q17:
            return do_q17();
        case TPCH_Q18:
            return do_q18();
        case TPCH_Q19:
            return do_q19();
        case TPCH_Q20:
            return do_q20();
        case TPCH_Q21:
            return do_q21();
        case TPCH_Q22:
            return do_q22();
        case TPCH_ALL:
            return do_all();
    }
    // should be unreachable
    return false;
}

tpch_benchmark_worker_types inline benchmark_statistics
    tpch_benchmark_worker_templ::get_statistics() const {
    return statistics_;
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_all() {
    for( workload_operation_enum op = TPCH_Q1; op <= TPCH_Q22; op++ ) {
        do_workload_operation_work( op );
    }
    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q1() {
    auto key_range = generator_.get_q1_scan_range();
    return do_q1_work( generator_.get_q1_delivery_time(),
                       std::get<0>( key_range ), std::get<1>( key_range ) );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
    tpch_benchmark_worker_templ::do_q1_work( uint64_t          delivery_time,
                                             const order_line& low_key,
                                             const order_line& high_key ) {

    auto scan_args = generate_q1_scan_args( low_key, high_key, delivery_time );

    scan_result scan_res;
    do_q1_work_by_scan( map_scan_args( scan_args ), scan_res );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q1_scan_args(
        const order_line& low_key, const order_line& high_key,
        uint64_t delivery_time ) const {

    scan_arguments scan_arg;
    scan_arg.label = 0;

    cell_predicate c_pred;
    c_pred.table_id = k_tpcc_order_line_table_id;
    c_pred.col_id = order_line_cols::ol_delivery_d_c_since;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( delivery_time );
    c_pred.predicate = predicate_type::type::GREATER_THAN;

    scan_arg.predicate.and_predicates.emplace_back( c_pred );
    scan_arg.read_ckrs = generate_q1_ckrs( low_key, high_key );

    std::vector<scan_arguments> ret = {scan_arg};
    return ret;
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q1_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res ) {

    DVLOG( 20 ) << "Do Q1 Work";

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    DCHECK_GE( 1, scan_args.size() );

    std::vector<uint32_t> project_cols = {
        order_line_cols::ol_number, order_line_cols::ol_quantity,
        order_line_cols::ol_amount, order_line_cols::ol_delivery_d_c_since};
    std::unordered_map<q1_kv_types>  map_res;

    if( scan_args.count( 0 ) == 1 ) {
        map_res = db_operators_.scan_mr<q1_types>(
            k_tpcc_order_line_table_id, scan_args.at( 0 ).read_ckrs,
            project_cols, scan_args.at( 0 ).predicate,
            (EmptyProbe) emptyProbeVal );
    }

    std::vector<result_tuple> results;
    for( const auto& res : map_res ) {
        result_tuple rt;
        rt.table_id = k_tpcc_order_line_table_id;
        rt.row_id = res.first;

        result_cell rc_q_cnt;
        rc_q_cnt.col_id = 0;
        rc_q_cnt.present = true;
        rc_q_cnt.type = data_type::type::INT64;
        rc_q_cnt.data = int64_to_string( res.second.quantity_count_ );

        result_cell rc_q_sum;
        rc_q_sum.col_id = 1;
        rc_q_sum.present = true;
        rc_q_sum.type = data_type::type::DOUBLE;
        rc_q_sum.data = double_to_string( res.second.quantity_sum_ );

        result_cell rc_a_cnt;
        rc_a_cnt.col_id = 2;
        rc_a_cnt.present = true;
        rc_a_cnt.type = data_type::type::INT64;
        rc_a_cnt.data = int64_to_string( res.second.amount_count_ );

        result_cell rc_a_sum;
        rc_a_sum.col_id = 3;
        rc_a_sum.present = true;
        rc_a_sum.type = data_type::type::DOUBLE;
        rc_a_sum.data = double_to_string( res.second.amount_sum_ );

        rt.cells = {rc_q_cnt, rc_q_sum, rc_a_cnt, rc_a_sum};

        results.emplace_back( rt );
    }
    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
    DVLOG( 40 ) << "Do Q1 Work OK";
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q1_ckrs(
        const order_line& low_key, const order_line& high_key ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {std::make_tuple<>(
        order_line_cols::ol_number, order_line_cols::ol_delivery_d_c_since )};

    return generate_order_line_ckrs( low_key, high_key, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_order_line_ckrs(
        const order_line& low_key, const order_line&       high_key,
        const std::vector<std::tuple<uint32_t, uint32_t>>& cols ) const {

    std::vector<cell_key_ranges> ckrs;

    order_line ol;

    for( int32_t w_id = low_key.ol_w_id; w_id <= high_key.ol_w_id; w_id++ ) {
        DVLOG( 40 ) << "Generate order line ckrs w_id:" << w_id;
        ol.ol_w_id = w_id;

        int32_t d_id_start = low_key.ol_d_id;
        int32_t d_id_end = high_key.ol_d_id;

        if( w_id != low_key.ol_w_id ) {
            d_id_start = 0;
        }
        if( w_id != high_key.ol_w_id ) {
            d_id_end = generator_.configs_.num_districts_per_warehouse_ - 1;
        }

        for( int32_t d_id = d_id_start; d_id <= d_id_end; d_id++ ) {
            DVLOG( 40 ) << "Generate order line ckrs d_id:" << d_id;
            ol.ol_d_id = d_id;
            ol.ol_o_id = 0;
            ol.ol_number = 0;

            uint64_t ol_low = make_order_line_key( ol, generator_.configs_ );

            ol.ol_o_id =
                generator_.configs_.expected_num_orders_per_cust_ *
                ( generator_.configs_.num_customers_per_district_ - 1 );
            ol.ol_number =
                generator_.configs_.max_num_order_lines_per_order_ - 1;

            uint64_t ol_high = make_order_line_key( ol, generator_.configs_ );
            for( const auto& col : cols ) {
                ckrs.emplace_back( create_cell_key_ranges(
                    k_tpcc_order_line_table_id, ol_low, ol_high,
                    std::get<0>( col ), std::get<1>( col ) ) );
            }
        }
    }
    return ckrs;
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q2() {
    auto stock_range = generator_.get_q11_stock_range();
    auto supplier_range = generator_.get_q5_supplier_range();
    auto nation_range = generator_.get_q8_nation_range();
    auto region_id = generator_.generate_rand_region_id();

    return do_q2_work(
        std::get<0>( stock_range ), std::get<1>( stock_range ),
        std::get<0>( supplier_range ), std::get<1>( supplier_range ),
        std::get<0>( nation_range ), std::get<1>( nation_range ), region_id );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
    tpch_benchmark_worker_templ::do_q2_work( const stock&    low_stock,
                                             const stock&    high_stock,
                                             const supplier& low_supplier,
                                             const supplier& high_supplier,
                                             const nation&   low_nation,
                                             const nation&   high_nation,
                                             int32_t         region_id ) {

    auto scan_args = generate_q2_scan_args( low_stock, high_stock, low_supplier,
                                            high_supplier, low_nation,
                                            high_nation, region_id );

    scan_result scan_res;
    do_q2_work_by_scan( map_scan_args( scan_args ), scan_res );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q2_scan_args(
        const stock& low_stock, const stock& high_stock,
        const supplier& low_supplier, const supplier& high_supplier,
        const nation& low_nation, const nation& high_nation,
        int32_t region_id ) const {

    scan_arguments nation_scan_arg;
    nation_scan_arg.label = 0;
    nation_scan_arg.read_ckrs =
        generate_q2_nation_ckrs( low_nation, high_nation );

    cell_predicate c_pred;

    c_pred.table_id = k_tpcc_nation_table_id;
    c_pred.col_id = nation_cols::r_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( region_id );
    c_pred.predicate = predicate_type::type::EQUALITY;
    nation_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments supplier_scan_arg;
    supplier_scan_arg.label = 1;
    supplier_scan_arg.read_ckrs =
        generate_q2_supplier_ckrs( low_supplier, high_supplier );

    c_pred.table_id = k_tpcc_supplier_table_id;
    c_pred.col_id = supplier_cols::n_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_nation.n_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    supplier_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_nation.n_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    supplier_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments item_scan_arg;
    item_scan_arg.label = 2;
    item_scan_arg.read_ckrs = generate_q2_item_ckrs( low_stock, high_stock );

    scan_arguments stock_scan_arg;
    stock_scan_arg.label = 3;
    stock_scan_arg.read_ckrs = generate_q2_stock_ckrs( low_stock, high_stock );

    c_pred.table_id = k_tpcc_stock_table_id;
    c_pred.col_id = stock_cols::s_s_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_supplier.s_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    stock_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_supplier.s_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    stock_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    std::vector<scan_arguments> scan_args = {nation_scan_arg, supplier_scan_arg,
                                             item_scan_arg, stock_scan_arg};

    return scan_args;
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q2_stock_ckrs(
        const stock& low_stock, const stock& high_stock ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( stock_cols::s_i_id, stock_cols::s_quantity )};
    return generate_stock_ckrs( low_stock, high_stock, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q2_item_ckrs(
        const stock& low_stock, const stock& high_stock ) const {
    item low_item;
    item high_item;

    low_item.i_id = low_stock.s_i_id;
    high_item.i_id = high_stock.s_i_id;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( item_cols::i_name, item_cols::i_data )};
    return generate_item_ckrs( low_item, high_item, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q2_supplier_ckrs(
        const supplier& low_supplier, const supplier& high_supplier ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( supplier_cols::n_id, supplier_cols::s_name ),
        std::make_tuple<>( supplier_cols::s_comment,
                           supplier_cols::s_comment )};
    return generate_supplier_ckrs( low_supplier, high_supplier, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q2_nation_ckrs(
        const nation& low_nation, const nation& high_nation ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( nation_cols::r_id, nation_cols::n_name )};
    return generate_nation_ckrs( low_nation, high_nation, cols );
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q2_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res ) {
    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    // first do the first scan
    DCHECK_GE( 4, scan_args.size() );

    // do the scans
    std::vector<uint32_t> nation_project_cols = {nation_cols::r_id,
                                                 nation_cols::n_name};
    std::unordered_map<q2_nation_kv_types> nation_map_res;
    if( scan_args.count( 0 ) == 1 ) {
        nation_map_res = db_operators_.scan_mr<q2_nation_types>(
            k_tpcc_nation_table_id, scan_args.at( 0 ).read_ckrs,
            nation_project_cols, scan_args.at( 0 ).predicate,
            (EmptyProbe) emptyProbeVal);
    }

    std::vector<uint32_t> supplier_project_cols = {
        supplier_cols::n_id, supplier_cols::s_name, supplier_cols::s_phone,
        supplier_cols::s_comment};
    q2_supplier_probe supplier_probe( nation_map_res, generator_.configs_);
    std::unordered_map<q2_supplier_kv_types> supplier_map_res;
    if( scan_args.count( 1 ) == 1 ) {
        supplier_map_res = db_operators_.scan_mr<q2_supplier_types>(
            k_tpcc_supplier_table_id, scan_args.at( 1 ).read_ckrs,
            supplier_project_cols, scan_args.at( 1 ).predicate,
            supplier_probe);
    }

    std::vector<uint32_t> item_project_cols = {item_cols::i_name,
                                               item_cols::i_data};
    std::unordered_map<q2_item_kv_types> item_map_res;
    if( scan_args.count( 2 ) == 1 ) {
        item_map_res = db_operators_.scan_mr<q2_item_types>(
            k_tpcc_item_table_id, scan_args.at( 2 ).read_ckrs,
            item_project_cols, scan_args.at( 2 ).predicate,
            (EmptyProbe) emptyProbeVal);
    }

    std::vector<uint32_t> stock_project_cols = {
        stock_cols::s_i_id,
        stock_cols::s_s_id, stock_cols::s_quantity};
    std::unordered_map<q2_stock_kv_types> stock_map_res;
    q2_stock_probe stock_probe( supplier_map_res, item_map_res, generator_.configs_);
    if( scan_args.count( 3 ) == 1 ) {
        stock_map_res = db_operators_.scan_mr<q2_stock_types>(
            k_tpcc_stock_table_id, scan_args.at( 3 ).read_ckrs,
            stock_project_cols, scan_args.at( 3 ).predicate,
            stock_probe);
    }

    std::vector<result_tuple> results;
    for( const auto& stock_entry : stock_map_res ) {
        int32_t     i_id = stock_entry.first;
        const auto& s = stock_entry.second;

        auto i_found = item_map_res.find( i_id );
        if( i_found == item_map_res.end() ) {
            continue;
        }
        const auto& i = i_found->second;

        auto s_found = supplier_map_res.find( s.s_s_id );
        if( s_found == supplier_map_res.end() ) {
            continue;
        }
        const auto& sup = s_found->second;

        auto n_found = nation_map_res.find( sup.n_id );
        if( n_found == nation_map_res.end() ) {
            continue;
        }
        const auto& nat = n_found->second;

        result_tuple rt;
        rt.row_id = make_stock_key( s, generator_.configs_ );
        rt.table_id = k_tpcc_supplier_table_id;

        result_cell cell;
        cell.present = true;

        cell.col_id = 0;
        cell.data = int64_to_string( sup.s_id );
        cell.type = data_type::INT64;
        rt.cells.emplace_back( cell );

        cell.col_id = 1;
        cell.data = sup.s_name;
        cell.type = data_type::STRING;
        rt.cells.emplace_back( cell );

        cell.col_id = 2;
        cell.data = nat.n_name;
        cell.type = data_type::STRING;
        rt.cells.emplace_back( cell );

        cell.col_id = 3;
        cell.data = int64_to_string( i.i_id );
        cell.type = data_type::INT64;
        rt.cells.emplace_back( cell );

        cell.col_id = 4;
        cell.data = i.i_name;
        cell.type = data_type::STRING;
        rt.cells.emplace_back( cell );

        cell.col_id = 5;
        cell.data = sup.s_phone;
        cell.type = data_type::STRING;
        rt.cells.emplace_back( cell );

        cell.col_id = 6;
        cell.data = sup.s_comment;
        cell.type = data_type::STRING;
        rt.cells.emplace_back( cell );

        results.emplace_back( rt );
    }
    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q3() {
    auto key_range = generator_.get_q4_order_range();
    auto low_time = generator_.get_q1_delivery_time();
    auto c_state_range = generator_.get_q3_customer_state_range();

    return do_q3_work( std::get<0>( key_range ), std::get<1>( key_range ),
                       std::get<0>( c_state_range ),
                       std::get<1>( c_state_range ), low_time );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types workload_operation_outcome_enum
    tpch_benchmark_worker_templ::do_q3_work( const order&       low_order,
                                             const order&       high_order,
                                             const std::string& low_c_state,
                                             const std::string& high_c_state,
                                             uint64_t low_delivery_time ) {

    auto scan_args = generate_q3_scan_args( low_order, high_order, low_c_state,
                                            high_c_state, low_delivery_time );

    scan_result scan_res;
    do_q3_work_by_scan( map_scan_args( scan_args ), scan_res );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q3_scan_args(
        const order& low_order, const order& high_order,
        const std::string& low_c_state, const std::string& high_c_state,
        uint64_t low_delivery_time ) const {
    auto order_ckrs = generate_q3_order_ckrs( low_order, high_order );
    auto order_line_ckrs = generate_q3_order_line_ckrs( low_order, high_order );

    scan_arguments cust_scan_arg;
    cust_scan_arg.label = 0;
    cust_scan_arg.read_ckrs =
        generate_q3_customer_ckrs( low_order, high_order );

    cell_predicate cust_pred;
    cust_pred.table_id = k_tpcc_customer_table_id;
    cust_pred.col_id = customer_cols::c_address_a_state;
    cust_pred.type = data_type::type::STRING;
    cust_pred.data = low_c_state;
    cust_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;

    cust_scan_arg.predicate.and_predicates.emplace_back( cust_pred );

    cust_pred.data = high_c_state;
    cust_pred.predicate = predicate_type::type::LESS_THAN;
    cust_scan_arg.predicate.and_predicates.emplace_back( cust_pred );

    scan_arguments order_scan_arg;
    order_scan_arg.label = 1;
    order_scan_arg.read_ckrs = order_ckrs;

    cell_predicate c_pred;
    c_pred.table_id = k_tpcc_order_table_id;
    c_pred.col_id = order_cols::o_entry_d_c_since;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( low_delivery_time );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;

    order_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments order_line_scan_arg;
    order_line_scan_arg.label = 2;
    order_line_scan_arg.read_ckrs = order_line_ckrs;

    cell_predicate ol_c_pred;
    ol_c_pred.table_id = k_tpcc_order_line_table_id;
    ol_c_pred.col_id = order_line_cols::ol_delivery_d_c_since;
    ol_c_pred.type = data_type::type::UINT64;
    ol_c_pred.data = uint64_to_string( low_delivery_time );
    ol_c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;

    order_line_scan_arg.predicate.and_predicates.emplace_back( ol_c_pred );

    std::vector<scan_arguments> scan_args = {cust_scan_arg, order_scan_arg,
                                             order_line_scan_arg};

    return scan_args;
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q3_order_ckrs(
        const order& low_key, const order& high_key ) const {

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_cols::o_id, order_cols::o_w_id ),
        std::make_tuple<>( order_cols::o_entry_d_c_since,
                           order_cols::o_entry_d_c_since )};
    return generate_order_ckrs( low_key, high_key, cols );

    std::vector<cell_key_ranges> ckrs;
    return ckrs;
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q3_order_line_ckrs(
        const order& low_key, const order& high_key ) const {
    order_line low_ol;
    order_line high_ol;

    low_ol.ol_w_id = low_key.o_w_id;
    high_ol.ol_w_id = high_key.o_w_id;

    low_ol.ol_d_id = low_key.o_d_id;
    high_ol.ol_d_id = high_key.o_d_id;

    low_ol.ol_o_id = low_key.o_id;
    low_ol.ol_number = order::MIN_OL_CNT;

    high_ol.ol_o_id = high_key.o_id;
    high_ol.ol_number = generator_.configs_.max_num_order_lines_per_order_ - 1;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_line_cols::ol_o_id, order_line_cols::ol_w_id ),
        std::make_tuple<>( order_line_cols::ol_amount,
                           order_line_cols::ol_delivery_d_c_since ),
    };

    return generate_order_line_ckrs( low_ol, high_ol, cols );
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q3_customer_ckrs(
        const order& low_key, const order& high_key ) const {
    customer low_cust;
    customer high_cust;

    low_cust.c_id = 0;
    high_cust.c_id = generator_.configs_.num_customers_per_district_ - 1;

    low_cust.c_d_id = low_key.o_d_id;
    high_cust.c_d_id = high_key.o_d_id;

    low_cust.c_w_id = low_key.o_w_id;
    high_cust.c_w_id = high_key.o_w_id;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( customer_cols::c_id, customer_cols::c_w_id ),
        std::make_tuple<>( customer_cols::c_address_a_state,
                           customer_cols::c_address_a_state )};

    return generate_customer_ckrs( low_cust, high_cust, cols );
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q3_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res ) {
    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    // first do the first scan
    DCHECK_GE( 3, scan_args.size() );

    // do the scans
    std::vector<uint32_t> customer_project_cols = {
        customer_cols::c_id, customer_cols::c_d_id, customer_cols::c_w_id,
        customer_cols::c_address_a_state};

    std::unordered_map<q3_customer_kv_types> cust_map_res;
    if( scan_args.count( 0 ) == 1 ) {
        cust_map_res = db_operators_.scan_mr<q3_customer_types>(
            k_tpcc_customer_table_id, scan_args.at( 0 ).read_ckrs,
            customer_project_cols, scan_args.at( 0 ).predicate,
            generator_.configs_ );
    }

    q3_order_probe order_probe( cust_map_res, generator_.configs_ );

    std::vector<uint32_t> order_project_cols = {
        order_cols::o_id, order_cols::o_d_id, order_cols::o_w_id,
        order_cols::o_entry_d_c_since};
    std::unordered_map<q3_order_kv_types> o_map_res;
    if( scan_args.count( 1 ) == 1 ) {
        o_map_res = db_operators_.scan_mr<q3_order_types>(
            k_tpcc_order_table_id, scan_args.at( 1 ).read_ckrs,
            order_project_cols, scan_args.at( 1 ).predicate, order_probe );
    }

    q3_order_line_probe ol_probe( o_map_res, generator_.configs_ );

    std::vector<uint32_t> order_line_project_cols = {
        order_line_cols::ol_o_id, order_line_cols::ol_d_id,
        order_line_cols::ol_w_id, order_line_cols::ol_amount,
        order_line_cols::ol_delivery_d_c_since};
    std::unordered_map<q3_order_line_kv_types> ol_map_res;
    if( scan_args.count( 2 ) == 1 ) {
        ol_map_res = db_operators_.scan_mr<q3_order_line_types>(
            k_tpcc_order_line_table_id, scan_args.at( 2 ).read_ckrs,
            order_line_project_cols, scan_args.at( 2 ).predicate, ol_probe );
    }

    std::vector<result_tuple> results;
    for( const auto& ol_entry : ol_map_res ) {
        auto o_found = o_map_res.find( ol_entry.first );
        if( o_found == o_map_res.end() ) {
            continue;
        }
        const auto& ol = ol_entry.second;

        result_tuple rt;
        rt.table_id = k_tpcc_order_line_table_id;
        rt.row_id = ol_entry.first;

        result_cell rc;

        rc.col_id = order_line_cols::ol_o_id;
        rc.present = true;
        rc.data = int64_to_string( ol.ol_o_id );
        rc.type = data_type::INT64;
        rt.cells.emplace_back( rc );

        rc.col_id = order_line_cols::ol_w_id;
        rc.data = int64_to_string( ol.ol_w_id );
        rt.cells.emplace_back( rc );

        rc.col_id = order_line_cols::ol_d_id;
        rc.data = int64_to_string( ol.ol_d_id );
        rt.cells.emplace_back( rc );

        rc.col_id = order_line_cols::ol_amount;
        rc.data = double_to_string( ol.ol_amount );
        rc.type = data_type::DOUBLE;
        rt.cells.emplace_back( rc );

        rc.col_id = order_line_cols::ol_delivery_d_c_since;
        rc.data = uint64_to_string( o_found->second.o_entry_d.c_since );
        rc.type = data_type::UINT64;
        rt.cells.emplace_back( rc );

        results.emplace_back( rt );
    }
    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q4() {
    auto key_range = generator_.get_q4_order_range();
    auto time_range = generator_.get_q4_delivery_time();

    return do_q4_work( std::get<0>( key_range ), std::get<1>( key_range ),
                       std::get<0>( time_range ), std::get<1>( time_range ) );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q4_order_line_ckrs(
        const order& low_key, const order& high_key ) const {
    order_line low_ol;
    order_line high_ol;

    low_ol.ol_w_id = low_key.o_w_id;
    high_ol.ol_w_id = high_key.o_w_id;

    low_ol.ol_d_id = low_key.o_d_id;
    high_ol.ol_d_id = high_key.o_d_id;

    low_ol.ol_o_id = low_key.o_id;
    low_ol.ol_number = order::MIN_OL_CNT;

    high_ol.ol_o_id = high_key.o_id;
    high_ol.ol_number = generator_.configs_.max_num_order_lines_per_order_ - 1;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_line_cols::ol_o_id, order_line_cols::ol_w_id ),
        std::make_tuple<>( order_line_cols::ol_delivery_d_c_since,
                           order_line_cols::ol_delivery_d_c_since ),
    };

    return generate_order_line_ckrs( low_ol, high_ol, cols );
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q4_order_ckrs(
        const order& low_key, const order& high_key ) const {

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_cols::o_id, order_cols::o_entry_d_c_since )};
    return generate_order_ckrs( low_key, high_key, cols );
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_stock_ckrs(
        const stock& low_stock, const stock&            high_stock,
        const std::vector<std::tuple<uint32_t, uint32_t>>& cols ) const {
    std::vector<cell_key_ranges> ckrs;

    stock s = low_stock;

    for( int32_t w_id = low_stock.s_w_id; w_id <= high_stock.s_w_id; w_id++ ) {
        s.s_w_id = w_id;
        s.s_i_id = low_stock.s_i_id;
        uint64_t low_key = make_stock_key( s, generator_.configs_ );
        s.s_i_id = high_stock.s_i_id;
        uint64_t high_key = make_stock_key( s, generator_.configs_ );

        for( const auto& col : cols ) {
            cell_key_ranges ckr = create_cell_key_ranges(
                k_tpcc_stock_table_id, low_key, high_key, std::get<0>( col ),
                std::get<1>( col ) );
            ckrs.emplace_back( ckr );
        }
    }
    return ckrs;
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_nation_ckrs(
        const nation& low_nation, const nation&            high_nation,
        const std::vector<std::tuple<uint32_t, uint32_t>>& cols ) const {
    uint64_t low_key = make_nation_key( low_nation, generator_.configs_ );
    uint64_t high_key = make_nation_key( high_nation, generator_.configs_ );
    std::vector<cell_key_ranges> ckrs;
    for( const auto& col : cols ) {
        ckrs.emplace_back(
            create_cell_key_ranges( k_tpcc_nation_table_id, low_key, high_key,
                                    std::get<0>( col ), std::get<1>( col ) ) );
    }
    return ckrs;
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_item_ckrs(
        const item& low_item, const item&                  high_item,
        const std::vector<std::tuple<uint32_t, uint32_t>>& cols ) const {
    uint64_t low_key = make_item_key( low_item, generator_.configs_ );
    uint64_t high_key = make_item_key( high_item, generator_.configs_ );
    std::vector<cell_key_ranges> ckrs;
    for( const auto& col : cols ) {
        ckrs.emplace_back(
            create_cell_key_ranges( k_tpcc_item_table_id, low_key, high_key,
                                    std::get<0>( col ), std::get<1>( col ) ) );
    }
    return ckrs;
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_supplier_ckrs(
        const supplier& low_supplier, const supplier&      high_supplier,
        const std::vector<std::tuple<uint32_t, uint32_t>>& cols ) const {
    uint64_t low_key = make_supplier_key( low_supplier, generator_.configs_ );
    uint64_t high_key = make_supplier_key( high_supplier, generator_.configs_ );
    std::vector<cell_key_ranges> ckrs;
    for( const auto& col : cols ) {
        ckrs.emplace_back(
            create_cell_key_ranges( k_tpcc_supplier_table_id, low_key, high_key,
                                    std::get<0>( col ), std::get<1>( col ) ) );
    }
    return ckrs;
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_customer_ckrs(
        const customer& low_cust, const customer&          high_cust,
        const std::vector<std::tuple<uint32_t, uint32_t>>& cols ) const {

    customer c = low_cust;

    std::vector<cell_key_ranges> ckrs;
    for( int32_t w_id = low_cust.c_w_id; w_id <= high_cust.c_w_id; w_id++ ) {
        c.c_w_id = w_id;

        int32_t d_id_start = low_cust.c_d_id;
        int32_t d_id_end = high_cust.c_d_id;

        if( w_id != low_cust.c_w_id ) {
            d_id_start = 0;
        }
        if( w_id != high_cust.c_w_id ) {
            d_id_end = generator_.configs_.num_districts_per_warehouse_ - 1;
        }

        for( int32_t d_id = d_id_start; d_id <= d_id_end; d_id++ ) {
            c.c_d_id = d_id;
            c.c_id = low_cust.c_id;
            uint64_t c_low = make_customer_key( c, generator_.configs_ );
            c.c_id = high_cust.c_id;
            uint64_t c_high = make_customer_key( c, generator_.configs_ );

            for( const auto& col : cols ) {
                ckrs.emplace_back( create_cell_key_ranges(
                    k_tpcc_customer_table_id, c_low, c_high, std::get<0>( col ),
                    std::get<1>( col ) ) );
            }
        }
    }

    return ckrs;
}


tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_order_ckrs(
        const order& low_key, const order&                 high_key,
        const std::vector<std::tuple<uint32_t, uint32_t>>& cols ) const {
    std::vector<cell_key_ranges> ckrs;

    order o;

    for( int32_t w_id = low_key.o_w_id; w_id <= high_key.o_w_id; w_id++ ) {
        o.o_w_id = w_id;

        int32_t d_id_start = low_key.o_d_id;
        int32_t d_id_end = high_key.o_d_id;

        if( w_id != low_key.o_w_id ) {
            d_id_start = 0;
        }
        if( w_id != high_key.o_w_id ) {
            d_id_end = generator_.configs_.num_districts_per_warehouse_ - 1;
        }

        for( int32_t d_id = d_id_start; d_id <= d_id_end; d_id++ ) {
            o.o_d_id = d_id;
            o.o_id = 0;
            o.o_c_id = 0;

            uint64_t o_low = make_order_key( o, generator_.configs_ );

            o.o_id = generator_.configs_.expected_num_orders_per_cust_ *
                     ( generator_.configs_.num_customers_per_district_ - 1 );
            o.o_c_id = generator_.configs_.num_customers_per_district_ - 1;

            uint64_t o_high = make_order_key( o, generator_.configs_ );
            for( const auto& col : cols ) {
                ckrs.emplace_back( create_cell_key_ranges(
                    k_tpcc_order_table_id, o_low, o_high, std::get<0>( col ),
                    std::get<1>( col ) ) );
            }
        }
    }
    return ckrs;
}

tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q4_scan_args(
        const order& low_order, const order& high_order,
        uint64_t low_delivery_time, uint64_t high_delivery_time ) const {
    auto order_ckrs = generate_q4_order_ckrs( low_order, high_order );
    auto order_line_ckrs = generate_q4_order_line_ckrs( low_order, high_order );

    scan_arguments order_scan_arg;
    order_scan_arg.label = 0;
    order_scan_arg.read_ckrs = order_ckrs;

    cell_predicate c_pred;
    c_pred.table_id = k_tpcc_order_table_id;
    c_pred.col_id = order_cols::o_entry_d_c_since;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( low_delivery_time );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;

    order_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = uint64_to_string( high_delivery_time );
    c_pred.predicate = predicate_type::type::LESS_THAN;

    order_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments order_line_scan_arg;
    order_line_scan_arg.label = 1;
    order_line_scan_arg.read_ckrs = order_line_ckrs;

    cell_predicate ol_c_pred;
    ol_c_pred.table_id = k_tpcc_order_line_table_id;
    ol_c_pred.col_id = order_line_cols::ol_delivery_d_c_since;
    ol_c_pred.type = data_type::type::UINT64;
    ol_c_pred.data = uint64_to_string( low_delivery_time );
    ol_c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;

    order_line_scan_arg.predicate.and_predicates.emplace_back( ol_c_pred );

    std::vector<scan_arguments> scan_args = {order_scan_arg,
                                             order_line_scan_arg};

    return scan_args;
}

tpch_benchmark_worker_types workload_operation_outcome_enum
    tpch_benchmark_worker_templ::do_q4_work( const order& low_order,
                                             const order& high_order,
                                             uint64_t     low_delivery_time,
                                             uint64_t     high_delivery_time ) {

    auto scan_args = generate_q4_scan_args(
        low_order, high_order, low_delivery_time, high_delivery_time );

    scan_result scan_res;
    do_q4_work_by_scan( map_scan_args( scan_args ), scan_res );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q4_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    // first do the first scan
    DCHECK_GE( 2, scan_args.size() );

    // do the first scan
    std::vector<uint32_t> order_project_cols = {
        order_cols::o_id, order_cols::o_d_id, order_cols::o_w_id,
        order_cols::o_ol_cnt, order_cols::o_entry_d_c_since};
    std::unordered_map<q4_order_kv_types> map_res;
    if( scan_args.count( 0 ) == 1 ) {
        map_res = db_operators_.scan_mr<q4_order_types>(
            k_tpcc_order_table_id, scan_args.at( 0 ).read_ckrs,
            order_project_cols, scan_args.at( 0 ).predicate,
            (EmptyProbe) emptyProbeVal );
    }

    q4_order_line_probe probe( map_res, generator_.configs_ );

    std::vector<uint32_t> order_line_project_cols = {
        order_line_cols::ol_o_id, order_line_cols::ol_d_id,
        order_line_cols::ol_w_id, order_line_cols::ol_delivery_d_c_since};
    std::unordered_map<q4_order_line_kv_types> ol_map_res;
    if( scan_args.count( 1 ) == 1 ) {
        ol_map_res = db_operators_.scan_mr<q4_order_line_types>(
            k_tpcc_order_line_table_id, scan_args.at( 1 ).read_ckrs,
            order_line_project_cols, scan_args.at( 1 ).predicate, probe );
    }

    std::unordered_map<int32_t, uint64_t> res_map;
    for( const auto& order_entry : map_res ) {
        if( ol_map_res.count( order_entry.first ) == 1 ) {
            res_map[order_entry.second.o_ol_cnt] += 1;
        }
    }

    std::vector<result_tuple> results;
    for( const auto& entry : res_map ) {
        result_tuple rt;
        rt.table_id = k_tpcc_order_table_id;
        rt.row_id = entry.first;

        result_cell cell;
        cell.col_id = 0;
        cell.present = true;
        cell.data = uint64_to_string( entry.second );
        cell.type = data_type::type::UINT64;
        rt.cells.emplace_back( cell );

        results.emplace_back( rt );
    }
    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q6() {
    auto key_range = generator_.get_q1_scan_range();
    auto time_range = generator_.get_q4_delivery_time();
    auto quantity_range = generator_.get_q6_quantity_range();

    return do_q6_work( std::get<0>( key_range ), std::get<1>( key_range ),
                       std::get<0>( time_range ), std::get<1>( time_range ),
                       std::get<0>( quantity_range ),
                       std::get<1>( quantity_range ) );

}

tpch_benchmark_worker_types workload_operation_outcome_enum
    tpch_benchmark_worker_templ::do_q6_work( const order_line& low_order,
                                             const order_line& high_order,
                                             uint64_t low_delivery_time,
                                             uint64_t high_delivery_time,
                                             int64_t  low_quantity,
                                             int64_t  high_quantity ) {

    auto scan_args = generate_q6_scan_args(
        low_order, high_order, low_delivery_time, high_delivery_time,
        low_quantity, high_quantity );

    scan_result scan_res;
    do_q6_work_by_scan( map_scan_args( scan_args ), scan_res );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q6_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    // first do the first scan
    DCHECK_GE( 1, scan_args.size() );

    // do the first scan
    std::vector<uint32_t> order_line_project_cols = {
        order_line_cols::ol_quantity, order_line_cols::ol_amount,
        order_line_cols::ol_delivery_d_c_since};
    std::unordered_map<q6_kv_types> map_res;
    if ( scan_args.count(0 ) == 1) {
        map_res = db_operators_.scan_mr<q6_types>(
            k_tpcc_order_line_table_id, scan_args.at( 0 ).read_ckrs,
            order_line_project_cols, scan_args.at( 0 ).predicate,
            (EmptyProbe) emptyProbeVal );
    }

    std::vector<result_tuple> results;
    for( const auto& entry : map_res ) {
        result_tuple rt;
        rt.table_id = k_tpcc_order_line_table_id;
        rt.row_id = entry.first;

        result_cell cell;
        cell.col_id = order_line_cols::ol_amount;
        cell.present = true;
        cell.data = double_to_string( (double) entry.second );
        cell.type = data_type::type::DOUBLE;
        rt.cells.emplace_back( cell );

        results.emplace_back( rt );
    }
    scan_res.res_tuples[scan_args.at( 0 ).label];

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q6_order_line_ckrs(
        const order_line& low_order, const order_line& high_order ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {std::make_tuple<>(
        order_line_cols::ol_quantity, order_line_cols::ol_delivery_d_c_since )};
    return generate_order_line_ckrs( low_order, high_order, cols );
}
tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q6_scan_args(
        const order_line& low_order, const order_line& high_order,
        uint64_t low_delivery_time, uint64_t high_delivery_time,
        int64_t low_quantity, int64_t high_quantity ) const {

    scan_arguments scan_arg;

    scan_arg.label = 0;
    scan_arg.read_ckrs = generate_q6_order_line_ckrs( low_order, high_order );

    cell_predicate c_pred;
    c_pred.table_id = k_tpcc_order_line_table_id;
    c_pred.col_id = order_line_cols::ol_delivery_d_c_since;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( low_delivery_time );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = uint64_to_string( high_delivery_time );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.col_id = order_line_cols::ol_quantity;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_quantity );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_quantity );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    scan_arg.predicate.and_predicates.emplace_back( c_pred );

    std::vector<scan_arguments> ret = {scan_arg};

    return ret;
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q5() {
    auto ol_range = generator_.get_q1_scan_range();
    auto low_time = generator_.get_q1_delivery_time();
    auto item_range = generator_.get_q14_item_range();
    auto supplier_range = generator_.get_q5_supplier_range();
    auto nation_range = generator_.get_q8_nation_range();
    auto region_id = generator_.generate_rand_region_id();

    return do_q5_work( std::get<0>( ol_range ), std::get<1>( ol_range ),
                       std::get<0>( item_range ), std::get<1>( item_range ),
                       std::get<0>( supplier_range ),
                       std::get<1>( supplier_range ),
                       std::get<0>( nation_range ), std::get<1>( nation_range ),
                       low_time, region_id );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q5_work(
        const order_line& low_order, const order_line& high_order,
        const item& low_item, const item& high_item,
        const supplier& low_supplier, const supplier& high_supplier,
        const nation& low_nation, const nation& high_nation,
        uint64_t low_delivery_time, int32_t region_id ) {

    auto scan_args = generate_q5_scan_args(
        low_order, high_order, low_item, high_item, low_supplier, high_supplier,
        low_nation, high_nation, low_delivery_time, region_id );

    scan_result scan_res;
    do_q5_work_by_scan( map_scan_args( scan_args ), scan_res );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q5_scan_args(
        const order_line& low_order, const order_line& high_order,
        const item& low_item, const item& high_item,
        const supplier& low_supplier, const supplier& high_supplier,
        const nation& low_nation, const nation& high_nation,
        uint64_t low_delivery_time, int32_t region_id ) const {

    scan_arguments nation_scan;
    nation_scan.label = 0;
    nation_scan.read_ckrs = generate_q5_nation_ckrs( low_nation, high_nation );

    cell_predicate c_pred;
    c_pred.table_id = k_tpcc_nation_table_id;
    c_pred.col_id = nation_cols::r_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( region_id );
    c_pred.predicate = predicate_type::type::EQUALITY;
    nation_scan.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments supplier_scan;
    supplier_scan.label = 1;
    supplier_scan.read_ckrs =
        generate_q5_supplier_ckrs( low_supplier, high_supplier );

    scan_arguments stock_scan;
    stock_scan.label = 2;
    stock_scan.read_ckrs =
        generate_q5_stock_ckrs( low_order, high_order, low_item, high_item );

    c_pred.table_id = k_tpcc_stock_table_id;
    c_pred.col_id = stock_cols::s_s_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_supplier.s_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    stock_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_supplier.s_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    stock_scan.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments ol_scan;
    ol_scan.label = 3;
    ol_scan.read_ckrs = generate_q5_order_line_ckrs( low_order, high_order );

    c_pred.table_id = k_tpcc_order_line_table_id;
    c_pred.col_id = order_line_cols::ol_delivery_d_c_since;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( low_delivery_time );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    ol_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.col_id = order_line_cols::ol_i_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_item.i_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    ol_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_item.i_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    ol_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.col_id = order_line_cols::ol_supply_w_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_order.ol_w_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    ol_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_order.ol_w_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    ol_scan.predicate.and_predicates.emplace_back( c_pred );

    std::vector<scan_arguments> ret = {nation_scan, supplier_scan, stock_scan,
                                       ol_scan};

    return ret;
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q5_nation_ckrs(
        const nation& low_nation, const nation& high_nation ) const {

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( nation_cols::r_id, nation_cols::n_name )};

    return generate_nation_ckrs( low_nation, high_nation, cols );
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q5_supplier_ckrs(
        const supplier& low_supplier, const supplier& high_supplier ) const {
    cell_key_ranges ckr = create_cell_key_ranges(
        k_tpcc_supplier_table_id, low_supplier.s_id, high_supplier.s_id,
        supplier_cols::n_id, supplier_cols::n_id );
    std::vector<cell_key_ranges> ckrs = {ckr};
    return ckrs;
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q5_stock_ckrs(
        const order_line& low_order, const order_line& high_order,
        const item& low_item, const item& high_item ) const {
    std::vector<cell_key_ranges> ckrs;
    stock                        low_stock;
    stock                        high_stock;

    low_stock.s_w_id = low_order.ol_w_id;
    high_stock.s_w_id = high_order.ol_w_id;

    low_stock.s_i_id = low_item.i_id;
    high_stock.s_i_id = high_item.i_id;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( stock_cols::s_i_id, stock_cols::s_s_id )};

    return generate_stock_ckrs( low_stock, high_stock, cols );
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q5_order_line_ckrs(
        const order_line& low_order, const order_line& high_order ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {std::make_tuple<>(
        order_line_cols::ol_i_id, order_line_cols::ol_delivery_d_c_since )};
    return generate_order_line_ckrs( low_order, high_order, cols );
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q5_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    // first do the first scan
    DCHECK_GE( 4, scan_args.size() );

    // do the first scan, group by item, and supply warehouse aggregate the
    // amount
    std::vector<uint32_t> ol_project_cols = {
        order_line_cols::ol_i_id, order_line_cols::ol_supply_w_id,
        order_line_cols::ol_amount, order_line_cols::ol_delivery_d_c_since};
    std::unordered_map<q5_order_line_kv_types> ol_map_res;

    if( scan_args.count( 3 ) == 1 ) {
        ol_map_res = db_operators_.scan_mr<q5_order_line_types>(
            k_tpcc_order_line_table_id, scan_args.at( 3 ).read_ckrs,
            ol_project_cols, scan_args.at( 3 ).predicate, generator_.configs_ );
    }

    std::vector<uint32_t> nation_project_cols = {nation_cols::r_id,
                                                 nation_cols::n_name};
    std::unordered_map<q5_nation_kv_types> nat_map_res;
    if( scan_args.count( 0 ) == 1 ) {
        nat_map_res = db_operators_.scan_mr<q5_nation_types>(
            k_tpcc_nation_table_id, scan_args.at( 0 ).read_ckrs,
            nation_project_cols, scan_args.at( 0 ).predicate,
            (EmptyProbe) emptyProbeVal );
    }

    auto sup_probe = q5_supplier_probe( nat_map_res, generator_.configs_ );

    std::vector<uint32_t> supplier_project_cols = {supplier_cols::n_id};
    std::unordered_map<q5_supplier_kv_types> sup_map_res;
    if( scan_args.count( 1 ) == 1 ) {
        sup_map_res = db_operators_.scan_mr<q5_supplier_types>(
            k_tpcc_supplier_table_id, scan_args.at( 1 ).read_ckrs,
            supplier_project_cols, scan_args.at( 1 ).predicate, sup_probe );
    }

    auto stock_probe =
        q5_stock_probe( sup_map_res, ol_map_res, generator_.configs_ );

    std::vector<uint32_t> stock_project_cols = {stock_cols::s_i_id,
                                                stock_cols::s_s_id};
    // filter out by s_s_id
    std::unordered_map<q5_stock_kv_types> stock_map_res;
    if( scan_args.count( 2 ) == 1 ) {
        stock_map_res = db_operators_.scan_mr<q5_stock_types>(
            k_tpcc_stock_table_id, scan_args.at( 2 ).read_ckrs,
            stock_project_cols, scan_args.at( 2 ).predicate, stock_probe );
    }

    // now go back and sum the results
    std::unordered_map<int32_t, float> nation_sums;
    for( const auto& stock_entry : stock_map_res ) {
        auto sup_found = sup_map_res.find( stock_entry.second.s_s_id );
        if( sup_found != sup_map_res.end() ) {
            int32_t nat_id = sup_found->second;
            auto    ol_found = ol_map_res.find( stock_entry.first );
            if( ol_found != ol_map_res.end() ) {
                nation_sums[nat_id] += ol_found->second;
            }
        }
    }

    std::vector<result_tuple> results;
    for( const auto& nat_sum : nation_sums ) {
        auto nat_found = nat_map_res.find( nat_sum.first );
        if( nat_found == nat_map_res.end() ) {
            continue;
        }
        result_tuple res;
        res.table_id = k_tpcc_nation_table_id;
        res.row_id = nat_sum.first;

        result_cell rc;
        rc.col_id = 0;
        rc.present = true;
        rc.data = nat_found->second.n_name;
        rc.type = data_type::STRING;

        res.cells.emplace_back( rc );

        rc.col_id = 1;
        rc.present = true;
        rc.data = double_to_string( (double) nat_sum.second );
        rc.type = data_type::DOUBLE;

        res.cells.emplace_back( rc );

        results.emplace_back( res );
    }

    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q7() {
    auto order_range = generator_.get_q4_order_range();
    auto item_range = generator_.get_q14_item_range();
    auto supplier_range = generator_.get_q5_supplier_range();
    auto nation_pairs = generator_.get_q7_nation_pair();
    auto time_range = generator_.get_q4_delivery_time();

    return do_q7_work( std::get<0>( order_range ), std::get<1>( order_range ),
                       std::get<0>( item_range ), std::get<1>( item_range ),
                       std::get<0>( supplier_range ),
                       std::get<1>( supplier_range ),
                       std::get<0>( nation_pairs ), std::get<1>( nation_pairs ),
                       std::get<0>( time_range ), std::get<1>( time_range ) );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q7_work(
        const order& low_order, const order& high_order, const item& low_item,
        const item& high_item, const supplier& low_supplier,
        const supplier& high_supplier, int32_t nation_id_1, int32_t nation_id_2,
        uint64_t low_delivery_time, uint64_t high_delivery_time ) {

    auto scan_args = generate_q7_scan_args(
        low_order, high_order, low_item, high_item, low_supplier, high_supplier,
        nation_id_1, nation_id_2, low_delivery_time, high_delivery_time );

    scan_result scan_res;
    do_q7_work_by_scan( map_scan_args( scan_args ), scan_res );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q7_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    // supplier, stock, customer, orders, order_line,
    DCHECK_GE( 5, scan_args.size() );

    std::vector<uint32_t>    supplier_proj_cols = {supplier_cols::n_id};
    std::unordered_map<q7_supplier_kv_types> sup_map_res;
    if( scan_args.count( 0 ) == 1 ) {
        sup_map_res = db_operators_.scan_mr<q7_supplier_types>(
            k_tpcc_supplier_table_id, scan_args.at( 0 ).read_ckrs,
            supplier_proj_cols, scan_args.at( 0 ).predicate,
            (EmptyProbe) emptyProbeVal );
    }

    std::vector<uint32_t> stock_proj_cols = {stock_cols::s_s_id};
    q7_stock_probe stock_probe( sup_map_res, generator_.configs_ );
    std::unordered_map<q7_stock_kv_types> stock_map_res;
    if( scan_args.count( 1 ) == 1 ) {
        stock_map_res = db_operators_.scan_mr<q7_stock_types>(
            k_tpcc_stock_table_id, scan_args.at( 1 ).read_ckrs, stock_proj_cols,
            scan_args.at( 1 ).predicate, stock_probe );
    }

    std::vector<uint32_t> customer_proj_cols = {customer_cols::c_n_id};
    std::unordered_map<q7_customer_kv_types> cust_map_res;
    if( scan_args.count( 2 ) == 1 ) {
        cust_map_res = db_operators_.scan_mr<q7_customer_types>(
            k_tpcc_customer_table_id, scan_args.at( 2 ).read_ckrs,
            customer_proj_cols, scan_args.at( 2 ).predicate,
            (EmptyProbe) emptyProbeVal );
    }

    std::vector<uint32_t> order_proj_cols = {
        order_cols::o_id, order_cols::o_c_id, order_cols::o_d_id,
        order_cols::o_w_id};
    q7_order_probe order_probe( cust_map_res, generator_.configs_ );
    std::unordered_map<q7_order_kv_types> order_map_res;
    if( scan_args.count( 3 ) == 1 ) {
        order_map_res = db_operators_.scan_mr<q7_order_types>(
            k_tpcc_order_table_id, scan_args.at( 3 ).read_ckrs, order_proj_cols,
            scan_args.at( 3 ).predicate, order_probe );
    }

    std::vector<uint32_t> ol_proj_cols = {
        order_line_cols::ol_o_id,
        order_line_cols::ol_d_id,
        order_line_cols::ol_w_id,
        order_line_cols::ol_i_id,
        order_line_cols::ol_supply_w_id,
        order_line_cols::ol_amount,
        order_line_cols::ol_delivery_d_c_since};
    q7_order_line_probe ol_probe( order_map_res, stock_map_res,
                                  generator_.configs_ );
    std::unordered_map<q7_order_line_kv_types> ol_map_res;
    if( scan_args.count( 4 ) == 1 ) {
        ol_map_res = db_operators_.scan_mr<q7_order_line_types>(
            k_tpcc_order_line_table_id, scan_args.at( 4 ).read_ckrs,
            ol_proj_cols, scan_args.at( 4 ).predicate, ol_probe );
    }

    std::unordered_map<q7_res_key, float, q7_res_key_hasher,
                       q7_res_key_equal_functor>
        res_map;
    for( const auto& ol_entry : ol_map_res ) {
        const auto& ol_key = ol_entry.first;
        float       ol_amount = ol_entry.second;

        const auto& stock_found = stock_map_res.find( ol_key.stock_key_ );
        if( stock_found == stock_map_res.end() ) {
            continue;
        }
        int32_t     sup_id = stock_found->second;
        const auto& sup_found = sup_map_res.find( sup_id );
        if( sup_found == sup_map_res.end() ) {
            continue;
        }
        int32_t sup_nat_id = sup_found->second;

        const auto& cust_found = cust_map_res.find( ol_key.customer_key_ );
        if( cust_found == cust_map_res.end() ) {
            continue;
        }

        int32_t cust_nat_id = cust_found->second;

        q7_res_key key( sup_nat_id, cust_nat_id, ol_key.year_ );
        res_map[key] += ol_amount;
    }

    std::vector<result_tuple> results;
    q7_res_key_hasher hasher;
    for( const auto& res : res_map ) {
        const auto& key = res.first;

        result_tuple rt;
        rt.row_id = hasher( key );
        rt.table_id = k_tpcc_supplier_table_id;

        result_cell cell;
        cell.present = true;

        cell.col_id = 0;
        cell.data = int64_to_string( key.supplier_n_id_ );
        cell.type = data_type::type::INT64;
        rt.cells.emplace_back( cell );

        cell.col_id = 1;
        cell.data = int64_to_string( key.customer_n_id_ );
        cell.type = data_type::type::INT64;
        rt.cells.emplace_back( cell );

        cell.col_id = 2;
        cell.data = uint64_to_string( key.year_ );
        cell.type = data_type::type::UINT64;
        rt.cells.emplace_back( cell );

        cell.col_id = 3;
        cell.data = double_to_string( (double) res.second );
        cell.type = data_type::type::DOUBLE;
        rt.cells.emplace_back( cell );

        results.emplace_back( rt );
    }
    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}

tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q7_scan_args(
        const order& low_order, const order& high_order, const item& low_item,
        const item& high_item, const supplier& low_supplier,
        const supplier& high_supplier, int32_t nation_id_1, int32_t nation_id_2,
        uint64_t low_delivery_time, uint64_t high_delivery_time ) const {

    scan_arguments supplier_arg;
    supplier_arg.label = 0;
    supplier_arg.read_ckrs =
        generate_q7_supplier_ckrs( low_supplier, high_supplier );

    cell_predicate c_pred;

    c_pred.table_id = k_tpcc_supplier_table_id;
    c_pred.col_id = supplier_cols::n_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( nation_id_1 );
    c_pred.predicate = predicate_type::type::EQUALITY;
    supplier_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( nation_id_2 );
    c_pred.predicate = predicate_type::type::EQUALITY;
    supplier_arg.predicate.or_predicates.emplace_back( c_pred );
    supplier_arg.predicate.is_and_join_predicates = false;

    scan_arguments stock_arg;
    stock_arg.label = 1;
    stock_arg.read_ckrs =
        generate_q7_stock_ckrs( low_item, high_item, low_order, high_order );

    c_pred.table_id = k_tpcc_stock_table_id;
    c_pred.col_id = stock_cols::s_s_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_supplier.s_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    stock_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_supplier.s_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    stock_arg.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments customer_arg;
    customer_arg.label = 2;
    customer_arg.read_ckrs = generate_q7_customer_ckrs( low_order, high_order );

    c_pred.table_id = k_tpcc_customer_table_id;
    c_pred.col_id = customer_cols::c_n_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( nation_id_1 );
    c_pred.predicate = predicate_type::type::EQUALITY;
    customer_arg.predicate.and_predicates.emplace_back( c_pred );
    customer_arg.predicate.is_and_join_predicates = false;

    c_pred.data = int64_to_string( nation_id_2 );
    c_pred.predicate = predicate_type::type::EQUALITY;
    customer_arg.predicate.or_predicates.emplace_back( c_pred );

    scan_arguments order_arg;
    order_arg.label = 3;
    order_arg.read_ckrs = generate_q7_order_ckrs( low_order, high_order );

    scan_arguments order_line_arg;
    order_line_arg.label = 4;
    order_line_arg.read_ckrs =
        generate_q7_order_line_ckrs( low_order, high_order );

    c_pred.table_id = k_tpcc_order_line_table_id;
    c_pred.col_id = order_line_cols::ol_i_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_item.i_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    order_line_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_item.i_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    order_line_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.table_id = k_tpcc_order_line_table_id;
    c_pred.col_id = order_line_cols::ol_delivery_d_c_since;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( low_delivery_time );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    order_line_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = uint64_to_string( high_delivery_time );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    order_line_arg.predicate.and_predicates.emplace_back( c_pred );

    std::vector<scan_arguments> ret = {supplier_arg, stock_arg, customer_arg,
                                       order_arg, order_line_arg};
    return ret;
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q7_supplier_ckrs(
        const supplier& low_supplier, const supplier& high_supplier ) const {

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( supplier_cols::n_id, supplier_cols::n_id )};
    return generate_supplier_ckrs( low_supplier, high_supplier, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q7_stock_ckrs(
        const item& low_item, const item& high_item, const order& low_order,
        const order& high_order ) const {
    stock low_stock;
    stock high_stock;

    low_stock.s_i_id = low_item.i_id;
    high_stock.s_i_id = high_item.i_id;

    low_stock.s_w_id = low_order.o_w_id;
    high_stock.s_w_id = high_order.o_w_id;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( stock_cols::s_s_id, stock_cols::s_s_id )};

    return generate_stock_ckrs( low_stock, high_stock, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q7_customer_ckrs(
        const order& low_o, const order& high_o ) const {
    customer low_cust;
    customer high_cust;

    low_cust.c_w_id = low_o.o_w_id;
    high_cust.c_w_id = high_o.o_w_id;

    low_cust.c_d_id = low_o.o_d_id;
    high_cust.c_d_id = high_o.o_d_id;

    low_cust.c_id = low_o.o_c_id;
    high_cust.c_id = high_o.o_c_id;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( customer_cols::c_n_id, customer_cols::c_n_id )};

    return generate_customer_ckrs( low_cust, high_cust, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q7_order_ckrs(
        const order& low_order, const order& high_order ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_cols::o_id, order_cols::o_w_id )};
    return generate_order_ckrs( low_order, high_order, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q7_order_line_ckrs(
        const order& low_order, const order& high_order ) const {
    order_line low_ol;
    order_line high_ol;

    low_ol.ol_o_id = low_order.o_id;
    high_ol.ol_o_id = high_order.o_id;

    low_ol.ol_d_id = low_order.o_d_id;
    high_ol.ol_d_id = high_order.o_d_id;

    low_ol.ol_w_id = low_order.o_w_id;
    high_ol.ol_w_id = high_order.o_w_id;

    low_ol.ol_number = 0;
    high_ol.ol_number = generator_.configs_.max_num_order_lines_per_order_ - 1;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_line_cols::ol_o_id, order_line_cols::ol_w_id ),
        std::make_tuple<>( order_line_cols::ol_i_id,
                           order_line_cols::ol_supply_w_id ),
        std::make_tuple<>( order_line_cols::ol_amount,
                           order_line_cols::ol_delivery_d_c_since ),
    };
    return generate_order_line_ckrs( low_ol, high_ol, cols );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q8() {
    auto    key_range = generator_.get_q4_order_range();
    auto    time_range = generator_.get_q4_delivery_time();
    auto    item_range = generator_.get_q14_item_range();
    auto    supplier_range = generator_.get_q5_supplier_range();
    auto    nation_range = generator_.get_q8_nation_range();
    int32_t region_id = generator_.generate_rand_region_id();

    return do_q8_work( std::get<0>( key_range ), std::get<1>( key_range ),
                       std::get<0>( item_range ), std::get<1>( item_range ),
                       std::get<0>( supplier_range ),
                       std::get<1>( supplier_range ),
                       std::get<0>( nation_range ),
                       std::get<1>( nation_range ),
                       std::get<0>( time_range ),
                       std::get<1>( time_range ), region_id );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q8_work(
        const order& low_order, const order& high_order, const item& low_item,
        const item& high_item, const supplier& low_supplier,
        const supplier& high_supplier, const nation& low_nation,
        const nation& high_nation, uint64_t low_delivery_time,
        uint64_t high_delivery_time, int32_t region_id ) {

    auto scan_args = generate_q8_scan_args(
        low_order, high_order, low_item, high_item, low_supplier, high_supplier,
        low_nation, high_nation, low_delivery_time, high_delivery_time,
        region_id );

    scan_result scan_res;
    do_q8_work_by_scan( map_scan_args( scan_args ), scan_res );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q8_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    // nation, supplier, stock, orders, order_line
    DCHECK_GE( 5, scan_args.size() );

    std::vector<uint32_t> nation_proj_cols = {nation_cols::r_id};
    std::unordered_map<q8_nation_kv_types> nat_map_res;
    if ( scan_args.count(0) == 1) {
        nat_map_res = db_operators_.scan_mr<q8_nation_types>(
            k_tpcc_nation_table_id, scan_args.at( 0 ).read_ckrs,
            nation_proj_cols, scan_args.at( 0 ).predicate,
            (EmptyProbe) emptyProbeVal );
    }

    q8_supplier_probe        supplier_probe( nat_map_res, generator_.configs_ );
    std::vector<uint32_t>    supplier_proj_cols = {supplier_cols::n_id};
    std::unordered_map<q8_supplier_kv_types> sup_map_res;
    if( scan_args.count( 1 ) == 1 ) {
        sup_map_res = db_operators_.scan_mr<q8_supplier_types>(
            k_tpcc_supplier_table_id, scan_args.at( 1 ).read_ckrs,
            supplier_proj_cols, scan_args.at( 1 ).predicate, supplier_probe );
    }

    q8_stock_probe           stock_probe( sup_map_res, generator_.configs_ );
    std::vector<uint32_t>    stock_proj_cols = {
        stock_cols::s_i_id, stock_cols::s_w_id, stock_cols::s_s_id};
    std::unordered_map<q8_stock_kv_types> stock_map_res;
    if( scan_args.count( 2 ) == 1 ) {
        stock_map_res = db_operators_.scan_mr<q8_stock_types>(
            k_tpcc_stock_table_id, scan_args.at( 2 ).read_ckrs, stock_proj_cols,
            scan_args.at( 2 ).predicate, stock_probe );
    }

    std::vector<uint32_t> order_proj_cols = {
        order_cols::o_id, order_cols::o_d_id, order_cols::o_w_id,
        order_cols::o_entry_d_c_since};
    std::unordered_map<q8_order_kv_types> order_map_res;
    if( scan_args.count( 3 ) == 1 ) {
        order_map_res = db_operators_.scan_mr<q8_order_types>(
            k_tpcc_order_table_id, scan_args.at( 3 ).read_ckrs, order_proj_cols,
            scan_args.at( 3 ).predicate, (EmptyProbe) emptyProbeVal );
    }

    q8_order_line_probe ol_probe( stock_map_res, order_map_res, generator_.configs_ );
    std::vector<uint32_t> ol_proj_cols = {
        order_line_cols::ol_o_id, order_line_cols::ol_d_id,
        order_line_cols::ol_w_id, order_line_cols::ol_i_id,
        order_line_cols::ol_amount};
    std::unordered_map<q8_order_line_kv_types> ol_map_res;
    if( scan_args.count( 4 ) == 1 ) {
        ol_map_res = db_operators_.scan_mr<q8_order_line_types>(
            k_tpcc_order_line_table_id, scan_args.at( 4 ).read_ckrs, ol_proj_cols,
            scan_args.at( 4 ).predicate, ol_probe);
    }

    std::unordered_map<int32_t /* nation */,
                       std::unordered_map<uint64_t /* year */, float>>
        map_res;
    for( const auto& ol_entry : ol_map_res ) {
        uint64_t stock_key = ol_entry.first.stock_key_;
        uint64_t year = ol_entry.first.year_;
        float    ol_amount = ol_entry.second;

        auto stock_found = stock_map_res.find( stock_key );
        if( stock_found == stock_map_res.end() ) {
            continue;
        }
        int32_t sup_id = stock_found->second;

        auto sup_found = sup_map_res.find( sup_id );
        if( sup_found == sup_map_res.end() ) {
            continue;
        }
        int32_t nat_id = sup_found->second;

        auto res_found = map_res.find( nat_id );
        if( res_found == map_res.end() ) {
            std::unordered_map<uint64_t /* year */, float> nat_res;
            nat_res.emplace( year, ol_amount );
            map_res.emplace( nat_id, nat_res );
        } else {
            auto nat_res_found = res_found->second.find( year );
            if( nat_res_found == res_found->second.end() ) {
                // insert
                res_found->second.emplace( year, ol_amount );
            } else {
                // add there
                nat_res_found->second += ol_amount;
            }
        }
    }

    std::vector<result_tuple> results;
    for( const auto& res_entry : map_res ) {
        int32_t nat_id = res_entry.first;
        for( const auto& year_entry : res_entry.second ) {
            uint64_t year = year_entry.first;
            float    amount = year_entry.second;

            result_tuple rt;
            rt.table_id = k_tpcc_customer_table_id;
            rt.row_id = ( year * nation::NUM_NATIONS ) + nat_id;

            result_cell cell;
            cell.present = true;

            cell.col_id = 0;
            cell.data = uint64_to_string( year );
            cell.type = data_type::type::UINT64;
            rt.cells.emplace_back( cell );

            cell.col_id = 1;
            cell.data = int64_to_string( nat_id );
            cell.type = data_type::type::INT64;
            rt.cells.emplace_back( cell );

            cell.col_id = 2;
            cell.data = double_to_string( (double) amount );
            cell.type = data_type::type::DOUBLE;
            rt.cells.emplace_back( cell );

            results.emplace_back( rt );
        }
    }
    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}

tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q8_scan_args(
        const order& low_order, const order& high_order, const item& low_item,
        const item& high_item, const supplier& low_supplier,
        const supplier& high_supplier, const nation& low_nation,
        const nation& high_nation, uint64_t low_delivery_time,
        uint64_t high_delivery_time, int32_t region_id ) const {

    scan_arguments nation_arg;
    nation_arg.label = 0;
    nation_arg.read_ckrs = generate_q8_nation_ckrs( low_nation, high_nation );

    cell_predicate c_pred;
    c_pred.table_id = k_tpcc_nation_table_id;
    c_pred.col_id = nation_cols::r_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( region_id );
    c_pred.predicate = predicate_type::type::EQUALITY;
    nation_arg.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments supplier_arg;
    supplier_arg.label = 1;
    supplier_arg.read_ckrs =
        generate_q8_supplier_ckrs( low_supplier, high_supplier );

    c_pred.table_id = k_tpcc_supplier_table_id;
    c_pred.col_id = supplier_cols::n_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_nation.n_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    supplier_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_nation.n_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    supplier_arg.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments stock_arg;
    stock_arg.label = 2;
    stock_arg.read_ckrs =
        generate_q8_stock_ckrs( low_item, high_item, low_order, high_order );

    c_pred.table_id = k_tpcc_stock_table_id;
    c_pred.col_id = stock_cols::s_s_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_supplier.s_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    stock_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_supplier.s_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    stock_arg.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments order_arg;
    order_arg.label = 3;
    order_arg.read_ckrs = generate_q8_order_ckrs( low_order, high_order );

    c_pred.table_id = k_tpcc_order_table_id;
    c_pred.col_id = order_cols::o_entry_d_c_since;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( low_delivery_time );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    order_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = uint64_to_string( high_delivery_time );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    order_arg.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments order_line_arg;
    order_line_arg.label = 4;
    order_line_arg.read_ckrs =
        generate_q8_order_line_ckrs( low_order, high_order );

    c_pred.table_id = k_tpcc_order_line_table_id;
    c_pred.col_id = order_line_cols::ol_i_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_item.i_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    order_line_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_item.i_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    order_line_arg.predicate.and_predicates.emplace_back( c_pred );

    std::vector<scan_arguments> ret = {nation_arg, supplier_arg, stock_arg,
                                       order_arg, order_line_arg};
    return ret;
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q8_nation_ckrs(
        const nation& low_nation, const nation& high_nation ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( nation_cols::r_id, nation_cols::r_id )};

    return generate_nation_ckrs( low_nation, high_nation, cols );
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q8_supplier_ckrs(
        const supplier& low_supplier, const supplier& high_supplier ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( supplier_cols::n_id, supplier_cols::n_id )};

    return generate_supplier_ckrs( low_supplier, high_supplier, cols );
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q8_stock_ckrs(
        const item& low_item, const item& high_item, const order& low_order,
        const order& high_order ) const {
    stock low_stock;
    stock high_stock;

    low_stock.s_i_id = low_item.i_id;
    high_stock.s_i_id = high_item.i_id;

    low_stock.s_w_id = low_order.o_w_id;
    high_stock.s_w_id = high_order.o_w_id;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( stock_cols::s_i_id, stock_cols::s_s_id )};

    return generate_stock_ckrs( low_stock, high_stock, cols );
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q8_order_ckrs(
        const order& low_order, const order& high_order ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_cols::o_id, order_cols::o_w_id ),
        std::make_tuple<>( order_cols::o_entry_d_c_since,
                           order_cols::o_entry_d_c_since ),
    };

    return generate_order_ckrs( low_order, high_order, cols );
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q8_order_line_ckrs(
        const order& low_order, const order& high_order ) const {
    order_line low_ol;
    order_line high_ol;

    low_ol.ol_o_id = low_order.o_id;
    high_ol.ol_o_id = high_order.o_id;

    low_ol.ol_d_id = low_order.o_d_id;
    high_ol.ol_d_id = high_order.o_d_id;

    low_ol.ol_w_id = low_order.o_w_id;
    high_ol.ol_w_id = high_order.o_w_id;

    low_ol.ol_number = 0;
    high_ol.ol_number = generator_.configs_.max_num_order_lines_per_order_ - 1;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_line_cols::ol_o_id, order_line_cols::ol_w_id ),
        std::make_tuple<>( order_line_cols::ol_i_id, order_line_cols::ol_i_id ),
        std::make_tuple<>( order_line_cols::ol_amount,
                           order_line_cols::ol_amount )};

    return generate_order_line_ckrs( low_ol, high_ol, cols );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q10() {
    auto    key_range = generator_.get_q4_order_range();
    auto low_time = generator_.get_q1_delivery_time();
    int32_t nation_id = generator_.generate_rand_nation_id();

    return do_q10_work( std::get<0>( key_range ), std::get<1>( key_range ),
                        low_time, nation_id );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
    tpch_benchmark_worker_templ::do_q10_work( const order& low_order,
                                              const order& high_order,
                                              uint64_t     low_delivery_time,
                                              int32_t      nation_id ) {

    auto scan_args = generate_q10_scan_args( low_order, high_order,
                                             low_delivery_time, nation_id );

    scan_result scan_res;
    do_q10_work_by_scan( map_scan_args( scan_args ), scan_res );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q10_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    // nation, customer, order, order_line
    DCHECK_GE( 4, scan_args.size() );

    std::vector<uint32_t> nat_project_cols = {nation_cols::n_name};
    std::unordered_map<q10_nation_kv_types> nat_map_res;

    if( scan_args.count( 0 ) == 1 ) {
        nat_map_res = db_operators_.scan_mr<q10_nation_types>(
            k_tpcc_nation_table_id, scan_args.at( 0 ).read_ckrs,
            nat_project_cols, scan_args.at( 0 ).predicate,
            (EmptyProbe) emptyProbeVal );
    }

    std::vector<uint32_t> cust_project_cols = {
        customer_cols::c_id,   customer_cols::c_d_id,
        customer_cols::c_w_id, customer_cols::c_n_id,
        customer_cols::c_last, customer_cols::c_address_a_city,
        customer_cols::c_phone};
    std::unordered_map<q10_customer_kv_types> cust_map_res;

    if( scan_args.count( 1 ) == 1 ) {
        cust_map_res = db_operators_.scan_mr<q10_customer_types>(
            k_tpcc_customer_table_id, scan_args.at( 1 ).read_ckrs,
            cust_project_cols, scan_args.at( 1 ).predicate,
            (EmptyProbe) emptyProbeVal );
    }

    std::vector<uint32_t> order_project_cols = {
        order_cols::o_id, order_cols::o_c_id, order_cols::o_d_id,
        order_cols::o_w_id, order_cols::o_entry_d_c_since};
    std::unordered_map<q10_order_kv_types> order_map_res;
    q10_order_probe order_probe( cust_map_res, generator_.configs_ );

    if( scan_args.count( 2 ) == 1 ) {
        order_map_res = db_operators_.scan_mr<q10_order_types>(
            k_tpcc_order_table_id, scan_args.at( 2 ).read_ckrs,
            order_project_cols, scan_args.at( 2 ).predicate, order_probe );
    }

    std::vector<uint32_t> order_line_project_cols = {
        order_line_cols::ol_o_id, order_line_cols::ol_d_id,
        order_line_cols::ol_w_id, order_line_cols::ol_amount,
        order_line_cols::ol_delivery_d_c_since};
    std::unordered_map<q10_order_line_kv_types> order_line_map_res;
    q10_order_line_probe order_line_probe( order_map_res, generator_.configs_ );

    if( scan_args.count( 3 ) == 1 ) {
        order_line_map_res = db_operators_.scan_mr<q10_order_line_types>(
            k_tpcc_order_line_table_id, scan_args.at( 3 ).read_ckrs,
            order_line_project_cols, scan_args.at( 3 ).predicate,
            order_line_probe );
    }

    std::unordered_map<uint64_t /* cust key*/,
                       std::tuple<customer, std::string /* n_name */, float>>
        res_map;
    for( const auto ol_res : order_line_map_res ) {
        uint64_t order_key = ol_res.first;
        auto     order_found = order_map_res.find( order_key );
        if( order_found == order_map_res.end() ) {
            continue;
        }
        auto     order_entry = order_found->second;
        uint64_t cust_id = std::get<0>( order_entry );
        auto     customer_found = cust_map_res.find( cust_id );
        if( customer_found == cust_map_res.end() ) {
            continue;
        }
        customer cust = customer_found->second;
        int32_t  n_id = cust.c_n_id;

        auto nation_found = nat_map_res.find( n_id );
        if( nation_found == nat_map_res.end() ) {
            continue;
        }
        nation nat = nation_found->second;

        auto res_found = res_map.find( cust_id );
        if( res_found == res_map.end() ) {
            res_map.emplace(
                cust_id, std::make_tuple<>( cust, nat.n_name, ol_res.second ) );
        } else {
            res_found->second = std::make_tuple<>(
                cust, nat.n_name,
                std::get<2>( res_found->second ) + ol_res.second );
        }
    }

    std::vector<result_tuple> results;
    for( const auto& entry : res_map ) {
        result_tuple rt;
        rt.table_id = k_tpcc_customer_table_id;
        rt.row_id = entry.first;

        result_cell cell;
        cell.present = true;

        cell.col_id = 0;
        cell.data = int64_to_string( std::get<0>( entry.second ).c_id );
        cell.type = data_type::type::INT64;
        rt.cells.emplace_back( cell );

        cell.col_id = 1;
        cell.data = string_to_string( std::get<0>( entry.second ).c_last );
        cell.type = data_type::type::STRING;
        rt.cells.emplace_back( cell );

        cell.col_id = 2;
        cell.data = double_to_string( (double) std::get<2>( entry.second ) );
        cell.type = data_type::type::DOUBLE;
        rt.cells.emplace_back( cell );

        cell.col_id = 3;
        cell.data =
            string_to_string( std::get<0>( entry.second ).c_address.a_city );
        cell.type = data_type::type::STRING;
        rt.cells.emplace_back( cell );

        cell.col_id = 4;
        cell.data = string_to_string( std::get<0>( entry.second ).c_phone );
        cell.type = data_type::type::STRING;
        rt.cells.emplace_back( cell );

        cell.col_id = 5;
        cell.data = string_to_string( std::get<1>( entry.second ) );
        cell.type = data_type::type::STRING;
        rt.cells.emplace_back( cell );

        results.emplace_back( rt );
    }
    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}

tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q10_scan_args(
        const order& low_order, const order& high_order,
        uint64_t low_delivery_time, int32_t nation_id ) const {

    scan_arguments nation_scan;
    nation_scan.label = 0;
    nation_scan.read_ckrs = generate_q10_nation_ckrs( nation_id );

    scan_arguments customer_scan;
    customer_scan.label = 1;
    customer_scan.read_ckrs =
        generate_q10_customer_ckrs( low_order, high_order );

    cell_predicate c_pred;
    c_pred.table_id = k_tpcc_customer_table_id;
    c_pred.col_id = customer_cols::c_n_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( nation_id );
    c_pred.predicate = predicate_type::type::EQUALITY;
    customer_scan.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments order_scan;
    order_scan.label = 2;
    order_scan.read_ckrs = generate_q10_order_ckrs( low_order, high_order );

    c_pred.table_id = k_tpcc_order_table_id;
    c_pred.col_id = order_cols::o_entry_d_c_since;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( low_delivery_time );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    order_scan.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments order_line_scan;
    order_line_scan.label = 3;
    order_line_scan.read_ckrs =
        generate_q10_order_line_ckrs( low_order, high_order );

    c_pred.table_id = k_tpcc_order_line_table_id;
    c_pred.col_id = order_line_cols::ol_delivery_d_c_since;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( low_delivery_time );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    order_line_scan.predicate.and_predicates.emplace_back( c_pred );

    std::vector<scan_arguments> ret = {nation_scan, customer_scan, order_scan,
                                       order_line_scan};

    return ret;
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q10_nation_ckrs(
        int32_t nation_id ) const {
    nation low_nation;
    nation high_nation;

    low_nation.n_id = nation_id;
    high_nation.n_id = nation_id;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( nation_cols::n_name, nation_cols::n_name )};

    return generate_nation_ckrs( low_nation, high_nation, cols );
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q10_customer_ckrs(
        const order& low_o, const order& high_o ) const {
    customer low_cust;
    customer high_cust;

    low_cust.c_w_id = low_o.o_w_id;
    high_cust.c_w_id = high_o.o_w_id;

    low_cust.c_d_id = low_o.o_d_id;
    high_cust.c_d_id = high_o.o_d_id;

    low_cust.c_id = low_o.o_c_id;
    high_cust.c_id = high_o.o_c_id;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( customer_cols::c_id, customer_cols::c_n_id ),
        std::make_tuple<>( customer_cols::c_last, customer_cols::c_phone )};

    return generate_customer_ckrs( low_cust, high_cust, cols );
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q10_order_ckrs(
        const order& low_o, const order& high_o ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_cols::o_id, order_cols::o_w_id ),
        std::make_tuple<>( order_cols::o_entry_d_c_since,
                           order_cols::o_entry_d_c_since )};
    return generate_order_ckrs( low_o, high_o, cols );
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q10_order_line_ckrs(
        const order& low_o, const order& high_o ) const {
    order_line low_ol;
    order_line high_ol;

    low_ol.ol_o_id = low_o.o_id;
    high_ol.ol_o_id = high_o.o_id;

    low_ol.ol_d_id = low_o.o_d_id;
    high_ol.ol_d_id = high_o.o_d_id;

    low_ol.ol_w_id = low_o.o_w_id;
    high_ol.ol_w_id = high_o.o_w_id;

    low_ol.ol_number = 0;
    high_ol.ol_number = generator_.configs_.max_num_order_lines_per_order_ - 1;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_line_cols::ol_o_id, order_line_cols::ol_w_id ),
        std::make_tuple<>( order_line_cols::ol_amount,
                           order_line_cols::ol_delivery_d_c_since )};
    return generate_order_line_ckrs( low_ol, high_ol, cols );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q11() {
    auto    stock_range = generator_.get_q11_stock_range();
    auto    supplier_range = generator_.get_q5_supplier_range();
    int32_t nation_id = generator_.generate_rand_nation_id();
    float multi = 0.005;

    return do_q11_work( std::get<0>( stock_range ), std::get<1>( stock_range ),
                        std::get<0>( supplier_range ),
                        std::get<1>( supplier_range ), nation_id, multi );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
    tpch_benchmark_worker_templ::do_q11_work( const stock&    low_stock,
                                              const stock&    high_stock,
                                              const supplier& low_supplier,
                                              const supplier& high_supplier,
                                              int32_t         nation_id,
                                              float           order_sum_mult ) {

    auto scan_args = generate_q11_scan_args(
        low_stock, high_stock, low_supplier, high_supplier, nation_id );

    scan_result scan_res;
    do_q11_work_by_scan( map_scan_args( scan_args ), scan_res,
                         order_sum_mult );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q11_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res, float order_sum_mult ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    // supplier, stock
    DCHECK_GE( 2, scan_args.size() );

    std::vector<uint32_t> sup_proj_cols = {supplier_cols::n_id};
    std::unordered_map<q11_supplier_kv_types> sup_map_res;
    if( scan_args.count( 0 ) == 1 ) {
        sup_map_res = db_operators_.scan_mr<q11_supplier_types>(
            k_tpcc_supplier_table_id, scan_args.at( 0 ).read_ckrs,
            sup_proj_cols, scan_args.at( 0 ).predicate, emptyProbeVal );
    }

    std::vector<uint32_t> stock_proj_cols = {
        stock_cols::s_i_id, stock_cols::s_s_id, stock_cols::s_order_cnt};
    q11_stock_probe stock_probe( sup_map_res, generator_.configs_ );
    std::unordered_map<q11_stock_kv_types> stock_map_res;
    if( scan_args.count( 1 ) == 1 ) {
        stock_map_res = db_operators_.scan_mr<q11_stock_types>(
            k_tpcc_stock_table_id, scan_args.at( 1 ).read_ckrs, stock_proj_cols,
            scan_args.at( 1 ).predicate, stock_probe );
    }

    int64_t tot_ol_amount = 0;
    for( const auto& stock_entry : stock_map_res ) {
        tot_ol_amount += stock_entry.second;
    }

    float ol_check = ( (float) tot_ol_amount ) * order_sum_mult;

    std::vector<result_tuple> results;
    for( const auto& stock_entry : stock_map_res ) {
        int32_t  i_id = stock_entry.first;
        int64_t ol_amount = stock_entry.second;

        if( ( (float) ol_amount ) <= ol_check ) {
            continue;
        }

        result_tuple rt;
        rt.table_id = k_tpcc_stock_table_id;
        rt.row_id = i_id;

        result_cell cell;
        cell.present = true;

        cell.col_id = 0;
        cell.data = int64_to_string( i_id );
        cell.type = data_type::type::INT64;
        rt.cells.emplace_back( cell );

        cell.col_id = 1;
        cell.data = double_to_string( ol_amount );
        cell.type = data_type::type::INT64;
        rt.cells.emplace_back( cell );

        results.emplace_back( rt );
    }
    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}

tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q11_scan_args(
        const stock& low_stock, const stock& high_stock,
        const supplier& low_supplier, const supplier& high_supplier,
        int32_t nation_id ) const {

    scan_arguments supplier_scan;
    supplier_scan.label = 0;
    supplier_scan.read_ckrs =
        generate_q11_supplier_ckrs( low_supplier, high_supplier );

    cell_predicate c_pred;
    c_pred.table_id = k_tpcc_supplier_table_id;
    c_pred.col_id = supplier_cols::n_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( nation_id );
    c_pred.predicate = predicate_type::type::EQUALITY;
    supplier_scan.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments stock_scan;
    stock_scan.label = 1;
    stock_scan.read_ckrs = generate_q11_stock_ckrs( low_stock, high_stock );

    c_pred.table_id = k_tpcc_stock_table_id;
    c_pred.col_id = stock_cols::s_s_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_supplier.s_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    stock_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_supplier.s_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    stock_scan.predicate.and_predicates.emplace_back( c_pred );

    std::vector<scan_arguments> ret = {supplier_scan, stock_scan};

    return ret;
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q11_stock_ckrs(
        const stock& low_stock, const stock& high_stock ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( stock_cols::s_i_id, stock_cols::s_i_id ),
        std::make_tuple<>( stock_cols::s_s_id, stock_cols::s_s_id ),
        std::make_tuple<>( stock_cols::s_order_cnt, stock_cols::s_order_cnt )};
    return generate_stock_ckrs( low_stock, high_stock, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q11_supplier_ckrs(
        const supplier& low_supplier, const supplier& high_supplier ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( supplier_cols::n_id, supplier_cols::n_id )};
    return generate_supplier_ckrs( low_supplier, high_supplier, cols );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q12() {
    auto    key_range = generator_.get_q4_order_range();
    auto low_time = generator_.get_q1_delivery_time();

    return do_q12_work( std::get<0>( key_range ), std::get<1>( key_range ),
                        low_time );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
    tpch_benchmark_worker_templ::do_q12_work( const order& low_order,
                                              const order& high_order,
                                              uint64_t     low_delivery_time ) {

    auto scan_args =
        generate_q12_scan_args( low_order, high_order, low_delivery_time );

    scan_result scan_res;
    do_q12_work_by_scan( map_scan_args( scan_args ), scan_res );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q12_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    // nation, customer, order, order_line
    DCHECK_GE( 2, scan_args.size() );

    std::vector<uint32_t> order_project_cols = {order_cols::o_id,
                                                order_cols::o_c_id,
                                                order_cols::o_d_id,
                                                order_cols::o_w_id,
                                                order_cols::o_carrier_id,
                                                order_cols::o_ol_cnt,
                                                order_cols::o_entry_d_c_since};
    std::unordered_map<q12_order_kv_types> order_map_res;

    if( scan_args.count( 0 ) == 1 ) {
        order_map_res = db_operators_.scan_mr<q12_order_types>(
            k_tpcc_order_table_id, scan_args.at( 0 ).read_ckrs,
            order_project_cols, scan_args.at( 0 ).predicate,
            generator_.configs_ );
    }

    std::vector<uint32_t> order_line_project_cols = {
        order_line_cols::ol_o_id, order_line_cols::ol_d_id,
        order_line_cols::ol_w_id, order_line_cols::ol_delivery_d_c_since};
    std::unordered_map<q12_order_line_kv_types> order_line_map_res;
    q12_order_line_probe order_line_probe( order_map_res, generator_.configs_ );

    if( scan_args.count( 1 ) == 1 ) {
        order_line_map_res = db_operators_.scan_mr<q12_order_line_types>(
            k_tpcc_order_line_table_id, scan_args.at( 1 ).read_ckrs,
            order_line_project_cols, scan_args.at( 1 ).predicate,
            order_line_probe );
    }

    std::unordered_map<int32_t /* o_ol_cnt */, std::tuple<uint64_t, uint64_t>>
        res_map;
    for( const auto ol_res : order_line_map_res ) {
        uint64_t order_key = ol_res.first;
        auto     order_found = order_map_res.find( order_key );
        if( order_found == order_map_res.end() ) {
            continue;
        }
        auto o = order_found->second;

        uint64_t low_cnt = 0;
        uint64_t high_cnt = 0;
        if( o.o_carrier_id <= 2 ) {
            low_cnt = 1;
        } else {
            high_cnt = 1;
        }
        auto res_found = res_map.find( o.o_ol_cnt );
        if( res_found == res_map.end() ) {
            res_map.emplace( o.o_ol_cnt,
                             std::make_tuple<>( low_cnt, high_cnt ) );
        } else {
            low_cnt += std::get<0>( res_found->second );
            high_cnt += std::get<1>( res_found->second );

            res_found->second = std::make_tuple<>( low_cnt, high_cnt );
        }
    }

    std::vector<result_tuple> results;
    for( const auto& entry : res_map ) {
        result_tuple rt;
        rt.table_id = k_tpcc_order_table_id;
        rt.row_id = entry.first;

        result_cell cell;
        cell.present = true;

        cell.col_id = 0;
        cell.data = int64_to_string( entry.first );
        cell.type = data_type::type::INT64;
        rt.cells.emplace_back( cell );

        cell.col_id = 1;
        cell.data = uint64_to_string( std::get<0>( entry.second ) );
        cell.type = data_type::type::UINT64;
        rt.cells.emplace_back( cell );

        cell.col_id = 2;
        cell.data = uint64_to_string( std::get<1>( entry.second ) );
        cell.type = data_type::type::UINT64;
        rt.cells.emplace_back( cell );

        results.emplace_back( rt );
    }
    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}

tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q12_scan_args(
        const order& low_order, const order& high_order,
        uint64_t low_delivery_time ) const {

    scan_arguments order_scan;
    order_scan.label = 0;
    order_scan.read_ckrs = generate_q12_order_ckrs( low_order, high_order );

    cell_predicate c_pred;
    c_pred.table_id = k_tpcc_order_table_id;
    c_pred.col_id = order_cols::o_entry_d_c_since;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( low_delivery_time );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    order_scan.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments order_line_scan;
    order_line_scan.label = 1;
    order_line_scan.read_ckrs =
        generate_q12_order_line_ckrs( low_order, high_order );

    c_pred.table_id = k_tpcc_order_line_table_id;
    c_pred.col_id = order_line_cols::ol_delivery_d_c_since;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( low_delivery_time );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    order_line_scan.predicate.and_predicates.emplace_back( c_pred );

    std::vector<scan_arguments> ret = {order_scan, order_line_scan};

    return ret;
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q12_order_line_ckrs(
        const order& low_order, const order& high_order ) const {
    order_line low_ol;
    order_line high_ol;

    low_ol.ol_o_id = low_order.o_id;
    high_ol.ol_o_id = high_order.o_id;

    low_ol.ol_d_id = low_order.o_d_id;
    high_ol.ol_d_id = high_order.o_d_id;

    low_ol.ol_w_id = low_order.o_w_id;
    high_ol.ol_w_id = high_order.o_w_id;

    low_ol.ol_number = 0;
    high_ol.ol_number = generator_.configs_.max_num_order_lines_per_order_ - 1;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_line_cols::ol_o_id, order_line_cols::ol_w_id ),
        std::make_tuple<>( order_line_cols::ol_delivery_d_c_since,
                           order_line_cols::ol_delivery_d_c_since )};

    return generate_order_line_ckrs( low_ol, high_ol, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q12_order_ckrs(
        const order& low_order, const order& high_order ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_cols::o_id, order_cols::o_entry_d_c_since )};

    return generate_order_ckrs( low_order, high_order, cols );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q13() {
    auto key_range = generator_.get_q4_order_range();
    int32_t carrier_id = 8;

    return do_q13_work( std::get<0>( key_range ), std::get<1>( key_range ),
                        carrier_id );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
    tpch_benchmark_worker_templ::do_q13_work( const order& low_order,
                                              const order& high_order,
                                              int32_t o_carrier_threshold ) {

    auto scan_args =
        generate_q13_scan_args( low_order, high_order, o_carrier_threshold );

    scan_result scan_res;
    do_q13_work_by_scan( map_scan_args( scan_args ), scan_res );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q13_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    // first do the first scan
    DCHECK_GE( 2, scan_args.size() );

    std::unordered_map<q13_order_kv_types> order_map_res;
    std::vector<uint32_t>                  order_proj_cols = {
        order_cols::o_c_id, order_cols::o_d_id, order_cols::o_w_id,
        order_cols::o_carrier_id};
    if( scan_args.count( 0 ) == 1 ) {
        order_map_res = db_operators_.scan_mr<q13_order_types>(
            k_tpcc_order_table_id, scan_args.at( 0 ).read_ckrs, order_proj_cols,
            scan_args.at( 0 ).predicate, generator_.configs_ );
    }

    q13_customer_probe    cust_probe( order_map_res, generator_.configs_ );
    std::vector<uint32_t> cust_proj_cols = {
        customer_cols::c_id, customer_cols::c_d_id, customer_cols::c_w_id};
    std::unordered_map<q13_customer_kv_types> cust_map_res;
    if( scan_args.count( 1 ) == 1 ) {
        cust_map_res = db_operators_.scan_mr<q13_customer_types>(
            k_tpcc_customer_table_id, scan_args.at( 1 ).read_ckrs,
            cust_proj_cols, scan_args.at( 1 ).predicate, cust_probe );
    }

    std::unordered_map<uint64_t, uint64_t> map_res;
    for( const auto& cust_entry : cust_map_res ) {
        map_res[cust_entry.second] += 1;
    }

    std::vector<result_tuple> results;
    for( const auto& entry : map_res ) {
        result_tuple rt;

        rt.table_id = k_tpcc_customer_table_id;
        rt.row_id = entry.first;
        result_cell cell;

        cell.col_id = 0;
        cell.present = true;
        cell.data = uint64_to_string( entry.second );
        cell.type = data_type::type::UINT64;
        rt.cells.emplace_back( cell );
        results.emplace_back( rt );
    }
    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q13_order_ckrs(
        const order& low_order, const order& high_order ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_cols::o_c_id, order_cols::o_carrier_id )};
    return generate_order_ckrs( low_order, high_order, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q13_customer_ckrs(
        const order& low_order, const order& high_order ) const {
    customer low_cust;
    customer high_cust;

    low_cust.c_id = low_order.o_c_id;
    high_cust.c_id = high_order.o_c_id;

    low_cust.c_w_id = low_order.o_w_id;
    high_cust.c_w_id = high_order.o_w_id;

    low_cust.c_d_id = low_order.o_d_id;
    high_cust.c_d_id = high_order.o_d_id;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( customer_cols::c_id, customer_cols::c_w_id )};

    return generate_customer_ckrs( low_cust, high_cust, cols );
}

tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q13_scan_args(
        const order& low_order, const order& high_order,
        int32_t o_carrier_threshold ) const {

    scan_arguments order_scan;
    order_scan.label = 0;
    order_scan.read_ckrs = generate_q13_order_ckrs( low_order, high_order );

    cell_predicate c_pred;
    c_pred.table_id = k_tpcc_order_table_id;
    c_pred.col_id = order_cols::o_carrier_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( o_carrier_threshold );
    c_pred.predicate = predicate_type::type::GREATER_THAN;
    order_scan.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments cust_scan;
    cust_scan.label = 1;
    cust_scan.read_ckrs = generate_q13_customer_ckrs( low_order, high_order );

    std::vector<scan_arguments> ret = {order_scan, cust_scan};

    return ret;
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q14() {
    auto key_range = generator_.get_q1_scan_range();
    auto time_range = generator_.get_q4_delivery_time();
    auto item_range = generator_.get_q14_item_range();

    return do_q14_work( std::get<0>( key_range ), std::get<1>( key_range ),
                        std::get<0>( item_range ), std::get<1>( item_range ),
                        std::get<0>( time_range ), std::get<1>( time_range ) );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
    tpch_benchmark_worker_templ::do_q14_work( const order_line& low_order,
                                              const order_line& high_order,
                                              const item&       low_item,
                                              const item&       high_item,
                                              uint64_t low_delivery_time,
                                              uint64_t high_delivery_time ) {

    auto scan_args =
        generate_q14_scan_args( low_order, high_order, low_item, high_item,
                                low_delivery_time, high_delivery_time );

    scan_result scan_res;
    do_q14_work_by_scan( map_scan_args( scan_args ), scan_res );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q14_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    // first do the first scan
    DCHECK_GE( 2, scan_args.size() );

    // do the first scan
    std::vector<uint32_t> item_project_cols = {item_cols::i_data};
    std::unordered_map<q14_item_kv_types> map_res;
    if( scan_args.count( 0 ) == 1 ) {
        map_res = db_operators_.scan_mr<q14_item_types>(
            k_tpcc_item_table_id, scan_args.at( 0 ).read_ckrs,
            item_project_cols, scan_args.at( 0 ).predicate,
            (EmptyProbe) emptyProbeVal );
    }

    q14_order_line_probe probe( map_res, generator_.configs_ );

    std::vector<uint32_t> order_line_project_cols = {
        order_line_cols::ol_i_id, order_line_cols::ol_amount,
        order_line_cols::ol_delivery_d_c_since};
    std::unordered_map<q14_order_line_kv_types> ol_map_res;
    if( scan_args.count( 1 ) == 1 ) {
        ol_map_res = db_operators_.scan_mr<q14_order_line_types>(
            k_tpcc_order_line_table_id, scan_args.at( 1 ).read_ckrs,
            order_line_project_cols, scan_args.at( 1 ).predicate, probe );
    }

    std::unordered_map<int32_t, std::tuple<item, order_line>> res_map;
    for( const auto& item_entry : map_res ) {
        if( ol_map_res.count( item_entry.first ) == 1 ) {
            res_map[item_entry.first] = std::make_tuple<>(
                item_entry.second, ol_map_res.at( item_entry.first ) );
        }
    }

    std::vector<result_tuple> results;
    for( const auto& entry : res_map ) {
        result_tuple rt;
        rt.table_id = k_tpcc_item_table_id;
        rt.row_id = entry.first;

        result_cell cell;
        cell.col_id = 0;
        cell.present = true;
        double comp_val = compute_q14_value( std::get<0>( entry.second ),
                                             std::get<1>( entry.second ) );
        cell.data = double_to_string( comp_val );
        cell.type = data_type::type::DOUBLE;
        rt.cells.emplace_back( cell );

        results.emplace_back( rt );
    }
    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q14_order_line_ckrs(
        const order_line& low_order, const order_line& high_order ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_line_cols::ol_i_id, order_line_cols::ol_i_id ),
        std::make_tuple<>( order_line_cols::ol_amount,
                           order_line_cols::ol_delivery_d_c_since )};
    return generate_order_line_ckrs( low_order, high_order, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q14_item_ckrs(
        const item& low_item, const item& high_item ) const {

    std::vector<cell_key_ranges> ckrs;
    uint64_t low_key = make_item_key( low_item, generator_.configs_ );
    uint64_t high_key = make_item_key( high_item, generator_.configs_ );

    ckrs.emplace_back( create_cell_key_ranges( k_tpcc_item_table_id, low_key,
                                               high_key, item_cols::i_data,
                                               item_cols::i_data ) );

    return ckrs;
}

tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q14_scan_args(
        const order_line& low_order, const order_line& high_order,
        const item& low_item, const item& high_item, uint64_t low_delivery_time,
        uint64_t high_delivery_time ) const {

    scan_arguments item_scan;
    item_scan.label = 0;
    item_scan.read_ckrs = generate_q14_item_ckrs( low_item, high_item );

    scan_arguments deliv_scan;
    deliv_scan.label = 1;
    deliv_scan.read_ckrs =
        generate_q14_order_line_ckrs( low_order, high_order );

    cell_predicate c_pred;
    c_pred.table_id = k_tpcc_order_line_table_id;
    c_pred.col_id = order_line_cols::ol_delivery_d_c_since;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( low_delivery_time );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    deliv_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = uint64_to_string( high_delivery_time );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    deliv_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.col_id = order_line_cols::ol_i_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = uint64_to_string( low_item.i_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    deliv_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = uint64_to_string( high_item.i_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    deliv_scan.predicate.and_predicates.emplace_back( c_pred );

    std::vector<scan_arguments> ret = {item_scan, deliv_scan};

    return ret;
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q15() {
    auto key_range = generator_.get_q1_scan_range();
    auto item_range = generator_.get_q14_item_range();
    auto supplier_range = generator_.get_q5_supplier_range();
    auto low_time = generator_.get_q1_delivery_time();

    return do_q15_work( std::get<0>( key_range ), std::get<1>( key_range ),
                        std::get<0>( item_range ), std::get<1>( item_range ),
                        std::get<0>( supplier_range ),
                        std::get<1>( supplier_range ), low_time );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
    tpch_benchmark_worker_templ::do_q15_work( const order_line& low_order,
                                              const order_line& high_order,
                                              const item&       low_item,
                                              const item&       high_item,
                                              const supplier&   low_supplier,
                                              const supplier&   high_supplier,
                                              uint64_t low_delivery_time ) {

    auto scan_args = generate_q15_scan_args( low_order, high_order, low_item,
                                             high_item, low_supplier,
                                             high_supplier, low_delivery_time );

    scan_result scan_res;
    do_q15_work_by_scan( map_scan_args( scan_args ), scan_res );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q15_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    // first do the first scan
    DCHECK_GE( 3, scan_args.size() );

    // order line, stock, supplier

    std::unordered_map<q15_order_line_kv_types> ol_map_res;
    std::vector<uint32_t>                       ol_proj_cols = {
        order_line_cols::ol_i_id, order_line_cols::ol_supply_w_id,
        order_line_cols::ol_amount, order_line_cols::ol_delivery_d_c_since};
    if( scan_args.count( 0 ) == 1 ) {
        ol_map_res = db_operators_.scan_mr<q15_order_line_types>(
            k_tpcc_order_line_table_id, scan_args.at( 0 ).read_ckrs,
            ol_proj_cols, scan_args.at( 0 ).predicate, generator_.configs_ );
    }

    q15_stock_probe stock_probe( ol_map_res, generator_.configs_ );
    std::unordered_map<q15_stock_kv_types> stock_map_res;
    std::vector<uint32_t>                  stock_proj_cols = {
        stock_cols::s_i_id, stock_cols::s_w_id, stock_cols::s_s_id};
    if( scan_args.count( 1 ) == 1 ) {
        stock_map_res = db_operators_.scan_mr<q15_stock_types>(
            k_tpcc_stock_table_id, scan_args.at( 1 ).read_ckrs, stock_proj_cols,
            scan_args.at( 1 ).predicate, stock_probe );
    }

    q15_supplier_probe supplier_probe( stock_map_res, generator_.configs_ );
    std::unordered_map<q15_supplier_kv_types> supplier_map_res;
    std::vector<uint32_t>                     supplier_proj_cols = {
        supplier_cols::s_name,
        supplier_cols::s_phone,
        supplier_cols::s_address_a_street_1,
        supplier_cols::s_address_a_street_2,
        supplier_cols::s_address_a_city,
        supplier_cols::s_address_a_state,
        supplier_cols::s_address_a_zip,
    };
    if( scan_args.count( 2 ) == 1 ) {
        supplier_map_res = db_operators_.scan_mr<q15_supplier_types>(
            k_tpcc_supplier_table_id, scan_args.at( 2 ).read_ckrs,
            supplier_proj_cols, scan_args.at( 2 ).predicate, supplier_probe );
    }

    std::vector<result_tuple> results;
    for( const auto& sup_entry : supplier_map_res ) {
        int32_t s_id = sup_entry.first;

        auto s_found = stock_map_res.find( s_id );
        if( s_found == stock_map_res.end() ) {
            continue;
        }

        float       s_amount = s_found->second;
        const auto& sup = sup_entry.second;

        result_tuple rt;
        rt.row_id = s_id;
        rt.table_id = k_tpcc_supplier_table_id;

        result_cell cell;
        cell.present = true;

        cell.col_id = 0;
        cell.data = int64_to_string( s_id );
        cell.type = data_type::type::INT64;
        rt.cells.emplace_back( cell );

        cell.col_id = 1;
        cell.data = sup.s_name;
        cell.type = data_type::type::STRING;
        rt.cells.emplace_back( cell );

        cell.col_id = 2;
        cell.data = sup.s_address.a_zip;
        cell.type = data_type::type::STRING;
        rt.cells.emplace_back( cell );

        cell.col_id = 3;
        cell.data = sup.s_phone;
        cell.type = data_type::type::STRING;
        rt.cells.emplace_back( cell );

        cell.col_id = 4;
        cell.data = double_to_string( (double) s_amount );
        cell.type = data_type::type::DOUBLE;
        rt.cells.emplace_back( cell );

        results.emplace_back( rt );
    }
    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q15_order_line_ckrs(
        const order_line& low_order, const order_line& high_order ) const {

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_line_cols::ol_i_id,
                           order_line_cols::ol_supply_w_id ),
        std::make_tuple<>( order_line_cols::ol_amount,
                           order_line_cols::ol_delivery_d_c_since )};
    return generate_order_line_ckrs( low_order, high_order, cols );
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q15_stock_ckrs(
        const order_line& low_order, const order_line& high_order,
        const item& low_item, const item& high_item ) const {

    stock low_stock;
    stock high_stock;

    low_stock.s_w_id = low_order.ol_w_id;
    high_stock.s_w_id = high_order.ol_w_id;

    low_stock.s_i_id = low_item.i_id;
    high_stock.s_i_id = high_item.i_id;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( stock_cols::s_i_id, stock_cols::s_s_id )};
    return generate_stock_ckrs( low_stock, high_stock, cols );
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q15_supplier_ckrs(
        const supplier& low_supplier, const supplier& high_supplier ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( supplier_cols::s_name,
                           supplier_cols::s_address_a_zip )};
    return generate_supplier_ckrs( low_supplier, high_supplier, cols );
}

tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q15_scan_args(
        const order_line& low_order, const order_line& high_order,
        const item& low_item, const item& high_item,
        const supplier& low_supplier, const supplier& high_supplier,
        uint64_t low_delivery_time ) const {

    scan_arguments order_line_scan;
    order_line_scan.label = 0;
    order_line_scan.read_ckrs =
        generate_q15_order_line_ckrs( low_order, high_order );

    cell_predicate c_pred;
    c_pred.table_id = k_tpcc_order_line_table_id;
    c_pred.col_id = order_line_cols::ol_delivery_d_c_since;
    c_pred.type = data_type::UINT64;
    c_pred.data = uint64_to_string( low_delivery_time );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    order_line_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.col_id = order_line_cols::ol_i_id;
    c_pred.type = data_type::INT64;
    c_pred.data = int64_to_string( low_item.i_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    order_line_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_item.i_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    order_line_scan.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments stock_scan;
    stock_scan.label = 1;
    stock_scan.read_ckrs =
        generate_q15_stock_ckrs( low_order, high_order, low_item, high_item );

    c_pred.table_id = k_tpcc_stock_table_id;
    c_pred.col_id = stock_cols::s_s_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_supplier.s_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    stock_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_supplier.s_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    stock_scan.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments supplier_scan;
    supplier_scan.label = 2;
    supplier_scan.read_ckrs =
        generate_q15_supplier_ckrs( low_supplier, high_supplier );

    std::vector<scan_arguments> ret = {order_line_scan, stock_scan,
                                       supplier_scan};

    return ret;
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q16() {
    auto stock_range = generator_.get_q11_stock_range();
    auto supplier_range = generator_.get_q5_supplier_range();
    auto item_data_range = generator_.get_q3_customer_state_range();
    auto supplier_data_range = generator_.get_q3_customer_state_range();

    return do_q16_work(
        std::get<0>( stock_range ), std::get<1>( stock_range ),
        std::get<0>( supplier_range ), std::get<1>( supplier_range ),
        std::get<0>( item_data_range ), std::get<1>( item_data_range ),
        std::get<0>( supplier_data_range ),
        std::get<1>( supplier_data_range ) );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q16_work(
        const stock& low_stock, const stock& high_stock,
        const supplier& low_supplier, const supplier& high_supplier,
        const std::string& i_data_low, const std::string& i_data_high,
        const std::string& supplier_comment_low,
        const std::string& supplier_comment_high ) {

    auto scan_args = generate_q16_scan_args(
        low_stock, high_stock, low_supplier, high_supplier, i_data_low,
        i_data_high, supplier_comment_low, supplier_comment_high );

    scan_result scan_res;
    do_q16_work_by_scan( map_scan_args( scan_args ), scan_res );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q16_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    // first do the first scan
    DCHECK_GE( 3, scan_args.size() );

    // item line, supplier, stock
    std::vector<uint32_t> item_proj_cols = {
        item_cols::i_price, item_cols::i_name, item_cols::i_data};
    std::unordered_map<q16_item_kv_types> item_map_res;
    if( scan_args.count( 0 ) == 1 ) {
        item_map_res = db_operators_.scan_mr<q16_item_types>(
            k_tpcc_item_table_id, scan_args.at( 0 ).read_ckrs, item_proj_cols,
            scan_args.at( 0 ).predicate, emptyProbeVal );
    }

    std::vector<uint32_t> supplier_proj_cols = {supplier_cols::s_comment};
    std::unordered_map<q16_supplier_kv_types> supplier_map_res;
    if( scan_args.count( 1 ) == 1 ) {
        supplier_map_res = db_operators_.scan_mr<q16_supplier_types>(
            k_tpcc_supplier_table_id, scan_args.at( 1 ).read_ckrs,
            supplier_proj_cols, scan_args.at( 1 ).predicate, emptyProbeVal );
    }

    std::vector<uint32_t> stock_proj_cols = {
        stock_cols::s_i_id, stock_cols::s_w_id, stock_cols::s_s_id};
    std::unordered_map<q16_stock_kv_types> stock_map_res;
    q16_stock_probe stock_probe( item_map_res, supplier_map_res,
                                 generator_.configs_ );
    if( scan_args.count( 2 ) == 1 ) {
        stock_map_res = db_operators_.scan_mr<q16_stock_types>(
            k_tpcc_stock_table_id, scan_args.at( 2 ).read_ckrs, stock_proj_cols,
            scan_args.at( 2 ).predicate, stock_probe );
    }

    std::vector<result_tuple> results;
    for( const auto& stock_entry : stock_map_res ) {
        int32_t  i_id = stock_entry.first;
        uint64_t i_cnt = stock_entry.second;

        auto i_found = item_map_res.find( i_id );
        if( i_found == item_map_res.end() ) {
            continue;
        }

        const auto& i = i_found->second;

        result_tuple rt;
        rt.table_id = k_tpcc_item_table_id;
        rt.row_id = i_id;

        result_cell rc;
        rc.present = true;

        rc.col_id = 0;
        rc.data = i.i_name;
        rc.type = data_type::STRING;
        rt.cells.emplace_back( rc );

        rc.col_id = 1;
        rc.data = i.i_data;
        rc.type = data_type::STRING;
        rt.cells.emplace_back( rc );

        rc.col_id = 2;
        rc.data = double_to_string( (double) i.i_price );
        rc.type = data_type::DOUBLE;
        rt.cells.emplace_back( rc );

        rc.col_id = 3;
        rc.data = uint64_to_string( i_cnt );
        rc.type = data_type::UINT64;
        rt.cells.emplace_back( rc );

        results.emplace_back( rt );
    }
    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q16_item_ckrs(
        const stock& low_stock, const stock& high_stock ) const {
    item low_item;
    item high_item;

    low_item.i_id = low_stock.s_i_id;
    high_item.i_id = high_stock.s_i_id;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( item_cols::i_price, item_cols::i_data )};
    return generate_item_ckrs( low_item, high_item, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q16_stock_ckrs(
        const stock& low_stock, const stock& high_stock ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( stock_cols::s_i_id, stock_cols::s_s_id )};
    return generate_stock_ckrs( low_stock, high_stock, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q16_supplier_ckrs(
        const supplier& low_supplier, const supplier& high_supplier ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {std::make_tuple<>(
        supplier_cols::s_comment, supplier_cols::s_comment )};
    return generate_supplier_ckrs( low_supplier, high_supplier, cols );
}

tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q16_scan_args(
        const stock& low_stock, const stock& high_stock,
        const supplier& low_supplier, const supplier& high_supplier,
        const std::string& i_data_low, const std::string& i_data_high,
        const std::string& supplier_comment_low,
        const std::string& supplier_comment_high ) const {

    scan_arguments item_scan;
    item_scan.label = 0;
    item_scan.read_ckrs = generate_q16_item_ckrs( low_stock, high_stock );

    cell_predicate c_pred;
    c_pred.table_id = k_tpcc_item_table_id;
    c_pred.col_id = item_cols::i_data;
    c_pred.type = data_type::type::STRING;
    c_pred.data = i_data_low;
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    item_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = i_data_high;
    c_pred.predicate = predicate_type::type::LESS_THAN;
    item_scan.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments supplier_scan;
    supplier_scan.label = 1;
    supplier_scan.read_ckrs =
        generate_q16_supplier_ckrs( low_supplier, high_supplier );

    c_pred.table_id = k_tpcc_supplier_table_id;
    c_pred.col_id = supplier_cols::s_comment;
    c_pred.type = data_type::type::STRING;
    c_pred.data = supplier_comment_low;
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    supplier_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = supplier_comment_high;
    c_pred.predicate = predicate_type::type::LESS_THAN;
    supplier_scan.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments stock_scan;
    stock_scan.label = 2;
    stock_scan.read_ckrs = generate_q16_stock_ckrs( low_stock, high_stock );

    c_pred.table_id = k_tpcc_stock_table_id;
    c_pred.col_id = stock_cols::s_s_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_supplier.s_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    stock_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_supplier.s_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    stock_scan.predicate.and_predicates.emplace_back( c_pred );

    std::vector<scan_arguments> ret = {item_scan, supplier_scan, stock_scan};

    return ret;
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q17() {
    auto ol_range = generator_.get_q1_scan_range();
    auto item_range = generator_.get_q14_item_range();
    auto item_data_range = generator_.get_q3_customer_state_range();

    return do_q17_work( std::get<0>( ol_range ), std::get<1>( ol_range ),
                        std::get<0>( item_range ), std::get<1>( item_range ),
                        std::get<0>( item_data_range ),
                        std::get<1>( item_data_range ) );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
    tpch_benchmark_worker_templ::do_q17_work( const order_line&  low_ol,
                                              const order_line&  high_ol,
                                              const item&        low_item,
                                              const item&        high_item,
                                              const std::string& i_data_low,
                                              const std::string& i_data_high ) {

    auto scan_args = generate_q17_scan_args(
        low_ol, high_ol, low_item, high_item, i_data_low, i_data_high );

    scan_result scan_res;
    do_q17_work_by_scan( map_scan_args( scan_args ), scan_res );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q17_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    // first do the first scan
    DCHECK_GE( 2, scan_args.size() );

    // item, order_line
    std::vector<uint32_t> item_proj_cols = {item_cols::i_data};
    std::unordered_map<q17_item_kv_types> item_map_res;

    if( scan_args.count( 0 ) == 1 ) {
        item_map_res = db_operators_.scan_mr<q17_item_types>(
            k_tpcc_item_table_id, scan_args.at( 0 ).read_ckrs, item_proj_cols,
            scan_args.at( 0 ).predicate, (EmptyProbe) emptyProbeVal );
    }

    std::vector<uint32_t> ol_proj_cols = {order_line_cols::ol_i_id,
                                          order_line_cols::ol_quantity,
                                          order_line_cols::ol_amount};
    std::unordered_map<q17_order_line_kv_types> ol_map_res;
    q17_order_line_probe ol_probe(item_map_res, generator_.configs_);

    if( scan_args.count( 1 ) == 1 ) {
        ol_map_res = db_operators_.scan_mr<q17_order_line_types>(
            k_tpcc_order_line_table_id, scan_args.at( 1 ).read_ckrs, ol_proj_cols,
            scan_args.at( 1 ).predicate, ol_probe );
    }

    std::vector<result_tuple> results;
    // MTODO
    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q17_item_ckrs(
        const item& low_item, const item& high_item ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( item_cols::i_data, item_cols::i_data )};
    return generate_item_ckrs( low_item, high_item, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q17_order_line_ckrs(
        const order_line& low_ol, const order_line& high_ol ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_line_cols::ol_i_id, order_line_cols::ol_i_id ),
        std::make_tuple<>( order_line_cols::ol_quantity,
                           order_line_cols::ol_amount ),
    };
    return generate_order_line_ckrs( low_ol, high_ol, cols );
}

tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q17_scan_args(
        const order_line& low_ol, const order_line& high_ol,
        const item& low_item, const item& high_item,
        const std::string& low_item_str,
        const std::string& high_item_str ) const {

    scan_arguments item_scan;
    item_scan.label = 0;
    item_scan.read_ckrs = generate_q17_item_ckrs( low_item, high_item );

    cell_predicate c_pred;
    c_pred.table_id = k_tpcc_item_table_id;
    c_pred.col_id = item_cols::i_data;
    c_pred.type = data_type::type::STRING;
    c_pred.data = low_item_str;
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    item_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = high_item_str;
    c_pred.predicate = predicate_type::type::LESS_THAN;
    item_scan.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments ol_scan;
    ol_scan.label = 1;
    ol_scan.read_ckrs = generate_q17_order_line_ckrs( low_ol, high_ol );

    c_pred.table_id = k_tpcc_order_line_table_id;
    c_pred.col_id = order_line_cols::ol_i_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_item.i_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    ol_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_item.i_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    ol_scan.predicate.and_predicates.emplace_back( c_pred );

    std::vector<scan_arguments> ret = {item_scan, ol_scan};

    return ret;
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q18() {
    auto key_range = generator_.get_q4_order_range();
    float ol_amount_threshold = 200.0;

    return do_q18_work( std::get<0>( key_range ), std::get<1>( key_range ),
                        ol_amount_threshold );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
    tpch_benchmark_worker_templ::do_q18_work( const order& low_order,
                                              const order& high_order,
                                              float ol_amount_threshold ) {

    auto scan_args = generate_q18_scan_args( low_order, high_order );

    scan_result scan_res;
    do_q18_work_by_scan( map_scan_args( scan_args ), scan_res,
                         ol_amount_threshold );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q18_order_line_ckrs(
        const order& low_order, const order& high_order ) const {

    order_line low_ol;
    order_line high_ol;

    low_ol.ol_o_id = low_order.o_id;
    high_ol.ol_o_id = high_order.o_id;

    low_ol.ol_d_id = low_order.o_d_id;
    high_ol.ol_d_id = high_order.o_d_id;

    low_ol.ol_w_id = low_order.o_w_id;
    high_ol.ol_w_id = high_order.o_w_id;

    low_ol.ol_number = 0;
    high_ol.ol_number = generator_.configs_.max_num_order_lines_per_order_ - 1;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_line_cols::ol_o_id, order_line_cols::ol_w_id ),
        std::make_tuple<>( order_line_cols::ol_amount,
                           order_line_cols::ol_amount )};

    return generate_order_line_ckrs( low_ol, high_ol, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q18_order_ckrs(
        const order& low_order, const order& high_order ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_cols::o_id, order_cols::o_w_id ),
        std::make_tuple<>( order_cols::o_entry_d_c_since,
                           order_cols::o_entry_d_c_since )};

    return generate_order_ckrs( low_order, high_order, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q18_customer_ckrs(
        const order& low_order, const order& high_order ) const {
    customer low_cust;
    customer high_cust;

    low_cust.c_id = 0;
    high_cust.c_id = generator_.configs_.num_customers_per_district_ - 1;

    low_cust.c_d_id = low_order.o_d_id;
    high_cust.c_d_id = high_order.o_d_id;

    low_cust.c_w_id = low_order.o_w_id;
    high_cust.c_w_id = high_order.o_w_id;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( customer_cols::c_id, customer_cols::c_w_id ),
        std::make_tuple<>( customer_cols::c_last, customer_cols::c_last )};

    return generate_customer_ckrs( low_cust, high_cust, cols );
}

tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q18_scan_args(
        const order& low_order, const order& high_order ) const {

    scan_arguments customer_scan;
    customer_scan.label = 0;
    customer_scan.read_ckrs =
        generate_q18_customer_ckrs( low_order, high_order );

    scan_arguments order_scan;
    order_scan.label = 1;
    order_scan.read_ckrs = generate_q18_order_ckrs( low_order, high_order );

    scan_arguments order_line_scan;
    order_line_scan.label = 2;
    order_line_scan.read_ckrs =
        generate_q18_order_line_ckrs( low_order, high_order );

    std::vector<scan_arguments> ret = {customer_scan, order_scan,
                                       order_line_scan};

    return ret;
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q18_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res, float ol_amount_threshold ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    // first do the first scan
    DCHECK_GE( 3, scan_args.size() );

    std::unordered_map<q18_customer_kv_types> cust_map_res;
    std::vector<uint32_t>                     cust_proj_cols = {
        customer_cols::c_id, customer_cols::c_d_id, customer_cols::c_w_id,
        customer_cols::c_last};

    if( scan_args.count( 0 ) == 1 ) {
        cust_map_res = db_operators_.scan_mr<q18_customer_types>(
            k_tpcc_customer_table_id, scan_args.at( 0 ).read_ckrs,
            cust_proj_cols, scan_args.at( 0 ).predicate,
            (EmptyProbe) emptyProbeVal );
    }

    std::unordered_map<q18_order_kv_types> order_map_res;
    std::vector<uint32_t>                  order_proj_cols = {
        order_cols::o_id, order_cols::o_c_id, order_cols::o_d_id,
        order_cols::o_w_id, order_cols::o_entry_d_c_since};
    q18_order_probe order_probe( cust_map_res, generator_.configs_ );

    if( scan_args.count( 1 ) == 1 ) {
        order_map_res = db_operators_.scan_mr<q18_order_types>(
            k_tpcc_order_table_id, scan_args.at( 1 ).read_ckrs, order_proj_cols,
            scan_args.at( 1 ).predicate, order_probe );
    }

    std::unordered_map<q18_order_line_kv_types>  order_line_map_res;
    std::vector<uint32_t>                        order_line_proj_cols = {
        order_line_cols::ol_o_id, order_line_cols::ol_d_id,
        order_line_cols::ol_w_id, order_line_cols::ol_amount};
    q18_order_line_probe order_line_probe( order_map_res, generator_.configs_ );

    if( scan_args.count( 2 ) == 1 ) {
        order_line_map_res = db_operators_.scan_mr<q18_order_line_types>(
            k_tpcc_order_line_table_id, scan_args.at( 2 ).read_ckrs,
            order_line_proj_cols, scan_args.at( 2 ).predicate,
            order_line_probe );
    }

    std::vector<result_tuple> results;
    for( const auto& order_entry : order_map_res ) {
        uint64_t order_id = order_entry.first;

        order    o = std::get<1>( order_entry.second );
        uint64_t cust_id = std::get<0>( order_entry.second );

        auto cust_found = cust_map_res.find( cust_id );
        if( cust_found == cust_map_res.end() ) {
            continue;
        }

        customer cust = cust_found->second;

        auto ol_found = order_line_map_res.find( order_id );
        if( ol_found == order_line_map_res.end() ) {
            continue;
        }

        double ol_amount = ol_found->second;

        if( ol_amount <= ol_amount_threshold ) {
            continue;
        }

        result_tuple rt;

        rt.table_id = k_tpcc_order_table_id;
        rt.row_id = order_id;

        result_cell rc;

        rc.present = true;

        rc.col_id = 0;
        rc.data = cust.c_last;
        rc.type = data_type::STRING;
        rt.cells.emplace_back( rc );

        rc.col_id = 1;
        rc.data = int64_to_string( cust.c_id );
        rc.type = data_type::INT64;
        rt.cells.emplace_back( rc );

        rc.col_id = 2;
        rc.data = int64_to_string( o.o_id );
        rc.type = data_type::INT64;
        rt.cells.emplace_back( rc );

        rc.col_id = 3;
        rc.data = uint64_to_string( o.o_entry_d.c_since );
        rc.type = data_type::UINT64;
        rt.cells.emplace_back( rc );

        rc.col_id = 4;
        rc.data = double_to_string( (double) ol_amount );
        rc.type = data_type::DOUBLE;
        rt.cells.emplace_back( rc );

        results.emplace_back( rt );
    }
    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q19() {
    auto ol_range = generator_.get_q1_scan_range();
    auto item_range = generator_.get_q14_item_range();
    auto i_data_range = generator_.get_q3_customer_state_range();
    auto ol_quantity = generator_.get_q19_ol_quantity();
    auto i_price = generator_.get_q19_i_price();

    return do_q19_work( std::get<0>( ol_range ), std::get<1>( ol_range ),
                        std::get<0>( item_range ), std::get<1>( item_range ),
                        std::get<0>( ol_quantity ), std::get<1>( ol_quantity ),
                        std::get<0>( i_price ), std::get<1>( i_price ),
                        std::get<0>( i_data_range ),
                        std::get<1>( i_data_range ) );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q19_work(
        const order_line& low_ol, const order_line& high_ol,
        const item& low_item, const item& high_item, int32_t low_ol_quantity,
        int32_t high_ol_quantity, float low_i_price, float high_i_price,
        const std::string& low_i_data, const std::string& high_i_data ) {

    auto scan_args = generate_q19_scan_args(
        low_ol, high_ol, low_item, high_item, low_ol_quantity, high_ol_quantity,
        low_i_price, high_i_price, low_i_data, high_i_data );

    scan_result scan_res;
    do_q19_work_by_scan( map_scan_args( scan_args ), scan_res );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q19_order_line_ckrs(
        const order_line& low_ol, const order_line& high_ol ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_line_cols::ol_i_id, order_line_cols::ol_i_id ),
        std::make_tuple<>( order_line_cols::ol_quantity,
                           order_line_cols::ol_amount )};
    return generate_order_line_ckrs( low_ol, high_ol, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q19_item_ckrs(
        const item& low_item, const item& high_item ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( item_cols::i_price, item_cols::i_price ),
        std::make_tuple<>( item_cols::i_data, item_cols::i_data ),
    };
    return generate_item_ckrs( low_item, high_item, cols );
}

tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q19_scan_args(
        const order_line& low_ol, const order_line& high_ol,
        const item& low_item, const item& high_item, int32_t low_ol_quantity,
        int32_t high_ol_quantity, float low_i_price, float high_i_price,
        const std::string& low_i_data, const std::string& high_i_data ) const {

    scan_arguments item_scan;
    item_scan.label = 0;
    item_scan.read_ckrs = generate_q19_item_ckrs( low_item, high_item );

    cell_predicate c_pred;
    c_pred.table_id = k_tpcc_item_table_id;
    c_pred.col_id = item_cols::i_data;
    c_pred.type = data_type::type::STRING;
    c_pred.data = low_i_data;
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    item_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = high_i_data;
    c_pred.predicate = predicate_type::type::LESS_THAN;
    item_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.col_id = item_cols::i_price;
    c_pred.type = data_type::type::DOUBLE;
    c_pred.data = double_to_string( low_i_price );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    item_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = double_to_string( high_i_price );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    item_scan.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments ol_scan;
    ol_scan.label = 1;
    ol_scan.read_ckrs = generate_q19_order_line_ckrs( low_ol, high_ol );

    c_pred.table_id = k_tpcc_order_line_table_id;
    c_pred.col_id = order_line_cols::ol_i_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_item.i_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    ol_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_item.i_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    ol_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.col_id = order_line_cols::ol_quantity;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_ol_quantity );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    ol_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_ol_quantity );
    c_pred.predicate = predicate_type::type::LESS_THAN;
    ol_scan.predicate.and_predicates.emplace_back( c_pred );

    std::vector<scan_arguments> ret = {item_scan, ol_scan};

    return ret;
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q19_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    // first do the first scan
    DCHECK_GE( 2, scan_args.size() );

    std::unordered_map<q19_item_kv_types> item_map_res;
    std::vector<uint32_t>                 item_proj_cols = {item_cols::i_price,
                                            item_cols::i_data};
    if( scan_args.count( 0 ) == 1 ) {
        item_map_res = db_operators_.scan_mr<q19_item_types>(
            k_tpcc_item_table_id, scan_args.at( 0 ).read_ckrs, item_proj_cols,
            scan_args.at( 0 ).predicate, (EmptyProbe) emptyProbeVal );
    }

    q19_order_line_probe ol_probe( item_map_res, generator_.configs_ );
    std::unordered_map<q19_order_line_kv_types> ol_map_res;
    std::vector<uint32_t> ol_proj_cols = {order_line_cols::ol_i_id,
                                          order_line_cols::ol_quantity,
                                          order_line_cols::ol_amount};
    if( scan_args.count( 1 ) == 1 ) {
        ol_map_res = db_operators_.scan_mr<q19_order_line_types>(
            k_tpcc_order_line_table_id, scan_args.at( 1 ).read_ckrs,
            ol_proj_cols, scan_args.at( 1 ).predicate, ol_probe );
    }

    float ol_amount = 0;
    for( const auto& ol_entry : ol_map_res ) {
        ol_amount += ol_entry.second;
    }

    result_tuple rt;
    rt.table_id = k_tpcc_order_line_table_id;
    rt.row_id = 0;

    result_cell cell;
    cell.present = true;
    cell.col_id = 0;
    cell.data = double_to_string( (double) ol_amount );
    cell.type = data_type::type::DOUBLE;
    rt.cells.emplace_back( cell );


    std::vector<result_tuple> results = {rt};
    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q20() {
    auto    ol_range = generator_.get_q1_scan_range();
    auto    item_range = generator_.get_q14_item_range();
    auto    supplier_range = generator_.get_q5_supplier_range();
    auto    low_time = generator_.get_q1_delivery_time();
    int32_t nation_id = generator_.generate_rand_nation_id();
    auto    item_pred_range = generator_.get_q3_customer_state_range();
    float   more_multi = 2;

    return do_q20_work( std::get<0>( ol_range ), std::get<1>( ol_range ),
                        std::get<0>( item_range ), std::get<1>( item_range ),
                        std::get<0>( supplier_range ),
                        std::get<1>( supplier_range ), nation_id,
                        std::get<0>( item_pred_range ),
                        std::get<1>( item_pred_range ), low_time, more_multi );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q20_work(
        const order_line& low_order, const order_line& high_order,
        const item& low_item, const item& high_item,
        const supplier& low_supplier, const supplier& high_supplier,
        int32_t nation_id, const std::string& low_i_data,
        const std::string& high_i_data, uint64_t low_delivery_time,
        float s_quant_mult ) {

    auto scan_args = generate_q20_scan_args(
        low_order, high_order, low_item, high_item, low_supplier, high_supplier,
        nation_id, low_i_data, high_i_data, low_delivery_time );

    scan_result scan_res;
    do_q20_work_by_scan( map_scan_args( scan_args ), scan_res, s_quant_mult );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q20_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res, float s_quant_multi ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    DCHECK_GE( 4, scan_args.size() );
    // item, stock, ol, supplier

    std::vector<uint32_t> item_proj_cols = {item_cols::i_data};
    std::unordered_map<q20_item_kv_types> item_map_res;
    if( scan_args.count( 0 ) == 1 ) {
        item_map_res = db_operators_.scan_mr<q20_item_types>(
            k_tpcc_item_table_id, scan_args.at( 0 ).read_ckrs, item_proj_cols,
            scan_args.at( 0 ).predicate, (EmptyProbe) emptyProbeVal );
    }

    std::vector<uint32_t> stock_proj_cols = {
        stock_cols::s_i_id, stock_cols::s_w_id, stock_cols::s_s_id,
        stock_cols::s_quantity};
    std::unordered_map<q20_stock_kv_types> stock_map_res;
    q20_stock_probe stock_probe( item_map_res, generator_.configs_ );
    if( scan_args.count( 1 ) == 1 ) {
        stock_map_res = db_operators_.scan_mr<q20_stock_types>(
            k_tpcc_stock_table_id, scan_args.at( 1 ).read_ckrs, stock_proj_cols,
            scan_args.at( 1 ).predicate, stock_probe );
    }

    std::vector<uint32_t> ol_proj_cols = {
        order_line_cols::ol_w_id, order_line_cols::ol_i_id,
        order_line_cols::ol_quantity, order_line_cols::ol_delivery_d_c_since};
    std::unordered_map<q20_order_line_kv_types> ol_map_res;
    q20_order_line_probe ol_probe( stock_map_res, generator_.configs_ );
    if( scan_args.count( 2 ) == 1 ) {
        ol_map_res = db_operators_.scan_mr<q20_order_line_types>(
            k_tpcc_order_line_table_id, scan_args.at( 2 ).read_ckrs,
            ol_proj_cols, scan_args.at( 2 ).predicate, ol_probe );
    }

    std::vector<uint32_t> supplier_proj_cols = {supplier_cols::n_id,
                                                supplier_cols::s_name};
    // now check if s_quant_multi * s_quantity > ol_quantity
    q20_supplier_probe supplier_probe( stock_map_res, ol_map_res,
                                       generator_.configs_ );
    supplier_probe.compute( s_quant_multi );
    std::unordered_map<q20_supplier_kv_types> sup_map_res;
    if( scan_args.count( 3 ) == 1 ) {
        sup_map_res = db_operators_.scan_mr<q20_supplier_types>(
            k_tpcc_supplier_table_id, scan_args.at( 3 ).read_ckrs,
            supplier_proj_cols, scan_args.at( 3 ).predicate, supplier_probe );
    }

    std::vector<result_tuple> results;
    for( const auto& sup_entry : sup_map_res ) {
        result_tuple rt;
        rt.row_id = sup_entry.first;
        rt.table_id = k_tpcc_supplier_table_id;

        result_cell cell;
        cell.present = true;

        cell.col_id = 0;
        cell.data = sup_entry.second.s_name;
        cell.type = data_type::type::STRING;
        rt.cells.emplace_back( cell );

        results.emplace_back( rt );
    }
    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}

tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q20_scan_args(
        const order_line& low_order, const order_line& high_order,
        const item& low_item, const item& high_item,
        const supplier& low_supplier, const supplier& high_supplier,
        int32_t nation_id, const std::string& low_i_data,
        const std::string& high_i_data, uint64_t low_delivery_time ) const {

    scan_arguments item_scan;
    item_scan.label = 0;
    item_scan.read_ckrs = generate_q20_item_ckrs( low_item, high_item );

    cell_predicate c_pred;
    c_pred.table_id = k_tpcc_item_table_id;
    c_pred.col_id = item_cols::i_data;
    c_pred.type = data_type::type::STRING;
    c_pred.data = low_i_data;
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    item_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = high_i_data;
    c_pred.predicate = predicate_type::type::LESS_THAN;
    item_scan.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments stock_scan;
    stock_scan.label = 1;
    stock_scan.read_ckrs =
        generate_q20_stock_ckrs( low_order, high_order, low_item, high_item );

    c_pred.table_id = k_tpcc_stock_table_id;
    c_pred.col_id = stock_cols::s_s_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_supplier.s_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    stock_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_supplier.s_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    stock_scan.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments ol_scan;
    ol_scan.label = 2;
    ol_scan.read_ckrs = generate_q20_order_line_ckrs( low_order, high_order );

    c_pred.table_id = k_tpcc_order_line_table_id;
    c_pred.col_id = order_line_cols::ol_i_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_item.i_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    ol_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_item.i_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    ol_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.col_id = order_line_cols::ol_delivery_d_c_since;
    c_pred.type = data_type::type::UINT64;
    c_pred.data = uint64_to_string( low_delivery_time );
    c_pred.predicate = predicate_type::type::GREATER_THAN;
    ol_scan.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments supplier_scan;
    supplier_scan.label = 3;
    supplier_scan.read_ckrs =
        generate_q20_supplier_ckrs( low_supplier, high_supplier );

    c_pred.table_id = k_tpcc_supplier_table_id;
    c_pred.col_id = supplier_cols::n_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( nation_id );
    c_pred.predicate = predicate_type::type::EQUALITY;
    supplier_scan.predicate.and_predicates.emplace_back( c_pred );

    std::vector<scan_arguments> ret = {item_scan, stock_scan, ol_scan,
                                       supplier_scan};
    return ret;
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q20_item_ckrs(
        const item& low_item, const item& high_item ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( item_cols::i_data, item_cols::i_data )};
    return generate_item_ckrs( low_item, high_item, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q20_stock_ckrs(
        const order_line& low_ol, const order_line& high_ol,
        const item& low_item, const item& high_item ) const {
    stock low_stock;
    stock high_stock;

    low_stock.s_i_id = low_item.i_id;
    high_stock.s_i_id = high_item.i_id;

    low_stock.s_w_id = low_ol.ol_w_id;
    high_stock.s_w_id = high_ol.ol_w_id;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( stock_cols::s_i_id, stock_cols::s_quantity )};
    return generate_stock_ckrs( low_stock, high_stock, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q20_order_line_ckrs(
        const order_line& low_ol, const order_line& high_ol ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_line_cols::ol_w_id, order_line_cols::ol_w_id ),
        std::make_tuple<>( order_line_cols::ol_i_id, order_line_cols::ol_i_id ),
        std::make_tuple<>( order_line_cols::ol_quantity,
                           order_line_cols::ol_quantity ),
        std::make_tuple<>( order_line_cols::ol_delivery_d_c_since,
                           order_line_cols::ol_delivery_d_c_since ),
    };
    return generate_order_line_ckrs( low_ol, high_ol, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q20_supplier_ckrs(
        const supplier& low_supplier, const supplier& high_supplier ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( supplier_cols::n_id, supplier_cols::s_name )};
    return generate_supplier_ckrs( low_supplier, high_supplier, cols );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q21() {
    auto    ol_range = generator_.get_q1_scan_range();
    auto    item_range = generator_.get_q14_item_range();
    auto    supplier_range = generator_.get_q5_supplier_range();
    int32_t nation_id = generator_.generate_rand_nation_id();

    return do_q21_work( std::get<0>( ol_range ), std::get<1>( ol_range ),
                        std::get<0>( item_range ), std::get<1>( item_range ),
                        std::get<0>( supplier_range ),
                        std::get<1>( supplier_range ), nation_id );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
    tpch_benchmark_worker_templ::do_q21_work( const order_line& low_order,
                                              const order_line& high_order,
                                              const item&       low_item,
                                              const item&       high_item,
                                              const supplier&   low_supplier,
                                              const supplier&   high_supplier,
                                              int32_t           nation_id ) {
    auto scan_args =
        generate_q21_scan_args( low_order, high_order, low_item, high_item,
                                low_supplier, high_supplier, nation_id );

    scan_result scan_res;
    do_q21_work_by_scan( map_scan_args( scan_args ), scan_res );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q21_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    // first do the first scan
    DCHECK_GE( 4, scan_args.size() );

    std::vector<uint32_t> supplier_proj_cols = {supplier_cols::n_id,
                                                supplier_cols::s_name};
    std::unordered_map<q21_supplier_kv_types> supplier_map_res;
    if( scan_args.count( 0 ) == 1 ) {
        supplier_map_res = db_operators_.scan_mr<q21_supplier_types>(
            k_tpcc_supplier_table_id, scan_args.at( 0 ).read_ckrs,
            supplier_proj_cols, scan_args.at( 0 ).predicate,
            (EmptyProbe) emptyProbeVal );
    }

    std::vector<uint32_t> stock_proj_cols = {
        stock_cols::s_i_id, stock_cols::s_w_id, stock_cols::s_s_id};
    std::unordered_map<q21_stock_kv_types> stock_map_res;
    q21_stock_probe       stock_probe( supplier_map_res, generator_.configs_ );
    if( scan_args.count( 1 ) == 1 ) {
        stock_map_res = db_operators_.scan_mr<q21_stock_types>(
            k_tpcc_stock_table_id, scan_args.at( 1 ).read_ckrs, stock_proj_cols,
            scan_args.at( 1 ).predicate, stock_probe );
    }

    std::vector<uint32_t> order_proj_cols = {
        order_cols::o_id, order_cols::o_d_id, order_cols::o_w_id,
        order_cols::o_entry_d_c_since};
    std::unordered_map<q21_order_kv_types> order_map_res;
    if( scan_args.count( 2 ) == 1 ) {
        order_map_res = db_operators_.scan_mr<q21_order_types>(
            k_tpcc_order_table_id, scan_args.at( 2 ).read_ckrs, order_proj_cols,
            scan_args.at( 2 ).predicate, (EmptyProbe) emptyProbeVal );
    }

    std::vector<uint32_t> ol_proj_cols = {
        order_line_cols::ol_o_id, order_line_cols::ol_d_id,
        order_line_cols::ol_w_id, order_line_cols::ol_i_id,
        order_line_cols::ol_delivery_d_c_since};
    std::unordered_map<q21_order_line_kv_types> ol_map_res;
    q21_order_line_probe ol_probe( stock_map_res, order_map_res,
                                   generator_.configs_ );
    if( scan_args.count( 3 ) == 1 ) {
        ol_map_res = db_operators_.scan_mr<q21_order_line_types>(
            k_tpcc_order_line_table_id, scan_args.at( 3 ).read_ckrs,
            ol_proj_cols, scan_args.at( 3 ).predicate, ol_probe );
    }

    std::unordered_map<int32_t, uint64_t> sup_counts;

    for( const auto& ol_entry : ol_map_res ) {
        auto stock_found = stock_map_res.find( ol_entry.second );
        if( stock_found == stock_map_res.end() ) {
            continue;
        }
        auto sup_found = supplier_map_res.find( stock_found->second );
        if( sup_found == supplier_map_res.end() ) {
            continue;
        }
        sup_counts[stock_found->second] += 1;
    }

    std::vector<result_tuple> results;
    for( const auto& sup_entry : sup_counts ) {
        const auto& sup = supplier_map_res[sup_entry.first];

        result_tuple rt;
        rt.table_id = k_tpcc_supplier_table_id;
        rt.row_id = sup_entry.first;

        result_cell cell;
        cell.present = true;

        cell.col_id = 0;
        cell.data = sup.s_name;
        cell.type = data_type::type::STRING;
        rt.cells.emplace_back( cell );

        cell.col_id = 1;
        cell.data = uint64_to_string( sup_entry.second );
        cell.type = data_type::type::UINT64;
        rt.cells.emplace_back( cell );

        results.emplace_back( rt );
    }
    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}
tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q21_scan_args(
        const order_line& low_order, const order_line& high_order,
        const item& low_item, const item& high_item,
        const supplier& low_supplier, const supplier& high_supplier,
        int32_t nation_id ) const {

    scan_arguments supplier_scan;
    supplier_scan.label = 0;
    supplier_scan.read_ckrs =
        generate_q21_supplier_ckrs( low_supplier, high_supplier );

    cell_predicate c_pred;
    c_pred.table_id = k_tpcc_supplier_table_id;
    c_pred.col_id = supplier_cols::n_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( nation_id );
    c_pred.predicate = predicate_type::type::EQUALITY;
    supplier_scan.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments stock_scan;
    stock_scan.label = 1;
    stock_scan.read_ckrs =
        generate_q21_stock_ckrs( low_order, high_order, low_item, high_item );

    c_pred.table_id = k_tpcc_stock_table_id;
    c_pred.col_id = stock_cols::s_s_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_supplier.s_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    stock_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_supplier.s_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    stock_scan.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments order_scan;
    order_scan.label = 2;
    order_scan.read_ckrs = generate_q21_order_ckrs( low_order, high_order );

    scan_arguments ol_scan;
    ol_scan.label = 3;
    ol_scan.read_ckrs = generate_q21_order_line_ckrs( low_order, high_order );

    c_pred.col_id = order_line_cols::ol_i_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_item.i_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    ol_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_item.i_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    ol_scan.predicate.and_predicates.emplace_back( c_pred );

    std::vector<scan_arguments> ret = {supplier_scan, stock_scan, order_scan,
                                       ol_scan};
    return ret;
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q21_supplier_ckrs(
        const supplier& low_supplier, const supplier& high_supplier ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( supplier_cols::n_id, supplier_cols::s_name )};
    return generate_supplier_ckrs( low_supplier, high_supplier, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q21_order_ckrs(
        const order_line& low_ol, const order_line& high_ol ) const {
    order low_order;
    order high_order;

    low_order.o_w_id = low_ol.ol_w_id;
    high_order.o_w_id = high_ol.ol_w_id;

    low_order.o_d_id = low_ol.ol_d_id;
    high_order.o_d_id = high_ol.ol_d_id;

    low_order.o_id = low_ol.ol_o_id;
    high_order.o_id = high_ol.ol_o_id;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_cols::o_id, order_cols::o_id ),
        std::make_tuple<>( order_cols::o_d_id, order_cols::o_w_id ),
        std::make_tuple<>( order_cols::o_entry_d_c_since,
                           order_cols::o_entry_d_c_since )};
    return generate_order_ckrs( low_order, high_order, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q21_order_line_ckrs(
        const order_line& low_ol, const order_line& high_ol ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_line_cols::ol_o_id, order_line_cols::ol_w_id ),
        std::make_tuple<>( order_line_cols::ol_i_id, order_line_cols::ol_i_id ),
        std::make_tuple<>( order_line_cols::ol_delivery_d_c_since,
                           order_line_cols::ol_delivery_d_c_since )};
    return generate_order_line_ckrs( low_ol, high_ol, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q21_stock_ckrs(
        const order_line& low_ol, const order_line& high_ol,
        const item& low_item, const item& high_item ) const {
    stock low_stock;
    stock high_stock;

    low_stock.s_w_id = low_ol.ol_w_id;
    high_stock.s_w_id = high_ol.ol_w_id;

    low_stock.s_i_id = low_item.i_id;
    high_stock.s_i_id = high_item.i_id;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( stock_cols::s_i_id, stock_cols::s_s_id )};
    return generate_stock_ckrs( low_stock, high_stock, cols );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q22() {
    auto order_range = generator_.get_q4_order_range();
    auto phone_range = generator_.get_q22_phone_range();

    return do_q22_work( std::get<0>( order_range ), std::get<1>( order_range ),
                        std::get<0>( phone_range ),
                        std::get<1>( phone_range ) );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
    tpch_benchmark_worker_templ::do_q22_work( const order&       low_order,
                                              const order&       high_order,
                                              const std::string& low_phone,
                                              const std::string& high_phone ) {

    auto scan_args =
        generate_q22_scan_args( low_order, high_order, low_phone, high_phone );

    scan_result scan_res;
    do_q22_work_by_scan( map_scan_args( scan_args ), scan_res );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q22_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    // first do the first scan
    DCHECK_GE( 2, scan_args.size() );

    std::vector<uint32_t> order_proj_cols = {
        order_cols::o_c_id, order_cols::o_d_id, order_cols::o_w_id};
    std::unordered_map<q22_order_kv_types> order_map_res;
    if( scan_args.count( 0 ) == 1 ) {
        order_map_res = db_operators_.scan_mr<q22_order_types>(
            k_tpcc_order_table_id, scan_args.at( 0 ).read_ckrs, order_proj_cols,
            scan_args.at( 0 ).predicate, generator_.configs_ );
    }

    q22_customer_probe    cust_probe( order_map_res, generator_.configs_ );
    std::unordered_map<q22_customer_kv_types> cust_map_res;
    std::vector<uint32_t>                     cust_proj_cols = {
        customer_cols::c_id, customer_cols::c_d_id, customer_cols::c_w_id,
        customer_cols::c_balance, customer_cols::c_phone};
    if( scan_args.count( 1 ) == 1 ) {
        cust_map_res = db_operators_.scan_mr<q22_customer_types>(
            k_tpcc_customer_table_id, scan_args.at( 1 ).read_ckrs,
            cust_proj_cols, scan_args.at( 1 ).predicate, cust_probe );
    }

    float    sum = 0;
    uint64_t cnt = 0;
    for( const auto& cust_entry : cust_map_res ) {
        cnt += 1;
        sum += cust_entry.second.c_balance;
    }

    float    balance_avg = 0;
    if ( cnt > 0) {
        balance_avg = sum / ( (float) cnt );
    }

    std::unordered_map<char, uint64_t> cust_count;
    std::unordered_map<char, float> cust_sum;
    for( const auto& cust_entry : cust_map_res ) {
        const auto& cust = cust_entry.second;
        if( cust.c_balance <= balance_avg ) {
            continue;
        }
        if( cust.c_phone.size() < 1 ) {
            continue;
        }
        char country = cust.c_phone.at( 0 );
        cust_count[country] += 1;
        cust_sum[country] += cust.c_balance;
    }

    std::vector<result_tuple> results;
    for( const auto& cust_entry : cust_count ) {
        result_tuple rt;
        rt.table_id = k_tpcc_customer_table_id;
        rt.row_id = (uint64_t) cust_entry.first;

        result_cell cell;
        cell.present = true;

        cell.col_id = 0;
        cell.data = std::string( 1, cust_entry.first );
        cell.type = data_type::type::STRING;
        rt.cells.emplace_back( cell );

        cell.col_id = 1;
        cell.data = uint64_to_string( cust_entry.second );
        cell.type = data_type::type::UINT64;
        rt.cells.emplace_back( cell );

        cell.col_id = 2;
        cell.data = double_to_string( (double) cust_sum[cust_entry.first] );
        cell.type = data_type::type::DOUBLE;
        rt.cells.emplace_back( cell );

        results.emplace_back( rt );
    }
    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}
tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q22_scan_args(
        const order& low_order, const order& high_order,
        const std::string& low_c_phone,
        const std::string& high_c_phone ) const {

    scan_arguments order_scan;
    order_scan.label = 0;
    order_scan.read_ckrs = generate_q22_order_ckrs( low_order, high_order );

    scan_arguments customer_scan;
    customer_scan.label = 0;
    customer_scan.read_ckrs =
        generate_q22_customer_ckrs( low_order, high_order );

    cell_predicate c_pred;
    c_pred.table_id = k_tpcc_customer_table_id;
    c_pred.col_id = customer_cols::c_phone;
    c_pred.type = data_type::type::STRING;
    c_pred.data = low_c_phone;
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;

    customer_scan.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = high_c_phone;
    c_pred.predicate = predicate_type::type::LESS_THAN;
    customer_scan.predicate.and_predicates.emplace_back( c_pred );

    std::vector<scan_arguments> ret = {order_scan, customer_scan};
    return ret;
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q22_order_ckrs(
        const order& low_order, const order& high_order ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_cols::o_c_id, order_cols::o_w_id )};

    return generate_order_ckrs( low_order, high_order, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q22_customer_ckrs(
        const order& low_order, const order& high_order ) const {
    customer low_cust;
    customer high_cust;

    low_cust.c_id = 0;
    high_cust.c_id = generator_.configs_.num_customers_per_district_ - 1;

    low_cust.c_d_id = low_order.o_d_id;
    high_cust.c_d_id = high_order.o_d_id;

    low_cust.c_w_id = low_order.o_w_id;
    high_cust.c_w_id = high_order.o_w_id;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( customer_cols::c_id, customer_cols::c_w_id ),
        std::make_tuple<>( customer_cols::c_balance, customer_cols::c_balance ),
        std::make_tuple<>( customer_cols::c_phone, customer_cols::c_phone )};

    return generate_customer_ckrs( low_cust, high_cust, cols );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q9() {
    auto key_range = generator_.get_q4_order_range();
    auto item_range = generator_.get_q14_item_range();
    auto supplier_range = generator_.get_q5_supplier_range();
    auto nation_range = generator_.get_q8_nation_range();
    auto item_pred_range = generator_.get_q3_customer_state_range();

    return do_q9_work(
        std::get<0>( key_range ), std::get<1>( key_range ),
        std::get<0>( item_range ), std::get<1>( item_range ),
        std::get<0>( supplier_range ), std::get<1>( supplier_range ),
        std::get<0>( nation_range ), std::get<1>( nation_range ),
        std::get<0>( item_pred_range ), std::get<1>( item_pred_range ) );
}

tpch_benchmark_worker_types workload_operation_outcome_enum
                            tpch_benchmark_worker_templ::do_q9_work(
        const order& low_order, const order& high_order, const item& low_item,
        const item& high_item, const supplier& low_supplier,
        const supplier& high_supplier, const nation& low_nation,
        const nation& high_nation, const std::string& low_item_str,
        const std::string& high_item_str ) {

    auto scan_args = generate_q9_scan_args(
        low_order, high_order, low_item, high_item, low_supplier, high_supplier,
        low_nation, high_nation, low_item_str, high_item_str );

    scan_result scan_res;
    do_q9_work_by_scan( map_scan_args( scan_args ), scan_res );

    return WORKLOAD_OP_SUCCESS;
}

tpch_benchmark_worker_types std::vector<scan_arguments>
                            tpch_benchmark_worker_templ::generate_q9_scan_args(
        const order& low_order, const order& high_order, const item& low_item,
        const item& high_item, const supplier& low_supplier,
        const supplier& high_supplier, const nation& low_nation,
        const nation& high_nation, const std::string& low_item_str,
        const std::string& high_item_str ) const {

    scan_arguments nation_scan_arg;
    nation_scan_arg.label = 0;
    nation_scan_arg.read_ckrs =
        generate_q9_nation_ckrs( low_nation, high_nation );

    scan_arguments item_scan_arg;
    item_scan_arg.label = 1;
    item_scan_arg.read_ckrs = generate_q9_item_ckrs( low_item, high_item );

    cell_predicate c_pred;
    c_pred.table_id = k_tpcc_item_table_id;
    c_pred.col_id = item_cols::i_data;
    c_pred.type = data_type::type::STRING;
    c_pred.data = low_item_str;
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    item_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = high_item_str;
    c_pred.predicate = predicate_type::type::LESS_THAN;
    item_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments supplier_scan_arg;
    supplier_scan_arg.label = 2;
    supplier_scan_arg.read_ckrs =
        generate_q9_supplier_ckrs( low_supplier, high_supplier );

    c_pred.table_id = k_tpcc_supplier_table_id;
    c_pred.col_id = supplier_cols::n_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_nation.n_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    supplier_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_nation.n_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    supplier_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments stock_scan_arg;
    stock_scan_arg.label = 3;
    stock_scan_arg.read_ckrs =
        generate_q9_stock_ckrs( low_item, high_item, low_order, high_order );

    c_pred.table_id = k_tpcc_stock_table_id;
    c_pred.col_id = stock_cols::s_s_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_supplier.s_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    stock_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_supplier.s_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    stock_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    scan_arguments order_scan_arg;
    order_scan_arg.label = 4;
    order_scan_arg.read_ckrs = generate_q9_order_ckrs( low_order, high_order );

    scan_arguments order_line_scan_arg;
    order_line_scan_arg.label = 5;
    order_line_scan_arg.read_ckrs =
        generate_q9_order_line_ckrs( low_order, high_order );

    c_pred.table_id = k_tpcc_order_line_table_id;
    c_pred.col_id = order_line_cols::ol_i_id;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( low_item.i_id );
    c_pred.predicate = predicate_type::type::GREATER_THAN_OR_EQUAL;
    order_line_scan_arg.predicate.and_predicates.emplace_back( c_pred );

    c_pred.data = int64_to_string( high_item.i_id );
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    order_line_scan_arg.predicate.and_predicates.emplace_back( c_pred );


    std::vector<scan_arguments> ret = {nation_scan_arg,   item_scan_arg,
                                       supplier_scan_arg, stock_scan_arg,
                                       order_scan_arg,    order_line_scan_arg};
    return ret;
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::do_q9_work_by_scan(
        const std::unordered_map<int, scan_arguments>& scan_args,
        scan_result& scan_res ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> scan_ckrs;
        get_scan_read_ckrs( scan_args, scan_ckrs );
        db_operators_.begin_scan_transaction( scan_ckrs );
        RETURN_VOID_AND_COMMIT_IF_SS_DB();
    }
    RETURN_VOID_IF_SS_DB();

    // nation, supplier, stock, orders, order_line
    DCHECK_GE( 6, scan_args.size() );

    std::vector<uint32_t> nation_proj_cols = {nation_cols::n_name};
    std::unordered_map<q9_nation_kv_types> nat_map_res;
    if( scan_args.count( 0 ) == 1 ) {
        nat_map_res = db_operators_.scan_mr<q9_nation_types>(
            k_tpcc_nation_table_id, scan_args.at( 0 ).read_ckrs,
            nation_proj_cols, scan_args.at( 0 ).predicate,
            (EmptyProbe) emptyProbeVal );
    }

    std::vector<uint32_t>                item_proj_cols = {item_cols::i_data};
    std::unordered_map<q9_item_kv_types> item_map_res;
    if( scan_args.count( 1 ) == 1 ) {
        item_map_res = db_operators_.scan_mr<q9_item_types>(
            k_tpcc_item_table_id, scan_args.at( 1 ).read_ckrs, item_proj_cols,
            scan_args.at( 1 ).predicate, (EmptyProbe) emptyProbeVal );
    }

    std::vector<uint32_t> supplier_proj_cols = {supplier_cols::n_id};
    q9_supplier_probe sup_probe( nat_map_res, generator_.configs_ );
    std::unordered_map<q9_supplier_kv_types> sup_map_res;
    if( scan_args.count( 2 ) == 1 ) {
        sup_map_res = db_operators_.scan_mr<q9_supplier_types>(
            k_tpcc_supplier_table_id, scan_args.at( 2 ).read_ckrs,
            supplier_proj_cols, scan_args.at( 2 ).predicate, sup_probe );
    }

    std::vector<uint32_t> stock_proj_cols = {
        stock_cols::s_i_id, stock_cols::s_w_id, stock_cols::s_s_id};
    q9_stock_probe stock_probe( item_map_res, sup_map_res,
                                generator_.configs_ );
    std::unordered_map<q9_stock_kv_types> stock_map_res;
    if( scan_args.count( 3 ) == 1 ) {
        stock_map_res = db_operators_.scan_mr<q9_stock_types>(
            k_tpcc_stock_table_id, scan_args.at( 3 ).read_ckrs, stock_proj_cols,
            scan_args.at( 3 ).predicate, stock_probe );
    }

    std::vector<uint32_t> order_proj_cols = {
        order_cols::o_id, order_cols::o_d_id, order_cols::o_w_id,
        order_cols::o_entry_d_c_since};
    std::unordered_map<q9_order_kv_types> order_map_res;
    if( scan_args.count( 4 ) == 1 ) {
        order_map_res = db_operators_.scan_mr<q9_order_types>(
            k_tpcc_order_table_id, scan_args.at( 4 ).read_ckrs, order_proj_cols,
            scan_args.at( 4 ).predicate, emptyProbeVal );
    }

    std::vector<uint32_t> ol_proj_cols = {
        order_line_cols::ol_o_id, order_line_cols::ol_d_id,
        order_line_cols::ol_w_id, order_line_cols::ol_i_id,
        order_line_cols::ol_amount};
    q9_order_line_probe ol_probe( stock_map_res, order_map_res, generator_.configs_ );
    std::unordered_map<q9_order_line_kv_types> ol_map_res;
    if( scan_args.count( 5 ) == 1 ) {
        ol_map_res = db_operators_.scan_mr<q9_order_line_types>(
            k_tpcc_order_line_table_id, scan_args.at( 5 ).read_ckrs, ol_proj_cols,
            scan_args.at( 5 ).predicate, ol_probe);
    }

    std::unordered_map<int32_t /* nation */,
                       std::unordered_map<uint64_t /* year */, float>>
        map_res;
    for( const auto& ol_entry : ol_map_res ) {
        uint64_t stock_key = ol_entry.first.stock_key_;
        uint64_t year = ol_entry.first.year_;
        float    ol_amount = ol_entry.second;

        auto stock_found = stock_map_res.find( stock_key );
        if( stock_found == stock_map_res.end() ) {
            continue;
        }
        int32_t sup_id = stock_found->second;

        auto sup_found = sup_map_res.find( sup_id );
        if( sup_found == sup_map_res.end() ) {
            continue;
        }
        int32_t nat_id = sup_found->second;

        if( nat_map_res.count( nat_id ) == 0 ) {
            continue;
        }

        auto res_found = map_res.find( nat_id );
        if( res_found == map_res.end() ) {
            std::unordered_map<uint64_t /* year */, float> nat_res;
            nat_res.emplace( year, ol_amount );
            map_res.emplace( nat_id, nat_res );
        } else {
            auto nat_res_found = res_found->second.find( year );
            if( nat_res_found == res_found->second.end() ) {
                // insert
                res_found->second.emplace( year, ol_amount );
            } else {
                // add there
                nat_res_found->second += ol_amount;
            }
        }
    }

    std::vector<result_tuple> results;
    for( const auto& res_entry : map_res ) {
        int32_t nat_id = res_entry.first;
        for( const auto& year_entry : res_entry.second ) {
            uint64_t year = year_entry.first;
            float    amount = year_entry.second;

            result_tuple rt;
            rt.table_id = k_tpcc_customer_table_id;
            rt.row_id = ( year * nation::NUM_NATIONS ) + nat_id;

            result_cell cell;
            cell.present = true;

            cell.col_id = 0;
            cell.data = uint64_to_string( year );
            cell.type = data_type::type::UINT64;
            rt.cells.emplace_back( cell );

            cell.col_id = 1;
            cell.data = nat_map_res[nat_id].n_name;
            cell.type = data_type::type::STRING;
            rt.cells.emplace_back( cell );

            cell.col_id = 2;
            cell.data = double_to_string( (double) amount );
            cell.type = data_type::type::DOUBLE;
            rt.cells.emplace_back( cell );

            results.emplace_back( rt );
        }
    }
    scan_res.res_tuples[0] = results;

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q9_nation_ckrs(
        const nation& low_nation, const nation& high_nation ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( nation_cols::n_name, nation_cols::n_name )};
    return generate_nation_ckrs( low_nation, high_nation, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q9_item_ckrs(
        const item& low_item, const item& high_item ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( item_cols::i_data, item_cols::i_data )};
    return generate_item_ckrs( low_item, high_item, cols );
}
tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q9_stock_ckrs(
        const item& low_item, const item& high_item, const order& low_order,
        const order& high_order ) const {
    stock low_stock;
    stock high_stock;

    low_stock.s_w_id = low_order.o_w_id;
    high_stock.s_w_id = high_order.o_w_id;

    low_stock.s_i_id = low_item.i_id;
    high_stock.s_i_id = high_item.i_id;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( stock_cols::s_i_id, stock_cols::s_s_id )};
    return generate_stock_ckrs( low_stock, high_stock, cols );
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q9_supplier_ckrs(
        const supplier& low_supplier, const supplier& high_supplier ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( supplier_cols::n_id, supplier_cols::n_id )};
    return generate_supplier_ckrs( low_supplier, high_supplier, cols );
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q9_order_ckrs(
        const order& low_order, const order& high_order ) const {
    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_cols::o_id, order_cols::o_w_id ),
        std::make_tuple<>( order_cols::o_entry_d_c_since,
                           order_cols::o_entry_d_c_since )};
    return generate_order_ckrs( low_order, high_order, cols );
}

tpch_benchmark_worker_types std::vector<cell_key_ranges>
                            tpch_benchmark_worker_templ::generate_q9_order_line_ckrs(
        const order& low_key, const order& high_key ) const {
    order_line low_ol;
    order_line high_ol;

    low_ol.ol_w_id = low_key.o_w_id;
    high_ol.ol_w_id = high_key.o_w_id;

    low_ol.ol_d_id = low_key.o_d_id;
    high_ol.ol_d_id = high_key.o_d_id;

    low_ol.ol_o_id = low_key.o_id;
    low_ol.ol_number = order::MIN_OL_CNT;

    high_ol.ol_o_id = high_key.o_id;
    high_ol.ol_number = generator_.configs_.max_num_order_lines_per_order_ - 1;

    std::vector<std::tuple<uint32_t, uint32_t>> cols = {
        std::make_tuple<>( order_line_cols::ol_o_id, order_line_cols::ol_w_id ),
        std::make_tuple<>( order_line_cols::ol_i_id, order_line_cols::ol_i_id ),
        std::make_tuple<>( order_line_cols::ol_amount,
                           order_line_cols::ol_amount )};
    return generate_order_line_ckrs( low_ol, high_ol, cols );
}

tpch_benchmark_worker_types void
    tpch_benchmark_worker_templ::set_transaction_partition_holder(
        transaction_partition_holder* txn_holder ) {
    db_operators_.set_transaction_partition_holder( txn_holder );
}
tpch_benchmark_worker_types tpcc_configs
                            tpch_benchmark_worker_templ::get_configs() const {
    return generator_.configs_;
}

