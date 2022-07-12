#pragma once

#include "../../common/perf_tracking.h"
#include "../../common/string_conversion.h"
#include "../../common/thread_utils.h"
#include "tpcc_db_operators.h"
#include <glog/logging.h>
#include <glog/stl_logging.h>

tpcc_benchmark_worker_types tpcc_benchmark_worker_templ::tpcc_benchmark_worker(
    uint32_t client_id, db_abstraction* db, zipf_distribution_cdf* z_cdf,
    const workload_operation_selector& op_selector, const tpcc_configs& configs,
    const db_abstraction_configs& abstraction_configs )
    : db_operators_( client_id, db, abstraction_configs,
                     configs.bench_configs_.limit_update_propagation_,
                     false /* store global state */ ),
      loader_( db, configs, abstraction_configs, client_id ),
      generator_( z_cdf, op_selector, client_id, configs ),
      data_sizes_( get_tpcc_data_sizes( configs ) ),
      statistics_(),
      worker_( nullptr ),
      done_( false ) {
    statistics_.init( k_tpcch_workload_operations );
}

tpcc_benchmark_worker_types
    tpcc_benchmark_worker_templ::~tpcc_benchmark_worker() {}

tpcc_benchmark_worker_types tpcc_configs
                            tpcc_benchmark_worker_templ::get_configs() const {
    return generator_.configs_;
}

tpcc_benchmark_worker_types void
    tpcc_benchmark_worker_templ::start_timed_workload() {
    worker_ = std::unique_ptr<std::thread>(
        new std::thread( &tpcc_benchmark_worker_templ::run_workload, this ) );
}

tpcc_benchmark_worker_types void
    tpcc_benchmark_worker_templ::stop_timed_workload() {
    done_ = true;
    DCHECK( worker_ );
    join_thread( *worker_ );
    worker_ = nullptr;
}

tpcc_benchmark_worker_types void tpcc_benchmark_worker_templ::run_workload() {
    // time me boys
    DVLOG( 10 ) << db_operators_.client_id_ << " starting workload";
    std::chrono::high_resolution_clock::time_point s =
        std::chrono::high_resolution_clock::now();

    // this will only every be true once so it's likely to not be done

    while( likely( !done_ ) ) {
        // we don't want to check this very often so we do a bunch of operations
        // in a loop
        for( uint32_t op_count = 0;
             op_count < generator_.get_num_ops_before_timer_check();
             op_count++ ) {
            do_workload_operation();
        }
    }
    std::chrono::high_resolution_clock::time_point e =
        std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::nano> elapsed = e - s;
    DVLOG( 10 ) << db_operators_.client_id_ << " ran for:" << elapsed.count()
                << " ns";
    statistics_.store_running_time( elapsed.count() );
}

tpcc_benchmark_worker_types void
    tpcc_benchmark_worker_templ::do_workload_operation() {

    workload_operation_enum op = generator_.get_operation();
    double lat;
    DCHECK_GE( op, TPCC_DELIVERY );
    DCHECK_LE( op, TPCC_STOCK_LEVEL );

    DVLOG( 30 ) << db_operators_.client_id_ << " performing "
                << workload_operation_string( op );
    start_timer( TPCC_WORKLOAD_OP_TIMER_ID );
    workload_operation_outcome_enum status = perform_workload_operation( op );
    stop_and_store_timer( TPCC_WORKLOAD_OP_TIMER_ID, lat );
    statistics_.add_outcome( op, status, (uint64_t) lat );

    DVLOG( 30 ) << db_operators_.client_id_ << " performing "
                << k_workload_operations_to_strings.at( op )
                << " status:" << status;
}

tpcc_benchmark_worker_types workload_operation_outcome_enum
                            tpcc_benchmark_worker_templ::perform_workload_operation(
        const workload_operation_enum& op ) {
    switch( op ) {
        case TPCC_DELIVERY:
            return do_delivery();
        case TPCC_NEW_ORDER:
            return do_new_order();
        case TPCC_ORDER_STATUS:
            return do_order_status();
        case TPCC_PAYMENT:
            return do_payment();
        case TPCC_STOCK_LEVEL:
            return do_stock_level();
    }
    // should be unreachable
    return false;
}

tpcc_benchmark_worker_types inline benchmark_statistics
    tpcc_benchmark_worker_templ::get_statistics() const {
    return statistics_;
}

tpcc_benchmark_worker_types workload_operation_outcome_enum
                            tpcc_benchmark_worker_templ::do_delivery() {
    return WORKLOAD_OP_SUCCESS;
}
tpcc_benchmark_worker_types workload_operation_outcome_enum
                            tpcc_benchmark_worker_templ::do_new_order() {
    int32_t w_id = generator_.generate_warehouse_id();
    int32_t d_id = generator_.generate_district_id();
    int32_t c_id = generator_.generate_customer_id();

    int32_t ol_cnt = generator_.generate_number_order_lines();

    DVLOG( 20 ) << "New order generated: w_id:" << w_id << ", d_id:" << d_id
                << ", c_id:" << c_id << ", ol_cnt:" << ol_cnt;

    std::vector<int32_t> item_ids( ol_cnt );
    std::vector<int32_t> supplier_w_ids( ol_cnt, w_id );
    std::vector<int32_t> order_quantities( ol_cnt );

    int32_t all_local = 1;

    // populate
    for( int32_t ol_i = 0; ol_i < ol_cnt; ol_i++ ) {
        item_ids.at( ol_i ) = generator_.generate_item_id();
        if( generator_.is_distributed_new_order() ) {
            supplier_w_ids.at( ol_i ) =
                generator_.generate_warehouse_excluding( w_id );
            // otherwise we already set the warehouse
            all_local = 0;
        }
        order_quantities.at( ol_i ) = generator_.dist_.get_uniform_int( 1, 10 );
        DVLOG( 20 ) << "New order order_line:" << ol_i
                    << ", item_id:" << item_ids.at( ol_i )
                    << ", supplier_w_id:" << supplier_w_ids.at( ol_i )
                    << ", order_quantity:" << order_quantities.at( ol_i );
    }

    // 1 percent will abort
    if( generator_.dist_.get_uniform_int( 0, 99 ) == 1 ) {
        item_ids.at( ol_cnt - 1 ) = item::INVALID_ITEM_ID;
        DVLOG( 20 ) << "Expected to abort";
    }

    // call the actual new order worker
    workload_operation_outcome_enum result = perform_new_order_work(
        w_id, d_id, c_id, ol_cnt, all_local, item_ids.data(),
        supplier_w_ids.data(), order_quantities.data() );

    if( ( result == WORKLOAD_OP_SUCCESS ) and
        generator_.configs_.track_and_use_recent_items_ ) {

        generator_.add_recent_order_items( item_ids,
                                           stock::STOCK_LEVEL_ORDERS );
    }

    return result;
}

tpcc_benchmark_worker_types workload_operation_outcome_enum
                            tpcc_benchmark_worker_templ::perform_new_order_work(
        int32_t w_id, int32_t d_id, int32_t c_id, int32_t o_ol_cnt,
        int o_all_local, const int32_t* item_ids, const int32_t* supplier_w_ids,
        const int32_t* order_quantities ) {

    bool      is_abort = item_ids[o_ol_cnt - 1] == item::INVALID_ITEM_ID;
    district* d = fetch_and_set_next_order_on_district( w_id, d_id, is_abort );
    DCHECK( is_abort == ( d == nullptr ) );
    if( is_abort ) {
        return WORKLOAD_OP_EXPECTED_ABORT;
    }
    if (d == nullptr) {
        return WORKLOAD_OP_SUCCESS;
    }

    workload_operation_outcome_enum ret = place_new_orders(
        d->d_next_o_id - 1, c_id, d->d_id, d->d_w_id, o_ol_cnt, o_all_local,
        item_ids, supplier_w_ids, order_quantities );

    delete d;

    return ret;
}

tpcc_benchmark_worker_types district*
                            tpcc_benchmark_worker_templ::fetch_and_set_next_order_on_district(
        int32_t w_id, int32_t d_id, bool is_abort ) {

    DVLOG( 20 ) << "TPCC fetch and set next order on district, warehouse:"
                << w_id << ", district:" << d_id << ", is abort:" << is_abort;
    district* default_district = new district();
    default_district->d_id = d_id;
    default_district->d_w_id = w_id;
    default_district->d_next_o_id =
        generator_.configs_.num_customers_per_district_;

    cell_identifier cid = create_cell_identifier(
        k_tpcc_district_table_id, district_cols::d_next_o_id,
        make_district_key( *default_district, generator_.configs_ ) );
    auto ckr = cell_key_ranges_from_cell_identifier( cid );

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> ckrs = {ckr};
        db_operators_.begin_transaction( ckrs, ckrs, "NewOrder" );
    }
    if( db_operators_.abstraction_configs_.db_type_ ==
        db_abstraction_type::SS_DB ) {
        if( is_abort ) {
            RETURN_AND_COMMIT_IF_SS_DB( nullptr );
        }
        RETURN_AND_COMMIT_IF_SS_DB( default_district );
    }

    bool read_ok =
        lookup_district( &db_operators_, default_district, cid.key_, ckr,
                         true /*latest */, true /* allow nullable */ );
    if( !read_ok ) {
        loader_.generate_district( default_district, w_id, d_id );
    }

    default_district->d_next_o_id = default_district->d_next_o_id + 1;

    DVLOG( 20 ) << "Got district and next_o_id is:"
                << default_district->d_next_o_id;

    // this should be *always* be propagated
    update_district( &db_operators_, default_district, cid.key_, ckr,
                     true /* prop*/ );

    if( is_abort ) {
        if( do_begin_commit ) {
            db_operators_.abort_transaction();
        }
        delete default_district;
        return nullptr;
    }
    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    return default_district;
}

tpcc_benchmark_worker_types customer_district*
                            tpcc_benchmark_worker_templ::fetch_and_set_next_order_on_customer(
        int32_t w_id, int32_t d_id, int32_t c_id, bool is_abort ) {

    DVLOG( 20 ) << "TPCC fetch and set next order on customer, warehouse:"
                << w_id << ", district:" << d_id << ", customer:" << c_id
                << ", is abort:" << is_abort;
    customer_district* lookup_cd = new customer_district();
    lookup_cd->c_id = c_id;
    lookup_cd->c_d_id = d_id;
    lookup_cd->c_w_id = w_id;

    cell_identifier cid = create_cell_identifier(
        k_tpcc_customer_district_table_id, customer_district_cols::c_next_o_id,
        make_customer_district_key( *lookup_cd, generator_.configs_ ) );
    auto ckr = cell_key_ranges_from_cell_identifier( cid );

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> ckrs = {ckr};
        db_operators_.begin_transaction( ckrs, ckrs, "NewOrder" );
    }
    if( db_operators_.abstraction_configs_.db_type_ ==
        db_abstraction_type::SS_DB ) {
        if( is_abort ) {
            RETURN_AND_COMMIT_IF_SS_DB( nullptr );
        }

        lookup_cd->c_next_o_id =
            loader_.generate_customer_order_id_start( w_id, d_id, c_id );

        RETURN_AND_COMMIT_IF_SS_DB( lookup_cd );
    }

    bool read_ok = lookup_customer_district( &db_operators_, lookup_cd,
                                             cid.key_, ckr, true /*latest */,
                                             true /* allow nullable */ );
    if( !read_ok ) {
        loader_.generate_customer_district(
            lookup_cd, c_id, d_id, w_id,
            loader_.generate_customer_order_id_start( w_id, d_id, c_id ) );
    }

    lookup_cd->c_next_o_id = lookup_cd->c_next_o_id + 1;

    if( lookup_cd->c_next_o_id >=
        (int32_t) loader_.generate_customer_order_id_end( w_id, d_id, c_id ) ) {
        lookup_cd->c_next_o_id =
            loader_.generate_customer_order_id_start( w_id, d_id, c_id );
    }

    DVLOG( 20 ) << "Got district and next_o_id is:" << lookup_cd->c_next_o_id;

    // this should be *always* be propagated
    update_customer_district( &db_operators_, lookup_cd, cid.key_, ckr,
                              true /* prop */ );

    if( is_abort ) {
        if( do_begin_commit ) {
            db_operators_.abort_transaction();
        }
        delete lookup_cd;
        return nullptr;
    }
    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    return lookup_cd;
}

tpcc_benchmark_worker_types workload_operation_outcome_enum
                            tpcc_benchmark_worker_templ::place_new_orders(
        int32_t o_id, int32_t c_id, int32_t d_id, int32_t w_id,
        int32_t o_ol_cnt, int o_all_local, const int32_t* item_ids,
        const int32_t* supplier_w_ids, const int32_t* order_quantities ) {
    DVLOG( 20 ) << "TPCC place new orders, warehouse:" << w_id
                << ", district:" << d_id << ", customer:" << c_id
                << ", order_id:" << o_id;

    std::vector<cell_key_ranges> read_ckrs;
    std::vector<cell_key_ranges> write_ckrs;

    order     o;
    new_order new_o;
    populate_order( o, new_o, o_id, c_id, d_id, w_id, o_ol_cnt, o_all_local );

    uint64_t order_key = make_order_key( o, generator_.configs_ );
    auto order_ckr =
        create_cell_key_ranges( k_tpcc_order_table_id, order_key, order_key, 0,
                                k_tpcc_order_num_columns - 1 );
    uint64_t new_order_key = make_new_order_key( new_o, generator_.configs_ );
    auto     new_order_ckr = create_cell_key_ranges(
        k_tpcc_new_order_table_id, new_order_key, new_order_key, 0,
        k_tpcc_new_order_num_columns - 1 );

    write_ckrs.emplace_back( order_ckr );
    write_ckrs.emplace_back( new_order_ckr );

    std::vector<order_line> order_lines( o_ol_cnt );
    std::vector<stock>      stocks( o_ol_cnt );

    std::vector<record_identifier> order_line_rids( o_ol_cnt );
    std::vector<record_identifier> stock_rids( o_ol_cnt );

    if( generator_.configs_.use_warehouse_ ) {
        read_ckrs.emplace_back(
            cell_key_ranges_from_cell_identifier( create_cell_identifier(
                k_tpcc_warehouse_table_id, warehouse_cols::w_tax, w_id ) ) );
    }
    if( generator_.configs_.use_district_ ) {
        district d;
        d.d_id = d_id;
        d.d_w_id = w_id;
        read_ckrs.emplace_back(
            cell_key_ranges_from_cell_identifier( create_cell_identifier(
                k_tpcc_district_table_id, district_cols::d_tax,
                make_district_key( d, generator_.configs_ ) ) ) );
    }
    customer c;
    c.c_id = c_id;
    c.c_d_id = d_id;
    c.c_w_id = w_id;
    read_ckrs.emplace_back(
        cell_key_ranges_from_cell_identifier( create_cell_identifier(
            k_tpcc_customer_table_id, customer_cols::c_discount,
            make_customer_key( c, generator_.configs_ ) ) ) );
    for( int32_t ol_idx = 0; ol_idx < o_ol_cnt; ol_idx++ ) {
        read_ckrs.emplace_back( cell_key_ranges_from_cell_identifier(
            create_cell_identifier( k_tpcc_item_table_id, item_cols::i_price,
                                    (uint32_t) item_ids[ol_idx] ) ) );
    }

    populate_order_and_stock( order_line_rids, order_lines, stock_rids, stocks,
                              o_id, d_id, w_id, o_ol_cnt, item_ids,
                              supplier_w_ids, order_quantities );
    for( const auto& stock_rid : stock_rids ) {
        auto stock_ckr = create_cell_key_ranges(
            k_tpcc_stock_table_id, stock_rid.key_, stock_rid.key_,
            stock_cols::s_quantity, stock_cols::s_remote_cnt );
        auto stock_dist = create_cell_key_ranges(
            k_tpcc_stock_table_id, stock_rid.key_, stock_rid.key_,
            stock_cols::s_dist_0 + ( d_id - 1 ),
            stock_cols::s_dist_0 + ( d_id - 1 ) );

        read_ckrs.emplace_back( stock_ckr );
        write_ckrs.emplace_back( stock_ckr );
        write_ckrs.emplace_back( stock_dist );
    }

    auto order_line_ckrs = create_cell_key_ranges(
        k_tpcc_order_line_table_id, order_line_rids.at( 0 ).key_,
        order_line_rids.at( order_line_rids.size() - 1 ).key_, 0,
        k_tpcc_order_line_num_columns - 1 );

    write_ckrs.emplace_back( order_line_ckrs );

    if( do_begin_commit ) {
        db_operators_.add_partitions( generate_partition_column_identifiers(
            k_tpcc_order_line_table_id, order_line_ckrs.row_id_start,
            order_line_ckrs.row_id_end, 0, k_tpcc_order_line_num_columns - 1,
            data_sizes_ ) );
        db_operators_.add_partitions( generate_partition_column_identifiers(
            k_tpcc_order_table_id, order_key, order_key, 0,
            k_tpcc_order_num_columns - 1, data_sizes_ ) );
        db_operators_.add_partitions( generate_partition_column_identifiers(
            k_tpcc_new_order_table_id, new_order_key, new_order_key, 0,
            k_tpcc_new_order_num_columns - 1, data_sizes_ ) );

        db_operators_.begin_transaction( write_ckrs, read_ckrs, "NewOrder" );
    }
    if( db_operators_.abstraction_configs_.db_type_ ==
        db_abstraction_type::SS_DB ) {
    }
    RETURN_AND_COMMIT_IF_SS_DB( WORKLOAD_OP_SUCCESS );

    update_order( &db_operators_, &o, order_key, order_ckr,
                  false /* no propagate */ );
    update_new_order( &db_operators_, &new_o, new_order_key, new_order_ckr,
                      false /* no propagate */ );

    // don't free!
    float w_tax = lookup_warehouse_tax( w_id );
    float d_tax = lookup_district_tax( w_id, d_id );
    float c_discount = lookup_customer_discount( w_id, d_id, c_id );
    float total_order = 0;

    std::unordered_map<uint64_t, stock*> seen_stocks;

    for( int32_t ol_idx = 0; ol_idx < o_ol_cnt; ol_idx++ ) {
        total_order +=
            order_item( w_id, d_id, seen_stocks, order_lines.at( ol_idx ),
                        order_line_rids.at( ol_idx ), stocks.at( ol_idx ),
                        stock_rids.at( ol_idx ) );
    }
    total_order *= ( 1 + w_tax + d_tax ) * ( 1 - c_discount );

    new_order_write_stock( seen_stocks, d_id );

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    return WORKLOAD_OP_SUCCESS;
}
tpcc_benchmark_worker_types inline float
    tpcc_benchmark_worker_templ::lookup_warehouse_tax( int32_t w_id ) {
    float      tax = warehouse::warehouse::MIN_TAX;
    if( !generator_.configs_.use_warehouse_ ) {
        return tax;
    }
    warehouse w;
    w.w_id = w_id;

    cell_identifier cid = create_cell_identifier(
        k_tpcc_warehouse_table_id, warehouse_cols::w_tax, w.w_id );

    auto ckr = cell_key_ranges_from_cell_identifier( cid );

    bool read_ok =
        lookup_warehouse( &db_operators_, &w, cid.key_, ckr, false /*latest */,
                          true /* allow nullable */ );
    if( read_ok ) {
        return w.w_tax;
    }
    return tax;
}
tpcc_benchmark_worker_types inline float
    tpcc_benchmark_worker_templ::lookup_district_tax( int32_t w_id,
                                                      int32_t d_id ) {
    float tax = warehouse::warehouse::MIN_TAX;
    if( !generator_.configs_.use_district_ ) {
        return tax;
    }

    district d;
    d.d_id = d_id;
    d.d_w_id = w_id;

    cell_identifier cid =
        create_cell_identifier( k_tpcc_district_table_id, district_cols::d_tax,
                                make_district_key( d, generator_.configs_ ) );

    auto ckr = cell_key_ranges_from_cell_identifier( cid );

    bool read_ok =
        lookup_district( &db_operators_, &d, cid.key_, ckr, false /*latest */,
                         true /* allow nullable */ );
    if( read_ok ) {
        tax = d.d_tax;
    }
    return tax;
}

tpcc_benchmark_worker_types inline float
    tpcc_benchmark_worker_templ::lookup_customer_discount( int32_t w_id,
                                                           int32_t d_id,
                                                           int32_t c_id ) {
    customer c;
    c.c_id = c_id;
    c.c_d_id = d_id;
    c.c_w_id = w_id;

    cell_identifier cid = create_cell_identifier(
        k_tpcc_customer_table_id, customer_cols::c_discount,
        make_customer_key( c, generator_.configs_ ) );

    auto ckr = cell_key_ranges_from_cell_identifier( cid );

    bool read_ok =
        lookup_customer( &db_operators_, &c, cid.key_, ckr, false /*latest */,
                         true /* allow nullable */ );
    float discount = customer::MIN_DISCOUNT;
    if( read_ok ) {
        discount = c.c_discount;
    }
    return discount;
}

#if 0 // MTODO-TPCC
tpcc_benchmark_worker_types void
    tpcc_benchmark_worker_templ::new_order_order_items(
        int32_t w_id, int32_t d_id, int32_t o_id, float w_tax, float d_tax,
        float c_discount, uint32_t num_orders, int32_t* order_lines,
        int32_t* order_quantities, int32_t* item_ids, int32_t* supplier_w_ids,
        int32_t* stock_descr_lens, char* stock_descr ) {
    order_line o_line;
    o_line.ol_o_id = o_id;
    o_line.ol_d_id = d_id;
    o_line.ol_w_id = w_id;

    record_identifier o_line_rid;
    o_line_rid.table_id_ = k_tpcc_order_line_table_id;

    float   total_order = 0;
    int32_t stock_descr_start = 0;

    for( uint32_t pos = 0; pos < num_orders; pos++ ) {
        o_line.ol_number = order_lines[pos];
        o_line.ol_i_id = item_ids[pos];
        o_line.ol_quantity = order_quantities[pos];
        o_line.ol_supply_w_id = supplier_w_ids[pos];
        o_line_rid.key_ = make_order_line_key( o_line, generator_.configs_ );

        int32_t stock_descr_len = stock_descr_lens[pos];

        total_order +=
            new_order_order_item( o_line, o_line_rid, stock_descr_len,
                                  stock_descr + stock_descr_start );

        stock_descr_start += stock_descr_len;
    }

    total_order *= ( 1 + w_tax + d_tax ) * ( 1 - c_discount );
}

tpcc_benchmark_worker_types std::tuple<int32_t*, std::string>
                            tpcc_benchmark_worker_templ::new_order_update_stocks(
        int32_t order_w_id, int32_t order_d_id, uint32_t num_orders,
        const int32_t* item_ids, const int32_t* supplier_w_ids,
        const int32_t* order_quantities ) {

    std::unordered_map<uint64_t, stock*> seen_stocks;

    int32_t*    stock_descr_lens = new int32_t[num_orders];
    std::string stock_descrs;

    stock             s;
    record_identifier s_rid;
    s_rid.table_id_ = k_tpcc_stock_table_id;

    for( uint32_t pos = 0; pos < num_orders; pos++ ) {
        int32_t item_id = item_ids[pos];
        int32_t supplier_w_id = supplier_w_ids[pos];
        int32_t order_quantity = order_quantities[pos];

        s.s_i_id = item_id;
        s.s_w_id = supplier_w_id;
        s_rid.key_ = make_stock_key( s, generator_.configs_ );

        new_order_update_stock( order_w_id, supplier_w_id, order_quantity,
                                item_id, s_rid, seen_stocks );
        stock* write_stock = seen_stocks[s_rid.key_];
        stock_descr_lens[pos] = stock::MAX_DISTRICT_STR_LEN;
        stock_descrs.append( write_stock->s_dist[order_d_id - 1],
                             stock_descr_lens[pos] );
    }

    new_order_write_stock( seen_stocks, d_id );
    return std::make_tuple<int32_t*, std::string>(
        std::move( stock_descr_lens ), std::move( stock_descrs ) );
}
#endif

tpcc_benchmark_worker_types inline void
    tpcc_benchmark_worker_templ::new_order_write_stock(
        std::unordered_map<uint64_t, stock*>& seen_stocks, int32_t d_id ) {
    DVLOG( 20 ) << "Writing stock from orders";
    stock*   stock_to_write;
    uint64_t key;

    for( auto& stock_entry : seen_stocks ) {
        key = stock_entry.first;
        stock_to_write = stock_entry.second;
        auto stock_ckr = create_cell_key_ranges( k_tpcc_stock_table_id, key,
                                                 key, stock_cols::s_quantity,
                                                 stock_cols::s_remote_cnt );
        auto stock_dist =
            create_cell_key_ranges( k_tpcc_stock_table_id, key, key,
                                    stock_cols::s_dist_0 + ( d_id - 1 ),
                                    stock_cols::s_dist_0 + ( d_id - 1 ) );

        // no prop and limit?
        update_stock( &db_operators_, stock_to_write, key, stock_ckr,
                      false /* prop */ );
        update_stock( &db_operators_, stock_to_write, key, stock_dist,
                      false /* prop */ );

        delete stock_to_write;
    }
}

tpcc_benchmark_worker_types float tpcc_benchmark_worker_templ::order_item(
    int32_t w_id, int32_t                 d_id,
    std::unordered_map<uint64_t, stock*>& seen_stocks, order_line& o_line,
    record_identifier& o_line_rid, stock& s, record_identifier& s_rid ) {
    DVLOG( 20 ) << "Ordering item:" << o_line.ol_i_id << ","
                << " stock from warehouse:" << s.s_w_id;

    new_order_update_stock( w_id, o_line.ol_supply_w_id, o_line.ol_quantity,
                            o_line.ol_i_id, s_rid, seen_stocks );
    stock* write_stock = seen_stocks[s_rid.key_];

    float   amount = 0;
    int32_t loc_d_id = d_id - 1;
    if( loc_d_id == 0 ) {
        amount = new_order_order_item( o_line, o_line_rid,
                                       write_stock->s_dist_0 );
    } else if( loc_d_id == 1 ) {
        amount = new_order_order_item( o_line, o_line_rid,
                                       write_stock->s_dist_1 );
    } else if( loc_d_id == 2 ) {
        amount = new_order_order_item( o_line, o_line_rid,
                                       write_stock->s_dist_2 );
    } else if( loc_d_id == 3 ) {
        amount = new_order_order_item( o_line, o_line_rid,
                                       write_stock->s_dist_3 );
    } else if( loc_d_id == 4 ) {
        amount = new_order_order_item( o_line, o_line_rid,
                                       write_stock->s_dist_4 );
    } else if( loc_d_id == 5 ) {
        amount = new_order_order_item( o_line, o_line_rid,
                                       write_stock->s_dist_5 );
    } else if( loc_d_id == 6 ) {
        amount = new_order_order_item( o_line, o_line_rid,
                                       write_stock->s_dist_6 );
    } else if( loc_d_id == 7 ) {
        amount = new_order_order_item( o_line, o_line_rid,
                                       write_stock->s_dist_7 );
    } else if( loc_d_id == 8 ) {
        amount = new_order_order_item( o_line, o_line_rid,
                                       write_stock->s_dist_8 );
    } else if( loc_d_id == 9 ) {
        amount = new_order_order_item( o_line, o_line_rid,
                                       write_stock->s_dist_9 );
    }

    return amount;
}

tpcc_benchmark_worker_types float
    tpcc_benchmark_worker_templ::lookup_item_price( int32_t i_id ) {
    float i_price = item::MIN_PRICE;

    item i;
    i.i_id = i_id;

    cell_identifier cid = create_cell_identifier(
        k_tpcc_item_table_id, item_cols::i_price, (uint64_t) i.i_id );

    bool read_ok = lookup_item( &db_operators_, &i, cid.key_,
                                cell_key_ranges_from_cell_identifier( cid ),
                                false /*latest */, true /* allow nullable */ );
    if( read_ok ) {
        i_price = i.i_price;
    }

    return i_price;
}

tpcc_benchmark_worker_types inline float
    tpcc_benchmark_worker_templ::new_order_order_item(
        order_line& o_line, const record_identifier& o_line_rid,
        std::string& stock_descr ) {
    DVLOG( 20 ) << "Ordering item:" << o_line.ol_i_id;

    float i_price = lookup_item_price( o_line.ol_i_id );

    // set order_line
    float amount = o_line.ol_quantity * i_price;
    o_line.ol_amount = amount;
    stock_descr.assign( o_line.ol_dist_info );

    auto oline_ckr = create_cell_key_ranges(
        k_tpcc_order_line_table_id, o_line_rid.key_, o_line_rid.key_, 0,
        k_tpcc_order_line_num_columns - 1 );

    update_order_line( &db_operators_, &o_line, o_line_rid.key_, oline_ckr,
                       false /* prop*/ );

    return amount;
}

tpcc_benchmark_worker_types inline void
    tpcc_benchmark_worker_templ::new_order_update_stock(
        int32_t order_w_id, int32_t supplier_w_id, int32_t ol_quantity,
        int32_t item_id, const record_identifier& s_rid,
        std::unordered_map<uint64_t, stock*>&     seen_stocks ) {
    DVLOG( 20 ) << "Updating stock item:" << item_id
                << ", stock from warehouse:" << supplier_w_id;

    stock* write_stock = nullptr;

    auto stock_ckr =
        create_cell_key_ranges( k_tpcc_stock_table_id, s_rid.key_, s_rid.key_,
                                stock_cols::s_quantity, stock_cols::s_remote_cnt );

    auto found_stock = seen_stocks.find( s_rid.key_ );
    if( found_stock != seen_stocks.end() ) {
        // You can order the same item multiple times, but we do not handle
        // multiple writes in the database, so we must track this ourselves.
        DVLOG( 30 ) << "Duplicate write to stock item";
        write_stock = found_stock->second;
    } else {
        stock* read_stock = new stock();
        read_stock->s_i_id = item_id;
        read_stock->s_w_id = supplier_w_id;
        write_stock = read_stock;
        bool read_ok =
            lookup_stock( &db_operators_, read_stock, s_rid.key_, stock_ckr,
                          true /*latest */, true /* allow nullable */ );
        if( !read_ok ) {
            loader_.generate_stock( write_stock, item_id, supplier_w_id,
                                    false /*original*/ );
        }
        seen_stocks.emplace( s_rid.key_, write_stock );
    }

    // set stock
    int32_t stock_quantity = write_stock->s_quantity;
    if( stock_quantity - ol_quantity >= 10 ) {
        write_stock->s_quantity = stock_quantity - ol_quantity;
    } else {
        write_stock->s_quantity = -ol_quantity + 91;
    }

    write_stock->s_ytd = write_stock->s_ytd + ol_quantity;
    if( supplier_w_id != order_w_id ) {
        write_stock->s_remote_cnt = write_stock->s_remote_cnt + 1;
    }
}

#if 0
// don't free this it's just a pointer to an existing structure
tpcc_benchmark_worker_types inline customer*
    tpcc_benchmark_worker_templ::lookup_customer( int32_t w_id, int32_t d_id,
                                                  int32_t c_id ) {
    customer c;
    c.c_id = c_id;
    c.c_d_id = d_id;
    c.c_w_id = w_id;

    record_identifier rid = {k_tpcc_customer_table_id,
                             make_customer_key( c, generator_.configs_ )};
    return db_operators_.lookup_nullable_struct<customer>( rid );
}

tpcc_benchmark_worker_types inline item*
    tpcc_benchmark_worker_templ::lookup_item( int32_t i_id ) {
    record_identifier rid = {k_tpcc_item_table_id, (uint64_t) i_id};
    return db_operators_.lookup_nullable_struct<item>( rid );
}

tpcc_benchmark_worker_types inline warehouse*
    tpcc_benchmark_worker_templ::lookup_warehouse( int32_t w_id ) {
    record_identifier rid = {k_tpcc_warehouse_table_id, (uint64_t) w_id};
    return db_operators_.lookup_nullable_struct<warehouse>( rid );
}

tpcc_benchmark_worker_types inline district*
    tpcc_benchmark_worker_templ::lookup_district( int32_t w_id, int32_t d_id ) {
    district d;
    d.d_w_id = w_id;
    d.d_id = d_id;

    record_identifier rid = {k_tpcc_district_table_id,
                             make_district_key( d, generator_.configs_ )};
    return db_operators_.lookup_nullable_struct<district>( rid );
}
#endif

tpcc_benchmark_worker_types void tpcc_benchmark_worker_templ::populate_order(
    order& o, new_order& new_o, int32_t o_id, int32_t c_id, int32_t d_id,
    int32_t w_id, int32_t o_ol_cnt, int32_t o_all_local ) {

    o.o_id = o_id;
    o.o_c_id = c_id;
    o.o_d_id = d_id;
    o.o_w_id = w_id;
    o.o_carrier_id = order::NULL_CARRIER_ID;
    o.o_ol_cnt = o_ol_cnt;
    o.o_all_local = o_all_local;
    o.o_entry_d = get_current_time();

    new_o.no_w_id = w_id;
    new_o.no_d_id = d_id;
    new_o.no_o_id = o.o_id;
}

#if 0  // MTODO-TPCC
tpcc_benchmark_worker_types void
    tpcc_benchmark_worker_templ::new_order_set_order(
        int32_t o_id, int32_t w_id, int32_t d_id, int32_t c_id,
        int32_t o_ol_cnt, int32_t o_all_local ) {
    DVLOG( 20 ) << "Set order:" << o_id;

    order o;
    o.o_id = o_id;
    o.o_c_id = c_id;
    o.o_d_id = d_id;
    o.o_w_id = w_id;
    o.o_carrier_id = order::NULL_CARRIER_ID;
    o.o_ol_cnt = o_ol_cnt;
    o.o_all_local = o_all_local;
    o.o_entry_d = get_current_time();

    record_identifier o_rid = {k_tpcc_order_table_id,
                               make_order_key( o, generator_.configs_ )};

    // and limit
    db_operators_.write_struct_no_propagate<order>( o_rid, &o );
}

tpcc_benchmark_worker_types void
    tpcc_benchmark_worker_templ::new_order_set_new_order( int32_t o_id,
                                                          int32_t w_id,
                                                          int32_t d_id ) {
    DVLOG( 20 ) << "Set new_order:" << o_id;

    new_order new_o;
    new_o.no_o_id = o_id;
    new_o.no_d_id = d_id;
    new_o.no_w_id = w_id;

    record_identifier new_o_rid = {
        k_tpcc_new_order_table_id,
        make_new_order_key( new_o, generator_.configs_ )};

    db_operators_.write_struct<new_order>( new_o_rid, &new_o );
}
#endif

tpcc_benchmark_worker_types void
    tpcc_benchmark_worker_templ::populate_order_and_stock(
        std::vector<record_identifier>& order_line_rids,
        std::vector<order_line>&        order_lines,
        std::vector<record_identifier>& stock_rids, std::vector<stock>& stocks,
        int32_t o_id, int32_t d_id, int32_t w_id, int32_t o_ol_cnt,
        const int32_t* item_ids, const int32_t* supplier_w_ids,
        const int32_t* order_quantities ) {
    int32_t index = 0;
    for( int32_t ol_num = 1; ol_num <= o_ol_cnt; ol_num++ ) {
        index = ol_num - 1;
        order_line& o_line = order_lines.at( index );
        o_line.ol_o_id = o_id;
        o_line.ol_d_id = d_id;
        o_line.ol_w_id = w_id;
        o_line.ol_number = ol_num;
        o_line.ol_i_id = item_ids[index];
        o_line.ol_quantity = order_quantities[index];
        o_line.ol_supply_w_id = supplier_w_ids[index];

        stock& s = stocks.at( index );
        s.s_i_id = o_line.ol_i_id;
        s.s_w_id = o_line.ol_supply_w_id;

        uint64_t rid_key = make_order_line_key( o_line, generator_.configs_ );

        if( index > 0 ) {
            // they should be adjacent
            DCHECK_EQ( rid_key - 1, order_line_rids.at( index - 1 ).key_ );
        }

        order_line_rids.at( index ) = {k_tpcc_order_line_table_id, rid_key};

        stock_rids.at( index ) = {k_tpcc_stock_table_id,
                                  make_stock_key( s, generator_.configs_ )};
    }
}

tpcc_benchmark_worker_types workload_operation_outcome_enum
                            tpcc_benchmark_worker_templ::do_order_status() {
    return WORKLOAD_OP_SUCCESS;
}

tpcc_benchmark_worker_types workload_operation_outcome_enum
                            tpcc_benchmark_worker_templ::do_payment() {

    int32_t w_id = generator_.generate_warehouse_id();
    int32_t d_id = generator_.generate_district_id();
    int32_t c_id = generator_.generate_customer_id();

    int32_t c_w_id;
    int32_t c_d_id;

    if( generator_.is_distributed_payment() ) {
        c_w_id = generator_.generate_warehouse_excluding( w_id );
        c_d_id = generator_.generate_district_id();
    } else {
        // own warehouse
        c_w_id = w_id;
        c_d_id = w_id;
    }
    float h_amount = generator_.generate_payment_amount();

    // don't do look up by customer last name, just assume an id

    perform_payment_work( w_id, d_id, c_w_id, c_d_id, c_id, h_amount );
    workload_operation_outcome_enum result = WORKLOAD_OP_SUCCESS;

    return result;
}

tpcc_benchmark_worker_types workload_operation_enum
                            tpcc_benchmark_worker_templ::perform_payment_work(
        int32_t w_id, int32_t d_id, int32_t c_w_id, int32_t c_d_id,
        int32_t c_id, float h_amount ) {
    DVLOG( 20 ) << "Payment (w_id:" << w_id << ", d_id:" << d_id
                << ", c_w_id:" << c_w_id << ", c_d_id:" << c_d_id
                << ", c_id:" << c_id << ", h_amount:" << h_amount << ")";

    std::vector<cell_key_ranges> payment_ckrs = get_payment_ckrs(
        w_id, d_id, c_w_id, c_d_id, c_id, generator_.configs_ );

    std::vector<cell_key_ranges> ckrs;
    for( const auto& ckr : payment_ckrs ) {
        bool add = true;
        if( ( ckr.table_id == k_tpcc_district_table_id ) and
            ( !generator_.configs_.use_district_ ) ) {
            // add = false;
        }
        if( add ) {
            ckrs.push_back( ckr );
        }
    }

    if( do_begin_commit ) {
#if 0
    // write_struct<history>( txn_holder_, client_id_, h_rid, &h );
      db_operators_.add_partitions({ generate_partition_identifier( k_tpcc_history_table_id, rids.at( ).key_, data_sizes_)} );

#endif
        db_operators_.begin_transaction( ckrs, ckrs, "Payment"  );
    }
    RETURN_AND_COMMIT_IF_SS_DB( WORKLOAD_OP_SUCCESS );

// add h_amount to ytd to warehouse and district
// add to history table
#if 0
    add_payment_to_warehouse( w_id, h_amount);
#endif
    if( generator_.configs_.use_district_ ) {
        add_payment_to_district( w_id, d_id, h_amount );
    }
#if 0
    add_payment_to_history( w_id, d_id, c_w_id, c_d_id, c_id, h_amount );
#endif
    // update customer --- if bad credit then add to history
    add_payment_to_customer( w_id, d_id, c_w_id, c_d_id, c_id, h_amount );

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
    DVLOG( 20 ) << "Payment (w_id:" << w_id << ", d_id:" << d_id
                << ", c_w_id:" << c_w_id << ", c_d_id:" << c_d_id
                << ", c_id:" << c_id << ", h_amount:" << h_amount << ") okay!";

    return WORKLOAD_OP_SUCCESS;
}

tpcc_benchmark_worker_types void
    tpcc_benchmark_worker_templ::add_payment_to_warehouse( int32_t w_id,
                                                           float   h_amount ) {

    auto cid = create_cell_identifier( k_tpcc_warehouse_table_id,
                                       warehouse_cols::w_ytd, w_id );
    auto ckr = cell_key_ranges_from_cell_identifier( cid );

    DVLOG( 20 ) << "Adding payment to warehouse:" << w_id;

    warehouse w;

    bool read_ok =
        lookup_warehouse( &db_operators_, &w, cid.key_, ckr, true /*latest */,
                          true /* allow nullable */ );
    if( !read_ok ) {
        loader_.generate_warehouse( w, w_id );
    }

    w.w_ytd = w.w_ytd + h_amount;

    update_warehouse( &db_operators_, &w, cid.key_, ckr, true /* prop*/ );
}

tpcc_benchmark_worker_types void
    tpcc_benchmark_worker_templ::add_payment_to_district( int32_t w_id,
                                                          int32_t d_id,
                                                          float   h_amount ) {

    DVLOG( 20 ) << "Adding payment to warehouse:" << w_id
                << ", district:" << d_id;

    district d;
    d.d_w_id = w_id;
    d.d_id = d_id;

    auto cid =
        create_cell_identifier( k_tpcc_district_table_id, district_cols::d_ytd,
                                make_district_key( d, generator_.configs_ ) );
    auto ckr = cell_key_ranges_from_cell_identifier( cid );

    bool read_ok =
        lookup_district( &db_operators_, &d, cid.key_, ckr, true /*latest */,
                         true /* allow nullable */ );
    if( !read_ok ) {
        loader_.generate_district( &d, w_id, d_id );
    }
    d.d_ytd = d.d_ytd + h_amount;

    update_district( &db_operators_, &d, cid.key_, ckr, true /* prop*/ );
}

tpcc_benchmark_worker_types void
    tpcc_benchmark_worker_templ::add_payment_to_history(
        int32_t w_id, int32_t d_id, int32_t c_w_id, int32_t c_d_id,
        int32_t c_id, float h_amount ) {

    DVLOG( 20 ) << "Adding payment to history, warehouse:" << w_id
                << ", district:" << d_id << ", c_warehouse:" << c_w_id
                << ", c_district:" << c_d_id << ", c_id:" << c_id;

    history h;
    h.h_w_id = w_id;
    h.h_d_id = d_id;
    h.h_c_w_id = c_w_id;
    h.h_c_d_id = c_d_id;
    h.h_c_id = c_id;
    h.h_amount = h_amount;
    h.h_date = get_current_time();
    // h.h_data should be made from append of warehouse and district, but w/e

    uint64_t hist_key = make_history_key( h, generator_.configs_ );
    auto     ckr =
        create_cell_key_ranges( k_tpcc_history_table_id, hist_key, hist_key, 0,
                                k_tpcc_history_num_columns - 1 );

    update_history( &db_operators_, &h, hist_key, ckr, true /* prop*/ );
}

tpcc_benchmark_worker_types void
    tpcc_benchmark_worker_templ::add_payment_to_customer(
        int32_t w_id, int32_t d_id, int32_t c_w_id, int32_t c_d_id,
        int32_t c_id, float h_amount ) {

    DVLOG( 20 ) << "Adding payment to customer:" << c_id
                << ", warehouse:" << c_w_id << ", district:" << c_d_id;

    customer c;
    c.c_id = c_id;
    c.c_d_id = c_d_id;
    c.c_w_id = c_w_id;
    uint64_t cust_key = make_customer_key( c, generator_.configs_ );

    auto ckr = create_cell_key_ranges( k_tpcc_customer_table_id, cust_key,
                                       cust_key, customer_cols::c_balance,
                                       customer_cols::c_payment_cnt );

    bool read_ok =
        lookup_customer( &db_operators_, &c, cust_key, ckr, true /*latest */,
                         true /* allow nullable */ );

    if( !read_ok ) {
        loader_.generate_customer( &c, c_id, c_d_id, c_w_id,
                                   true /* good credit*/ );
    }

    c.c_balance = c.c_balance - h_amount;
    c.c_ytd_payment = c.c_ytd_payment + h_amount;
    c.c_payment_cnt = c.c_payment_cnt + 1;

    update_customer( &db_operators_, &c, cust_key, ckr, true /* prop */ );
}

tpcc_benchmark_worker_types workload_operation_outcome_enum
                            tpcc_benchmark_worker_templ::do_stock_level() {
    int32_t w_id = generator_.generate_warehouse_id();
    int32_t d_id = generator_.generate_district_id();
    int32_t stock_threshold = generator_.dist_.get_uniform_int(
        stock::MIN_STOCK_LEVEL_THRESHOLD, stock::MAX_STOCK_LEVEL_THRESHOLD );
    DVLOG( 20 ) << "Stock level generated: w_id:" << w_id << ", d_id:" << d_id
                << ", stock threshold:" << stock_threshold;

    std::vector<int32_t> seen_items;
    if( generator_.configs_.track_and_use_recent_items_ ) {
        seen_items = generator_.get_recent_item_ids();
    }

    // call the actual stock level worker
    int32_t num_distinct = perform_stock_level_work_with_recent_items(
        w_id, d_id, stock_threshold,
        generator_.configs_.track_and_use_recent_items_, seen_items );
    (void) num_distinct;

    return WORKLOAD_OP_SUCCESS;
}

tpcc_benchmark_worker_types int32_t
                            tpcc_benchmark_worker_templ::perform_stock_level_work(
        int32_t w_id, int32_t d_id, int32_t stock_threshold ) {
    std::vector<int32_t> recent_items;
    return perform_stock_level_work_with_recent_items(
        w_id, d_id, stock_threshold, false, recent_items );
}
tpcc_benchmark_worker_types int32_t
                            tpcc_benchmark_worker_templ::perform_stock_level_work_with_recent_items(
        int32_t w_id, int32_t d_id, int32_t stock_threshold,
        bool have_recent_items, const std::vector<int32_t>& recent_items ) {
    DVLOG( 20 ) << "Stock level (w_id:" << w_id << ", d_id:" << d_id
                << ", stock_threshold:" << stock_threshold << ")";

    std::vector<int32_t> seen_items;

    if( !have_recent_items ) {
        int32_t max_ol_num = generator_.configs_.max_num_order_lines_per_order_;
        int32_t max_o_id = get_max_order_id_to_look_back_on( w_id, d_id );
        int32_t min_o_id =
            std::max( 0, max_o_id - (int32_t) stock::STOCK_LEVEL_ORDERS );
        DVLOG( 20 ) << "Stock level (w_id:" << w_id << ", d_id:" << d_id
                    << ", stock_threshold:" << stock_threshold << ")"
                    << ", looking for orders in range[" << min_o_id << ", "
                    << max_o_id << "]";
        seen_items =
            get_recent_items( w_id, d_id, min_o_id, max_o_id, max_ol_num );
    } else {
        seen_items = recent_items;
    }

    std::vector<int32_t> stock_below_threshold = get_stock_below_threshold(
        w_id, stock_threshold, seen_items.data(), seen_items.size() );

    // the return value of the equivalent sql
    int32_t num_distinct = stock_below_threshold.size();
    DVLOG( 20 ) << "Stock level (w_id:" << w_id << ", d_id:" << d_id
                << ", stock_threshold:" << stock_threshold
                << ") produced:" << num_distinct;
    return num_distinct;
}

tpcc_benchmark_worker_types std::vector<int32_t>
    tpcc_benchmark_worker_templ::get_recent_items( int32_t w_id, int32_t d_id,
                                                   int32_t min_o_id,
                                                   int32_t max_o_id,
                                                   int32_t max_ol_num ) {
    DVLOG( 20 ) << "Get recent items: w_id:" << w_id << ", d_id:" << d_id
                << ", min_o_id:" << min_o_id << ", max_o_id:" << max_o_id;

    std::vector<cell_key_ranges> read_ckrs = {create_cell_key_ranges(
        k_tpcc_order_line_table_id,
        make_w_d_o_ol_key( 1, min_o_id, d_id, w_id, generator_.configs_ ),
        make_w_d_o_ol_key( max_ol_num, max_o_id, d_id, w_id,
                           generator_.configs_ ),
        order_line_cols::ol_i_id, order_line_cols::ol_i_id )};

    return get_recent_items_by_ckrs( read_ckrs );
}
tpcc_benchmark_worker_types std::vector<int32_t>
                            tpcc_benchmark_worker_templ::get_recent_items_by_ckrs(
        const std::vector<cell_key_ranges>& read_ckrs ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> ckrs = read_ckrs;
        db_operators_.begin_scan_transaction( ckrs );
    }
    std::vector<int32_t>        items;
    std::unordered_set<int32_t> seen_items;

    if( db_operators_.abstraction_configs_.db_type_ ==
        db_abstraction_type::SS_DB ) {
        for( int32_t i = 1; i <= 5; i++ ) {
            items.push_back( i );
        }
        RETURN_AND_COMMIT_IF_SS_DB( items );
    }

    // for each order, look at each ordered item
    order_line o_line;

    for( const auto& ckr : read_ckrs ) {
        for( int64_t row_id = ckr.row_id_start; row_id <= ckr.row_id_end;
             row_id++ ) {
            DCHECK_GE( order_line_cols::ol_i_id, ckr.col_id_start );
            DCHECK_LE( order_line_cols::ol_i_id, ckr.col_id_end );
            cell_identifier cid = create_cell_identifier(
                k_tpcc_order_line_table_id, order_line_cols::ol_i_id, row_id );
            bool read_ok = lookup_order_line( &db_operators_, &o_line, cid.key_,
                                              ckr, false /*latest */,
                                              true /* allow nullable */ );

            if( !read_ok ) {
                // htere are no more order lines so break
                continue;
            }
            int32_t item_id = o_line.ol_i_id;
            DVLOG( 30 ) << "Stock level looking at item:" << item_id;
            if( seen_items.count( item_id ) == 0 ) {
                seen_items.emplace( item_id );
                items.emplace_back( item_id );
            }
        }
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    return items;
}

tpcc_benchmark_worker_types int32_t
                            tpcc_benchmark_worker_templ::get_max_order_id_to_look_back_on(
        int32_t w_id, int32_t d_id ) {
    DVLOG( 30 ) << "Considering max order id for warehouse:" << w_id
                << ", district:" << d_id;

    district d;
    d.d_id = d_id;
    d.d_w_id = w_id;
    auto cid = create_cell_identifier(
        k_tpcc_district_table_id, district_cols::d_next_o_id,
        make_district_key( d, generator_.configs_ ) );
    auto ckr = cell_key_ranges_from_cell_identifier( cid );

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> write_ckrs;
        std::vector<cell_key_ranges> read_ckrs = {ckr};
        db_operators_.begin_transaction( write_ckrs, read_ckrs, "StockLevel" );
    }
    RETURN_AND_COMMIT_IF_SS_DB(
        generator_.configs_.num_customers_per_district_ );

    int32_t max_order_id = 0;

    bool read_ok =
        lookup_district( &db_operators_, &d, cid.key_, ckr, false /*latest */,
                         true /* allow nullable */ );
    if( read_ok ) {
        max_order_id = d.d_next_o_id;
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 30 ) << "Max order id for warehouse:" << w_id
                << ", district:" << d_id << ", is order id:" << max_order_id;

    return max_order_id;
}

tpcc_benchmark_worker_types std::vector<int32_t>
                            tpcc_benchmark_worker_templ::get_stock_below_threshold(
        int32_t w_id, int32_t stock_threshold, int32_t* item_ids,
        uint32_t item_id_count ) {

    DVLOG( 20 ) << "Get stock below threshold: w_id:" << w_id;
    std::vector<int32_t> stocks_below;

    scan_arguments scan_arg;
    scan_arg.label = 0;

    for( uint32_t pos = 0; pos < item_id_count; pos++ ) {
        scan_arg.read_ckrs.push_back(
            cell_key_ranges_from_cell_identifier( create_cell_identifier(
                k_tpcc_stock_table_id, stock_cols::s_quantity,
                make_w_s_key( w_id, item_ids[pos], generator_.configs_ ) ) ) );
    }

    cell_predicate c_pred;
    c_pred.table_id = k_tpcc_stock_table_id;
    c_pred.col_id = stock_cols::s_quantity;
    c_pred.type = data_type::type::INT64;
    c_pred.data = int64_to_string( stock_threshold );
    c_pred.predicate = predicate_type::type::LESS_THAN;

    predicate_chain pred;
    pred.and_predicates.emplace_back( c_pred );

    scan_arg.predicate = pred;

    std::vector<scan_arguments> scan_args = {scan_arg};

    scan_result scan_res;
    get_stock_below_threshold_by_scan( scan_args, scan_res );

    std::vector<int32_t> items;
    for( const auto& res : scan_res.res_tuples ) {
        for( const auto& res_entry : res.second ) {
            items.emplace_back( get_item_from_key( k_tpcc_stock_table_id,
                                                   res_entry.row_id,
                                                   generator_.configs_ ) );
        }
    }

    return items;
}

tpcc_benchmark_worker_types void
    tpcc_benchmark_worker_templ::get_stock_below_threshold_by_scan(
        const std::vector<scan_arguments>& scan_args, scan_result& scan_res ) {

    if( do_begin_commit ) {
        std::vector<cell_key_ranges> write_ckrs;
        std::vector<cell_key_ranges> read_ckrs;
        for( const auto& scan_arg : scan_args ) {
            read_ckrs.insert( read_ckrs.end(), scan_arg.read_ckrs.begin(),
                              scan_arg.read_ckrs.end() );
        }
        db_operators_.begin_scan_transaction( read_ckrs );
    }
    RETURN_VOID_AND_COMMIT_IF_SS_DB();

    std::vector<uint32_t> proj_cols = {stock_cols::s_quantity};

    for( const auto& scan_arg : scan_args ) {
        std::vector<result_tuple> loc_res;
        for( const auto& ckr : scan_arg.read_ckrs ) {
            db_operators_.scan( ckr.table_id, ckr.row_id_start, ckr.row_id_end,
                                proj_cols, scan_arg.predicate, loc_res );
        }
        scan_res.res_tuples[scan_arg.label] = loc_res;
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }
}

tpcc_benchmark_worker_types inline bool
    tpcc_benchmark_worker_templ::is_stock_level_below_threshold(
        int32_t w_id, int32_t i_id, int32_t stock_threshold ) {
    stock s;
    s.s_i_id = i_id;
    s.s_w_id = w_id;

    cell_identifier cid =
        create_cell_identifier( k_tpcc_stock_table_id, stock_cols::s_quantity,
                                make_stock_key( s, generator_.configs_ ) );
    auto stock_ckr = cell_key_ranges_from_cell_identifier( cid );

    bool below_threshold = false;
    bool read_ok = lookup_stock( &db_operators_, &s, cid.key_, stock_ckr,
                                 false /*latest */, true /* allow nullable */ );

    if( read_ok ) {
        DVLOG( 30 ) << "Stock level looking at item:" << i_id
                    << ", has stock level:" << s.s_quantity;
        if( s.s_quantity < stock_threshold ) {
            DVLOG( 30 ) << "Stock level, found stock item below "
                           "threshold:"
                        << i_id;
            below_threshold = true;
        }
    }
    return below_threshold;
}

tpcc_benchmark_worker_types void
    tpcc_benchmark_worker_templ::set_transaction_partition_holder(
        transaction_partition_holder* txn_holder ) {
    db_operators_.set_transaction_partition_holder( txn_holder );
}

