#pragma once

#include <glog/logging.h>

#include "../../common/string_utils.h"
#include "../benchmark_interface.h"
#include "tpcc_db_operators.h"
#include "tpcc_table_ids.h"

// Based on
// https://github.com/evanj/tpccbench/blob/715f394ade83e545ffc0aa79c14fefc1c0987f21/tpccgenerator.cc

tpcc_loader_types tpcc_loader_templ::tpcc_loader(
    db_abstraction* db, const tpcc_configs& configs,
    const db_abstraction_configs& abstraction_configs, uint32_t client_id )
    : db_operators_( client_id, db, abstraction_configs,
                     configs.bench_configs_.limit_update_propagation_,
                     true /* store global state */ ),
      configs_( configs ),
      data_sizes_( get_tpcc_data_sizes( configs ) ),
      dist_( nullptr /* no need for zipf*/ ),
      nu_dist_(),
      now_( get_current_time() ) {}

tpcc_loader_types tpcc_loader_templ::~tpcc_loader() {}

tpcc_loader_types void tpcc_loader_templ::make_items_partitions() {
    db_operators_.add_partitions( generate_partition_column_identifiers(
        k_tpcc_item_table_id, 0, configs_.num_items_ - 1, 0,
        k_tpcc_item_num_columns - 1, data_sizes_ ) );
}

tpcc_loader_types void tpcc_loader_templ::make_nation_related_partitions() {

    if( configs_.h_num_clients_ <= 0 ) {
        return;
    }

    db_operators_.add_partitions( generate_partition_column_identifiers(
        k_tpcc_region_table_id, 0, region::NUM_REGIONS - 1, 0,
        k_tpcc_region_num_columns - 1, data_sizes_ ) );
    db_operators_.add_partitions( generate_partition_column_identifiers(
        k_tpcc_nation_table_id, 0, nation::NUM_NATIONS - 1, 0,
        k_tpcc_nation_num_columns - 1, data_sizes_ ) );

    db_operators_.add_partitions( generate_partition_column_identifiers(
        k_tpcc_supplier_table_id, 0, configs_.num_suppliers_ - 1, 0,
        k_tpcc_supplier_num_columns - 1, data_sizes_ ) );
}

tpcc_loader_types void tpcc_loader_templ::make_warehouse_related_partitions(
    uint32_t w_id ) {

    if( configs_.c_num_clients_ > 0 ) {
        db_operators_.add_partitions( generate_partition_column_identifiers(
            k_tpcc_warehouse_table_id, w_id, w_id, 0,
            k_tpcc_warehouse_num_columns - 1, data_sizes_ ) );
    }
    if( configs_.c_num_clients_ > 0 ) {
        db_operators_.add_partitions( generate_partition_column_identifiers(
            k_tpcc_district_table_id, make_w_d_key( 0, w_id, configs_ ),
            make_w_d_key( configs_.num_districts_per_warehouse_ - 1, w_id,
                          configs_ ),
            0, k_tpcc_district_num_columns - 1, data_sizes_ ) );
    }

    db_operators_.add_partitions( generate_partition_column_identifiers(
        k_tpcc_stock_table_id, make_w_s_key( w_id, 0, configs_ ),
        make_w_s_key( w_id, configs_.num_items_ - 1, configs_ ), 0,
        k_tpcc_stock_num_columns - 1, data_sizes_ ) );

    for( uint32_t d_id = 0; d_id < configs_.num_districts_per_warehouse_;
         d_id++ ) {
        db_operators_.add_partitions( generate_partition_column_identifiers(
            k_tpcc_customer_table_id, make_w_d_c_key( 0, d_id, w_id, configs_ ),
            make_w_d_c_key( configs_.num_customers_per_district_ - 1, d_id,
                            w_id, configs_ ),
            0, k_tpcc_customer_num_columns - 1, data_sizes_ ) );

        if( configs_.c_num_clients_ > 0 ) {
            db_operators_.add_partitions( generate_partition_column_identifiers(
                k_tpcc_history_table_id,
                make_w_d_c_key( 0, d_id, w_id, configs_ ),
                make_w_d_c_key( configs_.num_customers_per_district_ - 1, d_id,
                                w_id, configs_ ),
                0, k_tpcc_history_num_columns - 1, data_sizes_ ) );
            db_operators_.add_partitions( generate_partition_column_identifiers(
                k_tpcc_customer_district_table_id,
                make_w_d_c_key( 0, d_id, w_id, configs_ ),
                make_w_d_c_key( configs_.num_customers_per_district_ - 1, d_id,
                                w_id, configs_ ),
                0, k_tpcc_customer_district_num_columns - 1, data_sizes_ ) );
        }
    }

    /*
    make_customers_and_history( w_id );
    make_orders( w_id );
    */
}

tpcc_loader_types void tpcc_loader_templ::make_items_table() {
    uint32_t batch_size = configs_.partition_size_;

    uint64_t cur_start = 0;
    uint64_t cur_end = 0;
    uint64_t end = configs_.num_items_;
    while( cur_start < end ) {
        cur_end = std::min( end - 1, cur_start + batch_size );
        cell_key_ranges ckr =
            create_cell_key_ranges( k_tpcc_item_table_id, cur_start, cur_end, 0,
                                    k_tpcc_item_num_columns - 1 );
        load_item_range( ckr );
        cur_start = cur_end + 1;
    }
}

tpcc_loader_types void tpcc_loader_templ::make_nation_related_tables() {
    uint32_t batch_size = configs_.partition_size_;

    cell_key_ranges region_ckr = create_cell_key_ranges(
        k_tpcc_region_table_id, 0, region::NUM_REGIONS - 1, 0,
        k_tpcc_region_num_columns - 1 );
    load_region_range( region_ckr );

    cell_key_ranges nation_ckr = create_cell_key_ranges(
        k_tpcc_nation_table_id, 0, nation::NUM_NATIONS - 1, 0,
        k_tpcc_nation_num_columns - 1 );
    load_nation_range( nation_ckr );

    uint64_t cur_start = 0;
    uint64_t cur_end = 0;
    uint64_t end = configs_.num_suppliers_;
    while( cur_start < end ) {
        cur_end = std::min( end - 1, cur_start + batch_size );
        cell_key_ranges ckr = create_cell_key_ranges(
            k_tpcc_supplier_table_id, cur_start, cur_end, 0,
            k_tpcc_supplier_num_columns - 1 );
        load_supplier_range( ckr );
        cur_start = cur_end + 1;
    }
}

tpcc_loader_types void tpcc_loader_templ::load_item_range(
    const cell_key_ranges& ckr ) {
    DVLOG( 20 ) << "Loading item range:" << ckr;
    std::vector<cell_key_ranges> ckrs = {ckr};
    if( do_begin_commit ) {
        std::vector<cell_key_ranges> empty_ckrs;
        db_operators_.begin_transaction( ckrs, empty_ckrs );
    }
    RETURN_VOID_AND_COMMIT_IF_SS_DB();

    for( int64_t key = ckr.row_id_start; key <= ckr.row_id_end; key++) {
        load_item( (uint64_t) key, ckr );
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "Loading item range:" << ckr << " okay!";
}

tpcc_loader_types void tpcc_loader_templ::load_item(
    uint64_t id, const cell_key_ranges& ckr ) {
    cell_identifier cid =
        create_cell_identifier( k_tpcc_item_table_id, ckr.col_id_start, id );
    bool    original;
    int32_t rand_pct;
    item    i;
    rand_pct = dist_.get_uniform_int( 0, 100 );
    original = ( rand_pct > 90 );

    generate_item( &i, cid.key_, original );
    DVLOG( 30 ) << "Loading item i_id:" << i.i_id;

    insert_item( &db_operators_, &i, id, ckr );
}

tpcc_loader_types void tpcc_loader_templ::load_region_range(
    const cell_key_ranges& ckr ) {
    DVLOG( 20 ) << "Loading region range:" << ckr;
    std::vector<cell_key_ranges> ckrs = {ckr};
    if( do_begin_commit ) {
        std::vector<cell_key_ranges> empty_ckrs;
        db_operators_.begin_transaction( ckrs, empty_ckrs );
    }
    RETURN_VOID_AND_COMMIT_IF_SS_DB();

    for( int64_t key = ckr.row_id_start; key <= ckr.row_id_end; key++) {
        load_region( (uint64_t) key, ckr );
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "Loading region range:" << ckr << " okay!";
}

tpcc_loader_types void tpcc_loader_templ::load_region(
    uint64_t id, const cell_key_ranges& ckr ) {
    cell_identifier cid =
        create_cell_identifier( k_tpcc_region_table_id, ckr.col_id_start, id );
    region  r;

    generate_region( &r, cid.key_ );
    DVLOG( 30 ) << "Loading region r_id:" << r.r_id;

    insert_region( &db_operators_, &r, cid.key_, ckr );
}

tpcc_loader_types void tpcc_loader_templ::load_nation_range(
    const cell_key_ranges& ckr ) {
    DVLOG( 20 ) << "Loading nation range:" << ckr;
    std::vector<cell_key_ranges> ckrs = {ckr};
    if( do_begin_commit ) {
        std::vector<cell_key_ranges> empty_ckrs;
        db_operators_.begin_transaction( ckrs, empty_ckrs );
    }
    RETURN_VOID_AND_COMMIT_IF_SS_DB();

    for( int64_t key = ckr.row_id_start; key <= ckr.row_id_end; key++) {
        load_nation( (uint64_t) key, ckr );
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "Loading nation range:" << ckr << " okay!";
}

tpcc_loader_types void tpcc_loader_templ::load_nation(
    uint64_t id, const cell_key_ranges& ckr ) {
    cell_identifier cid =
        create_cell_identifier( k_tpcc_nation_table_id, ckr.col_id_start, id );
    nation  n;

    generate_nation( &n, cid.key_ );
    DVLOG( 30 ) << "Loading nation n_id:" << n.n_id;

    insert_nation( &db_operators_, &n, cid.key_, ckr );
}

tpcc_loader_types void tpcc_loader_templ::load_supplier_range(
    const cell_key_ranges& ckr ) {
    DVLOG( 20 ) << "Loading supplier range:" << ckr;
    std::vector<cell_key_ranges> ckrs = {ckr};
    if( do_begin_commit ) {
        std::vector<cell_key_ranges> empty_ckrs;
        db_operators_.begin_transaction( ckrs, empty_ckrs );
    }
    RETURN_VOID_AND_COMMIT_IF_SS_DB();

    for( int64_t key = ckr.row_id_start; key <= ckr.row_id_end; key++) {
        load_supplier( (uint64_t) key, ckr );
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "Loading supplier range:" << ckr << " okay!";
}

tpcc_loader_types void tpcc_loader_templ::load_supplier(
    uint64_t id, const cell_key_ranges& ckr ) {
    cell_identifier cid = create_cell_identifier( k_tpcc_supplier_table_id,
                                                  ckr.col_id_start, id );
    supplier s;

    generate_supplier( &s, cid.key_ );
    DVLOG( 30 ) << "Loading supplier s_id:" << s.s_id;

    insert_supplier( &db_operators_, &s, id, ckr );
}

tpcc_loader_types std::string tpcc_loader_templ::set_original(
    const std::string& s ) {

    std::string ret = s;
    uint32_t    pos = dist_.get_uniform_int( 0, s.length() - 8 );
    ret.replace( pos, 8, "ORIGINAL" );
    return ret;
}

tpcc_loader_types void tpcc_loader_templ::generate_item( item* i, uint64_t id,
                                                         bool is_original ) {
    i->i_id = (int32_t) id;
    i->i_im_id = dist_.get_uniform_int( item::MIN_IM, item::MAX_IM );
    i->i_price =
        dist_.get_uniform_double_in_range( item::MIN_PRICE, item::MAX_PRICE );
    i->i_name =
        dist_.write_uniform_str( item::MIN_NAME_LEN, item::MAX_NAME_LEN );
    i->i_data =
        dist_.write_uniform_str( item::MIN_DATA_LEN, item::MAX_DATA_LEN );
    if( is_original ) {
        i->i_data = set_original( i->i_data );
    }
}

tpcc_loader_types void tpcc_loader_templ::generate_address( address* a ) {
    a->a_street_1 = dist_.write_uniform_str( address::MIN_STREET_LEN,
                                             address::MAX_STREET_LEN );
    a->a_street_2 = dist_.write_uniform_str( address::MIN_STREET_LEN,
                                             address::MAX_STREET_LEN );
    a->a_city =
        dist_.write_uniform_str( address::MIN_CITY_LEN, address::MAX_CITY_LEN );
    a->a_state =
        dist_.write_uniform_str( address::STATE_LEN, address::STATE_LEN );

    // spec says do rand + 11111
    a->a_zip = dist_.write_uniform_nstr( 4, 4 );
    a->a_zip.append( "11111" );
}

tpcc_loader_types void tpcc_loader_templ::make_warehouse_related_tables(
    uint32_t w_id ) {
    if( configs_.c_num_clients_ > 0 ) {
        make_warehouse( w_id, create_cell_key_ranges(
                                  k_tpcc_warehouse_table_id, w_id, w_id, 0,
                                  k_tpcc_warehouse_num_columns - 1 ) );
    }
    make_stock( w_id, 0, configs_.num_items_ - 1,
                create_cell_key_ranges(
                    k_tpcc_stock_table_id, make_w_s_key( w_id, 0, configs_ ),
                    make_w_s_key( w_id, configs_.num_items_ - 1, configs_ ), 0,
                    k_tpcc_stock_num_columns - 1 ) );
    if( configs_.c_num_clients_ > 0 ) {
        make_districts(
            w_id, 0, configs_.num_districts_per_warehouse_ - 1,
            create_cell_key_ranges(
                k_tpcc_district_table_id, make_w_d_key( 0, w_id, configs_ ),
                make_w_d_key( configs_.num_districts_per_warehouse_ - 1, w_id,
                              configs_ ),
                0, k_tpcc_district_num_columns - 1 ) );
    }
    make_customers_and_history( w_id, 0,
                                configs_.num_districts_per_warehouse_ - 1 );
    make_orders( w_id );
}

tpcc_loader_types void tpcc_loader_templ::make_warehouse(
    uint32_t w_id, const cell_key_ranges& ckr ) {
    DVLOG( 30 ) << "Loading warehouse:" << w_id << ", CKR:" << ckr;

    std::vector<cell_key_ranges> ckrs = {ckr};
    if( do_begin_commit ) {
        std::vector<cell_key_ranges> empty_ckrs;
        db_operators_.begin_transaction( ckrs, empty_ckrs );
    }
    RETURN_VOID_AND_COMMIT_IF_SS_DB();

    warehouse w;
    generate_warehouse( &w, w_id );

    insert_warehouse( &db_operators_, &w, w_id, ckr );

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "Loading warehouse:" << w_id << " okay!";
}

tpcc_loader_types float tpcc_loader_templ::generate_tax() {
    float tax = dist_.get_uniform_double_in_range( warehouse::MIN_TAX,
                                                   warehouse::MAX_TAX );
    return tax;
}

tpcc_loader_types void tpcc_loader_templ::generate_warehouse( warehouse* w,
                                                              uint32_t w_id ) {
    w->w_id = (int32_t) w_id;
    w->w_tax = generate_tax();
    w->w_ytd = warehouse::INITIAL_YTD;

    w->w_name = dist_.write_uniform_str( warehouse::MIN_NAME_LEN,
                                         warehouse::MAX_NAME_LEN );

    generate_address( &( w->w_address ) );
}

tpcc_loader_types void tpcc_loader_templ::make_districts(
    uint32_t w_id, uint32_t d_id_start, uint32_t d_id_end,
    const cell_key_ranges& ckr ) {
    DVLOG( 20 ) << "Loading districts for warehouse:" << w_id;

    std::vector<cell_key_ranges> ckrs = {ckr};
    if( do_begin_commit ) {
        std::vector<cell_key_ranges> empty_ckrs;
        db_operators_.begin_transaction( ckrs, empty_ckrs );
    }
    RETURN_VOID_AND_COMMIT_IF_SS_DB();

    for( uint32_t d_id = d_id_start; d_id <= d_id_end; d_id++ ) {
        make_district( w_id, d_id, ckr );
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "Loading districts for warehouse:" << w_id << " okay!";
}

tpcc_loader_types void tpcc_loader_templ::make_district(
    uint32_t w_id, uint32_t d_id, const cell_key_ranges& ckr ) {
    district d;
    DVLOG( 30 ) << "Loading district w_id:" << w_id << ", d_id:" << d_id;
    generate_district( &d, w_id, d_id );
    insert_district( &db_operators_, &d, make_district_key( d, configs_ ),
                     ckr );
}

tpcc_loader_types void tpcc_loader_templ::generate_district( district* d,
                                                             uint32_t  w_id,
                                                             uint32_t  d_id ) {
    d->d_id = (int32_t) d_id;
    d->d_w_id = (int32_t) w_id;
    d->d_tax = generate_tax();
    d->d_ytd = district::INITIAL_YTD;
    // we insert 0 - num_customers_per_district_ - 1
    d->d_next_o_id = configs_.num_customers_per_district_;
    d->d_name = dist_.write_uniform_str( district::MIN_NAME_LEN,
                                         district::MAX_NAME_LEN );

    generate_address( &( d->d_address ) );
}

tpcc_loader_types void tpcc_loader_templ::make_customers_and_history(
    uint32_t w_id, uint32_t d_id_start, uint32_t d_id_end ) {
    DVLOG( 20 ) << "Loading customers and history for warehouse:" << w_id;

    uint32_t batch_size = configs_.partition_size_;

    for( uint32_t d_id = d_id_start; d_id <= d_id_end; d_id++ ) {

        uint64_t cur_start = 0;
        uint64_t cur_end = 0;
        uint64_t end = configs_.num_customers_per_district_;
        while( cur_start < end ) {
            cur_end = std::min( end - 1, cur_start + batch_size );
            load_customer_range(
                w_id, d_id, cur_start, cur_end,
                create_cell_key_ranges(
                    k_tpcc_customer_table_id,
                    make_w_d_c_key( cur_start, d_id, w_id, configs_ ),
                    make_w_d_c_key( cur_end, d_id, w_id, configs_ ), 0,
                    k_tpcc_customer_num_columns - 1 ) );
            if( configs_.c_num_clients_ > 0 ) {
                load_history_range(
                    w_id, d_id, cur_start, cur_end,
                    create_cell_key_ranges(
                        k_tpcc_history_table_id,
                        make_w_d_c_key( cur_start, d_id, w_id, configs_ ),
                        make_w_d_c_key( cur_end, d_id, w_id, configs_ ), 0,
                        k_tpcc_history_num_columns - 1 ) );

                load_customer_district_range(
                    w_id, d_id, cur_start, cur_end,
                    create_cell_key_ranges(
                        k_tpcc_customer_district_table_id,
                        make_w_d_c_key( cur_start, d_id, w_id, configs_ ),
                        make_w_d_c_key( cur_end, d_id, w_id, configs_ ), 0,
                        k_tpcc_customer_district_num_columns - 1 ) );
            }
            cur_start = cur_end + 1;
        }
    }

    DVLOG( 20 ) << "Loading customers and history for warehouse:" << w_id
                << " okay!";
}

tpcc_loader_types void tpcc_loader_templ::load_customer_range(
    uint32_t w_id, uint32_t d_id, uint64_t start, uint64_t end,
    const cell_key_ranges& ckr ) {
    DVLOG( 20 ) << "Loading customer range for warehouse: " << w_id
                << ", district:" << d_id << start << " to " << end;

    customer c_start;
    c_start.c_id = (int32_t) start;
    c_start.c_d_id = (int32_t) d_id;
    c_start.c_w_id = (int32_t) w_id;

    customer c_end;
    c_end.c_id = (int32_t) end;
    c_end.c_d_id = (int32_t) d_id;
    c_end.c_w_id = (int32_t) w_id;

    std::vector<cell_key_ranges> ckrs = {ckr};
    if( do_begin_commit ) {
        std::vector<cell_key_ranges> empty_ckrs;
        db_operators_.begin_transaction( ckrs, empty_ckrs );
    }
    RETURN_VOID_AND_COMMIT_IF_SS_DB();

    for( uint64_t c_id = start; c_id <= end; c_id++ ) {
        load_customer( w_id, d_id, (uint32_t) c_id, ckr );
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "Loading customer range for warehouse: " << w_id
                << ", district:" << d_id << start << " to " << end << " okay!";
}

tpcc_loader_types void tpcc_loader_templ::load_customer_district_range(
    uint32_t w_id, uint32_t d_id, uint64_t start, uint64_t end,
    const cell_key_ranges& ckr ) {
    DVLOG( 20 ) << "Loading customer district range for warehouse: " << w_id
                << ", district:" << d_id << start << " to " << end;

    std::vector<cell_key_ranges> ckrs = {ckr};
    if( do_begin_commit ) {
        std::vector<cell_key_ranges> empty_ckrs;
        db_operators_.begin_transaction( ckrs, empty_ckrs );
    }
    RETURN_VOID_AND_COMMIT_IF_SS_DB();

    for( uint64_t c_id = start; c_id <= end; c_id++ ) {
        load_customer_district( w_id, d_id, (uint32_t) c_id, ckr );
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "Loading customer range for warehouse: " << w_id
                << ", district:" << d_id << start << " to " << end << " okay!";
}

tpcc_loader_types void tpcc_loader_templ::load_customer_district(
    uint32_t w_id, uint32_t d_id, uint32_t c_id, const cell_key_ranges& ckr ) {

    uint32_t c_next_o_id = generate_customer_order_id_start( w_id, d_id, c_id );

    DVLOG( 30 ) << "Loading customer district w_id:" << w_id
                << ", d_id:" << d_id << ", c_id:" << c_id
                << ", c_next_o_id:" << c_next_o_id;
    customer_district c;
    generate_customer_district( &c, c_id, d_id, w_id, c_next_o_id );
    insert_customer_district( &db_operators_, &c,
                              make_customer_district_key( c, configs_ ), ckr );
}

tpcc_loader_types void tpcc_loader_templ::load_customer(
    uint32_t w_id, uint32_t d_id, uint32_t c_id, const cell_key_ranges& ckr ) {
    // 10% have bad credit
    int32_t rand_pct = dist_.get_uniform_int( 0, 100 );
    bool    bad_credit = ( rand_pct > 90 );

    DVLOG( 30 ) << "Loading customer w_id:" << w_id << ", d_id:" << d_id
                << ", c_id:" << c_id;
    customer c;
    generate_customer( &c, c_id, d_id, w_id, bad_credit );
    insert_customer( &db_operators_, &c, make_customer_key( c, configs_ ),
                     ckr );
}

tpcc_loader_types void tpcc_loader_templ::generate_customer_district(
    customer_district* c, uint32_t c_id, uint32_t d_id, uint32_t w_id,
    uint32_t c_next_o_id ) {
    c->c_id = (int32_t) c_id;
    c->c_d_id = (int32_t) d_id;
    c->c_w_id = (int32_t) w_id;
    c->c_next_o_id = (int32_t) c_next_o_id;
}

tpcc_loader_types int32_t
    tpcc_loader_templ::generate_nation_for_customer( const customer* c ) {
    // some say the first letter should map to the nation;
    int32_t n_id =
        make_w_d_key( c->c_d_id, c->c_w_id, configs_ ) % nation::NUM_NATIONS;
    return n_id;
}

tpcc_loader_types void tpcc_loader_templ::generate_customer(
    customer* c, uint32_t c_id, uint32_t d_id, uint32_t w_id,
    bool is_bad_credit ) {
    c->c_id = (int32_t) c_id;
    c->c_d_id = (int32_t) d_id;
    c->c_w_id = (int32_t) w_id;

    c->c_credit_lim = customer::INITIAL_CREDIT_LIM;
    c->c_discount = (float) dist_.get_uniform_double_in_range(
        customer::MIN_DISCOUNT, customer::MAX_DISCOUNT );
    c->c_balance = customer::INITIAL_BALANCE;
    c->c_ytd_payment = customer::INITIAL_YTD_PAYMENT;
    c->c_payment_cnt = customer::INITIAL_PAYMENT_CNT;
    c->c_delivery_cnt = customer::INITIAL_DELIVERY_CNT;

    c->c_first = dist_.write_uniform_str( customer::MIN_FIRST_LEN,
                                          customer::MAX_FIRST_LEN );

    c->c_n_id = generate_nation_for_customer( c );

    c->c_middle = "OE";

    if( c_id < 1000 ) {
        c->c_last = dist_.write_last_name( c_id );
    } else {
        c->c_last = make_last_name( c_id );
    }

    if( is_bad_credit ) {
        c->c_credit = "BC";
    } else {
        c->c_credit = "GC";
    }

    generate_address( &( c->c_address ) );

    c->c_phone =
        dist_.write_uniform_nstr( customer::PHONE_LEN, customer::PHONE_LEN );
    c->c_since = now_;

    c->c_data = dist_.write_uniform_str( customer::MIN_DATA_LEN,
                                         customer::MAX_DATA_LEN );
}

tpcc_loader_types void tpcc_loader_templ::generate_region( region*  r,
                                                           uint32_t r_id ) {
    DCHECK_LT( r_id, k_tpch_region_names.size() );

    r->r_id = r_id;
    r->r_name = k_tpch_region_names.at( r_id );

    r->r_comment =
        dist_.write_uniform_str( region::COMMENT_LEN, region::COMMENT_LEN );
}

tpcc_loader_types void tpcc_loader_templ::generate_nation( nation*  n,
                                                           uint32_t n_id ) {
    n->n_id = n_id;
    n->r_id = n_id % region::NUM_REGIONS;

    DCHECK_LT( n_id, k_tpch_nation_name.size() );
    for( uint32_t pos = 0; pos < nation::NAME_LEN; pos++ ) {
        n->n_name.push_back( k_tpch_nation_name.at( n_id ) );
    }

    n->n_comment =
        dist_.write_uniform_str( nation::COMMENT_LEN, nation::COMMENT_LEN );
}
tpcc_loader_types void tpcc_loader_templ::generate_supplier( supplier* s,
                                                             uint32_t  s_id ) {
    s->s_id = s_id;
    s->n_id = dist_.get_uniform_int( 0, nation::NUM_NATIONS );

    generate_address( &( s->s_address ) );

    s->s_name =
        dist_.write_uniform_str( supplier::NAME_LEN, supplier::NAME_LEN );
    s->s_phone =
        dist_.write_uniform_nstr( customer::PHONE_LEN, customer::PHONE_LEN );

    s->s_acctbal = dist_.fixed_point( 2, 1000.0, 10000.0 );
    s->s_comment =
        dist_.write_uniform_str( supplier::COMMENT_LEN, supplier::COMMENT_LEN );
}

tpcc_loader_types std::string tpcc_loader_templ::make_last_name(
    uint32_t c_id ) {
    uint32_t nur_id =
        nu_dist_.nu_rand_c_last( 0, std::min( (uint32_t) 999, c_id ) );
    return dist_.write_last_name( nur_id );
}

tpcc_loader_types void tpcc_loader_templ::load_history_range(
    uint32_t w_id, uint32_t d_id, uint64_t start, uint64_t end,
    const cell_key_ranges& ckr ) {
    DVLOG( 20 ) << "Loading history range for warehouse: " << w_id
                << ", district:" << d_id << start << " to " << end;

    std::vector<cell_key_ranges> ckrs = {ckr};
    if( do_begin_commit ) {
        std::vector<cell_key_ranges> empty_ckrs;
        db_operators_.begin_transaction( ckrs, empty_ckrs );
    }
    RETURN_VOID_AND_COMMIT_IF_SS_DB();

    for( uint64_t h_c_id = start; h_c_id <= end; h_c_id++ ) {
        load_history( w_id, d_id, h_c_id, ckr );
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "Loading history range for warehouse: " << w_id
                << ", district:" << d_id << start << " to " << end << " okay!";
}

tpcc_loader_types void tpcc_loader_templ::load_history(
    uint32_t w_id, uint32_t d_id, uint32_t h_c_id,
    const cell_key_ranges& ckr ) {
    DVLOG( 30 ) << "Loading history w_id:" << w_id << ", d_id:" << d_id
                << ", h_c_id:" << h_c_id;

    history h;
    generate_history( &h, h_c_id, d_id, w_id );
    insert_history( &db_operators_, &h, make_history_key( h, configs_ ), ckr );
}

tpcc_loader_types void tpcc_loader_templ::generate_history( history* h,
                                                            uint32_t h_c_id,
                                                            uint32_t d_id,
                                                            uint32_t w_id ) {
    h->h_c_id = (int32_t) h_c_id;
    h->h_c_d_id = (int32_t) d_id;
    h->h_c_w_id = (int32_t) w_id;
    h->h_d_id = (int32_t) d_id;
    h->h_w_id = (int32_t) w_id;

    h->h_amount = history::INITIAL_AMOUNT;
    h->h_date = now_;

    h->h_data =
        dist_.write_uniform_str( history::MIN_DATA_LEN, history::MAX_DATA_LEN );
}

tpcc_loader_types void tpcc_loader_templ::make_orders( uint32_t w_id ) {
    DVLOG( 20 ) << "Loading orders for warehouse:" << w_id;

    for( uint32_t d_id = 0; d_id < configs_.num_districts_per_warehouse_;
         d_id++ ) {
        std::vector<uint32_t> customers( configs_.num_customers_per_district_,
                                         0 );
        for( uint32_t c_id = 0; c_id < configs_.num_customers_per_district_;
             c_id++ ) {
            customers.at( c_id ) = c_id;
        }
        std::mt19937 dist;
        std::shuffle( customers.begin(), customers.end(), std::move( dist ) );

        uint32_t o_ol_cnt = 0;
        for( uint32_t o_id = 0; o_id < customers.size(); o_id++ ) {
            uint32_t c_id = customers.at( o_id );
            o_ol_cnt = dist_.get_uniform_int(
                order::MIN_OL_CNT, configs_.max_num_order_lines_per_order_ );
            load_order( w_id, d_id, c_id, o_id, o_ol_cnt );
        }
    }

    DVLOG( 20 ) << "Loading orders for warehouse:" << w_id << " okay!";
}

tpcc_loader_types bool tpcc_loader_templ::is_order_a_new_order(
    uint32_t o_id ) {
    return ( configs_.num_customers_per_district_ -
             configs_.initial_num_customers_per_district_ ) <= o_id;
}

tpcc_loader_types void tpcc_loader_templ::load_order(
    uint32_t w_id, uint32_t d_id, uint32_t c_id, uint32_t o_id,
    uint32_t o_ol_cnt, const std::vector<cell_key_ranges>& write_set ) {
    DVLOG( 20 ) << "Loading order for warehouse:" << w_id
                << ", district:" << d_id << " customer:" << c_id
                << ", order:" << o_id << ", order line count:" << o_ol_cnt;

    // the last orders per district are new
    bool is_new_order = is_order_a_new_order( o_id );

    order o;
    generate_order( &o, w_id, d_id, c_id, o_id, o_ol_cnt, is_new_order );
    uint64_t order_key = make_order_key( o, configs_ );

    order_line o_line;
    o_line.ol_o_id = o_id;
    o_line.ol_d_id = d_id;
    o_line.ol_w_id = w_id;
    o_line.ol_number = 1;

    uint64_t o_line_start = make_order_line_key( o_line, configs_ );
    o_line.ol_number = o.o_ol_cnt;
    uint64_t o_line_end = make_order_line_key( o_line, configs_ );

    if( do_begin_commit ) {
        db_operators_.add_partitions( generate_partition_column_identifiers(
            k_tpcc_order_table_id, order_key, order_key, 0,
            k_tpcc_order_num_columns - 1, data_sizes_ ) );

        db_operators_.add_partitions( generate_partition_column_identifiers(
            k_tpcc_order_line_table_id, o_line_start, o_line_end, 0,
            k_tpcc_order_line_num_columns - 1, data_sizes_ ) );
    }

    new_order       new_o;
    if( is_new_order ) {
        DVLOG( 20 ) << "Loading order is new order:" << o_id;

        generate_new_order( &new_o, w_id, d_id, o_id );
        uint64_t new_order_key = make_new_order_key( new_o, configs_ );

        if( do_begin_commit ) {
            db_operators_.add_partitions( generate_partition_column_identifiers(
                k_tpcc_new_order_table_id, new_order_key, new_order_key, 0,
                k_tpcc_new_order_num_columns - 1, data_sizes_ ) );
        }
    }

    if( do_begin_commit ) {
        DVLOG( 20 ) << "Begin write set:" << write_set;
        std::vector<cell_key_ranges> empty_ckrs;
        std::vector<cell_key_ranges> loc_write_set = write_set;
        db_operators_.begin_transaction( loc_write_set, empty_ckrs );
    }
    RETURN_VOID_AND_COMMIT_IF_SS_DB();

    for( const auto& ckr : write_set ) {
        if( ckr.table_id == k_tpcc_order_table_id ) {
            insert_order( &db_operators_, &o, order_key, ckr );
        } else if( ckr.table_id == k_tpcc_order_line_table_id ) {

            for( int32_t ol_pos = 0; ol_pos < o.o_ol_cnt; ol_pos++ ) {
                load_order_line( w_id, d_id, o_id, ol_pos + 1, ckr );
            }
        } else if( ckr.table_id == k_tpcc_new_order_table_id ) {

            if( is_new_order ) {
                insert_new_order( &db_operators_, &new_o, ckr.row_id_start,
                                  ckr );
            }
        }
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "Loading order for warehouse:" << w_id
                << ", district:" << d_id << " customer:" << c_id
                << ", order:" << o_id << " okay!";
}

// MTODO figure out if we need to pass ckrs to this
tpcc_loader_types void tpcc_loader_templ::load_order( uint32_t w_id,
                                                      uint32_t d_id,
                                                      uint32_t c_id,
                                                      uint32_t o_id,
                                                      uint32_t o_ol_cnt ) {
    // the last orders per district are new
    bool is_new_order = is_order_a_new_order( o_id );

    order o;
    generate_order( &o, w_id, d_id, c_id, o_id, o_ol_cnt, is_new_order );
    uint64_t order_key = make_order_key( o, configs_ );
    auto     o_ckr =
        create_cell_key_ranges( k_tpcc_order_table_id, order_key, order_key, 0,
                                k_tpcc_order_num_columns - 1 );

    std::vector<cell_key_ranges> write_set;

    write_set.push_back( o_ckr );

    order_line o_line;
    o_line.ol_o_id = o_id;
    o_line.ol_d_id = d_id;
    o_line.ol_w_id = w_id;
    o_line.ol_number = 1;

    uint64_t o_line_start = make_order_line_key( o_line, configs_ );
    o_line.ol_number = o.o_ol_cnt;
    uint64_t o_line_end = make_order_line_key( o_line, configs_ );

    auto o_line_ckr = create_cell_key_ranges(
        k_tpcc_order_line_table_id, o_line_start, o_line_end, 0,
        k_tpcc_order_line_num_columns - 1 );

    write_set.push_back( o_line_ckr );

    new_order       new_o;
    cell_key_ranges new_order_ckr;
    if( is_new_order ) {
        DVLOG( 20 ) << "Loading order is new order:" << o_id;

        generate_new_order( &new_o, w_id, d_id, o_id );
        uint64_t new_order_key = make_new_order_key( new_o, configs_ );
        new_order_ckr = create_cell_key_ranges(
            k_tpcc_new_order_table_id, new_order_key, new_order_key, 0,
            k_tpcc_new_order_num_columns - 1 );
        write_set.push_back( new_order_ckr );
    }

    return load_order( w_id, d_id, c_id, o_id, o_ol_cnt, write_set );
}

tpcc_loader_types void tpcc_loader_templ::load_only_order(
    uint32_t w_id, uint32_t d_id, uint32_t c_id, uint32_t o_id,
    uint32_t o_ol_cnt, const cell_key_ranges& ckr ) {
    DVLOG( 20 ) << "Loading only order for warehouse:" << w_id
                << ", district:" << d_id << " customer:" << c_id
                << ", order:" << o_id;

    bool is_new_order = is_order_a_new_order( o_id );

    order o;
    generate_order( &o, w_id, d_id, c_id, o_id, o_ol_cnt, is_new_order );
    insert_order( &db_operators_, &o, make_order_key( o, configs_ ), ckr );

    DVLOG( 20 ) << "Loading only order for warehouse:" << w_id
                << ", district:" << d_id << " customer:" << c_id
                << ", order:" << o_id << " okay!";
}

tpcc_loader_types void tpcc_loader_templ::load_order_line(
    uint32_t w_id, uint32_t d_id, uint32_t o_id, uint32_t ol_num,
    const cell_key_ranges& ckr ) {
    DVLOG( 20 ) << "Loading order line for warehouse:" << w_id
                << ", district:" << d_id << ", order:" << o_id
                << ", order_line:" << ol_num;

    bool       is_new_order = is_order_a_new_order( o_id );
    order_line o_line;
    generate_order_line( &o_line, w_id, d_id, o_id, ol_num, is_new_order );
    insert_order_line( &db_operators_, &o_line,
                       make_order_line_key( o_line, configs_ ), ckr );

    DVLOG( 20 ) << "Loading order line for warehouse:" << w_id
                << ", district:" << d_id << ", order:" << o_id
                << ", order_line:" << ol_num << " okay!";
}

tpcc_loader_types void tpcc_loader_templ::load_new_order(
    uint32_t w_id, uint32_t d_id, uint32_t o_id, const cell_key_ranges& ckr ) {
    DVLOG( 20 ) << "Loading new order for warehouse:" << w_id
                << ", district:" << d_id << ", order:" << o_id;

    new_order new_o;
    generate_new_order( &new_o, w_id, d_id, o_id );
    insert_new_order( &db_operators_, &new_o,
                      make_new_order_key( new_o, configs_ ), ckr );

    DVLOG( 20 ) << "Loading new order for warehouse:" << w_id
                << ", district:" << d_id << ", order:" << o_id << " okay!";
}

tpcc_loader_types void tpcc_loader_templ::generate_order(
    order* o, uint32_t w_id, uint32_t d_id, uint32_t c_id, uint32_t o_id,
    uint32_t o_ol_cnt, bool is_new_order ) {
    o->o_id = (int32_t) o_id;
    o->o_c_id = (int32_t) c_id;
    o->o_d_id = (int32_t) d_id;
    o->o_w_id = (int32_t) w_id;

    if( !is_new_order ) {
        o->o_carrier_id = dist_.get_uniform_int( order::MIN_CARRIER_ID,
                                                 order::MAX_CARRIER_ID );
    } else {
        o->o_carrier_id = order::NULL_CARRIER_ID;
    }

    o->o_ol_cnt = o_ol_cnt;

    o->o_all_local = order::INITIAL_ALL_LOCAL;
    o->o_entry_d = now_;
}

tpcc_loader_types void tpcc_loader_templ::generate_order_line(
    order_line* o_line, uint32_t w_id, uint32_t d_id, uint32_t o_id,
    uint32_t ol_number, bool is_new_order ) {
    o_line->ol_o_id = (int32_t) o_id;
    o_line->ol_d_id = (int32_t) d_id;
    o_line->ol_w_id = (int32_t) w_id;
    o_line->ol_number = (int32_t) ol_number;
    o_line->ol_i_id = dist_.get_uniform_int( 1, configs_.num_items_ );
    o_line->ol_supply_w_id = w_id;
    o_line->ol_quantity = order_line::INITIAL_QUANTITY;

    if( !is_new_order ) {
        o_line->ol_amount = 0.00;
        o_line->ol_delivery_d = now_;
    } else {
        o_line->ol_amount = (float) dist_.get_uniform_double_in_range(
            order_line::MIN_AMOUNT, order_line::MAX_AMOUNT );
        o_line->ol_delivery_d.c_since = datetime::EMPTY_DATE;
    }
    o_line->ol_dist_info = dist_.write_uniform_str(
        stock::MAX_DISTRICT_STR_LEN, stock::MAX_DISTRICT_STR_LEN );
}

tpcc_loader_types void tpcc_loader_templ::generate_new_order( new_order* new_o,
                                                              uint32_t   w_id,
                                                              uint32_t   d_id,
                                                              uint32_t o_id ) {
    new_o->no_w_id = (int32_t) w_id;
    new_o->no_d_id = (int32_t) d_id;
    new_o->no_o_id = (int32_t) o_id;
}

tpcc_loader_types void tpcc_loader_templ::make_stock(
    uint32_t w_id, uint64_t start, uint64_t end, const cell_key_ranges& ckr ) {
    uint32_t batch_size = configs_.partition_size_;

    uint64_t cur_start = start;
    uint64_t cur_end = start;
    while( cur_start <= end ) {
        cur_end = std::min( end, cur_start + batch_size );
        load_stock_range( w_id, cur_start, cur_end, ckr );
        cur_start = cur_end + 1;
    }
}

tpcc_loader_types int32_t
    tpcc_loader_templ::generate_supplier_for_stock( const stock* s ) {
    // MTODO-TPCC
    return 0;
}


tpcc_loader_types void tpcc_loader_templ::generate_stock( stock*   s,
                                                          uint64_t item_id,
                                                          uint32_t w_id,
                                                          bool is_original ) {

    s->s_i_id = (int32_t) item_id;
    s->s_w_id = (int32_t) w_id;
    s->s_quantity =
        dist_.get_uniform_int( stock::MIN_QUANTITY, stock::MAX_QUANTITY );
    s->s_ytd = 0;
    s->s_order_cnt = 0;
    s->s_remote_cnt = 0;

    s->s_s_id = generate_supplier_for_stock( s );

    s->s_dist_0 = dist_.write_uniform_str( stock::MAX_DISTRICT_STR_LEN,
                                           stock::MAX_DISTRICT_STR_LEN );
    s->s_dist_1 = dist_.write_uniform_str( stock::MAX_DISTRICT_STR_LEN,
                                           stock::MAX_DISTRICT_STR_LEN );
    s->s_dist_2 = dist_.write_uniform_str( stock::MAX_DISTRICT_STR_LEN,
                                           stock::MAX_DISTRICT_STR_LEN );
    s->s_dist_3 = dist_.write_uniform_str( stock::MAX_DISTRICT_STR_LEN,
                                           stock::MAX_DISTRICT_STR_LEN );
    s->s_dist_4 = dist_.write_uniform_str( stock::MAX_DISTRICT_STR_LEN,
                                           stock::MAX_DISTRICT_STR_LEN );
    s->s_dist_5 = dist_.write_uniform_str( stock::MAX_DISTRICT_STR_LEN,
                                           stock::MAX_DISTRICT_STR_LEN );
    s->s_dist_6 = dist_.write_uniform_str( stock::MAX_DISTRICT_STR_LEN,
                                           stock::MAX_DISTRICT_STR_LEN );
    s->s_dist_7 = dist_.write_uniform_str( stock::MAX_DISTRICT_STR_LEN,
                                           stock::MAX_DISTRICT_STR_LEN );
    s->s_dist_8 = dist_.write_uniform_str( stock::MAX_DISTRICT_STR_LEN,
                                           stock::MAX_DISTRICT_STR_LEN );
    s->s_dist_9 = dist_.write_uniform_str( stock::MAX_DISTRICT_STR_LEN,
                                           stock::MAX_DISTRICT_STR_LEN );

    s->s_data =
        dist_.write_uniform_str( item::MIN_DATA_LEN, item::MAX_DATA_LEN );
    if( is_original ) {
        s->s_data = set_original( s->s_data );
    }
}

tpcc_loader_types void tpcc_loader_templ::load_stock_range(
    uint32_t w_id, uint64_t start, uint64_t end, const cell_key_ranges& ckr ) {
    DVLOG( 20 ) << "Loading stock range for warehouse: " << w_id << ", "
                << start << " to " << end;
    std::vector<cell_key_ranges> ckrs = {ckr};
    if( do_begin_commit ) {
        std::vector<cell_key_ranges> empty_ckrs;
        db_operators_.begin_transaction( ckrs, empty_ckrs );
    }
    RETURN_VOID_AND_COMMIT_IF_SS_DB();

    for( uint64_t item_id = start; item_id <= end; item_id++ ) {
        load_stock( w_id, item_id, ckr );
    }

    if( do_begin_commit ) {
        db_operators_.commit_transaction();
    }

    DVLOG( 20 ) << "Loading stock range for warehouse: " << w_id << ", "
                << start << " to " << end << " okay!";
}

tpcc_loader_types void tpcc_loader_templ::load_stock(
    uint32_t w_id, uint64_t item_id, const cell_key_ranges& ckr ) {
    // 10% have original
    uint32_t rand_pct = dist_.get_uniform_int( 0, 100 );
    bool     original = ( rand_pct > 90 );

    DVLOG( 30 ) << "Loading stock w_id:" << w_id << ", i_id:" << item_id;

    stock s;
    generate_stock( &s, item_id, w_id, original );

    insert_stock( &db_operators_, &s, make_stock_key( s, configs_ ), ckr );
}

tpcc_loader_types void tpcc_loader_templ::set_transaction_partition_holder(
    transaction_partition_holder* txn_holder ) {
    db_operators_.set_transaction_partition_holder( txn_holder );
}

tpcc_loader_types std::vector<cell_key_ranges>
    tpcc_loader_templ::generate_write_set( uint32_t table_id, uint64_t row_start,
                                           uint64_t row_end, uint32_t col_start, uint32_t col_end ) {
    DCHECK_LE( row_start, row_end );
    DCHECK_LE( col_start, col_end );
    std::vector<cell_key_ranges> write_set = {create_cell_key_ranges(
        table_id, row_start, row_end, col_start, col_end )};
    return write_set;
}

tpcc_loader_types std::vector<primary_key>
    tpcc_loader_templ::generate_write_set_as_pks( uint32_t table_id,
                                                  uint64_t start,
                                                  uint64_t end ) {
    DCHECK_LE( start, end );
    std::vector<primary_key> write_set( ( end - start ) + 1 );
    for( uint64_t pos = 0; pos < write_set.size(); pos++ ) {
        write_set.at( pos ).table_id = table_id;
        write_set.at( pos ).row_id = start + pos;
    }
    return write_set;
}


tpcc_loader_types uint32_t tpcc_loader_templ::generate_customer_order_id_start(
    uint32_t w_id, uint32_t d_id, uint32_t c_id ) {
    uint32_t base_id = ( d_id * configs_.num_customers_per_district_ ) + c_id;
    uint32_t o_id = base_id * configs_.expected_num_orders_per_cust_;
    return o_id;
}
tpcc_loader_types uint32_t tpcc_loader_templ::generate_customer_order_id_end(
    uint32_t w_id, uint32_t d_id, uint32_t c_id ) {
    uint32_t o_id = generate_customer_order_id_start( w_id, d_id, c_id ) +
                    configs_.expected_num_orders_per_cust_ - 1;
    return o_id;
}

tpcc_loader_types tpcc_configs tpcc_loader_templ::get_configs() const {
    return configs_;
}

tpcc_loader_types snapshot_vector tpcc_loader_templ::get_snapshot() const {
    return db_operators_.global_state_;
}
