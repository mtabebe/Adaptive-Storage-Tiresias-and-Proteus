#include "tpcc_benchmark_worker.h"

#if 0 // MTODO-TPCC
std::vector<record_identifier> get_payment_rids( int32_t w_id, int32_t d_id,
                                                 int32_t c_w_id, int32_t c_d_id,
                                                 int32_t             c_id,
                                                 const tpcc_configs& configs ) {
    district d;
    d.d_id = d_id;
    d.d_w_id = w_id;
    record_identifier d_rid = {k_tpcc_district_table_id,
                               make_district_key( d, configs )};
#endif
#if 0
    record_identifier w_rid = {k_tpcc_warehouse_table_id, (uint64_t) w_id};
#endif
#if 0 // MTODO-TPCC

    customer c;
    c.c_id = c_id;
    c.c_d_id = c_d_id;
    c.c_w_id = c_w_id;
    record_identifier c_rid = {k_tpcc_customer_table_id,
                               make_customer_key( c, configs )};

#if 0
    history h;
    h.h_w_id = w_id;
    h.h_d_id = d_id;
    h.h_c_w_id = c_w_id;
    h.h_c_d_id = c_d_id;
    h.h_c_id = c_id;
    record_identifier h_rid = {k_tpcc_history_table_id,
                               make_history_key( h, configs )};
#endif

    std::vector<record_identifier> rids = {/*w_rid,*/ d_rid, c_rid /*, h_rid*/};
    return rids;
}
#endif

std::vector<cell_key_ranges> get_payment_ckrs( int32_t w_id, int32_t d_id,
                                               int32_t c_w_id, int32_t c_d_id,
                                               int32_t             c_id,
                                               const tpcc_configs& configs ) {
    std::vector<cell_key_ranges> payment_ckrs;
// #if 0
    // warehouse
    auto w_cid = create_cell_identifier( k_tpcc_warehouse_table_id,
                                                 warehouse_cols::w_ytd, w_id );
    auto w_ckr = cell_key_ranges_from_cell_identifier( w_cid );
    payment_ckrs.emplace_back( w_ckr );
// #endif

    // district
    district d;
    d.d_w_id = w_id;
    d.d_id = d_id;

    auto d_cid =
        create_cell_identifier( k_tpcc_district_table_id, district_cols::d_ytd,
                                make_district_key( d, configs ) );
    auto d_ckr = cell_key_ranges_from_cell_identifier( d_cid );
    payment_ckrs.emplace_back( d_ckr );

#if 0
    // history
    history h;
    h.h_w_id = w_id;
    h.h_d_id = d_id;
    h.h_c_w_id = c_w_id;
    h.h_c_d_id = c_d_id;
    h.h_c_id = c_id;

    uint64_t hist_key = make_history_key( h, configs );
    auto     h_ckr =
        create_cell_key_ranges( k_tpcc_history_table_id, hist_key, hist_key, 0,
                                k_tpcc_history_num_columns - 1 );
    payment_ckrs.emplace_back( h_ckr );

#endif

    // customer
    customer c;
    c.c_id = c_id;
    c.c_d_id = c_d_id;
    c.c_w_id = c_w_id;
    uint64_t cust_key = make_customer_key( c, configs );

    auto c_ckr = create_cell_key_ranges( k_tpcc_customer_table_id, cust_key,
                                         cust_key, customer_cols::c_balance,
                                         customer_cols::c_payment_cnt );
    payment_ckrs.emplace_back( c_ckr );

    return payment_ckrs;
}

