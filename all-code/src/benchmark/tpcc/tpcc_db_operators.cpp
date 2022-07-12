#include "tpcc_db_operators.h"

#include <glog/logging.h>

#include "../../common/string_conversion.h"
#include "../db_operators_macros.h"

void tpcc_create_tables( db_abstraction* db, const tpcc_configs& configs ) {
    DVLOG( 10 ) << "Creating tpcc tables";
    auto table_infos =
        create_tpcc_table_metadata( configs, db->get_site_location() );
    for( auto m : table_infos ) {
        db->create_table( m );
    }
    DVLOG( 10 ) << "Creating tpcc tables okay!";
}

void tpcc_create_tables( db* database, const tpcc_configs& configs ) {
    DVLOG( 10 ) << "Creating tpcc tables";

    auto table_infos =
        create_tpcc_table_metadata( configs, database->get_site_location() );
    for( auto m : table_infos ) {
        database->get_tables()->create_table( m );
    }
    DVLOG( 10 ) << "Creating tpcc tables okay!";
}

void insert_customer( benchmark_db_operators* db_ops, const customer* var,
                      uint64_t row_id, const cell_key_ranges& ckr ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case customer_cols::c_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, c_id, cid );
                break;
            }
            case customer_cols::c_d_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, c_d_id, cid );
                break;
            }
            case customer_cols::c_w_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, c_w_id, cid );
                break;
            }
            case customer_cols::c_n_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, c_n_id, cid );
                break;
            }
            case customer_cols::c_credit_lim: {
                DO_DB_OP( db_ops, insert_double, double, var, c_credit_lim,
                          cid );
                break;
            }
            case customer_cols::c_discount: {
                DO_DB_OP( db_ops, insert_double, double, var, c_discount, cid );
                break;
            }
            case customer_cols::c_balance: {
                DO_DB_OP( db_ops, insert_double, double, var, c_balance, cid );
                break;
            }
            case customer_cols::c_ytd_payment: {
                DO_DB_OP( db_ops, insert_double, double, var, c_ytd_payment,
                          cid );
                break;
            }
            case customer_cols::c_payment_cnt: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, c_payment_cnt,
                          cid );
                break;
            }
            case customer_cols::c_delivery_cnt: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, c_delivery_cnt,
                          cid );
                break;
            }
            case customer_cols::c_first: {
                DO_DB_OP( db_ops, insert_string, std::string, var, c_first,
                          cid );
                break;
            }
            case customer_cols::c_middle: {
                DO_DB_OP( db_ops, insert_string, std::string, var, c_middle,
                          cid );
                break;
            }
            case customer_cols::c_last: {
                DO_DB_OP( db_ops, insert_string, std::string, var, c_last,
                          cid );
                break;
            }
            case customer_cols::c_address_a_street_1: {
                DO_DB_OP( db_ops, insert_string, std::string, var,
                          c_address.a_street_1, cid );
                break;
            }
            case customer_cols::c_address_a_street_2: {
                DO_DB_OP( db_ops, insert_string, std::string, var,
                          c_address.a_street_2, cid );
                break;
            }
            case customer_cols::c_address_a_city: {
                DO_DB_OP( db_ops, insert_string, std::string, var,
                          c_address.a_city, cid );
                break;
            }
            case customer_cols::c_address_a_state: {
                DO_DB_OP( db_ops, insert_string, std::string, var,
                          c_address.a_state, cid );
                break;
            }
            case customer_cols::c_address_a_zip: {
                DO_DB_OP( db_ops, insert_string, std::string, var,
                          c_address.a_zip, cid );
                break;
            }
            case customer_cols::c_phone: {
                DO_DB_OP( db_ops, insert_string, std::string, var, c_phone,
                          cid );
                break;
            }
            case customer_cols::c_since_c_since: {
                DO_DB_OP( db_ops, insert_uint64, uint64_t, var, c_since.c_since,
                          cid );
                break;
            }
            case customer_cols::c_credit: {
                DO_DB_OP( db_ops, insert_string, std::string, var, c_credit,
                          cid );
                break;
            }
            case customer_cols::c_data: {
                DO_DB_OP( db_ops, insert_string, std::string, var, c_data,
                          cid );
                break;
            }
        }
    }
}

void update_customer( benchmark_db_operators* db_ops, const customer* var,
                      uint64_t row_id, const cell_key_ranges& ckr,
                      bool do_propagate ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case customer_cols::c_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, c_id, cid );
                break;
            }
            case customer_cols::c_d_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, c_d_id, cid );
                break;
            }
            case customer_cols::c_w_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, c_w_id, cid );
                break;
            }
            case customer_cols::c_n_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, c_n_id, cid );
                break;
            }
            case customer_cols::c_credit_lim: {
                DO_DB_OP( db_ops, write_double, double, var, c_credit_lim,
                          cid );
                break;
            }
            case customer_cols::c_discount: {
                DO_DB_OP( db_ops, write_double, double, var, c_discount, cid );
                break;
            }
            case customer_cols::c_balance: {
                DO_DB_OP( db_ops, write_double, double, var, c_balance, cid );
                break;
            }
            case customer_cols::c_ytd_payment: {
                DO_DB_OP( db_ops, write_double, double, var, c_ytd_payment,
                          cid );
                break;
            }
            case customer_cols::c_payment_cnt: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, c_payment_cnt,
                          cid );
                break;
            }
            case customer_cols::c_delivery_cnt: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, c_delivery_cnt,
                          cid );
                break;
            }
            case customer_cols::c_first: {
                DO_DB_OP( db_ops, write_string, std::string, var, c_first,
                          cid );
                break;
            }
            case customer_cols::c_middle: {
                DO_DB_OP( db_ops, write_string, std::string, var, c_middle,
                          cid );
                break;
            }
            case customer_cols::c_last: {
                DO_DB_OP( db_ops, write_string, std::string, var, c_last, cid );
                break;
            }
            case customer_cols::c_address_a_street_1: {
                DO_DB_OP( db_ops, write_string, std::string, var,
                          c_address.a_street_1, cid );
                break;
            }
            case customer_cols::c_address_a_street_2: {
                DO_DB_OP( db_ops, write_string, std::string, var,
                          c_address.a_street_2, cid );
                break;
            }
            case customer_cols::c_address_a_city: {
                DO_DB_OP( db_ops, write_string, std::string, var,
                          c_address.a_city, cid );
                break;
            }
            case customer_cols::c_address_a_state: {
                DO_DB_OP( db_ops, write_string, std::string, var,
                          c_address.a_state, cid );
                break;
            }
            case customer_cols::c_address_a_zip: {
                DO_DB_OP( db_ops, write_string, std::string, var,
                          c_address.a_zip, cid );
                break;
            }
            case customer_cols::c_phone: {
                DO_DB_OP( db_ops, write_string, std::string, var, c_phone,
                          cid );
                break;
            }
            case customer_cols::c_since_c_since: {
                DO_DB_OP( db_ops, write_uint64, uint64_t, var, c_since.c_since,
                          cid );
                break;
            }
            case customer_cols::c_credit: {
                DO_DB_OP( db_ops, write_string, std::string, var, c_credit,
                          cid );
                break;
            }
            case customer_cols::c_data: {
                DO_DB_OP( db_ops, write_string, std::string, var, c_data, cid );
                break;
            }
        }
    }
}

bool lookup_customer( benchmark_db_operators* db_ops, customer* var,
                      uint64_t row_id, const cell_key_ranges& ckr,
                      bool is_latest, bool is_nullable ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );
    bool ret = true;

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        bool loc_ret = true;
        cid.col_id_ = col;
        switch( col ) {
            case customer_cols::c_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, c_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case customer_cols::c_d_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, c_d_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case customer_cols::c_w_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, c_w_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case customer_cols::c_n_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, c_n_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case customer_cols::c_credit_lim: {
                DO_DOUBLE_READ_OP( loc_ret, db_ops, float, var, c_credit_lim,
                                   cid, is_latest, is_nullable );
                break;
            }
            case customer_cols::c_discount: {
                DO_DOUBLE_READ_OP( loc_ret, db_ops, float, var, c_discount, cid,
                                   is_latest, is_nullable );
                break;
            }
            case customer_cols::c_balance: {
                DO_DOUBLE_READ_OP( loc_ret, db_ops, float, var, c_balance, cid,
                                   is_latest, is_nullable );
                break;
            }
            case customer_cols::c_ytd_payment: {
                DO_DOUBLE_READ_OP( loc_ret, db_ops, float, var, c_ytd_payment,
                                   cid, is_latest, is_nullable );
                break;
            }
            case customer_cols::c_payment_cnt: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, c_payment_cnt,
                                  cid, is_latest, is_nullable );
                break;
            }
            case customer_cols::c_delivery_cnt: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, c_delivery_cnt,
                                  cid, is_latest, is_nullable );
                break;
            }
            case customer_cols::c_first: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, c_first,
                                   cid, is_latest, is_nullable );
                break;
            }
            case customer_cols::c_middle: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, c_middle,
                                   cid, is_latest, is_nullable );
                break;
            }
            case customer_cols::c_last: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, c_last,
                                   cid, is_latest, is_nullable );
                break;
            }
            case customer_cols::c_address_a_street_1: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   c_address.a_street_1, cid, is_latest,
                                   is_nullable );
                break;
            }
            case customer_cols::c_address_a_street_2: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   c_address.a_street_2, cid, is_latest,
                                   is_nullable );
                break;
            }
            case customer_cols::c_address_a_city: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   c_address.a_city, cid, is_latest,
                                   is_nullable );
                break;
            }
            case customer_cols::c_address_a_state: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   c_address.a_state, cid, is_latest,
                                   is_nullable );
                break;
            }
            case customer_cols::c_address_a_zip: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   c_address.a_zip, cid, is_latest,
                                   is_nullable );
                break;
            }
            case customer_cols::c_phone: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, c_phone,
                                   cid, is_latest, is_nullable );
                break;
            }
            case customer_cols::c_since_c_since: {
                DO_UINT64_READ_OP( loc_ret, db_ops, uint64_t, var,
                                   c_since.c_since, cid, is_latest,
                                   is_nullable );
                break;
            }
            case customer_cols::c_credit: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, c_credit,
                                   cid, is_latest, is_nullable );
                break;
            }
            case customer_cols::c_data: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, c_data,
                                   cid, is_latest, is_nullable );
                break;
            }
        }
        ret = ret and loc_ret;
    }
    return ret;
}

std::unordered_set<uint32_t> read_from_scan_customer( const result_tuple& res,
                                                      customer* var ) {
    std::unordered_set<uint32_t> ret;

    for( const auto& cell : res.cells ) {
        bool loc_ret = false;
        switch( cell.col_id ) {
            case customer_cols::c_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            c_id );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_id );
                }
                break;
            }
            case customer_cols::c_d_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            c_d_id );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_d_id );
                }
                break;
            }
            case customer_cols::c_w_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            c_w_id );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_w_id );
                }
                break;
            }
            case customer_cols::c_n_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            c_n_id );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_n_id );
                }
                break;
            }
            case customer_cols::c_credit_lim: {
                DO_SCAN_OP( loc_ret, string_to_double, float, cell, var,
                            c_credit_lim );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_credit_lim );
                }
                break;
            }
            case customer_cols::c_discount: {
                DO_SCAN_OP( loc_ret, string_to_double, float, cell, var,
                            c_discount );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_discount );
                }
                break;
            }
            case customer_cols::c_balance: {
                DO_SCAN_OP( loc_ret, string_to_double, float, cell, var,
                            c_balance );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_balance );
                }
                break;
            }
            case customer_cols::c_ytd_payment: {
                DO_SCAN_OP( loc_ret, string_to_double, float, cell, var,
                            c_ytd_payment );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_ytd_payment );
                }
                break;
            }
            case customer_cols::c_payment_cnt: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            c_payment_cnt );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_payment_cnt );
                }
                break;
            }
            case customer_cols::c_delivery_cnt: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            c_delivery_cnt );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_delivery_cnt );
                }
                break;
            }
            case customer_cols::c_first: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            c_first );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_first );
                }
                break;
            }
            case customer_cols::c_middle: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            c_middle );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_middle );
                }
                break;
            }
            case customer_cols::c_last: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            c_last );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_last );
                }
                break;
            }
            case customer_cols::c_address_a_street_1: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            c_address.a_street_1 );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_address_a_street_1 );
                }
                break;
            }
            case customer_cols::c_address_a_street_2: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            c_address.a_street_2 );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_address_a_street_2 );
                }
                break;
            }
            case customer_cols::c_address_a_city: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            c_address.a_city );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_address_a_city );
                }
                break;
            }
            case customer_cols::c_address_a_state: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            c_address.a_state );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_address_a_state );
                }
                break;
            }
            case customer_cols::c_address_a_zip: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            c_address.a_zip );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_address_a_zip );
                }
                break;
            }
            case customer_cols::c_phone: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            c_phone );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_phone );
                }
                break;
            }
            case customer_cols::c_since_c_since: {
                DO_SCAN_OP( loc_ret, string_to_uint64, uint64_t, cell, var,
                            c_since.c_since );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_since_c_since );
                }
                break;
            }
            case customer_cols::c_credit: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            c_credit );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_credit );
                }
                break;
            }
            case customer_cols::c_data: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            c_data );
                if( loc_ret ) {
                    ret.emplace( customer_cols::c_data );
                }
                break;
            }
        }
    }
    return ret;
}

void insert_customer_district( benchmark_db_operators*  db_ops,
                               const customer_district* var, uint64_t row_id,
                               const cell_key_ranges& ckr ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case customer_district_cols::c_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, c_id, cid );
                break;
            }
            case customer_district_cols::c_d_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, c_d_id, cid );
                break;
            }
            case customer_district_cols::c_w_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, c_w_id, cid );
                break;
            }
            case customer_district_cols::c_next_o_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, c_next_o_id,
                          cid );
                break;
            }
        }
    }
}

void update_customer_district( benchmark_db_operators*  db_ops,
                               const customer_district* var, uint64_t row_id,
                               const cell_key_ranges& ckr, bool do_propagate ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case customer_district_cols::c_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, c_id, cid );
                break;
            }
            case customer_district_cols::c_d_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, c_d_id, cid );
                break;
            }
            case customer_district_cols::c_w_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, c_w_id, cid );
                break;
            }
            case customer_district_cols::c_next_o_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, c_next_o_id, cid );
                break;
            }
        }
    }
}

bool lookup_customer_district( benchmark_db_operators* db_ops,
                               customer_district* var, uint64_t row_id,
                               const cell_key_ranges& ckr, bool is_latest,
                               bool is_nullable ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );
    bool ret = true;

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        bool loc_ret = true;
        cid.col_id_ = col;
        switch( col ) {
            case customer_district_cols::c_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, c_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case customer_district_cols::c_d_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, c_d_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case customer_district_cols::c_w_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, c_w_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case customer_district_cols::c_next_o_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, c_next_o_id,
                                  cid, is_latest, is_nullable );
                break;
            }
        }
        ret = ret and loc_ret;
    }
    return ret;
}

std::unordered_set<uint32_t> read_from_scan_customer_district(
    const result_tuple& res, customer_district* var ) {
    std::unordered_set<uint32_t> ret;

    for( const auto& cell : res.cells ) {
        bool loc_ret = false;
        switch( cell.col_id ) {
            case customer_district_cols::c_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            c_id );
                if( loc_ret ) {
                    ret.emplace( customer_district_cols::c_id );
                }
                break;
            }
            case customer_district_cols::c_d_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            c_d_id );
                if( loc_ret ) {
                    ret.emplace( customer_district_cols::c_d_id );
                }
                break;
            }
            case customer_district_cols::c_w_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            c_w_id );
                if( loc_ret ) {
                    ret.emplace( customer_district_cols::c_w_id );
                }
                break;
            }
            case customer_district_cols::c_next_o_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            c_next_o_id );
                if( loc_ret ) {
                    ret.emplace( customer_district_cols::c_next_o_id );
                }
                break;
            }
        }
    }
    return ret;
}

void insert_district( benchmark_db_operators* db_ops, const district* var,
                      uint64_t row_id, const cell_key_ranges& ckr ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case district_cols::d_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, d_id, cid );
                break;
            }
            case district_cols::d_w_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, d_w_id, cid );
                break;
            }
            case district_cols::d_tax: {
                DO_DB_OP( db_ops, insert_double, double, var, d_tax, cid );
                break;
            }
            case district_cols::d_ytd: {
                DO_DB_OP( db_ops, insert_double, double, var, d_ytd, cid );
                break;
            }
            case district_cols::d_next_o_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, d_next_o_id,
                          cid );
                break;
            }
            case district_cols::d_name: {
                DO_DB_OP( db_ops, insert_string, std::string, var, d_name,
                          cid );
                break;
            }
            case district_cols::d_address_a_street_1: {
                DO_DB_OP( db_ops, insert_string, std::string, var,
                          d_address.a_street_1, cid );
                break;
            }
            case district_cols::d_address_a_street_2: {
                DO_DB_OP( db_ops, insert_string, std::string, var,
                          d_address.a_street_2, cid );
                break;
            }
            case district_cols::d_address_a_city: {
                DO_DB_OP( db_ops, insert_string, std::string, var,
                          d_address.a_city, cid );
                break;
            }
            case district_cols::d_address_a_state: {
                DO_DB_OP( db_ops, insert_string, std::string, var,
                          d_address.a_state, cid );
                break;
            }
            case district_cols::d_address_a_zip: {
                DO_DB_OP( db_ops, insert_string, std::string, var,
                          d_address.a_zip, cid );
                break;
            }
        }
    }
}

void update_district( benchmark_db_operators* db_ops, const district* var,
                      uint64_t row_id, const cell_key_ranges& ckr,
                      bool do_propagate ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case district_cols::d_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, d_id, cid );
                break;
            }
            case district_cols::d_w_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, d_w_id, cid );
                break;
            }
            case district_cols::d_tax: {
                DO_DB_OP( db_ops, write_double, double, var, d_tax, cid );
                break;
            }
            case district_cols::d_ytd: {
                DO_DB_OP( db_ops, write_double, double, var, d_ytd, cid );
                break;
            }
            case district_cols::d_next_o_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, d_next_o_id, cid );
                break;
            }
            case district_cols::d_name: {
                DO_DB_OP( db_ops, write_string, std::string, var, d_name, cid );
                break;
            }
            case district_cols::d_address_a_street_1: {
                DO_DB_OP( db_ops, write_string, std::string, var,
                          d_address.a_street_1, cid );
                break;
            }
            case district_cols::d_address_a_street_2: {
                DO_DB_OP( db_ops, write_string, std::string, var,
                          d_address.a_street_2, cid );
                break;
            }
            case district_cols::d_address_a_city: {
                DO_DB_OP( db_ops, write_string, std::string, var,
                          d_address.a_city, cid );
                break;
            }
            case district_cols::d_address_a_state: {
                DO_DB_OP( db_ops, write_string, std::string, var,
                          d_address.a_state, cid );
                break;
            }
            case district_cols::d_address_a_zip: {
                DO_DB_OP( db_ops, write_string, std::string, var,
                          d_address.a_zip, cid );
                break;
            }
        }
    }
}

bool lookup_district( benchmark_db_operators* db_ops, district* var,
                      uint64_t row_id, const cell_key_ranges& ckr,
                      bool is_latest, bool is_nullable ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );
    bool ret = true;

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        bool loc_ret = true;
        cid.col_id_ = col;
        switch( col ) {
            case district_cols::d_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, d_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case district_cols::d_w_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, d_w_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case district_cols::d_tax: {
                DO_DOUBLE_READ_OP( loc_ret, db_ops, float, var, d_tax, cid,
                                   is_latest, is_nullable );
                break;
            }
            case district_cols::d_ytd: {
                DO_DOUBLE_READ_OP( loc_ret, db_ops, float, var, d_ytd, cid,
                                   is_latest, is_nullable );
                break;
            }
            case district_cols::d_next_o_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, d_next_o_id,
                                  cid, is_latest, is_nullable );
                break;
            }
            case district_cols::d_name: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, d_name,
                                   cid, is_latest, is_nullable );
                break;
            }
            case district_cols::d_address_a_street_1: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   d_address.a_street_1, cid, is_latest,
                                   is_nullable );
                break;
            }
            case district_cols::d_address_a_street_2: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   d_address.a_street_2, cid, is_latest,
                                   is_nullable );
                break;
            }
            case district_cols::d_address_a_city: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   d_address.a_city, cid, is_latest,
                                   is_nullable );
                break;
            }
            case district_cols::d_address_a_state: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   d_address.a_state, cid, is_latest,
                                   is_nullable );
                break;
            }
            case district_cols::d_address_a_zip: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   d_address.a_zip, cid, is_latest,
                                   is_nullable );
                break;
            }
        }
        ret = ret and loc_ret;
    }
    return ret;
}

std::unordered_set<uint32_t> read_from_scan_district( const result_tuple& res,
                                                      district* var ) {
    std::unordered_set<uint32_t> ret;

    for( const auto& cell : res.cells ) {
        bool loc_ret = false;
        switch( cell.col_id ) {
            case district_cols::d_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            d_id );
                if( loc_ret ) {
                    ret.emplace( district_cols::d_id );
                }
                break;
            }
            case district_cols::d_w_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            d_w_id );
                if( loc_ret ) {
                    ret.emplace( district_cols::d_w_id );
                }
                break;
            }
            case district_cols::d_tax: {
                DO_SCAN_OP( loc_ret, string_to_double, float, cell, var,
                            d_tax );
                if( loc_ret ) {
                    ret.emplace( district_cols::d_tax );
                }
                break;
            }
            case district_cols::d_ytd: {
                DO_SCAN_OP( loc_ret, string_to_double, float, cell, var,
                            d_ytd );
                if( loc_ret ) {
                    ret.emplace( district_cols::d_ytd );
                }
                break;
            }
            case district_cols::d_next_o_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            d_next_o_id );
                if( loc_ret ) {
                    ret.emplace( district_cols::d_next_o_id );
                }
                break;
            }
            case district_cols::d_name: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            d_name );
                if( loc_ret ) {
                    ret.emplace( district_cols::d_name );
                }
                break;
            }
            case district_cols::d_address_a_street_1: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            d_address.a_street_1 );
                if( loc_ret ) {
                    ret.emplace( district_cols::d_address_a_street_1 );
                }
                break;
            }
            case district_cols::d_address_a_street_2: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            d_address.a_street_2 );
                if( loc_ret ) {
                    ret.emplace( district_cols::d_address_a_street_2 );
                }
                break;
            }
            case district_cols::d_address_a_city: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            d_address.a_city );
                if( loc_ret ) {
                    ret.emplace( district_cols::d_address_a_city );
                }
                break;
            }
            case district_cols::d_address_a_state: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            d_address.a_state );
                if( loc_ret ) {
                    ret.emplace( district_cols::d_address_a_state );
                }
                break;
            }
            case district_cols::d_address_a_zip: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            d_address.a_zip );
                if( loc_ret ) {
                    ret.emplace( district_cols::d_address_a_zip );
                }
                break;
            }
        }
    }
    return ret;
}

void insert_history( benchmark_db_operators* db_ops, const history* var,
                     uint64_t row_id, const cell_key_ranges& ckr ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case history_cols::h_c_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, h_c_id, cid );
                break;
            }
            case history_cols::h_c_d_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, h_c_d_id, cid );
                break;
            }
            case history_cols::h_c_w_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, h_c_w_id, cid );
                break;
            }
            case history_cols::h_d_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, h_d_id, cid );
                break;
            }
            case history_cols::h_w_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, h_w_id, cid );
                break;
            }
            case history_cols::h_amount: {
                DO_DB_OP( db_ops, insert_double, double, var, h_amount, cid );
                break;
            }
            case history_cols::h_date_c_since: {
                DO_DB_OP( db_ops, insert_uint64, uint64_t, var, h_date.c_since,
                          cid );
                break;
            }
            case history_cols::h_data: {
                DO_DB_OP( db_ops, insert_string, std::string, var, h_data,
                          cid );
                break;
            }
        }
    }
}

void update_history( benchmark_db_operators* db_ops, const history* var,
                     uint64_t row_id, const cell_key_ranges& ckr,
                     bool do_propagate ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case history_cols::h_c_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, h_c_id, cid );
                break;
            }
            case history_cols::h_c_d_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, h_c_d_id, cid );
                break;
            }
            case history_cols::h_c_w_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, h_c_w_id, cid );
                break;
            }
            case history_cols::h_d_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, h_d_id, cid );
                break;
            }
            case history_cols::h_w_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, h_w_id, cid );
                break;
            }
            case history_cols::h_amount: {
                DO_DB_OP( db_ops, write_double, double, var, h_amount, cid );
                break;
            }
            case history_cols::h_date_c_since: {
                DO_DB_OP( db_ops, write_uint64, uint64_t, var, h_date.c_since,
                          cid );
                break;
            }
            case history_cols::h_data: {
                DO_DB_OP( db_ops, write_string, std::string, var, h_data, cid );
                break;
            }
        }
    }
}

bool lookup_history( benchmark_db_operators* db_ops, history* var,
                     uint64_t row_id, const cell_key_ranges& ckr,
                     bool is_latest, bool is_nullable ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );
    bool ret = true;

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        bool loc_ret = true;
        cid.col_id_ = col;
        switch( col ) {
            case history_cols::h_c_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, h_c_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case history_cols::h_c_d_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, h_c_d_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case history_cols::h_c_w_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, h_c_w_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case history_cols::h_d_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, h_d_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case history_cols::h_w_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, h_w_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case history_cols::h_amount: {
                DO_DOUBLE_READ_OP( loc_ret, db_ops, float, var, h_amount, cid,
                                   is_latest, is_nullable );
                break;
            }
            case history_cols::h_date_c_since: {
                DO_UINT64_READ_OP( loc_ret, db_ops, uint64_t, var,
                                   h_date.c_since, cid, is_latest,
                                   is_nullable );
                break;
            }
            case history_cols::h_data: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, h_data,
                                   cid, is_latest, is_nullable );
                break;
            }
        }
        ret = ret and loc_ret;
    }
    return ret;
}

std::unordered_set<uint32_t> read_from_scan_history( const result_tuple& res,
                                                     history*            var ) {
    std::unordered_set<uint32_t> ret;

    for( const auto& cell : res.cells ) {
        bool loc_ret = false;
        switch( cell.col_id ) {
            case history_cols::h_c_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            h_c_id );
                if( loc_ret ) {
                    ret.emplace( history_cols::h_c_id );
                }
                break;
            }
            case history_cols::h_c_d_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            h_c_d_id );
                if( loc_ret ) {
                    ret.emplace( history_cols::h_c_d_id );
                }
                break;
            }
            case history_cols::h_c_w_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            h_c_w_id );
                if( loc_ret ) {
                    ret.emplace( history_cols::h_c_w_id );
                }
                break;
            }
            case history_cols::h_d_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            h_d_id );
                if( loc_ret ) {
                    ret.emplace( history_cols::h_d_id );
                }
                break;
            }
            case history_cols::h_w_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            h_w_id );
                if( loc_ret ) {
                    ret.emplace( history_cols::h_w_id );
                }
                break;
            }
            case history_cols::h_amount: {
                DO_SCAN_OP( loc_ret, string_to_double, float, cell, var,
                            h_amount );
                if( loc_ret ) {
                    ret.emplace( history_cols::h_amount );
                }
                break;
            }
            case history_cols::h_date_c_since: {
                DO_SCAN_OP( loc_ret, string_to_uint64, uint64_t, cell, var,
                            h_date.c_since );
                if( loc_ret ) {
                    ret.emplace( history_cols::h_date_c_since );
                }
                break;
            }
            case history_cols::h_data: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            h_data );
                if( loc_ret ) {
                    ret.emplace( history_cols::h_data );
                }
                break;
            }
        }
    }
    return ret;
}

void insert_item( benchmark_db_operators* db_ops, const item* var,
                  uint64_t row_id, const cell_key_ranges& ckr ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case item_cols::i_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, i_id, cid );
                break;
            }
            case item_cols::i_im_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, i_im_id, cid );
                break;
            }
            case item_cols::i_price: {
                DO_DB_OP( db_ops, insert_double, double, var, i_price, cid );
                break;
            }
            case item_cols::i_name: {
                DO_DB_OP( db_ops, insert_string, std::string, var, i_name,
                          cid );
                break;
            }
            case item_cols::i_data: {
                DO_DB_OP( db_ops, insert_string, std::string, var, i_data,
                          cid );
                break;
            }
        }
    }
}

void update_item( benchmark_db_operators* db_ops, const item* var,
                  uint64_t row_id, const cell_key_ranges& ckr,
                  bool do_propagate ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case item_cols::i_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, i_id, cid );
                break;
            }
            case item_cols::i_im_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, i_im_id, cid );
                break;
            }
            case item_cols::i_price: {
                DO_DB_OP( db_ops, write_double, double, var, i_price, cid );
                break;
            }
            case item_cols::i_name: {
                DO_DB_OP( db_ops, write_string, std::string, var, i_name, cid );
                break;
            }
            case item_cols::i_data: {
                DO_DB_OP( db_ops, write_string, std::string, var, i_data, cid );
                break;
            }
        }
    }
}

bool lookup_item( benchmark_db_operators* db_ops, item* var, uint64_t row_id,
                  const cell_key_ranges& ckr, bool is_latest,
                  bool is_nullable ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );
    bool ret = true;

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        bool loc_ret = true;
        cid.col_id_ = col;
        switch( col ) {
            case item_cols::i_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, i_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case item_cols::i_im_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, i_im_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case item_cols::i_price: {
                DO_DOUBLE_READ_OP( loc_ret, db_ops, float, var, i_price, cid,
                                   is_latest, is_nullable );
                break;
            }
            case item_cols::i_name: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, i_name,
                                   cid, is_latest, is_nullable );
                break;
            }
            case item_cols::i_data: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, i_data,
                                   cid, is_latest, is_nullable );
                break;
            }
        }
        ret = ret and loc_ret;
    }
    return ret;
}

std::unordered_set<uint32_t> read_from_scan_item( const result_tuple& res,
                                                  item*               var ) {
    std::unordered_set<uint32_t> ret;

    for( const auto& cell : res.cells ) {
        bool loc_ret = false;
        switch( cell.col_id ) {
            case item_cols::i_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            i_id );
                if( loc_ret ) {
                    ret.emplace( item_cols::i_id );
                }
                break;
            }
            case item_cols::i_im_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            i_im_id );
                if( loc_ret ) {
                    ret.emplace( item_cols::i_im_id );
                }
                break;
            }
            case item_cols::i_price: {
                DO_SCAN_OP( loc_ret, string_to_double, float, cell, var,
                            i_price );
                if( loc_ret ) {
                    ret.emplace( item_cols::i_price );
                }
                break;
            }
            case item_cols::i_name: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            i_name );
                if( loc_ret ) {
                    ret.emplace( item_cols::i_name );
                }
                break;
            }
            case item_cols::i_data: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            i_data );
                if( loc_ret ) {
                    ret.emplace( item_cols::i_data );
                }
                break;
            }
        }
    }
    return ret;
}

void insert_nation( benchmark_db_operators* db_ops, const nation* var,
                    uint64_t row_id, const cell_key_ranges& ckr ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case nation_cols::n_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, n_id, cid );
                break;
            }
            case nation_cols::r_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, r_id, cid );
                break;
            }
            case nation_cols::n_name: {
                DO_DB_OP( db_ops, insert_string, std::string, var, n_name,
                          cid );
                break;
            }
            case nation_cols::n_comment: {
                DO_DB_OP( db_ops, insert_string, std::string, var, n_comment,
                          cid );
                break;
            }
        }
    }
}

void update_nation( benchmark_db_operators* db_ops, const nation* var,
                    uint64_t row_id, const cell_key_ranges& ckr,
                    bool do_propagate ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case nation_cols::n_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, n_id, cid );
                break;
            }
            case nation_cols::r_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, r_id, cid );
                break;
            }
            case nation_cols::n_name: {
                DO_DB_OP( db_ops, write_string, std::string, var, n_name, cid );
                break;
            }
            case nation_cols::n_comment: {
                DO_DB_OP( db_ops, write_string, std::string, var, n_comment,
                          cid );
                break;
            }
        }
    }
}

bool lookup_nation( benchmark_db_operators* db_ops, nation* var,
                    uint64_t row_id, const cell_key_ranges& ckr, bool is_latest,
                    bool is_nullable ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );
    bool ret = true;

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        bool loc_ret = true;
        cid.col_id_ = col;
        switch( col ) {
            case nation_cols::n_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, n_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case nation_cols::r_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, r_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case nation_cols::n_name: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, n_name,
                                   cid, is_latest, is_nullable );
                break;
            }
            case nation_cols::n_comment: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, n_comment,
                                   cid, is_latest, is_nullable );
                break;
            }
        }
        ret = ret and loc_ret;
    }
    return ret;
}

std::unordered_set<uint32_t> read_from_scan_nation( const result_tuple& res,
                                                    nation*             var ) {
    std::unordered_set<uint32_t> ret;

    for( const auto& cell : res.cells ) {
        bool loc_ret = false;
        switch( cell.col_id ) {
            case nation_cols::n_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            n_id );
                if( loc_ret ) {
                    ret.emplace( nation_cols::n_id );
                }
                break;
            }
            case nation_cols::r_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            r_id );
                if( loc_ret ) {
                    ret.emplace( nation_cols::r_id );
                }
                break;
            }
            case nation_cols::n_name: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            n_name );
                if( loc_ret ) {
                    ret.emplace( nation_cols::n_name );
                }
                break;
            }
            case nation_cols::n_comment: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            n_comment );
                if( loc_ret ) {
                    ret.emplace( nation_cols::n_comment );
                }
                break;
            }
        }
    }
    return ret;
}

void insert_new_order( benchmark_db_operators* db_ops, const new_order* var,
                       uint64_t row_id, const cell_key_ranges& ckr ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case new_order_cols::no_w_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, no_w_id, cid );
                break;
            }
            case new_order_cols::no_d_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, no_d_id, cid );
                break;
            }
            case new_order_cols::no_o_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, no_o_id, cid );
                break;
            }
        }
    }
}

void update_new_order( benchmark_db_operators* db_ops, const new_order* var,
                       uint64_t row_id, const cell_key_ranges& ckr,
                       bool do_propagate ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case new_order_cols::no_w_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, no_w_id, cid );
                break;
            }
            case new_order_cols::no_d_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, no_d_id, cid );
                break;
            }
            case new_order_cols::no_o_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, no_o_id, cid );
                break;
            }
        }
    }
}

bool lookup_new_order( benchmark_db_operators* db_ops, new_order* var,
                       uint64_t row_id, const cell_key_ranges& ckr,
                       bool is_latest, bool is_nullable ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );
    bool ret = true;

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        bool loc_ret = true;
        cid.col_id_ = col;
        switch( col ) {
            case new_order_cols::no_w_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, no_w_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case new_order_cols::no_d_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, no_d_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case new_order_cols::no_o_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, no_o_id, cid,
                                  is_latest, is_nullable );
                break;
            }
        }
        ret = ret and loc_ret;
    }
    return ret;
}

std::unordered_set<uint32_t> read_from_scan_new_order( const result_tuple& res,
                                                       new_order* var ) {
    std::unordered_set<uint32_t> ret;

    for( const auto& cell : res.cells ) {
        bool loc_ret = false;
        switch( cell.col_id ) {
            case new_order_cols::no_w_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            no_w_id );
                if( loc_ret ) {
                    ret.emplace( new_order_cols::no_w_id );
                }
                break;
            }
            case new_order_cols::no_d_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            no_d_id );
                if( loc_ret ) {
                    ret.emplace( new_order_cols::no_d_id );
                }
                break;
            }
            case new_order_cols::no_o_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            no_o_id );
                if( loc_ret ) {
                    ret.emplace( new_order_cols::no_o_id );
                }
                break;
            }
        }
    }
    return ret;
}

void insert_order( benchmark_db_operators* db_ops, const order* var,
                   uint64_t row_id, const cell_key_ranges& ckr ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case order_cols::o_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, o_id, cid );
                break;
            }
            case order_cols::o_c_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, o_c_id, cid );
                break;
            }
            case order_cols::o_d_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, o_d_id, cid );
                break;
            }
            case order_cols::o_w_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, o_w_id, cid );
                break;
            }
            case order_cols::o_carrier_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, o_carrier_id,
                          cid );
                break;
            }
            case order_cols::o_ol_cnt: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, o_ol_cnt, cid );
                break;
            }
            case order_cols::o_all_local: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, o_all_local,
                          cid );
                break;
            }
            case order_cols::o_entry_d_c_since: {
                DO_DB_OP( db_ops, insert_uint64, uint64_t, var,
                          o_entry_d.c_since, cid );
                break;
            }
        }
    }
}

void update_order( benchmark_db_operators* db_ops, const order* var,
                   uint64_t row_id, const cell_key_ranges& ckr,
                   bool do_propagate ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case order_cols::o_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, o_id, cid );
                break;
            }
            case order_cols::o_c_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, o_c_id, cid );
                break;
            }
            case order_cols::o_d_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, o_d_id, cid );
                break;
            }
            case order_cols::o_w_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, o_w_id, cid );
                break;
            }
            case order_cols::o_carrier_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, o_carrier_id,
                          cid );
                break;
            }
            case order_cols::o_ol_cnt: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, o_ol_cnt, cid );
                break;
            }
            case order_cols::o_all_local: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, o_all_local, cid );
                break;
            }
            case order_cols::o_entry_d_c_since: {
                DO_DB_OP( db_ops, write_uint64, uint64_t, var,
                          o_entry_d.c_since, cid );
                break;
            }
        }
    }
}

bool lookup_order( benchmark_db_operators* db_ops, order* var, uint64_t row_id,
                   const cell_key_ranges& ckr, bool is_latest,
                   bool is_nullable ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );
    bool ret = true;

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        bool loc_ret = true;
        cid.col_id_ = col;
        switch( col ) {
            case order_cols::o_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, o_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case order_cols::o_c_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, o_c_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case order_cols::o_d_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, o_d_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case order_cols::o_w_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, o_w_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case order_cols::o_carrier_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, o_carrier_id,
                                  cid, is_latest, is_nullable );
                break;
            }
            case order_cols::o_ol_cnt: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, o_ol_cnt, cid,
                                  is_latest, is_nullable );
                break;
            }
            case order_cols::o_all_local: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, o_all_local,
                                  cid, is_latest, is_nullable );
                break;
            }
            case order_cols::o_entry_d_c_since: {
                DO_UINT64_READ_OP( loc_ret, db_ops, uint64_t, var,
                                   o_entry_d.c_since, cid, is_latest,
                                   is_nullable );
                break;
            }
        }
        ret = ret and loc_ret;
    }
    return ret;
}

std::unordered_set<uint32_t> read_from_scan_order( const result_tuple& res,
                                                   order*              var ) {
    std::unordered_set<uint32_t> ret;

    for( const auto& cell : res.cells ) {
        bool loc_ret = false;
        switch( cell.col_id ) {
            case order_cols::o_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            o_id );
                if( loc_ret ) {
                    ret.emplace( order_cols::o_id );
                }
                break;
            }
            case order_cols::o_c_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            o_c_id );
                if( loc_ret ) {
                    ret.emplace( order_cols::o_c_id );
                }
                break;
            }
            case order_cols::o_d_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            o_d_id );
                if( loc_ret ) {
                    ret.emplace( order_cols::o_d_id );
                }
                break;
            }
            case order_cols::o_w_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            o_w_id );
                if( loc_ret ) {
                    ret.emplace( order_cols::o_w_id );
                }
                break;
            }
            case order_cols::o_carrier_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            o_carrier_id );
                if( loc_ret ) {
                    ret.emplace( order_cols::o_carrier_id );
                }
                break;
            }
            case order_cols::o_ol_cnt: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            o_ol_cnt );
                if( loc_ret ) {
                    ret.emplace( order_cols::o_ol_cnt );
                }
                break;
            }
            case order_cols::o_all_local: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            o_all_local );
                if( loc_ret ) {
                    ret.emplace( order_cols::o_all_local );
                }
                break;
            }
            case order_cols::o_entry_d_c_since: {
                DO_SCAN_OP( loc_ret, string_to_uint64, uint64_t, cell, var,
                            o_entry_d.c_since );
                if( loc_ret ) {
                    ret.emplace( order_cols::o_entry_d_c_since );
                }
                break;
            }
        }
    }
    return ret;
}

void insert_order_line( benchmark_db_operators* db_ops, const order_line* var,
                        uint64_t row_id, const cell_key_ranges& ckr ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case order_line_cols::ol_o_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, ol_o_id, cid );
                break;
            }
            case order_line_cols::ol_d_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, ol_d_id, cid );
                break;
            }
            case order_line_cols::ol_w_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, ol_w_id, cid );
                break;
            }
            case order_line_cols::ol_number: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, ol_number, cid );
                break;
            }
            case order_line_cols::ol_i_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, ol_i_id, cid );
                break;
            }
            case order_line_cols::ol_supply_w_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, ol_supply_w_id,
                          cid );
                break;
            }
            case order_line_cols::ol_quantity: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, ol_quantity,
                          cid );
                break;
            }
            case order_line_cols::ol_amount: {
                DO_DB_OP( db_ops, insert_double, double, var, ol_amount, cid );
                break;
            }
            case order_line_cols::ol_delivery_d_c_since: {
                DO_DB_OP( db_ops, insert_uint64, uint64_t, var,
                          ol_delivery_d.c_since, cid );
                break;
            }
            case order_line_cols::ol_dist_info: {
                DO_DB_OP( db_ops, insert_string, std::string, var, ol_dist_info,
                          cid );
                break;
            }
        }
    }
}

void update_order_line( benchmark_db_operators* db_ops, const order_line* var,
                        uint64_t row_id, const cell_key_ranges& ckr,
                        bool do_propagate ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case order_line_cols::ol_o_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, ol_o_id, cid );
                break;
            }
            case order_line_cols::ol_d_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, ol_d_id, cid );
                break;
            }
            case order_line_cols::ol_w_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, ol_w_id, cid );
                break;
            }
            case order_line_cols::ol_number: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, ol_number, cid );
                break;
            }
            case order_line_cols::ol_i_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, ol_i_id, cid );
                break;
            }
            case order_line_cols::ol_supply_w_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, ol_supply_w_id,
                          cid );
                break;
            }
            case order_line_cols::ol_quantity: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, ol_quantity, cid );
                break;
            }
            case order_line_cols::ol_amount: {
                DO_DB_OP( db_ops, write_double, double, var, ol_amount, cid );
                break;
            }
            case order_line_cols::ol_delivery_d_c_since: {
                DO_DB_OP( db_ops, write_uint64, uint64_t, var,
                          ol_delivery_d.c_since, cid );
                break;
            }
            case order_line_cols::ol_dist_info: {
                DO_DB_OP( db_ops, write_string, std::string, var, ol_dist_info,
                          cid );
                break;
            }
        }
    }
}

bool lookup_order_line( benchmark_db_operators* db_ops, order_line* var,
                        uint64_t row_id, const cell_key_ranges& ckr,
                        bool is_latest, bool is_nullable ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );
    bool ret = true;

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        bool loc_ret = true;
        cid.col_id_ = col;
        switch( col ) {
            case order_line_cols::ol_o_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, ol_o_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case order_line_cols::ol_d_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, ol_d_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case order_line_cols::ol_w_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, ol_w_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case order_line_cols::ol_number: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, ol_number, cid,
                                  is_latest, is_nullable );
                break;
            }
            case order_line_cols::ol_i_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, ol_i_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case order_line_cols::ol_supply_w_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, ol_supply_w_id,
                                  cid, is_latest, is_nullable );
                break;
            }
            case order_line_cols::ol_quantity: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, ol_quantity,
                                  cid, is_latest, is_nullable );
                break;
            }
            case order_line_cols::ol_amount: {
                DO_DOUBLE_READ_OP( loc_ret, db_ops, float, var, ol_amount, cid,
                                   is_latest, is_nullable );
                break;
            }
            case order_line_cols::ol_delivery_d_c_since: {
                DO_UINT64_READ_OP( loc_ret, db_ops, uint64_t, var,
                                   ol_delivery_d.c_since, cid, is_latest,
                                   is_nullable );
                break;
            }
            case order_line_cols::ol_dist_info: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   ol_dist_info, cid, is_latest, is_nullable );
                break;
            }
        }
        ret = ret and loc_ret;
    }
    return ret;
}

std::unordered_set<uint32_t> read_from_scan_order_line( const result_tuple& res,
                                                        order_line* var ) {
    std::unordered_set<uint32_t> ret;

    for( const auto& cell : res.cells ) {
        bool loc_ret = false;
        switch( cell.col_id ) {
            case order_line_cols::ol_o_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            ol_o_id );
                if( loc_ret ) {
                    ret.emplace( order_line_cols::ol_o_id );
                }
                break;
            }
            case order_line_cols::ol_d_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            ol_d_id );
                if( loc_ret ) {
                    ret.emplace( order_line_cols::ol_d_id );
                }
                break;
            }
            case order_line_cols::ol_w_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            ol_w_id );
                if( loc_ret ) {
                    ret.emplace( order_line_cols::ol_w_id );
                }
                break;
            }
            case order_line_cols::ol_number: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            ol_number );
                if( loc_ret ) {
                    ret.emplace( order_line_cols::ol_number );
                }
                break;
            }
            case order_line_cols::ol_i_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            ol_i_id );
                if( loc_ret ) {
                    ret.emplace( order_line_cols::ol_i_id );
                }
                break;
            }
            case order_line_cols::ol_supply_w_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            ol_supply_w_id );
                if( loc_ret ) {
                    ret.emplace( order_line_cols::ol_supply_w_id );
                }
                break;
            }
            case order_line_cols::ol_quantity: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            ol_quantity );
                if( loc_ret ) {
                    ret.emplace( order_line_cols::ol_quantity );
                }
                break;
            }
            case order_line_cols::ol_amount: {
                DO_SCAN_OP( loc_ret, string_to_double, float, cell, var,
                            ol_amount );
                if( loc_ret ) {
                    ret.emplace( order_line_cols::ol_amount );
                }
                break;
            }
            case order_line_cols::ol_delivery_d_c_since: {
                DO_SCAN_OP( loc_ret, string_to_uint64, uint64_t, cell, var,
                            ol_delivery_d.c_since );
                if( loc_ret ) {
                    ret.emplace( order_line_cols::ol_delivery_d_c_since );
                }
                break;
            }
            case order_line_cols::ol_dist_info: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            ol_dist_info );
                if( loc_ret ) {
                    ret.emplace( order_line_cols::ol_dist_info );
                }
                break;
            }
        }
    }
    return ret;
}

void insert_region( benchmark_db_operators* db_ops, const region* var,
                    uint64_t row_id, const cell_key_ranges& ckr ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case region_cols::r_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, r_id, cid );
                break;
            }
            case region_cols::r_name: {
                DO_DB_OP( db_ops, insert_string, std::string, var, r_name,
                          cid );
                break;
            }
            case region_cols::r_comment: {
                DO_DB_OP( db_ops, insert_string, std::string, var, r_comment,
                          cid );
                break;
            }
        }
    }
}

void update_region( benchmark_db_operators* db_ops, const region* var,
                    uint64_t row_id, const cell_key_ranges& ckr,
                    bool do_propagate ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case region_cols::r_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, r_id, cid );
                break;
            }
            case region_cols::r_name: {
                DO_DB_OP( db_ops, write_string, std::string, var, r_name, cid );
                break;
            }
            case region_cols::r_comment: {
                DO_DB_OP( db_ops, write_string, std::string, var, r_comment,
                          cid );
                break;
            }
        }
    }
}

bool lookup_region( benchmark_db_operators* db_ops, region* var,
                    uint64_t row_id, const cell_key_ranges& ckr, bool is_latest,
                    bool is_nullable ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );
    bool ret = true;

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        bool loc_ret = true;
        cid.col_id_ = col;
        switch( col ) {
            case region_cols::r_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, r_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case region_cols::r_name: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, r_name,
                                   cid, is_latest, is_nullable );
                break;
            }
            case region_cols::r_comment: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, r_comment,
                                   cid, is_latest, is_nullable );
                break;
            }
        }
        ret = ret and loc_ret;
    }
    return ret;
}

std::unordered_set<uint32_t> read_from_scan_region( const result_tuple& res,
                                                    region*             var ) {
    std::unordered_set<uint32_t> ret;

    for( const auto& cell : res.cells ) {
        bool loc_ret = false;
        switch( cell.col_id ) {
            case region_cols::r_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            r_id );
                if( loc_ret ) {
                    ret.emplace( region_cols::r_id );
                }
                break;
            }
            case region_cols::r_name: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            r_name );
                if( loc_ret ) {
                    ret.emplace( region_cols::r_name );
                }
                break;
            }
            case region_cols::r_comment: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            r_comment );
                if( loc_ret ) {
                    ret.emplace( region_cols::r_comment );
                }
                break;
            }
        }
    }
    return ret;
}

void insert_stock( benchmark_db_operators* db_ops, const stock* var,
                   uint64_t row_id, const cell_key_ranges& ckr ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case stock_cols::s_i_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, s_i_id, cid );
                break;
            }
            case stock_cols::s_w_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, s_w_id, cid );
                break;
            }
            case stock_cols::s_s_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, s_s_id, cid );
                break;
            }
            case stock_cols::s_quantity: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, s_quantity, cid );
                break;
            }
            case stock_cols::s_ytd: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, s_ytd, cid );
                break;
            }
            case stock_cols::s_order_cnt: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, s_order_cnt,
                          cid );
                break;
            }
            case stock_cols::s_remote_cnt: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, s_remote_cnt,
                          cid );
                break;
            }
            case stock_cols::s_dist_0: {
                DO_DB_OP( db_ops, insert_string, std::string, var, s_dist_0,
                          cid );
                break;
            }
            case stock_cols::s_dist_1: {
                DO_DB_OP( db_ops, insert_string, std::string, var, s_dist_1,
                          cid );
                break;
            }
            case stock_cols::s_dist_2: {
                DO_DB_OP( db_ops, insert_string, std::string, var, s_dist_2,
                          cid );
                break;
            }
            case stock_cols::s_dist_3: {
                DO_DB_OP( db_ops, insert_string, std::string, var, s_dist_3,
                          cid );
                break;
            }
            case stock_cols::s_dist_4: {
                DO_DB_OP( db_ops, insert_string, std::string, var, s_dist_4,
                          cid );
                break;
            }
            case stock_cols::s_dist_5: {
                DO_DB_OP( db_ops, insert_string, std::string, var, s_dist_5,
                          cid );
                break;
            }
            case stock_cols::s_dist_6: {
                DO_DB_OP( db_ops, insert_string, std::string, var, s_dist_6,
                          cid );
                break;
            }
            case stock_cols::s_dist_7: {
                DO_DB_OP( db_ops, insert_string, std::string, var, s_dist_7,
                          cid );
                break;
            }
            case stock_cols::s_dist_8: {
                DO_DB_OP( db_ops, insert_string, std::string, var, s_dist_8,
                          cid );
                break;
            }
            case stock_cols::s_dist_9: {
                DO_DB_OP( db_ops, insert_string, std::string, var, s_dist_9,
                          cid );
                break;
            }
            case stock_cols::s_data: {
                DO_DB_OP( db_ops, insert_string, std::string, var, s_data,
                          cid );
                break;
            }
        }
    }
}

void update_stock( benchmark_db_operators* db_ops, const stock* var,
                   uint64_t row_id, const cell_key_ranges& ckr,
                   bool do_propagate ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case stock_cols::s_i_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, s_i_id, cid );
                break;
            }
            case stock_cols::s_w_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, s_w_id, cid );
                break;
            }
            case stock_cols::s_s_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, s_s_id, cid );
                break;
            }
            case stock_cols::s_quantity: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, s_quantity, cid );
                break;
            }
            case stock_cols::s_ytd: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, s_ytd, cid );
                break;
            }
            case stock_cols::s_order_cnt: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, s_order_cnt, cid );
                break;
            }
            case stock_cols::s_remote_cnt: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, s_remote_cnt,
                          cid );
                break;
            }
            case stock_cols::s_dist_0: {
                DO_DB_OP( db_ops, write_string, std::string, var, s_dist_0,
                          cid );
                break;
            }
            case stock_cols::s_dist_1: {
                DO_DB_OP( db_ops, write_string, std::string, var, s_dist_1,
                          cid );
                break;
            }
            case stock_cols::s_dist_2: {
                DO_DB_OP( db_ops, write_string, std::string, var, s_dist_2,
                          cid );
                break;
            }
            case stock_cols::s_dist_3: {
                DO_DB_OP( db_ops, write_string, std::string, var, s_dist_3,
                          cid );
                break;
            }
            case stock_cols::s_dist_4: {
                DO_DB_OP( db_ops, write_string, std::string, var, s_dist_4,
                          cid );
                break;
            }
            case stock_cols::s_dist_5: {
                DO_DB_OP( db_ops, write_string, std::string, var, s_dist_5,
                          cid );
                break;
            }
            case stock_cols::s_dist_6: {
                DO_DB_OP( db_ops, write_string, std::string, var, s_dist_6,
                          cid );
                break;
            }
            case stock_cols::s_dist_7: {
                DO_DB_OP( db_ops, write_string, std::string, var, s_dist_7,
                          cid );
                break;
            }
            case stock_cols::s_dist_8: {
                DO_DB_OP( db_ops, write_string, std::string, var, s_dist_8,
                          cid );
                break;
            }
            case stock_cols::s_dist_9: {
                DO_DB_OP( db_ops, write_string, std::string, var, s_dist_9,
                          cid );
                break;
            }
            case stock_cols::s_data: {
                DO_DB_OP( db_ops, write_string, std::string, var, s_data, cid );
                break;
            }
        }
    }
}

bool lookup_stock( benchmark_db_operators* db_ops, stock* var, uint64_t row_id,
                   const cell_key_ranges& ckr, bool is_latest,
                   bool is_nullable ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );
    bool ret = true;

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        bool loc_ret = true;
        cid.col_id_ = col;
        switch( col ) {
            case stock_cols::s_i_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, s_i_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case stock_cols::s_w_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, s_w_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case stock_cols::s_s_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, s_s_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case stock_cols::s_quantity: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, s_quantity,
                                  cid, is_latest, is_nullable );
                break;
            }
            case stock_cols::s_ytd: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, s_ytd, cid,
                                  is_latest, is_nullable );
                break;
            }
            case stock_cols::s_order_cnt: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, s_order_cnt,
                                  cid, is_latest, is_nullable );
                break;
            }
            case stock_cols::s_remote_cnt: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, s_remote_cnt,
                                  cid, is_latest, is_nullable );
                break;
            }
            case stock_cols::s_dist_0: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, s_dist_0,
                                   cid, is_latest, is_nullable );
                break;
            }
            case stock_cols::s_dist_1: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, s_dist_1,
                                   cid, is_latest, is_nullable );
                break;
            }
            case stock_cols::s_dist_2: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, s_dist_2,
                                   cid, is_latest, is_nullable );
                break;
            }
            case stock_cols::s_dist_3: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, s_dist_3,
                                   cid, is_latest, is_nullable );
                break;
            }
            case stock_cols::s_dist_4: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, s_dist_4,
                                   cid, is_latest, is_nullable );
                break;
            }
            case stock_cols::s_dist_5: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, s_dist_5,
                                   cid, is_latest, is_nullable );
                break;
            }
            case stock_cols::s_dist_6: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, s_dist_6,
                                   cid, is_latest, is_nullable );
                break;
            }
            case stock_cols::s_dist_7: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, s_dist_7,
                                   cid, is_latest, is_nullable );
                break;
            }
            case stock_cols::s_dist_8: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, s_dist_8,
                                   cid, is_latest, is_nullable );
                break;
            }
            case stock_cols::s_dist_9: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, s_dist_9,
                                   cid, is_latest, is_nullable );
                break;
            }
            case stock_cols::s_data: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, s_data,
                                   cid, is_latest, is_nullable );
                break;
            }
        }
        ret = ret and loc_ret;
    }
    return ret;
}

std::unordered_set<uint32_t> read_from_scan_stock( const result_tuple& res,
                                                   stock*              var ) {
    std::unordered_set<uint32_t> ret;

    for( const auto& cell : res.cells ) {
        bool loc_ret = false;
        switch( cell.col_id ) {
            case stock_cols::s_i_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            s_i_id );
                if( loc_ret ) {
                    ret.emplace( stock_cols::s_i_id );
                }
                break;
            }
            case stock_cols::s_w_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            s_w_id );
                if( loc_ret ) {
                    ret.emplace( stock_cols::s_w_id );
                }
                break;
            }
            case stock_cols::s_s_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            s_s_id );
                if( loc_ret ) {
                    ret.emplace( stock_cols::s_s_id );
                }
                break;
            }
            case stock_cols::s_quantity: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            s_quantity );
                if( loc_ret ) {
                    ret.emplace( stock_cols::s_quantity );
                }
                break;
            }
            case stock_cols::s_ytd: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            s_ytd );
                if( loc_ret ) {
                    ret.emplace( stock_cols::s_ytd );
                }
                break;
            }
            case stock_cols::s_order_cnt: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            s_order_cnt );
                if( loc_ret ) {
                    ret.emplace( stock_cols::s_order_cnt );
                }
                break;
            }
            case stock_cols::s_remote_cnt: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            s_remote_cnt );
                if( loc_ret ) {
                    ret.emplace( stock_cols::s_remote_cnt );
                }
                break;
            }
            case stock_cols::s_dist_0: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            s_dist_0 );
                if( loc_ret ) {
                    ret.emplace( stock_cols::s_dist_0 );
                }
                break;
            }
            case stock_cols::s_dist_1: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            s_dist_1 );
                if( loc_ret ) {
                    ret.emplace( stock_cols::s_dist_1 );
                }
                break;
            }
            case stock_cols::s_dist_2: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            s_dist_2 );
                if( loc_ret ) {
                    ret.emplace( stock_cols::s_dist_2 );
                }
                break;
            }
            case stock_cols::s_dist_3: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            s_dist_3 );
                if( loc_ret ) {
                    ret.emplace( stock_cols::s_dist_3 );
                }
                break;
            }
            case stock_cols::s_dist_4: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            s_dist_4 );
                if( loc_ret ) {
                    ret.emplace( stock_cols::s_dist_4 );
                }
                break;
            }
            case stock_cols::s_dist_5: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            s_dist_5 );
                if( loc_ret ) {
                    ret.emplace( stock_cols::s_dist_5 );
                }
                break;
            }
            case stock_cols::s_dist_6: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            s_dist_6 );
                if( loc_ret ) {
                    ret.emplace( stock_cols::s_dist_6 );
                }
                break;
            }
            case stock_cols::s_dist_7: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            s_dist_7 );
                if( loc_ret ) {
                    ret.emplace( stock_cols::s_dist_7 );
                }
                break;
            }
            case stock_cols::s_dist_8: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            s_dist_8 );
                if( loc_ret ) {
                    ret.emplace( stock_cols::s_dist_8 );
                }
                break;
            }
            case stock_cols::s_dist_9: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            s_dist_9 );
                if( loc_ret ) {
                    ret.emplace( stock_cols::s_dist_9 );
                }
                break;
            }
            case stock_cols::s_data: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            s_data );
                if( loc_ret ) {
                    ret.emplace( stock_cols::s_data );
                }
                break;
            }
        }
    }
    return ret;
}

void insert_supplier( benchmark_db_operators* db_ops, const supplier* var,
                      uint64_t row_id, const cell_key_ranges& ckr ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case supplier_cols::s_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, s_id, cid );
                break;
            }
            case supplier_cols::n_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, n_id, cid );
                break;
            }
            case supplier_cols::s_name: {
                DO_DB_OP( db_ops, insert_string, std::string, var, s_name,
                          cid );
                break;
            }
            case supplier_cols::s_phone: {
                DO_DB_OP( db_ops, insert_string, std::string, var, s_phone,
                          cid );
                break;
            }
            case supplier_cols::s_address_a_street_1: {
                DO_DB_OP( db_ops, insert_string, std::string, var,
                          s_address.a_street_1, cid );
                break;
            }
            case supplier_cols::s_address_a_street_2: {
                DO_DB_OP( db_ops, insert_string, std::string, var,
                          s_address.a_street_2, cid );
                break;
            }
            case supplier_cols::s_address_a_city: {
                DO_DB_OP( db_ops, insert_string, std::string, var,
                          s_address.a_city, cid );
                break;
            }
            case supplier_cols::s_address_a_state: {
                DO_DB_OP( db_ops, insert_string, std::string, var,
                          s_address.a_state, cid );
                break;
            }
            case supplier_cols::s_address_a_zip: {
                DO_DB_OP( db_ops, insert_string, std::string, var,
                          s_address.a_zip, cid );
                break;
            }
            case supplier_cols::s_acctbal: {
                DO_DB_OP( db_ops, insert_double, double, var, s_acctbal, cid );
                break;
            }
            case supplier_cols::s_comment: {
                DO_DB_OP( db_ops, insert_string, std::string, var, s_comment,
                          cid );
                break;
            }
        }
    }
}

void update_supplier( benchmark_db_operators* db_ops, const supplier* var,
                      uint64_t row_id, const cell_key_ranges& ckr,
                      bool do_propagate ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case supplier_cols::s_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, s_id, cid );
                break;
            }
            case supplier_cols::n_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, n_id, cid );
                break;
            }
            case supplier_cols::s_name: {
                DO_DB_OP( db_ops, write_string, std::string, var, s_name, cid );
                break;
            }
            case supplier_cols::s_phone: {
                DO_DB_OP( db_ops, write_string, std::string, var, s_phone,
                          cid );
                break;
            }
            case supplier_cols::s_address_a_street_1: {
                DO_DB_OP( db_ops, write_string, std::string, var,
                          s_address.a_street_1, cid );
                break;
            }
            case supplier_cols::s_address_a_street_2: {
                DO_DB_OP( db_ops, write_string, std::string, var,
                          s_address.a_street_2, cid );
                break;
            }
            case supplier_cols::s_address_a_city: {
                DO_DB_OP( db_ops, write_string, std::string, var,
                          s_address.a_city, cid );
                break;
            }
            case supplier_cols::s_address_a_state: {
                DO_DB_OP( db_ops, write_string, std::string, var,
                          s_address.a_state, cid );
                break;
            }
            case supplier_cols::s_address_a_zip: {
                DO_DB_OP( db_ops, write_string, std::string, var,
                          s_address.a_zip, cid );
                break;
            }
            case supplier_cols::s_acctbal: {
                DO_DB_OP( db_ops, write_double, double, var, s_acctbal, cid );
                break;
            }
            case supplier_cols::s_comment: {
                DO_DB_OP( db_ops, write_string, std::string, var, s_comment,
                          cid );
                break;
            }
        }
    }
}

bool lookup_supplier( benchmark_db_operators* db_ops, supplier* var,
                      uint64_t row_id, const cell_key_ranges& ckr,
                      bool is_latest, bool is_nullable ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );
    bool ret = true;

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        bool loc_ret = true;
        cid.col_id_ = col;
        switch( col ) {
            case supplier_cols::s_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, s_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case supplier_cols::n_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, n_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case supplier_cols::s_name: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, s_name,
                                   cid, is_latest, is_nullable );
                break;
            }
            case supplier_cols::s_phone: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, s_phone,
                                   cid, is_latest, is_nullable );
                break;
            }
            case supplier_cols::s_address_a_street_1: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   s_address.a_street_1, cid, is_latest,
                                   is_nullable );
                break;
            }
            case supplier_cols::s_address_a_street_2: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   s_address.a_street_2, cid, is_latest,
                                   is_nullable );
                break;
            }
            case supplier_cols::s_address_a_city: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   s_address.a_city, cid, is_latest,
                                   is_nullable );
                break;
            }
            case supplier_cols::s_address_a_state: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   s_address.a_state, cid, is_latest,
                                   is_nullable );
                break;
            }
            case supplier_cols::s_address_a_zip: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   s_address.a_zip, cid, is_latest,
                                   is_nullable );
                break;
            }
            case supplier_cols::s_acctbal: {
                DO_DOUBLE_READ_OP( loc_ret, db_ops, float, var, s_acctbal, cid,
                                   is_latest, is_nullable );
                break;
            }
            case supplier_cols::s_comment: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, s_comment,
                                   cid, is_latest, is_nullable );
                break;
            }
        }
        ret = ret and loc_ret;
    }
    return ret;
}

std::unordered_set<uint32_t> read_from_scan_supplier( const result_tuple& res,
                                                      supplier* var ) {
    std::unordered_set<uint32_t> ret;

    for( const auto& cell : res.cells ) {
        bool loc_ret = false;
        switch( cell.col_id ) {
            case supplier_cols::s_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            s_id );
                if( loc_ret ) {
                    ret.emplace( supplier_cols::s_id );
                }
                break;
            }
            case supplier_cols::n_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            n_id );
                if( loc_ret ) {
                    ret.emplace( supplier_cols::n_id );
                }
                break;
            }
            case supplier_cols::s_name: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            s_name );
                if( loc_ret ) {
                    ret.emplace( supplier_cols::s_name );
                }
                break;
            }
            case supplier_cols::s_phone: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            s_phone );
                if( loc_ret ) {
                    ret.emplace( supplier_cols::s_phone );
                }
                break;
            }
            case supplier_cols::s_address_a_street_1: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            s_address.a_street_1 );
                if( loc_ret ) {
                    ret.emplace( supplier_cols::s_address_a_street_1 );
                }
                break;
            }
            case supplier_cols::s_address_a_street_2: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            s_address.a_street_2 );
                if( loc_ret ) {
                    ret.emplace( supplier_cols::s_address_a_street_2 );
                }
                break;
            }
            case supplier_cols::s_address_a_city: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            s_address.a_city );
                if( loc_ret ) {
                    ret.emplace( supplier_cols::s_address_a_city );
                }
                break;
            }
            case supplier_cols::s_address_a_state: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            s_address.a_state );
                if( loc_ret ) {
                    ret.emplace( supplier_cols::s_address_a_state );
                }
                break;
            }
            case supplier_cols::s_address_a_zip: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            s_address.a_zip );
                if( loc_ret ) {
                    ret.emplace( supplier_cols::s_address_a_zip );
                }
                break;
            }
            case supplier_cols::s_acctbal: {
                DO_SCAN_OP( loc_ret, string_to_double, float, cell, var,
                            s_acctbal );
                if( loc_ret ) {
                    ret.emplace( supplier_cols::s_acctbal );
                }
                break;
            }
            case supplier_cols::s_comment: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            s_comment );
                if( loc_ret ) {
                    ret.emplace( supplier_cols::s_comment );
                }
                break;
            }
        }
    }
    return ret;
}

void insert_warehouse( benchmark_db_operators* db_ops, const warehouse* var,
                       uint64_t row_id, const cell_key_ranges& ckr ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case warehouse_cols::w_id: {
                DO_DB_OP( db_ops, insert_int64, int64_t, var, w_id, cid );
                break;
            }
            case warehouse_cols::w_tax: {
                DO_DB_OP( db_ops, insert_double, double, var, w_tax, cid );
                break;
            }
            case warehouse_cols::w_ytd: {
                DO_DB_OP( db_ops, insert_double, double, var, w_ytd, cid );
                break;
            }
            case warehouse_cols::w_name: {
                DO_DB_OP( db_ops, insert_string, std::string, var, w_name,
                          cid );
                break;
            }
            case warehouse_cols::w_address_a_street_1: {
                DO_DB_OP( db_ops, insert_string, std::string, var,
                          w_address.a_street_1, cid );
                break;
            }
            case warehouse_cols::w_address_a_street_2: {
                DO_DB_OP( db_ops, insert_string, std::string, var,
                          w_address.a_street_2, cid );
                break;
            }
            case warehouse_cols::w_address_a_city: {
                DO_DB_OP( db_ops, insert_string, std::string, var,
                          w_address.a_city, cid );
                break;
            }
            case warehouse_cols::w_address_a_state: {
                DO_DB_OP( db_ops, insert_string, std::string, var,
                          w_address.a_state, cid );
                break;
            }
            case warehouse_cols::w_address_a_zip: {
                DO_DB_OP( db_ops, insert_string, std::string, var,
                          w_address.a_zip, cid );
                break;
            }
        }
    }
}

void update_warehouse( benchmark_db_operators* db_ops, const warehouse* var,
                       uint64_t row_id, const cell_key_ranges& ckr,
                       bool do_propagate ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        cid.col_id_ = col;
        switch( col ) {
            case warehouse_cols::w_id: {
                DO_DB_OP( db_ops, write_int64, int64_t, var, w_id, cid );
                break;
            }
            case warehouse_cols::w_tax: {
                DO_DB_OP( db_ops, write_double, double, var, w_tax, cid );
                break;
            }
            case warehouse_cols::w_ytd: {
                DO_DB_OP( db_ops, write_double, double, var, w_ytd, cid );
                break;
            }
            case warehouse_cols::w_name: {
                DO_DB_OP( db_ops, write_string, std::string, var, w_name, cid );
                break;
            }
            case warehouse_cols::w_address_a_street_1: {
                DO_DB_OP( db_ops, write_string, std::string, var,
                          w_address.a_street_1, cid );
                break;
            }
            case warehouse_cols::w_address_a_street_2: {
                DO_DB_OP( db_ops, write_string, std::string, var,
                          w_address.a_street_2, cid );
                break;
            }
            case warehouse_cols::w_address_a_city: {
                DO_DB_OP( db_ops, write_string, std::string, var,
                          w_address.a_city, cid );
                break;
            }
            case warehouse_cols::w_address_a_state: {
                DO_DB_OP( db_ops, write_string, std::string, var,
                          w_address.a_state, cid );
                break;
            }
            case warehouse_cols::w_address_a_zip: {
                DO_DB_OP( db_ops, write_string, std::string, var,
                          w_address.a_zip, cid );
                break;
            }
        }
    }
}

bool lookup_warehouse( benchmark_db_operators* db_ops, warehouse* var,
                       uint64_t row_id, const cell_key_ranges& ckr,
                       bool is_latest, bool is_nullable ) {
    DCHECK( db_ops );
    DCHECK( var );
    auto cid = create_cell_identifier( ckr.table_id, ckr.col_id_start, row_id );
    bool ret = true;

    for( int32_t col = ckr.col_id_start; col <= ckr.col_id_end; col++ ) {
        bool loc_ret = true;
        cid.col_id_ = col;
        switch( col ) {
            case warehouse_cols::w_id: {
                DO_INT64_READ_OP( loc_ret, db_ops, int32_t, var, w_id, cid,
                                  is_latest, is_nullable );
                break;
            }
            case warehouse_cols::w_tax: {
                DO_DOUBLE_READ_OP( loc_ret, db_ops, float, var, w_tax, cid,
                                   is_latest, is_nullable );
                break;
            }
            case warehouse_cols::w_ytd: {
                DO_DOUBLE_READ_OP( loc_ret, db_ops, float, var, w_ytd, cid,
                                   is_latest, is_nullable );
                break;
            }
            case warehouse_cols::w_name: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var, w_name,
                                   cid, is_latest, is_nullable );
                break;
            }
            case warehouse_cols::w_address_a_street_1: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   w_address.a_street_1, cid, is_latest,
                                   is_nullable );
                break;
            }
            case warehouse_cols::w_address_a_street_2: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   w_address.a_street_2, cid, is_latest,
                                   is_nullable );
                break;
            }
            case warehouse_cols::w_address_a_city: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   w_address.a_city, cid, is_latest,
                                   is_nullable );
                break;
            }
            case warehouse_cols::w_address_a_state: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   w_address.a_state, cid, is_latest,
                                   is_nullable );
                break;
            }
            case warehouse_cols::w_address_a_zip: {
                DO_STRING_READ_OP( loc_ret, db_ops, std::string, var,
                                   w_address.a_zip, cid, is_latest,
                                   is_nullable );
                break;
            }
        }
        ret = ret and loc_ret;
    }
    return ret;
}

std::unordered_set<uint32_t> read_from_scan_warehouse( const result_tuple& res,
                                                       warehouse* var ) {
    std::unordered_set<uint32_t> ret;

    for( const auto& cell : res.cells ) {
        bool loc_ret = false;
        switch( cell.col_id ) {
            case warehouse_cols::w_id: {
                DO_SCAN_OP( loc_ret, string_to_int64, int32_t, cell, var,
                            w_id );
                if( loc_ret ) {
                    ret.emplace( warehouse_cols::w_id );
                }
                break;
            }
            case warehouse_cols::w_tax: {
                DO_SCAN_OP( loc_ret, string_to_double, float, cell, var,
                            w_tax );
                if( loc_ret ) {
                    ret.emplace( warehouse_cols::w_tax );
                }
                break;
            }
            case warehouse_cols::w_ytd: {
                DO_SCAN_OP( loc_ret, string_to_double, float, cell, var,
                            w_ytd );
                if( loc_ret ) {
                    ret.emplace( warehouse_cols::w_ytd );
                }
                break;
            }
            case warehouse_cols::w_name: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            w_name );
                if( loc_ret ) {
                    ret.emplace( warehouse_cols::w_name );
                }
                break;
            }
            case warehouse_cols::w_address_a_street_1: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            w_address.a_street_1 );
                if( loc_ret ) {
                    ret.emplace( warehouse_cols::w_address_a_street_1 );
                }
                break;
            }
            case warehouse_cols::w_address_a_street_2: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            w_address.a_street_2 );
                if( loc_ret ) {
                    ret.emplace( warehouse_cols::w_address_a_street_2 );
                }
                break;
            }
            case warehouse_cols::w_address_a_city: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            w_address.a_city );
                if( loc_ret ) {
                    ret.emplace( warehouse_cols::w_address_a_city );
                }
                break;
            }
            case warehouse_cols::w_address_a_state: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            w_address.a_state );
                if( loc_ret ) {
                    ret.emplace( warehouse_cols::w_address_a_state );
                }
                break;
            }
            case warehouse_cols::w_address_a_zip: {
                DO_SCAN_OP( loc_ret, string_to_string, std::string, cell, var,
                            w_address.a_zip );
                if( loc_ret ) {
                    ret.emplace( warehouse_cols::w_address_a_zip );
                }
                break;
            }
        }
    }
    return ret;
}

