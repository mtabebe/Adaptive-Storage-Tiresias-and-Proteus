#include "tpcc_prep_stmts.h"

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "../../../benchmark/tpcc/record-types/tpcc_record_types.h"
#include "../../../benchmark/tpcc/tpcc_configs.h"
#include "../../../common/scan_results.h"
#include "../../../common/string_conversion.h"
#include "../sproc_helpers.h"

void serialize_tpcc_result( sproc_result &               res,
                            std::vector<void *> &        result_values,
                            const std::vector<arg_code> &result_codes ) {
    char * buff = NULL;
    size_t serialize_len =
        serialize_for_sproc( result_codes, result_values, &buff );
    DCHECK_GE( serialize_len, 4 );
    res.res_args.assign( buff, serialize_len );
}

sproc_result tpcc_create_database(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpcc_configs configs;

    DCHECK_EQ( codes.size(), values.size() );
    DCHECK_GE( codes.size(), 1 );
    if( codes.size() == 1 or values.at( 1 ) == nullptr ) {
        configs = construct_tpcc_configs();
    } else {
        configs = *(tpcc_configs *) values.at( 1 );
    }
    db *database = (db *) values.at( 0 );

    DVLOG( 5 ) << "Creating database for tpcc_benchmark from configs:"
               << configs;

    tpcc_create_tables( database, configs );
    sproc_helper->init( database );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result tpcc_no_op( transaction_partition_holder *      partition_holder,
                         const clientid                      id,
                         const std::vector<cell_key_ranges> &write_ckrs,
                         const std::vector<cell_key_ranges> &read_ckrs,
                         std::vector<arg_code> &             codes,
                         std::vector<void *> &values, void *sproc_opaque ) {
    check_args( k_tpcc_no_op_arg_codes, codes, values );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result tpcc_load_warehouse_and_districts(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    check_args( k_tpcc_load_warehouse_and_districts_arg_codes, codes, values );

    uint32_t w_id = *( (uint32_t *) values.at( 0 ) );

    tpcc_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );
    for( const auto &ckr : write_ckrs ) {
        if( ckr.table_id == k_tpcc_warehouse_table_id ) {
            loader->make_warehouse( w_id, ckr );
        } else {
            uint32_t d_id_start = get_district_from_key(
                ckr.table_id, ckr.row_id_start, loader->get_configs() );
            uint32_t d_id_end = get_district_from_key(
                ckr.table_id, ckr.row_id_end, loader->get_configs() );
            loader->make_districts( w_id, d_id_start, d_id_end, ckr );
        }
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}
sproc_result tpcc_load_warehouse(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_load_warehouse_arg_codes, codes, values );
    tpcc_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    uint32_t w_id = *( (uint32_t *) values.at( 0 ) );
    for( const auto &ckr : write_ckrs ) {
        loader->make_warehouse( w_id, ckr );
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result tpcc_load_assorted_districts(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_load_assorted_districts_arg_codes, codes, values );
    tpcc_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    uint32_t  w_id = *( (uint32_t *) values.at( 0 ) );
    uint32_t *d_ids = (uint32_t *) values.at( 1 );
    unsigned  num_d_ids = codes.at( 1 ).array_length;

    for( unsigned pos = 0; pos < num_d_ids; pos++ ) {
        uint32_t d_id = d_ids[pos];
        int64_t  row = make_w_d_key( d_id, w_id, loader->get_configs() );
        for( const auto &ckr : write_ckrs ) {
            if( ( row >= ckr.row_id_start ) and ( row <= ckr.row_id_end ) ) {
                loader->make_district( w_id, d_id, ckr );
            }
        }
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result tpcc_load_stock_range(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    check_args( k_tpcc_load_stock_range_arg_codes, codes, values );

    uint32_t w_id = *( (uint32_t *) values.at( 0 ) );
    uint64_t start = *( (uint64_t *) values.at( 1 ) );
    uint64_t end = *( (uint64_t *) values.at( 2 ) );

    tpcc_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );
    for( const auto &ckr : write_ckrs ) {
        uint32_t start_key = std::max(
            (uint32_t) start, get_item_from_key( ckr.table_id, ckr.row_id_start,
                                                 loader->get_configs() ) );
        uint32_t end_key = std::min(
            (uint32_t) end, get_item_from_key( ckr.table_id, ckr.row_id_end,
                                               loader->get_configs() ) );
        loader->load_stock_range( w_id, start_key, end_key, ckr );
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

#if 0  // MTODO-TPCC

sproc_result tpcc_load_assorted_stock(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_load_assorted_stock_arg_codes, codes, values );

    tpcc_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    uint32_t  w_id = *( (uint32_t *) values.at( 0 ) );
    uint64_t *item_ids = (uint64_t *) values.at( 1 );
    unsigned  num_items = codes.at( 1 ).array_length;

    for( unsigned pos = 0; pos < num_items; pos++ ) {
        uint64_t i_id = item_ids[pos];
        loader->load_stock( w_id, i_id );
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}
#endif

sproc_result tpcc_load_customer_and_history_range(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    check_args( k_tpcc_load_customer_and_history_range_arg_codes, codes,
                values );

    uint32_t w_id = *( (uint32_t *) values.at( 0 ) );
    uint32_t d_id = *( (uint32_t *) values.at( 1 ) );
    uint64_t start = *( (uint64_t *) values.at( 2 ) );
    uint64_t end = *( (uint64_t *) values.at( 3 ) );

    tpcc_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );
    for( const auto &ckr : write_ckrs ) {
        uint32_t start_key =
            std::max( (uint32_t) start,
                      get_customer_from_key( ckr.table_id, ckr.row_id_start,
                                             loader->get_configs() ) );
        uint32_t end_key = std::min(
            (uint32_t) end, get_customer_from_key( ckr.table_id, ckr.row_id_end,
                                                   loader->get_configs() ) );
        if( ckr.table_id == k_tpcc_customer_table_id ) {
            loader->load_customer_range( w_id, d_id, start_key, end_key, ckr );
        } else if( ckr.table_id == k_tpcc_history_table_id ) {
            loader->load_history_range( w_id, d_id, start_key, end_key, ckr );
        } else if( ckr.table_id == k_tpcc_customer_district_table_id ) {
            loader->load_customer_district_range( w_id, d_id, start_key,
                                                  end_key, ckr );
        }
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}
#if 0  // MTODO-TPCC

sproc_result tpcc_load_assorted_customers(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_load_assorted_customers_arg_codes, codes, values );
    tpcc_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    uint32_t  w_id = *( (uint32_t *) values.at( 0 ) );
    uint32_t  d_id = *( (uint32_t *) values.at( 1 ) );
    uint32_t *c_ids = (uint32_t *) values.at( 2 );
    unsigned  num_c_ids = codes.at( 2 ).array_length;

    for( unsigned pos = 0; pos < num_c_ids; pos++ ) {
        uint32_t c_id = c_ids[pos];
        loader->load_customer( w_id, d_id, c_id );
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}
sproc_result tpcc_load_assorted_history(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_load_assorted_customers_arg_codes, codes, values );
    tpcc_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    uint32_t  w_id = *( (uint32_t *) values.at( 0 ) );
    uint32_t  d_id = *( (uint32_t *) values.at( 1 ) );
    uint32_t *h_c_ids = (uint32_t *) values.at( 2 );
    unsigned  num_h_c_ids = codes.at( 2 ).array_length;

    for( unsigned pos = 0; pos < num_h_c_ids; pos++ ) {
        uint32_t h_c_id = h_c_ids[pos];
        loader->load_history( w_id, d_id, h_c_id );
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}
#endif

sproc_result tpcc_load_order( transaction_partition_holder *partition_holder,
                              const clientid                id,
                              const std::vector<cell_key_ranges> &write_ckrs,
                              const std::vector<cell_key_ranges> &read_ckrs,
                              std::vector<arg_code> &             codes,
                              std::vector<void *> &               values,
                              void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    check_args( k_tpcc_load_order_arg_codes, codes, values );

    uint32_t w_id = *( (uint32_t *) values.at( 0 ) );
    uint32_t d_id = *( (uint32_t *) values.at( 1 ) );
    uint32_t c_id = *( (uint32_t *) values.at( 2 ) );
    uint32_t o_id = *( (uint32_t *) values.at( 3 ) );
    uint32_t o_ol_cnt = *( (uint32_t *) values.at( 4 ) );

    // MTODO figure out these ckrs

    tpcc_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );
    loader->load_order( w_id, d_id, c_id, o_id, o_ol_cnt, write_ckrs );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

#if 0  // MTODO-TPCC

sproc_result tpcc_load_only_order(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_load_only_order_arg_codes, codes, values );
    tpcc_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    uint32_t w_id = *( (uint32_t *) values.at( 0 ) );
    uint32_t d_id = *( (uint32_t *) values.at( 1 ) );
    uint32_t c_id = *( (uint32_t *) values.at( 2 ) );
    uint32_t o_id = *( (uint32_t *) values.at( 3 ) );
    uint32_t o_ol_cnt = *( (uint32_t *) values.at( 4 ) );

    loader->load_only_order( w_id, d_id, c_id, o_id, o_ol_cnt );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}
sproc_result tpcc_load_order_lines(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_load_order_lines_arg_codes, codes, values );
    tpcc_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    uint32_t  w_id = *( (uint32_t *) values.at( 0 ) );
    uint32_t  d_id = *( (uint32_t *) values.at( 1 ) );
    uint32_t  o_id = *( (uint32_t *) values.at( 2 ) );
    uint32_t *ol_nums = (uint32_t *) values.at( 3 );

    unsigned num_order_lines = codes.at( 3 ).array_length;

    for( unsigned pos = 0; pos < num_order_lines; pos++ ) {
        uint32_t ol_num = ol_nums[pos];

        loader->load_order_line( w_id, d_id, o_id, ol_num );
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}
sproc_result tpcc_load_new_order(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_load_new_order_arg_codes, codes, values );
    tpcc_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    uint32_t w_id = *( (uint32_t *) values.at( 0 ) );
    uint32_t d_id = *( (uint32_t *) values.at( 1 ) );
    uint32_t o_id = *( (uint32_t *) values.at( 2 ) );

    loader->load_new_order( w_id, d_id, o_id );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}
#endif

sproc_result tpcc_load_item_range(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    check_args( k_tpcc_load_item_range_arg_codes, codes, values );

    tpcc_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );
    for( const auto &ckr : write_ckrs ) {
        loader->load_item_range( ckr );
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result tpcc_load_region_range(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    check_args( k_tpcc_load_region_range_arg_codes, codes, values );

    tpcc_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );
    for( const auto &ckr : write_ckrs ) {
        loader->load_region_range( ckr );
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result tpcc_load_nation_range(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    check_args( k_tpcc_load_nation_range_arg_codes, codes, values );

    tpcc_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );
    for( const auto &ckr : write_ckrs ) {
        loader->load_nation_range( ckr );
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result tpcc_load_supplier_range(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    check_args( k_tpcc_load_supplier_range_arg_codes, codes, values );

    tpcc_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );
    for( const auto &ckr : write_ckrs ) {
        loader->load_supplier_range( ckr );
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

void tpch_q1( transaction_partition_holder *partition_holder, const clientid id,
              const std::vector<scan_arguments> &scan_args,
              std::vector<arg_code> &codes, std::vector<void *> &values,
              void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q1_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    worker->do_q1_work_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpch_q2( transaction_partition_holder *partition_holder, const clientid id,
              const std::vector<scan_arguments> &scan_args,
              std::vector<arg_code> &codes, std::vector<void *> &values,
              void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q2_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    worker->do_q2_work_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpch_q3( transaction_partition_holder *partition_holder, const clientid id,
              const std::vector<scan_arguments> &scan_args,
              std::vector<arg_code> &codes, std::vector<void *> &values,
              void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q3_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    worker->do_q3_work_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpch_q4( transaction_partition_holder *partition_holder, const clientid id,
              const std::vector<scan_arguments> &scan_args,
              std::vector<arg_code> &codes, std::vector<void *> &values,
              void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q4_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    worker->do_q4_work_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpch_q5( transaction_partition_holder *partition_holder, const clientid id,
              const std::vector<scan_arguments> &scan_args,
              std::vector<arg_code> &codes, std::vector<void *> &values,
              void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q4_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    worker->do_q5_work_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpch_q6( transaction_partition_holder *partition_holder, const clientid id,
              const std::vector<scan_arguments> &scan_args,
              std::vector<arg_code> &codes, std::vector<void *> &values,
              void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q6_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    worker->do_q6_work_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpch_q7( transaction_partition_holder *partition_holder, const clientid id,
              const std::vector<scan_arguments> &scan_args,
              std::vector<arg_code> &codes, std::vector<void *> &values,
              void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q7_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    worker->do_q7_work_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpch_q8( transaction_partition_holder *partition_holder, const clientid id,
              const std::vector<scan_arguments> &scan_args,
              std::vector<arg_code> &codes, std::vector<void *> &values,
              void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q8_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    worker->do_q8_work_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpch_q9( transaction_partition_holder *partition_holder, const clientid id,
              const std::vector<scan_arguments> &scan_args,
              std::vector<arg_code> &codes, std::vector<void *> &values,
              void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q9_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    worker->do_q9_work_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpch_q10( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q10_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    worker->do_q10_work_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpch_q11( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q11_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    float mult = *( (float *) values.at( 0 ) );

    worker->do_q11_work_by_scan( map_scan_args( scan_args ), ret_val, mult );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpch_q12( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q12_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    worker->do_q12_work_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpch_q13( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q13_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    worker->do_q13_work_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}


void tpch_q14( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q14_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    worker->do_q14_work_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpch_q15( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q15_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    worker->do_q15_work_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpch_q16( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q16_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    worker->do_q16_work_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpch_q17( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q17_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    worker->do_q17_work_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpch_q18( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q18_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    float ol_amount_threshold = *( (float *) values.at( 0 ) );

    worker->do_q18_work_by_scan( map_scan_args( scan_args ), ret_val,
                                 ol_amount_threshold );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpch_q19( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q19_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    worker->do_q19_work_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpch_q20( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q20_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    float s_quant_mult = *( (float *) values.at( 0 ) );

    worker->do_q20_work_by_scan( map_scan_args( scan_args ), ret_val,
                                 s_quant_mult );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpch_q21( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q21_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    worker->do_q21_work_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpch_q22( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val ) {
    check_args( k_tpch_q22_arg_codes, codes, values );

    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    tpch_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_h_worker_and_set_holder( id, partition_holder );

    worker->do_q22_work_by_scan( map_scan_args( scan_args ), ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

#if 0  // MTODO-TPCC
sproc_result tpcc_load_items_arg_range(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    check_args( k_tpcc_load_items_arg_range_arg_codes, codes, values );

    uint64_t *item_ids = (uint64_t *) values.at( 0 );
    int32_t * i_im_ids = (int32_t *) values.at( 1 );
    float *   i_prices = (float *) values.at( 2 );
    int32_t * i_name_lens = (int32_t *) values.at( 3 );
    char *    i_names = (char *) values.at( 4 );
    int32_t * i_data_lens = (int32_t *) values.at( 5 );
    char *    i_datas = (char *) values.at( 6 );

    unsigned num_items = codes.at( 0 ).array_length;

    DCHECK_EQ( num_items, codes.at( 1 ).array_length );  // im_ids
    DCHECK_EQ( num_items, codes.at( 2 ).array_length );  // i_prices
    DCHECK_EQ( num_items, codes.at( 3 ).array_length );  // i_name_lens
    DCHECK_EQ( num_items, codes.at( 5 ).array_length );  // i_data_lens

    tpcc_loader_templ_no_commit *loader =
        sproc_helper->get_loader_and_set_holder( id, partition_holder );

    unsigned all_name_len = codes.at( 4 ).array_length;
    unsigned all_data_len = codes.at( 6 ).array_length;

    int32_t name_start = 0;
    int32_t data_start = 0;
    for( unsigned pos = 0; pos < num_items; pos++ ) {
        uint64_t i_id = item_ids[pos];
        int32_t  i_im_id = i_im_ids[pos];
        float    i_price = i_prices[pos];
        int32_t  i_name_len = i_name_lens[pos];
        int32_t  i_data_len = i_data_lens[pos];

        DCHECK_LE( name_start + i_name_len, all_name_len );
        DCHECK_LE( data_start + i_data_len, all_data_len );

        char *i_name = i_names + name_start;
        char *i_data = i_datas + data_start;

        loader->load_item_with_args( i_id, i_im_id, i_price, i_name_len, i_name,
                                     i_data_len, i_data );

        name_start += i_name_len;
        data_start += i_data_len;
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}
#endif

sproc_result tpcc_new_order_fetch_and_set_next_order_on_district(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    check_args( k_tpcc_new_order_fetch_and_set_next_order_on_district_arg_codes,
                codes, values );

    int32_t w_id = *( (int32_t *) values.at( 0 ) );
    int32_t d_id = *( (int32_t *) values.at( 1 ) );
    bool    is_abort = *( (bool *) values.at( 2 ) );

    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    district *d =
        worker->fetch_and_set_next_order_on_district( w_id, d_id, is_abort );

    sproc_result res;
    res.status = exec_status_type::NONFATAL_ERROR;

    uint32_t o_id = 0;
    bool     update_ok = false;

    if( d != nullptr ) {
        res.status = exec_status_type::COMMAND_OK;
        o_id = d->d_next_o_id - 1;
        update_ok = true;
    }
    std::vector<void *> ret_ptrs = {(void *) &update_ok, (void *) &o_id};
    serialize_tpcc_result(
        res, ret_ptrs,
        k_tpcc_new_order_fetch_and_set_next_order_on_district_return_arg_codes );
    delete d;

    return res;
}
sproc_result tpcc_new_order_fetch_and_set_next_order_on_customer(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    check_args( k_tpcc_new_order_fetch_and_set_next_order_on_customer_arg_codes,
                codes, values );

    int32_t w_id = *( (int32_t *) values.at( 0 ) );
    int32_t d_id = *( (int32_t *) values.at( 1 ) );
    int32_t c_id = *( (int32_t *) values.at( 2 ) );
    bool    is_abort = *( (bool *) values.at( 3 ) );

    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    customer_district *d = worker->fetch_and_set_next_order_on_customer(
        w_id, d_id, c_id, is_abort );

    sproc_result res;
    res.status = exec_status_type::NONFATAL_ERROR;

    uint32_t o_id = 0;
    bool     update_ok = false;

    if( d != nullptr ) {
        res.status = exec_status_type::COMMAND_OK;
        o_id = d->c_next_o_id - 1;
        update_ok = true;
    }
    std::vector<void *> ret_ptrs = {(void *) &update_ok, (void *) &o_id};
    serialize_tpcc_result(
        res, ret_ptrs,
        k_tpcc_new_order_fetch_and_set_next_order_on_customer_return_arg_codes );

    delete d;

    return res;
}

sproc_result tpcc_new_order_place_new_orders(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;

    check_args( k_tpcc_new_order_place_new_orders_arg_codes, codes, values );

    int32_t  w_id = *( (int32_t *) values.at( 0 ) );
    int32_t  d_id = *( (int32_t *) values.at( 1 ) );
    int32_t  c_id = *( (int32_t *) values.at( 2 ) );
    int32_t  o_id = *( (int32_t *) values.at( 3 ) );
    int32_t  o_all_local = *( (int32_t *) values.at( 4 ) );
    int32_t *item_ids = (int32_t *) values.at( 5 );
    int32_t *supplier_w_ids = (int32_t *) values.at( 6 );
    int32_t *order_quantities = (int32_t *) values.at( 7 );

    int32_t o_ol_cnt = codes.at( 5 ).array_length;

    DCHECK_EQ( codes.at( 5 ).array_length, codes.at( 6 ).array_length );
    DCHECK_EQ( codes.at( 5 ).array_length, codes.at( 7 ).array_length );

    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    workload_operation_outcome_enum outcome =
        worker->place_new_orders( o_id, c_id, d_id, w_id, o_ol_cnt, o_all_local,
                                  item_ids, supplier_w_ids, order_quantities );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    if( outcome != WORKLOAD_OP_SUCCESS ) {
        res.status = exec_status_type::FATAL_ERROR;
    }

    return res;
}

#if 0 // MTODO-TPCC
sproc_result tpcc_new_order_get_warehouse_tax(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_new_order_get_warehouse_tax_arg_codes, codes, values );
    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    uint32_t w_id = *( (uint32_t *) values.at( 0 ) );

    float w_tax = worker->lookup_warehouse_tax( w_id );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    std::vector<void *> ret_ptrs = {(void *) &w_tax};

    serialize_tpcc_result(
        res, ret_ptrs, k_tpcc_new_order_get_warehouse_tax_return_arg_codes );

    return res;
}

sproc_result tpcc_new_order_get_district_tax(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_new_order_get_district_tax_arg_codes, codes, values );
    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    uint32_t w_id = *( (uint32_t *) values.at( 0 ) );
    uint32_t d_id = *( (uint32_t *) values.at( 1 ) );

    float d_tax = worker->lookup_district_tax( w_id, d_id );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    std::vector<void *> ret_ptrs = {(void *) &d_tax};

    serialize_tpcc_result( res, ret_ptrs,
                           k_tpcc_new_order_get_district_tax_return_arg_codes );

    return res;
}

sproc_result tpcc_new_order_get_customer_discount(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_new_order_get_customer_discount_arg_codes, codes,
                values );
    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    uint32_t w_id = *( (uint32_t *) values.at( 0 ) );
    uint32_t d_id = *( (uint32_t *) values.at( 1 ) );
    uint32_t c_id = *( (uint32_t *) values.at( 2 ) );

    float c_discount = worker->lookup_customer_discount( w_id, d_id, c_id );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    std::vector<void *> ret_ptrs = {(void *) &c_discount};

    serialize_tpcc_result(
        res, ret_ptrs,
        k_tpcc_new_order_get_customer_discount_return_arg_codes );

    return res;
}

sproc_result tpcc_new_order_update_stocks(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_new_order_update_stocks_arg_codes, codes, values );
    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    int32_t  order_w_id = *( (int32_t *) values.at( 0 ) );
    int32_t  order_d_id = *( (int32_t *) values.at( 1 ) );
    int32_t *item_ids = (int32_t *) values.at( 2 );
    int32_t *supplier_w_ids = (int32_t *) values.at( 3 );
    int32_t *order_quantities = (int32_t *) values.at( 4 );

    uint32_t num_orders = codes.at( 2 ).array_length;
    DCHECK_EQ( num_orders, codes.at( 3 ).array_length );
    DCHECK_EQ( num_orders, codes.at( 4 ).array_length );

    std::tuple<int32_t *, std::string> stock_descr =
        worker->new_order_update_stocks( order_w_id, order_d_id, num_orders,
                                         item_ids, supplier_w_ids,
                                         order_quantities );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;
    int32_t *          stock_descr_lens = std::get<0>( stock_descr );
    const std::string &stock_descr_str = std::get<1>( stock_descr );
    const char *       stock_descrs = stock_descr_str.c_str();

    std::vector<void *> return_args = {
        (void *) supplier_w_ids, (void *) item_ids, (void *) stock_descr_lens,
        (void *) stock_descrs};
    std::vector<arg_code> return_arg_codes = {INTEGER_ARRAY_CODE,
                                              INTEGER_ARRAY_CODE,
                                              INTEGER_ARRAY_CODE, STRING_CODE};
    return_arg_codes.at( 0 ).array_length = num_orders;
    return_arg_codes.at( 1 ).array_length = num_orders;
    return_arg_codes.at( 2 ).array_length = num_orders;
    return_arg_codes.at( 3 ).array_length = stock_descr_str.size();

    serialize_tpcc_result( res, return_args, return_arg_codes );

    delete[] stock_descr_lens;

    return res;
}

sproc_result tpcc_new_order_order_items(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_new_order_order_items_arg_codes, codes, values );
    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    int32_t w_id = *( (int32_t *) values.at( 0 ) );
    int32_t d_id = *( (int32_t *) values.at( 1 ) );
    int32_t c_id = *( (int32_t *) values.at( 2 ) );
    int32_t o_id = *( (int32_t *) values.at( 3 ) );

    int32_t *order_lines = (int32_t *) values.at( 4 );
    int32_t *item_ids = (int32_t *) values.at( 5 );
    int32_t *supplier_w_ids = (int32_t *) values.at( 6 );
    int32_t *order_quantities = (int32_t *) values.at( 7 );
    int32_t *stock_descr_lens = (int32_t *) values.at( 8 );
    char *   stock_descrs = (char *) values.at( 9 );

    uint32_t num_orders = codes.at( 4 ).array_length;
    DCHECK_EQ( num_orders, codes.at( 5 ).array_length );
    DCHECK_EQ( num_orders, codes.at( 6 ).array_length );
    DCHECK_EQ( num_orders, codes.at( 7 ).array_length );
    DCHECK_EQ( num_orders, codes.at( 8 ).array_length );

    float w_tax = worker->lookup_warehouse_tax( w_id );
    float d_tax = worker->lookup_district_tax( w_id, d_id );
    float c_discount = worker->lookup_customer_discount( w_id, d_id, c_id );

    worker->new_order_order_items( w_id, d_id, o_id, w_tax, d_tax, c_discount,
                                   num_orders, order_lines, order_quantities,
                                   item_ids, supplier_w_ids, stock_descr_lens,
                                   stock_descrs );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result tpcc_new_order_order_items_no_reads(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_new_order_order_items_no_reads_arg_codes, codes,
                values );
    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    int32_t w_id = *( (int32_t *) values.at( 0 ) );
    int32_t d_id = *( (int32_t *) values.at( 1 ) );
    int32_t o_id = *( (int32_t *) values.at( 2 ) );

    float w_tax = *( (float *) values.at( 3 ) );
    float d_tax = *( (float *) values.at( 4 ) );
    float c_discount = *( (float *) values.at( 5 ) );

    int32_t *order_lines = (int32_t *) values.at( 6 );
    int32_t *item_ids = (int32_t *) values.at( 7 );
    int32_t *supplier_w_ids = (int32_t *) values.at( 7 );
    int32_t *order_quantities = (int32_t *) values.at( 8 );
    int32_t *stock_descr_lens = (int32_t *) values.at( 9 );
    char *   stock_descrs = (char *) values.at( 10 );

    uint32_t num_orders = codes.at( 6 ).array_length;
    DCHECK_EQ( num_orders, codes.at( 7 ).array_length );
    DCHECK_EQ( num_orders, codes.at( 8 ).array_length );
    DCHECK_EQ( num_orders, codes.at( 9 ).array_length );
    DCHECK_EQ( num_orders, codes.at( 10 ).array_length );

    worker->new_order_order_items( w_id, d_id, o_id, w_tax, d_tax, c_discount,
                                   num_orders, order_lines, order_quantities,
                                   item_ids, supplier_w_ids, stock_descr_lens,
                                   stock_descrs );
    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result tpcc_new_order_set_order(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_new_order_set_order_arg_codes, codes, values );
    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    int32_t w_id = *( (int32_t *) values.at( 0 ) );
    int32_t d_id = *( (int32_t *) values.at( 1 ) );
    int32_t c_id = *( (int32_t *) values.at( 2 ) );
    int32_t o_id = *( (int32_t *) values.at( 3 ) );
    int32_t o_ol_cnt = *( (int32_t *) values.at( 4 ) );
    int32_t o_all_local = *( (int32_t *) values.at( 5 ) );

    worker->new_order_set_order( o_id, w_id, d_id, c_id, o_ol_cnt,
                                 o_all_local );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result tpcc_new_order_set_new_order(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_new_order_set_new_order_arg_codes, codes, values );
    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    int32_t w_id = *( (int32_t *) values.at( 0 ) );
    int32_t d_id = *( (int32_t *) values.at( 1 ) );
    int32_t o_id = *( (int32_t *) values.at( 2 ) );

    worker->new_order_set_new_order( o_id, w_id, d_id );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result tpcc_stock_level( transaction_partition_holder *partition_holder,
                               const clientid id, std::vector<arg_code> &codes,
                               std::vector<void *> &values,
                               void *               sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_stock_level_arg_codes, codes, values );
    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    int32_t w_id = *( (int32_t *) values.at( 0 ) );
    int32_t d_id = *( (int32_t *) values.at( 1 ) );
    int32_t stock_threshold = *( (int32_t *) values.at( 2 ) );

    int32_t num_distinct_items =
        worker->perform_stock_level_work( w_id, d_id, stock_threshold );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    if( num_distinct_items < 0 ) {
        res.status = exec_status_type::FATAL_ERROR;
    }

    return res;
}
#endif

sproc_result tpcc_stock_level_get_max_order_id_to_look_back_on(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_stock_level_get_max_order_id_to_look_back_on_arg_codes,
                codes, values );
    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    int32_t w_id = *( (int32_t *) values.at( 0 ) );
    int32_t d_id = *( (int32_t *) values.at( 1 ) );

    int32_t max_o_id = worker->get_max_order_id_to_look_back_on( w_id, d_id );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    std::vector<void *> return_args = {(void *) &max_o_id};

    serialize_tpcc_result(
        res, return_args,
        k_tpcc_stock_level_get_max_order_id_to_look_back_on_return_arg_codes );

    return res;
}

#if 0 // MTODO-TPCC
sproc_result tpcc_stock_level_get_item_ids_from_order_line(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_stock_level_get_item_ids_from_order_line_arg_codes,
                codes, values );
    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    int32_t w_id = *( (int32_t *) values.at( 0 ) );
    int32_t d_id = *( (int32_t *) values.at( 1 ) );
    int32_t *o_ids = (int32_t *) values.at( 2 );
    int32_t *order_line_nums = (int32_t *) values.at( 3 );

    int32_t num_order_ids = codes.at( 2 ).array_length;
    DCHECK_EQ( codes.at( 2 ).array_length, codes.at( 3 ).array_length );

    std::unordered_set<int32_t> item_ids;
    std::vector<int32_t>        item_ids_list;

    for( int32_t pos = 0; pos < num_order_ids; pos++ ) {
        int32_t     o_id_to_lookup = o_ids[pos];
        int32_t     ol_num = order_line_nums[pos];
        order_line *o_line =
            worker->lookup_order_line( w_id, d_id, o_id_to_lookup, ol_num );
        if( o_line != NULL ) {
            int32_t item_id = o_line->ol_i_id;
            if( item_ids.count( item_id ) == 0 ) {
                item_ids_list.emplace_back( item_id );
            }
            item_ids.emplace( item_id );
        }
    }

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    std::vector<void *>   return_args = {(void *) item_ids_list.data()};
    std::vector<arg_code> return_arg_codes = {INTEGER_ARRAY_CODE};
    return_arg_codes[0].array_length = item_ids_list.size();

    serialize_tpcc_result( res, return_args, return_arg_codes );

    return res;
}
#endif

void tpcc_stock_level_get_recent_items(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<scan_arguments> &scan_args, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque, scan_result &ret_val ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_stock_level_get_recent_items_arg_codes, codes, values );
    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    for( const auto &scan_arg : scan_args ) {
        std::vector<int32_t> item_ids_list =
            worker->get_recent_items_by_ckrs( scan_arg.read_ckrs );
        std::vector<result_tuple> res;
        for( const auto &i_id : item_ids_list ) {
            result_cell cell;
            cell.col_id = item_cols::i_id;
            cell.present = true;
            cell.data = int64_to_string( i_id );
            cell.type = data_type::type::INT64;
            result_tuple rt;
            rt.row_id = i_id;
            rt.table_id = k_tpcc_item_table_id;
            rt.cells.emplace_back( cell );
            res.emplace_back( rt );
        }
        ret_val.res_tuples[scan_arg.label] = res;
    }

    ret_val.status = exec_status_type::COMMAND_OK;
}

void tpcc_stock_level_get_stock_below_threshold(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<scan_arguments> &scan_args, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque, scan_result &ret_val ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_stock_level_get_stock_below_threshold_arg_codes, codes,
                values );
    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    worker->get_stock_below_threshold_by_scan( scan_args, ret_val );

    ret_val.status = exec_status_type::COMMAND_OK;
}

sproc_result tpcc_payment( transaction_partition_holder *      partition_holder,
                           const clientid                      id,
                           const std::vector<cell_key_ranges> &write_ckrs,
                           const std::vector<cell_key_ranges> &read_ckrs,
                           std::vector<arg_code> &             codes,
                           std::vector<void *> &values, void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_payment_arg_codes, codes, values );
    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    int32_t w_id = *( (int32_t *) values.at( 0 ) );
    int32_t d_id = *( (int32_t *) values.at( 1 ) );
    int32_t c_w_id = *( (int32_t *) values.at( 2 ) );
    int32_t c_d_id = *( (int32_t *) values.at( 3 ) );
    int32_t c_id = *( (int32_t *) values.at( 4 ) );
    float   h_amount = *( (float *) values.at( 5 ) );

    worker->perform_payment_work( w_id, d_id, c_w_id, c_d_id, c_id, h_amount );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

#if 0 // MTODO-TPCC
sproc_result tpcc_add_payment_to_warehouse(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_add_payment_to_warehouse_arg_codes, codes, values );
    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    int32_t w_id = *( (int32_t *) values.at( 0 ) );
    float   h_amount = *( (float *) values.at( 1 ) );

    worker->add_payment_to_warehouse( w_id, h_amount );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result tpcc_add_payment_to_district(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_add_payment_to_district_arg_codes, codes, values );
    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    int32_t w_id = *( (int32_t *) values.at( 0 ) );
    int32_t d_id = *( (int32_t *) values.at( 1 ) );
    float   h_amount = *( (float *) values.at( 2 ) );

    worker->add_payment_to_district( w_id, d_id, h_amount );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result tpcc_add_payment_to_history(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_add_payment_to_history_arg_codes, codes, values );
    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    int32_t w_id = *( (int32_t *) values.at( 0 ) );
    int32_t d_id = *( (int32_t *) values.at( 1 ) );
    int32_t c_w_id = *( (int32_t *) values.at( 2 ) );
    int32_t c_d_id = *( (int32_t *) values.at( 3 ) );
    int32_t c_id = *( (int32_t *) values.at( 4 ) );
    float   h_amount = *( (float *) values.at( 5 ) );

    worker->add_payment_to_history( w_id, d_id, c_w_id, c_d_id, c_id,
                                    h_amount );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}

sproc_result tpcc_add_payment_to_customer(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque ) {
    DCHECK( sproc_opaque );
    tpcc_sproc_helper_holder *sproc_helper =
        (tpcc_sproc_helper_holder *) sproc_opaque;
    check_args( k_tpcc_add_payment_to_customer_arg_codes, codes, values );
    tpcc_benchmark_worker_templ_no_commit *worker =
        sproc_helper->get_c_worker_and_set_holder( id, partition_holder );

    int32_t w_id = *( (int32_t *) values.at( 0 ) );
    int32_t d_id = *( (int32_t *) values.at( 1 ) );
    int32_t c_w_id = *( (int32_t *) values.at( 2 ) );
    int32_t c_d_id = *( (int32_t *) values.at( 3 ) );
    int32_t c_id = *( (int32_t *) values.at( 4 ) );
    float   h_amount = *( (float *) values.at( 5 ) );

    worker->add_payment_to_customer( w_id, d_id, c_w_id, c_d_id, c_id,
                                     h_amount );

    sproc_result res;
    res.status = exec_status_type::COMMAND_OK;

    return res;
}
#endif
