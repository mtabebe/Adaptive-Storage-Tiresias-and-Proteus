#pragma once

#include "../../../benchmark/tpcc/tpcc_loader.h"
#include "../../../templates/tpcc_benchmark_types.h"
#include "../../site-manager/serialize.h"
#include "../../site-manager/sproc_lookup_table.h"
#include "tpcc_sproc_helper_holder.h"

static const std::string           k_tpcc_no_op_sproc_name = "tpcc_no_op";
static const std::vector<arg_code> k_tpcc_no_op_arg_codes = {};

static const std::string k_tpcc_load_warehouse_and_districts_sproc_name =
    "tpcc_load_warehouse_and_districts";
static const std::vector<arg_code>
                         k_tpcc_load_warehouse_and_districts_arg_codes = {INTEGER_CODE /*w_id*/};

static const std::string k_tpcc_load_warehouse_sproc_name =
    "tpcc_load_warehouse";
static const std::vector<arg_code> k_tpcc_load_warehouse_arg_codes = {
    INTEGER_CODE /*w_id*/};
static const std::string k_tpcc_load_assorted_districts_sproc_name =
    "tpcc_load_assorted_districts";
static const std::vector<arg_code> k_tpcc_load_assorted_districts_arg_codes = {
    INTEGER_CODE /*w_id*/, INTEGER_ARRAY_CODE /*d_ids*/};

static const std::string k_tpcc_load_item_range_sproc_name =
    "tpcc_load_item_range";
static const std::vector<arg_code> k_tpcc_load_item_range_arg_codes = {};

static const std::string k_tpcc_load_region_range_sproc_name =
    "tpcc_load_region_range";
static const std::vector<arg_code> k_tpcc_load_region_range_arg_codes = {};

static const std::string k_tpcc_load_nation_range_sproc_name =
    "tpcc_load_nation_range";
static const std::vector<arg_code> k_tpcc_load_nation_range_arg_codes = {};

static const std::string k_tpcc_load_supplier_range_sproc_name =
    "tpcc_load_supplier_range";
static const std::vector<arg_code> k_tpcc_load_supplier_range_arg_codes = {};

#if 0 // MTODO-TPCC
static const std::string k_tpcc_load_items_arg_range_sproc_name =
    "tpcc_load_items_arg_range";
static const std::vector<arg_code> k_tpcc_load_items_arg_range_arg_codes = {
    BIGINT_ARRAY_CODE /*i_id*/,
    INTEGER_ARRAY_CODE /*i_im_id*/,
    FLOAT_ARRAY_CODE /*i_price*/,
    INTEGER_ARRAY_CODE /*i_name_len*/,
    STRING_CODE /*i_name*/,
    INTEGER_ARRAY_CODE /*i_data_len*/,
    STRING_CODE /*i_data*/
};
#endif

static const std::string k_tpcc_load_customer_and_history_range_sproc_name =
    "tpcc_load_customer_and_history_range";
static const std::vector<arg_code>
    k_tpcc_load_customer_and_history_range_arg_codes = {
        INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, BIGINT_CODE /*start*/,
        BIGINT_CODE /*end*/};

#if 0 // MTODO-TPCC

static const std::string k_tpcc_load_assorted_customers_sproc_name =
    "tpcc_load_assorted_customers";
static const std::vector<arg_code> k_tpcc_load_assorted_customers_arg_codes = {
    INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, INTEGER_ARRAY_CODE /*c_ids*/};
static const std::string k_tpcc_load_assorted_history_sproc_name =
    "tpcc_load_assorted_history";
static const std::vector<arg_code> k_tpcc_load_assorted_history_arg_codes = {
    INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, INTEGER_ARRAY_CODE /*c_ids*/};
#endif

static const std::string k_tpcc_load_stock_range_sproc_name =
    "tpcc_load_stock_range";
static const std::vector<arg_code> k_tpcc_load_stock_range_arg_codes = {
    INTEGER_CODE /*w_id*/, BIGINT_CODE /*start*/, BIGINT_CODE /*end*/};

#if 0 // MTODO-TPCC
static const std::string k_tpcc_load_assorted_stock_sproc_name =
    "tpcc_load_assorted_stock";
static const std::vector<arg_code> k_tpcc_load_assorted_stock_arg_codes = {
    INTEGER_CODE /*w_id*/, BIGINT_ARRAY_CODE /* i_id*/};

#endif

static const std::string k_tpcc_load_order_sproc_name = "tpcc_load_order";
static const std::vector<arg_code> k_tpcc_load_order_arg_codes = {
    INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, INTEGER_CODE /*c_id*/,
    INTEGER_CODE /*o_id*/, INTEGER_CODE /*o_ol_cnt*/};

static const std::string           k_tpch_q1_sproc_name = "tpch_q1";
static const std::vector<arg_code> k_tpch_q1_arg_codes = {};

static const std::string           k_tpch_q2_sproc_name = "tpch_q2";
static const std::vector<arg_code> k_tpch_q2_arg_codes = {};

static const std::string           k_tpch_q3_sproc_name = "tpch_q3";
static const std::vector<arg_code> k_tpch_q3_arg_codes = {};

static const std::string           k_tpch_q4_sproc_name = "tpch_q4";
static const std::vector<arg_code> k_tpch_q4_arg_codes = {};

static const std::string           k_tpch_q5_sproc_name = "tpch_q5";
static const std::vector<arg_code> k_tpch_q5_arg_codes = {};

static const std::string           k_tpch_q6_sproc_name = "tpch_q6";
static const std::vector<arg_code> k_tpch_q6_arg_codes = {};

static const std::string           k_tpch_q7_sproc_name = "tpch_q7";
static const std::vector<arg_code> k_tpch_q7_arg_codes = {};

static const std::string           k_tpch_q8_sproc_name = "tpch_q8";
static const std::vector<arg_code> k_tpch_q8_arg_codes = {};

static const std::string           k_tpch_q9_sproc_name = "tpch_q9";
static const std::vector<arg_code> k_tpch_q9_arg_codes = {};

static const std::string           k_tpch_q10_sproc_name = "tpch_q10";
static const std::vector<arg_code> k_tpch_q10_arg_codes = {};

static const std::string           k_tpch_q11_sproc_name = "tpch_q11";
static const std::vector<arg_code> k_tpch_q11_arg_codes = {FLOAT_CODE};

static const std::string           k_tpch_q12_sproc_name = "tpch_q12";
static const std::vector<arg_code> k_tpch_q12_arg_codes = {};

static const std::string           k_tpch_q13_sproc_name = "tpch_q13";
static const std::vector<arg_code> k_tpch_q13_arg_codes = {};

static const std::string           k_tpch_q14_sproc_name = "tpch_q14";
static const std::vector<arg_code> k_tpch_q14_arg_codes = {};

static const std::string           k_tpch_q15_sproc_name = "tpch_q15";
static const std::vector<arg_code> k_tpch_q15_arg_codes = {};

static const std::string           k_tpch_q16_sproc_name = "tpch_q16";
static const std::vector<arg_code> k_tpch_q16_arg_codes = {};

static const std::string           k_tpch_q17_sproc_name = "tpch_q17";
static const std::vector<arg_code> k_tpch_q17_arg_codes = {};

static const std::string           k_tpch_q18_sproc_name = "tpch_q18";
static const std::vector<arg_code> k_tpch_q18_arg_codes = {FLOAT_CODE};

static const std::string           k_tpch_q19_sproc_name = "tpch_q19";
static const std::vector<arg_code> k_tpch_q19_arg_codes = {};

static const std::string           k_tpch_q20_sproc_name = "tpch_q20";
static const std::vector<arg_code> k_tpch_q20_arg_codes = {FLOAT_CODE};

static const std::string           k_tpch_q21_sproc_name = "tpch_q21";
static const std::vector<arg_code> k_tpch_q21_arg_codes = {};

static const std::string           k_tpch_q22_sproc_name = "tpch_q22";
static const std::vector<arg_code> k_tpch_q22_arg_codes = {};

#if 0 // MTODO-TPCC
static const std::string k_tpcc_load_only_order_sproc_name =
    "tpcc_load_only_order";
static const std::vector<arg_code> k_tpcc_load_only_order_arg_codes = {
    INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, INTEGER_CODE /*c_id*/,
    INTEGER_CODE /*o_id*/, INTEGER_CODE /*o_ol_cnt*/};
static const std::string k_tpcc_load_order_lines_sproc_name =
    "tpcc_load_order_lines";
static const std::vector<arg_code> k_tpcc_load_order_lines_arg_codes = {
    INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, INTEGER_CODE /*o_id*/,
    INTEGER_ARRAY_CODE /*o_ol_num*/};
static const std::string k_tpcc_load_new_order_sproc_name =
    "tpcc_load_new_order";
static const std::vector<arg_code> k_tpcc_load_new_order_arg_codes = {
    INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, INTEGER_CODE /*o_id*/};

static const std::string k_tpcc_stock_level_sproc_name = "tpcc_stock_level";
static const std::vector<arg_code> k_tpcc_stock_level_arg_codes = {
    INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/,
    INTEGER_CODE /*stock level threshold*/
};
#endif

static const std::string
    k_tpcc_stock_level_get_max_order_id_to_look_back_on_sproc_name =
        "tpcc_stock_level_get_max_order_id_to_look_back_on";
static const std::vector<arg_code>
    k_tpcc_stock_level_get_max_order_id_to_look_back_on_arg_codes = {
        INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/
};
static const std::vector<arg_code>
    k_tpcc_stock_level_get_max_order_id_to_look_back_on_return_arg_codes = {
        INTEGER_CODE /*o_id*/};

#if 0 // MTODO-TPCC
static const std::string
    k_tpcc_stock_level_get_item_ids_from_order_line_sproc_name =
        "tpcc_stock_level_get_item_ids_from_order_line";
static const std::vector<arg_code>
    k_tpcc_stock_level_get_item_ids_from_order_line_arg_codes = {
        INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/,
        INTEGER_ARRAY_CODE /* o_ids*/, INTEGER_ARRAY_CODE /*order_line_num*/
};
static const std::vector<arg_code>
    k_tpcc_stock_level_get_item_ids_from_order_line_return_arg_codes = {
        INTEGER_ARRAY_CODE /*item_ids*/};
#endif


static const std::string
    k_tpcc_stock_level_get_recent_items_sproc_name =
        "tpcc_stock_level_get_recent_items";
static const std::vector<arg_code>
    k_tpcc_stock_level_get_recent_items_arg_codes = {};


static const std::string
    k_tpcc_stock_level_get_stock_below_threshold_sproc_name =
        "tpcc_stock_level_get_stock_below_threshold";
static const std::vector<arg_code>
    k_tpcc_stock_level_get_stock_below_threshold_arg_codes = {};

static const std::string
    k_tpcc_new_order_fetch_and_set_next_order_on_district_sproc_name =
        "tpcc_new_order_fetch_and_set_next_order_on_district";
static const std::vector<arg_code>
    k_tpcc_new_order_fetch_and_set_next_order_on_district_arg_codes = {
        INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, BOOL_CODE /*is_abort*/};
static const std::vector<arg_code>
    k_tpcc_new_order_fetch_and_set_next_order_on_district_return_arg_codes = {
        BOOL_CODE /*is okay*/, INTEGER_CODE /*o_id*/};
static const std::string
    k_tpcc_new_order_fetch_and_set_next_order_on_customer_sproc_name =
        "tpcc_new_order_fetch_and_set_next_order_on_customer";
static const std::vector<arg_code>
    k_tpcc_new_order_fetch_and_set_next_order_on_customer_arg_codes = {
        INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, INTEGER_CODE /*c_id*/,
        BOOL_CODE /*is_abort*/};
static const std::vector<arg_code>
    k_tpcc_new_order_fetch_and_set_next_order_on_customer_return_arg_codes =
        k_tpcc_new_order_fetch_and_set_next_order_on_district_return_arg_codes;

static const std::string k_tpcc_new_order_place_new_orders_sproc_name =
    "tpcc_new_order_place_new_orders";
static const std::vector<arg_code> k_tpcc_new_order_place_new_orders_arg_codes =
    {
        INTEGER_CODE /*w_id*/,
        INTEGER_CODE /*d_id*/,
        INTEGER_CODE /*c_id*/,
        INTEGER_CODE /*o_id*/,
        INTEGER_CODE /*o_all_local*/,
        INTEGER_ARRAY_CODE /*item ids*/,
        INTEGER_ARRAY_CODE /*supplier w_ids */,
        INTEGER_ARRAY_CODE /*order_quantities*/
};

#if 0 // MTODO-TPCC

static const std::string k_tpcc_new_order_get_warehouse_tax_sproc_name =
    "tpcc_new_order_get_warehouse_tax";
static const std::vector<arg_code>
    k_tpcc_new_order_get_warehouse_tax_arg_codes = {INTEGER_CODE /*w_id*/};
static const std::vector<arg_code>
    k_tpcc_new_order_get_warehouse_tax_return_arg_codes = {
        FLOAT_CODE /*w_tax*/};

static const std::string k_tpcc_new_order_get_district_tax_sproc_name =
    "tpcc_new_order_get_district_tax";
static const std::vector<arg_code> k_tpcc_new_order_get_district_tax_arg_codes =
    {INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/};
static const std::vector<arg_code>
    k_tpcc_new_order_get_district_tax_return_arg_codes = {FLOAT_CODE /*d_tax*/};

static const std::string k_tpcc_new_order_get_customer_discount_sproc_name =
    "tpcc_new_order_get_customer_discount";
static const std::vector<arg_code>
    k_tpcc_new_order_get_customer_discount_arg_codes = {
        INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, INTEGER_CODE /*c_id*/};
static const std::vector<arg_code>
    k_tpcc_new_order_get_customer_discount_return_arg_codes = {
        FLOAT_CODE /*c_discount*/};

static const std::string k_tpcc_new_order_update_stocks_sproc_name =
    "tpcc_new_order_update_stocks";
static const std::vector<arg_code> k_tpcc_new_order_update_stocks_arg_codes = {
    INTEGER_CODE /*order_w_id*/, INTEGER_CODE /*order_d_id*/,
    INTEGER_ARRAY_CODE /*item_ids*/, INTEGER_ARRAY_CODE /*supplier_w_ids*/,
    INTEGER_ARRAY_CODE /*order_quantities*/
};
static const std::vector<arg_code>
    k_tpcc_new_order_update_stocks_return_arg_codes = {
        INTEGER_ARRAY_CODE /* supplier_w_ids*/, INTEGER_ARRAY_CODE /*item_ids*/,
        INTEGER_ARRAY_CODE /*stock descr lengths*/,
        STRING_CODE /*stock descrs*/};

static const std::string k_tpcc_new_order_order_items_sproc_name =
    "tpcc_new_order_order_items";
static const std::vector<arg_code> k_tpcc_new_order_order_items_arg_codes = {
    INTEGER_CODE /*order_w_id*/,
    INTEGER_CODE /*d_id*/,
    INTEGER_CODE /*c_id*/,
    INTEGER_CODE /*o_id*/,
    INTEGER_ARRAY_CODE /*order_lines*/,
    INTEGER_ARRAY_CODE /*item_ids*/,
    INTEGER_ARRAY_CODE /*supplier_w_ids*/,
    INTEGER_ARRAY_CODE /*order_quantities*/,
    INTEGER_ARRAY_CODE /*stock_descr_lens*/,
    STRING_CODE /*stock_descrs*/,
};

static const std::string k_tpcc_new_order_order_items_no_reads_sproc_name =
    "tpcc_new_order_order_items_no_reads";
static const std::vector<arg_code>
    k_tpcc_new_order_order_items_no_reads_arg_codes = {
        INTEGER_CODE /*order_w_id*/,
        INTEGER_CODE /*d_id*/,
        INTEGER_CODE /*o_id*/,
        FLOAT_CODE /*w_tax*/,
        FLOAT_CODE /*d_tax*/,
        FLOAT_CODE /*c_discount*/,
        INTEGER_ARRAY_CODE /*order_lines*/,
        INTEGER_ARRAY_CODE /*item_ids*/,
        INTEGER_ARRAY_CODE /*supplier_w_ids*/,
        INTEGER_ARRAY_CODE /*order_quantities*/,
        INTEGER_ARRAY_CODE /*stock_descr_lens*/,
        STRING_CODE /*stock_descrs*/,
};

static const std::string k_tpcc_new_order_set_order_sproc_name =
    "tpcc_new_order_set_order";
static const std::vector<arg_code> k_tpcc_new_order_set_order_arg_codes = {
    INTEGER_CODE /*w_id*/,     INTEGER_CODE /*d_id*/,
    INTEGER_CODE /*c_id*/,     INTEGER_CODE /*o_id*/,
    INTEGER_CODE /*o_ol_cnt*/, INTEGER_CODE /*o_all_local*/,
};

static const std::string k_tpcc_new_order_set_new_order_sproc_name =
    "tpcc_new_order_set_new_order";
static const std::vector<arg_code> k_tpcc_new_order_set_new_order_arg_codes = {
    INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, INTEGER_CODE /*o_id*/,
};
#endif

static const std::string           k_tpcc_payment_sproc_name = "tpcc_payment";
static const std::vector<arg_code> k_tpcc_payment_arg_codes = {
    INTEGER_CODE /*w_id*/,   INTEGER_CODE /*d_id*/, INTEGER_CODE /*c_w_id*/,
    INTEGER_CODE /*c_d_id*/, INTEGER_CODE /*c_id*/, FLOAT_CODE /*h_amount*/
};

#if 0 // MTODO-TPCC

static const std::string k_tpcc_add_payment_to_warehouse_sproc_name =
    "add_payment_to_warehouse";
static const std::vector<arg_code> k_tpcc_add_payment_to_warehouse_arg_codes = {
    INTEGER_CODE /*w_id*/, FLOAT_CODE /*h_amount*/
};

static const std::string k_tpcc_add_payment_to_district_sproc_name =
    "add_payment_to_district";
static const std::vector<arg_code> k_tpcc_add_payment_to_district_arg_codes = {
    INTEGER_CODE /*w_id*/, INTEGER_CODE /*d_id*/, FLOAT_CODE /*h_amount*/
};

static const std::string k_tpcc_add_payment_to_history_sproc_name =
    "add_payment_to_history";
static const std::vector<arg_code> k_tpcc_add_payment_to_history_arg_codes = {
    INTEGER_CODE /*w_id*/,   INTEGER_CODE /*d_id*/, INTEGER_CODE /*c_w_id*/,
    INTEGER_CODE /*c_d_id*/, INTEGER_CODE /*c_id*/, FLOAT_CODE /*h_amount*/
};

static const std::string k_tpcc_add_payment_to_customer_sproc_name =
    "add_payment_to_customer";
static const std::vector<arg_code> k_tpcc_add_payment_to_customer_arg_codes = {
    INTEGER_CODE /*w_id*/,   INTEGER_CODE /*d_id*/, INTEGER_CODE /*c_w_id*/,
    INTEGER_CODE /*c_d_id*/, INTEGER_CODE /*c_id*/, FLOAT_CODE /* h_amount*/
};
#endif

sproc_result tpcc_create_database(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );

sproc_result tpcc_no_op( transaction_partition_holder *      partition_holder,
                         const clientid                      id,
                         const std::vector<cell_key_ranges> &write_ckrs,
                         const std::vector<cell_key_ranges> &read_ckrs,
                         std::vector<arg_code> &             codes,
                         std::vector<void *> &values, void *sproc_opaque );

sproc_result tpcc_load_warehouse_and_districts(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );
sproc_result tpcc_load_warehouse(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );
sproc_result tpcc_load_assorted_districts(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );

sproc_result tpcc_load_customer_and_history_range(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );
#if 0 // MTODO-TPCC
sproc_result tpcc_load_assorted_customers(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
sproc_result tpcc_load_assorted_history(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
#endif

sproc_result tpcc_load_stock_range(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );
#if 0 // MTODO-TPCC
sproc_result tpcc_load_assorted_stock(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
#endif

sproc_result tpcc_load_order( transaction_partition_holder *partition_holder,
                              const clientid                id,
                              const std::vector<cell_key_ranges> &write_ckrs,
                              const std::vector<cell_key_ranges> &read_ckrs,
                              std::vector<arg_code> &             codes,
                              std::vector<void *> &values, void *sproc_opaque );
#if 0 // MTODO-TPCC
sproc_result tpcc_load_only_order(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
sproc_result tpcc_load_order_lines(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
sproc_result tpcc_load_new_order(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
#endif

sproc_result tpcc_load_item_range(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );

sproc_result tpcc_load_nation_range(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );

sproc_result tpcc_load_region_range(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );

sproc_result tpcc_load_supplier_range(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );

void tpch_q1( transaction_partition_holder *partition_holder, const clientid id,
              const std::vector<scan_arguments> &scan_args,
              std::vector<arg_code> &codes, std::vector<void *> &values,
              void *sproc_opaque, scan_result &ret_val );

void tpch_q2( transaction_partition_holder *partition_holder, const clientid id,
              const std::vector<scan_arguments> &scan_args,
              std::vector<arg_code> &codes, std::vector<void *> &values,
              void *sproc_opaque, scan_result &ret_val );

void tpch_q3( transaction_partition_holder *partition_holder, const clientid id,
              const std::vector<scan_arguments> &scan_args,
              std::vector<arg_code> &codes, std::vector<void *> &values,
              void *sproc_opaque, scan_result &ret_val );

void tpch_q4( transaction_partition_holder *partition_holder, const clientid id,
              const std::vector<scan_arguments> &scan_args,
              std::vector<arg_code> &codes, std::vector<void *> &values,
              void *sproc_opaque, scan_result &ret_val );

void tpch_q5( transaction_partition_holder *partition_holder, const clientid id,
              const std::vector<scan_arguments> &scan_args,
              std::vector<arg_code> &codes, std::vector<void *> &values,
              void *sproc_opaque, scan_result &ret_val );

void tpch_q6( transaction_partition_holder *partition_holder, const clientid id,
              const std::vector<scan_arguments> &scan_args,
              std::vector<arg_code> &codes, std::vector<void *> &values,
              void *sproc_opaque, scan_result &ret_val );

void tpch_q7( transaction_partition_holder *partition_holder, const clientid id,
              const std::vector<scan_arguments> &scan_args,
              std::vector<arg_code> &codes, std::vector<void *> &values,
              void *sproc_opaque, scan_result &ret_val );

void tpch_q8( transaction_partition_holder *partition_holder, const clientid id,
              const std::vector<scan_arguments> &scan_args,
              std::vector<arg_code> &codes, std::vector<void *> &values,
              void *sproc_opaque, scan_result &ret_val );

void tpch_q9( transaction_partition_holder *partition_holder, const clientid id,
              const std::vector<scan_arguments> &scan_args,
              std::vector<arg_code> &codes, std::vector<void *> &values,
              void *sproc_opaque, scan_result &ret_val );


void tpch_q10( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val );

void tpch_q11( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val );

void tpch_q12( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val );

void tpch_q13( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val );


void tpch_q14( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val );

void tpch_q15( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val );

void tpch_q16( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val );

void tpch_q17( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val );

void tpch_q18( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val );

void tpch_q19( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val );

void tpch_q20( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val );

void tpch_q21( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val );


void tpch_q22( transaction_partition_holder *partition_holder,
               const clientid id, const std::vector<scan_arguments> &scan_args,
               std::vector<arg_code> &codes, std::vector<void *> &values,
               void *sproc_opaque, scan_result &ret_val );

#if 0 // MTODO-TPCC
sproc_result tpcc_load_items_arg_range(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
#endif

sproc_result tpcc_new_order_fetch_and_set_next_order_on_district(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );
sproc_result tpcc_new_order_fetch_and_set_next_order_on_customer(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );

sproc_result tpcc_new_order_place_new_orders(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );

#if 0  // MTODO-TPCC

sproc_result tpcc_new_order_get_warehouse_tax(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
sproc_result tpcc_new_order_get_district_tax(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
sproc_result tpcc_new_order_get_customer_discount(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );

sproc_result tpcc_new_order_update_stocks(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
sproc_result tpcc_new_order_order_items(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
sproc_result tpcc_new_order_order_items_no_reads(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );

sproc_result tpcc_new_order_set_order(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
sproc_result tpcc_new_order_set_new_order(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );

sproc_result tpcc_stock_level( transaction_partition_holder *partition_holder,
                               const clientid id, std::vector<arg_code> &codes,
                               std::vector<void *> &values,
                               void *               sproc_opaque );
#endif

sproc_result tpcc_stock_level_get_max_order_id_to_look_back_on(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );

#if 0 // MTODO-TPCC
sproc_result tpcc_stock_level_get_item_ids_from_order_line(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
#endif

void tpcc_stock_level_get_recent_items(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<scan_arguments> &scan_args, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque, scan_result &ret_val );

void tpcc_stock_level_get_stock_below_threshold(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<scan_arguments> &scan_args, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque, scan_result &ret_val );

sproc_result tpcc_payment( transaction_partition_holder *      partition_holder,
                           const clientid                      id,
                           const std::vector<cell_key_ranges> &write_ckrs,
                           const std::vector<cell_key_ranges> &read_ckrs,
                           std::vector<arg_code> &             codes,
                           std::vector<void *> &values, void *sproc_opaque );

#if 0  // MTODO-TPCC
sproc_result tpcc_add_payment_to_warehouse(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );

sproc_result tpcc_add_payment_to_district(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );

sproc_result tpcc_add_payment_to_history(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );

sproc_result tpcc_add_payment_to_customer(
    transaction_partition_holder *partition_holder, const clientid id,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
#endif

void serialize_tpcc_result( sproc_result &               res,
                            std::vector<void *> &        result_values,
                            const std::vector<arg_code> &result_codes );

