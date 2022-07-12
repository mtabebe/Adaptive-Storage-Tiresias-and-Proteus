#include "stored_procedures_executor.h"

#include "smallbank/smallbank_prep_stmts.h"
#include "tpcc/tpcc_prep_stmts.h"
#include "twitter/twitter_prep_stmts.h"
#include "ycsb/ycsb_prep_stmts.h"

std::unique_ptr<sproc_lookup_table> construct_ycsb_sproc_lookup_table() {
    std::unique_ptr<sproc_lookup_table> sproc_tab =
        std::make_unique<sproc_lookup_table>( 11, 1 );
    function_identifier create_func_id( k_create_tables_sproc_name,
                                        k_create_tables_arg_codes );
    sproc_tab->register_function( create_func_id, ycsb_create_database );

    function_identifier create_config_func_id(
        k_create_config_tables_sproc_name, k_create_config_tables_arg_codes );
    sproc_tab->register_function( create_config_func_id, ycsb_create_database );

    function_identifier read_sproc_id( k_ycsb_read_sproc_name,
                                       k_ycsb_read_arg_codes );
    sproc_tab->register_function( read_sproc_id, ycsb_read_record );
    function_identifier delete_sproc_id( k_ycsb_delete_sproc_name,
                                         k_ycsb_delete_arg_codes );
    sproc_tab->register_function( delete_sproc_id, ycsb_delete_record );

    function_identifier update_sproc_id( k_ycsb_update_sproc_name,
                                         k_ycsb_update_arg_codes );
    sproc_tab->register_function( update_sproc_id, ycsb_update_record );

    function_identifier rmw_sproc_id( k_ycsb_rmw_sproc_name,
                                      k_ycsb_rmw_arg_codes );
    sproc_tab->register_function( rmw_sproc_id, ycsb_rmw_record );

    function_identifier scan_sproc_id( k_ycsb_scan_sproc_name,
                                       k_ycsb_scan_arg_codes );
    sproc_tab->register_function( scan_sproc_id, ycsb_scan_records );

    function_identifier scan_data_sproc_id( k_ycsb_scan_data_sproc_name,
                                            k_ycsb_scan_data_arg_codes );
    sproc_tab->register_scan_function( scan_data_sproc_id, ycsb_scan_data );

    function_identifier insert_sproc_id( k_ycsb_insert_sproc_name,
                                         k_ycsb_insert_arg_codes );
    sproc_tab->register_function( insert_sproc_id, ycsb_insert_records );

    function_identifier ckr_insert_sproc_id( k_ycsb_ckr_insert_sproc_name,
                                             k_ycsb_ckr_insert_arg_codes );
    sproc_tab->register_function( ckr_insert_sproc_id, ycsb_ckr_insert );

    function_identifier mk_rmw_sproc_id( k_ycsb_mk_rmw_sproc_name,
                                         k_ycsb_mk_rmw_arg_codes );
    sproc_tab->register_function( mk_rmw_sproc_id, ycsb_mk_rmw );

    return sproc_tab;
}

std::unique_ptr<sproc_lookup_table> construct_tpcc_sproc_lookup_table() {
    std::unique_ptr<sproc_lookup_table> sproc_tab =
        // always update this to the number of sprocs
        std::make_unique<sproc_lookup_table>( 41, 24 );

    function_identifier create_func_id( k_create_tables_sproc_name,
                                        k_create_tables_arg_codes );
    sproc_tab->register_function( create_func_id, tpcc_create_database );

    function_identifier create_config_func_id(
        k_create_config_tables_sproc_name, k_create_config_tables_arg_codes );
    sproc_tab->register_function( create_config_func_id, tpcc_create_database );

    function_identifier no_op_func_id( k_tpcc_no_op_sproc_name,
                                       k_tpcc_no_op_arg_codes );
    sproc_tab->register_function( no_op_func_id, tpcc_no_op );

    function_identifier load_w_and_d_func_id(
        k_tpcc_load_warehouse_and_districts_sproc_name,
        k_tpcc_load_warehouse_and_districts_arg_codes );
    sproc_tab->register_function( load_w_and_d_func_id,
                                  tpcc_load_warehouse_and_districts );

    function_identifier load_warehouse_func_id(
        k_tpcc_load_warehouse_sproc_name, k_tpcc_load_warehouse_arg_codes );
    sproc_tab->register_function( load_warehouse_func_id, tpcc_load_warehouse );
    function_identifier load_assorted_districts_func_id(
        k_tpcc_load_assorted_districts_sproc_name,
        k_tpcc_load_assorted_districts_arg_codes );
    sproc_tab->register_function( load_assorted_districts_func_id,
                                  tpcc_load_assorted_districts );

    function_identifier load_stock_func_id( k_tpcc_load_stock_range_sproc_name,
                                            k_tpcc_load_stock_range_arg_codes );
    sproc_tab->register_function( load_stock_func_id, tpcc_load_stock_range );

#if 0  // MTODO-HTAP
    function_identifier load_assorted_stock_func_id(
        k_tpcc_load_assorted_stock_sproc_name,
        k_tpcc_load_assorted_stock_arg_codes );
    sproc_tab->register_function( load_assorted_stock_func_id,
                                  tpcc_load_assorted_stock );
#endif

    function_identifier load_cust_and_hist_func_id(
        k_tpcc_load_customer_and_history_range_sproc_name,
        k_tpcc_load_customer_and_history_range_arg_codes );
    sproc_tab->register_function( load_cust_and_hist_func_id,
                                  tpcc_load_customer_and_history_range );

#if 0  // MTODO-TPCC
    function_identifier load_assorted_customers_func_id(
        k_tpcc_load_assorted_customers_sproc_name,
        k_tpcc_load_assorted_customers_arg_codes );
    sproc_tab->register_function( load_assorted_customers_func_id,
                                  tpcc_load_assorted_customers );

    function_identifier load_assorted_history_func_id(
        k_tpcc_load_assorted_history_sproc_name,
        k_tpcc_load_assorted_history_arg_codes );
    sproc_tab->register_function( load_assorted_history_func_id,
                                  tpcc_load_assorted_history );
#endif

    function_identifier load_order_func_id( k_tpcc_load_order_sproc_name,
                                            k_tpcc_load_order_arg_codes );
    sproc_tab->register_function( load_order_func_id, tpcc_load_order );

#if 0  // MTODO-TPCC
    function_identifier load_only_order_func_id(
        k_tpcc_load_only_order_sproc_name, k_tpcc_load_only_order_arg_codes );
    sproc_tab->register_function( load_only_order_func_id,
                                  tpcc_load_only_order );
    function_identifier load_order_lines_func_id(
        k_tpcc_load_order_lines_sproc_name, k_tpcc_load_order_lines_arg_codes );
    sproc_tab->register_function( load_order_lines_func_id,
                                  tpcc_load_order_lines );
    function_identifier load_new_order_func_id(
        k_tpcc_load_new_order_sproc_name, k_tpcc_load_new_order_arg_codes );
    sproc_tab->register_function( load_new_order_func_id, tpcc_load_new_order );
#endif

    function_identifier load_item_range_func_id(
        k_tpcc_load_item_range_sproc_name, k_tpcc_load_item_range_arg_codes );
    sproc_tab->register_function( load_item_range_func_id,
                                  tpcc_load_item_range );

    function_identifier load_region_range_func_id(
        k_tpcc_load_region_range_sproc_name,
        k_tpcc_load_region_range_arg_codes );
    sproc_tab->register_function( load_region_range_func_id,
                                  tpcc_load_region_range );
    function_identifier load_nation_range_func_id(
        k_tpcc_load_nation_range_sproc_name,
        k_tpcc_load_nation_range_arg_codes );
    sproc_tab->register_function( load_nation_range_func_id,
                                  tpcc_load_nation_range );
    function_identifier load_supplier_range_func_id(
        k_tpcc_load_supplier_range_sproc_name,
        k_tpcc_load_supplier_range_arg_codes );
    sproc_tab->register_function( load_supplier_range_func_id,
                                  tpcc_load_supplier_range );

    function_identifier q1_sproc_id( k_tpch_q1_sproc_name,
                                     k_tpch_q1_arg_codes );
    sproc_tab->register_scan_function( q1_sproc_id, tpch_q1 );

    function_identifier q2_sproc_id( k_tpch_q2_sproc_name,
                                     k_tpch_q2_arg_codes );
    sproc_tab->register_scan_function( q2_sproc_id, tpch_q2 );

    function_identifier q3_sproc_id( k_tpch_q3_sproc_name,
                                     k_tpch_q3_arg_codes );
    sproc_tab->register_scan_function( q3_sproc_id, tpch_q3 );

    function_identifier q4_sproc_id( k_tpch_q4_sproc_name,
                                     k_tpch_q4_arg_codes );
    sproc_tab->register_scan_function( q4_sproc_id, tpch_q4 );

    function_identifier q5_sproc_id( k_tpch_q5_sproc_name,
                                     k_tpch_q5_arg_codes );
    sproc_tab->register_scan_function( q5_sproc_id, tpch_q5 );

    function_identifier q6_sproc_id( k_tpch_q6_sproc_name,
                                     k_tpch_q6_arg_codes );
    sproc_tab->register_scan_function( q6_sproc_id, tpch_q6 );

    function_identifier q7_sproc_id( k_tpch_q7_sproc_name,
                                     k_tpch_q7_arg_codes );
    sproc_tab->register_scan_function( q7_sproc_id, tpch_q7 );

    function_identifier q14_sproc_id( k_tpch_q14_sproc_name,
                                      k_tpch_q14_arg_codes );
    sproc_tab->register_scan_function( q14_sproc_id, tpch_q14 );

    function_identifier q8_sproc_id( k_tpch_q8_sproc_name,
                                     k_tpch_q8_arg_codes );
    sproc_tab->register_scan_function( q8_sproc_id, tpch_q8 );

    function_identifier q9_sproc_id( k_tpch_q9_sproc_name,
                                     k_tpch_q9_arg_codes );
    sproc_tab->register_scan_function( q9_sproc_id, tpch_q9 );

    function_identifier q10_sproc_id( k_tpch_q10_sproc_name,
                                      k_tpch_q10_arg_codes );
    sproc_tab->register_scan_function( q10_sproc_id, tpch_q10 );

    function_identifier q11_sproc_id( k_tpch_q11_sproc_name,
                                      k_tpch_q11_arg_codes );
    sproc_tab->register_scan_function( q11_sproc_id, tpch_q11 );

    function_identifier q12_sproc_id( k_tpch_q12_sproc_name,
                                      k_tpch_q12_arg_codes );
    sproc_tab->register_scan_function( q12_sproc_id, tpch_q12 );

    function_identifier q13_sproc_id( k_tpch_q13_sproc_name,
                                      k_tpch_q13_arg_codes );
    sproc_tab->register_scan_function( q13_sproc_id, tpch_q13 );

    function_identifier q15_sproc_id( k_tpch_q15_sproc_name,
                                      k_tpch_q15_arg_codes );
    sproc_tab->register_scan_function( q15_sproc_id, tpch_q15 );

    function_identifier q16_sproc_id( k_tpch_q16_sproc_name,
                                      k_tpch_q16_arg_codes );
    sproc_tab->register_scan_function( q16_sproc_id, tpch_q16 );

    function_identifier q17_sproc_id( k_tpch_q17_sproc_name,
                                      k_tpch_q17_arg_codes );
    sproc_tab->register_scan_function( q17_sproc_id, tpch_q17 );

    function_identifier q18_sproc_id( k_tpch_q18_sproc_name,
                                      k_tpch_q18_arg_codes );
    sproc_tab->register_scan_function( q18_sproc_id, tpch_q18 );

    function_identifier q19_sproc_id( k_tpch_q19_sproc_name,
                                      k_tpch_q19_arg_codes );
    sproc_tab->register_scan_function( q19_sproc_id, tpch_q19 );

    function_identifier q20_sproc_id( k_tpch_q20_sproc_name,
                                      k_tpch_q20_arg_codes );
    sproc_tab->register_scan_function( q20_sproc_id, tpch_q20 );

    function_identifier q21_sproc_id( k_tpch_q21_sproc_name,
                                      k_tpch_q21_arg_codes );
    sproc_tab->register_scan_function( q21_sproc_id, tpch_q21 );

    function_identifier q22_sproc_id( k_tpch_q22_sproc_name,
                                      k_tpch_q22_arg_codes );
    sproc_tab->register_scan_function( q22_sproc_id, tpch_q22 );

#if 0  // MTODO-TPCC
    function_identifier load_items_arg_range_func_id(
        k_tpcc_load_items_arg_range_sproc_name,
        k_tpcc_load_items_arg_range_arg_codes );
    sproc_tab->register_function( load_items_arg_range_func_id,
                                  tpcc_load_items_arg_range );
#endif

    function_identifier new_order_fetch_next_order_district_func_id(
        k_tpcc_new_order_fetch_and_set_next_order_on_district_sproc_name,
        k_tpcc_new_order_fetch_and_set_next_order_on_district_arg_codes );
    sproc_tab->register_function(
        new_order_fetch_next_order_district_func_id,
        tpcc_new_order_fetch_and_set_next_order_on_district );

    function_identifier new_order_fetch_next_order_customer_func_id(
        k_tpcc_new_order_fetch_and_set_next_order_on_customer_sproc_name,
        k_tpcc_new_order_fetch_and_set_next_order_on_customer_arg_codes );
    sproc_tab->register_function(
        new_order_fetch_next_order_customer_func_id,
        tpcc_new_order_fetch_and_set_next_order_on_customer );

    function_identifier new_order_place_new_orders_func_id(
        k_tpcc_new_order_place_new_orders_sproc_name,
        k_tpcc_new_order_place_new_orders_arg_codes );
    sproc_tab->register_function( new_order_place_new_orders_func_id,
                                  tpcc_new_order_place_new_orders );

#if 0  // MTODO-TPCC

    function_identifier new_order_get_warehouse_tax_func_id(
        k_tpcc_new_order_get_warehouse_tax_sproc_name,
        k_tpcc_new_order_get_warehouse_tax_arg_codes );
    sproc_tab->register_function(
        new_order_get_warehouse_tax_func_id,
        tpcc_new_order_get_warehouse_tax );

    function_identifier new_order_get_district_tax_func_id(
        k_tpcc_new_order_get_district_tax_sproc_name,
        k_tpcc_new_order_get_district_tax_arg_codes );
    sproc_tab->register_function(
        new_order_get_district_tax_func_id,
        tpcc_new_order_get_district_tax );

    function_identifier new_order_get_customer_discount_func_id(
        k_tpcc_new_order_get_customer_discount_sproc_name,
        k_tpcc_new_order_get_customer_discount_arg_codes );
    sproc_tab->register_function(
        new_order_get_customer_discount_func_id,
        tpcc_new_order_get_customer_discount );

    function_identifier new_order_update_stocks_func_id(
        k_tpcc_new_order_update_stocks_sproc_name,
        k_tpcc_new_order_update_stocks_arg_codes );
    sproc_tab->register_function(
        new_order_update_stocks_func_id,
        tpcc_new_order_update_stocks );

#endif
#if 0
    function_identifier new_order_order_items_func_id(
        k_tpcc_new_order_order_items_sproc_name,
        k_tpcc_new_order_order_items_arg_codes );
    sproc_tab->register_function(
        new_order_order_items_func_id,
        tpcc_new_order_order_items );
#endif
#if 0  // MTODO-HTAP

    function_identifier new_order_order_items_no_reads_func_id(
        k_tpcc_new_order_order_items_no_reads_sproc_name,
        k_tpcc_new_order_order_items_no_reads_arg_codes );
    sproc_tab->register_function(
        new_order_order_items_no_reads_func_id,
        tpcc_new_order_order_items_no_reads );

    function_identifier new_order_set_order_func_id(
        k_tpcc_new_order_set_order_sproc_name,
        k_tpcc_new_order_set_order_arg_codes );
    sproc_tab->register_function(
        new_order_set_order_func_id,
        tpcc_new_order_set_order );

    function_identifier new_order_set_new_order_func_id(
        k_tpcc_new_order_set_new_order_sproc_name,
        k_tpcc_new_order_set_new_order_arg_codes );
    sproc_tab->register_function(
        new_order_set_new_order_func_id,
        tpcc_new_order_set_new_order );

    function_identifier stock_level_func_id(
        k_tpcc_stock_level_sproc_name,
        k_tpcc_stock_level_arg_codes );
    sproc_tab->register_function(
        stock_level_func_id,
        tpcc_stock_level );
#endif

    function_identifier stock_level_get_max_order_id_to_look_back_on_func_id(
        k_tpcc_stock_level_get_max_order_id_to_look_back_on_sproc_name,
        k_tpcc_stock_level_get_max_order_id_to_look_back_on_arg_codes );
    sproc_tab->register_function(
        stock_level_get_max_order_id_to_look_back_on_func_id,
        tpcc_stock_level_get_max_order_id_to_look_back_on );

#if 0  // MTODO-TPCC
    function_identifier stock_level_get_item_ids_from_order_line_func_id(
        k_tpcc_stock_level_get_item_ids_from_order_line_sproc_name,
        k_tpcc_stock_level_get_item_ids_from_order_line_arg_codes );
    sproc_tab->register_function(
        stock_level_get_item_ids_from_order_line_func_id,
        tpcc_stock_level_get_item_ids_from_order_line );
#endif

    function_identifier stock_level_get_recent_items_func_id(
        k_tpcc_stock_level_get_recent_items_sproc_name,
        k_tpcc_stock_level_get_recent_items_arg_codes );
    sproc_tab->register_scan_function( stock_level_get_recent_items_func_id,
                                       tpcc_stock_level_get_recent_items );

    function_identifier stock_level_get_stock_below_threshold_func_id(
        k_tpcc_stock_level_get_stock_below_threshold_sproc_name,
        k_tpcc_stock_level_get_stock_below_threshold_arg_codes );
    sproc_tab->register_scan_function(
        stock_level_get_stock_below_threshold_func_id,
        tpcc_stock_level_get_stock_below_threshold );

    function_identifier payment_func_id( k_tpcc_payment_sproc_name,
                                         k_tpcc_payment_arg_codes );
    sproc_tab->register_function( payment_func_id, tpcc_payment );

#if 0  // MTODO-TPCC
    function_identifier add_payment_to_warehouse_func_id(
        k_tpcc_add_payment_to_warehouse_sproc_name,
        k_tpcc_add_payment_to_warehouse_arg_codes );
    sproc_tab->register_function( add_payment_to_warehouse_func_id,
                                  tpcc_add_payment_to_warehouse );

    function_identifier add_payment_to_district_func_id(
        k_tpcc_add_payment_to_district_sproc_name,
        k_tpcc_add_payment_to_district_arg_codes );
    sproc_tab->register_function( add_payment_to_district_func_id,
                                  tpcc_add_payment_to_district );

    function_identifier add_payment_to_history_func_id(
        k_tpcc_add_payment_to_history_sproc_name,
        k_tpcc_add_payment_to_history_arg_codes );
    sproc_tab->register_function( add_payment_to_history_func_id,
                                  tpcc_add_payment_to_history );

    function_identifier add_payment_to_customer_func_id(
        k_tpcc_add_payment_to_customer_sproc_name,
        k_tpcc_add_payment_to_customer_arg_codes );
    sproc_tab->register_function( add_payment_to_customer_func_id,
                                  tpcc_add_payment_to_customer );
#endif

    return sproc_tab;
}

std::unique_ptr<sproc_lookup_table> construct_smallbank_sproc_lookup_table() {
    std::unique_ptr<sproc_lookup_table> sproc_tab =
        std::make_unique<sproc_lookup_table>( 18, 1 );

    function_identifier create_func_id( k_create_tables_sproc_name,
                                        k_create_tables_arg_codes );
    sproc_tab->register_function(
        create_func_id, smallbank_create_database );

    function_identifier create_config_func_id(
        k_create_config_tables_sproc_name, k_create_config_tables_arg_codes );
    sproc_tab->register_function(
        create_config_func_id,
        smallbank_create_database );

    function_identifier smallbank_load_range_of_accounts_func_id(
        k_smallbank_load_range_of_accounts_sproc_name,
        k_smallbank_load_range_of_accounts_arg_codes );
    sproc_tab->register_function(
        smallbank_load_range_of_accounts_func_id,
        smallbank_load_range_of_accounts );

    function_identifier smallbank_load_assorted_accounts_func_id(
        k_smallbank_load_assorted_accounts_sproc_name,
        k_smallbank_load_assorted_accounts_arg_codes );
    sproc_tab->register_function( smallbank_load_assorted_accounts_func_id,
                                  smallbank_load_assorted_accounts );

    function_identifier smallbank_load_assorted_checkings_func_id(
        k_smallbank_load_assorted_checkings_sproc_name,
        k_smallbank_load_assorted_checkings_arg_codes );
    sproc_tab->register_function( smallbank_load_assorted_checkings_func_id,
                                  smallbank_load_assorted_checkings );

    function_identifier smallbank_load_assorted_savings_func_id(
        k_smallbank_load_assorted_savings_sproc_name,
        k_smallbank_load_assorted_savings_arg_codes );
    sproc_tab->register_function( smallbank_load_assorted_savings_func_id,
                                  smallbank_load_assorted_savings );

    function_identifier smallbank_load_checking_with_args_func_id(
        k_smallbank_load_checking_with_args_sproc_name,
        k_smallbank_load_checking_with_args_arg_codes );
    sproc_tab->register_function( smallbank_load_checking_with_args_func_id,
                                  smallbank_load_checking_with_args );

    function_identifier smallbank_load_account_with_args_func_id(
        k_smallbank_load_account_with_args_sproc_name,
        k_smallbank_load_account_with_args_arg_codes );
    sproc_tab->register_function( smallbank_load_account_with_args_func_id,
                                  smallbank_load_account_with_args );

    function_identifier smallbank_load_saving_with_args_func_id(
        k_smallbank_load_saving_with_args_sproc_name,
        k_smallbank_load_saving_with_args_arg_codes );
    sproc_tab->register_function( smallbank_load_saving_with_args_func_id,
                                  smallbank_load_saving_with_args );

    function_identifier smallbank_amalgamate_func_id(
        k_smallbank_amalgamate_sproc_name, k_smallbank_amalgamate_arg_codes );
    sproc_tab->register_function( smallbank_amalgamate_func_id,
                                  smallbank_amalgamate );

    function_identifier smallbank_balance_func_id(
        k_smallbank_balance_sproc_name, k_smallbank_balance_arg_codes );
    sproc_tab->register_function( smallbank_balance_func_id,
                                  smallbank_balance );

    function_identifier smallbank_deposit_checking_func_id(
        k_smallbank_deposit_checking_sproc_name,
        k_smallbank_deposit_checking_arg_codes );
    sproc_tab->register_function(
        smallbank_deposit_checking_func_id,
        smallbank_deposit_checking );

    function_identifier smallbank_send_payment_func_id(
        k_smallbank_send_payment_sproc_name,
        k_smallbank_send_payment_arg_codes );
    sproc_tab->register_function(
        smallbank_send_payment_func_id,
        smallbank_send_payment );

    function_identifier smallbank_transact_savings_func_id(
        k_smallbank_transact_savings_sproc_name,
        k_smallbank_transact_savings_arg_codes );
    sproc_tab->register_function(
        smallbank_transact_savings_func_id,
        smallbank_transact_savings );

    function_identifier smallbank_write_check_func_id(
        k_smallbank_write_check_sproc_name, k_smallbank_write_check_arg_codes );
    sproc_tab->register_function( smallbank_write_check_func_id,
                                  smallbank_write_check );

    function_identifier smallbank_account_balance_func_id(
        k_smallbank_account_balance_sproc_name,
        k_smallbank_account_balance_arg_codes );
    sproc_tab->register_function(
        smallbank_account_balance_func_id,
        smallbank_account_balance );

    function_identifier smallbank_distributed_charge_account_func_id(
        k_smallbank_distributed_charge_account_sproc_name,
        k_smallbank_distributed_charge_account_arg_codes );
    sproc_tab->register_function( smallbank_distributed_charge_account_func_id,
                                  smallbank_distributed_charge_account );

#if 0 // MTODO-HTAP
#endif
    return sproc_tab;
}

std::unique_ptr<sproc_lookup_table> construct_twitter_sproc_lookup_table() {
    std::unique_ptr<sproc_lookup_table> sproc_tab =
        std::make_unique<sproc_lookup_table>( 20, 5 );

    function_identifier create_func_id( k_create_tables_sproc_name,
                                        k_create_tables_arg_codes );
    sproc_tab->register_function(
        create_func_id, twitter_create_database );

    function_identifier create_config_func_id(
        k_create_config_tables_sproc_name, k_create_config_tables_arg_codes );
    sproc_tab->register_function(
        create_config_func_id,
        twitter_create_database );

    function_identifier insert_entire_user_func_id(
        k_twitter_insert_entire_user_sproc_name,
        k_twitter_insert_entire_user_arg_codes );
    sproc_tab->register_function( insert_entire_user_func_id,
                                  twitter_insert_entire_user );

    function_identifier insert_user_func_id( k_twitter_insert_user_sproc_name,
                                             k_twitter_insert_user_arg_codes );
    sproc_tab->register_function( insert_user_func_id, twitter_insert_user );

    function_identifier insert_user_follows_func_id(
        k_twitter_insert_user_follows_sproc_name,
        k_twitter_insert_user_follows_arg_codes );
    sproc_tab->register_function( insert_user_follows_func_id,
                                  twitter_insert_user_follows );

    function_identifier insert_user_followers_func_id(
        k_twitter_insert_user_followers_sproc_name,
        k_twitter_insert_user_followers_arg_codes );
    sproc_tab->register_function( insert_user_followers_func_id,
                                  twitter_insert_user_followers );

    function_identifier insert_user_tweets_func_id(
        k_twitter_insert_user_tweets_sproc_name,
        k_twitter_insert_user_tweets_arg_codes );
    sproc_tab->register_function( insert_user_tweets_func_id,
                                  twitter_insert_user_tweets );

    function_identifier get_tweet_func_id(
        k_twitter_get_tweet_sproc_name, k_twitter_get_tweet_arg_codes );
    sproc_tab->register_scan_function( get_tweet_func_id, twitter_get_tweet );

    function_identifier get_tweets_from_following_func_id(
        k_twitter_get_tweets_from_following_sproc_name,
        k_twitter_get_tweets_from_following_arg_codes );
    sproc_tab->register_scan_function( get_tweets_from_following_func_id,
                                       twitter_get_tweets_from_following );

    function_identifier get_tweets_from_followers_func_id(
        k_twitter_get_tweets_from_followers_sproc_name,
        k_twitter_get_tweets_from_followers_arg_codes );
    sproc_tab->register_scan_function( get_tweets_from_followers_func_id,
                                       twitter_get_tweets_from_followers );

    function_identifier get_followers_func_id(
        k_twitter_get_followers_sproc_name, k_twitter_get_followers_arg_codes );
    sproc_tab->register_scan_function( get_followers_func_id,
                                       twitter_get_followers );

    function_identifier fetch_and_set_next_tweet_id_func_id(
        k_twitter_fetch_and_set_next_tweet_id_sproc_name,
        k_twitter_fetch_and_set_next_tweet_id_arg_codes );
    sproc_tab->register_function( fetch_and_set_next_tweet_id_func_id,
                                  twitter_fetch_and_set_next_tweet_id );
    function_identifier insert_tweet_func_id(
        k_twitter_insert_tweet_sproc_name, k_twitter_insert_tweet_arg_codes );
    sproc_tab->register_function( insert_tweet_func_id, twitter_insert_tweet );

    function_identifier get_follower_counts_func_id(
        k_twitter_get_follower_counts_sproc_name,
        k_twitter_get_follower_counts_arg_codes );
    sproc_tab->register_function( get_follower_counts_func_id,
                                  twitter_get_follower_counts );

    function_identifier update_follower_and_follows_func_id(
        k_twitter_update_follower_and_follows_sproc_name,
        k_twitter_update_follower_and_follows_arg_codes );
    sproc_tab->register_function( update_follower_and_follows_func_id,
                                  twitter_update_follower_and_follows );

    return sproc_tab;
}

std::unique_ptr<sproc_lookup_table> construct_sproc_lookup_table(
    const workload_type w_type ) {
    DVLOG( 1 ) << "Constructing sproc_lookup_table:"
               << workload_type_string( w_type );
    switch( w_type ) {
        case YCSB:
            return construct_ycsb_sproc_lookup_table();
        case TPCC:
            return construct_tpcc_sproc_lookup_table();
        case SMALLBANK:
            return construct_smallbank_sproc_lookup_table();
        case TWITTER:
            return construct_twitter_sproc_lookup_table();
        case UNKNOWN_WORKLOAD:
            LOG( WARNING ) << "Trying to run unknown workload type";
    }
    LOG( WARNING ) << "Unknown workload type";
    return nullptr;
}

std::unique_ptr<db> construct_database(
    std::shared_ptr<update_destination_generator> update_gen,
    std::shared_ptr<update_enqueuers> enqueuers, uint64_t num_clients,
    int32_t num_client_threads, const workload_type w_type,
    uint32_t site_location, uint32_t gc_sleep_time,
    bool enable_secondary_storage, const std::string& secondary_storage_dir ) {
    DVLOG( 1 ) << "Constructing sproc_lookup_table:"
               << workload_type_string( w_type );
    uint32_t num_tables = 0;
    switch( w_type ) {
        case YCSB:
            num_tables = 1;
            break;
        case TPCC:
            num_tables = 13;
            break;
        case SMALLBANK:
            num_tables = 3;
            break;
        case TWITTER:
            num_tables = 4;
            break;
        case UNKNOWN_WORKLOAD:
            LOG( WARNING ) << "Trying to run unknown workload type";
    }
    std::unique_ptr<db> database = std::make_unique<db>();
    database->init(
        update_gen, enqueuers,
        create_tables_metadata(
            num_tables, site_location, num_clients * num_client_threads,
            gc_sleep_time, enable_secondary_storage, secondary_storage_dir ) );
    return std::move( database );
}

void* construct_ycsb_opaque_pointer( const ycsb_configs& configs ) {
    ycsb_sproc_helper_holder* holder = new ycsb_sproc_helper_holder(
        configs,
        construct_db_abstraction_configs( db_abstraction_type::PLAIN_DB ) );
    return (void*) holder;
}
void* construct_tpcc_opaque_pointer( const tpcc_configs& configs ) {

    tpcc_sproc_helper_holder* holder = new tpcc_sproc_helper_holder(
        configs,
        construct_db_abstraction_configs( db_abstraction_type::PLAIN_DB ) );
    return (void*) holder;
}

void* construct_smallbank_opaque_pointer( const smallbank_configs& configs ) {

    smallbank_sproc_helper_holder* holder = new smallbank_sproc_helper_holder(
        configs,
        construct_db_abstraction_configs( db_abstraction_type::PLAIN_DB ) );
    return (void*) holder;
}

void* construct_twitter_opaque_pointer( const twitter_configs& configs ) {
    twitter_sproc_helper_holder* holder = new twitter_sproc_helper_holder(
        configs,
        construct_db_abstraction_configs( db_abstraction_type::PLAIN_DB ) );
    return (void*) holder;
}

void destroy_ycsb_opaque_pointer( void* opaque ) {
    DCHECK( opaque );
    ycsb_sproc_helper_holder* holder = (ycsb_sproc_helper_holder*) opaque;
    delete holder;

}
void destroy_tpcc_opaque_pointer( void* opaque ) {
    DCHECK( opaque );
    tpcc_sproc_helper_holder* holder = (tpcc_sproc_helper_holder*) opaque;
    delete holder;
}

void destroy_smallbank_opaque_pointer( void* opaque ) {
    DCHECK( opaque );
    smallbank_sproc_helper_holder* holder =
        (smallbank_sproc_helper_holder*) opaque;
    delete holder;
}
void destroy_twitter_opaque_pointer( void* opaque ) {
    DCHECK( opaque );
    twitter_sproc_helper_holder* holder = (twitter_sproc_helper_holder*) opaque;
    delete holder;
}

void* construct_sproc_opaque_pointer( const workload_type w_type ) {
    switch( w_type ) {
        case YCSB:
            return construct_ycsb_opaque_pointer();
        case TPCC:
            return construct_tpcc_opaque_pointer();
        case SMALLBANK:
            return construct_smallbank_opaque_pointer();
        case TWITTER:
            return construct_twitter_opaque_pointer();
        case UNKNOWN_WORKLOAD:
            LOG( WARNING ) << "Trying to run unknown workload type";
    }
    LOG( WARNING ) << "Unknown workload type";
    return nullptr;
}
void destroy_sproc_opaque_pointer( void* opaque, const workload_type w_type ) {
    if( opaque == nullptr ) {
        return;
    }
    switch( w_type ) {
        case YCSB:
            return destroy_ycsb_opaque_pointer( opaque );
        case TPCC:
            return destroy_tpcc_opaque_pointer( opaque );
        case SMALLBANK:
            return destroy_smallbank_opaque_pointer( opaque );
        case TWITTER:
            return destroy_smallbank_opaque_pointer( opaque );
        case UNKNOWN_WORKLOAD:
            LOG( WARNING ) << "Trying to run unknown workload type";
    }
    LOG( WARNING ) << "Unknown workload type";
}
