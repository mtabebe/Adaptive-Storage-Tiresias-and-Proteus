#pragma once

// Including all these things became hella annoying. So I made a file to include
// other files, so you can just include this file in all your other files

#include "../../struct_conversion.h"
#include "address.h"
#include "customer.h"
#include "customer_district.h"
#include "datetime.h"
#include "district.h"
#include "history.h"
#include "item.h"
#include "nation.h"
#include "new_order.h"
#include "order.h"
#include "order_line.h"
#include "region.h"
#include "stock.h"
#include "supplier.h"
#include "warehouse.h"

#include "../../../common/cell_data_type.h"

static constexpr uint32_t k_tpcc_customer_num_columns = 22;
static const std::vector<cell_data_type> k_tpcc_customer_col_types = {
	cell_data_type::INT64 /* c_id */,
	cell_data_type::INT64 /* c_d_id */,
	cell_data_type::INT64 /* c_w_id */,
	cell_data_type::INT64 /* c_n_id */,
	cell_data_type::DOUBLE /* c_credit_lim */,
	cell_data_type::DOUBLE /* c_discount */,
	cell_data_type::DOUBLE /* c_balance */,
	cell_data_type::DOUBLE /* c_ytd_payment */,
	cell_data_type::INT64 /* c_payment_cnt */,
	cell_data_type::INT64 /* c_delivery_cnt */,
	cell_data_type::STRING /* c_first */,
	cell_data_type::STRING /* c_middle */,
	cell_data_type::STRING /* c_last */,
	cell_data_type::STRING /* c_address.a_street_1 */,
	cell_data_type::STRING /* c_address.a_street_2 */,
	cell_data_type::STRING /* c_address.a_city */,
	cell_data_type::STRING /* c_address.a_state */,
	cell_data_type::STRING /* c_address.a_zip */,
	cell_data_type::STRING /* c_phone */,
	cell_data_type::UINT64 /* c_since.c_since */,
	cell_data_type::STRING /* c_credit */,
	cell_data_type::STRING /* c_data */,
};

static constexpr uint32_t k_tpcc_customer_district_num_columns = 4;
static const std::vector<cell_data_type> k_tpcc_customer_district_col_types = {
	cell_data_type::INT64 /* c_id */,
	cell_data_type::INT64 /* c_d_id */,
	cell_data_type::INT64 /* c_w_id */,
	cell_data_type::INT64 /* c_next_o_id */,
};

static constexpr uint32_t k_tpcc_district_num_columns = 11;
static const std::vector<cell_data_type> k_tpcc_district_col_types = {
	cell_data_type::INT64 /* d_id */,
	cell_data_type::INT64 /* d_w_id */,
	cell_data_type::DOUBLE /* d_tax */,
	cell_data_type::DOUBLE /* d_ytd */,
	cell_data_type::INT64 /* d_next_o_id */,
	cell_data_type::STRING /* d_name */,
	cell_data_type::STRING /* d_address.a_street_1 */,
	cell_data_type::STRING /* d_address.a_street_2 */,
	cell_data_type::STRING /* d_address.a_city */,
	cell_data_type::STRING /* d_address.a_state */,
	cell_data_type::STRING /* d_address.a_zip */,
};

static constexpr uint32_t k_tpcc_history_num_columns = 8;
static const std::vector<cell_data_type> k_tpcc_history_col_types = {
	cell_data_type::INT64 /* h_c_id */,
	cell_data_type::INT64 /* h_c_d_id */,
	cell_data_type::INT64 /* h_c_w_id */,
	cell_data_type::INT64 /* h_d_id */,
	cell_data_type::INT64 /* h_w_id */,
	cell_data_type::DOUBLE /* h_amount */,
	cell_data_type::UINT64 /* h_date.c_since */,
	cell_data_type::STRING /* h_data */,
};

static constexpr uint32_t k_tpcc_item_num_columns = 5;
static const std::vector<cell_data_type> k_tpcc_item_col_types = {
	cell_data_type::INT64 /* i_id */,
	cell_data_type::INT64 /* i_im_id */,
	cell_data_type::DOUBLE /* i_price */,
	cell_data_type::STRING /* i_name */,
	cell_data_type::STRING /* i_data */,
};

static constexpr uint32_t k_tpcc_nation_num_columns = 4;
static const std::vector<cell_data_type> k_tpcc_nation_col_types = {
	cell_data_type::INT64 /* n_id */,
	cell_data_type::INT64 /* r_id */,
	cell_data_type::STRING /* n_name */,
	cell_data_type::STRING /* n_comment */,
};

static constexpr uint32_t k_tpcc_new_order_num_columns = 3;
static const std::vector<cell_data_type> k_tpcc_new_order_col_types = {
	cell_data_type::INT64 /* no_w_id */,
	cell_data_type::INT64 /* no_d_id */,
	cell_data_type::INT64 /* no_o_id */,
};

static constexpr uint32_t k_tpcc_order_num_columns = 8;
static const std::vector<cell_data_type> k_tpcc_order_col_types = {
	cell_data_type::INT64 /* o_id */,
	cell_data_type::INT64 /* o_c_id */,
	cell_data_type::INT64 /* o_d_id */,
	cell_data_type::INT64 /* o_w_id */,
	cell_data_type::INT64 /* o_carrier_id */,
	cell_data_type::INT64 /* o_ol_cnt */,
	cell_data_type::INT64 /* o_all_local */,
	cell_data_type::UINT64 /* o_entry_d.c_since */,
};

static constexpr uint32_t k_tpcc_order_line_num_columns = 10;
static const std::vector<cell_data_type> k_tpcc_order_line_col_types = {
	cell_data_type::INT64 /* ol_o_id */,
	cell_data_type::INT64 /* ol_d_id */,
	cell_data_type::INT64 /* ol_w_id */,
	cell_data_type::INT64 /* ol_number */,
	cell_data_type::INT64 /* ol_i_id */,
	cell_data_type::INT64 /* ol_supply_w_id */,
	cell_data_type::INT64 /* ol_quantity */,
	cell_data_type::DOUBLE /* ol_amount */,
	cell_data_type::UINT64 /* ol_delivery_d.c_since */,
	cell_data_type::STRING /* ol_dist_info */,
};

static constexpr uint32_t k_tpcc_region_num_columns = 3;
static const std::vector<cell_data_type> k_tpcc_region_col_types = {
	cell_data_type::INT64 /* r_id */,
	cell_data_type::STRING /* r_name */,
	cell_data_type::STRING /* r_comment */,
};

static constexpr uint32_t k_tpcc_stock_num_columns = 18;
static const std::vector<cell_data_type> k_tpcc_stock_col_types = {
	cell_data_type::INT64 /* s_i_id */,
	cell_data_type::INT64 /* s_w_id */,
	cell_data_type::INT64 /* s_s_id */,
	cell_data_type::INT64 /* s_quantity */,
	cell_data_type::INT64 /* s_ytd */,
	cell_data_type::INT64 /* s_order_cnt */,
	cell_data_type::INT64 /* s_remote_cnt */,
	cell_data_type::STRING /* s_dist_0 */,
	cell_data_type::STRING /* s_dist_1 */,
	cell_data_type::STRING /* s_dist_2 */,
	cell_data_type::STRING /* s_dist_3 */,
	cell_data_type::STRING /* s_dist_4 */,
	cell_data_type::STRING /* s_dist_5 */,
	cell_data_type::STRING /* s_dist_6 */,
	cell_data_type::STRING /* s_dist_7 */,
	cell_data_type::STRING /* s_dist_8 */,
	cell_data_type::STRING /* s_dist_9 */,
	cell_data_type::STRING /* s_data */,
};

static constexpr uint32_t k_tpcc_supplier_num_columns = 11;
static const std::vector<cell_data_type> k_tpcc_supplier_col_types = {
	cell_data_type::INT64 /* s_id */,
	cell_data_type::INT64 /* n_id */,
	cell_data_type::STRING /* s_name */,
	cell_data_type::STRING /* s_phone */,
	cell_data_type::STRING /* s_address.a_street_1 */,
	cell_data_type::STRING /* s_address.a_street_2 */,
	cell_data_type::STRING /* s_address.a_city */,
	cell_data_type::STRING /* s_address.a_state */,
	cell_data_type::STRING /* s_address.a_zip */,
	cell_data_type::DOUBLE /* s_acctbal */,
	cell_data_type::STRING /* s_comment */,
};

static constexpr uint32_t k_tpcc_warehouse_num_columns = 9;
static const std::vector<cell_data_type> k_tpcc_warehouse_col_types = {
	cell_data_type::INT64 /* w_id */,
	cell_data_type::DOUBLE /* w_tax */,
	cell_data_type::DOUBLE /* w_ytd */,
	cell_data_type::STRING /* w_name */,
	cell_data_type::STRING /* w_address.a_street_1 */,
	cell_data_type::STRING /* w_address.a_street_2 */,
	cell_data_type::STRING /* w_address.a_city */,
	cell_data_type::STRING /* w_address.a_state */,
	cell_data_type::STRING /* w_address.a_zip */,
};
