#pragma once

#include <iostream>

#include "../../common/constants.h"
#include "../../common/hw.h"
#include "../../data-site/db/partition_metadata.h"
#include "../benchmark_configs.h"
#include "record-types/tpcc_record_types.h"
#include "tpcc_table_ids.h"

class tpcc_layout_configs {
   public:
    uint32_t                warehouse_column_partition_size_;
    partition_type::type    warehouse_part_type_;
    storage_tier_type::type warehouse_storage_type_;

    uint32_t                item_column_partition_size_;
    partition_type::type    item_part_type_;
    storage_tier_type::type item_storage_type_;

    uint32_t                stock_column_partition_size_;
    partition_type::type    stock_part_type_;
    storage_tier_type::type stock_storage_type_;

    uint32_t                district_column_partition_size_;
    partition_type::type    district_part_type_;
    storage_tier_type::type district_storage_type_;

    uint32_t                customer_column_partition_size_;
    partition_type::type    customer_part_type_;
    storage_tier_type::type customer_storage_type_;

    uint32_t                history_column_partition_size_;
    partition_type::type    history_part_type_;
    storage_tier_type::type history_storage_type_;

    uint32_t                order_line_column_partition_size_;
    partition_type::type    order_line_part_type_;
    storage_tier_type::type order_line_storage_type_;

    uint32_t                new_order_column_partition_size_;
    partition_type::type    new_order_part_type_;
    storage_tier_type::type new_order_storage_type_;

    uint32_t                order_column_partition_size_;
    partition_type::type    order_part_type_;
    storage_tier_type::type order_storage_type_;

    uint32_t                customer_district_column_partition_size_;
    partition_type::type    customer_district_part_type_;
    storage_tier_type::type customer_district_storage_type_;

    uint32_t                region_column_partition_size_;
    partition_type::type    region_part_type_;
    storage_tier_type::type region_storage_type_;

    uint32_t                nation_column_partition_size_;
    partition_type::type    nation_part_type_;
    storage_tier_type::type nation_storage_type_;

    uint32_t                supplier_column_partition_size_;
    partition_type::type    supplier_part_type_;
    storage_tier_type::type supplier_storage_type_;
};

class tpcc_configs {
   public:
    uint32_t num_warehouses_;  // scale factor
    uint64_t num_items_;       // scale factor
    uint64_t num_suppliers_;

    uint32_t num_districts_per_warehouse_;
    uint32_t num_customers_per_district_;
    uint32_t max_num_order_lines_per_order_;
    uint32_t initial_num_customers_per_district_;

    bool     limit_number_of_records_propagated_;
    uint64_t number_of_updates_needed_for_propagation_;

    // fixed sizes
    uint32_t expected_num_orders_per_cust_;  // rough metric for tput

    uint32_t item_partition_size_;
    uint32_t partition_size_;
    uint32_t district_partition_size_;
    uint32_t customer_partition_size_;
    uint32_t supplier_partition_size_;

    uint32_t new_order_within_warehouse_likelihood_;
    uint32_t payment_within_warehouse_likelihood_;

    bool track_and_use_recent_items_;

    bool use_warehouse_;
    bool use_district_;

    bool h_scans_full_tables_;

    tpcc_layout_configs layouts_;

    uint32_t c_num_clients_;
    uint32_t h_num_clients_;

    benchmark_configs bench_configs_;

    std::vector<uint32_t> c_workload_probs_;
    std::vector<uint32_t> h_workload_probs_;
};
tpcc_layout_configs construct_tpcc_layout_configs(
    uint32_t warehouse_column_partition_size =
        k_tpcc_warehouse_column_partition_size,
    const partition_type::type& warehouse_part_type =
        k_tpcc_warehouse_part_type,
    const storage_tier_type::type& warehouse_storage_type =
        k_tpcc_warehouse_storage_type,
    uint32_t item_column_partition_size = k_tpcc_item_column_partition_size,
    const partition_type::type&    item_part_type = k_tpcc_item_part_type,
    const storage_tier_type::type& item_storage_type = k_tpcc_item_storage_type,
    uint32_t stock_column_partition_size = k_tpcc_stock_column_partition_size,
    const partition_type::type&    stock_part_type = k_tpcc_stock_part_type,
    const storage_tier_type::type& stock_storage_type =
        k_tpcc_stock_storage_type,
    uint32_t district_column_partition_size =
        k_tpcc_district_column_partition_size,
    const partition_type::type& district_part_type = k_tpcc_district_part_type,
    const storage_tier_type::type& district_storage_type =
        k_tpcc_district_storage_type,
    uint32_t customer_column_partition_size =
        k_tpcc_customer_column_partition_size,
    const partition_type::type& customer_part_type = k_tpcc_customer_part_type,
    const storage_tier_type::type& customer_storage_type =
        k_tpcc_customer_storage_type,
    uint32_t history_column_partition_size =
        k_tpcc_history_column_partition_size,
    const partition_type::type&    history_part_type = k_tpcc_history_part_type,
    const storage_tier_type::type& history_storage_type =
        k_tpcc_history_storage_type,
    uint32_t order_line_column_partition_size =
        k_tpcc_order_line_column_partition_size,
    const partition_type::type& order_line_part_type =
        k_tpcc_order_line_part_type,
    const storage_tier_type::type& order_line_storage_type =
        k_tpcc_order_line_storage_type,
    uint32_t new_order_column_partition_size =
        k_tpcc_new_order_column_partition_size,
    const partition_type::type& new_order_part_type =
        k_tpcc_new_order_part_type,
    const storage_tier_type::type& new_order_storage_type =
        k_tpcc_new_order_storage_type,
    uint32_t order_column_partition_size = k_tpcc_order_column_partition_size,
    const partition_type::type&    order_part_type = k_tpcc_order_part_type,
    const storage_tier_type::type& order_storage_type =
        k_tpcc_order_storage_type,
    uint32_t customer_district_column_partition_size =
        k_tpcc_customer_district_column_partition_size,
    const partition_type::type& customer_district_part_type =
        k_tpcc_customer_district_part_type,
    const storage_tier_type::type& customer_district_storage_type =
        k_tpcc_customer_district_storage_type,
    uint32_t region_column_partition_size = k_tpcc_region_column_partition_size,
    const partition_type::type&    region_part_type = k_tpcc_region_part_type,
    const storage_tier_type::type& region_storage_type =
        k_tpcc_region_storage_type,
    uint32_t nation_column_partition_size = k_tpcc_nation_column_partition_size,
    const partition_type::type&    nation_part_type = k_tpcc_nation_part_type,
    const storage_tier_type::type& nation_storage_type =
        k_tpcc_nation_storage_type,
    uint32_t supplier_column_partition_size =
        k_tpcc_supplier_column_partition_size,
    const partition_type::type& supplier_part_type = k_tpcc_supplier_part_type,
    const storage_tier_type::type& supplier_storage_type =
        k_tpcc_supplier_storage_type );

tpcc_configs construct_tpcc_configs(
    uint32_t num_warehouses = k_tpcc_num_warehouses,
    uint64_t num_items = k_tpcc_num_items,
    uint64_t num_supppliers = k_tpcc_num_suppliers,
    uint32_t expected_num_orders_per_cust = k_tpcc_expected_num_orders_per_cust,
    uint32_t item_partition_size = k_tpcc_item_partition_size,
    uint32_t partition_size = k_tpcc_partition_size,
    uint32_t district_partition_size = k_tpcc_district_partition_size,
    uint32_t customer_partition_size = k_tpcc_customer_partition_size,
    uint32_t supplier_partition_size = k_tpcc_supplier_partition_size,
    uint32_t new_order_within_warehouse_likelihood =
        k_tpcc_new_order_within_warehouse_likelihood,
    uint32_t payment_within_warehouse_likelihood =
        k_tpcc_payment_within_warehouse_likelihood,
    bool     track_and_use_recent_items = k_tpcc_track_and_use_recent_items,
    uint32_t c_num_clients = k_tpcc_num_clients,
    uint32_t h_num_clients = k_tpch_num_clients,
    const benchmark_configs& bench_configs = construct_benchmark_configs(),
    uint32_t                 delivery_prob = k_tpcc_delivery_prob,
    uint32_t                 new_order_prob = k_tpcc_new_order_prob,
    uint32_t                 order_status_prob = k_tpcc_order_status_prob,
    uint32_t                 payment_prob = k_tpcc_payment_prob,
    uint32_t                 stock_level_prob = k_tpcc_stock_level_prob,
    uint32_t q1_prob = k_tpch_q1_prob, uint32_t q2_prob = k_tpch_q2_prob,
    uint32_t q3_prob = k_tpch_q3_prob, uint32_t q4_prob = k_tpch_q4_prob,
    uint32_t q5_prob = k_tpch_q5_prob, uint32_t q6_prob = k_tpch_q6_prob,
    uint32_t q7_prob = k_tpch_q7_prob, uint32_t q8_prob = k_tpch_q8_prob,
    uint32_t q9_prob = k_tpch_q9_prob, uint32_t q10_prob = k_tpch_q10_prob,
    uint32_t q11_prob = k_tpch_q11_prob, uint32_t q12_prob = k_tpch_q12_prob,
    uint32_t q13_prob = k_tpch_q13_prob, uint32_t q14_prob = k_tpch_q14_prob,
    uint32_t q15_prob = k_tpch_q15_prob, uint32_t q16_prob = k_tpch_q16_prob,
    uint32_t q17_prob = k_tpch_q17_prob, uint32_t q18_prob = k_tpch_q18_prob,
    uint32_t q19_prob = k_tpch_q19_prob, uint32_t q20_prob = k_tpch_q20_prob,
    uint32_t q21_prob = k_tpch_q21_prob, uint32_t q22_prob = k_tpch_q22_prob,
    uint32_t h_all_prob = k_tpch_all_prob,
    uint32_t num_districts_per_warehouse =
        k_tpcc_spec_num_districts_per_warehouse,
    uint32_t num_customers_per_district =
        k_tpcc_spec_num_customers_per_district,
    uint32_t max_num_order_lines_per_order =
        k_tpcc_spec_max_num_order_lines_per_order,
    uint32_t initial_num_customers_per_district =
        k_tpcc_spec_initial_num_customers_per_district,
    bool limit_number_of_records_propagated =
        k_limit_number_of_records_propagated,
    uint64_t number_of_updates_needed_for_propagation =
        k_number_of_updates_needed_for_propagation,
    bool                       use_warehouse = k_tpcc_use_warehouse,
    bool                       use_district = k_tpcc_use_district,
    bool                       h_scans_full_tables = k_tpcc_h_scan_full_tables,
    const tpcc_layout_configs& layouts = construct_tpcc_layout_configs() );

std::ostream& operator<<( std::ostream&              os,
                          const tpcc_layout_configs& configs );
std::ostream& operator<<( std::ostream& os, const tpcc_configs& configs );

std::vector<table_metadata> create_tpcc_table_metadata(
    const tpcc_configs& configs = construct_tpcc_configs(),
    int32_t             site = k_site_location_identifier );
