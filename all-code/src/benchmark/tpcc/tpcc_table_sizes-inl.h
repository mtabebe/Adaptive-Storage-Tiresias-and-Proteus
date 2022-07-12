#pragma once

#include "record-types/tpcc_record_types.h"

inline uint64_t get_warehouse_size( const tpcc_configs& confs ) {
    return confs.num_warehouses_;
}
inline uint64_t get_item_size( const tpcc_configs& confs ) {
    return confs.num_items_;
}
inline uint64_t get_stock_size( const tpcc_configs& confs ) {
    return get_warehouse_size( confs ) * get_item_size( confs );
}

inline uint64_t get_district_size( const tpcc_configs& confs ) {
    return get_warehouse_size( confs ) * confs.num_districts_per_warehouse_;
}
inline uint64_t get_customer_size( const tpcc_configs& confs ) {
    return get_district_size( confs ) * confs.num_customers_per_district_;
}
inline uint64_t get_customer_district_size( const tpcc_configs& confs ) {
    return get_customer_size( confs );
}

inline uint64_t get_history_size( const tpcc_configs& confs ) {
    return get_customer_size( confs );
}

inline uint64_t get_order_line_size( const tpcc_configs& confs ) {
    return get_history_size(
        confs );  // * confs.max_num_order_lines_per_order_;
}

inline uint64_t get_new_order_size( const tpcc_configs& confs ) {
    return get_history_size( confs );
}

inline uint64_t get_order_size( const tpcc_configs& confs ) {
    return get_history_size( confs );
}

inline uint64_t get_region_size( const tpcc_configs& confs ) {
    return k_tpch_region_names.size();
}

inline uint64_t get_nation_size( const tpcc_configs& confs ) {
    return k_tpch_nation_name.size();
}

inline uint64_t get_supplier_size( const tpcc_configs& confs ) {
    return confs.num_suppliers_;
}

inline std::vector<table_partition_information> get_tpcc_data_sizes(
    const tpcc_configs& configs ) {
    std::vector<table_partition_information> sizes;
    uint64_t warehouse_size = get_warehouse_size( configs ) + 1;

    sizes.emplace_back(
        k_tpcc_warehouse_table_id, warehouse_size, 1 /*per warehouse lock*/,
        configs.layouts_.warehouse_column_partition_size_,
        k_tpcc_warehouse_num_columns, k_tpcc_warehouse_col_types,
        configs.layouts_.warehouse_part_type_,
        configs.layouts_.warehouse_storage_type_ );
    uint64_t item_size = get_item_size( configs ) + 1;
    uint64_t num_item_partitions = configs.item_partition_size_;
    /*
     * we never write this outside of loading so save space, also it's a read
     * only table, so locks for it are more or less pointless
     */
    sizes.emplace_back( k_tpcc_item_table_id, item_size, num_item_partitions,
                        configs.layouts_.item_column_partition_size_,
                        k_tpcc_item_num_columns, k_tpcc_item_col_types,
                        configs.layouts_.item_part_type_,
                        configs.layouts_.item_storage_type_ );

    uint64_t stock_size = get_stock_size( configs ) + 1;
    sizes.emplace_back(
        k_tpcc_stock_table_id, stock_size, configs.partition_size_,
        configs.layouts_.stock_column_partition_size_, k_tpcc_stock_num_columns,
        k_tpcc_stock_col_types, configs.layouts_.stock_part_type_,
        configs.layouts_.stock_storage_type_ );
    uint64_t district_size = get_district_size( configs ) + 1;
    sizes.emplace_back( k_tpcc_district_table_id, district_size,
                        configs.district_partition_size_
                        /* there should be one lock per warehouse, or we could
         * district_size go one per district*/,
                        configs.layouts_.district_column_partition_size_,
                        k_tpcc_district_num_columns, k_tpcc_district_col_types,
                        configs.layouts_.district_part_type_,
                        configs.layouts_.district_storage_type_ );
    uint64_t customer_size = get_customer_size( configs ) + 1;
    sizes.emplace_back( k_tpcc_customer_table_id, customer_size,
                        configs.customer_partition_size_
                        /* there should be one lock per district*/,
                        configs.layouts_.customer_column_partition_size_,
                        k_tpcc_customer_num_columns, k_tpcc_customer_col_types,
                        configs.layouts_.customer_part_type_,
                        configs.layouts_.customer_storage_type_ );
    uint64_t history_size = get_history_size( configs ) + 1;
    sizes.emplace_back( k_tpcc_history_table_id, history_size,
                        configs.customer_partition_size_,
                        configs.layouts_.history_column_partition_size_,
                        k_tpcc_history_num_columns, k_tpcc_history_col_types,
                        configs.layouts_.history_part_type_,
                        configs.layouts_.history_storage_type_ );
    uint64_t order_line_size = get_order_line_size( configs ) + 1;
    sizes.emplace_back(
        k_tpcc_order_line_table_id, order_line_size, configs.partition_size_,
        configs.layouts_.order_line_column_partition_size_,
        k_tpcc_order_line_num_columns, k_tpcc_order_line_col_types,
        configs.layouts_.order_line_part_type_,
        configs.layouts_.order_line_storage_type_ );
    uint64_t new_order_size = get_new_order_size( configs ) + 1;
    sizes.emplace_back(
        k_tpcc_new_order_table_id, new_order_size, configs.partition_size_,
        configs.layouts_.new_order_column_partition_size_,
        k_tpcc_new_order_num_columns, k_tpcc_new_order_col_types,
        configs.layouts_.new_order_part_type_,
        configs.layouts_.new_order_storage_type_ );
    uint64_t order_size = get_order_size( configs ) + 1;
    sizes.emplace_back(
        k_tpcc_order_table_id, order_size, configs.partition_size_,
        configs.layouts_.order_column_partition_size_, k_tpcc_order_num_columns,
        k_tpcc_order_col_types, configs.layouts_.order_part_type_,
        configs.layouts_.order_storage_type_ );
    uint64_t customer_district_size = get_customer_district_size( configs ) + 1;
    sizes.emplace_back(
        k_tpcc_customer_district_table_id, customer_district_size,
        configs.customer_partition_size_
        /* there should be one lock per district*/,
        configs.layouts_.customer_district_column_partition_size_,
        k_tpcc_customer_district_num_columns,
        k_tpcc_customer_district_col_types,
        configs.layouts_.customer_district_part_type_,
        configs.layouts_.customer_district_storage_type_ );

    uint64_t region_size = get_region_size( configs ) + 1;
    sizes.emplace_back(
        k_tpcc_region_table_id, region_size,
        region_size /* there should be one region partition */,
        configs.layouts_.region_column_partition_size_,
        k_tpcc_region_num_columns,
        k_tpcc_region_col_types,
        configs.layouts_.region_part_type_,
        configs.layouts_.region_storage_type_ );

    uint64_t nation_size = get_nation_size( configs ) + 1;
    sizes.emplace_back(
        k_tpcc_nation_table_id, nation_size,
        nation_size /* there should be one nation partition */,
        configs.layouts_.nation_column_partition_size_,
        k_tpcc_nation_num_columns,
        k_tpcc_nation_col_types,
        configs.layouts_.nation_part_type_,
        configs.layouts_.nation_storage_type_ );

    uint64_t supplier_size = get_supplier_size( configs ) + 1;
    uint32_t supplier_part_size = configs.supplier_partition_size_;
    sizes.emplace_back( k_tpcc_supplier_table_id, supplier_size,
                        supplier_part_size,
                        configs.layouts_.supplier_column_partition_size_,
                        k_tpcc_supplier_num_columns, k_tpcc_supplier_col_types,
                        configs.layouts_.supplier_part_type_,
                        configs.layouts_.supplier_storage_type_ );

    return sizes;
}
