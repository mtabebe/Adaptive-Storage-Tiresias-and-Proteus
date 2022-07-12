#pragma once

#include "../../common/bucket_funcs.h"
#include "../../common/hw.h"
#include "tpcc_configs.h"

ALWAYS_INLINE uint64_t get_warehouse_size( const tpcc_configs& confs );
ALWAYS_INLINE uint64_t get_item_size( const tpcc_configs& confs );
ALWAYS_INLINE uint64_t get_stock_size( const tpcc_configs& confs );

ALWAYS_INLINE uint64_t get_district_size( const tpcc_configs& confs );
ALWAYS_INLINE uint64_t get_customer_size( const tpcc_configs& confs );
ALWAYS_INLINE uint64_t get_customer_district_size( const tpcc_configs& confs );
ALWAYS_INLINE uint64_t get_history_size( const tpcc_configs& confs );

ALWAYS_INLINE uint64_t get_order_line_size( const tpcc_configs& confs );
ALWAYS_INLINE uint64_t get_new_order_size( const tpcc_configs& confs );
ALWAYS_INLINE uint64_t get_order_size( const tpcc_configs& confs );

ALWAYS_INLINE uint64_t get_region_size( const tpcc_configs& confs );
ALWAYS_INLINE uint64_t get_nation_size( const tpcc_configs& confs );
ALWAYS_INLINE uint64_t get_supplier_size( const tpcc_configs& confs );

ALWAYS_INLINE std::vector<table_partition_information> get_tpcc_data_sizes(
    const tpcc_configs& configs );

#include "tpcc_table_sizes-inl.h"
