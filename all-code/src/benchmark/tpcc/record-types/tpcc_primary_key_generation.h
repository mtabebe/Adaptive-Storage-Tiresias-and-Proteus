#pragma once

#include "../../../common/bucket_funcs.h"
#include "../../../common/hw.h"
#include "../../../common/partition_funcs.h"
#include "../../../data-site/db/cell_identifier.h"
#include "../../primary_key_generation.h"
#include "../tpcc_configs.h"
#include "tpcc_record_types.h"

ALWAYS_INLINE uint64_t make_warehouse_key( const warehouse&    w,
                                           const tpcc_configs& configs );

ALWAYS_INLINE uint64_t make_item_key( const item&         i,
                                      const tpcc_configs& configs );

ALWAYS_INLINE uint64_t make_stock_key( const stock&        s,
                                       const tpcc_configs& configs );

ALWAYS_INLINE uint64_t make_district_key( const district&     d,
                                          const tpcc_configs& configs );

ALWAYS_INLINE uint64_t make_customer_key( const customer&     c,
                                          const tpcc_configs& configs );
ALWAYS_INLINE uint64_t make_customer_district_key(
    const customer_district& cd, const tpcc_configs& configs );

ALWAYS_INLINE uint64_t make_history_key( const history&      c,
                                         const tpcc_configs& configs );

ALWAYS_INLINE uint64_t make_order_key( const order&        o,
                                       const tpcc_configs& configs );

ALWAYS_INLINE uint64_t make_new_order_key( const new_order&    new_o,
                                           const tpcc_configs& configs );

ALWAYS_INLINE uint64_t make_order_line_key( const order_line&   o_line,
                                            const tpcc_configs& configs );

ALWAYS_INLINE uint64_t make_supplier_key( const supplier&     s,
                                          const tpcc_configs& configs );
ALWAYS_INLINE uint64_t make_nation_key( const nation&       n,
                                        const tpcc_configs& configs );
ALWAYS_INLINE uint64_t make_region_key( const region&       r,
                                        const tpcc_configs& configs );

// helpers
ALWAYS_INLINE uint64_t make_w_s_key( int32_t w_id, int32_t item_id,
                                     const tpcc_configs& configs );
ALWAYS_INLINE uint64_t make_w_d_key( int32_t d_id, int32_t w_id,
                                     const tpcc_configs& configs );
ALWAYS_INLINE uint64_t make_w_d_c_key( int32_t c_id, int32_t d_id, int32_t w_id,
                                       const tpcc_configs& configs );
ALWAYS_INLINE uint64_t make_w_d_c_o_key( int32_t o_id, int32_t c_id,
                                         int32_t d_id, int32_t w_id,
                                         const tpcc_configs& configs );
ALWAYS_INLINE uint64_t make_w_d_o_ol_key( int32_t ol_id, int32_t o_id,
                                          int32_t d_id, int32_t w_id,
                                          const tpcc_configs& configs );
ALWAYS_INLINE cell_key_ranges
    cell_key_ranges_from_cid( const cell_identifier& cid );

ALWAYS_INLINE uint32_t get_warehouse_from_key( int32_t table_id, uint64_t key,
                                               const tpcc_configs& configs );
ALWAYS_INLINE uint32_t get_district_from_key( int32_t table_id, uint64_t key,
                                              const tpcc_configs& configs );
ALWAYS_INLINE uint32_t get_item_from_key( int32_t table_id, uint64_t key,
                                          const tpcc_configs& configs );
ALWAYS_INLINE uint32_t get_customer_from_key( int32_t table_id, uint64_t key,
                                              const tpcc_configs& configs );

#include "tpcc_primary_key_generation-inl.h"
