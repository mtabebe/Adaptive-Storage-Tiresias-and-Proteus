#pragma once

#include <unordered_set>

#include "../../common/hw.h"
#include "../../data-site/db/db.h"
#include "../../data-site/single-site-db/db_abstraction.h"
#include "../../data-site/single-site-db/db_abstraction_types.h"
#include "../benchmark_db_operators.h"
#include "record-types/tpcc_primary_key_generation.h"
#include "record-types/tpcc_record_types.h"
#include "tpcc_table_ids.h"
#include "tpcc_table_sizes.h"
#include "tpcc_workload_generator.h"

void tpcc_create_tables( db_abstraction* db, const tpcc_configs& configs );
void tpcc_create_tables( db* database, const tpcc_configs& configs );

void insert_customer( benchmark_db_operators* db_ops, const customer* var,
                      uint64_t row_id, const cell_key_ranges& ckr );

void update_customer( benchmark_db_operators* db_ops, const customer* var,
                      uint64_t row_id, const cell_key_ranges& ckr,
                      bool do_propagate );

bool lookup_customer( benchmark_db_operators* db_ops, customer* var,
                      uint64_t row_id, const cell_key_ranges& ckr,
                      bool is_lates, bool is_nullable );
std::unordered_set<uint32_t> read_from_scan_customer( const result_tuple& res,
                                                      customer*           var );

void insert_customer_district( benchmark_db_operators*  db_ops,
                               const customer_district* var, uint64_t row_id,
                               const cell_key_ranges& ckr );

void update_customer_district( benchmark_db_operators*  db_ops,
                               const customer_district* var, uint64_t row_id,
                               const cell_key_ranges& ckr, bool do_propagate );

bool lookup_customer_district( benchmark_db_operators* db_ops,
                               customer_district* var, uint64_t row_id,
                               const cell_key_ranges& ckr, bool is_lates,
                               bool is_nullable );
std::unordered_set<uint32_t> read_from_scan_customer_district(
    const result_tuple& res, customer_district* var );

void insert_district( benchmark_db_operators* db_ops, const district* var,
                      uint64_t row_id, const cell_key_ranges& ckr );

void update_district( benchmark_db_operators* db_ops, const district* var,
                      uint64_t row_id, const cell_key_ranges& ckr,
                      bool do_propagate );

bool lookup_district( benchmark_db_operators* db_ops, district* var,
                      uint64_t row_id, const cell_key_ranges& ckr,
                      bool is_lates, bool is_nullable );
std::unordered_set<uint32_t> read_from_scan_district( const result_tuple& res,
                                                      district*           var );

void insert_history( benchmark_db_operators* db_ops, const history* var,
                     uint64_t row_id, const cell_key_ranges& ckr );

void update_history( benchmark_db_operators* db_ops, const history* var,
                     uint64_t row_id, const cell_key_ranges& ckr,
                     bool do_propagate );

bool lookup_history( benchmark_db_operators* db_ops, history* var,
                     uint64_t row_id, const cell_key_ranges& ckr, bool is_lates,
                     bool is_nullable );
std::unordered_set<uint32_t> read_from_scan_history( const result_tuple& res,
                                                     history*            var );

void insert_item( benchmark_db_operators* db_ops, const item* var,
                  uint64_t row_id, const cell_key_ranges& ckr );

void update_item( benchmark_db_operators* db_ops, const item* var,
                  uint64_t row_id, const cell_key_ranges& ckr,
                  bool do_propagate );

bool lookup_item( benchmark_db_operators* db_ops, item* var, uint64_t row_id,
                  const cell_key_ranges& ckr, bool is_lates, bool is_nullable );
std::unordered_set<uint32_t> read_from_scan_item( const result_tuple& res,
                                                  item*               var );

void insert_nation( benchmark_db_operators* db_ops, const nation* var,
                    uint64_t row_id, const cell_key_ranges& ckr );

void update_nation( benchmark_db_operators* db_ops, const nation* var,
                    uint64_t row_id, const cell_key_ranges& ckr,
                    bool do_propagate );

bool lookup_nation( benchmark_db_operators* db_ops, nation* var,
                    uint64_t row_id, const cell_key_ranges& ckr, bool is_lates,
                    bool is_nullable );
std::unordered_set<uint32_t> read_from_scan_nation( const result_tuple& res,
                                                    nation*             var );

void insert_new_order( benchmark_db_operators* db_ops, const new_order* var,
                       uint64_t row_id, const cell_key_ranges& ckr );

void update_new_order( benchmark_db_operators* db_ops, const new_order* var,
                       uint64_t row_id, const cell_key_ranges& ckr,
                       bool do_propagate );

bool lookup_new_order( benchmark_db_operators* db_ops, new_order* var,
                       uint64_t row_id, const cell_key_ranges& ckr,
                       bool is_lates, bool is_nullable );
std::unordered_set<uint32_t> read_from_scan_new_order( const result_tuple& res,
                                                       new_order* var );

void insert_order( benchmark_db_operators* db_ops, const order* var,
                   uint64_t row_id, const cell_key_ranges& ckr );

void update_order( benchmark_db_operators* db_ops, const order* var,
                   uint64_t row_id, const cell_key_ranges& ckr,
                   bool do_propagate );

bool lookup_order( benchmark_db_operators* db_ops, order* var, uint64_t row_id,
                   const cell_key_ranges& ckr, bool is_lates,
                   bool is_nullable );
std::unordered_set<uint32_t> read_from_scan_order( const result_tuple& res,
                                                   order*              var );

void insert_order_line( benchmark_db_operators* db_ops, const order_line* var,
                        uint64_t row_id, const cell_key_ranges& ckr );

void update_order_line( benchmark_db_operators* db_ops, const order_line* var,
                        uint64_t row_id, const cell_key_ranges& ckr,
                        bool do_propagate );

bool lookup_order_line( benchmark_db_operators* db_ops, order_line* var,
                        uint64_t row_id, const cell_key_ranges& ckr,
                        bool is_lates, bool is_nullable );
std::unordered_set<uint32_t> read_from_scan_order_line( const result_tuple& res,
                                                        order_line* var );

void insert_region( benchmark_db_operators* db_ops, const region* var,
                    uint64_t row_id, const cell_key_ranges& ckr );

void update_region( benchmark_db_operators* db_ops, const region* var,
                    uint64_t row_id, const cell_key_ranges& ckr,
                    bool do_propagate );

bool lookup_region( benchmark_db_operators* db_ops, region* var,
                    uint64_t row_id, const cell_key_ranges& ckr, bool is_lates,
                    bool is_nullable );
std::unordered_set<uint32_t> read_from_scan_region( const result_tuple& res,
                                                    region*             var );

void insert_stock( benchmark_db_operators* db_ops, const stock* var,
                   uint64_t row_id, const cell_key_ranges& ckr );

void update_stock( benchmark_db_operators* db_ops, const stock* var,
                   uint64_t row_id, const cell_key_ranges& ckr,
                   bool do_propagate );

bool lookup_stock( benchmark_db_operators* db_ops, stock* var, uint64_t row_id,
                   const cell_key_ranges& ckr, bool is_lates,
                   bool is_nullable );
std::unordered_set<uint32_t> read_from_scan_stock( const result_tuple& res,
                                                   stock*              var );

void insert_supplier( benchmark_db_operators* db_ops, const supplier* var,
                      uint64_t row_id, const cell_key_ranges& ckr );

void update_supplier( benchmark_db_operators* db_ops, const supplier* var,
                      uint64_t row_id, const cell_key_ranges& ckr,
                      bool do_propagate );

bool lookup_supplier( benchmark_db_operators* db_ops, supplier* var,
                      uint64_t row_id, const cell_key_ranges& ckr,
                      bool is_lates, bool is_nullable );
std::unordered_set<uint32_t> read_from_scan_supplier( const result_tuple& res,
                                                      supplier*           var );

void insert_warehouse( benchmark_db_operators* db_ops, const warehouse* var,
                       uint64_t row_id, const cell_key_ranges& ckr );

void update_warehouse( benchmark_db_operators* db_ops, const warehouse* var,
                       uint64_t row_id, const cell_key_ranges& ckr,
                       bool do_propagate );

bool lookup_warehouse( benchmark_db_operators* db_ops, warehouse* var,
                       uint64_t row_id, const cell_key_ranges& ckr,
                       bool is_lates, bool is_nullable );
std::unordered_set<uint32_t> read_from_scan_warehouse( const result_tuple& res,
                                                       warehouse* var );
