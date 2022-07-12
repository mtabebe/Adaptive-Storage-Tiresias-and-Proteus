#pragma once

#include <cstdint>
#include <cstring>
#include <string>

#include "../../gen-cpp/gen-cpp/proto_types.h"
#include "packed_cell_data.h"
#include "packed_column_records.h"
#include "row_record.h"

bool evaluate_predicate_on_cell_data( const packed_cell_data& pcd,
                                      const cell_predicate&   pred );

bool evaluate_predicate_on_row_record( const row_record*      row_rec,
                                       const predicate_chain& predicate );

bool evaluate_predicate_on_column_stats( const column_stats<uint64_t>& stats,
                                         const predicate_chain& predicate );
bool evaluate_predicate_on_column_stats( const column_stats<int64_t>& stats,
                                         const predicate_chain& predicate );
bool evaluate_predicate_on_column_stats( const column_stats<double>& stats,
                                         const predicate_chain& predicate );
bool evaluate_predicate_on_column_stats( const column_stats<std::string>& stats,
                                         const predicate_chain& predicate );
bool evaluate_predicate_on_column_stats(
    const column_stats<multi_column_data>& stats,
    const predicate_chain&                 predicate );

bool evaluate_predicate_on_col_data( const uint64_t&        data,
                                     const predicate_chain& predicate );
bool evaluate_predicate_on_col_data( const int64_t&        data,
                                     const predicate_chain& predicate );
bool evaluate_predicate_on_col_data( const double&        data,
                                     const predicate_chain& predicate );
bool evaluate_predicate_on_col_data( const std::string&     data,
                                     const predicate_chain& predicate );
bool evaluate_predicate_on_col_data( const multi_column_data& data,
                                     const predicate_chain&   predicate );

bool evaluate_cell_predicate_on_multi_column_data(
    const multi_column_data& data, const cell_predicate& pred );

bool evaluate_predicate_on_uint64_data( uint64_t data, uint64_t pred_data,
                                        const predicate_type::type& p_type );
bool evaluate_predicate_on_int64_data( int64_t data, int64_t pred_data,
                                       const predicate_type::type& p_type );
bool evaluate_predicate_on_double_data( double data, double pred_data,
                                       const predicate_type::type& p_type );
bool evaluate_predicate_on_string_data( const std::string&          data,
                                        const std::string&          pred_data,
                                        const predicate_type::type& p_type );

predicate_chain translate_predicate_to_partition_id(
    const predicate_chain& predicate, const partition_column_identifier& pid,
    const std::vector<cell_data_type>& col_types );
std::vector<uint32_t> get_col_ids_to_scan_from_partition_id(
    uint64_t low_key, uint64_t high_key,
    const std::vector<uint32_t>&       project_cols,
    const partition_column_identifier& pid );
