#include "scan_results.h"

#include <glog/logging.h>

void insert_cell_into_result_tuple( result_tuple&      res,
                                    const result_cell& cell ) {

    for( uint32_t pos = 0; pos < res.cells.size(); pos++ ) {
        if( cell.col_id <= res.cells.at( pos ).col_id ) {
            if( cell.col_id < res.cells.at( pos ).col_id ) {
                res.cells.insert( res.cells.begin() + pos, cell );
            }
            return;
        }
    }
    res.cells.emplace_back( cell );
}

void add_to_scan_result(
    std::vector<result_tuple>& res_tuples, const result_tuple& res,
    std::unordered_map<
        int32_t /* table */,
        std::unordered_map<int64_t /* row */, uint32_t /* pos */>>& mapping ) {
    DVLOG( 40 ) << "Trying to add result tuple:" << res;
    auto table_found = mapping.find( res.table_id );
    if( table_found == mapping.end() ) {

        DVLOG( 40 ) << "Table not found in mapping, adding table";
        std::unordered_map<int64_t /* row */, uint32_t /* pos */>
             new_row_mapping;
        auto table_insert = mapping.emplace( res.table_id, new_row_mapping );
        table_found = table_insert.first;
    }
    DCHECK( table_found != mapping.end() );
    DCHECK_EQ( table_found->first, res.table_id );


    auto& row_mapping = table_found->second;

    auto row_found = row_mapping.find( res.row_id );
    if( row_found == row_mapping.end() ) {
        DVLOG( 40 ) << "Row not found in mapping, adding row";
        result_tuple new_res;
        new_res.table_id = res.table_id;
        new_res.row_id = res.row_id;

        auto row_insert = row_mapping.emplace( res.row_id, res_tuples.size() );
        row_found = row_insert.first;

        res_tuples.emplace_back( new_res );
    }

    DCHECK( row_found != row_mapping.end() );
    DCHECK_EQ( row_found->first, res.row_id );
    DCHECK_LT( row_found->second, res_tuples.size() );


    auto& res_tuple = res_tuples.at( row_found->second );
    DVLOG( 40 ) << "Adding result tuple cells to result tuple:" << res_tuple
                << ", found in position:" << row_found->second;
    DCHECK_EQ( res_tuple.table_id, res.table_id );
    DCHECK_EQ( res_tuple.row_id, res.row_id );
    for ( const auto& cell : res.cells) {
        insert_cell_into_result_tuple( res_tuple, cell );
    }
}

void join_scan_results(
    one_shot_scan_result&                    _return,
    const std::vector<one_shot_scan_result>& per_site_results,
    const std::vector<scan_arguments>&       scan_args ) {
    std::unordered_map<
        int32_t /* label */,
        std::unordered_map<
            int32_t /* table */,
            std::unordered_map<int64_t /* row */, uint32_t /* pos */>>>
        mapping;

    // built initial state
    for( const auto& scan_arg : scan_args ) {
        std::unordered_map<
            int32_t /* table */,
            std::unordered_map<int64_t /* row */, uint32_t /* pos */>>
            label_mapping;
        mapping[scan_arg.label] = label_mapping;
        std::vector<result_tuple> res_tuples;
        _return.res_tuples[scan_arg.label] = res_tuples;
    }

    _return.status = exec_status_type::COMMAND_OK;
    _return.return_code = 0;

    // add results
    for( const auto& site_res : per_site_results ) {
        if( site_res.status != exec_status_type::COMMAND_OK ) {
            _return.status = site_res.status;
            return;
        }

        _return.session_version_vector.insert(
            site_res.session_version_vector.begin(),
            site_res.session_version_vector.end() );

        for( const auto& res_entry : site_res.res_tuples ) {
            int32_t label = res_entry.first;
            DCHECK_EQ( _return.res_tuples.count( label ), 1 );
            DCHECK_EQ( mapping.count( label ), 1 );
            for( const auto& res_tuple : res_entry.second ) {
                add_to_scan_result( _return.res_tuples.at( label ), res_tuple,
                                    mapping.at( label ) );
            }  // res_tuples
        }      // res_tuple map
    }          // site
}

std::unordered_map<int, scan_arguments> map_scan_args(
    const std::vector<scan_arguments>& scan_args ) {
    std::unordered_map<int, scan_arguments> ret;
    for( const auto& scan_arg : scan_args ) {
        ret[scan_arg.label] = scan_arg;
    }
    return ret;
}

void get_scan_read_ckrs(
    const std::unordered_map<int, scan_arguments>& scan_args,
    std::vector<cell_key_ranges>& ckrs ) {
    for( const auto& scan_arg_entry : scan_args ) {
        const auto& scan_ckrs = scan_arg_entry.second.read_ckrs;
        ckrs.insert( ckrs.end(), scan_ckrs.begin(), scan_ckrs.end() );
    }
}
void get_scan_read_ckrs(
    const std::vector<scan_arguments>& scan_args,
    std::vector<cell_key_ranges>& ckrs ) {
    for( const auto& scan_arg_entry : scan_args ) {
        const auto& scan_ckrs = scan_arg_entry.read_ckrs;
        ckrs.insert( ckrs.end(), scan_ckrs.begin(), scan_ckrs.end() );
    }
}
