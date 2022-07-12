#pragma once

#include <glog/logging.h>
#ifndef GLOG_STL_LOGGING_FOR_UNORDERED
#define GLOG_STL_LOGGING_FOR_UNORDERED
#endif
#include <glog/stl_logging.h>

template <
    class Key, class Val, class KHash, class KEqual, class Probe,
    void( MapF )( const std::vector<result_tuple>& res_tuples,
                  std::unordered_map<Key, Val, KHash, KEqual>& res,
                  const Probe& p ),
    void( ReduceF )(
        const std::unordered_map<Key, std::vector<Val>, KHash, KEqual>& input,
        std::unordered_map<Key, Val, KHash, KEqual>& res, const Probe& p )>
std::unordered_map<Key, Val, KHash, KEqual>
    transaction_partition_holder::scan_mr(
        uint32_t table_id, const std::vector<cell_key_ranges>& ckrs,
        const std::vector<uint32_t>& project_cols,
        const predicate_chain& predicate, const Probe& probe ) {

    auto        scan_bounds = get_scan_bounds( table_id );
    const auto& lower_bound = std::get<0>( scan_bounds );
    const auto& upper_bound = std::get<1>( scan_bounds );

    std::vector<partition_column_identifier> pcids;
    for( const auto& ckr : ckrs ) {
        pcids.emplace_back( get_scan_pcid( table_id, ckr.row_id_start,
                                           ckr.row_id_end, project_cols ) );
    }

    std::unordered_map<Key, std::vector<Val>, KHash, KEqual> mapped;

    for( auto iter = lower_bound; iter != upper_bound; iter++ ) {
        auto pid = *iter;
        auto part = get_partition( pid );
        DCHECK( part );
        DVLOG( 40 ) << "Scan PID:" << pid;
        std::vector<result_tuple> results;
        for( const auto& pred_pcid : pcids ) {
            if( do_pcids_intersect( pid, pred_pcid ) ) {
                auto part = get_partition( pid );
                DVLOG( 40 ) << "Scan PID:" << pid << ", within range";
                part->scan( pred_pcid.partition_start, pred_pcid.partition_end,
                            predicate, project_cols, snapshot_, results );
            }
        }
        if( results.size() > 0 ) {
            std::unordered_map<Key, Val, KHash, KEqual> loc_res;
            MapF( results, loc_res, probe );
            for( const auto& loc_entry : loc_res ) {
                mapped[loc_entry.first].emplace_back( loc_entry.second );
            }
        }
    }

    std::unordered_map<Key, Val, KHash, KEqual> res;
    ReduceF( mapped, res, probe );

    return res;
}

template <class Key, class Val, class KHash, class KEqual, class Probe,
          std::tuple<bool, Key, Val>( MapF )( const result_tuple& res_tuple,
                                              const Probe& p ),
          Val( ReduceF )( const std::vector<Val>& input, const Probe& p )>
std::unordered_map<Key, Val, KHash, KEqual>
    transaction_partition_holder::scan_simple_mr(
        uint32_t table_id, const std::vector<cell_key_ranges>& ckrs,
        const std::vector<uint32_t>& project_cols,
        const predicate_chain& predicate, const Probe& probe ) {

    auto        scan_bounds = get_scan_bounds( table_id );
    const auto& lower_bound = std::get<0>( scan_bounds );
    const auto& upper_bound = std::get<1>( scan_bounds );

    std::vector<partition_column_identifier> pcids;
    for( const auto& ckr : ckrs ) {
        pcids.emplace_back( get_scan_pcid( table_id, ckr.row_id_start,
                                           ckr.row_id_end, project_cols ) );
    }

    std::unordered_map<Key, std::vector<Val>, KHash, KEqual> mapped;

    for( auto iter = lower_bound; iter != upper_bound; iter++ ) {
        auto pid = *iter;
        auto part = get_partition( pid );
        DCHECK( part );
        DVLOG( 40 ) << "Scan PID:" << pid;
        for( const auto& pred_pcid : pcids ) {
            if( do_pcids_intersect( pid, pred_pcid ) ) {
                auto part = get_partition( pid );
                DVLOG( 40 ) << "Scan PID:" << pid << ", within range";
                std::vector<result_tuple> results;
                part->scan( pred_pcid.partition_start, pred_pcid.partition_end,
                            predicate, project_cols, snapshot_, results );

                for( const auto& res : results ) {
                    auto map_result = MapF( res, probe );
                    if( std::get<0>( map_result ) ) {
                        mapped[std::get<1>( map_result )].emplace_back(
                            std::get<2>( map_result ) );
                    }
                }
            }
        }
    }

    std::unordered_map<Key, Val, KHash, KEqual> res;
    for( const auto& entry : mapped ) {
        res[entry.first] = ReduceF( entry.second, probe );
    }

    return res;
}

