#pragma once

#include <glog/logging.h>

template <
    class Key, class Val, class KHash, class KEqual, class Probe,
    void( MapF )( const std::vector<result_tuple>& res_tuples,
                  std::unordered_map<Key, Val, KHash, KEqual>& res,
                  const Probe& p ),
    void( ReduceF )(
        const std::unordered_map<Key, std::vector<Val>, KHash, KEqual>& input,
        std::unordered_map<Key, Val, KHash, KEqual>& res, const Probe p )>
std::unordered_map<Key, Val, KHash, KEqual> benchmark_db_operators::scan_mr(
    uint32_t table_id, uint64_t low_key, uint64_t high_key,
    const std::vector<uint32_t>& project_cols, const predicate_chain& predicate,
    const Probe& p ) {
    std::vector<cell_key_ranges> ckrs = {create_cell_key_ranges(
        table_id, low_key, high_key, project_cols.at( 0 ),
        project_cols.at( project_cols.size() - 1 ) )};
    return txn_holder_->scan_mr<Key, Val, KHash, KEqual, Probe, MapF, ReduceF>(
        table_id, ckrs, project_cols, predicate, p );
}

template <class Key, class Val, class KHash, class KEqual, class Probe,
          std::tuple<bool, Key, Val>( MapF )( const result_tuple& res_tuple,
                                              const Probe& p ),
          Val( ReduceF )( const std::vector<Val>& input, const Probe& p )>
std::unordered_map<Key, Val, KHash, KEqual>
    benchmark_db_operators::scan_simple_mr(
        uint32_t table_id, uint64_t low_key, uint64_t high_key,
        const std::vector<uint32_t>& project_cols,
        const predicate_chain& predicate, const Probe& probe ) {
    std::vector<cell_key_ranges> ckrs = {create_cell_key_ranges(
        table_id, low_key, high_key, project_cols.at( 0 ),
        project_cols.at( project_cols.size() - 1 ) )};
    return txn_holder_
        ->scan_simple_mr<Key, Val, KHash, KEqual, Probe, MapF, ReduceF>(
            table_id, ckrs, project_cols, predicate, probe );
}

template <
    class Key, class Val, class KHash, class KEqual, class Probe,
    void( MapF )( const std::vector<result_tuple>& res_tuples,
                  std::unordered_map<Key, Val, KHash, KEqual>& res,
                  const Probe& p ),
    void( ReduceF )(
        const std::unordered_map<Key, std::vector<Val>, KHash, KEqual>& input,
        std::unordered_map<Key, Val, KHash, KEqual>& res, const Probe& p )>
std::unordered_map<Key, Val, KHash, KEqual> benchmark_db_operators::scan_mr(
    uint32_t table_id, const std::vector<cell_key_ranges>& ckrs,
    const std::vector<uint32_t>& project_cols, const predicate_chain& predicate,
    const Probe& probe ) {
    return txn_holder_->scan_mr<Key, Val, KHash, KEqual, Probe, MapF, ReduceF>(
        table_id, ckrs, project_cols, predicate, probe );
}

template <class Key, class Val, class KHash, class KEqual, class Probe,
          std::tuple<Key, Val>( MapF )( const result_tuple& res_tuple,
                                        const Probe& p ),
          Val( ReduceF )( const std::vector<Val>& input, const Probe& p )>
std::unordered_map<Key, Val, KHash, KEqual>
    benchmark_db_operators::scan_simple_mr(
        uint32_t table_id, const std::vector<cell_key_ranges>& ckrs,
        const std::vector<uint32_t>& project_cols,
        const predicate_chain& predicate, const Probe& p ) {
    return txn_holder_
        ->scan_simple_mr<Key, Val, KHash, KEqual, Probe, MapF, ReduceF>(
            table_id, ckrs, project_cols, predicate, p );
}

