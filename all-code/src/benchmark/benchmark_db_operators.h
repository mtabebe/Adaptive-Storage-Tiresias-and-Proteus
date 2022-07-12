#pragma once

#include "../common/hw.h"
#include "../data-site/db/db.h"
#include "../data-site/single-site-db/db_abstraction.h"

class benchmark_db_operators {
   public:
    benchmark_db_operators( uint32_t client_id, db_abstraction* db,
                            const db_abstraction_configs& abstraction_configs,
                            bool limit_propagation, bool merge_snapshots );

    void begin_transaction( std::vector<cell_key_ranges>& write_ckrs,
                            std::vector<cell_key_ranges>& read_ckrs,
                            const std::string&            name = "" );

    void begin_scan_transaction( std::vector<cell_key_ranges>& read_ckrs,
                                 const std::string&            name = "" );

    void commit_transaction();
    void abort_transaction();

    uint32_t get_site_location();

    void split_partition( const cell_identifier& cid, bool split_vertically );

    void merge_partition( const cell_identifier& cid, bool merge_vertically );
    void remaster_partition( const cell_identifier& cid, uint32_t new_master );

    void add_partitions( const std::vector<partition_column_identifier>& pids );

    void change_partition_types(
        const std::vector<partition_column_identifier>& pids,
        const std::vector<partition_type::type>&        part_types,
        const std::vector<storage_tier_type::type>&     storage_types );

    void set_transaction_partition_holder(
        transaction_partition_holder* txn_holder );

    void remove_data( const cell_identifier& cid );

    void write_uint64( const cell_identifier& ck, uint64_t data );
    void write_int64( const cell_identifier& ck, int64_t data );
    void write_double( const cell_identifier& ck, double data );
    void write_string( const cell_identifier& ck, const std::string& data );

    void insert_uint64( const cell_identifier& ck, uint64_t data );
    void insert_int64( const cell_identifier& ck, int64_t data );
    void insert_double( const cell_identifier& ck, double data );
    void insert_string( const cell_identifier& ck, const std::string& data );

    void write_uint64_no_propagate( const cell_identifier& ck, uint64_t data );
    void write_int64_no_propagate( const cell_identifier& ck, int64_t data );
    void write_double_no_propagate( const cell_identifier& ck, double data );
    void write_string_no_propagate( const cell_identifier& ck,
                                    const std::string&     data );

    void insert_uint64_no_propagate( const cell_identifier& ck, uint64_t data );
    void insert_int64_no_propagate( const cell_identifier& ck, int64_t data );
    void insert_double_no_propagate( const cell_identifier& ck, double data );
    void insert_string_no_propagate( const cell_identifier& ck,
                                     const std::string&     data );

    uint64_t lookup_uint64( const cell_identifier& ck );
    int64_t lookup_int64( const cell_identifier& ck );
    double lookup_double( const cell_identifier& ck );
    std::string lookup_string( const cell_identifier& ck );

    std::tuple<bool, uint64_t> lookup_nullable_uint64(
        const cell_identifier& ck );
    std::tuple<bool, int64_t> lookup_nullable_int64(
        const cell_identifier& ck );
    std::tuple<bool, double> lookup_nullable_double(
        const cell_identifier& ck );
    std::tuple<bool, std::string> lookup_nullable_string(
        const cell_identifier& ck );

    uint64_t lookup_latest_uint64( const cell_identifier& ck );
    int64_t lookup_latest_int64( const cell_identifier& ck );
    double lookup_latest_double( const cell_identifier& ck );
    std::string lookup_latest_string( const cell_identifier& ck );

    std::tuple<bool, uint64_t> lookup_latest_nullable_uint64(
        const cell_identifier& ck );
    std::tuple<bool, int64_t> lookup_latest_nullable_int64(
        const cell_identifier& ck );
    std::tuple<bool, double> lookup_latest_nullable_double(
        const cell_identifier& ck );
    std::tuple<bool, std::string> lookup_latest_nullable_string(
        const cell_identifier& ck );

    std::vector<result_tuple> scan( uint32_t table_id, uint64_t low_key,
                                    uint64_t                     high_key,
                                    const std::vector<uint32_t>& project_cols,
                                    const predicate_chain&       predicate );
    void scan( uint32_t table_id, uint64_t low_key, uint64_t high_key,
               const std::vector<uint32_t>& project_cols,
               const predicate_chain&       predicate,
               std::vector<result_tuple>&   results );

    // class KHash = std::hash<Key>
    // class KEqual = std::equal_to<Key>
    template <class Key, class Val, class KHash, class KEqual, class Probe,
              void( MapF )( const std::vector<result_tuple>& res_tuples,
                            std::unordered_map<Key, Val, KHash, KEqual>& res,
                            const Probe& p ),
              void( ReduceF )( const std::unordered_map<Key, std::vector<Val>,
                                                        KHash, KEqual>& input,
                               std::unordered_map<Key, Val, KHash, KEqual>& res,
                               const Probe p )>
    std::unordered_map<Key, Val, KHash, KEqual> scan_mr(
        uint32_t table_id, uint64_t low_key, uint64_t high_key,
        const std::vector<uint32_t>& project_cols,
        const predicate_chain& predicate, const Probe& p );

    template <class Key, class Val, class KHash, class KEqual, class Probe,
              std::tuple<bool, Key, Val>( MapF )( const result_tuple& res_tuple,
                                                  const Probe& p ),
              Val( ReduceF )( const std::vector<Val>& input, const Probe& p )>
    std::unordered_map<Key, Val, KHash, KEqual> scan_simple_mr(
        uint32_t table_id, uint64_t low_key, uint64_t high_key,
        const std::vector<uint32_t>& project_cols,
        const predicate_chain& predicate, const Probe& p );

    template <class Key, class Val, class KHash, class KEqual, class Probe,
              void( MapF )( const std::vector<result_tuple>& res_tuples,
                            std::unordered_map<Key, Val, KHash, KEqual>& res,
                            const Probe& p ),
              void( ReduceF )( const std::unordered_map<Key, std::vector<Val>,
                                                        KHash, KEqual>& input,
                               std::unordered_map<Key, Val, KHash, KEqual>& res,
                               const Probe& p )>
    std::unordered_map<Key, Val, KHash, KEqual> scan_mr(
        uint32_t table_id, const std::vector<cell_key_ranges>& ckrs,
        const std::vector<uint32_t>& project_cols,
        const predicate_chain& predicate, const Probe& p );

    template <class Key, class Val, class KHash, class KEqual, class Probe,
              std::tuple<Key, Val>( MapF )( const result_tuple& res_tuple,
                                            const Probe& p ),
              Val( ReduceF )( const std::vector<Val>& input, const Probe& p )>
    std::unordered_map<Key, Val, KHash, KEqual> scan_simple_mr(
        uint32_t table_id, const std::vector<cell_key_ranges>& ckrs,
        const std::vector<uint32_t>& project_cols,
        const predicate_chain& predicate, const Probe& p );

    uint32_t               client_id_;
    db_abstraction_configs abstraction_configs_;
    db_abstraction*        db_;

    transaction_partition_holder* txn_holder_;
    snapshot_vector               state_;
    snapshot_vector               global_state_;

    bool limit_propagation_;
    bool merge_snapshots_;
};

#include "benchmark_db_operators.tcc"
