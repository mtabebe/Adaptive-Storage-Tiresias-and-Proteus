#pragma once

#include "../../common/hw.h"

#include "../update-propagation/update_destination_generator.h"
#include "cell_identifier.h"
#include "op_codes.h"
#include "partition.h"

enum partition_lookup_operation {
    GET_OR_CREATE,
    GET,
    GET_ALLOW_MISSING,
    GET_IF_ACTIVE,
    GET_WRITE_MAY_CREATE_READS,
    GET_WRITE_IF_ACTIVE_MAY_CREATE_READS,
};

class transaction_partition_holder {
   public:
    ALWAYS_INLINE transaction_partition_holder();
    ALWAYS_INLINE ~transaction_partition_holder();

    bool begin_transaction( const snapshot_vector& snapshot );
    snapshot_vector commit_transaction();
    snapshot_vector abort_transaction();

    snapshot_vector execute_split_merge(
        const snapshot_vector& snapshot, split_partition_result split,
        uint32_t col_split_point, uint64_t row_split_point, uint32_t op,
        const std::vector<storage_tier_type::type>& storage_tiers,
        uint64_t                                    poll_epoch );
    snapshot_vector remaster_partitions( uint32_t new_master );
    std::tuple<snapshot_vector, std::vector<std::shared_ptr<partition>>>
        remaster_partitions(
            const std::vector<partition_column_identifier>& pids,
            bool is_release, uint32_t new_master,
            const std::vector<propagation_configuration>& new_prop_configs );

    void change_partition_output_destination(
        std::shared_ptr<update_destination_interface> new_update_destination );

    void set_partition_update_destinations(
        const std::vector<partition_column_identifier>& pids,
        const std::vector<propagation_configuration>&   new_prop_configs,
        std::shared_ptr<update_destination_generator>   update_gen );

    ALWAYS_INLINE void add_do_not_apply_last(
        const partition_column_identifier_set& pids );
    ALWAYS_INLINE void add_do_not_apply_last(
        const std::vector<partition_column_identifier>& pids );

    ALWAYS_INLINE void add_write_partition(
        const partition_column_identifier& pid,
        std::shared_ptr<partition>         part );
    ALWAYS_INLINE void add_read_partition(
        const partition_column_identifier& pid,
        std::shared_ptr<partition>         part );

    bool remove_data( const cell_identifier& ci ) const;

    // point inserts
    bool insert_uint64_data( const cell_identifier& ci, uint64_t data ) const;
    bool insert_int64_data( const cell_identifier& ci, int64_t data ) const;
    bool insert_string_data( const cell_identifier& ci,
                             const std::string&     data ) const;
    bool insert_double_data( const cell_identifier& ci, double data ) const;

    // point updates
    bool update_uint64_data( const cell_identifier& ci, uint64_t data ) const;
    bool update_int64_data( const cell_identifier& ci, int64_t data ) const;
    bool update_string_data( const cell_identifier& ci,
                             const std::string&     data ) const;
    bool update_double_data( const cell_identifier& ci, double data ) const;

    // point lookups
    std::tuple<bool, uint64_t> get_uint64_data(
        const cell_identifier& ci ) const;
    std::tuple<bool, int64_t> get_int64_data( const cell_identifier& ci ) const;
    std::tuple<bool, double> get_double_data( const cell_identifier& ci ) const;
    std::tuple<bool, std::string> get_string_data(
        const cell_identifier& ci ) const;

    std::tuple<bool, uint64_t> get_latest_uint64_data(
        const cell_identifier& ci ) const;
    std::tuple<bool, int64_t> get_latest_int64_data(
        const cell_identifier& ci ) const;
    std::tuple<bool, double> get_latest_double_data(
        const cell_identifier& ci ) const;
    std::tuple<bool, std::string> get_latest_string_data(
        const cell_identifier& ci ) const;

    std::vector<result_tuple> scan( uint32_t table_id, uint64_t low_key,
                                    uint64_t                     high_key,
                                    const std::vector<uint32_t>& project_cols,
                                    const predicate_chain& predicate ) const;
    void scan( uint32_t table_id, uint64_t low_key, uint64_t high_key,
               const std::vector<uint32_t>& project_cols,
               const predicate_chain&       predicate,
               std::vector<result_tuple>&   scan ) const;

    // class KHash = std::hash<Key>
    // class KEqual = std::equal_to<Key>
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
              std::tuple<bool, Key, Val>( MapF )( const result_tuple& res_tuple,
                                                  const Probe& p ),
              Val( ReduceF )( const std::vector<Val>& input, const Probe& p )>
    std::unordered_map<Key, Val, KHash, KEqual> scan_simple_mr(
        uint32_t table_id, const std::vector<cell_key_ranges>& ckrs,
        const std::vector<uint32_t>& project_cols,
        const predicate_chain& predicate, const Probe& p );

    ALWAYS_INLINE std::shared_ptr<partition> get_partition(
        const partition_column_identifier& pid ) const;
    std::shared_ptr<partition> get_partition(
        const cell_identifier& cid ) const;

    snapshot_partition_column_state snapshot_partition(
        const partition_column_identifier& pid );
    snapshot_partition_column_state snapshot_partition_with_optional_commit(
        const partition_column_identifier& pid, bool do_commit );

    bool does_site_master_write_set( uint32_t site );

    std::vector<uint32_t> get_tables() const;
    void add_table_poll_epoch( uint32_t table_id, uint64_t poll_epoch );


    // rpc_* apis
    result_tuple get_data( const cell_identifier& cid,
                           const cell_data_type&  c_type ) const;
    bool insert_data( const cell_identifier& cid, const std::string& data,
                      const cell_data_type& c_type ) const;
    bool update_data( const cell_identifier& cid, const std::string& data,
                      const cell_data_type& c_type ) const;

   private:
    snapshot_vector commit_transaction_with_unlock( bool do_unlock );
    void unlock_write_partitions();

    ALWAYS_INLINE void add_partition( const partition_column_identifier& pid,
                                      std::shared_ptr<partition>         part,
                                      partition_column_identifier_set& parts );
    ALWAYS_INLINE void assert_readable_partition(
        const partition_column_identifier& pid ) const;
    ALWAYS_INLINE void assert_writeable_partition(
        const partition_column_identifier& pid ) const;
    void abort_writes( const partition_column_identifier& undo_point );

    bool begin_with_lock_already_acquired();

    bool do_not_apply_last_update(
        const partition_column_identifier& pid ) const;

    std::shared_ptr<partition> internal_get_partition(
        const cell_identifier& cid ) const;

    std::tuple<const partition_column_identifier_set::iterator,
               const partition_column_identifier_set::iterator>
        get_scan_bounds( uint32_t table_id ) const;
    partition_column_identifier get_scan_pcid(
        uint32_t table_id, uint64_t low_key, uint64_t high_key,
        const std::vector<uint32_t>& project_cols ) const;

    partition_column_identifier_partition_map partitions_;
    mutable std::unordered_map<cell_identifier, std::shared_ptr<partition>,
                               cell_identifier_key_hasher,
                               cell_identifier_equal_functor>
                                    records_to_partitions_;
    partition_column_identifier_set write_partition_ids_;
    partition_column_identifier_set read_partition_ids_;
    partition_column_identifier_set do_not_apply_last_partition_ids_;

    snapshot_vector snapshot_;
    snapshot_vector write_snapshot_;

    mutable std::shared_ptr<partition> prev_part_;

    std::unordered_map<uint32_t, uint64_t> table_to_poll_epoch_;
};

#include "transaction_partition_holder-inl.h"
#include "transaction_partition_holder.tcc"
