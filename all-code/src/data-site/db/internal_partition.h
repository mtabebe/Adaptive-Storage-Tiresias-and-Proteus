#pragma once

#include "packed_column_records.h"
#include "shared_partition_state.h"

class internal_partition {
   public:
    internal_partition();
    virtual ~internal_partition();

    // init
    virtual void init(
        const partition_metadata&          metadata,
        const std::vector<cell_data_type>& col_types, uint64_t version,
        std::shared_ptr<shared_partition_state>& shared_partition,
        void*                                    parent_partition );
    virtual void init_records() = 0;
    void         init_partition_from_snapshot(
        const snapshot_vector&                    snapshot,
        const std::vector<snapshot_column_state>& snapshot_columns,
        uint32_t                                  master_location );

    virtual void set_commit_version( uint64_t version );
    void set_commit_version_and_update_queue_if_able(
        uint64_t expected_version );

    // metadata
    virtual partition_type::type get_partition_type() const = 0;

    // begin and commit
    virtual bool begin_read_wait_and_build_snapshot( snapshot_vector& snapshot,
                                                     bool no_apply_last );

    virtual bool begin_read_wait_for_version( snapshot_vector& snapshot,
                                              bool             no_apply_last );
    virtual bool begin_write( snapshot_vector& snapshot, bool acquire_lock );

    uint64_t commit_write_transaction(
        const snapshot_vector& dependency_snapshot, uint64_t table_epoch );
    virtual void abort_write_transaction();

    // point deletion
    virtual bool remove_data( const cell_identifier& ci,
                              const snapshot_vector& snapshot ) = 0;

    // point inserts
    virtual bool insert_uint64_data( const cell_identifier& ci, uint64_t data,
                                     const snapshot_vector& snapshot ) = 0;
    virtual bool insert_int64_data( const cell_identifier& ci, int64_t data,
                                    const snapshot_vector& snapshot ) = 0;
    virtual bool insert_string_data( const cell_identifier& ci,
                                     const std::string&     data,
                                     const snapshot_vector& snapshot ) = 0;
    virtual bool insert_double_data( const cell_identifier& ci, double data,
                                     const snapshot_vector& snapshot ) = 0;

    // point updates
    virtual bool update_uint64_data( const cell_identifier& ci, uint64_t data,
                                     const snapshot_vector& snapshot ) = 0;
    virtual bool update_int64_data( const cell_identifier& ci, int64_t data,
                                    const snapshot_vector& snapshot ) = 0;
    virtual bool update_string_data( const cell_identifier& ci,
                                     const std::string&     data,
                                     const snapshot_vector& snapshot ) = 0;
    virtual bool update_double_data( const cell_identifier& ci, double data,
                                     const snapshot_vector& snapshot ) = 0;

    // point lookups
    virtual std::tuple<bool, uint64_t> get_uint64_data(
        const cell_identifier& ci , const snapshot_vector& snapshot) const = 0;
    virtual std::tuple<bool, int64_t> get_int64_data(
        const cell_identifier& ci, const snapshot_vector& snapshot ) const = 0;
    virtual std::tuple<bool, double> get_double_data(
        const cell_identifier& ci, const snapshot_vector& snapshot ) const = 0;
    virtual std::tuple<bool, std::string> get_string_data(
        const cell_identifier& ci, const snapshot_vector& snapshot ) const = 0;

    virtual std::tuple<bool, uint64_t> get_latest_uint64_data(
        const cell_identifier& ci ) const = 0;
    virtual std::tuple<bool, int64_t> get_latest_int64_data(
        const cell_identifier& ci ) const = 0;
    virtual std::tuple<bool, double> get_latest_double_data(
        const cell_identifier& ci ) const = 0;
    virtual std::tuple<bool, std::string> get_latest_string_data(
        const cell_identifier& ci ) const = 0;

    virtual void scan( uint64_t low_key, uint64_t high_key,
                       const predicate_chain&       predicate,
                       const std::vector<uint32_t>& project_cols,
                       const snapshot_vector&       snapshot,
                       std::vector<result_tuple>&   result_tuples ) const = 0;

    // partition wide transactions
    // e.g. SCAN, MIN, MAX, COUNT, AVG

    // partition wide operations
    void set_master_location( uint32_t new_master );
    void remaster( uint32_t new_master );

    virtual uint64_t wait_until_version_or_apply_updates(
        uint64_t req_version );
    uint64_t wait_for_dependency_version( uint64_t wait_version );
    uint64_t wait_for_version_no_apply_last( const snapshot_vector& snapshot );

    void begin_read_build_snapshot( uint64_t         read_version,
                                    snapshot_vector& snapshot );

    std::shared_ptr<partition_column_version_holder>
        get_partition_column_version_holder() const;

    uint32_t add_stashed_update( stashed_update&& update );
    void set_update_queue_expected_version( uint64_t expected_version );

    uint32_t apply_k_updates_or_empty( uint32_t num_updates_to_apply );
    uint64_t apply_updates_until_version_or_empty( uint64_t version );

    void execute_deserialized_partition_update( deserialized_update* update );

    // record this as an op
    void switch_update_destination(
        std::shared_ptr<update_destination_interface> new_update_destination );
    // just set the pointer
    void set_update_destination(
        std::shared_ptr<update_destination_interface> new_update_destination );
    update_propagation_information get_update_propagation_information();
    void                           add_subscription_state_to_write_buffer(
        const update_propagation_information& update_prop_info,
        bool                                  do_start_subscribing );
    void add_switch_subscription_state_to_write_buffer(
        const update_propagation_information& old_sub,
        const update_propagation_information& new_sub );

    void add_repartition_op( const partition_metadata& metadata,
                             uint32_t col_split, uint64_t row_split,
                             uint32_t op_code );

    virtual void snapshot_partition_data(
        snapshot_partition_column_state& snapshot ) = 0;

    virtual void finalize_commit( const snapshot_vector& dependency_snapshot,
                                  uint64_t version, uint64_t table_epoch,
                                  bool no_prop_commit );

    virtual void repartition_cell_into_partition( const cell_identifier& ci,
                                                  internal_partition* src ) = 0;
    virtual void finalize_repartitions( uint64_t version ) = 0;
    virtual void finalize_change_type( uint64_t version ) = 0;

    virtual bool apply_propagated_cell_update(
        const deserialized_cell_op& du_cell, const snapshot_vector& snapshot,
        bool do_insert, bool do_store, uint64_t version ) = 0;

    void snapshot_partition_metadata(
        snapshot_partition_column_state& snapshot );

    virtual void install_snapshotted_column(
        const snapshot_column_state& column,
        const snapshot_vector&       snapshot ) = 0;

    uint64_t commit_write_transaction_no_unlock_no_propagate(
        const snapshot_vector& dependency_snapshot, uint64_t table_epoch );

    void execute_deserialized_partition_update_with_locks(
        deserialized_update* update );
    void execute_deserialized_cell_update(
        const deserialized_cell_op& du_record, const snapshot_vector& snapshot,
        uint64_t version );
    void execute_deserialized_partition_operation_update(
        const partition_column_operation_identifier& poi, uint64_t version,
        uint64_t table_epoch, deserialized_update* deserialized_ptr );

    void handle_partition_split( const partition_column_identifier& pid,
                                 uint32_t col_split, uint64_t row_split,
                                 deserialized_update* deserialized_ptr,
                                 uint64_t version, uint64_t table_epoch );
    void handle_partition_merge( const partition_column_identifier& pid,
                                 uint32_t col_split, uint64_t row_split,
                                 deserialized_update* deserialized_ptr,
                                 uint64_t version, uint64_t table_epoch );
    void handle_original_partition_merge(
        const partition_column_identifier& pid,
        deserialized_update*               deserialized_ptr );

    virtual void done_with_updates();

    virtual void persist_data( data_persister* part_persister ) const = 0;
    virtual column_stats<multi_column_data> persist_to_disk(
        data_persister* part_persister ) const = 0;
    virtual void restore_from_disk( data_reader* part_reader,
                                    uint64_t     version ) = 0;

    void remove_table_stats_data( const cell_identifier& ci, int32_t sz );
    void update_table_stats_data( const cell_identifier& ci, int32_t old_size,
                                  int32_t new_sz );

    void check_cell_identifier_within_partition(
        const cell_identifier& ci ) const;
    void check_record_identifier_within_partition(
        const record_identifier& ri ) const;


    uint32_t translate_column( uint32_t raw_col_id ) const;
    std::vector<uint32_t> get_col_ids_to_scan(
        uint64_t low_key, uint64_t high_key,
        const std::vector<uint32_t>& project_cols ) const;
    predicate_chain translate_predicate_to_partition(
        const predicate_chain& predicate ) const;

    cell_data_type get_cell_type( const cell_identifier& ci ) const;

    void lock_partition();
    bool try_lock_partition();
    void unlock_partition();

    void pull_data_if_not_in_memory();

    void chomp_metadata( data_reader* reader ) const;

    partition_metadata                      metadata_;
    std::shared_ptr<shared_partition_state> shared_partition_;
    void*                                   parent_partition_;
};
