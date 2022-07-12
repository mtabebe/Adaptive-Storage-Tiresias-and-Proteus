#pragma once

#include "../../common/hw.h"
#include "../../common/partition_funcs.h"
#include "../../concurrency/mutex_lock.h"
#include "../../concurrency/semaphore.h"
#include "../../persistence/data_persister.h"
#include "../../persistence/data_reader.h"
#include "../update-propagation/update_destination_interface.h"
#include "../update-propagation/update_queue.h"
#include "../update-propagation/write_buffer_serialization.h"

#include "cell_identifier.h"
#include "partition_column_version_holder.h"
#include "partition_dependency.h"
#include "partition_metadata.h"
#include "predicate.h"
#include "record_identifier.h"
#include "transaction_execution_state.h"

class shared_partition_state {
   public:
    shared_partition_state();
    ~shared_partition_state();

    // init
    void init(
        const partition_metadata&                        metadata,
        std::shared_ptr<update_destination_interface>    update_destination,
        std::shared_ptr<partition_column_version_holder> part_version_holder,
        const std::vector<cell_data_type>& col_types, void* table_ptr,
        uint64_t version );
    void         init_partition_from_snapshot(
        const snapshot_vector&                    snapshot,
        const std::vector<snapshot_column_state>& snapshot_columns,
        uint32_t                                  master_location );

    void set_commit_version( uint64_t version );
    void set_commit_version_and_update_queue_if_able(
        uint64_t expected_version );

    void set_storage_tier( const storage_tier_type::type& tier );
    storage_tier_type::type get_storage_tier();
    void set_storage_stats( column_stats<multi_column_data>& stats );

    void lock_storage();
    void unlock_storage();

    uint64_t persist_metadata( data_persister* part_persister );
    uint64_t restore_metadata( data_reader* part_reader );

    // locking
    void lock_partition();
    bool try_lock_partition();
    void unlock_partition();

    void pin_partition();
    void unpin_partition();
    bool is_pinned();

    bool is_active();
    void mark_as_inactive();
    void mark_as_active();

    bool is_safe_to_remove();

    uint64_t apply_updates_until_empty( uint64_t desired_version_or_num_updates,
                                        bool     is_until_version );

    // metadata
    partition_metadata get_metadata() const;
    uint32_t           get_master_location() const;
    cell_data_type get_cell_type( const cell_identifier& ci ) const;

    // begin and commit
    bool begin_read_wait_and_build_snapshot( snapshot_vector& snapshot,
                                             bool             no_apply_last );

    bool begin_read_wait_for_version( snapshot_vector& snapshot,
                                      bool             no_apply_last );
    bool begin_write( snapshot_vector& snapshot, bool acquire_lock );

    uint64_t get_most_recent_version();
    void get_most_recent_snapshot_vector( snapshot_vector& snapshot_vector );
    void build_commit_snapshot( snapshot_vector& snapshot );
    uint64_t commit_write_transaction(
        const snapshot_vector& dependency_snapshot, uint64_t table_epoch );
    void abort_write_transaction();

    // partition wide transactions
    // e.g. SCAN, MIN, MAX, COUNT, AVG

    // partition wide operations
    void set_master_location( uint32_t new_master );
    void remaster( uint32_t new_master );

    uint64_t wait_until_version_or_apply_updates( uint64_t req_version );
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

    void finalize_commit( const snapshot_vector& dependency_snapshot,
                          uint64_t version, uint64_t table_epoch,
                          bool no_prop_commit );

    void remove_table_stats_data( const cell_identifier& ci, int32_t sz );
    void update_table_stats_data( const cell_identifier& ci, int32_t old_size,
                                  int32_t new_sz );

    void snapshot_partition_metadata(
        snapshot_partition_column_state& snapshot ) const;

    void check_cell_identifier_within_partition(
        const cell_identifier& ci ) const;
    void check_record_identifier_within_partition(
        const record_identifier& ri ) const;

    void internal_init(
        const partition_metadata&                        metadata,
        std::shared_ptr<update_destination_interface>    update_destination,
        std::shared_ptr<partition_column_version_holder> part_version_holder,
        const std::vector<cell_data_type>& col_types, void* table_ptr,
        uint64_t specified_version );
    uint32_t translate_column( uint32_t raw_col_id ) const;

    uint64_t commit_write_transaction_no_unlock_no_propagate(
        const snapshot_vector& dependency_snapshot, uint64_t table_epoch );

    void propagate_updates( uint64_t version );

    void record_version_on_table( uint64_t version );
    void record_version_and_epoch_on_table( uint64_t version,
                                            uint64_t table_epoch );

    void execute_deserialized_partition_update_with_locks(
        deserialized_update* update );
    void execute_deserialized_cell_update(
        const deserialized_cell_op& du_record, const snapshot_vector& snapshot,
        uint64_t version );
    void execute_deserialized_partition_operation_update(
        const partition_column_operation_identifier& poi, uint64_t version,
        uint64_t table_epoch, deserialized_update* deserialized_ptr );

    void handle_change_partition_destination(
        const partition_column_identifier& pid,
        deserialized_update*               deserialized_ptr );
    void handle_partition_remaster( const partition_column_identifier& pid,
                                    uint32_t             new_master,
                                    deserialized_update* deserialized_ptr );
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

    std::vector<uint32_t> get_col_ids_to_scan(
        uint64_t low_key, uint64_t high_key,
        const std::vector<uint32_t>& project_cols ) const;
    predicate_chain translate_predicate_to_partition(
        const predicate_chain& predicate ) const;

    std::tuple<bool, std::string> get_disk_file_name() const;

    partition_metadata metadata_;

    transaction_execution_state txn_execution_state_;
    std::atomic<uint32_t>       master_location_;

    partition_dependency dependency_;

    update_queue          update_queue_;
    std::atomic<uint64_t> apply_version_;

    std::shared_ptr<update_destination_interface>    update_destination_;
    std::shared_ptr<partition_column_version_holder> part_version_holder_;

    std::vector<cell_data_type> col_types_;

    std::atomic<storage_tier_type::type> storage_tier_;
    semaphore                            storage_lock_;
    std::atomic<uint64_t>                storage_version_;
    column_stats<multi_column_data>      stored_stats_;

    void* table_ptr_;
};

std::tuple<partition_column_identifier, partition_column_identifier>
    construct_split_partition_column_identifiers(
        const partition_column_identifier& pid, int64_t split_row_point,
        int32_t split_col_point );


