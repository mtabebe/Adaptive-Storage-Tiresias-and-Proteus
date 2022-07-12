#pragma once

#include "internal_partition.h"
#include "shared_partition_state.h"

class partition {
   public:
    partition( std::shared_ptr<internal_partition>& internal );
    ~partition();

    // init
    void init(
        const partition_metadata&                        metadata,
        std::shared_ptr<update_destination_interface>    update_destination,
        std::shared_ptr<partition_column_version_holder> part_version_holder,
        const std::vector<cell_data_type>& col_types, void* table_ptr,
        uint64_t version );
    void init_records();
    void init_partition_from_snapshot(
        const snapshot_vector&                    snapshot,
        const std::vector<snapshot_column_state>& snapshot_columns,
        uint32_t                                  master_location );

    void set_commit_version( uint64_t version );
    void set_commit_version_and_update_queue_if_able(
        uint64_t expected_version );

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
    partition_metadata      get_metadata() const;
    uint32_t                get_master_location() const;
    partition_type::type    get_partition_type() const;
    storage_tier_type::type get_storage_tier_type() const;
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
        const snapshot_vector& dependency_snapshot, uint64_t table_epoch,
        bool do_unlock );
    void abort_write_transaction();

    bool change_partition_and_storage_type(
        const partition_type::type&    part_type,
        const storage_tier_type::type& storage_type );
    bool change_type( const partition_type::type& part_type );
    bool change_storage_tier( const storage_tier_type::type& tier );

    // point deletion
    bool remove_data( const cell_identifier& ci,
                      const snapshot_vector& snapshot );

    // point inserts
    bool insert_uint64_data( const cell_identifier& ci, uint64_t data,
                             const snapshot_vector& snapshot );
    bool insert_int64_data( const cell_identifier& ci, int64_t data,
                            const snapshot_vector& snapshot );
    bool insert_string_data( const cell_identifier& ci, const std::string& data,
                             const snapshot_vector& snapshot );
    bool insert_double_data( const cell_identifier& ci, double data,
                             const snapshot_vector& snapshot );

    // point updates
    bool update_uint64_data( const cell_identifier& ci, uint64_t data,
                             const snapshot_vector& snapshot );
    bool update_int64_data( const cell_identifier& ci, int64_t data,
                            const snapshot_vector& snapshot );
    bool update_string_data( const cell_identifier& ci, const std::string& data,
                             const snapshot_vector& snapshot );
    bool update_double_data( const cell_identifier& ci, double data,
                             const snapshot_vector& snapshot );

    // point lookups
    std::tuple<bool, uint64_t> get_uint64_data(
        const cell_identifier& ci, const snapshot_vector& snapshot ) const;
    std::tuple<bool, int64_t> get_int64_data(
        const cell_identifier& ci, const snapshot_vector& snapshot ) const;
    std::tuple<bool, double> get_double_data(
        const cell_identifier& ci, const snapshot_vector& snapshot ) const;
    std::tuple<bool, std::string> get_string_data(
        const cell_identifier& ci, const snapshot_vector& snapshot ) const;

    std::tuple<bool, uint64_t> get_latest_uint64_data(
        const cell_identifier& ci ) const;
    std::tuple<bool, int64_t> get_latest_int64_data(
        const cell_identifier& ci ) const;
    std::tuple<bool, double> get_latest_double_data(
        const cell_identifier& ci ) const;
    std::tuple<bool, std::string> get_latest_string_data(
        const cell_identifier& ci ) const;

    void scan( uint64_t low_key, uint64_t high_key,
               const predicate_chain&       predicate,
               const std::vector<uint32_t>& project_cols,
               const snapshot_vector&       snapshot,
               std::vector<result_tuple>&   result_tuples );

    // partition wide transactions
    // e.g. SCAN, MIN, MAX, COUNT, AVG

    // partition wide operations
    void set_master_location( uint32_t new_master );
    void remaster( uint32_t new_master );

    uint64_t wait_until_version_or_apply_updates( uint64_t req_version );
    uint64_t wait_for_dependency_version( uint64_t wait_version );
    uint64_t wait_for_version_no_apply_last( const snapshot_vector& snapshot );

    uint64_t internal_wait_until_version_or_apply_updates(
        uint64_t req_version );

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

    void snapshot_partition_data( snapshot_partition_column_state& snapshot );
    void snapshot_partition_metadata(
        snapshot_partition_column_state& snapshot ) const;

    void split_records( partition* low_p, partition* high_p,
                        uint32_t col_split_point, uint64_t row_split_point );
    void merge_records( partition* low_p, partition* high_p,
                        uint32_t col_merge_point, uint64_t row_merge_point );

    void persist_data(
        data_persister* part_persister,
        const std::tuple<uint32_t, uint64_t>& update_information_slot );

    void pull_data_if_not_in_memory( bool do_locking, bool do_notify );

   protected:
    bool change_storage_tier_internal( const storage_tier_type::type& tier,
                                       bool do_locking );

    void pull_from_disk_internal();
    void evict_to_disk_internal();

    uint64_t persist_to_disk( data_persister* part_persister );
    void restore_from_disk( data_reader* part_reader );

    uint64_t persist_to_disk_internal( data_persister* part_persister );
    void restore_from_disk_internal( data_reader* part_reader );

    void transfer_records( partition*                         src,
                           const partition_column_identifier& pcid );
    void repartition_cell_into_partition( const cell_identifier& ci,
                                          partition*             src );

    bool change_type_internal( const partition_type::type& part_type,
                               bool                        do_notify );
    void generic_change_type( std::shared_ptr<internal_partition>& cur_part,
                              std::shared_ptr<internal_partition>& new_part,
                              uint64_t version ) const;
    void change_from_sorted_to_unsorted_column(
        std::shared_ptr<internal_partition>& cur_part,
        std::shared_ptr<internal_partition>& new_part, uint64_t version ) const;
    void change_from_sorted_to_unsorted_multi_column(
        std::shared_ptr<internal_partition>& cur_part,
        std::shared_ptr<internal_partition>& new_part, uint64_t version ) const;

    void finalize_repartitions( uint64_t version );
    bool apply_propagated_cell_update( const deserialized_cell_op& du_cell,
                                       const snapshot_vector&      snapshot,
                                       bool do_insert, bool do_store,
                                       uint64_t version );

    void install_snapshotted_column( const snapshot_column_state& column,
                                     const snapshot_vector&       snapshot );

    void generic_split_records( partition* low_p, partition* high_p,
                                uint32_t col_split_point,
                                uint64_t row_split_point, uint64_t low_version,
                                uint64_t high_version );
    void generic_merge_records( partition* low_p, partition* high_p,
                                uint32_t col_merge_point,
                                uint64_t row_merge_point,
                                uint64_t merge_version );

    void split_col_records( partition* low_p, partition* high_p,
                            uint32_t col_split_point, uint64_t row_split_point,
                            uint64_t low_version, uint64_t high_version );
    void split_multi_col_records( partition* low_p, partition* high_p,
                                  uint32_t col_split_point,
                                  uint64_t row_split_point,
                                  uint64_t low_version, uint64_t high_version );
    void split_row_records( partition* low_p, partition* high_p,
                            uint32_t col_split_point, uint64_t row_split_point,
                            uint64_t low_version, uint64_t high_version );
    void merge_col_records( partition* low_p, partition* high_p,
                            uint32_t col_merge_point, uint64_t row_merge_point,
                            uint64_t merge_version );
    void merge_multi_col_records( partition* low_p, partition* high_p,
                                  uint32_t col_merge_point,
                                  uint64_t row_merge_point,
                                  uint64_t merge_version );
    void merge_row_records( partition* low_p, partition* high_p,
                            uint32_t col_merge_point, uint64_t row_merge_point,
                            uint64_t merge_version );

    void internal_init(
        const partition_metadata&                        metadata,
        std::shared_ptr<update_destination_interface>    update_destination,
        std::shared_ptr<partition_column_version_holder> part_version_holder,
        const std::vector<cell_data_type>& col_types, void* table_ptr,
        uint64_t specified_version );

    uint64_t commit_write_transaction_no_unlock_no_propagate(
        const snapshot_vector& dependency_snapshot, uint64_t table_epoch );

    void finalize_commit( const snapshot_vector& dependency_snapshot,
                          uint64_t version, uint64_t table_epoch,
                          bool no_prop_commit );

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
    void handle_split_partition_completion( bool is_locked_by_us,
                                            std::shared_ptr<partition> part,
                                            uint64_t version );
    void advance_if_locked( bool                       is_locked_by_us,
                            std::shared_ptr<partition> part, uint64_t version );
    void done_with_updates();

    void change_active_bits( uint16_t is_active );
    void change_pin_counter( int64_t op_val );

    void create_new_internal_partition();

    bool evaluate_predicate_on_storage_tier(
        const predicate_chain&       predicate,
        const std::vector<uint32_t>& project_cols ) const;

    void scan_internal( uint64_t low_key, uint64_t high_key,
                        const predicate_chain&       predicate,
                        const std::vector<uint32_t>& project_cols,
                        const snapshot_vector&       snapshot,
                        std::vector<result_tuple>&   result_tuples );

    partition_metadata metadata_;

    std::shared_ptr<shared_partition_state> shared_partition_;
    std::shared_ptr<internal_partition>     internal_partition_;

    // int is the pin counter, bool is whether it is active
    atomic_packed_pointer active_and_pin_state_;

    // lock
    semaphore lock_;
};

typedef partition_column_identifier_map_t<std::shared_ptr<partition>>
    partition_column_identifier_partition_map;

class split_partition_result {
   public:
    bool okay_;

    std::shared_ptr<partition> partition_low_;
    std::shared_ptr<partition> partition_high_;
    std::shared_ptr<partition> partition_cover_;
    snapshot_vector            snapshot_;
};


