#pragma once

#include <map>
#include <memory>

#include "internal_partition.h"
#include "row_partition-inl.h"
#include "versioned_row_records.h"

class row_partition : public internal_partition {
   public:
    row_partition();

    // init
    void init( const partition_metadata&          metadata,
               const std::vector<cell_data_type>& col_types, uint64_t version,
               std::shared_ptr<shared_partition_state>& shared_partition,
               void* parent_partition ) override;

    partition_type::type get_partition_type() const override;

    void init_records() override;
    void set_row_records( std::shared_ptr<versioned_row_records> records );
    std::shared_ptr<versioned_row_records> get_row_records() const;

    void abort_write_transaction() override;
    void finalize_commit( const snapshot_vector& dependency_snapshot,
                          uint64_t version, uint64_t table_epoch,
                          bool no_prop_commit ) override;

    bool begin_read_wait_and_build_snapshot( snapshot_vector& snapshot,
                                             bool no_apply_last ) override;

    bool begin_read_wait_for_version( snapshot_vector& snapshot,
                                      bool             no_apply_last ) override;
    bool begin_write( snapshot_vector& snapshot, bool acquire_lock ) override;



    bool remove_data( const cell_identifier& ci,
                      const snapshot_vector& snapshot ) override;

    // point inserts
    bool insert_uint64_data( const cell_identifier& ci, uint64_t data,
                             const snapshot_vector& snapshot ) override;
    bool insert_int64_data( const cell_identifier& ci, int64_t data,
                            const snapshot_vector& snapshot ) override;
    bool insert_string_data( const cell_identifier& ci, const std::string& data,
                             const snapshot_vector& snapshot ) override;
    bool insert_double_data( const cell_identifier& ci, double data,
                             const snapshot_vector& snapshot ) override;

    // point updates
    bool update_uint64_data( const cell_identifier& ci, uint64_t data,
                             const snapshot_vector& snapshot ) override;
    bool update_int64_data( const cell_identifier& ci, int64_t data,
                            const snapshot_vector& snapshot ) override;
    bool update_string_data( const cell_identifier& ci, const std::string& data,
                             const snapshot_vector& snapshot ) override;
    bool update_double_data( const cell_identifier& ci, double data,
                             const snapshot_vector& snapshot ) override;

    // point lookups
    std::tuple<bool, uint64_t> get_uint64_data(
        const cell_identifier& ci,
        const snapshot_vector& snapshot ) const override;
    std::tuple<bool, int64_t> get_int64_data(
        const cell_identifier& ci,
        const snapshot_vector& snapshot ) const override;
    std::tuple<bool, double> get_double_data(
        const cell_identifier& ci,
        const snapshot_vector& snapshot ) const override;
    std::tuple<bool, std::string> get_string_data(
        const cell_identifier& ci,
        const snapshot_vector& snapshot ) const override;

    std::tuple<bool, uint64_t> get_latest_uint64_data(
        const cell_identifier& ci ) const override;
    std::tuple<bool, int64_t> get_latest_int64_data(
        const cell_identifier& ci ) const override;
    std::tuple<bool, double> get_latest_double_data(
        const cell_identifier& ci ) const override;
    std::tuple<bool, std::string> get_latest_string_data(
        const cell_identifier& ci ) const override;

    void scan( uint64_t low_key, uint64_t high_key,
               const predicate_chain&       predicate,
               const std::vector<uint32_t>& project_cols,
               const snapshot_vector&       snapshot,
               std::vector<result_tuple>&   result_tuples ) const override;

    void merge_row_records( row_partition* low_p, row_partition* high_p,
                            uint32_t col_merge_point, uint64_t row_merge_point,
                            uint64_t merge_version );
    void split_row_records( row_partition* low_p, row_partition* high_p,
                            uint32_t col_split_point, uint64_t row_split_point,
                            uint64_t low_version, uint64_t high_version );

    void snapshot_partition_data(
        snapshot_partition_column_state& snapshot ) override;

    void persist_data( data_persister* part_persister ) const override;
    column_stats<multi_column_data> persist_to_disk(
        data_persister* part_persister ) const override;
    void restore_from_disk( data_reader* reader, uint64_t version ) override;

    // partition wide transactions
    // e.g. SCAN, MIN, MAX, COUNT, AVG
    //
   private:
    const row_record* get_row_record( uint64_t               key,
                                      const snapshot_vector& snapshot ) const;

    packed_cell_data* read_cell_data( const cell_identifier& ci,
                                      const snapshot_vector& snapshot ) const;
    packed_cell_data* read_latest_cell_data(
        const cell_identifier& ci, const snapshot_vector& snapshot ) const;
    packed_cell_data* get_cell_data_from_row_record(
        const cell_identifier& ci, const row_record* row_rec ) const;
    packed_cell_data* read_row_from_disk(
        const cell_identifier& ci, const snapshot_vector& snapshot ) const;
    packed_cell_data* read_row_from_disk_file(
        data_reader* reader, const cell_identifier& ci ) const;

    std::tuple<int32_t, packed_cell_data*> write_cell_data(
        const cell_identifier& ci, const snapshot_vector& snapshot,
        uint32_t op_code, bool do_prop, bool set_version, uint64_t version );
    std::tuple<int32_t, packed_cell_data*> update_cell_data(
        const cell_identifier& ci, const snapshot_vector& snapshot,
        bool do_prop, bool set_version, uint64_t version );
    std::tuple<int32_t, packed_cell_data*> insert_cell_data(
        const cell_identifier& ci, const snapshot_vector& snapshot,
        bool do_prop, bool set_version, uint64_t version );
    bool internal_remove_data( const cell_identifier& ci,
                               const snapshot_vector& snapshot, bool do_prop,
                               bool set_version, uint64_t version );

    void init_record( row_record* rec );
    void add_to_write_buffer( const cell_identifier& ci, row_record* row_rec,
                              uint32_t op_code );

    void transfer_row_records_horizontally(
        row_partition*                          src,
        std::shared_ptr<versioned_row_records>& dest_records,
        const partition_metadata&               metadata );

    void merge_row_records_vertically(
        std::shared_ptr<versioned_row_records>& cover, uint64_t cover_v,
        uint32_t merge_num_columns, uint32_t merge_point,
        std::shared_ptr<versioned_row_records>& low,
        std::shared_ptr<versioned_row_records>& high );
    void split_row_records_vertically(
        uint32_t split_col_point, std::shared_ptr<versioned_row_records>& cover,
        std::shared_ptr<versioned_row_records>& low, row_partition* low_p,
        uint64_t low_v, std::shared_ptr<versioned_row_records>& high,
        row_partition* high_p, uint64_t high_v );

    void repartition_row_record( uint64_t key, uint64_t new_partition_id );

    void repartition_cell_into_partition( const cell_identifier& ci,
                                          internal_partition*    src ) override;
    bool internal_repartition_cell_into_partition( const cell_identifier& ci,
                                                   internal_partition*    src );

    void finalize_repartitions( uint64_t version ) override;
    void finalize_change_type( uint64_t version ) override;

    bool apply_propagated_cell_update( const deserialized_cell_op& du_cell,
                                       const snapshot_vector&      snapshot,
                                       bool do_insert, bool do_store,
                                       uint64_t version ) override;

    void install_snapshotted_column( const snapshot_column_state& column,
                                     const snapshot_vector& snapshot ) override;
    bool install_snapshotted_column_pos( const std::vector<int64_t>& keys,
                                         const std::string&           data,
                                         const int32_t&               col_id,
                                         const data_type::type&       d_type,
                                         const snapshot_vector&       snapshot,
                                         uint64_t                     version );

    void update_stats_for_persistence(
        row_record* rec, column_stats<multi_column_data>& stats ) const;

    result_tuple generate_result_tuple(
        const row_record* row_rec, const std::vector<uint32_t>& cols_to_project,
        uint64_t row ) const;

    std::shared_ptr<versioned_row_records> records_;

    std::unordered_map<uint64_t, row_record*> inflight_updates_;
};

