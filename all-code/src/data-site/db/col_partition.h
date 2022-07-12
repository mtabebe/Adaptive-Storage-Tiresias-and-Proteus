#pragma once

#include <map>
#include <memory>


#include "partition.h"
#include "versioned_col_records.h"


class col_partition : public internal_partition {
   public:
    col_partition();
    col_partition( bool is_sorted );

    // init
    void init( const partition_metadata&          metadata,
               const std::vector<cell_data_type>& col_types, uint64_t version,
               std::shared_ptr<shared_partition_state>& shared_partition,
               void* parent_partition ) override;

    partition_type::type get_partition_type() const override;

    void init_records() override;
    void set_commit_version( uint64_t version ) override;
    void abort_write_transaction() override;
    void finalize_commit( const snapshot_vector& dependency_snapshot,
                          uint64_t version, uint64_t table_epoch,
                          bool no_prop_commit ) override;

    bool begin_read_wait_and_build_snapshot( snapshot_vector& snapshot,
                                             bool no_apply_last ) override;

    bool begin_read_wait_for_version( snapshot_vector& snapshot,
                                      bool             no_apply_last ) override;
    bool begin_write( snapshot_vector& snapshot, bool acquire_lock ) override;

    void set_records( std::shared_ptr<versioned_col_records> records );

    uint64_t wait_until_version_or_apply_updates(
        uint64_t req_version ) override;

    // point deletion
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

    void change_type_from_sorted_column( const col_partition* sorted_col,
                                         uint64_t             version );

    void merge_col_records( col_partition* low_p, col_partition* high_p,
                            uint32_t col_merge_point, uint64_t row_merge_point,
                            uint64_t merge_version );

    void split_col_records( col_partition* low_p, col_partition* high_p,
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
  private:
   std::shared_ptr<packed_column_records> read_cell_data(
       const cell_identifier& ci, const snapshot_vector& snapshot ) const;
   std::shared_ptr<packed_column_records> read_latest_cell_data(
       const cell_identifier& ci, const snapshot_vector& snapshot ) const;

   std::shared_ptr<packed_column_records> write_cell_data(
       const cell_identifier& ci );

   void lock_share_col_snapshot() const;
   void unlock_share_col_snapshot() const;

   void add_to_write_buffer( const cell_identifier& ci, packed_cell_data* pcd,
                             uint32_t op_code );

   void repartition_cell_into_partition( const cell_identifier& ci,
                                         internal_partition*    src ) override;
   void finalize_repartitions( uint64_t version ) override;
   void finalize_change_type( uint64_t version ) override;

   bool apply_propagated_cell_update( const deserialized_cell_op& du_cell,
                                      const snapshot_vector&      snapshot,
                                      bool do_insert, bool do_store,
                                      uint64_t version ) override;
   bool internal_remove_data( const cell_identifier& ci,
                              const snapshot_vector& snapshot, bool do_prop,
                              bool do_update_stats );
   void install_snapshotted_column( const snapshot_column_state& column,
                                    const snapshot_vector& snapshot ) override;
   bool install_snapshotted_column_pos( const std::vector<int64_t>& keys,
                                        const std::string&          data,
                                        const int32_t&              col_id,
                                        const data_type::type&      d_type,
                                        const snapshot_vector&      snapshot );

   void done_with_updates() override;

   void internal_scan( uint64_t low_key, uint64_t high_key,
                       const predicate_chain&       predicate,
                       const std::vector<uint32_t>& project_cols,
                       const snapshot_vector&       snapshot,
                       std::vector<result_tuple>&   result_tuples ) const;

   template <typename T>
   std::tuple<bool, T> read_cell_from_disk(
       const cell_identifier& ci, const snapshot_vector& snapshot ) const;
   int32_t get_index_position_from_file( data_reader*           reader,
                                         const cell_identifier& ci ) const;

   std::shared_ptr<versioned_col_records> records_;

   bool is_sorted_;

   bool do_stats_maintenance_;
};

class sorted_col_partition : public col_partition {
   public:
    sorted_col_partition();

    partition_type::type get_partition_type() const override;
};


#include "col_partition-inl.h"
