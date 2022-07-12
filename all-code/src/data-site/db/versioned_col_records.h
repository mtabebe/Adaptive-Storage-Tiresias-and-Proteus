#pragma once

#include <memory>
#include <vector>
#include <shared_mutex>

#include "../../common/hw.h"
#include "cell_identifier.h"
#include "packed_column_records.h"
#include "partition_metadata.h"

class versioned_col_records {
   public:
    versioned_col_records( const partition_column_identifier& pcid,
                           bool                               is_column_sorted,
                           const std::vector<cell_data_type>& col_types );

    void init_records();
    uint64_t merge_updates();

    void lock_snapshot_shared() const;
    void unlock_snapshot_shared() const;

    std::shared_ptr<packed_column_records> get_snapshot_columns(
        uint64_t version ) const;
    std::shared_ptr<packed_column_records> get_current_columns() const;
    std::tuple<std::shared_ptr<packed_column_records>, uint64_t>
        get_current_columns_and_version() const;

    uint64_t get_snapshot_version() const;
    uint64_t get_current_version() const;
    uint64_t get_snapshot_version_that_satisfies_requirement(
        uint64_t req_version ) const;

    void set_updated_version( uint64_t version );

    void set_current_records(
        std::shared_ptr<packed_column_records> cur_columns, uint64_t version );

    packed_cell_data* update_uint64_data(
        const cell_identifier& ci, uint64_t data,
        const snapshot_vector& snapshot );
    packed_cell_data* update_int64_data(
        const cell_identifier& ci, int64_t data,
        const snapshot_vector& snapshot );
    packed_cell_data* update_string_data(
        const cell_identifier& ci, const std::string& data,
        const snapshot_vector& snapshot );
    packed_cell_data* update_double_data(
        const cell_identifier& ci, double data,
        const snapshot_vector& snapshot );
    packed_cell_data* get_or_create_packed_cell_data(
        const cell_identifier& ci );

    void commit_latest_updates( uint64_t version );
    void clear_latest_updates();

    cell_data_type get_col_type() const;

   private:
    void commit_latest_multi_column_updates();
    void update_latest_records( const cell_identifier& ci,
                                packed_cell_data*      pcd,
                                const cell_data_type&  cell_type );
    void update_multi_column_record( multi_column_data*    mc,
                                     packed_cell_data*     pcd,
                                     const cell_data_type& cell_type,
                                     uint32_t              col_pos );
    void insert_into_snapshot_map(
        std::shared_ptr<packed_column_records> record, uint64_t version );
    uint64_t search_for_version( uint64_t req_version, uint32_t low_pos,
                                 uint32_t high_pos ) const;

    partition_column_identifier pcid_;
    bool                        is_column_sorted_;
    std::vector<cell_data_type> col_types_;

    folly::ConcurrentHashMap<uint64_t, std::shared_ptr<packed_column_records>>
                                           snapshot_records_;
    std::shared_ptr<packed_column_records> current_records_;

    cell_identifier_map_t<packed_cell_data*> ongoing_writes_;

    std::vector<uint64_t> snapshot_versions_;
    std::atomic<uint64_t> updated_version_;

    // guards the snapshot records
    mutable std::shared_mutex snapshot_lock_;

    // guards the most recent snapshot
    mutable std::shared_mutex updated_lock_;

    bool do_stats_maintenance_;
};

