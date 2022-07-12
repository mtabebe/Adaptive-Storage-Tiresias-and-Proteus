#pragma once

#include <atomic>
#include <mutex>

#include "partition_metadata.h"

#include "../../common/partition_funcs.h"
#include "../../common/stat_funcs.h"

class table_stats {
   public:
    table_stats( const table_metadata& metadata );
    ~table_stats();

    std::vector<cell_widths_stats> get_average_cell_widths() const;
    std::vector<selectivity_stats> get_selectivity_stats() const;
    void                           get_storage_tier_changes(
        partition_column_identifier_map_t<storage_tier_type::type>& changes );

    void update_width( uint32_t col_id, int32_t old_width,
                       int32_t new_width );
    void add_width( uint32_t col_id, int32_t new_width );
    void remove_width( uint32_t col_id, int32_t old_width );

    void add_selectivity( uint32_t col_id, double selectivity );

    void record_storage_tier_change( const partition_column_identifier& pid,
                                     const storage_tier_type::type&     tier );

   private:
    uint32_t                    table_id_;
    std::vector<cell_data_type> col_types_;

    std::vector<std::atomic<double>> running_col_sizes_;
    std::vector<std::atomic<int64_t>> col_counts_;

    std::vector<std::atomic<double>>  running_selectivity_;
    std::vector<std::atomic<int64_t>> selectivity_count_;

    std::mutex storage_tier_lock_;
    partition_column_identifier_map_t<storage_tier_type::type>
        storage_tier_changes_;
};
