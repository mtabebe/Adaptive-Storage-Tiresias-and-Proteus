#pragma once

#include <atomic>
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>

#include "../common/bucket_funcs.h"
#include "../common/constants.h"
#include "../data-site/db/partition_metadata.h"
#include "data_site_storage_stats.h"
#include "multi_version_partition_table.h"
#include "site_selector_metadata.h"
#include "site_selector_query_stats.h"

typedef std::unordered_map<int, std::shared_ptr<std::vector<::bucket_key>>>
    site_ws_map;

// A class representing a means to find and lock data_item locations
class partition_data_location_table {
   public:
    partition_data_location_table(
        const partition_data_location_table_configs& configs );
    ~partition_data_location_table();

    // not thread safe, not sure if it matters
    uint32_t create_table( const table_metadata& metadata );

    uint32_t get_num_tables() const;

    std::vector<std::shared_ptr<partition_payload>> get_partitions(
        const std::vector<cell_key_ranges>& cks,
        const partition_lock_mode&          lock_mode ) const;

    std::shared_ptr<partition_payload> get_partition(
        const cell_key& pk, const partition_lock_mode& lock_mode ) const;
    std::shared_ptr<partition_payload> get_partition(
        const partition_column_identifier& pid,
        const partition_lock_mode&         lock_mode ) const;
    std::vector<std::shared_ptr<partition_payload>> get_partition(
        const cell_key_ranges&     ckr,
        const partition_lock_mode& lock_mode ) const;

    std::vector<std::shared_ptr<partition_payload>> get_or_create_partitions(
        const std::vector<cell_key_ranges>& cks,
        const partition_lock_mode&          lock_mode );

    grouped_partition_information get_partitions_and_group(
        const std::vector<cell_key_ranges>& sorted_write_cks,
        const std::vector<cell_key_ranges>& sorted_read_cks,
        const partition_lock_mode& lock_mode, int num_sites,
        bool allow_missing ) const;
    grouped_partition_information get_or_create_partitions_and_group(
        const std::vector<cell_key_ranges>& sorted_write_cks,
        const std::vector<cell_key_ranges>& sorted_read_cks,
        const partition_lock_mode& lock_mode, int num_sites );

    std::shared_ptr<partition_payload> insert_partition(
        std::shared_ptr<partition_payload> payload,
        const partition_lock_mode&         lock_mode );
    std::shared_ptr<partition_payload> remove_partition(
        std::shared_ptr<partition_payload> payload );

    std::vector<std::shared_ptr<partition_payload>> split_partition(
        std::shared_ptr<partition_payload> partition, uint64_t row_split_point,
        uint32_t col_split_point, const partition_type::type& low_type,
        const partition_type::type&    high_type,
        const storage_tier_type::type& low_storage_type,
        const storage_tier_type::type& high_storage_type,
        uint32_t                       low_update_destination_slot,
        uint32_t                       high_update_destination_slot,
        const partition_lock_mode&     lock_mode );
    std::shared_ptr<partition_payload> merge_partition(
        std::shared_ptr<partition_payload> low_partition,
        std::shared_ptr<partition_payload> high_partition,
        const partition_type::type&        merge_type,
        const storage_tier_type::type&     merge_storage_type,
        uint32_t                           update_destination_slot,
        const partition_lock_mode&         lock_mode );

    uint64_t get_default_partition_size_for_table( uint32_t table_id ) const;

    ALWAYS_INLINE void increase_sample_based_site_read_access_count(
        int site, uint64_t count );
    ALWAYS_INLINE void decrease_sample_based_site_read_access_count(
        int site, uint64_t count );
    ALWAYS_INLINE void increase_sample_based_site_write_access_count(
        int site, uint64_t count );
    ALWAYS_INLINE void decrease_sample_based_site_write_access_count(
        int site, uint64_t count );

    ALWAYS_INLINE void modify_site_read_access_count( int site, int64_t count );
    ALWAYS_INLINE void modify_site_write_access_count( int     site,
                                                       int64_t count );
    ALWAYS_INLINE void set_max_write_accesses_if_greater( int64_t count );
    ALWAYS_INLINE void blend_max_write_accesses_if_greater( int64_t count,
                                                            double blend_rate );

    ALWAYS_INLINE std::vector<std::atomic<int64_t>>*
                  get_site_read_access_counts();
    ALWAYS_INLINE std::vector<std::atomic<int64_t>>*
                  get_site_write_access_counts();

    double compute_average_write_accesses() const;
    double compute_average_read_accesses() const;

    double compute_standard_deviation_write_accesses() const;

    double              get_current_load_balance_score() const;
    std::vector<double> get_per_site_load_factors() const;

    uint64_t get_approx_total_number_of_partitions() const;

    site_selector_query_stats* get_stats() const;
    data_site_storage_stats*   get_storage_stats() const;

    void increase_tracking_counts(
        int                                              destination,
        std::vector<std::shared_ptr<partition_payload>>& read_partitions,
        std::vector<std::shared_ptr<partition_payload>>& write_partitions );

    table_metadata get_table_metadata( uint32_t table_id ) const;

    multi_version_partition_data_location_table* get_partition_location_table();

    partition_data_location_table_configs get_configs() const;

   private:
    partition_data_location_table_configs configs_;

    std::vector<std::atomic<uint64_t>> sample_based_site_read_access_counts_;
    std::vector<std::atomic<uint64_t>> sample_based_site_write_access_counts_;

    std::vector<std::atomic<int64_t>> site_read_access_counts_;
    std::vector<std::atomic<int64_t>> site_write_access_counts_;

    std::atomic<int64_t> max_write_access_to_partition_;

    std::unique_ptr<site_selector_query_stats> query_stats_;
    std::unique_ptr<data_site_storage_stats> storage_stats_;

    multi_version_partition_data_location_table table_;
};

#include "partition_data_location_table-inl.h"

