#pragma once

#include <folly/concurrency/ConcurrentHashMap.h>
#include <memory>

#include "../common/partition_funcs.h"
#include "../data-site/db/partition_metadata.h"
#include "../persistence/site_selector_table_persister.h"
#include "grouped_partition_information.h"
#include "partition_payload.h"
#include "site_selector_metadata.h"

class internal_hash_multi_version_partition_payload_table {
   public:
    internal_hash_multi_version_partition_payload_table(
        const table_metadata& metadata );
    ~internal_hash_multi_version_partition_payload_table();

    std::shared_ptr<partition_payload> get_partition(
        const cell_key& ck, const partition_lock_mode& lock_mode,
        partition_column_identifier_unordered_set& already_locked_set ) const;
    std::shared_ptr<partition_payload> get_partition(
        const partition_column_identifier&         pid,
        const partition_lock_mode&                 lock_mode,
        partition_column_identifier_unordered_set& already_locked_set ) const;
    std::vector<std::shared_ptr<partition_payload>> get_partition(
        const cell_key_ranges& ckr, const partition_lock_mode& lock_mode,
        partition_column_identifier_unordered_set& already_locked_set ) const;

    std::shared_ptr<partition_payload> get_or_create(
        const cell_key& ck, const partition_column_identifier& default_pid,
        const partition_lock_mode&                 lock_mode,
        partition_column_identifier_unordered_set& already_locked_set );
    std::shared_ptr<partition_payload> get_or_create(
        const cell_key& ck, const partition_lock_mode& lock_mode,
        partition_column_identifier_unordered_set& already_locked_set );

    std::vector<std::shared_ptr<partition_payload>> get_or_create(
        const cell_key_ranges& ckr, const partition_lock_mode& lock_mode,
        partition_column_identifier_unordered_set& already_locked_set );

    std::shared_ptr<partition_payload> remove_partition(
        std::shared_ptr<partition_payload> payload );
    std::shared_ptr<partition_payload> insert_partition(
        std::shared_ptr<partition_payload> payload,
        const partition_lock_mode&         lock_mode );

    std::shared_ptr<partition_payload> merge_partition(
        std::shared_ptr<partition_payload> low_payload,
        std::shared_ptr<partition_payload> high_payload,
        const partition_type::type&        merge_type,
        const storage_tier_type::type&     merge_storage_type,
        uint32_t                           update_destination_slot,
        const partition_lock_mode&         lock_mode );
    std::vector<std::shared_ptr<partition_payload>> split_partition(
        std::shared_ptr<partition_payload> ori_payload,
        uint64_t row_split_point, uint32_t col_split_point,
        const partition_type::type&    low_type,
        const partition_type::type&    high_type,
        const storage_tier_type::type& low_storage_type,
        const storage_tier_type::type& high_storage_type,
        uint32_t                       low_update_destination_slot,
        uint32_t                       high_update_destination_slot,
        const partition_lock_mode&     lock_mode );

    uint64_t get_approx_total_number_of_partitions() const;

    table_metadata get_table_metadata() const;

    void persist_table_location( site_selector_table_persister* persister );

    uint64_t get_default_partition_size_for_table() const;
    uint32_t get_default_column_size_for_table() const;

    void build_partition_access_frequencies(
        partition_access_entry_priority_queue& sorted_partition_access_counts,
        partition_column_identifier_unordered_set&    partition_accesses ) const;

   private:
    std::shared_ptr<partition_payload_row_holder> get_or_insert_row(
        int64_t row );
    std::tuple<bool, std::shared_ptr<partition_payload>> get_or_insert_into_row(
        const cell_key&                     ck,
        std::shared_ptr<partition_payload>& created_payload );
    std::tuple<bool, std::shared_ptr<partition_payload>>
        get_or_insert_into_row_and_expand(
            const cell_key&                     ck,
            std::shared_ptr<partition_payload>& created_payload );

    std::shared_ptr<partition_payload> lookup_by_key(
        const cell_key& ck ) const;
    std::shared_ptr<partition_payload> try_lock_from_found(
        std::shared_ptr<partition_payload>         found_payload,
        const partition_lock_mode&                 lock_mode,
        partition_column_identifier_unordered_set& alread_locked_set ) const;

    partition_column_identifier map_cell_key_to_partition_column_identifier(
        const cell_key& ck ) const;

    table_metadata metadata_;

    folly::ConcurrentHashMap<int64_t,
                             std::shared_ptr<partition_payload_row_holder>>
        rows_to_partitions_;

    folly::ConcurrentHashMap<
        partition_column_identifier, std::shared_ptr<partition_payload>,
        partition_column_identifier_key_hasher, partition_column_identifier_equal_functor>
        partition_map_;
};

class multi_version_partition_data_location_table {
   public:
    multi_version_partition_data_location_table();
    ~multi_version_partition_data_location_table();

    // not thread safe, not sure if it matters
    uint32_t create_table( const table_metadata& metadata );

    std::vector<std::shared_ptr<partition_payload>> get_partitions(
        const std::vector<cell_key_ranges>& cks,
        const partition_lock_mode&          lock_mode ) const;

    std::shared_ptr<partition_payload> get_partition(
        const cell_key& ck, const partition_lock_mode& lock_mode ) const;
    std::shared_ptr<partition_payload> get_partition(
        const partition_column_identifier& pid,
        const partition_lock_mode&         lock_mode ) const;
    std::vector<std::shared_ptr<partition_payload>> get_partition(
        const cell_key_ranges& ckr, const partition_lock_mode& lock_mode ) const;

    std::vector<std::shared_ptr<partition_payload>> get_or_create_partitions(
        const std::vector<cell_key_ranges>& cks,
        const partition_lock_mode&          lock_mode );
    std::shared_ptr<partition_payload> get_or_create_partition(
        const cell_key& ck, const partition_column_identifier& default_pid,
        const partition_lock_mode& lock_mode );
    std::vector<std::shared_ptr<partition_payload>> get_or_create_partition(
        const cell_key_ranges& ckr, const partition_lock_mode& lock_mode );

    grouped_partition_information get_partitions_and_group(
        const std::vector<cell_key_ranges>& sorted_write_cks,
        const std::vector<cell_key_ranges>& sorted_read_cks,
        const partition_lock_mode& lock_mode, int num_sites,
        bool allow_missing ) const;
    grouped_partition_information get_or_create_partitions_and_group(
        const std::vector<cell_key_ranges>& sorted_write_cks,
        const std::vector<cell_key_ranges>& sorted_read_cks,
        const partition_lock_mode& lock_mode, int num_sites );

    std::shared_ptr<partition_payload> remove_partition(
        std::shared_ptr<partition_payload> payload );
    std::shared_ptr<partition_payload> insert_partition(
        std::shared_ptr<partition_payload> payload,
        const partition_lock_mode&         lock_mode );

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

    uint64_t get_approx_total_number_of_partitions() const;

    table_metadata get_table_metadata( uint32_t table_id ) const;
    uint32_t get_num_tables() const;

    void persist_table_location( uint32_t                       table_id,
                                 site_selector_table_persister* persister );

    uint64_t get_default_partition_size_for_table( uint32_t table_id ) const;
    uint32_t get_default_column_size_for_table( uint32_t table_id ) const;

    void build_partition_access_frequencies(
        partition_access_entry_priority_queue& sorted_partition_access_counts,
        partition_column_identifier_unordered_set& partition_accesses ) const;

   private:
    internal_hash_multi_version_partition_payload_table* get_table(
        uint32_t table_id ) const;

    std::vector<internal_hash_multi_version_partition_payload_table*> tables_;
};

