#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "../common/partition_funcs.h"
#include "../gen-cpp/gen-cpp/SiteSelector.h"
#include "partition_payload.h"

class grouped_partition_information {
   public:
    grouped_partition_information( int num_sites );

    void add_partition_information(
        const std::vector<std::shared_ptr<partition_payload>>& payloads,
        const cell_key_ranges& ck, bool is_write,
        const partition_lock_mode& lock_mode );
    void add_partition_information(
        const std::shared_ptr<partition_payload>& payloads,
        const cell_key_ranges& ck, bool is_write,
        const partition_lock_mode& lock_mode );

    void add_to_site_mappings( bool is_write, int location );

    std::vector<int> get_no_change_destinations() const;

    std::vector<std::shared_ptr<partition_payload>> payloads_;
    partition_column_identifier_set                partitions_set_;

    partition_column_identifier_to_ckrs write_pids_to_shaped_ckrs_;
    partition_column_identifier_to_ckrs read_pids_to_shaped_ckrs_;

    partition_column_identifier_map_t<
        std::shared_ptr<partition_location_information>>
        partition_location_informations_;

    std::vector<std::shared_ptr<partition_payload>> new_partitions_;
    std::vector<std::shared_ptr<partition_payload>> existing_write_partitions_;
    std::vector<std::shared_ptr<partition_payload>> existing_read_partitions_;
    std::vector<std::shared_ptr<partition_payload>> new_write_partitions_;
    std::vector<std::shared_ptr<partition_payload>> new_read_partitions_;

    partition_column_identifier_set new_partitions_set_;
    partition_column_identifier_set existing_write_partitions_set_;
    partition_column_identifier_set existing_read_partitions_set_;
    partition_column_identifier_set new_write_partitions_set_;
    partition_column_identifier_set new_read_partitions_set_;

    std::vector<int>  write_site_count_;
    std::vector<int>  read_site_count_;
    std::vector<bool> site_locations_;

    std::vector<partition_column_identifier> inflight_pids_;
    snapshot_vector                          inflight_snapshot_vector_;
};

class per_site_grouped_partition_information {
   public:
    per_site_grouped_partition_information();
    per_site_grouped_partition_information( int site_id );

    int site_;

    std::vector<partition_column_identifier>        write_pids_;
    std::vector<partition_column_identifier>        read_pids_;
    std::vector<std::shared_ptr<partition_payload>> payloads_;
    std::vector<std::shared_ptr<partition_payload>> read_payloads_;
    std::vector<std::shared_ptr<partition_payload>> write_payloads_;
};

