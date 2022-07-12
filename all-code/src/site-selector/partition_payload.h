#pragma once

#include "../common/partition_funcs.h"
#include "../concurrency/spinlock.h"
#include "../data-site/db/op_codes.h"
#include "multi_query_tracking.h"
#include "partition_access.h"
#include "partition_lock_mode.h"
#include "transition_statistics.h"

#include <atomic>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <unordered_set>

class partition_location_information {
   public:
    partition_location_information();

    void set_from_existing_location_information_and_increment_version(
        const std::shared_ptr<partition_location_information>& existing );

    std::tuple<bool, partition_type::type> get_partition_type(
        uint32_t site ) const;
    std::tuple<bool, storage_tier_type::type> get_storage_type(
        uint32_t site ) const;

    uint32_t             master_location_;

    std::unordered_set<uint32_t> replica_locations_;

    std::unordered_map<uint32_t, partition_type::type>    partition_types_;
    std::unordered_map<uint32_t, storage_tier_type::type> storage_types_;

    uint32_t update_destination_slot_;

    uint64_t version_;
    uint64_t session_req_;
    bool     in_flight_;

    uint32_t repartition_op_;
    partition_column_identifier repartitioned_one_;
    partition_column_identifier repartitioned_two_;
};

bool can_merge_partition_location_informations(
    const partition_location_information& pr1,
    const partition_location_information& pr2 );

std::ostream& operator<<( std::ostream& out, const partition_location_information& val );

class partition_payload {
   public:
    partition_payload();
    partition_payload( const partition_column_identifier& id );

    std::shared_ptr<partition_location_information> get_location_information(
        const partition_lock_mode& lock_mode ) const;

    std::shared_ptr<partition_location_information> get_location_information() const;
    std::shared_ptr<partition_location_information> atomic_get_location_information() const;

    // can be called without a lock
    bool compare_and_swap_location_information(
        std::shared_ptr<partition_location_information> new_location_information,
        std::shared_ptr<partition_location_information> expected_location_information );

    // held with lock
    void set_location_information( std::shared_ptr<partition_location_information> new_location_information );

    bool lock( const partition_lock_mode& mode );
    void unlock( const partition_lock_mode& mode );

    void lock();
    bool try_lock();

    void unlock();

    double get_contention() const;
    double get_read_load() const;

    uint32_t get_partition_size() const;

    uint32_t get_num_rows() const;
    uint32_t get_num_columns() const;
    uint32_t get_num_cells() const;

    std::tuple<bool, partition_type::type> get_partition_type(
        uint32_t site ) const;
    std::tuple<bool, storage_tier_type::type> get_storage_type(
        uint32_t site ) const;

    transition_statistics within_rr_txn_statistics_;
    transition_statistics within_rw_txn_statistics_;
    transition_statistics within_wr_txn_statistics_;
    transition_statistics within_ww_txn_statistics_;

    transition_statistics across_rr_txn_statistics_;
    transition_statistics across_rw_txn_statistics_;
    transition_statistics across_wr_txn_statistics_;
    transition_statistics across_ww_txn_statistics_;

    std::atomic<int64_t> sample_based_read_accesses_;
    std::atomic<int64_t> sample_based_write_accesses_;

    std::atomic<int64_t> read_accesses_;
    std::atomic<int64_t> write_accesses_;

    std::shared_ptr<multi_query_partition_entry> multi_query_entry_;

    partition_column_identifier identifier_;

   private:
    std::timed_mutex                               lock_;
    mutable spinlock                               spin_;

    std::shared_ptr<partition_location_information> location_information_;
};

class partition_payload_row_holder {
  public:
   partition_payload_row_holder( uint64_t row );

   std::shared_ptr<partition_payload> get_payload( const cell_key& ck ) const;
   std::shared_ptr<partition_payload> get_or_create_payload(
       const cell_key& ck, std::shared_ptr<partition_payload>& payload );
   std::shared_ptr<partition_payload> get_or_create_payload_and_expand(
       const cell_key& ck, std::shared_ptr<partition_payload>& payload );

   bool remove_payload( std::shared_ptr<partition_payload>& part );
   bool split_payload( std::shared_ptr<partition_payload>& ori,
                       std::shared_ptr<partition_payload>& low,
                       std::shared_ptr<partition_payload>& high );
   bool merge_payload( std::shared_ptr<partition_payload>& low,
                       std::shared_ptr<partition_payload>& high,
                       std::shared_ptr<partition_payload>& merged );

  private:
   bool replace_payload_horizontally(
       std::shared_ptr<partition_payload>& ori,
       std::shared_ptr<partition_payload>& new_part );
   bool split_payload_vertically( std::shared_ptr<partition_payload>& ori,
                                  std::shared_ptr<partition_payload>& low,
                                  std::shared_ptr<partition_payload>& high );

   bool merge_payload_vertically( std::shared_ptr<partition_payload>& low,
                                  std::shared_ptr<partition_payload>& high,
                                  std::shared_ptr<partition_payload>& merged );

   uint64_t row_;

   mutable std::shared_mutex                      lock_;
   std::vector<std::shared_ptr<partition_payload>> column_payloads_;
};

std::tuple<std::shared_ptr<partition_payload>,
           std::shared_ptr<partition_payload>>
    split_partition_payload( std::shared_ptr<partition_payload> payload,
                             uint64_t row_split_point, uint32_t col_split_point,
                             const partition_type::type&    low_type,
                             const partition_type::type&    high_type,
                             const storage_tier_type::type& low_storage_type,
                             const storage_tier_type::type& high_storage_type,
                             uint32_t low_update_destination_slot,
                             uint32_t high_update_destination_slot,
                             const partition_lock_mode& lock_mode );

std::shared_ptr<partition_payload> merge_partition_payloads(
    std::shared_ptr<partition_payload> low_p,
    std::shared_ptr<partition_payload> high_p,
    const partition_type::type&        merge_type,
    const storage_tier_type::type&     merge_storage_type,
    uint32_t update_destination_slot, const partition_lock_mode& lock_mode );

void unlock_payloads(
    std::vector<std::shared_ptr<partition_payload>>& payloads,
    const partition_lock_mode&                       lock_mode );

std::vector<partition_column_identifier> payloads_to_identifiers(
    const std::vector<std::shared_ptr<partition_payload>>& parts );

void add_split_to_partition_payload(
    std::shared_ptr<partition_payload>& partition, uint64_t split_row_id,
    uint32_t split_col_id );
void add_merge_to_partition_payloads(
    std::shared_ptr<partition_payload>& left_partition,
    std::shared_ptr<partition_payload>& right_partition );
void remove_repartition_op_from_partition_payload(
    std::shared_ptr<partition_payload>& partition );

std::ostream& operator<<( std::ostream& out, const partition_payload& val );

class partition_access_entry {
   public:
    partition_access_entry( double                             access_counts,
                            const partition_column_identifier& pid, int site,
                            std::shared_ptr<partition_payload> payload );

    double                             access_counts_;
    partition_column_identifier        pid_;
    int                                site_;
    std::shared_ptr<partition_payload> payload_;
};

struct partition_access_entry_comparator {
    bool operator()( const partition_access_entry& l,
                     const partition_access_entry& r ) {
        // these do the reverse
        return ( l.access_counts_ < r.access_counts_ );
    }
};

typedef std::priority_queue<partition_access_entry,
                            std::vector<partition_access_entry>,
                            partition_access_entry_comparator>
    partition_access_entry_priority_queue;

partition_access_entry build_partition_access_entry(
    std::shared_ptr<partition_payload>& payload );
