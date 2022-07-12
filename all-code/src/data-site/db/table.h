#pragma once

#include <folly/concurrency/ConcurrentHashMap.h>

#include "partition.h"
#include "table_stats.h"

#include "../../common/hw.h"
#include "../../concurrency/mutex_lock.h"
#include "../../concurrency/semaphore.h"
#include "../../concurrency/spinlock.h"
#if 0  // HDB-OUT
#include "../../persistence/table_persister.h"
#endif

#include "../../common/partition_funcs.h"
#include "../../gen-cpp/gen-cpp/dynamic_mastering_types.h"
#include "../../persistence/data_site_table_persister.h"
#include "../update-propagation/update_destination_generator.h"
#include "../update-propagation/update_enqueuers.h"

class table {
   public:
    table();
    ~table();

    void init( const table_metadata&                         metadata,
               std::shared_ptr<update_destination_generator> update_gen,
               std::shared_ptr<update_enqueuers>             enqueuers );

    void get_partitions(
        const partition_column_identifier_set&     pids,
        partition_column_identifier_partition_map& partitions );
    std::shared_ptr<partition> get_partition_if_active(
        const partition_column_identifier& id ) const;
    std::shared_ptr<partition> get_partition(
        const partition_column_identifier& id ) const;

    std::shared_ptr<partition> add_partition(
        const partition_column_identifier& pid );
    std::shared_ptr<partition> add_partition(
        const partition_column_identifier& pid,
        const partition_type::type&        p_type,
        const storage_tier_type::type&     s_type );
    std::shared_ptr<partition> add_partition(
        const partition_column_identifier& pid,
        const partition_type::type&        p_type,
        const storage_tier_type::type& s_type, bool do_pin, bool do_lock );
    std::shared_ptr<partition> add_partition(
        const partition_column_identifier& pid,
        const partition_type::type&        p_type,
        const storage_tier_type::type& s_type, bool do_pin,
        uint64_t specified_version, bool do_lock,
        std::shared_ptr<update_destination_interface> update_destination );

    bool insert_partition( const partition_column_identifier& pid,
                           std::shared_ptr<partition>         part );
    bool set_partition( const partition_column_identifier& pid,
                        std::shared_ptr<partition>         part );

    std::shared_ptr<partition> get_or_create_partition(
        const partition_column_identifier& pid );
    std::shared_ptr<partition> get_or_create_partition_and_pin(
        const partition_column_identifier& pid );

    void remove_partition( const partition_column_identifier& pid );

    partition* create_partition_without_adding_to_table(
        const partition_column_identifier& pid,
        const partition_type::type&        p_type,
        const storage_tier_type::type& s_type, bool do_pin,
        uint64_t specified_version );
    partition* create_partition_without_adding_to_table(
        const partition_column_identifier& pid,
        const partition_type::type&        p_type,
        const storage_tier_type::type& s_type, bool do_pin,
        uint64_t                                      specified_version,
        std::shared_ptr<update_destination_interface> update_destination );

    split_partition_result split_partition(
        const snapshot_vector&             snapshot,
        const partition_column_identifier& old_pid, uint64_t split_row_point,
        uint32_t split_col_point, const partition_type::type& low_type,
        const partition_type::type&                   high_type,
        const storage_tier_type::type&                low_storage_type,
        const storage_tier_type::type&                high_storage_type,
        std::shared_ptr<update_destination_interface> low_update_destination,
        std::shared_ptr<update_destination_interface> high_update_destination );

    split_partition_result merge_partition(
        const snapshot_vector&                        snapshot,
        const partition_column_identifier&            low_pid,
        const partition_column_identifier&            high_pid,
        const partition_type::type&                   merge_type,
        const storage_tier_type::type&                merge_storage_type,
        std::shared_ptr<update_destination_interface> update_destination );

    const table_metadata& get_metadata() const;

    void stop_subscribing(
        const std::vector<update_propagation_information>& subscriptions,
        const snapshot_vector& snapshot, const std::string& cause );
    void start_subscribing(
        const std::vector<update_propagation_information>& subscriptions,
        const snapshot_vector& snapshot, const std::string& cause );
    void mark_subscription_end(
        const std::vector<update_propagation_information>& subscriptions,
        const snapshot_vector&                             snapshot );

    void switch_subscription(
        const std::vector<update_propagation_information>& subscription_switch,
        const std::string&                                 cause );

    void mark_partition_as_active( const partition_column_identifier& pid,
                                   std::shared_ptr<partition>         part );

    void gc_inactive_partitions();

    void persist_table( data_site_table_persister* persister );

    void get_update_partition_states(
        std::vector<polled_partition_column_version_information>&
            polled_versions );

    uint64_t get_poll_epoch() const;

    uint64_t get_approx_total_number_of_partitions() const;

    std::vector<cell_data_type> get_cell_data_types(
        const partition_column_identifier& pid ) const;
    cell_data_type get_cell_data_type( const cell_identifier& cid ) const;

    table_stats* get_stats() const;

   private:
    void mark_partition_as_inactive_and_add_partition_to_gc(
        const partition_column_identifier& pid,
        std::shared_ptr<partition>         part );
    void pin_and_mark_partition_as_active(
        const partition_column_identifier& pid,
        std::shared_ptr<partition>         part );

    std::shared_ptr<partition> get_or_create_partition_and_pin_helper(
        const partition_column_identifier& pid, bool do_pin );

    void remove_pid_from_part_counters(
        const partition_column_identifier& pid );

    table_metadata metadata_;

    folly::ConcurrentHashMap<partition_column_identifier,
                             std::shared_ptr<partition>,
                             partition_column_identifier_key_hasher,
                             partition_column_identifier_equal_functor>
        partition_map_;
    folly::ConcurrentHashMap<partition_column_identifier,
                             std::shared_ptr<partition>,
                             partition_column_identifier_key_hasher,
                             partition_column_identifier_equal_functor>
        gc_partition_map_;

    folly::ConcurrentHashMap<partition_column_identifier,
                             std::shared_ptr<partition_column_version_holder>,
                             partition_column_identifier_key_hasher,
                             partition_column_identifier_equal_functor>
        part_counters_;

    std::shared_ptr<update_destination_generator> update_gen_;
    std::shared_ptr<update_enqueuers>             enqueuers_;

    std::atomic<uint64_t>        poll_epoch_;
    std::unique_ptr<table_stats> stats_;
};

