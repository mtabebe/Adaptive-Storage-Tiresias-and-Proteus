#pragma once

#include <condition_variable>
#include <mutex>
#include <thread>

#include <folly/concurrency/ConcurrentHashMap.h>

#include "../../concurrency/thread_holders.h"
#include "partition_subscription_bounds.h"
#include "update_propagation_configs.h"
#include "update_queue.h"
#include "update_source_interface.h"
#include "write_buffer_serialization.h"

class grouped_propagation_config {
   public:
    grouped_propagation_config();

    uint64_t offset_;
    bool     do_seek_;
    std::vector<partition_column_identifier> pids_;
};

class update_enqueuer {
   public:
    update_enqueuer();
    ~update_enqueuer();

    void set_state(
        void* tables, partition_subscription_bounds* pid_bounds,
        update_enqueuer_subscription_information* partition_subscription_info );
    void set_update_consumption_source(
        std::shared_ptr<update_source_interface> update_consumption_source,
        const update_enqueuer_configs&           configs );

    void start_enqueuing();
    void stop_enqueuing();

    bool add_source( const propagation_configuration& config,
                     const partition_column_identifier& pid, bool do_seek,
                     const std::string& cause );
    bool add_sources( const propagation_configuration&         config,
                      const std::vector<partition_column_identifier>& pids,
                      bool do_seek, const std::string& cause );

    std::tuple<bool, int64_t> remove_source(
        const propagation_configuration& config,
        const partition_column_identifier& pid, const std::string& cause );

    void enqueue_stashed_update( stashed_update&& update );

    void begin_enqueue_batch();
    void end_enqueue_batch();

    int gc_split_threads();

    void build_subscription_offsets(
        std::unordered_map<
            propagation_configuration, int64_t /*offset*/,
            propagation_configuration_ignore_offset_hasher,
            propagation_configuration_ignore_offset_equal_functor>& offsets );

   private:
    void sleep_or_yield(
        const std::chrono::high_resolution_clock::time_point& start,
        const std::chrono::high_resolution_clock::time_point& end,
        std::mutex& mut, std::condition_variable& cv );

    // network
    void run_network_enqueuer_thread();
    void enqueue_updates();
    bool create_and_pin_relevant_partitions(
        deserialized_update* deserialized_ptr );

    void start_split_thread( partition_column_identifier pid, uint64_t version );
    void run_split_thread_holder( thread_holder*       t_holder,
                                  partition_column_identifier pid, uint64_t version );
    void run_split_thread( partition_column_identifier pid, uint64_t version );

    // applier
    void run_applier_thread();
    void add_partition_to_apply( const partition_column_identifier& pid,
                                 uint32_t num_updates_to_stash );
    // applier thread
    void apply_updates();
    void apply_update_to_partition( const partition_column_identifier& pid,
                                    uint32_t num_updates_to_apply );

    // shared
    void*                   tables_;
    update_enqueuer_configs configs_;

    // network
    std::unique_ptr<std::thread> network_worker_thread_;
    std::shared_ptr<update_source_interface> update_consumption_source_;
    partition_subscription_bounds*            pid_bounds_;
    update_enqueuer_subscription_information* partition_subscription_info_;

    std::condition_variable      network_cv_;
    std::mutex                   network_mutex_;

    // applier
    std::condition_variable      applier_cv_;
    std::mutex                   applier_mutex_;

    std::unique_ptr<std::thread> applier_worker_thread_;

    partition_column_identifier_map_t<uint32_t>
                                         partitions_to_apply_in_current_nw_batch_;
    partition_column_identifier_map_t<uint32_t> partitions_for_applier_to_apply_;

    thread_holders split_threads_;

    // shared
    bool                         done_;

};
