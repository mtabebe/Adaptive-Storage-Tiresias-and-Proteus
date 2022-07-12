#pragma once

#include <vector>

#include "update_enqueuer.h"
#include "update_enqueuer_subscription_information.h"

class update_enqueuers {
   public:
    update_enqueuers();
    ~update_enqueuers();

    void create_table( const table_metadata& metadata );

    void add_enqueuer( std::shared_ptr<update_enqueuer> enqueuer );

    void add_source( const propagation_configuration& config,
                     const partition_column_identifier&      pid,
                     const snapshot_vector& snapshot, bool do_seek,
                     const std::string& cause );
    void add_sources( const propagation_configuration&         config,
                      const std::vector<partition_column_identifier>& pid,
                      const snapshot_vector& snapshot, bool do_seek,
                      const std::string& cause );

    void add_subscriptions_together_with_grouping(
        const std::vector<update_propagation_information>& subscriptions,
        const snapshot_vector& snapshot, const std::string& cause );
    void add_sources_together_with_grouping(
        const std::vector<propagation_configuration>& configs,
        const std::vector<partition_column_identifier>&      pids,
        const snapshot_vector& snapshot, const std::vector<bool>& do_seeks,
        const std::string& cause );

    void mark_upper_bound_of_source( const propagation_configuration& config,
                                     const partition_column_identifier&      pid );
    void mark_lower_bound_of_source(
        std::shared_ptr<partition_column_version_holder> version_holder,
        const partition_column_identifier&               pid );

    void remove_source( const propagation_configuration& config,
                        const partition_column_identifier&      pid,
                        const snapshot_vector&           snapshot,
                        const std::string&               cause );

    void change_sources( const partition_column_identifier&      pid,
                         const propagation_configuration& old_source,
                         const propagation_configuration& new_source,
                         const std::string&               cause );

    void remove_subscription( const partition_column_identifier& pid,
                              const snapshot_vector&      snapshot,
                              const std::string&          cause );

    void set_tables( const tables_metadata& t_metadata, void* tables );
    void set_do_not_subscribe_topics(
        const std::vector<propagation_configuration>& configs );

    void add_interest_in_partition(
        const partition_column_identifier& pid, uint64_t version,
        std::shared_ptr<partition_column_version_holder> version_holder );
    void add_interest_in_partitions(
        const std::vector<partition_column_identifier>& pids,
        const snapshot_vector&                          snapshot,
        const std::vector<std::shared_ptr<partition_column_version_holder>>&
            version_holders );

    void start_enqueuers();
    void stop_enqueuers();

    void gc_split_threads();

   private:
    bool should_add_source( const propagation_configuration& config );

    void add_to_sources(
        const std::unordered_map<
            propagation_configuration, grouped_propagation_config,
            propagation_configuration_ignore_offset_hasher,
            propagation_configuration_ignore_offset_equal_functor>&
                               min_offset_and_pids,
        const snapshot_vector& snapshot, const std::string& cause );
    void add_to_min_offset_and_pids(
        std::unordered_map<
            propagation_configuration, grouped_propagation_config,
            propagation_configuration_ignore_offset_hasher,
            propagation_configuration_ignore_offset_equal_functor>&
                                         min_offset_and_pids,
        const propagation_configuration& config,
        const partition_column_identifier& pid, bool do_seek );

    void build_subscription_offsets(
        std::unordered_map<
            propagation_configuration, int64_t /*offset*/,
            propagation_configuration_ignore_offset_hasher,
            propagation_configuration_ignore_offset_equal_functor>& offsets );

    tables_metadata t_metadata_;

    std::vector<std::shared_ptr<update_enqueuer>>     enqueuers_;

    partition_subscription_bounds pid_bounds_;
    std::vector<partition_column_identifier_folly_concurrent_hash_map_t<
        propagation_configuration>*>
                                             pid_to_configs_;
    update_enqueuer_subscription_information partition_subscription_info_;

    std::unordered_set<propagation_configuration,
                       propagation_configuration_ignore_offset_hasher,
                       propagation_configuration_ignore_offset_equal_functor>
        do_not_subscribe_to_topics_;
};

std::shared_ptr<update_enqueuers> make_update_enqueuers();

