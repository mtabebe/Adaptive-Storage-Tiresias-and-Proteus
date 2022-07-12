#pragma once

#include <atomic>
#include <thread>
#include <glog/logging.h>

#include "../common/partition_funcs.h"
#include "../common/thread_utils.h"
#include "client_conn_pool.h"
#include "cost_modeller.h"
#include "partition_data_location_table.h"
#include "partition_tier_tracking.h"
#include "site_partition_version_information.h"
#include "site_selector_metadata.h"
#include "site_selector_query_stats.h"

class periodic_site_selector_operations {
   public:
    periodic_site_selector_operations(
        std::shared_ptr<client_conn_pools> conn_pool,
        std::shared_ptr<cost_modeller2>    cost_model2,
        std::shared_ptr<sites_partition_version_information>
                                                       site_partition_version_info,
        std::shared_ptr<partition_data_location_table> data_loc_tab,
        site_selector_query_stats*                     stats,
        std::shared_ptr<partition_tier_tracking>       tier_tracking,
        std::shared_ptr<query_arrival_predictor>       query_predictor,
        std::vector<std::atomic<int64_t>>*             site_read_counts,
        std::vector<std::atomic<int64_t>>*             site_write_counts,
        uint32_t num_sites, uint32_t client_id,
        const periodic_site_selector_operations_configs& configs );
    ~periodic_site_selector_operations();

    void start_polling();
    void stop_polling();

    propagation_configuration
        get_site_propagation_configuration_by_hashing_partition_column_identifier(
            uint32_t site_id, const partition_column_identifier& pid ) const;
    std::vector<propagation_configuration> get_site_propagation_configurations(
        uint32_t site_id, const std::vector<partition_column_identifier>& pids ) const;

    std::vector<std::vector<propagation_configuration>>
        get_all_site_propagation_configurations() const;

    propagation_configuration get_site_propagation_configuration_by_position(
        uint32_t site_id, uint32_t pos ) const;
    std::vector<propagation_configuration> get_site_propagation_configurations(
        uint32_t site_id, const std::vector<uint32_t>& positions ) const;

    std::vector<site_load_information> get_site_load_information() const;
    std::vector<double>                get_site_loads() const;
    double get_site_load( uint32_t site_id ) const;

    int32_t get_num_sites() const;

   private:
    void run_poller( uint32_t id, uint32_t start_site, uint32_t end_site,
                     bool do_cost_model );
    void poll_site( uint32_t t_id, uint32_t site );
    void build_initial_state();

    void make_load_prediction( uint32_t t_id, uint32_t site_id,
                               const std::vector<context_timer>& timers );

    void wait_for_all_pollers( bool do_cost_model );
    void release_pollers( bool do_cost_model );

    bool check_configs_match( const propagation_configuration& a,
                              const propagation_configuration& b );

    void update_table_stats();

    void update_storage_tier_state(
        const std::vector<storage_tier_change>& storage_changes );

    std::shared_ptr<client_conn_pools> conn_pool_;
    std::shared_ptr<cost_modeller2>    cost_model2_;
    std::shared_ptr<sites_partition_version_information>
        site_partition_version_info_;

    std::shared_ptr<partition_data_location_table> data_loc_tab_;

    site_selector_query_stats*               stats_;
    std::shared_ptr<partition_tier_tracking> tier_tracking_;
    std::shared_ptr<query_arrival_predictor> query_predictor_;

    std::vector<std::atomic<int64_t>>* site_read_counts_;
    std::vector<std::atomic<int64_t>>* site_write_counts_;

    std::vector<int64_t> old_read_counts_;
    std::vector<int64_t> old_write_counts_;

    uint32_t                                  num_sites_;
    uint32_t                                  client_id_;
    periodic_site_selector_operations_configs configs_;

    partition_column_identifier_key_hasher hasher_;

    std::vector<std::vector<propagation_configuration>>
                                       site_propagation_configs_;
    std::vector<std::atomic<int64_t>>  prop_offsets_;

    std::vector<machine_statistics> site_mach_stats_;

    std::vector<std::vector<std::vector<selectivity_stats>>>
                                                            site_selectivities_;
    std::vector<std::vector<std::vector<cell_widths_stats>>> site_col_widths_;

    std::vector<std::unordered_map<int, context_timer>> timers_;

    std::vector<site_load_information> site_load_infos_;

    std::atomic<bool>        shutdown_;
    std::vector<std::thread> polling_threads_;

    uint64_t                 wait_counter_;
    std::mutex               guard_;
    std::condition_variable  cv_;
};
