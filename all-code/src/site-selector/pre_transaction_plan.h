#pragma once

#include <mutex>
#include <queue>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../common/partition_funcs.h"
#include "../gen-cpp/gen-cpp/SiteSelector.h"
#include "cost_modeller_types.h"
#include "multi_query_tracking.h"
#include "pre_action.h"
#include "site_selector_query_stats.h"
#include "stat_tracked_enumerator_holder.h"

class pre_transaction_plan {
   public:
    pre_transaction_plan(
        std::shared_ptr<stat_tracked_enumerator_decision_holder>
            stat_decision_holder );

    void                        build_work_queue();
    std::shared_ptr<pre_action> get_next_pre_action_work_item();
    void add_pre_action_work_item( std::shared_ptr<pre_action> action );

    void add_background_work_item( std::shared_ptr<pre_action> action );
    const std::vector<std::shared_ptr<pre_action>>& get_background_work_items()
        const;

    uint32_t get_num_work_items() const;

    std::shared_ptr<partition_payload> get_partition(
        std::shared_ptr<partition_payload> payload,
        const partition_column_identifier& pid, bool assert_exists = true );
    std::vector<std::shared_ptr<partition_payload>> get_partitions(
        const std::vector<std::shared_ptr<partition_payload>>& payloads,
        const std::vector<partition_column_identifier>&        pids );
    void add_to_partition_mappings(
        const std::vector<std::shared_ptr<partition_payload>>& payloads );
    void add_to_partition_mappings(
        std::shared_ptr<partition_payload> payload );

    void add_to_location_information_mappings(
        partition_column_identifier_map_t<
            std::shared_ptr<partition_location_information>>&&
            partition_location_informations );

    void sort_and_finalize_pids();

    void unlock_payloads_if_able(
        uint32_t                                         completed_id,
        std::vector<std::shared_ptr<partition_payload>>& payloads,
        partition_lock_mode                              mode );
    void unlock_payload_if_able( uint32_t completed_id,
                                 std::shared_ptr<partition_payload> payload,
                                 partition_lock_mode                mode );
    void unlock_remaining_payloads( partition_lock_mode mode );
    void unlock_payloads_because_of_failure( partition_lock_mode mode );
    void unlock_background_partitions();

    void           mark_plan_as_failed();
    action_outcome get_plan_outcome();

    double get_partition_contention( const partition_column_identifier& pid );
    double get_write_set_contention();

    std::vector<transaction_prediction_stats>
        build_and_get_transaction_prediction_stats(
            const std::vector<::cell_key_ranges>& sorted_ckr_write_set,
            const std::vector<::cell_key_ranges>& sorted_ckr_read_set,
            const site_selector_query_stats*      query_stats,
            std::shared_ptr<cost_modeller2>&      cost_model2 );

    int                                        destination_;
    std::vector<::partition_column_identifier> write_pids_;
    std::vector<::partition_column_identifier> read_pids_;
    std::vector<::partition_column_identifier> inflight_pids_;
    snapshot_vector                            svv_;
    double estimated_number_of_updates_to_wait_for_;

    std::vector<transaction_prediction_stats> txn_stats_;

    std::shared_ptr<multi_query_plan_entry> mq_plan_;
    std::shared_ptr<stat_tracked_enumerator_decision_holder>
        stat_decision_holder_;

    double cost_;

   private:
    std::shared_ptr<partition_payload> get_payload(
        const partition_column_identifier& pid, bool assert_exists );
    void add_payload( std::shared_ptr<partition_payload> payload );

    void add_location_information(
        const partition_column_identifier&              pid,
        std::shared_ptr<partition_location_information> location_information );
    std::shared_ptr<partition_location_information> get_location_information(
        const partition_column_identifier& pid );

    void update_transaction_stats(
        const partition_column_identifier& pid, const cell_key_ranges& ckr,
        bool is_write, bool is_read,
        partition_column_identifier_map_t<transaction_prediction_stats>&
                                         pid_to_transaction_pred_stats,
        cached_ss_stat_holder&           cached_stats,
        const site_selector_query_stats* query_stats,
        std::shared_ptr<cost_modeller2>& cost_model2 );

    std::vector<std::shared_ptr<pre_action>> actions_;
    std::queue<uint32_t>                     work_queue_;
    std::vector<std::shared_ptr<pre_action>> background_work_items_;

    std::atomic<action_outcome> plan_outcome_;

    // id1 --> id2 implies that id2 has id1 as a dep
    std::vector<std::unordered_set<uint32_t>> edges_;
    // id --> count, implies that id has count incoming edges
    std::vector<uint32_t> edge_counts_;
    std::queue<uint32_t>  actions_with_no_edges_;

    partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
        partition_column_identifier_to_payload_;
    partition_column_identifier_map_t<int32_t>
        partition_column_identifier_to_unlock_state_;

    std::shared_timed_mutex mapping_mutex_;
    std::mutex              mutex_;
};

class multi_site_pre_transaction_plan {
   public:
    multi_site_pre_transaction_plan(
        std::shared_ptr<stat_tracked_enumerator_decision_holder>
            stat_decision_holder );

    std::unordered_map<int, std::vector<scan_arguments>> scan_arg_sites_;
    std::unordered_map<int, std::vector<partition_column_identifier>>
        read_pids_;
    std::unordered_map<int, std::vector<partition_column_identifier>>
        inflight_pids_;

    std::unordered_map<int, std::vector<std::shared_ptr<partition_payload>>>
        per_site_parts_;
    std::unordered_map<
        int, std::vector<std::shared_ptr<partition_location_information>>>
        per_site_part_locs_;

    std::shared_ptr<pre_transaction_plan> plan_;

    std::unordered_map<int, std::vector<transaction_prediction_stats>>
        txn_stats_;

    std::vector<double> part_costs_;
    std::unordered_map<double, std::vector<std::shared_ptr<partition_payload>>>
        per_part_costs_;
};

