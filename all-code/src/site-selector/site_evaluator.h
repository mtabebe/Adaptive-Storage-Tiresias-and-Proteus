#pragma once

#include <memory>
#include <vector>

#include "../common/constants.h"
#include "../common/partition_funcs.h"
#include "../gen-cpp/gen-cpp/SiteSelector.h"
#include "client_conn_pool.h"
#include "cost_modeller.h"
#include "multi_version_partition_table.h"
#include "partition_data_location_table.h"
#include "partition_tier_tracking.h"
#include "pre_transaction_plan.h"
#include "query_arrival_predictor.h"
#include "site_partition_version_information.h"
#include "site_selector_metadata.h"
#include "ss_types.h"
#include "stat_tracked_enumerator_holder.h"

//Forward ref
class adaptive_reservoir_sampler;

// An abstract site_evaluator used to determine where a transaction should run
// For now, only considers the write site.
class abstract_site_evaluator {
   public:
    // Here, there be dragons.
    //
    // This function has tight dependencies on the stored procedures, and
    // changing
    // this function without consideration for the locking in stored procedures
    // is
    // liable to break everything.
    //
    // Ex.: The stored procedures determine whether a transaction can run
    // single-sited, and
    // if so, acquire weaker locks (CMT) under the assumption that the
    // transaction will run
    // there. If this function decides to move stuff anyways, we lose all of
    // consistency
    // guarantees, and we are liable to cause deadlocks in the stored
    // procedures.
    virtual int get_no_change_destination(
        const std::vector<int> &no_change_destinations,
        const std::vector<std::shared_ptr<partition_payload>> &write_partitions,
        const std::vector<std::shared_ptr<partition_payload>> &read_partitions,
        const partition_column_identifier_set &write_partitions_set,
        const partition_column_identifier_set &read_partitions_set,
        ::snapshot_vector &                    svv ) = 0;

    virtual std::shared_ptr<pre_transaction_plan> build_no_change_plan(
        int destination, const grouped_partition_information &grouped_info,
        std::shared_ptr<multi_query_plan_entry> &mq_plan_entry,
        ::snapshot_vector &                      svv ) const = 0;

    virtual std::shared_ptr<multi_site_pre_transaction_plan>
        build_no_change_scan_plan(
            const ::clientid                     id,
            const grouped_partition_information &grouped_info,
            const std::vector<scan_arguments> &  scan_args,
            const std::unordered_map<uint32_t /* label*/,
                                     partition_column_identifier_to_ckrs>
                &labels_to_pids_and_ckrs,
            const partition_column_identifier_map_t<std::unordered_map<
                uint32_t /* label */, std::vector<cell_key_ranges>>>
                &pids_to_labels_and_ckrs,
            const std::unordered_map<uint32_t /* label */, uint32_t /* pos */>
                &                                     label_to_pos,
            const std::vector<site_load_information> &site_load_infos,
            const std::unordered_map<storage_tier_type::type,
                                     std::vector<double>> &storage_sizes,
            std::shared_ptr<multi_query_plan_entry> &      mq_plan_entry,
            ::snapshot_vector &                            svv ) = 0;
    virtual std::shared_ptr<multi_site_pre_transaction_plan>
        generate_scan_plan_from_current_planners(
            const ::clientid                     id,
            const grouped_partition_information &grouped_info,
            const std::vector<scan_arguments> &  scan_args,
            const std::unordered_map<uint32_t /* label*/,
                                     partition_column_identifier_to_ckrs>
                &labels_to_pids_and_ckrs,
            const partition_column_identifier_map_t<std::unordered_map<
                uint32_t /* label */, std::vector<cell_key_ranges>>>
                &pids_to_labels_and_ckrs,
            const std::unordered_map<uint32_t /* label */, uint32_t /* pos */>
                &                                     label_to_pos,
            const std::vector<site_load_information> &site_load_infos,
            const std::unordered_map<storage_tier_type::type,
                                     std::vector<double>> &storage_sizes,
            std::shared_ptr<multi_query_plan_entry> &mq_plan_entry,
            snapshot_vector &                        session ) = 0;

    virtual std::shared_ptr<multi_site_pre_transaction_plan> generate_scan_plan(
        const ::clientid id, const grouped_partition_information &grouped_info,
        const std::vector<scan_arguments> &scan_args,
        const std::unordered_map<uint32_t /* label*/,
                                 partition_column_identifier_to_ckrs>
            &labels_to_pids_and_ckrs,
        const partition_column_identifier_map_t<std::unordered_map<
            uint32_t /* label */, std::vector<cell_key_ranges>>>
            &pids_to_labels_and_ckrs,
        const std::unordered_map<uint32_t /* label */, uint32_t /* pos */>
            &                                     label_to_pos,
        const std::vector<site_load_information> &site_load_infos,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                                    storage_sizes,
        std::shared_ptr<multi_query_plan_entry> &mq_plan_entry,
        snapshot_vector &                        session ) = 0;

    virtual std::shared_ptr<pre_transaction_plan> get_plan_from_current_planners(
        const grouped_partition_information &    grouped_info,
        std::shared_ptr<multi_query_plan_entry> &mq_plan_entry,
        ::snapshot_vector &                      svv ) = 0;

    virtual bool get_should_force_change(
        const ::clientid                      id,
        const std::vector<::cell_key_ranges> &ckr_write_set,
        const std::vector<::cell_key_ranges> &ckr_read_set ) = 0;

    virtual void decay() = 0;

    virtual int generate_insert_only_partition_destination(
        std::vector<std::shared_ptr<partition_payload>>
            &insert_partitions ) = 0;

    virtual uint32_t get_new_partition_update_destination_slot(
        int destination, std::vector<std::shared_ptr<partition_payload>>
                             &insert_partitions ) = 0;
    virtual std::tuple<std::vector<partition_type::type>,
                       std::vector<storage_tier_type::type>>
        get_new_partition_types(
            int destination, std::shared_ptr<pre_transaction_plan> &plan,
            const site_load_information &site_load_info,
            const std::unordered_map<storage_tier_type::type,
                                     std::vector<double>> &storage_sizes,
            std::vector<std::shared_ptr<partition_payload>>
                &insert_partitions ) = 0;

    virtual std::shared_ptr<pre_transaction_plan>
        generate_multi_site_insert_only_plan(
            const ::clientid                          id,
            const std::vector<site_load_information> &site_load_infos,
            const std::unordered_map<storage_tier_type::type,
                                     std::vector<double>> &  storage_sizes,
            std::vector<std::shared_ptr<partition_payload>> &new_partitions,
            bool is_fully_replicated ) = 0;

    virtual std::shared_ptr<pre_transaction_plan> generate_plan(
        const ::clientid id, const std::vector<bool> &site_locations,
        const std::vector<std::shared_ptr<partition_payload>> &new_partitions,
        const std::vector<std::shared_ptr<partition_payload>> &write_partitions,
        const std::vector<std::shared_ptr<partition_payload>> &read_partitions,
        const partition_column_identifier_set &write_partitions_set,
        const partition_column_identifier_set &read_partitions_set,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_to_ckrs &write_pids_to_ckrs,
        const partition_column_identifier_to_ckrs &read_pids_to_ckrs,
        const std::vector<site_load_information> & site_load_infos,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                                    storage_sizes,
        std::shared_ptr<multi_query_plan_entry> &mq_plan_entry,
        const snapshot_vector &                  session ) = 0;

    virtual int      get_num_sites() = 0;
    virtual uint32_t get_num_update_destinations_per_site() = 0;

    virtual void set_samplers(
        std::vector<std::unique_ptr<adaptive_reservoir_sampler>>
            *samplers ) = 0;

    virtual heuristic_site_evaluator_configs get_configs() const = 0;
};

abstract_site_evaluator *construct_site_evaluator(
    const heuristic_site_evaluator_configs &       configs,
    std::shared_ptr<cost_modeller2>                cost_model2,
    std::shared_ptr<partition_data_location_table> data_loc_tab,
    std::shared_ptr<sites_partition_version_information>
                                                    site_partition_version_info,
    std::shared_ptr<query_arrival_predictor>        query_predictor,
    std::shared_ptr<stat_tracked_enumerator_holder> enumerator_holder,
    std::shared_ptr<partition_tier_tracking>        tier_tracking );
#if 0  // HDB-TODO
    std::shared_ptr<client_conn_pools>        client_conns,
    std::shared_ptr<hash_data_location_table> data_tab,
    const ss_strategy_type ss_strategy = k_ss_strategy_type
#endif
