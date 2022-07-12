#pragma once

#include "../distributions/adaptive_reservoir.h"
#include "../distributions/distributions.h"
#include "adaptive_reservoir_sampler.h"
#include "cost_modeller.h"
#include "site_evaluator.h"
#include "stat_tracked_enumerator_holder.h"

#include "heuristic_site_evaluator_types.h"

class heuristic_site_evaluator : public abstract_site_evaluator {
    enum class partition_adjustment { split, merge };

   public:
    heuristic_site_evaluator(
        const heuristic_site_evaluator_configs &       configs,
        std::shared_ptr<cost_modeller2>                cost_model2,
        std::shared_ptr<partition_data_location_table> data_loc_tab,
        std::shared_ptr<sites_partition_version_information>
                                                        site_partition_version_info,
        std::shared_ptr<query_arrival_predictor>        query_predictor,
        std::shared_ptr<stat_tracked_enumerator_holder> enumerator_holder,
        std::shared_ptr<partition_tier_tracking>        tier_tracking );
    ~heuristic_site_evaluator();

    heuristic_site_evaluator_configs get_configs() const;
    bool                             get_should_force_change(
        const ::clientid                      id,
        const std::vector<::cell_key_ranges> &ckr_write_set,
        const std::vector<::cell_key_ranges> &ckr_read_set );

    void decay();

    void set_samplers(
        std::vector<std::unique_ptr<adaptive_reservoir_sampler>> *samplers );

    std::shared_ptr<pre_transaction_plan> build_no_change_plan(
        int destination, const grouped_partition_information &grouped_info,
        std::shared_ptr<multi_query_plan_entry> &mq_plan_entry,
        ::snapshot_vector &                      svv ) const;

    std::shared_ptr<multi_site_pre_transaction_plan> build_no_change_scan_plan(
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
        ::snapshot_vector &                      svv );

    std::shared_ptr<multi_site_pre_transaction_plan>
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
            std::shared_ptr<multi_query_plan_entry> &      mq_plan_entry,
            snapshot_vector &                              session );

    int get_no_change_destination(
        const std::vector<int> &no_change_destinations,
        const std::vector<std::shared_ptr<partition_payload>> &write_partitions,
        const std::vector<std::shared_ptr<partition_payload>> &read_partitions,
        const partition_column_identifier_set &write_partitions_set,
        const partition_column_identifier_set &read_partitions_set,
        ::snapshot_vector &                    svv );

    std::shared_ptr<pre_transaction_plan> get_plan_from_current_planners(
        const grouped_partition_information &    grouped_info,
        std::shared_ptr<multi_query_plan_entry> &mq_plan_entry,
        ::snapshot_vector &                      svv );

    int generate_insert_only_partition_destination(
        std::vector<std::shared_ptr<partition_payload>> &insert_partitions );

    uint32_t get_new_partition_update_destination_slot(
        int                                              destination,
        std::vector<std::shared_ptr<partition_payload>> &insert_partitions );
    std::tuple<std::vector<partition_type::type>,
               std::vector<storage_tier_type::type>>
        get_new_partition_types(
            int destination, std::shared_ptr<pre_transaction_plan> &plan,
            const site_load_information &site_load_info,
            const std::unordered_map<storage_tier_type::type,
                                     std::vector<double>> &storage_sizes,
            std::vector<std::shared_ptr<partition_payload>>
                &insert_partitions );

    std::shared_ptr<pre_transaction_plan> generate_multi_site_insert_only_plan(
        const ::clientid                          id,
        const std::vector<site_load_information> &site_load_infos,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &storage_sizes,
        std::vector<std::shared_ptr<partition_payload>> &new_partitions,
        bool                                             is_fully_replicated );

    std::shared_ptr<pre_transaction_plan> generate_plan(
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
        const snapshot_vector &                  session );

    std::shared_ptr<multi_site_pre_transaction_plan> generate_scan_plan(
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
        snapshot_vector &                        session );

    int      get_num_sites();
    uint32_t get_num_update_destinations_per_site();

    std::shared_ptr<pre_transaction_plan> generate_plan_for_site(
        int site, const ::clientid id,
        const std::vector<std::shared_ptr<partition_payload>> &new_partitions,
        const std::vector<std::shared_ptr<partition_payload>> &write_partitions,
        const std::vector<std::shared_ptr<partition_payload>> &read_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const std::vector<shared_split_plan_state> &partitions_to_split,
        const std::vector<shared_left_right_merge_plan_state>
            &                                      partitions_to_merge,
        double                                     avg_write_count,
        const partition_column_identifier_to_ckrs &write_pids_to_ckrs,
        const partition_column_identifier_to_ckrs &read_pids_to_ckrs,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        double                                    average_load_per_site,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double storage_size_cost, const snapshot_vector &session,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    std::shared_ptr<pre_transaction_plan> generate_plan_for_site(
        int site, const ::clientid id, plan_for_site_args &args );

    plan_for_site_args generate_plan_for_site_args(
        const ::clientid                                       id,
        const std::vector<std::shared_ptr<partition_payload>> &new_partitions,
        const std::vector<std::shared_ptr<partition_payload>> &write_partitions,
        const std::vector<std::shared_ptr<partition_payload>> &read_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_to_ckrs &write_pids_to_ckrs,
        const partition_column_identifier_to_ckrs &read_pids_to_ckrs,
        const std::vector<site_load_information> & site_load_infos,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                  storage_sizes,
        const snapshot_vector &session );

    void split_partitions_and_add_to_plan(
        int site, const std::vector<shared_split_plan_state> &split_states,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &write_partitions_map,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &                                  read_partitions_map,
        std::shared_ptr<pre_transaction_plan> &plan,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &                                pid_dep_map,
        partition_column_identifier_to_ckrs &write_modifiable_pids_to_ckrs,
        partition_column_identifier_to_ckrs &read_modifiable_pids_to_ckrs,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                                      storage_sizes,
        partition_column_identifier_unordered_set &no_merge_partitions );

    void merge_partitions_and_add_to_plan(
        int site, const std::vector<shared_left_right_merge_plan_state>
                      &partitions_to_merge,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &write_partitions_map,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &                                  read_partitions_map,
        std::shared_ptr<pre_transaction_plan> &plan,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                                storage_sizes,
        partition_column_identifier_to_ckrs &write_modifiable_pids_to_ckrs,
        partition_column_identifier_to_ckrs &read_modifiable_pids_to_ckrs,
        partition_column_identifier_unordered_set &no_merge_partitions,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &    other_write_locked_partitions,
        uint64_t avg_write_count );

    void change_partition_types(
        int site, partition_column_identifier_map_t<
                      std::shared_ptr<partition_location_information>>
                      &partition_location_informations,
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_payload>> &write_partitions_map,
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_payload>> &read_partitions_map,
        std::shared_ptr<pre_transaction_plan> &  plan,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &storage_sizes,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    std::tuple<partition_type::type, storage_tier_type::type> get_change_type(
        const std::shared_ptr<partition_payload> &part,
        const std::shared_ptr<partition_location_information>
            &                          part_location_info,
        const partition_type::type &   cur_type,
        const storage_tier_type::type &storage_type, uint32_t site,
        std::shared_ptr<pre_transaction_plan> &plan,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &storage_sizes,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats, bool is_write );

    void add_all_replicas(
        int site, partition_column_identifier_map_t<
                      std::shared_ptr<partition_location_information>>
                      &partition_location_informations,
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_payload>> &write_partitions_map,
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_payload>> &        read_partitions_map,
        std::shared_ptr<pre_transaction_plan> &          plan,
        std::vector<std::shared_ptr<partition_payload>> &write_part_payloads,
        std::vector<std::shared_ptr<partition_payload>> &read_part_payloads,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &storage_sizes,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    void add_necessary_replicas(
        int site, partition_column_identifier_map_t<
                      std::shared_ptr<partition_location_information>>
                      &partition_location_informations,
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_payload>> &        partitions_map,
        std::shared_ptr<pre_transaction_plan> &          plan,
        std::vector<partition_column_identifier> &       plan_pids,
        std::vector<std::shared_ptr<partition_payload>> &part_list,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &                                 other_write_locked_partitions,
        master_destination_partition_entries &to_add_replicas,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &storage_sizes,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        partition_column_identifier_unordered_set &       already_seen,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    std::tuple<partition_type::type, storage_tier_type::type> get_replica_type(
        const std::shared_ptr<partition_payload> &part,
        const std::shared_ptr<partition_location_information>
            &    part_location_info,
        uint32_t master, uint32_t site,
        std::shared_ptr<pre_transaction_plan> plan,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &storage_sizes,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    void add_necessary_remastering(
        int site, partition_column_identifier_map_t<
                      std::shared_ptr<partition_location_information>>
                      &partition_location_informations,
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_payload>> &write_partitions_map,
        std::shared_ptr<pre_transaction_plan> &  plan,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        const snapshot_vector &session, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    void tack_on_beneficial_spin_ups(
        uint32_t site, const ::clientid id,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_payload>> &write_partitions_map,
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_payload>> &read_partitions_map,
        std::shared_ptr<pre_transaction_plan>    plan,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::unordered_set<uint32_t> &      candidate_sites,
        const std::vector<site_load_information> &site_loads,
        double                                    average_load_per_site,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &storage_sizes,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

   private:
    bool should_add_multi_query_plan_to_partitions( const ::clientid id );

    std::unordered_set<int> generate_plan_reuse_sites(
        const std::vector<std::shared_ptr<partition_payload>> &write_partitions,
        const std::vector<std::shared_ptr<partition_payload>> &read_partitions,
        const partition_column_identifier_set &write_partitions_set,
        const partition_column_identifier_set &read_partitions_set ) const;

    void change_partition_types_as_needed(
        int site, partition_column_identifier_map_t<
                      std::shared_ptr<partition_location_information>>
                      &partition_location_informations,
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_payload>> &partitions_map,
        std::shared_ptr<pre_transaction_plan> &  plan,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &                                 other_write_locked_partitions,
        master_destination_partition_entries &to_add_replicas,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &storage_sizes,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        partition_column_identifier_unordered_set &       already_seen,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats, bool is_write );
    void add_change_type_to_plan(
        const partition_column_identifier &pid,
        std::shared_ptr<partition_payload> payload,
        std::shared_ptr<partition_location_information>
                                              part_location_information,
        const partition_type::type &          p_type,
        const storage_tier_type::type &       s_type,
        std::shared_ptr<pre_transaction_plan> plan, int destination,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &original_read_partition_set,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const std::vector<site_load_information> &site_loads,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    void add_change_types_to_plan(
        const std::vector<partition_column_identifier> & pids_to_change,
        std::vector<std::shared_ptr<partition_payload>> &parts_to_change,
        std::vector<std::shared_ptr<partition_location_information>>
            &                                       parts_to_change_infos,
        const std::vector<partition_type::type> &   types_to_change,
        const std::vector<storage_tier_type::type> &storage_to_change,
        std::shared_ptr<pre_transaction_plan> plan, int destination,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &original_read_partition_set,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const std::vector<site_load_information> &site_loads,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    double get_expected_benefit_of_changing_types(
        std::vector<std::shared_ptr<partition_payload>> &parts_to_change,
        const std::vector<std::shared_ptr<partition_location_information>>
            &                                       parts_to_change_locations,
        const std::vector<partition_type::type> &   types_to_change,
        const std::vector<storage_tier_type::type> &storage_to_change,
        const std::vector<partition_type::type> &   old_types,
        const std::vector<storage_tier_type::type> &old_storage,
        const std::vector<change_types_stats> &change_stats, int destination,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    double get_sample_benefit_of_changing_types(
        const std::shared_ptr<partition_payload> &part_to_change,
        const std::shared_ptr<partition_location_information>
            &                     part_to_change_location,
        const change_types_stats &change_stats, int destination,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    std::tuple<double, double> get_sample_score_of_changing_types(
        const std::shared_ptr<partition_payload> &part_to_change,
        const change_types_stats &change_stat, int destination,
        const std::vector<site_load_information> &site_loads,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    void get_partitions_to_adjust(
        const std::vector<std::shared_ptr<partition_payload>>
            &  all_write_partitions,
        double avg_write_count, double std_dev_write_count,
        std::vector<shared_split_plan_state> &           split_partitions,
        std::vector<shared_left_right_merge_plan_state> &merge_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &sampled_partition_accesses_index_by_write_partition,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &sampled_partition_accesses_index_by_read_partition,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    std::tuple<bool, shared_split_plan_state> should_split_partition(
        const std::shared_ptr<partition_payload> &      partition,
        std::shared_ptr<partition_location_information> location_information,
        double avg_write_count, double std_dev_write_count,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &sampled_partition_accesses_index_by_write_partition,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &sampled_partition_accesses_index_by_read_partition,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance,

        cached_ss_stat_holder &     cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    std::tuple<bool, shared_left_right_merge_plan_state> should_merge_partition(
        const std::shared_ptr<partition_payload> &partition,
        const std::shared_ptr<partition_location_information>
            &  location_information,
        double avg_write_count, double std_dev_write_count,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    bool within_merge_ratio( std::shared_ptr<partition_payload> partition,
                             double avg_write_count );

    shared_left_right_merge_plan_state merge_partition_if_beneficial(
        std::shared_ptr<partition_payload>              partition_to_merge,
        std::shared_ptr<partition_location_information> location_information,
        double avg_write_count, double std_dev_write_count,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    shared_merge_plan_state merge_partition_left_row_if_beneficial(
        std::shared_ptr<partition_payload> partition_to_merge,
        std::shared_ptr<partition_location_information>
               to_merge_location_information,
        double avg_write_count, double std_dev_write_count,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    shared_merge_plan_state merge_partition_right_row_if_beneficial(
        std::shared_ptr<partition_payload> partition_to_merge,
        std::shared_ptr<partition_location_information>
               to_merge_location_information,
        double avg_write_count, double std_dev_write_count,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    shared_merge_plan_state merge_partition_left_col_if_beneficial(
        std::shared_ptr<partition_payload> partition_to_merge,
        std::shared_ptr<partition_location_information>
               to_merge_location_information,
        double avg_write_count, double std_dev_write_count,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    shared_merge_plan_state merge_partition_right_col_if_beneficial(
        std::shared_ptr<partition_payload> partition_to_merge,
        std::shared_ptr<partition_location_information>
               to_merge_location_information,
        double avg_write_count, double std_dev_write_count,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    shared_merge_plan_state merge_partition_if_beneficial_with_ck(
        std::shared_ptr<partition_payload> partition_to_merge,
        std::shared_ptr<partition_location_information>
               to_merge_location_information,
        double avg_write_count, double std_dev_write_count,
        const ::cell_key other_ck, bool is_vertical_merge, bool ori_is_left,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    void get_merge_cost(
        shared_merge_plan_state &merge_res, const merge_stats &merge_stat,
        const partition_column_identifier &left_pid,
        const partition_column_identifier &right_pid, double site_load,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        double left_storage_size, double right_storage_size,
        double ori_storage_imbalance,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &    storage_sizes,
        uint32_t master_site, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    double get_merge_benefit_from_samples(
        const partition_column_identifier &low_pid,
        const partition_column_identifier &high_pid,
        const merge_stats &                merge_stat,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    double get_default_expected_merge_benefit(
        const partition_column_identifier &merged_pid,
        const partition_column_identifier &low_pid,
        const partition_column_identifier &high_pid,
        const merge_stats &merge_stat, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    double get_expected_merge_benefit(
        const partition_column_identifier &left_pid,
        const partition_column_identifier &right_pid,
        const merge_stats &                merge_stat,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    bool add_split_to_plan_if_possible(
        int site, const shared_split_plan_state &shared_split_res,
        const split_plan_state &split_res,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &write_partitions_map,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &                                  read_partitions_map,
        std::shared_ptr<pre_transaction_plan> &plan,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &                                pid_dep_map,
        partition_column_identifier_to_ckrs &write_modifiable_pids_to_ckrs,
        partition_column_identifier_to_ckrs &read_modifiable_pids_to_ckrs,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                                      storage_sizes,
        partition_column_identifier_unordered_set &no_merge_partitions );

    void update_partition_maps_from_split(
        std::shared_ptr<partition_payload> &partition_to_split,
        const partition_column_identifier & low_pid,
        const partition_column_identifier & high_pid,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &                                partitions_map,
        partition_column_identifier_to_ckrs &modifiable_pids_to_ckrs ) const;

    void split_partition_if_beneficial(
        split_plan_state &                              split_res,
        std::shared_ptr<partition_payload>              partition_to_split,
        std::shared_ptr<partition_location_information> location_information,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &sampled_partition_accesses_index_by_write_partition,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &  sampled_partition_accesses_index_by_read_partition,
        double avg_write_count, double std_dev_write_count,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    split_plan_state split_partition_vertically_if_beneficial(
        std::shared_ptr<partition_payload>              partition_to_split,
        std::shared_ptr<partition_location_information> location_information,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &sampled_partition_accesses_index_by_write_partition,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &  sampled_partition_accesses_index_by_read_partition,
        double avg_write_count, double std_dev_write_count,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    split_plan_state split_partition_horizontally_if_beneficial(
        std::shared_ptr<partition_payload>              partition_to_split,
        std::shared_ptr<partition_location_information> location_information,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &sampled_partition_accesses_index_by_write_partition,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &  sampled_partition_accesses_index_by_read_partition,
        double avg_write_count, double std_dev_write_count,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &  storage_sizes,
        double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    cell_key get_col_split_point(
        std::shared_ptr<partition_payload> partition_to_split );
    cell_key get_row_split_point(
        std::shared_ptr<partition_payload> partition_to_split );

    double get_expected_split_benefit(
        const partition_column_identifier &partition_to_split,
        const partition_column_identifier &low_pid,
        const partition_column_identifier &high_pid,
        const split_stats &split_stat, const split_plan_state &split_state,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    double get_split_benefit_from_samples(
        const partition_column_identifier &probe_pid,
        const partition_column_identifier &low_pid,
        const partition_column_identifier &high_pid,
        const split_stats &split_stat, double split_ratio,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    void get_split_cost(
        split_plan_state &split_res, const split_stats &split_stat,
        const partition_column_identifier &ori_pid,
        const partition_column_identifier &low_pid,
        const partition_column_identifier &high_pid, double site_load,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &sampled_partition_accesses_index_by_write_partition,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &  sampled_partition_accesses_index_by_read_partition,
        double left_storage_size, double right_storage_size,
        double ori_storage_imbalance,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &    storage_sizes,
        uint32_t site, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    double get_default_expected_split_benefit(
        const partition_column_identifier &ori_pid,
        const partition_column_identifier &low_pid,
        const partition_column_identifier &high_pid,
        const split_stats &split_stat, double split_ratio,
        cached_ss_stat_holder &     cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    partitioning_cost_benefit get_expected_amount_of_remastering(
        const partition_type::type &merge_type,
        const partition_type::type &low_type,
        const partition_type::type &high_type, double low_contention,
        double high_contention, uint64_t low_part_size,
        uint64_t high_part_size );
    double get_expected_amount_of_remastering_from_merge(
        const merge_stats &merge_stat );
    double get_expected_amount_of_remastering_from_split(
        const split_stats &split_stat, bool is_vertical );

    partitioning_cost_benefit get_partitioning_benefit_from_samples(
        const partition_column_identifier &probe_pid,
        const partition_column_identifier &combined_pid,
        const partition_column_identifier &low_pid,
        const partition_column_identifier &high_pid,
        const partition_type::type &       combined_type,
        const partition_type::type &       low_type,
        const partition_type::type &       high_type,
        const storage_tier_type::type &    combined_storage_type,
        const storage_tier_type::type &    low_storage_type,
        const storage_tier_type::type &high_storage_type, double low_cont,
        double high_cont,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    partitioning_cost_benefit get_default_expected_partitioning_benefit(
        const partition_column_identifier &combined_pid,
        const partition_column_identifier &low_pid,
        const partition_column_identifier &high_pid,
        const partition_type::type &       combined_type,
        const partition_type::type &       low_type,
        const partition_type::type &       high_type,
        const storage_tier_type::type &    combined_storage_type,
        const storage_tier_type::type &    low_storage_type,
        const storage_tier_type::type &high_storage_type, double low_cont,
        double low_prob, double high_cont, double high_prob, bool is_merge,
        cached_ss_stat_holder &     cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    bool can_merge_partition_identifiers(
        const partition_column_identifier &a,
        const partition_column_identifier &b ) const;

    bool eligible_for_merging(
        std::shared_ptr<partition_payload>               partition,
        const partition_column_identifier_unordered_set &no_merge_partitions );

    bool merge_if_possible(
        int site, std::shared_ptr<partition_payload> partition_to_merge,
        std::shared_ptr<partition_location_information>
                                       to_merge_location_information,
        const shared_merge_plan_state &merge_plan_state,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &write_partitions_map,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &                                  read_partitions_map,
        std::shared_ptr<pre_transaction_plan> &plan,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                                storage_sizes,
        partition_column_identifier_to_ckrs &write_modifiable_pids_to_ckrs,
        partition_column_identifier_to_ckrs &read_modifiable_pids_to_ckrs,
        partition_column_identifier_unordered_set &no_merge_partitions,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions );

    void add_merge_to_plan(
        int site, std::shared_ptr<partition_payload> partition_to_merge,
        std::shared_ptr<partition_payload>              partition_to_consider,
        std::shared_ptr<partition_location_information> location_information,
        const partition_type::type &                    merge_type,
        const storage_tier_type::type &merge_storage_type, double cost,
        const cost_model_prediction_holder &model_holder,
        const merge_stats &                 merge_stat,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &write_partitions_map,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &                                  read_partitions_map,
        std::shared_ptr<pre_transaction_plan> &plan,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                                storage_sizes,
        partition_column_identifier_to_ckrs &write_modifiable_pids_to_ckrs,
        partition_column_identifier_to_ckrs &read_modifiable_pids_to_ckrs,
        partition_column_identifier_unordered_set &no_merge_partitions );
    void update_partition_maps_from_merge(
        std::shared_ptr<partition_payload> &partition_to_merge,
        const partition_column_identifier & merged_pid,
        const partition_column_identifier & left_pid,
        const partition_column_identifier & right_pid,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
                                             partitions_map,
        partition_column_identifier_to_ckrs &modifiable_pids_to_ckrs ) const;

    void add_replica_to_plan(
        const partition_column_identifier &pid,
        std::shared_ptr<partition_payload> payload,
        std::shared_ptr<partition_location_information>
                                              part_location_information,
        const partition_type::type &          p_type,
        const storage_tier_type::type &       s_type,
        std::shared_ptr<pre_transaction_plan> plan, int destination,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &original_read_partition_set,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const std::vector<site_load_information> &site_loads,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    void add_replicas_to_plan(
        const std::vector<partition_column_identifier> & pids_to_replicate,
        std::vector<std::shared_ptr<partition_payload>> &parts_to_replicate,
        std::vector<std::shared_ptr<partition_location_information>>
            &                                       parts_to_replicate_infos,
        const std::vector<partition_type::type> &   types_to_replicate,
        const std::vector<storage_tier_type::type> &storage_types_to_replicate,
        std::shared_ptr<pre_transaction_plan> plan, int master, int destination,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &original_read_partition_set,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const std::vector<site_load_information> &site_loads,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    void add_remastering_to_plan(
        const partition_column_identifier &pid,
        std::shared_ptr<partition_payload> payload,
        std::shared_ptr<partition_location_information>
                                              part_location_information,
        std::shared_ptr<pre_transaction_plan> plan, int destination,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        const snapshot_vector &session, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    void add_remasters_to_plan(
        const std::vector<partition_column_identifier> & pids_to_remaster,
        std::vector<std::shared_ptr<partition_payload>> &parts_to_remaster,
        std::vector<std::shared_ptr<partition_location_information>>
            &                                 parts_to_remaster_infos,
        std::shared_ptr<pre_transaction_plan> plan, int master, int destination,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &original_read_partition_set,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const std::vector<site_load_information> &site_loads,
        const snapshot_vector &session, cached_ss_stat_holder &cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    double compute_remastering_op_score(
        const partition_column_identifier & pid,
        std::shared_ptr<partition_payload> &payload,
        const std::shared_ptr<partition_location_information>
            &  part_location_information,
        double part_contention, int master, int destination,
        const partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        const std::vector<site_load_information> &new_site_loads,
        cached_ss_stat_holder &                   cached_stats,
        query_arrival_cached_stats &              cached_query_arrival_stats );

    template <double( get_change_in_cost )(
        const std::shared_ptr<cost_modeller2> &                cost_model,
        const std::shared_ptr<partition_data_location_table> & data_loc_tab,
        const partition_column_identifier &                    pid,
        const std::shared_ptr<partition_location_information> &part,
        const std::shared_ptr<partition_location_information>
            &  correlated_write_part,
        double score_adjustment, int master, int destination,
        const partition_type::type &   repl_type,
        const storage_tier_type::type &repl_storage_type,
        bool also_accessing_other_part, double part_contention,
        double                                    other_part_contention,
        const std::vector<site_load_information> &site_loads,
        const std::vector<site_load_information> &new_site_loads,
        cached_ss_stat_holder &                   cached_stats,
        query_arrival_cached_stats &              cached_query_arrival_stats )>
    double compute_correlation_score(
        const partition_column_identifier &pid,
        const std::shared_ptr<partition_location_information>
            &  part_location_information,
        double part_contention, int total_partition_accesses,
        transition_statistics &stats, int master, int destination,
        const partition_type::type &   repl_type,
        const storage_tier_type::type &repl_storage_type,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set &original_partition_set,
        const std::vector<site_load_information> &       site_loads,
        const std::vector<site_load_information> &       new_site_loads,
        cached_ss_stat_holder &                          cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    void tack_on_beneficial_spin_ups_for_sample(
        tracking_summary &                  summary,
        const std::unordered_set<uint32_t> &candidate_sites,
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_payload>> &read_partitions_map,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        std::unordered_map<uint32_t, partition_column_identifier_unordered_set>
            &                                     spinup_replica_sets,
        const std::vector<site_load_information> &site_loads,
        double expected_load_balance_per_site );
    void build_runnable_locations_information(
        bool part_of_read_set, std::vector<uint32_t> &all_parts_site_counts,
        uint32_t *             all_part_counts,
        std::vector<uint32_t> &filtered_parts_site_counts,
        uint32_t *             filtered_part_counts,
        std::vector<partition_column_identifier_unordered_set>
            &                              read_partitions_missing_at_site,
        const partition_column_identifier &access_pid,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        uint32_t                              num_sites );

    std::unordered_set<uint32_t> get_involved_sites(
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_payload>> &write_partitions_map,
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_payload>> &read_partitions_map,
        const partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        uint32_t                              site );

    void add_spinup_replicas_to_plan(
        std::shared_ptr<pre_transaction_plan> plan,
        std::unordered_map<uint32_t, partition_column_identifier_unordered_set>
            &spinup_replica_sets,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_payload>> &read_partitions_map,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &storage_sizes,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    double get_expected_benefit_of_remastering(
        std::vector<std::shared_ptr<partition_payload>> &parts_to_remaster,
        std::vector<std::shared_ptr<partition_location_information>>
            &parts_to_remaster_locations,
        int master, int destination,
        const partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        cached_ss_stat_holder &                   cached_stats,
        query_arrival_cached_stats &              cached_query_arrival_stats );

    double get_sample_benefit_of_remastering(
        std::vector<std::shared_ptr<partition_payload>> &parts_to_remaster,
        const std::vector<std::shared_ptr<partition_location_information>>
            &parts_to_remaster_location,
        int master, int destination,
        const partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        const std::vector<site_load_information> &new_site_loads,
        cached_ss_stat_holder &                   cached_stats,
        query_arrival_cached_stats &              cached_query_arrival_stats );

    double get_sample_benefit_of_removing_replica(
        std::vector<std::shared_ptr<partition_payload>> &parts_to_remove,
        const std::vector<std::shared_ptr<partition_location_information>>
            &                                    parts_to_remove_location,
        const std::vector<partition_type::type> &types_to_remove,
        int                                      destination,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        const std::vector<site_load_information> &new_site_loads,
        cached_ss_stat_holder &                   cached_stats,
        query_arrival_cached_stats &              cached_query_arrival_stats );

    double get_expected_benefit_of_adding_replicas(
        std::vector<std::shared_ptr<partition_payload>> &parts_to_replicate,
        const std::vector<std::shared_ptr<partition_location_information>>
            &                                    parts_to_replicate_locations,
        const std::vector<partition_type::type> &types_to_replicate,
        const std::vector<storage_tier_type::type> &storage_types_to_replicate,
        int master, int destination,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    double compute_add_replica_op_score(
        const partition_column_identifier & pid,
        std::shared_ptr<partition_payload> &payload,
        const std::shared_ptr<partition_location_information>
            &  part_location_information,
        double part_contention, int master, int destination,
        const partition_type::type &   repl_type,
        const storage_tier_type::type &repl_storage_type,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        const std::vector<site_load_information> &new_site_loads,
        cached_ss_stat_holder &                   cached_stats,
        query_arrival_cached_stats &              cached_query_arrival_stats );
    double compute_remove_replica_op_score(
        const partition_column_identifier & pid,
        std::shared_ptr<partition_payload> &payload,
        const std::shared_ptr<partition_location_information>
            &  part_location_information,
        double part_contention, int master, int destination,
        const partition_type::type &repl_type,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        const std::vector<site_load_information> &new_site_loads,
        cached_ss_stat_holder &                   cached_stats,
        query_arrival_cached_stats &              cached_query_arrival_stats );

    double get_sample_benefit_of_adding_replica(
        std::vector<std::shared_ptr<partition_payload>> &parts_to_replicate,
        const std::vector<std::shared_ptr<partition_location_information>>
            &                                       parts_to_replicate_location,
        const std::vector<partition_type::type> &   types_to_replicate,
        const std::vector<storage_tier_type::type> &storage_types_to_replicate,
        int master, int destination,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        const std::vector<site_load_information> &new_site_loads,
        cached_ss_stat_holder &                   cached_stats,
        query_arrival_cached_stats &              cached_query_arrival_stats );

    double get_expected_benefit_of_removing_replicas(
        std::vector<std::shared_ptr<partition_payload>> &parts_to_remove,
        const std::vector<std::shared_ptr<partition_location_information>>
            &                                    parts_to_remove_locations,
        const std::vector<partition_type::type> &types_to_remove,
        int                                      destination,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        cached_ss_stat_holder &                   cached_stats,
        query_arrival_cached_stats &              cached_query_arrival_stats );

    void remove_un_needed_replicas_for_sample(
        tracking_summary &                  summary,
        const std::unordered_set<uint32_t> &candidate_sites,
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_payload>> &read_partitions_map,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        std::unordered_map<uint32_t, partition_column_identifier_unordered_set>
            &                                     remove_replica_sets,
        const std::vector<site_load_information> &site_loads,
        double                                    average_load_per_site,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &storage_sizes );
    void remove_replicas_or_change_types_for_capacity(
        uint32_t site, const storage_tier_type::type &tier,
        std::unordered_map<uint32_t, partition_column_identifier_unordered_set>
            &                                 remove_replica_sets,
        master_destination_partition_entries &to_change_types,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_payload>> &write_partitions_map,
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_payload>> &read_partitions_map,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &storage_sizes,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );

    void add_remove_replicas_to_plan(
        std::shared_ptr<pre_transaction_plan> plan,
        std::unordered_map<uint32_t, partition_column_identifier_unordered_set>
            &remove_replica_sets,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_payload>> &read_partitions_map,
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_payload>> &write_partitions_map,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::vector<site_load_information> &site_loads,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                       storage_sizes,
        cached_ss_stat_holder &     cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    void remove_un_needed_replicas(
        uint32_t site, const ::clientid id,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_payload>> &write_partitions_map,
        const partition_column_identifier_map_t<
            std::shared_ptr<partition_payload>> &read_partitions_map,
        std::shared_ptr<pre_transaction_plan>    plan,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &                                     original_read_partition_set,
        const std::unordered_set<uint32_t> &      candidate_sites,
        const std::vector<site_load_information> &site_loads,
        double                                    average_load_per_site,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &storage_sizes,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        cached_ss_stat_holder &                           cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    void build_remove_replica_runnable_locations_information(
        bool part_of_read_set, std::vector<uint32_t> &all_parts_site_counts,
        uint32_t *             all_part_counts,
        std::vector<uint32_t> &filtered_parts_site_counts,
        uint32_t *             filtered_part_counts,
        std::vector<partition_column_identifier_unordered_set>
            &                              read_partitions_at_site,
        const partition_column_identifier &access_pid,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        uint32_t                              num_sites );

    std::vector<site_load_information> get_new_site_load_for_adding_replicas(
        const std::vector<std::shared_ptr<partition_payload>>
            &parts_to_replicate,
        const std::vector<std::shared_ptr<partition_location_information>>
            &parts_to_replicate_locations,
        int master, int destination,
        const std::vector<site_load_information> &site_loads );
    std::vector<site_load_information> get_new_site_load_for_removing_replicas(
        const std::vector<std::shared_ptr<partition_payload>> &parts_to_remove,
        const std::vector<std::shared_ptr<partition_location_information>>
            &parts_to_remove_locations,
        int destination, const std::vector<site_load_information> &site_loads );
    std::vector<site_load_information> get_new_site_load_for_remastering(
        const std::vector<std::shared_ptr<partition_payload>>
            &parts_to_remaster,
        int master, int destination,
        const std::vector<site_load_information> &site_loads );

    double get_default_benefit_of_changing_load(
        const std::vector<site_load_information> &site_loads,
        const std::vector<site_load_information> &new_site_loads );

    // concurrency stuff
    std::shared_ptr<partition_location_information>
        get_location_information_for_partition(
            std::shared_ptr<partition_payload> &partition,
            const partition_column_identifier_map_t<
                std::shared_ptr<partition_location_information>>
                &partition_location_informations );
    std::shared_ptr<partition_location_information>
        get_location_information_for_partition_or_from_payload(
            std::shared_ptr<partition_payload> &partition,
            partition_column_identifier_map_t<
                std::shared_ptr<partition_location_information>>
                &                      partition_location_informations,
            const partition_lock_mode &lock_mode );

    std::tuple<std::shared_ptr<partition_payload>, bool>
        get_partition_from_data_location_table_or_already_locked(
            const partition_column_identifier &pid,
            partition_column_identifier_map_t<std::shared_ptr<
                partition_payload>> &  other_write_locked_partitions,
            const partition_lock_mode &lock_mode );

    void unlock_if_new(
        std::tuple<std::shared_ptr<partition_payload>, bool> &entry,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_lock_mode &           lock_mode );

    uint32_t get_remaster_update_destination_slot(
        int destination, std::shared_ptr<partition_payload>,
        std::shared_ptr<partition_location_information>
            part_location_information );

    void build_sampled_transactions(
        const ::clientid id,
        partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder );
    void add_to_sample_mapping(
        partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &write_sample_holder,
        partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>> &read_sample_holder,
        const tracking_summary &                          summary );

    void add_to_deps( const partition_column_identifier &pid,
                      const partition_column_identifier_map_t<
                          std::shared_ptr<pre_action>> &        pid_dep_map,
                      std::vector<std::shared_ptr<pre_action>> &deps,
                      std::unordered_set<uint32_t> &            add_deps );

    double get_site_load_imbalance_cost(
        uint32_t site, const std::vector<site_load_information> &site_loads,
        double average_load_per_site );
    double get_site_storage_imbalance_cost(
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &site_storages ) const;
    double get_change_in_storage_imbalance_cost(
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &after,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &before ) const;
    double get_change_in_storage_imbalance_costs( double after,
                                                  double before ) const;

    void build_min_partition_sizes( uint32_t num_tables );
    double get_split_min_partition_size( uint32_t table_id ) const;

    std::tuple<std::vector<partition_type::type>,
               std::vector<storage_tier_type::type>>
        internal_get_new_partition_types(
            int destination, std::shared_ptr<pre_transaction_plan> &plan,
            const site_load_information &                    site_load_info,
            std::vector<std::shared_ptr<partition_payload>> &insert_partitions,
            std::unordered_map<storage_tier_type::type, std::vector<double>>
                &  storage_sizes,
            double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
            query_arrival_cached_stats &cached_query_arrival_stats );

    std::tuple<partition_type::type, storage_tier_type::type>
        get_new_partition_type(
            int destination, std::shared_ptr<pre_transaction_plan> &plan,
            const site_load_information &       site_load_info,
            std::shared_ptr<partition_payload> &payload,
            partition_column_identifier_map_t<partition_type::type>
                &observed_types,
            partition_column_identifier_map_t<storage_tier_type::type>
                &storage_types,
            std::unordered_map<storage_tier_type::type, std::vector<double>>
                &  storage_sizes,
            double ori_storage_imbalance, cached_ss_stat_holder &cached_stats,
            query_arrival_cached_stats &cached_query_arrival_stats );
    bool can_plan_execute_at_site(
        const grouped_partition_information &grouped_info, int site ) const;

    double get_upcoming_query_arrival_score(
        const partition_column_identifier &pid, bool allow_updates,
        query_arrival_cached_stats &cached_arrival_stats );

    partition_type::type get_partition_type(
        const partition_column_identifier &pid, uint32_t destination,
        const std::shared_ptr<partition_location_information>
            &part_location_information,
        const partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types ) const;
    storage_tier_type::type get_storage_type(
        const partition_column_identifier &pid, uint32_t destination,
        const std::shared_ptr<partition_location_information>
            &part_location_information,
        const partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types ) const;

    int get_scan_destination(
        std::shared_ptr<multi_site_pre_transaction_plan> &     plan,
        const std::shared_ptr<partition_payload> &             part,
        const std::shared_ptr<partition_location_information> &part_loc_info,
        const std::vector<cell_key_ranges> &ckrs, uint32_t label,
        const scan_arguments &                    scan_arg,
        const std::vector<site_load_information> &site_load_infos,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                    storage_sizes,
        const ::snapshot_vector &svv,
        std::unordered_map<int /* site */, double> &costs_per_site,
        cached_ss_stat_holder &     cached_stats,
        query_arrival_cached_stats &cached_arrival_stats );
    double add_to_scan_plan(
        std::shared_ptr<multi_site_pre_transaction_plan> &plan, int destination,
        const std::shared_ptr<partition_payload> &             payload,
        const std::shared_ptr<partition_location_information> &part_loc_info,
        const partition_column_identifier &                    pid,
        const std::vector<cell_key_ranges> &                   ckrs,
        const scan_arguments &                                 scan_arg,
        partition_column_identifier_map_t<int> &               pids_to_site,
        std::unordered_map<
            int /*site */,
            std::unordered_map<uint32_t /* label */, uint32_t /* pos */>>
            &                                     site_label_positions,
        const std::vector<site_load_information> &site_load_infos,
        std::unordered_map<int /* site */, double> &costs_per_site,
        cached_ss_stat_holder &cached_stats );
    double add_to_scan_plan(
        std::shared_ptr<multi_site_pre_transaction_plan> &plan, int destination,
        const partition_column_identifier &                    pid,
        const std::shared_ptr<partition_payload> &             payload,
        const std::shared_ptr<partition_location_information> &part_loc_info,
        const std::vector<cell_key_ranges> &                   ckrs,
        const transaction_prediction_stats &                   txn_stat,
        const scan_arguments &                                 scan_arg,
        partition_column_identifier_map_t<int> &               pids_to_site,
        std::unordered_map<
            int /*site */,
            std::unordered_map<uint32_t /* label */, uint32_t /* pos */>>
            &                                     site_label_positions,
        const std::vector<site_load_information> &site_load_infos,
        std::unordered_map<int /* site */, double> &costs_per_site );
    double get_scan_destination_cost(
        std::shared_ptr<multi_site_pre_transaction_plan> &plan, int destination,
        const partition_type::type &   part_type,
        const storage_tier_type::type &storage_type,
        transaction_prediction_stats &txn_stat, uint32_t label,
        const scan_arguments &                    scan_arg,
        const std::vector<site_load_information> &site_load_infos,
        const std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                    storage_sizes,
        const ::snapshot_vector &svv,
        std::unordered_map<int /* site */, double> &costs_per_site,
        cached_ss_stat_holder &     cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    std::tuple<double, double> add_partition_to_scan_plan(
        std::shared_ptr<multi_site_pre_transaction_plan> &     plan,
        const std::shared_ptr<partition_payload> &             payload,
        const partition_column_identifier &                    pid,
        const std::shared_ptr<partition_location_information> &part_loc_info,
        const std::unordered_map<uint32_t /* label */,
                                 std::vector<cell_key_ranges>> &labels_to_ckrs,
        const std::unordered_map<uint32_t /* label */, uint32_t /* pos */>
            &                                     label_to_pos,
        const std::vector<scan_arguments> &       scan_args,
        const std::vector<site_load_information> &site_load_infos,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                                   storage_sizes,
        const snapshot_vector &                 svv,
        partition_column_identifier_map_t<int> &pids_to_site,
        std::unordered_map<
            int /*site */,
            std::unordered_map<uint32_t /* label */, uint32_t /* pos */>>
            &site_label_positions,
        std::unordered_map<int /* site */, double> &costs_per_site,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        bool allow_changes,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &original_read_partition_set,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &sampled_partition_accesses_index_by_write_partition,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &sampled_partition_accesses_index_by_read_partition,
        master_destination_partition_entries &to_add_replicas,
        master_destination_partition_entries &to_change_types,
        cached_ss_stat_holder &               cached_stats,
        query_arrival_cached_stats &          cached_query_arrival_stats );

    std::tuple<int, double /* cost */, double /* best no change */>
        add_change_scan_destination_to_plan(
            std::shared_ptr<multi_site_pre_transaction_plan> &plan,
            int                                               prev_destination,
            const std::shared_ptr<partition_payload> &        part,
            const partition_column_identifier &               pid,
            const std::shared_ptr<partition_location_information>
                &                               part_loc_info,
            const std::vector<cell_key_ranges> &ckrs, int label,
            const scan_arguments &                  scan_arg,
            partition_column_identifier_map_t<int> &pids_to_site,
            std::unordered_map<
                int /*site */,
                std::unordered_map<uint32_t /* label */, uint32_t /* pos */>>
                &                                     site_label_positions,
            const std::vector<site_load_information> &site_load_infos,
            std::unordered_map<storage_tier_type::type, std::vector<double>>
                &                  storage_sizes,
            const snapshot_vector &svv,
            std::unordered_map<int /* site */, double> &costs_per_site,
            partition_column_identifier_map_t<std::shared_ptr<pre_action>>
                &pid_dep_map,
            partition_column_identifier_map_t<std::unordered_map<
                uint32_t, partition_type::type>> &created_partition_types,
            partition_column_identifier_map_t<std::unordered_map<
                uint32_t, storage_tier_type::type>> &created_storage_types,
            bool allow_changes, partition_column_identifier_map_t<
                                    std::shared_ptr<partition_payload>>
                                    &other_write_locked_partitions,
            partition_column_identifier_map_t<
                std::shared_ptr<partition_location_information>>
                &partition_location_informations,
            const partition_column_identifier_unordered_set
                &original_write_partition_set,
            const partition_column_identifier_unordered_set
                &original_read_partition_set,
            const partition_column_identifier_map_t<
                std::vector<transaction_partition_accesses>>
                &sampled_partition_accesses_index_by_write_partition,
            const partition_column_identifier_map_t<
                std::vector<transaction_partition_accesses>>
                &sampled_partition_accesses_index_by_read_partition,
            master_destination_partition_entries &to_add_replicas,
            master_destination_partition_entries &to_change_types,
            cached_ss_stat_holder &               cached_stats,
            query_arrival_cached_stats &          cached_query_arrival_stats );

    void finalize_scan_plan(
        const ::clientid                                  id,
        std::shared_ptr<multi_site_pre_transaction_plan> &plan,
        const grouped_partition_information &             grouped_info,
        const partition_column_identifier_map_t<int> &    pids_to_site,
        std::shared_ptr<multi_query_plan_entry> &         mq_plan_entry,
        snapshot_vector &                                 session );

    void add_to_partition_operation_tracking(
        master_destination_partition_entries &                 to_add,
        const master_destination_dependency_version &          master_dest,
        const partition_column_identifier &                    pid,
        const std::shared_ptr<partition_payload> &             part,
        const std::shared_ptr<partition_location_information> &part_loc_info,
        const partition_type::type &                           p_type,
        const storage_tier_type::type &                        s_type ) const;
    void add_to_partition_operation_tracking(
        master_destination_partition_entries &                 to_add,
        const master_destination_dependency_version &          master_dest,
        const partition_column_identifier &                    pid,
        const std::shared_ptr<partition_payload> &             part,
        const std::shared_ptr<partition_location_information> &part_loc_info )
        const;

    change_scan_plan get_change_scan_destination_and_cost(
        std::shared_ptr<multi_site_pre_transaction_plan> &plan, int destination,
        const std::shared_ptr<partition_payload> &             part,
        const partition_column_identifier &                    pid,
        const std::shared_ptr<partition_location_information> &part_loc_info,
        const std::vector<cell_key_ranges> &ckrs, int label,
        const scan_arguments &scan_arg, transaction_prediction_stats &txn_stat,
        partition_column_identifier_map_t<int> &pids_to_site,
        std::unordered_map<
            int /*site */,
            std::unordered_map<uint32_t /* label */, uint32_t /* pos */>>
            &                                     site_label_positions,
        const std::vector<site_load_information> &site_load_infos,
        std::unordered_map<storage_tier_type::type, std::vector<double>>
            &                  storage_sizes,
        const snapshot_vector &svv,
        std::unordered_map<int /* site */, double> &costs_per_site,
        partition_column_identifier_map_t<std::shared_ptr<pre_action>>
            &pid_dep_map,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, partition_type::type>> &created_partition_types,
        partition_column_identifier_map_t<std::unordered_map<
            uint32_t, storage_tier_type::type>> &created_storage_types,
        partition_column_identifier_map_t<std::shared_ptr<partition_payload>>
            &other_write_locked_partitions,
        partition_column_identifier_map_t<std::shared_ptr<
            partition_location_information>> &partition_location_informations,
        const partition_column_identifier_unordered_set
            &original_write_partition_set,
        const partition_column_identifier_unordered_set
            &original_read_partition_set,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &sampled_partition_accesses_index_by_write_partition,
        const partition_column_identifier_map_t<
            std::vector<transaction_partition_accesses>>
            &sampled_partition_accesses_index_by_read_partition,
        cached_ss_stat_holder &     cached_stats,
        query_arrival_cached_stats &cached_query_arrival_stats );
    double get_scan_selectivity(
        const std::shared_ptr<partition_payload> &payload,
        const scan_arguments &                    scan_arg,
        cached_ss_stat_holder &                   cached_stats ) const;
    std::shared_ptr<multi_site_pre_transaction_plan>
        generate_scan_plan_from_sorted_planners(
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
            const std::vector<std::tuple<
                double, uint64_t, std::shared_ptr<multi_query_plan_entry>>>
                                                     sorted_plans,
            std::shared_ptr<multi_query_plan_entry> &mq_plan_entry,
            snapshot_vector &                        session );

    bool has_converged( const std::vector<double> &changes ) const;

    heuristic_site_evaluator_configs               configs_;
    std::shared_ptr<cost_modeller2>                cost_model2_;
    std::shared_ptr<partition_data_location_table> data_loc_tab_;
    const site_selector_query_stats *              query_stats_;
    const data_site_storage_stats *                storage_stats_;

    std::shared_ptr<sites_partition_version_information>
                                                    site_partition_version_info_;
    std::shared_ptr<query_arrival_predictor>        query_predictor_;
    std::shared_ptr<stat_tracked_enumerator_holder> enumerator_holder_;
    std::shared_ptr<partition_tier_tracking>        tier_tracking_;

    std::vector<std::unique_ptr<adaptive_reservoir_sampler>> *samplers_;
    std::vector<adaptive_reservoir *>                         force_reservoirs_;
    distributions                                             rand_dist_;
    std::vector<double>                                       min_part_sizes_;
};

