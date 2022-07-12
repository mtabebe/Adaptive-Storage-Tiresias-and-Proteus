#pragma once

#include <chrono>

#include "../common/perf_tracking.h"
#include "../common/predictor/predictor.h"
#include "../common/predictor2.h"
#include "../distributions/distributions.h"

#include "cost_modeller_types.h"

class cost_modeller2 {
   public:
    cost_modeller2( const cost_modeller_configs& configs );
    ~cost_modeller2();

    void update_model();

    void add_results( cost_model_prediction_holder& prediction_and_results );

    double normalize_contention_by_time( double contention ) const;

    cost_model_prediction_holder predict_distributed_scan_execution_time(
        const std::unordered_map<int, double>& txn_stats ) const;
    cost_model_prediction_holder predict_distributed_scan_execution_time(
        const std::vector<double>& site_loads,
        const std::unordered_map<
            int, std::vector<transaction_prediction_stats>>& txn_stats ) const;
    cost_model_prediction_holder predict_distributed_scan_execution_time(
        double max_latency, double min_latency ) const;

    cost_model_prediction_holder predict_single_site_transaction_execution_time(
        double                                           site_load,
        const std::vector<transaction_prediction_stats>& txn_stats ) const;

    cost_model_prediction_holder predict_add_replica_execution_time(
        double source_load, double dest_load,
        const std::vector<add_replica_stats>& replicas ) const;
    cost_model_prediction_holder predict_remove_replica_execution_time(
        double                                   site_load,
        const std::vector<remove_replica_stats>& removes ) const;

    cost_model_prediction_holder predict_changing_type_execution_time(
        double                                 site_load,
        const std::vector<change_types_stats>& changes ) const;

    cost_model_prediction_holder predict_vertical_split_execution_time(
        double site_load, const split_stats& split ) const;
    cost_model_prediction_holder predict_horizontal_split_execution_time(
        double site_load, const split_stats& split ) const;

    cost_model_prediction_holder predict_vertical_merge_execution_time(
        double site_load, const merge_stats& merge ) const;
    cost_model_prediction_holder predict_horizontal_merge_execution_time(
        double site_load, const merge_stats& merge ) const;

    cost_model_prediction_holder predict_remaster_execution_time(
        double source_load, double dest_load,
        const std::vector<remaster_stats>& remasters ) const;

    cost_model_prediction_holder predict_wait_for_service_time(
        double site_load ) const;

    cost_model_prediction_holder predict_site_operation_count(
        double count ) const;

    double predict_site_load( const site_load_information& site_load ) const;
    double predict_site_load( double write_count, double read_count,
                              double update_count ) const;
    std::vector<double> get_load_normalization() const;
    std::vector<double> get_load_model_max_inputs() const;

    double get_site_operation_count( double count ) const;
    site_load_information get_site_operation_counts(
        const site_load_information& input ) const;

    void add_result( const cost_model_component_type& model_type,
                     const predictor3_result&         result );

    double get_default_remaster_num_updates_required_count();
    double get_default_previous_results_num_updates_required_count();
    double get_default_write_num_updates_required_count();

    cost_modeller_configs get_configs() const;

    friend std::ostream& operator<<( std::ostream&         os,
                                     const cost_modeller2& c );

   protected:
    // for cout
    const std::vector<std::vector<predictor3*>>& get_predictors() const;
    const std::vector<std::vector<double>>& get_normalizations() const;

   private:
    void predict_change_type_execution_time(
        cost_model_prediction_holder& prediction_holder,
        const change_types_stats&     stat ) const;

    void predict_horizontal_row_split(
        cost_model_prediction_holder& prediction_holder,
        const split_stats& split, const partition_type::type& other_type,
        uint32_t other_num_entries ) const;
    void predict_horizontal_sorted_column_split(
        cost_model_prediction_holder& prediction_holder,
        const split_stats& split, const partition_type::type& other_type,
        uint32_t other_num_entries ) const;
    void predict_horizontal_column_split(
        cost_model_prediction_holder& prediction_holder,
        const split_stats& split, const partition_type::type& other_type,
        uint32_t other_num_entries ) const;

    void predict_horizontal_row_merge(
        cost_model_prediction_holder& prediction_holder,
        const merge_stats& merge, const partition_type::type& other_type,
        const std::vector<double>& other_cell_widths,
        uint32_t                   other_num_entries ) const;
    void predict_horizontal_column_merge(
        cost_model_prediction_holder& prediction_holder,
        const merge_stats& merge, const partition_type::type& other_type,
        const std::vector<double>& other_cell_widths,
        uint32_t                   other_num_entries ) const;
    void predict_horizontal_sorted_column_merge(
        cost_model_prediction_holder& prediction_holder,
        const merge_stats& merge, const partition_type::type& other_type,
        const std::vector<double>& other_cell_widths,
        uint32_t                   other_num_entries ) const;

    void predict_vertical_row_split(
        cost_model_prediction_holder& prediction_holder,
        const split_stats& split, const partition_type::type& other_type,
        const std::vector<double>& cell_widths ) const;
    void predict_vertical_column_split(
        cost_model_prediction_holder& prediction_holder,
        const split_stats& split, const partition_type::type& other_type,
        const std::vector<double>& cell_widths ) const;
    void predict_vertical_sorted_column_split(
        cost_model_prediction_holder& prediction_holder,
        const split_stats& split, const partition_type::type& other_type,
        const std::vector<double>& cell_widths, bool is_left ) const;
    void predict_vertical_multi_column_split(
        cost_model_prediction_holder& prediction_holder,
        const split_stats& split, const partition_type::type& other_type,
        const std::vector<double>& cell_widths ) const;

    void predict_vertical_row_merge(
        cost_model_prediction_holder& prediction_holder,
        const merge_stats& merge, const partition_type::type& other_type,
        const std::vector<double>& cell_widths ) const;
    void predict_vertical_column_merge(
        cost_model_prediction_holder& prediction_holder,
        const merge_stats& merge, const partition_type::type& other_type,
        const std::vector<double>& cell_widths ) const;
    void predict_vertical_sorted_column_merge(
        cost_model_prediction_holder& prediction_holder,
        const merge_stats& merge, const partition_type::type& other_type,
        const std::vector<double>& cell_widths, bool is_left ) const;
    void predict_vertical_multi_column_merge(
        cost_model_prediction_holder& prediction_holder,
        const merge_stats& merge, const partition_type::type& other_type,
        const std::vector<double>& cell_widths ) const;

    void predict_common_split_merge_execution_time(
        cost_model_prediction_holder& prediction_holder, double site_load,
        const std::vector<double>& contention ) const;
    void predict_common_split_merge_storage_time(
        cost_model_prediction_holder&               prediction_holder,
        const std::vector<partition_type::type>&    ori_types,
        const std::vector<storage_tier_type::type>& ori_storage_types,
        const std::vector<std::vector<double>>&     ori_widths,
        const std::vector<uint32_t>&                ori_num_entries,
        const std::vector<partition_type::type>&    new_types,
        const std::vector<storage_tier_type::type>& new_storage_types,
        const std::vector<std::vector<double>>&     new_widths,
        const std::vector<uint32_t>&                new_num_entries ) const;

    void predict_generic_split_merge_execution_time(
        cost_model_prediction_holder& prediction_holder,
        const partition_type::type&   ori_type,
        const partition_type::type    dest_type,
        const std::vector<double>& cell_widths, uint32_t num_entries ) const;

    predictor3_result make_prediction(
        const cost_model_component_type& model_type,
        const partition_type::type&      part_type,
        const std::vector<double>&       input ) const;
    void add_predictor3( const cost_model_component_type& model_type,
                         const predictor_type&            pred_type,
                         const std::vector<double>&       model_weights,
                         double init_bias, const std::vector<double>& normals,
                         const std::vector<double>& max_inputs, double bound );

    std::tuple<bool, predictor3_result> make_read_time_prediction(
        const partition_type::type& part_type,
        const std::vector<double>& cell_widths, uint32_t num_reads ) const;
    std::tuple<bool, predictor3_result> make_read_disk_time_prediction(
        const partition_type::type& part_type,
        const std::vector<double>& cell_widths, uint32_t num_reads ) const;

    std::tuple<bool, predictor3_result> make_write_time_prediction(
        const partition_type::type& part_type,
        const std::vector<double>& cell_widths, uint32_t num_writes ) const;

    std::tuple<bool, predictor3_result> make_scan_time_prediction(
        const partition_type::type& part_type,
        const std::vector<double>& cell_widths, uint32_t num_entries,
        double selectivity ) const;
    std::tuple<bool, predictor3_result> make_scan_disk_time_prediction(
        const partition_type::type& part_type,
        const std::vector<double>& cell_widths, uint32_t num_entries,
        double selectivity ) const;

    std::tuple<bool, predictor3_result> make_evict_to_disk_time_prediction(
        const partition_type::type& part_type,
        const std::vector<double>& cell_widths, uint32_t num_entries ) const;
    std::tuple<bool, predictor3_result> make_pull_from_disk_time_prediction(
        const partition_type::type& part_type,
        const std::vector<double>& cell_widths, uint32_t num_entries ) const;

    std::tuple<bool, predictor3_result> make_memory_assignment_prediction(
        const partition_type::type& part_type,
        const std::vector<double>& cell_widths, uint32_t num_entries ) const;

    std::tuple<bool, predictor3_result> make_memory_allocation_prediction(
        const partition_type::type& part_type,
        const std::vector<double>& cell_widths, uint32_t num_entries ) const;
    std::tuple<bool, predictor3_result> make_memory_deallocation_prediction(
        const std::vector<double>& cell_widths, uint32_t num_entries ) const;

    std::tuple<bool, predictor3_result> make_lock_time_prediction(
        const partition_type::type& part_type, double contention ) const;

    std::tuple<bool, predictor3_result>
        make_build_commit_snapshot_time_prediction(
            uint32_t write_pid_size ) const;
    std::tuple<bool, predictor3_result>
        make_commit_serialize_update_time_prediction(
            uint32_t write_pk_size ) const;

    std::tuple<bool, predictor3_result> make_distributed_scan_time_prediction(
        double max_lat, double min_lat ) const;
    std::tuple<bool, predictor3_result> make_wait_for_service_time_prediction(
        double site_loads ) const;
    std::tuple<bool, predictor3_result>
        make_wait_for_session_version_time_prediction(
            const partition_type::type& part_type,
            double                      estimated_num_updates ) const;
    std::tuple<bool, predictor3_result>
        make_wait_for_session_snapshot_time_prediction(
            const partition_type::type& part_type,
            double                      estimated_num_updates ) const;

    std::tuple<bool, predictor3_result> make_site_operation_count_prediction(
        double operation_count ) const;

    void make_and_add_read_predictor3();
    void make_and_add_write_predictor3();
    void make_and_add_scan_predictor3();
    void make_and_add_lock_predictor3();
    void make_and_add_commit_and_serialize_update_predictor3();
    void make_and_add_commit_and_build_snapshot_predictor3();
    void make_and_add_wait_for_service_predictor3();
    void make_and_add_wait_for_session_version_predictor3();
    void make_and_add_wait_for_session_snapshot_predictor3();
    void make_and_add_site_operation_count_predictor3();
    void make_and_add_site_load_predictor3();
    void make_and_add_memory_allocation_predictor3();
    void make_and_add_memory_deallocation_predictor3();
    void make_and_add_memory_assignment_predictor3();
    void make_and_add_evict_to_disk_predictor3();
    void make_and_add_pull_from_disk_predictor3();
    void make_and_add_read_disk_predictor3();
    void make_and_add_scan_disk_predictor3();
    void make_and_add_distributed_scan_predictor3();

    std::tuple<int, int> get_predictor3_pos(
        const cost_model_component_type& model_type,
        const partition_type::type&      part_type ) const;
    int get_predictor3_pos( const cost_model_component_type& model_type ) const;

    predictor3* get_predictor3( const cost_model_component_type& model_type,
                                const partition_type::type& part_type ) const;

    int get_predictor3_from_result_by_type(
        const cost_model_prediction_holder& prediction_and_results,
        const cost_model_component_type&    cost_type,
        const partition_type::type&         part_type );
    std::tuple<int, cost_model_component_type, partition_type::type>
        find_predictor3_from_timer(
            int                                 timer_id,
            const cost_model_prediction_holder& prediction_and_results );

    std::vector<std::vector<predictor3*>> predictor3s_;
    std::vector<std::vector<double>>     normalizations_;
    std::vector<double>                  pred_bounds_;

    distributions dists_;

    cost_modeller_configs configs_;

    std::chrono::high_resolution_clock::time_point start_time_;
};
