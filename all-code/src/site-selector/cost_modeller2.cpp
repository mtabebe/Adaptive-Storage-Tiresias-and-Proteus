#include "cost_modeller.h"

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "../common/constants.h"

predictor3_result add_predictions( const predictor3_result& a,
                                   const predictor3_result& b ) {
    std::vector<double> input = a.get_input();
    std::vector<double> b_input = b.get_input();

    DCHECK_EQ( input.size(), b_input.size() );
    for( uint32_t pos = 0; pos < input.size(); pos++ ) {
        input.at( pos ) = input.at( pos ) + b_input.at( pos );
    }

    DCHECK_EQ( a.get_partition_type(), b.get_partition_type() );

    predictor3_result new_pred( input, a.get_prediction() + b.get_prediction(),
                                a.get_partition_type() );
    return new_pred;
}

cost_modeller2::cost_modeller2( const cost_modeller_configs& configs )
    : predictor3s_(),
      normalizations_(),
      pred_bounds_(),
      dists_( nullptr ),
      configs_( configs ) {

    make_and_add_read_predictor3();
    make_and_add_write_predictor3();
    make_and_add_scan_predictor3();
    make_and_add_lock_predictor3();
    make_and_add_commit_and_serialize_update_predictor3();
    make_and_add_commit_and_build_snapshot_predictor3();
    make_and_add_wait_for_service_predictor3();
    make_and_add_wait_for_session_version_predictor3();
    make_and_add_wait_for_session_snapshot_predictor3();
    make_and_add_site_operation_count_predictor3();
    make_and_add_site_load_predictor3();
    make_and_add_memory_allocation_predictor3();
    make_and_add_memory_deallocation_predictor3();
    make_and_add_memory_assignment_predictor3();
    make_and_add_evict_to_disk_predictor3();
    make_and_add_pull_from_disk_predictor3();
    make_and_add_read_disk_predictor3();
    make_and_add_scan_disk_predictor3();
    make_and_add_distributed_scan_predictor3();

    start_time_ = std::chrono::high_resolution_clock::now();
    DVLOG( 40 ) << "Create cost modeller";
}

cost_modeller2::~cost_modeller2() {
    for( uint32_t pos = 0; pos < predictor3s_.size(); pos++ ) {
        for( uint32_t i = 0; i < predictor3s_.at( pos ).size(); i++ ) {
            delete predictor3s_.at( pos ).at( i );
            predictor3s_.at( pos ).at( i ) = nullptr;
        }
        predictor3s_.at( pos ).clear();
    }
    predictor3s_.clear();
}

void cost_modeller2::add_result( const cost_model_component_type& model_type,
                                 const predictor3_result&         result ) {
    DVLOG( k_cost_model_log_level )
        << "Cost modeller, add result:"
        << get_cost_model_component_type_name( model_type )
        << ", actual:" << result.get_actual_output()
        << ", input:" << result.get_input()
        << ", prediction:" << result.get_prediction()
        << ", error:" << result.get_error();

    if( std::fabs( result.get_error() ) > k_cost_model_log_error_threshold ) {
        VLOG( k_slow_timer_error_log_level )
            << "POOR PREDICTION: Cost modeller, add result:"
            << get_cost_model_component_type_name( model_type )
            << ", actual:" << result.get_actual_output()
            << ", input:" << result.get_input()
            << ", prediction:" << result.get_prediction()
            << ", error:" << result.get_error();
    }

    partition_type::type part_type = result.get_partition_type();
    auto                 pred = get_predictor3( model_type, part_type );

    pred->add_observation( result );
}

void cost_modeller2::update_model() {
    DVLOG( 40 ) << "Update cost modeller";
    if( configs_.is_static_model_ ) {
        DVLOG( 40 ) << "Update cost modeller, is_static done";
        return;
    }
    for( int pos = 0; pos < (int) predictor3s_.size(); pos++ ) {
        cost_model_component_type c_type = (cost_model_component_type) pos;
        DVLOG( 40 ) << "Update weights:"
                    << get_cost_model_component_type_name( c_type );
        for( int i = 0; i < (int) predictor3s_.at( pos ).size(); i++ ) {
            predictor3s_.at( pos ).at( i )->update_weights();
        }
    }
    DVLOG( k_periodic_cost_model_log_level ) << "New cost model:" << *this;
    DVLOG( 40 ) << "Update cost modeller, done";
}

double cost_modeller2::normalize_contention_by_time( double contention ) const {
    std::chrono::high_resolution_clock::time_point cur_time =
        std::chrono::high_resolution_clock::now();

    std::chrono::duration<double, std::milli> diff_time =
        cur_time - start_time_;

    double normalized_contention = contention / ( 1 + diff_time.count() );
    DVLOG( 40 ) << "Input contention: " << contention
                << ", normalized contention:" << normalized_contention
                << ", diff time:" << diff_time.count();

    return normalized_contention;
}

cost_model_prediction_holder
    cost_modeller2::predict_distributed_scan_execution_time(
        const std::unordered_map<int, double>& txn_stats ) const {
    bool   seen_one = false;
    double max_latency = 0;
    double min_latency = 0;

    for( const auto& entry : txn_stats ) {
        double latency = entry.second;
        if( !seen_one ) {
            seen_one = true;
            max_latency = latency;
            min_latency = latency;
        }
        if( latency > max_latency ) {
            max_latency = latency;
        }
        if( latency < min_latency ) {
            min_latency = latency;
        }
    }

    DCHECK( seen_one );
    return predict_distributed_scan_execution_time( max_latency, min_latency );
}
cost_model_prediction_holder
    cost_modeller2::predict_distributed_scan_execution_time(
        const std::vector<double>& site_loads,
        const std::unordered_map<
            int, std::vector<transaction_prediction_stats>>& txn_stats ) const {

    bool   seen_one = false;
    double max_latency = 0;
    double min_latency = 0;

    for( const auto& entry : txn_stats ) {
        int    site = entry.first;
        DCHECK_LT( site, site_loads.size() );
        auto cost_pred = predict_single_site_transaction_execution_time(
            site_loads.at( site ), entry.second );
        double latency = cost_pred.get_prediction();
        if( !seen_one ) {
            seen_one = true;
            max_latency = latency;
            min_latency = latency;
        }
        if( latency > max_latency ) {
            max_latency = latency;
        }
        if( latency < min_latency ) {
            min_latency = latency;
        }
    }

    DCHECK( seen_one );

    return predict_distributed_scan_execution_time( max_latency, min_latency );
}

cost_model_prediction_holder
    cost_modeller2::predict_distributed_scan_execution_time(
        double max_latency, double min_latency ) const {
    cost_model_prediction_holder prediction_holder;

    prediction_holder.add_prediction_linearly(
        cost_model_component_type::DISTRIBUTED_SCAN,
        make_distributed_scan_time_prediction( max_latency, min_latency ) );

    return prediction_holder;
}

cost_model_prediction_holder
    cost_modeller2::predict_single_site_transaction_execution_time(
        double                                           site_load,
        const std::vector<transaction_prediction_stats>& txn_stats ) const {
    cost_model_prediction_holder prediction_holder;

    prediction_holder.add_prediction_linearly(
        cost_model_component_type::WAIT_FOR_SERVICE,
        make_wait_for_service_time_prediction( site_load ) );

    uint32_t num_write_parts = 0;
    uint32_t num_cell_updates = 0;

    for( const auto& stat : txn_stats ) {

        prediction_holder.add_prediction_max(
            cost_model_component_type::WAIT_FOR_SESSION_VERSION,
            make_wait_for_session_version_time_prediction(
                stat.part_type_, stat.num_updates_needed_ ) );
        if( stat.is_scan_ or stat.is_point_read_ ) {
            prediction_holder.add_prediction_max(
                cost_model_component_type::WAIT_FOR_SESSION_SNAPSHOT,
                make_wait_for_session_snapshot_time_prediction(
                    stat.part_type_, stat.num_updates_needed_ ) );
        }

        if( stat.is_scan_ ) {
            if( stat.storage_type_ == storage_tier_type::type::MEMORY ) {
                prediction_holder.add_prediction_linearly(
                    cost_model_component_type::SCAN_EXECUTION,
                    make_scan_time_prediction(
                        stat.part_type_, stat.avg_cell_widths_,
                        stat.num_entries_, 1 /* selectivity*/ ) );
            } else if( stat.storage_type_ == storage_tier_type::type::DISK ) {
                prediction_holder.add_prediction_linearly(
                    cost_model_component_type::SCAN_DISK_EXECUTION,
                    make_scan_disk_time_prediction(
                        stat.part_type_, stat.avg_cell_widths_,
                        stat.num_entries_, 1 /* selectivity*/ ) );
            }
        }

        if( stat.is_point_read_ ) {
            if( stat.storage_type_ == storage_tier_type::type::MEMORY ) {
                prediction_holder.add_prediction_linearly(
                    cost_model_component_type::READ_EXECUTION,
                    make_read_time_prediction( stat.part_type_,
                                               stat.avg_cell_widths_,
                                               stat.num_point_reads_ ) );
            } else if( stat.storage_type_ == storage_tier_type::type::DISK ) {
                prediction_holder.add_prediction_linearly(
                    cost_model_component_type::READ_DISK_EXECUTION,
                    make_read_disk_time_prediction( stat.part_type_,
                                                    stat.avg_cell_widths_,
                                                    stat.num_point_reads_ ) );
            }
        }

        if( stat.is_point_update_ ) {
            if( stat.storage_type_ == storage_tier_type::type::DISK ) {
                prediction_holder.add_prediction_linearly(
                    cost_model_component_type::PULL_FROM_DISK_EXECUTION,
                    make_pull_from_disk_time_prediction( stat.part_type_,
                                                         stat.avg_cell_widths_,
                                                         stat.num_entries_ ) );
            }

            prediction_holder.add_prediction_linearly(
                cost_model_component_type::LOCK_ACQUISITION,
                make_lock_time_prediction( stat.part_type_,
                                           stat.contention_ ) );

            num_write_parts += 1;
            num_cell_updates += stat.num_point_updates_;

            prediction_holder.add_prediction_linearly(
                cost_model_component_type::WRITE_EXECUTION,
                make_write_time_prediction( stat.part_type_,
                                            stat.avg_cell_widths_,
                                            stat.num_point_updates_ ) );
        }
    }

    prediction_holder.add_prediction_linearly(
        cost_model_component_type::COMMIT_BUILD_SNAPSHOT,
        make_build_commit_snapshot_time_prediction( num_write_parts ) );
    prediction_holder.add_prediction_linearly(
        cost_model_component_type::COMMIT_SERIALIZE_UPDATE,
        make_commit_serialize_update_time_prediction( num_cell_updates ) );

    DVLOG( 40 ) << "Predicted single site transaction execution time:"
                << prediction_holder.get_prediction();

    return prediction_holder;
}

cost_model_prediction_holder cost_modeller2::predict_add_replica_execution_time(
    double source_load, double dest_load,
    const std::vector<add_replica_stats>& replicas ) const {
    cost_model_prediction_holder prediction_holder;

    prediction_holder.add_prediction_linearly(
        cost_model_component_type::WAIT_FOR_SERVICE,
        make_wait_for_service_time_prediction( source_load + dest_load ) );

    for( const auto& stat : replicas ) {
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::LOCK_ACQUISITION,
            make_lock_time_prediction( stat.source_type_, stat.contention_ ) );

        if( stat.source_storage_type_ == storage_tier_type::type::DISK ) {
            prediction_holder.add_prediction_linearly(
                cost_model_component_type::PULL_FROM_DISK_EXECUTION,
                make_pull_from_disk_time_prediction( stat.source_type_,
                                                     stat.avg_cell_widths_,
                                                     stat.num_entries_ ) );
        }

        prediction_holder.add_prediction_linearly(
            cost_model_component_type::SCAN_EXECUTION,
            make_scan_time_prediction( stat.source_type_, stat.avg_cell_widths_,
                                       stat.num_entries_,
                                       1 /* selectivity*/ ) );

        prediction_holder.add_prediction_linearly(
            cost_model_component_type::WRITE_EXECUTION,
            make_write_time_prediction( stat.dest_type_, stat.avg_cell_widths_,
                                        stat.num_entries_ ) );


        if( stat.dest_storage_type_ == storage_tier_type::type::DISK ) {
            prediction_holder.add_prediction_linearly(
                cost_model_component_type::EVICT_TO_DISK_EXECUTION,
                make_evict_to_disk_time_prediction( stat.dest_type_,
                                                    stat.avg_cell_widths_,
                                                    stat.num_entries_ ) );
        }
    }

    DVLOG( 40 ) << "Predicted add replica execution time:"
                << prediction_holder.get_prediction();

    return prediction_holder;
}

cost_model_prediction_holder
    cost_modeller2::predict_changing_type_execution_time(
        double                                 site_load,
        const std::vector<change_types_stats>& changes ) const {
    cost_model_prediction_holder prediction_holder;

    prediction_holder.add_prediction_linearly(
        cost_model_component_type::WAIT_FOR_SERVICE,
        make_wait_for_service_time_prediction( site_load ) );

    for( const auto& stat : changes ) {
        predict_change_type_execution_time( prediction_holder, stat );
    }

    DVLOG( 40 ) << "Predicted add replica execution time:"
                << prediction_holder.get_prediction();

    return prediction_holder;
}

void cost_modeller2::predict_change_type_execution_time(
    cost_model_prediction_holder& prediction_holder,
    const change_types_stats&     stat ) const {
    if( ( stat.ori_type_ == stat.new_type_ ) and
        ( stat.ori_storage_type_ == stat.new_storage_type_ ) ) {
        return;
    }

    prediction_holder.add_prediction_linearly(
        cost_model_component_type::LOCK_ACQUISITION,
        make_lock_time_prediction( stat.ori_type_, stat.contention_ ) );

    if( stat.ori_type_ != stat.new_type_ ) {

        if( ( stat.ori_type_ == partition_type::type::SORTED_COLUMN ) and
            ( stat.new_type_ == partition_type::COLUMN ) ) {
            prediction_holder.add_prediction_linearly(
                cost_model_component_type::MEMORY_ALLOCATION,
                make_memory_allocation_prediction( stat.new_type_,
                                                   stat.avg_cell_widths_,
                                                   stat.num_entries_ ) );

        } else if( ( stat.ori_type_ ==
                     partition_type::type::SORTED_MULTI_COLUMN ) and
                   ( stat.new_type_ == partition_type::MULTI_COLUMN ) ) {
            prediction_holder.add_prediction_linearly(
                cost_model_component_type::MEMORY_ALLOCATION,
                make_memory_allocation_prediction( stat.new_type_,
                                                   stat.avg_cell_widths_,
                                                   stat.num_entries_ ) );

        } else {

            prediction_holder.add_prediction_linearly(
                cost_model_component_type::READ_EXECUTION,
                make_read_time_prediction( stat.ori_type_,
                                           stat.avg_cell_widths_,
                                           stat.num_entries_ ) );

            prediction_holder.add_prediction_linearly(
                cost_model_component_type::WRITE_EXECUTION,
                make_write_time_prediction( stat.new_type_,
                                            stat.avg_cell_widths_,
                                            stat.num_entries_ ) );
        }
    } else if( stat.ori_storage_type_ != stat.new_storage_type_ ) {
        if( stat.ori_storage_type_ == storage_tier_type::type::DISK ) {
            prediction_holder.add_prediction_linearly(
                cost_model_component_type::PULL_FROM_DISK_EXECUTION,
                make_pull_from_disk_time_prediction( stat.ori_type_,
                                                     stat.avg_cell_widths_,
                                                     stat.num_entries_ ) );
        } else if( stat.new_storage_type_ == storage_tier_type::type::DISK ) {
            prediction_holder.add_prediction_linearly(
                cost_model_component_type::EVICT_TO_DISK_EXECUTION,
                make_evict_to_disk_time_prediction( stat.new_type_,
                                                    stat.avg_cell_widths_,
                                                    stat.num_entries_ ) );
        }
    }
}

cost_model_prediction_holder
    cost_modeller2::predict_remove_replica_execution_time(
        double                                   site_load,
        const std::vector<remove_replica_stats>& removes ) const {

    cost_model_prediction_holder prediction_holder;

    prediction_holder.add_prediction_linearly(
        cost_model_component_type::WAIT_FOR_SERVICE,
        make_wait_for_service_time_prediction( site_load ) );

    for( const auto& stat : removes ) {
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::LOCK_ACQUISITION,
            make_lock_time_prediction( stat.part_type_, stat.contention_ ) );

        prediction_holder.add_prediction_linearly(
            cost_model_component_type::MEMORY_DEALLOCATION,
            make_memory_deallocation_prediction( stat.avg_cell_widths_,
                                                 stat.num_entries_ ) );
    }

    DVLOG( 40 ) << "Predicted add replica execution time:"
                << prediction_holder.get_prediction();

    return prediction_holder;
}

cost_model_prediction_holder
    cost_modeller2::predict_vertical_split_execution_time(
        double site_load, const split_stats& split ) const {

    cost_model_prediction_holder prediction_holder;

    std::vector<double> contentions = {split.contention_};
    predict_common_split_merge_execution_time( prediction_holder, site_load,
                                               contentions );

    std::vector<double> left_cell_widths;
    std::vector<double> right_cell_widths;
    DCHECK_LT( split.split_point_, split.avg_cell_widths_.size() );
    for( uint32_t pos = 0; pos < split.split_point_; pos++ ) {
        left_cell_widths.emplace_back( split.avg_cell_widths_.at( pos ) );
    }
    for( uint32_t pos = split.split_point_; pos < split.avg_cell_widths_.size();
         pos++ ) {
        right_cell_widths.emplace_back( split.avg_cell_widths_.at( pos ) );
    }

    predict_common_split_merge_storage_time(
        prediction_holder, {split.ori_type_}, {split.ori_storage_type_},
        {split.avg_cell_widths_}, {split.num_entries_},
        {split.left_type_, split.right_type_},
        {split.left_storage_type_, split.right_storage_type_},
        {left_cell_widths, right_cell_widths},
        {split.num_entries_, split.num_entries_} );

    switch( split.ori_type_ ) {
        case partition_type::type::ROW: {
            predict_vertical_row_split( prediction_holder, split,
                                        split.left_type_, left_cell_widths );
            predict_vertical_row_split( prediction_holder, split,
                                        split.right_type_, right_cell_widths );
            break;
        }
        case partition_type::type::SORTED_COLUMN: {
            predict_vertical_sorted_column_split(
                prediction_holder, split, split.left_type_, left_cell_widths,
                true /*is left*/ );
            predict_vertical_sorted_column_split(
                prediction_holder, split, split.right_type_, right_cell_widths,
                false /* is right*/ );
            break;
        }
        case partition_type::type::COLUMN: {
            predict_vertical_column_split( prediction_holder, split,
                                           split.left_type_, left_cell_widths );
            predict_vertical_column_split( prediction_holder, split,
                                           split.right_type_,
                                           right_cell_widths );

            break;
        }
        case partition_type::type::MULTI_COLUMN:
        case partition_type::type::SORTED_MULTI_COLUMN: {
            predict_vertical_multi_column_split(
                prediction_holder, split, split.left_type_, left_cell_widths );
            predict_vertical_multi_column_split( prediction_holder, split,
                                                 split.right_type_,
                                                 right_cell_widths );
            break;
        }
    }

    DVLOG( 40 ) << "Predicted vertical split execution time:"
                << prediction_holder.get_prediction();

    return prediction_holder;
}

void cost_modeller2::predict_vertical_row_split(
    cost_model_prediction_holder& prediction_holder, const split_stats& split,
    const partition_type::type& other_type,
    const std::vector<double>&  cell_widths ) const {

    DCHECK_EQ( split.ori_type_, partition_type::ROW );

    if( other_type == partition_type::ROW ) {
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::MEMORY_ALLOCATION,
            make_memory_allocation_prediction( other_type, cell_widths,
                                               split.num_entries_ ) );
    } else {
        predict_generic_split_merge_execution_time(
            prediction_holder, split.ori_type_, other_type, cell_widths,
            split.num_entries_ );
    }
}
void cost_modeller2::predict_vertical_column_split(
    cost_model_prediction_holder& prediction_holder, const split_stats& split,
    const partition_type::type& other_type,
    const std::vector<double>&  cell_widths ) const {

    DCHECK_EQ( split.ori_type_, partition_type::COLUMN );

    if( other_type == partition_type::COLUMN ) {
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::MEMORY_ALLOCATION,
            make_memory_allocation_prediction( other_type, cell_widths,
                                               split.num_entries_ ) );
    } else {
        predict_generic_split_merge_execution_time(
            prediction_holder, split.ori_type_, other_type, cell_widths,
            split.num_entries_ );
    }
}

void cost_modeller2::predict_vertical_multi_column_split(
    cost_model_prediction_holder& prediction_holder, const split_stats& split,
    const partition_type::type& other_type,
    const std::vector<double>&  cell_widths ) const {

    DCHECK( ( split.ori_type_ == partition_type::MULTI_COLUMN ) or
            ( split.ori_type_ == partition_type::SORTED_MULTI_COLUMN ) );

    if( ( other_type == partition_type::MULTI_COLUMN ) or
        ( other_type == partition_type::SORTED_MULTI_COLUMN ) ) {
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::MEMORY_ALLOCATION,
            make_memory_allocation_prediction( other_type, cell_widths,
                                               split.num_entries_ ) );
    } else {
        predict_generic_split_merge_execution_time(
            prediction_holder, split.ori_type_, other_type, cell_widths,
            split.num_entries_ );
    }
}

void cost_modeller2::predict_vertical_sorted_column_split(
    cost_model_prediction_holder& prediction_holder, const split_stats& split,
    const partition_type::type& other_type,
    const std::vector<double>& cell_widths, bool is_left ) const {

    DCHECK_EQ( split.ori_type_, partition_type::SORTED_COLUMN );

    bool is_memory_allocation =
        ( other_type == partition_type::COLUMN ) or
        ( ( other_type == partition_type::SORTED_COLUMN ) and ( is_left ) );

    if( is_memory_allocation ) {
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::MEMORY_ALLOCATION,
            make_memory_allocation_prediction( other_type, cell_widths,
                                               split.num_entries_ ) );
    } else {
        predict_generic_split_merge_execution_time(
            prediction_holder, split.ori_type_, split.left_type_, cell_widths,
            split.num_entries_ );
    }
}

cost_model_prediction_holder
    cost_modeller2::predict_horizontal_split_execution_time(
        double site_load, const split_stats& split ) const {

    cost_model_prediction_holder prediction_holder;

    std::vector<double> contentions = {split.contention_};
    predict_common_split_merge_execution_time( prediction_holder, site_load,
                                               contentions );

    DCHECK_LE( split.split_point_, split.num_entries_ );
    DCHECK_GT( split.split_point_, 0 );
    uint32_t left_num_entries = split.split_point_ + 1;
    uint32_t right_num_entries =
        ( split.num_entries_ - split.split_point_ ) + 1;

    predict_common_split_merge_storage_time(
        prediction_holder, {split.ori_type_}, {split.ori_storage_type_},
        {split.avg_cell_widths_}, {split.num_entries_},
        {split.left_type_, split.right_type_},
        {split.left_storage_type_, split.right_storage_type_},
        {split.avg_cell_widths_, split.avg_cell_widths_},
        {left_num_entries, right_num_entries} );

    switch( split.ori_type_ ) {
        case partition_type::type::ROW: {
            predict_horizontal_row_split( prediction_holder, split,
                                          split.left_type_, left_num_entries );
            predict_horizontal_row_split( prediction_holder, split,
                                          split.right_type_,
                                          right_num_entries );
            break;
        }
        case partition_type::type::MULTI_COLUMN:
        case partition_type::type::COLUMN: {
            predict_horizontal_column_split(
                prediction_holder, split, split.left_type_, left_num_entries );
            predict_horizontal_column_split( prediction_holder, split,
                                             split.right_type_,
                                             right_num_entries );
            break;
        }
        case partition_type::type::SORTED_MULTI_COLUMN:
        case partition_type::type::SORTED_COLUMN: {
            predict_horizontal_sorted_column_split(
                prediction_holder, split, split.left_type_, left_num_entries );
            predict_horizontal_sorted_column_split( prediction_holder, split,
                                                    split.right_type_,
                                                    right_num_entries );
            break;
        }
    }

    DVLOG( 40 ) << "Predicted vertical split execution time:"
                << prediction_holder.get_prediction();

    return prediction_holder;
}
void cost_modeller2::predict_horizontal_row_split(
    cost_model_prediction_holder& prediction_holder, const split_stats& split,
    const partition_type::type& other_type, uint32_t other_num_entries ) const {
    DCHECK_EQ( split.ori_type_, partition_type::ROW );

    if( other_type == partition_type::ROW ) {
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::MEMORY_ASSIGNMENT,
            make_memory_assignment_prediction(
                other_type, split.avg_cell_widths_, other_num_entries ) );
    } else {
        predict_generic_split_merge_execution_time(
            prediction_holder, split.ori_type_, other_type,
            split.avg_cell_widths_, other_num_entries );
    }
}
void cost_modeller2::predict_horizontal_sorted_column_split(
    cost_model_prediction_holder& prediction_holder, const split_stats& split,
    const partition_type::type& other_type, uint32_t other_num_entries ) const {
    DCHECK( ( split.ori_type_ == partition_type::SORTED_COLUMN ) or
            ( split.ori_type_ == partition_type::SORTED_MULTI_COLUMN ) );

    if( ( split.ori_type_ == partition_type::SORTED_COLUMN ) and
        ( ( other_type == partition_type::COLUMN ) or
          ( other_type == partition_type::SORTED_COLUMN ) ) ) {
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::MEMORY_ASSIGNMENT,
            make_memory_assignment_prediction(
                other_type, split.avg_cell_widths_, other_num_entries ) );
    } else if( ( split.ori_type_ == partition_type::SORTED_MULTI_COLUMN ) and
               ( ( other_type == partition_type::MULTI_COLUMN ) or
                 ( other_type == partition_type::SORTED_MULTI_COLUMN ) ) ) {
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::MEMORY_ASSIGNMENT,
            make_memory_assignment_prediction(
                other_type, split.avg_cell_widths_, other_num_entries ) );
    } else {
        predict_generic_split_merge_execution_time(
            prediction_holder, split.ori_type_, other_type,
            split.avg_cell_widths_, other_num_entries );
    }
}
void cost_modeller2::predict_horizontal_column_split(
    cost_model_prediction_holder& prediction_holder, const split_stats& split,
    const partition_type::type& other_type, uint32_t other_num_entries ) const {
    DCHECK( ( split.ori_type_ == partition_type::COLUMN ) or
            ( split.ori_type_ == partition_type::MULTI_COLUMN ) );

    // same types
    if( other_type == split.ori_type_ ) {
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::MEMORY_ASSIGNMENT,
            make_memory_assignment_prediction(
                other_type, split.avg_cell_widths_, other_num_entries ) );
    } else {
        predict_generic_split_merge_execution_time(
            prediction_holder, split.ori_type_, other_type,
            split.avg_cell_widths_, other_num_entries );
    }
}

cost_model_prediction_holder
    cost_modeller2::predict_horizontal_merge_execution_time(
        double site_load, const merge_stats& merge ) const {
    cost_model_prediction_holder prediction_holder;

    std::vector<double> contentions = {merge.left_contention_,
                                       merge.right_contention_};
    predict_common_split_merge_execution_time( prediction_holder, site_load,
                                               contentions );

    DCHECK_EQ( merge.left_cell_widths_.size(),
               merge.right_cell_widths_.size() );
    std::vector<double> avg_widths;
    for( uint32_t pos = 0; pos < merge.left_cell_widths_.size(); pos++ ) {
        double avg = ( merge.left_cell_widths_.at( pos ) +
                       merge.right_cell_widths_.at( pos ) ) /
                     2;
        avg_widths.emplace_back( avg );
    }

    predict_common_split_merge_storage_time(
        prediction_holder, {merge.left_type_, merge.right_type_},
        {merge.left_storage_type_, merge.right_storage_type_},
        {merge.left_cell_widths_, merge.right_cell_widths_},
        {merge.left_num_entries_, merge.right_num_entries_},
        {merge.merge_type_}, {merge.merge_storage_type_}, {avg_widths},
        {merge.left_num_entries_ + merge.right_num_entries_} );

    switch( merge.merge_type_ ) {
        case partition_type::type::ROW: {
            predict_horizontal_row_merge(
                prediction_holder, merge, merge.left_type_,
                merge.left_cell_widths_, merge.left_num_entries_ );
            predict_horizontal_row_merge(
                prediction_holder, merge, merge.right_type_,
                merge.right_cell_widths_, merge.right_num_entries_ );
            break;
        }
        case partition_type::type::MULTI_COLUMN:
        case partition_type::type::COLUMN: {
            predict_horizontal_column_merge(
                prediction_holder, merge, merge.left_type_,
                merge.left_cell_widths_, merge.left_num_entries_ );
            predict_horizontal_column_merge(
                prediction_holder, merge, merge.right_type_,
                merge.right_cell_widths_, merge.right_num_entries_ );
            break;
        }
        case partition_type::type::SORTED_MULTI_COLUMN:
        case partition_type::type::SORTED_COLUMN: {
            predict_horizontal_sorted_column_merge(
                prediction_holder, merge, merge.left_type_,
                merge.left_cell_widths_, merge.left_num_entries_ );
            predict_horizontal_sorted_column_merge(
                prediction_holder, merge, merge.right_type_,
                merge.right_cell_widths_, merge.right_num_entries_ );
            break;
        }
    }

    DVLOG( 40 ) << "Predicted vertical split execution time:"
                << prediction_holder.get_prediction();

    return prediction_holder;
}
void cost_modeller2::predict_horizontal_row_merge(
    cost_model_prediction_holder& prediction_holder, const merge_stats& merge,
    const partition_type::type& other_type,
    const std::vector<double>&  other_cell_widths,
    uint32_t                    other_num_entries ) const {
    DCHECK_EQ( merge.merge_type_, partition_type::ROW );

    if( other_type == partition_type::ROW ) {
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::MEMORY_ASSIGNMENT,
            make_memory_assignment_prediction( other_type, other_cell_widths,
                                               other_num_entries ) );
    } else {
        predict_generic_split_merge_execution_time(
            prediction_holder, merge.merge_type_, other_type, other_cell_widths,
            other_num_entries );
    }
}
void cost_modeller2::predict_horizontal_column_merge(
    cost_model_prediction_holder& prediction_holder, const merge_stats& merge,
    const partition_type::type& other_type,
    const std::vector<double>&  other_cell_widths,
    uint32_t                    other_num_entries ) const {
    DCHECK( ( merge.merge_type_ == partition_type::MULTI_COLUMN ) or
            ( merge.merge_type_ == partition_type::COLUMN ) );

    if( ( merge.merge_type_ == partition_type::COLUMN ) and
        ( ( other_type == partition_type::COLUMN ) or
          ( other_type == partition_type::SORTED_COLUMN ) ) ) {
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::MEMORY_ASSIGNMENT,
            make_memory_assignment_prediction( other_type, other_cell_widths,
                                               other_num_entries ) );
    } else if( ( merge.merge_type_ == partition_type::MULTI_COLUMN ) and
               ( ( other_type == partition_type::MULTI_COLUMN ) or
                 ( other_type == partition_type::SORTED_MULTI_COLUMN ) ) ) {
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::MEMORY_ASSIGNMENT,
            make_memory_assignment_prediction( other_type, other_cell_widths,
                                               other_num_entries ) );
    } else {
        predict_generic_split_merge_execution_time(
            prediction_holder, merge.merge_type_, other_type, other_cell_widths,
            other_num_entries );
    }
}
void cost_modeller2::predict_horizontal_sorted_column_merge(
    cost_model_prediction_holder& prediction_holder, const merge_stats& merge,
    const partition_type::type& other_type,
    const std::vector<double>&  other_cell_widths,
    uint32_t                    other_num_entries ) const {
    DCHECK( ( merge.merge_type_ == partition_type::SORTED_MULTI_COLUMN ) or
            ( merge.merge_type_ == partition_type::SORTED_COLUMN ) );

    // SORTED_COLUMN
    if( other_type == merge.merge_type_ ) {
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::MEMORY_ASSIGNMENT,
            make_memory_assignment_prediction( other_type, other_cell_widths,
                                               other_num_entries ) );
    } else {
        predict_generic_split_merge_execution_time(
            prediction_holder, merge.merge_type_, other_type, other_cell_widths,
            other_num_entries );
    }
}

cost_model_prediction_holder
    cost_modeller2::predict_vertical_merge_execution_time(
        double site_load, const merge_stats& merge ) const {

    cost_model_prediction_holder prediction_holder;

    std::vector<double> contentions = {merge.left_contention_,
                                       merge.right_contention_};
    predict_common_split_merge_execution_time( prediction_holder, site_load,
                                               contentions );

    DCHECK_EQ( merge.left_num_entries_, merge.right_num_entries_ );

    std::vector<double> widths = merge.left_cell_widths_;
    for( double d : merge.right_cell_widths_ ) {
        widths.emplace_back( d );
    }

    predict_common_split_merge_storage_time(
        prediction_holder, {merge.left_type_, merge.right_type_},
        {merge.left_storage_type_, merge.right_storage_type_},
        {merge.left_cell_widths_, merge.right_cell_widths_},
        {merge.left_num_entries_, merge.right_num_entries_},
        {merge.merge_type_}, {merge.merge_storage_type_}, {widths},
        {merge.left_num_entries_} );

    switch( merge.merge_type_ ) {
        case partition_type::type::ROW: {
            predict_vertical_row_merge( prediction_holder, merge,
                                        merge.left_type_,
                                        merge.left_cell_widths_ );
            predict_vertical_row_merge( prediction_holder, merge,
                                        merge.right_type_,
                                        merge.right_cell_widths_ );
            break;
        }
        case partition_type::type::SORTED_COLUMN: {
            predict_vertical_sorted_column_merge(
                prediction_holder, merge, merge.left_type_,
                merge.left_cell_widths_, true /*is left*/ );
            predict_vertical_sorted_column_merge(
                prediction_holder, merge, merge.right_type_,
                merge.right_cell_widths_, false /* is right*/ );
            break;
        }
        case partition_type::type::COLUMN: {
            predict_vertical_column_merge( prediction_holder, merge,
                                           merge.left_type_,
                                           merge.left_cell_widths_ );
            predict_vertical_column_merge( prediction_holder, merge,
                                           merge.right_type_,
                                           merge.right_cell_widths_ );

            break;
        }
        case partition_type::type::MULTI_COLUMN:
        case partition_type::type::SORTED_MULTI_COLUMN: {
            predict_vertical_multi_column_merge( prediction_holder, merge,
                                                 merge.left_type_,
                                                 merge.left_cell_widths_ );
            predict_vertical_multi_column_merge( prediction_holder, merge,
                                                 merge.right_type_,
                                                 merge.right_cell_widths_ );

            break;
        }
    }

    DVLOG( 40 ) << "Predicted vertical merge execution time:"
                << prediction_holder.get_prediction();

    return prediction_holder;
}
void cost_modeller2::predict_vertical_row_merge(
    cost_model_prediction_holder& prediction_holder, const merge_stats& merge,
    const partition_type::type& other_type,
    const std::vector<double>&  cell_widths ) const {

    DCHECK_EQ( merge.merge_type_, partition_type::ROW );
    DCHECK_EQ( merge.left_num_entries_, merge.right_num_entries_ );

    if( other_type == partition_type::ROW ) {
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::MEMORY_ALLOCATION,
            make_memory_allocation_prediction( other_type, cell_widths,
                                               merge.left_num_entries_ ) );
    } else {
        predict_generic_split_merge_execution_time(
            prediction_holder, merge.merge_type_, other_type, cell_widths,
            merge.left_num_entries_ );
    }
}
void cost_modeller2::predict_vertical_column_merge(
    cost_model_prediction_holder& prediction_holder, const merge_stats& merge,
    const partition_type::type& other_type,
    const std::vector<double>&  cell_widths ) const {

    DCHECK_EQ( merge.merge_type_, partition_type::COLUMN );
    DCHECK_EQ( merge.left_num_entries_, merge.right_num_entries_ );

    if( ( other_type == partition_type::COLUMN ) or
        ( other_type == partition_type::SORTED_COLUMN ) ) {
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::MEMORY_ALLOCATION,
            make_memory_allocation_prediction( other_type, cell_widths,
                                               merge.left_num_entries_ ) );
    } else {
        predict_generic_split_merge_execution_time(
            prediction_holder, merge.merge_type_, other_type, cell_widths,
            merge.left_num_entries_ );
    }
}

void cost_modeller2::predict_vertical_multi_column_merge(
    cost_model_prediction_holder& prediction_holder, const merge_stats& merge,
    const partition_type::type& other_type,
    const std::vector<double>&  cell_widths ) const {

    DCHECK( ( merge.merge_type_ == partition_type::MULTI_COLUMN ) or
            ( merge.merge_type_ == partition_type::SORTED_MULTI_COLUMN ) );
    DCHECK_EQ( merge.left_num_entries_, merge.right_num_entries_ );

    if( ( other_type == partition_type::MULTI_COLUMN ) or
        ( other_type == partition_type::SORTED_MULTI_COLUMN ) ) {
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::MEMORY_ALLOCATION,
            make_memory_allocation_prediction( other_type, cell_widths,
                                               merge.left_num_entries_ ) );
    } else {
        predict_generic_split_merge_execution_time(
            prediction_holder, merge.merge_type_, other_type, cell_widths,
            merge.left_num_entries_ );
    }
}

void cost_modeller2::predict_vertical_sorted_column_merge(
    cost_model_prediction_holder& prediction_holder, const merge_stats& merge,
    const partition_type::type& other_type,
    const std::vector<double>& cell_widths, bool is_left ) const {

    DCHECK_EQ( merge.merge_type_, partition_type::SORTED_COLUMN );
    DCHECK_EQ( merge.left_num_entries_, merge.right_num_entries_ );

    bool is_memory_allocation =
        ( merge.left_type_ == partition_type::SORTED_COLUMN ) and
        ( merge.right_type_ == partition_type::SORTED_COLUMN );

    if( is_memory_allocation ) {
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::MEMORY_ALLOCATION,
            make_memory_allocation_prediction( other_type, cell_widths,
                                               merge.left_num_entries_ ) );
    } else {
        predict_generic_split_merge_execution_time(
            prediction_holder, merge.merge_type_, merge.merge_type_,
            cell_widths, merge.left_num_entries_ );
    }
}

void cost_modeller2::predict_generic_split_merge_execution_time(
    cost_model_prediction_holder& prediction_holder,
    const partition_type::type& ori_type, const partition_type::type dest_type,
    const std::vector<double>& cell_widths, uint32_t num_entries ) const {
    prediction_holder.add_prediction_linearly(
        cost_model_component_type::READ_EXECUTION,
        make_read_time_prediction( ori_type, cell_widths, num_entries ) );
    prediction_holder.add_prediction_linearly(
        cost_model_component_type::WRITE_EXECUTION,
        make_write_time_prediction( dest_type, cell_widths, num_entries ) );
}

void cost_modeller2::predict_common_split_merge_execution_time(
    cost_model_prediction_holder& prediction_holder, double site_load,
    const std::vector<double>& contentions ) const {
    prediction_holder.add_prediction_linearly(
        cost_model_component_type::WAIT_FOR_SERVICE,
        make_wait_for_service_time_prediction( site_load ) );

    for( double contention : contentions ) {
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::LOCK_ACQUISITION,
            make_wait_for_service_time_prediction( contention ) );
    }

    prediction_holder.add_prediction_linearly(
        cost_model_component_type::COMMIT_BUILD_SNAPSHOT,
        make_build_commit_snapshot_time_prediction( 3 ), 2 );
    prediction_holder.add_prediction_linearly(
        cost_model_component_type::COMMIT_SERIALIZE_UPDATE,
        make_commit_serialize_update_time_prediction( 3 ), 2 );
}

void cost_modeller2::predict_common_split_merge_storage_time(
    cost_model_prediction_holder&               prediction_holder,
    const std::vector<partition_type::type>&    ori_types,
    const std::vector<storage_tier_type::type>& ori_storage_types,
    const std::vector<std::vector<double>>&     ori_widths,
    const std::vector<uint32_t>&                ori_num_entries,
    const std::vector<partition_type::type>&    new_types,
    const std::vector<storage_tier_type::type>& new_storage_types,
    const std::vector<std::vector<double>>&     new_widths,
    const std::vector<uint32_t>&                new_num_entries ) const {

    DCHECK_EQ( ori_types.size(), ori_storage_types.size() );
    DCHECK_EQ( ori_types.size(), ori_widths.size() );
    DCHECK_EQ( ori_types.size(), ori_num_entries.size() );

    DCHECK_EQ( new_types.size(), new_storage_types.size() );
    DCHECK_EQ( new_types.size(), new_widths.size() );
    DCHECK_EQ( new_types.size(), new_num_entries.size() );

    for( uint32_t ori_pos = 0; ori_pos < ori_types.size(); ori_pos++ ) {
        if( ori_storage_types.at( ori_pos ) == storage_tier_type::type::DISK ) {
            prediction_holder.add_prediction_linearly(
                cost_model_component_type::PULL_FROM_DISK_EXECUTION,
                make_pull_from_disk_time_prediction(
                    ori_types.at( ori_pos ), ori_widths.at( ori_pos ),
                    ori_num_entries.at( ori_pos ) ) );
        }
    }
    for( uint32_t new_pos = 0; new_pos < new_types.size(); new_pos++ ) {
        if( new_storage_types.at( new_pos ) == storage_tier_type::type::DISK ) {
            prediction_holder.add_prediction_linearly(
                cost_model_component_type::EVICT_TO_DISK_EXECUTION,
                make_pull_from_disk_time_prediction(
                    new_types.at( new_pos ), new_widths.at( new_pos ),
                    new_num_entries.at( new_pos ) ) );
        }
    }
}

cost_model_prediction_holder cost_modeller2::predict_remaster_execution_time(
    double source_load, double dest_load,
    const std::vector<remaster_stats>& remasters ) const {

    cost_model_prediction_holder prediction_holder;

    prediction_holder.add_prediction_linearly(
        cost_model_component_type::WAIT_FOR_SERVICE,
        make_wait_for_service_time_prediction( source_load + dest_load ) );

    for( const auto& stat : remasters ) {
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::LOCK_ACQUISITION,
            make_lock_time_prediction( stat.source_type_, stat.contention_ ) );
        prediction_holder.add_prediction_linearly(
            cost_model_component_type::LOCK_ACQUISITION,
            make_lock_time_prediction( stat.dest_type_, stat.contention_ ) );

        prediction_holder.add_prediction_max(
            cost_model_component_type::WAIT_FOR_SESSION_VERSION,
            make_wait_for_session_version_time_prediction(
                stat.dest_type_, stat.num_updates_ ) );
    }

    // serialization and commit will happen twice. Here the number of pids
    // matters not keys
    prediction_holder.add_prediction_linearly(
        cost_model_component_type::COMMIT_BUILD_SNAPSHOT,
        make_build_commit_snapshot_time_prediction( remasters.size() ), 2 );
    prediction_holder.add_prediction_linearly(
        cost_model_component_type::COMMIT_SERIALIZE_UPDATE,
        make_commit_serialize_update_time_prediction( remasters.size() ), 2 );

    DVLOG( 40 ) << "Predicted remaster execution time:"
                << prediction_holder.get_prediction();

    return prediction_holder;
}

int cost_modeller2::get_predictor3_from_result_by_type(
    const cost_model_prediction_holder& prediction_and_results,
    const cost_model_component_type&    cost_type,
    const partition_type::type&         part_type ) {
    if( ( prediction_and_results.component_types_to_prediction_positions_.count(
              cost_type ) == 0 ) or
        ( prediction_and_results.component_types_to_prediction_positions_
              .at( cost_type )
              .count( part_type ) == 0 ) ) {
        return -1;
    }

    return prediction_and_results.component_types_to_prediction_positions_
        .at( cost_type )
        .at( part_type );
}

std::tuple<int, cost_model_component_type, partition_type::type>
    cost_modeller2::find_predictor3_from_timer(
        int                                 timer_id,
        const cost_model_prediction_holder& prediction_and_results ) {
    std::tuple<cost_model_component_type, partition_type::type> found_type =
        get_cost_model_component_type_from_timer_id( timer_id );
    auto comp_type = std::get<0>( found_type );
    if( comp_type != cost_model_component_type::UNKNOWN_TYPE ) {
        return std::make_tuple<>(
            get_predictor3_from_result_by_type(
                prediction_and_results, comp_type, std::get<1>( found_type ) ),
            comp_type, std::get<1>( found_type ) );
    } else {
        std::vector<std::tuple<cost_model_component_type, partition_type::type>>
            comp_types = get_multiple_cost_model_component_types_from_timer_id(
                timer_id );
        for( const auto& comp_type : comp_types ) {
            int pos = get_predictor3_from_result_by_type(
                prediction_and_results, std::get<0>( comp_type ),
                std::get<1>( comp_type ) );
            if( pos >= 0 ) {
                return std::make_tuple<>( pos, std::get<0>( comp_type ),
                                          std::get<1>( comp_type ) );
            }
        }
    }

    return std::make_tuple<>( -1, cost_model_component_type::UNKNOWN_TYPE,
                              partition_type::type::ROW );
}

void cost_modeller2::add_results(
    cost_model_prediction_holder& prediction_and_results ) {

    // go over all the timers
    for( const auto& timer : prediction_and_results.timers_ ) {
        std::tuple<int /*pos*/, cost_model_component_type, partition_type::type>
            found_res = find_predictor3_from_timer( timer.counter_id,
                                                    prediction_and_results );
        int pos = std::get<0>( found_res );

        DVLOG( 40 ) << "Add results:" << timer.counter_id << ", pos:" << pos;

        // skip if didn't make a prediction on this
        if( pos < 0 ) {
            continue;
        }

        cost_model_component_type comp_type = std::get<1>( found_res );
        predictor3_result&        p_res =
            prediction_and_results.component_predictions_.at( pos );
        p_res.add_actual_output( timer.counter_seen_count *
                                 timer.timer_us_average );

        add_result( comp_type, p_res );

        prediction_and_results.component_types_to_prediction_positions_
            .at( comp_type )
            .erase( std::get<2>( found_res ) );
    }

    for( const auto& component_pos :
         prediction_and_results.component_types_to_prediction_positions_ ) {
        for( const auto& prediction_pos : component_pos.second ) {

            int pos = prediction_pos.second;

            predictor3_result& p_res =
                prediction_and_results.component_predictions_.at( pos );
            // never happend
            DVLOG( 10 )
                << "Cost modeller, no result:"
                << get_cost_model_component_type_name(
                       (cost_model_component_type) prediction_pos.first )
                << ", input:" << p_res.get_input()
                << ", prediction:" << p_res.get_prediction();

#if 0
        p_res.add_actual_output( 0 );
        add_result( (cost_model_component_type) prediction_pos.first, p_res );
#endif
        }
    }
}

// helpers deal with normalization
std::tuple<bool, predictor3_result> cost_modeller2::make_read_time_prediction(
    const partition_type::type& part_type,
    const std::vector<double>& cell_widths, uint32_t num_reads ) const {
    if( num_reads == 0 ) {
        predictor3_result pred;
        return std::make_tuple<>( false, pred );
    }

    return std::make_tuple<>(
        true,
        make_prediction( cost_model_component_type::READ_EXECUTION, part_type,
                         {sum_widths( cell_widths ), (double) num_reads} ) );
}
std::tuple<bool, predictor3_result>
    cost_modeller2::make_read_disk_time_prediction(
        const partition_type::type& part_type,
        const std::vector<double>& cell_widths, uint32_t num_reads ) const {
    if( num_reads == 0 ) {
        predictor3_result pred;
        return std::make_tuple<>( false, pred );
    }

    return std::make_tuple<>(
        true, make_prediction(
                  cost_model_component_type::READ_DISK_EXECUTION, part_type,
                  {sum_widths( cell_widths ), (double) num_reads} ) );
}

std::tuple<bool, predictor3_result> cost_modeller2::make_write_time_prediction(
    const partition_type::type& part_type,
    const std::vector<double>& cell_widths, uint32_t num_writes ) const {
    if( num_writes == 0 ) {
        predictor3_result pred;
        return std::make_tuple<>( false, pred );
    }
    std::vector<double> inputs = {sum_widths( cell_widths ),
                                  (double) num_writes};
    return std::make_tuple<>(
        true, make_prediction( cost_model_component_type::WRITE_EXECUTION,
                               part_type, inputs ) );
}

std::tuple<bool, predictor3_result> cost_modeller2::make_scan_time_prediction(
    const partition_type::type& part_type,
    const std::vector<double>& cell_widths, uint32_t num_entries,
    double selectivity ) const {
    std::vector<double> inputs = {sum_widths( cell_widths ),
                                  (double) num_entries, selectivity};

    return std::make_tuple<>(
        true, make_prediction( cost_model_component_type::SCAN_EXECUTION,
                               part_type, inputs ) );
}
std::tuple<bool, predictor3_result>
    cost_modeller2::make_scan_disk_time_prediction(
        const partition_type::type& part_type,
        const std::vector<double>& cell_widths, uint32_t num_entries,
        double selectivity ) const {
    std::vector<double> inputs = {sum_widths( cell_widths ),
                                  (double) num_entries, selectivity};

    return std::make_tuple<>(
        true, make_prediction( cost_model_component_type::SCAN_DISK_EXECUTION,
                               part_type, inputs ) );
}
std::tuple<bool, predictor3_result>
    cost_modeller2::make_pull_from_disk_time_prediction(
        const partition_type::type& part_type,
        const std::vector<double>& cell_widths, uint32_t num_entries ) const {
    std::vector<double> inputs = {sum_widths( cell_widths ),
                                  (double) num_entries};

    return std::make_tuple<>(
        true,
        make_prediction( cost_model_component_type::PULL_FROM_DISK_EXECUTION,
                         part_type, inputs ) );
}
std::tuple<bool, predictor3_result>
    cost_modeller2::make_evict_to_disk_time_prediction(
        const partition_type::type& part_type,
        const std::vector<double>& cell_widths, uint32_t num_entries ) const {
    std::vector<double> inputs = {sum_widths( cell_widths ),
                                  (double) num_entries};

    return std::make_tuple<>(
        true,
        make_prediction( cost_model_component_type::EVICT_TO_DISK_EXECUTION,
                         part_type, inputs ) );
}

std::tuple<bool, predictor3_result>
    cost_modeller2::make_memory_assignment_prediction(
        const partition_type::type& part_type,
        const std::vector<double>& cell_widths, uint32_t num_entries ) const {
    std::vector<double> inputs = {sum_widths( cell_widths ),
                                  (double) num_entries};

    return std::make_tuple<>(
        true, make_prediction( cost_model_component_type::MEMORY_ASSIGNMENT,
                               part_type, inputs ) );
}

std::tuple<bool, predictor3_result>
    cost_modeller2::make_memory_allocation_prediction(
        const partition_type::type& part_type,
        const std::vector<double>& cell_widths, uint32_t num_entries ) const {
    std::vector<double> inputs = {sum_widths( cell_widths ),
                                  (double) num_entries};
    return std::make_tuple<>(
        true, make_prediction( cost_model_component_type::MEMORY_ALLOCATION,
                               part_type, inputs ) );
}

std::tuple<bool, predictor3_result>
    cost_modeller2::make_memory_deallocation_prediction(
        const std::vector<double>& cell_widths, uint32_t num_entries ) const {
    std::vector<double> inputs = {sum_widths( cell_widths ),
                                  (double) num_entries};

    return std::make_tuple<>(
        true, make_prediction( cost_model_component_type::MEMORY_DEALLOCATION,
                               partition_type::type::ROW, inputs ) );
}

std::tuple<bool, predictor3_result> cost_modeller2::make_lock_time_prediction(
    const partition_type::type& part_type, double contention ) const {
    std::vector<double> inputs = {contention};
    return std::make_tuple<>(
        true, make_prediction( cost_model_component_type::LOCK_ACQUISITION,
                               part_type, inputs ) );
}
std::tuple<bool, predictor3_result>
    cost_modeller2::make_build_commit_snapshot_time_prediction(
        uint32_t write_pid_size ) const {
    if( write_pid_size == 0 ) {
        predictor3_result pred;
        return std::make_tuple<>( false, pred );
    }
    std::vector<double> inputs = {(double) write_pid_size};
    return std::make_tuple<>(
        true, make_prediction( cost_model_component_type::COMMIT_BUILD_SNAPSHOT,
                               partition_type::type::ROW, inputs ) );
}
std::tuple<bool, predictor3_result>
    cost_modeller2::make_commit_serialize_update_time_prediction(
        uint32_t write_pk_size ) const {
    if( write_pk_size == 0 ) {
        predictor3_result pred;
        return std::make_tuple<>( false, pred );
    }
    std::vector<double> inputs = {(double) write_pk_size};

    return std::make_tuple<>(
        true,
        make_prediction( cost_model_component_type::COMMIT_SERIALIZE_UPDATE,
                         partition_type::type::ROW, inputs ) );
}

std::tuple<bool, predictor3_result>
    cost_modeller2::make_distributed_scan_time_prediction(
        double max_lat, double min_lat ) const {
    std::vector<double> inputs = {max_lat, min_lat};
    return std::make_tuple<>(
        true, make_prediction( cost_model_component_type::DISTRIBUTED_SCAN,
                               partition_type::type::ROW, inputs ) );
}

std::tuple<bool, predictor3_result>
    cost_modeller2::make_wait_for_service_time_prediction(
        double site_loads ) const {
    std::vector<double> inputs = {site_loads};
    return std::make_tuple<>(
        true, make_prediction( cost_model_component_type::WAIT_FOR_SERVICE,
                               partition_type::type::ROW, inputs ) );
}

std::tuple<bool, predictor3_result>
    cost_modeller2::make_wait_for_session_version_time_prediction(
        const partition_type::type& part_type,
        double                      estimated_num_updates ) const {
    std::vector<double> inputs = {estimated_num_updates};
    return std::make_tuple<>(
        true,
        make_prediction( cost_model_component_type::WAIT_FOR_SESSION_VERSION,
                         part_type, inputs ) );
}

std::tuple<bool, predictor3_result>
    cost_modeller2::make_wait_for_session_snapshot_time_prediction(
        const partition_type::type& part_type,
        double                      estimated_num_updates ) const {
    std::vector<double> inputs = {estimated_num_updates};
    return std::make_tuple<>(
        true,
        make_prediction( cost_model_component_type::WAIT_FOR_SESSION_SNAPSHOT,
                         part_type, inputs ) );
}

std::tuple<bool, predictor3_result>
    cost_modeller2::make_site_operation_count_prediction(
        double operation_count ) const {
    std::vector<double> inputs = {operation_count};
    return std::make_tuple<>(
        true, make_prediction( cost_model_component_type::SITE_OPERATION_COUNT,
                               partition_type::type::ROW, inputs ) );
}

void cost_modeller2::make_and_add_memory_allocation_predictor3() {
    std::vector<double> model_weights = {
        configs_.widths_weight_, configs_.memory_allocation_num_rows_weight_};
    std::vector<double> normal = {configs_.widths_normalization_,
                                  configs_.memory_num_rows_normalization_};
    std::vector<double> maxes = {configs_.widths_max_input_,
                                 configs_.memory_num_rows_max_input_};

    add_predictor3( cost_model_component_type::MEMORY_ALLOCATION,
                    configs_.read_predictor_type_, model_weights,
                    configs_.memory_allocation_bias_, normal, maxes,
                    configs_.max_time_prediction_scale_ );
}

void cost_modeller2::make_and_add_memory_deallocation_predictor3() {
    std::vector<double> model_weights = {
        configs_.widths_weight_, configs_.memory_deallocation_num_rows_weight_};
    std::vector<double> normal = {configs_.widths_normalization_,
                                  configs_.memory_num_rows_normalization_};
    std::vector<double> maxes = {configs_.widths_max_input_,
                                 configs_.memory_num_rows_max_input_};

    add_predictor3( cost_model_component_type::MEMORY_DEALLOCATION,
                    configs_.read_predictor_type_, model_weights,
                    configs_.memory_deallocation_bias_, normal, maxes,
                    configs_.max_time_prediction_scale_ );
}

void cost_modeller2::make_and_add_memory_assignment_predictor3() {
    std::vector<double> model_weights = {
        configs_.widths_weight_, configs_.memory_assignment_num_rows_weight_};
    std::vector<double> normal = {configs_.widths_normalization_,
                                  configs_.memory_num_rows_normalization_};
    std::vector<double> maxes = {configs_.widths_max_input_,
                                 configs_.memory_num_rows_max_input_};

    add_predictor3( cost_model_component_type::MEMORY_ASSIGNMENT,
                    configs_.read_predictor_type_, model_weights,
                    configs_.memory_assignment_bias_, normal, maxes,
                    configs_.max_time_prediction_scale_ );
}

void cost_modeller2::make_and_add_read_predictor3() {
    std::vector<double> model_weights = {configs_.widths_weight_,
                                         configs_.num_reads_weight_};
    std::vector<double> normal = {configs_.widths_normalization_,
                                  configs_.num_reads_normalization_};
    std::vector<double> maxes = {configs_.widths_max_input_,
                                 configs_.num_reads_normalization_};

    add_predictor3( cost_model_component_type::READ_EXECUTION,
                    configs_.read_predictor_type_, model_weights,
                    configs_.read_bias_, normal, maxes,
                    configs_.max_time_prediction_scale_ );
}
void cost_modeller2::make_and_add_read_disk_predictor3() {
    std::vector<double> model_weights = {configs_.widths_weight_,
                                         configs_.num_reads_disk_weight_};
    std::vector<double> normal = {configs_.widths_normalization_,
                                  configs_.num_reads_normalization_};
    std::vector<double> maxes = {configs_.widths_max_input_,
                                 configs_.num_reads_normalization_};

    add_predictor3( cost_model_component_type::READ_DISK_EXECUTION,
                    configs_.read_disk_predictor_type_, model_weights,
                    configs_.read_disk_bias_, normal, maxes,
                    configs_.max_time_prediction_scale_ );
}

void cost_modeller2::make_and_add_scan_predictor3() {
    std::vector<double> model_weights = {
      configs_.widths_weight_,
      configs_.scan_num_rows_weight_,
      configs_.scan_selectivity_weight_};
    std::vector<double> normal = {configs_.widths_normalization_,
                                  configs_.scan_num_rows_normalization_,
                                  configs_.scan_selectivity_normalization_};
    std::vector<double> maxes = {configs_.widths_max_input_,
                                 configs_.scan_num_rows_max_input_,
                                 configs_.scan_selectivity_max_input_};

    add_predictor3( cost_model_component_type::SCAN_EXECUTION,
                    configs_.scan_predictor_type_, model_weights,
                    configs_.scan_bias_, normal, maxes,
                    configs_.max_time_prediction_scale_ );
}
void cost_modeller2::make_and_add_scan_disk_predictor3() {
    std::vector<double> model_weights = {
      configs_.widths_weight_,
      configs_.scan_disk_num_rows_weight_,
      configs_.scan_disk_selectivity_weight_};
    std::vector<double> normal = {configs_.widths_normalization_,
                                  configs_.scan_num_rows_normalization_,
                                  configs_.scan_selectivity_normalization_};
    std::vector<double> maxes = {configs_.widths_max_input_,
                                 configs_.scan_num_rows_max_input_,
                                 configs_.scan_selectivity_max_input_};

    add_predictor3( cost_model_component_type::SCAN_DISK_EXECUTION,
                    configs_.scan_disk_predictor_type_, model_weights,
                    configs_.scan_disk_bias_, normal, maxes,
                    configs_.max_time_prediction_scale_ );
}

void cost_modeller2::make_and_add_write_predictor3() {
    std::vector<double> model_weights = {configs_.widths_weight_,
                                         configs_.num_writes_weight_};
    std::vector<double> normal = {configs_.widths_normalization_,
                                  configs_.num_writes_normalization_};
    std::vector<double> maxes = {configs_.widths_max_input_,
                                 configs_.num_writes_max_input_};

    add_predictor3( cost_model_component_type::WRITE_EXECUTION,
                    configs_.write_predictor_type_, model_weights,
                    configs_.write_bias_, normal, maxes,
                    configs_.max_time_prediction_scale_ );
}
void cost_modeller2::make_and_add_lock_predictor3() {
    std::vector<double> model_weights = {configs_.lock_weight_};
    std::vector<double> normal = {configs_.lock_normalization_};
    std::vector<double> maxes = {configs_.lock_max_input_};

    add_predictor3( cost_model_component_type::LOCK_ACQUISITION,
                    configs_.lock_predictor_type_, model_weights,
                    configs_.lock_bias_, normal, maxes,
                    configs_.max_time_prediction_scale_ );
}
void cost_modeller2::make_and_add_commit_and_serialize_update_predictor3() {
    std::vector<double> model_weights = {configs_.commit_serialize_weight_};
    std::vector<double> normal = {configs_.commit_serialize_normalization_};
    std::vector<double> maxes = {configs_.commit_serialize_max_input_};

    add_predictor3( cost_model_component_type::COMMIT_SERIALIZE_UPDATE,
                    configs_.commit_serialize_predictor_type_, model_weights,
                    configs_.commit_serialize_bias_, normal, maxes,
                    configs_.max_time_prediction_scale_ );
}
void cost_modeller2::make_and_add_commit_and_build_snapshot_predictor3() {
    std::vector<double> model_weights = {
        configs_.commit_build_snapshot_weight_};
    std::vector<double> normal = {
        configs_.commit_build_snapshot_normalization_};
    std::vector<double> maxes = {configs_.commit_build_snapshot_max_input_};

    add_predictor3( cost_model_component_type::COMMIT_BUILD_SNAPSHOT,

                    configs_.commit_build_snapshot_predictor_type_,
                    model_weights, configs_.commit_build_snapshot_bias_, normal,
                    maxes, configs_.max_time_prediction_scale_ );
}
void cost_modeller2::make_and_add_distributed_scan_predictor3() {
    std::vector<double> model_weights = {configs_.distributed_scan_max_weight_,
                                         configs_.distributed_scan_min_weight_};
    std::vector<double> normal = {configs_.distributed_scan_normalization_,
                                  configs_.distributed_scan_normalization_};
    std::vector<double> maxes = {configs_.distributed_scan_max_input_,
                                 configs_.distributed_scan_max_input_};

    add_predictor3( cost_model_component_type::DISTRIBUTED_SCAN,
                    configs_.distributed_scan_predictor_type_, model_weights,
                    configs_.distributed_scan_bias_, normal, maxes,
                    configs_.max_time_prediction_scale_ );
}
void cost_modeller2::make_and_add_wait_for_service_predictor3() {
    std::vector<double> model_weights = {configs_.wait_for_service_weight_};
    std::vector<double> normal = {configs_.wait_for_service_normalization_};
    std::vector<double> maxes = {configs_.wait_for_service_max_input_};

    add_predictor3( cost_model_component_type::WAIT_FOR_SERVICE,
                    configs_.wait_for_service_predictor_type_, model_weights,
                    configs_.wait_for_service_bias_, normal, maxes,
                    configs_.max_time_prediction_scale_ );
}
void cost_modeller2::make_and_add_site_operation_count_predictor3() {
    std::vector<double> model_weights = {configs_.site_operation_count_weight_};
    std::vector<double> normal = {configs_.site_operation_count_normalization_};
    std::vector<double> maxes = {configs_.site_operation_count_max_input_};

    add_predictor3( cost_model_component_type::SITE_OPERATION_COUNT,

                    configs_.site_operation_count_predictor_type_,
                    model_weights, configs_.site_operation_count_bias_, normal,
                    maxes, configs_.site_operation_count_max_scale_ );
}
void cost_modeller2::make_and_add_wait_for_session_version_predictor3() {
    std::vector<double> model_weights = {configs_.wait_for_session_version_weight_};
    std::vector<double> normal = {configs_.wait_for_session_version_normalization_};
    std::vector<double> maxes = {configs_.wait_for_session_version_max_input_};

    add_predictor3( cost_model_component_type::WAIT_FOR_SESSION_VERSION,
                    configs_.wait_for_session_version_predictor_type_,
                    model_weights, configs_.wait_for_session_version_bias_,
                    normal, maxes, configs_.max_time_prediction_scale_ );
}
void cost_modeller2::make_and_add_wait_for_session_snapshot_predictor3() {
    std::vector<double> model_weights = {
        configs_.wait_for_session_snapshot_weight_};
    std::vector<double> normal = {
        configs_.wait_for_session_snapshot_normalization_};
    std::vector<double> maxes = {configs_.wait_for_session_snapshot_max_input_};

    add_predictor3( cost_model_component_type::WAIT_FOR_SESSION_SNAPSHOT,
                    configs_.wait_for_session_snapshot_predictor_type_,
                    model_weights, configs_.wait_for_session_snapshot_bias_,
                    normal, maxes, configs_.max_time_prediction_scale_ );
}

void cost_modeller2::make_and_add_evict_to_disk_predictor3() {
    std::vector<double> model_weights = {
        configs_.widths_weight_, configs_.evict_to_disk_num_rows_weight_};
    std::vector<double> normal = {configs_.widths_normalization_,
                                  configs_.disk_num_rows_normalization_};
    std::vector<double> maxes = {configs_.widths_max_input_,
                                 configs_.disk_num_rows_max_input_};

    add_predictor3( cost_model_component_type::EVICT_TO_DISK_EXECUTION,
                    configs_.evict_to_disk_predictor_type_, model_weights,
                    configs_.evict_to_disk_bias_, normal, maxes,
                    configs_.max_time_prediction_scale_ );
}

void cost_modeller2::make_and_add_pull_from_disk_predictor3() {
    std::vector<double> model_weights = {
        configs_.widths_weight_, configs_.pull_from_disk_num_rows_weight_};
    std::vector<double> normal = {
        configs_.widths_normalization_,
        configs_.disk_num_rows_normalization_};
    std::vector<double> maxes = {configs_.widths_max_input_,
                                 configs_.disk_num_rows_max_input_};

    add_predictor3( cost_model_component_type::PULL_FROM_DISK_EXECUTION,
                    configs_.pull_from_disk_predictor_type_, model_weights,
                    configs_.pull_from_disk_bias_, normal, maxes,
                    configs_.max_time_prediction_scale_ );
}

void cost_modeller2::make_and_add_site_load_predictor3() {
    std::vector<double> weights = {
        configs_.site_load_prediction_write_weight_,
        configs_.site_load_prediction_read_weight_,
        configs_.site_load_prediction_update_weight_};

    std::vector<double> normals = {
        configs_.site_load_prediction_write_normalization_,
        configs_.site_load_prediction_read_normalization_,
        configs_.site_load_prediction_update_normalization_};

    std::vector<double> max_inputs = {
        configs_.site_load_prediction_write_max_input_,
        configs_.site_load_prediction_read_max_input_,
        configs_.site_load_prediction_update_max_input_};

    add_predictor3( cost_model_component_type::SITE_LOAD,
                    configs_.site_load_predictor_type_, weights,
                    configs_.site_load_prediction_bias_, normals, max_inputs,
                    configs_.site_load_prediction_max_scale_ );
}

void cost_modeller2::add_predictor3(
    const cost_model_component_type& model_type,
    const predictor_type& pred_type, const std::vector<double>& model_weights,
    double init_bias, const std::vector<double>& normal,
    const std::vector<double>& max_input, double bound ) {

    DCHECK_EQ( model_weights.size(), normal.size() );
    DCHECK_EQ( model_weights.size(), max_input.size() );

    predictor_type conv_type = pred_type;
    if( ( conv_type == MULTI_LINEAR_PREDICTOR ) and
        ( model_weights.size() == 1 ) ) {
        conv_type = SINGLE_LINEAR_PREDICTOR;
    }
    int pos = get_predictor3_pos( model_type );
    DCHECK_EQ( pos, predictor3s_.size() );
    DCHECK_EQ( pos, normalizations_.size() );
    DCHECK_EQ( pos, pred_bounds_.size() );

    std::vector<predictor3*> preds;
    for( uint32_t model_pos = 0; model_pos <= k_num_partition_types;
         model_pos++ ) {
        preds.emplace_back( new predictor3(
            conv_type, (partition_type::type) model_pos, model_weights,
            init_bias, configs_.learning_rate_, configs_.regularization_,
            configs_.bias_regularization_, configs_.momentum_,
            configs_.max_internal_model_size_scalar_ * max_input.size(),
            configs_.kernel_gamma_,
            configs_.layer_1_nodes_scalar_ * max_input.size(),
            configs_.layer_2_nodes_scalar_ * max_input.size(), max_input,
            -bound, bound, configs_.is_static_model_ ) );
    }

    predictor3s_.push_back( preds );

    normalizations_.emplace_back( normal );
    pred_bounds_.emplace_back( bound );
}

std::tuple<int, int> cost_modeller2::get_predictor3_pos(
    const cost_model_component_type& model_type,
    const partition_type::type&      part_type ) const {
    DCHECK_LE( part_type, k_num_partition_types );
    return std::make_tuple<>( get_predictor3_pos( model_type ), part_type );
}

int cost_modeller2::get_predictor3_pos(
    const cost_model_component_type& model_type ) const {
    int pos = model_type;

    return pos;
}

predictor3* cost_modeller2::get_predictor3(
    const cost_model_component_type& model_type,
    const partition_type::type&      part_type ) const {
    auto pos = get_predictor3_pos( model_type, part_type );
    DCHECK_LT( std::get<0>( pos ), predictor3s_.size() );
    DCHECK_LT( std::get<1>( pos ),
               predictor3s_.at( std::get<0>( pos ) ).size() );

    return predictor3s_.at( std::get<0>( pos ) ).at( std::get<1>( pos ) );
}

cost_model_prediction_holder cost_modeller2::predict_wait_for_service_time(
    double site_load ) const {
    cost_model_prediction_holder prediction_holder;

    prediction_holder.add_prediction_linearly(
        cost_model_component_type::WAIT_FOR_SERVICE,
        make_wait_for_service_time_prediction( site_load ) );

    return prediction_holder;
}

cost_model_prediction_holder cost_modeller2::predict_site_operation_count(
    double count ) const {
    cost_model_prediction_holder prediction_holder;
    prediction_holder.add_prediction_linearly(
        cost_model_component_type::SITE_OPERATION_COUNT,
        make_site_operation_count_prediction( count ) );
    return prediction_holder;
}

double cost_modeller2::predict_site_load(
    const site_load_information& site_load ) const {
    std::vector<double> input = {site_load.write_count_, site_load.read_count_,
                                 site_load.update_count_};
    auto pred = make_prediction( cost_model_component_type::SITE_LOAD,
                                 partition_type::type::ROW, input );
    return pred.get_prediction();
}

double cost_modeller2::predict_site_load( double write_count, double read_count,
                                          double update_count ) const {
    std::vector<double> input = {write_count, read_count, update_count};
    auto pred = make_prediction( cost_model_component_type::SITE_LOAD,
                                 partition_type::type::ROW, input );
    return pred.get_prediction();
}

std::vector<double> cost_modeller2::get_load_normalization() const {
    return normalizations_.at(
        get_predictor3_pos( cost_model_component_type::SITE_LOAD ) );
}

std::vector<double> cost_modeller2::get_load_model_max_inputs() const {
    return get_predictor3( cost_model_component_type::SITE_LOAD,
                           partition_type::type::ROW )
        ->get_max_inputs();
}


double cost_modeller2::get_site_operation_count( double count ) const {
    auto ret = make_site_operation_count_prediction( count );
    return std::get<1>( ret ).get_prediction();
}
site_load_information cost_modeller2::get_site_operation_counts(
    const site_load_information& input ) const {
    site_load_information output = input;

    output.write_count_ = get_site_operation_count( input.write_count_ );
    output.read_count_ = get_site_operation_count( input.read_count_ );
    output.update_count_ = get_site_operation_count( input.update_count_ );

    return output;
}

cost_model_prediction_holder::cost_model_prediction_holder()
    : prediction_( 0 ),
      component_predictions_(),
      component_types_to_prediction_positions_(),
      timers_() {}

cost_model_prediction_holder::cost_model_prediction_holder(
    double                                prediction,
    const std::vector<predictor3_result>& component_predictions,
    const std::unordered_map<int, std::unordered_map<int, int32_t>>&
        component_types_to_prediction_positions )
    : prediction_( prediction ),
      component_predictions_( component_predictions ),
      component_types_to_prediction_positions_(
          component_types_to_prediction_positions ),
      timers_() {}

void cost_model_prediction_holder::add_prediction_linearly(
    const cost_model_component_type& model_type,
    const std::tuple<bool, predictor3_result>& pred_res_tuple,
    double prediction_multiplier ) {
    if( !std::get<0>( pred_res_tuple ) ) {
        return;
    }
    auto pred_res = std::get<1>( pred_res_tuple );

    bool is_insert = false;
    int pos = component_predictions_.size();
    if( ( component_types_to_prediction_positions_.count( model_type ) == 0 ) or
        ( component_types_to_prediction_positions_.at( model_type )
              .count( pred_res.get_partition_type() ) == 0 ) ) {
        is_insert = true;
        pos = component_predictions_.size();
    } else {
        is_insert = false;
        pos = component_types_to_prediction_positions_.at( model_type )
                  .at( pred_res.get_partition_type() );
    }

    if( is_insert) {
        component_types_to_prediction_positions_[model_type]
                                                [pred_res
                                                     .get_partition_type()] =
                                                    pos;
        component_predictions_.push_back( pred_res );
    } else {
        component_predictions_.at( pos ) =
            add_predictions( component_predictions_.at( pos ), pred_res );
    }
    prediction_ += ( prediction_multiplier * pred_res.get_prediction() );
}

void cost_model_prediction_holder::add_prediction_max(
    const cost_model_component_type& model_type,
    const std::tuple<bool, predictor3_result>& pred_res_tuple ) {
    if( !std::get<0>( pred_res_tuple ) ) {
        return;
    }
    auto   pred_res = std::get<1>( pred_res_tuple );
    double pred_val = pred_res.get_prediction();

    bool is_insert = false;
    int pos = component_predictions_.size();
    if( ( component_types_to_prediction_positions_.count( model_type ) == 0 ) or
        ( component_types_to_prediction_positions_.at( model_type )
              .count( pred_res.get_partition_type() ) == 0 ) ) {
        is_insert = true;
        pos = component_predictions_.size();
    } else {
        is_insert = false;
        pos = component_types_to_prediction_positions_.at( model_type )
                  .at( pred_res.get_partition_type() );
    }

    if (is_insert) {
            component_predictions_.push_back( pred_res );
            prediction_ += pred_val;
            component_types_to_prediction_positions_
                [model_type][pred_res.get_partition_type()] = pos;
    } else {
        double old_pred = component_predictions_.at( pos ).get_prediction();
        if( old_pred < pred_val ) {
            component_predictions_.at( pos ) = pred_res;
            prediction_ += ( ( pred_val - old_pred ) );
        }
    }
}

double cost_model_prediction_holder::get_prediction() const {
    return prediction_;
}
void cost_model_prediction_holder::add_real_timers(
    const std::vector<context_timer>& timers ) {
    timers_ = timers;
}

void cost_model_prediction_holder::add_prediction_linearly(
    const cost_model_component_type& model_type,
    const std::tuple<bool, predictor2_result>& pred_res,
    double prediction_multiplier ) {}
void       cost_model_prediction_holder::add_prediction_max(
    const cost_model_component_type& model_type,
    const std::tuple<bool, predictor2_result>& pred_res ) {}

void merge_context_timer_into_map(
    const context_timer& timer,
    std::unordered_map<int64_t, context_timer>& timer_map ) {

    int64_t timer_id = timer.counter_id;
    auto    search = timer_map.find( timer_id );
    if( search == timer_map.end() ) {
        timer_map[timer_id] = timer;
    } else {
        context_timer& found = search->second;

        found.timer_us_average =
            ( ( found.timer_us_average * found.counter_seen_count ) +
              ( timer.timer_us_average * timer.counter_seen_count ) ) /
            ( timer.counter_seen_count + found.counter_seen_count );
        found.counter_seen_count =
            found.counter_seen_count + timer.counter_seen_count;
    }
}

std::vector<context_timer> merge_context_timers(
    const std::vector<context_timer>& left,
    const std::vector<context_timer>& right ) {

    std::unordered_map<int64_t, context_timer> merged_map;

    for( const auto& ctx : left ) {
        merge_context_timer_into_map( ctx, merged_map );
    }
    for( const auto& ctx : right ) {
        merge_context_timer_into_map( ctx, merged_map );
    }

    std::vector<context_timer> results;
    for( const auto& entry : merged_map ) {
        results.push_back( entry.second );
    }

    return results;
}

double cost_modeller2::get_default_remaster_num_updates_required_count() {
    double normalization =
        normalizations_
            .at( get_predictor3_pos(
                cost_model_component_type::WAIT_FOR_SESSION_SNAPSHOT ) )
            .at( 0 );
    double pct = configs_.wait_for_session_remaster_default_pct_;

    double default_val = normalization * pct;
    double range = ( default_val / 10 ) * ( dists_.get_uniform_double() - 0.5 );

    return default_val + range;
}

double
    cost_modeller2::get_default_previous_results_num_updates_required_count() {
    return 0;
}
double cost_modeller2::get_default_write_num_updates_required_count() {
    return 0;
}

cost_modeller_configs cost_modeller2::get_configs() const { return configs_; }

const std::vector<std::vector<predictor3*>>& cost_modeller2::get_predictors()
    const {
    return predictor3s_;
}

const std::vector<std::vector<double>>& cost_modeller2::get_normalizations()
    const {
    return normalizations_;
}
predictor3_result cost_modeller2::make_prediction(
    const cost_model_component_type& model_type,
    const partition_type::type&      part_type,
    const std::vector<double>&       input ) const {

    auto positions = get_predictor3_pos( model_type, part_type );
    int  model_pos = std::get<0>( positions );

    std::vector<double> normalized_input = input;
    bool                too_large = false;

    DCHECK_EQ( normalized_input.size(), input.size() );

    for( uint32_t pos = 0; pos < input.size(); pos++ ) {
        normalized_input.at( pos ) =
            input.at( pos ) / normalizations_.at( model_pos ).at( pos );

        too_large = too_large or ( normalized_input.at( pos ) >
                                   normalizations_.at( model_pos ).at( pos ) );
    }

    predictor3_result res = predictor3s_.at( model_pos )
                                .at( std::get<1>( positions ) )
                                ->make_prediction_result( normalized_input );
    res.rescale_prediction_to_range( -pred_bounds_.at( model_pos ),
                                     pred_bounds_.at( model_pos ) );

    DVLOG( 40 ) << "Cost modeller type: "
                << get_cost_model_component_type_name( model_type )
                << ", part_type:" << part_type << ", input:" << normalized_input
                << ", raw_input:" << input
                << ", prediction:" << res.get_prediction();
    if( too_large ) {
        VLOG( k_slow_timer_error_log_level )
            << "PREDICTION TOO LARGE, Cost modeller type: "
            << get_cost_model_component_type_name( model_type )
            << ", input:" << normalized_input << ", raw_input:" << input
            << ", normalization:" << normalizations_.at( model_pos )
            << ", prediction:" << res.get_prediction();
    }

    return res;
}

std::ostream& operator<<( std::ostream& os, const cost_modeller2& c ) {
    os << "[ cost_modeller2:";

    const auto& c_predictor3s = c.get_predictors();
    const auto& c_normalizations = c.get_normalizations();

    for( uint32_t i = 0; i < c_predictor3s.size(); i++ ) {
        os << "{ predictor3:" << i
           << ", normalization:" << c_normalizations.at( i );
        for( uint32_t j = 0; j < c_predictor3s.at( i ).size(); j++ ) {
            os << "type pos:" << j
               << ", predictor:" << *( c_predictor3s.at( i ).at( j ) );
        }
        os << " }";
    }

    os << " ]";

    return os;
}

