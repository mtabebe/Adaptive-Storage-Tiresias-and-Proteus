#include "cost_modeller_types.h"

#include <glog/logging.h>

#include "../common/perf_tracking.h"
#include "../data-site/db/cell_identifier.h"

double sum_widths( const std::vector<double>& widths ) {
    double sum = 0;
    for( const double& w : widths ) {
        sum += w;
    }
    return sum;
}

double bucketize_double( double in, double bucket_size ) {
    double bucket_multiplier = 1.0 / bucket_size;
    // four examples
    // input 27, bucket size 10 ==> round(27 * 0.1) = 3, 3 / 0.1 = 30
    // input 27, bucket size 0.1 ==> round (27 * 10) = 270, 270/10 = 27
    // input 0.27, bucket size 10 ==> round (0.27 * 0.1) = 0, 0/0.1 = 0
    // input 0.27, bucket size 0.1 ==> round (0.27 * 10) = 3, 3/10 = 0.3
    double bucket = round( in * bucket_multiplier ) / bucket_multiplier;
    return bucket;
}
int bucketize_int( int in, int bucket_size ) {
    int bucket = ( (int) ( in / bucket_size ) ) * bucket_size;
    return bucket;
}

site_load_information::site_load_information()
    : write_count_( 0 ), read_count_( 0 ), update_count_( 0 ), cpu_load_( 0 ) {}

site_load_information::site_load_information( double write_count,
                                              double read_count,
                                              double update_count,
                                              double cpu_load )
    : write_count_( write_count ),
      read_count_( read_count ),
      update_count_( update_count ),
      cpu_load_( cpu_load ) {}

std::tuple<cost_model_component_type, partition_type::type>
    get_cost_model_component_type_from_timer_id( int timer_id ) {
    // MTODO-HTAP
    partition_type::type part_type = partition_type::type::ROW;
    switch( timer_id ) {
      case ROW_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::LOCK_ACQUISITION,
                                    partition_type::type::ROW );
      case COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::LOCK_ACQUISITION,
                                    partition_type::type::COLUMN );
      case SORTED_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::LOCK_ACQUISITION,
                                    partition_type::type::SORTED_COLUMN );
      case MULTI_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::LOCK_ACQUISITION,
                                    partition_type::type::MULTI_COLUMN );
      case SORTED_MULTI_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::LOCK_ACQUISITION,
                                    partition_type::type::SORTED_MULTI_COLUMN );

      case ROW_READ_RECORD_TIMER_ID:
      case ROW_READ_LATEST_RECORD_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::READ_EXECUTION,
                                    partition_type::type::ROW );
      case COL_READ_RECORD_TIMER_ID:
      case COL_READ_LATEST_RECORD_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::READ_EXECUTION,
                                    partition_type::type::COLUMN );
      case SORTED_COL_READ_RECORD_TIMER_ID:
      case SORTED_COL_READ_LATEST_RECORD_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::READ_EXECUTION,
                                    partition_type::type::SORTED_COLUMN );
      case MULTI_COL_READ_RECORD_TIMER_ID:
      case MULTI_COL_READ_LATEST_RECORD_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::READ_EXECUTION,
                                    partition_type::type::MULTI_COLUMN );
      case SORTED_MULTI_COL_READ_RECORD_TIMER_ID:
      case SORTED_MULTI_COL_READ_LATEST_RECORD_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::READ_EXECUTION,
                                    partition_type::type::SORTED_MULTI_COLUMN );

      case ROW_WRITE_RECORD_TIMER_ID:
      case ROW_INSERT_RECORD_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::WRITE_EXECUTION,
                                    partition_type::type::ROW );
      case COLUMN_WRITE_RECORD_TIMER_ID:
      case COLUMN_INSERT_RECORD_TIMER_ID:
      case COL_REPARTITION_INSERT_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::WRITE_EXECUTION,
                                    partition_type::type::COLUMN );
      case SORTED_COLUMN_WRITE_RECORD_TIMER_ID:
      case SORTED_COLUMN_INSERT_RECORD_TIMER_ID:
      case SORTED_COL_REPARTITION_INSERT_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::WRITE_EXECUTION,
                                    partition_type::type::SORTED_COLUMN );
      case MULTI_COLUMN_WRITE_RECORD_TIMER_ID:
      case MULTI_COLUMN_INSERT_RECORD_TIMER_ID:
      case MULTI_COL_REPARTITION_INSERT_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::WRITE_EXECUTION,
                                    partition_type::type::MULTI_COLUMN );
      case SORTED_MULTI_COLUMN_WRITE_RECORD_TIMER_ID:
      case SORTED_MULTI_COLUMN_INSERT_RECORD_TIMER_ID:
      case SORTED_MULTI_COL_REPARTITION_INSERT_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::WRITE_EXECUTION,
                                    partition_type::type::SORTED_MULTI_COLUMN );

      case ROW_SCAN_RECORDS_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::SCAN_EXECUTION,
                                    partition_type::type::ROW );
      case COL_SCAN_RECORDS_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::SCAN_EXECUTION,
                                    partition_type::type::COLUMN );
      case SORTED_COL_SCAN_RECORDS_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::SCAN_EXECUTION,
                                    partition_type::type::SORTED_COLUMN );
      case MULTI_COL_SCAN_RECORDS_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::SCAN_EXECUTION,
                                    partition_type::type::MULTI_COLUMN );
      case SORTED_MULTI_COL_SCAN_RECORDS_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::SCAN_EXECUTION,
                                    partition_type::type::SORTED_MULTI_COLUMN );

      case ROW_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::WAIT_FOR_SESSION_VERSION,
              partition_type::type::ROW );
      case COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::WAIT_FOR_SESSION_VERSION,
              partition_type::type::COLUMN );
      case SORTED_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::WAIT_FOR_SESSION_VERSION,
              partition_type::type::SORTED_COLUMN );
      case MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::WAIT_FOR_SESSION_VERSION,
              partition_type::type::MULTI_COLUMN );
      case SORTED_MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::WAIT_FOR_SESSION_VERSION,
              partition_type::type::SORTED_MULTI_COLUMN );

      case ROW_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::WAIT_FOR_SESSION_SNAPSHOT,
              partition_type::type::ROW );
      case COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::WAIT_FOR_SESSION_SNAPSHOT,
              partition_type::type::COLUMN );
      case SORTED_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::WAIT_FOR_SESSION_SNAPSHOT,
              partition_type::type::SORTED_COLUMN );
      case MULTI_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::WAIT_FOR_SESSION_SNAPSHOT,
              partition_type::type::MULTI_COLUMN );
      case SORTED_MULTI_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::WAIT_FOR_SESSION_SNAPSHOT,
              partition_type::type::SORTED_MULTI_COLUMN );

      case SERIALIZE_WRITE_BUFFER_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::COMMIT_SERIALIZE_UPDATE, part_type );
      case PARTITION_BUILD_COMMIT_SNAPSHOT_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::COMMIT_BUILD_SNAPSHOT, part_type );
      case RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::WAIT_FOR_SERVICE,
                                    part_type );
      case CHANGE_TYPE_FROM_SORTED_COLUMN_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::MEMORY_ALLOCATION,
              partition_type::COLUMN );
      case CHANGE_TYPE_FROM_SORTED_MULTI_COLUMN_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::MEMORY_ALLOCATION,
              partition_type::MULTI_COLUMN );

      case MERGE_ROW_RECORDS_VERTICALLY_TIMER_ID:
      case SPLIT_ROW_RECORDS_VERTICALLY_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::MEMORY_ALLOCATION,
              partition_type::ROW );

      case MERGE_COL_RECORDS_VERTICALLY_UNSORTED_TIMER_ID:
      case SPLIT_COL_RECORDS_VERTICALLY_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::MEMORY_ALLOCATION,
              partition_type::COLUMN );
      case MERGE_COL_RECORDS_VERTICALLY_SORTED_TIMER_ID:
      case SPLIT_COL_RECORDS_VERTICALLY_SORTED_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::MEMORY_ALLOCATION,
              partition_type::SORTED_COLUMN );

      case SPLIT_MULTI_COL_RECORDS_VERTICALLY_TIMER_ID:
      case MERGE_MULTI_COL_RECORDS_VERTICALLY_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::MEMORY_ALLOCATION,
              partition_type::MULTI_COLUMN );
      case SPLIT_MULTI_SORTED_COL_RECORDS_VERTICALLY_TIMER_ID:
      case MERGE_MULTI_SORTED_COL_RECORDS_VERTICALLY_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::MEMORY_ALLOCATION,
              partition_type::SORTED_MULTI_COLUMN );


      case MERGE_COL_RECORDS_VERTICALLY_SORTED_LEFT_RIGHT_UNSORTED_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::MEMORY_ALLOCATION,
              partition_type::COLUMN );

      case MERGE_ROW_RECORDS_HORIZONTALLY_TIMER_ID:
      case SPLIT_ROW_RECORDS_HORIZONTALLY_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::MEMORY_ASSIGNMENT,
              partition_type::ROW );

      case ROW_RESTORE_FROM_DISK_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::PULL_FROM_DISK_EXECUTION,
              partition_type::type::ROW );
      case COL_RESTORE_FROM_DISK_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::PULL_FROM_DISK_EXECUTION,
              partition_type::type::COLUMN );
      case SORTED_COL_RESTORE_FROM_DISK_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::PULL_FROM_DISK_EXECUTION,
              partition_type::type::SORTED_COLUMN );
      case MULTI_COL_RESTORE_FROM_DISK_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::PULL_FROM_DISK_EXECUTION,
              partition_type::type::MULTI_COLUMN );
      case SORTED_MULTI_COL_RESTORE_FROM_DISK_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::PULL_FROM_DISK_EXECUTION,
              partition_type::type::SORTED_MULTI_COLUMN );

      case ROW_PERSIST_TO_DISK_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::EVICT_TO_DISK_EXECUTION,
              partition_type::type::ROW );
      case COL_PERSIST_TO_DISK_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::EVICT_TO_DISK_EXECUTION,
              partition_type::type::COLUMN );
      case SORTED_COL_PERSIST_TO_DISK_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::EVICT_TO_DISK_EXECUTION,
              partition_type::type::SORTED_COLUMN );
      case MULTI_COL_PERSIST_TO_DISK_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::EVICT_TO_DISK_EXECUTION,
              partition_type::type::MULTI_COLUMN );
      case SORTED_MULTI_COL_PERSIST_TO_DISK_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::EVICT_TO_DISK_EXECUTION,
              partition_type::type::SORTED_MULTI_COLUMN );

      case READ_ROW_FROM_DISK_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::READ_DISK_EXECUTION,
              partition_type::type::ROW );
      case READ_COL_FROM_DISK_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::READ_DISK_EXECUTION,
              partition_type::type::COLUMN );
      case READ_SORTED_COL_FROM_DISK_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::READ_DISK_EXECUTION,
              partition_type::type::SORTED_COLUMN );
      case READ_MULTI_COL_FROM_DISK_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::READ_DISK_EXECUTION,
              partition_type::type::MULTI_COLUMN );
      case READ_SORTED_MULTI_COL_FROM_DISK_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::READ_DISK_EXECUTION,
              partition_type::type::SORTED_MULTI_COLUMN );

      case ROW_SCAN_DISK_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::SCAN_DISK_EXECUTION,
              partition_type::type::ROW );
      case COL_SCAN_DISK_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::SCAN_DISK_EXECUTION,
              partition_type::type::COLUMN );
      case SORTED_COL_SCAN_DISK_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::SCAN_DISK_EXECUTION,
              partition_type::type::SORTED_COLUMN );
      case MULTI_COL_SCAN_DISK_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::SCAN_DISK_EXECUTION,
              partition_type::type::MULTI_COLUMN );
      case SORTED_MULTI_COL_SCAN_DISK_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::SCAN_DISK_EXECUTION,
              partition_type::type::SORTED_MULTI_COLUMN );

      case REMOVE_PARTITION_TIMER_ID:
          return std::make_tuple<>(
              cost_model_component_type::MEMORY_DEALLOCATION,
              partition_type::ROW );

      case SS_EXECUTE_ONE_SHOT_SCAN_AT_SITES_TIMER_ID:
          return std::make_tuple<>( cost_model_component_type::DISTRIBUTED_SCAN,
                                    partition_type::ROW );

      default:
          DVLOG( 40 ) << "Unknown timer id to cost model component type:"
                      << timer_id;
    }
    // return a default
    return std::make_tuple<>( cost_model_component_type::UNKNOWN_TYPE,
                              part_type );
}

std::vector<std::tuple<cost_model_component_type, partition_type::type>>
    get_multiple_cost_model_component_types_from_timer_id( int timer_id ) {
    std::vector<std::tuple<cost_model_component_type, partition_type::type>>
        comp_types;

    switch( timer_id ) {

        case SPLIT_SORTED_COL_RECORDS_HORIZONTALLY_TIMER_ID:
            comp_types.emplace_back(
                std::make_tuple<>( cost_model_component_type::MEMORY_ASSIGNMENT,
                                   partition_type::SORTED_COLUMN ) );
            comp_types.emplace_back(
                std::make_tuple<>( cost_model_component_type::MEMORY_ASSIGNMENT,
                                   partition_type::SORTED_MULTI_COLUMN ) );
            break;
        case SPLIT_COL_RECORDS_HORIZONTALLY_TIMER_ID:
            comp_types.emplace_back(
                std::make_tuple<>( cost_model_component_type::MEMORY_ASSIGNMENT,
                                   partition_type::COLUMN ) );
            comp_types.emplace_back(
                std::make_tuple<>( cost_model_component_type::MEMORY_ASSIGNMENT,
                                   partition_type::MULTI_COLUMN ) );
            break;
        case MERGE_SORTED_COLUMN_RECORDS_HORIZONTALLY_TIMER_ID:
            comp_types.emplace_back(
                std::make_tuple<>( cost_model_component_type::MEMORY_ASSIGNMENT,
                                   partition_type::SORTED_COLUMN ) );
            comp_types.emplace_back(
                std::make_tuple<>( cost_model_component_type::MEMORY_ASSIGNMENT,
                                   partition_type::SORTED_MULTI_COLUMN ) );
            break;
        case MERGE_COLUMN_RECORDS_HORIZONTALLY_TIMER_ID:
            comp_types.emplace_back(
                std::make_tuple<>( cost_model_component_type::MEMORY_ASSIGNMENT,
                                   partition_type::COLUMN ) );
            comp_types.emplace_back(
                std::make_tuple<>( cost_model_component_type::MEMORY_ASSIGNMENT,
                                   partition_type::MULTI_COLUMN ) );
            break;
    }

#if 0
    if( timer_id == TRANSACTION_PARTITION_HOLDER_WAIT_FOR_SESSION_TIMER_ID ) {
        comp_types.emplace_back(
            std::make_tuple<>( cost_model_component_type::WAIT_FOR_SESSION,
                               partition_type::type::ROW ) );
        comp_types.emplace_back( std::make_tuple<>(
            cost_model_component_type::WAIT_FOR_SESSION_REMASTER,
            partition_type::type::ROW ) );
    }
#endif

    return comp_types;
}

std::string get_cost_model_component_type_name(
    const cost_model_component_type& model_type ) {
    switch( model_type ) {
        case READ_EXECUTION:
            return "read_execution";
        case WRITE_EXECUTION:
            return "write_execution";
        case SCAN_EXECUTION:
            return "scan_execution";
        case LOCK_ACQUISITION:
            return "lock_acquisition";
        case COMMIT_SERIALIZE_UPDATE:
            return "commit_serialize_update";
        case COMMIT_BUILD_SNAPSHOT:
            return "commit_build_snapshot";
        case WAIT_FOR_SERVICE:
            return "wait_for_service";
        case WAIT_FOR_SESSION_VERSION:
            return "wait_for_session_version";
        case WAIT_FOR_SESSION_SNAPSHOT:
            return "wait_for_session_snapshot";
        case SITE_OPERATION_COUNT:
            return "site_operation_count";
        case SITE_LOAD:
            return "site_load";
        case MEMORY_ALLOCATION:
            return "memory_allocation";
        case MEMORY_DEALLOCATION:
            return "memory_deallocation";
        case MEMORY_ASSIGNMENT:
            return "memory_assignment";
        case EVICT_TO_DISK_EXECUTION:
            return "evict_to_disk";
        case PULL_FROM_DISK_EXECUTION:
            return "pull_from_disk";
        case READ_DISK_EXECUTION:
            return "read_disk";
        case SCAN_DISK_EXECUTION:
            return "scan_disk";
        case DISTRIBUTED_SCAN:
            return "distributed_scan";

        case UNKNOWN_TYPE:
            return "unknown_type";
        default:
            LOG( FATAL ) << "Unknown model type:" << model_type;
    }
    return "UNKNOWN_TYPE";
}

int transform_timer_id_to_load_input_position( int timer_id ) {
    switch( timer_id ) {
        case PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID:
            return 0;
        case PARTITION_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID:
            return 1;
        case ENQUEUE_STASHED_UPDATE_TIMER_ID:
            return 2;
        default:
            return -1;
    }
    return -1;
}

cost_modeller_configs construct_cost_modeller_configs(
    bool is_static_model, double learning_rate, double regularization,
    double bias_regularization, double momentum, double kernel_gamma,
    uint32_t max_internal_model_size_scalar, uint32_t layer_1_nodes_scalar,
    uint32_t layer_2_nodes_scalar, double max_time_prediction_scale,
    double num_reads_weight, double num_reads_normalization,
    double num_reads_max_input, double read_bias,
    predictor_type read_predictor_type, double num_reads_disk_weight,
    double read_disk_bias, predictor_type read_disk_predictor_type,
    double scan_num_rows_weight, double scan_num_rows_normalization,
    double scan_num_rows_max_input, double scan_selectivity_weight,
    double scan_selectivity_normalization, double scan_selectivity_max_input,
    double scan_bias, predictor_type scan_predictor_type,
    double scan_disk_num_rows_weight, double scan_disk_selectivity_weight,
    double scan_disk_bias, predictor_type scan_disk_predictor_type,
    double num_writes_weight, double num_writes_normalization,
    double num_writes_max_input, double write_bias,
    predictor_type write_predictor_type, double lock_weight,
    double lock_normalization, double lock_max_input, double lock_bias,
    predictor_type lock_predictor_type, double commit_serialize_weight,
    double commit_serialize_normalization, double commit_serialize_max_input,
    double         commit_serialize_bias,
    predictor_type commit_serialize_predictor_type,
    double         commit_build_snapshot_weight,
    double         commit_build_snapshot_normalization,
    double commit_build_snapshot_max_input, double commit_build_snapshot_bias,
    predictor_type commit_build_snapshot_predictor_type,
    double wait_for_service_weight, double wait_for_service_normalization,
    double wait_for_service_max_input, double wait_for_service_bias,
    predictor_type wait_for_service_predictor_type,
    double         wait_for_session_version_weight,
    double         wait_for_session_version_normalization,
    double         wait_for_session_version_max_input,
    double         wait_for_session_version_bias,
    predictor_type wait_for_session_version_predictor_type,
    double         wait_for_session_snapshot_weight,
    double         wait_for_session_snapshot_normalization,
    double         wait_for_session_snapshot_max_input,
    double         wait_for_session_snapshot_bias,
    predictor_type wait_for_session_snapshot_predictor_type,
    double         site_load_prediction_write_weight,
    double         site_load_prediction_write_normalization,
    double         site_load_prediction_write_max_input,
    double         site_load_prediction_read_weight,
    double         site_load_prediction_read_normalization,
    double         site_load_prediction_read_max_input,
    double         site_load_prediction_update_weight,
    double         site_load_prediction_update_normalization,
    double         site_load_prediction_update_max_input,
    double site_load_prediction_bias, double site_load_prediction_max_scale,
    predictor_type site_load_predictor_type, double site_operation_count_weight,
    double site_operation_count_normalization,
    double site_operation_count_max_input,
    double site_operation_count_max_scale, double site_operation_count_bias,
    predictor_type site_operation_count_predictor_type,
    double distributed_scan_max_weight, double distributed_scan_min_weight,
    double distributed_scan_normalization, double distributed_scan_max_input,
    double         distributed_scan_bias,
    predictor_type distributed_scan_predictor_type,
    double memory_num_rows_normalization, double memory_num_rows_max_input,
    double memory_allocation_num_rows_weight, double memory_allocation_bias,
    predictor_type memory_allocation_predictor_type,
    double memory_deallocation_num_rows_weight, double memory_deallocation_bias,
    predictor_type memory_deallocation_predictor_type,
    double memory_assignment_num_rows_weight, double memory_assignment_bias,
    predictor_type memory_assignment_predictor_type,
    double disk_num_rows_normalization, double disk_num_rows_max_input,
    double evict_to_disk_num_rows_weight, double evict_to_disk_bias,
    predictor_type evict_to_disk_predictor_type,
    double pull_from_disk_num_rows_weight, double pull_from_disk_bias,
    predictor_type pull_from_disk_predictor_type, double widths_weight,
    double widths_normalization, double widths_max_input,
    double wait_for_session_remaster_default_pct ) {

    cost_modeller_configs configs;

    configs.is_static_model_ = is_static_model;
    configs.learning_rate_ = learning_rate;
    configs.regularization_ = regularization;
    configs.bias_regularization_ = bias_regularization;

    configs.momentum_ = momentum;
    configs.kernel_gamma_ = kernel_gamma;

    configs.max_internal_model_size_scalar_ = max_internal_model_size_scalar;
    configs.layer_1_nodes_scalar_ = layer_1_nodes_scalar;
    configs.layer_2_nodes_scalar_ = layer_2_nodes_scalar;

    configs.max_time_prediction_scale_ = max_time_prediction_scale;

    configs.num_reads_weight_ = num_reads_weight;
    configs.num_reads_normalization_ = num_reads_normalization;
    configs.num_reads_max_input_ = num_reads_max_input;
    configs.read_bias_ = read_bias;
    configs.read_predictor_type_ = read_predictor_type;

    configs.num_reads_disk_weight_ = num_reads_disk_weight;
    configs.read_disk_bias_ = read_disk_bias;
    configs.read_disk_predictor_type_ = read_disk_predictor_type;

    configs.scan_num_rows_weight_ = scan_num_rows_weight;
    configs.scan_num_rows_normalization_ = scan_num_rows_normalization;
    configs.scan_num_rows_max_input_ = scan_num_rows_max_input;
    configs.scan_selectivity_weight_ = scan_selectivity_weight;
    configs.scan_selectivity_normalization_ = scan_selectivity_normalization;
    configs.scan_selectivity_max_input_ = scan_selectivity_max_input;
    configs.scan_bias_ = scan_bias;
    configs.scan_predictor_type_ = scan_predictor_type;

    configs.scan_disk_num_rows_weight_ = scan_disk_num_rows_weight;
    configs.scan_disk_selectivity_weight_ = scan_disk_selectivity_weight;
    configs.scan_disk_bias_ = scan_disk_bias;
    configs.scan_disk_predictor_type_ = scan_disk_predictor_type;

    configs.num_writes_weight_ = num_writes_weight;
    configs.num_writes_normalization_ = num_writes_normalization;
    configs.num_writes_max_input_ = num_writes_max_input;
    configs.write_bias_ = write_bias;
    configs.write_predictor_type_ = write_predictor_type;

    configs.lock_weight_ = lock_weight;
    configs.lock_normalization_ = lock_normalization;
    configs.lock_max_input_ = lock_max_input;
    configs.lock_bias_ = lock_bias;
    configs.lock_predictor_type_ = lock_predictor_type;

    configs.commit_serialize_weight_ = commit_serialize_weight;
    configs.commit_serialize_normalization_ = commit_serialize_normalization;
    configs.commit_serialize_max_input_ = commit_serialize_max_input;
    configs.commit_serialize_bias_ = commit_serialize_bias;
    configs.commit_serialize_predictor_type_ = commit_serialize_predictor_type;

    configs.commit_build_snapshot_weight_ = commit_build_snapshot_weight;
    configs.commit_build_snapshot_normalization_ =
        commit_build_snapshot_normalization;
    configs.commit_build_snapshot_max_input_ = commit_build_snapshot_max_input;
    configs.commit_build_snapshot_bias_ = commit_build_snapshot_bias;
    configs.commit_build_snapshot_predictor_type_ =
        commit_build_snapshot_predictor_type;

    configs.wait_for_service_weight_ = wait_for_service_weight;
    configs.wait_for_service_normalization_ = wait_for_service_normalization;
    configs.wait_for_service_max_input_ = wait_for_service_max_input;
    configs.wait_for_service_bias_ = wait_for_service_bias;
    configs.wait_for_service_predictor_type_ = wait_for_service_predictor_type;

    configs.wait_for_session_version_weight_ = wait_for_session_version_weight;
    configs.wait_for_session_version_normalization_ =
        wait_for_session_version_normalization;
    configs.wait_for_session_version_max_input_ =
        wait_for_session_version_max_input;
    configs.wait_for_session_version_bias_ = wait_for_session_version_bias;
    configs.wait_for_session_version_predictor_type_ =
        wait_for_session_version_predictor_type;

    configs.wait_for_session_snapshot_weight_ =
        wait_for_session_snapshot_weight;
    configs.wait_for_session_snapshot_normalization_ =
        wait_for_session_snapshot_normalization;
    configs.wait_for_session_snapshot_max_input_ =
        wait_for_session_snapshot_max_input;
    configs.wait_for_session_snapshot_bias_ = wait_for_session_snapshot_bias;
    configs.wait_for_session_snapshot_predictor_type_ =
        wait_for_session_snapshot_predictor_type;

    configs.site_load_prediction_write_weight_ =
        site_load_prediction_write_weight;
    configs.site_load_prediction_write_normalization_ =
        site_load_prediction_write_normalization;
    configs.site_load_prediction_write_max_input_ =
        site_load_prediction_write_max_input;
    configs.site_load_prediction_read_weight_ =
        site_load_prediction_read_weight;
    configs.site_load_prediction_read_normalization_ =
        site_load_prediction_read_normalization;
    configs.site_load_prediction_read_max_input_ =
        site_load_prediction_read_max_input;
    configs.site_load_prediction_update_weight_ =
        site_load_prediction_update_weight;
    configs.site_load_prediction_update_normalization_ =
        site_load_prediction_update_normalization;
    configs.site_load_prediction_update_max_input_ =
        site_load_prediction_update_max_input;
    configs.site_load_prediction_bias_ = site_load_prediction_bias;
    configs.site_load_prediction_max_scale_ = site_load_prediction_max_scale;
    configs.site_load_predictor_type_ = site_load_predictor_type;

    configs.site_operation_count_weight_ = site_operation_count_weight;
    configs.site_operation_count_normalization_ =
        site_operation_count_normalization;
    configs.site_operation_count_max_input_ = site_operation_count_max_input;
    configs.site_operation_count_max_scale_ = site_operation_count_max_scale;
    configs.site_operation_count_bias_ = site_operation_count_bias;
    configs.site_operation_count_predictor_type_ =
        site_operation_count_predictor_type;

    configs.distributed_scan_max_weight_ = distributed_scan_max_weight;
    configs.distributed_scan_min_weight_ = distributed_scan_min_weight;
    configs.distributed_scan_normalization_ = distributed_scan_normalization;
    configs.distributed_scan_max_input_ = distributed_scan_max_input;
    configs.distributed_scan_bias_ = distributed_scan_bias;
    configs.distributed_scan_predictor_type_ = distributed_scan_predictor_type;

    configs.memory_num_rows_normalization_ = memory_num_rows_normalization;
    configs.memory_num_rows_max_input_ = memory_num_rows_max_input;

    configs.memory_allocation_num_rows_weight_ =
        memory_allocation_num_rows_weight;
    configs.memory_allocation_bias_ = memory_allocation_bias;
    configs.memory_allocation_predictor_type_ =
        memory_allocation_predictor_type;

    configs.memory_deallocation_num_rows_weight_ =
        memory_deallocation_num_rows_weight;
    configs.memory_deallocation_bias_ = memory_deallocation_bias;
    configs.memory_deallocation_predictor_type_ =
        memory_deallocation_predictor_type;

    configs.memory_assignment_num_rows_weight_ =
        memory_assignment_num_rows_weight;
    configs.memory_assignment_bias_ = memory_assignment_bias;
    configs.memory_assignment_predictor_type_ =
        memory_assignment_predictor_type;

    configs.disk_num_rows_normalization_ = disk_num_rows_normalization;
    configs.disk_num_rows_max_input_ = disk_num_rows_max_input;

    configs.evict_to_disk_num_rows_weight_ = evict_to_disk_num_rows_weight;
    configs.evict_to_disk_bias_ = evict_to_disk_bias;
    configs.evict_to_disk_predictor_type_ = evict_to_disk_predictor_type;

    configs.pull_from_disk_num_rows_weight_ = pull_from_disk_num_rows_weight;
    configs.pull_from_disk_bias_ = pull_from_disk_bias;
    configs.pull_from_disk_predictor_type_ = pull_from_disk_predictor_type;

    configs.widths_weight_ = widths_weight;
    configs.widths_normalization_ = widths_normalization;
    configs.widths_max_input_ = widths_max_input;

    configs.wait_for_session_remaster_default_pct_ =
        wait_for_session_remaster_default_pct;

    DVLOG( 50 ) << "Constructed Cost Modeller Configs:" << configs;

    return configs;
}

std::ostream& operator<<( std::ostream&                os,
                          const cost_modeller_configs& configs ) {
    os << "Cost Modeller Configs: ["
       << ", is_static_model:" << configs.is_static_model_
       << ", learning_rate:" << configs.learning_rate_
       << ", regularization:" << configs.regularization_
       << ", bias_regularization:" << configs.bias_regularization_
       << ", momentum:" << configs.momentum_
       << ", kernel_gamma:" << configs.kernel_gamma_
       << ", max_internal_model_size_scalar:"
       << configs.max_internal_model_size_scalar_
       << ", layer_1_nodes_scalar:" << configs.layer_1_nodes_scalar_
       << ", layer_2_nodes_scalar:" << configs.layer_2_nodes_scalar_
       << ", max_time_prediction_scale:" << configs.max_time_prediction_scale_
       << ", num_reads_weight:" << configs.num_reads_weight_
       << ", num_reads_normalization:" << configs.num_reads_normalization_
       << ", num_reads_max_input:" << configs.num_reads_max_input_
       << ", read_bias:" << configs.read_bias_ << ", read_predictor_type:"
       << predictor_type_to_string( configs.read_predictor_type_ )
       << ", read_disk_bias:" << configs.read_disk_bias_
       << ", read_disk_predictor_type:"
       << ", num_reads_disk_weight:" << configs.num_reads_disk_weight_
       << predictor_type_to_string( configs.read_disk_predictor_type_ )
       << ", scan_num_rows_weight:" << configs.scan_num_rows_weight_
       << ", scan_num_rows_normalization:"
       << configs.scan_num_rows_normalization_
       << ", scan_num_rows_max_input:" << configs.scan_num_rows_max_input_
       << ", scan_selectivity_weight:" << configs.scan_selectivity_weight_
       << ", scan_selectivity_normalization:"
       << configs.scan_selectivity_normalization_
       << ", scan_selectivity_max_input:" << configs.scan_selectivity_max_input_
       << ", scan_bias:" << configs.scan_bias_ << ", scan_predictor_type:"
       << predictor_type_to_string( configs.scan_predictor_type_ )
       << ", scan_disk_num_rows_weight:" << configs.scan_disk_num_rows_weight_
       << ", scan_disk_selectivity_weight:"
       << configs.scan_disk_selectivity_weight_
       << ", scan_disk_bias:" << configs.scan_disk_bias_
       << ", scan_disk_predictor_type:"
       << predictor_type_to_string( configs.scan_disk_predictor_type_ )
       << ", num_writes_weight:" << configs.num_writes_weight_
       << ", num_writes_normalization:" << configs.num_writes_normalization_
       << ", num_writes_max_input:" << configs.num_writes_max_input_
       << ", write_bias:" << configs.write_bias_ << ", write_predictor_type:"
       << predictor_type_to_string( configs.write_predictor_type_ )
       << ", lock_weight:" << configs.lock_weight_
       << ", lock_normalization:" << configs.lock_normalization_
       << ", lock_max_input:" << configs.lock_max_input_
       << ", lock_bias:" << configs.lock_bias_ << ", lock_predictor_type:"
       << predictor_type_to_string( configs.lock_predictor_type_ )
       << ", commit_serialize_weight:" << configs.commit_serialize_weight_
       << ", commit_serialize_normalization:"
       << configs.commit_serialize_normalization_
       << ", commit_serialize_max_input:" << configs.commit_serialize_max_input_
       << ", commit_serialize_bias:" << configs.commit_serialize_bias_
       << ", commit_serialize_predictor_type:"
       << predictor_type_to_string( configs.commit_serialize_predictor_type_ )
       << ", commit_build_snapshot_weight:"
       << configs.commit_build_snapshot_weight_
       << ", commit_build_snapshot_normalization:"
       << configs.commit_build_snapshot_normalization_
       << ", commit_build_snapshot_max_input:"
       << configs.commit_build_snapshot_max_input_
       << ", commit_build_snapshot_bias:" << configs.commit_build_snapshot_bias_
       << ", commit_build_snapshot_predictor_type:"
       << predictor_type_to_string(
              configs.commit_build_snapshot_predictor_type_ )
       << ", wait_for_service_weight:" << configs.wait_for_service_weight_
       << ", wait_for_service_normalization:"
       << configs.wait_for_service_normalization_
       << ", wait_for_service_max_input:" << configs.wait_for_service_max_input_
       << ", wait_for_service_bias:" << configs.wait_for_service_bias_
       << ", wait_for_service_predictor_type:"
       << predictor_type_to_string( configs.wait_for_service_predictor_type_ )
       << ", wait_for_session_version_weight:"
       << configs.wait_for_session_version_weight_
       << ", wait_for_session_version_normalization:"
       << configs.wait_for_session_version_normalization_
       << ", wait_for_session_version_max_input:"
       << configs.wait_for_session_version_max_input_
       << ", wait_for_session_version_bias:"
       << configs.wait_for_session_version_bias_
       << ", wait_for_session_version_predictor_type:"
       << predictor_type_to_string(
              configs.wait_for_session_version_predictor_type_ )
       << ", wait_for_session_snapshot_weight:"
       << configs.wait_for_session_snapshot_weight_
       << ", wait_for_session_snapshot_normalization:"
       << configs.wait_for_session_snapshot_normalization_
       << ", wait_for_session_snapshot_max_input:"
       << configs.wait_for_session_snapshot_max_input_
       << ", wait_for_session_snapshot_bias:"
       << configs.wait_for_session_snapshot_bias_
       << ", wait_for_session_snapshot_predictor_type:"
       << predictor_type_to_string(
              configs.wait_for_session_snapshot_predictor_type_ )
       << ", site_load_prediction_write_weight:"
       << configs.site_load_prediction_write_weight_
       << ", site_load_prediction_write_normalization:"
       << configs.site_load_prediction_write_normalization_
       << ", site_load_prediction_write_max_input:"
       << configs.site_load_prediction_write_max_input_
       << ", site_load_prediction_read_weight:"
       << configs.site_load_prediction_read_weight_
       << ", site_load_prediction_read_normalization:"
       << configs.site_load_prediction_read_normalization_
       << ", site_load_prediction_read_max_input:"
       << configs.site_load_prediction_read_max_input_
       << ", site_load_prediction_update_weight:"
       << configs.site_load_prediction_update_weight_
       << ", site_load_prediction_update_normalization:"
       << configs.site_load_prediction_update_normalization_
       << ", site_load_prediction_update_max_input:"
       << configs.site_load_prediction_update_max_input_
       << ", site_load_prediction_bias:" << configs.site_load_prediction_bias_
       << ", site_load_prediction_max_scale:"
       << configs.site_load_prediction_max_scale_
       << ", site_load_predictor_type:"
       << predictor_type_to_string( configs.site_load_predictor_type_ )
       << ", site_operation_count_weight:"
       << configs.site_operation_count_weight_
       << ", site_operation_count_normalization:"
       << configs.site_operation_count_normalization_
       << ", site_operation_count_max_input:"
       << configs.site_operation_count_max_input_
       << ", site_operation_count_max_scale:"
       << configs.site_operation_count_max_scale_
       << ", site_operation_count_bias:" << configs.site_operation_count_bias_
       << ", site_operation_count_predictor_type:"
       << predictor_type_to_string(
              configs.site_operation_count_predictor_type_ )
       << ", distributed_scan_max_weight:"
       << configs.distributed_scan_max_weight_
       << ", distributed_scan_min_weight:"
       << configs.distributed_scan_min_weight_
       << ", distributed_scan_normalization:"
       << configs.distributed_scan_normalization_
       << ", distributed_scan_max_input:" << configs.distributed_scan_max_input_
       << ", distributed_scan_bias:" << configs.distributed_scan_bias_
       << ", distributed_scan_predictor_type:"
       << predictor_type_to_string( configs.distributed_scan_predictor_type_ )
       << ", memory_num_rows_normalization:"
       << configs.memory_num_rows_normalization_
       << ", memory_num_rows_max_input:" << configs.memory_num_rows_max_input_
       << ", memory_allocation_num_rows_weight:"
       << configs.memory_allocation_num_rows_weight_
       << ", memory_allocation_bias:" << configs.memory_allocation_bias_
       << ", memory_allocation_predictor_type:"
       << predictor_type_to_string( configs.memory_allocation_predictor_type_ )
       << ", memory_deallocation_num_rows_weight:"
       << configs.memory_deallocation_num_rows_weight_
       << ", memory_deallocation_bias:" << configs.memory_deallocation_bias_
       << ", memory_deallocation_predictor_type:"
       << predictor_type_to_string(
              configs.memory_deallocation_predictor_type_ )
       << ", memory_assignment_num_rows_weight:"
       << configs.memory_assignment_num_rows_weight_
       << ", memory_assignment_bias:" << configs.memory_assignment_bias_
       << ", memory_assignment_predictor_type:"
       << predictor_type_to_string( configs.memory_assignment_predictor_type_ )
       << ", disk_num_rows_normalization:"
       << configs.disk_num_rows_normalization_
       << ", disk_num_rows_max_input:" << configs.disk_num_rows_max_input_
       << ", evict_to_disk_num_rows_weight:"
       << configs.evict_to_disk_num_rows_weight_
       << ", evict_to_disk_bias:" << configs.evict_to_disk_bias_
       << ", evict_to_disk_predictor_type:"
       << predictor_type_to_string( configs.evict_to_disk_predictor_type_ )
       << ", pull_from_disk_num_rows_weight:"
       << configs.pull_from_disk_num_rows_weight_
       << ", pull_from_disk_bias:" << configs.pull_from_disk_bias_
       << ", pull_from_disk_predictor_type:"
       << predictor_type_to_string( configs.pull_from_disk_predictor_type_ )
       << ", widths_weight:" << configs.widths_weight_
       << ", widths_normalization:" << configs.widths_normalization_
       << ", widths_max_input:" << configs.widths_max_input_
       << ", wait_for_session_remaster_default_pct:"
       << configs.wait_for_session_remaster_default_pct_ << " ]";
    return os;
}

std::ostream& operator<<( std::ostream&                os,
                          const site_load_information& load_info ) {
    os << "Site Load Information: [ write_count_:" << load_info.write_count_
       << ", read_count_:" << load_info.read_count_
       << ", update_count_:" << load_info.update_count_
       << ", cpu_load_:" << load_info.cpu_load_ << " ]";

    return os;
}

transaction_prediction_stats::transaction_prediction_stats()
    : part_type_( partition_type::type::ROW ),
      storage_type_( storage_tier_type::type::MEMORY ),
      contention_( 0 ),
      num_updates_needed_( 0 ),
      avg_cell_widths_(),
      num_entries_( 0 ),
      is_scan_( false ),
      scan_selectivity_( 0.0 ),
      is_point_read_( false ),
      num_point_reads_( 0 ),
      is_point_update_( false ),
      num_point_updates_( 0 ) {}
transaction_prediction_stats::transaction_prediction_stats(
    const partition_type::type&    part_type,
    const storage_tier_type::type& storage_type, double contention,
    double num_updates_needed, const std::vector<double>& avg_cell_widths,
    uint32_t num_entries, bool is_scan, double scan_selectivity,
    bool is_point_read, uint32_t num_point_reads, bool is_point_update,
    uint32_t num_point_updates )
    : part_type_( part_type ),
      contention_( contention ),
      num_updates_needed_( num_updates_needed ),
      avg_cell_widths_( avg_cell_widths ),
      num_entries_( num_entries ),
      is_scan_( is_scan ),
      scan_selectivity_( scan_selectivity ),
      is_point_read_( is_point_read ),
      num_point_reads_( num_point_reads ),
      is_point_update_( is_point_update ),
      num_point_updates_( num_point_updates ) {}

transaction_prediction_stats transaction_prediction_stats::bucketize(
    const transaction_prediction_stats& other ) const {
    double bucketized_contention =
        bucketize_double( other.contention_, contention_ );
    double bucketized_num_updates_needed =
        bucketize_double( other.num_updates_needed_, num_updates_needed_ );
    uint32_t bucketized_num_entries =
        bucketize_int( other.num_entries_, num_entries_ );

    double bucketized_scan_selectivity =
        bucketize_double( other.scan_selectivity_, scan_selectivity_ );
    uint32_t bucketized_num_point_reads =
        bucketize_int( other.num_point_reads_, num_point_reads_ );
    uint32_t bucketized_num_point_updates =
        bucketize_int( other.num_point_updates_, num_point_updates_ );

    DCHECK_EQ( 1, avg_cell_widths_.size() );
    std::vector<double> bucketized_cell_widths = {bucketize_double(
        sum_widths( other.avg_cell_widths_ ), avg_cell_widths_.at( 0 ) )};

    return transaction_prediction_stats(
        part_type_ /* use bucketized type */,
        other.storage_type_ /* use their storage type */, bucketized_contention,
        bucketized_num_updates_needed, bucketized_cell_widths,
        bucketized_num_entries, other.is_scan_, bucketized_scan_selectivity,
        other.is_point_read_, bucketized_num_point_reads,
        other.is_point_update_, bucketized_num_point_updates );

    return transaction_prediction_stats();
}

std::ostream& operator<<( std::ostream&                os,
                          const transaction_prediction_stats& stats ) {
    os << "Transaction Prediction Stats: ["
       << " part_type_:" << stats.part_type_
       << ", storage_type_:" << stats.storage_type_
       << ", contention_:" << stats.contention_
       << ", num_updates_needed_:" << stats.num_updates_needed_
       << ", avg_cell_widths_: { ";
    for( const auto& c : stats.avg_cell_widths_ ) {
        os << c << ", ";
    }
    os << " }, num_entries_:" << stats.num_entries_
       << ", is_scan_:" << stats.is_scan_
       << ", scan_selectivity_:" << stats.scan_selectivity_
       << ", is_point_read_:" << stats.is_point_read_
       << ", num_point_reads_:" << stats.num_point_reads_
       << ", is_point_update_:" << stats.is_point_update_
       << ", num_point_updates_:" << stats.num_point_updates_ << "]";

    return os;
}


add_replica_stats::add_replica_stats()
    : source_type_( partition_type::type::ROW ),
      source_storage_type_( storage_tier_type::type::MEMORY ),
      dest_type_( partition_type::type::ROW ),
      dest_storage_type_( storage_tier_type::type::MEMORY ),
      contention_( 0 ),
      avg_cell_widths_(),
      num_entries_( 0 ) {}
add_replica_stats::add_replica_stats(
    const partition_type::type&    source_type,
    const storage_tier_type::type& source_storage_type,
    const partition_type::type&    dest_type,
    const storage_tier_type::type& dest_storage_type, double contention,
    const std::vector<double>& avg_cell_widths, uint32_t num_entries )
    : source_type_( source_type ),
      source_storage_type_( source_storage_type ),
      dest_type_( dest_type ),
      dest_storage_type_( dest_storage_type ),
      contention_( contention ),
      avg_cell_widths_( avg_cell_widths ),
      num_entries_( num_entries ) {}

add_replica_stats add_replica_stats::bucketize(
    const add_replica_stats& other ) const {
    double bucketized_contention =
        bucketize_double( other.contention_, contention_ );
    uint32_t bucketized_num_entries =
        bucketize_int( other.num_entries_, num_entries_ );
    DCHECK_EQ( 1, avg_cell_widths_.size() );
    std::vector<double> bucketized_cell_widths = {bucketize_double(
        sum_widths( other.avg_cell_widths_ ), avg_cell_widths_.at( 0 ) )};

    return add_replica_stats(
        other.source_type_ /* use their source type */,
        other.source_storage_type_ /* use their source storage type */,
        dest_type_, dest_storage_type_
        /* use bucketized destination type */,
        bucketized_contention, bucketized_cell_widths, bucketized_num_entries );
}

remove_replica_stats::remove_replica_stats()
    : part_type_( partition_type::type::ROW ),
      contention_( 0 ),
      avg_cell_widths_(),
      num_entries_( 0 ) {}
remove_replica_stats::remove_replica_stats(
    const partition_type::type& part_type, double contention,
    const std::vector<double>& avg_cell_widths, uint32_t num_entries )
    : part_type_( part_type ),
      contention_( contention ),
      avg_cell_widths_( avg_cell_widths ),
      num_entries_( num_entries ) {}


remaster_stats::remaster_stats()
    : source_type_( partition_type::type::ROW ),
      dest_type_( partition_type::type::ROW ),
      num_updates_( 0 ),
      contention_( 0 ) {}
remaster_stats::remaster_stats( const partition_type::type& source_type,
                                const partition_type::type& dest_type,
                                double num_updates, double contention )
    : source_type_( source_type ),
      dest_type_( dest_type ),
      num_updates_( num_updates ),
      contention_( contention ) {}

split_stats::split_stats()
    : ori_type_( partition_type::type::ROW ),
      left_type_( partition_type::type::ROW ),
      right_type_( partition_type::type::ROW ),
      ori_storage_type_( storage_tier_type::type::MEMORY ),
      left_storage_type_( storage_tier_type::type::MEMORY ),
      right_storage_type_( storage_tier_type::type::MEMORY ),
      contention_( 0 ),
      avg_cell_widths_(),
      num_entries_( 0 ),
      split_point_( k_unassigned_col ) {}
split_stats::split_stats( const partition_type::type&    ori_type,
                          const partition_type::type&    left_type,
                          const partition_type::type&    right_type,
                          const storage_tier_type::type& ori_storage_type,
                          const storage_tier_type::type& left_storage_type,
                          const storage_tier_type::type& right_storage_type,
                          double                         contention,
                          const std::vector<double>&     avg_cell_widths,
                          uint32_t num_entries, uint32_t split_point )
    : ori_type_( ori_type ),
      left_type_( left_type ),
      right_type_( right_type ),
      ori_storage_type_( ori_storage_type ),
      left_storage_type_( left_storage_type ),
      right_storage_type_( right_storage_type ),
      contention_( contention ),
      avg_cell_widths_( avg_cell_widths ),
      num_entries_( num_entries ),
      split_point_( split_point ) {}
split_stats split_stats::bucketize( const split_stats& other ) const {
    double bucketized_contention =
        bucketize_double( other.contention_, contention_ );

    uint32_t bucketized_num_entries =
        bucketize_int( other.num_entries_, num_entries_ );

    DCHECK_EQ( 1, avg_cell_widths_.size() );
    std::vector<double> bucketized_cell_widths = {bucketize_double(
        sum_widths( other.avg_cell_widths_ ), avg_cell_widths_.at( 0 ) )};

    return split_stats( other.ori_type_ /* use their ori type */,
                        left_type_ /* use bucketized left type */,
                        right_type_ /* use bucketized right type */,
                        other.ori_storage_type_ /* use their storage type */,
                        left_storage_type_ /* use bucketized */,
                        right_storage_type_ /* use bucketized */,
                        bucketized_contention, bucketized_cell_widths,
                        bucketized_num_entries,
                        split_point_ /* use split point */ );
}

merge_stats::merge_stats()
    : left_type_( partition_type::type::ROW ),
      right_type_( partition_type::type::ROW ),
      merge_type_( partition_type::type::ROW ),
      left_storage_type_( storage_tier_type::type::MEMORY ),
      right_storage_type_( storage_tier_type::type::MEMORY ),
      merge_storage_type_( storage_tier_type::type::MEMORY ),
      left_contention_( 0 ),
      right_contention_( 0 ),
      left_cell_widths_(),
      right_cell_widths_(),
      left_num_entries_( 0 ),
      right_num_entries_( 0 ) {}
merge_stats::merge_stats( const partition_type::type&    left_type,
                          const partition_type::type&    right_type,
                          const partition_type::type&    merge_type,
                          const storage_tier_type::type& left_storage_type,
                          const storage_tier_type::type& right_storage_type,
                          const storage_tier_type::type& merge_storage_type,
                          double left_contention, double right_contention,
                          const std::vector<double>& left_cell_widths,
                          const std::vector<double>& right_cell_widths,
                          uint32_t                   left_num_entries,
                          uint32_t                   right_num_entries )
    : left_type_( left_type ),
      right_type_( right_type ),
      merge_type_( merge_type ),
      left_storage_type_( left_storage_type ),
      right_storage_type_( right_storage_type ),
      merge_storage_type_( merge_storage_type ),
      left_contention_( left_contention ),
      right_contention_( right_contention ),
      left_cell_widths_( left_cell_widths ),
      right_cell_widths_( right_cell_widths ),
      left_num_entries_( left_num_entries ),
      right_num_entries_( right_num_entries ) {}
merge_stats merge_stats::bucketize( const merge_stats& other ) const {
    double bucketized_left_contention =
        bucketize_double( other.left_contention_, left_contention_ );
    double bucketized_right_contention =
        bucketize_double( other.right_contention_, right_contention_ );

    double bucketized_left_num_entries =
        bucketize_int( other.left_num_entries_, left_num_entries_ );
    double bucketized_right_num_entries =
        bucketize_int( other.right_num_entries_, right_num_entries_ );

    DCHECK_EQ( 1, left_cell_widths_.size() );
    std::vector<double> bucketized_left_cell_widths = {bucketize_double(
        sum_widths( other.left_cell_widths_ ), left_cell_widths_.at( 0 ) )};
    DCHECK_EQ( 1, right_cell_widths_.size() );
    std::vector<double> bucketized_right_cell_widths = {bucketize_double(
        sum_widths( other.right_cell_widths_ ), right_cell_widths_.at( 0 ) )};

    return merge_stats(
        other.left_type_ /* use their left type */,
        other.right_type_ /* use their left type */, merge_type_
        /* use bucketized new type */,
        other.left_storage_type_ /*use their storage type */,
        other.right_storage_type_ /*use their storage type */,
        merge_storage_type_ /* use bucketized storage type */,
        bucketized_left_contention, bucketized_right_contention,
        bucketized_left_cell_widths, bucketized_right_cell_widths,
        bucketized_left_num_entries, bucketized_right_num_entries );
}

change_types_stats::change_types_stats()
    : ori_type_( partition_type::type::ROW ),
      new_type_( partition_type::type::ROW ),
      ori_storage_type_( storage_tier_type::type::MEMORY ),
      new_storage_type_( storage_tier_type::type::MEMORY ),
      contention_( 0 ),
      avg_cell_widths_(),
      num_entries_( 0 ) {}
change_types_stats::change_types_stats(
    const partition_type::type& ori_type, const partition_type::type& new_type,
    const storage_tier_type::type& ori_storage_type,
    const storage_tier_type::type& new_storage_type, double contention,
    const std::vector<double>& avg_cell_widths, uint32_t num_entries )
    : ori_type_( ori_type ),
      new_type_( new_type ),
      ori_storage_type_( ori_storage_type ),
      new_storage_type_( new_storage_type ),
      contention_( contention ),
      avg_cell_widths_( avg_cell_widths ),
      num_entries_( num_entries ) {}

change_types_stats change_types_stats::bucketize(
    const change_types_stats& other ) const {
    double bucketized_contention =
        bucketize_double( other.contention_, contention_ );
    uint32_t bucketized_num_entries =
        bucketize_int( other.num_entries_, num_entries_ );

    DCHECK_EQ( 1, avg_cell_widths_.size() );
    std::vector<double> bucketized_cell_widths = {bucketize_double(
        sum_widths( other.avg_cell_widths_ ), avg_cell_widths_.at( 0 ) )};

    return change_types_stats(
        other.ori_type_ /* use their ori type */, new_type_
        /* use bucketized new type */,
        other.ori_storage_type_ /* use their storage type */, new_storage_type_,
        bucketized_contention, bucketized_cell_widths, bucketized_num_entries );
}


