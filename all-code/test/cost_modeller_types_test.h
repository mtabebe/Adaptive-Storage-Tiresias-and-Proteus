#define GTEST_HAS_TR1_TUPLE 0

#include <gmock/gmock.h>

#include "../src/site-selector/cost_modeller_types.h"

static const cost_modeller_configs k_cost_model_test_configs =
    construct_cost_modeller_configs(
        true /* is_static_model */, 0.01 /* learning_rate */,
        0.01 /* regularization */, 0.001 /* bias_regularization */,
        0.8 /* momentum */, 0.001 /* kernel gamma */,
        50 /* max internal model size scalar */, 10 /* layer 1 scalar */,
        5 /* layer 2 scalar */, 10000 /* max_time_prediction_scale */,
        100 /* num_reads_weight */, 1000 /*num_reads_normalization */,
        10 /*num_reads_max_input */, 0 /* read_bias */,
        predictor_type::MULTI_LINEAR_PREDICTOR /* read_predictor_type */,
        1000 /* num_reads_disk_weight */, 100 /* read_disk_bias */,
        predictor_type::MULTI_LINEAR_PREDICTOR /* read_disk_predictor_type */,
        100 /* scan_num_rows_weight */, 1000 /* scan_num_rows_normalization  */
        ,
        1000 /* scan_num_rows_max_input */, 50 /* scan_selectivity_weight */,
        1 /* scan_selectivity_normalization */,
        1 /* scan_selectivity_max_input */, 0 /* scan_bias */,
        predictor_type::MULTI_LINEAR_PREDICTOR /* scan_predictor_type */,
        1000 /* scan_disk_num_rows_weight */,
        50 /* scan_disk_selectivity_weight */, 100 /* scan_disk_bias */,
        predictor_type::MULTI_LINEAR_PREDICTOR /* scan_disk_predictor_type */,
        200 /* num_writes_weight*/, 1000 /* num_writes_normalization*/,
        10 /* num_writes_max_input */, 0 /* write_bias */,
        predictor_type::MULTI_LINEAR_PREDICTOR /* write_predictor_type*/,
        10 /* lock_weight */, 100 /* lock_normalization */,
        10 /* lock_max_input */, 50 /* lock_bias */,
        predictor_type::MULTI_LINEAR_PREDICTOR /* lock_predictor_type */,
        75 /* commit_serialize_weight */,
        1000 /* commit_serialize_normalization */,
        10 /* commit_serialize_max_input */, 5 /* commit_serialize_bias */,
        predictor_type::
            MULTI_LINEAR_PREDICTOR /* commit_serialize_predictor_type */,
        33 /* commit_build_snapshot_weight */,
        10 /* commit_build_snapshot_normalization */,
        10 /* commit_build_snapshot_max_input  */,
        3 /* commit_build_snapshot_bias */,
        predictor_type::
            MULTI_LINEAR_PREDICTOR /* commit_build_snapshot_predictor_type */,
        1 /* wait_for_service_weight */,
        1200 /* wait_for_service_normalization */,
        10 /* wait_for_service_max_input */, 0 /* wait_for_service_bias */,
        predictor_type::
            MULTI_LINEAR_PREDICTOR /* wait_for_service_predictor_type */,
        1 /* wait_for_session_version_weight */,
        1200 /* wait_for_session_version_normalization */,
        10 /*wait_for_session_version_max_input*/,
        0 /*wait_for_session_version_bias*/,
        predictor_type::
            MULTI_LINEAR_PREDICTOR /* wait_for_session_version_predictor_type */,
        1 /* wait_for_session_snapshot_weight */,
        12000 /* wait_for_session_snapshot_normalization */
        ,
        1 /* wait_for_session_snapshot_max_input */,
        0 /* wait_for_session_snapshot_bias */,
        predictor_type::
            MULTI_LINEAR_PREDICTOR /* wait_for_session_snapshot_predictor_type */,
        10 /*site_load_prediction_write_weight*/,
        1000 /*site_load_prediction_write_normalization*/,
        10 /*site_load_prediction_write_max_input*/,
        5 /*site_load_prediction_read_weight*/,
        1000 /*site_load_prediction_read_normalization*/,
        10 /*site_load_prediction_read_max_input*/,
        2 /*site_load_prediction_update_weight*/,
        1000 /*site_load_prediction_update_normalization*/,
        10 /*site_load_prediction_update_max_input*/,
        1 /*site_load_prediction_bias*/,
        1200 /* site_load_prediction_max_scale_ */,
        predictor_type::MULTI_LINEAR_PREDICTOR /* site_load_predictor_type */,
        1 /*site_operation_count_weight*/,
        10 /*site_operation_count_normalization*/,
        10 /*site_operation_count_max_input*/,
        1000 /* site_operation_count_max_scale_ */,
        0 /*site_operation_count_bias */,
        predictor_type::
            MULTI_LINEAR_PREDICTOR /* site_operation_count_predictor_type */,
        1.2 /* distributed_scan_max_weight */,
        0.5 /* distributed_scan_min_weight */,
        10 /* distributed_scan_normalization */,
        100 /* distributed_scan_max_input */, 0 /* distributed_scan_bias */,
        predictor_type::
            MULTI_LINEAR_PREDICTOR /* distributed_scan_predictor_type */,
        1000 /* memory_num_rows_normalization */,
        100 /* memory_num_rows_max_input */,
        0.1 /* memory_allocation_num_rows_weight */,
        0 /* memory_allocation_bias */,
        predictor_type::
            MULTI_LINEAR_PREDICTOR /*memory_allocation_predictor_type */,
        0.1 /* memory_deallocation_num_rows_weight */,
        0 /* memory_deallocation_bias */,
        predictor_type::
            MULTI_LINEAR_PREDICTOR /* memory_deallocation_predictor_type */,
        0.1 /* memory_assignment_num_rows_weight */,
        0 /* memory_assignment_bias */,
        predictor_type::
            MULTI_LINEAR_PREDICTOR /* memory_assignment_predictor_type  */,
        1000 /* disk_num_rows_normalization */,
        100 /* disk_num_rows_max_input */,
        10 /* evict_to_disk_num_rows_weight */, 100 /* evict_to_disk_bias */,
        predictor_type::
            MULTI_LINEAR_PREDICTOR /* evict_to_disk_predictor_type */,
        10 /* pull_from_disk_num_rows_weight */, 100 /* pull_from_disk_bias */,
        predictor_type::
            MULTI_LINEAR_PREDICTOR /* pull_from_disk_predictor_type */,
        1 /* widths_weight */, 100 /* widths_normalization */,
        10 /* widths_max_input */,
        0.5 /* cost_model_wait_for_session_remaster_default_pct */ );

