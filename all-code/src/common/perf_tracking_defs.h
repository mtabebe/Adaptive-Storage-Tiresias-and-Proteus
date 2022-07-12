#pragma once

// definitions
#define ARS_ADD_TO_ACROSS_CORR_TIMER_ID 1
#define ARS_ADD_TO_WITHIN_CORR_TIMER_ID 2
#define ARS_PUT_NEW_SAMPLE_TIMER_ID 3
#define ARS_REMOVE_FROM_ACROSS_CORR_TIMER_ID 4
#define ARS_REMOVE_FROM_WITHIN_CORR_TIMER_ID 5
#define ARS_REMOVE_SKEW_COUNTS_FROM_SITES_TIMER_ID 6
#define BEGIN_FOR_DEST_TIMER_ID 7
#define BEGIN_FOR_WRITE_TIMER_ID 8
#define CHANGE_TYPE_FROM_SORTED_COLUMN_TIMER_ID 9
#define COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID 10
#define COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID 11
#define COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID 12
#define COL_READ_LATEST_RECORD_TIMER_ID 13
#define COL_READ_RECORD_TIMER_ID 14
#define COL_REPARTITION_INSERT_TIMER_ID 15
#define COL_SCAN_RECORDS_TIMER_ID 16
#define COLUMN_INSERT_RECORD_TIMER_ID 17
#define COLUMN_WRITE_RECORD_TIMER_ID 18
#define ENQUEUE_STASHED_UPDATE_TIMER_ID 19
#define HOLDER_GET_AND_SET_PARTITION_TIMER_ID 20
#define HOLDER_GET_PARTITION_TIMER_ID 21
#define KAFKA_ENQUEUE_UPDATES_FOR_TOPIC_TIMER_ID 22
#define KAFKA_SEEK_TOPIC_TIMER_ID 23
#define KAFKA_SEND_UPDATE_TIMER_ID 24
#define MERGE_COL_RECORDS_VERTICALLY_SORTED_LEFT_RIGHT_UNSORTED_TIMER_ID 25
#define MERGE_COL_RECORDS_VERTICALLY_SORTED_TIMER_ID 26
#define MERGE_COL_RECORDS_VERTICALLY_UNSORTED_TIMER_ID 27
#define MERGE_COLUMN_RECORDS_HORIZONTALLY_TIMER_ID 28
#define MERGE_ROW_RECORDS_HORIZONTALLY_TIMER_ID 29
#define MERGE_ROW_RECORDS_VERTICALLY_TIMER_ID 30
#define MERGE_SNAPSHOTS_TIMER_ID 31
#define MERGE_SORTED_COLUMN_RECORDS_HORIZONTALLY_TIMER_ID 32
#define PARTITION_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID 33
#define PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID 34
#define PARTITION_BUILD_COMMIT_SNAPSHOT_TIMER_ID 35
#define PARTITION_COMMIT_WRITE_TRANSACTION_TIMER_ID 36
#define PARTITION_EXECUTE_DESERIALIZED_PARTITION_UPDATE_TIMER_ID 37
#define PARTITION_GET_MOST_RECENT_SNAPSHOT_VECTOR_TIMER_ID 38
#define PARTITION_INIT_PARTITION_FROM_SNAPSHOT_TIMER_ID 39
#define PARTITION_WAIT_UNTIL_VERSION_OR_APPLY_UPDATES_TIMER_ID 40
#define PERFORM_REMASTER_TIMER_ID 41
#define REMASTER_BEGIN_TIMER_ID 42
#define REMASTER_FINALIZE_TIMER_ID 43
#define REMOVE_PARTITION_TIMER_ID 44
#define REPARTITION_RECORD_TIMER_ID 45
#define ROW_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID 46
#define ROW_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID 47
#define ROW_INSERT_RECORD_TIMER_ID 48
#define ROW_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID 49
#define ROW_READ_LATEST_RECORD_TIMER_ID 50
#define ROW_READ_RECORD_TIMER_ID 51
#define ROW_SCAN_RECORDS_TIMER_ID 52
#define ROW_WRITE_RECORD_TIMER_ID 53
#define RPC_ABORT_TRANSACTION_TIMER_ID 54
#define RPC_ADD_PARTITIONS_TIMER_ID 55
#define RPC_ADD_REPLICA_PARTITIONS_TIMER_ID 56
#define RPC_BEGIN_TRANSACTION_TIMER_ID 57
#define RPC_CHANGE_PARTITION_CHANGE_TYPE_TIMER_ID 58
#define RPC_CHANGE_PARTITION_OUTPUT_DESTINATION_TIMER_ID 59
#define RPC_COMMIT_TRANSACTION_TIMER_ID 60
#define RPC_DELETE_TIMER_ID 61
#define RPC_GRANT_MASTERSHIP_TIMER_ID 62
#define RPC_INSERT_TIMER_ID 63
#define RPC_MERGE_PARTITION_TIMER_ID 64
#define RPC_ONE_SHOT_SPROC_TIMER_ID 65
#define RPC_RELEASE_MASTERSHIP_TIMER_ID 66
#define RPC_REMOVE_PARTITIONS_TIMER_ID 67
#define RPC_SELECT_TIMER_ID 68
#define RPC_SNAPSHOT_PARTITIONS_TIMER_ID 69
#define RPC_SPLIT_PARTITION_TIMER_ID 70
#define RPC_SPROC_DESERIALIZE_TIMER_ID 71
#define RPC_SPROC_EXECUTE_TIMER_ID 72
#define RPC_STORED_PROCEDURE_TIMER_ID 73
#define RPC_UPDATE_TIMER_ID 74
#define SEMAPHORE_LOCK_TIMER_ID 75
#define SEMAPHORE_TRY_LOCK_TIMER_ID 76
#define SERIALIZE_WRITE_BUFFER_TIMER_ID 77
#define SMALLBANK_WORKLOAD_OP_TIMER_ID 78
#define SORTED_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID 79
#define SORTED_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID 80
#define SORTED_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID 81
#define SORTED_COL_READ_LATEST_RECORD_TIMER_ID 82
#define SORTED_COL_READ_RECORD_TIMER_ID 83
#define SORTED_COL_REPARTITION_INSERT_TIMER_ID 84
#define SORTED_COL_SCAN_RECORDS_TIMER_ID 85
#define SORTED_COLUMN_INSERT_RECORD_TIMER_ID 86
#define SORTED_COLUMN_WRITE_RECORD_TIMER_ID 87
#define SPLIT_COL_RECORDS_HORIZONTALLY_TIMER_ID 88
#define SPLIT_COL_RECORDS_VERTICALLY_SORTED_TIMER_ID 89
#define SPLIT_COL_RECORDS_VERTICALLY_TIMER_ID 90
#define SPLIT_ROW_RECORDS_HORIZONTALLY_TIMER_ID 91
#define SPLIT_ROW_RECORDS_VERTICALLY_TIMER_ID 92
#define SPLIT_SORTED_COL_RECORDS_HORIZONTALLY_TIMER_ID 93
#define SPROC_YCSB_CKR_INSERTS_TIMER_ID 94
#define SPROC_YCSB_DELETE_RECORD_TIMER_ID 95
#define SPROC_YCSB_INSERT_RECORDS_TIMER_ID 96
#define SPROC_YCSB_MK_RMW_TIMER_ID 97
#define SPROC_YCSB_READ_RECORD_TIMER_ID 98
#define SPROC_YCSB_RMW_RECORD_TIMER_ID 99
#define SPROC_YCSB_SCAN_RECORDS_TIMER_ID 100
#define SPROC_YCSB_UPDATE_RECORD_TIMER_ID 101
#define SS_ADD_PREVIOUS_CONTEXT_TIMER_INFORMATION_TIMER_ID 102
#define SS_ADD_REMOVE_REPLICAS_TO_PLAN_TIMER_ID 103
#define SS_ADD_SPINUP_REPLICAS_TO_PLAN_TIMER_ID 104
#define SS_CALL_RPC_ONE_SHOT_SPROC_TIMER_ID 105
#define SS_DATA_LOCATION_GET_OR_CREATE_PARTITIONS_AND_GROUP_TIMER_ID 106
#define SS_DATA_LOCATION_GET_OR_CREATE_PARTITIONS_TIMER_ID 107
#define SS_DATA_LOCATION_GET_OR_CREATE_PARTITION_TIMER_ID 108
#define SS_DATA_LOCATION_GET_PARTITIONS_AND_GROUP_TIMER_ID 109
#define SS_DATA_LOCATION_GET_PARTITIONS_TIMER_ID 110
#define SS_DATA_LOCATION_GET_PARTITION_TIMER_ID 111
#define SS_DATA_LOCATION_INSERT_PARTITION_TIMER_ID 112
#define SS_DATA_LOCATION_MERGE_PARTITION_TIMER_ID 113
#define SS_DATA_LOCATION_REMOVE_PARTITION_TIMER_ID 114
#define SS_DATA_LOCATION_SPLIT_PARTITION_TIMER_ID 115
#define SS_EXECUTE_ADD_PARTITIONS_ACTION_TIMER_ID 116
#define SS_EXECUTE_ADD_REPLICA_PARTITIONS_ACTION_TIMER_ID 117
#define SS_EXECUTE_CHANGE_PARTITIONS_OUTPUT_DESTINATION_ACTION_TIMER_ID 118
#define SS_EXECUTE_CHANGE_PARTITIONS_TYPE_ACTION_TIMER_ID 119
#define SS_EXECUTE_MERGE_PARTITIONS_ACTION_TIMER_ID 120
#define SS_EXECUTE_ONE_SHOT_SPROC_WITH_FORCE_STRATEGY_TIMER_ID 121
#define SS_EXECUTE_ONE_SHOT_SPROC_WITH_STRATEGY_TIMER_ID 122
#define SS_EXECUTE_PRE_TRANSACTION_PLAN_ACTIONS_WORKER_TIMER_ID 123
#define SS_EXECUTE_PRE_TRANSACTION_PLAN_TIMER_ID 124
#define SS_EXECUTE_REMASTER_PARTITIONS_ACTION_TIMER_ID 125
#define SS_EXECUTE_REMOVE_PARTITIONS_ACTION_TIMER_ID 126
#define SS_EXECUTE_SPLIT_PARTITIONS_ACTION_TIMER_ID 127
#define SS_EXECUTE_TRANSFER_REMASTER_PARTITIONS_ACTION_TIMER_ID 128
#define SS_GENERATE_INSERT_ONLY_PARTITION_DESTINATION_TIMER_ID 129
#define SS_GENERATE_NO_CHANGE_DESTINATION_PLAN_IF_POSSIBLE_TIMER_ID 130
#define SS_GENERATE_NO_CHANGE_DESTINATION_TIMER_ID 131
#define SS_GENERATE_OPTIMISTIC_NO_CHANGE_PLAN_TIMER_ID 132
#define SS_GENERATE_PLAN_FOR_SITE_ARGS_TIMER_ID 133
#define SS_GENERATE_PLAN_FOR_SITE_TIMER_ID 134
#define SS_GENERATE_PLAN_TIMER_ID 135
#define SS_GENERATE_PLAN_WITH_PHYSICAL_ADJUSTMENTS_TIMER_ID 136
#define SS_GET_INVOLVED_SITES_TIMER_ID 137
#define SS_GET_NO_CHANGE_DESTINATIONS_POSSIBILITIES_TIMER_ID 138
#define SS_GET_PARTITIONS_TO_ADJUST_TIMER_ID 139
#define SS_GET_PRE_TRANSACTION_EXECUTION_PLAN_TIMER_ID 140
#define SS_INCREASE_TRACKING_COUNTS_TIMER_ID 141
#define SS_INTERNAL_DATA_LOCATION_GET_OR_CREATE_PARTITION_TIMER_ID 142
#define SS_INTERNAL_DATA_LOCATION_GET_PARTITION_TIMER_ID 143
#define SS_PLAN_ADD_NECESSARY_REMASTERING_TIMER_ID 144
#define SS_PLAN_ADD_NECESSARY_REPLICAS_TIMER_ID 145
#define SS_PLAN_BUILD_SAMPLED_TRANSACTIONS_TIMER_ID 146
#define SS_PLAN_CHANGE_PARTITION_TYPES_TIMER_ID 147
#define SS_PLAN_REMOVE_UN_NEEDED_REPLICAS_TIMER_ID 148
#define SS_PLAN_TACK_ON_BENEFICIAL_SPIN_UPS_TIMER_ID 149
#define SS_PREPARE_TRANSACTION_DATA_ITEMS_FOR_SINGLE_SITE_EXECUTION_TIMER_ID 150
#define SS_REMOVE_UN_NEEDED_REPLICAS_FOR_SAMPLE_TIMER_ID 151
#define SS_RPC_BEGIN_TRANSACTION_TIMER_ID 152
#define SS_RPC_ONE_SHOT_SPROC_TRANSACTION_TIMER_ID 153
#define SS_SAMPLE_TRANSACTION_ACCESSES_TIMER_ID 154
#define SS_SORT_FINALIZE_AND_ADD_METADATA_TO_PLAN_TIMER_ID 155
#define SS_TACK_ON_BENEFICIAL_SPIN_UPS_FOR_SAMPLE_TIMER_ID 156
#define SS_WAIT_FOR_DOWNGRADE_TO_COMPLETE_AND_RELEASE_PARTITIONS_TIMER_ID 157
#define TRANSACTION_PARTITION_HOLDER_WAIT_FOR_SESSION_TIMER_ID 158
#define UPDATE_ENQUEUERS_ADD_SOURCE_TIMER_ID 159
#define UPDATE_ENQUEUERS_CHANGE_SOURCES_TIMER_ID 160
#define UPDATE_ENQUEUERS_REMOVE_SOURCE_TIMER_ID 161
#define UPDATE_QUEUE_GET_NEXT_UPDATE_TIMER_ID 162
#define UPDATE_SVV_FROM_RESULT_SET_TIMER_ID 163
#define CHANGE_TYPE_FROM_SORTED_MULTI_COLUMN_TIMER_ID 164
#define MULTI_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID 165
#define MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID 166
#define MULTI_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID 167
#define MULTI_COL_READ_LATEST_RECORD_TIMER_ID 168
#define MULTI_COL_READ_RECORD_TIMER_ID 169
#define MULTI_COL_REPARTITION_INSERT_TIMER_ID 170
#define MULTI_COL_SCAN_RECORDS_TIMER_ID 171
#define MULTI_COLUMN_INSERT_RECORD_TIMER_ID 172
#define MULTI_COLUMN_WRITE_RECORD_TIMER_ID 173
#define SORTED_MULTI_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID 174
#define SORTED_MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID 175
#define SORTED_MULTI_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID 176
#define SORTED_MULTI_COL_READ_LATEST_RECORD_TIMER_ID 177
#define SORTED_MULTI_COL_READ_RECORD_TIMER_ID 178
#define SORTED_MULTI_COL_REPARTITION_INSERT_TIMER_ID 179
#define SORTED_MULTI_COL_SCAN_RECORDS_TIMER_ID 180
#define SORTED_MULTI_COLUMN_INSERT_RECORD_TIMER_ID 181
#define SORTED_MULTI_COLUMN_WRITE_RECORD_TIMER_ID 182
#define SPLIT_MULTI_COL_RECORDS_VERTICALLY_TIMER_ID 183
#define MERGE_MULTI_COL_RECORDS_VERTICALLY_TIMER_ID 184
#define SPLIT_MULTI_SORTED_COL_RECORDS_VERTICALLY_TIMER_ID 185
#define MERGE_MULTI_SORTED_COL_RECORDS_VERTICALLY_TIMER_ID 186
#define RPC_CHANGE_PARTITION_STORAGE_TYPE_TIMER_ID 187
#define SS_EXECUTE_CHANGE_STORAGE_TYPE_ACTION_TIMER_ID 188
#define ROW_RESTORE_FROM_DISK_TIMER_ID 189
#define COL_RESTORE_FROM_DISK_TIMER_ID 190
#define SORTED_COL_RESTORE_FROM_DISK_TIMER_ID 191
#define MULTI_COL_RESTORE_FROM_DISK_TIMER_ID 192
#define SORTED_MULTI_COL_RESTORE_FROM_DISK_TIMER_ID 193
#define ROW_PERSIST_TO_DISK_TIMER_ID 194
#define COL_PERSIST_TO_DISK_TIMER_ID 195
#define SORTED_COL_PERSIST_TO_DISK_TIMER_ID 196
#define MULTI_COL_PERSIST_TO_DISK_TIMER_ID 197
#define SORTED_MULTI_COL_PERSIST_TO_DISK_TIMER_ID 198
#define READ_ROW_FROM_DISK_TIMER_ID 199
#define READ_COL_FROM_DISK_TIMER_ID 200
#define READ_SORTED_COL_FROM_DISK_TIMER_ID 201
#define READ_MULTI_COL_FROM_DISK_TIMER_ID 202
#define READ_SORTED_MULTI_COL_FROM_DISK_TIMER_ID 203
#define ROW_SCAN_DISK_TIMER_ID 204
#define COL_SCAN_DISK_TIMER_ID 205
#define SORTED_COL_SCAN_DISK_TIMER_ID 206
#define MULTI_COL_SCAN_DISK_TIMER_ID 207
#define SORTED_MULTI_COL_SCAN_DISK_TIMER_ID 208
#define RPC_ONE_SHOT_SCAN_TIMER_ID 209
#define RPC_SCAN_EXECUTE_TIMER_ID 210
#define RPC_SCAN_TIMER_ID 211
#define SS_RPC_ONE_SHOT_SCAN_TRANSACTION_TIMER_ID 212
#define SS_CALL_RPC_ONE_SHOT_SCAN_TIMER_ID 213
#define SS_EXECUTE_ONE_SHOT_SCAN_AT_SITES_TIMER_ID 214
#define SS_PREPARE_TRANSACTION_DATA_ITEMS_FOR_SCAN_EXECUTION_TIMER_ID 215
#define SS_GENERATE_OPTIMISTIC_NO_CHANGE_SCAN_PLAN_TIMER_ID 216
#define SS_GENERATE_SCAN_PLAN_WITH_PHYSICAL_ADJUSTMENTS_TIMER_ID 217
#define SS_SORT_FINALIZE_AND_ADD_METADATA_TO_SCAN_PLAN_TIMER_ID 218
#define SS_GENERATE_NO_CHANGE_DESTINATION_SCAN_PLAN_IF_POSSIBLE_TIMER_ID 219
#define SS_GET_PRE_TRANSACTION_EXECUTION_SCAN_PLAN_TIMER_ID 220
#define TPCC_WORKLOAD_OP_TIMER_ID 221
#define TPCH_WORKLOAD_OP_TIMER_ID 222
#define YCSB_WORKLOAD_OP_TIMER_ID 223
#define TWITTER_WORKLOAD_OP_TIMER_ID 224

#define CRACK_ADD_CRACK_TIMER_ID 225
#define CRACK_COUNT_QUERY_TIMER_ID 226
#define CRACK_CRACK_TIMER_ID 227
#define CRACK_FIND_PIECE_TIMER_ID 228
#define CRACK_INSERT_TIMER_ID 229
#define CRACK_MERGE_RIPPLE_TIMER_ID 230
#define CRACK_PARTITION_TIMER_ID 231
#define CRACK_REMOVE_TIMER_ID 232
#define CRACK_VIEW_QUERY_TIMER_ID 233
#define CRACK_SPLIT_AB_TIMER_ID 234
#define CRACK_ONE_PREDICTION_TIMER_ID 235
#define CRACK_RECORD_QUERY_TIMER_ID 236
#define CRACK_PREDICTIVE_CRACKING_TIMER_ID 237
#define CRACK_QUERY_TIMER_ID 238
#define CRACK_DDR_FIND_TIMER_ID 239

// always be at the end
#define RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID 240

#define END_TIMER_COUNTERS RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID

// strings
static const std::string timer_names[END_TIMER_COUNTERS + 1] = {
    "off_by_one_stub",
    "ars_add_to_across_corr",
    "ars_add_to_within_corr",
    "ars_put_new_sample",
    "ars_remove_from_across_corr",
    "ars_remove_from_within_corr",
    "ars_remove_skew_counts_from_sites",
    "begin_for_dest",
    "begin_for_write",
    "change_type_from_sorted_column",
    "col_begin_read_wait_and_build_snapshot",
    "col_begin_read_wait_for_version",
    "col_partition_begin_write_acquire_locks",
    "col_read_latest_record",
    "col_read_record",
    "col_repartition_insert",
    "col_scan_records",
    "column_insert_record",
    "column_write_record",
    "enqueue_stashed_update",
    "holder_get_and_set_partition",
    "holder_get_partition",
    "kafka_enqueue_updates_for_topic",
    "kafka_seek_topic",
    "kafka_send_update",
    "merge_col_records_vertically_sorted_left_right_unsorted",
    "merge_col_records_vertically_sorted",
    "merge_col_records_vertically_unsorted",
    "merge_column_records_horizontally",
    "merge_row_records_horizontally",
    "merge_row_records_vertically",
    "merge_snapshots",
    "merge_sorted_column_records_horizontally",
    "partition_begin_read_wait_and_build_snapshot",
    "partition_begin_write_acquire_locks",
    "partition_build_commit_snapshot",
    "partition_commit_write_transaction",
    "partition_execute_deserialized_partition_update",
    "partition_get_most_recent_snapshot_vector",
    "partition_init_partition_from_snapshot",
    "partition_wait_until_version_or_apply_updates",
    "perform_remaster",
    "remaster_begin",
    "remaster_finalize",
    "remove_partition",
    "repartition_record",
    "row_begin_read_wait_and_build_snapshot",
    "row_begin_read_wait_for_version",
    "row_insert_record",
    "row_partition_begin_write_acquire_locks",
    "row_read_latest_record",
    "row_read_record",
    "row_scan_records",
    "row_write_record",
    "rpc_abort_transaction",
    "rpc_add_partitions",
    "rpc_add_replica_partitions",
    "rpc_begin_transaction",
    "rpc_change_partition_change_type",
    "rpc_change_partition_output_destination",
    "rpc_commit_transaction",
    "rpc_delete",
    "rpc_grant_mastership",
    "rpc_insert",
    "rpc_merge_partition",
    "rpc_one_shot_sproc",
    "rpc_release_mastership",
    "rpc_remove_partitions",
    "rpc_select",
    "rpc_snapshot_partitions",
    "rpc_split_partition",
    "rpc_sproc_deserialize",
    "rpc_sproc_execute",
    "rpc_stored_procedure",
    "rpc_update",
    "semaphore_lock",
    "semaphore_try_lock",
    "serialize_write_buffer",
    "smallbank_workload_op",
    "sorted_col_begin_read_wait_and_build_snapshot",
    "sorted_col_begin_read_wait_for_version",
    "sorted_col_partition_begin_write_acquire_locks",
    "sorted_col_read_latest_record",
    "sorted_col_read_record",
    "sorted_col_repartition_insert",
    "sorted_col_scan_records",
    "sorted_column_insert_record",
    "sorted_column_write_record",
    "split_col_records_horizontally",
    "split_col_records_vertically_sorted",
    "split_col_records_vertically",
    "split_row_records_horizontally",
    "split_row_records_vertically",
    "split_sorted_col_records_horizontally",
    "sproc_ycsb_ckr_inserts",
    "sproc_ycsb_delete_record",
    "sproc_ycsb_insert_records",
    "sproc_ycsb_mk_rmw",
    "sproc_ycsb_read_record",
    "sproc_ycsb_rmw_record",
    "sproc_ycsb_scan_records",
    "sproc_ycsb_update_record",
    "ss_add_previous_context_timer_information",
    "ss_add_remove_replicas_to_plan",
    "ss_add_spinup_replicas_to_plan",
    "ss_call_rpc_one_shot_sproc",
    "ss_data_location_get_or_create_partitions_and_group",
    "ss_data_location_get_or_create_partitions",
    "ss_data_location_get_or_create_partition",
    "ss_data_location_get_partitions_and_group",
    "ss_data_location_get_partitions",
    "ss_data_location_get_partition",
    "ss_data_location_insert_partition",
    "ss_data_location_merge_partition",
    "ss_data_location_remove_partition",
    "ss_data_location_split_partition",
    "ss_execute_add_partitions_action",
    "ss_execute_add_replica_partitions_action",
    "ss_execute_change_partitions_output_destination_action",
    "ss_execute_change_partitions_type_action",
    "ss_execute_merge_partitions_action",
    "ss_execute_one_shot_sproc_with_force_strategy",
    "ss_execute_one_shot_sproc_with_strategy",
    "ss_execute_pre_transaction_plan_actions_worker",
    "ss_execute_pre_transaction_plan",
    "ss_execute_remaster_partitions_action",
    "ss_execute_remove_partitions_action",
    "ss_execute_split_partitions_action",
    "ss_execute_transfer_remaster_partitions_action",
    "ss_generate_insert_only_partition_destination",
    "ss_generate_no_change_destination_plan_if_possible",
    "ss_generate_no_change_destination",
    "ss_generate_optimistic_no_change_plan",
    "ss_generate_plan_for_site_args",
    "ss_generate_plan_for_site",
    "ss_generate_plan",
    "ss_generate_plan_with_physical_adjustments",
    "ss_get_involved_sites",
    "ss_get_no_change_destinations_possibilities",
    "ss_get_partitions_to_adjust",
    "ss_get_pre_transaction_execution_plan",
    "ss_increase_tracking_counts",
    "ss_internal_data_location_get_or_create_partition",
    "ss_internal_data_location_get_partition",
    "ss_plan_add_necessary_remastering",
    "ss_plan_add_necessary_replicas",
    "ss_plan_build_sampled_transactions",
    "ss_plan_change_partition_types",
    "ss_plan_remove_un_needed_replicas",
    "ss_plan_tack_on_beneficial_spin_ups",
    "ss_prepare_transaction_data_items_for_single_site_execution",
    "ss_remove_un_needed_replicas_for_sample",
    "ss_rpc_begin_transaction",
    "ss_rpc_one_shot_sproc_transaction",
    "ss_sample_transaction_accesses",
    "ss_sort_finalize_and_add_metadata_to_plan",
    "ss_tack_on_beneficial_spin_ups_for_sample",
    "ss_wait_for_downgrade_to_complete_and_release_partitions",
    "transaction_partition_holder_wait_for_session",
    "update_enqueuers_add_source",
    "update_enqueuers_change_sources",
    "update_enqueuers_remove_source",
    "update_queue_get_next_update",
    "update_svv_from_result_set",
    "change_type_from_sorted_multi_column",
    "multi_col_begin_read_wait_and_build_snapshot",
    "multi_col_begin_read_wait_for_version",
    "multi_col_partition_begin_write_acquire_locks",
    "multi_col_read_latest_record",
    "multi_col_read_record",
    "multi_col_repartition_insert",
    "multi_col_scan_records",
    "multi_column_insert_record",
    "multi_column_write_record",
    "sorted_multi_col_begin_read_wait_and_build_snapshot",
    "sorted_multi_col_begin_read_wait_for_version",
    "sorted_multi_col_partition_begin_write_acquire_locks",
    "sorted_multi_col_read_latest_record",
    "sorted_multi_col_read_record",
    "sorted_multi_col_repartition_insert",
    "sorted_multi_col_scan_records",
    "sorted_multi_column_insert_record",
    "sorted_multi_column_write_record",
    "split_multi_col_records_vertically",
    "merge_multi_col_records_vertically",
    "split_multi_sorted_col_records_vertically",
    "merge_multi_sorted_col_records_vertically",
    "rpc_change_partition_storage_type",
    "ss_execute_change_storage_type_action",
    "row_restore_from_disk",
    "col_restore_from_disk",
    "sorted_col_restore_from_disk",
    "multi_col_restore_from_disk",
    "sorted_multi_col_restore_from_disk",
    "row_persist_to_disk",
    "col_persist_to_disk",
    "sorted_col_perist_to_disk",
    "multi_col_perist_to_disk",
    "sorted_multi_col_persist_to_disk",
    "read_row_from_disk",
    "read_col_from_disk",
    "read_sorted_col_from_disk",
    "read_multi_col_from_disk",
    "read_sorted_multi_col_from_disk",
    "row_scan_disk",
    "col_scan_disk",
    "sorted_col_scan_disk",
    "multi_col_scan_disk",
    "sorted_multi_col_scan_disk",
    "rpc_one_shot_scan",
    "rpc_scan_execute",
    "rpc_scan",
    "ss_rpc_one_shot_scan_transaction",
    "ss_call_rpc_one_shot_scan",
    "ss_execute_one_shot_scan_at_sites",
    "ss_prepare_transaction_data_timers_for_scan_execution",
    "ss_generate_optimistic_no_change_scan_plan",
    "ss_generate_scan_plan_with_physical_adjustments",
    "ss_sort_finalize_and_add_metadata_to_scan_plan",
    "ss_generate_no_change_destination_scan_plan_if_possible",
    "ss_get_pre_transaction_execution_scan_plan",
    "tpcc_workload_op",
    "tpch_workload_op",
    "ycsb_workload_op",
    "twitter_workload_op",

    "crack_add_crack",
    "crack_count_query",
    "crack_crack",
    "crack_find_piece",
    "crack_insert",
    "crack_merge_ripple",
    "crack_partition",
    "crack_remove",
    "crack_view_query",
    "crack_split_ab",
    "crack_one_prediction",
    "crack_record_query",
    "crack_predictive_cracking",
    "crack_query",
    "crack_ddr_find",

    "rpc_at_data_site_service_time",
};

// model information
static const int64_t model_timer_positions[END_TIMER_COUNTERS + 1] = {
    -1 /* off_by_one_stub */,
    -1 /* ars_add_to_across_corr */,
    -1 /* ars_add_to_within_corr */,
    -1 /* ars_put_new_sample */,
    -1 /* ars_remove_from_across_corr */,
    -1 /* ars_remove_from_within_corr */,
    -1 /* ars_remove_skew_counts_from_sites */,
    -1 /* begin_for_dest */,
    -1 /* begin_for_write */,
    0 /* change_type_from_sorted_column */,
    1 /* col_begin_read_wait_and_build_snapshot */,
    2 /* col_begin_read_wait_for_version */,
    3 /* col_partition_begin_write_acquire_locks */,
    4 /* col_read_latest_record */,
    5 /* col_read_record */,
    6 /* col_repartition_insert */,
    7 /* col_scan_records */,
    8 /* column_insert_record */,
    9 /* column_write_record */,
    10 /* enqueue_stashed_update */,
    -1 /* holder_get_and_set_partition */,
    -1 /* holder_get_partition */,
    -1 /* kafka_enqueue_updates_for_topic */,
    -1 /* kafka_seek_topic */,
    -1 /* kafka_send_update */,
    11 /* merge_col_records_vertically_sorted_left_right_unsorted */,
    12 /* merge_col_records_vertically_sorted */,
    13 /* merge_col_records_vertically_unsorted */,
    14 /* merge_column_records_horizontally */,
    15 /* merge_row_records_horizontally */,
    16 /* merge_row_records_vertically */,
    -1 /* merge_snapshots */,
    17 /* merge_sorted_column_records_horizontally */,
    18 /* partition_begin_read_wait_and_build_snapshot */,
    19 /* partition_begin_write_acquire_locks */,
    20 /* partition_build_commit_snapshot */,
    -1 /* partition_commit_write_transaction */,
    -1 /* partition_execute_deserialized_partition_update */,
    -1 /* partition_get_most_recent_snapshot_vector */,
    -1 /* partition_init_partition_from_snapshot */,
    -1 /* partition_wait_until_version_or_apply_updates */,
    -1 /* perform_remaster */,
    -1 /* remaster_begin */,
    -1 /* remaster_finalize */,
    21 /* remove_partition */,
    -1 /* repartition_record */,
    22 /* row_begin_read_wait_and_build_snapshot */,
    23 /* row_begin_read_wait_for_version */,
    24 /* row_insert_record */,
    25 /* row_partition_begin_write_acquire_locks */,
    26 /* row_read_latest_record */,
    27 /* row_read_record */,
    28 /* row_scan_records */,
    29 /* row_write_record */,
    -1 /* rpc_abort_transaction */,
    -1 /* rpc_add_partitions */,
    -1 /* rpc_add_replica_partitions */,
    -1 /* rpc_begin_transaction */,
    -1 /* rpc_change_partition_change_type */,
    -1 /* rpc_change_partition_output_destination */,
    -1 /* rpc_commit_transaction */,
    -1 /* rpc_delete */,
    -1 /* rpc_grant_mastership */,
    -1 /* rpc_insert */,
    -1 /* rpc_merge_partition */,
    -1 /* rpc_one_shot_sproc */,
    -1 /* rpc_release_mastership */,
    -1 /* rpc_remove_partitions */,
    -1 /* rpc_select */,
    -1 /* rpc_snapshot_partitions */,
    -1 /* rpc_split_partition */,
    -1 /* rpc_sproc_deserialize */,
    -1 /* rpc_sproc_execute */,
    -1 /* rpc_stored_procedure */,
    -1 /* rpc_update */,
    -1 /* semaphore_lock */,
    -1 /* semaphore_try_lock */,
    30 /* serialize_write_buffer */,
    -1 /* smallbank_workload_op */,
    31 /* sorted_col_begin_read_wait_and_build_snapshot */,
    32 /* sorted_col_begin_read_wait_for_version */,
    33 /* sorted_col_partition_begin_write_acquire_locks */,
    34 /* sorted_col_read_latest_record */,
    35 /* sorted_col_read_record */,
    36 /* sorted_col_repartition_insert */,
    37 /* sorted_col_scan_records */,
    38 /* sorted_column_insert_record */,
    39 /* sorted_column_write_record */,
    40 /* split_col_records_horizontally */,
    41 /* split_col_records_vertically_sorted */,
    42 /* split_col_records_vertically */,
    43 /* split_row_records_horizontally */,
    44 /* split_row_records_vertically */,
    45 /* split_sorted_col_records_horizontally */,
    -1 /* sproc_ycsb_ckr_inserts */,
    -1 /* sproc_ycsb_delete_record */,
    -1 /* sproc_ycsb_insert_records */,
    -1 /* sproc_ycsb_mk_rmw */,
    -1 /* sproc_ycsb_read_record */,
    -1 /* sproc_ycsb_rmw_record */,
    -1 /* sproc_ycsb_scan_records */,
    -1 /* sproc_ycsb_update_record */,
    -1 /* ss_add_previous_context_timer_information */,
    -1 /* ss_add_remove_replicas_to_plan */,
    -1 /* ss_add_spinup_replicas_to_plan */,
    -1 /* ss_call_rpc_one_shot_sproc */,
    -1 /* ss_data_location_get_or_create_partitions_and_group */,
    -1 /* ss_data_location_get_or_create_partitions */,
    -1 /* ss_data_location_get_or_create_partition */,
    -1 /* ss_data_location_get_partitions_and_group */,
    -1 /* ss_data_location_get_partitions */,
    -1 /* ss_data_location_get_partition */,
    -1 /* ss_data_location_insert_partition */,
    -1 /* ss_data_location_merge_partition */,
    -1 /* ss_data_location_remove_partition */,
    -1 /* ss_data_location_split_partition */,
    -1 /* ss_execute_add_partitions_action */,
    -1 /* ss_execute_add_replica_partitions_action */,
    -1 /* ss_execute_change_partitions_output_destination_action */,
    -1 /* ss_execute_change_partitions_type_action */,
    -1 /* ss_execute_merge_partitions_action */,
    -1 /* ss_execute_one_shot_sproc_with_force_strategy */,
    -1 /* ss_execute_one_shot_sproc_with_strategy */,
    -1 /* ss_execute_pre_transaction_plan_actions_worker */,
    -1 /* ss_execute_pre_transaction_plan */,
    -1 /* ss_execute_remaster_partitions_action */,
    -1 /* ss_execute_remove_partitions_action */,
    -1 /* ss_execute_split_partitions_action */,
    -1 /* ss_execute_transfer_remaster_partitions_action */,
    -1 /* ss_generate_insert_only_partition_destination */,
    -1 /* ss_generate_no_change_destination_plan_if_possible */,
    -1 /* ss_generate_no_change_destination */,
    -1 /* ss_generate_optimistic_no_change_plan */,
    -1 /* ss_generate_plan_for_site_args */,
    -1 /* ss_generate_plan_for_site */,
    -1 /* ss_generate_plan */,
    -1 /* ss_generate_plan_with_physical_adjustments */,
    -1 /* ss_get_involved_sites */,
    -1 /* ss_get_no_change_destinations_possibilities */,
    -1 /* ss_get_partitions_to_adjust */,
    -1 /* ss_get_pre_transaction_execution_plan */,
    -1 /* ss_increase_tracking_counts */,
    -1 /* ss_internal_data_location_get_or_create_partition */,
    -1 /* ss_internal_data_location_get_partition */,
    -1 /* ss_plan_add_necessary_remastering */,
    -1 /* ss_plan_add_necessary_replicas */,
    -1 /* ss_plan_build_sampled_transactions */,
    -1 /* ss_plan_change_partition_types */,
    -1 /* ss_plan_remove_un_needed_replicas */,
    -1 /* ss_plan_tack_on_beneficial_spin_ups */,
    -1 /* ss_prepare_transaction_data_items_for_single_site_execution */,
    -1 /* ss_remove_un_needed_replicas_for_sample */,
    -1 /* ss_rpc_begin_transaction */,
    -1 /* ss_rpc_one_shot_sproc_transaction */,
    -1 /* ss_sample_transaction_accesses */,
    -1 /* ss_sort_finalize_and_add_metadata_to_plan */,
    -1 /* ss_tack_on_beneficial_spin_ups_for_sample */,
    -1 /* ss_wait_for_downgrade_to_complete_and_release_partitions */,
    -1 /* transaction_partition_holder_wait_for_session */,
    -1 /* update_enqueuers_add_source */,
    -1 /* update_enqueuers_change_sources */,
    -1 /* update_enqueuers_remove_source */,
    -1 /* update_queue_get_next_update */,
    -1 /* update_svv_from_result_set */,
    46 /* change_type_from_sorted_multi_column */,
    47 /* multi_col_begin_read_wait_and_build_snapshot */,
    48 /* multi_col_begin_read_wait_for_version */,
    49 /* multi_col_partition_begin_write_acquire_locks */,
    50 /* multi_col_read_latest_record */,
    51 /* multi_col_read_record */,
    52 /* multi_col_repartition_insert */,
    53 /* multi_col_scan_records */,
    54 /* multi_column_insert_record */,
    55 /* multi_column_write_record */,
    56 /* sorted_multi_col_begin_read_wait_and_build_snapshot */,
    57 /* sorted_multi_col_begin_read_wait_for_version */,
    58 /* sorted_multi_col_partition_begin_write_acquire_locks */,
    59 /* sorted_multi_col_read_latest_record */,
    60 /* sorted_multi_col_read_record */,
    61 /* sorted_multi_col_repartition_insert */,
    62 /* sorted_multi_col_scan_records */,
    63 /* sorted_multi_column_insert_record */,
    64 /* sorted_multi_column_write_record */,
    65 /* split_multi_col_records_vertically */,
    66 /* merge_multi_col_records_vertically */,
    67 /* split_multi_sorted_col_records_vertically */,
    68 /* merge_multi_sorted_col_records_vertically */,
    -1 /* rpc_change_partition_storage_type */,
    -1 /* ss_execute_change_storage_type_action */,
    69 /* row_restore_from_disk */,
    70 /* col_restore_from_disk */,
    71 /* sorted_col_restore_from_disk */,
    72 /* multi_col_restore_from_disk */,
    73 /* sorted_multi_col_restore_from_disk */,
    74 /* row_persist_to_disk */,
    75 /* col_persist_to_disk */,
    76 /* sorted_col_perist_to_disk */,
    77 /* multi_col_perist_to_disk */,
    78 /* sorted_multi_col_persist_to_disk */,
    79 /* read_row_from_disk */,
    80 /* read_col_from_disk */,
    81 /* read_sorted_col_from_disk */,
    82 /* read_multi_col_from_disk */,
    83 /* read_sorted_multi_col_from_disk */,
    84 /* row_scan_disk */,
    85 /* col_scan_disk */,
    86 /* sorted_col_scan_disk */,
    87 /* multi_col_scan_disk */,
    88 /* sorted_multi_col_scan_disk */,
    -1 /* rpc_one_shot_scan */,
    -1 /* rpc_scan_execute */,
    -1 /* rpc_scan */,
    -1 /* ss_rpc_one_shot_scan_transaction */,
    -1 /* ss_call_rpc_one_shot_scan */,
    89 /* ss_execute_one_shot_scan_at_sites */,
    -1 /* ss_prepare_transaction_data_timers_for_scan_execution */,
    -1 /* ss_generate_optimistic_no_change_scan_plan */,
    -1 /* ss_generate_scan_plan_with_physical_adjustments */,
    -1 /* ss_sort_finalize_and_add_metadata_to_scan_plan */,
    -1 /* ss_generate_no_change_destination_scan_plan_if_possible */,
    -1 /* ss_get_pre_transaction_execution_scan_plan */,
    -1 /* tpcc_workload_op */,
    -1 /* tpch_workload_op */,
    -1 /* ycsb_workload_op */,
    -1 /* twitter_workload_op */,

    -1 /* crack_add_crack */,
    -1 /* crack_count_query */,
    -1 /* crack_crack */,
    -1 /* crack_find_piece */,
    -1 /* crack_insert */,
    -1 /* crack_merge_ripple */,
    -1 /* crack_partition */,
    -1 /* crack_remove */,
    -1 /* crack_view_query */,
    -1 /* crack_split_ab */,
    -1 /* crack_one_prediction */,
    -1 /* crack_record_query */,
    -1 /* crack_predictive_cracking */,
    -1 /* crack_query */,
    -1 /* crack_ddr_find */,

    90 /* rpc_at_data_site_service_time */,
};
