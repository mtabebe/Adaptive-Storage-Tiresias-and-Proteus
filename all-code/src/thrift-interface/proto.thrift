namespace java com.adapt_htap //

include "dynamic_mastering.thrift"

struct context_timer {
  1:required i64 counter_id,
  2:required i64 counter_seen_count,
  3:required double timer_us_average,
}

struct previous_context_timer_information {
  1:required bool is_present,
  2:required i32 num_requests,
  3:required list<context_timer> timers,
  4:required i32 site,
  5:required list<dynamic_mastering.cell_key_ranges> write_ckrs,
  6:required list<i32> write_ckr_counts,
  7:required list<dynamic_mastering.cell_key_ranges> read_ckrs,
  8:required list<i32> read_ckr_counts,
}

enum data_type
{
  UINT64,
  INT64,
  DOUBLE,
  STRING,
}

struct cell_widths_stats {
  1:required double avg_width,
  2:required i64 num_observations,
}
struct selectivity_stats {
  1:required double avg_selectivity,
  2:required i64 num_observations,
}

struct result_cell {
    1:required i32 col_id,
    2:required bool present,
    3:required string data,
    4:required data_type type,
}

struct result_tuple {
  1: required i32 table_id,
  2: required i64 row_id,
  3: required list<result_cell> cells,
}

struct query_result {
    1:dynamic_mastering.exec_status_type status
    2:string errMsg,
    3:list<result_tuple> tuples,
    4:list<context_timer> timers,
}

struct site_selector_begin_result {
    1:dynamic_mastering.exec_status_type status,
    2:i32 site_id,
    3:string ip,
    4:i32 port,
    5:list<dynamic_mastering.cell_key_ranges> items_at_site,
}

struct begin_result {
    1:dynamic_mastering.exec_status_type status,
    2:string errMsg,
    3:list<context_timer> timers,
}

struct abort_result {
    1:dynamic_mastering.exec_status_type status,
    2:string errMsg,
    3:list<context_timer> timers,
}

struct commit_result {
    1:dynamic_mastering.exec_status_type status,
    2:dynamic_mastering.snapshot_vector session_version_vector,
    3:string errMsg,
    4:list<context_timer> timers,
}

struct grant_result {
    1:dynamic_mastering.exec_status_type status,
    2:dynamic_mastering.snapshot_vector session_version_vector,
    3:string errMsg,
    4:list<context_timer> timers,
}

struct release_result {
    1:dynamic_mastering.exec_status_type status,
    2:dynamic_mastering.snapshot_vector session_version_vector,
    3:string errMsg,
    4:list<context_timer> timers,
}

enum propagation_type
{
    NO_OP,
    VECTOR,
    KAFKA,
}

enum partition_type
{
  ROW,
  COLUMN,
  SORTED_COLUMN,
  MULTI_COLUMN,
  SORTED_MULTI_COLUMN,
}

enum storage_tier_type
{
  MEMORY,
  DISK,
}

struct storage_tier_change {
  1:dynamic_mastering.partition_column_identifier pid,
  2:i32 site,
  3:storage_tier_type change,
}


struct propagation_configuration {
  1:propagation_type type,
  2:string topic,
  3:i32 partition,
  4:i64 offset,
}

struct machine_statistics {
  1:double average_cpu_load
  2:i64 average_interval
  3:double average_overall_load
  4:i64 memory_usage,
}

struct polled_partition_column_version_information {
    1:dynamic_mastering.partition_column_identifier pcid,
    2:i64 version,
}

struct site_statistics_results {
    1:dynamic_mastering.exec_status_type status,
    2:dynamic_mastering.snapshot_vector snapshot_vector,
    3:list<propagation_configuration> prop_configs,
    4:list<i64> prop_counts,
    5:machine_statistics mach_stats,
    6:list<polled_partition_column_version_information> partition_col_versions,
    7:list<list<cell_widths_stats>> col_widths,
    8:list<list<selectivity_stats>> selectivities,
    9:list<storage_tier_change> storage_changes,
    10:list<context_timer> timers,
    // future: whatever else we need to do site selector strategies
}

struct sproc_result {
    1:dynamic_mastering.exec_status_type status,
    2:i32 return_code,
    3:binary res_args,
    4:list<context_timer> timers,
}

struct one_shot_sproc_result {
    1:dynamic_mastering.exec_status_type status,
    2:dynamic_mastering.snapshot_vector session_version_vector,
    3:i32 site_of_exec_id;
    4:string site_of_exec_ip;
    5:i32 site_of_exec_port;
    6:list<dynamic_mastering.partition_column_identifier> write_set;
    7:list<dynamic_mastering.partition_column_identifier> read_set;
    8:i32 return_code,
    9:binary res_args,
    10:list<context_timer> timers,
}

enum predicate_type
{
  EQUALITY,
  INEQUALITY,
  LESS_THAN,
  GREATER_THAN,
  LESS_THAN_OR_EQUAL,
  GREATER_THAN_OR_EQUAL,
}


struct cell_predicate {
  1: required i32 table_id,
  2: required i32 col_id,
  3: required data_type type,
  4: required string data,
  5: required predicate_type predicate,
}

struct predicate_chain {
  1: list<cell_predicate> and_predicates,
  2: list<cell_predicate> or_predicates,
  3: bool is_and_join_predicates,
}



struct scan_result {
    1:dynamic_mastering.exec_status_type status,
    2:i32 return_code,
    3:map<i32, list<result_tuple>> res_tuples,
    4:list<context_timer> timers,
}

struct scan_arguments {
    1:i32 label,
    2:list<dynamic_mastering.cell_key_ranges> read_ckrs,
    3:predicate_chain predicate,
}

struct one_shot_scan_result {
    1:dynamic_mastering.exec_status_type status,
    2:dynamic_mastering.snapshot_vector session_version_vector,
    3:list<i32> site_of_exec_ids;
    4:list<string> site_of_exec_ips;
    5:list<i32> site_of_exec_ports;
    6:list<dynamic_mastering.partition_column_identifier> write_set;
    7:list<dynamic_mastering.partition_column_identifier> read_set;
    8:i32 return_code,
    9:map<i32, list<result_tuple>> res_tuples,
    10:list<context_timer> timers,
}


struct persistence_result {
    1:dynamic_mastering.exec_status_type status,
}

struct snapshot_cell_state {
    1:required i64 row_id,
    2:required i32 col_id,
    3:required bool present,
    4:required string data,
    5:required data_type type,
    6:required i64 version,
}

struct snapshot_column_state {
    1:required i32 col_id,
    2:required data_type type,
    3:required list<list<i64>> keys,
    4:required list<string> data,
}

struct snapshot_partition_column_state {
    1:dynamic_mastering.partition_column_identifier pcid,
    2:dynamic_mastering.snapshot_vector session_version_vector,
    3:list<snapshot_column_state> columns,
    4:i32 master_location,
    5:propagation_configuration prop_config,
}

struct snapshot_partition_columns_results {
    1:dynamic_mastering.exec_status_type status,
    2:list<snapshot_partition_column_state> snapshots,
    3:list<context_timer> timers,
}


service site_manager {
    begin_result rpc_begin_transaction(1:dynamic_mastering.clientid id,
                                       2:dynamic_mastering.snapshot_vector client_session_version_vector,
                                       3:list<dynamic_mastering.partition_column_identifier> write_set,
                                       4:list<dynamic_mastering.partition_column_identifier> read_set,
                                       5:list<dynamic_mastering.partition_column_identifier> inflight_pcids),
    query_result rpc_select(1:dynamic_mastering.clientid id,
                            2:list<dynamic_mastering.cell_key> read_keys),
    query_result rpc_insert(1:dynamic_mastering.clientid id,
                            2:list<dynamic_mastering.cell_key> write_keys,
                            3:list<string> write_vals),
    query_result rpc_delete(1:dynamic_mastering.clientid id,
                            2:list<dynamic_mastering.cell_key> delete_keys),
    query_result rpc_update(1:dynamic_mastering.clientid id,
                            2:list<dynamic_mastering.cell_key> write_keys,
                            3:list<string> write_vals),

    sproc_result rpc_stored_procedure(1:dynamic_mastering.clientid id,
                                      2:string name,
                                      3:list<dynamic_mastering.cell_key_ranges> write_ckrs,
                                      4:list<dynamic_mastering.cell_key_ranges> read_ckrs,
                                      5:binary sproc_args),

    scan_result rpc_scan( 1:dynamic_mastering.clientid id,
                          2:string name,
                          3: list<scan_arguments> scan_args,
                          4:binary sproc_args),


    commit_result rpc_prepare_transaction(1:dynamic_mastering.clientid id),
    commit_result rpc_commit_transaction(1:dynamic_mastering.clientid id),
    abort_result rpc_abort_transaction(1:dynamic_mastering.clientid id),

    grant_result rpc_grant_mastership(1:dynamic_mastering.clientid id,
                                      2:list<dynamic_mastering.partition_column_identifier> keys,
                                      3:list<propagation_configuration> new_propagation_configs,
                                      4:dynamic_mastering.snapshot_vector session_version_vector),
    release_result rpc_release_mastership(1:dynamic_mastering.clientid id,
                                          2:list<dynamic_mastering.partition_column_identifier> keys,
                                          3:i32 destination,
                                          4:list<propagation_configuration> new_propagation_configs,
                                          5:dynamic_mastering.snapshot_vector session_version_vector),

    snapshot_partition_columns_results rpc_release_transfer_mastership(1:dynamic_mastering.clientid id,
                                                                      2:list<dynamic_mastering.partition_column_identifier> keys,
                                                                      3:i32 destination,
                                                                      4:dynamic_mastering.snapshot_vector session_version_vector),
    grant_result rpc_grant_transfer_mastership( 1:dynamic_mastering.clientid id,
                                                2:list<snapshot_partition_column_state> snapshots,
                                                3:list<partition_type> p_types,
                                                4:list<storage_tier_type> s_types),

    one_shot_sproc_result rpc_one_shot_sproc(1:dynamic_mastering.clientid id,
                                             2:dynamic_mastering.snapshot_vector client_session_version_vector,
                                             3:list<dynamic_mastering.partition_column_identifier> write_set,
                                             4:list<dynamic_mastering.partition_column_identifier> read_set,
                                             5:list<dynamic_mastering.partition_column_identifier> inflight_pcids,
                                             6:string name,
                                             7:list<dynamic_mastering.cell_key_ranges> write_ckrs,
                                             8:list<dynamic_mastering.cell_key_ranges> read_ckrs,
                                             9:binary sproc_args),

    one_shot_scan_result rpc_one_shot_scan( 1:dynamic_mastering.clientid id,
                                            2:dynamic_mastering.snapshot_vector client_session_version_vector,
                                            3:list<dynamic_mastering.partition_column_identifier> read_set,
                                            4:list<dynamic_mastering.partition_column_identifier> inflight_pcids,
                                            5:string name,
                                            6:list<scan_arguments> scan_args,
                                            7:binary sproc_args ),

    commit_result rpc_add_partitions(1:dynamic_mastering.clientid id,
                                     2:dynamic_mastering.snapshot_vector client_session_version_vector,
                                     3:list<dynamic_mastering.partition_column_identifier> pcids_to_add,
                                     4:i32 master_location,
                                     5:list<partition_type> p_types,
                                     6:list<storage_tier_type> s_types,
                                     7:propagation_configuration prop_config),

    commit_result rpc_add_replica_partitions(1:dynamic_mastering.clientid id,
                                             2:list<snapshot_partition_column_state> snapshots,
                                             3:list<partition_type> p_types,
                                             4:list<storage_tier_type> s_types),

    commit_result rpc_remove_partitions(1:dynamic_mastering.clientid id,
                                        2:dynamic_mastering.snapshot_vector client_session_version_vector,
                                        3:list<dynamic_mastering.partition_column_identifier> pcids_to_remove),
    commit_result rpc_merge_partition(1:dynamic_mastering.clientid id,
                                      2:dynamic_mastering.snapshot_vector client_session_version_vector,
                                      3:dynamic_mastering.partition_column_identifier lower_partition,
                                      4:dynamic_mastering.partition_column_identifier higher_partition,
                                      5:partition_type merge_type,
                                      6:storage_tier_type merge_storage_type,
                                      7:propagation_configuration prop_config),
    commit_result rpc_split_partition(1:dynamic_mastering.clientid id,
                                      2:dynamic_mastering.snapshot_vector client_session_version_vector,
                                      3:dynamic_mastering.partition_column_identifier partition,
                                      4:dynamic_mastering.cell_key split_point,
                                      5:partition_type low_type,
                                      6:partition_type high_type,
                                      7:storage_tier_type low_storage_type,
                                      8:storage_tier_type high_storage_type,
                                      9:list<propagation_configuration> prop_configs),

    commit_result rpc_change_partition_output_destination( 1:dynamic_mastering.clientid id,
                                                           2:dynamic_mastering.snapshot_vector client_session_version_vector,
                                                           3:list<dynamic_mastering.partition_column_identifier> pcids,
                                                           4:propagation_configuration new_propagation_config),

    snapshot_partition_columns_results rpc_snapshot_partitions(1:dynamic_mastering.clientid id,
                                                               2:list<dynamic_mastering.partition_column_identifier> pcids),

    commit_result rpc_change_partition_type( 1:dynamic_mastering.clientid id,
                                             2:list<dynamic_mastering.partition_column_identifier> pcids,
                                             3:list<partition_type> p_types,
                                             4:list<storage_tier_type> s_types),

    site_statistics_results rpc_get_site_statistics(),

    persistence_result rpc_persist_db_to_file(1: dynamic_mastering.clientid id,
                                              2: string out_name,
                                              3: string out_part_name,
                                              4: list<list<propagation_configuration>> site_propagation_configs),
    persistence_result rpc_restore_db_from_file(1: dynamic_mastering.clientid id,
                                                2: string in_name,
                                                3: string in_part_name,
                                                4: list<list<propagation_configuration>> site_propagation_configs),
}

service SiteSelector {
    list<site_selector_begin_result>
    rpc_begin_transaction(1:dynamic_mastering.clientid id,
                          2:dynamic_mastering.snapshot_vector client_session_version_vector,
                          3:list<dynamic_mastering.cell_key_ranges> write_set,
                          4:list<dynamic_mastering.cell_key_ranges> read_set),

    list<site_selector_begin_result>
    rpc_begin_fully_replicated_transaction(1:dynamic_mastering.clientid id,
                                           2:dynamic_mastering.snapshot_vector client_session_version_vector,
                                           3:list<dynamic_mastering.cell_key_ranges> fully_replicated_write_set),


    one_shot_sproc_result rpc_one_shot_sproc(1:dynamic_mastering.clientid id,
                                             2:dynamic_mastering.snapshot_vector client_session_version_vector,
                                             3:list<dynamic_mastering.cell_key_ranges> write_set,
                                             4:list<dynamic_mastering.cell_key_ranges> read_set,
                                             5:string name,
                                             6:binary sproc_args,
                                             7:bool allow_force_change,
                                             8:previous_context_timer_information previous_contexts ),

    one_shot_scan_result rpc_one_shot_scan( 1:dynamic_mastering.clientid id,
                                            2:dynamic_mastering.snapshot_vector client_session_version_vector,
                                            3:list<scan_arguments> scan_args,
                                            4:string name,
                                            5:binary sproc_args,
                                            6:bool allow_missing_data,
                                            7:bool allow_force_change,
                                            8:previous_context_timer_information previous_contexts ),


    persistence_result rpc_wait_for_stable_state_among_sites( 1: dynamic_mastering.clientid id),

    persistence_result rpc_persist_state_to_files(1: dynamic_mastering.clientid id,
                                                  2: string out_ss_name,
                                                  3: string out_db_name,
                                                  4: string out_part_name),
    persistence_result rpc_restore_state_from_files(1: dynamic_mastering.clientid id,
                                                    2: string in_ss_name,
                                                    3: string in_db_name,
                                                    4: string in_part_name),
}
