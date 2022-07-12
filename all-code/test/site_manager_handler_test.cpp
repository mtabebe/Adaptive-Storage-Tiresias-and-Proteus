#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/benchmark/benchmark_executor.h"
#include "../src/benchmark/ycsb/ycsb_record_types.h"
#include "../src/data-site/site-manager/site_manager_handler.h"
#include "../src/data-site/stored-procedures/ycsb/ycsb_prep_stmts.h"

class site_manager_test : public ::testing::Test {};

void sproc_helper( site_manager_handler *handler, ::clientid cid,
                   const std::string &          sproc_name,
                   const std::vector<cell_key_ranges>& write_ckrs,
                   const std::vector<cell_key_ranges>& read_ckrs,
                   const std::vector<arg_code> &arg_codes,
                   const std::vector<void *> &  arg_ptrs ) {
    char * buff = NULL;
    size_t serialize_len = serialize_for_sproc( arg_codes, arg_ptrs, &buff );
    EXPECT_GE( serialize_len, 4 );
    std::string str_buff( buff, serialize_len );
    EXPECT_GE( str_buff.size(), 4 );

    sproc_result sproc_res;
    handler->rpc_stored_procedure( sproc_res, cid, sproc_name, write_ckrs,
                                   read_ckrs, str_buff );
    EXPECT_EQ( sproc_res.status, exec_status_type::COMMAND_OK );

    free( buff );
}

#define check_timers_contain( _timers, _expected, _opt_expected )              \
    DVLOG( 1 ) << "check timers:" << _timers;                                  \
    EXPECT_GE( _timers.size(), _expected.size() );                             \
    EXPECT_LE( _timers.size(), _expected.size() + _opt_expected.size() );      \
    for( const auto &timer : _timers ) {                                       \
        int  id = timer.counter_id;                                            \
        auto search = _expected.find( id );                                    \
        auto _opt_search = _opt_expected.find( id );                           \
        DVLOG( 1 ) << "Check timer id:" << id;                                 \
        if( search != _expected.end() ) {                                      \
            EXPECT_EQ( search->second, timer.counter_seen_count );             \
        } else if( _opt_search != _opt_expected.end() ) {                      \
            EXPECT_EQ( _opt_search->second, timer.counter_seen_count );        \
        } else {                                                               \
            EXPECT_TRUE( false );                                              \
            DLOG( WARNING ) << "Timer:" << timer                               \
                            << ", not found in expected:"; /* << _expected; */ \
        }                                                                      \
    }

void handler_sproc_implementations( const partition_type::type &p_type ) {
    uint32_t num_clients = 2;
    uint32_t site_loc = 0;
    uint32_t gc_sleep_time = 10;

    auto no_op_update_gen = make_no_op_update_destination_generator();
    auto prop_config =
        no_op_update_gen->get_propagation_configurations().at( 0 );

    std::unique_ptr<db> database = construct_database(
        no_op_update_gen, make_update_enqueuers(), num_clients,
        1 /*num threads */, workload_type::YCSB, site_loc /*site id*/,
        gc_sleep_time, false /* enable sec storage */,
        "/tmp" /*sec storage dir*/ );

    std::unique_ptr<sproc_lookup_table> sproc_table =
        construct_ycsb_sproc_lookup_table();

    benchmark_configs b_cfg = construct_benchmark_configs(
        num_clients /*num clients*/, gc_sleep_time, 1 /*  bench time*/,
        1000 /* num operations before checking*/, false /* limit update prop*/,
        false /* enable sec storage */, "/tmp/" /*sec storage dir*/ );
    ycsb_configs ycsb_cfg = construct_ycsb_configs(
        1000 /*num keys*/, 10 /*value size*/, 5 /*partition size*/,
        7 /*num opers per txn*/, 1.0 /*zipf*/, false /*limit update prop*/,
        0.1 /*scan selectivity*/, 0 /* update prop limit*/,
        2 /*col partition size*/, p_type, storage_tier_type::type::MEMORY,
        true /* allow scan conflicts */, true /* allow rmw conflicts */,
        true /*store scan results */, b_cfg, 20 /*write*/, 20 /*read*/,
        20 /*rmw*/, 20 /*scan*/, 20 /*multi_rmw*/ );

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;

    void *opaque_ptr = construct_ycsb_opaque_pointer( ycsb_cfg );

    site_manager_handler handler( std::move( database ),
                                  std::move( sproc_table ), opaque_ptr );
    DVLOG( 10 ) << "Constructed handler";
    handler.init( num_clients, 1 /*num client threads*/, (void *) &ycsb_cfg );

    snapshot_vector cli_state;
    clientid        cid = 1;

    partition_column_identifier p_0_10_0_1 =
        create_partition_column_identifier( 0, 0, 10, 0, 1 );
    partition_column_identifier p_11_15_0_1 =
        create_partition_column_identifier( 0, 11, 15, 0, 1 );
    partition_column_identifier p_0_10_2_3 =
        create_partition_column_identifier( 0, 0, 10, 2, 3 );
    partition_column_identifier p_11_15_2_3 =
        create_partition_column_identifier( 0, 11, 15, 2, 3 );

    ::cell_key ck1;
    ::cell_key ck2;
    ::cell_key ck3;
    ck1.table_id = 0;
    ck1.row_id = 3;
    ck1.col_id = 0;
    ck2.table_id = 0;
    ck2.row_id = 4;
    ck3.col_id = 0;
    ck3.table_id = 0;
    ck3.row_id = 5;
    ck3.col_id = 0;

    commit_result commit_res;
    handler.rpc_add_partitions(
        commit_res, cid, cli_state,
        {p_0_10_0_1, p_11_15_0_1, p_0_10_2_3, p_11_15_2_3}, site_loc,
        {p_type, p_type, p_type, p_type}, {s_type, s_type, s_type, s_type},
        prop_config );
    EXPECT_EQ( commit_res.status, exec_status_type::COMMAND_OK );

    const std::vector<partition_column_identifier> begin_pid_wset = {
        p_0_10_0_1, p_0_10_2_3};
    const std::vector<partition_column_identifier> begin_pid_rset;
    const std::vector<partition_column_identifier> begin_pid_ifset;

    begin_result begin_res;
    handler.rpc_begin_transaction( begin_res, cid, cli_state, begin_pid_wset,
                                   begin_pid_rset, begin_pid_ifset );
    EXPECT_EQ( begin_res.status, exec_status_type::COMMAND_OK );
    std::unordered_map<int, uint64_t> timer_map = {
        {PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID, begin_pid_wset.size()},
        {RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID, 1},
    };
    if ( p_type == partition_type::type::ROW) {
        timer_map.emplace( ROW_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID,
                           begin_pid_wset.size() );
        timer_map.emplace( ROW_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID,
                           begin_pid_wset.size() );
    } else if ( p_type == partition_type::type::COLUMN) {
        timer_map.emplace( COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID,
                           begin_pid_wset.size() );
        timer_map.emplace( COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID,
                           begin_pid_wset.size() );
    } else if ( p_type == partition_type::type::SORTED_COLUMN) {
        timer_map.emplace(
            SORTED_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID,
            begin_pid_wset.size() );
        timer_map.emplace( SORTED_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID,
                           begin_pid_wset.size() );
    } else if ( p_type == partition_type::type::MULTI_COLUMN) {
        timer_map.emplace( MULTI_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID,
                           begin_pid_wset.size() );
        timer_map.emplace( MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID,
                           begin_pid_wset.size() );
    } else if ( p_type == partition_type::type::SORTED_MULTI_COLUMN) {
        timer_map.emplace(
            SORTED_MULTI_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID,
            begin_pid_wset.size() );
        timer_map.emplace(
            SORTED_MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID,
            begin_pid_wset.size() );
    }

    std::unordered_map<int, uint64_t> opt_timer_map = {};

    check_timers_contain( begin_res.timers, timer_map, opt_timer_map );

    std::vector<::cell_key> insert_keys = {ck1, ck2, ck3};

    // insert

    // uint64_t    insert_serialize_keys[3] = {3, 4, 5};

    std::vector<cell_key_ranges> write_ckrs;
    std::vector<cell_key_ranges> read_ckrs;

    write_ckrs.emplace_back(
        create_cell_key_ranges( 0, 3, 5, ycsb_row::id, ycsb_row::field1 ) );

    std::vector<arg_code> insert_arg_codes = {};
    std::vector<void *>   insert_arg_ptrs = {};
    sproc_helper( &handler, cid, k_ycsb_ckr_insert_sproc_name, write_ckrs,
                  read_ckrs, insert_arg_codes, insert_arg_ptrs );

    handler.rpc_commit_transaction( commit_res, cid );
    timer_map = {
        {PARTITION_BUILD_COMMIT_SNAPSHOT_TIMER_ID, begin_pid_wset.size()},
        {SERIALIZE_WRITE_BUFFER_TIMER_ID, begin_pid_wset.size()},
        {RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID, 1}};
    opt_timer_map = {};
    check_timers_contain( commit_res.timers, timer_map, opt_timer_map );
    cli_state = commit_res.session_version_vector;

    // remaster;
    std::vector<::partition_column_identifier> remaster_pids = {p_11_15_0_1};
    // HDB-TODO-UP
    std::vector<propagation_configuration> new_prop_configs;
    release_result                         release_res;
    handler.rpc_release_mastership( release_res, cid, remaster_pids, 1,
                                    new_prop_configs, cli_state );
    EXPECT_EQ( release_res.status, exec_status_type::MASTER_CHANGE_OK );
    cli_state = release_res.session_version_vector;

    // now trying to update pk2 should fail (it isn't mastered here anymore)
    handler.rpc_begin_transaction( begin_res, cid, cli_state, remaster_pids,
                                   begin_pid_rset, begin_pid_ifset );
    EXPECT_EQ( begin_res.status, exec_status_type::ERROR_WITH_WRITE_SET );
    // should be aborted
    /*
    abort_result abort_res;
    handler.rpc_abort_transaction( abort_res, cid );
    EXPECT_EQ( abort_res.status, exec_status_type::COMMAND_OK );
    */

    // scan
    handler.rpc_begin_transaction( begin_res, cid, cli_state, {}, {p_0_10_2_3},
                                   begin_pid_ifset );
    EXPECT_EQ( commit_res.status, exec_status_type::COMMAND_OK );
    uint64_t low_scan = 3;
    uint64_t high_scan = 5;
    uint32_t scan_col = 2;
    EXPECT_EQ( begin_res.status, exec_status_type::COMMAND_OK );
    timer_map = {{RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID, 1},
                 {PARTITION_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1}};
    if ( p_type == partition_type::type::ROW) {
        timer_map.emplace( ROW_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace( ROW_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1 );
    } else if ( p_type == partition_type::type::COLUMN) {
        timer_map.emplace( COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace( COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1 );
    } else if ( p_type == partition_type::type::SORTED_COLUMN) {
        timer_map.emplace( SORTED_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace(
            SORTED_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1 );
    } else if ( p_type == partition_type::type::MULTI_COLUMN) {
        timer_map.emplace( MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace(
            MULTI_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1 );
    } else if ( p_type == partition_type::type::SORTED_MULTI_COLUMN) {
        timer_map.emplace(
            SORTED_MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace(
            SORTED_MULTI_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1 );
    }


    opt_timer_map = {};
    check_timers_contain( begin_res.timers, timer_map, opt_timer_map );

    std::vector<arg_code> scan_arg_codes = {INTEGER_CODE, STRING_CODE};
    std::string           scan_pred = "E";
    scan_arg_codes.at( 1 ).array_length = scan_pred.size();
    std::vector<void *>   scan_arg_ptrs = {(void *) &scan_col,
                                         (void *) scan_pred.c_str()};

    write_ckrs.clear();
    read_ckrs.emplace_back(
        create_cell_key_ranges( 0, low_scan, high_scan, scan_col, scan_col ) );

    sproc_helper( &handler, cid, k_ycsb_scan_sproc_name, write_ckrs, read_ckrs,
                  scan_arg_codes, scan_arg_ptrs );
    handler.rpc_commit_transaction( commit_res, cid );
    EXPECT_EQ( commit_res.status, exec_status_type::COMMAND_OK );
    cli_state = commit_res.session_version_vector;
    timer_map = {{RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID, 1}};
    opt_timer_map = {};
    check_timers_contain( commit_res.timers, timer_map, opt_timer_map );

    // one shot scan
    one_shot_sproc_result one_shot_ret;
    char *                one_shot_buff = NULL;
    size_t                one_shot_serialize_len =
        serialize_for_sproc( scan_arg_codes, scan_arg_ptrs, &one_shot_buff );
    EXPECT_GE( one_shot_serialize_len, 4 );
    std::string one_shot_str_buff( one_shot_buff, one_shot_serialize_len );
    EXPECT_GE( one_shot_str_buff.size(), 4 );

    handler.rpc_one_shot_sproc( one_shot_ret, cid, cli_state, {}, {p_0_10_2_3},
                                begin_pid_ifset, k_ycsb_scan_sproc_name,
                                write_ckrs, read_ckrs, one_shot_str_buff );
    EXPECT_EQ( one_shot_ret.status, exec_status_type::COMMAND_OK );
    timer_map = {
        {PARTITION_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1},
        {RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID, 1},
    };
    if( p_type == partition_type::type::ROW ) {
        timer_map.emplace( ROW_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace( ROW_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1 );
        timer_map.emplace( ROW_SCAN_RECORDS_TIMER_ID, 1 );
    } else if( p_type == partition_type::type::COLUMN ) {
        timer_map.emplace( COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace( COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1 );
        timer_map.emplace( COL_SCAN_RECORDS_TIMER_ID, 1 );
    } else if( p_type == partition_type::type::SORTED_COLUMN ) {
        timer_map.emplace( SORTED_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace(
            SORTED_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1 );
        timer_map.emplace( SORTED_COL_SCAN_RECORDS_TIMER_ID, 1 );
    } else if( p_type == partition_type::type::MULTI_COLUMN ) {
        timer_map.emplace( MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace(
            MULTI_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1 );
        timer_map.emplace( MULTI_COL_SCAN_RECORDS_TIMER_ID, 1 );
    } else if( p_type == partition_type::type::SORTED_MULTI_COLUMN ) {
        timer_map.emplace(
            SORTED_MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace(
            SORTED_MULTI_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1 );
        timer_map.emplace( SORTED_MULTI_COL_SCAN_RECORDS_TIMER_ID, 1 );
    }

    opt_timer_map = {};
    check_timers_contain( one_shot_ret.timers, timer_map, opt_timer_map );
    EXPECT_EQ( 0, one_shot_ret.write_set.size() );
    EXPECT_EQ( 1, one_shot_ret.read_set.size() );
    EXPECT_EQ( p_0_10_2_3, one_shot_ret.read_set.at( 0 ) );

    cli_state = one_shot_ret.session_version_vector;

    free( one_shot_buff );

    one_shot_scan_result one_scan_ret;
    char *                one_scan_buff = NULL;

    std::vector<arg_code> one_scan_arg_codes = {};
    std::vector<void *>   one_scan_arg_ptrs = {};
    size_t                one_scan_serialize_len = serialize_for_sproc(
        one_scan_arg_codes, one_scan_arg_ptrs, &one_scan_buff );
    EXPECT_GE( one_scan_serialize_len, 4 );
    std::string one_scan_str_buff( one_scan_buff, one_scan_serialize_len );
    EXPECT_GE( one_scan_str_buff.size(), 4 );

    predicate_chain pred_chain;
    cell_predicate  c_pred;
    c_pred.table_id = 0;
    c_pred.col_id = scan_col;
    c_pred.type = data_type::type::STRING;
    c_pred.data = "foo";
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    predicate_chain pred;
    pred.and_predicates.emplace_back( c_pred );

    scan_arguments scan_arg;
    scan_arg.label = 0;
    scan_arg.predicate = pred;
    scan_arg.read_ckrs = read_ckrs;
    std::vector<scan_arguments> scan_args = {scan_arg};

    handler.rpc_one_shot_scan( one_scan_ret, cid, cli_state, {p_0_10_2_3},
                               begin_pid_ifset, k_ycsb_scan_data_sproc_name,
                               scan_args, one_scan_str_buff );
    EXPECT_EQ( one_scan_ret.status, exec_status_type::COMMAND_OK );
    timer_map = {
        {PARTITION_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1},
        {RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID, 1},
    };
    if( p_type == partition_type::type::ROW ) {
        timer_map.emplace( ROW_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace( ROW_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1 );
        timer_map.emplace( ROW_SCAN_RECORDS_TIMER_ID, 1 );
    } else if( p_type == partition_type::type::COLUMN ) {
        timer_map.emplace( COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace( COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1 );
        timer_map.emplace( COL_SCAN_RECORDS_TIMER_ID, 1 );
    } else if( p_type == partition_type::type::SORTED_COLUMN ) {
        timer_map.emplace( SORTED_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace(
            SORTED_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1 );
        timer_map.emplace( SORTED_COL_SCAN_RECORDS_TIMER_ID, 1 );
    } else if( p_type == partition_type::type::MULTI_COLUMN ) {
        timer_map.emplace( MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace(
            MULTI_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1 );
        timer_map.emplace( MULTI_COL_SCAN_RECORDS_TIMER_ID, 1 );
    } else if( p_type == partition_type::type::SORTED_MULTI_COLUMN ) {
        timer_map.emplace(
            SORTED_MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace(
            SORTED_MULTI_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1 );
        timer_map.emplace( SORTED_MULTI_COL_SCAN_RECORDS_TIMER_ID, 1 );
    }

    opt_timer_map = {};
    check_timers_contain( one_scan_ret.timers, timer_map, opt_timer_map );
    EXPECT_EQ( 0, one_scan_ret.write_set.size() );
    EXPECT_EQ( 1, one_scan_ret.read_set.size() );
    EXPECT_EQ( p_0_10_2_3, one_scan_ret.read_set.at( 0 ) );

    cli_state = one_scan_ret.session_version_vector;

    free( one_scan_buff );
#if 0
#endif
    (void) handler;
}

TEST_F( site_manager_test, handler_sproc_implementations_row ) {
    handler_sproc_implementations( partition_type::type::ROW );
}
TEST_F( site_manager_test, handler_sproc_implementations_col ) {
    handler_sproc_implementations( partition_type::type::COLUMN );
}
TEST_F( site_manager_test, handler_sproc_implementations_sorted_col ) {
    handler_sproc_implementations( partition_type::type::SORTED_COLUMN );
}
TEST_F( site_manager_test, handler_sproc_implementations_multi_col ) {
    handler_sproc_implementations( partition_type::type::MULTI_COLUMN );
}
TEST_F( site_manager_test, handler_sproc_implementations_sorted_multi_col ) {
    handler_sproc_implementations( partition_type::type::SORTED_MULTI_COLUMN );
}

void handler_rpcs( const partition_type::type &p_type ) {
    uint32_t num_clients = 2;
    uint32_t site_loc = 0;
    uint32_t gc_sleep_time = 10;

    auto no_op_update_gen = make_no_op_update_destination_generator();
    auto prop_config =
        no_op_update_gen->get_propagation_configurations().at( 0 );

    std::unique_ptr<db> database = construct_database(
        no_op_update_gen, make_update_enqueuers(), num_clients,
        1 /*num threads*/, workload_type::YCSB, site_loc /*site id*/,
        gc_sleep_time, false /* enable sec storage */,
        "/tmp" /*sec storage dir*/
        );

    std::unique_ptr<sproc_lookup_table> sproc_table =
        construct_ycsb_sproc_lookup_table();

    benchmark_configs b_cfg = construct_benchmark_configs(
        num_clients /*num clients*/, gc_sleep_time, 1 /*  bench time*/,
        1000 /* num operations before checking*/, false /* limit update prop*/,
        false /* enable sec storage */, "/tmp/" /*sec storage dir*/ );
    ycsb_configs ycsb_cfg = construct_ycsb_configs(
        1000 /*num keys*/, 10 /*value size*/, 5 /*partition size*/,
        7 /*num opers per txn*/, 1.0 /*zipf*/, false /*limit update prop*/,
        0.1 /*scan selectivity*/, 0 /* update prop limit*/,
        2 /*col partition size*/, p_type, storage_tier_type::type::MEMORY,
        true /* allow scan conflicts */, true /* allow rmw conflicts */,
        true /* store scan results */, b_cfg, 20 /*write*/, 20 /*read*/,
        20 /*rmw*/, 20 /*scan*/, 20 /*multi_rmw*/ );

    void *opaque_ptr = construct_ycsb_opaque_pointer( ycsb_cfg );

    site_manager_handler handler( std::move( database ),
                                  std::move( sproc_table ), opaque_ptr );
    handler.init( num_clients, 1 /* num client threads */, (void *) &ycsb_cfg );

    storage_tier_type::type s_type = storage_tier_type::type::MEMORY;

    snapshot_vector cli_state;
    clientid        cid = 1;

    partition_column_identifier p_0_10_0_1 =
        create_partition_column_identifier( 0, 0, 10, 0, 1 );
    partition_column_identifier p_11_15_0_1 =
        create_partition_column_identifier( 0, 11, 15, 0, 1 );
    partition_column_identifier p_0_10_2_3 =
        create_partition_column_identifier( 0, 0, 10, 2, 3 );
    partition_column_identifier p_11_15_2_3 =
        create_partition_column_identifier( 0, 11, 15, 2, 3 );

    commit_result commit_res;
    handler.rpc_add_partitions(
        commit_res, cid, cli_state,
        {p_0_10_0_1, p_11_15_0_1, p_0_10_2_3, p_11_15_2_3}, site_loc,
        {p_type, p_type, p_type, p_type}, {s_type, s_type, s_type, s_type},
        prop_config );

    ::cell_key ck1;
    ::cell_key ck2;
    ::cell_key ck3;
    ck1.table_id = 0;
    ck1.row_id = 3;
    ck1.col_id = ycsb_row::field1;
    ck2.table_id = 0;
    ck2.row_id = 4;
    ck2.col_id = ycsb_row::field1;
    ck3.table_id = 0;
    ck3.row_id = 5;
    ck3.col_id = ycsb_row::field1;

    // insert
    std::vector<::cell_key>    insert_keys = {ck1, ck2, ck3};
    std::vector<std::string>   insert_vals = {"three-0", "four-0", "five-0"};

    std::vector<::partition_column_identifier> write_pids = {p_0_10_2_3};
    std::vector<::partition_column_identifier> read_pids = {};
    std::vector<::partition_column_identifier> begin_pid_ifset = {};

    begin_result begin_res;
    handler.rpc_begin_transaction( begin_res, cid, cli_state, write_pids,
                                   read_pids, begin_pid_ifset );
    EXPECT_EQ( begin_res.status, exec_status_type::COMMAND_OK );
    std::unordered_map<int, uint64_t> timer_map = {
        {PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID, write_pids.size()},
        {RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID, 1},
    };
    if ( p_type == partition_type::type::ROW) {
        timer_map.emplace( ROW_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace( ROW_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID, 1 );
    } else if ( p_type == partition_type::type::COLUMN) {
        timer_map.emplace( COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace( COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID, 1 );
    } else if ( p_type == partition_type::type::SORTED_COLUMN) {
        timer_map.emplace( SORTED_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace(
            SORTED_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID, 1 );
    } else if( p_type == partition_type::type::MULTI_COLUMN ) {
        timer_map.emplace( MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace(
            MULTI_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID, 1 );
    } else if( p_type == partition_type::type::SORTED_MULTI_COLUMN ) {
        timer_map.emplace(
            SORTED_MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace(
            SORTED_MULTI_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID, 1 );
    }


    std::unordered_map<int, uint64_t> opt_timer_map = {};
    check_timers_contain( begin_res.timers, timer_map, opt_timer_map );

    query_result query_res;
    handler.rpc_insert( query_res, cid, insert_keys, insert_vals );
    EXPECT_EQ( query_res.status, exec_status_type::COMMAND_OK );

    handler.rpc_commit_transaction( commit_res, cid );
    EXPECT_EQ( commit_res.status, exec_status_type::COMMAND_OK );
    cli_state = commit_res.session_version_vector;
    timer_map = {{PARTITION_BUILD_COMMIT_SNAPSHOT_TIMER_ID, write_pids.size()},
                 {SERIALIZE_WRITE_BUFFER_TIMER_ID, write_pids.size()},
                 {RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID, 1}};
    opt_timer_map = {};
    check_timers_contain( commit_res.timers, timer_map, opt_timer_map );

    // delete
    std::vector<::cell_key> delete_keys = {ck2};
    handler.rpc_begin_transaction( begin_res, cid, cli_state, write_pids,
                                   read_pids, begin_pid_ifset );
    EXPECT_EQ( begin_res.status, exec_status_type::COMMAND_OK );
    timer_map = {
        {PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID, write_pids.size()},
        {RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID, 1},
    };
    if ( p_type == partition_type::type::ROW) {
        timer_map.emplace( ROW_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace( ROW_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID, 1 );
    } else if ( p_type == partition_type::type::COLUMN) {
        timer_map.emplace( COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace( COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID, 1 );
    } else if ( p_type == partition_type::type::SORTED_COLUMN) {
        timer_map.emplace( SORTED_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace(
            SORTED_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID, 1 );
    } else if( p_type == partition_type::type::MULTI_COLUMN ) {
        timer_map.emplace( MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace(
            MULTI_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID, 1 );
    } else if( p_type == partition_type::type::SORTED_MULTI_COLUMN ) {
        timer_map.emplace(
            SORTED_MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace(
            SORTED_MULTI_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID, 1 );
    }

    opt_timer_map = {};
    check_timers_contain( begin_res.timers, timer_map, opt_timer_map );

    handler.rpc_delete( query_res, cid, delete_keys );
    EXPECT_EQ( commit_res.status, exec_status_type::COMMAND_OK );

    handler.rpc_commit_transaction( commit_res, cid );
    EXPECT_EQ( commit_res.status, exec_status_type::COMMAND_OK );
    cli_state = commit_res.session_version_vector;
    timer_map = {
        {PARTITION_BUILD_COMMIT_SNAPSHOT_TIMER_ID, write_pids.size()},
        {SERIALIZE_WRITE_BUFFER_TIMER_ID, write_pids.size()},
        {RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID, 1},
    };
    opt_timer_map = {};
    check_timers_contain( commit_res.timers, timer_map, opt_timer_map );

    // update
    std::vector<::cell_key>    update_keys = {ck3};
    std::vector<std::string>   update_vals = {"five-1"};
    handler.rpc_begin_transaction( begin_res, cid, cli_state, write_pids,
                                   read_pids, begin_pid_ifset );
    EXPECT_EQ( begin_res.status, exec_status_type::COMMAND_OK );
    timer_map = {
        {PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID, write_pids.size()},
        {RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID, 1},
    };
    if ( p_type == partition_type::type::ROW) {
        timer_map.emplace( ROW_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID,
                           update_keys.size() );
        timer_map.emplace( ROW_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID,
                           update_keys.size() );
    } else if ( p_type == partition_type::type::COLUMN) {
        timer_map.emplace( COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID,
                           update_keys.size() );
        timer_map.emplace( COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID,
                           update_keys.size() );
    } else if ( p_type == partition_type::type::SORTED_COLUMN) {
        timer_map.emplace(
            SORTED_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID,
            update_keys.size() );
        timer_map.emplace( SORTED_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID,
                           update_keys.size() );
    } else if( p_type == partition_type::type::MULTI_COLUMN ) {
        timer_map.emplace(
            MULTI_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID,
            update_keys.size() );
        timer_map.emplace( MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID,
                           update_keys.size() );
    } else if( p_type == partition_type::type::SORTED_MULTI_COLUMN ) {
        timer_map.emplace(
            SORTED_MULTI_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID,
            update_keys.size() );
        timer_map.emplace(
            SORTED_MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID,
            update_keys.size() );
    }

    opt_timer_map = {};
    check_timers_contain( begin_res.timers, timer_map, opt_timer_map );

    handler.rpc_update( query_res, cid, update_keys, update_vals );
    EXPECT_EQ( query_res.status, exec_status_type::COMMAND_OK );

    timer_map = {{RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID, 1}};
    if ( p_type == partition_type::type::ROW) {
        timer_map.emplace( ROW_WRITE_RECORD_TIMER_ID,
                           update_keys.size() );
    } else if ( p_type == partition_type::type::COLUMN) {
        timer_map.emplace( COLUMN_WRITE_RECORD_TIMER_ID, update_keys.size() );
        timer_map.emplace( COL_READ_LATEST_RECORD_TIMER_ID,
                           update_keys.size() );
    } else if ( p_type == partition_type::type::SORTED_COLUMN) {
        timer_map.emplace( SORTED_COLUMN_WRITE_RECORD_TIMER_ID,
                           update_keys.size() );
        timer_map.emplace( SORTED_COL_READ_LATEST_RECORD_TIMER_ID,
                           update_keys.size() );
    } else if( p_type == partition_type::type::MULTI_COLUMN ) {
        timer_map.emplace( MULTI_COLUMN_WRITE_RECORD_TIMER_ID,
                           update_keys.size() );
        timer_map.emplace( MULTI_COL_READ_LATEST_RECORD_TIMER_ID,
                           update_keys.size() );
    } else if( p_type == partition_type::type::SORTED_MULTI_COLUMN ) {
        timer_map.emplace( SORTED_MULTI_COLUMN_WRITE_RECORD_TIMER_ID,
                           update_keys.size() );
        timer_map.emplace( SORTED_MULTI_COL_READ_LATEST_RECORD_TIMER_ID,
                           update_keys.size() );
   }

    opt_timer_map = {};
    check_timers_contain( query_res.timers, timer_map, opt_timer_map );

    handler.rpc_commit_transaction( commit_res, cid );
    EXPECT_EQ( commit_res.status, exec_status_type::COMMAND_OK );
    cli_state = commit_res.session_version_vector;
    timer_map = {{PARTITION_BUILD_COMMIT_SNAPSHOT_TIMER_ID, write_pids.size()},
                 {SERIALIZE_WRITE_BUFFER_TIMER_ID, write_pids.size()},
                 {RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID, 1}};

    opt_timer_map = {};
    check_timers_contain( commit_res.timers, timer_map, opt_timer_map );

    // select
    std::vector<::cell_key> select_keys = {ck1, ck2, ck3};
    handler.rpc_begin_transaction( begin_res, cid, cli_state, {}, {p_0_10_2_3},
                                   begin_pid_ifset );
    EXPECT_EQ( begin_res.status, exec_status_type::COMMAND_OK );
    timer_map = {
        {PARTITION_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1},
        {RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID, 1},
    };
    if ( p_type == partition_type::type::ROW) {
        timer_map.emplace( ROW_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace( ROW_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1 );
    } else if( p_type == partition_type::type::COLUMN ) {
        timer_map.emplace( COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace( COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1 );
    } else if( p_type == partition_type::type::SORTED_COLUMN ) {
        timer_map.emplace( SORTED_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace( SORTED_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1 );
    } else if( p_type == partition_type::type::MULTI_COLUMN ) {
        timer_map.emplace( MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace( MULTI_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1 );
    } else if( p_type == partition_type::type::SORTED_MULTI_COLUMN ) {
        timer_map.emplace(
            SORTED_MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID, 1 );
        timer_map.emplace(
            SORTED_MULTI_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID, 1 );
    }

    opt_timer_map = {};
    check_timers_contain( begin_res.timers, timer_map, opt_timer_map );

    handler.rpc_select( query_res, cid, select_keys );
    EXPECT_EQ( query_res.status, exec_status_type::TUPLES_OK );

    EXPECT_EQ( query_res.tuples.size(), 3 );

    EXPECT_EQ( query_res.tuples.at( 0 ).table_id, 0 );
    EXPECT_EQ( query_res.tuples.at( 0 ).row_id, 3 );
    EXPECT_EQ( query_res.tuples.at( 0 ).cells.size(), 1 );
    EXPECT_EQ( query_res.tuples.at( 0 ).cells.at( 0 ).col_id, ck1.col_id );
    EXPECT_TRUE( query_res.tuples.at( 0 ).cells.at( 0 ).present );
    EXPECT_EQ( query_res.tuples.at( 0 ).cells.at( 0 ).type,
               data_type::type::STRING );
    EXPECT_EQ( query_res.tuples.at( 0 ).cells.at( 0 ).data, "three-0" );

    EXPECT_EQ( query_res.tuples.at( 1 ).table_id, 0 );
    EXPECT_EQ( query_res.tuples.at( 1 ).row_id, 4 );
    EXPECT_EQ( query_res.tuples.at( 1 ).cells.at( 0 ).col_id, ck1.col_id );
    EXPECT_FALSE( query_res.tuples.at( 1 ).cells.at( 0 ).present );

    EXPECT_EQ( query_res.tuples.at( 2 ).table_id, 0 );
    EXPECT_EQ( query_res.tuples.at( 2 ).row_id, 5 );
    EXPECT_EQ( query_res.tuples.at( 2 ).cells.size(), 1 );
    EXPECT_EQ( query_res.tuples.at( 2 ).cells.at( 0 ).col_id, ck1.col_id );
    EXPECT_TRUE( query_res.tuples.at( 2 ).cells.at( 0 ).present );
    EXPECT_EQ( query_res.tuples.at( 2 ).cells.at( 0 ).type,
               data_type::type::STRING );
    EXPECT_EQ( query_res.tuples.at( 2 ).cells.at( 0 ).data, "five-1" );

    timer_map = {{RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID, 1}};
    if( p_type == partition_type::type::ROW ) {
        timer_map.emplace( ROW_READ_RECORD_TIMER_ID, select_keys.size() );
    } else if( p_type == partition_type::type::COLUMN ) {
        timer_map.emplace( COL_READ_RECORD_TIMER_ID, select_keys.size() );
    } else if( p_type == partition_type::type::SORTED_COLUMN ) {
        timer_map.emplace( SORTED_COL_READ_RECORD_TIMER_ID,
                           select_keys.size() );
    } else if( p_type == partition_type::type::MULTI_COLUMN ) {
        timer_map.emplace( MULTI_COL_READ_RECORD_TIMER_ID, select_keys.size() );
    } else if( p_type == partition_type::type::SORTED_MULTI_COLUMN ) {
        timer_map.emplace( SORTED_MULTI_COL_READ_RECORD_TIMER_ID,
                           select_keys.size() );
    }

    opt_timer_map = {};
    check_timers_contain( query_res.timers, timer_map, opt_timer_map );

    handler.rpc_commit_transaction( commit_res, cid );
    EXPECT_EQ( commit_res.status, exec_status_type::COMMAND_OK );
    cli_state = commit_res.session_version_vector;
    timer_map = {{RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID, 1}};
    opt_timer_map = {};
    check_timers_contain( commit_res.timers, timer_map, opt_timer_map );

    // let's try something that doesn't exist here fully and so it should return
    // an error!
    partition_column_identifier p_30_35 =
        create_partition_column_identifier( 0, 30, 35, 0, 1 );
    handler.rpc_begin_transaction( begin_res, cid, cli_state,
                                   {p_0_10_2_3, p_30_35}, {}, begin_pid_ifset );
    EXPECT_EQ( begin_res.status,
               exec_status_type::DATA_NOT_LOCATED_AT_ONE_SITE );

    // it shouldn't lock the partition so before so it should be fine
    handler.rpc_begin_transaction( begin_res, cid, cli_state, {p_0_10_2_3}, {},
                                   begin_pid_ifset );
    EXPECT_EQ( begin_res.status, exec_status_type::COMMAND_OK );

    handler.rpc_commit_transaction( commit_res, cid );
    EXPECT_EQ( commit_res.status, exec_status_type::COMMAND_OK );
    cli_state = commit_res.session_version_vector;
#if 0
#endif
    (void) handler;
}

TEST_F( site_manager_test, handler_rpcs_row ) {
    handler_rpcs( partition_type::type::ROW );
}
TEST_F( site_manager_test, handler_rpcs_col ) {
    handler_rpcs( partition_type::type::COLUMN );
}
TEST_F( site_manager_test, handler_rpcs_sorted_col ) {
    handler_rpcs( partition_type::type::SORTED_COLUMN );
}
TEST_F( site_manager_test, handler_rpcs_multi_col ) {
    handler_rpcs( partition_type::type::MULTI_COLUMN );
}
TEST_F( site_manager_test, handler_rpcs_sorted_multi_col ) {
    handler_rpcs( partition_type::type::SORTED_MULTI_COLUMN );
}


