#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/data-site/stored-procedures/stored_procedures_executor.h"
#include "../src/data-site/stored-procedures/ycsb/ycsb_prep_stmts.h"

class ycsb_sproc_test : public ::testing::Test {};

void ycsb_sproc_execute_test( const partition_type::type& p_type ) {
    uint32_t site_loc = 1;
    uint32_t num_tables = 1;
    uint32_t num_clients = 10;
    uint32_t gc_sleep_time = 10;
    bool enable_sec_storage = false;
    std::string sec_storage_dir = "/tmp";

    db database;
    database.init( make_no_op_update_destination_generator(),
                   make_update_enqueuers(),
                   create_tables_metadata( num_tables, site_loc, num_clients,
                                           gc_sleep_time, enable_sec_storage,
                                           sec_storage_dir ) );

    snapshot_vector       c1_state;
    std::unique_ptr<sproc_lookup_table> sproc_table =
        construct_ycsb_sproc_lookup_table();

    benchmark_configs b_cfg = construct_benchmark_configs(
        5 /*num clients*/, 1 /*  bench time*/, gc_sleep_time,
        500 /* num operations before checking*/, false /*limit update prop*/,
        enable_sec_storage, sec_storage_dir );
    ycsb_configs ycsb_cfg = construct_ycsb_configs(
        500 /*num keys*/, 10 /*value size*/, 5 /*partition size*/,
        7 /*num opers per txn*/, 1.0 /*zipf*/, false /*limit update prop*/,
        0.1 /*scan selectivity*/, 0 /* update prop limit*/,
        2 /* col partition size*/, p_type, storage_tier_type::type::MEMORY,
        true /* allow scan conflicts */, true /* allow rmw conflicts */,
        true /* store scan results */, b_cfg, 20 /*write*/, 20 /*read*/,
        20 /*rmw*/, 20 /*scan*/, 20 /*multi_rmw*/ );

    void* opaque_ptr = construct_ycsb_opaque_pointer( ycsb_cfg );

    std::vector<arg_code> create_table_args = {BIGINT_CODE, BIGINT_CODE};
    std::vector<void*>    create_table_vals = {(void*) &database,
                                            (void*) &ycsb_cfg};
    function_identifier   create_func_id( k_create_config_tables_sproc_name,
                                        create_table_args );

    sproc_result res;

    function_skeleton create_function =
        sproc_table->lookup_function( create_func_id );

    std::vector<cell_key_ranges> write_ckrs;
    std::vector<cell_key_ranges> read_ckrs;


    create_function( nullptr, 0, write_ckrs, read_ckrs, create_table_args,
                     create_table_vals, opaque_ptr );

    ycsb_sproc_helper_holder* holder = (ycsb_sproc_helper_holder*) opaque_ptr;
    ycsb_benchmark_worker_templ_no_commit* worker =
        holder->get_worker_and_set_holder( 0, nullptr );

    uint64_t key_3 = 3;
    uint64_t key_4 = 4;
    uint64_t key_5 = 5;

    // just do an insert here
    partition_lookup_operation lookup_op =
        partition_lookup_operation::GET_OR_CREATE;

    auto write_pid_list =
        worker->generate_partition_set( 0, 9, ycsb_row::id, ycsb_row::field1 );

    write_ckrs.push_back( create_cell_key_ranges( 0, key_3, key_5, ycsb_row::id,
                                                  ycsb_row::field1 ) );

    partition_column_identifier_set write_pids;
    partition_column_identifier_set read_pids;
    partition_column_identifier_set inflight_pids;
    write_pids.insert( write_pid_list.begin(), write_pid_list.end() );

    transaction_partition_holder* txn_holder =
        database.get_partitions_with_begin( 1, c1_state, write_pids, read_pids,
                                            inflight_pids, lookup_op );
    EXPECT_NE( txn_holder, nullptr );

    uint64_t insert_keys[3] = {key_3, key_4, key_5};

    std::vector<void*> insert_vals = {(void*) insert_keys};

    std::vector<arg_code> insert_args = {{BIGINT, 3, true}};
    function_identifier insert_func_id( k_ycsb_insert_sproc_name, insert_args );
    function_skeleton   insert_function =
        sproc_table->lookup_function( insert_func_id );
    EXPECT_NE( insert_function, nullptr );
    res = insert_function( txn_holder, 1, write_ckrs, read_ckrs, insert_args,
                           insert_vals, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    txn_holder = nullptr;

    lookup_op = partition_lookup_operation::GET;

    // uint64_t keys[2] = {key_3, key_4};
    write_pid_list = worker->generate_partition_set(
        key_3, key_4, ycsb_row::timestamp, ycsb_row::field0 );

    write_pids.clear();
    write_pids.insert( write_pid_list.begin(), write_pid_list.end() );
    read_pids = write_pids;

    write_ckrs.clear();
    read_ckrs.clear();

    write_ckrs.emplace_back( create_cell_key_ranges(
        0, key_3, key_4, ycsb_row::timestamp, ycsb_row::field0 ) );
    read_ckrs.emplace_back( create_cell_key_ranges(
        0, key_3, key_4, ycsb_row::timestamp, ycsb_row::field0 ) );


    txn_holder = database.get_partitions_with_begin(
        1, c1_state, write_pids, read_pids, inflight_pids, lookup_op );
    EXPECT_NE( txn_holder, nullptr );

    std::vector<arg_code> mk_rmw_args = {{BOOL, 0, false}};

    uint32_t col = ycsb_row::field0;
    bool               do_update_prop = true;
    std::vector<void*> mk_vals = {(void*) &do_update_prop};

    function_identifier mk_rmw_func_id( k_ycsb_mk_rmw_sproc_name, mk_rmw_args );
    function_skeleton   mk_rmw_function =
        sproc_table->lookup_function( mk_rmw_func_id );
    EXPECT_NE( mk_rmw_function, nullptr );
    mk_rmw_function( txn_holder, 1, write_ckrs, read_ckrs, mk_rmw_args, mk_vals,
                     opaque_ptr );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    txn_holder = nullptr;

    std::vector<arg_code> rmw_4_args = {};
    write_pid_list = worker->generate_partition_set(
        key_4, key_4, ycsb_row::timestamp, ycsb_row::field0 );

    write_ckrs.clear();
    read_ckrs.clear();

    write_ckrs.emplace_back(
        create_cell_key_ranges( 0, key_4, key_4, col, col ) );
    read_ckrs.emplace_back(
        create_cell_key_ranges( 0, key_4, key_4, col, col ) );


    write_pids.clear();
    write_pids.insert( write_pid_list.begin(), write_pid_list.end() );
    read_pids = write_pids;

    txn_holder = database.get_partitions_with_begin(
        1, c1_state, write_pids, read_pids, inflight_pids, lookup_op );
    EXPECT_NE( txn_holder, nullptr );

    function_identifier rmw_func_id( k_ycsb_rmw_sproc_name, rmw_4_args );
    function_skeleton   rmw_function =
        sproc_table->lookup_function( rmw_func_id );
    EXPECT_NE( rmw_function, nullptr );
    std::vector<void*> rmw_vals = {};
    res = rmw_function( txn_holder, 1, write_ckrs, read_ckrs, rmw_4_args,
                        rmw_vals, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    txn_holder = nullptr;

    write_ckrs.clear();
    read_ckrs.clear();

    write_pids.clear();
    read_pids.clear();
    auto read_pid_list = worker->generate_partition_set(
        key_4, key_5, ycsb_row::field0, ycsb_row::field0 );

    read_ckrs.emplace_back( create_cell_key_ranges(
        0, key_4, key_5, ycsb_row::field0, ycsb_row::field0 ) );

    read_pids.insert( read_pid_list.begin(), read_pid_list.end() );

    txn_holder = database.get_partitions_with_begin(
        1, c1_state, write_pids, read_pids, inflight_pids, lookup_op );
    EXPECT_NE( txn_holder, nullptr );

    std::vector<arg_code> scan_code = {INTEGER_CODE, STRING_CODE};
	std::string pred_str = "foo";
    scan_code.at( 1 ).array_length = pred_str.size();
    function_identifier   scan_func_id( k_ycsb_scan_sproc_name, scan_code );
    function_skeleton     scan_function =
        sproc_table->lookup_function( scan_func_id );
    EXPECT_NE( scan_function, nullptr );
    std::vector<void*> scan_vals = {(void*) &col, (void*) pred_str.c_str()};
    res = scan_function( txn_holder, 1, write_ckrs, read_ckrs, scan_code,
                         scan_vals, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

    std::vector<arg_code> scan_record_code = {};

    predicate_chain pred_chain;
    cell_predicate  c_pred;
    c_pred.table_id = 0;
    c_pred.col_id = col;
    c_pred.type = data_type::type::STRING;
    c_pred.data = pred_str;
    c_pred.predicate = predicate_type::type::LESS_THAN_OR_EQUAL;
    predicate_chain pred;
    pred.and_predicates.emplace_back( c_pred );

    scan_arguments scan_arg;
    scan_arg.label = 0;
    scan_arg.predicate = pred;
    scan_arg.read_ckrs = read_ckrs;

    std::vector<scan_arguments> scan_args = {scan_arg};

    function_identifier scan_record_func_id( k_ycsb_scan_data_sproc_name,
                                             scan_record_code );
    scan_function_skeleton scan_record_function =
        sproc_table->lookup_scan_function( scan_record_func_id );
    EXPECT_NE( scan_record_function, nullptr );
    std::vector<void*> scan_record_vals = {};
    scan_result        scan_res;
    scan_record_function( txn_holder, 1, scan_args, scan_record_code,
                          scan_record_vals, opaque_ptr, scan_res );
    EXPECT_EQ( scan_res.status, exec_status_type::COMMAND_OK );

    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    txn_holder = nullptr;

    std::vector<arg_code> update_5_args = {};
    read_pids.clear();
    write_pids.clear();

    write_ckrs.clear();
    read_ckrs.clear();

    write_ckrs.emplace_back( create_cell_key_ranges(
        0, key_5, key_5, ycsb_row::id, ycsb_row::field1 ) );

    write_pid_list = worker->generate_partition_set( key_5, key_5, ycsb_row::id,
                                                     ycsb_row::field1 );
    write_pids.insert( write_pid_list.begin(), write_pid_list.end() );
    txn_holder = database.get_partitions_with_begin(
        1, c1_state, write_pids, read_pids, inflight_pids, lookup_op );
    EXPECT_NE( txn_holder, nullptr );
    function_identifier update_func_id( k_ycsb_update_sproc_name,
                                        update_5_args );
    function_skeleton update_function =
        sproc_table->lookup_function( update_func_id );
    EXPECT_NE( update_function, nullptr );
    std::vector<void*> update_vals = {};
    res = update_function( txn_holder, 1, write_ckrs, read_ckrs, update_5_args,
                           update_vals, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    txn_holder = nullptr;

    write_ckrs.clear();
    read_ckrs.clear();

    read_ckrs.emplace_back( create_cell_key_ranges(
        0, key_3, key_3, ycsb_row::id, ycsb_row::field1 ) );

    read_pids.clear();
    write_pids.clear();
    read_pid_list = worker->generate_partition_set( key_3, key_3, ycsb_row::id,
                                                    ycsb_row::field1 );
    read_pids.insert( read_pid_list.begin(), read_pid_list.end() );
    txn_holder = database.get_partitions_with_begin(
        1, c1_state, write_pids, read_pids, inflight_pids, lookup_op );
    EXPECT_NE( txn_holder, nullptr );
    std::vector<arg_code> read_code = {};
    function_identifier   read_func_id( k_ycsb_read_sproc_name, read_code );
    function_skeleton     read_function =
        sproc_table->lookup_function( read_func_id );
    EXPECT_NE( read_function, nullptr );
    std::vector<void*> read_vals = {};
    res = read_function( txn_holder, 1, write_ckrs, read_ckrs, read_code,
                         read_vals, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    txn_holder = nullptr;

    write_ckrs.clear();
    read_ckrs.clear();

    write_ckrs.emplace_back( create_cell_key_ranges(
        0, key_5, key_5, ycsb_row::id, ycsb_row::field1 ) );

    read_pids.clear();
    write_pids.clear();
    write_pid_list = worker->generate_partition_set( key_5, key_5, ycsb_row::id,
                                                     ycsb_row::field1 );
    write_pids.insert( write_pid_list.begin(), write_pid_list.end() );

    txn_holder = database.get_partitions_with_begin(
        1, c1_state, write_pids, read_pids, inflight_pids, lookup_op );
    EXPECT_NE( txn_holder, nullptr );
    std::vector<arg_code> delete_code = {};
    function_identifier delete_func_id( k_ycsb_delete_sproc_name, delete_code );
    function_skeleton   delete_function =
        sproc_table->lookup_function( delete_func_id );
    EXPECT_NE( delete_function, nullptr );
    std::vector<void*> delete_vals = {};
    res = delete_function( txn_holder, 1, write_ckrs, read_ckrs, delete_code,
                           delete_vals, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    txn_holder = nullptr;
}
TEST_F( ycsb_sproc_test, ycsb_sproc_execute_test_row ) {
    ycsb_sproc_execute_test( partition_type::type::ROW );
}
TEST_F( ycsb_sproc_test, ycsb_sproc_execute_test_col ) {
    ycsb_sproc_execute_test( partition_type::type::COLUMN );
}
TEST_F( ycsb_sproc_test, ycsb_sproc_execute_test_sorted_col ) {
    ycsb_sproc_execute_test( partition_type::type::SORTED_COLUMN );
}
TEST_F( ycsb_sproc_test, ycsb_sproc_execute_test_multi_col ) {
    ycsb_sproc_execute_test( partition_type::type::MULTI_COLUMN );
}
TEST_F( ycsb_sproc_test, ycsb_sproc_execute_test_sorted_multi_col ) {
    ycsb_sproc_execute_test( partition_type::type::SORTED_MULTI_COLUMN );
}
