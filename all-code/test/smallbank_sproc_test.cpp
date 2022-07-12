#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/data-site/site-manager/serialize.h"
#include "../src/data-site/stored-procedures/smallbank/smallbank_prep_stmts.h"
#include "../src/data-site/stored-procedures/stored_procedures_executor.h"

class smallbank_sproc_test : public ::testing::Test {};

snapshot_vector create_and_load_smallbank_table(
    db* database, sproc_lookup_table* sproc_table,
    const smallbank_configs& smallbank_cfg, void* opaque_ptr ) {
    snapshot_vector                c1_state;
    smallbank_sproc_helper_holder* holder =
        (smallbank_sproc_helper_holder*) opaque_ptr;
    std::vector<arg_code> create_table_args = {BIGINT_CODE, BIGINT_CODE};
    std::vector<void*>    create_table_vals = {(void*) database,
                                            (void*) &smallbank_cfg};
    function_identifier create_func_id( k_create_config_tables_sproc_name,
                                        create_table_args );

    sproc_result res;

    uint32_t cli_id = 0;

    std::vector<cell_key_ranges> write_ckrs;
    std::vector<cell_key_ranges> read_ckrs;

    function_skeleton create_function =
        sproc_table->lookup_function( create_func_id );
    res = create_function( nullptr, cli_id, write_ckrs, read_ckrs,
                           create_table_args, create_table_vals, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

    smallbank_loader_templ_no_commit* loader =
        holder->get_loader_and_set_holder( cli_id, nullptr );

    std::vector<arg_code> load_range_of_accounts_arg_codes = {BIGINT_CODE,
                                                              BIGINT_CODE};
    uint64_t            a_start = 0;
    uint64_t            a_end = smallbank_cfg.num_accounts_ - 1;
    std::vector<void*>  insert_args = {(void*) &a_start, (void*) &a_end};
    function_identifier insert_accounts_func_id(
        k_smallbank_load_range_of_accounts_sproc_name,
        load_range_of_accounts_arg_codes );
    function_skeleton insert_accounts_function =
        sproc_table->lookup_function( insert_accounts_func_id );

    std::vector<uint32_t> table_ids = {k_smallbank_accounts_table_id,
                                       k_smallbank_savings_table_id,
                                       k_smallbank_checkings_table_id};
    auto tables_meta = create_smallbank_table_metadata( smallbank_cfg );
    partition_column_identifier_set insert_pids;

    for( uint32_t table_id : table_ids ) {
        auto pids = loader->generate_partition_column_identifiers(
            table_id, a_start, a_end, tables_meta.at( table_id ).num_columns_ );

        write_ckrs.emplace_back( create_cell_key_ranges(
            table_id, a_start, a_end, 0,
            tables_meta.at( table_id ).num_columns_ - 1 ) );

        insert_pids.insert( pids.begin(), pids.end() );
    }
    partition_column_identifier_set inflight_pids;

    // just do an add here
    partition_lookup_operation lookup_op =
        partition_lookup_operation::GET_OR_CREATE;

    transaction_partition_holder* txn_holder =
        database->get_partitions_with_begin( cli_id, c1_state, insert_pids, {},
                                             inflight_pids, lookup_op );
    loader = holder->get_loader_and_set_holder( cli_id, txn_holder );

    res = insert_accounts_function( txn_holder, cli_id, read_ckrs, write_ckrs,
                                    load_range_of_accounts_arg_codes,
                                    insert_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();

    delete txn_holder;

    return c1_state;
}

snapshot_vector create_and_load_arg_based_smallbank_table(
    db* database, sproc_lookup_table* sproc_table,
    const smallbank_configs& smallbank_cfg, void* opaque_ptr ) {
    snapshot_vector                c1_state;
    smallbank_sproc_helper_holder* holder =
        (smallbank_sproc_helper_holder*) opaque_ptr;

    std::vector<cell_key_ranges> write_ckrs;
    std::vector<cell_key_ranges> read_ckrs;

    std::vector<arg_code> create_table_args = {BIGINT_CODE, BIGINT_CODE};
    std::vector<void*>    create_table_vals = {(void*) database,
                                            (void*) &smallbank_cfg};
    function_identifier create_func_id( k_create_config_tables_sproc_name,
                                        create_table_args );

    sproc_result res;

    uint32_t cli_id = 0;

    function_skeleton create_function =
        sproc_table->lookup_function( create_func_id );
    res = create_function( nullptr, cli_id, write_ckrs, read_ckrs,
                           create_table_args, create_table_vals, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

    smallbank_loader_templ_no_commit* loader =
        holder->get_loader_and_set_holder( cli_id, nullptr );

    uint64_t a_start = 0;
    uint64_t a_end = smallbank_cfg.num_accounts_ - 1;

    std::vector<std::string> sproc_names = {
        k_smallbank_load_assorted_accounts_sproc_name,
        k_smallbank_load_assorted_savings_sproc_name,
        k_smallbank_load_assorted_checkings_sproc_name};
    std::vector<uint32_t> table_ids = {k_smallbank_accounts_table_id,
                                       k_smallbank_savings_table_id,
                                       k_smallbank_checkings_table_id};
    auto tables_meta = create_smallbank_table_metadata( smallbank_cfg );
    EXPECT_EQ( sproc_names.size(), table_ids.size() );
    partition_column_identifier_set inflight_pids;

    for( uint32_t pos = 0; pos < table_ids.size(); pos++ ) {
        uint32_t    table_id = table_ids.at( pos );
        std::string sproc_name = sproc_names.at( pos );

        DLOG( INFO ) << "Loading:" << table_id << ", sproc name:" << sproc_name;

        std::vector<uint64_t> rows;
        for( uint64_t key = a_start; key <= a_end; key++ ) {
            rows.push_back( key );
        }
        auto pids = loader->generate_partition_column_identifiers(
            table_id, a_start, a_end, tables_meta.at( table_id ).num_columns_ );
        partition_column_identifier_set insert_pids;
        insert_pids.insert( pids.begin(), pids.end() );
        write_ckrs.emplace_back( create_cell_key_ranges(
            table_id, a_start, a_end, 0,
            tables_meta.at( table_id ).num_columns_ - 1 ) );

        std::vector<void*> insert_args = {(void*) rows.data()};

        arg_code insert_arg_code = BIGINT_ARRAY_CODE;
        insert_arg_code.array_length = rows.size();
        std::vector<arg_code> insert_codes = {insert_arg_code};

        DLOG( INFO ) << "Looking up function id, sproc name:" << sproc_name;

        function_identifier insert_func_id( sproc_name, insert_codes );
        function_skeleton   insert_function =
            sproc_table->lookup_function( insert_func_id );

        // just do an add here
        partition_lookup_operation lookup_op =
            partition_lookup_operation::GET_OR_CREATE;

        transaction_partition_holder* txn_holder =
            database->get_partitions_with_begin( cli_id, c1_state, insert_pids,
                                                 {}, inflight_pids, lookup_op );
        loader = holder->get_loader_and_set_holder( cli_id, txn_holder );

        res = insert_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                               insert_codes, insert_args, opaque_ptr );
        EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
        c1_state = txn_holder->commit_transaction();

        delete txn_holder;

        write_ckrs.clear();
        read_ckrs.clear();
    }

    return c1_state;
}

void smallbank_sproc_execute_test( const partition_type::type& p_type ) {
    uint32_t site_loc = 1;
    uint32_t num_tables = 3;
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

    uint32_t cli_id = 0;

    snapshot_vector                     c1_state;
    std::unique_ptr<sproc_lookup_table> sproc_table =
        construct_smallbank_sproc_lookup_table();

    benchmark_configs b_cfg = construct_benchmark_configs(
        1 /*num clients*/, gc_sleep_time, 1 /*  bench time*/,
        1000 /* num operations before checking*/, false /* limit update prop*/,
        enable_sec_storage, sec_storage_dir );

    smallbank_configs smallbank_cfg = construct_smallbank_configs(
        500 /*num acounts*/, false /*hotspot used fixed size*/,
        25.0 /* hotspot percentage*/, 100 /* hotspot fixed size*/,
        10 /*partition size*/, 10 /*account spread*/, 1 /* account col size*/,
        p_type, storage_tier_type::type::MEMORY, 1 /* banking col type*/,
        p_type, storage_tier_type::type::MEMORY, b_cfg, 15 /*amalgamate */,
        15 /*balance*/, 15 /*deposit checking*/, 25 /*send payment*/,
        15 /* transact savings*/, 15 /* write check */ );

    void* opaque_ptr = construct_smallbank_opaque_pointer( smallbank_cfg );
    c1_state = create_and_load_smallbank_table( &database, sproc_table.get(),
                                                smallbank_cfg, opaque_ptr );

    smallbank_sproc_helper_holder* holder =
        (smallbank_sproc_helper_holder*) opaque_ptr;
    smallbank_loader_templ_no_commit* loader =
        holder->get_loader_and_set_holder( 0, nullptr );

    uint64_t source_id = 17;
    uint64_t dest_id = 23;
    int      amount = 3;

    partition_column_identifier source_saving_pid =
        loader->generate_partition_column_identifier(
            k_smallbank_savings_table_id, source_id, smallbank_saving::balance,
            k_smallbank_savings_num_columns );
    cell_key_ranges source_saving_ckr = create_cell_key_ranges(
        k_smallbank_savings_table_id, source_id, source_id,
        smallbank_saving::balance, k_smallbank_savings_num_columns - 1 );
    partition_column_identifier source_checking_pid =
        loader->generate_partition_column_identifier(
            k_smallbank_checkings_table_id, source_id,
            smallbank_checking::balance, k_smallbank_checkings_num_columns );
    cell_key_ranges source_checking_ckr = create_cell_key_ranges(
        k_smallbank_checkings_table_id, source_id, source_id,
        smallbank_checking::balance, k_smallbank_checkings_num_columns - 1 );
    partition_column_identifier dest_saving_pid =
        loader->generate_partition_column_identifier(
            k_smallbank_savings_table_id, dest_id, smallbank_saving::balance,
            k_smallbank_savings_num_columns );
    cell_key_ranges dest_saving_ckr = create_cell_key_ranges(
        k_smallbank_savings_table_id, dest_id, dest_id,
        smallbank_saving::balance, k_smallbank_savings_num_columns - 1 );
    partition_column_identifier dest_checking_pid =
        loader->generate_partition_column_identifier(
            k_smallbank_checkings_table_id, dest_id,
            smallbank_checking::balance, k_smallbank_checkings_num_columns );
    cell_key_ranges dest_checking_ckr = create_cell_key_ranges(
        k_smallbank_checkings_table_id, dest_id, dest_id,
        smallbank_checking::balance, k_smallbank_checkings_num_columns - 1 );

    sproc_result res;

    std::vector<cell_key_ranges> write_ckrs;
    std::vector<cell_key_ranges> read_ckrs;

    // balance
    std::vector<arg_code> balance_arg_codes = {BIGINT_CODE};
    std::vector<void*>    balance_args = {(void*) &source_id};

    function_identifier balance_func_id( k_smallbank_balance_sproc_name,
                                         balance_arg_codes );
    function_skeleton balance_function =
        sproc_table->lookup_function( balance_func_id );

    partition_lookup_operation      lookup_op = partition_lookup_operation::GET;
    partition_column_identifier_set write_pids;
    partition_column_identifier_set read_pids;
    partition_column_identifier_set inflight_pids;

    read_pids.emplace( source_saving_pid );
    read_pids.emplace( source_checking_pid );

    read_ckrs.emplace_back( source_saving_ckr );
    read_ckrs.emplace_back( source_checking_ckr );

    transaction_partition_holder* txn_holder =
        database.get_partitions_with_begin(
            cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );

    res = balance_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                            balance_arg_codes, balance_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    read_pids.clear();
    read_ckrs.clear();

    // send payment
    std::vector<arg_code> send_payment_arg_codes = {BIGINT_CODE, BIGINT_CODE,
                                                    INTEGER_CODE};
    std::vector<void*> send_payment_args = {(void*) &source_id,
                                            (void*) &dest_id, (void*) &amount};

    function_identifier send_payment_func_id(
        k_smallbank_send_payment_sproc_name, send_payment_arg_codes );
    function_skeleton send_payment_function =
        sproc_table->lookup_function( send_payment_func_id );

    write_pids.emplace( source_saving_pid );
    write_ckrs.emplace_back( source_saving_ckr );
    read_pids.emplace( source_saving_pid );
    read_ckrs.emplace_back( source_saving_ckr );
    write_pids.emplace( dest_checking_pid );
    write_ckrs.emplace_back( dest_checking_ckr );
    read_pids.emplace( dest_checking_pid );
    read_ckrs.emplace_back( dest_checking_ckr );

    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );
    res = send_payment_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                                 send_payment_arg_codes, send_payment_args,
                                 opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    write_pids.clear();
    read_pids.clear();
    write_ckrs.clear();
    read_ckrs.clear();

    // deposit checking
    std::vector<arg_code> deposit_checking_arg_codes = {BIGINT_CODE,
                                                        INTEGER_CODE};
    std::vector<void*> deposit_checking_args = {(void*) &source_id,
                                                (void*) &amount};

    function_identifier deposit_checking_func_id(
        k_smallbank_deposit_checking_sproc_name, deposit_checking_arg_codes );
    function_skeleton deposit_checking_function =
        sproc_table->lookup_function( deposit_checking_func_id );

    write_pids.emplace( source_checking_pid );
    read_pids.emplace( source_checking_pid );
    write_ckrs.emplace_back( source_checking_ckr );
    read_ckrs.emplace_back( source_checking_ckr );
    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );
    res = deposit_checking_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                                     deposit_checking_arg_codes,
                                     deposit_checking_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    write_pids.clear();
    read_pids.clear();
    write_ckrs.clear();
    read_ckrs.clear();

    // transact savings
    std::vector<arg_code> transact_savings_arg_codes = {BIGINT_CODE,
                                                        INTEGER_CODE};
    std::vector<void*> transact_savings_args = {(void*) &source_id,
                                                (void*) &amount};

    function_identifier transact_savings_func_id(
        k_smallbank_transact_savings_sproc_name, transact_savings_arg_codes );
    function_skeleton transact_savings_function =
        sproc_table->lookup_function( transact_savings_func_id );

    write_pids.emplace( source_saving_pid );
    write_ckrs.emplace_back( source_saving_ckr );
    read_pids.emplace( source_saving_pid );
    read_ckrs.emplace_back( source_saving_ckr );
    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );

    res = transact_savings_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                                     transact_savings_arg_codes,
                                     transact_savings_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    write_pids.clear();
    read_pids.clear();
    write_ckrs.clear();
    read_ckrs.clear();

    // write check
    std::vector<arg_code> write_check_arg_codes = {BIGINT_CODE, INTEGER_CODE};
    std::vector<void*> write_check_args = {(void*) &source_id, (void*) &amount};

    function_identifier write_check_func_id( k_smallbank_write_check_sproc_name,
                                             write_check_arg_codes );
    function_skeleton write_check_function =
        sproc_table->lookup_function( write_check_func_id );

    write_pids.emplace( source_saving_pid );
    write_ckrs.emplace_back(source_saving_ckr);
    read_pids.emplace( source_saving_pid );
    read_ckrs.emplace_back(source_saving_ckr);
    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );
    res = write_check_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                                write_check_arg_codes, write_check_args,
                                opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    write_pids.clear();
    read_pids.clear();
    write_ckrs.clear();
    read_ckrs.clear();

    // amalgamate
    std::vector<arg_code> amalgamate_arg_codes = {BIGINT_CODE, BIGINT_CODE};
    std::vector<void*> amalgamate_args = {(void*) &source_id, (void*) &dest_id};

    function_identifier amalgamate_func_id( k_smallbank_amalgamate_sproc_name,
                                            amalgamate_arg_codes );
    function_skeleton amalgamate_function =
        sproc_table->lookup_function( amalgamate_func_id );

    write_pids.emplace( source_saving_pid );
    write_ckrs.emplace_back( source_saving_ckr );
    read_pids.emplace( source_saving_pid );
    read_ckrs.emplace_back( source_saving_ckr );
    write_pids.emplace( dest_checking_pid );
    write_ckrs.emplace_back( dest_checking_ckr );
    read_pids.emplace( dest_checking_pid );
    read_ckrs.emplace_back( dest_checking_ckr );
    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );
    res = amalgamate_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                               amalgamate_arg_codes, amalgamate_args,
                               opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
}

TEST_F( smallbank_sproc_test, smallbank_sproc_execute_test_row ) {
    smallbank_sproc_execute_test( partition_type::type::ROW );
}
TEST_F( smallbank_sproc_test, smallbank_sproc_execute_test_column ) {
    smallbank_sproc_execute_test( partition_type::type::COLUMN );
}
TEST_F( smallbank_sproc_test, smallbank_sproc_execute_test_sorted_column ) {
    smallbank_sproc_execute_test( partition_type::type::SORTED_COLUMN );
}

void smallbank_args_sproc_execute_test( const partition_type::type& p_type ) {
    uint32_t site_loc = 1;
    uint32_t num_tables = 3;
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

    uint32_t cli_id = 0;

    snapshot_vector                     c1_state;
    std::unique_ptr<sproc_lookup_table> sproc_table =
        construct_smallbank_sproc_lookup_table();

    benchmark_configs b_cfg = construct_benchmark_configs(
        1 /*num clients*/, gc_sleep_time, 1 /*  bench time*/,
        1000 /* num operations before checking*/, false /* limit update prop*/,
        enable_sec_storage, sec_storage_dir );
    smallbank_configs smallbank_cfg = construct_smallbank_configs(
        500 /*num acounts*/, false /*hotspot used fixed size*/,
        25.0 /* hotspot percentage*/, 100 /* hotspot fixed size*/,
        10 /*partition size*/, 10 /*account spread*/, 1 /* account col size*/,
        p_type, storage_tier_type::type::MEMORY, 1 /* banking col type*/,
        p_type, storage_tier_type::type::MEMORY, b_cfg, 15 /*amalgamate */,
        15 /*balance*/, 15 /*deposit checking*/, 25 /*send payment*/,
        15 /* transact savings*/, 15 /* write check */ );

    void* opaque_ptr = construct_smallbank_opaque_pointer( smallbank_cfg );
    c1_state = create_and_load_arg_based_smallbank_table(
        &database, sproc_table.get(), smallbank_cfg, opaque_ptr );

    smallbank_sproc_helper_holder* holder =
        (smallbank_sproc_helper_holder*) opaque_ptr;
    smallbank_loader_templ_no_commit* loader =
        holder->get_loader_and_set_holder( 0, nullptr );

    uint64_t source_id = 17;
    uint64_t dest_id = 23;
    int      amount = 3;

    uint32_t checkings_id = k_smallbank_checkings_table_id;
    uint32_t savings_id = k_smallbank_savings_table_id;

    partition_column_identifier source_saving_pid =
        loader->generate_partition_column_identifier(
            k_smallbank_savings_table_id, source_id, smallbank_saving::balance,
            k_smallbank_savings_num_columns );
    cell_key_ranges source_saving_ckr = create_cell_key_ranges(
        k_smallbank_savings_table_id, source_id, source_id,
        smallbank_saving::balance, k_smallbank_savings_num_columns - 1 );
    partition_column_identifier source_checking_pid =
        loader->generate_partition_column_identifier(
            k_smallbank_checkings_table_id, source_id,
            smallbank_checking::balance, k_smallbank_checkings_num_columns );
    cell_key_ranges source_checking_ckr = create_cell_key_ranges(
        k_smallbank_checkings_table_id, source_id, source_id,
        smallbank_checking::balance, k_smallbank_checkings_num_columns - 1 );
    partition_column_identifier dest_saving_pid =
        loader->generate_partition_column_identifier(
            k_smallbank_savings_table_id, dest_id, smallbank_saving::balance,
            k_smallbank_savings_num_columns );
    cell_key_ranges dest_saving_ckr = create_cell_key_ranges(
        k_smallbank_savings_table_id, dest_id, dest_id,
        smallbank_saving::balance, k_smallbank_savings_num_columns - 1 );
    partition_column_identifier dest_checking_pid =
        loader->generate_partition_column_identifier(
            k_smallbank_checkings_table_id, dest_id,
            smallbank_checking::balance, k_smallbank_checkings_num_columns );
    cell_key_ranges dest_checking_ckr = create_cell_key_ranges(
        k_smallbank_checkings_table_id, dest_id, dest_id,
        smallbank_checking::balance, k_smallbank_checkings_num_columns - 1 );

    sproc_result res;

    // balance
    std::vector<arg_code> balance_arg_codes = {INTEGER_CODE, BIGINT_CODE};
    std::vector<void*>    balance_savings_args = {(void*) &savings_id,
                                               (void*) &source_id};
    std::vector<void*> balance_checkings_args = {(void*) &checkings_id,
                                                 (void*) &source_id};

    function_identifier balance_func_id( k_smallbank_account_balance_sproc_name,
                                         balance_arg_codes );
    function_skeleton balance_function =
        sproc_table->lookup_function( balance_func_id );

    partition_lookup_operation      lookup_op = partition_lookup_operation::GET;
    partition_column_identifier_set write_pids;
    partition_column_identifier_set read_pids;
    partition_column_identifier_set inflight_pids;

    std::vector<cell_key_ranges> write_ckrs;
    std::vector<cell_key_ranges> read_ckrs;

    read_pids.emplace( source_saving_pid );
    read_ckrs.emplace_back( source_saving_ckr );
    read_pids.emplace( source_checking_pid );
    read_ckrs.emplace_back( source_checking_ckr );

    transaction_partition_holder* txn_holder =
        database.get_partitions_with_begin(
            cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );

    std::vector<void*>    balance_results;
    std::vector<arg_code> balance_results_arg_codes;
    res =
        balance_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                          balance_arg_codes, balance_savings_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    deserialize_result( res, balance_results, balance_results_arg_codes );
    EXPECT_EQ( 2, balance_results_arg_codes.size() );
    EXPECT_EQ( BOOL_CODE.code, balance_results_arg_codes.at( 0 ).code );
    EXPECT_EQ( INTEGER_CODE.code, balance_results_arg_codes.at( 1 ).code );
    bool decision = *( (bool*) balance_results.at( 0 ) );
    EXPECT_TRUE( decision );
    EXPECT_GE( *( (int*) balance_results.at( 1 ) ), 0 );
    balance_results_arg_codes.clear();
    balance_results.clear();

    res = balance_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                            balance_arg_codes, balance_checkings_args,
                            opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    deserialize_result( res, balance_results, balance_results_arg_codes );
    EXPECT_EQ( 2, balance_results_arg_codes.size() );
    EXPECT_EQ( BOOL_CODE.code, balance_results_arg_codes.at( 0 ).code );
    EXPECT_EQ( INTEGER_CODE.code, balance_results_arg_codes.at( 1 ).code );
    decision = *( (bool*) balance_results.at( 0 ) );
    EXPECT_TRUE( decision );
    EXPECT_GE( *( (int*) balance_results.at( 1 ) ), 0 );
    balance_results_arg_codes.clear();
    balance_results.clear();

    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    read_pids.clear();
    read_ckrs.clear();

    // send payment
    bool send_is_debit = false;
    bool recv_is_debit = true;

    std::vector<arg_code> send_charge_arg_codes = {INTEGER_CODE, BIGINT_CODE,
                                                   INTEGER_CODE, BOOL_CODE};
    std::vector<void*> send_payment_send_args = {
        (void*) &savings_id, (void*) &source_id, (void*) &amount,
        (void*) &send_is_debit};
    std::vector<void*> send_payment_recv_args = {
        (void*) &checkings_id, (void*) &dest_id, (void*) &amount,
        (void*) &recv_is_debit};
    function_identifier charge_func_id(
        k_smallbank_distributed_charge_account_sproc_name,
        send_charge_arg_codes );
    function_skeleton charge_function =
        sproc_table->lookup_function( charge_func_id );

    write_pids.emplace( source_saving_pid );
    write_ckrs.emplace_back( source_saving_ckr );
    read_pids.emplace( source_saving_pid );
    read_ckrs.emplace_back( source_saving_ckr);
    write_pids.emplace( dest_checking_pid );
    write_ckrs.emplace_back( dest_checking_ckr );
    read_pids.emplace( dest_checking_pid );
    read_ckrs.emplace_back( dest_checking_ckr );

    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );

    std::vector<void*>    charge_results;
    std::vector<arg_code> charge_results_arg_codes;
    res = charge_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                           send_charge_arg_codes, send_payment_send_args,
                           opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    deserialize_result( res, charge_results, charge_results_arg_codes );
    EXPECT_EQ( 1, charge_results_arg_codes.size() );
    EXPECT_EQ( BOOL_CODE.code, charge_results_arg_codes.at( 0 ).code );
    decision = *( (bool*) charge_results.at( 0 ) );
    EXPECT_TRUE( decision );
    charge_results_arg_codes.clear();
    charge_results.clear();

    res = charge_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                           send_charge_arg_codes, send_payment_recv_args,
                           opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    deserialize_result( res, charge_results, charge_results_arg_codes );
    EXPECT_EQ( 1, charge_results_arg_codes.size() );
    EXPECT_EQ( BOOL_CODE.code, charge_results_arg_codes.at( 0 ).code );
    decision = *( (bool*) charge_results.at( 0 ) );
    EXPECT_TRUE( decision );
    charge_results_arg_codes.clear();
    charge_results.clear();

    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    write_pids.clear();
    read_pids.clear();
    write_ckrs.clear();
    read_ckrs.clear();

    // deposit checking
    std::vector<arg_code> deposit_checking_arg_codes = {BIGINT_CODE,
                                                        INTEGER_CODE};
    std::vector<void*> deposit_checking_args = {(void*) &source_id,
                                                (void*) &amount};

    function_identifier deposit_checking_func_id(
        k_smallbank_deposit_checking_sproc_name, deposit_checking_arg_codes );
    function_skeleton deposit_checking_function =
        sproc_table->lookup_function( deposit_checking_func_id );

    write_pids.emplace( source_checking_pid );
    write_ckrs.emplace_back( source_checking_ckr );
    read_pids.emplace( source_checking_pid );
    read_ckrs.emplace_back( source_checking_ckr );

    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );
    res = deposit_checking_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                                     deposit_checking_arg_codes,
                                     deposit_checking_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    write_pids.clear();
    read_pids.clear();
    write_ckrs.clear();
    read_ckrs.clear();

    // transact savings
    std::vector<arg_code> transact_savings_arg_codes = {BIGINT_CODE,
                                                        INTEGER_CODE};
    std::vector<void*> transact_savings_args = {(void*) &source_id,
                                                (void*) &amount};

    function_identifier transact_savings_func_id(
        k_smallbank_transact_savings_sproc_name, transact_savings_arg_codes );
    function_skeleton transact_savings_function =
        sproc_table->lookup_function( transact_savings_func_id );

    write_pids.emplace( source_saving_pid );
    write_ckrs.emplace_back( source_saving_ckr );
    read_pids.emplace( source_saving_pid );
    read_ckrs.emplace_back( source_saving_ckr );

    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );
    res = transact_savings_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                                     transact_savings_arg_codes,
                                     transact_savings_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    write_pids.clear();
    read_pids.clear();
    write_ckrs.clear();
    read_ckrs.clear();

    // write check
    std::vector<arg_code> write_check_arg_codes = {BIGINT_CODE, INTEGER_CODE};
    std::vector<void*> write_check_args = {(void*) &source_id, (void*) &amount};

    function_identifier write_check_func_id( k_smallbank_write_check_sproc_name,
                                             write_check_arg_codes );
    function_skeleton write_check_function =
        sproc_table->lookup_function( write_check_func_id );

    write_pids.emplace( source_saving_pid );
    write_ckrs.emplace_back( source_saving_ckr );
    read_pids.emplace( source_saving_pid );
    read_ckrs.emplace_back( source_saving_ckr);

    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );
    res = write_check_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                                write_check_arg_codes, write_check_args,
                                opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    write_pids.clear();
    read_pids.clear();
    write_ckrs.clear();
    read_ckrs.clear();

    // amalgamate
    std::vector<void*> amalgamate_balance_args = {(void*) &savings_id,
                                                  (void*) &source_id};

    write_pids.emplace( source_saving_pid );
    write_ckrs.emplace_back( source_saving_ckr );
    read_pids.emplace( source_saving_pid );
    read_ckrs.emplace_back( source_saving_ckr );
    write_pids.emplace( dest_checking_pid );
    write_ckrs.emplace_back( dest_checking_ckr );
    read_pids.emplace( dest_checking_pid );
    read_ckrs.emplace_back( dest_checking_ckr );

    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );

    res = balance_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                            balance_arg_codes, amalgamate_balance_args,
                            opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    deserialize_result( res, balance_results, balance_results_arg_codes );
    EXPECT_EQ( 2, balance_results_arg_codes.size() );
    EXPECT_EQ( BOOL_CODE.code, balance_results_arg_codes.at( 0 ).code );
    EXPECT_EQ( INTEGER_CODE.code, balance_results_arg_codes.at( 1 ).code );
    decision = *( (bool*) balance_results.at( 0 ) );
    EXPECT_TRUE( decision );
    int savings_balance = *( (int*) balance_results.at( 1 ) );
    EXPECT_GE( savings_balance, 0 );
    balance_results_arg_codes.clear();
    balance_results.clear();

    std::vector<void*> send_amalgamate_send_args = {
        (void*) &savings_id, (void*) &source_id, (void*) &savings_balance,
        (void*) &send_is_debit};
    std::vector<void*> recv_amalgamate_recv_args = {
        (void*) &checkings_id, (void*) &dest_id, (void*) &savings_balance,
        (void*) &recv_is_debit};

    res = charge_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                           send_charge_arg_codes, send_amalgamate_send_args,
                           opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    deserialize_result( res, charge_results, charge_results_arg_codes );
    EXPECT_EQ( 1, charge_results_arg_codes.size() );
    EXPECT_EQ( BOOL_CODE.code, charge_results_arg_codes.at( 0 ).code );
    decision = *( (bool*) charge_results.at( 0 ) );
    EXPECT_TRUE( decision );
    charge_results_arg_codes.clear();
    charge_results.clear();

    res = charge_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                           send_charge_arg_codes, recv_amalgamate_recv_args,
                           opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    deserialize_result( res, charge_results, charge_results_arg_codes );
    EXPECT_EQ( 1, charge_results_arg_codes.size() );
    EXPECT_EQ( BOOL_CODE.code, charge_results_arg_codes.at( 0 ).code );
    decision = *( (bool*) charge_results.at( 0 ) );
    EXPECT_TRUE( decision );
    charge_results_arg_codes.clear();
    charge_results.clear();

    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    write_pids.clear();
    read_pids.clear();
    write_ckrs.clear();
    read_ckrs.clear();
}

TEST_F( smallbank_sproc_test, smallbank_args_sproc_execute_test_row ) {
    smallbank_args_sproc_execute_test( partition_type::type::ROW );
}
TEST_F( smallbank_sproc_test, smallbank_args_sproc_execute_test_column ) {
    smallbank_args_sproc_execute_test( partition_type::type::COLUMN );
}
TEST_F( smallbank_sproc_test,
        smallbank_args_sproc_execute_test_sorted_column ) {
    smallbank_args_sproc_execute_test( partition_type::type::SORTED_COLUMN );
}

