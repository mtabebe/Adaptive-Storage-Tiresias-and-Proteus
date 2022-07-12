#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/common/gdcheck.h"
#include "../src/data-site/site-manager/serialize.h"
#include "../src/data-site/stored-procedures/stored_procedures_executor.h"
#include "../src/data-site/stored-procedures/twitter/twitter_prep_stmts.h"

class twitter_sproc_test : public ::testing::Test {};

#define DO_TWITTER_SCAN_SPROC_ARGS( _scan_args, _expected_scan_size,         \
                                    _expected_res, _sproc_name, _database,   \
                                    _c1_state, _data_sizes, _scan_arg_codes, \
                                    _scan_vals, _opaque_ptr )                \
    do {                                                                     \
        DCHECK_EQ( _expected_scan_size, _scan_args.size() );                 \
        partition_column_identifier_set _write_pids;                         \
        partition_column_identifier_set _read_pids;                          \
        partition_column_identifier_set _inflight_pids;                      \
        for( const auto& _scan_arg : _scan_args ) {                          \
            std::vector<partition_column_identifier> _pids =                 \
                generate_partition_column_identifiers_from_ckrs(             \
                    _scan_arg.read_ckrs, _data_sizes );                      \
            _read_pids.insert( _pids.begin(), _pids.end() );                 \
        }                                                                    \
        function_identifier    _func_id( _sproc_name, _scan_arg_codes );     \
        scan_function_skeleton _scan_function =                              \
            sproc_table->lookup_scan_function( _func_id );                   \
        EXPECT_NE( _scan_function, nullptr );                                \
        auto _txn_holder = _database.get_partitions_with_begin(              \
            0, _c1_state, _write_pids, _read_pids, _inflight_pids,           \
            partition_lookup_operation::GET_ALLOW_MISSING );                 \
        EXPECT_NE( _txn_holder, nullptr );                                   \
        scan_result _scan_res;                                               \
        _scan_function( _txn_holder, 0, _scan_args, _scan_arg_codes,         \
                        _scan_vals, _opaque_ptr, _scan_res );                \
        EXPECT_EQ( _scan_res.status, exec_status_type::COMMAND_OK );         \
        EXPECT_EQ( _scan_res.res_tuples.size(), 1 );                         \
        EXPECT_EQ( _scan_res.res_tuples.count( _expected_res ), 1 );         \
        EXPECT_GE( _scan_res.res_tuples.at( _expected_res ).size(), 0 );     \
        _c1_state = _txn_holder->commit_transaction();                       \
        delete _txn_holder;                                                  \
        _txn_holder = nullptr;                                               \
        _read_pids.clear();                                                  \
        _write_pids.clear();                                                 \
        _inflight_pids.clear();                                              \
    } while( 0 )

#define DO_TWITTER_SCAN_SPROC( _scan_args, _expected_scan_size, _expected_res, \
                               _sproc_name, _database, _c1_state, _data_sizes, \
                               _opaque_ptr )                                   \
    do {                                                                       \
        std::vector<arg_code> _codes;                                          \
        std::vector<void*>    _vals = {};                                      \
        DO_TWITTER_SCAN_SPROC_ARGS(                                            \
            _scan_args, _expected_scan_size, _expected_res, _sproc_name,       \
            _database, _c1_state, _data_sizes, _codes, _vals, _opaque_ptr );   \
    } while( 0 )

snapshot_vector create_and_load_twitter_table(
    db* database, sproc_lookup_table* sproc_table,
    const twitter_configs& twitter_cfg, void* opaque_ptr ) {
    snapshot_vector c1_state;
    std::vector<arg_code> create_table_args = {BIGINT_CODE, BIGINT_CODE};
    std::vector<void*>    create_table_vals = {(void*) database,
                                            (void*) &twitter_cfg};
    function_identifier create_func_id( k_create_config_tables_sproc_name,
                                        create_table_args );

    sproc_result res;

    uint32_t cli_id = 0;

    function_skeleton create_function =
        sproc_table->lookup_function( create_func_id );

    std::vector<cell_key_ranges> write_ckrs;
    std::vector<cell_key_ranges> read_ckrs;

    res = create_function( nullptr, cli_id, write_ckrs, read_ckrs,
                           create_table_args, create_table_vals, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

    // everyone follows 4 people
    // everyone has 4 tweets
    uint32_t min_id = 0;
    uint32_t max_id = 4;
    std::vector<uint32_t> ids;
    for( uint32_t i = min_id; i <= max_id; i++ ) {
        ids.emplace_back( i );
    }
    partition_column_identifier_set   read_pids;
    partition_column_identifier_set   inflight_pids;


    auto data_sizes = get_twitter_data_sizes( twitter_cfg );

    partition_column_identifier user_pid = create_partition_column_identifier(
        k_twitter_user_profiles_table_id, 0, 0, 0,
        k_twitter_user_profile_num_columns - 1 );
    partition_column_identifier follows_pid =
        create_partition_column_identifier( k_twitter_follows_table_id, 0, 0, 0,
                                            k_twitter_follows_num_columns - 1 );
    partition_column_identifier follower_pid =
        create_partition_column_identifier(
            k_twitter_followers_table_id, 0, 0, 0,
            k_twitter_followers_num_columns - 1 );
    partition_column_identifier tweet_pid = create_partition_column_identifier(
        k_twitter_tweets_table_id, 0, 0, 0, k_twitter_tweets_num_columns - 1 );

    std::vector<arg_code> insert_entire_user_arg_codes = {
        INTEGER_CODE, INTEGER_CODE, INTEGER_ARRAY_CODE, INTEGER_ARRAY_CODE};
    function_identifier insert_entire_user_func_id(
        k_twitter_insert_entire_user_sproc_name, insert_entire_user_arg_codes );
    function_skeleton insert_entire_user_function =
        sproc_table->lookup_function( insert_entire_user_func_id );

    std::vector<arg_code> insert_user_arg_codes =
        k_twitter_insert_user_arg_codes;
    function_identifier insert_user_func_id( k_twitter_insert_user_sproc_name,
                                             insert_user_arg_codes );
    function_skeleton insert_user_function =
        sproc_table->lookup_function( insert_user_func_id );

    std::vector<arg_code> insert_user_tweets_arg_codes = {INTEGER_CODE};
    function_identifier insert_user_tweets_func_id(
        k_twitter_insert_user_tweets_sproc_name, insert_user_tweets_arg_codes );
    function_skeleton insert_user_tweets_function =
        sproc_table->lookup_function( insert_user_tweets_func_id );

    std::vector<arg_code> insert_user_follows_arg_codes = {
        INTEGER_CODE, INTEGER_ARRAY_CODE};
    insert_user_follows_arg_codes.at( 1 ).array_length = ids.size();
    function_identifier insert_user_follows_func_id(
        k_twitter_insert_user_follows_sproc_name,
        insert_user_follows_arg_codes );
    function_skeleton insert_user_follows_function =
        sproc_table->lookup_function( insert_user_follows_func_id );

    std::vector<arg_code> insert_user_followers_arg_codes = {
        INTEGER_CODE, INTEGER_ARRAY_CODE};
    insert_user_followers_arg_codes.at( 1 ).array_length = ids.size();
    function_identifier insert_user_followers_func_id(
        k_twitter_insert_user_followers_sproc_name,
        insert_user_followers_arg_codes );
    function_skeleton insert_user_followers_function =
        sproc_table->lookup_function( insert_user_followers_func_id );

    for( uint32_t u_id = 0; u_id < twitter_cfg.num_users_; u_id++ ) {
        write_ckrs.clear();

        partition_column_identifier_set write_pids;

        user_pid.partition_start = u_id;
        user_pid.partition_end = u_id;

        std::vector<partition_column_identifier> pids =
            generate_partition_column_identifiers(
                user_pid.table_id, user_pid.partition_start,
                user_pid.partition_end, user_pid.column_start,
                user_pid.column_end, data_sizes );
        for( const auto& pid : pids ) {
            write_pids.insert( pid );
        }

        follows_pid.partition_start = combine_keys( u_id, min_id );
        follows_pid.partition_end = combine_keys( u_id, max_id - 1 );
        pids = generate_partition_column_identifiers(
            follows_pid.table_id, follows_pid.partition_start,
            follows_pid.partition_end, follows_pid.column_start,
            follows_pid.column_end, data_sizes );
        for( const auto& pid : pids ) {
            write_pids.insert( pid );
        }

        follower_pid.partition_start = combine_keys( u_id, min_id );
        follower_pid.partition_end = combine_keys( u_id, max_id - 1 );
        pids = generate_partition_column_identifiers(
            follower_pid.table_id, follower_pid.partition_start,
            follower_pid.partition_end, follower_pid.column_start,
            follower_pid.column_end, data_sizes );
        for( const auto& pid : pids ) {
            write_pids.insert( pid );
        }

        tweet_pid.partition_start = combine_keys( u_id, min_id );
        tweet_pid.partition_end = combine_keys( u_id, ( max_id - 1 ) * 2 );
        pids = generate_partition_column_identifiers(
            tweet_pid.table_id, tweet_pid.partition_start,
            tweet_pid.partition_end, tweet_pid.column_start,
            tweet_pid.column_end, data_sizes );
        for( const auto& pid : pids ) {
            write_pids.insert( pid );
        }

        std::vector<uint32_t> following;
        std::vector<uint32_t> followers;
        int32_t               user = ( u_id );
        for( int32_t offset = (int32_t) min_id + 1; offset <= (int32_t) max_id;
             offset++ ) {
            following.emplace_back( ( user + offset ) %
                                    twitter_cfg.num_users_ );
            followers.emplace_back( ( user - offset ) %
                                    twitter_cfg.num_users_ );
        }

        partition_lookup_operation lookup_op =
            partition_lookup_operation::GET_OR_CREATE;

        transaction_partition_holder* txn_holder =
            database->get_partitions_with_begin( cli_id, c1_state, write_pids,
                                                 write_pids, inflight_pids,
                                                 lookup_op );

        if( user % 2 == 0 ) {
            DVLOG( 10 ) << "Load entire user";
            std::vector<void*> insert_entire_user_args = {
                (void*) &u_id, (void*) &max_id /*num tweets*/,
                (void*) following.data(), (void*) followers.data()};

            insert_entire_user_arg_codes.at( 2 ).array_length =
                following.size();
            insert_entire_user_arg_codes.at( 3 ).array_length =
                followers.size();

            write_ckrs.emplace_back( cell_key_ranges_from_pcid( user_pid ) );
            write_ckrs.emplace_back( cell_key_ranges_from_pcid( tweet_pid ) );
            write_ckrs.emplace_back( cell_key_ranges_from_pcid( follows_pid ) );
            write_ckrs.emplace_back(
                cell_key_ranges_from_pcid( follower_pid ) );

            res = insert_entire_user_function(
                txn_holder, cli_id, write_ckrs, read_ckrs,
                insert_entire_user_arg_codes, insert_entire_user_args,
                opaque_ptr );
            EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
        } else {
            DVLOG( 10 ) << "Load user in parts";

            std::vector<void*> insert_user_args = {
                (void*) &u_id, (void*) &max_id /*num_followers*/,
                (void*) &max_id /*num_following*/,
                (void*) &max_id /*num_tweets*/
            };

            std::vector<void*> insert_user_tweets_args = {(void*) &u_id};
            std::vector<void*> insert_user_followers_args = {
                (void*) &u_id, (void*) followers.data()};
            std::vector<void*> insert_user_follows_args = {
                (void*) &u_id, (void*) following.data()};

            write_ckrs.emplace_back( cell_key_ranges_from_pcid( user_pid ) );
            res = insert_user_function( txn_holder, cli_id, write_ckrs,
                                        read_ckrs, insert_user_arg_codes,
                                        insert_user_args, opaque_ptr );
            EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
            write_ckrs.clear();

            write_ckrs.emplace_back( cell_key_ranges_from_pcid( tweet_pid ) );
            res = insert_user_tweets_function(
                txn_holder, cli_id, write_ckrs, read_ckrs,
                insert_user_tweets_arg_codes, insert_user_tweets_args,
                opaque_ptr );
            EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
            write_ckrs.clear();

            write_ckrs.emplace_back( cell_key_ranges_from_pcid( follows_pid ) );
            res = insert_user_follows_function(
                txn_holder, cli_id, write_ckrs, read_ckrs,
                insert_user_follows_arg_codes, insert_user_follows_args,
                opaque_ptr );
            EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
            write_ckrs.clear();

            write_ckrs.emplace_back(
                cell_key_ranges_from_pcid( follower_pid ) );
            res = insert_user_followers_function(
                txn_holder, cli_id, write_ckrs, read_ckrs,
                insert_user_followers_arg_codes, insert_user_followers_args,
                opaque_ptr );
            EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
            write_ckrs.clear();
        }

        c1_state = txn_holder->commit_transaction();
        delete txn_holder;
    }

    return c1_state;
}
#if 0
snapshot_vector create_and_load_arg_based_twitter_table(
    db* database, sproc_lookup_table* sproc_table,
    const twitter_configs& twitter_cfg, void* opaque_ptr ) {
    snapshot_vector c1_state;
    twitter_sproc_helper_holder* holder =
        (twitter_sproc_helper_holder*) opaque_ptr;

    std::vector<arg_code> create_table_args = {BIGINT_CODE, BIGINT_CODE};
    std::vector<void*>    create_table_vals = {(void*) database,
                                            (void*) &twitter_cfg};
    function_identifier create_func_id( k_create_config_tables_sproc_name,
                                        create_table_args );

    sproc_result res;

    uint32_t cli_id = 0;

    function_skeleton create_function =
        sproc_table->lookup_function( create_func_id );
    res = create_function( nullptr, cli_id, create_table_args,
                           create_table_vals, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );

    twitter_loader_templ_no_commit* loader =
        holder->get_loader_and_set_holder( cli_id, nullptr );

    uint64_t a_start = 0;
    uint64_t a_end = twitter_cfg.num_accounts_ - 1;

    std::vector<std::string> sproc_names = {
        k_twitter_load_assorted_accounts_sproc_name,
        k_twitter_load_assorted_savings_sproc_name,
        k_twitter_load_assorted_checkings_sproc_name};
    std::vector<uint32_t> table_ids = {k_twitter_accounts_table_id,
                                       k_twitter_savings_table_id,
                                       k_twitter_checkings_table_id};
    EXPECT_EQ( sproc_names.size(), table_ids.size() );
    partition_identifier_set inflight_pids;

    for( uint32_t pos = 0; pos < table_ids.size(); pos++ ) {
        uint32_t                       table_id = table_ids.at( pos );
        std::string                    sproc_name = sproc_names.at( pos );

        DLOG( INFO ) << "Loading:" << table_id << ", sproc name:" << sproc_name;

        std::vector<record_identifier> write_rids =
            loader->generate_write_set( table_id, a_start, a_end );
        std::vector<uint64_t> rows;
        for( const auto rid : write_rids ) {
            rows.push_back( rid.key_ );
        }
        auto pids =
            loader->generate_partition_identifiers( table_id, a_start, a_end );
        partition_identifier_set insert_pids;
        insert_pids.insert( pids.begin(), pids.end() );


        std::vector<void*>  insert_args = {(void*) rows.data()};

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

        res = insert_function( txn_holder, cli_id, insert_codes, insert_args,
                               opaque_ptr );
        EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
        c1_state = txn_holder->commit_transaction();

        delete txn_holder;
    }

    return c1_state;
}
#endif

void twitter_sproc_execute_test( const partition_type::type& p_type ) {
    uint32_t site_loc = 1;
    uint32_t num_tables = 4;
    uint32_t num_clients = 10;
    uint32_t gc_sleep_time = 10;

    bool        enable_sec_storage = false;
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
        construct_twitter_sproc_lookup_table();

    benchmark_configs b_cfg = construct_benchmark_configs(
        1 /*num clients*/, gc_sleep_time, 1 /*  bench time*/,
        1000 /* num operations before checking*/ );
    twitter_configs twitter_cfg = construct_twitter_configs(
        500 /* num users*/, 2000 /*num tweets*/, 1.0 /*tweet skew */,
        1.75 /*follow skew*/, 100 /* account_partition_size */,
        50 /* max_follow_per_user */, 16 /*max_tweets per use*/,
        100 /* limit_tweets */, 10 /* limit_tweets_for_uid */,
        20 /* limit_followers */, b_cfg, 10 /* get_tweet_prob ori 10*/,
        8 /*get_tweets_from_following_prob ori 10 */,
        20 /* get_followers_prob ori 25 */,
        30 /* get_user_tweets_prob ori 40 */,
        12 /* get_insert_tweet_prob */,  // LEAVE IT
        7 /* get_recent_tweets_prob */, 8 /* get_tweets_from_followers_prob */,
        2 /* get_tweets_like_prob */, 3 /* update followers prob */
    );
    twitter_cfg.layouts_.user_profile_part_type_ = p_type;
    twitter_cfg.layouts_.followers_part_type_ = p_type;
    twitter_cfg.layouts_.follows_part_type_ = p_type;
    twitter_cfg.layouts_.tweets_part_type_ = p_type;

    void* opaque_ptr = construct_twitter_opaque_pointer( twitter_cfg );
    c1_state = create_and_load_twitter_table( &database, sproc_table.get(),
                                              twitter_cfg, opaque_ptr );

    twitter_sproc_helper_holder* holder =
        (twitter_sproc_helper_holder*) opaque_ptr;
    (void) holder;

    auto twitter_worker = holder->get_worker_and_set_holder( cli_id, nullptr );

    partition_lookup_operation lookup_op = partition_lookup_operation::GET;
    partition_column_identifier_set   write_pids;
    partition_column_identifier_set   read_pids;
    partition_column_identifier_set   inflight_pids;
    auto data_sizes = get_twitter_data_sizes( twitter_cfg );


    uint32_t min_id = 0;
    uint32_t max_id = 4;

    uint32_t user_id = 10;
    uint32_t tweet_id = 1;

    std::vector<scan_arguments> get_tweet_scan_args =
        twitter_worker->generate_get_tweet_scan_args( user_id, tweet_id );
    (void) user_id;
    (void) tweet_id;

    DO_TWITTER_SCAN_SPROC( get_tweet_scan_args, 1, 0,
                           k_twitter_get_tweet_sproc_name, database, c1_state,
                           data_sizes, opaque_ptr );

    std::vector<arg_code> fetch_and_set_arg_codes = {};
    function_identifier fetch_and_set_func_id(
        k_twitter_fetch_and_set_next_tweet_id_sproc_name,
        fetch_and_set_arg_codes );
    function_skeleton fetch_and_set_function =
        sproc_table->lookup_function( fetch_and_set_func_id );

    std::vector<arg_code> insert_tweet_arg_codes = {};
    function_identifier insert_tweet_func_id( k_twitter_insert_tweet_sproc_name,
                                              insert_tweet_arg_codes );
    function_skeleton insert_function =
        sproc_table->lookup_function( insert_tweet_func_id );

    std::vector<cell_key_ranges> write_ckrs = {create_cell_key_ranges(
        k_twitter_user_profiles_table_id, user_id, user_id,
        twitter_user_profile_cols::next_tweet_id,
        twitter_user_profile_cols::next_tweet_id )};
    std::vector<cell_key_ranges> read_ckrs = write_ckrs;

    auto write_pid_vec = generate_partition_column_identifiers_from_ckrs(
        write_ckrs, data_sizes );
    for( const auto& pid : write_pid_vec ) {
        write_pids.insert( pid );
        read_pids.insert( pid );
    }

    std::vector<void*> fetch_and_set_args = {};

    transaction_partition_holder* txn_holder =
        database.get_partitions_with_begin(
            cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );

    sproc_result res = fetch_and_set_function(
        txn_holder, cli_id, write_ckrs, read_ckrs, fetch_and_set_arg_codes,
        fetch_and_set_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    write_pids.clear();
    write_ckrs.clear();
    write_pid_vec.clear();
    read_pids.clear();
    read_ckrs.clear();

    (void) insert_function;
    std::vector<void*>    fetch_and_set_results;
    std::vector<arg_code> fetch_and_set_results_arg_codes;
    deserialize_result( res, fetch_and_set_results,
                        fetch_and_set_results_arg_codes );
    GASSERT_EQ( 1, fetch_and_set_results_arg_codes.size() );
    EXPECT_EQ( INTEGER_CODE.code,
               fetch_and_set_results_arg_codes.at( 0 ).code );
    EXPECT_EQ( 1, fetch_and_set_results_arg_codes.at( 0 ).array_length );
    uint32_t* next_tweet_ids = ( (uint32_t*) fetch_and_set_results.at( 0 ) );

    EXPECT_EQ( max_id, next_tweet_ids[0] );

    std::vector<void*> insert_args = {};

    write_ckrs.emplace_back( create_cell_key_ranges(
        k_twitter_tweets_table_id, combine_keys( user_id, max_id ),
        combine_keys( user_id, max_id ), 0,
        k_twitter_tweets_num_columns - 1 ) );

    write_pid_vec = generate_partition_column_identifiers_from_ckrs(
        write_ckrs, data_sizes );
    for( const auto& pid : write_pid_vec ) {
        write_pids.insert( pid );
    }


    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );

    res = insert_function( txn_holder, cli_id, write_ckrs, read_ckrs,
                           insert_tweet_arg_codes, insert_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    write_pids.clear();
    write_ckrs.clear();

    std::vector<scan_arguments> get_tweets_from_following_scan_args =
        twitter_worker->generate_get_tweets_from_following_scan_args(
            user_id, 0, max_id, 0, max_id, 0, max_id * 2 );

    DO_TWITTER_SCAN_SPROC( get_tweets_from_following_scan_args, 2, 0,
                           k_twitter_get_tweets_from_following_sproc_name,
                           database, c1_state, data_sizes, opaque_ptr );

    std::vector<scan_arguments> get_user_tweets_scan_args =
        twitter_worker->generate_get_user_tweets_scan_args( user_id, 0,
                                                            max_id * 2 );
    DO_TWITTER_SCAN_SPROC( get_user_tweets_scan_args, 1, 0,
                           k_twitter_get_tweet_sproc_name, database, c1_state,
                           data_sizes, opaque_ptr );

    std::vector<scan_arguments> get_followers_scan_args =
        twitter_worker->generate_get_followers_scan_args( user_id, 0, max_id, 0,
                                                          max_id );
    DO_TWITTER_SCAN_SPROC( get_followers_scan_args, 2, 0,
                           k_twitter_get_followers_sproc_name, database,
                           c1_state, data_sizes, opaque_ptr );

    uint64_t cur_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch() )
            .count();

    std::vector<scan_arguments> get_recent_tweets_scan_args =
        twitter_worker->generate_get_recent_tweet_scan_args(
            0, max_id, 0, max_id, cur_time - 1000, cur_time );
    DO_TWITTER_SCAN_SPROC( get_recent_tweets_scan_args, 1, 0,
                           k_twitter_get_tweet_sproc_name, database, c1_state,
                           data_sizes, opaque_ptr );

    std::string min_tweet_str = "B";
    std::string max_tweet_str = "C";
    std::vector<scan_arguments> get_tweets_like_scan_args =
        twitter_worker->generate_get_tweets_like_scan_args(
            0, max_id, 0, max_id, min_tweet_str, max_tweet_str );
    DO_TWITTER_SCAN_SPROC( get_tweets_like_scan_args, 1, 0,
                           k_twitter_get_tweet_sproc_name, database, c1_state,
                           data_sizes, opaque_ptr );

    std::vector<scan_arguments> get_tweets_from_followers_scan_args =
        twitter_worker->generate_get_tweets_from_followers_scan_args(
            user_id, 0, max_id, 0, max_id, 0, max_id * 2 );

    DO_TWITTER_SCAN_SPROC( get_tweets_from_followers_scan_args, 2, 0,
                           k_twitter_get_tweets_from_followers_sproc_name,
                           database, c1_state, data_sizes, opaque_ptr );

    // update followers
    std::vector<arg_code> get_follower_counts_arg_codes = {};
    std::vector<void*>    get_follower_counts_args = {};
    function_identifier   get_follower_counts_func_id(
        k_twitter_get_follower_counts_sproc_name,
        get_follower_counts_arg_codes );
    function_skeleton get_follower_counts_function =
        sproc_table->lookup_function( get_follower_counts_func_id );

    read_ckrs.emplace_back( create_cell_key_ranges(
        k_twitter_user_profiles_table_id, user_id, user_id,
        twitter_user_profile_cols::num_followers,
        twitter_user_profile_cols::num_followers ) );
    read_ckrs.emplace_back( create_cell_key_ranges(
        k_twitter_user_profiles_table_id, max_id, max_id,
        twitter_user_profile_cols::num_following,
        twitter_user_profile_cols::num_following ) );

    auto read_pid_vec = generate_partition_column_identifiers_from_ckrs(
        read_ckrs, data_sizes );
    for( const auto& pid : read_pid_vec ) {
        read_pids.insert( pid );
    }
    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );

    res = get_follower_counts_function(
        txn_holder, cli_id, write_ckrs, read_ckrs,
        get_follower_counts_arg_codes, get_follower_counts_args, opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    write_pids.clear();
    write_ckrs.clear();
    write_pid_vec.clear();
    read_pids.clear();
    read_ckrs.clear();


    std::vector<void*>    get_follower_counts_results;
    std::vector<arg_code> get_follower_counts_results_arg_codes;
    deserialize_result( res, get_follower_counts_results,
                        get_follower_counts_results_arg_codes );
    GASSERT_EQ( 2, get_follower_counts_results_arg_codes.size() );
    EXPECT_EQ( INTEGER_ARRAY_CODE.code,
               get_follower_counts_results_arg_codes.at( 0 ).code );
    EXPECT_EQ( INTEGER_ARRAY_CODE.code,
               get_follower_counts_results_arg_codes.at( 1 ).code );
    EXPECT_EQ( 1, get_follower_counts_results_arg_codes.at( 0 ).array_length );
    EXPECT_EQ( 1, get_follower_counts_results_arg_codes.at( 1 ).array_length );
    int32_t* num_followers_arr =
        ( (int32_t*) get_follower_counts_results.at( 0 ) );
    int32_t* num_follows_arr =
        ( (int32_t*) get_follower_counts_results.at( 1 ) );
    int32_t u_num_followers = num_followers_arr[0];
    int32_t u_num_follows = num_follows_arr[0];

    EXPECT_GE( u_num_followers, 1 );
    EXPECT_GE( u_num_follows, 1 );

    u_num_followers -= 1;
    u_num_follows -= 1;

    std::vector<void*> update_follower_and_follows_args = {
        &user_id, &max_id, &u_num_followers, &u_num_follows };
    std::vector<arg_code> update_follower_and_follows_arg_codes = {
        INTEGER_CODE, INTEGER_CODE, INTEGER_CODE, INTEGER_CODE };
    function_identifier update_follower_and_follows_func_id(
        k_twitter_update_follower_and_follows_sproc_name,
        update_follower_and_follows_arg_codes );
    function_skeleton update_follower_and_follows_function =
        sproc_table->lookup_function( update_follower_and_follows_func_id );

    write_ckrs.emplace_back( create_cell_key_ranges(
        k_twitter_followers_table_id,
        combine_keys( user_id, u_num_followers ),
        combine_keys( user_id, u_num_followers ),
        twitter_followers_cols::follower_u_id,
        twitter_followers_cols::follower_u_id ) );
    write_ckrs.emplace_back( create_cell_key_ranges(
        k_twitter_follows_table_id, combine_keys( max_id, u_num_follows ),
        combine_keys( max_id, u_num_follows ),
        twitter_follows_cols::following_u_id,
        twitter_follows_cols::following_u_id ) );

    write_pid_vec = generate_partition_column_identifiers_from_ckrs(
        write_ckrs, data_sizes );
    for( const auto& pid : write_pid_vec ) {
        write_pids.insert( pid );
    }
    txn_holder = database.get_partitions_with_begin(
        cli_id, c1_state, write_pids, read_pids, inflight_pids, lookup_op );

    res = update_follower_and_follows_function(
        txn_holder, cli_id, write_ckrs, read_ckrs,
        update_follower_and_follows_arg_codes, update_follower_and_follows_args,
        opaque_ptr );
    EXPECT_EQ( res.status, exec_status_type::COMMAND_OK );
    c1_state = txn_holder->commit_transaction();
    delete txn_holder;
    write_pids.clear();
    write_ckrs.clear();
    write_pid_vec.clear();
    read_pids.clear();
    read_ckrs.clear();


    (void) min_id;
    (void) max_id;
    (void) cli_id;
    (void) lookup_op;
}

TEST_F( twitter_sproc_test, twitter_sproc_execute_test_row ) {
    twitter_sproc_execute_test( partition_type::type::ROW );
}
TEST_F( twitter_sproc_test, twitter_sproc_execute_test_column ) {
    twitter_sproc_execute_test( partition_type::type::COLUMN );
}
TEST_F( twitter_sproc_test, twitter_sproc_execute_test_sorted_column ) {
    twitter_sproc_execute_test( partition_type::type::SORTED_COLUMN );
}
TEST_F( twitter_sproc_test, twitter_sproc_execute_test_multi_column ) {
    twitter_sproc_execute_test( partition_type::type::MULTI_COLUMN );
}
TEST_F( twitter_sproc_test, twitter_sproc_execute_test_sorted_multi_column ) {
    twitter_sproc_execute_test( partition_type::type::SORTED_MULTI_COLUMN );
}


