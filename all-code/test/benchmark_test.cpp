#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/benchmark/smallbank/smallbank_benchmark.h"
#include "../src/benchmark/tpcc/tpcc_benchmark.h"
#include "../src/benchmark/twitter/twitter_benchmark.h"
#include "../src/benchmark/ycsb/ycsb_benchmark.h"

class benchmark_test : public ::testing::Test {};

TEST_F( benchmark_test, workload_operation_selector_test ) {

    workload_operation_selector op_selector;
    op_selector.init( {YCSB_WRITE, YCSB_SCAN, YCSB_MULTI_RMW}, {25, 50, 25} );

    std::unordered_map<workload_operation_enum, std::vector<int32_t>>
        expected_ranges = {
            {YCSB_WRITE, {0, 24}},
            {YCSB_SCAN, {25, 74}},
            {YCSB_MULTI_RMW, {75, 99}},
        };

    for( const auto& expected_iter : expected_ranges ) {
        workload_operation_enum op = expected_iter.first;
        int32_t                 start = expected_iter.second.at( 0 );
        int32_t                 end = expected_iter.second.at( 1 );
        for( int32_t i = start; i <= end; i++ ) {
            EXPECT_EQ( op, op_selector.get_operation( i ) );
        }
    }
}

TEST_F( benchmark_test, benchmark_statistics_test ) {
    benchmark_statistics s1;
    benchmark_statistics s2;
    benchmark_statistics s3;
    benchmark_statistics sum;

    std::vector<workload_operation_enum> ops = {YCSB_WRITE, YCSB_SCAN,
                                                YCSB_MULTI_RMW};
    s1.init( ops );
    s2.init( ops );
    s3.init( ops );
    sum.init( ops );

    s1.add_outcome( YCSB_WRITE, WORKLOAD_OP_SUCCESS, 30.0 );
    s1.add_outcome( YCSB_WRITE, WORKLOAD_OP_SUCCESS, 25.0, 4 );
    s1.add_outcome( YCSB_WRITE, WORKLOAD_OP_FAILURE, 72.0, 4 );
    s1.store_running_time( 57 );

    EXPECT_EQ( 5, s1.get_outcome( YCSB_WRITE, WORKLOAD_OP_SUCCESS ) );
    EXPECT_EQ( 4, s1.get_outcome( YCSB_WRITE, WORKLOAD_OP_FAILURE ) );
    EXPECT_DOUBLE_EQ( 55, s1.get_latency( YCSB_WRITE, WORKLOAD_OP_SUCCESS ) );
    EXPECT_DOUBLE_EQ( 57, s1.get_running_time() );

    s2.add_outcome( YCSB_WRITE, WORKLOAD_OP_SUCCESS, 32.0, 6 );
    s2.add_outcome( YCSB_SCAN, WORKLOAD_OP_SUCCESS, 72.0, 3 );
    s2.store_running_time( 40 );

    s3.add_outcome( YCSB_WRITE, WORKLOAD_OP_SUCCESS, 19.0, 7 );
    s3.add_outcome( YCSB_SCAN, WORKLOAD_OP_SUCCESS, 21.0, 3 );
    s3.add_outcome( YCSB_SCAN, WORKLOAD_OP_FAILURE, 23.0, 4 );
    s3.store_running_time( 61 );

    sum.merge( s1 );
    sum.merge( s2 );
    sum.merge( s3 );

    EXPECT_EQ( 18, sum.get_outcome( YCSB_WRITE, WORKLOAD_OP_SUCCESS ) );
    EXPECT_DOUBLE_EQ( 106, sum.get_latency( YCSB_WRITE, WORKLOAD_OP_SUCCESS ) );
    EXPECT_EQ( 4, sum.get_outcome( YCSB_WRITE, WORKLOAD_OP_FAILURE ) );
    EXPECT_EQ( 6, sum.get_outcome( YCSB_SCAN, WORKLOAD_OP_SUCCESS ) );
    EXPECT_EQ( 4, sum.get_outcome( YCSB_SCAN, WORKLOAD_OP_FAILURE ) );
    EXPECT_EQ( 0, sum.get_outcome( YCSB_MULTI_RMW, WORKLOAD_OP_SUCCESS ) );
    EXPECT_EQ( 0, sum.get_outcome( YCSB_MULTI_RMW, WORKLOAD_OP_FAILURE ) );

    EXPECT_DOUBLE_EQ( 61, sum.get_running_time() );
}

void ycsb_benchmark_test( const db_abstraction_type&  db_type,
                          const partition_type::type& p_type ) {

    benchmark_configs b_cfg = construct_benchmark_configs(
        1 /*num clients*/, 10 /* gc sleep time*/, 1 /*  bench time*/,
        500 /* num operations before checking*/, false /*limit prop updates*/,
        false /* enable sec storage */, "/tmp/" /*sec storage dir*/ );
    ycsb_configs ycsb_cfg = construct_ycsb_configs(
        500 /*num keys*/, 10 /*value size*/, 5 /*partition size*/,
        7 /*num opers per txn*/, 1.0 /*zipf*/, false /*limit update prop*/,
        0.1 /*scan selectivity*/, 0 /* update prop limit*/,
        2 /* col partition size*/, p_type, storage_tier_type::type::MEMORY,
        true /* allow scan conflicts */, true /* allow rmw conflicts */,
        true /*store scan results */, b_cfg, 18 /*write*/, 18 /*read*/,
        18 /*rmw*/, 18 /*scan*/, 18 /*multi_rmw*/, 3 /*split*/, 3 /*merge*/,
        4 /*remaster*/ );

    auto db_abstraction_configs = construct_db_abstraction_configs( db_type );


    ycsb_benchmark bench( ycsb_cfg, db_abstraction_configs );

    bench.init();

    bench.create_database();
    bench.load_database();
    bench.run_workload();
    benchmark_statistics stats = bench.get_statistics();

    for( const workload_operation_enum& op : k_ycsb_workload_operations ) {
        // we should do everythign at least once
        EXPECT_GT( stats.get_outcome( op, WORKLOAD_OP_SUCCESS ), 0 );
        if( stats.get_outcome( op, WORKLOAD_OP_SUCCESS ) == 0 ) {
            DLOG( INFO ) << "Get outcome:" << op;
        }
        // we should never fail
        EXPECT_EQ( 0, stats.get_outcome( op, WORKLOAD_OP_EXPECTED_ABORT ) );
        EXPECT_EQ( 0, stats.get_outcome( op, WORKLOAD_OP_FAILURE ) );
    }
}

TEST_F( benchmark_test, ycsb_plain_db_test) {
    ycsb_benchmark_test( db_abstraction_type::PLAIN_DB,
                         partition_type::type::ROW );
    ycsb_benchmark_test( db_abstraction_type::PLAIN_DB,
                         partition_type::type::COLUMN );
    ycsb_benchmark_test( db_abstraction_type::PLAIN_DB,
                         partition_type::type::SORTED_COLUMN );
    ycsb_benchmark_test( db_abstraction_type::PLAIN_DB,
                         partition_type::type::MULTI_COLUMN );
    ycsb_benchmark_test( db_abstraction_type::PLAIN_DB,
                         partition_type::type::SORTED_MULTI_COLUMN );

}
TEST_F( benchmark_test, ycsb_single_site_db_test) {
    ycsb_benchmark_test( db_abstraction_type::SINGLE_SITE_DB,
                         partition_type::type::ROW );
    ycsb_benchmark_test( db_abstraction_type::SINGLE_SITE_DB,
                         partition_type::type::COLUMN );
    ycsb_benchmark_test( db_abstraction_type::SINGLE_SITE_DB,
                         partition_type::type::SORTED_COLUMN );
    ycsb_benchmark_test( db_abstraction_type::SINGLE_SITE_DB,
                         partition_type::type::MULTI_COLUMN );
    ycsb_benchmark_test( db_abstraction_type::SINGLE_SITE_DB,
                         partition_type::type::SORTED_MULTI_COLUMN );

}
TEST_F( benchmark_test, ycsb_ss_db_test) {
    ycsb_benchmark_test( db_abstraction_type::SS_DB,
                         partition_type::type::ROW );
    ycsb_benchmark_test( db_abstraction_type::SS_DB,
                         partition_type::type::COLUMN );
    ycsb_benchmark_test( db_abstraction_type::SS_DB,
                         partition_type::type::SORTED_COLUMN );
    ycsb_benchmark_test( db_abstraction_type::SS_DB,
                         partition_type::type::MULTI_COLUMN );
    ycsb_benchmark_test( db_abstraction_type::SS_DB,
                         partition_type::type::SORTED_MULTI_COLUMN );

}

void tpcc_benchmark_test_generic( const db_abstraction_type&  db_type,
                                  const partition_type::type& p_type,
                                  bool use_full_scans, int32_t bench_time,
                                  int32_t c_clients, int32_t h_clients ) {
    bool     limit_number_of_records_propagated = false;
    uint64_t number_of_updates_needed_for_propagation = 1000;

    benchmark_configs b_cfg = construct_benchmark_configs(
        c_clients + h_clients /*num clients*/, 10 /* gc sleep time*/,
        bench_time /*  bench time*/, 100 /* num operations before checking*/,
        limit_number_of_records_propagated );

    tpcc_configs tpcc_cfg = construct_tpcc_configs(
        2 /*num warehouses*/, 1000 /*num items*/, 1000 /* num suppliers */,
        10 /* expected num orders per cust*/, 1000 /*item partition size*/,
        10 /*partition size*/, 2 /*district partiion size */,
        10 /* customer partition size*/, 100 /* suppliers partition size */,
        1 /*new order distributed likelihood*/,
        15 /*payment distributed likelihood*/,
        false /*track_and_use_recent_items*/, c_clients /* tpcc num clients */,
        h_clients /* tpch num clients */, b_cfg, 4 /*delivery prob*/,
        45 /* new order*/, 4 /*order status*/, 43 /* payment*/,
        4 /*stock level*/, 0 /* q1 prob */, 0 /* q2 prob */, 0 /* q3 prob */,
        0 /* q4 prob */, 0 /* q5 prob */, 0 /* q6 prob */, 0 /* q7 prob */,
        0 /* q8 prob */, 0 /* q9 prob */, 0 /* q10 prob */, 0 /* q11 prob */,
        0 /* q12 prob */, 0 /* q13 prob */, 0 /* q14 prob */, 0 /* q15 prob */,
        0 /* q16 prob */, 0 /* q17 prob */, 0 /* q18 prob */, 0 /* q19 prob */,
        0 /* q20 prob */, 0 /* q21 prob */, 0 /* q22 prob */,
        100 /* h_all prob */, 2 /*  dist per whouse */,
        300 /*cust per warehouse*/, 6 /*order lines*/,
        90 /*initial num customers per district*/,
        limit_number_of_records_propagated /*limit number of records propagated*/,
        number_of_updates_needed_for_propagation /*number of updates needed for propagtion*/,
        true /*use warehouse*/, true /* use district*/,
        use_full_scans /* h scans full tables */ );

    tpcc_cfg.layouts_.warehouse_part_type_ = p_type;
    tpcc_cfg.layouts_.item_part_type_ = p_type;
    tpcc_cfg.layouts_.stock_part_type_ = p_type;
    tpcc_cfg.layouts_.district_part_type_ = p_type;
    tpcc_cfg.layouts_.customer_part_type_ = p_type;
    tpcc_cfg.layouts_.history_part_type_ = p_type;
    tpcc_cfg.layouts_.order_line_part_type_ = p_type;
    tpcc_cfg.layouts_.new_order_part_type_ = p_type;
    tpcc_cfg.layouts_.order_part_type_ = p_type;
    tpcc_cfg.layouts_.customer_district_part_type_ = p_type;
    tpcc_cfg.layouts_.region_part_type_ = p_type;
    tpcc_cfg.layouts_.nation_part_type_ = p_type;
    tpcc_cfg.layouts_.supplier_part_type_ = p_type;


    auto db_abstraction_configs = construct_db_abstraction_configs( db_type );

    tpcc_benchmark bench( tpcc_cfg, db_abstraction_configs );

    bench.init();

    DLOG( WARNING ) << "TPCC:" << p_type;
    DLOG( WARNING ) << "Create db";
    bench.create_database();
    DLOG( WARNING ) << "Load db";
    bench.load_database();
    DLOG( WARNING ) << "Run workload";
    bench.run_workload();
    benchmark_statistics stats = bench.get_statistics();

    stats.log_statistics();

    for( const workload_operation_enum& op : k_tpcc_workload_operations ) {
        // we should do everythign at least once
        EXPECT_GT( stats.get_outcome( op, WORKLOAD_OP_SUCCESS ), 0 );
        // we should never fail
        EXPECT_EQ( 0, stats.get_outcome( op, WORKLOAD_OP_FAILURE ) );
    }

    EXPECT_GE( stats.get_outcome( TPCC_NEW_ORDER, WORKLOAD_OP_EXPECTED_ABORT ),
               0 );
}

void tpcc_benchmark_test( const db_abstraction_type&  db_type,
                          const partition_type::type& p_type ) {
    tpcc_benchmark_test_generic( db_type, p_type, false /* use full scans */,
                                 1 /* bench time */, 1 /*c clients */,
                                 2 /* h clients */ );
}

void tpcc_benchmark_test_config( const db_abstraction_type&  db_type,
                                 const partition_type::type& p_type ) {

    std::vector<bool>    use_full_scans = {true, false};
    std::vector<int32_t> bench_times = {1, 60};
    std::vector<std::tuple<int32_t, int32_t>> clients = {
        std::make_tuple<>( 1, 1 ), std::make_tuple<>( 1, 11 ),
        std::make_tuple<>( 6, 6 ), std::make_tuple<>( 11, 1 ),
    };

    for( const auto& use_scans : use_full_scans ) {
        for( const auto& bench_time : bench_times ) {
            for( const auto& client : clients ) {
                int32_t c_client = std::get<0>( client );
                int32_t h_client = std::get<0>( client );
                LOG( INFO )
                    << "P type:" << p_type << ", use_full_scan:" << use_scans
                    << ", bench_time:" << bench_time
                    << ", c_client:" << c_client << ", h_client:" << h_client;
                tpcc_benchmark_test_generic( db_type, p_type, use_scans,
                                             bench_time, c_client, h_client );
            }
        }
    }
}

#if 0
TEST_F( benchmark_test, plain_db_tpcc_config_test ) {
    tpcc_benchmark_test_config( db_abstraction_type::PLAIN_DB,
                                partition_type::type::ROW );
    tpcc_benchmark_test_config( db_abstraction_type::PLAIN_DB,
                                partition_type::type::COLUMN );
    tpcc_benchmark_test_config( db_abstraction_type::PLAIN_DB,
                                partition_type::type::SORTED_COLUMN );
    tpcc_benchmark_test_config( db_abstraction_type::PLAIN_DB,
                                partition_type::type::MULTI_COLUMN );
    tpcc_benchmark_test_config( db_abstraction_type::PLAIN_DB,
                                partition_type::type::SORTED_MULTI_COLUMN );
}
#endif

TEST_F( benchmark_test, tpcc_plain_db_test) {
    tpcc_benchmark_test( db_abstraction_type::PLAIN_DB,
                         partition_type::type::ROW );
    tpcc_benchmark_test( db_abstraction_type::PLAIN_DB,
                         partition_type::type::COLUMN );
    tpcc_benchmark_test( db_abstraction_type::PLAIN_DB,
                         partition_type::type::SORTED_COLUMN );
    tpcc_benchmark_test( db_abstraction_type::PLAIN_DB,
                         partition_type::type::MULTI_COLUMN );
    tpcc_benchmark_test( db_abstraction_type::PLAIN_DB,
                         partition_type::type::SORTED_MULTI_COLUMN );
}
TEST_F( benchmark_test, tpcc_single_site_db_test) {
    tpcc_benchmark_test( db_abstraction_type::SINGLE_SITE_DB,
                         partition_type::type::ROW );
    tpcc_benchmark_test( db_abstraction_type::SINGLE_SITE_DB,
                         partition_type::type::COLUMN );
    tpcc_benchmark_test( db_abstraction_type::SINGLE_SITE_DB,
                         partition_type::type::SORTED_COLUMN );
    tpcc_benchmark_test( db_abstraction_type::SINGLE_SITE_DB,
                         partition_type::type::MULTI_COLUMN );
    tpcc_benchmark_test( db_abstraction_type::SINGLE_SITE_DB,
                         partition_type::type::SORTED_MULTI_COLUMN );
}
TEST_F( benchmark_test, tpcc_ss_db_test) {
    tpcc_benchmark_test( db_abstraction_type::SS_DB,
                         partition_type::type::ROW );
    tpcc_benchmark_test( db_abstraction_type::SS_DB,
                         partition_type::type::COLUMN );
    tpcc_benchmark_test( db_abstraction_type::SS_DB,
                         partition_type::type::SORTED_COLUMN );
    tpcc_benchmark_test( db_abstraction_type::SS_DB,
                         partition_type::type::MULTI_COLUMN );
    tpcc_benchmark_test( db_abstraction_type::SS_DB,
                         partition_type::type::SORTED_MULTI_COLUMN );
}

void smallbank_benchmark_test( const db_abstraction_type&  db_type,
                               const partition_type::type& account_col_type,
                               const partition_type::type& banking_col_type ) {

    benchmark_configs b_cfg = construct_benchmark_configs(
        1 /*num clients*/, 10 /* gc sleep time*/, 1 /*  bench time*/,
        500 /* num operations before checking*/,
        false /*limit propagate updates*/, false /* enable sec storage */,
        "/tmp/" /*sec storage dir*/ );

    smallbank_configs smallbank_cfg = construct_smallbank_configs(
        500 /*num acounts*/, false /*hotspot used fixed size*/,
        25.0 /* hotspot percentage*/, 100 /* hotspot fixed size*/,
        10 /*partition size*/, 10 /*account spread*/, 1 /* account col size*/,
        account_col_type, storage_tier_type::type::MEMORY,
        1 /* banking col type*/, banking_col_type,
        storage_tier_type::type::MEMORY, b_cfg, 15 /*amalgamate */,
        15 /*balance*/, 15 /*deposit checking*/, 25 /*send payment*/,
        15 /* transact savings*/, 15 /* write check */ );

    auto db_abstraction_configs = construct_db_abstraction_configs( db_type );

    smallbank_benchmark bench( smallbank_cfg, db_abstraction_configs );

    bench.init();

    bench.create_database();
    bench.load_database();
    bench.run_workload();
    benchmark_statistics stats = bench.get_statistics();

     for( const workload_operation_enum& op : k_smallbank_workload_operations ) {
        // we should do everythign at least once
        EXPECT_GT( stats.get_outcome( op, WORKLOAD_OP_SUCCESS ), 0 );
        // we should never fail
        EXPECT_EQ( 0, stats.get_outcome( op, WORKLOAD_OP_FAILURE ) );
     }
}

TEST_F( benchmark_test, smallbank_plain_db_test ) {
    smallbank_benchmark_test( db_abstraction_type::PLAIN_DB,
                              partition_type::type::ROW, partition_type::ROW );
    smallbank_benchmark_test( db_abstraction_type::PLAIN_DB,
                              partition_type::type::COLUMN,
                              partition_type::COLUMN );
    smallbank_benchmark_test( db_abstraction_type::PLAIN_DB,
                              partition_type::type::SORTED_COLUMN,
                              partition_type::SORTED_COLUMN );
    smallbank_benchmark_test( db_abstraction_type::PLAIN_DB,
                              partition_type::type::MULTI_COLUMN,
                              partition_type::MULTI_COLUMN );
    smallbank_benchmark_test( db_abstraction_type::PLAIN_DB,
                              partition_type::type::SORTED_MULTI_COLUMN,
                              partition_type::SORTED_MULTI_COLUMN );

}

TEST_F( benchmark_test, smallbank_single_site_db_test ) {
    smallbank_benchmark_test( db_abstraction_type::SINGLE_SITE_DB,
                              partition_type::type::ROW, partition_type::ROW );
    smallbank_benchmark_test( db_abstraction_type::SINGLE_SITE_DB,
                              partition_type::type::COLUMN,
                              partition_type::COLUMN );
    smallbank_benchmark_test( db_abstraction_type::SINGLE_SITE_DB,
                              partition_type::type::SORTED_COLUMN,
                              partition_type::SORTED_COLUMN );
    smallbank_benchmark_test( db_abstraction_type::SINGLE_SITE_DB,
                              partition_type::type::MULTI_COLUMN,
                              partition_type::MULTI_COLUMN );
    smallbank_benchmark_test( db_abstraction_type::SINGLE_SITE_DB,
                              partition_type::type::SORTED_MULTI_COLUMN,
                              partition_type::SORTED_MULTI_COLUMN );
}
TEST_F( benchmark_test, smallbank_ss_db_test ) {
    smallbank_benchmark_test( db_abstraction_type::SS_DB,
                              partition_type::type::ROW, partition_type::ROW );
    smallbank_benchmark_test( db_abstraction_type::SS_DB,
                              partition_type::type::COLUMN,
                              partition_type::COLUMN );
    smallbank_benchmark_test( db_abstraction_type::SS_DB,
                              partition_type::type::SORTED_COLUMN,
                              partition_type::SORTED_COLUMN );
    smallbank_benchmark_test( db_abstraction_type::SS_DB,
                              partition_type::type::MULTI_COLUMN,
                              partition_type::MULTI_COLUMN );
    smallbank_benchmark_test( db_abstraction_type::SS_DB,
                              partition_type::type::SORTED_MULTI_COLUMN,
                              partition_type::SORTED_MULTI_COLUMN );
}

void twitter_benchmark_test( const db_abstraction_type&  db_type,
                             const partition_type::type& part_type ) {

    benchmark_configs b_cfg = construct_benchmark_configs(
        1 /*num clients*/, 10 /* gc sleep time*/, 1 /*  bench time*/,
        1000 /* num operations before checking*/,
        false /*limit propagate updates*/ );

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
        2 /* get_tweets_like_prob */, 3 /* update followers prob */ );

    twitter_cfg.layouts_.user_profile_part_type_ = part_type;
    twitter_cfg.layouts_.followers_part_type_ = part_type;
    twitter_cfg.layouts_.follows_part_type_ = part_type;
    twitter_cfg.layouts_.tweets_part_type_ = part_type;

    auto db_abstraction_configs = construct_db_abstraction_configs( db_type );

    twitter_benchmark bench( twitter_cfg, db_abstraction_configs );

    bench.init();

    bench.create_database();
    bench.load_database();
    bench.run_workload();
    benchmark_statistics stats = bench.get_statistics();

     for( const workload_operation_enum& op : k_twitter_workload_operations ) {
        // we should do everythign at least once
        EXPECT_GT( stats.get_outcome( op, WORKLOAD_OP_SUCCESS ), 0 );
        // we should never fail
        EXPECT_EQ( 0, stats.get_outcome( op, WORKLOAD_OP_FAILURE ) );
    }
}

TEST_F( benchmark_test, twitter_plain_db_test ) {
    twitter_benchmark_test( db_abstraction_type::PLAIN_DB,
                            partition_type::type::ROW );
    twitter_benchmark_test( db_abstraction_type::PLAIN_DB,
                            partition_type::type::COLUMN );
    twitter_benchmark_test( db_abstraction_type::PLAIN_DB,
                            partition_type::type::SORTED_COLUMN );
    twitter_benchmark_test( db_abstraction_type::PLAIN_DB,
                            partition_type::type::MULTI_COLUMN );
    twitter_benchmark_test( db_abstraction_type::PLAIN_DB,
                            partition_type::type::SORTED_MULTI_COLUMN );
}
TEST_F( benchmark_test, twitter_single_site_db_test) {
    twitter_benchmark_test( db_abstraction_type::SINGLE_SITE_DB,
                            partition_type::type::ROW );
}
TEST_F( benchmark_test, twitter_ss_db_test) {
    twitter_benchmark_test( db_abstraction_type::SS_DB,
                            partition_type::type::ROW );
}

TEST_F( benchmark_test, twitter_key_test ) {
    uint32_t u_1 = 1;
    uint32_t u_5 = 5;
    uint32_t t_7 = 7;
    uint32_t t_13 = 13;
    uint32_t following_id = 17;
    uint32_t follower_id = 23;

    twitter_follows follows;
    follows.u_id = u_5;
    follows.f_id = following_id;
    follows.following_u_id = u_1;

    twitter_followers followers;
    followers.u_id = u_1;
    followers.f_id = follower_id;
    followers.follower_u_id = u_5;

    twitter_tweets tweet_7;
    tweet_7.u_id = u_1;
    tweet_7.tweet_id = t_7;

    twitter_tweets tweet_13;
    tweet_13.u_id = u_5;
    tweet_13.tweet_id = t_13;

    uint64_t followers_key = make_followers_key( followers );
    uint64_t follows_key = make_follows_key( follows );
    uint64_t tweet_7_key = make_tweets_key( tweet_7 );
    uint64_t tweet_13_key = make_tweets_key( tweet_13 );

    EXPECT_EQ( get_user_from_key( k_twitter_followers_table_id, followers_key ),
               u_1 );
    EXPECT_EQ( get_user_from_key( k_twitter_follows_table_id, follows_key ),
               u_5 );
    EXPECT_EQ( get_user_from_key( k_twitter_tweets_table_id, tweet_7_key ),
               u_1 );
    EXPECT_EQ( get_user_from_key( k_twitter_tweets_table_id, tweet_13_key ),
               u_5 );

    EXPECT_EQ(
        get_follows_id_from_key( k_twitter_followers_table_id, followers_key ),
        follower_id );
    EXPECT_EQ(
        get_follows_id_from_key( k_twitter_follows_table_id, follows_key ),
        following_id );

    EXPECT_EQ( get_tweet_from_key( k_twitter_tweets_table_id, tweet_7_key ),
               t_7 );
    EXPECT_EQ( get_tweet_from_key( k_twitter_tweets_table_id, tweet_13_key ),
               t_13 );
}
