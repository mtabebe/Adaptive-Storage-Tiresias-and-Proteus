#include "constants.h"

#include <glog/logging.h>

#include "../benchmark/smallbank/smallbank_record_types.h"
#include "../benchmark/tpcc/record-types/tpcc_record_types.h"
#include "../benchmark/twitter/record-types/twitter_record_types.h"
#include "hw.h"
#include "partition_funcs.h"

DEFINE_bool( enable_timer_dumps, true, "Should the system dump timers" );

DEFINE_bool( skip_slow_tests, true, "Whether slow tests should be skipped" );

DEFINE_uint32( num_sites, 1, "The number of sites in the system" );
DEFINE_uint32( site_location_identifier, 0,
               "The site identifier of the data site" );

DEFINE_uint32( perf_counter_client_multiplier, 10,
               "The amount of multiplication in the perf counter tracking" );

DEFINE_int32( num_records_in_chain, 128,
              "The number of records to store in an MVCC chain" );
DEFINE_int32( num_records_in_snapshot_chain, 128,
              "The number of records to store in a snapshot chain" );

DEFINE_uint32( num_records_in_multi_entry_table, 4,
               "The number of records in arrays that back multi entry tables" );

DEFINE_uint64( spinlock_back_off_duration, 0,
               "The number of busy loops will perform prior to rechecking."
               " 0 means no backoff" );
DEFINE_string( lock_type, "spinlock",
               "The type of lock to use (spinlock, mutex_lock, semaphore)" );

DEFINE_uint64( expected_tps, 1000000,
               "Expected # of transactions per second." );

DEFINE_uint32( gc_sleep_time, 1000, "Time in ms, between gc iterations" );
DEFINE_double( gc_memory_threshold, 3.0,
               "The factor of extra space used by old version chains" );
DEFINE_int32( gc_thread_priority, 0, "The thread priority of the gc thread" );
DEFINE_int32( always_gc_on_writes, 1, "Should writes always try and GC" );

DEFINE_double( memory_tier_site_space_limit,
               1 /*gb */ * 1000 /*MB*/ * 1000 /*KB*/ * 1000 /* Bytes */,
               "The space limit of the memory tier in bytes" );
DEFINE_double( disk_tier_site_space_limit,
               FLAGS_memory_tier_site_space_limit * 50.0,
               "The space limit of the memory tier in bytes" );

DEFINE_uint64( max_spin_time_us, 10,
               "Maximum spin time in microseconds for counter_locks." );

DEFINE_string( workload_type, "ycsb", "The workload to run (ycsb, tpcc)" );

DEFINE_int32( spar_width, 1440,
              "Number of time slots (denoted by T) per unit of time for "
              "periodicity (ex: 1 day)" );

DEFINE_int32( spar_slot, 1000 * 60,
              "The number of ms in a time slot (ex: 1 minute)" );

DEFINE_int32( spar_training_interval, 60 * 24 * 2,
              "The number of slots in a training interval (ex: 2 days = "
              "60*24*2 1 minute slots)" );

DEFINE_int32( spar_rounding_factor_temporal, 1000 * 60,
              "The granularity of a time segment considered (temporal), in ms "
              "(ex: 1 minute)" );

DEFINE_int32( spar_rounding_factor_periodic, 10 * 1000 * 60,
              "The granularity of a time segment considered (periodic), in ms "
              "(ex: 10 minutes)" );
DEFINE_int32( spar_predicted_arrival_window, 60,
              "Predicted arrival search window for queries, in slots (ex: 60 1 "
              "minute slots)" );
DEFINE_double( spar_normalization_value, 0,
               "Normalization value used to normalize coefficients (0 means "
               "none applied)" );

DEFINE_double( spar_gamma, 0.05, "Gamma hyper-parameter for SPAR model" );
DEFINE_double( spar_learning_rate, 0.01,
               "Learning rate hyper-parameter for SPAR model" );

DEFINE_double( spar_init_weight, 0.0,
               "Initial coefficient weights for SPAR model" );
DEFINE_double( spar_init_bias, 0.0, "Bias hyper-parameter for SPAR model" );
DEFINE_double( spar_regularization, 0.01,
               "Regularization hyper-parameter for SPAR model" );
DEFINE_double( spar_bias_regularization, 0.0001,
               "Initial biased regularization hyper-paremeter for SPAR model" );
DEFINE_int32( spar_batch_size, 500, "Batch size used for training SPAR model" );

DEFINE_uint32( ensemble_slot, 1000 * 60,
               "The number of ms in a time slot (ex: 1 minute)" );

DEFINE_uint32(
    ensemble_search_window, 60,
    "The default search window size for predicted arrival times, in slots" );

DEFINE_uint32(
    ensemble_linear_training_interval, 60 * 4,
    "The training interval size of the linear model, in number of slots" );
DEFINE_uint32(
    ensemble_rnn_training_interval, 60 * 24 * 2,
    "The training interval size of the rnn model, in number of slots" );
DEFINE_uint32(
    ensemble_krls_training_interval, 60 * 24 * 2,
    "The training interval size of the krls model, in number of slots" );

DEFINE_double( ensemble_rnn_learning_rate, 0.001,
               "The learning rate of the rnn model" );
DEFINE_double( ensemble_krls_learning_rate, 0.001,
               "The learning rate of the krls model" );

DEFINE_double( ensemble_outlier_threshold, 1.5,
               "Outlier threshold for using the outlier based model" );
DEFINE_double( ensemble_krls_gamma, 0.001,
               "The gamma factor for krls predictor" );
DEFINE_uint32( ensemble_rnn_epoch_count, 25,
               "The number of epochs to train the rnn model with" );
DEFINE_uint32( ensemble_rnn_sequence_size, 12,
               "The number of items in a sequence fed into the lstm model" );
DEFINE_uint32( ensemble_rnn_layer_count, 5,
               "The number of layers provided into the lstm model" );
DEFINE_bool( ensemble_apply_scheduled_outlier_model, true,
             "Toggle if the scheduled model is used for outlier prediction" );

DEFINE_int32( bench_num_clients, 24,
              "The number of concurrent clients for transactions" );
DEFINE_int32( bench_time_sec, 120,
              "The time in seconds that a benchmark should run for" );
DEFINE_uint64( bench_num_ops_before_timer_check, 10000,
               "The number of operations (per thread) between timer checks for "
               "a benchmark" );

DEFINE_string( db_abstraction_type, "plain_db",
               "The db to run (plain_db, single_site_db, ss_db)" );

DEFINE_int64( ycsb_max_key, 1000000,
              "The number of items in the YCSB database" );
DEFINE_int32( ycsb_value_size, 100, "The size of values in the YCSB database" );
DEFINE_int32( ycsb_partition_size, 100,
              "The size of partitions in YCSB ycsb database " );
DEFINE_int32( ycsb_column_size, 1,
              "The size of columns in YCSB ycsb database " );
DEFINE_int32( ycsb_num_ops_per_transaction, 10,
              "The number of multi-key operations to perform in a multi-key-op "
              "for YCSB" );
DEFINE_double( ycsb_zipf_alpha, 1.0, "The zipf alpha parameter for YCSB" );
DEFINE_double( ycsb_scan_selectivity, 0.1, "The scan selectivity of YCSB" );
DEFINE_int32( ycsb_column_partition_size, 1,
              "The number of columns in a partition size" );
DEFINE_string( ycsb_partition_type, "ROW", "The partition type of YCSB" );
DEFINE_string( ycsb_storage_type, "MEMORY", "The storage type of YCSB" );
DEFINE_bool( ycsb_scan_restricted, false,
             "Restrict scans to non conflicting with RMWs" );
DEFINE_bool( ycsb_rmw_restricted, false,
             "Restrict RMWs to non conflicting with scans" );
DEFINE_bool( ycsb_store_scan_results, false,
             "Whether to return the stored scan results" );

DEFINE_int32( ycsb_write_prob, 50,
              "The percentage of YCSB operations that are writes" );
DEFINE_int32( ycsb_read_prob, 50,
              "The percentage of YCSB operations that are reads" );
DEFINE_int32( ycsb_rmw_prob, 0,
              "The percentage of YCSB operations that are read-modify-writes" );
DEFINE_int32( ycsb_scan_prob, 0,
              "The percentage of YCSB operations that are scans" );
DEFINE_int32( ycsb_multi_rmw_prob, 0,
              "The percentage of YCSB operations that are multi-key RMW" );

DEFINE_int32( db_split_prob, 0,
              "The percentage of operations that are DB split operations" );
DEFINE_int32( db_merge_prob, 0,
              "The percentage of operations that are DB merge operations" );
DEFINE_int32( db_remaster_prob, 0,
              "The percentage of operations that are DB remaster operations" );

DEFINE_uint32( tpcc_num_warehouses, 10,
               "The number of warehouses in TPCC (scalefactor for TPCC)" );
DEFINE_uint32( tpcc_num_items, 100000,
               "The number of items in item (scalefactor for TPCC)" );
DEFINE_uint32( tpcc_num_suppliers, 10000,
               "The number of suppliers in supplier (scalefactor for TPCH)" );

DEFINE_uint32(
    tpcc_expected_num_orders_per_cust, 10,
    "An estimate of the number of orders that each customer will place" );
DEFINE_uint32( tpcc_item_partition_size, 163,
               "The size of partitions for TPCC items table" );
DEFINE_uint32( tpcc_partition_size, 100,
               "The size of partitions for TPCC (unless by warehouse, and "
               "district, and customer)" );
DEFINE_uint32(
    tpcc_district_partition_size, 1,
    "The size of partitions for TPCC district (unless by warehouse, and "
    "district, and customer)" );
DEFINE_uint32(
    tpcc_customer_partition_size, 300,
    "The size of partitions for TPCC customers (unless by warehouse, and "
    "district, and customer)" );
DEFINE_uint32( tpcc_supplier_partition_size, 100,
               "The size of suppliers for TPCC suppliers table" );

DEFINE_uint32( tpcc_new_order_within_warehouse_likelihood, 1,
               "The likelihood of a selecting a remote warehouse for the new "
               "order transaction" );
DEFINE_uint32( tpcc_payment_within_warehouse_likelihood, 15,
               "The likelihood of a selecting a remote warehouse for the "
               "payment transaction" );
DEFINE_bool( tpcc_track_and_use_recent_items, false,
             "Should TPCC track and use recent items for stock level" );

DEFINE_uint32( tpcc_num_clients, 1, "The number of clients for TPCC");
DEFINE_uint32( tpch_num_clients, 1, "The number of clients for TPCH");

// tpcc workload likelihoods
DEFINE_uint32( tpcc_delivery_prob, 4,
               "The percentage of TPCC operations that are delivery" );
DEFINE_uint32( tpcc_new_order_prob, 45,
               "The percentage of TPCC operations that are new order" );
DEFINE_uint32( tpcc_order_status_prob, 4,
               "The percentage of TPCC operations that are order status" );
DEFINE_uint32( tpcc_payment_prob, 43,
               "The percentage of TPCC operations that are payment" );
DEFINE_uint32( tpcc_stock_level_prob, 4,
               "The percentage of TPCC operations that are stock level" );

DEFINE_uint32( tpcc_customer_column_partition_size, k_tpcc_customer_num_columns,
               "The number of columns in a customer partition" );
DEFINE_string( tpcc_customer_part_type, "ROW",
               "The partition type of customer" );
DEFINE_string( tpcc_customer_storage_type, "MEMORY",
               "The storage tier type of customer" );
DEFINE_uint32( tpcc_customer_district_column_partition_size,
               k_tpcc_customer_district_num_columns,
               "The number of columns in a customer_district partition" );
DEFINE_string( tpcc_customer_district_part_type, "ROW",
               "The partition type of customer_district" );
DEFINE_string( tpcc_customer_district_storage_type, "MEMORY",
               "The storage tier type of customer_district" );
DEFINE_uint32( tpcc_district_column_partition_size, k_tpcc_district_num_columns,
               "The number of columns in a district partition" );
DEFINE_string( tpcc_district_part_type, "ROW",
               "The partition type of district" );
DEFINE_string( tpcc_district_storage_type, "MEMORY",
               "The storage tier type of district" );
DEFINE_uint32( tpcc_history_column_partition_size, k_tpcc_history_num_columns,
               "The number of columns in a history partition" );
DEFINE_string( tpcc_history_part_type, "ROW", "The partition type of history" );
DEFINE_string( tpcc_history_storage_type, "MEMORY",
               "The storage tier type of history" );
DEFINE_uint32( tpcc_item_column_partition_size, k_tpcc_item_num_columns,
               "The number of columns in a item partition" );
DEFINE_string( tpcc_item_part_type, "ROW", "The partition type of item" );
DEFINE_string( tpcc_item_storage_type, "MEMORY",
               "The storage tier type of item" );
DEFINE_uint32( tpcc_nation_column_partition_size, k_tpcc_nation_num_columns,
               "The number of columns in a nation partition" );
DEFINE_string( tpcc_nation_part_type, "ROW", "The partition type of nation" );
DEFINE_string( tpcc_nation_storage_type, "MEMORY",
               "The storage tier type of nation" );
DEFINE_uint32( tpcc_new_order_column_partition_size,
               k_tpcc_new_order_num_columns,
               "The number of columns in a new_order partition" );
DEFINE_string( tpcc_new_order_part_type, "ROW",
               "The partition type of new_order" );
DEFINE_string( tpcc_new_order_storage_type, "MEMORY",
               "The storage tier type of new_order" );
DEFINE_uint32( tpcc_order_column_partition_size, k_tpcc_order_num_columns,
               "The number of columns in a order partition" );
DEFINE_string( tpcc_order_part_type, "ROW", "The partition type of order" );
DEFINE_string( tpcc_order_storage_type, "MEMORY",
               "The storage tier type of order" );
DEFINE_uint32( tpcc_order_line_column_partition_size,
               k_tpcc_order_line_num_columns,
               "The number of columns in a order_line partition" );
DEFINE_string( tpcc_order_line_part_type, "ROW",
               "The partition type of order_line" );
DEFINE_string( tpcc_order_line_storage_type, "MEMORY",
               "The storage tier type of order_line" );
DEFINE_uint32( tpcc_region_column_partition_size, k_tpcc_region_num_columns,
               "The number of columns in a region partition" );
DEFINE_string( tpcc_region_part_type, "ROW", "The partition type of region" );
DEFINE_string( tpcc_region_storage_type, "MEMORY",
               "The storage tier type of region" );
DEFINE_uint32( tpcc_stock_column_partition_size, k_tpcc_stock_num_columns,
               "The number of columns in a stock partition" );
DEFINE_string( tpcc_stock_part_type, "ROW", "The partition type of stock" );
DEFINE_string( tpcc_stock_storage_type, "MEMORY",
               "The storage tier type of stock" );
DEFINE_uint32( tpcc_supplier_column_partition_size, k_tpcc_supplier_num_columns,
               "The number of columns in a supplier partition" );
DEFINE_string( tpcc_supplier_part_type, "ROW",
               "The partition type of supplier" );
DEFINE_string( tpcc_supplier_storage_type, "MEMORY",
               "The storage tier type of supplier" );
DEFINE_uint32( tpcc_warehouse_column_partition_size,
               k_tpcc_warehouse_num_columns,
               "The number of columns in a warehouse partition" );
DEFINE_string( tpcc_warehouse_part_type, "ROW",
               "The partition type of warehouse" );
DEFINE_string( tpcc_warehouse_storage_type, "MEMORY",
               "The storage tier type of warehouse" );

DEFINE_uint32( tpch_q1_prob, 0, "The percentage of TPCH for Query 1" );
DEFINE_uint32( tpch_q2_prob, 0, "The percentage of TPCH for Query 2" );
DEFINE_uint32( tpch_q3_prob, 0, "The percentage of TPCH for Query 3" );
DEFINE_uint32( tpch_q4_prob, 0, "The percentage of TPCH for Query 4" );
DEFINE_uint32( tpch_q5_prob, 0, "The percentage of TPCH for Query 5" );
DEFINE_uint32( tpch_q6_prob, 0, "The percentage of TPCH for Query 6" );
DEFINE_uint32( tpch_q7_prob, 0, "The percentage of TPCH for Query 7" );
DEFINE_uint32( tpch_q8_prob, 0, "The percentage of TPCH for Query 8" );
DEFINE_uint32( tpch_q9_prob, 0, "The percentage of TPCH for Query 9" );
DEFINE_uint32( tpch_q10_prob, 0, "The percentage of TPCH for Query 10" );
DEFINE_uint32( tpch_q11_prob, 0, "The percentage of TPCH for Query 11" );
DEFINE_uint32( tpch_q12_prob, 0, "The percentage of TPCH for Query 12" );
DEFINE_uint32( tpch_q13_prob, 0, "The percentage of TPCH for Query 13" );
DEFINE_uint32( tpch_q14_prob, 0, "The percentage of TPCH for Query 14" );
DEFINE_uint32( tpch_q15_prob, 0, "The percentage of TPCH for Query 15" );
DEFINE_uint32( tpch_q16_prob, 0, "The percentage of TPCH for Query 16" );
DEFINE_uint32( tpch_q17_prob, 0, "The percentage of TPCH for Query 17" );
DEFINE_uint32( tpch_q18_prob, 0, "The percentage of TPCH for Query 18" );
DEFINE_uint32( tpch_q19_prob, 0, "The percentage of TPCH for Query 19" );
DEFINE_uint32( tpch_q20_prob, 0, "The percentage of TPCH for Query 20" );
DEFINE_uint32( tpch_q21_prob, 0, "The percentage of TPCH for Query 21" );
DEFINE_uint32( tpch_q22_prob, 0, "The percentage of TPCH for Query 22" );

DEFINE_uint32( tpch_all_prob, 100, "The percentage of TPCH for all queries" );

DEFINE_uint32( tpcc_spec_num_districts_per_warehouse,
               district::NUM_DISTRICTS_PER_WAREHOUSE,
               "The number of districts per TPCC warehouse" );
DEFINE_uint32( tpcc_spec_num_customers_per_district,
               customer::NUM_CUSTOMERS_PER_DISTRICT,
               "The number of customers per TPCC district" );
DEFINE_uint32( tpcc_spec_max_num_order_lines_per_order, order::MAX_OL_CNT,
               "The max number of orders in a TPCC new order" );
DEFINE_uint32( tpcc_spec_initial_num_customers_per_district,
               new_order::INITIAL_NUM_PER_DISTRICT,
               "The initial number of customers in a TPCC district" );

DEFINE_bool( tpcc_should_fully_replicate_warehouses, false,
             "Should warehouses be fully replicated" );
DEFINE_bool( tpcc_should_fully_replicate_districts, false,
             "Should districts be fully replicated" );

DEFINE_bool( tpcc_use_warehouse, false, "Should use warehouse" );
DEFINE_bool( tpcc_use_district, false, "Should use district" );
DEFINE_bool( tpcc_h_scan_full_tables, false, "Should TPC-H queries scan full tables" );

DEFINE_uint64( smallbank_num_accounts, 1000,
               "The number of smallbank accounts (1000 * scale factor)" );
DEFINE_bool( smallbank_hotspot_use_fixed_size, false,
             "Whether or not the hotspot should use a fixed size" );
DEFINE_double( smallbank_hotspot_percentage, 25.0, "The hotspot percentage" );
DEFINE_uint32( smallbank_hotspot_fixed_size, 100, "The hotspot fixed size" );
DEFINE_uint32( smallbank_partition_size, 100,
               "The default smallbank partition size" );
DEFINE_uint32( smallbank_customer_account_partition_spread, 10,
               "The spread on generating customer ids" );

DEFINE_uint32( smallbank_accounts_col_size, k_smallbank_accounts_num_columns,
               "The number of columns in a smallbank account partition" );
DEFINE_string( smallbank_accounts_partition_data_type, "ROW",
               "The partition type of smallbank account" );
DEFINE_string( smallbank_accounts_storage_data_type, "MEMORY",
               "The storage type of smallbank account" );
DEFINE_uint32( smallbank_banking_col_size, k_smallbank_savings_num_columns,
               "The number of columns in a smallbank banking partition" );
DEFINE_string( smallbank_banking_partition_data_type, "ROW",
               "The partition type of smallbank banking" );
DEFINE_string( smallbank_banking_storage_data_type, "MEMORY",
               "The storage type of smallbank banking" );

// probabilities
DEFINE_uint32( smallbank_amalgamate_prob, 15,
               "The probability of the amalgamate transaction" );
DEFINE_uint32( smallbank_balance_prob, 15,
               "The probability of the amalgamate transaction" );
DEFINE_uint32( smallbank_deposit_checking_prob, 15,
               "The probability of the deposit checking transaction" );
DEFINE_uint32( smallbank_send_payment_prob, 25,
               "The probability of the send payment transaction" );
DEFINE_uint32( smallbank_transact_savings_prob, 15,
               "The probability of the transact savings transaction" );
DEFINE_uint32( smallbank_write_check_prob, 15,
               "The probability of the write check transaction" );

DEFINE_uint64( twitter_num_users, 500, "The number of users" );
DEFINE_uint64( twitter_num_tweets, 20000, "The number of tweets baseline" );
DEFINE_double( twitter_tweet_skew, 1.0, "The skew of who generates tweets" );
DEFINE_double( twitter_follow_skew, 1.75, "The skew of who follows who" );
DEFINE_uint32( twitter_account_partition_size, 500,
               "The account partition size baseline" );
DEFINE_uint32( twitter_max_follow_per_user, 50,
               "The max number of follow per user" );
DEFINE_uint32( twitter_max_tweets_per_user,
               ( FLAGS_twitter_num_tweets / FLAGS_twitter_num_users ) *
                   ( FLAGS_twitter_num_tweets / FLAGS_twitter_num_users ),
               "The max number of tweets per user" );
DEFINE_uint32( twitter_limit_tweets, 100, "The number of tweets to return" );
DEFINE_uint32( twitter_limit_tweets_for_uid, 10,
               "The number of tweets for uid" );
DEFINE_uint32( twitter_limit_followers, 20,
               "The number of followers to return" );

DEFINE_bool( twitter_should_fully_replicate_users, false,
             "Should fully replicate the users table" );
DEFINE_bool( twitter_should_fully_replicate_follows, false,
             "Should fully replicate the follows and followers tables" );
DEFINE_bool( twitter_should_fully_replicate_tweets, false,
             "Should fully replicate the tweets table" );

DEFINE_uint32( twitter_followers_column_partition_size,
               k_twitter_followers_num_columns,
               "The number of columns in a twitter_followers partition" );
DEFINE_string( twitter_followers_part_type, "ROW",
               "The partition type of twitter_followers" );
DEFINE_string( twitter_followers_storage_type, "MEMORY",
               "The storage tier type of twitter_followers" );
DEFINE_uint32( twitter_follows_column_partition_size,
               k_twitter_follows_num_columns,
               "The number of columns in a twitter_follows partition" );
DEFINE_string( twitter_follows_part_type, "ROW",
               "The partition type of twitter_follows" );
DEFINE_string( twitter_follows_storage_type, "MEMORY",
               "The storage tier type of twitter_follows" );
DEFINE_uint32( twitter_tweets_column_partition_size,
               k_twitter_tweets_num_columns,
               "The number of columns in a twitter_tweets partition" );
DEFINE_string( twitter_tweets_part_type, "ROW",
               "The partition type of twitter_tweets" );
DEFINE_string( twitter_tweets_storage_type, "MEMORY",
               "The storage tier type of twitter_tweets" );
DEFINE_uint32( twitter_user_profile_column_partition_size,
               k_twitter_user_profile_num_columns,
               "The number of columns in a twitter_user_profile partition" );
DEFINE_string( twitter_user_profile_part_type, "ROW",
               "The partition type of twitter_user_profile" );
DEFINE_string( twitter_user_profile_storage_type, "MEMORY",
               "The storage tier type of twitter_user_profile" );

// probabilities
DEFINE_uint32( twitter_get_tweet_prob, 1,
               "The probability of the get tweet transaction" );
DEFINE_uint32( twitter_get_tweets_from_following_prob, 1,
               "The probability of the get tweets from following transaction" );
DEFINE_uint32( twitter_get_followers_prob, 7,
               "The probability of the get followers transaction" );
DEFINE_uint32( twitter_get_user_tweets_prob, 89,
               "The probability of the get user tweets transaction" );
DEFINE_uint32( twitter_insert_tweet_prob, 2,
               "The probability of the insert tweet transaction" );
DEFINE_uint32( twitter_get_recent_tweets_prob, 0,
               "The probability of getting recent tweets transaction" );
DEFINE_uint32( twitter_get_tweets_from_followers_prob, 0,
               "The probability of getting tweets from followers transaction" );
DEFINE_uint32( twitter_get_tweets_like_prob, 0,
               "The probability of getting tweets like transaction" );
DEFINE_uint32( twitter_update_followers_prob, 0,
               "The probability of updating twitter followers" );


DEFINE_uint64( ss_bucket_size, 256,
               "The size of buckets in the site selector" );

DEFINE_int32( astream_tracking_interval_ms, 100,
              "tracking interval milliseconds" );
DEFINE_int32( astream_mod, 1,
              "num_clients / astream_mod will have their astreams tracked" );
DEFINE_int32( astream_empty_wait_ms, 100,
              "time to wait before rechecking astream if empty" );
DEFINE_uint64( astream_max_queue_size, 1000,
               "maximum number of entries that can be in queue" );
DEFINE_double( ss_max_write_blend_rate, 0.5,
               "The rate that the max write to partititions are blended" );

DEFINE_bool( ss_run_background_workers, true,
             "Should we run background executor workers" );
DEFINE_uint32(
    ss_num_background_work_items_to_execute, 5,
    "The maximum number of background work items to execute per iteration" );

DEFINE_int32(
    num_site_selector_worker_threads_per_client, 4,
    "The number of threads per site selector worker threads per client" );

DEFINE_uint32( estore_clay_periodic_interval_ms, 10000,
               "The time in ms between estore or clay repartitioning" );
DEFINE_double(
    estore_load_epsilon_threshold, 0.025,
    "The ratio of reads and writes that sites must be within the average" );
DEFINE_double( estore_hot_partition_pct_thresholds, 0.01,
               "The percent of partitions that are considered hot, and "
               "eligible for splitting" );
DEFINE_double( adr_multiplier, 2,
               "The ratio that a read (write) access count must be greater "
               "than a write (read)" );

DEFINE_bool( ss_enable_plan_reuse, true, "Reuse plans in SS" );
DEFINE_double( ss_plan_reuse_threshold, 0.75,
               "Reuse plans if greater than threshold" );
DEFINE_bool( ss_enable_wait_for_plan_reuse, true,
             "Wait for reuse plans in SS" );

DEFINE_string( ss_optimistic_lock_mode, "no_lock",
               "The lock mode for optimistic transactions" );
DEFINE_string( ss_physical_adjustment_lock_mode, "try_lock",
               "The lock mode for physical adjustment transactions" );
DEFINE_string( ss_force_change_lock_mode, "try_lock",
               "The lock mode for force change transactions" );

DEFINE_string( ss_strategy_type, "heuristic",
               "The type of remastering strategy to use (heuristic, naive)" );
DEFINE_string(
    ss_mastering_type, "adapt",
    "The type of mastering strategy to use (single_master_multi_slave, two_pc, "
    "dynamic_mastering, partitioned_store, drp, adapt)" );

DEFINE_double( ss_skew_weight, 0.33,
               "skew weight for heuristic site_evaluator" );
DEFINE_double( ss_skew_exp_constant, 10.0,
               "skew exponential constant for heuristic site_evaluator" );
DEFINE_double( ss_svv_delay_weight, 0.33,
               "svv delay weight for heuristic site_evaluator" );
DEFINE_double(
    ss_single_site_within_transactions_weight, 0.33,
    "single site within transactions weight for heuristic site_evaluator" );
DEFINE_double(
    ss_single_site_across_transactions_weight, 0.33,
    "single site across transactions weight for heuristic site_evaluator" );
DEFINE_double( ss_wait_time_normalization_constant, 100.0,
               "normalization constant for svv wait differences" );

DEFINE_double( ss_split_partition_ratio, 1.5,
               "Ratio of writes compared to average to split a partition." );
DEFINE_double( ss_merge_partition_ratio, 0.5,
               "Ratio of writes compared to average to merge a partition." );
DEFINE_int32(
    ss_num_sampled_transactions_for_client, 50,
    "Number of transactions to sample from the clients when making decisions" );
DEFINE_int32( ss_num_sampled_transactions_from_other_clients, 50,
              "Number of transactions to sample from other clients when making "
              "decisions" );
DEFINE_double( ss_upfront_cost_weight, 1.0,
               "The weight to apply to the upfront cost" );
DEFINE_double( ss_scan_upfront_cost_weight, 0.1,
               "The weight to apply to the scan upfront cost" );
DEFINE_double( ss_horizon_weight, 1.0,
               "The weight to apply to costs from the future benefit" );
DEFINE_double( ss_default_horizon_benefit_weight, 1.0,
               "The weight to apply to costs from the future benefit using the "
               "default cost function" );
DEFINE_double( ss_repartition_remaster_horizon_weight, 1.0,
               "The weight to apply to costs from future remastering when "
               "repartitioning" );
DEFINE_double( ss_horizon_storage_weight, 1.0,
               "The weight to apply to costs from changes to storage" );

DEFINE_double( ss_repartition_prob_rescale, 1.0,
               "The scale to apply to costs from repartitioning" );
DEFINE_double( ss_outlier_repartition_std_dev, 1.0,
               "The scale to apply to the standard deviation of the write "
               "load, to force repartitioning" );
DEFINE_double( ss_split_partition_size_ratio, 0.05,
               "The fraction of the default partition size, which triggers "
               "scaling the standard deviation, which decreases the likelihood "
               "of small partitions" );

DEFINE_double( ss_plan_site_load_balance_weight, 1.0,
               "The weight to apply to the plan at the site load" );
DEFINE_double( ss_plan_site_storage_balance_weight, 1.0,
               "The weight to apply to the plan at the site storage" );

DEFINE_double(
    ss_plan_site_storage_overlimit_weight, 100.0,
    "The weight to apply to the plan if the storage limit is overweight" );
DEFINE_double( ss_plan_site_storage_variance_weight, 1.0,
               "The weight to apply to the plan based on the variance in the "
               "storage limit" );
DEFINE_double( ss_plan_site_memory_overlimit_weight, 10.0,
               "The weight to apply to the plan if memory is overlimit" );
DEFINE_double( ss_plan_site_disk_overlimit_weight, 1.0,
               "The weight to apply to the plan if disk is overlimit" );
DEFINE_double( ss_plan_site_memory_variance_weight, 10.0,
               "The weight to apply to the plan based on memory variance" );
DEFINE_double( ss_plan_site_disk_variance_weight, 1.0,
               "The weight to apply to the plan based on disk variance" );

DEFINE_bool( ss_allow_force_site_selector_changes, false,
             "Should the site selector force physical design changes "
             "probabilistically" );

DEFINE_bool( ss_plan_scan_in_cost_order, true,
             "Should the site selector scan plan in most cost based order" );
DEFINE_bool( ss_plan_scan_limit_changes, true,
             "Should the site selector limit changing scan plans once a ratio "
             "is exceeded" );
DEFINE_double( ss_plan_scan_limit_ratio, 0.8,
               "Limit of the ratio between scan plan change vs no change to "
               "continue allowing changes" );

DEFINE_string( ss_acceptable_new_partition_types, "ROW",
               "The acceptable new partition types, list" );
DEFINE_string( ss_acceptable_partition_types, "ROW",
               "The acceptable partition types, list" );
DEFINE_string( ss_acceptable_new_storage_types, "MEMORY",
               "The acceptable new partition types, list" );
DEFINE_string( ss_acceptable_storage_types, "MEMORY",
               "The acceptable storage types, list" );

DEFINE_bool( use_query_arrival_predictor, true, "Use query arrival predictor" );
DEFINE_string( query_arrival_predictor_type, "SIMPLE_QUERY_PREDICTOR",
               "The predictor to use to predict query arrival" );
DEFINE_double( query_arrival_access_threshold, 0.5,
               "The threshold of number of queries to be considered arrived" );
DEFINE_uint64( query_arrival_time_bucket, 30,
               "The bucket of time for a predicted query" );
DEFINE_double( query_arrival_count_bucket, 10,
               "The bucket of query coutns for a predicted query" );
DEFINE_double( query_arrival_time_score_scalar, 1.0,
               "The scaling of query arrival time that should be used when "
               "costing operations" );
DEFINE_double( query_arrival_default_score, 0.001,
               "The default score when a query is not predicted" );

DEFINE_bool( cost_model_is_static_model, false,
             "Should the cost model be static, and not learning" );
DEFINE_double( cost_model_learning_rate, 0.01,
               "The learning rate of the cost model" );
DEFINE_double( cost_model_regularization, 0.01,
               "The regularization of the cost model" );
DEFINE_double( cost_model_bias_regularization, 0.001,
               "The bias regularization of the cost model" );

DEFINE_double( cost_model_momentum, 0.8, "The momentum of the cost model");
DEFINE_double( cost_model_kernel_gamma, 0.001,
               "The gamma used in kernel functions" );

DEFINE_int32( cost_model_max_internal_model_size_scalar, 50,
              "The maximum model size of a KRLS model (scaled by the number of "
              "input parameters)" );
DEFINE_int32( cost_model_layer_1_nodes_scalar, 10,
              "The size of the first layer of perceptron model (scaled by the "
              "number of input parameters)" );
DEFINE_int32( cost_model_layer_2_nodes_scalar, 5,
              "The size of the second layer of perceptron model (scaled by the "
              "number of input parameters)" );

DEFINE_double( cost_model_max_time_prediction_scale, 0.01,
               "max_time_prediction_scale" );

DEFINE_double( cost_model_num_reads_weight, 1,
               "The weight of reading a record" );
DEFINE_double( cost_model_num_reads_normalization, 1000,
                   "The normalization of reading a record" );
DEFINE_double( cost_model_read_weight, 1, "The weight of reading a record" );
DEFINE_double( cost_model_read_bias, 0, "The bias of reading a record" );
DEFINE_double( cost_model_read_normalization, 1000 /* number of records read */,
               "The normalization of reading a record" );
DEFINE_double( cost_model_num_reads_max_input, 1000,
               "The max input of reading a record" );
DEFINE_string( cost_model_read_predictor_type, "MULTI_LINEAR_PREDICTOR",
               "Read predictor type" );

DEFINE_double(cost_model_num_reads_disk_weight, 10,
              "The weight of reading a record from disk");
DEFINE_double(cost_model_read_disk_bias, 100,
              "the bias of reading a record from disk");
DEFINE_string( cost_model_read_disk_predictor_type, "MULTI_LINEAR_PREDICTOR",
               "Read from disk predictor type" );

DEFINE_double( cost_model_scan_num_rows_weight, 1,
               "The weight of scanning records" );
DEFINE_double( cost_model_scan_num_rows_normalization, 1000,
               "The normalization of scanning a record" );
DEFINE_double( cost_model_scan_num_rows_max_input, 10000,
               "The max input of scanning records" );
DEFINE_double( cost_model_scan_selectivity_weight, 1,
               "The weight of scan selectivity" );
DEFINE_double( cost_model_scan_selectivity_normalization, 1,
               "Normalization of scan selectivity" );
DEFINE_double( cost_model_scan_selectivity_max_input, 1,
               "The max selectivity of a scan" );
DEFINE_double( cost_model_scan_bias, 0, "The bias of scanning" );
DEFINE_string( cost_model_scan_predictor_type, "MULTI_LINEAR_PREDICTOR",
               "Scan predictor type" );

DEFINE_double( cost_model_scan_disk_num_rows_weight, 10,
               "The weight of scanning records from disk" );
DEFINE_double( cost_model_scan_disk_selectivity_weight, 1,
               "The weight of scan selectivity from disk" );
DEFINE_double( cost_model_scan_disk_bias, 100, "The bias of scanning from disk" );
DEFINE_string( cost_model_scan_disk_predictor_type, "MULTI_LINEAR_PREDICTOR",
               "Scan from disk predictor type" );

DEFINE_double( cost_model_num_writes_weight, 1,
               "The weight of writing a record" );
DEFINE_double( cost_model_num_writes_normalization, 100,
               "The normalization of writing a record" );
DEFINE_double( cost_model_num_writes_max_input,
               FLAGS_cost_model_num_reads_max_input,
               "The max input of writing a record" );
DEFINE_double( cost_model_write_bias, 0, "The bias of writing a record" );
DEFINE_string( cost_model_write_predictor_type, "MULTI_LINEAR_PREDICTOR",
               "Write predictor type" );

DEFINE_double( cost_model_repartition_weight, 1,
               "The weight of repartitioning (merge/split) a record" );
DEFINE_double( cost_model_repartition_bias, 0,
               "The bias of repartitioning (merge/split) a record" );
DEFINE_double( cost_model_repartition_normalization,
               1000 /*number of records in a partition*/,
               "The normalization of repartitioning (merge/split) a record" );
DEFINE_double( cost_model_repartition_max_input,
               50 /*10 * number of records in a partition / 1000*/,
               "The max_input of repartitioning (merge/split) a record" );

DEFINE_double( cost_model_lock_weight, 1, "The weight of acquiring a lock" );
DEFINE_double( cost_model_lock_normalization, 10 /* num writes to a partition*/,
               "The normalization of acquiring a lock" );
DEFINE_double( cost_model_lock_max_input, 10 /* num writes to partition*/,
               "The max input of acquiring a lock" );
DEFINE_double( cost_model_lock_bias, 0, "The bias of acquiring a lock" );
DEFINE_string( cost_model_lock_predictor_type, "MULTI_LINEAR_PREDICTOR",
               "Lock predictor type" );

DEFINE_double( cost_model_commit_serialize_weight, 1,
               "The weight of serializing a commit record" );
DEFINE_double( cost_model_commit_serialize_normalization,
               100 /*number of records written*/,
               "The normalization of serializing a commit record" );
DEFINE_double( cost_model_commit_serialize_max_input,
               10 /*number of records written*/,
               "The max input of serializing a commit record" );
DEFINE_double( cost_model_commit_serialize_bias, 0,
               "The bias of serializing a commit record" );
DEFINE_string( cost_model_commit_serialize_predictor_type,
               "MULTI_LINEAR_PREDICTOR", "Commit serialize predictor type" );

DEFINE_double( cost_model_commit_build_snapshot_weight, 1,
               "The weight of building a commit snapshot" );
DEFINE_double( cost_model_commit_build_snapshot_normalization,
               100 /*num partitions in a transaction*/,
               "The normalization of building a commit snapshot" );
DEFINE_double( cost_model_commit_build_snapshot_max_input,
               10 /*num partitions in a transaction*/,
               "The max input of building a commit snapshot" );
DEFINE_double( cost_model_commit_build_snapshot_bias, 0,
               "The bias of building a commit snapshot" );
DEFINE_string( cost_model_commit_build_snapshot_predictor_type,
               "MULTI_LINEAR_PREDICTOR",
               "Commit build snapshot predictor type" );

DEFINE_double( cost_model_wait_for_service_weight, 1,
               "The weight of waiting for service" );
DEFINE_double( cost_model_wait_for_service_normalization, 12 /* max load */,
               "The normalization of waiting for service" );
DEFINE_double( cost_model_wait_for_service_max_input, 10 /* max load */,
               "The max input of waiting for service" );
DEFINE_double( cost_model_wait_for_service_bias, 0,
               "The bias of waiting for service" );
DEFINE_string( cost_model_wait_for_service_predictor_type,
               "MULTI_LINEAR_PREDICTOR", "Wait for service predictor type" );

DEFINE_double( cost_model_wait_for_session_version_weight, 1,
               "The weight of waiting for session version" );
DEFINE_double( cost_model_wait_for_session_version_normalization,
               5000 /* num updates to topic per second ~ tput / num topics*/,
               "The normalization of waiting for session version" );
DEFINE_double( cost_model_wait_for_session_version_max_input,
               10 /* num updates to topic per second ~ tput / num topics*/,
               "The max input of waiting for session version" );
DEFINE_double( cost_model_wait_for_session_version_bias, 0,
               "The bias of waiting for session version" );
DEFINE_string( cost_model_wait_for_session_version_predictor_type,
               "MULTI_LINEAR_PREDICTOR",
               "Wait for session version predictor type" );

DEFINE_double( cost_model_wait_for_session_snapshot_weight, 1,
               "The weight of waiting for session for snapshot" );
DEFINE_double( cost_model_wait_for_session_snapshot_normalization,
               5000 /* num updates to topic per second ~ tput / num topics*/,
               "The normalization of waiting for session for snapshot" );
DEFINE_double( cost_model_wait_for_session_snapshot_max_input,
               10 /* num updates to topic per second ~ tput / num topics*/,
               "The max input of waiting for session for snapshot" );
DEFINE_double( cost_model_wait_for_session_snapshot_bias, 0,
               "The bias of waiting for session for snapshot" );
DEFINE_string( cost_model_wait_for_session_snapshot_predictor_type,
               "MULTI_LINEAR_PREDICTOR",
               "Wait for session snapshot predictor type" );

DEFINE_double( cost_model_wait_for_session_remaster_weight, 1,
               "The weight of waiting for session for remastering" );
DEFINE_double( cost_model_wait_for_session_remaster_bias, 0,
               "The bias of waiting for session for remastering" );
DEFINE_double( cost_model_wait_for_session_remaster_normalization,
               100000 /* num updates to topic per second ~ tput / num topics*/,
               "The normalization of waiting for session for remastering" );
DEFINE_double( cost_model_wait_for_session_remaster_max_input,
               1 /* num updates to topic per second ~ tput / num topics*/,
               "The max input of waiting for session for remastering" );

DEFINE_double( cost_model_site_load_prediction_write_weight, 1,
               "The weight of write count to site load" );
DEFINE_double( cost_model_site_load_prediction_write_normalization,
               100 /* tput per ms*/,
               "The normalization of write count to site load" );
DEFINE_double( cost_model_site_load_prediction_write_max_input,
               10 /* tput per ms*/,
               "The max input of write count to site load" );

DEFINE_double( cost_model_site_load_prediction_read_weight,
               FLAGS_cost_model_site_load_prediction_write_weight,
               "The weight of read count to site load" );
DEFINE_double( cost_model_site_load_prediction_read_normalization,
               FLAGS_cost_model_site_load_prediction_write_normalization,
               "The normalization of read count to site load" );
DEFINE_double( cost_model_site_load_prediction_read_max_input,
               FLAGS_cost_model_site_load_prediction_write_max_input,
               "The max input of read count to site load" );

DEFINE_double( cost_model_site_load_prediction_update_weight,
               FLAGS_cost_model_site_load_prediction_write_weight,
               "The weight of update count to site load" );
DEFINE_double( cost_model_site_load_prediction_update_normalization,
               FLAGS_cost_model_site_load_prediction_write_normalization,
               "The normalization of update count to site load" );
DEFINE_double( cost_model_site_load_prediction_update_max_input,
               FLAGS_cost_model_site_load_prediction_write_max_input,
               "The max input of update count to site load" );

DEFINE_double( cost_model_site_load_prediction_bias, 1,
               "The bias of predicting site load" );
DEFINE_double( cost_model_site_load_prediction_max_scale, 2400,
               "The max scale of predicting site load" );
DEFINE_string( cost_model_site_load_predictor_type, "MULTI_LINEAR_PREDICTOR",
               "Site load predictor type" );

DEFINE_double( cost_model_site_operation_count_weight, 1,
               "The weight of operation count translation" );
DEFINE_double( cost_model_site_operation_count_bias, 0,
               "The bias of operation count translation" );
DEFINE_double( cost_model_site_operation_count_normalization, 10,
               "The normalization of operation count translation" );
DEFINE_double( cost_model_site_operation_count_max_input, 10,
               "The max_input of operation count translation" );
DEFINE_double( cost_model_site_operation_count_max_scale, 250,
               "The max scale of operation count translation" );
DEFINE_string( cost_model_site_operation_count_predictor_type,
               "MULTI_LINEAR_PREDICTOR",
               "Site operation count predictor type" );

DEFINE_double( cost_model_distributed_scan_max_weight, 1.0,
               "The weight of distributed scan based on the max input" );
DEFINE_double( cost_model_distributed_scan_min_weight, 0.05,
               "The weight of distributed scan based on the min input" );
DEFINE_double( cost_model_distributed_scan_normalization, 1.0,
               "The normalization of distributed scan" );
DEFINE_double( cost_model_distributed_scan_max_input, 100,
               "The max input of distributed scan" );
DEFINE_double( cost_model_distributed_scan_bias, 0.0,
               "The bias of distributed scan" );
DEFINE_string( cost_model_distributed_scan_predictor_type,
               "MULTI_LINEAR_PREDICTOR", "Distributed scan predictor type" );

DEFINE_double( cost_model_memory_num_rows_normalization, 1000,
               "Cost model memory num rows normalization" );
DEFINE_double( cost_model_memory_num_rows_max_input, 100,
               "Cost model memory num rows max input" );

DEFINE_double( cost_model_memory_allocation_num_rows_weight, 1,
               "Memory allocation num rows weight" );
DEFINE_double( cost_model_memory_allocation_bias, 0, "Memory allocation bias" );
DEFINE_string( cost_model_memory_allocation_predictor_type,
               "MULTI_LINEAR_PREDICTOR", "Memory allocation predictor type" );

DEFINE_double( cost_model_memory_deallocation_num_rows_weight, 1,
               "Memory deallocation num rows weight" );
DEFINE_double( cost_model_memory_deallocation_bias, 0,
               "Memory deallocation bias" );
DEFINE_string( cost_model_memory_deallocation_predictor_type,
               "MULTI_LINEAR_PREDICTOR", "Memory deallocation predictor type" );

DEFINE_double( cost_model_memory_assignment_num_rows_weight, 1,
               "Memory assignment num rows weight" );
DEFINE_double( cost_model_memory_assignment_bias, 0, "Memory assignment bias" );
DEFINE_string( cost_model_memory_assignment_predictor_type,
               "MULTI_LINEAR_PREDICTOR", "Memory assignment predictor type" );

DEFINE_double( cost_model_disk_num_rows_normalization, 1000,
               "The normalization of num rows for disk" );
DEFINE_double( cost_model_disk_num_rows_max_input, 100,
               "The max input of num rows for disk" );

DEFINE_double( cost_model_evict_to_disk_num_rows_weight, 10,
               "Evict to disk num rows weight" );
DEFINE_double( cost_model_evict_to_disk_bias, 100, "Evict to disk bias" );
DEFINE_string( cost_model_evict_to_disk_predictor_type,
               "MULTI_LINEAR_PREDICTOR", "Evict to disk predictor type" );

DEFINE_double( cost_model_pull_from_disk_num_rows_weight, 10,
               "Pull from disk num rows weight" );
DEFINE_double( cost_model_pull_from_disk_bias, 100, "Pull from disk bias" );
DEFINE_string( cost_model_pull_from_disk_predictor_type,
               "MULTI_LINEAR_PREDICTOR", "Pull from disk predictor type" );

DEFINE_double( cost_model_widths_weight, 1, "Cell widths weight" );
DEFINE_double( cost_model_widths_normalization, 100,
               "Cell widths normalization" );
DEFINE_double( cost_model_widths_max_input, 10, "Cell widths max input" );

DEFINE_double( cost_model_wait_for_session_remaster_default_pct, 0.5,
               "The default ratio of the number of updates for remastering "
               "from the normalization factor" );

DEFINE_bool( ss_enum_enable_track_stats, true,
             "Whether or not the site selector should track stats for "
             "intelligent enumeration" );
DEFINE_double(
    ss_enum_prob_of_returning_default, 0.1,
    "The probability of returning a default for intelligent enumeration" );
DEFINE_double( ss_enum_min_threshold, 0.25,
               "The minimum probability to be considered in intelligent "
               "enumeration" );
DEFINE_uint64( ss_enum_min_count_threshold, 50,
               "The minimum count to go beyond default in intelligent "
               "enumeration" );
DEFINE_double( ss_enum_contention_bucket, 0.01,
               "The size of the bucket for contention" );
DEFINE_double( ss_enum_cell_width_bucket, 10,
               "The size of the bucket for cell width" );
DEFINE_int32( ss_enum_num_entries_bucket, 100,
              "The size of the buckets for num entries" );
DEFINE_double( ss_enum_num_updates_needed_bucket, 50,
               "The size of the bucket for num updates needed" );
DEFINE_double( ss_enum_scan_selectivity_bucket, 0.01,
               "The size of the bucket for scan selectivity" );
DEFINE_int32( ss_enum_num_point_reads_bucket, 10,
              "The size of the bucket for num point reads" );
DEFINE_int32( ss_enum_num_point_updates_bucket, 10,
              "The size of the bucket for num point updates" );
DEFINE_int32( predictor_max_internal_model_size, 10000,
              "The default maximum dictionary size for kernel-based predictors" );

DEFINE_int32( ss_update_stats_sleep_delay_ms, 2000,
              "time spent between stats update polling" );
DEFINE_int32( ss_num_poller_threads, 1, "the number of poller threads" );

DEFINE_int32( ss_colocation_keys_sampled, 5,
              "number of keys and correlations sampled in ss calcs" );
DEFINE_double( ss_partition_coverage_threshold, 0.25,
               "The amount that minimum and maximum keys need "
               "to span over the "
               "partition" );

DEFINE_string( update_destination_type, "no_op_destination",
               "The update destination type (no_op_destination, "
               "vector_destination, kafka_destination)" );
DEFINE_string( update_source_type, "no_op_source",
               "The update source type (no_op_source, vector_source, "
               "kafka_source)" );
DEFINE_int64( kafka_buffer_read_backoff, 100000,
              "the backoff for spinning on kafka reads" );
DEFINE_bool( limit_number_of_records_propagated, false,
             "Should the number of updates that are being "
             "shipped be limited" );
DEFINE_int32( number_of_updates_needed_for_propagation, 2147483647,
              "The number of updates that need to be between writes to "
              "propagate a new order transaction" );

DEFINE_int32( num_update_destinations, 10,
              "The number of distinct update destinations" );
DEFINE_int32( num_update_sources, 10, "The number of distinct update sources" );

DEFINE_int32( sleep_time_between_update_enqueue_iterations, 1000,
              "The time to sleep between fetching updates (in ms)" );
DEFINE_int32( num_updates_before_apply_self, 100,
              "The number of updates that must be enqueued "
              "before the enqueuer "
              "does the application itself" );
DEFINE_int32( num_updates_to_apply_self, 25,
              "The number of updates that an enqueuer will itself apply" );

DEFINE_int64( kafka_poll_count, 10000,
              "The number of operations before polling kafka" );
DEFINE_int64( kafka_poll_sleep_ms, 100,
              "During between background thread for writer "
              "polling kafka" );
DEFINE_int32( kafka_backoff_sleep_ms, 10,
              "If the rdkafka buffer is full, sleep for this "
              "amount of time "
              "before trying to insert again." );
DEFINE_int32( kafka_poll_timeout, 0,
              "The time that should be spent blocking on a poll "
              "to kafka" );
DEFINE_int32( kafka_flush_wait_ms, 10000,
              "The time that should be spent waiting to flush "
              "kafka on shutdown" );
DEFINE_int32( kafka_consume_timeout_ms, 1,
              "The time that should be spent waiting for "
              "messages on consume" );
DEFINE_int32( kafka_seek_timeout_ms, 1,
              "The time that should be spent waiting for a seek "
              "on consume" );

DEFINE_string( kafka_fetch_max_ms, "1",
               "The maximum time the broker can wait to fill response "
               "(fetch.wait.max.ms, fetch.error.backoff.ms)" );
DEFINE_string( kafka_queue_max_ms, "0",
               "The maximum delay to wait for messages to build up before "
               "sending them to kafka (queue.buffering.max.ms)" );
DEFINE_string( kafka_socket_blocking_max_ms, "1000",
               "The maximum time a broker socket operation may block "
               "(socket.blocking.max.ms)" );
DEFINE_string( kafka_consume_max_messages, "0",
               "The max number of messages to consume in one consume (0 = "
               "unlimited) (consume.callback.max.messages)" );
DEFINE_string( kafka_produce_max_messages, "10000",
               "Max number of messages batched writing to kafka "
               "(batch.num.messages)" );
DEFINE_string( kafka_required_acks, "1",
               "The number of acknowledgements the kafka leader must "
               "receive "
               "before responding to the client (request.required.acks)" );
DEFINE_string( kafka_send_max_retries, "10",
               "The number of times the kafka producer should "
               "retry sending a "
               "message (message.send.max.retries)" );
DEFINE_string( kafka_max_buffered_messages, "10000000",
               "The number of total messages allowed to be "
               "queued by the producer"
               "(queue.buffering.max.messages)" );
DEFINE_string( kafka_max_buffered_messages_size, "2097151",
               "The max amount of data that is buffered at the producer"
               "(queue.buffering.max.kbytes)" );

DEFINE_string(
    rewriter_type, "hash_rewriter",
    "The rewriter type to use (hash_rewriter, range_rewriter, "
    "single_master_rewriter, warehouse_rewriter, probability_range_rewriter)" );
DEFINE_double( prob_range_rewriter_threshold, 0.75,
               "The threshold of putting same range on the same site" );

DEFINE_bool( load_chunked_tables, true,
             "Whether tables being loaded are chunked" );
DEFINE_bool( persist_chunked_tables, true,
             "Whether tables being persisted should be chunked" );
DEFINE_int32( chunked_table_size, 5000, "The size of table chunks" );

DEFINE_bool( enable_secondary_storage, true,
             "Should secondary storage be enabled" );
DEFINE_string( secondary_storage_dir, "/tmp/dyna-mast-storage-tier",
               "The location of the secondary storage dir" );

DEFINE_int32( sample_reservoir_size_per_client, 1000,
              "The number of entries allowed in the per-client "
              "reservoirs for sample tracking" );
DEFINE_double( sample_reservoir_initial_multiplier, 2.0,
               "The initial weight value of the reservoir "
               "chosen as a multiple of reservoir size" );
DEFINE_double( sample_reservoir_weight_increment, 2.0,
               "The increment used to increase the weight of "
               "the reservoir" );
DEFINE_double( sample_reservoir_decay_rate, 0.2,
               "When the reservoir is decayed, multiply the "
               "current weight by this value" );
DEFINE_int32( force_change_reservoir_size_per_client, 100,
              "The number of entries allowed in the per-client "
              "reservoirs to force changes" );
DEFINE_double( force_change_reservoir_initial_multiplier, 100.0,
               "The initial weight value of the reservoir "
               "chosen as a multiple of reservoir size" );
DEFINE_double( force_change_reservoir_weight_increment, 2.0,
               "The increment used to increase the weight of "
               "the reservoir" );
DEFINE_double( force_change_reservoir_decay_rate, 0.2,
               "When the reservoir is decayed, multiply the "
               "current weight by this value" );
DEFINE_int32( reservoir_decay_seconds, 30,
              "How often decay() is called on each reservoir in seconds" );

DEFINE_double( ss_storage_removal_threshold, 0.97,
               "When storage is above the limit" );
DEFINE_int32( ss_num_remove_replicas_iteration, 100,
              "The number of iterations to remove replicas" );

DEFINE_bool( ss_allow_vertical_partitioning, true,
             "Should the planner allow vertical split/merges" );
DEFINE_bool( ss_allow_horizontal_partitioning, true,
             "Should the planner allow horizontal split/merges" );
DEFINE_bool( ss_allow_change_partition_types, true,
             "Should the planner allow changing partition types" );
DEFINE_bool( ss_allow_change_storage_types, true,
             "Should the planner allow changing storage types" );

DEFINE_int32( timer_log_level, 5, "The default log level for timers" );
DEFINE_int32( slow_timer_error_log_level, 1,
              "The default log level for slow timers, or prediction errors" );
DEFINE_int32(
    cost_model_log_level, 5,
    "The default log level for cost model prediction results and weights" );
DEFINE_int32( heuristic_evaluator_log_level, 5,
              "The default log level for the heuristic site evalutor" );

DEFINE_int32( periodic_cost_model_log_level, FLAGS_cost_model_log_level,
              "The default log level for periodic cost model prediction "
              "results and weights" );

DEFINE_int32( update_propagation_log_level, 5,
              "The default log level for update propagation state" );
DEFINE_int32( significant_update_propagation_log_level,
              FLAGS_update_propagation_log_level,
              "The default log level for significant propagation state" );
DEFINE_int32(
    ss_action_executor_log_level, 5,
    "The default log level for logging pre execution actions occuring" );
DEFINE_int32(
    rpc_handler_log_level, 7,
    "The default log level for logging receive and return of rpc handler" );

DEFINE_double( slow_timer_log_time_threshold, 500000000,
               "The time (in ns) that triggers logging the timer" );

DEFINE_double( cost_model_log_error_threshold, 500000,
               "The error (in us) that triggers logging the error" );

bool k_enable_timer_dumps = FLAGS_enable_timer_dumps;
bool k_skip_slow_tests = FLAGS_skip_slow_tests;

uint32_t k_num_sites = FLAGS_num_sites;
uint32_t k_site_location_identifier = FLAGS_site_location_identifier;

uint32_t k_perf_counter_client_multiplier =
    FLAGS_perf_counter_client_multiplier;

uint64_t k_spinlock_back_off_duration = FLAGS_spinlock_back_off_duration;
int32_t  k_num_records_in_chain = FLAGS_num_records_in_chain;
int32_t  k_num_records_in_snapshot_chain = FLAGS_num_records_in_snapshot_chain;
uint32_t k_num_records_in_multi_entry_table =
    FLAGS_num_records_in_multi_entry_table;
lock_type k_lock_type = lock_type::SPINLOCK;

workload_type k_workload_type = workload_type::YCSB;
int32_t       k_bench_num_clients = FLAGS_bench_num_clients;
int32_t       k_bench_time_sec = FLAGS_bench_time_sec;
uint64_t      k_bench_num_ops_before_timer_check =
    FLAGS_bench_num_ops_before_timer_check;

db_abstraction_type k_db_abstraction_type = db_abstraction_type::PLAIN_DB;

int32_t k_spar_width = FLAGS_spar_width;
int32_t k_spar_slot = FLAGS_spar_slot;
int32_t k_spar_training_interval = FLAGS_spar_training_interval;
int32_t k_spar_rounding_factor_temporal = FLAGS_spar_rounding_factor_temporal;
int32_t k_spar_rounding_factor_periodic = FLAGS_spar_rounding_factor_periodic;
int32_t k_spar_predicted_arrival_window = FLAGS_spar_predicted_arrival_window;
double  k_spar_normalization_value = FLAGS_spar_normalization_value;
double  k_spar_gamma = FLAGS_spar_gamma;
double  k_spar_learning_rate = FLAGS_spar_learning_rate;
double  k_spar_init_weight = FLAGS_spar_init_weight;
double  k_spar_init_bias = FLAGS_spar_init_bias;
double  k_spar_regularization = FLAGS_spar_regularization;
double  k_spar_bias_regularization = FLAGS_spar_bias_regularization;
int32_t k_spar_batch_size = FLAGS_spar_batch_size;

// Ensemble configs
uint32_t k_ensemble_slot = FLAGS_ensemble_slot;
uint32_t k_ensemble_search_window = FLAGS_ensemble_search_window;
uint32_t k_ensemble_linear_training_interval =
    FLAGS_ensemble_linear_training_interval;
uint32_t k_ensemble_rnn_training_interval =
    FLAGS_ensemble_rnn_training_interval;
uint32_t k_ensemble_krls_training_interval =
    FLAGS_ensemble_krls_training_interval;
double   k_ensemble_rnn_learning_rate = FLAGS_ensemble_rnn_learning_rate;
double   k_ensemble_krls_learning_rate = FLAGS_ensemble_krls_learning_rate;
double   k_ensemble_outlier_threshold = FLAGS_ensemble_outlier_threshold;
double   k_ensemble_krls_gamma = FLAGS_ensemble_krls_gamma;
uint32_t k_ensemble_rnn_epoch_count = FLAGS_ensemble_rnn_epoch_count;
uint32_t k_ensemble_rnn_sequence_size = FLAGS_ensemble_rnn_sequence_size;
uint32_t k_ensemble_rnn_layer_count = FLAGS_ensemble_rnn_layer_count;
bool     k_ensemble_apply_scheduled_outlier_model =
    FLAGS_ensemble_apply_scheduled_outlier_model;

// YCSB configs
int64_t k_ycsb_max_key = FLAGS_ycsb_max_key;
int32_t k_ycsb_value_size = FLAGS_ycsb_value_size;
int32_t k_ycsb_partition_size = FLAGS_ycsb_partition_size;
int32_t k_ycsb_column_size = FLAGS_ycsb_column_size;
int32_t k_ycsb_num_ops_per_transaction = FLAGS_ycsb_num_ops_per_transaction;
double  k_ycsb_zipf_alpha = FLAGS_ycsb_zipf_alpha;
double  k_ycsb_scan_selectivity = FLAGS_ycsb_scan_selectivity;
int32_t k_ycsb_column_partition_size = FLAGS_ycsb_column_partition_size;
partition_type::type k_ycsb_partition_type = partition_type::type::ROW;
storage_tier_type::type k_ycsb_storage_type = storage_tier_type::type::MEMORY;
bool                    k_ycsb_scan_restricted = FLAGS_ycsb_scan_restricted;
bool                    k_ycsb_rmw_restricted = FLAGS_ycsb_rmw_restricted;
bool k_ycsb_store_scan_results = FLAGS_ycsb_store_scan_results;

int32_t k_ycsb_write_prob = FLAGS_ycsb_write_prob;
int32_t k_ycsb_read_prob = FLAGS_ycsb_read_prob;
int32_t k_ycsb_rmw_prob = FLAGS_ycsb_rmw_prob;
int32_t k_ycsb_scan_prob = FLAGS_ycsb_scan_prob;
int32_t k_ycsb_multi_rmw_prob = FLAGS_ycsb_multi_rmw_prob;

int32_t k_db_split_prob = FLAGS_db_split_prob;
int32_t k_db_merge_prob = FLAGS_db_merge_prob;
int32_t k_db_remaster_prob = FLAGS_db_remaster_prob;

// TPCC configs
uint32_t k_tpcc_num_warehouses = FLAGS_tpcc_num_warehouses;
uint32_t k_tpcc_num_items = FLAGS_tpcc_num_items;
uint32_t k_tpcc_num_suppliers = FLAGS_tpcc_num_suppliers;
uint32_t k_tpcc_expected_num_orders_per_cust =
    FLAGS_tpcc_expected_num_orders_per_cust;
uint32_t k_tpcc_partition_size = FLAGS_tpcc_partition_size;
uint32_t k_tpcc_item_partition_size = FLAGS_tpcc_item_partition_size;
uint32_t k_tpcc_district_partition_size = FLAGS_tpcc_district_partition_size;
uint32_t k_tpcc_customer_partition_size = FLAGS_tpcc_customer_partition_size;
uint32_t k_tpcc_supplier_partition_size = FLAGS_tpcc_supplier_partition_size;
uint32_t k_tpcc_new_order_within_warehouse_likelihood =
    FLAGS_tpcc_new_order_within_warehouse_likelihood;
uint32_t k_tpcc_payment_within_warehouse_likelihood =
    FLAGS_tpcc_payment_within_warehouse_likelihood;
bool k_tpcc_track_and_use_recent_items = FLAGS_tpcc_track_and_use_recent_items;

uint32_t k_tpcc_num_clients = FLAGS_tpcc_num_clients;
uint32_t k_tpch_num_clients = FLAGS_tpch_num_clients;

// tpcc workload likelihoods
uint32_t k_tpcc_delivery_prob = FLAGS_tpcc_delivery_prob;
uint32_t k_tpcc_new_order_prob = FLAGS_tpcc_new_order_prob;
uint32_t k_tpcc_order_status_prob = FLAGS_tpcc_order_status_prob;
uint32_t k_tpcc_payment_prob = FLAGS_tpcc_payment_prob;
uint32_t k_tpcc_stock_level_prob = FLAGS_tpcc_stock_level_prob;

uint32_t k_tpcc_customer_column_partition_size =
    FLAGS_tpcc_customer_column_partition_size;
partition_type::type    k_tpcc_customer_part_type = partition_type::type::ROW;
storage_tier_type::type k_tpcc_customer_storage_type =
    storage_tier_type::type::MEMORY;
uint32_t k_tpcc_customer_district_column_partition_size =
    FLAGS_tpcc_customer_district_column_partition_size;
partition_type::type k_tpcc_customer_district_part_type =
    partition_type::type::ROW;
storage_tier_type::type k_tpcc_customer_district_storage_type =
    storage_tier_type::type::MEMORY;
uint32_t k_tpcc_district_column_partition_size =
    FLAGS_tpcc_district_column_partition_size;
partition_type::type    k_tpcc_district_part_type = partition_type::type::ROW;
storage_tier_type::type k_tpcc_district_storage_type =
    storage_tier_type::type::MEMORY;
uint32_t k_tpcc_history_column_partition_size =
    FLAGS_tpcc_history_column_partition_size;
partition_type::type    k_tpcc_history_part_type = partition_type::type::ROW;
storage_tier_type::type k_tpcc_history_storage_type =
    storage_tier_type::type::MEMORY;
uint32_t k_tpcc_item_column_partition_size =
    FLAGS_tpcc_item_column_partition_size;
partition_type::type    k_tpcc_item_part_type = partition_type::type::ROW;
storage_tier_type::type k_tpcc_item_storage_type =
    storage_tier_type::type::MEMORY;
uint32_t k_tpcc_nation_column_partition_size =
    FLAGS_tpcc_nation_column_partition_size;
partition_type::type    k_tpcc_nation_part_type = partition_type::type::ROW;
storage_tier_type::type k_tpcc_nation_storage_type =
    storage_tier_type::type::MEMORY;
uint32_t k_tpcc_new_order_column_partition_size =
    FLAGS_tpcc_new_order_column_partition_size;
partition_type::type    k_tpcc_new_order_part_type = partition_type::type::ROW;
storage_tier_type::type k_tpcc_new_order_storage_type =
    storage_tier_type::type::MEMORY;
uint32_t k_tpcc_order_column_partition_size =
    FLAGS_tpcc_order_column_partition_size;
partition_type::type    k_tpcc_order_part_type = partition_type::type::ROW;
storage_tier_type::type k_tpcc_order_storage_type =
    storage_tier_type::type::MEMORY;
uint32_t k_tpcc_order_line_column_partition_size =
    FLAGS_tpcc_order_line_column_partition_size;
partition_type::type    k_tpcc_order_line_part_type = partition_type::type::ROW;
storage_tier_type::type k_tpcc_order_line_storage_type =
    storage_tier_type::type::MEMORY;
uint32_t k_tpcc_region_column_partition_size =
    FLAGS_tpcc_region_column_partition_size;
partition_type::type    k_tpcc_region_part_type = partition_type::type::ROW;
storage_tier_type::type k_tpcc_region_storage_type =
    storage_tier_type::type::MEMORY;
uint32_t k_tpcc_stock_column_partition_size =
    FLAGS_tpcc_stock_column_partition_size;
partition_type::type    k_tpcc_stock_part_type = partition_type::type::ROW;
storage_tier_type::type k_tpcc_stock_storage_type =
    storage_tier_type::type::MEMORY;
uint32_t k_tpcc_supplier_column_partition_size =
    FLAGS_tpcc_supplier_column_partition_size;
partition_type::type    k_tpcc_supplier_part_type = partition_type::type::ROW;
storage_tier_type::type k_tpcc_supplier_storage_type =
    storage_tier_type::type::MEMORY;
uint32_t k_tpcc_warehouse_column_partition_size =
    FLAGS_tpcc_warehouse_column_partition_size;
partition_type::type    k_tpcc_warehouse_part_type = partition_type::type::ROW;
storage_tier_type::type k_tpcc_warehouse_storage_type =
    storage_tier_type::type::MEMORY;

// tpch workload likelihoods
uint32_t k_tpch_q1_prob = FLAGS_tpch_q1_prob;
uint32_t k_tpch_q2_prob = FLAGS_tpch_q2_prob;
uint32_t k_tpch_q3_prob = FLAGS_tpch_q3_prob;
uint32_t k_tpch_q4_prob = FLAGS_tpch_q4_prob;
uint32_t k_tpch_q5_prob = FLAGS_tpch_q5_prob;
uint32_t k_tpch_q6_prob = FLAGS_tpch_q6_prob;
uint32_t k_tpch_q7_prob = FLAGS_tpch_q7_prob;
uint32_t k_tpch_q8_prob = FLAGS_tpch_q8_prob;
uint32_t k_tpch_q9_prob = FLAGS_tpch_q9_prob;
uint32_t k_tpch_q10_prob = FLAGS_tpch_q10_prob;
uint32_t k_tpch_q11_prob = FLAGS_tpch_q11_prob;
uint32_t k_tpch_q12_prob = FLAGS_tpch_q12_prob;
uint32_t k_tpch_q13_prob = FLAGS_tpch_q13_prob;
uint32_t k_tpch_q14_prob = FLAGS_tpch_q14_prob;
uint32_t k_tpch_q15_prob = FLAGS_tpch_q15_prob;
uint32_t k_tpch_q16_prob = FLAGS_tpch_q16_prob;
uint32_t k_tpch_q17_prob = FLAGS_tpch_q17_prob;
uint32_t k_tpch_q18_prob = FLAGS_tpch_q18_prob;
uint32_t k_tpch_q19_prob = FLAGS_tpch_q19_prob;
uint32_t k_tpch_q20_prob = FLAGS_tpch_q20_prob;
uint32_t k_tpch_q21_prob = FLAGS_tpch_q21_prob;
uint32_t k_tpch_q22_prob = FLAGS_tpch_q22_prob;
uint32_t k_tpch_all_prob = FLAGS_tpch_all_prob;

// tpcc spec
uint32_t k_tpcc_spec_num_districts_per_warehouse =
    FLAGS_tpcc_spec_num_districts_per_warehouse;
uint32_t k_tpcc_spec_num_customers_per_district =
    FLAGS_tpcc_spec_num_customers_per_district;
uint32_t k_tpcc_spec_max_num_order_lines_per_order =
    FLAGS_tpcc_spec_max_num_order_lines_per_order;
uint32_t k_tpcc_spec_initial_num_customers_per_district =
    FLAGS_tpcc_spec_initial_num_customers_per_district;

bool k_tpcc_should_fully_replicate_warehouses =
    FLAGS_tpcc_should_fully_replicate_warehouses;
bool k_tpcc_should_fully_replicate_districts =
    FLAGS_tpcc_should_fully_replicate_districts;

bool k_tpcc_use_warehouse = FLAGS_tpcc_use_warehouse;
bool k_tpcc_use_district = FLAGS_tpcc_use_district;
bool k_tpcc_h_scan_full_tables = FLAGS_tpcc_h_scan_full_tables;

// smallbank configs
uint64_t k_smallbank_num_accounts = FLAGS_smallbank_num_accounts;
bool     k_smallbank_hotspot_use_fixed_size =
    FLAGS_smallbank_hotspot_use_fixed_size;
double   k_smallbank_hotspot_percentage = FLAGS_smallbank_hotspot_percentage;
uint32_t k_smallbank_hotspot_fixed_size = FLAGS_smallbank_hotspot_fixed_size;
uint32_t k_smallbank_partition_size = FLAGS_smallbank_partition_size;
uint32_t k_smallbank_customer_account_partition_spread =
    FLAGS_smallbank_customer_account_partition_spread;

uint32_t k_smallbank_accounts_col_size = FLAGS_smallbank_accounts_col_size;
partition_type::type k_smallbank_accounts_partition_data_type =
    partition_type::type::ROW;
storage_tier_type::type k_smallbank_accounts_storage_data_type =
    storage_tier_type::type::MEMORY;
uint32_t k_smallbank_banking_col_size = FLAGS_smallbank_banking_col_size;
partition_type::type k_smallbank_banking_partition_data_type =
    partition_type::type::ROW;
storage_tier_type::type k_smallbank_banking_storage_data_type =
    storage_tier_type::type::MEMORY;

// smallbank probabilities
uint32_t k_smallbank_amalgamate_prob = FLAGS_smallbank_amalgamate_prob;
uint32_t k_smallbank_balance_prob = FLAGS_smallbank_balance_prob;
uint32_t k_smallbank_deposit_checking_prob =
    FLAGS_smallbank_deposit_checking_prob;
uint32_t k_smallbank_send_payment_prob = FLAGS_smallbank_send_payment_prob;
uint32_t k_smallbank_transact_savings_prob =
    FLAGS_smallbank_transact_savings_prob;
uint32_t k_smallbank_write_check_prob = FLAGS_smallbank_write_check_prob;

uint64_t k_twitter_num_users = FLAGS_twitter_num_users;
uint64_t k_twitter_num_tweets = FLAGS_twitter_num_tweets;
double   k_twitter_tweet_skew = FLAGS_twitter_tweet_skew;
double   k_twitter_follow_skew = FLAGS_twitter_follow_skew;
uint32_t k_twitter_account_partition_size =
    FLAGS_twitter_account_partition_size;
uint32_t k_twitter_max_follow_per_user = FLAGS_twitter_max_follow_per_user;
uint32_t k_twitter_max_tweets_per_user = FLAGS_twitter_max_tweets_per_user;
uint32_t k_twitter_limit_tweets = FLAGS_twitter_limit_tweets;
uint32_t k_twitter_limit_tweets_for_uid = FLAGS_twitter_limit_tweets_for_uid;
uint32_t k_twitter_limit_followers = FLAGS_twitter_limit_followers;

uint32_t k_twitter_followers_column_partition_size =
    FLAGS_twitter_followers_column_partition_size;
partition_type::type k_twitter_followers_part_type =
    partition_type::type::ROW;
storage_tier_type::type k_twitter_followers_storage_type =
    storage_tier_type::type::MEMORY;
uint32_t k_twitter_follows_column_partition_size =
    FLAGS_twitter_follows_column_partition_size;
partition_type::type k_twitter_follows_part_type =
    partition_type::type::ROW;
storage_tier_type::type k_twitter_follows_storage_type =
    storage_tier_type::type::MEMORY;
uint32_t k_twitter_tweets_column_partition_size =
    FLAGS_twitter_tweets_column_partition_size;
partition_type::type k_twitter_tweets_part_type =
    partition_type::type::ROW;
storage_tier_type::type k_twitter_tweets_storage_type =
    storage_tier_type::type::MEMORY;
uint32_t k_twitter_user_profile_column_partition_size =
    FLAGS_twitter_user_profile_column_partition_size;
partition_type::type k_twitter_user_profile_part_type =
    partition_type::type::ROW;
storage_tier_type::type k_twitter_user_profile_storage_type =
    storage_tier_type::type::MEMORY;

bool k_twitter_should_fully_replicate_users =
    FLAGS_twitter_should_fully_replicate_users;
bool k_twitter_should_fully_replicate_follows =
    FLAGS_twitter_should_fully_replicate_follows;
bool k_twitter_should_fully_replicate_tweets =
    FLAGS_twitter_should_fully_replicate_tweets;

// probabilities
uint32_t k_twitter_get_tweet_prob = FLAGS_twitter_get_tweet_prob;
uint32_t k_twitter_get_tweets_from_following_prob =
    FLAGS_twitter_get_tweets_from_following_prob;
uint32_t k_twitter_get_followers_prob = FLAGS_twitter_get_followers_prob;
uint32_t k_twitter_get_user_tweets_prob = FLAGS_twitter_get_user_tweets_prob;
uint32_t k_twitter_insert_tweet_prob = FLAGS_twitter_insert_tweet_prob;
uint32_t k_twitter_get_recent_tweets_prob =
    FLAGS_twitter_get_recent_tweets_prob;
uint32_t k_twitter_get_tweets_from_followers_prob =
    FLAGS_twitter_get_tweets_from_followers_prob;
uint32_t k_twitter_get_tweets_like_prob = FLAGS_twitter_get_tweets_like_prob;
uint32_t k_twitter_update_followers_prob = FLAGS_twitter_update_followers_prob;

// Counter lock backoffs
uint64_t k_expected_tps = FLAGS_expected_tps;
uint64_t k_max_spin_time_us = FLAGS_max_spin_time_us;

uint32_t k_gc_sleep_time = FLAGS_gc_sleep_time;
double   k_gc_memory_threshold = FLAGS_gc_memory_threshold;
int32_t  k_gc_thread_priority = FLAGS_gc_thread_priority;
int32_t  k_always_gc_on_writes = FLAGS_always_gc_on_writes;

double k_memory_tier_site_space_limit = FLAGS_memory_tier_site_space_limit;
double k_disk_tier_site_space_limit = FLAGS_disk_tier_site_space_limit;

// Site selector
uint64_t k_ss_bucket_size = FLAGS_ss_bucket_size;

uint64_t k_astream_max_queue_size = FLAGS_astream_max_queue_size;
int32_t  k_astream_empty_wait_ms = FLAGS_astream_empty_wait_ms;
int32_t  k_astream_mod = FLAGS_astream_mod;
int32_t  k_astream_tracking_interval_ms = FLAGS_astream_tracking_interval_ms;

double k_ss_max_write_blend_rate = FLAGS_ss_max_write_blend_rate;

bool     k_ss_run_background_workers = FLAGS_ss_run_background_workers;
uint32_t k_ss_num_background_work_items_to_execute =
    FLAGS_ss_num_background_work_items_to_execute;

uint32_t k_estore_clay_periodic_interval_ms =
    FLAGS_estore_clay_periodic_interval_ms;
double k_estore_load_epsilon_threshold = FLAGS_estore_load_epsilon_threshold;
double k_estore_hot_partition_pct_thresholds =
    FLAGS_estore_hot_partition_pct_thresholds;
double k_adr_multiplier = FLAGS_adr_multiplier;

bool k_ss_enable_plan_reuse = FLAGS_ss_enable_plan_reuse;
double k_ss_plan_reuse_threshold = FLAGS_ss_plan_reuse_threshold;
bool   k_ss_enable_wait_for_plan_reuse = FLAGS_ss_enable_wait_for_plan_reuse;

partition_lock_mode k_ss_optimistic_lock_mode = partition_lock_mode::no_lock;
partition_lock_mode k_ss_physical_adjustment_lock_mode =
    partition_lock_mode::try_lock;
partition_lock_mode k_ss_force_change_lock_mode = partition_lock_mode::try_lock;

uint32_t k_num_site_selector_worker_threads_per_client =
    FLAGS_num_site_selector_worker_threads_per_client;

ss_strategy_type  k_ss_strategy_type = ss_strategy_type::HEURISTIC;
ss_mastering_type k_ss_mastering_type = ss_mastering_type::ADAPT;

double k_ss_skew_weight = FLAGS_ss_skew_weight;
double k_ss_skew_exp_constant = FLAGS_ss_skew_exp_constant;
double k_ss_svv_delay_weight = FLAGS_ss_svv_delay_weight;
double k_ss_single_site_within_transactions_weight =
    FLAGS_ss_single_site_within_transactions_weight;
double k_ss_single_site_across_transactions_weight =
    FLAGS_ss_single_site_across_transactions_weight;
double k_ss_wait_time_normalization_constant =
    FLAGS_ss_wait_time_normalization_constant;
double  k_ss_split_partition_ratio = FLAGS_ss_split_partition_ratio;
double  k_ss_merge_partition_ratio = FLAGS_ss_merge_partition_ratio;
int32_t k_ss_num_sampled_transactions_for_client =
    FLAGS_ss_num_sampled_transactions_for_client;
int32_t k_ss_num_sampled_transactions_from_other_clients =
    FLAGS_ss_num_sampled_transactions_from_other_clients;
double k_ss_upfront_cost_weight = FLAGS_ss_upfront_cost_weight;
double k_ss_scan_upfront_cost_weight = FLAGS_ss_scan_upfront_cost_weight;
double k_ss_horizon_weight = FLAGS_ss_horizon_weight;
double k_ss_default_horizon_benefit_weight =
    FLAGS_ss_default_horizon_benefit_weight;
double k_ss_repartition_remaster_horizon_weight =
    FLAGS_ss_repartition_remaster_horizon_weight;
double k_ss_horizon_storage_weight = FLAGS_ss_horizon_storage_weight;
double k_ss_repartition_prob_rescale = FLAGS_ss_repartition_prob_rescale;
double k_ss_outlier_repartition_std_dev = FLAGS_ss_outlier_repartition_std_dev;
double k_ss_split_partition_size_ratio = FLAGS_ss_split_partition_size_ratio;
double k_ss_plan_site_load_balance_weight =
    FLAGS_ss_plan_site_load_balance_weight;

double k_ss_plan_site_storage_balance_weight =
    FLAGS_ss_plan_site_storage_balance_weight;
double k_ss_plan_site_storage_overlimit_weight =
    FLAGS_ss_plan_site_storage_overlimit_weight;
double k_ss_plan_site_storage_variance_weight =
    FLAGS_ss_plan_site_storage_variance_weight;
double k_ss_plan_site_memory_overlimit_weight =
    FLAGS_ss_plan_site_memory_overlimit_weight;
double k_ss_plan_site_disk_overlimit_weight =
    FLAGS_ss_plan_site_disk_overlimit_weight;
double k_ss_plan_site_memory_variance_weight =
    FLAGS_ss_plan_site_memory_variance_weight;
double k_ss_plan_site_disk_variance_weight =
    FLAGS_ss_plan_site_disk_variance_weight;

bool k_ss_allow_force_site_selector_changes =
    FLAGS_ss_allow_force_site_selector_changes;

bool   k_ss_plan_scan_in_cost_order = FLAGS_ss_plan_scan_in_cost_order;
bool   k_ss_plan_scan_limit_changes = FLAGS_ss_plan_scan_limit_changes;
double k_ss_plan_scan_limit_ratio = FLAGS_ss_plan_scan_limit_ratio;

std::vector<partition_type::type> k_ss_acceptable_new_partition_types = {
    partition_type::type::ROW};
std::vector<partition_type::type> k_ss_acceptable_partition_types = {
    partition_type::type::ROW};
std::vector<storage_tier_type::type> k_ss_acceptable_new_storage_types = {
    storage_tier_type::type::MEMORY};
std::vector<storage_tier_type::type> k_ss_acceptable_storage_types = {
    storage_tier_type::type::MEMORY};

bool k_use_query_arrival_predictor = FLAGS_use_query_arrival_predictor;
query_arrival_predictor_type k_query_arrival_predictor_type =
    query_arrival_predictor_type::SIMPLE_QUERY_PREDICTOR;
double k_query_arrival_access_threshold = FLAGS_query_arrival_access_threshold;
uint64_t k_query_arrival_time_bucket = FLAGS_query_arrival_time_bucket;
double   k_query_arrival_count_bucket = FLAGS_query_arrival_count_bucket;
double   k_query_arrival_time_score_scalar =
    FLAGS_query_arrival_time_score_scalar;
double k_query_arrival_default_score = FLAGS_query_arrival_default_score;

bool   k_cost_model_is_static_model = FLAGS_cost_model_is_static_model;
double k_cost_model_learning_rate = FLAGS_cost_model_learning_rate;
double k_cost_model_regularization = FLAGS_cost_model_regularization;
double k_cost_model_bias_regularization = FLAGS_cost_model_bias_regularization;

double k_cost_model_momentum = FLAGS_cost_model_momentum;
double k_cost_model_kernel_gamma = FLAGS_cost_model_kernel_gamma;

uint32_t k_cost_model_max_internal_model_size_scalar =
    FLAGS_cost_model_max_internal_model_size_scalar;
uint32_t k_cost_model_layer_1_nodes_scalar =
    FLAGS_cost_model_layer_1_nodes_scalar;
uint32_t k_cost_model_layer_2_nodes_scalar =
    FLAGS_cost_model_layer_2_nodes_scalar;

double k_cost_model_num_reads_weight = FLAGS_cost_model_num_reads_weight;
double k_cost_model_num_reads_normalization =
    FLAGS_cost_model_num_reads_normalization;
double k_cost_model_num_reads_max_input = FLAGS_cost_model_num_reads_max_input;
double k_cost_model_read_bias = FLAGS_cost_model_read_bias;
predictor_type k_cost_model_read_predictor_type =
    predictor_type::MULTI_LINEAR_PREDICTOR;

double k_cost_model_num_reads_disk_weight =
    FLAGS_cost_model_num_reads_disk_weight;
double k_cost_model_read_disk_bias = FLAGS_cost_model_read_disk_bias;
predictor_type k_cost_model_read_disk_predictor_type =
    predictor_type::MULTI_LINEAR_PREDICTOR;

double k_cost_model_scan_num_rows_weight =
    FLAGS_cost_model_scan_num_rows_weight;
double k_cost_model_scan_num_rows_normalization =
    FLAGS_cost_model_scan_num_rows_normalization;
double k_cost_model_scan_num_rows_max_input =
    FLAGS_cost_model_scan_num_rows_max_input;
double k_cost_model_scan_selectivity_weight =
    FLAGS_cost_model_scan_selectivity_weight;
double k_cost_model_scan_selectivity_normalization =
    FLAGS_cost_model_scan_selectivity_normalization;
double k_cost_model_scan_selectivity_max_input =
    FLAGS_cost_model_scan_selectivity_max_input;
double         k_cost_model_scan_bias = FLAGS_cost_model_scan_bias;
predictor_type k_cost_model_scan_predictor_type =
    predictor_type::MULTI_LINEAR_PREDICTOR;

double k_cost_model_scan_disk_num_rows_weight =
    FLAGS_cost_model_scan_disk_num_rows_weight;
double k_cost_model_scan_disk_selectivity_weight =
    FLAGS_cost_model_scan_disk_selectivity_weight;
double         k_cost_model_scan_disk_bias = FLAGS_cost_model_scan_disk_bias;
predictor_type k_cost_model_scan_disk_predictor_type =
    predictor_type::MULTI_LINEAR_PREDICTOR;

double k_cost_model_num_writes_weight = FLAGS_cost_model_num_writes_weight;
double k_cost_model_num_writes_normalization =
    FLAGS_cost_model_num_writes_normalization;
double k_cost_model_num_writes_max_input =
    FLAGS_cost_model_num_writes_max_input;
double         k_cost_model_write_bias = FLAGS_cost_model_write_bias;
predictor_type k_cost_model_write_predictor_type =
    predictor_type::MULTI_LINEAR_PREDICTOR;

double k_cost_model_lock_weight = FLAGS_cost_model_lock_weight;
double k_cost_model_lock_normalization = FLAGS_cost_model_lock_normalization;
double k_cost_model_lock_max_input = FLAGS_cost_model_lock_max_input;
double k_cost_model_lock_bias = FLAGS_cost_model_lock_bias;
predictor_type k_cost_model_lock_predictor_type =
    predictor_type::MULTI_LINEAR_PREDICTOR;

double k_cost_model_commit_serialize_weight =
    FLAGS_cost_model_commit_serialize_weight;

double k_cost_model_commit_serialize_bias =
    FLAGS_cost_model_commit_serialize_bias;
double k_cost_model_commit_serialize_normalization =
    FLAGS_cost_model_commit_serialize_normalization;
double k_cost_model_commit_serialize_max_input =
    FLAGS_cost_model_commit_serialize_max_input;
predictor_type k_cost_model_commit_serialize_predictor_type =
    predictor_type::MULTI_LINEAR_PREDICTOR;

double k_cost_model_commit_build_snapshot_weight =
    FLAGS_cost_model_commit_build_snapshot_weight;
double k_cost_model_commit_build_snapshot_normalization =
    FLAGS_cost_model_commit_build_snapshot_normalization;
double k_cost_model_commit_build_snapshot_max_input =
    FLAGS_cost_model_commit_build_snapshot_max_input;
double k_cost_model_commit_build_snapshot_bias =
    FLAGS_cost_model_commit_build_snapshot_bias;
predictor_type k_cost_model_commit_build_snapshot_predictor_type =
    predictor_type::MULTI_LINEAR_PREDICTOR;

double k_cost_model_wait_for_service_weight =
    FLAGS_cost_model_wait_for_service_weight;
double k_cost_model_wait_for_service_normalization =
    FLAGS_cost_model_wait_for_service_normalization;
double k_cost_model_wait_for_service_max_input =
    FLAGS_cost_model_wait_for_service_max_input;
double k_cost_model_wait_for_service_bias =
    FLAGS_cost_model_wait_for_service_bias;
predictor_type k_cost_model_wait_for_service_predictor_type =
    predictor_type::MULTI_LINEAR_PREDICTOR;

double k_cost_model_wait_for_session_version_weight =
    FLAGS_cost_model_wait_for_session_version_weight;
double k_cost_model_wait_for_session_version_normalization =
    FLAGS_cost_model_wait_for_session_version_normalization;
double k_cost_model_wait_for_session_version_max_input =
    FLAGS_cost_model_wait_for_session_version_max_input;
double k_cost_model_wait_for_session_version_bias =
    FLAGS_cost_model_wait_for_session_version_bias;
predictor_type k_cost_model_wait_for_session_version_predictor_type =
    predictor_type::MULTI_LINEAR_PREDICTOR;

double k_cost_model_wait_for_session_snapshot_weight =
    FLAGS_cost_model_wait_for_session_snapshot_weight;
double k_cost_model_wait_for_session_snapshot_normalization =
    FLAGS_cost_model_wait_for_session_snapshot_normalization;
double k_cost_model_wait_for_session_snapshot_max_input =
    FLAGS_cost_model_wait_for_session_snapshot_max_input;
double k_cost_model_wait_for_session_snapshot_bias =
    FLAGS_cost_model_wait_for_session_snapshot_bias;
predictor_type k_cost_model_wait_for_session_snapshot_predictor_type =
    predictor_type::MULTI_LINEAR_PREDICTOR;

double k_cost_model_site_load_prediction_write_weight =
    FLAGS_cost_model_site_load_prediction_write_weight;
double k_cost_model_site_load_prediction_write_normalization =
    FLAGS_cost_model_site_load_prediction_write_normalization;
double k_cost_model_site_load_prediction_write_max_input =
    FLAGS_cost_model_site_load_prediction_write_max_input;
double k_cost_model_site_load_prediction_read_weight =
    FLAGS_cost_model_site_load_prediction_read_weight;
double k_cost_model_site_load_prediction_read_normalization =
    FLAGS_cost_model_site_load_prediction_read_normalization;
double k_cost_model_site_load_prediction_read_max_input =
    FLAGS_cost_model_site_load_prediction_read_max_input;
double k_cost_model_site_load_prediction_update_weight =
    FLAGS_cost_model_site_load_prediction_update_weight;
double k_cost_model_site_load_prediction_update_normalization =
    FLAGS_cost_model_site_load_prediction_update_normalization;
double k_cost_model_site_load_prediction_update_max_input =
    FLAGS_cost_model_site_load_prediction_update_max_input;
double k_cost_model_site_load_prediction_bias =
    FLAGS_cost_model_site_load_prediction_bias;
double k_cost_model_site_load_prediction_max_scale =
    FLAGS_cost_model_site_load_prediction_max_scale;
predictor_type k_cost_model_site_load_predictor_type =
    predictor_type::MULTI_LINEAR_PREDICTOR;

double k_cost_model_site_operation_count_weight =
    FLAGS_cost_model_site_operation_count_weight;
double k_cost_model_site_operation_count_normalization =
    FLAGS_cost_model_site_operation_count_normalization;
double k_cost_model_site_operation_count_max_input =
    FLAGS_cost_model_site_operation_count_max_input;
double k_cost_model_site_operation_count_max_scale =
    FLAGS_cost_model_site_operation_count_max_scale;
double k_cost_model_site_operation_count_bias =
    FLAGS_cost_model_site_operation_count_bias;
predictor_type k_cost_model_site_operation_count_predictor_type =
    predictor_type::MULTI_LINEAR_PREDICTOR;

double k_cost_model_distributed_scan_max_weight =
    FLAGS_cost_model_distributed_scan_max_weight;
double k_cost_model_distributed_scan_min_weight =
    FLAGS_cost_model_distributed_scan_min_weight;
double k_cost_model_distributed_scan_normalization =
    FLAGS_cost_model_distributed_scan_normalization;
double k_cost_model_distributed_scan_max_input =
    FLAGS_cost_model_distributed_scan_max_input;
double k_cost_model_distributed_scan_bias =
    FLAGS_cost_model_distributed_scan_bias;
predictor_type k_cost_model_distributed_scan_predictor_type =
    predictor_type::MULTI_LINEAR_PREDICTOR;

double k_cost_model_memory_num_rows_normalization =
    FLAGS_cost_model_memory_num_rows_normalization;
double k_cost_model_memory_num_rows_max_input =
    FLAGS_cost_model_memory_num_rows_max_input;

double k_cost_model_memory_allocation_num_rows_weight =
    FLAGS_cost_model_memory_allocation_num_rows_weight;
double k_cost_model_memory_allocation_bias =
    FLAGS_cost_model_memory_allocation_bias;
predictor_type k_cost_model_memory_allocation_predictor_type =
    predictor_type::MULTI_LINEAR_PREDICTOR;

double k_cost_model_memory_deallocation_num_rows_weight =
    FLAGS_cost_model_memory_deallocation_num_rows_weight;
double k_cost_model_memory_deallocation_bias =
    FLAGS_cost_model_memory_deallocation_bias;
predictor_type k_cost_model_memory_deallocation_predictor_type =
    predictor_type::MULTI_LINEAR_PREDICTOR;

double k_cost_model_memory_assignment_num_rows_weight =
    FLAGS_cost_model_memory_assignment_num_rows_weight;
double k_cost_model_memory_assignment_bias =
    FLAGS_cost_model_memory_assignment_bias;
predictor_type k_cost_model_memory_assignment_predictor_type =
    predictor_type::MULTI_LINEAR_PREDICTOR;

double k_cost_model_disk_num_rows_normalization =
    FLAGS_cost_model_disk_num_rows_normalization;
double k_cost_model_disk_num_rows_max_input =
    FLAGS_cost_model_disk_num_rows_max_input;

double k_cost_model_evict_to_disk_num_rows_weight =
    FLAGS_cost_model_evict_to_disk_num_rows_weight;
double k_cost_model_evict_to_disk_bias = FLAGS_cost_model_evict_to_disk_bias;
predictor_type k_cost_model_evict_to_disk_predictor_type =
    predictor_type::MULTI_LINEAR_PREDICTOR;

double k_cost_model_pull_from_disk_num_rows_weight =
    FLAGS_cost_model_pull_from_disk_num_rows_weight;
double k_cost_model_pull_from_disk_bias = FLAGS_cost_model_pull_from_disk_bias;
predictor_type k_cost_model_pull_from_disk_predictor_type =
    predictor_type::MULTI_LINEAR_PREDICTOR;

double k_cost_model_widths_weight = FLAGS_cost_model_widths_weight;
double k_cost_model_widths_normalization =
    FLAGS_cost_model_widths_normalization;
double k_cost_model_widths_max_input = FLAGS_cost_model_widths_max_input;

double k_cost_model_wait_for_session_remaster_default_pct =
    FLAGS_cost_model_wait_for_session_remaster_default_pct;

bool   k_ss_enum_enable_track_stats = FLAGS_ss_enum_enable_track_stats;
double k_ss_enum_prob_of_returning_default =
    FLAGS_ss_enum_prob_of_returning_default;
double   k_ss_enum_min_threshold = FLAGS_ss_enum_min_threshold;
uint64_t k_ss_enum_min_count_threshold = FLAGS_ss_enum_min_count_threshold;
double   k_ss_enum_contention_bucket = FLAGS_ss_enum_contention_bucket;
double   k_ss_enum_cell_width_bucket = FLAGS_ss_enum_cell_width_bucket;
uint32_t k_ss_enum_num_entries_bucket = FLAGS_ss_enum_num_entries_bucket;
double   k_ss_enum_num_updates_needed_bucket =
    FLAGS_ss_enum_num_updates_needed_bucket;
double k_ss_enum_scan_selectivity_bucket =
    FLAGS_ss_enum_scan_selectivity_bucket;
uint32_t k_ss_enum_num_point_reads_bucket =
    FLAGS_ss_enum_num_point_reads_bucket;
uint32_t k_ss_enum_num_point_updates_bucket =
    FLAGS_ss_enum_num_point_updates_bucket;

uint32_t k_predictor_max_internal_model_size =
    FLAGS_predictor_max_internal_model_size;

int32_t k_ss_update_stats_sleep_delay_ms = FLAGS_ss_update_stats_sleep_delay_ms;
int32_t k_ss_num_poller_threads = FLAGS_ss_num_poller_threads;
int32_t k_ss_colocation_keys_sampled = FLAGS_ss_colocation_keys_sampled;
double  k_ss_partition_coverage_threshold =
    FLAGS_ss_partition_coverage_threshold;

update_destination_type k_update_destination_type =
    update_destination_type::NO_OP_DESTINATION;
update_source_type k_update_source_type = update_source_type::NO_OP_SOURCE;
uint64_t k_kafka_buffer_read_backoff = FLAGS_kafka_buffer_read_backoff;

uint32_t k_num_update_sources = FLAGS_num_update_sources;
uint32_t k_num_update_destinations = FLAGS_num_update_destinations;
uint32_t k_sleep_time_between_update_enqueue_iterations =
    FLAGS_sleep_time_between_update_enqueue_iterations;
uint32_t k_num_updates_before_apply_self = FLAGS_num_updates_before_apply_self;
uint32_t k_num_updates_to_apply_self = FLAGS_num_updates_to_apply_self;

bool k_limit_number_of_records_propagated =
    FLAGS_limit_number_of_records_propagated;
int32_t k_number_of_updates_needed_for_propagation =
    FLAGS_number_of_updates_needed_for_propagation;

uint64_t k_kafka_poll_count = FLAGS_kafka_poll_count;
uint64_t k_kafka_poll_sleep_ms = FLAGS_kafka_poll_sleep_ms;
uint32_t k_kafka_backoff_sleep_ms = FLAGS_kafka_backoff_sleep_ms;
int32_t  k_kafka_poll_timeout = FLAGS_kafka_poll_timeout;
int32_t  k_kafka_flush_wait_ms = FLAGS_kafka_flush_wait_ms;
int32_t  k_kafka_consume_timeout_ms = FLAGS_kafka_consume_timeout_ms;
int32_t  k_kafka_seek_timeout_ms = FLAGS_kafka_seek_timeout_ms;

rewriter_type k_rewriter_type = rewriter_type::HASH_REWRITER;
double k_prob_range_rewriter_threshold = FLAGS_prob_range_rewriter_threshold;

bool    k_load_chunked_tables = FLAGS_load_chunked_tables;
bool    k_persist_chunked_tables = FLAGS_persist_chunked_tables;
int32_t k_chunked_table_size = FLAGS_chunked_table_size;

std::string k_kafka_fetch_max_ms = FLAGS_kafka_fetch_max_ms;
std::string k_kafka_queue_max_ms = FLAGS_kafka_queue_max_ms;
std::string k_kafka_socket_blocking_max_ms = FLAGS_kafka_socket_blocking_max_ms;
std::string k_kafka_consume_max_messages = FLAGS_kafka_consume_max_messages;
std::string k_kafka_produce_max_messages = FLAGS_kafka_produce_max_messages;
std::string k_kafka_required_acks = FLAGS_kafka_required_acks;
std::string k_kafka_send_max_retries = FLAGS_kafka_send_max_retries;
std::string k_kafka_max_buffered_messages = FLAGS_kafka_max_buffered_messages;
std::string k_kafka_max_buffered_messages_size =
    FLAGS_kafka_max_buffered_messages_size;

bool        k_enable_secondary_storage = FLAGS_enable_secondary_storage;
std::string k_secondary_storage_dir = FLAGS_secondary_storage_dir;

int32_t k_sample_reservoir_size_per_client =
    FLAGS_sample_reservoir_size_per_client;
double k_sample_reservoir_initial_multiplier =
    FLAGS_sample_reservoir_initial_multiplier;
double k_sample_reservoir_weight_increment =
    FLAGS_sample_reservoir_weight_increment;
double  k_sample_reservoir_decay_rate = FLAGS_sample_reservoir_decay_rate;
int32_t k_force_change_reservoir_size_per_client =
    FLAGS_force_change_reservoir_size_per_client;
double k_force_change_reservoir_initial_multiplier =
    FLAGS_force_change_reservoir_initial_multiplier;
double k_force_change_reservoir_weight_increment =
    FLAGS_force_change_reservoir_weight_increment;
double k_force_change_reservoir_decay_rate =
    FLAGS_force_change_reservoir_decay_rate;
int32_t k_reservoir_decay_seconds = FLAGS_reservoir_decay_seconds;
double  k_ss_storage_removal_threshold = FLAGS_ss_storage_removal_threshold;
uint32_t k_ss_num_remove_replicas_iteration =
    FLAGS_ss_num_remove_replicas_iteration;

bool k_ss_allow_vertical_partitioning = FLAGS_ss_allow_vertical_partitioning;
bool k_ss_allow_horizontal_partitioning =
    FLAGS_ss_allow_horizontal_partitioning;
bool k_ss_allow_change_partition_types = FLAGS_ss_allow_change_partition_types;
bool k_ss_allow_change_storage_types = FLAGS_ss_allow_change_storage_types;

int32_t k_timer_log_level = FLAGS_timer_log_level;
bool    k_timer_log_level_on = true;
int32_t k_slow_timer_error_log_level = FLAGS_slow_timer_error_log_level;
int32_t k_cost_model_log_level = FLAGS_cost_model_log_level;
int32_t k_heuristic_evaluator_log_level = FLAGS_heuristic_evaluator_log_level;
int32_t k_periodic_cost_model_log_level = FLAGS_periodic_cost_model_log_level;
int32_t k_update_propagation_log_level = FLAGS_update_propagation_log_level;
int32_t k_significant_update_propagation_log_level =
    FLAGS_significant_update_propagation_log_level;
int32_t k_ss_action_executor_log_level = FLAGS_ss_action_executor_log_level;
int32_t k_rpc_handler_log_level = FLAGS_rpc_handler_log_level;

double k_slow_timer_log_time_threshold = FLAGS_slow_timer_log_time_threshold;
double k_cost_model_log_error_threshold = FLAGS_cost_model_log_error_threshold;

std::chrono::system_clock::time_point k_process_start_time =
    std::chrono::system_clock::now();

void init_constants() {
    VLOG( 0 ) << "Initializing constants";

#ifdef NDEBUG
    VLOG( 0 ) << "k_build_type:release";
#else
    VLOG( 0 ) << "k_build_type:debug";
#endif

    VLOG( 0 ) << "git_version: " << GIT_VERSION;

    // https://github.com/google/glog/blob/b2c3afecaacfb70611cff6600d7db145689b1637/src/glog/vlog_is_on.h.in#L103
    VLOG( 0 ) << "k_log_verbosity:" << FLAGS_v;

    VLOG( 0 ) << "Compile time constants";

    VLOG( 0 ) << "k_kafka_out_buffer_size:" << KAFKA_OUT_BUFFER_SIZE;
    VLOG( 0 ) << "k_kafka_in_buffer_size:" << KAFKA_IN_BUFFER_SIZE;
    VLOG( 0 ) << "k_cacheline_size:" << CACHELINE_SIZE;
    VLOG( 0 ) << "k_timer_depth:" << TIMER_DEPTH;

    VLOG( 0 ) << "k_spar_periodic_quantity:" << SPAR_PERIODIC_QUANTITY;
    VLOG( 0 ) << "k_spar_temporal_quantity:" << SPAR_TEMPORAL_QUANTITY;

    k_timer_log_level = FLAGS_timer_log_level;
    VLOG( 0 ) << "k_timer_log_level:" << k_timer_log_level;
    k_timer_log_level_on = k_timer_log_level_on >= FLAGS_v;
    VLOG( 0 ) << "k_timer_log_level_on:" << k_timer_log_level_on;
    k_slow_timer_error_log_level = FLAGS_slow_timer_error_log_level;
    VLOG( 0 ) << "k_slow_timer_error_log_level:"
              << k_slow_timer_error_log_level;
    k_cost_model_log_level = FLAGS_cost_model_log_level;
    VLOG( 0 ) << "k_cost_model_log_level:" << k_cost_model_log_level;
    k_heuristic_evaluator_log_level = FLAGS_heuristic_evaluator_log_level;
    VLOG( 0 ) << "k_heuristic_evaluator_log_level:"
              << k_heuristic_evaluator_log_level;
    k_periodic_cost_model_log_level = FLAGS_periodic_cost_model_log_level;
    VLOG( 0 ) << "k_periodic_cost_model_log_level:"
              << k_periodic_cost_model_log_level;
    k_update_propagation_log_level = FLAGS_update_propagation_log_level;
    VLOG( 0 ) << "k_update_propagation_log_level:"
              << k_update_propagation_log_level;
    k_significant_update_propagation_log_level =
        FLAGS_significant_update_propagation_log_level;
    VLOG( 0 ) << "k_significant_update_propagation_log_level:"
              << k_significant_update_propagation_log_level;
    k_ss_action_executor_log_level = FLAGS_ss_action_executor_log_level;
    VLOG( 0 ) << "k_ss_action_executor_log_level:"
              << k_ss_action_executor_log_level;
    k_rpc_handler_log_level = FLAGS_rpc_handler_log_level;
    VLOG( 0 ) << "k_rpc_handler_log_level:" << k_rpc_handler_log_level;

    k_slow_timer_log_time_threshold = FLAGS_slow_timer_log_time_threshold;
    VLOG( 0 ) << "k_slow_timer_log_time_threshold:"
              << k_slow_timer_log_time_threshold;

    k_cost_model_log_error_threshold = FLAGS_cost_model_log_error_threshold;
    VLOG( 0 ) << "k_cost_model_log_error_threshold:"
              << k_cost_model_log_error_threshold;

    k_enable_timer_dumps = FLAGS_enable_timer_dumps;
    VLOG( 0 ) << "k_enable_timer_dumps:" << k_enable_timer_dumps;

    k_skip_slow_tests = FLAGS_skip_slow_tests;
    VLOG(0) << "k_skip_slow_tests:" << k_skip_slow_tests;

    k_num_sites = FLAGS_num_sites;
    VLOG( 0 ) << "k_num_sites:" << k_num_sites;
    k_site_location_identifier = FLAGS_site_location_identifier;
    VLOG( 0 ) << "k_site_location_identifier:" << k_site_location_identifier;

    k_perf_counter_client_multiplier = FLAGS_perf_counter_client_multiplier;
    VLOG( 0 ) << "k_perf_counter_client_multiplier:"
              << k_perf_counter_client_multiplier;

    VLOG( 0 ) << "DB FLAGS";
    k_spinlock_back_off_duration = FLAGS_spinlock_back_off_duration;
    VLOG( 0 ) << "k_spinlock_back_off_duration:"
              << k_spinlock_back_off_duration;
    k_num_records_in_chain = FLAGS_num_records_in_chain;
    VLOG( 0 ) << "k_num_records_in_chain:" << k_num_records_in_chain;
    DCHECK_GE( k_num_records_in_chain, 2 );
    k_num_records_in_snapshot_chain = FLAGS_num_records_in_snapshot_chain;
    VLOG( 0 ) << "k_num_records_in_snapshot_chain:"
              << k_num_records_in_snapshot_chain;
    DCHECK_GE( k_num_records_in_snapshot_chain, 2 );

    k_num_records_in_multi_entry_table = FLAGS_num_records_in_multi_entry_table;
    VLOG( 0 ) << "k_num_records_in_multi_entry_table:"
              << k_num_records_in_multi_entry_table;

    k_expected_tps = FLAGS_expected_tps;
    VLOG( 0 ) << "k_expected_tps:" << k_expected_tps;

    k_max_spin_time_us = FLAGS_max_spin_time_us;
    VLOG( 0 ) << "k_max_spin_time_us:" << k_max_spin_time_us;

    k_gc_sleep_time = FLAGS_gc_sleep_time;
    VLOG( 0 ) << "k_gc_sleep_time:" << k_gc_sleep_time;
    k_gc_memory_threshold = FLAGS_gc_memory_threshold;
    VLOG( 0 ) << "k_gc_memory_threshold:" << k_gc_memory_threshold;

    k_gc_thread_priority = FLAGS_gc_thread_priority;
    VLOG( 0 ) << "k_gc_thread_priority:" << k_gc_thread_priority;

    k_always_gc_on_writes = FLAGS_always_gc_on_writes;
    VLOG( 0 ) << "k_always_gc_on_writes:" << k_always_gc_on_writes;

    k_memory_tier_site_space_limit = FLAGS_memory_tier_site_space_limit;
    VLOG( 0 ) << "k_memory_tier_site_space_limit:"
              << k_memory_tier_site_space_limit;
    k_disk_tier_site_space_limit = FLAGS_disk_tier_site_space_limit;
    VLOG( 0 ) << "k_disk_tier_site_space_limit:"
              << k_disk_tier_site_space_limit;

    k_lock_type = string_to_lock_type( FLAGS_lock_type );
    VLOG( 0 ) << "k_lock_type:" << lock_type_string( k_lock_type );

    VLOG( 0 ) << "Benchmark FLAGS";
    k_workload_type = string_to_workload_type( FLAGS_workload_type );
    VLOG( 0 ) << "k_workload_type:" << workload_type_string( k_workload_type );

    k_bench_num_clients = FLAGS_bench_num_clients;
    VLOG( 0 ) << "k_bench_num_clients:" << k_bench_num_clients;
    k_bench_time_sec = FLAGS_bench_time_sec;
    VLOG( 0 ) << "k_bench_time_sec:" << k_bench_time_sec;
    k_bench_num_ops_before_timer_check = FLAGS_bench_num_ops_before_timer_check;
    VLOG( 0 ) << "k_bench_num_ops_before_timer_check:"
              << k_bench_num_ops_before_timer_check;

    k_db_abstraction_type =
        string_to_db_abstraction_type( FLAGS_db_abstraction_type );
    VLOG( 0 ) << "k_db_abstraction_type:"
              << db_abstraction_type_string( k_db_abstraction_type );

    VLOG( 0 ) << "Early query prediction keys";
    k_spar_width = FLAGS_spar_width;
    VLOG( 0 ) << "k_spar_width:" << k_spar_width;
    k_spar_slot = FLAGS_spar_slot;
    VLOG( 0 ) << "k_spar_slot:" << k_spar_slot;

    VLOG( 0 ) << "k_spar_rounding_factor_temporal:"
              << k_spar_rounding_factor_temporal;
    VLOG( 0 ) << "k_spar_rounding_factor_periodic:"
              << k_spar_rounding_factor_periodic;
    VLOG( 0 ) << "k_spar_predicted_arrival_window:"
              << k_spar_predicted_arrival_window;
    VLOG( 0 ) << "k_spar_normalization_value:" << k_spar_normalization_value;
    VLOG( 0 ) << "k_spar_gamma:" << k_spar_gamma;
    VLOG( 0 ) << "k_spar_learning_rate:" << k_spar_learning_rate;

    VLOG( 0 ) << "k_spar_init_weight:" << k_spar_init_weight;
    VLOG( 0 ) << "k_spar_init_bias:" << k_spar_init_bias;
    VLOG( 0 ) << "k_spar_regularization:" << k_spar_regularization;
    VLOG( 0 ) << "k_spar_bias_regularization:" << k_spar_bias_regularization;
    VLOG( 0 ) << "k_spar_batch_size:" << k_spar_batch_size;

    VLOG( 0 ) << "Ensemble FLAGS";
    VLOG( 0 ) << "k_ensemble_slot: " << k_ensemble_slot;
    VLOG( 0 ) << "k_ensemble_search_window: " << k_ensemble_search_window;
    VLOG( 0 ) << "k_ensemble_linear_training_interval: "
              << k_ensemble_linear_training_interval;
    VLOG( 0 ) << "k_ensemble_rnn_training_interval: "
              << k_ensemble_rnn_training_interval;
    VLOG( 0 ) << "k_ensemble_krls_training_interval: "
              << k_ensemble_krls_training_interval;
    VLOG( 0 ) << "k_ensemble_rnn_learning_rate: "
              << k_ensemble_rnn_learning_rate;
    VLOG( 0 ) << "k_ensemble_krls_learning_rate: "
              << k_ensemble_krls_learning_rate;
    VLOG( 0 ) << "k_ensemble_outlier_threshold: "
              << k_ensemble_outlier_threshold;
    VLOG( 0 ) << "k_ensemble_krls_gamma: " << k_ensemble_krls_gamma;
    VLOG( 0 ) << "k_ensemble_rnn_epoch_count: " << k_ensemble_rnn_epoch_count;
    VLOG( 0 ) << "k_ensemble_rnn_sequence_size: "
              << k_ensemble_rnn_sequence_size;
    VLOG( 0 ) << "k_ensemble_rnn_layer_count: " << k_ensemble_rnn_layer_count;
    VLOG( 0 ) << "k_ensemble_apply_scheduled_outlier_model: "
              << k_ensemble_apply_scheduled_outlier_model;

    VLOG( 0 ) << "YCSB FLAGS";
    k_ycsb_max_key = FLAGS_ycsb_max_key;
    VLOG( 0 ) << "k_ycsb_max_key:" << k_ycsb_max_key;
    k_ycsb_value_size = FLAGS_ycsb_value_size;
    VLOG( 0 ) << "k_ycsb_value_size:" << k_ycsb_value_size;
    k_ycsb_partition_size = FLAGS_ycsb_partition_size;
    VLOG( 0 ) << "k_ycsb_partition_size:" << k_ycsb_partition_size;
    k_ycsb_column_size = FLAGS_ycsb_column_size;
    VLOG( 0 ) << "k_ycsb_column_size:" << k_ycsb_column_size;
    k_ycsb_num_ops_per_transaction = FLAGS_ycsb_num_ops_per_transaction;
    VLOG( 0 ) << "k_ycsb_num_ops_per_transaction:"
              << k_ycsb_num_ops_per_transaction;
    k_ycsb_zipf_alpha = FLAGS_ycsb_zipf_alpha;
    VLOG( 0 ) << "k_ycsb_zipf_alpha:" << k_ycsb_zipf_alpha;
    k_ycsb_scan_selectivity = FLAGS_ycsb_scan_selectivity;
    VLOG( 0 ) << "k_ycsb_scan_selectivity:" << k_ycsb_scan_selectivity;
    k_ycsb_column_partition_size = FLAGS_ycsb_column_partition_size;
    VLOG( 0 ) << "k_ycsb_column_partition_size:"
              << k_ycsb_column_partition_size;
    k_ycsb_partition_type =
        string_to_partition_type( FLAGS_ycsb_partition_type );
    VLOG( 0 ) << "k_ycsb_partition_type:" << k_ycsb_partition_type;
    k_ycsb_storage_type =
        string_to_storage_type( FLAGS_ycsb_storage_type );
    VLOG( 0 ) << "k_ycsb_storage_type:" << k_ycsb_storage_type;
    k_ycsb_scan_restricted = FLAGS_ycsb_scan_restricted;
    VLOG( 0 ) << "k_ycsb_scan_restricted:" << k_ycsb_scan_restricted;
    k_ycsb_rmw_restricted = FLAGS_ycsb_rmw_restricted;
    VLOG( 0 ) << "k_ycsb_rmw_restricted:" << k_ycsb_rmw_restricted;
    k_ycsb_store_scan_results = FLAGS_ycsb_store_scan_results;
    VLOG( 0 ) << "k_ycsb_store_scan_results:" << k_ycsb_store_scan_results;

    VLOG( 0 ) << "YCSB Workload Probabilities";
    k_ycsb_write_prob = FLAGS_ycsb_write_prob;
    VLOG( 0 ) << "k_ycsb_write_prob:" << k_ycsb_write_prob;
    k_ycsb_read_prob = FLAGS_ycsb_read_prob;
    VLOG( 0 ) << "k_ycsb_read_prob:" << k_ycsb_read_prob;
    k_ycsb_rmw_prob = FLAGS_ycsb_rmw_prob;
    VLOG( 0 ) << "k_ycsb_rmw_prob:" << k_ycsb_rmw_prob;
    k_ycsb_scan_prob = FLAGS_ycsb_scan_prob;
    VLOG( 0 ) << "k_ycsb_scan_prob:" << k_ycsb_scan_prob;
    k_ycsb_multi_rmw_prob = FLAGS_ycsb_multi_rmw_prob;
    VLOG( 0 ) << "k_ycsb_multi_rmw_prob:" << k_ycsb_multi_rmw_prob;

    k_db_split_prob = FLAGS_db_split_prob;
    VLOG( 0 ) << "k_db_split_prob:" << k_db_split_prob;
    k_db_merge_prob = FLAGS_db_merge_prob;
    VLOG( 0 ) << "k_db_merge_prob:" << k_db_merge_prob;
    k_db_remaster_prob = FLAGS_db_remaster_prob;
    VLOG( 0 ) << "k_db_remaster_prob:" << k_db_remaster_prob;

    VLOG( 0 ) << "TPCC FLAGS";
    k_tpcc_num_warehouses = FLAGS_tpcc_num_warehouses;
    VLOG( 0 ) << "k_tpcc_num_warehouses:" << k_tpcc_num_warehouses;
    k_tpcc_num_items = FLAGS_tpcc_num_items;
    VLOG( 0 ) << "k_tpcc_num_items:" << k_tpcc_num_items;
    k_tpcc_num_suppliers = FLAGS_tpcc_num_suppliers;
    VLOG( 0 ) << "k_tpcc_num_suppliers:" << k_tpcc_num_suppliers;
    k_tpcc_expected_num_orders_per_cust =
        FLAGS_tpcc_expected_num_orders_per_cust;
    VLOG( 0 ) << "k_tpcc_expected_num_orders_per_cust:"
              << k_tpcc_expected_num_orders_per_cust;
    k_tpcc_item_partition_size = FLAGS_tpcc_item_partition_size;
    VLOG( 0 ) << "k_tpcc_item_partition_size:" << k_tpcc_item_partition_size;
    k_tpcc_partition_size = FLAGS_tpcc_partition_size;
    VLOG( 0 ) << "k_tpcc_partition_size:" << k_tpcc_partition_size;
    k_tpcc_district_partition_size = FLAGS_tpcc_district_partition_size;
    VLOG( 0 ) << "k_tpcc_district_partition_size:"
              << k_tpcc_district_partition_size;
    k_tpcc_customer_partition_size = FLAGS_tpcc_customer_partition_size;
    VLOG( 0 ) << "k_tpcc_customer_partition_size:"
              << k_tpcc_customer_partition_size;
    k_tpcc_supplier_partition_size = FLAGS_tpcc_supplier_partition_size;
    VLOG( 0 ) << "k_tpcc_supplier_partition_size:"
              << k_tpcc_supplier_partition_size;

    k_tpcc_new_order_within_warehouse_likelihood =
        FLAGS_tpcc_new_order_within_warehouse_likelihood;
    VLOG( 0 ) << "k_tpcc_new_order_within_warehouse_likelihood:"
              << k_tpcc_new_order_within_warehouse_likelihood;
    k_tpcc_payment_within_warehouse_likelihood =
        FLAGS_tpcc_payment_within_warehouse_likelihood;
    VLOG( 0 ) << "k_tpcc_payment_within_warehouse_likelihood:"
              << k_tpcc_payment_within_warehouse_likelihood;
    k_tpcc_track_and_use_recent_items = FLAGS_tpcc_track_and_use_recent_items;
    VLOG( 0 ) << "k_tpcc_track_and_use_recent_items:"
              << k_tpcc_track_and_use_recent_items;

    k_tpcc_num_clients = FLAGS_tpcc_num_clients;
    VLOG( 0 ) << "k_tpcc_num_clients:" << k_tpcc_num_clients;
    k_tpch_num_clients = FLAGS_tpch_num_clients;
    VLOG( 0 ) << "k_tpch_num_clients:" << k_tpch_num_clients;

    VLOG( 0 ) << "TPCC Workload Probabilities";
    k_tpcc_delivery_prob = FLAGS_tpcc_delivery_prob;
    VLOG( 0 ) << "k_tpcc_delivery_prob:" << k_tpcc_delivery_prob;
    k_tpcc_new_order_prob = FLAGS_tpcc_new_order_prob;
    VLOG( 0 ) << "k_tpcc_new_order_prob:" << k_tpcc_new_order_prob;
    k_tpcc_order_status_prob = FLAGS_tpcc_order_status_prob;
    VLOG( 0 ) << "k_tpcc_order_status_prob:" << k_tpcc_order_status_prob;
    k_tpcc_payment_prob = FLAGS_tpcc_payment_prob;
    VLOG( 0 ) << "k_tpcc_payment_prob:" << k_tpcc_payment_prob;
    k_tpcc_stock_level_prob = FLAGS_tpcc_stock_level_prob;
    VLOG( 0 ) << "k_tpcc_stock_level_prob:" << k_tpcc_stock_level_prob;

    k_tpcc_customer_column_partition_size =
        FLAGS_tpcc_customer_column_partition_size;
    VLOG( 0 ) << "k_tpcc_customer_column_partition_size:"
              << k_tpcc_customer_column_partition_size;
    k_tpcc_customer_part_type =
        string_to_partition_type( FLAGS_tpcc_customer_part_type );
    VLOG( 0 ) << "k_tpcc_customer_part_type:" << k_tpcc_customer_part_type;
    k_tpcc_customer_storage_type =
        string_to_storage_type( FLAGS_tpcc_customer_storage_type );
    VLOG( 0 ) << "k_tpcc_customer_storage_type:"
              << k_tpcc_customer_storage_type;

    k_tpcc_customer_district_column_partition_size =
        FLAGS_tpcc_customer_district_column_partition_size;
    VLOG( 0 ) << "k_tpcc_customer_district_column_partition_size:"
              << k_tpcc_customer_district_column_partition_size;
    k_tpcc_customer_district_part_type =
        string_to_partition_type( FLAGS_tpcc_customer_district_part_type );
    VLOG( 0 ) << "k_tpcc_customer_district_part_type:"
              << k_tpcc_customer_district_part_type;
    k_tpcc_customer_district_storage_type =
        string_to_storage_type( FLAGS_tpcc_customer_district_storage_type );
    VLOG( 0 ) << "k_tpcc_customer_district_storage_type:"
              << k_tpcc_customer_district_storage_type;

    k_tpcc_district_column_partition_size =
        FLAGS_tpcc_district_column_partition_size;
    VLOG( 0 ) << "k_tpcc_district_column_partition_size:"
              << k_tpcc_district_column_partition_size;
    k_tpcc_district_part_type =
        string_to_partition_type( FLAGS_tpcc_district_part_type );
    VLOG( 0 ) << "k_tpcc_district_part_type:" << k_tpcc_district_part_type;
    k_tpcc_district_storage_type =
        string_to_storage_type( FLAGS_tpcc_district_storage_type );
    VLOG( 0 ) << "k_tpcc_district_storage_type:"
              << k_tpcc_district_storage_type;

    k_tpcc_history_column_partition_size =
        FLAGS_tpcc_history_column_partition_size;
    VLOG( 0 ) << "k_tpcc_history_column_partition_size:"
              << k_tpcc_history_column_partition_size;
    k_tpcc_history_part_type =
        string_to_partition_type( FLAGS_tpcc_history_part_type );
    VLOG( 0 ) << "k_tpcc_history_part_type:" << k_tpcc_history_part_type;
    k_tpcc_history_storage_type =
        string_to_storage_type( FLAGS_tpcc_history_storage_type );
    VLOG( 0 ) << "k_tpcc_history_storage_type:" << k_tpcc_history_storage_type;

    k_tpcc_item_column_partition_size = FLAGS_tpcc_item_column_partition_size;
    VLOG( 0 ) << "k_tpcc_item_column_partition_size:"
              << k_tpcc_item_column_partition_size;
    k_tpcc_item_part_type =
        string_to_partition_type( FLAGS_tpcc_item_part_type );
    VLOG( 0 ) << "k_tpcc_item_part_type:" << k_tpcc_item_part_type;
    k_tpcc_item_storage_type =
        string_to_storage_type( FLAGS_tpcc_item_storage_type );
    VLOG( 0 ) << "k_tpcc_item_storage_type:" << k_tpcc_item_storage_type;

    k_tpcc_nation_column_partition_size =
        FLAGS_tpcc_nation_column_partition_size;
    VLOG( 0 ) << "k_tpcc_nation_column_partition_size:"
              << k_tpcc_nation_column_partition_size;
    k_tpcc_nation_part_type =
        string_to_partition_type( FLAGS_tpcc_nation_part_type );
    VLOG( 0 ) << "k_tpcc_nation_part_type:" << k_tpcc_nation_part_type;
    k_tpcc_nation_storage_type =
        string_to_storage_type( FLAGS_tpcc_nation_storage_type );
    VLOG( 0 ) << "k_tpcc_nation_storage_type:" << k_tpcc_nation_storage_type;

    k_tpcc_new_order_column_partition_size =
        FLAGS_tpcc_new_order_column_partition_size;
    VLOG( 0 ) << "k_tpcc_new_order_column_partition_size:"
              << k_tpcc_new_order_column_partition_size;
    k_tpcc_new_order_part_type =
        string_to_partition_type( FLAGS_tpcc_new_order_part_type );
    VLOG( 0 ) << "k_tpcc_new_order_part_type:" << k_tpcc_new_order_part_type;
    k_tpcc_new_order_storage_type =
        string_to_storage_type( FLAGS_tpcc_new_order_storage_type );
    VLOG( 0 ) << "k_tpcc_new_order_storage_type:"
              << k_tpcc_new_order_storage_type;

    k_tpcc_order_column_partition_size = FLAGS_tpcc_order_column_partition_size;
    VLOG( 0 ) << "k_tpcc_order_column_partition_size:"
              << k_tpcc_order_column_partition_size;
    k_tpcc_order_part_type =
        string_to_partition_type( FLAGS_tpcc_order_part_type );
    VLOG( 0 ) << "k_tpcc_order_part_type:" << k_tpcc_order_part_type;
    k_tpcc_order_storage_type =
        string_to_storage_type( FLAGS_tpcc_order_storage_type );
    VLOG( 0 ) << "k_tpcc_order_storage_type:" << k_tpcc_order_storage_type;

    k_tpcc_order_line_column_partition_size =
        FLAGS_tpcc_order_line_column_partition_size;
    VLOG( 0 ) << "k_tpcc_order_line_column_partition_size:"
              << k_tpcc_order_line_column_partition_size;
    k_tpcc_order_line_part_type =
        string_to_partition_type( FLAGS_tpcc_order_line_part_type );
    VLOG( 0 ) << "k_tpcc_order_line_part_type:" << k_tpcc_order_line_part_type;
    k_tpcc_order_line_storage_type =
        string_to_storage_type( FLAGS_tpcc_order_line_storage_type );
    VLOG( 0 ) << "k_tpcc_order_line_storage_type:"
              << k_tpcc_order_line_storage_type;

    k_tpcc_region_column_partition_size =
        FLAGS_tpcc_region_column_partition_size;
    VLOG( 0 ) << "k_tpcc_region_column_partition_size:"
              << k_tpcc_region_column_partition_size;
    k_tpcc_region_part_type =
        string_to_partition_type( FLAGS_tpcc_region_part_type );
    VLOG( 0 ) << "k_tpcc_region_part_type:" << k_tpcc_region_part_type;
    k_tpcc_region_storage_type =
        string_to_storage_type( FLAGS_tpcc_region_storage_type );
    VLOG( 0 ) << "k_tpcc_region_storage_type:" << k_tpcc_region_storage_type;

    k_tpcc_stock_column_partition_size = FLAGS_tpcc_stock_column_partition_size;
    VLOG( 0 ) << "k_tpcc_stock_column_partition_size:"
              << k_tpcc_stock_column_partition_size;
    k_tpcc_stock_part_type =
        string_to_partition_type( FLAGS_tpcc_stock_part_type );
    VLOG( 0 ) << "k_tpcc_stock_part_type:" << k_tpcc_stock_part_type;
    k_tpcc_stock_storage_type =
        string_to_storage_type( FLAGS_tpcc_stock_storage_type );
    VLOG( 0 ) << "k_tpcc_stock_storage_type:" << k_tpcc_stock_storage_type;

    k_tpcc_supplier_column_partition_size =
        FLAGS_tpcc_supplier_column_partition_size;
    VLOG( 0 ) << "k_tpcc_supplier_column_partition_size:"
              << k_tpcc_supplier_column_partition_size;
    k_tpcc_supplier_part_type =
        string_to_partition_type( FLAGS_tpcc_supplier_part_type );
    VLOG( 0 ) << "k_tpcc_supplier_part_type:" << k_tpcc_supplier_part_type;
    k_tpcc_supplier_storage_type =
        string_to_storage_type( FLAGS_tpcc_supplier_storage_type );
    VLOG( 0 ) << "k_tpcc_supplier_storage_type:"
              << k_tpcc_supplier_storage_type;

    k_tpcc_warehouse_column_partition_size =
        FLAGS_tpcc_warehouse_column_partition_size;
    VLOG( 0 ) << "k_tpcc_warehouse_column_partition_size:"
              << k_tpcc_warehouse_column_partition_size;
    k_tpcc_warehouse_part_type =
        string_to_partition_type( FLAGS_tpcc_warehouse_part_type );
    VLOG( 0 ) << "k_tpcc_warehouse_part_type:" << k_tpcc_warehouse_part_type;
    k_tpcc_warehouse_storage_type =
        string_to_storage_type( FLAGS_tpcc_warehouse_storage_type );
    VLOG( 0 ) << "k_tpcc_warehouse_storage_type:"
              << k_tpcc_warehouse_storage_type;

    VLOG( 0 ) << "TPCH Workload Probabilities";
    k_tpch_q1_prob = FLAGS_tpch_q1_prob;
    VLOG( 0 ) << "k_tpch_q1_prob:" << k_tpch_q1_prob;
    k_tpch_q1_prob = FLAGS_tpch_q2_prob;
    VLOG( 0 ) << "k_tpch_q2_prob:" << k_tpch_q2_prob;
    k_tpch_q3_prob = FLAGS_tpch_q3_prob;
    VLOG( 0 ) << "k_tpch_q3_prob:" << k_tpch_q3_prob;
    k_tpch_q4_prob = FLAGS_tpch_q4_prob;
    VLOG( 0 ) << "k_tpch_q4_prob:" << k_tpch_q4_prob;
    k_tpch_q5_prob = FLAGS_tpch_q5_prob;
    VLOG( 0 ) << "k_tpch_q5_prob:" << k_tpch_q5_prob;
    k_tpch_q6_prob = FLAGS_tpch_q6_prob;
    VLOG( 0 ) << "k_tpch_q6_prob:" << k_tpch_q6_prob;
    k_tpch_q7_prob = FLAGS_tpch_q7_prob;
    VLOG( 0 ) << "k_tpch_q7_prob:" << k_tpch_q7_prob;
    k_tpch_q8_prob = FLAGS_tpch_q8_prob;
    VLOG( 0 ) << "k_tpch_q8_prob:" << k_tpch_q8_prob;
    k_tpch_q9_prob = FLAGS_tpch_q9_prob;
    VLOG( 0 ) << "k_tpch_q9_prob:" << k_tpch_q9_prob;
    k_tpch_q10_prob = FLAGS_tpch_q10_prob;
    VLOG( 0 ) << "k_tpch_q10_prob:" << k_tpch_q10_prob;
    k_tpch_q11_prob = FLAGS_tpch_q11_prob;
    VLOG( 0 ) << "k_tpch_q11_prob:" << k_tpch_q11_prob;
    k_tpch_q12_prob = FLAGS_tpch_q12_prob;
    VLOG( 0 ) << "k_tpch_q12_prob:" << k_tpch_q12_prob;
    k_tpch_q13_prob = FLAGS_tpch_q13_prob;
    VLOG( 0 ) << "k_tpch_q13_prob:" << k_tpch_q13_prob;
    k_tpch_q14_prob = FLAGS_tpch_q14_prob;
    VLOG( 0 ) << "k_tpch_q14_prob:" << k_tpch_q14_prob;
    k_tpch_q15_prob = FLAGS_tpch_q15_prob;
    VLOG( 0 ) << "k_tpch_q15_prob:" << k_tpch_q15_prob;
    k_tpch_q16_prob = FLAGS_tpch_q16_prob;
    VLOG( 0 ) << "k_tpch_q16_prob:" << k_tpch_q16_prob;
    k_tpch_q17_prob = FLAGS_tpch_q17_prob;
    VLOG( 0 ) << "k_tpch_q17_prob:" << k_tpch_q17_prob;
    k_tpch_q18_prob = FLAGS_tpch_q18_prob;
    VLOG( 0 ) << "k_tpch_q18_prob:" << k_tpch_q18_prob;
    k_tpch_q19_prob = FLAGS_tpch_q19_prob;
    VLOG( 0 ) << "k_tpch_q19_prob:" << k_tpch_q19_prob;
    k_tpch_q20_prob = FLAGS_tpch_q20_prob;
    VLOG( 0 ) << "k_tpch_q20_prob:" << k_tpch_q20_prob;
    k_tpch_q21_prob = FLAGS_tpch_q21_prob;
    VLOG( 0 ) << "k_tpch_q21_prob:" << k_tpch_q21_prob;
    k_tpch_q22_prob = FLAGS_tpch_q22_prob;
    VLOG( 0 ) << "k_tpch_q22_prob:" << k_tpch_q22_prob;

    k_tpch_all_prob = FLAGS_tpch_all_prob;
    VLOG( 0 ) << "k_tpch_all_prob:" << k_tpch_all_prob;

    VLOG( 0 ) << "TPCC Spec Parameters";
    k_tpcc_spec_num_districts_per_warehouse =
        FLAGS_tpcc_spec_num_districts_per_warehouse;
    VLOG( 0 ) << "k_tpcc_spec_num_districts_per_warehouse:"
              << k_tpcc_spec_num_districts_per_warehouse;
    k_tpcc_spec_num_customers_per_district =
        FLAGS_tpcc_spec_num_customers_per_district;
    VLOG( 0 ) << "k_tpcc_spec_num_customers_per_district:"
              << k_tpcc_spec_num_customers_per_district;
    k_tpcc_spec_max_num_order_lines_per_order =
        FLAGS_tpcc_spec_max_num_order_lines_per_order;
    VLOG( 0 ) << "k_tpcc_spec_max_num_order_lines_per_order:"
              << k_tpcc_spec_max_num_order_lines_per_order;
    k_tpcc_spec_initial_num_customers_per_district =
        FLAGS_tpcc_spec_initial_num_customers_per_district;
    VLOG( 0 ) << "k_tpcc_spec_initial_num_customers_per_district:"
              << k_tpcc_spec_initial_num_customers_per_district;

    k_tpcc_should_fully_replicate_warehouses =
        FLAGS_tpcc_should_fully_replicate_warehouses;
    VLOG( 0 ) << "k_tpcc_should_fully_replicate_warehouses:"
              << k_tpcc_should_fully_replicate_warehouses;
    k_tpcc_should_fully_replicate_districts =
        FLAGS_tpcc_should_fully_replicate_districts;
    VLOG( 0 ) << "k_tpcc_should_fully_replicate_districts:"
              << k_tpcc_should_fully_replicate_districts;

    k_tpcc_use_warehouse = FLAGS_tpcc_use_warehouse;
    VLOG( 0 ) << "k_tpcc_use_warehouse:" << k_tpcc_use_warehouse;
    k_tpcc_use_district = FLAGS_tpcc_use_district;
    VLOG( 0 ) << "k_tpcc_use_district:" << k_tpcc_use_district;

    k_tpcc_h_scan_full_tables = FLAGS_tpcc_h_scan_full_tables;
    VLOG( 0 ) << "k_tpcc_h_scan_full_tables:" << k_tpcc_h_scan_full_tables;

    VLOG( 0 ) << "SmallBank";
    k_smallbank_num_accounts = FLAGS_smallbank_num_accounts;
    VLOG( 0 ) << "k_smallbank_num_accounts:" << k_smallbank_num_accounts;
    k_smallbank_hotspot_use_fixed_size = FLAGS_smallbank_hotspot_use_fixed_size;
    VLOG( 0 ) << "k_smallbank_hotspot_use_fixed_size:"
              << k_smallbank_hotspot_use_fixed_size;
    k_smallbank_hotspot_percentage = FLAGS_smallbank_hotspot_percentage;
    VLOG( 0 ) << "k_smallbank_hotspot_percentage:"
              << k_smallbank_hotspot_percentage;
    k_smallbank_hotspot_fixed_size = FLAGS_smallbank_hotspot_fixed_size;
    VLOG( 0 ) << "k_smallbank_hotspot_fixed_size:"
              << k_smallbank_hotspot_fixed_size;
    k_smallbank_partition_size = FLAGS_smallbank_partition_size;
    VLOG( 0 ) << "k_smallbank_partition_size:" << k_smallbank_partition_size;
    k_smallbank_customer_account_partition_spread =
        FLAGS_smallbank_customer_account_partition_spread;
    VLOG( 0 ) << "k_smallbank_customer_account_partition_spread:"
              << k_smallbank_customer_account_partition_spread;

    k_smallbank_accounts_col_size = FLAGS_smallbank_accounts_col_size;
    VLOG( 0 ) << "k_smallbank_accounts_col_size:"
              << k_smallbank_accounts_col_size;
    k_smallbank_accounts_partition_data_type = string_to_partition_type(
        FLAGS_smallbank_accounts_partition_data_type );
    VLOG( 0 ) << "k_smallbank_accounts_partition_data_type:"
              << k_smallbank_accounts_partition_data_type;
    k_smallbank_accounts_storage_data_type =
        string_to_storage_type( FLAGS_smallbank_accounts_storage_data_type );
    VLOG( 0 ) << "k_smallbank_accounts_storage_data_type:"
              << k_smallbank_accounts_storage_data_type;
    k_smallbank_banking_col_size = FLAGS_smallbank_banking_col_size;
    VLOG( 0 ) << "k_smallbank_banking_col_size:"
              << k_smallbank_banking_col_size;
    k_smallbank_banking_partition_data_type = string_to_partition_type(
        FLAGS_smallbank_accounts_partition_data_type );
    VLOG( 0 ) << "k_smallbank_banking_partition_data_type:"
              << k_smallbank_banking_partition_data_type;
    k_smallbank_banking_storage_data_type =
        string_to_storage_type( FLAGS_smallbank_accounts_storage_data_type );
    VLOG( 0 ) << "k_smallbank_banking_storage_data_type:"
              << k_smallbank_banking_storage_data_type;

    // smallbank probabilities
    k_smallbank_amalgamate_prob = FLAGS_smallbank_amalgamate_prob;
    VLOG( 0 ) << "k_smallbank_amalgamate_prob:" << k_smallbank_amalgamate_prob;
    k_smallbank_balance_prob = FLAGS_smallbank_balance_prob;
    VLOG( 0 ) << "k_smallbank_balance_prob:" << k_smallbank_balance_prob;
    k_smallbank_deposit_checking_prob = FLAGS_smallbank_deposit_checking_prob;
    VLOG( 0 ) << "k_smallbank_deposit_checking_prob:"
              << k_smallbank_deposit_checking_prob;
    k_smallbank_send_payment_prob = FLAGS_smallbank_send_payment_prob;
    VLOG( 0 ) << "k_smallbank_send_payment_prob:"
              << k_smallbank_send_payment_prob;
    k_smallbank_transact_savings_prob = FLAGS_smallbank_transact_savings_prob;
    VLOG( 0 ) << "k_smallbank_transact_savings_prob:"
              << k_smallbank_transact_savings_prob;
    k_smallbank_write_check_prob = FLAGS_smallbank_write_check_prob;
    VLOG( 0 ) << "k_smallbank_write_check_prob:"
              << k_smallbank_write_check_prob;

    VLOG( 0 ) << "Twitter";
    k_twitter_num_users = FLAGS_twitter_num_users;
    VLOG( 0 ) << "k_twitter_num_users:" << k_twitter_num_users;
    k_twitter_num_tweets = FLAGS_twitter_num_tweets;
    VLOG( 0 ) << "k_twitter_num_tweets:" << k_twitter_num_tweets;
    k_twitter_tweet_skew = FLAGS_twitter_tweet_skew;
    VLOG( 0 ) << "k_twitter_tweet_skew:" << k_twitter_tweet_skew;
    k_twitter_follow_skew = FLAGS_twitter_follow_skew;
    VLOG( 0 ) << "k_twitter_follow_skew:" << k_twitter_follow_skew;
    k_twitter_account_partition_size = FLAGS_twitter_account_partition_size;
    VLOG( 0 ) << "k_twitter_account_partition_size:"
              << k_twitter_account_partition_size;
    k_twitter_max_follow_per_user = FLAGS_twitter_max_follow_per_user;
    VLOG( 0 ) << "k_twitter_max_follow_per_user:"
              << k_twitter_max_follow_per_user;
    k_twitter_max_tweets_per_user = FLAGS_twitter_max_tweets_per_user;
    VLOG( 0 ) << "k_twitter_max_tweets_per_user:"
              << k_twitter_max_tweets_per_user;
    k_twitter_limit_tweets = FLAGS_twitter_limit_tweets;
    VLOG( 0 ) << "k_twitter_limit_tweets:" << k_twitter_limit_tweets;
    k_twitter_limit_tweets_for_uid = FLAGS_twitter_limit_tweets_for_uid;
    VLOG( 0 ) << "k_twitter_limit_tweets_for_uid:"
              << k_twitter_limit_tweets_for_uid;
    k_twitter_limit_followers = FLAGS_twitter_limit_followers;
    VLOG( 0 ) << "k_twitter_limit_followers:" << k_twitter_limit_followers;

    k_twitter_should_fully_replicate_users =
        FLAGS_twitter_should_fully_replicate_users;
    VLOG( 0 ) << "k_twitter_should_fully_replicate_users:"
              << k_twitter_should_fully_replicate_users;
    k_twitter_should_fully_replicate_follows =
        FLAGS_twitter_should_fully_replicate_follows;
    VLOG( 0 ) << "k_twitter_should_fully_replicate_follows:"
              << k_twitter_should_fully_replicate_follows;
    k_twitter_should_fully_replicate_tweets =
        FLAGS_twitter_should_fully_replicate_tweets;
    VLOG( 0 ) << "k_twitter_should_fully_replicate_tweets:"
              << k_twitter_should_fully_replicate_tweets;

    k_twitter_get_tweet_prob = FLAGS_twitter_get_tweet_prob;
    VLOG( 0 ) << "k_twitter_get_tweet_prob:" << k_twitter_get_tweet_prob;
    k_twitter_get_tweets_from_following_prob =
        FLAGS_twitter_get_tweets_from_following_prob;
    VLOG( 0 ) << "k_twitter_get_tweets_from_following_prob:"
              << k_twitter_get_tweets_from_following_prob;
    k_twitter_get_followers_prob = FLAGS_twitter_get_followers_prob;
    VLOG( 0 ) << "k_twitter_get_followers_prob:"
              << k_twitter_get_followers_prob;
    k_twitter_get_user_tweets_prob = FLAGS_twitter_get_user_tweets_prob;
    VLOG( 0 ) << "k_twitter_get_user_tweets_prob:"
              << k_twitter_get_user_tweets_prob;
    k_twitter_insert_tweet_prob = FLAGS_twitter_insert_tweet_prob;
    VLOG( 0 ) << "k_twitter_insert_tweet_prob:" << k_twitter_insert_tweet_prob;

    k_twitter_get_recent_tweets_prob = FLAGS_twitter_get_recent_tweets_prob;
    VLOG( 0 ) << "k_twitter_get_recent_tweets_prob:"
              << k_twitter_get_recent_tweets_prob;
    k_twitter_get_tweets_from_followers_prob =
        FLAGS_twitter_get_tweets_from_followers_prob;
    VLOG( 0 ) << "k_twitter_get_tweets_from_followers_prob:"
              << k_twitter_get_tweets_from_followers_prob;
    k_twitter_get_tweets_like_prob = FLAGS_twitter_get_tweets_like_prob;
    VLOG( 0 ) << "k_twitter_get_tweets_like_prob:"
              << k_twitter_get_tweets_like_prob;
    k_twitter_update_followers_prob = FLAGS_twitter_update_followers_prob;
    VLOG( 0 ) << "k_twitter_update_followers_prob:"
              << k_twitter_update_followers_prob;

    k_twitter_followers_column_partition_size =
        FLAGS_twitter_followers_column_partition_size;
    VLOG( 0 ) << "k_twitter_followers_column_partition_size:"
              << k_twitter_followers_column_partition_size;
    k_twitter_followers_part_type =
        string_to_partition_type( FLAGS_twitter_followers_part_type );
    VLOG( 0 ) << "k_twitter_followers_part_type:"
              << k_twitter_followers_part_type;
    k_twitter_followers_storage_type =
        string_to_storage_type( FLAGS_twitter_followers_storage_type );
    VLOG( 0 ) << "k_twitter_followers_storage_type:"
              << k_twitter_followers_storage_type;

    k_twitter_follows_column_partition_size =
        FLAGS_twitter_follows_column_partition_size;
    VLOG( 0 ) << "k_twitter_follows_column_partition_size:"
              << k_twitter_follows_column_partition_size;
    k_twitter_follows_part_type =
        string_to_partition_type( FLAGS_twitter_follows_part_type );
    VLOG( 0 ) << "k_twitter_follows_part_type:"
              << k_twitter_follows_part_type;
    k_twitter_follows_storage_type =
        string_to_storage_type( FLAGS_twitter_follows_storage_type );
    VLOG( 0 ) << "k_twitter_follows_storage_type:"
              << k_twitter_follows_storage_type;

    k_twitter_tweets_column_partition_size =
        FLAGS_twitter_tweets_column_partition_size;
    VLOG( 0 ) << "k_twitter_tweets_column_partition_size:"
              << k_twitter_tweets_column_partition_size;
    k_twitter_tweets_part_type =
        string_to_partition_type( FLAGS_twitter_tweets_part_type );
    VLOG( 0 ) << "k_twitter_tweets_part_type:"
              << k_twitter_tweets_part_type;
    k_twitter_tweets_storage_type =
        string_to_storage_type( FLAGS_twitter_tweets_storage_type );
    VLOG( 0 ) << "k_twitter_tweets_storage_type:"
              << k_twitter_tweets_storage_type;

    k_twitter_user_profile_column_partition_size =
        FLAGS_twitter_user_profile_column_partition_size;
    VLOG( 0 ) << "k_twitter_user_profile_column_partition_size:"
              << k_twitter_user_profile_column_partition_size;
    k_twitter_user_profile_part_type =
        string_to_partition_type( FLAGS_twitter_user_profile_part_type );
    VLOG( 0 ) << "k_twitter_user_profile_part_type:"
              << k_twitter_user_profile_part_type;
    k_twitter_user_profile_storage_type =
        string_to_storage_type( FLAGS_twitter_user_profile_storage_type );
    VLOG( 0 ) << "k_twitter_user_profile_storage_type:"
              << k_twitter_user_profile_storage_type;

    VLOG( 0 ) << "Site Selector";
    k_ss_bucket_size = FLAGS_ss_bucket_size;
    VLOG( 0 ) << "k_ss_bucket_size:" << k_ss_bucket_size;
    k_astream_max_queue_size = FLAGS_astream_max_queue_size;
    VLOG( 0 ) << "k_astream_max_queue_size:" << k_astream_max_queue_size;
    k_astream_empty_wait_ms = FLAGS_astream_empty_wait_ms;
    VLOG( 0 ) << "k_astream_empty_wait_ms:" << k_astream_empty_wait_ms;
    k_astream_tracking_interval_ms = FLAGS_astream_tracking_interval_ms;
    VLOG( 0 ) << "k_astream_tracking_interval_ms:"
              << k_astream_tracking_interval_ms;
    k_astream_mod = FLAGS_astream_mod;
    VLOG( 0 ) << "k_astream_mod:" << k_astream_mod;

    k_ss_max_write_blend_rate = FLAGS_ss_max_write_blend_rate;
    VLOG( 0 ) << "k_ss_max_write_blend_rate:" << k_ss_max_write_blend_rate;

    k_ss_run_background_workers = FLAGS_ss_run_background_workers;
    VLOG( 0 ) << "k_ss_run_background_workers:" << k_ss_run_background_workers;
    k_ss_num_background_work_items_to_execute =
        FLAGS_ss_num_background_work_items_to_execute;
    VLOG( 0 ) << "k_ss_num_background_work_items_to_execute:"
              << k_ss_num_background_work_items_to_execute;

    k_estore_clay_periodic_interval_ms = FLAGS_estore_clay_periodic_interval_ms;
    VLOG( 0 ) << "k_estore_clay_periodic_interval_ms:"
              << k_estore_clay_periodic_interval_ms;
    k_estore_load_epsilon_threshold = FLAGS_estore_load_epsilon_threshold;
    VLOG( 0 ) << "k_estore_load_epsilon_threshold:"
              << k_estore_load_epsilon_threshold;
    k_estore_hot_partition_pct_thresholds =
        FLAGS_estore_hot_partition_pct_thresholds;
    VLOG( 0 ) << "k_estore_hot_partition_pct_thresholds:"
              << k_estore_hot_partition_pct_thresholds;
    k_adr_multiplier = FLAGS_adr_multiplier;
    VLOG( 0 ) << "k_adr_multiplier:" << k_adr_multiplier;

    k_ss_enable_plan_reuse = FLAGS_ss_enable_plan_reuse;
    VLOG( 0 ) << "k_ss_enable_plan_reuse:" << k_ss_enable_plan_reuse;
    k_ss_plan_reuse_threshold = FLAGS_ss_plan_reuse_threshold;
    VLOG( 0 ) << "k_ss_plan_reuse_threshold:" << k_ss_plan_reuse_threshold;
    k_ss_enable_wait_for_plan_reuse = FLAGS_ss_enable_wait_for_plan_reuse;
    VLOG( 0 ) << "k_ss_enable_wait_for_plan_reuse:"
              << k_ss_enable_wait_for_plan_reuse;

    k_ss_optimistic_lock_mode =
        string_to_partition_lock_mode( FLAGS_ss_optimistic_lock_mode );
    VLOG( 0 ) << "k_ss_optimistic_lock_mode:"
              << partition_lock_mode_string( k_ss_optimistic_lock_mode );
    k_ss_physical_adjustment_lock_mode =
        string_to_partition_lock_mode( FLAGS_ss_physical_adjustment_lock_mode );
    VLOG( 0 ) << "k_ss_physical_adjustment_lock_mode:"
              << partition_lock_mode_string(
                     k_ss_physical_adjustment_lock_mode );
    k_ss_force_change_lock_mode =
        string_to_partition_lock_mode( FLAGS_ss_force_change_lock_mode );
    VLOG( 0 ) << "k_ss_force_change_lock_mode:"
              << partition_lock_mode_string( k_ss_force_change_lock_mode );

    k_num_site_selector_worker_threads_per_client =
        FLAGS_num_site_selector_worker_threads_per_client;
    VLOG( 0 ) << "k_num_site_selector_worker_threads_per_client:"
              << k_num_site_selector_worker_threads_per_client;

    k_ss_strategy_type = string_to_ss_strategy_type( FLAGS_ss_strategy_type );
    VLOG( 0 ) << "k_ss_strategy_type:"
              << ss_strategy_type_string( k_ss_strategy_type );
    k_ss_mastering_type =
        string_to_ss_mastering_type( FLAGS_ss_mastering_type );
    VLOG( 0 ) << "k_ss_mastering_type:"
              << ss_mastering_type_string( k_ss_mastering_type );

    k_ss_skew_weight = FLAGS_ss_skew_weight;
    VLOG( 0 ) << "k_ss_skew_weight:" << k_ss_skew_weight;
    k_ss_svv_delay_weight = FLAGS_ss_svv_delay_weight;

    VLOG( 0 ) << "k_ss_skew_exp_constant:" << k_ss_skew_exp_constant;
    k_ss_skew_exp_constant = FLAGS_ss_skew_exp_constant;

    VLOG( 0 ) << "k_ss_svv_delay_weight:" << k_ss_svv_delay_weight;
    k_ss_single_site_within_transactions_weight =
        FLAGS_ss_single_site_within_transactions_weight;
    VLOG( 0 ) << "k_ss_single_site_within_transactions_weight:"
              << k_ss_single_site_within_transactions_weight;
    k_ss_single_site_across_transactions_weight =
        FLAGS_ss_single_site_across_transactions_weight;
    VLOG( 0 ) << "k_ss_single_site_across_transactions_weight:"
              << k_ss_single_site_across_transactions_weight;
    k_ss_wait_time_normalization_constant =
        FLAGS_ss_wait_time_normalization_constant;
    VLOG( 0 ) << "k_ss_wait_time_normalization_constant:"
              << k_ss_wait_time_normalization_constant;
    k_ss_split_partition_ratio = FLAGS_ss_split_partition_ratio;
    VLOG( 0 ) << "k_ss_split_partition_ratio:" << k_ss_split_partition_ratio;
    k_ss_merge_partition_ratio = FLAGS_ss_merge_partition_ratio;
    VLOG( 0 ) << "k_ss_merge_partition_ratio:" << k_ss_merge_partition_ratio;
    k_ss_num_sampled_transactions_for_client =
        FLAGS_ss_num_sampled_transactions_for_client;
    VLOG( 0 ) << "k_ss_num_sampled_transactions_for_client:"
              << k_ss_num_sampled_transactions_for_client;
    k_ss_num_sampled_transactions_from_other_clients =
        FLAGS_ss_num_sampled_transactions_from_other_clients;
    VLOG( 0 ) << "k_ss_num_sampled_transactions_from_other_clients:"
              << k_ss_num_sampled_transactions_from_other_clients;
    k_ss_upfront_cost_weight = FLAGS_ss_upfront_cost_weight;
    VLOG( 0 ) << "k_ss_upfront_cost_weight:" << k_ss_upfront_cost_weight;
    k_ss_scan_upfront_cost_weight = FLAGS_ss_scan_upfront_cost_weight;
    VLOG( 0 ) << "k_ss_scan_upfront_cost_weight:"
              << k_ss_scan_upfront_cost_weight;
    k_ss_horizon_weight = FLAGS_ss_horizon_weight;
    VLOG( 0 ) << "k_ss_horizon_weight:" << k_ss_horizon_weight;
    k_ss_default_horizon_benefit_weight =
        FLAGS_ss_default_horizon_benefit_weight;
    VLOG( 0 ) << "k_ss_default_horizon_benefit_weight:"
              << k_ss_default_horizon_benefit_weight;
    k_ss_repartition_remaster_horizon_weight =
        FLAGS_ss_repartition_remaster_horizon_weight;
    VLOG( 0 ) << "k_ss_repartition_remaster_horizon_weight:"
              << k_ss_repartition_remaster_horizon_weight;
    k_ss_horizon_storage_weight = FLAGS_ss_horizon_storage_weight;
    VLOG( 0 ) << "k_ss_horizon_storage_weight:" << k_ss_horizon_storage_weight;
    k_ss_repartition_prob_rescale = FLAGS_ss_repartition_prob_rescale;
    VLOG( 0 ) << "k_ss_repartition_prob_rescale:"
              << k_ss_repartition_prob_rescale;
    k_ss_outlier_repartition_std_dev = FLAGS_ss_outlier_repartition_std_dev;
    VLOG( 0 ) << "k_ss_outlier_repartition_std_dev:"
              << k_ss_outlier_repartition_std_dev;
    k_ss_split_partition_size_ratio = FLAGS_ss_split_partition_size_ratio;
    VLOG( 0 ) << "k_ss_split_partition_size_ratio:"
              << k_ss_split_partition_size_ratio;
    k_ss_plan_site_load_balance_weight = FLAGS_ss_plan_site_load_balance_weight;
    VLOG( 0 ) << "k_ss_plan_site_load_balance_weight:"
              << k_ss_plan_site_load_balance_weight;

    k_ss_plan_site_storage_balance_weight =
        FLAGS_ss_plan_site_storage_balance_weight;
    VLOG( 0 ) << "k_ss_plan_site_storage_balance_weight:"
              << k_ss_plan_site_storage_balance_weight;
    k_ss_plan_site_storage_overlimit_weight =
        FLAGS_ss_plan_site_storage_overlimit_weight;
    VLOG( 0 ) << "k_ss_plan_site_storage_overlimit_weight:"
              << k_ss_plan_site_storage_overlimit_weight;
    k_ss_plan_site_storage_variance_weight =
        FLAGS_ss_plan_site_storage_variance_weight;
    VLOG( 0 ) << "k_ss_plan_site_storage_variance_weight:"
              << k_ss_plan_site_storage_variance_weight;
    k_ss_plan_site_memory_overlimit_weight =
        FLAGS_ss_plan_site_memory_overlimit_weight;
    VLOG( 0 ) << "k_ss_plan_site_memory_overlimit_weight:"
              << k_ss_plan_site_memory_overlimit_weight;
    k_ss_plan_site_disk_overlimit_weight =
        FLAGS_ss_plan_site_disk_overlimit_weight;
    VLOG( 0 ) << "k_ss_plan_site_disk_overlimit_weight:"
              << k_ss_plan_site_disk_overlimit_weight;
    k_ss_plan_site_memory_variance_weight =
        FLAGS_ss_plan_site_memory_variance_weight;
    VLOG( 0 ) << "k_ss_plan_site_memory_variance_weight:"
              << k_ss_plan_site_memory_variance_weight;
    k_ss_plan_site_disk_variance_weight =
        FLAGS_ss_plan_site_disk_variance_weight;
    VLOG( 0 ) << "k_ss_plan_site_disk_variance_weight:"
              << k_ss_plan_site_disk_variance_weight;

    k_ss_allow_force_site_selector_changes =
        FLAGS_ss_allow_force_site_selector_changes;
    VLOG( 0 ) << "k_ss_allow_force_site_selector_changes:"
              << k_ss_allow_force_site_selector_changes;

    k_ss_plan_scan_in_cost_order = FLAGS_ss_plan_scan_in_cost_order;
    VLOG( 0 ) << "k_ss_plan_scan_in_cost_order:"
              << k_ss_plan_scan_in_cost_order;
    k_ss_plan_scan_limit_changes = FLAGS_ss_plan_scan_limit_changes;
    VLOG( 0 ) << "k_ss_plan_scan_limit_changes:"
              << k_ss_plan_scan_limit_changes;
    k_ss_plan_scan_limit_ratio = FLAGS_ss_plan_scan_limit_ratio;
    VLOG( 0 ) << "k_ss_plan_scan_limit_ratio:" << k_ss_plan_scan_limit_ratio;

    k_ss_acceptable_new_partition_types =
        strings_to_partition_types( FLAGS_ss_acceptable_new_partition_types );
    VLOG( 0 ) << "k_ss_acceptable_new_partition_types:"
              << FLAGS_ss_acceptable_new_partition_types;
    k_ss_acceptable_partition_types =
        strings_to_partition_types( FLAGS_ss_acceptable_partition_types );
    VLOG( 0 ) << "k_ss_acceptable_partition_types:"
              << FLAGS_ss_acceptable_partition_types;
    k_ss_acceptable_new_storage_types =
        strings_to_storage_types( FLAGS_ss_acceptable_new_storage_types );
    VLOG( 0 ) << "k_ss_acceptable_new_storage_types:"
              << FLAGS_ss_acceptable_new_storage_types;
    k_ss_acceptable_storage_types =
        strings_to_storage_types( FLAGS_ss_acceptable_storage_types );
    VLOG( 0 ) << "k_ss_acceptable_storage_types:"
              << FLAGS_ss_acceptable_storage_types;

    k_use_query_arrival_predictor = FLAGS_use_query_arrival_predictor;
    VLOG( 0 ) << "k_use_query_arrival_predictor:"
              << k_use_query_arrival_predictor;
    k_query_arrival_predictor_type = string_to_query_arrival_predictor_type(
        FLAGS_query_arrival_predictor_type );
    VLOG( 0 ) << "k_query_arrival_predictor_type:"
              << query_arrival_predictor_type_string(
                     k_query_arrival_predictor_type );
    k_query_arrival_access_threshold = FLAGS_query_arrival_access_threshold;
    VLOG( 0 ) << "k_query_arrival_access_threshold:"
              << k_query_arrival_access_threshold;
    k_query_arrival_time_bucket = FLAGS_query_arrival_time_bucket;
    VLOG( 0 ) << "k_query_arrival_time_bucket:" << k_query_arrival_time_bucket;
    k_query_arrival_count_bucket = FLAGS_query_arrival_count_bucket;
    VLOG( 0 ) << "k_query_arrival_count_bucket:"
              << k_query_arrival_count_bucket;
    k_query_arrival_time_score_scalar = FLAGS_query_arrival_time_score_scalar;
    VLOG( 0 ) << "k_query_arrival_time_score_scalar:"
              << k_query_arrival_time_score_scalar;
    k_query_arrival_default_score = FLAGS_query_arrival_default_score;
    VLOG( 0 ) << "k_query_arrival_default_score:"
              << k_query_arrival_default_score;

    k_cost_model_is_static_model = FLAGS_cost_model_is_static_model;
    VLOG( 0 ) << "k_cost_model_is_static_model:"
              << k_cost_model_is_static_model;
    k_cost_model_learning_rate = FLAGS_cost_model_learning_rate;
    VLOG( 0 ) << "k_cost_model_learning_rate:" << k_cost_model_learning_rate;
    k_cost_model_regularization = FLAGS_cost_model_regularization;
    VLOG( 0 ) << "k_cost_model_regularization:" << k_cost_model_regularization;
    k_cost_model_bias_regularization = FLAGS_cost_model_bias_regularization;
    VLOG( 0 ) << "k_cost_model_bias_regularization:"
              << k_cost_model_bias_regularization;

    k_cost_model_momentum = FLAGS_cost_model_momentum;
    VLOG( 0 ) << "k_cost_model_momentum:" << k_cost_model_momentum;
    k_cost_model_kernel_gamma = FLAGS_cost_model_kernel_gamma;
    VLOG( 0 ) << "k_cost_model_kernel_gamma:" << k_cost_model_kernel_gamma;

    k_cost_model_max_internal_model_size_scalar =
        FLAGS_cost_model_max_internal_model_size_scalar;
    VLOG( 0 ) << "k_cost_model_max_internal_model_size_scalar:"
              << k_cost_model_max_internal_model_size_scalar;
    k_cost_model_layer_1_nodes_scalar = FLAGS_cost_model_layer_1_nodes_scalar;
    VLOG( 0 ) << "k_cost_model_layer_1_nodes_scalar:"
              << k_cost_model_layer_1_nodes_scalar;
    k_cost_model_layer_2_nodes_scalar = FLAGS_cost_model_layer_2_nodes_scalar;
    VLOG( 0 ) << "k_cost_model_layer_2_nodes_scalar:"
              << k_cost_model_layer_2_nodes_scalar;

    k_cost_model_num_reads_weight = FLAGS_cost_model_num_reads_weight;
    VLOG( 0 ) << "k_cost_model_num_reads_weight:"
              << k_cost_model_num_reads_weight;
    k_cost_model_num_reads_normalization =
        FLAGS_cost_model_num_reads_normalization;
    VLOG( 0 ) << "k_cost_model_num_reads_normalization:"
              << k_cost_model_num_reads_normalization;
    k_cost_model_num_reads_max_input = FLAGS_cost_model_num_reads_max_input;
    VLOG( 0 ) << "k_cost_model_num_reads_max_input:"
              << k_cost_model_num_reads_max_input;
    k_cost_model_read_bias = FLAGS_cost_model_read_bias;
    VLOG( 0 ) << "k_cost_model_read_bias:" << k_cost_model_read_bias;
    k_cost_model_read_predictor_type =
        string_to_predictor_type( FLAGS_cost_model_read_predictor_type );
    VLOG( 0 ) << "k_cost_model_read_predictor_type:"
              << predictor_type_to_string( k_cost_model_read_predictor_type );

    k_cost_model_num_reads_disk_weight = FLAGS_cost_model_num_reads_disk_weight;
    VLOG( 0 ) << "k_cost_model_num_reads_disk_weight:"
              << k_cost_model_num_reads_disk_weight;
    k_cost_model_read_disk_bias = FLAGS_cost_model_read_disk_bias;
    VLOG( 0 ) << "k_cost_model_read_disk_bias:" << k_cost_model_read_disk_bias;
    k_cost_model_read_disk_predictor_type =
        string_to_predictor_type( FLAGS_cost_model_read_disk_predictor_type );
    VLOG( 0 ) << "k_cost_model_read_disk_predictor_type:"
              << predictor_type_to_string(
                     k_cost_model_read_disk_predictor_type );

    k_cost_model_scan_num_rows_weight = FLAGS_cost_model_scan_num_rows_weight;
    VLOG( 0 ) << "k_cost_model_scan_num_rows_weight:"
              << k_cost_model_scan_num_rows_weight;
    k_cost_model_scan_num_rows_normalization =
        FLAGS_cost_model_scan_num_rows_normalization;
    VLOG( 0 ) << "k_cost_model_scan_num_rows_normalization:"
              << k_cost_model_scan_num_rows_normalization;
    k_cost_model_scan_num_rows_max_input =
        FLAGS_cost_model_scan_num_rows_max_input;
    VLOG( 0 ) << "k_cost_model_scan_num_rows_max_input:"
              << k_cost_model_scan_num_rows_max_input;
    k_cost_model_scan_selectivity_weight =
        FLAGS_cost_model_scan_selectivity_weight;
    VLOG( 0 ) << "k_cost_model_scan_selectivity_weight:"
              << k_cost_model_scan_selectivity_weight;
    k_cost_model_scan_selectivity_normalization =
        FLAGS_cost_model_scan_selectivity_normalization;
    VLOG( 0 ) << "k_cost_model_scan_selectivity_normalization:"
              << k_cost_model_scan_selectivity_normalization;
    k_cost_model_scan_selectivity_max_input =
        FLAGS_cost_model_scan_selectivity_max_input;
    VLOG( 0 ) << "k_cost_model_scan_selectivity_max_input:"
              << k_cost_model_scan_selectivity_max_input;
    k_cost_model_scan_bias = FLAGS_cost_model_scan_bias;
    VLOG( 0 ) << "k_cost_model_scan_bias:" << k_cost_model_scan_bias;
    k_cost_model_scan_predictor_type =
        string_to_predictor_type( FLAGS_cost_model_scan_predictor_type );
    VLOG( 0 ) << "k_cost_model_scan_predictor_type:"
              << predictor_type_to_string( k_cost_model_scan_predictor_type );

    k_cost_model_scan_disk_num_rows_weight =
        FLAGS_cost_model_scan_disk_num_rows_weight;
    VLOG( 0 ) << "k_cost_model_scan_disk_num_rows_weight:"
              << k_cost_model_scan_disk_num_rows_weight;
    k_cost_model_scan_disk_selectivity_weight =
        FLAGS_cost_model_scan_disk_selectivity_weight;
    VLOG( 0 ) << "k_cost_model_scan_disk_selectivity_weight:"
              << k_cost_model_scan_disk_selectivity_weight;
    k_cost_model_scan_disk_bias = FLAGS_cost_model_scan_disk_bias;
    VLOG( 0 ) << "k_cost_model_scan_disk_bias:" << k_cost_model_scan_disk_bias;
    k_cost_model_scan_disk_predictor_type =
        string_to_predictor_type( FLAGS_cost_model_scan_disk_predictor_type );
    VLOG( 0 ) << "k_cost_model_scan_disk_predictor_type:"
              << predictor_type_to_string(
                     k_cost_model_scan_disk_predictor_type );

    k_cost_model_num_writes_weight = FLAGS_cost_model_num_writes_weight;
    VLOG( 0 ) << "k_cost_model_num_writes_weight:"
              << k_cost_model_num_writes_weight;
    k_cost_model_num_writes_normalization =
        FLAGS_cost_model_num_writes_normalization;
    VLOG( 0 ) << "k_cost_model_num_writes_normalization:"
              << k_cost_model_num_writes_normalization;
    k_cost_model_num_writes_max_input = FLAGS_cost_model_num_writes_max_input;
    VLOG( 0 ) << "k_cost_model_num_writes_max_input:"
              << k_cost_model_num_writes_max_input;
    k_cost_model_write_bias = FLAGS_cost_model_write_bias;
    VLOG( 0 ) << "k_cost_model_write_bias:" << k_cost_model_write_bias;
    k_cost_model_write_predictor_type =
        string_to_predictor_type( FLAGS_cost_model_write_predictor_type );
    VLOG( 0 ) << "k_cost_model_write_predictor_type:"
              << predictor_type_to_string( k_cost_model_write_predictor_type );

    k_cost_model_lock_weight = FLAGS_cost_model_lock_weight;
    VLOG( 0 ) << "k_cost_model_lock_weight:" << k_cost_model_lock_weight;
    k_cost_model_lock_normalization = FLAGS_cost_model_lock_normalization;
    VLOG( 0 ) << "k_cost_model_lock_normalization:"
              << k_cost_model_lock_normalization;
    k_cost_model_lock_max_input = FLAGS_cost_model_lock_max_input;
    VLOG( 0 ) << "k_cost_model_lock_max_input:" << k_cost_model_lock_max_input;
    k_cost_model_lock_bias = FLAGS_cost_model_lock_bias;
    VLOG( 0 ) << "k_cost_model_lock_bias:" << k_cost_model_lock_bias;
    k_cost_model_lock_predictor_type =
        string_to_predictor_type( FLAGS_cost_model_lock_predictor_type );
    VLOG( 0 ) << "k_cost_model_lock_predictor_type:"
              << predictor_type_to_string( k_cost_model_lock_predictor_type );

    k_cost_model_commit_serialize_weight =
        FLAGS_cost_model_commit_serialize_weight;
    VLOG( 0 ) << "k_cost_model_commit_serialize_weight:"
              << k_cost_model_commit_serialize_weight;
    k_cost_model_commit_serialize_normalization =
        FLAGS_cost_model_commit_serialize_normalization;
    VLOG( 0 ) << "k_cost_model_commit_serialize_normalization:"
              << k_cost_model_commit_serialize_normalization;
    k_cost_model_commit_serialize_max_input =
        FLAGS_cost_model_commit_serialize_max_input;
    VLOG( 0 ) << "k_cost_model_commit_serialize_max_input:"
              << k_cost_model_commit_serialize_max_input;
    k_cost_model_commit_serialize_bias = FLAGS_cost_model_commit_serialize_bias;
    VLOG( 0 ) << "k_cost_model_commit_serialize_bias:"
              << k_cost_model_commit_serialize_bias;
    k_cost_model_commit_serialize_predictor_type = string_to_predictor_type(
        FLAGS_cost_model_commit_serialize_predictor_type );
    VLOG( 0 ) << "k_cost_model_commit_serialize_predictor_type:"
              << predictor_type_to_string(
                     k_cost_model_commit_serialize_predictor_type );

    k_cost_model_commit_build_snapshot_weight =
        FLAGS_cost_model_commit_build_snapshot_weight;
    VLOG( 0 ) << "k_cost_model_commit_build_snapshot_weight:"
              << k_cost_model_commit_build_snapshot_weight;
    k_cost_model_commit_build_snapshot_normalization =
        FLAGS_cost_model_commit_build_snapshot_normalization;
    VLOG( 0 ) << "k_cost_model_commit_build_snapshot_normalization:"
              << k_cost_model_commit_build_snapshot_normalization;
    k_cost_model_commit_build_snapshot_max_input =
        FLAGS_cost_model_commit_build_snapshot_max_input;
    VLOG( 0 ) << "k_cost_model_commit_build_snapshot_max_input:"
              << k_cost_model_commit_build_snapshot_max_input;
    k_cost_model_commit_build_snapshot_bias =
        FLAGS_cost_model_commit_build_snapshot_bias;
    VLOG( 0 ) << "k_cost_model_commit_build_snapshot_bias:"
              << k_cost_model_commit_build_snapshot_bias;
    k_cost_model_commit_build_snapshot_predictor_type =
        string_to_predictor_type(
            FLAGS_cost_model_commit_build_snapshot_predictor_type );
    VLOG( 0 ) << "k_cost_model_commit_build_snapshot_predictor_type:"
              << predictor_type_to_string(
                     k_cost_model_commit_build_snapshot_predictor_type );

    k_cost_model_wait_for_service_weight =
        FLAGS_cost_model_wait_for_service_weight;
    VLOG( 0 ) << "k_cost_model_wait_for_service_weight:"
              << k_cost_model_wait_for_service_weight;
    k_cost_model_wait_for_service_normalization =
        FLAGS_cost_model_wait_for_service_normalization;
    VLOG( 0 ) << "k_cost_model_wait_for_service_normalization:"
              << k_cost_model_wait_for_service_normalization;
    k_cost_model_wait_for_service_max_input =
        FLAGS_cost_model_wait_for_service_max_input;
    VLOG( 0 ) << "k_cost_model_wait_for_service_max_input:"
              << k_cost_model_wait_for_service_max_input;
    k_cost_model_wait_for_service_bias = FLAGS_cost_model_wait_for_service_bias;
    VLOG( 0 ) << "k_cost_model_wait_for_service_bias:"
              << k_cost_model_wait_for_service_bias;
    k_cost_model_wait_for_service_predictor_type = string_to_predictor_type(
        FLAGS_cost_model_wait_for_service_predictor_type );
    VLOG( 0 ) << "k_cost_model_wait_for_service_predictor_type:"
              << predictor_type_to_string(
                     k_cost_model_wait_for_service_predictor_type );

    k_cost_model_wait_for_session_version_weight =
        FLAGS_cost_model_wait_for_session_version_weight;
    VLOG( 0 ) << "k_cost_model_wait_for_session_version_weight:"
              << k_cost_model_wait_for_session_version_weight;
    k_cost_model_wait_for_session_version_normalization =
        FLAGS_cost_model_wait_for_session_version_normalization;
    VLOG( 0 ) << "k_cost_model_wait_for_session_version_normalization:"
              << k_cost_model_wait_for_session_version_normalization;
    k_cost_model_wait_for_session_version_max_input =
        FLAGS_cost_model_wait_for_session_version_max_input;
    VLOG( 0 ) << "k_cost_model_wait_for_session_version_max_input:"
              << k_cost_model_wait_for_session_version_max_input;
    k_cost_model_wait_for_session_version_bias =
        FLAGS_cost_model_wait_for_session_version_bias;
    VLOG( 0 ) << "k_cost_model_wait_for_session_version_bias:"
              << k_cost_model_wait_for_session_version_bias;
    k_cost_model_wait_for_session_version_predictor_type =
        string_to_predictor_type(
            FLAGS_cost_model_wait_for_session_version_predictor_type );
    VLOG( 0 ) << "k_cost_model_wait_for_session_version_predictor_type:"
              << predictor_type_to_string(
                     k_cost_model_wait_for_session_version_predictor_type );

    k_cost_model_wait_for_session_snapshot_weight =
        FLAGS_cost_model_wait_for_session_snapshot_weight;
    VLOG( 0 ) << "k_cost_model_wait_for_session_snapshot_weight:"
              << k_cost_model_wait_for_session_snapshot_weight;
    k_cost_model_wait_for_session_snapshot_normalization =
        FLAGS_cost_model_wait_for_session_snapshot_normalization;
    VLOG( 0 ) << "k_cost_model_wait_for_session_snapshot_normalization:"
              << k_cost_model_wait_for_session_snapshot_normalization;
    k_cost_model_wait_for_session_snapshot_max_input =
        FLAGS_cost_model_wait_for_session_snapshot_max_input;
    VLOG( 0 ) << "k_cost_model_wait_for_session_snapshot_max_input:"
              << k_cost_model_wait_for_session_snapshot_max_input;
    k_cost_model_wait_for_session_snapshot_bias =
        FLAGS_cost_model_wait_for_session_snapshot_bias;
    VLOG( 0 ) << "k_cost_model_wait_for_session_snapshot_bias:"
              << k_cost_model_wait_for_session_snapshot_bias;
    k_cost_model_wait_for_session_snapshot_predictor_type =
        string_to_predictor_type(
            FLAGS_cost_model_wait_for_session_snapshot_predictor_type );
    VLOG( 0 ) << "k_cost_model_wait_for_session_snapshot_predictor_type:"
              << predictor_type_to_string(
                     k_cost_model_wait_for_session_snapshot_predictor_type );

    k_cost_model_site_load_prediction_write_weight =
        FLAGS_cost_model_site_load_prediction_write_weight;
    VLOG( 0 ) << "k_cost_model_site_load_prediction_write_weight:"
              << k_cost_model_site_load_prediction_write_weight;
    k_cost_model_site_load_prediction_write_normalization =
        FLAGS_cost_model_site_load_prediction_write_normalization;
    VLOG( 0 ) << "k_cost_model_site_load_prediction_write_normalization:"
              << k_cost_model_site_load_prediction_write_normalization;
    k_cost_model_site_load_prediction_write_max_input =
        FLAGS_cost_model_site_load_prediction_write_max_input;
    VLOG( 0 ) << "k_cost_model_site_load_prediction_write_max_input:"
              << k_cost_model_site_load_prediction_write_max_input;
    k_cost_model_site_load_prediction_read_weight =
        FLAGS_cost_model_site_load_prediction_read_weight;
    VLOG( 0 ) << "k_cost_model_site_load_prediction_read_weight:"
              << k_cost_model_site_load_prediction_read_weight;
    k_cost_model_site_load_prediction_read_normalization =
        FLAGS_cost_model_site_load_prediction_read_normalization;
    VLOG( 0 ) << "k_cost_model_site_load_prediction_read_normalization:"
              << k_cost_model_site_load_prediction_read_normalization;
    k_cost_model_site_load_prediction_read_max_input =
        FLAGS_cost_model_site_load_prediction_read_max_input;
    VLOG( 0 ) << "k_cost_model_site_load_prediction_read_max_input:"
              << k_cost_model_site_load_prediction_read_max_input;
    k_cost_model_site_load_prediction_update_weight =
        FLAGS_cost_model_site_load_prediction_update_weight;
    VLOG( 0 ) << "k_cost_model_site_load_prediction_update_weight:"
              << k_cost_model_site_load_prediction_update_weight;
    k_cost_model_site_load_prediction_update_normalization =
        FLAGS_cost_model_site_load_prediction_update_normalization;
    VLOG( 0 ) << "k_cost_model_site_load_prediction_update_normalization:"
              << k_cost_model_site_load_prediction_update_normalization;
    k_cost_model_site_load_prediction_update_max_input =
        FLAGS_cost_model_site_load_prediction_update_max_input;
    VLOG( 0 ) << "k_cost_model_site_load_prediction_update_max_input:"
              << k_cost_model_site_load_prediction_update_max_input;
    k_cost_model_site_load_prediction_bias =
        FLAGS_cost_model_site_load_prediction_bias;
    VLOG( 0 ) << "k_cost_model_site_load_prediction_bias:"
              << k_cost_model_site_load_prediction_bias;
    k_cost_model_site_load_prediction_max_scale =
        FLAGS_cost_model_site_load_prediction_max_scale;
    VLOG( 0 ) << "k_cost_model_site_load_prediction_max_scale:"
              << k_cost_model_site_load_prediction_max_scale;
    k_cost_model_site_load_predictor_type =
        string_to_predictor_type( FLAGS_cost_model_site_load_predictor_type );
    VLOG( 0 ) << "k_cost_model_site_load_predictor_type:"
              << predictor_type_to_string(
                     k_cost_model_site_load_predictor_type );

    k_cost_model_site_operation_count_weight =
        FLAGS_cost_model_site_operation_count_weight;
    VLOG( 0 ) << "k_cost_model_site_operation_count_weight:"
              << k_cost_model_site_operation_count_weight;
    k_cost_model_site_operation_count_normalization =
        FLAGS_cost_model_site_operation_count_normalization;
    VLOG( 0 ) << "k_cost_model_site_operation_count_normalization:"
              << k_cost_model_site_operation_count_normalization;
    k_cost_model_site_operation_count_max_input =
        FLAGS_cost_model_site_operation_count_max_input;
    VLOG( 0 ) << "k_cost_model_site_operation_count_max_input:"
              << k_cost_model_site_operation_count_max_input;
    k_cost_model_site_operation_count_max_scale =
        FLAGS_cost_model_site_operation_count_max_scale;
    VLOG( 0 ) << "k_cost_model_site_operation_count_max_scale:"
              << k_cost_model_site_operation_count_max_scale;
    k_cost_model_site_operation_count_bias =
        FLAGS_cost_model_site_operation_count_bias;
    VLOG( 0 ) << "k_cost_model_site_operation_count_bias:"
              << k_cost_model_site_operation_count_bias;
    k_cost_model_site_operation_count_predictor_type = string_to_predictor_type(
        FLAGS_cost_model_site_operation_count_predictor_type );
    VLOG( 0 ) << "k_cost_model_site_operation_count_predictor_type:"
              << predictor_type_to_string(
                     k_cost_model_site_operation_count_predictor_type );

    k_cost_model_distributed_scan_max_weight =
        FLAGS_cost_model_distributed_scan_max_weight;
    VLOG( 0 ) << "k_cost_model_distributed_scan_max_weight:"
              << k_cost_model_distributed_scan_max_weight;
    k_cost_model_distributed_scan_min_weight =
        FLAGS_cost_model_distributed_scan_min_weight;
    VLOG( 0 ) << "k_cost_model_distributed_scan_min_weight:"
              << k_cost_model_distributed_scan_min_weight;
    k_cost_model_distributed_scan_normalization =
        FLAGS_cost_model_distributed_scan_normalization;
    VLOG( 0 ) << "k_cost_model_distributed_scan_normalization:"
              << k_cost_model_distributed_scan_normalization;
    k_cost_model_distributed_scan_max_input =
        FLAGS_cost_model_distributed_scan_max_input;
    VLOG( 0 ) << "k_cost_model_distributed_scan_max_input:"
              << k_cost_model_distributed_scan_max_input;
    k_cost_model_distributed_scan_bias = FLAGS_cost_model_distributed_scan_bias;
    VLOG( 0 ) << "k_cost_model_distributed_scan_bias:"
              << k_cost_model_distributed_scan_bias;
    predictor_type k_cost_model_distributed_scan_predictor_type =
        string_to_predictor_type(
            FLAGS_cost_model_distributed_scan_predictor_type );
    VLOG( 0 ) << "k_cost_model_distributed_scan_predictor_type:"
              << predictor_type_to_string(
                     k_cost_model_distributed_scan_predictor_type );

    k_cost_model_memory_num_rows_normalization =
        FLAGS_cost_model_memory_num_rows_normalization;
    VLOG( 0 ) << "k_cost_model_memory_num_rows_normalization:"
              << k_cost_model_memory_num_rows_normalization;
    k_cost_model_memory_num_rows_max_input =
        FLAGS_cost_model_memory_num_rows_max_input;
    VLOG( 0 ) << "k_cost_model_memory_num_rows_max_input:"
              << k_cost_model_memory_num_rows_max_input;

    k_cost_model_memory_allocation_num_rows_weight =
        FLAGS_cost_model_memory_allocation_num_rows_weight;
    VLOG( 0 ) << "k_cost_model_memory_allocation_num_rows_weight:"
              << k_cost_model_memory_allocation_num_rows_weight;
    k_cost_model_memory_allocation_bias =
        FLAGS_cost_model_memory_allocation_bias;
    VLOG( 0 ) << "k_cost_model_memory_allocation_bias:"
              << k_cost_model_memory_allocation_bias;
    k_cost_model_memory_allocation_predictor_type = string_to_predictor_type(
        FLAGS_cost_model_memory_allocation_predictor_type );
    VLOG( 0 ) << "k_cost_model_memory_allocation_predictor_type:"
              << predictor_type_to_string(
                     k_cost_model_memory_allocation_predictor_type );

    k_cost_model_memory_deallocation_num_rows_weight =
        FLAGS_cost_model_memory_deallocation_num_rows_weight;
    VLOG( 0 ) << "k_cost_model_memory_deallocation_num_rows_weight:"
              << k_cost_model_memory_deallocation_num_rows_weight;
    k_cost_model_memory_deallocation_bias =
        FLAGS_cost_model_memory_deallocation_bias;
    VLOG( 0 ) << "k_cost_model_memory_deallocation_bias:"
              << k_cost_model_memory_deallocation_bias;
    k_cost_model_memory_deallocation_predictor_type = string_to_predictor_type(
        FLAGS_cost_model_memory_deallocation_predictor_type );
    VLOG( 0 ) << "k_cost_model_memory_deallocation_predictor_type:"
              << predictor_type_to_string(
                     k_cost_model_memory_deallocation_predictor_type );

    k_cost_model_memory_assignment_num_rows_weight =
        FLAGS_cost_model_memory_assignment_num_rows_weight;
    VLOG( 0 ) << "k_cost_model_memory_assignment_num_rows_weight:"
              << k_cost_model_memory_assignment_num_rows_weight;
    k_cost_model_memory_assignment_bias =
        FLAGS_cost_model_memory_assignment_bias;
    VLOG( 0 ) << "k_cost_model_memory_assignment_bias:"
              << k_cost_model_memory_assignment_bias;
    k_cost_model_memory_assignment_predictor_type = string_to_predictor_type(
        FLAGS_cost_model_memory_assignment_predictor_type );
    VLOG( 0 ) << "k_cost_model_memory_assignment_predictor_type:"
              << predictor_type_to_string(
                     k_cost_model_memory_assignment_predictor_type );

    k_cost_model_disk_num_rows_normalization =
        FLAGS_cost_model_disk_num_rows_normalization;
    VLOG( 0 ) << "k_cost_model_disk_num_rows_normalization:"
              << k_cost_model_disk_num_rows_normalization;
    k_cost_model_disk_num_rows_max_input =
        FLAGS_cost_model_disk_num_rows_max_input;
    VLOG( 0 ) << "k_cost_model_disk_num_rows_max_input:"
              << k_cost_model_disk_num_rows_max_input;

    k_cost_model_evict_to_disk_num_rows_weight =
        FLAGS_cost_model_evict_to_disk_num_rows_weight;
    VLOG( 0 ) << "k_cost_model_evict_to_disk_num_rows_weight:"
              << k_cost_model_evict_to_disk_num_rows_weight;
    k_cost_model_evict_to_disk_bias = FLAGS_cost_model_evict_to_disk_bias;
    VLOG( 0 ) << "k_cost_model_evict_to_disk_bias:"
              << k_cost_model_evict_to_disk_bias;
    k_cost_model_evict_to_disk_predictor_type = string_to_predictor_type(
        FLAGS_cost_model_evict_to_disk_predictor_type );
    VLOG( 0 ) << "k_cost_model_evict_to_disk_predictor_type:"
              << predictor_type_to_string(
                     k_cost_model_evict_to_disk_predictor_type );

    k_cost_model_pull_from_disk_num_rows_weight =
        FLAGS_cost_model_pull_from_disk_num_rows_weight;
    VLOG( 0 ) << "k_cost_model_pull_from_disk_num_rows_weight:"
              << k_cost_model_pull_from_disk_num_rows_weight;
    k_cost_model_pull_from_disk_bias = FLAGS_cost_model_pull_from_disk_bias;
    VLOG( 0 ) << "k_cost_model_pull_from_disk_bias:"
              << k_cost_model_pull_from_disk_bias;
    k_cost_model_pull_from_disk_predictor_type = string_to_predictor_type(
        FLAGS_cost_model_pull_from_disk_predictor_type );
    VLOG( 0 ) << "k_cost_model_pull_from_disk_predictor_type:"
              << predictor_type_to_string(
                     k_cost_model_pull_from_disk_predictor_type );

    k_cost_model_widths_weight = FLAGS_cost_model_widths_weight;
    VLOG( 0 ) << "k_cost_model_widths_weight:" << k_cost_model_widths_weight;
    k_cost_model_widths_normalization = FLAGS_cost_model_widths_normalization;
    VLOG( 0 ) << "k_cost_model_widths_normalization:"
              << k_cost_model_widths_normalization;
    k_cost_model_widths_max_input = FLAGS_cost_model_widths_max_input;
    VLOG( 0 ) << "k_cost_model_widths_max_input:"
              << k_cost_model_widths_max_input;

    k_cost_model_wait_for_session_remaster_default_pct =
        FLAGS_cost_model_wait_for_session_remaster_default_pct;
    VLOG( 0 ) << "k_cost_model_wait_for_session_remaster_default_pct:"
              << k_cost_model_wait_for_session_remaster_default_pct;

    k_ss_enum_enable_track_stats = FLAGS_ss_enum_enable_track_stats;
    VLOG( 0 ) << "k_ss_enum_enable_track_stats:"
              << k_ss_enum_enable_track_stats;
    k_ss_enum_prob_of_returning_default =
        FLAGS_ss_enum_prob_of_returning_default;
    VLOG( 0 ) << "k_ss_enum_prob_of_returning_default:"
              << k_ss_enum_prob_of_returning_default;
    k_ss_enum_min_threshold = FLAGS_ss_enum_min_threshold;
    VLOG( 0 ) << "k_ss_enum_min_threshold:" << k_ss_enum_min_threshold;
    k_ss_enum_min_count_threshold = FLAGS_ss_enum_min_count_threshold;
    VLOG( 0 ) << "k_ss_enum_min_count_threshold:"
              << k_ss_enum_min_count_threshold;
    k_ss_enum_contention_bucket = FLAGS_ss_enum_contention_bucket;
    VLOG( 0 ) << "k_ss_enum_contention_bucket:" << k_ss_enum_contention_bucket;
    k_ss_enum_cell_width_bucket = FLAGS_ss_enum_cell_width_bucket;
    VLOG( 0 ) << "k_ss_enum_cell_width_bucket:" << k_ss_enum_cell_width_bucket;
    k_ss_enum_num_entries_bucket = FLAGS_ss_enum_num_entries_bucket;
    VLOG( 0 ) << "k_ss_enum_num_entries_bucket:"
              << k_ss_enum_num_entries_bucket;
    k_ss_enum_num_updates_needed_bucket =
        FLAGS_ss_enum_num_updates_needed_bucket;
    VLOG( 0 ) << "k_ss_enum_num_updates_needed_bucket:"
              << k_ss_enum_num_updates_needed_bucket;
    k_ss_enum_scan_selectivity_bucket = FLAGS_ss_enum_scan_selectivity_bucket;
    VLOG( 0 ) << "k_ss_enum_scan_selectivity_bucket:"
              << k_ss_enum_scan_selectivity_bucket;
    k_ss_enum_num_point_reads_bucket = FLAGS_ss_enum_num_point_reads_bucket;
    VLOG( 0 ) << "k_ss_enum_num_point_reads_bucket:"
              << k_ss_enum_num_point_reads_bucket;
    k_ss_enum_num_point_updates_bucket = FLAGS_ss_enum_num_point_updates_bucket;
    VLOG( 0 ) << "k_ss_enum_num_point_updates_bucket:"
              << k_ss_enum_num_point_updates_bucket;

    k_predictor_max_internal_model_size =
        FLAGS_predictor_max_internal_model_size;
    VLOG( 0 ) << "k_predictor_max_internal_model_size:"
              << k_predictor_max_internal_model_size;

    k_ss_update_stats_sleep_delay_ms = FLAGS_ss_update_stats_sleep_delay_ms;
    VLOG( 0 ) << "k_ss_update_stats_sleep_delay_ms:"
              << k_ss_update_stats_sleep_delay_ms;
    k_ss_num_poller_threads = FLAGS_ss_num_poller_threads;
    VLOG( 0 ) << "k_ss_num_poller_threads:" << k_ss_num_poller_threads;

    k_ss_colocation_keys_sampled = FLAGS_ss_colocation_keys_sampled;
    VLOG( 0 ) << "k_ss_colocation_keys_sampled:"
              << k_ss_colocation_keys_sampled;
    k_ss_partition_coverage_threshold = FLAGS_ss_partition_coverage_threshold;
    VLOG( 0 ) << "k_ss_partition_coverage_threshold:"
              << k_ss_partition_coverage_threshold;

    VLOG( 0 ) << "Site Manager";
    k_update_destination_type =
        string_to_update_destination_type( FLAGS_update_destination_type );
    VLOG( 0 ) << "k_update_destination_type:"
              << update_destination_type_string( k_update_destination_type );
    k_update_source_type =
        string_to_update_source_type( FLAGS_update_source_type );
    VLOG( 0 ) << "k_update_source_type:"
              << update_source_type_string( k_update_source_type );
    k_kafka_buffer_read_backoff = FLAGS_kafka_buffer_read_backoff;
    VLOG( 0 ) << "k_kafka_buffer_read_backoff:" << k_kafka_buffer_read_backoff;

    k_num_update_sources = FLAGS_num_update_sources;
    VLOG( 0 ) << "k_num_update_sources:" << k_num_update_sources;
    k_num_update_destinations = FLAGS_num_update_destinations;
    VLOG( 0 ) << "k_num_update_destinations:" << k_num_update_destinations;

    k_sleep_time_between_update_enqueue_iterations =
        FLAGS_sleep_time_between_update_enqueue_iterations;
    VLOG( 0 ) << "k_sleep_time_between_update_enqueue_iterations:"
              << k_sleep_time_between_update_enqueue_iterations;

    k_num_updates_before_apply_self = FLAGS_num_updates_before_apply_self;
    VLOG( 0 ) << "k_num_updates_before_apply_self:"
              << k_num_updates_before_apply_self;

    k_num_updates_to_apply_self = FLAGS_num_updates_to_apply_self;
    VLOG( 0 ) << "k_num_updates_to_apply_self:" << k_num_updates_to_apply_self;

    k_limit_number_of_records_propagated =
        FLAGS_limit_number_of_records_propagated;
    VLOG( 0 ) << "k_limit_number_of_records_propagated:"
              << k_limit_number_of_records_propagated;
    k_number_of_updates_needed_for_propagation =
        FLAGS_number_of_updates_needed_for_propagation;
    VLOG( 0 ) << "k_number_of_updates_needed_for_propagation:"
              << k_number_of_updates_needed_for_propagation;

    VLOG( 0 ) << "Rewriter flags";
    k_rewriter_type = string_to_rewriter_type( FLAGS_rewriter_type );
    VLOG( 0 ) << "k_rewriter_type:" << rewriter_type_string( k_rewriter_type );
    k_prob_range_rewriter_threshold = FLAGS_prob_range_rewriter_threshold;
    VLOG( 0 ) << "k_prob_range_rewriter_threshold:"
              << k_prob_range_rewriter_threshold;

    k_load_chunked_tables = FLAGS_load_chunked_tables;
    VLOG( 0 ) << "k_load_chunked_tables:" << k_load_chunked_tables;
    k_persist_chunked_tables = FLAGS_persist_chunked_tables;
    VLOG( 0 ) << "k_persist_chunked_tables:" << k_persist_chunked_tables;
    k_chunked_table_size = FLAGS_chunked_table_size;
    VLOG( 0 ) << "k_chunked_table_size" << k_chunked_table_size;

    VLOG( 0 ) << "Kafka";
    k_kafka_poll_count = FLAGS_kafka_poll_count;
    VLOG( 0 ) << "k_kafka_poll_count:" << k_kafka_poll_count;
    k_kafka_poll_sleep_ms = FLAGS_kafka_poll_sleep_ms;
    VLOG( 0 ) << "k_kafka_poll_sleep_ms:" << k_kafka_poll_sleep_ms;
    k_kafka_backoff_sleep_ms = FLAGS_kafka_backoff_sleep_ms;
    VLOG( 0 ) << "k_kafka_backoff_sleep_ms:" << k_kafka_backoff_sleep_ms;

    k_kafka_poll_timeout = FLAGS_kafka_poll_timeout;
    VLOG( 0 ) << "k_kafka_poll_timeout:" << k_kafka_poll_timeout;
    k_kafka_flush_wait_ms = FLAGS_kafka_flush_wait_ms;
    VLOG( 0 ) << "k_kafka_flush_wait_ms:" << k_kafka_flush_wait_ms;
    k_kafka_consume_timeout_ms = FLAGS_kafka_consume_timeout_ms;
    VLOG( 0 ) << "k_kafka_consume_timeout_ms:" << k_kafka_consume_timeout_ms;
    k_kafka_seek_timeout_ms = FLAGS_kafka_seek_timeout_ms;
    VLOG( 0 ) << "k_kafka_seek_timeout_ms:" << k_kafka_seek_timeout_ms;

    k_kafka_fetch_max_ms = FLAGS_kafka_fetch_max_ms;
    VLOG( 0 ) << "k_kafka_fetch_max_ms:" << k_kafka_fetch_max_ms;
    k_kafka_queue_max_ms = FLAGS_kafka_queue_max_ms;
    VLOG( 0 ) << "k_kafka_queue_max_ms:" << k_kafka_queue_max_ms;
    k_kafka_socket_blocking_max_ms = FLAGS_kafka_socket_blocking_max_ms;
    VLOG( 0 ) << "k_kafka_socket_blocking_max_ms:"
              << k_kafka_socket_blocking_max_ms;
    k_kafka_consume_max_messages = FLAGS_kafka_consume_max_messages;
    VLOG( 0 ) << "k_kafka_consume_max_messages:"
              << k_kafka_consume_max_messages;

    k_kafka_produce_max_messages = FLAGS_kafka_produce_max_messages;
    VLOG( 0 ) << "k_kafka_produce_max_messages:"
              << k_kafka_produce_max_messages;
    k_kafka_required_acks = FLAGS_kafka_required_acks;
    VLOG( 0 ) << "k_kafka_required_acks:" << k_kafka_required_acks;
    k_kafka_send_max_retries = FLAGS_kafka_send_max_retries;
    VLOG( 0 ) << "k_kafka_send_max_retries:" << k_kafka_send_max_retries;
    k_kafka_max_buffered_messages = FLAGS_kafka_max_buffered_messages;
    VLOG( 0 ) << "k_kafka_max_buffered_messages:"
              << k_kafka_max_buffered_messages;
    k_kafka_max_buffered_messages_size = FLAGS_kafka_max_buffered_messages_size;
    VLOG( 0 ) << "k_kafka_max_buffered_messages_size:"
              << k_kafka_max_buffered_messages_size;

    k_enable_secondary_storage = FLAGS_enable_secondary_storage;
    VLOG( 0 ) << "k_enable_secondary_storage:" << k_enable_secondary_storage;
    k_secondary_storage_dir = FLAGS_secondary_storage_dir;
    VLOG( 0 ) << "k_secondary_storage_dir:" << k_secondary_storage_dir;

    k_sample_reservoir_size_per_client = FLAGS_sample_reservoir_size_per_client;
    VLOG( 0 ) << "k_sample_reservoir_size_per_client:"
              << k_sample_reservoir_size_per_client;
    k_sample_reservoir_initial_multiplier =
        FLAGS_sample_reservoir_initial_multiplier;
    VLOG( 0 ) << "k_sample_reservoir_initial_multiplier:"
              << k_sample_reservoir_initial_multiplier;
    k_sample_reservoir_weight_increment =
        FLAGS_sample_reservoir_weight_increment;
    VLOG( 0 ) << "k_sample_reservoir_weight_increment:"
              << k_sample_reservoir_weight_increment;
    k_sample_reservoir_decay_rate = FLAGS_sample_reservoir_decay_rate;
    VLOG( 0 ) << "k_sample_reservoir_decay_rate:"
              << k_sample_reservoir_decay_rate;

    k_force_change_reservoir_size_per_client =
        FLAGS_force_change_reservoir_size_per_client;
    VLOG( 0 ) << "k_force_change_reservoir_size_per_client:"
              << k_force_change_reservoir_size_per_client;
    k_force_change_reservoir_initial_multiplier =
        FLAGS_force_change_reservoir_initial_multiplier;
    VLOG( 0 ) << "k_force_change_reservoir_initial_multiplier:"
              << k_force_change_reservoir_initial_multiplier;
    k_force_change_reservoir_weight_increment =
        FLAGS_force_change_reservoir_weight_increment;
    VLOG( 0 ) << "k_force_change_reservoir_weight_increment:"
              << k_force_change_reservoir_weight_increment;
    k_force_change_reservoir_decay_rate =
        FLAGS_force_change_reservoir_decay_rate;
    VLOG( 0 ) << "k_force_change_reservoir_decay_rate:"
              << k_force_change_reservoir_decay_rate;

    k_reservoir_decay_seconds = FLAGS_reservoir_decay_seconds;
    VLOG( 0 ) << "k_reservoir_decay_seconds:" << k_reservoir_decay_seconds;

    k_ss_storage_removal_threshold = FLAGS_ss_storage_removal_threshold;
    VLOG( 0 ) << "k_ss_storage_removal_threshold:"
              << k_ss_storage_removal_threshold;
    k_ss_num_remove_replicas_iteration = FLAGS_ss_num_remove_replicas_iteration;
    VLOG( 0 ) << "k_ss_num_remove_replicas_iteration:"
              << k_ss_num_remove_replicas_iteration;

    k_ss_allow_vertical_partitioning = FLAGS_ss_allow_vertical_partitioning;
    VLOG( 0 ) << "k_ss_allow_vertical_partitioning:"
              << k_ss_allow_vertical_partitioning;
    k_ss_allow_horizontal_partitioning = FLAGS_ss_allow_horizontal_partitioning;
    VLOG( 0 ) << "k_ss_allow_horizontal_partitioning:"
              << k_ss_allow_horizontal_partitioning;
    k_ss_allow_change_partition_types = FLAGS_ss_allow_change_partition_types;
    VLOG( 0 ) << "k_ss_allow_change_partition_types:"
              << k_ss_allow_change_partition_types;
    k_ss_allow_change_storage_types = FLAGS_ss_allow_change_storage_types;
    VLOG( 0 ) << "k_ss_allow_change_storage_types:"
              << k_ss_allow_change_storage_types;
}
