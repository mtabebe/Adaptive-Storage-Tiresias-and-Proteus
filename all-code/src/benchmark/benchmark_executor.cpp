#include "benchmark_executor.h"

#include <glog/logging.h>

#include "smallbank/smallbank_table_sizes.h"
#include "tpcc/tpcc_table_sizes.h"
#include "twitter/twitter_table_sizes.h"

void execute_benchmark( std::unique_ptr<benchmark_interface> bench ) {
    DVLOG( 1 ) << "Initializing benchmark";
    bench->init();

    DVLOG( 1 ) << "Creating database";
    bench->create_database();

    DVLOG( 1 ) << "Loading database";
    bench->load_database();

    DVLOG( 1 ) << "Running workload";
    bench->run_workload();

    DVLOG( 1 ) << "Getting statistics";
    benchmark_statistics stats = bench->get_statistics();
    stats.log_statistics();
}

void execute_ycsb_benchmark() {
    ycsb_configs                       configs = construct_ycsb_configs();
    db_abstraction_configs             abstraction_configs =
        construct_db_abstraction_configs();
    std::unique_ptr<ycsb_benchmark> bench =
        std::make_unique<ycsb_benchmark>( configs, abstraction_configs );
    execute_benchmark( std::move( bench ) );
}

void execute_tpcc_benchmark() {
    tpcc_configs                       configs = construct_tpcc_configs();
    db_abstraction_configs abstraction_configs =
        construct_db_abstraction_configs();
    std::unique_ptr<tpcc_benchmark> bench =
        std::make_unique<tpcc_benchmark>( configs, abstraction_configs );
    execute_benchmark( std::move( bench ) );
}

void execute_smallbank_benchmark() {
    smallbank_configs configs = construct_smallbank_configs();
    db_abstraction_configs abstraction_configs =
        construct_db_abstraction_configs();
    std::unique_ptr<smallbank_benchmark>    bench =
        std::make_unique<smallbank_benchmark>( configs, abstraction_configs );
    execute_benchmark( std::move( bench ) );
}

void execute_twitter_benchmark() {
    twitter_configs                       configs = construct_twitter_configs();
    db_abstraction_configs abstraction_configs =
        construct_db_abstraction_configs();
    std::unique_ptr<twitter_benchmark> bench =
        std::make_unique<twitter_benchmark>( configs, abstraction_configs );
    execute_benchmark( std::move( bench ) );
}

void run_benchmark( const workload_type w_type) {
    DVLOG( 1 ) << "Running:" << workload_type_string( w_type );
    switch( w_type ) {
        case YCSB:
            return execute_ycsb_benchmark();
        case TPCC:
            return execute_tpcc_benchmark();
        case SMALLBANK:
            return execute_smallbank_benchmark();
        case TWITTER:
            return execute_twitter_benchmark();
        case UNKNOWN_WORKLOAD:
            LOG( WARNING ) << "Trying to run unknown workload type";
            return;
    }
    LOG( WARNING ) << "Unknown workload type";
}

std::vector<table_partition_information> get_ycsb_benchmark_data_sizes(
    const ycsb_configs& configs ) {
    std::vector<table_partition_information> info;
    info.emplace_back( 0, configs.max_key_ + 1, configs.partition_size_,
                       configs.column_partition_size_, k_ycsb_num_columns,
                       k_ycsb_col_types, configs.partition_type_,
                       configs.storage_type_ );
    return info;
}

uint64_t get_ycsb_cross_bucket_count( const ycsb_configs& configs ) {
    // rough estimate we won't have more than this number of entries
    return configs.max_key_;
}

std::vector<table_partition_information> get_tpcc_benchmark_data_sizes(
    const tpcc_configs& configs ) {
    return get_tpcc_data_sizes( configs );
}

uint64_t get_tpcc_cross_bucket_count( const tpcc_configs& configs ) {
    // rough estimate we won't have more than this number of entries
    return get_order_line_size( configs );
}

std::vector<table_partition_information> get_smallbank_benchmark_data_sizes(
    const smallbank_configs& configs ) {
    return get_smallbank_data_sizes( configs );
}

uint64_t get_smallbank_cross_bucket_count( const smallbank_configs& configs ) {
    // rough estimate we won't have more than this number of entries
    return configs.num_accounts_;
}

std::vector<table_partition_information> get_twitter_benchmark_data_sizes(
    const twitter_configs& configs ) {
    return get_twitter_data_sizes( configs );
}

uint64_t get_twitter_cross_bucket_count( const twitter_configs& configs ) {
    // rough estimate we won't have more than this number of entries
    return configs.num_tweets_ * configs.num_users_;
}

std::vector<table_partition_information> get_benchmark_data_sizes(
    const workload_type w_type ) {
    switch( w_type ) {
        case YCSB:
            return get_ycsb_benchmark_data_sizes();
        case TPCC:
            return get_tpcc_benchmark_data_sizes();
        case SMALLBANK:
            return get_smallbank_benchmark_data_sizes();
        case TWITTER:
            return get_twitter_benchmark_data_sizes();
        case UNKNOWN_WORKLOAD:
            LOG( WARNING ) << "Trying to run unknown workload type";
            return {};
    }
    LOG( WARNING ) << "Unknown workload type";
    return {};
}

std::vector<table_metadata> get_benchmark_tables_metadata(
    const workload_type w_type, int site ) {
    switch( w_type ) {
        case YCSB:
            return {
                create_ycsb_table_metadata( construct_ycsb_configs(), site )};
        case TPCC:
            return create_tpcc_table_metadata( construct_tpcc_configs(), site );
        case SMALLBANK:
            return create_smallbank_table_metadata(
                construct_smallbank_configs(), site );
        case TWITTER:
            return create_twitter_table_metadata(
                construct_twitter_configs(), site );
        case UNKNOWN_WORKLOAD:
            LOG( WARNING ) << "Trying to run unknown workload type";
            return {};
    }
    LOG( WARNING ) << "Unknown workload type";
    return {};
}

uint64_t get_cross_bucket_count( const workload_type w_type ) {
    switch( w_type ) {
        case YCSB:
            return get_ycsb_cross_bucket_count();
        case TPCC:
            return get_tpcc_cross_bucket_count();
        case SMALLBANK:
            return get_smallbank_cross_bucket_count();
        case TWITTER:
            return get_twitter_cross_bucket_count();
        case UNKNOWN_WORKLOAD:
            LOG( WARNING ) << "Trying to run unknown workload type";
            return 0;
    }
    LOG( WARNING ) << "Unknown workload type";
    return 0;
}
