#pragma once

#include <memory>

#include "../common/bucket_funcs.h"
#include "../common/constants.h"
#include "../concurrency/lock_type.h"
#include "smallbank/smallbank_benchmark.h"
#include "tpcc/tpcc_benchmark.h"
#include "tpcc/tpcc_configs.h"
#include "twitter/twitter_benchmark.h"
#include "twitter/twitter_configs.h"
#include "workload_type.h"
#include "ycsb/ycsb_benchmark.h"
#include "ycsb/ycsb_configs.h"

// Main entry point to running a benchmark
void run_benchmark( const workload_type w_type = k_workload_type );
void execute_ycsb_benchmark();

std::vector<table_partition_information> get_ycsb_benchmark_data_sizes(
    const ycsb_configs& configs = construct_ycsb_configs() );
uint64_t get_ycsb_cross_bucket_count(
    const ycsb_configs& configs = construct_ycsb_configs() );

uint64_t get_cross_bucket_count( const workload_type w_type = k_workload_type );
std::vector<table_partition_information> get_benchmark_data_sizes(
    const workload_type w_type = k_workload_type );
std::vector<table_metadata> get_benchmark_tables_metadata(
    const workload_type w_type = k_workload_type,
    int                 site = k_site_location_identifier );

void execute_tpcc_benchmark();
void execute_benchmark( std::unique_ptr<benchmark_interface> bench );

std::vector<table_partition_information> get_tpcc_benchmark_data_sizes(
    const tpcc_configs& configs = construct_tpcc_configs() );
uint64_t get_tpcc_cross_bucket_count(
    const tpcc_configs& configs = construct_tpcc_configs() );


void execute_smallbank_benchmark();
std::vector<table_partition_information> get_smallbank_benchmark_data_sizes(
    const smallbank_configs& configs = construct_smallbank_configs() );
uint64_t get_smallbank_cross_bucket_count(
    const smallbank_configs& configs = construct_smallbank_configs() );

void                                     execute_twitter_benchmark();
std::vector<table_partition_information> get_twitter_benchmark_data_sizes(
    const twitter_configs& configs = construct_twitter_configs() );
uint64_t get_twitter_cross_bucket_count(
    const twitter_configs& configs = construct_twitter_configs() );

