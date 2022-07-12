#pragma once

#include "../../benchmark/smallbank/smallbank_configs.h"
#include "../../benchmark/tpcc/tpcc_configs.h"
#include "../../benchmark/twitter/twitter_configs.h"
#include "../../benchmark/workload_type.h"
#include "../../benchmark/ycsb/ycsb_configs.h"
#include "../../common/constants.h"
#include "../db/db.h"
#include "../db/transaction_partition_holder.h"
#include "../single-site-db/db_abstraction.h"
#include "../site-manager/sproc_lookup_table.h"
#include "smallbank/smallbank_sproc_helper_holder.h"
#if 0 // MTODO-HTAP
#include "ycsb/ycsb_sproc_helper_holder.h"
#endif
#include "tpcc/tpcc_sproc_helper_holder.h"
#include "twitter/twitter_sproc_helper_holder.h"

static const std::string           k_create_tables_sproc_name = "create_tables";
static const std::vector<arg_code> k_create_tables_arg_codes = {BIGINT_CODE};

static const std::string k_create_config_tables_sproc_name =
    "create_config_tables";
static const std::vector<arg_code> k_create_config_tables_arg_codes = {
    BIGINT_CODE, BIGINT_CODE};

std::unique_ptr<sproc_lookup_table> construct_sproc_lookup_table(
    const workload_type w_type = k_workload_type );

std::unique_ptr<sproc_lookup_table> construct_ycsb_sproc_lookup_table();

std::unique_ptr<sproc_lookup_table> construct_tpcc_sproc_lookup_table();

std::unique_ptr<sproc_lookup_table> construct_smallbank_sproc_lookup_table();

std::unique_ptr<sproc_lookup_table> construct_twitter_sproc_lookup_table();

std::unique_ptr<db> construct_database(
    std::shared_ptr<update_destination_generator> update_gen,
    std::shared_ptr<update_enqueuers> enqueuers, uint64_t num_clients,
    int32_t num_client_threads, const workload_type w_type = k_workload_type,
    uint32_t           site_location = k_site_location_identifier,
    uint32_t           gc_sleep_time = k_gc_sleep_time,
    bool               enable_secondary_storage = k_enable_secondary_storage,
    const std::string& secondary_storage_dir = k_secondary_storage_dir );

void* construct_sproc_opaque_pointer(
    const workload_type w_type = k_workload_type );
void destroy_sproc_opaque_pointer(
    void* opaque, const workload_type w_type = k_workload_type );

void* construct_ycsb_opaque_pointer(
    const ycsb_configs& configs = construct_ycsb_configs() );
void* construct_tpcc_opaque_pointer(
    const tpcc_configs& configs = construct_tpcc_configs() );
void* construct_smallbank_opaque_pointer(
    const smallbank_configs& configs = construct_smallbank_configs() );
void* construct_twitter_opaque_pointer(
    const twitter_configs& configs = construct_twitter_configs() );

void destroy_tpcc_opaque_pointer( void* opaque );
void destroy_ycsb_opaque_pointer( void* opaque );
void destroy_smallbank_opaque_pointer( void* opaque );
void destroy_twitter_opaque_pointer( void* opaque );

