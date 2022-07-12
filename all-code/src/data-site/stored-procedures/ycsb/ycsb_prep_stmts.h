#pragma once

#include "../../single-site-db/db_abstraction.h"
#include "../../site-manager/serialize.h"
#include "../../site-manager/sproc_lookup_table.h"
#include "ycsb_sproc_helper_holder.h"

static const std::string           k_ycsb_read_sproc_name = "ycsb_read_record";
static const std::vector<arg_code> k_ycsb_read_arg_codes = {};
static const std::string k_ycsb_delete_sproc_name = "ycsb_delete_record";
static const std::vector<arg_code> k_ycsb_delete_arg_codes = {};
static const std::string k_ycsb_update_sproc_name = "ycsb_update_record";
static const std::vector<arg_code> k_ycsb_update_arg_codes = {};
static const std::string           k_ycsb_rmw_sproc_name = "ycsb_rmw_record";
static const std::vector<arg_code> k_ycsb_rmw_arg_codes = {};
static const std::string           k_ycsb_scan_sproc_name = "ycsb_scan_records";
static const std::vector<arg_code> k_ycsb_scan_arg_codes = {
    INTEGER_CODE /* col */, STRING_CODE /* predicate */};
static const std::string           k_ycsb_scan_data_sproc_name = "ycsb_scan_data";
static const std::vector<arg_code> k_ycsb_scan_data_arg_codes = {};

static const std::string k_ycsb_insert_sproc_name = "ycsb_insert_records";
static const std::vector<arg_code> k_ycsb_insert_arg_codes = {
    BIGINT_ARRAY_CODE};
static const std::string k_ycsb_ckr_insert_sproc_name = "ycsb_ckr_insert";
static const std::vector<arg_code> k_ycsb_ckr_insert_arg_codes = {};
static const std::string           k_ycsb_mk_rmw_sproc_name = "ycsb_mk_rmw";
static const std::vector<arg_code> k_ycsb_mk_rmw_arg_codes = {BOOL_CODE};

extern uint32_t k_ycsb_table_id;

sproc_result ycsb_create_database(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
sproc_result ycsb_read_record( transaction_partition_holder *partition_holder,
                               const clientid                id,
                               const std::vector<cell_key_ranges> &write_ckrs,
                               const std::vector<cell_key_ranges> &read_ckrs,
                               std::vector<arg_code> &             codes,
                               std::vector<void *> &               values,
                               void *sproc_opaque );
sproc_result ycsb_read_record( transaction_partition_holder *partition_holder,
                               const clientid                id,
                               const std::vector<cell_key_ranges> &write_ckrs,
                               const std::vector<cell_key_ranges> &read_ckrs,
                               std::vector<arg_code> &             codes,
                               std::vector<void *> &               values,
                               void *sproc_opaque );
sproc_result ycsb_delete_record( transaction_partition_holder *partition_holder,
                                 const clientid                id,
                                 const std::vector<cell_key_ranges> &write_ckrs,
                                 const std::vector<cell_key_ranges> &read_ckrs,
                                 std::vector<arg_code> &             codes,
                                 std::vector<void *> &               values,
                                 void *sproc_opaque );

sproc_result ycsb_update_record( transaction_partition_holder *partition_holder,
                                 const clientid                id,
                                 const std::vector<cell_key_ranges> &write_ckrs,
                                 const std::vector<cell_key_ranges> &read_ckrs,
                                 std::vector<arg_code> &             codes,
                                 std::vector<void *> &               values,
                                 void *sproc_opaque );

sproc_result ycsb_rmw_record( transaction_partition_holder *partition_holder,
                              const clientid                id,
                              const std::vector<cell_key_ranges> &write_ckrs,
                              const std::vector<cell_key_ranges> &read_ckrs,
                              std::vector<arg_code> &             codes,
                              std::vector<void *> &values, void *sproc_opaque );

sproc_result ycsb_scan_records( transaction_partition_holder *partition_holder,
                                const clientid                id,
                                const std::vector<cell_key_ranges> &write_ckrs,
                                const std::vector<cell_key_ranges> &read_ckrs,
                                std::vector<arg_code> &             codes,
                                std::vector<void *> &               values,
                                void *sproc_opaque );
void ycsb_scan_data( transaction_partition_holder *     partition_holder,
                     const clientid                     id,
                     const std::vector<scan_arguments> &scan_args,
                     std::vector<arg_code> &codes, std::vector<void *> &values,
                     void *sproc_opaque, scan_result &result );

sproc_result ycsb_insert_records(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );
sproc_result ycsb_ckr_insert( transaction_partition_holder *partition_holder,
                              const clientid                id,
                              const std::vector<cell_key_ranges> &write_ckrs,
                              const std::vector<cell_key_ranges> &read_ckrs,
                              std::vector<arg_code> &             codes,
                              std::vector<void *> &values, void *sproc_opaque );
sproc_result ycsb_mk_rmw( transaction_partition_holder *      partition_holder,
                          const clientid                      id,
                          const std::vector<cell_key_ranges> &write_ckrs,
                          const std::vector<cell_key_ranges> &read_ckrs,
                          std::vector<arg_code> &             codes,
                          std::vector<void *> &values, void *sproc_opaque );

