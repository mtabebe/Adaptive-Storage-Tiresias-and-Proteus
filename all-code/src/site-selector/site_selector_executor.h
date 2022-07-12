#pragma once

#include "../common/constants.h"
#include "partition_data_location_table.h"
#include "query_arrival_predictor.h"
#include "site_selector_handler.h"
#include "ss_types.h"

std::shared_ptr<partition_data_location_table>
    construct_partition_data_location_table(
        const partition_data_location_table_configs& configs,
        const workload_type                          w_type = k_workload_type );

std::shared_ptr<partition_data_location_table>
    make_partition_data_location_table(
        const partition_data_location_table_configs& configs );

void add_ycsb_tables(
    std::shared_ptr<partition_data_location_table> data_loc_tab );
void add_tpcc_tables(
    std::shared_ptr<partition_data_location_table> data_loc_tab );
void add_smallbank_tables(
    std::shared_ptr<partition_data_location_table> data_loc_tab );
void add_twitter_tables(
    std::shared_ptr<partition_data_location_table> data_loc_tab );

void add_tables_to_query_arrival_predictor(
    std::shared_ptr<query_arrival_predictor>&             qap,
    const std::shared_ptr<partition_data_location_table>& table );
