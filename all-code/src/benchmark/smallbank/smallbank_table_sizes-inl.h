#pragma once

#include "smallbank_record_types.h"

inline std::vector<table_partition_information> get_smallbank_data_sizes(
    const smallbank_configs& configs ) {

    std::vector<table_partition_information> sizes;
    uint64_t table_size = configs.num_accounts_;
    uint64_t partition_size = configs.partition_size_;

    sizes.emplace_back(
        k_smallbank_accounts_table_id, table_size, partition_size,
        configs.accounts_col_size_, k_smallbank_accounts_num_columns,
        k_smallbank_accounts_col_types, configs.accounts_partition_data_type_,
        configs.accounts_storage_data_type_ );
    sizes.emplace_back(
        k_smallbank_savings_table_id, table_size, partition_size,
        configs.banking_col_size_, k_smallbank_savings_num_columns,
        k_smallbank_savings_col_types, configs.banking_partition_data_type_,
        configs.banking_storage_data_type_ );
    sizes.emplace_back(
        k_smallbank_checkings_table_id, table_size, partition_size,
        configs.banking_col_size_, k_smallbank_checkings_num_columns,
        k_smallbank_checkings_col_types, configs.banking_partition_data_type_,
        configs.banking_storage_data_type_ );

    return sizes;
}
