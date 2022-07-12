#pragma once

#include <iostream>

#include "../../common/constants.h"
#include "../../common/hw.h"
#include "../../data-site/db/partition_metadata.h"
#include "../benchmark_configs.h"

class smallbank_configs {
  public:
    uint64_t num_accounts_;

    bool hotspot_use_fixed_size_;
    double hotspot_percentage_;
    uint32_t hotspot_fixed_size_;

    uint32_t partition_size_;
    uint32_t customer_account_partition_spread_;

    uint32_t                accounts_col_size_;
    partition_type::type    accounts_partition_data_type_;
    storage_tier_type::type accounts_storage_data_type_;
    uint32_t                banking_col_size_;
    partition_type::type    banking_partition_data_type_;
    storage_tier_type::type banking_storage_data_type_;

    benchmark_configs bench_configs_;
    std::vector<uint32_t> workload_probs_;
};

smallbank_configs construct_smallbank_configs(
    uint64_t num_accounts = k_smallbank_num_accounts,
    bool     hotspot_use_fixed_size = k_smallbank_hotspot_use_fixed_size,
    double   hotspot_percentage = k_smallbank_hotspot_percentage,
    uint32_t hotspot_fixed_size = k_smallbank_hotspot_fixed_size,
    uint32_t partition_size = k_smallbank_partition_size,
    uint32_t customer_account_partition_spread =
        k_smallbank_customer_account_partition_spread,
    uint32_t             accounts_col_size = k_smallbank_accounts_col_size,
    partition_type::type accounts_partition_data_type =
        k_smallbank_accounts_partition_data_type,
    storage_tier_type::type accounts_storage_data_type =
        k_smallbank_accounts_storage_data_type,
    uint32_t             banking_col_size = k_smallbank_banking_col_size,
    partition_type::type banking_partition_data_type =
        k_smallbank_banking_partition_data_type,
    storage_tier_type::type banking_storage_data_type =
        k_smallbank_banking_storage_data_type,
    const benchmark_configs &bench_configs = construct_benchmark_configs(),
    uint32_t                 amalgamate_prob = k_smallbank_amalgamate_prob,
    uint32_t                 balance_prob = k_smallbank_balance_prob,
    uint32_t deposit_checking_prob = k_smallbank_deposit_checking_prob,
    uint32_t send_payment_prob = k_smallbank_send_payment_prob,
    uint32_t transact_savings_prob = k_smallbank_transact_savings_prob,
    uint32_t write_check_prob = k_smallbank_write_check_prob );

std::ostream &operator<<( std::ostream &os, const smallbank_configs &configs );

std::vector<table_metadata> create_smallbank_table_metadata(
    const smallbank_configs &configs = construct_smallbank_configs(),
    int32_t                  site = k_site_location_identifier );

