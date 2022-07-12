#include "smallbank_configs.h"

#include <glog/logging.h>

#include "smallbank_table_sizes.h"

smallbank_configs construct_smallbank_configs(
    uint64_t num_accounts, bool hotspot_use_fixed_size,
    double hotspot_percentage, uint32_t hotspot_fixed_size,
    uint32_t partition_size, uint32_t customer_account_partition_spread,
    uint32_t             accounts_col_size,
    partition_type::type accounts_partition_data_type,
    storage_tier_type::type accounts_storage_data_type,
    uint32_t banking_col_size, partition_type::type banking_partition_data_type,
    storage_tier_type::type  banking_storage_data_type,
    const benchmark_configs& bench_configs, uint32_t amalgamate_prob,
    uint32_t balance_prob, uint32_t deposit_checking_prob,
    uint32_t send_payment_prob, uint32_t transact_savings_prob,
    uint32_t write_check_prob ) {
    smallbank_configs configs;

    configs.num_accounts_ = num_accounts;
    configs.hotspot_use_fixed_size_ = hotspot_use_fixed_size;
    configs.hotspot_percentage_ = hotspot_percentage;
    configs.hotspot_fixed_size_ = hotspot_fixed_size;
    configs.partition_size_ = partition_size;
    configs.customer_account_partition_spread_ =
        customer_account_partition_spread;

    configs.accounts_col_size_ = accounts_col_size;
    configs.accounts_partition_data_type_ = accounts_partition_data_type;
    configs.accounts_storage_data_type_ = accounts_storage_data_type;
    configs.banking_col_size_ = banking_col_size;
    configs.banking_partition_data_type_ = banking_partition_data_type;
    configs.banking_storage_data_type_ = banking_storage_data_type;

    configs.bench_configs_ = bench_configs;
    configs.workload_probs_ = {amalgamate_prob,       balance_prob,
                               deposit_checking_prob, send_payment_prob,
                               transact_savings_prob, write_check_prob};
    uint32_t cum_workload_prob = 0;
    for( uint32_t prob : configs.workload_probs_ ) {
        DCHECK_GE( prob, 0 );
        cum_workload_prob += prob;
    }

    DCHECK_EQ( 100, cum_workload_prob );

    DVLOG( 1 ) << "Created: " << configs;

    return configs;

}

std::ostream& operator<<( std::ostream& os, const smallbank_configs& config ) {
    os << "SmallBank Config: [ "
       << " Num Accounts:" << config.num_accounts_ << ","
       << " Hotspot Use Fixed Size:" << config.hotspot_use_fixed_size_ << ","
       << " Hotspot Percentage:" << config.hotspot_percentage_ << ","
       << " Hotspot Fixed Size:" << config.hotspot_fixed_size_ << ","
       << " Partition Size:" << config.partition_size_ << ","
       << "Customer Account Partition Spread:"
       << config.customer_account_partition_spread_ << ", "
       << " Accounts Column Size:" << config.accounts_col_size_ << ", "
       << " Accounts Partition Data Type:"
       << config.accounts_partition_data_type_ << ", "
       << " Accounts Storage Data Type:"
       << config.accounts_storage_data_type_ << ", "
       << " Banking Column Size:" << config.banking_col_size_ << ", "
       << " Banking Partition Data Type:" << config.banking_partition_data_type_
       << " Banking Storage Data Type:" << config.banking_storage_data_type_
       << ", " << config.bench_configs_ << " ]";
    return os;
}

std::vector<table_metadata> create_smallbank_table_metadata(
    const smallbank_configs& configs, int32_t site ) {

    std::vector<table_metadata> table_metas;

    std::vector<table_partition_information> table_info =
        get_smallbank_data_sizes( configs );

    int32_t  chain_size = k_num_records_in_chain;
    int32_t  chain_size_for_snapshot = k_num_records_in_snapshot_chain;
    uint32_t site_location = site;

    std::vector<std::string> names = {"smallbank_accounts", "smallbank_savings",
                                      "smallbank_checkings"};
    std::vector<uint32_t> table_ids = {k_smallbank_accounts_table_id,
                                       k_smallbank_savings_table_id,
                                       k_smallbank_checkings_table_id};
    DCHECK_EQ( names.size(), table_info.size() );
    DCHECK_EQ( names.size(), table_ids.size() );

        for( uint32_t table_id : table_ids ) {
        auto t_info = table_info.at( table_id );

        DCHECK_EQ( table_id, std::get<0>( t_info ) );
        // create tables
        table_metas.push_back( create_table_metadata(
            names.at( table_id ), table_id,
            std::get<4>( t_info ) /* num columns*/,
            std::get<5>( t_info ) /* col types*/, chain_size,
            chain_size_for_snapshot, site_location,
            /* partition size*/ std::get<2>( t_info ), /* col size*/
            std::get<3>( t_info ),
            /* partition tracking size*/ std::get<2>(
                t_info ), /* col tracking size*/
            std::get<3>( t_info ),
            /* part type*/ std::get<6>( t_info ),
            /* storage type*/ std::get<7>( t_info ) ) );
    }

    return table_metas;
}

