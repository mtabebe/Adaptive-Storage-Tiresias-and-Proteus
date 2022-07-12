#pragma once

#include "../../../benchmark/smallbank/smallbank_db_operators.h"
#include "../../../benchmark/smallbank/smallbank_loader.h"
#include "../../../templates/smallbank_benchmark_types.h"
#include "../../site-manager/serialize.h"
#include "../../site-manager/sproc_lookup_table.h"
#include "smallbank_sproc_helper_holder.h"

static const std::string k_smallbank_load_range_of_accounts_sproc_name =
    "smallbank_load_range_of_accounts";
static const std::vector<arg_code>
    k_smallbank_load_range_of_accounts_arg_codes = {BIGINT_CODE /*start*/,
                                                    BIGINT_CODE /*end*/};

static const std::string k_smallbank_load_assorted_accounts_sproc_name =
    "smallbank_load_assorted_accounts";
static const std::vector<arg_code>
    k_smallbank_load_assorted_accounts_arg_codes = {
        BIGINT_ARRAY_CODE /*accounts*/
};

static const std::string k_smallbank_load_assorted_savings_sproc_name =
    "smallbank_load_assorted_savings";
static const std::vector<arg_code> k_smallbank_load_assorted_savings_arg_codes =
    {
        BIGINT_ARRAY_CODE /*accounts*/
};

static const std::string k_smallbank_load_assorted_checkings_sproc_name =
    "smallbank_load_assorted_checkings";
static const std::vector<arg_code>
    k_smallbank_load_assorted_checkings_arg_codes = {
        BIGINT_ARRAY_CODE /*accounts*/
};

static const std::string k_smallbank_load_account_with_args_sproc_name =
    "smallbank_load_account_with_args";
static const std::vector<arg_code>
    k_smallbank_load_account_with_args_arg_codes = {
        BIGINT_CODE /*account*/, STRING_CODE /*name*/
};
static const std::string k_smallbank_load_saving_with_args_sproc_name =
    "smallbank_load_saving_with_args";
static const std::vector<arg_code> k_smallbank_load_saving_with_args_arg_codes =
    {
        BIGINT_CODE /*account*/, INTEGER_CODE /*amount*/
};
static const std::string k_smallbank_load_checking_with_args_sproc_name =
    "smallbank_load_checking_with_args";
static const std::vector<arg_code>
    k_smallbank_load_checking_with_args_arg_codes = {
        BIGINT_CODE /*account*/, INTEGER_CODE /*amount*/
};

static const std::string k_smallbank_amalgamate_sproc_name =
    "smallbank_amalgamate";
static const std::vector<arg_code> k_smallbank_amalgamate_arg_codes = {
    BIGINT_CODE /*source*/, BIGINT_CODE /*dest*/};

static const std::string k_smallbank_balance_sproc_name = "smallbank_balance";
static const std::vector<arg_code> k_smallbank_balance_arg_codes = {
    BIGINT_CODE /*cust*/};

static const std::string k_smallbank_deposit_checking_sproc_name =
    "smallbank_deposit_checking";
static const std::vector<arg_code> k_smallbank_deposit_checking_arg_codes = {
    BIGINT_CODE /*cust*/, INTEGER_CODE /*amount*/};

static const std::string k_smallbank_send_payment_sproc_name =
    "smallbank_send_payment";
static const std::vector<arg_code> k_smallbank_send_payment_arg_codes = {
    BIGINT_CODE /*source*/, BIGINT_CODE /*dest*/, INTEGER_CODE /*amount*/};

static const std::string k_smallbank_transact_savings_sproc_name =
    "smallbank_transact_savings";
static const std::vector<arg_code> k_smallbank_transact_savings_arg_codes = {
    BIGINT_CODE /*cust*/, INTEGER_CODE /*amount*/};

static const std::string k_smallbank_write_check_sproc_name =
    "smallbank_write_check";
static const std::vector<arg_code> k_smallbank_write_check_arg_codes = {
    BIGINT_CODE /*cust*/, INTEGER_CODE /*amount*/};

static const std::string k_smallbank_account_balance_sproc_name =
    "smallbank_account_balance";
static const std::vector<arg_code> k_smallbank_account_balance_arg_codes = {
    INTEGER_CODE /*table id*/, BIGINT_CODE /*cust*/};

static const std::string k_smallbank_distributed_charge_account_sproc_name =
    "smallbank_distributed_charge_account";
static const std::vector<arg_code>
    k_smallbank_distributed_charge_account_arg_codes = {
        INTEGER_CODE /*table id*/, BIGINT_CODE /*cust*/,
        INTEGER_CODE /*amount*/, BOOL_CODE /*is_debit*/
};

sproc_result smallbank_create_database(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );

sproc_result smallbank_load_range_of_accounts(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );

sproc_result smallbank_load_assorted_accounts(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges> &write_ckrs,
    const std::vector<cell_key_ranges> &read_ckrs, std::vector<arg_code> &codes,
    std::vector<void *> &values, void *sproc_opaque );
sproc_result smallbank_load_assorted_checkings(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
sproc_result smallbank_load_assorted_savings(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );

sproc_result smallbank_load_account_with_args(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
sproc_result smallbank_load_checking_with_args(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
sproc_result smallbank_load_saving_with_args(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );

sproc_result smallbank_amalgamate(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
sproc_result smallbank_balance( transaction_partition_holder *partition_holder,
                                const clientid                id,
                                const std::vector<cell_key_ranges> &write_ckrs,
                                const std::vector<cell_key_ranges> &read_ckrs,
                                std::vector<arg_code> &             codes,
                                std::vector<void *> &               values,
                                void *sproc_opaque );
sproc_result smallbank_deposit_checking(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
sproc_result smallbank_send_payment(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );

sproc_result smallbank_deposit_send_payment(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
sproc_result smallbank_transact_savings(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
sproc_result smallbank_write_check(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );

sproc_result smallbank_account_balance(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );
sproc_result smallbank_distributed_charge_account(
    transaction_partition_holder *partition_holder, const clientid id,
    const std::vector<cell_key_ranges>& write_ckrs,
    const std::vector<cell_key_ranges>& read_ckrs,
    std::vector<arg_code> &codes, std::vector<void *> &values,
    void *sproc_opaque );

void serialize_smallbank_result( sproc_result &               res,
                                 std::vector<void *> &        result_values,
                                 const std::vector<arg_code> &result_codes );
void serialize_balance_result( sproc_result &res, bool is_okay, int amount );

