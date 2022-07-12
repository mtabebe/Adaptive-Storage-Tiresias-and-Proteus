#pragma once

#include "../../common/cell_data_type.h"

static const uint32_t k_smallbank_accounts_num_columns = 2;

static const std::vector<cell_data_type> k_smallbank_accounts_col_types = {
    cell_data_type::UINT64, cell_data_type::STRING};

struct smallbank_account {
   public:
    static const uint32_t NAME_LEN = 64;

    // make it big so they don't run out of money
    static const uint32_t MIN_BALANCE = 10000;
    static const uint32_t MAX_BALANCE = 50000;

    static constexpr int PARAM_SEND_PAYMENT_AMOUNT = 5;
    static constexpr int PARAM_DEPOSIT_CHECKING_AMOUNT = 1;
    static constexpr int PARAM_TRANSACT_SAVINGS_AMOUNT = 20;
    static constexpr int PARAM_WRITE_CHECK_AMOUNT = 5;

    static const uint32_t  cust_id = 0;  // uint64_t
    static const uint32_t  name = 1;     // string
};

static const uint32_t                    k_smallbank_savings_num_columns = 2;
static const std::vector<cell_data_type> k_smallbank_savings_col_types = {
    cell_data_type::UINT64, cell_data_type::INT64};

struct smallbank_saving {
   public:
    static const uint32_t cust_id = 0;  // uint64_t
    static const uint32_t balance = 1;  // int64_t
};

static const uint32_t k_smallbank_checkings_num_columns =
    k_smallbank_savings_num_columns;
static const std::vector<cell_data_type> k_smallbank_checkings_col_types =
    k_smallbank_savings_col_types;

struct smallbank_checking {
   public:
    static const uint32_t cust_id = 0;  // uint64_t
    static const uint32_t balance = 1;  // int64_t
};

