#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "../common/hw.h"

typedef uint32_t workload_operation_enum;
// workload_operations
const static workload_operation_enum UNDEFINED_OP = 0;
const static workload_operation_enum YCSB_WRITE = 1;
const static workload_operation_enum YCSB_READ = 2;
const static workload_operation_enum YCSB_RMW = 3;
const static workload_operation_enum YCSB_SCAN = 4;
const static workload_operation_enum YCSB_MULTI_RMW = 5;
const static workload_operation_enum TPCC_DELIVERY = 6;
const static workload_operation_enum TPCC_NEW_ORDER = 7;
const static workload_operation_enum TPCC_ORDER_STATUS = 8;
const static workload_operation_enum TPCC_PAYMENT = 9;
const static workload_operation_enum TPCC_STOCK_LEVEL = 10;

const static workload_operation_enum TPCH_Q1 = 11;
const static workload_operation_enum TPCH_Q2 = 12;
const static workload_operation_enum TPCH_Q3 = 13;
const static workload_operation_enum TPCH_Q4 = 14;
const static workload_operation_enum TPCH_Q5 = 15;
const static workload_operation_enum TPCH_Q6 = 16;
const static workload_operation_enum TPCH_Q7 = 17;
const static workload_operation_enum TPCH_Q8 = 18;
const static workload_operation_enum TPCH_Q9 = 19;
const static workload_operation_enum TPCH_Q10 = 20;
const static workload_operation_enum TPCH_Q11 = 21;
const static workload_operation_enum TPCH_Q12 = 22;
const static workload_operation_enum TPCH_Q13 = 23;
const static workload_operation_enum TPCH_Q14 = 24;
const static workload_operation_enum TPCH_Q15 = 25;
const static workload_operation_enum TPCH_Q16 = 26;
const static workload_operation_enum TPCH_Q17 = 27;
const static workload_operation_enum TPCH_Q18 = 28;
const static workload_operation_enum TPCH_Q19 = 29;
const static workload_operation_enum TPCH_Q20 = 30;
const static workload_operation_enum TPCH_Q21 = 31;
const static workload_operation_enum TPCH_Q22 = 32;

const static workload_operation_enum TPCH_ALL = 33;

const static workload_operation_enum SMALLBANK_AMALGAMATE = 34;
const static workload_operation_enum SMALLBANK_BALANCE = 35;
const static workload_operation_enum SMALLBANK_DEPOSIT_CHECKING = 36;
const static workload_operation_enum SMALLBANK_SEND_PAYMENT = 37;
const static workload_operation_enum SMALLBANK_TRANSACT_SAVINGS = 38;
const static workload_operation_enum SMALLBANK_WRITE_CHECK = 39;

const static workload_operation_enum TWITTER_GET_TWEET = 40;
const static workload_operation_enum TWITTER_GET_TWEETS_FROM_FOLLOWING = 41;
const static workload_operation_enum TWITTER_GET_FOLLOWERS = 42;
const static workload_operation_enum TWITTER_GET_USER_TWEETS = 43;
const static workload_operation_enum TWITTER_INSERT_TWEET = 44;
const static workload_operation_enum TWITTER_GET_RECENT_TWEETS = 45;
const static workload_operation_enum TWITTER_GET_TWEETS_FROM_FOLLOWERS = 46;
const static workload_operation_enum TWITTER_GET_TWEETS_LIKE = 47;
const static workload_operation_enum TWITTER_UPDATE_FOLLOWERS = 48;

const static workload_operation_enum DB_SPLIT = 49;
const static workload_operation_enum DB_MERGE = 50;
const static workload_operation_enum DB_REMASTER = 51;

static const std::vector<workload_operation_enum> k_ycsb_workload_operations = {
    YCSB_WRITE,     YCSB_READ, YCSB_RMW, YCSB_SCAN,
    YCSB_MULTI_RMW, DB_SPLIT,  DB_MERGE, DB_REMASTER};
static const std::vector<workload_operation_enum> k_tpcc_workload_operations = {
    TPCC_DELIVERY, TPCC_NEW_ORDER, TPCC_ORDER_STATUS, TPCC_PAYMENT,
    TPCC_STOCK_LEVEL};
static const std::vector<workload_operation_enum> k_tpch_workload_operations = {
    TPCH_Q1,  TPCH_Q2,  TPCH_Q3,  TPCH_Q4,  TPCH_Q5,  TPCH_Q6,
    TPCH_Q7,  TPCH_Q8,  TPCH_Q9,  TPCH_Q10, TPCH_Q11, TPCH_Q12,
    TPCH_Q13, TPCH_Q14, TPCH_Q15, TPCH_Q16, TPCH_Q17, TPCH_Q18,
    TPCH_Q19, TPCH_Q20, TPCH_Q21, TPCH_Q22, TPCH_ALL,
};

static const std::vector<workload_operation_enum> k_tpcch_workload_operations =
    {TPCC_DELIVERY,    TPCC_NEW_ORDER, TPCC_ORDER_STATUS, TPCC_PAYMENT,
     TPCC_STOCK_LEVEL, TPCH_Q1,        TPCH_Q2,           TPCH_Q3,
     TPCH_Q4,          TPCH_Q5,        TPCH_Q6,           TPCH_Q7,
     TPCH_Q8,          TPCH_Q9,        TPCH_Q10,          TPCH_Q11,
     TPCH_Q12,         TPCH_Q13,       TPCH_Q14,          TPCH_Q15,
     TPCH_Q16,         TPCH_Q17,       TPCH_Q18,          TPCH_Q19,
     TPCH_Q20,         TPCH_Q21,       TPCH_Q22,          TPCH_ALL};

static const std::vector<workload_operation_enum>
    k_smallbank_workload_operations = {
        SMALLBANK_AMALGAMATE,       SMALLBANK_BALANCE,
        SMALLBANK_DEPOSIT_CHECKING, SMALLBANK_SEND_PAYMENT,
        SMALLBANK_TRANSACT_SAVINGS, SMALLBANK_WRITE_CHECK};
static const std::vector<workload_operation_enum>
    k_twitter_workload_operations = { TWITTER_GET_TWEET,
                                      TWITTER_GET_TWEETS_FROM_FOLLOWING,
                                      TWITTER_GET_FOLLOWERS,
                                      TWITTER_GET_USER_TWEETS,
                                      TWITTER_INSERT_TWEET,
                                      TWITTER_GET_RECENT_TWEETS,
                                      TWITTER_GET_TWEETS_FROM_FOLLOWERS,
                                      TWITTER_GET_TWEETS_LIKE,
                                      TWITTER_UPDATE_FOLLOWERS };

static const std::unordered_map<workload_operation_enum, std::string>
    k_workload_operations_to_strings = {
        { UNDEFINED_OP, "UNDEFINED_OP" },
        { YCSB_WRITE, "YCSB_WRITE" },
        { YCSB_READ, "YCSB_READ" },
        { YCSB_RMW, "YCSB_RMW" },
        { YCSB_SCAN, "YCSB_SCAN" },
        { YCSB_MULTI_RMW, "YCSB_MULTI_RMW" },
        { TPCC_DELIVERY, "TPCC_DELIVERY" },
        { TPCC_NEW_ORDER, "TPCC_NEW_ORDER" },
        { TPCC_ORDER_STATUS, "TPCC_ORDER_STATUS" },
        { TPCC_PAYMENT, "TPCC_PAYMENT" },
        { TPCC_STOCK_LEVEL, "TPCC_STOCK_LEVEL" },
        { TPCH_Q1, "TPCH_Q1" },
        { TPCH_Q2, "TPCH_Q2" },
        { TPCH_Q3, "TPCH_Q3" },
        { TPCH_Q4, "TPCH_Q4" },
        { TPCH_Q5, "TPCH_Q5" },
        { TPCH_Q6, "TPCH_Q6" },
        { TPCH_Q7, "TPCH_Q7" },
        { TPCH_Q8, "TPCH_Q8" },
        { TPCH_Q9, "TPCH_Q9" },
        { TPCH_Q10, "TPCH_Q10" },
        { TPCH_Q11, "TPCH_Q11" },
        { TPCH_Q12, "TPCH_Q12" },
        { TPCH_Q13, "TPCH_Q13" },
        { TPCH_Q14, "TPCH_Q14" },
        { TPCH_Q15, "TPCH_Q15" },
        { TPCH_Q16, "TPCH_Q16" },
        { TPCH_Q17, "TPCH_Q17" },
        { TPCH_Q18, "TPCH_Q18" },
        { TPCH_Q19, "TPCH_Q19" },
        { TPCH_Q20, "TPCH_Q20" },
        { TPCH_Q21, "TPCH_Q21" },
        { TPCH_Q22, "TPCH_Q22" },
        { TPCH_ALL, "TPCH_ALL" },
        { SMALLBANK_AMALGAMATE, "SMALLBANK_AMALGAMATE" },
        { SMALLBANK_BALANCE, "SMALLBANK_BALANCE" },
        { SMALLBANK_DEPOSIT_CHECKING, "SMALLBANK_DEPOSIT_CHECKING" },
        { SMALLBANK_SEND_PAYMENT, "SMALLBANK_SEND_PAYMENT" },
        { SMALLBANK_TRANSACT_SAVINGS, "SMALLBANK_TRANSACT_SAVINGS" },
        { SMALLBANK_WRITE_CHECK, "SMALLBANK_WRITE_CHECK" },
        { TWITTER_GET_TWEET, "TWITTER_GET_TWEET" },
        { TWITTER_GET_TWEETS_FROM_FOLLOWING,
          "TWITTER_GET_TWEETS_FROM_FOLLOWING" },
        { TWITTER_GET_FOLLOWERS, "TWITTER_GET_FOLLOWERS" },
        { TWITTER_GET_USER_TWEETS, "TWITTER_GET_USER_TWEETS" },
        { TWITTER_INSERT_TWEET, "TWITTER_INSERT_TWEET" },
        { TWITTER_GET_RECENT_TWEETS, "TWITTER_GET_RECENT_TWEETS" },
        { TWITTER_GET_TWEETS_FROM_FOLLOWERS,
          "TWITTER_GET_TWEETS_FROM_FOLLOWERS" },
        { TWITTER_GET_TWEETS_LIKE, "TWITTER_GET_TWEETS_LIKE" },
        { TWITTER_UPDATE_FOLLOWERS, "TWITTER_UPDATE_FOLLOWERS" },
        { DB_SPLIT, "DB_SPLIT" },
        { DB_MERGE, "DB_MERGE" },
        { DB_REMASTER, "DB_REMASTER" } };

ALWAYS_INLINE
const std::string& workload_operation_string(
    const workload_operation_enum& op );

typedef uint32_t                             workload_operation_outcome_enum;
const static workload_operation_outcome_enum WORKLOAD_OP_SUCCESS = 0;
const static workload_operation_outcome_enum WORKLOAD_OP_FAILURE = 1;
const static workload_operation_outcome_enum WORKLOAD_OP_EXPECTED_ABORT = 2;

static const std::unordered_map<workload_operation_outcome_enum, std::string>
    k_workload_operation_outcomes_to_strings = {
        {WORKLOAD_OP_SUCCESS, "SUCCESS"},
        {WORKLOAD_OP_FAILURE, "FAILURE"},
        {WORKLOAD_OP_EXPECTED_ABORT, "EXPECTED_ABORT"}};

ALWAYS_INLINE
const std::string& workload_operation_outcome_string(
    const workload_operation_outcome_enum& op );

#include "workload_operation_enum-inl.h"
