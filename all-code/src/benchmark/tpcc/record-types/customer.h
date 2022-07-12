#pragma once

#include "address.h"
#include "datetime.h"

struct customer {
   public:
    static constexpr int NUM_CUSTOMERS_PER_DISTRICT = 3000;

    static constexpr float INITIAL_CREDIT_LIM = 50000.00;
    static constexpr float MIN_DISCOUNT = 0.0000;
    static constexpr float MAX_DISCOUNT = 0.5000;
    static constexpr float INITIAL_BALANCE = -10.00;
    static constexpr float INITIAL_YTD_PAYMENT = 10.00;

    static constexpr int32_t INITIAL_PAYMENT_CNT = 1;
    static constexpr int32_t INITIAL_DELIVERY_CNT = 0;

    static constexpr uint32_t MIN_FIRST_LEN = 6;
    static constexpr uint32_t MAX_FIRST_LEN = 10;
    static constexpr uint32_t MIDDLE_LEN = 2;
    static constexpr uint32_t MAX_LAST_LEN = 16;
    static constexpr uint32_t PHONE_LEN = 16;

    static constexpr uint32_t CREDIT_LEN = 2;

    static constexpr uint32_t MIN_DATA_LEN = 300;
    static constexpr uint32_t MAX_DATA_LEN = 500;

    int32_t c_id;
    int32_t c_d_id;
    int32_t c_w_id;

    int32_t c_n_id;

    float   c_credit_lim;
    float   c_discount;
    float   c_balance;
    float   c_ytd_payment;
    int32_t c_payment_cnt;
    int32_t c_delivery_cnt;

    std::string c_first;
    std::string c_middle;
    std::string c_last;

    address c_address;

    std::string c_phone;

    datetime c_since;

    std::string     c_credit;
    std::string     c_data;
};

struct customer_cols {
   public:
    static constexpr uint32_t c_id = 0;
    static constexpr uint32_t c_d_id = 1;
    static constexpr uint32_t c_w_id = 2;
    static constexpr uint32_t c_n_id = 3;
    static constexpr uint32_t c_credit_lim = 4;
    static constexpr uint32_t c_discount = 5;
    static constexpr uint32_t c_balance = 6;
    static constexpr uint32_t c_ytd_payment = 7;
    static constexpr uint32_t c_payment_cnt = 8;
    static constexpr uint32_t c_delivery_cnt = 9;
    static constexpr uint32_t c_first = 10;
    static constexpr uint32_t c_middle = 11;
    static constexpr uint32_t c_last = 12;
    static constexpr uint32_t c_address_a_street_1 = 13;
    static constexpr uint32_t c_address_a_street_2 = 14;
    static constexpr uint32_t c_address_a_city = 15;
    static constexpr uint32_t c_address_a_state = 16;
    static constexpr uint32_t c_address_a_zip = 17;
    static constexpr uint32_t c_phone = 18;
    static constexpr uint32_t c_since_c_since = 19;
    static constexpr uint32_t c_credit = 20;
    static constexpr uint32_t c_data = 21;
};
