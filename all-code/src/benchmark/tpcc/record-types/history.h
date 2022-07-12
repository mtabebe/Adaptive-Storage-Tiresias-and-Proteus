#pragma once

#include "datetime.h"

struct history {
   public:
    static constexpr float INITIAL_AMOUNT = 10.00f;

    static constexpr uint32_t MIN_DATA_LEN = 12;
    static constexpr uint32_t MAX_DATA_LEN = 24;

    static constexpr float MIN_PAYMENT_AMOUNT = 1.00;
    static constexpr float MAX_PAYMENT_AMOUNT = 5000.00;

    int32_t h_c_id;
    int32_t h_c_d_id;
    int32_t h_c_w_id;
    int32_t h_d_id;
    int32_t h_w_id;
    float   h_amount;

    datetime h_date;

    std::string h_data;
};

struct history_cols {
   public:
    static constexpr uint32_t h_c_id = 0;
    static constexpr uint32_t h_c_d_id = 1;
    static constexpr uint32_t h_c_w_id = 2;
    static constexpr uint32_t h_d_id = 3;
    static constexpr uint32_t h_w_id = 4;
    static constexpr uint32_t h_amount = 5;

    static constexpr uint32_t h_date_c_since = 6;

    static constexpr uint32_t h_data = 7;
};
