#pragma once

#include "datetime.h"
#include "stock.h"

struct order_line {
   public:
    static constexpr uint32_t  INITIAL_QUANTITY = 5;
    static constexpr float MIN_AMOUNT = 0.01f;
    static constexpr float MAX_AMOUNT = 9999.99f;

    int32_t ol_o_id;
    int32_t ol_d_id;
    int32_t ol_w_id;
    int32_t ol_number;
    int32_t ol_i_id;
    int32_t ol_supply_w_id;
    int32_t ol_quantity;
    float   ol_amount;

    datetime ol_delivery_d;

    std::string ol_dist_info;
};

struct order_line_cols {
   public:
    static constexpr uint32_t ol_o_id = 0;
    static constexpr uint32_t ol_d_id = 1;
    static constexpr uint32_t ol_w_id = 2;
    static constexpr uint32_t ol_number = 3;
    static constexpr uint32_t ol_i_id = 4;
    static constexpr uint32_t ol_supply_w_id = 5;
    static constexpr uint32_t ol_quantity = 6;
    static constexpr uint32_t ol_amount = 7;

    static constexpr uint32_t ol_delivery_d_c_since = 8;

    static constexpr uint32_t ol_dist_info = 9;
};

