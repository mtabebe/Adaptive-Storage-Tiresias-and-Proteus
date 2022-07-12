#pragma once

#include "datetime.h"

struct order {
   public:
    static constexpr uint32_t MIN_CARRIER_ID = 1;
    static constexpr uint32_t MAX_CARRIER_ID = 10;
    // HACK: This is not strictly correct, but it works
    static constexpr uint32_t NULL_CARRIER_ID = 0;
    static constexpr uint32_t MIN_OL_CNT = 5;
    static constexpr uint32_t MAX_OL_CNT = 15;
    static constexpr uint32_t INITIAL_ALL_LOCAL = 1;

    int32_t o_id;
    int32_t o_c_id;
    int32_t o_d_id;
    int32_t o_w_id;
    int32_t o_carrier_id;
    int32_t o_ol_cnt;
    int32_t o_all_local;

    datetime o_entry_d;
};

struct order_cols {
   public:
    static constexpr uint32_t o_id = 0;
    static constexpr uint32_t o_c_id = 1;
    static constexpr uint32_t o_d_id = 2;
    static constexpr uint32_t o_w_id = 3;
    static constexpr uint32_t o_carrier_id = 4;
    static constexpr uint32_t o_ol_cnt = 5;
    static constexpr uint32_t o_all_local = 6;
    static constexpr uint32_t o_entry_d_c_since = 7;
};
