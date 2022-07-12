#pragma once

struct new_order {
   public:
    static constexpr uint32_t INITIAL_NUM_PER_DISTRICT = 900;

    int32_t no_w_id;
    int32_t no_d_id;
    int32_t no_o_id;
};

struct new_order_cols {
   public:
    static constexpr uint32_t no_w_id = 0;
    static constexpr uint32_t no_d_id = 1;
    static constexpr uint32_t no_o_id = 2;
};
