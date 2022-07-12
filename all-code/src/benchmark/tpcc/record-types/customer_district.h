#pragma once

struct customer_district {
   public:

    int32_t c_id;
    int32_t c_d_id;
    int32_t c_w_id;

    int32_t c_next_o_id;
};

struct customer_district_cols {
   public:
    static constexpr uint32_t c_id = 0;
    static constexpr uint32_t c_d_id = 1;
    static constexpr uint32_t c_w_id = 2;

    static constexpr uint32_t c_next_o_id = 3;
};
