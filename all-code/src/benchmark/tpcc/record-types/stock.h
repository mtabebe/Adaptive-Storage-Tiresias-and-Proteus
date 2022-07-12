#pragma once

struct stock {
   public:
    static constexpr uint32_t MIN_QUANTITY = 10;
    static constexpr uint32_t MAX_QUANTITY = 100;

    static constexpr uint32_t MIN_DATA = 26;
    static constexpr uint32_t MAX_DATA = 50;

    static constexpr uint32_t NUM_DISTRICTS_PER_STOCK = 10;
    static constexpr uint32_t MAX_DISTRICT_STR_LEN = 24;

    static constexpr uint32_t MIN_STOCK_LEVEL_THRESHOLD = 10;
    static constexpr uint32_t MAX_STOCK_LEVEL_THRESHOLD = 20;
    static constexpr uint32_t STOCK_LEVEL_ORDERS = 20;

    int32_t s_i_id;
    int32_t s_w_id;

    int32_t s_s_id;

    int32_t s_quantity;
    int32_t s_ytd;
    int32_t s_order_cnt;
    int32_t s_remote_cnt;

    // the worklaod always uses fixed sized strings so we don't need
    // lengths;
    std::string s_dist_0;
    std::string s_dist_1;
    std::string s_dist_2;
    std::string s_dist_3;
    std::string s_dist_4;
    std::string s_dist_5;
    std::string s_dist_6;
    std::string s_dist_7;
    std::string s_dist_8;
    std::string s_dist_9;

    std::string s_data;
};

struct stock_cols {
   public:
    static constexpr uint32_t s_i_id = 0;
    static constexpr uint32_t s_w_id = 1;
    static constexpr uint32_t s_s_id = 2;
    static constexpr uint32_t s_quantity = 3;
    static constexpr uint32_t s_ytd = 4;
    static constexpr uint32_t s_order_cnt = 5;
    static constexpr uint32_t s_remote_cnt = 6;
    static constexpr uint32_t s_dist_0 = 7;
    static constexpr uint32_t s_dist_1 = 8;
    static constexpr uint32_t s_dist_2 = 9;
    static constexpr uint32_t s_dist_3 = 10;
    static constexpr uint32_t s_dist_4 = 11;
    static constexpr uint32_t s_dist_5 = 12;
    static constexpr uint32_t s_dist_6 = 13;
    static constexpr uint32_t s_dist_7 = 14;
    static constexpr uint32_t s_dist_8 = 15;
    static constexpr uint32_t s_dist_9 = 16;
    static constexpr uint32_t s_data = 17;
};
