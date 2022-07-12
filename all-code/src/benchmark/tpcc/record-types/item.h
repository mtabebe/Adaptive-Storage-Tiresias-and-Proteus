#pragma once

struct item {
   public:
    static constexpr uint32_t MIN_IM = 1;
    static constexpr uint32_t MAX_IM = 10000;
    static constexpr uint32_t NUM_ITEMS = 100000;
    static constexpr uint32_t INVALID_ITEM_ID = NUM_ITEMS + 1;

    static constexpr float MIN_PRICE = 1.00;
    static constexpr float MAX_PRICE = 100.00;

    static constexpr uint32_t MIN_NAME_LEN = 14;
    static constexpr uint32_t MAX_NAME_LEN = 24;

    static constexpr uint32_t MIN_DATA_LEN = 26;
    static constexpr uint32_t MAX_DATA_LEN = 50;

    int32_t i_id;
    int32_t i_im_id;
    float   i_price;

    std::string i_name;

    std::string i_data;
};

struct item_cols {
   public:
    static constexpr uint32_t   i_id = 0;
    static constexpr uint32_t   i_im_id = 1;
    static constexpr uint32_t   i_price = 2;

    static constexpr uint32_t i_name = 3;

    static constexpr uint32_t i_data = 4;
};
