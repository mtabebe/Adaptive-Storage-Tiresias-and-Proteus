#pragma once

struct district {
   public:
    static constexpr uint32_t NUM_DISTRICTS_PER_WAREHOUSE = 10;

    // also has tax, but same as warehouse
    static constexpr float INITIAL_YTD = 30000.00;  // different from Warehouse

    static constexpr uint32_t MIN_NAME_LEN = 6;
    static constexpr uint32_t MAX_NAME_LEN = 10;

    int32_t d_id;
    int32_t d_w_id;
    float   d_tax;
    float   d_ytd;
    int32_t d_next_o_id;

    std::string d_name;

    address d_address;
};

struct district_cols {
   public:
    static constexpr uint32_t d_id = 0;
    static constexpr uint32_t d_w_id = 1;
    static constexpr uint32_t d_tax = 2;
    static constexpr uint32_t d_ytd = 3;
    static constexpr uint32_t d_next_o_id = 4;
    static constexpr uint32_t d_name = 5;
    static constexpr uint32_t d_address_a_street_1 = 6;
    static constexpr uint32_t d_address_a_street_2 = 7;
    static constexpr uint32_t d_address_a_city = 8;
    static constexpr uint32_t d_address_a_state = 9;
    static constexpr uint32_t d_address_a_zip = 10;
};
