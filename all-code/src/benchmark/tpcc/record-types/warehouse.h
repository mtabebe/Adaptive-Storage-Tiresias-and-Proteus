#pragma once

#include "address.h"

struct warehouse {
   public:
    static constexpr float MIN_TAX = 0;
    static constexpr float MAX_TAX = 0.2000f;
    static constexpr float INITIAL_YTD = 300000.00f;
    static constexpr uint32_t  MIN_NAME_LEN = 6;
    static constexpr uint32_t  MAX_NAME_LEN = 10;

    int32_t w_id;
    float   w_tax;
    float   w_ytd;

    std::string w_name;

    address w_address;
};

struct warehouse_cols {
   public:
    static constexpr uint32_t w_id = 0;
    static constexpr uint32_t w_tax = 1;
    static constexpr uint32_t w_ytd = 2;

    static constexpr uint32_t w_name = 3;

    static constexpr uint32_t w_address_a_street_1 = 4;
    static constexpr uint32_t w_address_a_street_2 = 5;
    static constexpr uint32_t w_address_a_city = 6;
    static constexpr uint32_t w_address_a_state = 7;
    static constexpr uint32_t w_address_a_zip = 8;
};
