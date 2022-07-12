#pragma once

#include "address.h"

struct supplier {
   public:
    static constexpr float INITIAL_BALANCE = -10.00;

    static constexpr uint32_t NAME_LEN = 25;
    static constexpr uint32_t COMMENT_LEN = 100;

    int32_t s_id;
    int32_t n_id;

    std::string s_name;
    std::string s_phone;

    address s_address;

    float       s_acctbal;
    std::string s_comment;
};

struct supplier_cols {
   public:
    static constexpr uint32_t s_id = 0;
    static constexpr uint32_t n_id = 1;
    static constexpr uint32_t s_name = 2;
    static constexpr uint32_t s_phone = 3;
    static constexpr uint32_t s_address_a_street_1 = 4;
    static constexpr uint32_t s_address_a_street_2 = 5;
    static constexpr uint32_t s_address_a_city = 6;
    static constexpr uint32_t s_address_a_state = 7;
    static constexpr uint32_t s_address_a_zip = 8;
    static constexpr uint32_t s_acctbal = 9;
    static constexpr uint32_t s_comment = 10;
};
