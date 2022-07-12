#pragma once

struct address {
   public:
    static constexpr uint32_t MIN_STREET_LEN = 10;
    static constexpr uint32_t MAX_STREET_LEN = 20;
    static constexpr uint32_t MIN_CITY_LEN = 10;
    static constexpr uint32_t MAX_CITY_LEN = 20;
    static constexpr uint32_t STATE_LEN = 2;
    static constexpr uint32_t ZIP_LEN = 9;

    std::string a_street_1;
    std::string a_street_2;
    std::string a_city;
    std::string a_state;
    std::string a_zip;
};

struct address_cols {
   public:
    static constexpr uint32_t a_street_1 = 0;
    static constexpr uint32_t a_street_2 = 1;
    static constexpr uint32_t a_city = 2;
    static constexpr uint32_t a_state = 3;
    static constexpr uint32_t a_zip_len = 4;
};
