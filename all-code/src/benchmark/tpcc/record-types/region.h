#pragma once

static const std::vector<std::string> k_tpch_region_names = {
    "AMERICA", "EUROPE", "ASIA", "AFRICA", "OCEANIA"};

struct region {
   public:
    static constexpr uint32_t NAME_LEN = 25;
    static constexpr uint32_t COMMENT_LEN = 100;
    static constexpr uint32_t NUM_REGIONS = 5;

    int32_t     r_id;

    std::string r_name;
    std::string r_comment;
};

struct region_cols {
   public:
    static constexpr uint32_t r_id = 0;
    static constexpr uint32_t r_name = 1;
    static constexpr uint32_t r_comment = 2;
};
