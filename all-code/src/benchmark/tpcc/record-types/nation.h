#pragma once

static const std::string k_tpch_nation_name =
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

struct nation {
   public:
    static constexpr uint32_t NAME_LEN = 25;
    static constexpr uint32_t COMMENT_LEN = 100;
    static constexpr uint32_t NUM_NATIONS = 62;

    int32_t     n_id;
    int32_t     r_id;
    std::string n_name;
    std::string n_comment;
};

struct nation_cols {
   public:
    static constexpr uint32_t n_id = 0;
    static constexpr uint32_t r_id = 1;
    static constexpr uint32_t n_name = 2;
    static constexpr uint32_t n_comment = 3;
};
