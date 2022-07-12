#pragma once

#include "../../common/cell_data_type.h"

static const uint32_t k_ycsb_num_columns = 4;

static const std::vector<cell_data_type> k_ycsb_col_types = {
    cell_data_type::UINT64, cell_data_type::INT64, cell_data_type::STRING,
    cell_data_type::STRING};

struct ycsb_row {
   public:
    static const uint32_t id = 0;         // uint64_t
    static const uint32_t timestamp = 1;  // int64_t
    static const uint32_t field0 = 2;     // string
    static const uint32_t field1 = 3;     // string
};
