#pragma once

#include <cstdint>

const static uint32_t K_NO_OP = 0;
const static uint32_t K_INSERT_OP = 1;
const static uint32_t K_WRITE_OP = 1 << 1;     // 2
const static uint32_t K_DELETE_OP = 1 << 2;    // 4
const static uint32_t K_REMASTER_OP = 1 << 3;  // 8
const static uint32_t K_SPLIT_OP = 1 << 4;  // 16
const static uint32_t K_MERGE_OP = 1 << 5;  // 32
const static uint32_t K_CHANGE_DESTINATION_OP = 1 << 6;  // 64
