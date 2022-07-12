#pragma once

#include "../common/bucket_funcs.h"
#include "../common/hw.h"
#include "../common/partition_funcs.h"
#include "../data-site/db/cell_identifier.h"

ALWAYS_INLINE std::vector<partition_column_identifier>
              generate_partition_column_identifiers(
        uint32_t table_id, uint64_t r_start, uint64_t r_end, uint32_t c_start,
        uint32_t                                        c_end,
        const std::vector<table_partition_information>& data_sizes );
ALWAYS_INLINE std::vector<partition_column_identifier>
              generate_partition_column_identifiers_from_ckrs(
        const std::vector<cell_key_ranges>&             ckrs,
        const std::vector<table_partition_information>& data_sizes );

ALWAYS_INLINE partition_column_identifier generate_partition_column_identifier(
    uint32_t table_id, uint64_t row, uint32_t col,
    const std::vector<table_partition_information>& data_sizes );
ALWAYS_INLINE partition_column_identifier
              generate_partition_column_identifier_from_cid(
        const cell_identifier&                          cid,
        const std::vector<table_partition_information>& data_sizes );

#include "primary_key_generation-inl.h"
