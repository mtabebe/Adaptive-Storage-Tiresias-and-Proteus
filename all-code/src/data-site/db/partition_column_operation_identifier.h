#pragma once

#include "../../common/hw.h"
#include "../../common/partition_funcs.h"
#include "op_codes.h"

class partition_column_operation_identifier {
   public:
    ALWAYS_INLINE partition_column_operation_identifier();
    ALWAYS_INLINE ~partition_column_operation_identifier();

    ALWAYS_INLINE void add_partition_op( const partition_column_identifier& id,
                                         uint64_t data_64, uint32_t data_32,
                                         uint32_t op );

    partition_column_identifier identifier_;
    uint64_t                    data_64_;
    uint32_t                    data_32_;
    uint32_t                    op_code_;
};

#include "partition_column_operation_identifier-inl.h"
