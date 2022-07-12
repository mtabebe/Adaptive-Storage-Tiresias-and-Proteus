#pragma once

#include "../../common/hw.h"
#include "../db/cell_identifier.h"
#include "../db/op_codes.h"

class deserialized_cell_op {
   public:
    deserialized_cell_op();
    ~deserialized_cell_op();

    ALWAYS_INLINE void set_cell_identifier( uint32_t table_id, uint32_t col_id,
                                            uint64_t key );

    void add_update( uint64_t db_op, char* update_buffer,
                     uint32_t update_length );
    void add_delete();

    ALWAYS_INLINE uint64_t get_op() const;

    ALWAYS_INLINE char*    get_update_buffer() const;
    ALWAYS_INLINE uint32_t get_update_buffer_length() const;

    cell_identifier   cid_;
    uint64_t          op_code_;
    char*             payload_;
    uint32_t          payload_length_;
};

#include "deserialized_cell_op-inl.h"
