#pragma once

#include "../../common/hw.h"
#include "cell_identifier.h"
#include "op_codes.h"
#include "row_record.h"

template <typename T>
class versioned_data_identifier {
   public:
    versioned_data_identifier();
    ~versioned_data_identifier();

    void add_db_op( const cell_identifier& rid, T* r, uint32_t op_code );

    ALWAYS_INLINE uint32_t get_op() const;
    ALWAYS_INLINE bool     is_delete_op() const;

    ALWAYS_INLINE T* get_data() const;

    // N.B.: If you add special stuff in here that needs to be
    // deconstructed/unallocated
    // on object destruction, make sure you add it to write_buffer_deleter.

    cell_identifier   identifier_;
    uint32_t          op_code_;
    T*                data_;
};

using versioned_row_record_identifier = versioned_data_identifier<row_record>;
using versioned_cell_data_identifier =
    versioned_data_identifier<packed_cell_data>;

#include "versioned_data_identifier.tcc"

