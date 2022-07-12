#pragma once

#include "../../common/hw.h"
#include "../../common/snapshot_vector_funcs.h"

enum row_record_creation_type {
    TXN_STATE,
    VERSION,
    SNAPSHOT_VEC,
};

struct snapshot_vector_pointer_and_version_holder {
    uint64_t         version_;
    snapshot_vector* vector_ptr_;
};

