#pragma once

#include <glog/logging.h>

const static uint16_t K_IS_PTR = 0;
const static uint16_t K_IS_INT = 1;

const static uint32_t K_IS_REGULAR_RECORD = 0;
const static uint32_t K_IS_TOMBSTONE = 1;

inline void row_record::set_transaction_state( transaction_state* txn_state_ptr ) {
    // So this sets the transaction state PTR for the current transaction
    mvcc_ptr_.store( packed_pointer_ops::set_packed_pointer_ptr(
                         (void*) txn_state_ptr, K_IS_PTR ),
                     std::memory_order_release );
}

inline void row_record::set_version( uint64_t version ) {
    DCHECK_LT( version, K_COMMITTED );
    mvcc_ptr_.store(
        packed_pointer_ops::set_packed_pointer_int( version, K_IS_INT ),
        std::memory_order_release );
}



