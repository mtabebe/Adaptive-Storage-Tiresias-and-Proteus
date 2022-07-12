#include "transaction_state.h"

#include <glog/logging.h>

static_assert( sizeof( transaction_state ) == 8,
               "Size of transaction_state should be 8 bytes" );

transaction_state::transaction_state()
    : version_state_( packed_pointer_ops::set_packed_pointer_int(
          0, K_NOT_COMMITTED_FLAG ) ) {}

transaction_state::~transaction_state() {}

void transaction_state::set_not_committed() {
    version_state_.store(
        packed_pointer_ops::set_packed_pointer_int( 0, K_NOT_COMMITTED_FLAG ),
        std::memory_order_release );
}
void transaction_state::set_committing() {
    version_state_.store(
        packed_pointer_ops::set_packed_pointer_int( 0, K_IN_COMMIT_FLAG ),
        std::memory_order_release );
}
void transaction_state::set_aborted() {
    version_state_.store(
        packed_pointer_ops::set_packed_pointer_int( 0, K_ABORTED_FLAG ),
        std::memory_order_release );
}

void transaction_state::set_committed( uint64_t version ) {
    DCHECK_LT( version, K_COMMITTED );
    version_state_.store(
        packed_pointer_ops::set_packed_pointer_int( version, K_COMMITTED_FLAG ),
        std::memory_order_release );
}

uint64_t transaction_state::get_version() const {
    packed_pointer s = version_state_.load( std::memory_order_acquire );
    uint16_t       flags = packed_pointer_ops::get_flag_bits( s );
    uint64_t       ret = K_2_47 + flags;
    if( flags == K_COMMITTED_FLAG ) {
        ret = packed_pointer_ops::get_int( s );
    }
    return ret;
}
