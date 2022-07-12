#include "spinlock.h"

#include <glog/logging.h>

spinlock::spinlock() : ticket_( 0 ), serving_( 0 ) {}

spinlock::~spinlock() {}

bool spinlock::lock() {
    // we only need this instruction to be atomic with respect to the ticket
    uint64_t my_ticket = ticket_.fetch_add( 1, std::memory_order_relaxed );
    uint64_t now_serving;

    // proportional backoff

    while( true ) {
        // prevent re-ordering with the ticket (all reads and writes in current
        // thread
        // are guaranteed to be ordered)
        // if all writes to serving_ are with release then we know that the load
        // will see the correct state
        now_serving = serving_.load( std::memory_order_acquire );
        if( now_serving == my_ticket ) {
            return true;
        }

        uint64_t wait_len = my_ticket - now_serving;
        uint64_t wait_iters = k_spinlock_back_off_duration * wait_len;
        for( uint64_t i = 0; i < wait_iters; i++ ) {
            CPU_RELAX();
        }
    }

    return false;
}

bool spinlock::unlock() {
    // the linked ticket lock actually separates this into a load that is
    // relaxes
    // and a store that is release semantics. Here we simply use the
    // acquire-release
    // semantics.
    uint64_t current_serve = serving_.load( std::memory_order_relaxed );
    serving_.store( current_serve + 1, std::memory_order_release );
    return true;
}
