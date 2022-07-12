#include "update_spinner.h"
#include <glog/logging.h>

uint64_t spin_or_yield_until( const uint64_t &             desired,
                              const std::atomic<uint64_t> &value ) {
POST_YIELD:
    uint64_t c = value.load( std::memory_order_acquire );
    if( c >= desired ) {
        return c;
    }

    if( should_yield( desired - c ) ) {
        std::this_thread::yield();
        goto POST_YIELD;
    } else {
        uint64_t loops = 0;
        c = value.load( std::memory_order_acquire );
        while( c < desired ) {
            c = value.load( std::memory_order_acquire );
            if( ++loops > get_num_loops_before_backoff() ) {
                std::this_thread::yield();
                goto POST_YIELD;
            }
        }
    }
    DCHECK( c >= desired );
    return c;
}
