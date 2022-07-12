#pragma once

#include <atomic>

#include "../common/constants.h"
#include "../common/hw.h"
#include "lock_interface.h"

// taken from https://geidav.wordpress.com/2016/04/09/the-ticket-spinlock/
class spinlock : public lock_interface {
   public:
    spinlock();
    ~spinlock();

    bool lock();
    bool unlock();

   private:
    // separate on cache lines to prevent false sharing
    // this increases the size of the object but it seems like a reasonable
    // tradeoff
    alignas( CACHELINE_SIZE ) std::atomic<uint64_t> ticket_;
    alignas( CACHELINE_SIZE ) std::atomic<uint64_t> serving_;
};
