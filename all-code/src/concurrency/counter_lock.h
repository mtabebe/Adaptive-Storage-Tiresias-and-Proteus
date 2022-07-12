#pragma once
#include "../common/constants.h"
#include "../common/hw.h"
#include <condition_variable>
#include <memory>
#include <mutex>

struct counter_lock_cv_entry {
    std::condition_variable cv_;
    uint64_t                value_to_wait_for_;  // 0 not valid
};

// Locks to ensure that a certain version has been reached before
// allowing transaction execution
class alignas( CACHELINE_SIZE ) counter_lock {
    std::mutex                                          mut_;
    uint64_t                                            value_;
    std::vector<std::unique_ptr<counter_lock_cv_entry>> cv_slab_;

   public:
    counter_lock();

    // update counter
    uint64_t update_counter( uint64_t new_count );
    uint64_t update_counter_if_less_than( uint64_t new_count );

    // increment counter
    uint64_t incr_counter();

    // Return the current counter value
    uint64_t get_counter();
    // no lock
    uint64_t get_counter_no_lock();

    // Wait until the value has been reached
    uint64_t wait_until( uint64_t count );

    void wake_up_waiters();
};
