#pragma once

#include <condition_variable>
#include <mutex>

#include "lock_interface.h"

// Semaphores support locks and unlocks from multiple threads
class semaphore : public lock_interface {
   public:
    semaphore();
    ~semaphore();

    bool lock();
    bool try_lock();
    bool unlock();

   private:
    std::mutex              mutex_;
    std::condition_variable cv_;

    bool    lockState_;
    int32_t owner_;
};
