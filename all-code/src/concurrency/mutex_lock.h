#pragma once

#include <mutex>

#include "lock_interface.h"

// mutexes don't support lock and unlock across threads.
class mutex_lock : public lock_interface {
   public:
    mutex_lock();
    ~mutex_lock();

    bool lock();
    bool unlock();

   private:
    std::mutex mutex_;
};
