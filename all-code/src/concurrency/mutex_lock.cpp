#include "mutex_lock.h"

#include <glog/logging.h>

mutex_lock::mutex_lock() : mutex_() {}
mutex_lock::~mutex_lock() {}

bool mutex_lock::lock() {
    mutex_.lock();
    return true;
}

bool mutex_lock::unlock() {
    mutex_.unlock();
    return true;
}
