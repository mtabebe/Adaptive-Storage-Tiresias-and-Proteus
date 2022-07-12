#include "semaphore.h"

#include <glog/logging.h>

#include "../common/constants.h"
#include "../common/perf_tracking.h"

semaphore::semaphore() : mutex_(), cv_(), lockState_( false ), owner_(0) {}
semaphore::~semaphore() {}

bool semaphore::lock() {
    start_extra_timer( SEMAPHORE_LOCK_TIMER_ID );

    DVLOG( 40 ) << "lock semaphore:" << this;

    std::chrono::high_resolution_clock::time_point start_point =
        std::chrono::high_resolution_clock::now();

    std::chrono::high_resolution_clock::time_point end_point = start_point;
    std::chrono::duration<double, std::nano> elapsed = end_point - start_point;
    int32_t prev_owner = 0;

    {
        std::unique_lock<std::mutex> lock( mutex_ );
        while( lockState_ ) {
            DVLOG( 40 ) << "waiting on lock";
            if( k_timer_log_level_on ) {

                auto ret = cv_.wait_for(
                    lock, std::chrono::nanoseconds(
                              (uint64_t) k_slow_timer_log_time_threshold ) );
                end_point = std::chrono::high_resolution_clock::now();
                elapsed = end_point - start_point;

                if( !lockState_ ) {
                    prev_owner = owner_;
                }

                if( ret == std::cv_status::timeout ) {
                    VLOG( k_timer_log_level )
                        << "Timed out waiting for semaphore:" << this
                        << ", have waited:" << elapsed.count()
                        << " , so far, prev_owner:" << prev_owner;
                }

            } else {
                cv_.wait( lock );
                end_point = std::chrono::high_resolution_clock::now();
                elapsed = end_point - start_point;
            }
        }
        DCHECK( !lockState_ );
        lockState_ = true;
        owner_ = get_thread_id();
    }

    if( elapsed.count() > k_slow_timer_log_time_threshold ) {
        VLOG( k_slow_timer_error_log_level )
            << "Locked: semaphore:" << this
            << ", took long time:" << elapsed.count()
            << ", prev_owner:" << prev_owner;
    }



    DVLOG( 40 ) << "Got lock";

    stop_extra_timer( SEMAPHORE_LOCK_TIMER_ID );

    return true;
}

bool semaphore::try_lock() {
    start_extra_timer( SEMAPHORE_TRY_LOCK_TIMER_ID );

    DVLOG( 40 ) << "try lock semaphore:" << this;
    bool got_lock = false;
    {
        std::unique_lock<std::mutex> lock( mutex_ );
        got_lock = !lockState_;
        if (got_lock) {
            lockState_ = true;
            owner_ = get_thread_id();
        }
    }
    DVLOG( 40 ) << "try lock:" << got_lock;

    stop_extra_timer( SEMAPHORE_TRY_LOCK_TIMER_ID );
    return got_lock;
}

bool semaphore::unlock() {

    DVLOG( 40 ) << "Unlock semaphore" << this;
    {
        std::unique_lock<std::mutex> lock( mutex_ );
        DVLOG( 40 ) << "Got internal mutex_";
        DCHECK( lockState_ );

        lockState_ = false;
        owner_ = 0;
    }
    DVLOG( 40 ) << "notifying";
    cv_.notify_one();

    DVLOG( 40 ) << "unlocked";
    return true;
}


