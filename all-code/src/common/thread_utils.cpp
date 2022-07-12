#include "thread_utils.h"

#include <errno.h>
#include <glog/logging.h>

void set_thread_priority( std::thread& t, int policy, int priority ) {
    DVLOG( 20 ) << "Setting thread:" << t.get_id()
                << " to use policy:" << policy << ", and priority:" << priority;
    sched_param sch_params;
    sch_params.sched_priority = priority;
    // implicit assumption we are using pthreads
    int ret_code =
        pthread_setschedparam( t.native_handle(), policy, &sch_params );
    if( ret_code ) {
        LOG( WARNING ) << "Failed to set thread policy and priority : " << errno
                       << ", " << strerror( errno );
    }
}

void join_threads( std::vector<std::thread>& threads ) {
    for( auto& thread : threads ) {
        join_thread( thread );
    }
}

void join_thread( std::thread& thread ) {
    try {
        if( thread.joinable() ) {
            thread.join();
        }
    } catch( std::exception& ex ) {
        LOG( WARNING ) << "Error joining thread:" << ex.what();
    }
}

void thread_sleep_or_yield(
    const std::chrono::high_resolution_clock::time_point& start,
    const std::chrono::high_resolution_clock::time_point& end, std::mutex& mut,
    std::condition_variable& cv, uint32_t sleep_time ) {

    std::chrono::milliseconds base_sleep_time( sleep_time );
    std::chrono::milliseconds elapsed_time =
        std::chrono::duration_cast<std::chrono::milliseconds>( end - start );

    if( elapsed_time < base_sleep_time ) {
        std::chrono::milliseconds sleep_time = base_sleep_time - elapsed_time;
        std::unique_lock<std::mutex> lock( mut );
        cv.wait_for( lock, sleep_time );
    } else {
        // yield
        std::this_thread::yield();
    }
}
