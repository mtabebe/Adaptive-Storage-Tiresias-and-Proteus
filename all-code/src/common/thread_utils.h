#pragma once

#include <condition_variable>
#include <mutex>
#include <pthread.h>
#include <thread>
#include <vector>

void set_thread_priority( std::thread& t, int policy, int priority );
void join_threads( std::vector<std::thread>& threads );
void join_thread( std::thread& thread );

void thread_sleep_or_yield(
    const std::chrono::high_resolution_clock::time_point& start,
    const std::chrono::high_resolution_clock::time_point& end, std::mutex& mut,
    std::condition_variable& cv, uint32_t sleep_time );
