#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <thread>
#include <thread>

class thread_holder {
   public:
    thread_holder();
    ~thread_holder();

    void set_thread( std::unique_ptr<std::thread> t );

    void mark_as_done();

    void wait_for_completion();
    bool is_complete();

   private:
    std::condition_variable      cv_;
    std::mutex                   lock_;

    bool                         done_;
    bool                         joined_;

    std::unique_ptr<std::thread> worker_thread_;
};

class thread_holders {
   public:
    thread_holders();
    ~thread_holders();

    void add_holder( thread_holder* holder );

    void wait_for_all_to_complete();
    int gc_inactive_threads();

   private:

    std::mutex                lock_;
    std::list<thread_holder*> workers_;
};
