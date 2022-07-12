#include "thread_holders.h"

#include "../common/thread_utils.h"

thread_holder::thread_holder()
    : cv_(),
      lock_(),
      done_( false ),
      joined_( false ),
      worker_thread_( nullptr ) {}
thread_holder::~thread_holder() { wait_for_completion(); }

bool thread_holder::is_complete() {
  bool complete = false;
  {
      std::unique_lock<std::mutex> guard( lock_ );
      complete = done_;
  }
  return complete;
}

void thread_holder::wait_for_completion() {
  std::thread* t;
    {
        std::unique_lock<std::mutex> guard( lock_ );
        if( joined_ ) {
            // wait for it to finish
            while( !done_ ) {
                cv_.wait( guard );
            }

            return;
        }
        t = worker_thread_.get();
        joined_ = true;
    }

    if( t ) {
        join_thread( *t );
    }
    {
        std::unique_lock<std::mutex> guard( lock_ );
        worker_thread_ = nullptr;
    }
}

void thread_holder::mark_as_done() {
    {
        std::unique_lock<std::mutex> guard( lock_ );
        done_ = true;
    }

    cv_.notify_all();
}

void thread_holder::set_thread( std::unique_ptr<std::thread> t ) {
    {
        std::unique_lock<std::mutex> guard( lock_ );
        worker_thread_ = std::move( t );
    }
}

thread_holders::thread_holders() : lock_(), workers_() {}
thread_holders::~thread_holders() { wait_for_all_to_complete(); }

void thread_holders::add_holder( thread_holder* holder ) {
    {
        std::unique_lock<std::mutex> guard( lock_ );
        workers_.push_back( holder );
    }
}

void thread_holders::wait_for_all_to_complete() {
    {
        std::unique_lock<std::mutex> guard( lock_ );
        while( !workers_.empty() ) {
            thread_holder* holder = workers_.front();
            workers_.pop_front();
            holder->wait_for_completion();
            delete holder;
        }
    }
}
int thread_holders::gc_inactive_threads() {
    int ret = 0;

    std::list<thread_holder*> original_holders;
    {
        // swap out the list
        std::unique_lock<std::mutex> guard( lock_ );
        workers_.swap( original_holders );
    }
    std::list<thread_holder*> holders_to_keep;
    // delete things that are joinable
    while( !original_holders.empty() ) {
        thread_holder* holder = original_holders.front();
        original_holders.pop_front();
        if( holder->is_complete()) {
          holder->wait_for_completion();
          delete holder;
          ret += 1;
        } else {
            holders_to_keep.push_back( holder );
        }
    }

    {
        // add the others back to the list
        std::unique_lock<std::mutex> guard( lock_ );
        workers_.splice( workers_.begin(), holders_to_keep );
    }

    return ret;
}

