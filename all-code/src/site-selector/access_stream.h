#pragma once

#include "adaptive_reservoir_sampler.h"
#include "partition_access.h"
#include <chrono>
#include <folly/ProducerConsumerQueue.h>
#include <thread>

typedef std::chrono::time_point<std::chrono::high_resolution_clock>
    high_res_time;

struct access_stream_record {

    access_stream_record( transaction_partition_accesses txn_partition_accesses, high_res_time time, bool is_start_point )
        : txn_partition_accesses_( txn_partition_accesses ), time_( time ), is_start_point_( is_start_point ) {}

    access_stream_record( const access_stream_record &o )
        : txn_partition_accesses_( o.txn_partition_accesses_ ), time_( o.time_ ), is_start_point_( o.is_start_point_ ) {}

    transaction_partition_accesses txn_partition_accesses_;
    high_res_time                  time_;
    bool                           is_start_point_;
};

class access_stream {
   public:
    ~access_stream() {
        shutdown_ = true;
        access_stream_thread_.join();
    }

    // max_queue_size indicates how big the queue can grow before it is full
    // This should be bigger than the maximum number of transactions that can
    // execute in the tracking interval, unless you don't mind missing some
    // tracking records
    access_stream( uint64_t max_queue_size, adaptive_reservoir_sampler *sampler,
                   std::chrono::milliseconds empty_queue_sleep_time,
                   std::chrono::milliseconds tracking_interval )
        : guard_(),
          stream_( max_queue_size ),
          sampler_( sampler ),
          empty_queue_sleep_time_( empty_queue_sleep_time ),
          tracking_interval_( tracking_interval ),
          shutdown_( false ),
          access_stream_thread_( &access_stream::access_stream_thread_func,
                                 this ) {}

    // Write the transaction_partition_accesses to the stream with the start
    // being txn_ts is start point indicates if we are sampling this, or if it
    // is just being stored for another sample
    void write_access_to_stream(
        transaction_partition_accesses &txn_partition_accesses,
        high_res_time txn_ts, bool is_start_point );

    inline std::thread *get_thread_ptr() { return &access_stream_thread_; }

   private:
    // The consumer thread processes the next record, building connections in
    // the hash table
    // that are used to calculate probabilistic bucket transitions
    void process_next_access_record();

    // A function that repeatedly calls process_next_access_record
    void access_stream_thread_func();

    // Grab the next start record, return false if we have none
    bool get_start_record( access_stream_record **wr );

    // Spin until enough time has elapsed, then fill the in_flight_records
    // buffer with
    // all related records
    void fill_in_flight_buffer( access_stream_record *wr );

    // ProducerConsumerQueue is a one producer and one consumer queue without
    // locks
    // Now we have MULTIPLE producers, but a SINGLE consumer. Furthermore the
    // folly
    // queue is such that the producer and consumer are independent. Therefore
    // we
    // can safely lock on writes, but avoid locking on reads.
    // If you get seg faults with corrupt memory addresses for the write record
    // stat keys that come out of this code, you should probably take a look at
    // this code and check for culprits.

    std::mutex guard_;

    // The main queue, which provides fast and atomic inserts/reads
    folly::ProducerConsumerQueue<access_stream_record> stream_;

    // Since we can't access the internal state of the queue, we pull off a set
    // of records
    // representing access_sets for transactions executed within
    // tracking_interval of the record
    // we are currently processing
    std::list<access_stream_record *> in_flight_records_;

    adaptive_reservoir_sampler *sampler_;

    // Time to sleep if the queue is empty
    std::chrono::milliseconds empty_queue_sleep_time_;

    // The duration that we must look forward in the queue to find "related"
    // access-sets
    std::chrono::milliseconds tracking_interval_;

    // indicates that we are shutting down, and that we should join the thread
    // Non-volatile because we don't need to immediately see the value, even
    // though we
    // are "sort-of" spinning on it
    bool shutdown_;

    std::thread access_stream_thread_;
};
