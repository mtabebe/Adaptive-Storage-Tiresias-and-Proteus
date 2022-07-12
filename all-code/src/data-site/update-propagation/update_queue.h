#pragma once

#include <condition_variable>
#include <mutex>
#include <queue>
#include <vector>

#include "write_buffer_serialization.h"

struct stashed_update {
   public:
    uint64_t             commit_version_;
    serialized_update    serialized_;
    deserialized_update* deserialized_;
};

struct stashed_statistics {
  public:
   uint32_t num_stashed_;
   bool     is_partition_operation_;
};

struct stashed_update_comparator {
    bool operator()( const stashed_update& l, const stashed_update& r ) {
        // these do the reverse
        return ( l.commit_version_ > r.commit_version_ );
    }
};

void destroy_stashed_update( stashed_update& stashed );
stashed_update create_stashed_update( const serialized_update& update );

class update_queue {
   public:
    update_queue();
    ~update_queue();

    uint32_t add_update( stashed_update&& update );
    stashed_update get_next_update( bool     wait_for_update = true,
                                    uint64_t req_version = K_NOT_COMMITTED,
                                    uint64_t current_version = 0 );
    void set_next_expected_version( uint64_t expected_version );

    void add_dummy_op( uint64_t version );

   private:
    uint64_t expected_next_update_;
    std::priority_queue<stashed_update, std::vector<stashed_update>,
                        stashed_update_comparator>
                            stashed_updates_;
    std::mutex              lock_;
    std::condition_variable cv_;
};
