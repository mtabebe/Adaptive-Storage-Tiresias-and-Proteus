#pragma once

#include "../../common/partition_funcs.h"
#include "../../concurrency/counter_lock.h"
#include "dependency_map.h"

class partition_dependency {
  public:
    partition_dependency();
    ~partition_dependency();

    void init( uint64_t partition_id, uint64_t version,
               const snapshot_vector& base_snapshot,
               int32_t                num_records_in_chain );

    void store_dependency( const snapshot_vector& snapshot,
                           int32_t                num_records_in_chain,
                           const snapshot_vector& gc_lwm,
                           int32_t always_gc = k_always_gc_on_writes );

    void set_version( uint64_t version );
    uint64_t update_version_if_less_than( uint64_t new_version );

    uint64_t get_version();
    uint64_t get_version_no_lock();

    void make_version_visible( uint64_t expected_version );
    uint64_t wait_until( uint64_t version );

    void get_dependency( snapshot_vector& snapshot );

   private:
    counter_lock                 version_counter_;
    dependency_map*              dependencies_;
    uint64_t                     partition_id_;
};
