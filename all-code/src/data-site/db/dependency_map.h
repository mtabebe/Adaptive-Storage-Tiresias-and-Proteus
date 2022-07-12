#pragma once

#include "../../common/snapshot_vector_funcs.h"
#include "mvcc_chain.h"

class dependency_map {
   public:
    dependency_map();
    ~dependency_map();

    void init( uint64_t partition_id, dependency_map* next_map,
               const snapshot_vector& base_snapshot,
               int32_t                num_records_in_chain );

    dependency_map* store_dependency( uint64_t partition_id, uint64_t version,
                                      const snapshot_vector& snapshot,
                                      int32_t num_records_in_chain,
                                      const snapshot_vector& gc_lwm,
                                      int32_t                always_gc );
    void get_dependency( uint64_t partition_id, uint64_t version,
                         snapshot_vector& snapshot_to_update );

   private:
    void gc_snapshot( const snapshot_vector& gc_lwm, uint64_t partition_id );
    void write_record_to_chain( uint64_t               version,
                                const snapshot_vector& snapshot );
    snapshot_vector* generate_snapshot_to_store(
        const snapshot_vector& snapshot );

    dependency_map* next_map_;

    mvcc_chain      chain_;
    snapshot_vector base_map_;
};
#if 0
void merge_snapshots( snapshot_vector&       snapshot_to_update,
                      const snapshot_vector& base_snapshot,
                      const snapshot_vector& delta_snapshot );
#endif

void merge_snapshots( snapshot_vector&       snapshot_to_update,
                      const snapshot_vector& delta_snapshot );

