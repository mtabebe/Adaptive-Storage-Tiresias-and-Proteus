#pragma once

#include "../gen-cpp/gen-cpp/dynamic_mastering_types.h"

uint64_t get_snapshot_version( const snapshot_vector& snapshot,
                               uint64_t               partition_id );
uint64_t get_snapshot_version( const snapshot_vector&      snapshot,
                               const partition_identifier& pid );
uint64_t get_snapshot_version( const snapshot_vector&             snapshot,
                               const partition_column_identifier& pcid );

void set_snapshot_version( snapshot_vector& snapshot, uint64_t partition_id,
                           uint64_t version );
void set_snapshot_version( snapshot_vector&            snapshot,
                           const partition_identifier& pid, uint64_t version );
void set_snapshot_version( snapshot_vector&                   snapshot,
                           const partition_column_identifier& pcid,
                           uint64_t                           version );

void set_snapshot_version_if_larger( snapshot_vector& snapshot,
                                     uint64_t partition_id, uint64_t version );

void merge_snapshot_versions_if_larger(
    snapshot_vector& snapshot, const snapshot_vector& snapshot_to_merge );
