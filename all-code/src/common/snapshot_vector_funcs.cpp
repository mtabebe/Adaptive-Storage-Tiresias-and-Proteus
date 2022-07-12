#include "snapshot_vector_funcs.h"
#include "partition_funcs.h"

uint64_t get_snapshot_version( const snapshot_vector& snapshot,
                               uint64_t               partition_id ) {

    uint64_t    version = 0;
    const auto& found = snapshot.find( partition_id );
    if( found != snapshot.end() ) {
        version = found->second;
    }
    return version;
}

uint64_t get_snapshot_version( const snapshot_vector&      snapshot,
                               const partition_identifier& pid ) {

    return get_snapshot_version( snapshot,
                                 partition_identifier_key_hasher{}( pid ) );
}
uint64_t get_snapshot_version( const snapshot_vector&             snapshot,
                               const partition_column_identifier& pcid ) {

    return get_snapshot_version(
        snapshot, partition_column_identifier_key_hasher{}( pcid ) );
}

void set_snapshot_version( snapshot_vector& snapshot, uint64_t partition_id,
                           uint64_t version ) {
    if( version > 0 ) {
        snapshot[partition_id] = version;
    }
}
void set_snapshot_version( snapshot_vector&            snapshot,
                           const partition_identifier& pid, uint64_t version ) {
    set_snapshot_version( snapshot, partition_identifier_key_hasher{}( pid ),
                          version );
}
void set_snapshot_version( snapshot_vector&                   snapshot,
                           const partition_column_identifier& pcid,
                           uint64_t                           version ) {
    set_snapshot_version(
        snapshot, partition_column_identifier_key_hasher{}( pcid ), version );
}

void set_snapshot_version_if_larger( snapshot_vector& snapshot,
                                     uint64_t partition_id, uint64_t version ) {
    uint64_t s_version = get_snapshot_version( snapshot, partition_id );
    if ( version > s_version) {
        set_snapshot_version( snapshot, partition_id, version );
    }
}

void merge_snapshot_versions_if_larger(
    snapshot_vector& snapshot, const snapshot_vector& snapshot_to_merge ) {
    for( const auto& entry : snapshot_to_merge ) {
        set_snapshot_version_if_larger( snapshot, entry.first, entry.second );
    }
}
