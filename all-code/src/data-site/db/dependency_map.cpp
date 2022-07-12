#include "dependency_map.h"

#include <glog/logging.h>

#include "../../common/perf_tracking.h"

dependency_map::dependency_map() {}
dependency_map::~dependency_map() {}

void dependency_map::init( uint64_t partition_id, dependency_map* next_map,
                           const snapshot_vector& base_snapshot,
                           int32_t                num_records_in_chain ) {
    next_map_ = next_map;
#if 0 // HDB-MERGE_SNAPSHOTS
    base_map_ = base_snapshot;
#endif
    chain_.init( num_records_in_chain, partition_id, nullptr );
    if ( !base_snapshot.empty()) {
        store_dependency( partition_id,
                          get_snapshot_version( base_snapshot, partition_id ),
                          base_snapshot, num_records_in_chain, {}, false );
    }
}

dependency_map* dependency_map::store_dependency(
    uint64_t partition_id, uint64_t version, const snapshot_vector& snapshot,
    int32_t num_records_in_chain, const snapshot_vector& gc_lwm,
    int32_t always_gc ) {
    dependency_map* dep_map = this;

    bool assigned_new = false;

    if( chain_.is_chain_full() ) {
        // create a new dependency_map;
        assigned_new = true;
        dep_map = new dependency_map();
#if 0 // HDB-MERGE_SNAPSHOTS
        dep_map->init( partition_id, this, snapshot, num_records_in_chain );
#endif
        dep_map->init( partition_id, this, {}, num_records_in_chain );
    }

    dep_map->write_record_to_chain( version, snapshot );

    if( ( !gc_lwm.empty() ) and ( assigned_new or always_gc ) ) {
        DVLOG( 20 ) << "Created new chain, GC old chains, with lwm:" << gc_lwm;
        gc_snapshot( gc_lwm, partition_id );
    }

    return dep_map;
}


void dependency_map::gc_snapshot( const snapshot_vector& gc_lwm,
                                  uint64_t               partition_id ) {
    uint64_t        version = get_snapshot_version( gc_lwm, partition_id );
    dependency_map* dep_map = next_map_;
    dependency_map* dep_parent = this;
    bool            do_gc = false;

    while( dep_map != nullptr ) {
        row_record* r = dep_map->chain_.get_latest_row_record( false );
        if( r == nullptr ) {
            break;
        }

        uint64_t r_version = r->get_version();
        if( r_version < version ) {
          do_gc = true;
          break;
        }

        dep_parent = dep_map;
        dep_map = dep_map->next_map_;
    }

    (void) do_gc;
    (void) dep_parent;
#if 0 // HDB-OUT
    if( do_gc ) {
        dep_parent->next_map_ = nullptr;
        delete dep_map;
    }
#endif
}

void dependency_map::write_record_to_chain( uint64_t               version,
                                            const snapshot_vector& snapshot ) {
    snapshot_vector* snapshot_to_store = generate_snapshot_to_store( snapshot );
    row_record* r = chain_.create_new_row_record( version, snapshot_to_store );
    DCHECK( r );
    DCHECK_EQ( K_SNAPSHOT_VECTOR_RECORD_TYPE,
               r->get_row_data()->get_type() );
    }

void dependency_map::get_dependency( uint64_t partition_id, uint64_t version,
                                     snapshot_vector& snapshot_to_update ) {

    DVLOG( 40 ) << "Get dependency: partition_id:" << partition_id
                << ", version:" << version
                << ", snapshot_to_update:" << snapshot_to_update;

    snapshot_vector read_snapshot;
    read_snapshot[partition_id] = version;

    dependency_map* dep_map = this;
    bool found = false;
    uint64_t        high_version = 0;
    uint64_t        low_version = 0;
    while( dep_map != nullptr ) {
        low_version = 0;
#if 0 // HDB-MERGE_SNAPSHOTS
        low_version = get_snapshot_version( dep_map->base_map_, partition_id );
        high_version = low_version;
#endif
        row_record* r = dep_map->chain_.get_latest_row_record( false );
        if( r == nullptr ) {
#if 0 // HDB-MERGE_SNAPSHOTS
            found = ( low_version <= version ) and ( high_version >= version );
#endif
            found = false;
            break;
        }
        high_version = r->get_version();
#if 0 // HDB-MERGE_SNAPSHOTS
        if( ( low_version <= version ) and ( high_version >= version ) ) {
#endif
        if( high_version >= version ) {
            found = true;
            break;
        } else if( high_version < version ) {
            // went to far down the chain
            DVLOG( 10 ) << "Cannot find:" << version
                        << ", in dependency chain. [ this=" << this
                        << ", dep_map:" << dep_map
                        << ", high_version:" << high_version
                        << ", low_version:" << low_version << " ]";
            return;
        }
        // otherwise, low version should be > version

        dep_map = dep_map->next_map_;
    }

    if( !found ) {
        DVLOG( 5 ) << "Cannot find:" << version
                   << ", in dependency chain. [ this=" << this
                   << ", dep_map:" << dep_map << " ]";
        return;
    }

#if 0 // HDB-MERGE_SNAPSHOT
    if( low_version == version ) {
        snapshot_vector            delta_snapshot;
        merge_snapshots( snapshot_to_update, dep_map->base_map_,
                         delta_snapshot );
        merge_snapshots( snapshot_to_update, {}, delta_snapshot );

        return;
    }
#endif

    row_record* snapshot_rec = dep_map->chain_.find_row_record( read_snapshot );
    if( !snapshot_rec ) {
        // WARNING
        DVLOG( 50 ) << "Cannot find:" << version
                    << ", in dependency chain. [ this=" << this
                    << ", dep_map:" << dep_map << " ]";
        return;
    }

    DCHECK_EQ( K_SNAPSHOT_VECTOR_RECORD_TYPE,
               snapshot_rec->get_row_data()->get_type() );
    snapshot_vector* found_snapshot =
        (snapshot_vector*) snapshot_rec->get_row_data()->get_pointer_data();
#if 0 // HDB-MERGE_SNAPSHOT
    merge_snapshots( snapshot_to_update, dep_map->base_map_, *found_snapshot );
#endif
    merge_snapshots( snapshot_to_update, *found_snapshot );
}

void merge_snapshots( snapshot_vector&       snapshot_to_update,
                      const snapshot_vector& delta_snapshot ) {
    start_extra_timer( MERGE_SNAPSHOTS_TIMER_ID );

    DVLOG( 40 ) << "merge_snapshots, snapshot_to_update:" << snapshot_to_update
                << ", delta_snapshot:" << delta_snapshot;

    // iterate over the keys
    auto to_update_iter = snapshot_to_update.begin();
    auto delta_iter = delta_snapshot.cbegin();

    while( to_update_iter != snapshot_to_update.cend() ) {
        // until we hit the end of the snapshot we are updating
        if( delta_iter == delta_snapshot.cend() ) {
          // if we have nothing left to add, break
            break;
        }

        if ( to_update_iter->first == delta_iter->first) {
            // both are present, update if smaller
            if( to_update_iter->second < delta_iter->second ) {
                to_update_iter->second = delta_iter->second;
            }
        } else {
            // look up the key, so we skip ahead
            to_update_iter = snapshot_to_update.find( delta_iter->first );
            if ( to_update_iter == snapshot_to_update.cend()) {
                // it's not present so add it
                auto ret = snapshot_to_update.emplace( delta_iter->first,
                                                       delta_iter->second );
                to_update_iter = ret.first;
            } else if( to_update_iter->second < delta_iter->second ) {
                // it is present, and it's too small
                to_update_iter->second = delta_iter->second;
            }
        }
        // bump the iters forward
        to_update_iter++;
        delta_iter++;
    }
    if( delta_iter != delta_snapshot.cend() ) {
        // if there is anything else bulk insert
        snapshot_to_update.insert( delta_iter, delta_snapshot.cend() );
    }

    DVLOG( 40 ) << "merge_snapshots, snapshot_to_update:" << snapshot_to_update
                << ", delta_snapshot:" << delta_snapshot << ", done!";

    stop_extra_timer( MERGE_SNAPSHOTS_TIMER_ID );
}

#if 0
void merge_snapshots( snapshot_vector&       snapshot_to_update,
                      const snapshot_vector& base_snapshot,
                      const snapshot_vector& delta_snapshot ) {
    start_timer( MERGE_SNAPSHOTS_TIMER_ID );

    DVLOG( 40 ) << "update_snapshot, snapshot_to_update:" << snapshot_to_update
                << ", base_snapshot:" << base_snapshot
                << ", delta_snapshot:" << delta_snapshot;
    auto base_iter = base_snapshot.cbegin();
    auto delta_iter = delta_snapshot.cbegin();

    // iterate over all the results
    while( ( base_iter != base_snapshot.cend() ) or
           ( delta_iter != delta_snapshot.cend() ) ) {
        bool advance_base = false;
        bool advance_delta = false;
        // if an iterator is at the end, then the other one will always
        // be advanced
        if( base_iter == base_snapshot.cend() ) {
            advance_delta = true;
        }
        if( delta_iter == delta_snapshot.cend() ) {
            advance_base = true;
        }

        if( !advance_base and !advance_delta ) {
            // both iterators have values
            uint64_t b_id = base_iter->first;
            uint64_t b_version = base_iter->second;
            uint64_t d_id = delta_iter->first;
            uint64_t d_version = delta_iter->second;

            // compare partition ids
            if( b_id == d_id ) {
              // if both have the same partition id, then the delta will
              // always have the largest value, but we advance both iterators
              advance_base = true;
              advance_delta = true;

              set_snapshot_version_if_larger( snapshot_to_update, b_id, b_version );
            } else if( b_id < d_id ) {
                // if the base is lower, than set if larger
                advance_base = true;
                set_snapshot_version_if_larger( snapshot_to_update, b_id, b_version );
            } else {
                // otherwise the delta is lower, so set that value.
                advance_delta = true;
                set_snapshot_version_if_larger( snapshot_to_update, d_id, d_version );
            }
        } else if( advance_base ) {
            // if only base has values, then set the version
            uint64_t id = base_iter->first;
            uint64_t version = base_iter->second;
            set_snapshot_version_if_larger( snapshot_to_update, id, version );
        } else if( advance_delta ) {
            // if only delta has values, then set the version
            uint64_t id = delta_iter->first;
            uint64_t version = delta_iter->second;
            set_snapshot_version_if_larger( snapshot_to_update, id, version );
        }
        // advance the iterators
        if( advance_base ) {
            std::advance( base_iter, 1 );
        }
        if( advance_delta ) {
            std::advance( delta_iter, 1 );
        }
    }
    stop_timer( MERGE_SNAPSHOTS_TIMER_ID );
}
#endif

snapshot_vector* dependency_map::generate_snapshot_to_store(
    const snapshot_vector& snapshot ) {
    // do not free this pointer it's going to a record
    snapshot_vector* to_store = new snapshot_vector( snapshot );
#if 0 // HDB-MERGE_SNAPSHOT
    snapshot_vector* to_store = new snapshot_vector( );
    for( const auto& snap_iter : snapshot ) {
        uint64_t id = snap_iter.first;
        uint64_t version = snap_iter.second;

        uint64_t base_version = get_snapshot_version( base_map_, id );
        if (base_version != version) {
            set_snapshot_version( *to_store, id, version );
        }
    }
#endif // HDB-MERGE_SNAPSHOT

    return to_store;
}


