#include "client_state_information.h"

#include <numeric>

#include <glog/logging.h>

client_state_information::client_state_information()
    : client_partition_holders_(),
      site_identifier_( 0 )
#if 0 // HDB-OUT
      active_clients_(),
      client_session_state_(),
      client_low_watermark_(),
      per_client_partitions_(),
      per_client_write_sets_(),
      computed_low_watermark_(),
      new_watermark_tmp_()
#endif
{}

client_state_information::~client_state_information() {
    // TODO free up inflight changes?
    // client_session_state_.clear();
}

void client_state_information::init( uint64_t num_clients,
                                     uint32_t site_identifier ) {
    site_identifier_ = site_identifier;
    client_partition_holders_ =
        std::vector<transaction_partition_holder*>( num_clients );

#if 0  // HDB-OUT
    active_clients_ = std::vector<std::atomic<uint32_t>>( num_clients );
    client_session_state_.reserve( num_clients );
    per_client_partitions_.reserve( num_clients );
    per_client_write_sets_.reserve( num_clients );
    computed_low_watermark_.assign( num_sites, 0 );
#endif

    DVLOG( 10 ) << "Initializing client state information  for:" << num_clients
                << " clients";

    for( uint64_t client_id = 0; client_id < num_clients; client_id++ ) {
        client_partition_holders_.emplace_back( nullptr );
#if 0 // HDB-OUT

        // construct the internal array of all 0s
        client_session_state_.emplace_back( num_sites_, 0 );
        client_low_watermark_.emplace_back( num_sites_, 0 );
        per_client_partitions_.emplace_back();
        per_client_write_sets_.emplace_back();
#endif
    }
}

#if 0 // HDB-OUT
client_version_vector client_state_information::get_low_watermark(
    std::vector<counter_lock>* site_version_vector ) const {

    DCHECK_EQ( num_sites_, site_version_vector->size() );

    //Compute this aside so that we can max the old vector with it.
    new_watermark_tmp_.assign( num_sites_, UINT64_MAX );

    uint64_t num_updates_seen = 0;
    // check the site version vector for a lower bound on what new transactions
    // will see
    for( uint32_t site_id = 0; site_id < num_sites_; site_id++ ) {
        uint64_t counter_val = site_version_vector->at( site_id ).get_counter();
        new_watermark_tmp_.at( site_id ) = counter_val;
        num_updates_seen += counter_val;
    }
    if( num_updates_seen == 0 ) {
        //No transactions observed, can't GC
        //return max vector
        //Don't store this because when we max it we will never GC
        return new_watermark_tmp_;
    }

    uint32_t active = 0;
    for( uint32_t client_id = 0; client_id < client_session_state_.size();
         client_id++ ) {
        // if they are not active they will get fast forwarded to our site
        // version vector
        active =
            active_clients_.at( client_id ).load( std::memory_order_acquire );
        if( active == K_INACTIVE_CLIENT ) {
            continue;  // they are going to be fast forwarded

        // Update propagator
        // Do not GC things we haven't pushed to Kafka
        } else if( active == K_MAX_SESSION_CLIENT ) {
            LOG(FATAL) << "Shouldn't be seeing K_MAX_SESSION_CLIENT";
            continue;
        }

        //Active Client
        //TODO: SIMD with many sites?
        for( uint32_t site = 0; site < num_sites_; site++ ) {
            // Version this client sees
            uint64_t v = client_session_state_.at( client_id ).at( site );
            // What the counter locks think we have
            uint64_t cur = new_watermark_tmp_.at( site );
            // Can't GC anything the client may see
            new_watermark_tmp_.at( site ) = std::min( v, cur );
        }
    }

    // If a previously inactive client has come back after a long time and
    // has not read determined their new session, the watermark we compute
    // here may be lower than a watermark we have computed before because
    // we have stored their old session as their active session.
    // This is OK because the client will figure out their true session from
    // the counter locks and update their active session, so they won't read
    // things we have GC'd before. However, it is unintuitive to see a watermark
    // move backward, so we min each index with what we GC'd before to compute
    // the true GC timestamp.
    // TODO: SIMD?
    for( uint32_t site = 0; site < num_sites_; site++ ) {
        computed_low_watermark_.at( site ) = std::max( computed_low_watermark_.at( site ), new_watermark_tmp_.at( site ) );
    }

    return computed_low_watermark_;
}
#endif
