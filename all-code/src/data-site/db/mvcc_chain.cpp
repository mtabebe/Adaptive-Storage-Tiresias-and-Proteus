#include "mvcc_chain.h"

#include <glog/logging.h>
#include <glog/stl_logging.h>

static_assert( sizeof( mvcc_chain ) == 32, "mvcc_chain should be 32 bytes" );

mvcc_chain::mvcc_chain()
    : row_record_len_( 0 ),
      next_row_record_pos_( 0 ),
      partition_id_( 0 ),
      row_records_( nullptr ),
      next_chain_( nullptr ) {
}

mvcc_chain::~mvcc_chain() {
    row_record_len_ = 0;
    // Destructor will delete the next chain which is recursive
    // so don't pass in references to next_chain
    if( next_chain_ != nullptr ) {
        delete next_chain_;
    }

    next_chain_ = nullptr;
    delete[] row_records_;
}

void mvcc_chain::init( int32_t num_row_records, uint64_t partition_id,
                       mvcc_chain* next_chain ) {
    DCHECK_GE( num_row_records, 2 );
    DVLOG( 19 ) << "Initializing chain: " << this
                << ", num_row_records=" << num_row_records
                << ", partition_id:" << partition_id
                << ", next_chain:" << next_chain;
    row_record_len_ = num_row_records;
    partition_id_ = partition_id;
    next_chain_ = next_chain;

    row_records_ = new row_record[num_row_records];
    DVLOG( 20 ) << "Init chain:" << this << ", row_records_:" << row_records_;

    next_row_record_pos_ = row_record_len_ - 1;
}

row_record* mvcc_chain::create_new_row_record( transaction_state* txn_state ) {
    return create_new_row_record<row_record_creation_type::TXN_STATE>( txn_state );
}

row_record* mvcc_chain::create_new_row_record( uint64_t version ) {
    return create_new_row_record<row_record_creation_type::VERSION>( &version );
}
row_record* mvcc_chain::create_new_row_record( uint64_t         version,
                                       snapshot_vector* vector_p ) {

    snapshot_vector_pointer_and_version_holder holder;
    holder.version_ = version;
    holder.vector_ptr_ = vector_p;

    return create_new_row_record<row_record_creation_type::SNAPSHOT_VEC>(
        &holder );
}

row_record* mvcc_chain::internal_get_latest_row_record(
    bool recurse, uint64_t& partition_id ) const {
    DVLOG( 19 ) << "Trying to find latest row_record in chain: " << this;

    int32_t  found = -1;
    uint64_t row_record_v = 0;

    partition_id = partition_id_.load( std::memory_order_acquire );
    int32_t next_pos_tmp = next_row_record_pos_.load( std::memory_order_acquire );
    //Reload everything below this as of the next_row_record_pos
    int32_t next_pos = std::max( 0, next_pos_tmp );
    int32_t start = next_pos;
    DVLOG( 20 ) << "Get latest chain:" << this << ", row_records_:" << row_records_;
    DCHECK_NE( (void*) row_records_, (void*) this );
    for( int32_t pos = start; pos < row_record_len_; pos++ ) {
        DVLOG( 20 ) << "find latest chain:" << this << ", pos:" << pos;
        const row_record& r = row_records_[pos];
        DVLOG( 20 ) << "find latest chain:" << this << ", pos:" << pos
                    << ", row_record:" << &r;

        bool is_changed = r.update_if_tombstone( partition_id );
        if( is_changed ) {
            continue;
        }
        row_record_v = r.get_version();
        if( row_record_v < K_ABORTED ) {
            found = pos;
            break;
        }
    }
    row_record* ret = nullptr;
    if( found >= 0 ) {
        ret = &( row_records_[found] );
        DVLOG( 19 ) << "Found latest row_record in chain at pos:" << found
                    << ", row_record:" << ret;
    } else if( ( next_chain_ != nullptr ) and ( recurse ) ) {
        DVLOG( 19 ) << "Finding latest row_record in next chain:" << next_chain_;
        ret = next_chain_->internal_get_latest_row_record( true, partition_id );
    }
    // otherwise we return a null row_record
    return ret;
}

std::tuple<uint64_t, uint64_t> mvcc_chain::get_max_version_on_chain() const {
    // we can always guarantee that a row_record will be in the chain because
    // when we remaster we take a copy of the latest row_record and copy it
    // in so we know that there is always a copy of data.
    // This vastly simplifies GC
    uint64_t partition_id = k_unassigned_partition;
    row_record*  r = internal_get_latest_row_record( false, partition_id );
    if( r == nullptr ) {
        LOG( WARNING ) << "Getting max version, but no row_record has been written";
        return std::make_tuple( k_unassigned_partition, K_COMMITTED );
    }
    uint64_t version = r->get_version();
    DVLOG( 20 ) << "Found row_record:" << r << ", version:" << version;
    return std::make_tuple( k_unassigned_partition, version );
}

#if 0 // HDB-OUT
bool mvcc_chain::safe_to_delete( const client_version_vector& min_version,
                                 const client_version_vector& chain_version ) {
    // if chain is up to delete
    DVLOG( 19 ) << "Checking if safe to delete lwm:" << min_version
                << ", chain version:" << chain_version;
    DCHECK_EQ( min_version.size(), chain_version.size() );
    for( uint32_t pos = 0; pos < min_version.size(); pos++ ) {
        bool is_chain_larger = chain_version.at( pos ) >= min_version.at( pos );
        bool is_elem_non_zero = chain_version.at( pos ) > 0;
        if( is_chain_larger and is_elem_non_zero) {
            DVLOG( 19 ) << "Is not safe to delete!";
            DVLOG( 30 ) << "Pos:" << pos
                        << ", chain_version:" << chain_version.at( pos )
                        << ", lwm version:" << min_version.at( pos )
                        << ", is_chain_larger:" << is_chain_larger
                        << ", is_elem_non_zero:" << is_elem_non_zero;
            return false;
        }
    }
    DVLOG( 19 ) << "Is safe to delete child!";
    return true;
}
#endif

mvcc_chain* mvcc_chain::garbage_collect_chain(
    const snapshot_vector& min_version ) {
    return nullptr;
}
#if 0
mvcc_chain* mvcc_chain::garbage_collect_chain(
    const snapshot_vector& min_version ) {
    DVLOG( 19 ) << "GC chain:" << this << " with min version:" << min_version;

    // traverse all the chains adding them to a vector
    mvcc_chain*              chain = this;
    std::vector<mvcc_chain*> seen_chains;
    uint32_t                 stop_pos = 0;
    uint32_t                 chain_pos = 0;
    while( chain != nullptr ) {
        seen_chains.push_back( chain );
        if( chain->is_chain_empty() ) {
            // we can't gc past an empty chain because people will hop past this
            stop_pos = chain_pos + 1;
        }
        chain_pos++;
        chain = chain->next_chain_;
    }

    // we now go back over the chains and build up the version vector
    // and decide if it is safe to delete
    client_version_vector version_of_chain( min_version.size(), 0 );
    mvcc_chain*           parent = nullptr;
    mvcc_chain*           chain_to_delete = nullptr;
    // don't look at the min pos, because that could be
    // (the current head chain) or a chain that people will traverse to see
    // versions
    DVLOG( 20 ) << "Iterating back, until stop_pos:" << stop_pos;
    for( uint32_t pos = seen_chains.size() - 1; pos > stop_pos; pos-- ) {
        DVLOG( 20 ) << "Looking at chain in pos:" << pos;
        chain = seen_chains.at( pos );
        std::tuple<uint32_t, uint64_t> max_versions =
            chain->get_max_version_on_chain();
        uint32_t master_location = std::get<0>( max_versions );
        uint64_t max_version_of_chain = std::get<1>( max_versions );
        DVLOG( 20 ) << "Got master_location:" << master_location
                    << ", max version:" << max_version_of_chain;
        if (master_location == k_unassigned_master) {
          continue;
        }
        DCHECK_NE( master_location, k_unassigned_master );
        DCHECK_GE( max_version_of_chain,
                   version_of_chain.at( master_location ) );
        version_of_chain.at( master_location ) = max_version_of_chain;

        if( safe_to_delete( min_version, version_of_chain ) ) {
            //Delete the child chain, row_record us as the parent
            if( chain->next_chain_ != nullptr ) {
                chain_to_delete = chain->next_chain_;
                parent = chain;
            }
        } else {
            break;
        }
    }

    if( chain_to_delete != nullptr ) {
        DVLOG( 10 ) << "GC chain:" << this
                    << " found chain to delete:" << chain_to_delete;

        // remove the references
        // I don't believe there is a need for atomics.
        // It is up to the caller to make sure no one is in this chain
        // So nobody should be looking at this next_chain value
        parent->next_chain_ = nullptr;
    } else {
        DVLOG( 20 ) << "GC chain:" << this << " found no chain to delete";
    }
    return chain_to_delete;
}

void mvcc_chain::persist_chain( table_persister& persister ) const {
    row_record*     ret_row_record = nullptr;
    found_version_information found_info;
    // set this so that it gets fixed recursively
    found_info.remaster_location_ = k_unassigned_master;

    DVLOG( 19 ) << "Persist chain:" << this
                << " with snapshot:" << persister.get_snapshot();

    ret_row_record = internal_find_row_record<true /*persist info*/>(
        persister.get_snapshot(), &found_info );

    DVLOG( 25 ) << "Persist chain,  Found info:["
                << "master_location_:" << found_info.master_location_
                << ", remaster_location_:" << found_info.remaster_location_
                << ", largest_satisfying_version_:"
                << found_info.largest_satisfying_version_ << " ]";

    if( !ret_row_record ) {
        persister.persist_no_row_record_chain();
        DVLOG( 19 ) << "Persist chain:" << this
                    << " with snapshot:" << persister.get_snapshot()
                    << " is empty!";
        return;
    }


    DCHECK( ret_row_record );
    DCHECK( found_info.found_chain_ );

    persister.persist_row_record_location_and_version(
        found_info.master_location_, found_info.largest_satisfying_version_,
        found_info.remaster_location_ );
    persister.persist_row_record_data( ret_row_record );

    DVLOG( 19 ) << "Persist chain:" << this
                << " with snapshot:" << persister.get_snapshot() << " okay!";
}
#endif
