#include "versioned_row_record.h"

#include <glog/logging.h>
#include <glog/stl_logging.h>

static_assert( sizeof( versioned_row_record ) == 16,
               "versioned_row_record should be 16 bytes" );

versioned_row_record::versioned_row_record()
    : record_key_( k_unassigned_key ), record_chain_( nullptr ) {}

versioned_row_record::~versioned_row_record() {
    // Make sure we terminate GC threads first
    if( record_chain_ != nullptr ) {
        DVLOG( 40 ) << "Deleting record key_:" << record_key_;
        delete record_chain_;
        record_chain_ = nullptr;
    }
    record_key_ = INT64_MAX;
}

void versioned_row_record::init( int64_t key, uint64_t partition_id,
                             int32_t num_records_in_chain ) {
    record_key_ = key;
    DVLOG( 20 ) << "versioned_row_record::init: num_records_in_chain:"
                << num_records_in_chain << ", key:" << key
                << ", partition_id:" << partition_id;

    if( record_chain_ == nullptr ) {
        mvcc_chain* chain = new mvcc_chain();
        chain->init( num_records_in_chain, partition_id, nullptr );
        record_chain_ = chain;
    } else {
        record_chain_->set_partition_id( partition_id );
    }
}

bool versioned_row_record::repartition(
    std::vector<row_record*>& repartitioned_records, int64_t key,
    uint64_t partition_id, transaction_state* txn_state,
    int32_t num_records_in_chain, const snapshot_vector& gc_lwm ) {

    // if it's full we allocate space for at least two records, one to
    // copy over the most recently written record, and store it.
    // We will fill it's version in with the version of the repartition op,
    // because that is in when it is visible. Because it's a copy of data
    // it doesn't matter that it's not immediately visible.
    // This takes a HARD assumption on the version that will be written comes
    // from the site that does the repartition. Which makes sense because
    // repartition takes a write lock. While this adds overhead it greatly
    // simplifies GC, and allows us to figure out where something is
    // repartition at any given snapshot.
    DVLOG( 20 ) << "Repartition key:" << key;
    if( record_key_ == k_unassigned_key ) {
        DVLOG( 10 ) << "Repartition:" << key << ", but record unassigned";
    }
    if( record_chain_->is_chain_full() ) {
        DVLOG( 20 ) << "Repartitioning key:" << key << ", chain is full";
        DCHECK_GE( num_records_in_chain, 2 );
        // READ BEFORE WRITE
        // BECAUSE IF IT IS UPDATE PROPAGATION WE ASSIGN A COMMIT VERSION
        // BEFORE WE ACTUALLY COMMIT, SO READ LATEST RECORD WILL JUST RETURN
        // THE NEWLY ALLOCATED RECORD
        row_record* latest_record = read_latest_record( key );
        if( latest_record != nullptr ) {
            row_record* write_rec =
                write_record( key, txn_state, num_records_in_chain, gc_lwm );
            if( write_rec == nullptr ) {
                return false;
            }
            DVLOG( 20 ) << "Deep copying key:" << key << ", chain is full";
            DCHECK_GE( num_records_in_chain, 2 );
            write_rec->deep_copy( *latest_record );
            repartitioned_records.push_back( write_rec );
        }
    }
    // if we have nothing there then we can't write a record with a
    // repartitioned_records
    if( !record_chain_->is_chain_empty() ) {
        DVLOG( 20 ) << "Repartitioning key:" << key
                    << ", writing repartition record";
        row_record* repartition_record =
            write_record( key, txn_state, num_records_in_chain, gc_lwm );
        DCHECK( repartition_record );
        // guaranteed to be the most recent chain because we have an exclusive
        // lock
        repartition_record->set_tombstone( record_chain_->get_partition_id() );
        repartitioned_records.push_back( repartition_record );
    }
    record_chain_->set_partition_id( partition_id );
    return true;
}

uint64_t versioned_row_record::get_partition_id( int64_t key ) const {
    if( ( key == k_unassigned_key ) or ( record_key_ == k_unassigned_key ) ) {
        DVLOG( 10 ) << "Trying to get partition_id, but key is unassigned:"
                    << key;
        return k_unassigned_partition;
    } else if( key != record_key_ ) {
        DVLOG( 5 )
            << "Trying to get partition_id where keys don't match. Expected:"
            << key << ", actual: " << record_key_;
        return k_unassigned_partition;
    }
    uint64_t ret = record_chain_->get_partition_id();
    return ret;
}

row_record* versioned_row_record::read_record(
    int64_t key, const snapshot_vector& snapshot ) const {
    row_record* ret = nullptr;
    if( key != record_key_ ) {
        DVLOG( 5 ) << "Trying to read record where keys don't match. Expected:"
                   << key << ", actual: " << record_key_;
        return ret;
    }
    ret = record_chain_->find_row_record( snapshot );
    return ret;
}

row_record* versioned_row_record::read_latest_record( int64_t key ) const {
    row_record* ret = nullptr;
    if( key != record_key_ ) {
        DVLOG( 5 ) << "Trying to read record where keys don't match. Expected:"
                   << key << ", actual: " << record_key_;
        return ret;
    }
    ret = record_chain_->get_latest_row_record();
    return ret;
}

row_record* versioned_row_record::write_record( int64_t            key,
                                                transaction_state* txn_state,
                                                int32_t num_records_in_chain,
                                                const snapshot_vector& gc_lwm,
                                                int32_t always_gc ) {
    row_record* ret = nullptr;

    bool assigned_new = false;
    if( key != record_key_ ) {
        // TODO: should we always just return an error here
        if( record_key_ != k_unassigned_key ) {
            DVLOG( 5 )
                << "Trying to write record where keys don't match. Expected:"
                << key << ", actual: " << record_key_;
            return ret;
        } else {
            // assign key
            record_key_ = key;
        }
    }

    mvcc_chain* chain = record_chain_;
    if( record_chain_->is_chain_full() ) {
        assigned_new = true;
        mvcc_chain* new_chain = new mvcc_chain();
        new_chain->init( num_records_in_chain,
                         record_chain_->get_partition_id(), record_chain_ );
        chain = new_chain;
    }

    ret = chain->create_new_row_record( txn_state );

    if( assigned_new ) {
        record_chain_ = chain;
    }
    if( ( !gc_lwm.empty() ) and ( assigned_new or always_gc ) ) {
        DVLOG( 20 ) << "Created new chain, GC old chains, with lwm:" << gc_lwm;
        gc_record( gc_lwm );
    }

    return ret;
}
row_record* versioned_row_record::insert_record(
    int64_t key, transaction_state* txn_state, uint64_t partition_id,
    int32_t num_records_in_chain ) {
    row_record* ret = nullptr;
    if( record_key_ != k_unassigned_key ) {
        if( record_key_ != key ) {
            DVLOG( 5 ) << "Trying to insert record where key is not unassigned "
                          "don't match. Expected:"
                       << k_unassigned_key << ", actual: " << record_key_;
            return ret;
        }
    } else {
        record_key_ = key;
    }

    if( !record_chain_->is_chain_empty() ) {
        LOG( WARNING ) << "Trying to insert record when chain is not empty";
        return ret;
    }

    record_chain_->set_partition_id( partition_id );
    ret = record_chain_->create_new_row_record( txn_state );

    return ret;
}

std::tuple<row_record*, uint64_t>
    versioned_row_record::read_latest_record_and_partition_column_information(
        int64_t key ) const {
    row_record* ret = nullptr;
    if( key != record_key_ ) {
        DVLOG( 5 ) << "Trying to read record where keys don't match. Expected:"
                   << key << ", actual: " << record_key_;
        return std::make_tuple<>( ret, 0 );
    }
    uint64_t pcid_hash = 0;
    ret = record_chain_->internal_get_latest_row_record( true, pcid_hash );
    return std::make_tuple<>( ret, pcid_hash );
}

bool versioned_row_record::has_record_been_inserted() const {
    if( record_chain_ == nullptr ) {
        return false;
    }
    bool has_been_inserted = !record_chain_->is_chain_empty();
    return has_been_inserted;
}

#if 0  // HDB-OUT
void versioned_row_record::persist_versioned_row_record(
    table_persister& persister ) const {
    DVLOG( 20 ) << "Persisting record:" << record_key_;

    if( record_key_ == k_unassigned_key ) {
        DVLOG( 20 ) << "Skipping persisting unassigned record:" << record_key_;
        return;
    }

    persister.persist_record_key( record_key_ );
    record_chain_->persist_chain( persister );

    DVLOG( 20 ) << "Persisting record:" << record_key_ << " okay!";
}
#endif

void versioned_row_record::gc_record( const snapshot_vector& min_version ) {
    // get it and delete it
    DVLOG( 20 ) << "GC-ing record:" << record_key_;
    mvcc_chain* to_gc = record_chain_->garbage_collect_chain( min_version );
    if( to_gc ) {
        DVLOG( 10 ) << "Deleting record chain for record_key_:" << record_key_;
        delete to_gc;
    }
}
