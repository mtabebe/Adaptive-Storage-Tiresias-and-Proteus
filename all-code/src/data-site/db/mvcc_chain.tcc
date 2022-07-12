#pragma once

#include <glog/logging.h>
#ifndef GLOG_STL_LOGGING_FOR_UNORDERED
#define GLOG_STL_LOGGING_FOR_UNORDERED
#endif
#include <glog/stl_logging.h>

template <bool store_version_information>
row_record*        mvcc_chain::internal_find_row_record(
    const snapshot_vector&     snapshot,
    found_version_information* found_info ) const {
    DVLOG( 19 ) << "Trying to find row_record in chain: " << this
                << ", that satisfies version:" << snapshot;
    row_record* ret_row_record = nullptr;
    if( store_version_information ) {
        DCHECK( found_info );
        found_info->found_chain_ = nullptr;
        if( found_info->change_partition_id_ == k_unassigned_partition ) {
            found_info->largest_satisfying_version_ = K_NOT_COMMITTED;
            found_info->change_partition_id_ = k_unassigned_partition;
        }
    }
    // partition id of the most recent row_records
    // different partition ids possible further to the right, but we will
    // overwrite our local partition id
    uint64_t partition_id = partition_id_.load( std::memory_order_acquire );
    if( partition_id == k_unassigned_partition ) {
        DVLOG( 19 ) << "Cannot find in chain with unassigned partition id";
        return nullptr;
    }
    uint64_t partition_version = get_snapshot_version( snapshot, partition_id );

    // scan from front to back
    int32_t found = -1;
    int32_t in_commit_pos = -2;

    uint64_t row_record_v = K_NOT_COMMITTED;

    int32_t next_pos_tmp = next_row_record_pos_.load( std::memory_order_relaxed );
    int32_t next_pos = std::max( 0, next_pos_tmp );
    int32_t start = next_pos;
    for( int32_t pos = start; pos < row_record_len_; pos++ ) {
        const row_record& r = row_records_[pos];
        row_record_v = r.get_version();


        bool is_changed = r.update_if_tombstone( partition_id );
        partition_version = get_snapshot_version( snapshot, partition_id );
        DVLOG( 40 ) << "Partition:" << (int64_t) partition_id << " / "
                    << partition_id
                    << ", version to look for:" << partition_version
                    << ", row_record version:" << row_record_v
                    << ", is_changed:" << is_changed;

        if( is_changed ) {
            if( store_version_information ) {
                if( row_record_v <= partition_version ) {
                    if( found_info->change_partition_id_ ==
                        k_unassigned_partition ) {
                        found_info->change_partition_id_ = partition_id;
                        found_info->largest_satisfying_version_ = row_record_v;
                        DVLOG( 25 ) << "Found partition information, new "
                                       "partition_id:"
                                    << partition_id;
                    }
                }
            }

            continue;
        }
        if( row_record_v <= partition_version ) {
            found = pos;
            break;
        } else if( row_record_v == K_IN_COMMIT ) {
            in_commit_pos = pos;
        }
    }
    uint64_t local_partition_id = partition_id;
    uint64_t old_repartition_id = 0;
    (void) old_repartition_id;  // used in store_version_information
    if( store_version_information ) {
        found_info->partition_id_ = local_partition_id;
        old_repartition_id = found_info->change_partition_id_;
        if( found_info->largest_satisfying_version_ == K_NOT_COMMITTED ) {
            // hasn't been overwritten which means the largest version is
            // the thing we found
            DVLOG( 25 )
                << "Never found partition chnage row_record so setting local "
                   "partition id";
            found_info->change_partition_id_ = local_partition_id;
            found_info->largest_satisfying_version_ = row_record_v;
        }
        DVLOG( 25 ) << "Found info:["
                    << "partition_id_:" << found_info->partition_id_
                    << ", change_partition_id_:"
                    << found_info->change_partition_id_
                    << ", largest_satisfying_version_:"
                    << found_info->largest_satisfying_version_ << " ]";
    }

    // we need to make sure we aren't in the following position
    // in_commit, v=5, v=1
    // if we requested v=6, it could be that in_commit will be assigned v=6,
    // so we need to spin until in_commit is assigned.
    // if however it is assigned v=7, then we need to return the v=5 version
    // however if we requested v=4, then we don't need to spin
    // This case is if the in_commit is directly next to the found pos
    //
    if( found >= 0 ) {
        if( found == in_commit_pos + 1 ) {
            DVLOG( 19 ) << "Must check in_commit position:" << found;
            const row_record& r = row_records_[in_commit_pos];
            row_record_v = r.spin_until_not_in_commit_version();
            // TODO: do we need to invalidate our registers after this spin?
            // the partition id should be what we think it is, because we
            // got a local snapshot, otherwise we scanned past it to the right
            partition_version = get_snapshot_version( snapshot, partition_id );
            if( row_record_v <= partition_version ) {
                found = in_commit_pos;
            }
        }
        ret_row_record = &( row_records_[found] );
        if( store_version_information ) {
            found_info->found_chain_ = (mvcc_chain*) this;
        }
        DVLOG( 19 ) << "Found viable row_record in chain at pos:" << found
                    << ", row_record:" << ret_row_record
                    << ", version:" << ret_row_record->get_version()
                    << ", for partition id:" << partition_id;
    } else {
        if( next_chain_ != nullptr ) {
            DVLOG( 19 ) << "Finding next row_record in next chain:" << next_chain_;
            ret_row_record =
                next_chain_->internal_find_row_record<store_version_information>(
                    snapshot, found_info );
        }
        // could be the case that the in commit spans across chains
        if( in_commit_pos == row_record_len_ - 1 ) {
            DVLOG( 19 ) << "Must check in_commit position in the last position:"
                        << in_commit_pos;
            const row_record& r = row_records_[in_commit_pos];
            row_record_v = r.spin_until_not_in_commit_version();
            // TODO: do we need to invalidate our registers after this spin?
            // it could be overriden when we recurse
            partition_id = local_partition_id;
            partition_version = get_snapshot_version( snapshot, partition_id );
            if( row_record_v <= partition_version ) {
                ret_row_record = &( row_records_[in_commit_pos] );
                if( store_version_information ) {
                    found_info->found_chain_ = (mvcc_chain*) this;
                    found_info->partition_id_ = local_partition_id;
                    // has to be the largest and current location
                    found_info->largest_satisfying_version_ = row_record_v;
                    found_info->change_partition_id_ = local_partition_id;
                    DVLOG( 25 ) << "Found info:["
                                << "partition_id_:" << found_info->partition_id_
                                << ", change_partition_id_:"
                                << found_info->change_partition_id_
                                << ", largest_satisfying_version_:"
                                << found_info->largest_satisfying_version_
                                << " ]";
                }
            }
        }
    }
    // otherwise we return a null row_record and the client needs to retry, this
    // is possible if the chain is full and there is nothing in the chain that
    // satisfies this version, and likely something went wrong
    return ret_row_record;
}

template <row_record_creation_type rec_type>
row_record* mvcc_chain::create_new_row_record( void* v ) {
    DVLOG( 19 ) << "Trying to create new row_record:" << this;
    row_record* r = nullptr;
    if( is_chain_full() ) {
        DVLOG( 19 ) << "Failed to create new row_record, chain full";
        return r;
    } else if( partition_id_ == k_unassigned_partition ) {
        DVLOG( 19 ) << "Cannot write to chain with unassigned partition_id";
        return r;
    }

    // guaranteed only one based on locks
    r = &( row_records_[next_row_record_pos_] );
    switch( rec_type ) {
        case row_record_creation_type::TXN_STATE: {
            transaction_state* txn_state = (transaction_state*) v;
            r->set_transaction_state( txn_state );
            break;
        }
        case row_record_creation_type::VERSION: {
            uint64_t version = *( (uint64_t*) v );
            r->set_version( version );
            break;
        }
        case row_record_creation_type::SNAPSHOT_VEC: {
            snapshot_vector_pointer_and_version_holder* snap_holder =
                (snapshot_vector_pointer_and_version_holder*) v;
            // we need to store this boy before we mark it as committed
            r->set_snapshot_vector( snap_holder->vector_ptr_ );
            r->set_version( snap_holder->version_ );
            break;
        }
    }

    DVLOG( 19 ) << "Created new row_record in position:" << next_row_record_pos_
                << ", row_record:" << r;
    next_row_record_pos_--;

    return r;
}
