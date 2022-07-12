#include "partition.h"

#include <glog/logging.h>

#include "../../common/perf_tracking.h"
#include "col_partition.h"
#include "partition_util.h"
#include "row_partition.h"
#include "table.h"
#include "transaction_partition_holder.h"

uint64_t get_table_poll_epoch( void* ptr ) {
    return 0;
    if( ptr == nullptr ) {
        return 0;
    }
    table* t = (table*) ptr;
    return t->get_poll_epoch();
}

partition::partition( std::shared_ptr<internal_partition>& internal )
    : metadata_(),
      shared_partition_( nullptr ),
      internal_partition_( internal ),
      active_and_pin_state_( 0 ),
      lock_() {
    shared_partition_ = std::make_shared<shared_partition_state>();
}

partition::~partition() {}

void partition::lock_partition() { lock_.lock(); }
void partition::unlock_partition() { lock_.unlock(); }
bool partition::try_lock_partition() { return lock_.try_lock(); }

partition_metadata partition::get_metadata() const {
    return shared_partition_->get_metadata();
}
uint32_t partition::get_master_location() const {
    return shared_partition_->get_master_location();
}

partition_type::type partition::get_partition_type() const {
    return internal_partition_->get_partition_type();
}
storage_tier_type::type partition::get_storage_tier_type() const {
    return shared_partition_->get_storage_tier();
}
cell_data_type partition::get_cell_type( const cell_identifier& ci ) const {
    return shared_partition_->get_cell_type( ci );
}

void partition::set_master_location( uint32_t new_master ) {
    shared_partition_->set_master_location( new_master );
}
void partition::remaster( uint32_t new_master ) {
    shared_partition_->remaster( new_master );
}

void partition::init(
    const partition_metadata&                        metadata,
    std::shared_ptr<update_destination_interface>    update_destination,
    std::shared_ptr<partition_column_version_holder> part_version_holder,
    const std::vector<cell_data_type>& col_types, void* table_ptr,
    uint64_t specified_version ) {

    metadata_ = metadata;
    shared_partition_->metadata_ = metadata_;
    shared_partition_->part_version_holder_ = part_version_holder;
    shared_partition_->col_types_ = col_types;
    DVLOG( 40 ) << "Internal init, partition metadata:" << metadata_
                << ", col_types:" << shared_partition_->col_types_;
    DCHECK_EQ( shared_partition_->col_types_.size(),
               ( metadata_.partition_id_.column_end -
                 metadata_.partition_id_.column_start ) +
                   1 );
    shared_partition_->table_ptr_ = table_ptr;

    internal_partition_->init( metadata, col_types, specified_version,
                               shared_partition_, this );

    // store the master location here as well, otherwise you won't prop your
    // update if the partition was created as say a merge or split
    shared_partition_->txn_execution_state_.reset_state(
        metadata_.site_location_ );

    set_master_location( metadata_.site_location_ );
    shared_partition_->update_destination_ = update_destination;
    DCHECK_LE( metadata_.partition_id_.partition_start,
               metadata_.partition_id_.partition_end );

    // MTODO figure out if we need version information
    snapshot_vector base_snapshot;
    set_snapshot_version( base_snapshot, metadata_.partition_id_hash_,
                          specified_version );
    DVLOG( 40 ) << "Init partition:" << metadata_.partition_id_
                << " specified version:" << specified_version;
    shared_partition_->dependency_.init(
        metadata_.partition_id_hash_, specified_version, base_snapshot,
        metadata_.num_records_in_snapshot_chain_ );

    record_version_on_table( specified_version );

    active_and_pin_state_.store( packed_pointer_ops::set_flag_bits(
        active_and_pin_state_.load(), 1 /*active*/ ) );

    shared_partition_->apply_version_ = specified_version + 1;
    DVLOG( 10 ) << "Init partition:" << metadata_
                << "version:" << specified_version;
}

void partition::init_records() { internal_partition_->init_records(); }

bool partition::begin_read_wait_and_build_snapshot( snapshot_vector& snapshot,
                                                    bool no_apply_last ) {

    return internal_partition_->begin_read_wait_and_build_snapshot(
        snapshot, no_apply_last );
}
void partition::begin_read_build_snapshot( uint64_t         read_version,
                                           snapshot_vector& snapshot ) {
    internal_partition_->begin_read_build_snapshot( read_version, snapshot );
}

bool partition::begin_read_wait_for_version( snapshot_vector& snapshot,
                                             bool             no_apply_last ) {
    return internal_partition_->begin_read_wait_for_version( snapshot,
                                                             no_apply_last );
}

bool partition::begin_write( snapshot_vector& snapshot, bool acquire_lock ) {
    pull_data_if_not_in_memory( true, true );
    return internal_partition_->begin_write( snapshot, acquire_lock );
}

uint64_t partition::get_most_recent_version() {
    return shared_partition_->get_most_recent_version();
}

void partition::get_most_recent_snapshot_vector( snapshot_vector& snapshot ) {
    shared_partition_->get_most_recent_snapshot_vector( snapshot );
}

void partition::build_commit_snapshot( snapshot_vector& snapshot ) {
    shared_partition_->build_commit_snapshot( snapshot );
}

uint64_t partition::commit_write_transaction(
    const snapshot_vector& dependency_snapshot, uint64_t table_epoch,
    bool do_unlock ) {

    start_timer( PARTITION_COMMIT_WRITE_TRANSACTION_TIMER_ID );

    uint64_t version = commit_write_transaction_no_unlock_no_propagate(
        dependency_snapshot, table_epoch );

    // MTODO
    // move the write buffer into the output network thread
    propagate_updates( version );
    done_with_updates();

    if( do_unlock ) {
        unlock_partition();
    }

    stop_timer( PARTITION_COMMIT_WRITE_TRANSACTION_TIMER_ID );

    return version;
}

uint64_t partition::commit_write_transaction_no_unlock_no_propagate(
    const snapshot_vector& dependency_snapshot, uint64_t table_epoch ) {

    DVLOG( 20 ) << "Committing write transaction:" << metadata_ << ", "
                << dependency_snapshot;

    if( shared_partition_->txn_execution_state_.get_write_buffer()->empty() and
        shared_partition_->txn_execution_state_.get_write_no_prop_buffer()
            ->empty() ) {
        DVLOG( 20 ) << "Transaction did not write, no operations to commit";
        return get_snapshot_version( dependency_snapshot,
                                     metadata_.partition_id_hash_ );
    }

    shared_partition_->txn_execution_state_.get_transaction_state()
        ->set_committing();
    uint64_t version = get_snapshot_version( dependency_snapshot,
                                             metadata_.partition_id_hash_ );

    bool no_prop_commit =
        shared_partition_->txn_execution_state_.get_write_buffer()->empty();

    if( !no_prop_commit ) {
        shared_partition_->dependency_.store_dependency(
            dependency_snapshot, metadata_.num_records_in_snapshot_chain_,
            shared_partition_->txn_execution_state_.get_low_watermark() );
    }

    finalize_commit( dependency_snapshot, version, table_epoch,
                     no_prop_commit );

    return version;
}

void partition::init_partition_from_snapshot(
    const snapshot_vector&                    snapshot,
    const std::vector<snapshot_column_state>& columns,
    uint32_t                                  master_location ) {

    start_timer( PARTITION_INIT_PARTITION_FROM_SNAPSHOT_TIMER_ID );

    DVLOG( 40 ) << "Initializing partition from snapshot:"
                << metadata_.partition_id_ << ", columns:" << columns;

    snapshot_vector snapshot_copy = snapshot;

    uint64_t base_version =
        get_snapshot_version( snapshot, metadata_.partition_id_hash_ );
    DVLOG( 40 ) << "Base version:" << base_version;
    // similar to applying updates
    set_master_location( master_location );
    // also set the state here as well
    shared_partition_->txn_execution_state_.reset_state( master_location );

    if( base_version == 0 ) {
        DVLOG( 40 ) << "No need to instantiate columns, base version is 0";
        stop_timer( PARTITION_INIT_PARTITION_FROM_SNAPSHOT_TIMER_ID );
        return;
    }

    begin_write( snapshot_copy, false /* no need to acquire locks */ );
    shared_partition_->dependency_.store_dependency(
        snapshot_copy, metadata_.num_records_in_snapshot_chain_,
        shared_partition_->txn_execution_state_.get_low_watermark() );

    // for each record update apply the updates
    DVLOG( 40 ) << "Instantiate columns:" << columns.size();
    DCHECK_EQ( shared_partition_->col_types_.size(), columns.size() );

    for( uint32_t col = 0; col < shared_partition_->col_types_.size(); col++ ) {
        DCHECK_EQ( col + metadata_.partition_id_.column_start,
                   columns.at( col ).col_id );
        DCHECK_EQ( cell_data_type_to_data_type(
                       shared_partition_->col_types_.at( col ) ),
                   columns.at( col ).type );
        install_snapshotted_column( columns.at( col ), snapshot_copy );
    }

    uint64_t table_epoch =
        get_table_poll_epoch( shared_partition_->table_ptr_ );
    finalize_commit( snapshot, base_version, table_epoch,
                     false /*no prop commit*/ );

    done_with_updates();

    shared_partition_->apply_version_.store( base_version + 1 );
    set_update_queue_expected_version( base_version + 1 );

    stop_timer( PARTITION_INIT_PARTITION_FROM_SNAPSHOT_TIMER_ID );

    DVLOG( 40 ) << "Done instantiating partition";
}

void partition::set_commit_version( uint64_t version ) {
    shared_partition_->set_commit_version( version );
}

void partition::set_commit_version_and_update_queue_if_able(
    uint64_t expected_version ) {
    shared_partition_->set_commit_version_and_update_queue_if_able(
        expected_version );
}

void partition::finalize_commit( const snapshot_vector& dependency_snapshot,
                                 uint64_t version, uint64_t table_epoch,
                                 bool no_prop_commit ) {
    internal_partition_->finalize_commit( dependency_snapshot, version,
                                          table_epoch, no_prop_commit );
}

uint32_t partition::add_stashed_update( stashed_update&& update ) {
    return shared_partition_->add_stashed_update( std::move( update ) );
}

void partition::set_update_queue_expected_version( uint64_t expected_version ) {
    shared_partition_->set_update_queue_expected_version( expected_version );
}

void partition::propagate_updates( uint64_t version ) {
    shared_partition_->propagate_updates( version );
}

void partition::abort_write_transaction() {
    internal_partition_->abort_write_transaction();
}

uint32_t partition::apply_k_updates_or_empty( uint32_t num_updates_to_apply ) {
    return (uint32_t) apply_updates_until_empty(
        (uint64_t) num_updates_to_apply, false /*is not until version*/ );
}
uint64_t partition::apply_updates_until_version_or_empty( uint64_t version ) {
    return apply_updates_until_empty( version, true /*is until version*/ );
}
uint64_t partition::apply_updates_until_empty(
    uint64_t desired_version_or_num_updates, bool is_until_version ) {
    uint64_t num_updates_applied = 0;
    if( is_until_version ) {
        DVLOG( 40 ) << "Applying until version "
                    << desired_version_or_num_updates << " or until empty:";
    } else {
        DVLOG( 40 ) << "Applying " << desired_version_or_num_updates
                    << " updates or until empty:";
    }

    bool locked = try_lock_partition();
    if( !locked ) {
        return 0;
    }

    for( ;; ) {
        if( !is_until_version &&
            ( ( num_updates_applied >= desired_version_or_num_updates ) ) ) {
            DVLOG( 40 ) << "Applied:" << num_updates_applied
                        << " updates, wanted to apply up to:"
                        << desired_version_or_num_updates << ", so breaking";
            break;
        }
        stashed_update stashed =
            shared_partition_->update_queue_.get_next_update(
                false /*don't wait*/ );
        if( stashed.commit_version_ == K_NOT_COMMITTED ) {
            // there was no update
            DVLOG( 40 ) << "Applied until no more versions, breaking";
            break;
        }
        uint64_t current_version = shared_partition_->dependency_.get_version();
        if( stashed.commit_version_ <= current_version ) {
            // unnecessary operation, we have already applied
            DVLOG( 5 ) << "Already applied update:" << stashed.commit_version_
                       << " to PID:" << metadata_.partition_id_;
        } else {

            execute_deserialized_partition_update_with_locks(
                stashed.deserialized_ );
        }
        destroy_stashed_update( stashed );
        num_updates_applied += 1;
        if( is_until_version and
            ( current_version >= desired_version_or_num_updates ) ) {
            DVLOG( 40 ) << "Applied until version:" << current_version
                        << ", wanted to apply up to version:"
                        << desired_version_or_num_updates << ", so breaking";
            break;
        }
    }

    unlock_partition();
    return num_updates_applied;
}

uint64_t partition::wait_until_version_or_apply_updates(
    uint64_t req_version ) {
    if( shared_partition_->get_storage_tier() != storage_tier_type::MEMORY ) {
        uint64_t storage_version = shared_partition_->storage_version_;
        if( storage_version >= req_version ) {
            return storage_version;
        }
    }
    pull_data_if_not_in_memory( true, true );

    return internal_partition_->wait_until_version_or_apply_updates(
        req_version );
}

uint64_t partition::internal_wait_until_version_or_apply_updates(
    uint64_t req_version ) {

    start_timer( PARTITION_WAIT_UNTIL_VERSION_OR_APPLY_UPDATES_TIMER_ID );

    uint64_t current_version = shared_partition_->dependency_.get_version();
    while( current_version < req_version ) {
        // get lock
        DVLOG( 20 ) << "Waiting until version or apply updates:"
                    << current_version << ", required version:" << req_version
                    << ", id:" << metadata_.partition_id_;
        bool locked = try_lock_partition();

        if( locked ) {
            shared_partition_->apply_version_.store( req_version );
            DVLOG( 40 ) << "Applying updates until:" << req_version;
            // if got the lock, apply the updates,
            while( current_version < req_version ) {
                stashed_update stashed =
                    shared_partition_->update_queue_.get_next_update(
                        true, req_version, current_version );
                if( stashed.commit_version_ <= current_version ) {
                    // unnecessary operation, we have already applied
                    DVLOG( 5 )
                        << "Already applied update:" << stashed.commit_version_
                        << " to PID:" << metadata_.partition_id_;

                } else {
                    execute_deserialized_partition_update_with_locks(
                        stashed.deserialized_ );
                    current_version = stashed.commit_version_;
                }
                destroy_stashed_update( stashed );
            }
            // unlock
            current_version = shared_partition_->dependency_.get_version();
            unlock_partition();
        } else {
            uint64_t applying_version =
                shared_partition_->apply_version_.load();
            // wait until min of req version and the update value
            uint64_t wait_version = std::min( applying_version, req_version );
            DVLOG( 40 ) << "Waiting for updates until:" << wait_version
                        << ", someeone is applying until:" << applying_version;

            current_version = wait_for_dependency_version( wait_version );
        }
    }
    stop_timer( PARTITION_WAIT_UNTIL_VERSION_OR_APPLY_UPDATES_TIMER_ID );

    return current_version;
}

uint64_t partition::wait_for_dependency_version( uint64_t wait_version ) {
    return shared_partition_->wait_for_dependency_version( wait_version );
}

uint64_t partition::wait_for_version_no_apply_last(
    const snapshot_vector& snapshot ) {
    return internal_partition_->wait_for_version_no_apply_last( snapshot );
}

void partition::record_version_on_table( uint64_t version ) {
    shared_partition_->record_version_on_table( version );
}

void partition::record_version_and_epoch_on_table( uint64_t version,
                                                   uint64_t epoch ) {
    shared_partition_->record_version_and_epoch_on_table( version, epoch );
}

std::shared_ptr<partition_column_version_holder>
    partition::get_partition_column_version_holder() const {
    return shared_partition_->get_partition_column_version_holder();
}

void partition::execute_deserialized_partition_update(
    deserialized_update* deserialized_ptr ) {
    lock_partition();
    execute_deserialized_partition_update_with_locks( deserialized_ptr );
    unlock_partition();
}

void partition::execute_deserialized_partition_update_with_locks(
    deserialized_update* deserialized_ptr ) {
    if( deserialized_ptr == nullptr ) {
        DVLOG( 40 ) << "Got update that is null, as a consequence of splits";
        return;
    }

    pull_data_if_not_in_memory( true, true );

    uint64_t table_epoch =
        get_table_poll_epoch( shared_partition_->table_ptr_ );

    start_timer( PARTITION_EXECUTE_DESERIALIZED_PARTITION_UPDATE_TIMER_ID );

    DCHECK_EQ( metadata_.partition_id_, deserialized_ptr->pcid_ );
    uint64_t version = get_snapshot_version( deserialized_ptr->commit_vv_,
                                             metadata_.partition_id_hash_ );

    DVLOG( 40 ) << "execute_deserialized_partition_update:"
                << deserialized_ptr->pcid_ << ", "
                << deserialized_ptr->commit_vv_;
    uint64_t dep_version = shared_partition_->dependency_.get_version();
    DVLOG( 10 ) << "Executing deserialized_update_partition_update, version:"
                << version << ", current version:" << dep_version;

#if defined( RECORD_COMMIT_TS )
    std::chrono::high_resolution_clock::time_point cur_point =
        std::chrono::high_resolution_clock::now();
    std::chrono::high_resolution_clock::time_point produce_point(
        std::chrono::nanoseconds( (uint64_t) deserialized_ptr->txn_ts_ ) );
    std::chrono::duration<double, std::nano> elapsed =
        cur_point - produce_point;

    if( elapsed.count() > k_slow_timer_log_time_threshold ) {
        VLOG( k_timer_log_level )
            << "delayed execute_deserialized_partition_update:"
            << deserialized_ptr->pcid_ << ", " << version
            << ", applying after:" << elapsed.count()
            << " time, first occured at:" << deserialized_ptr->txn_ts_;
    }
    DVLOG( 10 ) << "execute_deserialized_partition_update:"
                << deserialized_ptr->pcid_ << ", " << version
                << ", applying after:" << elapsed.count()
                << " time, first occured at:" << deserialized_ptr->txn_ts_;
#endif

    if( dep_version >= version ) {
        DVLOG( 10 ) << "Version got advanced underneath us, skipping applying";
        stop_timer( PARTITION_EXECUTE_DESERIALIZED_PARTITION_UPDATE_TIMER_ID );
        return;
    }

    // ensure the update is the correct version
    DCHECK_EQ( version - 1, dep_version );

    // store the dependency, it is safe to do this early
    begin_write( deserialized_ptr->commit_vv_, false /*we have the lock*/ );
    shared_partition_->dependency_.store_dependency(
        deserialized_ptr->commit_vv_, metadata_.num_records_in_snapshot_chain_,
        shared_partition_->txn_execution_state_.get_low_watermark() );

    DVLOG( 40 ) << "Update partitions:"
                << deserialized_ptr->partition_updates_.size();
    for( const partition_column_operation_identifier& poi :
         deserialized_ptr->partition_updates_ ) {
        execute_deserialized_partition_operation_update(
            poi, version, table_epoch, deserialized_ptr );
    }

    // for each record update apply the updates
    DVLOG( 40 ) << "Update num cells:"
                << deserialized_ptr->cell_updates_.size();
    for( const deserialized_cell_op& du_cell :
         deserialized_ptr->cell_updates_ ) {
        execute_deserialized_cell_update( du_cell, deserialized_ptr->commit_vv_,
                                          version );
    }

    // store the commit version, which "finalizes" this commit
    finalize_commit( deserialized_ptr->commit_vv_, version, table_epoch,
                     false /*prop commit*/ );
    done_with_updates();

    DVLOG( 40 ) << "execute_deserialized_partition_update:"
                << deserialized_ptr->pcid_ << ", "
                << deserialized_ptr->commit_vv_ << " okay!";

    stop_timer( PARTITION_EXECUTE_DESERIALIZED_PARTITION_UPDATE_TIMER_ID );
}

void partition::execute_deserialized_partition_operation_update(
    const partition_column_operation_identifier& poi, uint64_t version,
    uint64_t table_epoch, deserialized_update* deserialized_ptr ) {

    uint32_t                           op = poi.op_code_;
    uint64_t                           data_64 = poi.data_64_;
    uint32_t                           data_32 = poi.data_32_;
    const partition_column_identifier& pid = poi.identifier_;

    DVLOG( 20 ) << "Execute partition operation update:" << op
                << ", on partition:" << pid;

    // MTODO: this forces the site selector to embed this in begin timestamps
    if( op == K_REMASTER_OP ) {
        handle_partition_remaster( pid, data_32, deserialized_ptr );
    } else if( op == K_INSERT_OP ) {
        // either an insert, or the result of a a merge (for now always merge)
        // if we are inserted from a merge, then wait for the two original ones
        // to be up to date, then do the merge.
        handle_partition_merge( pid, data_32, data_64, deserialized_ptr,
                                version, table_epoch );
    } else if( op == K_SPLIT_OP ) {
        handle_partition_split( pid, data_32, data_64, deserialized_ptr,
                                version, table_epoch );
    } else if( op == K_MERGE_OP ) {
        // merge doesn't reauire any action, other than adding and removing
        // subscriptions.  In this case, let the new partition do the work,
        // after we subscribe
        handle_original_partition_merge( pid, deserialized_ptr );
    } else if( op == K_CHANGE_DESTINATION_OP ) {
        handle_change_partition_destination( pid, deserialized_ptr );
    } else {
        LOG( WARNING ) << "Unknown op:" << op << ", pid:" << pid;
    }
}

void partition::handle_change_partition_destination(
    const partition_column_identifier& pid,
    deserialized_update*               deserialized_ptr ) {
    table* t = (table*) shared_partition_->table_ptr_;
    if( deserialized_ptr->switch_subscription_.size() > 0 ) {
        DCHECK_EQ( 0, deserialized_ptr->switch_subscription_.size() % 2 );
        // actually apply the subscription switch
        t->switch_subscription( deserialized_ptr->switch_subscription_,
                                k_destination_change_cause_string );
    }
}

void partition::handle_original_partition_merge(
    const partition_column_identifier& pid,
    deserialized_update*               deserialized_ptr ) {
    DVLOG( 40 ) << "Handle original partition merge:"
                << metadata_.partition_id_;
    // stop subscribing to the updates
    table* t = (table*) shared_partition_->table_ptr_;
    DCHECK_EQ( 1, deserialized_ptr->start_subscribing_.size() );
    DCHECK_EQ( 1, deserialized_ptr->stop_subscribing_.size() );

    DCHECK_EQ( pid, deserialized_ptr->start_subscribing_.at( 0 ).identifier_ );
    DCHECK_EQ( metadata_.partition_id_,
               deserialized_ptr->stop_subscribing_.at( 0 ).identifier_ );
    t->stop_subscribing( deserialized_ptr->stop_subscribing_,
                         deserialized_ptr->commit_vv_, k_merge_cause_string );
    // the start subscription already happens in the update enqueuer
}

void partition::handle_partition_remaster(
    const partition_column_identifier& pid, uint32_t new_master,
    deserialized_update* deserialized_ptr ) {
    set_master_location( new_master );

    table* t = (table*) shared_partition_->table_ptr_;
    if( deserialized_ptr->switch_subscription_.size() > 0 ) {
        DCHECK_EQ( 0, deserialized_ptr->switch_subscription_.size() % 2 );
        // actually apply the subscription switch
        t->switch_subscription( deserialized_ptr->switch_subscription_,
                                k_remaster_at_replica_cause_string );
    }
}

void partition::handle_partition_split( const partition_column_identifier& pid,
                                        uint32_t col_split, uint64_t row_split,
                                        deserialized_update* deserialized_ptr,
                                        uint64_t             version,
                                        uint64_t             table_epoch ) {
    std::tuple<partition_column_identifier, partition_column_identifier> pids =
        construct_split_partition_column_identifiers( pid, row_split,
                                                      col_split );
    partition_column_identifier low_pid = std::get<0>( pids );
    partition_column_identifier high_pid = std::get<1>( pids );

    table* t = (table*) shared_partition_->table_ptr_;

    // in theory we can call get, but in reality, it could be removed under us
    // get these things, we have the locks, because of the update queue
    auto low_part = std::move( t->get_partition( low_pid ) );
    auto high_part = std::move( t->get_partition( high_pid ) );

    // in theory these should exist because the enqueuer creates them
    DCHECK( low_part );
    DCHECK( high_part );

    uint64_t low_v =
        get_snapshot_version( deserialized_ptr->commit_vv_, low_pid );
    uint64_t high_v =
        get_snapshot_version( deserialized_ptr->commit_vv_, high_pid );

    // drain the versions
    low_part->apply_updates_until_version_or_empty( low_v - 1 );
    high_part->apply_updates_until_version_or_empty( low_v - 1 );

    low_part->set_commit_version( low_v - 1 );
    high_part->set_commit_version( high_v - 1 );

    bool low_locked = low_part->try_lock_partition();
    bool high_locked = high_part->try_lock_partition();

    // set their location
    uint32_t master_loc = get_master_location();
    low_part->set_master_location( master_loc );
    high_part->set_master_location( master_loc );

    DVLOG( 40 ) << "HANDLE_SPLIT:" << low_locked << ", " << high_locked;

    DCHECK_EQ( 1, deserialized_ptr->stop_subscribing_.size() );
    DCHECK_EQ( 2, deserialized_ptr->start_subscribing_.size() );

    DCHECK_EQ( metadata_.partition_id_,
               deserialized_ptr->stop_subscribing_.at( 0 ).identifier_ );
    t->stop_subscribing( deserialized_ptr->stop_subscribing_,
                         deserialized_ptr->commit_vv_, k_merge_cause_string );
    // The start subscribtion already happens in the update enqueuer

    // if you didn't get the lock, it doesn't matter someone is waiting on us
    // so we have free reign

    // reset this state
    low_part->shared_partition_->txn_execution_state_.reset_state( master_loc );
    high_part->shared_partition_->txn_execution_state_.reset_state(
        master_loc );

    split_records( low_part.get(), high_part.get(), col_split, row_split );

    // add the op
    low_part->add_repartition_op( metadata_, col_split, row_split, K_NO_OP );
    high_part->add_repartition_op( metadata_, col_split, row_split, K_NO_OP );

    // do the logical commit, but do not unlock
    low_part->commit_write_transaction_no_unlock_no_propagate(
        deserialized_ptr->commit_vv_, table_epoch );
    high_part->commit_write_transaction_no_unlock_no_propagate(
        deserialized_ptr->commit_vv_, table_epoch );

    advance_if_locked( low_locked, low_part, low_v );
    advance_if_locked( high_locked, high_part, high_v );

    // remove self, so no one else can see
    t->remove_partition( pid );

    handle_split_partition_completion( low_locked, low_part, low_v );
    handle_split_partition_completion( high_locked, high_part, high_v );

    t->mark_partition_as_active( low_pid, low_part );
    t->mark_partition_as_active( high_pid, high_part );

    low_part->unpin_partition();
    high_part->unpin_partition();
}

void partition::done_with_updates() {
    internal_partition_->done_with_updates();
}

void partition::advance_if_locked( bool                       is_locked_by_us,
                                   std::shared_ptr<partition> part,
                                   uint64_t                   version ) {
    // MTODO-HTAP ?? true or ??
    if( true or is_locked_by_us ) {
        part->set_update_queue_expected_version( version + 1 );
    }
}

void partition::handle_split_partition_completion(
    bool is_locked_by_us, std::shared_ptr<partition> part, uint64_t version ) {
    done_with_updates();

    if( is_locked_by_us ) {
        part->unlock_partition();
    } else {
        // we didn't have the lock, so we need to push a dummy update, that
        // someone, will see and do nothing with
        DVLOG( 40 ) << "Add dummy op:" << version
                    << " to:" << part->metadata_.partition_id_;
        part->shared_partition_->update_queue_.add_dummy_op( version );
    }
}

void partition::handle_partition_merge( const partition_column_identifier& pid,
                                        uint32_t col_split, uint64_t row_split,
                                        deserialized_update* deserialized_ptr,
                                        uint64_t             version,
                                        uint64_t             table_epoch ) {
    std::tuple<partition_column_identifier, partition_column_identifier> pids =
        construct_split_partition_column_identifiers( pid, row_split,
                                                      col_split );
    partition_column_identifier low_pid = std::get<0>( pids );
    partition_column_identifier high_pid = std::get<1>( pids );

    table* t = (table*) shared_partition_->table_ptr_;
    {
        // get these things
        auto low_part = std::move( t->get_partition( low_pid ) );
        auto high_part = std::move( t->get_partition( high_pid ) );

        uint64_t low_v =
            get_snapshot_version( deserialized_ptr->commit_vv_, low_pid );
        uint64_t high_v =
            get_snapshot_version( deserialized_ptr->commit_vv_, high_pid );

        // wait for these versions to be satisfied
        DCHECK( low_part );
        low_part->wait_until_version_or_apply_updates( low_v );
        DCHECK( high_part );
        high_part->wait_until_version_or_apply_updates( high_v );

        // now lock these partitions
        bool low_locked = low_part->try_lock_partition();
        bool high_locked = high_part->try_lock_partition();

        DVLOG( 40 ) << "Merge try lock low:" << low_locked
                    << ", try lock high:" << high_locked;

        /*
        DCHECK_EQ( low_part->get_master_location(),
                   high_part->get_master_location() );
                   */
        merge_records( low_part.get(), high_part.get(), col_split, row_split );

        // this needs to be here to set the committed values
        low_part->add_repartition_op( metadata_, col_split, row_split,
                                      K_NO_OP );
        high_part->add_repartition_op( metadata_, col_split, row_split,
                                       K_NO_OP );

        handle_split_partition_completion( low_locked, low_part, low_v );
        handle_split_partition_completion( high_locked, high_part, high_v );

        low_part->unpin_partition();
        high_part->unpin_partition();

        // each of them will have been pinned themselves
        unpin_partition();
        unpin_partition();
    }

    t->remove_partition( low_pid );
    t->remove_partition( high_pid );
}

void partition::execute_deserialized_cell_update(
    const deserialized_cell_op& du_cell, const snapshot_vector& snapshot,
    uint64_t version ) {
    uint32_t op = du_cell.get_op();

    DVLOG( 40 ) << "deserialized_cell_op cid:" << du_cell.cid_ << ", op:" << op
                << ", version:" << version;

    bool do_insert = false;
    bool do_store = false;

    if( op == K_INSERT_OP ) {
        DVLOG( 30 ) << "Inserting:" << du_cell.cid_ << " version:" << version;
        do_insert = true;
        do_store = true;
    } else if( op == K_WRITE_OP ) {
        do_insert = false;
        do_store = true;
    } else if( op == K_DELETE_OP ) {
        do_insert = false;
        do_store = false;
    } else {
        LOG( WARNING ) << "Unknown op:" << du_cell.cid_;
    }

    bool apply_ok = apply_propagated_cell_update( du_cell, snapshot, do_insert,
                                                  do_store, version );
    DCHECK( apply_ok );
}

void partition::switch_update_destination(
    std::shared_ptr<update_destination_interface> new_update_destination ) {
    shared_partition_->switch_update_destination( new_update_destination );
}

void partition::set_update_destination(
    std::shared_ptr<update_destination_interface> new_update_destination ) {
    shared_partition_->set_update_destination( new_update_destination );
}
update_propagation_information partition::get_update_propagation_information() {
    return shared_partition_->get_update_propagation_information();
}
void partition::add_subscription_state_to_write_buffer(
    const update_propagation_information& update_prop_info,
    bool                                  do_start_subscribing ) {
    shared_partition_->add_subscription_state_to_write_buffer(
        update_prop_info, do_start_subscribing );
}
void partition::add_switch_subscription_state_to_write_buffer(
    const update_propagation_information& old_sub,
    const update_propagation_information& new_sub ) {
    shared_partition_->add_switch_subscription_state_to_write_buffer( old_sub,
                                                                      new_sub );
}

void partition::add_repartition_op( const partition_metadata& metadata,
                                    uint32_t col_split, uint64_t row_split,
                                    uint32_t op_code ) {
    shared_partition_->add_repartition_op( metadata, col_split, row_split,
                                           op_code );
}

bool partition::is_pinned() {
    uint64_t pin_count =
        packed_pointer_ops::get_int( active_and_pin_state_.load() );
    // pinned if it is greater than 0
    DVLOG( 20 ) << "Is pinned partition:" << pin_count;
    return ( pin_count > 0 );
}

void partition::change_pin_counter( int64_t op_val ) {
    for( ;; ) {
        packed_pointer ori_pp = active_and_pin_state_.load();
        int64_t new_counter = op_val + packed_pointer_ops::get_int( ori_pp );
        if( new_counter < 0 ) {
            LOG( WARNING ) << "Changing pin counter to less than zero:"
                           << new_counter << ", op_val:" << op_val
                           << ", ori int:"
                           << packed_pointer_ops::get_int( ori_pp )
                           << ", resetting to 0";
            // this is real bad
            new_counter = 0;
        }
        packed_pointer new_pp =
            packed_pointer_ops::set_as_int( ori_pp, new_counter );
        bool swapped =
            active_and_pin_state_.compare_exchange_strong( ori_pp, new_pp );
        if( swapped ) {
            DVLOG( 20 ) << "Change partition:" << metadata_.partition_id_
                        << " pin counter:" << new_counter;

            break;
        }
    }
}
void partition::change_active_bits( uint16_t is_active ) {
    for( ;; ) {
        packed_pointer ori_pp = active_and_pin_state_.load();
        packed_pointer new_pp =
            packed_pointer_ops::set_flag_bits( ori_pp, is_active );
        bool swapped =
            active_and_pin_state_.compare_exchange_strong( ori_pp, new_pp );
        if( swapped ) {
            DVLOG( 20 ) << "Change partition:" << metadata_.partition_id_
                        << " active bits to:" << is_active;
            break;
        }
    }
}

void partition::unpin_partition() {
    DVLOG( 20 ) << "Unpin partition:" << metadata_.partition_id_;
    change_pin_counter( -1 );
}
void partition::pin_partition() {
    DVLOG( 20 ) << "Pin partition:" << metadata_.partition_id_;
    change_pin_counter( 1 );
}

bool partition::is_active() {
    uint16_t active_bits =
        packed_pointer_ops::get_flag_bits( active_and_pin_state_.load() );
    // active if flag bits is greater than 0
    DVLOG( 20 ) << "Is active partition:" << active_bits;
    return ( active_bits > 0 );
}

void partition::mark_as_inactive() { change_active_bits( 0 ); }
void partition::mark_as_active() { change_active_bits( 1 ); }

bool partition::is_safe_to_remove() {
    packed_pointer ori_pp = active_and_pin_state_.load();

    uint64_t pin_count = packed_pointer_ops::get_int( ori_pp );
    uint64_t active_bits = packed_pointer_ops::get_flag_bits( ori_pp );
    if( !( ( pin_count == 0 ) and ( active_bits == 0 ) ) ) {
        return false;
    }

    DCHECK_EQ( pin_count, 0 );
    DCHECK_EQ( active_bits, 0 );

    // it shouldn't change from under us
    packed_pointer latter_pp = active_and_pin_state_.load();
    return ( ori_pp == latter_pp );
}

void partition::transfer_records( partition*                         src,
                                  const partition_column_identifier& pcid ) {
    cell_identifier cid;
    cid.table_id_ = pcid.table_id;

    for( int32_t col_id = pcid.column_start; col_id <= pcid.column_end;
         col_id++ ) {
        cid.col_id_ = col_id;
        for( int64_t row_id = pcid.partition_start;
             row_id <= pcid.partition_end; row_id++ ) {
            cid.key_ = row_id;
            repartition_cell_into_partition( cid, src );
        }
    }
    set_master_location( src->get_master_location() );
}

void partition::repartition_cell_into_partition( const cell_identifier& ci,
                                                 partition*             src ) {
    internal_partition_->repartition_cell_into_partition(
        ci, src->internal_partition_.get() );
}

void partition::finalize_repartitions( uint64_t version ) {
    internal_partition_->finalize_repartitions( version );
}

bool partition::apply_propagated_cell_update(
    const deserialized_cell_op& du_cell, const snapshot_vector& snapshot,
    bool do_insert, bool do_store, uint64_t version ) {
    return internal_partition_->apply_propagated_cell_update(
        du_cell, snapshot, do_insert, do_store, version );
}

void partition::install_snapshotted_column( const snapshot_column_state& column,
                                            const snapshot_vector& snapshot ) {

    internal_partition_->install_snapshotted_column( column, snapshot );
}

void partition::snapshot_partition_data(
    snapshot_partition_column_state& snapshot ) {
    internal_partition_->snapshot_partition_data( snapshot );
}

void partition::snapshot_partition_metadata(
    snapshot_partition_column_state& snapshot ) const {
    shared_partition_->snapshot_partition_metadata( snapshot );
}

void partition::split_records( partition* low_p, partition* high_p,
                               uint32_t col_split_point,
                               uint64_t row_split_point ) {
    uint64_t version = get_most_recent_version();
    if( get_partition_type() == partition_type::type::ROW ) {
        if( ( low_p->get_partition_type() == partition_type::type::ROW ) and
            ( high_p->get_partition_type() == partition_type::type::ROW ) ) {
            return split_row_records( low_p, high_p, col_split_point,
                                      row_split_point, version, version );
        }
    } else if( ( get_partition_type() == partition_type::type::COLUMN ) or
               ( get_partition_type() ==
                 partition_type::type::SORTED_COLUMN ) ) {
        if( ( ( low_p->get_partition_type() == partition_type::type::COLUMN ) or
              ( low_p->get_partition_type() ==
                partition_type::type::SORTED_COLUMN ) ) and
            ( ( high_p->get_partition_type() ==
                partition_type::type::COLUMN ) or
              ( high_p->get_partition_type() ==
                partition_type::type::SORTED_COLUMN ) ) ) {
            return split_col_records( low_p, high_p, col_split_point,
                                      row_split_point, version, version );
        }
    } else if( ( get_partition_type() == partition_type::type::MULTI_COLUMN ) or
               ( get_partition_type() ==
                 partition_type::type::SORTED_MULTI_COLUMN ) ) {
        if( ( ( low_p->get_partition_type() ==
                partition_type::type::MULTI_COLUMN ) or
              ( low_p->get_partition_type() ==
                partition_type::type::SORTED_MULTI_COLUMN ) ) and
            ( ( high_p->get_partition_type() ==
                partition_type::type::MULTI_COLUMN ) or
              ( high_p->get_partition_type() ==
                partition_type::type::SORTED_MULTI_COLUMN ) ) ) {
            return split_multi_col_records( low_p, high_p, col_split_point,
                                            row_split_point, version, version );
        }
    }

    return generic_split_records( low_p, high_p, col_split_point,
                                  row_split_point, version, version );
}
void partition::merge_records( partition* low_p, partition* high_p,
                               uint32_t col_merge_point,
                               uint64_t row_merge_point ) {
    uint64_t version = std::max( low_p->get_most_recent_version(),
                                 high_p->get_most_recent_version() );
    if( get_partition_type() == partition_type::type::ROW ) {
        if( ( low_p->get_partition_type() == partition_type::type::ROW ) and
            ( high_p->get_partition_type() == partition_type::type::ROW ) ) {
            return merge_row_records( low_p, high_p, col_merge_point,
                                      row_merge_point, version );
        }
    } else if( ( get_partition_type() == partition_type::type::COLUMN ) or
               ( get_partition_type() ==
                 partition_type::type::SORTED_COLUMN ) ) {
        if( ( ( low_p->get_partition_type() == partition_type::type::COLUMN ) or
              ( low_p->get_partition_type() ==
                partition_type::type::SORTED_COLUMN ) ) and
            ( ( high_p->get_partition_type() ==
                partition_type::type::COLUMN ) or
              ( high_p->get_partition_type() ==
                partition_type::type::SORTED_COLUMN ) ) ) {
            return merge_col_records( low_p, high_p, col_merge_point,
                                      row_merge_point, version );
        }
    } else if( ( get_partition_type() == partition_type::type::MULTI_COLUMN ) or
               ( get_partition_type() ==
                 partition_type::type::SORTED_MULTI_COLUMN ) ) {
        if( ( ( low_p->get_partition_type() ==
                partition_type::type::MULTI_COLUMN ) or
              ( low_p->get_partition_type() ==
                partition_type::type::SORTED_MULTI_COLUMN ) ) and
            ( ( high_p->get_partition_type() ==
                partition_type::type::MULTI_COLUMN ) or
              ( high_p->get_partition_type() ==
                partition_type::type::SORTED_MULTI_COLUMN ) ) ) {
            return merge_multi_col_records( low_p, high_p, col_merge_point,
                                            row_merge_point, version );
        }
    }
    return generic_merge_records( low_p, high_p, col_merge_point,
                                  row_merge_point, version );
}

void partition::generic_split_records( partition* low_p, partition* high_p,
                                       uint32_t col_split_point,
                                       uint64_t row_split_point,
                                       uint64_t low_version,
                                       uint64_t high_version ) {
    low_p->transfer_records( this, low_p->get_metadata().partition_id_ );
    high_p->transfer_records( this, high_p->get_metadata().partition_id_ );
    low_p->finalize_repartitions( low_version );
    high_p->finalize_repartitions( high_version );
}

void partition::generic_merge_records( partition* low_p, partition* high_p,
                                       uint32_t col_merge_point,
                                       uint64_t row_merge_point,
                                       uint64_t merge_version ) {
    transfer_records( low_p, low_p->get_metadata().partition_id_ );
    transfer_records( high_p, high_p->get_metadata().partition_id_ );
    this->finalize_repartitions( merge_version );
}

void partition::split_col_records( partition* low_p, partition* high_p,
                                   uint32_t col_split_point,
                                   uint64_t row_split_point,
                                   uint64_t low_version,
                                   uint64_t high_version ) {
    DCHECK( ( low_p->get_partition_type() == partition_type::type::COLUMN ) or
            ( low_p->get_partition_type() ==
              partition_type::type::SORTED_COLUMN ) );
    DCHECK( ( high_p->get_partition_type() == partition_type::type::COLUMN ) or
            ( high_p->get_partition_type() ==
              partition_type::type::SORTED_COLUMN ) );
    DCHECK( ( get_partition_type() == partition_type::type::COLUMN ) or
            ( get_partition_type() == partition_type::type::SORTED_COLUMN ) );


    col_partition* low_internal =
        (col_partition*) low_p->internal_partition_.get();
    col_partition* high_internal = (col_partition*) high_p->internal_partition_.get();
    col_partition* cur_internal = (col_partition*) internal_partition_.get();

    cur_internal->split_col_records( low_internal, high_internal,
                                     col_split_point, row_split_point,
                                     low_version, high_version );
}
void partition::split_multi_col_records( partition* low_p, partition* high_p,
                                         uint32_t col_split_point,
                                         uint64_t row_split_point,
                                         uint64_t low_version,
                                         uint64_t high_version ) {
    DCHECK(
        ( low_p->get_partition_type() == partition_type::type::MULTI_COLUMN ) or
        ( low_p->get_partition_type() ==
          partition_type::type::SORTED_MULTI_COLUMN ) );
    DCHECK( ( high_p->get_partition_type() ==
              partition_type::type::MULTI_COLUMN ) or
            ( high_p->get_partition_type() ==
              partition_type::type::SORTED_MULTI_COLUMN ) );
    DCHECK(
        ( get_partition_type() == partition_type::type::MULTI_COLUMN ) or
        ( get_partition_type() == partition_type::type::SORTED_MULTI_COLUMN ) );

    multi_col_partition* low_internal =
        (multi_col_partition*) low_p->internal_partition_.get();
    multi_col_partition* high_internal =
        (multi_col_partition*) high_p->internal_partition_.get();
    multi_col_partition* cur_internal =
        (multi_col_partition*) internal_partition_.get();

    cur_internal->split_multi_col_records( low_internal, high_internal,
                                           col_split_point, row_split_point,
                                           low_version, high_version );
}


void partition::split_row_records( partition* low_p, partition* high_p,
                                   uint32_t col_split_point,
                                   uint64_t row_split_point,
                                   uint64_t low_version,
                                   uint64_t high_version ) {
    DCHECK_EQ( partition_type::type::ROW, low_p->get_partition_type() );
    DCHECK_EQ( partition_type::type::ROW, high_p->get_partition_type() );
    DCHECK_EQ( partition_type::type::ROW, get_partition_type() );

    row_partition* low_internal =
        (row_partition*) low_p->internal_partition_.get();
    row_partition* high_internal =
        (row_partition*) high_p->internal_partition_.get();
    row_partition* cur_internal = (row_partition*) internal_partition_.get();

    cur_internal->split_row_records( low_internal, high_internal,
                                     col_split_point, row_split_point,
                                     low_version, high_version );
}
void partition::merge_col_records( partition* low_p, partition* high_p,
                                   uint32_t col_merge_point,
                                   uint64_t row_merge_point,
                                   uint64_t merge_version ) {
    DCHECK( ( low_p->get_partition_type() == partition_type::type::COLUMN ) or
            ( low_p->get_partition_type() ==
              partition_type::type::SORTED_COLUMN ) );
    DCHECK( ( high_p->get_partition_type() == partition_type::type::COLUMN ) or
            ( high_p->get_partition_type() ==
              partition_type::type::SORTED_COLUMN ) );
    DCHECK( ( get_partition_type() == partition_type::type::COLUMN ) or
            ( get_partition_type() == partition_type::type::SORTED_COLUMN ) );

    col_partition* low_internal =
        (col_partition*) low_p->internal_partition_.get();
    col_partition* high_internal =
        (col_partition*) high_p->internal_partition_.get();
    col_partition* cur_internal = (col_partition*) internal_partition_.get();

    cur_internal->merge_col_records( low_internal, high_internal,
                                     col_merge_point, row_merge_point,
                                     merge_version );
}
void partition::merge_multi_col_records( partition* low_p, partition* high_p,
                                   uint32_t col_merge_point,
                                   uint64_t row_merge_point,
                                   uint64_t merge_version ) {
    DCHECK(
        ( low_p->get_partition_type() == partition_type::type::MULTI_COLUMN ) or
        ( low_p->get_partition_type() ==
          partition_type::type::SORTED_MULTI_COLUMN ) );
    DCHECK( ( high_p->get_partition_type() ==
              partition_type::type::MULTI_COLUMN ) or
            ( high_p->get_partition_type() ==
              partition_type::type::SORTED_MULTI_COLUMN ) );
    DCHECK(
        ( get_partition_type() == partition_type::type::MULTI_COLUMN ) or
        ( get_partition_type() == partition_type::type::SORTED_MULTI_COLUMN ) );

    multi_col_partition* low_internal =
        (multi_col_partition*) low_p->internal_partition_.get();
    multi_col_partition* high_internal =
        (multi_col_partition*) high_p->internal_partition_.get();
    multi_col_partition* cur_internal =
        (multi_col_partition*) internal_partition_.get();

    cur_internal->merge_multi_col_records( low_internal, high_internal,
                                           col_merge_point, row_merge_point,
                                           merge_version );
}

void partition::merge_row_records( partition* low_p, partition* high_p,
                                   uint32_t col_merge_point,
                                   uint64_t row_merge_point,
                                   uint64_t merge_version ) {
    DCHECK_EQ( partition_type::type::ROW, low_p->get_partition_type() );
    DCHECK_EQ( partition_type::type::ROW, high_p->get_partition_type() );
    DCHECK_EQ( partition_type::type::ROW, get_partition_type() );

    row_partition* low_internal =
        (row_partition*) low_p->internal_partition_.get();
    row_partition* high_internal =
        (row_partition*) high_p->internal_partition_.get();
    row_partition* cur_internal = (row_partition*) internal_partition_.get();

    cur_internal->merge_row_records( low_internal, high_internal,
                                     col_merge_point, row_merge_point,
                                     merge_version );
}

bool partition::change_partition_and_storage_type(
    const partition_type::type&    part_type,
    const storage_tier_type::type& storage_type ) {

    bool ret = change_type_internal( part_type, false /*don't notify */ );
    if( !ret ) {
        return ret;
    }
    ret = change_storage_tier( storage_type );

    return ret;
}
bool partition::change_type( const partition_type::type& part_type ) {
    return change_type_internal( part_type, true );
}
bool partition::change_type_internal( const partition_type::type& part_type,
                                      bool                        do_notify ) {

    if( part_type == get_partition_type() ) {
        return true;
    }

    pull_data_if_not_in_memory( true, do_notify );

    auto new_internal = create_internal_partition( part_type );
    uint64_t specified_version = get_most_recent_version();
    new_internal->init( metadata_, shared_partition_->col_types_, specified_version,
                        shared_partition_, this );
    new_internal->init_records();

    if( ( part_type == partition_type::type::COLUMN ) and
        ( get_partition_type() == partition_type::type::SORTED_COLUMN ) ) {
        change_from_sorted_to_unsorted_column(
            internal_partition_, new_internal, specified_version );
    } else if( ( part_type == partition_type::type::MULTI_COLUMN ) and
               ( get_partition_type() ==
                 partition_type::type::SORTED_MULTI_COLUMN ) ) {
        change_from_sorted_to_unsorted_multi_column(
            internal_partition_, new_internal, specified_version );
    } else {
        generic_change_type( internal_partition_, new_internal,
                             specified_version );
    }

    new_internal->finalize_change_type( specified_version );

    internal_partition_ = new_internal;

    return true;
}

void partition::change_from_sorted_to_unsorted_column(
    std::shared_ptr<internal_partition>& cur_part,
    std::shared_ptr<internal_partition>& new_part, uint64_t version ) const {

    DCHECK_EQ( partition_type::type::COLUMN, new_part->get_partition_type() );
    DCHECK_EQ( partition_type::type::SORTED_COLUMN,
               cur_part->get_partition_type() );

    col_partition* cur_col_part = (col_partition*) cur_part.get();
    col_partition* new_col_part = (col_partition*) new_part.get();

    new_col_part->change_type_from_sorted_column( cur_col_part, version );
}

void partition::change_from_sorted_to_unsorted_multi_column(
    std::shared_ptr<internal_partition>& cur_part,
    std::shared_ptr<internal_partition>& new_part, uint64_t version ) const {

    DCHECK_EQ( partition_type::type::MULTI_COLUMN,
               new_part->get_partition_type() );
    DCHECK_EQ( partition_type::type::SORTED_MULTI_COLUMN,
               cur_part->get_partition_type() );

    multi_col_partition* cur_col_part = (multi_col_partition*) cur_part.get();
    multi_col_partition* new_col_part = (multi_col_partition*) new_part.get();

    new_col_part->change_type_from_sorted_column( cur_col_part, version );
}


void partition::generic_change_type(
    std::shared_ptr<internal_partition>& cur_part,
    std::shared_ptr<internal_partition>& new_part, uint64_t version ) const {

    internal_partition* cur_part_ptr = cur_part.get();

    auto pcid = metadata_.partition_id_;

    cell_identifier cid;
    cid.table_id_ = pcid.table_id;

    for( int32_t col_id = pcid.column_start; col_id <= pcid.column_end;
         col_id++ ) {
        cid.col_id_ = col_id;
        for( int64_t row_id = pcid.partition_start;
             row_id <= pcid.partition_end; row_id++ ) {
            cid.key_ = row_id;
            new_part->repartition_cell_into_partition( cid, cur_part_ptr );
        }
    }
}

bool partition::remove_data( const cell_identifier& ci,
                             const snapshot_vector& snapshot ) {
    return internal_partition_->remove_data( ci, snapshot );
}

bool partition::insert_uint64_data( const cell_identifier& ci, uint64_t data,
                                    const snapshot_vector& snapshot ) {
    return internal_partition_->insert_uint64_data( ci, data, snapshot );
}
bool partition::insert_int64_data( const cell_identifier& ci, int64_t data,
                                   const snapshot_vector& snapshot ) {
    return internal_partition_->insert_int64_data( ci, data, snapshot );
}
bool partition::insert_string_data( const cell_identifier& ci,
                                    const std::string&     data,
                                    const snapshot_vector& snapshot ) {
    return internal_partition_->insert_string_data( ci, data, snapshot );
}
bool partition::insert_double_data( const cell_identifier& ci, double data,
                                    const snapshot_vector& snapshot ) {
    return internal_partition_->insert_double_data( ci, data, snapshot );
}

bool partition::update_uint64_data( const cell_identifier& ci, uint64_t data,
                                    const snapshot_vector& snapshot ) {
    return internal_partition_->update_uint64_data( ci, data, snapshot );
}
bool partition::update_int64_data( const cell_identifier& ci, int64_t data,
                                   const snapshot_vector& snapshot ) {
    return internal_partition_->update_int64_data( ci, data, snapshot );
}
bool partition::update_string_data( const cell_identifier& ci,
                                    const std::string&     data,
                                    const snapshot_vector& snapshot ) {
    return internal_partition_->update_string_data( ci, data, snapshot );
}
bool partition::update_double_data( const cell_identifier& ci, double data,
                                    const snapshot_vector& snapshot ) {
    return internal_partition_->update_double_data( ci, data, snapshot );
}

std::tuple<bool, uint64_t> partition::get_uint64_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    return internal_partition_->get_uint64_data( ci, snapshot );
}
std::tuple<bool, int64_t> partition::get_int64_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    return internal_partition_->get_int64_data( ci, snapshot );
}
std::tuple<bool, double> partition::get_double_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    return internal_partition_->get_double_data( ci, snapshot );
}
std::tuple<bool, std::string> partition::get_string_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    return internal_partition_->get_string_data( ci, snapshot );
}

std::tuple<bool, uint64_t> partition::get_latest_uint64_data(
    const cell_identifier& ci ) const {
    return internal_partition_->get_latest_uint64_data( ci );
}
std::tuple<bool, int64_t> partition::get_latest_int64_data(
    const cell_identifier& ci ) const {
    return internal_partition_->get_latest_int64_data( ci );
}
std::tuple<bool, double> partition::get_latest_double_data(
    const cell_identifier& ci ) const {
    return internal_partition_->get_latest_double_data( ci );
}
std::tuple<bool, std::string> partition::get_latest_string_data(
    const cell_identifier& ci ) const {
    return internal_partition_->get_latest_string_data( ci );
}

void partition::scan( uint64_t low_key, uint64_t high_key,
                      const predicate_chain&       predicate,
                      const std::vector<uint32_t>& project_cols,
                      const snapshot_vector&       snapshot,
                      std::vector<result_tuple>&   result_tuples ) {
    bool is_on_disk = ( shared_partition_->get_storage_tier() ==
                        storage_tier_type::type::DISK );
    if( is_on_disk ) {
        switch( get_partition_type() ) {
            case partition_type::type::ROW: {
                start_timer( ROW_SCAN_DISK_TIMER_ID );
                scan_internal( low_key, high_key, predicate, project_cols,
                               snapshot, result_tuples );
                stop_timer( ROW_SCAN_DISK_TIMER_ID );
                break;
            }
            case partition_type::type::COLUMN: {
                start_timer( COL_SCAN_DISK_TIMER_ID );
                scan_internal( low_key, high_key, predicate, project_cols,
                               snapshot, result_tuples );
                stop_timer( COL_SCAN_DISK_TIMER_ID );
                break;
            }
            case partition_type::type::SORTED_COLUMN: {
                start_timer( SORTED_COL_SCAN_DISK_TIMER_ID );
                scan_internal( low_key, high_key, predicate, project_cols,
                               snapshot, result_tuples );
                stop_timer( SORTED_COL_SCAN_DISK_TIMER_ID );
                break;
            }
            case partition_type::type::MULTI_COLUMN: {
                start_timer( MULTI_COL_SCAN_DISK_TIMER_ID );
                scan_internal( low_key, high_key, predicate, project_cols,
                               snapshot, result_tuples );
                stop_timer( MULTI_COL_SCAN_DISK_TIMER_ID );
                break;
            }
            case partition_type::type::SORTED_MULTI_COLUMN: {
                start_timer( SORTED_MULTI_COL_SCAN_DISK_TIMER_ID );
                scan_internal( low_key, high_key, predicate, project_cols,
                               snapshot, result_tuples );
                stop_timer( SORTED_MULTI_COL_SCAN_DISK_TIMER_ID );
                break;
            }
        }
    } else {
        scan_internal( low_key, high_key, predicate, project_cols, snapshot,
                       result_tuples );
    }
}
void partition::scan_internal( uint64_t low_key, uint64_t high_key,
                               const predicate_chain&       predicate,
                               const std::vector<uint32_t>& project_cols,
                               const snapshot_vector&       snapshot,
                               std::vector<result_tuple>&   result_tuples ) {
    if( shared_partition_->get_storage_tier() ==
        storage_tier_type::type::DISK ) {
        shared_partition_->lock_storage();
        bool need_to_scan = true;
        if( shared_partition_->get_storage_tier() ==
            storage_tier_type::type::DISK ) {
            need_to_scan =
                evaluate_predicate_on_storage_tier( predicate, project_cols );
            if( need_to_scan ) {
                pull_data_if_not_in_memory( false, true );
            }
        }
        shared_partition_->unlock_storage();

        if( !need_to_scan ) {
            return;
        }
    }

    internal_partition_->scan( low_key, high_key, predicate, project_cols,
                               snapshot, result_tuples );
}

bool partition::evaluate_predicate_on_storage_tier(
    const predicate_chain&       predicate,
    const std::vector<uint32_t>& project_cols ) const {
    // look at the stored stats and decide if we need to pull into
    // memory
    DVLOG( 40 ) << "Evaluate predicate on storage tier:"
                << metadata_.partition_id_;

    DVLOG( 40 ) << "Stored stats:" << shared_partition_->stored_stats_;

    predicate_chain translated_predicate = translate_predicate_to_partition_id(
        predicate, metadata_.partition_id_, shared_partition_->col_types_ );

    bool on_stats = evaluate_predicate_on_column_stats(
        shared_partition_->stored_stats_, translated_predicate );

    DVLOG( 40 ) << "Evaluate predicate on storage tier:"
                << metadata_.partition_id_ << ", ret:" << on_stats;

    return on_stats;
}

void partition::persist_data(
    data_persister* part_persister,
    const std::tuple<uint32_t, uint64_t>& update_information_slot ) {
    DCHECK( part_persister );

    part_persister->open();
    uint64_t version = shared_partition_->persist_metadata( part_persister );

    DVLOG( 40 ) << "Persisting master location:" << get_master_location()
                << ", version:" << version;

    part_persister->write_i32( std::get<0>( update_information_slot ) );
    part_persister->write_i64( std::get<1>( update_information_slot ) );

    internal_partition_->persist_data( part_persister );

    part_persister->write_i64( k_unassigned_key );
    part_persister->close();
}

uint64_t partition::persist_to_disk( data_persister* part_persister ) {
    uint64_t ret = 0;

    switch( get_partition_type() ) {
        case partition_type::type::ROW: {
            start_timer( ROW_PERSIST_TO_DISK_TIMER_ID );
            ret = persist_to_disk_internal( part_persister );
            stop_timer( ROW_PERSIST_TO_DISK_TIMER_ID );
            break;
        }
        case partition_type::type::COLUMN: {
            start_timer( COL_PERSIST_TO_DISK_TIMER_ID );
            ret = persist_to_disk_internal( part_persister );
            stop_timer( COL_PERSIST_TO_DISK_TIMER_ID );
            break;
        }
        case partition_type::type::SORTED_COLUMN: {
            start_timer( SORTED_COL_PERSIST_TO_DISK_TIMER_ID );
            ret = persist_to_disk_internal( part_persister );
            stop_timer( SORTED_COL_PERSIST_TO_DISK_TIMER_ID );
            break;
        }
        case partition_type::type::MULTI_COLUMN: {
            start_timer( MULTI_COL_PERSIST_TO_DISK_TIMER_ID );
            ret = persist_to_disk_internal( part_persister );
            stop_timer( MULTI_COL_PERSIST_TO_DISK_TIMER_ID );
            break;
        }
        case partition_type::type::SORTED_MULTI_COLUMN: {
            start_timer( SORTED_MULTI_COL_PERSIST_TO_DISK_TIMER_ID );
            ret = persist_to_disk_internal( part_persister );
            stop_timer( SORTED_MULTI_COL_PERSIST_TO_DISK_TIMER_ID );
            break;
        }
    }

    return ret;
}

uint64_t partition::persist_to_disk_internal( data_persister* part_persister ) {
    DCHECK( part_persister );

    part_persister->open();

    uint64_t version = shared_partition_->persist_metadata( part_persister );
    (void) version;

    part_persister->write_i32( (uint32_t) get_partition_type() );

    auto stats = internal_partition_->persist_to_disk( part_persister );
    shared_partition_->set_storage_stats( stats );

    part_persister->close();

    return version;
}

void partition::restore_from_disk( data_reader* part_reader ) {
    switch( get_partition_type() ) {
        case partition_type::type::ROW: {
            start_timer( ROW_RESTORE_FROM_DISK_TIMER_ID );
            restore_from_disk_internal( part_reader );
            stop_timer( ROW_RESTORE_FROM_DISK_TIMER_ID );
            break;
        }
        case partition_type::type::COLUMN: {
            start_timer( COL_RESTORE_FROM_DISK_TIMER_ID );
            restore_from_disk_internal( part_reader );
            stop_timer( COL_RESTORE_FROM_DISK_TIMER_ID );
            break;
        }
        case partition_type::type::SORTED_COLUMN: {
            start_timer( SORTED_COL_RESTORE_FROM_DISK_TIMER_ID );
            restore_from_disk_internal( part_reader );
            stop_timer( SORTED_COL_RESTORE_FROM_DISK_TIMER_ID );
            break;
        }
        case partition_type::type::MULTI_COLUMN: {
            start_timer( MULTI_COL_RESTORE_FROM_DISK_TIMER_ID );
            restore_from_disk_internal( part_reader );
            stop_timer( MULTI_COL_RESTORE_FROM_DISK_TIMER_ID );
            break;
        }
        case partition_type::type::SORTED_MULTI_COLUMN: {
            start_timer( SORTED_MULTI_COL_RESTORE_FROM_DISK_TIMER_ID );
            restore_from_disk_internal( part_reader );
            stop_timer( SORTED_MULTI_COL_RESTORE_FROM_DISK_TIMER_ID );
            break;
        }
    }
}
void partition::restore_from_disk_internal( data_reader* part_reader ) {
    DCHECK( part_reader );

    part_reader->open();
    uint64_t version = shared_partition_->restore_metadata( part_reader );
    (void) version;

    uint32_t part_type;
    part_reader->read_i32( (uint32_t*) &part_type );
    DCHECK_EQ( (partition_type::type) part_type, get_partition_type() );

    internal_partition_->restore_from_disk( part_reader, version );

    part_reader->close();
}

void partition::create_new_internal_partition() {
    auto     new_internal = create_internal_partition( get_partition_type() );
    uint64_t specified_version = get_most_recent_version();
    new_internal->init( metadata_, shared_partition_->col_types_,
                        specified_version, shared_partition_, this );
    new_internal->init_records();

    internal_partition_ = new_internal;
}

bool partition::change_storage_tier( const storage_tier_type::type& tier ) {
    return change_storage_tier_internal( tier, true );
}
bool partition::change_storage_tier_internal( const storage_tier_type::type& tier,
                                     bool do_locking ) {
    if( do_locking ) {
        shared_partition_->lock_storage();
    }
    auto current_tier = shared_partition_->get_storage_tier();
    if( current_tier != tier ) {
        switch( tier ) {
            case storage_tier_type::type::MEMORY: {
                pull_from_disk_internal();
                break;
            }
            case storage_tier_type::type::DISK: {
                evict_to_disk_internal();
                break;
            }
        }
    }
    if( do_locking ) {
        shared_partition_->unlock_storage();
    }
    return true;
}

void partition::pull_from_disk_internal() {
    auto file_name = shared_partition_->get_disk_file_name();
    if( std::get<0>( file_name ) ) {
        DVLOG( 40 ) << "Begin pulling from disk:" << std::get<1>( file_name );
        create_new_internal_partition();
        data_reader reader( std::get<1>( file_name ) );
        restore_from_disk( &reader );
        shared_partition_->set_storage_tier( storage_tier_type::MEMORY );
        DVLOG( 40 ) << "Done pulling from disk:" << std::get<1>( file_name );
    }
}

void partition::evict_to_disk_internal() {
    auto file_name = shared_partition_->get_disk_file_name();
    if( std::get<0>( file_name ) ) {
        DVLOG( 40 ) << "Begin evicting to disk:" << std::get<1>( file_name );
        data_persister persister( std::get<1>( file_name ) );
        uint64_t       version = persist_to_disk( &persister );
        create_new_internal_partition();
        shared_partition_->set_storage_tier( storage_tier_type::DISK );
        shared_partition_->storage_version_ = version;
        DVLOG( 40 ) << "Done evicting to disk:" << std::get<1>( file_name );
    }
}

void partition::pull_data_if_not_in_memory( bool do_locking, bool do_notify ) {
    auto tier = shared_partition_->get_storage_tier();
    if( tier != storage_tier_type::type::MEMORY ) {
        DVLOG( 40 ) << "Pull data if not data in memory:"
                    << metadata_.partition_id_;

        auto ret = change_storage_tier_internal(
            storage_tier_type::type::MEMORY, do_locking );
        table* t = (table*) shared_partition_->table_ptr_;
        if( t and do_notify ) {
            auto stats = t->get_stats();
            if( stats ) {
                DVLOG( 40 )
                    << "Pull data if not data in memory, recording stats:"
                    << metadata_.partition_id_;
                stats->record_storage_tier_change(
                    metadata_.partition_id_, storage_tier_type::type::MEMORY );
            }
        }
        DCHECK( ret );
    }
}
