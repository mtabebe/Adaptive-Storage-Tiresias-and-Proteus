#include "internal_partition.h"

#include <glog/logging.h>

#include "../../common/perf_tracking.h"
#include "partition.h"

internal_partition::internal_partition()
    : metadata_(), shared_partition_( nullptr ), parent_partition_( nullptr ) {}

internal_partition::~internal_partition() {}
// init
void internal_partition::init(
    const partition_metadata&          metadata,
    const std::vector<cell_data_type>& col_types, uint64_t version,
    std::shared_ptr<shared_partition_state>& shared_partition,
    void*                                    parent_partition ) {
    metadata_ = metadata;
    shared_partition_ = shared_partition;
    parent_partition_ = parent_partition;
}

void internal_partition::finalize_commit(
    const snapshot_vector& dependency_snapshot, uint64_t version,
    uint64_t table_epoch, bool no_prop_commit ) {
    shared_partition_->finalize_commit( dependency_snapshot, version,
                                        table_epoch, no_prop_commit );
}

bool internal_partition::begin_read_wait_and_build_snapshot(
    snapshot_vector& snapshot, bool no_apply_last ) {

    start_timer( PARTITION_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID );

    DVLOG( 20 ) << "Begin read wait and build snapshot" << metadata_;

    uint64_t read_version = 0;
    if( no_apply_last ) {
        read_version = wait_for_version_no_apply_last( snapshot );
    } else {
        read_version = wait_until_version_or_apply_updates(
            get_snapshot_version( snapshot, metadata_.partition_id_hash_ ) );
    }
    begin_read_build_snapshot( read_version, snapshot );

    stop_timer( PARTITION_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID );

    return true;
}
void internal_partition::set_master_location( uint32_t new_master ) {
    shared_partition_->set_master_location( new_master );
}
void internal_partition::remaster( uint32_t new_master ) {
    shared_partition_->remaster( new_master );
}

uint64_t internal_partition::wait_until_version_or_apply_updates(
    uint64_t req_version ) {
    DCHECK( parent_partition_ );
    partition* p = (partition*) parent_partition_;
    return p->internal_wait_until_version_or_apply_updates( req_version );
}

void internal_partition::pull_data_if_not_in_memory() {
    DCHECK( parent_partition_ );
    partition* p = (partition*) parent_partition_;
    p->pull_data_if_not_in_memory( true /* do locking*/, true /* do notify */ );
}

uint64_t internal_partition::wait_for_version_no_apply_last(
    const snapshot_vector& snapshot ) {
    uint64_t version =
        get_snapshot_version( snapshot, metadata_.partition_id_hash_ );
    uint64_t read_version = 0;
    if( version > 0 ) {
        read_version = wait_until_version_or_apply_updates( version - 1 );
    }
    read_version = wait_for_dependency_version( version );
    return read_version;
}

uint64_t internal_partition::wait_for_dependency_version(
    uint64_t wait_version ) {
    return shared_partition_->wait_for_dependency_version( wait_version );
}

void internal_partition::begin_read_build_snapshot(
    uint64_t read_version, snapshot_vector& snapshot ) {
    set_snapshot_version( snapshot, metadata_.partition_id_hash_,
                          read_version );

    DVLOG( 20 ) << "Get dependency:" << snapshot;
    shared_partition_->dependency_.get_dependency( snapshot );
    DVLOG( 20 ) << "Got dependency:" << snapshot;
}

bool internal_partition::begin_read_wait_for_version( snapshot_vector& snapshot,
                                                      bool no_apply_last ) {
    DVLOG( 20 ) << "Begin read wait for version" << metadata_;
    if( no_apply_last ) {
        wait_for_version_no_apply_last( snapshot );
    } else {
        wait_until_version_or_apply_updates(
            get_snapshot_version( snapshot, metadata_.partition_id_hash_ ) );
    }
    return true;
}


bool internal_partition::begin_write( snapshot_vector& snapshot,
                                      bool             acquire_lock ) {
    start_timer( PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID );

    // wait for updates
    // grab lock
    DVLOG( 20 ) << "Begin write transaction:" << metadata_;
    if( acquire_lock ) {
        lock_partition();
    }
    // assert it is still a valid partition (wasn't split under us)
    // store the master site at the beginning of the transaction
    shared_partition_->txn_execution_state_.reset_state(
        shared_partition_->get_master_location() );
    shared_partition_->get_most_recent_snapshot_vector( snapshot );
    DVLOG( 20 ) << "Begin write own lock:" << metadata_
                << ", snapshot:" << snapshot;

    DVLOG( 20 ) << "Begin write transaction:" << metadata_ << ", okay!";

    stop_timer( PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID );

    return true;
}

void internal_partition::lock_partition() {
    DCHECK( parent_partition_ );
    partition* p = (partition*) parent_partition_;
    p->lock_partition();
}
bool internal_partition::try_lock_partition() {
    DCHECK( parent_partition_ );
    partition* p = (partition*) parent_partition_;
    return p->try_lock_partition();
}
void internal_partition::unlock_partition() {
    DCHECK( parent_partition_ );
    partition* p = (partition*) parent_partition_;
    p->unlock_partition();
}

void internal_partition::remove_table_stats_data( const cell_identifier& ci,
                                                  int32_t                sz ) {
    shared_partition_->remove_table_stats_data( ci, sz );
}
void internal_partition::update_table_stats_data( const cell_identifier& ci,
                                                  int32_t old_size,
                                                  int32_t new_size ) {
    shared_partition_->update_table_stats_data( ci, old_size, new_size );
}

uint32_t internal_partition::translate_column( uint32_t raw_col_id ) const {
    return shared_partition_->translate_column( raw_col_id );
}
std::vector<uint32_t> internal_partition::get_col_ids_to_scan(
    uint64_t low_key, uint64_t high_key,
    const std::vector<uint32_t>& project_cols ) const {
    return shared_partition_->get_col_ids_to_scan( low_key, high_key,
                                                   project_cols );
}

predicate_chain internal_partition::translate_predicate_to_partition(
    const predicate_chain& predicate ) const {
    return shared_partition_->translate_predicate_to_partition( predicate );
}

void internal_partition::check_cell_identifier_within_partition(
    const cell_identifier& ci ) const {
    shared_partition_->check_cell_identifier_within_partition( ci );
}
void internal_partition::check_record_identifier_within_partition(
    const record_identifier& ri ) const {
    shared_partition_->check_record_identifier_within_partition( ri );
}

void internal_partition::set_commit_version( uint64_t version ) {
    shared_partition_->set_commit_version( version );
}
void internal_partition::abort_write_transaction() {
    // free the write buffer
    shared_partition_->txn_execution_state_.abort_write();
    // unlock
    unlock_partition();
}

void internal_partition::done_with_updates() {}
cell_data_type internal_partition::get_cell_type(
    const cell_identifier& ci ) const {
    return shared_partition_->get_cell_type( ci );
}

void internal_partition::snapshot_partition_metadata(
    snapshot_partition_column_state& snapshot ) {
    shared_partition_->snapshot_partition_metadata( snapshot );
}

void internal_partition::chomp_metadata( data_reader* reader ) const {
    uint64_t version = shared_partition_->restore_metadata( reader );
    (void) version;

    uint32_t part_type;
    reader->read_i32( (uint32_t*) &part_type );
    DCHECK_EQ( (partition_type::type) part_type, get_partition_type() );
}
