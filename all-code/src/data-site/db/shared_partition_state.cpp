#include "shared_partition_state.h"

#include <glog/logging.h>

#include "../../common/partition_funcs.h"
#include "../../common/perf_tracking.h"
#include "predicate.h"
#include "table.h"
#include "transaction_partition_holder.h"

shared_partition_state::shared_partition_state()
    : metadata_(),
      txn_execution_state_(),
      master_location_( k_unassigned_master ),
      dependency_(),
      update_queue_(),
      apply_version_( 1 ),
      update_destination_( nullptr ),
      part_version_holder_( nullptr ),
      col_types_(),
      storage_tier_( storage_tier_type::type::MEMORY ),
      storage_lock_(),
      storage_version_( 0 ),
      stored_stats_(),
      table_ptr_( nullptr ) {}

shared_partition_state::~shared_partition_state() {}

partition_metadata shared_partition_state::get_metadata() const {
    return metadata_;
}
uint32_t shared_partition_state::get_master_location() const {
    uint32_t master_location = master_location_.load();
    return master_location;
}
cell_data_type shared_partition_state::get_cell_type(
    const cell_identifier& ci ) const {
    check_cell_identifier_within_partition( ci );
    return col_types_.at( translate_column( ci.col_id_ ) );
}

void shared_partition_state::set_master_location( uint32_t new_master ) {
    master_location_.store( new_master );
}
void shared_partition_state::remaster( uint32_t new_master ) {
    set_master_location( new_master );
    txn_execution_state_.add_to_write_buffer(
        metadata_.partition_id_, 0, (uint32_t) new_master, K_REMASTER_OP );
}

void shared_partition_state::check_cell_identifier_within_partition(
    const cell_identifier& ci ) const {
    DCHECK_EQ( ci.table_id_, metadata_.partition_id_.table_id );
    DCHECK_GE( ci.key_, metadata_.partition_id_.partition_start );
    DCHECK_LE( ci.key_, metadata_.partition_id_.partition_end );
    DCHECK_GE( ci.col_id_, metadata_.partition_id_.column_start );
    DCHECK_LE( ci.col_id_, metadata_.partition_id_.column_end );
}
void shared_partition_state::check_record_identifier_within_partition(
    const record_identifier& ri ) const {
    DCHECK_EQ( ri.table_id_, metadata_.partition_id_.table_id );
    DCHECK_GE( ri.key_, metadata_.partition_id_.partition_start );
    DCHECK_LE( ri.key_, metadata_.partition_id_.partition_end );
}

uint64_t shared_partition_state::get_most_recent_version() {
    return dependency_.get_version();
}

void shared_partition_state::get_most_recent_snapshot_vector(
    snapshot_vector& snapshot ) {
    start_extra_timer( PARTITION_GET_MOST_RECENT_SNAPSHOT_VECTOR_TIMER_ID );

    DVLOG(20) << "Getting most recent snapshot vector";

    set_snapshot_version( snapshot, metadata_.partition_id_hash_,
                          dependency_.get_version() );

    dependency_.get_dependency( snapshot );

    stop_extra_timer( PARTITION_GET_MOST_RECENT_SNAPSHOT_VECTOR_TIMER_ID );
}

void shared_partition_state::build_commit_snapshot(
    snapshot_vector& snapshot ) {
    start_timer( PARTITION_BUILD_COMMIT_SNAPSHOT_TIMER_ID );

    uint64_t expected_commit_version = dependency_.get_version();
    DVLOG( 20 ) << "Snapshot:" << snapshot << ", this:" << metadata_;

    if( txn_execution_state_.get_write_buffer()->empty() ) {
        expected_commit_version =
            get_snapshot_version( snapshot, metadata_.partition_id_hash_ );
    } else {
        expected_commit_version += 1;
        DVLOG( 20 ) << "Setting snapshot:" << metadata_.partition_id_hash_
                    << " = " << expected_commit_version;
        set_snapshot_version( snapshot, metadata_.partition_id_hash_,
                              expected_commit_version );
        DVLOG( 20 ) << "New Snapshot:" << snapshot;
    }

    stop_timer( PARTITION_BUILD_COMMIT_SNAPSHOT_TIMER_ID );
}

void shared_partition_state::set_commit_version( uint64_t version ) {
    dependency_.set_version( version );
    record_version_on_table( version );
}

void shared_partition_state::set_commit_version_and_update_queue_if_able(
    uint64_t expected_version ) {
    uint64_t new_version =
        dependency_.update_version_if_less_than( expected_version - 1 );
    if( new_version == expected_version - 1 ) {
        set_update_queue_expected_version( expected_version );
        set_commit_version( new_version );
    }
}
void shared_partition_state::finalize_commit(
    const snapshot_vector& dependency_snapshot, uint64_t version,
    uint64_t table_epoch, bool no_prop_commit ) {
    txn_execution_state_.set_committed( dependency_snapshot,
                                        metadata_.partition_id_,
                                        metadata_.partition_id_hash_, version );

    if( !no_prop_commit ) {

        dependency_.make_version_visible( version );

        // Advancing update queue
        set_update_queue_expected_version( version + 1 );

        record_version_and_epoch_on_table( version, table_epoch );
    }
}

uint32_t shared_partition_state::add_stashed_update( stashed_update&& update ) {

    DVLOG( 40 ) << "Adding stashed update to:" << metadata_.partition_id_
                << ", update:" << update.deserialized_->pcid_
                << ", version:" << update.commit_version_;
    if( update.commit_version_ <= dependency_.get_version_no_lock() ) {
        DVLOG( 40 ) << "Already applied this update, destroying";
        destroy_stashed_update( update );
        return 0;
    }
    return update_queue_.add_update( std::move( update ) );
}

void shared_partition_state::set_update_queue_expected_version( uint64_t expected_version ) {
    DVLOG( 30 ) << "Setting partition: " << metadata_.partition_id_
                << " update queue expected version to:" << expected_version;
    update_queue_.set_next_expected_version( expected_version );
}

void shared_partition_state::propagate_updates( uint64_t version ) {
    write_buffer* write_buf = txn_execution_state_.get_write_buffer();
    DVLOG( 15 ) << "propagate_updates:" << metadata_.partition_id_
                << ", version:" << version;
    if( ( txn_execution_state_.get_write_buffer()->empty() ) or
        ( write_buf->do_not_propagate_ ) ) {
        DVLOG( 20 ) << "propagate_updates:" << metadata_.partition_id_
                    << ", version:" << version << ", empty or do_not_prop";
        return;
    } else if( metadata_.site_location_ !=
               txn_execution_state_.get_master_site() ) {
        DVLOG( 20 ) << "propagate_updates:" << metadata_.partition_id_
                    << ", version:" << version << ", not mastered:"
                    << txn_execution_state_.get_master_site()
                    << ", metadata_.site_location:" << metadata_.site_location_;
        // txn_execution_state_ has the master state at the beginning of the
        // transaction, so if there is a remastering then it will reflect that
        return;
    }

    // write the update out
    serialized_update update = serialize_write_buffer( write_buf );
    DCHECK_EQ( get_serialized_update_message_id( update ), version );
    DVLOG( 40 ) << "Sending update:" << metadata_.partition_id_
                << ", version:" << version;

    update_destination_->send_update( std::move( update ),
                                      metadata_.partition_id_ );

    // if change destination then change it here
    auto new_update_destination =
        txn_execution_state_.get_new_update_destination();
    if( new_update_destination ) {
        update_destination_ = new_update_destination;
    }
}

uint64_t shared_partition_state::wait_for_dependency_version(
    uint64_t wait_version ) {
    DVLOG( 40 ) << "Waiting for updates until:" << wait_version;
    return dependency_.wait_until( wait_version );
}

void shared_partition_state::record_version_on_table( uint64_t version ) {
    part_version_holder_->set_version( version );
}

void shared_partition_state::record_version_and_epoch_on_table(
    uint64_t version, uint64_t epoch ) {
    part_version_holder_->set_version_and_epoch( version, epoch );
}

std::shared_ptr<partition_column_version_holder>
    shared_partition_state::get_partition_column_version_holder() const {
    return part_version_holder_;
}

uint32_t shared_partition_state::translate_column( uint32_t raw_col_id ) const {
    return normalize_column_id( raw_col_id, metadata_.partition_id_ );
}

void shared_partition_state::switch_update_destination(
    std::shared_ptr<update_destination_interface> new_update_destination ) {
    DVLOG( 20 ) << "Partition: " << metadata_.partition_id_
                << ", switching update destination to:"
                << new_update_destination->get_propagation_configuration();

    // somehow store this new update_destination, but can't just do anything
    // because if we change the update_destination, then we will write to the
    // wrong place
    txn_execution_state_.add_new_update_destination( new_update_destination );

    update_propagation_information old_info;
    update_propagation_information new_info;

    old_info.propagation_config_ =
        update_destination_->get_propagation_configuration();
    new_info.propagation_config_ =
        new_update_destination->get_propagation_configuration();

    old_info.identifier_ = metadata_.partition_id_;
    new_info.identifier_ = metadata_.partition_id_;

    old_info.do_seek_ = false;
    new_info.do_seek_ = should_seek_topic( old_info, new_info );

    add_switch_subscription_state_to_write_buffer( old_info, new_info );
    txn_execution_state_.add_to_write_buffer( metadata_.partition_id_, 0, 0,
                                              K_CHANGE_DESTINATION_OP );
}

void shared_partition_state::set_update_destination(
    std::shared_ptr<update_destination_interface> new_update_destination ) {
    DVLOG( 20 ) << "Partition: " << metadata_.partition_id_
                << ", setting update destination:"
                << new_update_destination->get_propagation_configuration();
    update_destination_ = new_update_destination;
}
update_propagation_information
    shared_partition_state::get_update_propagation_information() {
    update_propagation_information info;
    info.identifier_ = metadata_.partition_id_;
    info.propagation_config_ =
        update_destination_->get_propagation_configuration();
    info.do_seek_ = true;
    return info;
}
void shared_partition_state::add_subscription_state_to_write_buffer(
    const update_propagation_information& update_prop_info,
    bool                                  do_start_subscribing ) {
    txn_execution_state_.add_to_write_buffer( update_prop_info,
                                              do_start_subscribing );
}
void shared_partition_state::add_switch_subscription_state_to_write_buffer(
    const update_propagation_information& old_sub,
    const update_propagation_information& new_sub ) {
    txn_execution_state_.add_to_write_buffer( old_sub, new_sub );
}

void shared_partition_state::add_repartition_op(
    const partition_metadata& metadata, uint32_t col_split, uint64_t row_split,
    uint32_t op_code ) {
    txn_execution_state_.add_to_write_buffer( metadata.partition_id_, row_split,
                                              col_split, op_code );
}

std::tuple<partition_column_identifier, partition_column_identifier>
    construct_split_partition_column_identifiers(
        const partition_column_identifier& pid, int64_t split_row_point,
        int32_t split_col_point ) {
    DVLOG( 40 ) << "Construct split partition identifiers:" << pid
                << " @ row:" << split_row_point
                << ", @ col:" << split_col_point;

    partition_column_identifier low_pid = pid;
    partition_column_identifier high_pid = pid;

    if( split_row_point != k_unassigned_key ) {
        DCHECK_EQ( split_col_point, k_unassigned_col );

        DCHECK_GT( split_row_point, pid.partition_start );
        DCHECK_LE( split_row_point, pid.partition_end );

        low_pid.partition_end = split_row_point - 1;
        high_pid.partition_start = split_row_point;
    }

    if( split_col_point != k_unassigned_col ) {
        DCHECK_EQ( split_row_point, k_unassigned_key );

        DCHECK_GT( split_col_point, pid.column_start );
        DCHECK_LE( split_col_point, pid.column_end );

        low_pid.column_end = split_col_point - 1;
        high_pid.column_start = split_col_point;
    }

    return std::make_tuple<partition_column_identifier,
                           partition_column_identifier>(
        std::move( low_pid ), std::move( high_pid ) );
}

void shared_partition_state::snapshot_partition_metadata(
    snapshot_partition_column_state& snapshot ) const {
    for( int32_t col_id = metadata_.partition_id_.column_start;
         col_id <= metadata_.partition_id_.column_end; col_id++ ) {
        uint32_t pos = snapshot.columns.size();
        snapshot.columns.emplace_back( snapshot_column_state() );
        snapshot.columns.at( pos ).col_id = col_id;
        snapshot.columns.at( pos ).type =
            cell_data_type_to_data_type( col_types_.at( pos ) );
    }
}

std::vector<uint32_t> shared_partition_state::get_col_ids_to_scan(
    uint64_t low_key, uint64_t high_key,
    const std::vector<uint32_t>& project_cols ) const {
    return get_col_ids_to_scan_from_partition_id(
        low_key, high_key, project_cols, metadata_.partition_id_ );
}

predicate_chain shared_partition_state::translate_predicate_to_partition(
    const predicate_chain& predicate ) const {
    return translate_predicate_to_partition_id(
        predicate, metadata_.partition_id_, col_types_ );
}

void shared_partition_state::remove_table_stats_data( const cell_identifier& ci,
                                                      int32_t sz ) {
    if( table_ptr_ and
        ( get_master_location() == metadata_.site_location_ ) ) {
        table* t = (table*) table_ptr_;
        // MTODO-STRATEGIES decide if should do this every time?
        t->get_stats()->remove_width( ci.col_id_, sz );
    }
}
void shared_partition_state::update_table_stats_data( const cell_identifier& ci,
                                                      int32_t old_size,
                                                      int32_t new_size ) {
    if( table_ptr_ and
        ( get_master_location() == metadata_.site_location_ ) ) {
        table* t = (table*) table_ptr_;
        // MTODO-STRATEGIES decide if should do this every time?
        DVLOG( 20 ) << "Update table stats:" << ci << ", old_size:" << old_size
                    << ", new_size:" << new_size;
        if( old_size < 0 ) {
            t->get_stats()->add_width( ci.col_id_, new_size );
        } else {
            t->get_stats()->update_width( ci.col_id_, old_size, new_size );
        }
    }
}

std::tuple<bool, std::string> shared_partition_state::get_disk_file_name()
    const {
    bool        enable = false;
    const auto& pid = metadata_.partition_id_;
    std::string partition_directory = "";
    if( table_ptr_ ) {
        table* t = (table*) table_ptr_;
        enable = t->get_metadata().enable_secondary_storage_;
        partition_directory = t->get_metadata().secondary_storage_dir_;
    }
    std::string outfile = partition_directory + "/" +
                          std::to_string( pid.table_id ) + "-" +
                          std::to_string( pid.partition_start ) + "-" +
                          std::to_string( pid.partition_end ) + "-" +
                          std::to_string( pid.column_start ) + "-" +
                          std::to_string( pid.column_end ) + ".p";

    return std::make_tuple<>( enable, outfile );
}

void shared_partition_state::lock_storage() { storage_lock_.lock(); }
void shared_partition_state::unlock_storage() { storage_lock_.unlock(); }

void shared_partition_state::set_storage_tier(
    const storage_tier_type::type& tier ) {
    storage_tier_ = tier;
}
storage_tier_type::type shared_partition_state::get_storage_tier() {
    storage_tier_type::type tier = storage_tier_.load();
    return tier;
}

void shared_partition_state::set_storage_stats(
    column_stats<multi_column_data>& stats ) {
    stored_stats_.count_ = stats.count_;
    if ( stored_stats_.count_ > 0) {
        stored_stats_.min_.deep_copy( stats.min_ );
        stored_stats_.max_.deep_copy( stats.max_ );
        stored_stats_.average_.deep_copy( stats.average_ );
        stored_stats_.sum_.deep_copy( stats.sum_ );
    } else {
        stored_stats_.min_.deep_copy( get_zero<multi_column_data>() );
        stored_stats_.max_.deep_copy( get_zero<multi_column_data>() );
        stored_stats_.average_.deep_copy( get_zero<multi_column_data>() );
        stored_stats_.sum_.deep_copy( get_zero<multi_column_data>() );
    }
    DVLOG( 40 ) << "Set storage stats to:" << stored_stats_;
}

uint64_t shared_partition_state::persist_metadata(
    data_persister* part_persister ) {

    auto pid = get_metadata().partition_id_;

    part_persister->write_i32( pid.table_id );
    part_persister->write_i64( pid.partition_start );
    part_persister->write_i64( pid.partition_end );
    part_persister->write_i32( pid.column_start );
    part_persister->write_i32( pid.column_end );

    part_persister->write_i32( get_master_location() );

    uint64_t pid_hash = get_metadata().partition_id_hash_;

    snapshot_vector snap;
    get_most_recent_snapshot_vector( snap );
    uint64_t version = get_snapshot_version( snap, pid_hash );

    part_persister->write_i64( version );

    return version;
}

uint64_t shared_partition_state::restore_metadata( data_reader* part_reader ) {

    auto pid = get_metadata().partition_id_;

    int32_t read_table_id;
    int64_t read_part_start;
    int64_t read_part_end;
    int32_t read_col_start;
    int32_t read_col_end;

    part_reader->read_i32( (uint32_t*) &read_table_id );
    part_reader->read_i64( (uint64_t*) &read_part_start );
    part_reader->read_i64( (uint64_t*) &read_part_end );
    part_reader->read_i32( (uint32_t*) &read_col_start );
    part_reader->read_i32( (uint32_t*) &read_col_end );

    DCHECK_EQ( read_table_id, pid.table_id );
    DCHECK_EQ( read_part_start, pid.partition_start );
    DCHECK_EQ( read_part_end, pid.partition_end );
    DCHECK_EQ( read_col_start, pid.column_start );
    DCHECK_EQ( read_col_end, pid.column_end );

    uint32_t read_master_loc;
    part_reader->read_i32( (uint32_t*) &read_master_loc );

    uint64_t read_version;

    part_reader->read_i64( (uint64_t*) &read_version );

    return read_version;
}

