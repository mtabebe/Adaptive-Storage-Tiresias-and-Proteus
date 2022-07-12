#include "table.h"

#include <glog/logging.h>

#include "../../persistence/persistence_utils.h"
#include "col_partition.h"
#include "partition.h"
#include "partition_util.h"
#include "row_partition.h"
#include "transaction_partition_holder.h"

table::table()
    : metadata_(),
      partition_map_(),
      gc_partition_map_(),
      part_counters_(),
      update_gen_( nullptr ),
      enqueuers_( nullptr ),
      poll_epoch_( 0 ),
      stats_( nullptr ) {}
table::~table() {}

void table::init( const table_metadata&                         metadata,
                  std::shared_ptr<update_destination_generator> update_gen,
                  std::shared_ptr<update_enqueuers>             enqueuers ) {
    metadata_ = metadata;
    DCHECK_EQ( metadata_.num_columns_, metadata_.col_types_.size() );
    update_gen_ = update_gen;
    enqueuers_ = enqueuers;
    stats_ = std::make_unique<table_stats>( metadata );

    if ( metadata_.enable_secondary_storage_) {
        create_directory( metadata_.secondary_storage_dir_ );
    }
}

void table::get_partitions(
    const partition_column_identifier_set&     pids,
    partition_column_identifier_partition_map& partitions ) {
    for( const auto& pid : pids ) {
        partitions[pid] = std::move( get_partition( pid ) );
    }
}

std::shared_ptr<partition> table::add_partition(
    const partition_column_identifier& pid) {
    return add_partition( pid, metadata_.default_partition_type_,
                          metadata_.default_storage_type_ );
}

std::shared_ptr<partition> table::add_partition(
    const partition_column_identifier& pid, const partition_type::type& p_type,
    const storage_tier_type::type& s_type ) {
    return add_partition( pid, p_type, s_type, false /* pin*/, true /*lock*/ );
}
std::shared_ptr<partition> table::add_partition(
    const partition_column_identifier& pid, const partition_type::type& p_type,
    const storage_tier_type::type& s_type, bool do_pin, bool do_lock ) {
    return add_partition(
        pid, p_type, s_type, do_pin, 0, do_lock,
        update_gen_->get_update_destination_by_hashing_partition_id( pid ) );
}
std::shared_ptr<partition> table::add_partition(
    const partition_column_identifier& pid, const partition_type::type& p_type,
    const storage_tier_type::type& s_type, bool do_pin,
    uint64_t specified_version, bool acquire_lock,
    std::shared_ptr<update_destination_interface> update_destination ) {
    DVLOG( 20 ) << "Add partition:" << pid << ", type:" << p_type
                << ", storage type:" << s_type;

    partition* part = create_partition_without_adding_to_table(
        pid, p_type, s_type, do_pin, specified_version, update_destination );

    partition_column_identifier pid_copy = pid;
    std::shared_ptr<partition>  part_ptr( part );

    DVLOG( 20 ) << "Insert partition:" << pid;
    auto insertion = partition_map_.insert( std::move( pid_copy ), part_ptr );

    if( !insertion.second ) {
        DVLOG( 20 ) << "Unlock the partition we inserted";
        // already exists so delete our old one
        part_ptr->unlock_partition();
        DVLOG( 20 ) << "Delete the partition we inserted";
        // delete part;
        part_ptr.reset();  // free the object
        DVLOG( 20 ) << "Getting existing partition";
        part_ptr = insertion.first->second;
        if( acquire_lock ) {
            DVLOG( 20 ) << "Locking existing partition";
            part_ptr->lock_partition();
            // MTODO-PIN do we need to do something different here?
            part_ptr->set_update_destination( update_destination );
        }
        if( do_pin ) {
            part_ptr->pin_partition();
        }
    } else if( !acquire_lock ) {
        part_ptr->unlock_partition();
    }

    if( insertion.second ) {
        part_counters_.insert_or_assign(
            pid, part_ptr->get_partition_column_version_holder() );
    }

    DVLOG( 20 ) << "Partition:" << part_ptr->get_metadata();
    DVLOG( 20 ) << "Add partition:" << pid << "=" << part_ptr.get();

    return std::move( part_ptr );
}

partition* table::create_partition_without_adding_to_table(
    const partition_column_identifier& pid, const partition_type::type& p_type,
    const storage_tier_type::type& s_type, bool do_pin,
    uint64_t specified_version ) {
    return create_partition_without_adding_to_table(
        pid, p_type, s_type, do_pin, specified_version,
        update_gen_->get_update_destination_by_hashing_partition_id( pid ) );
}

partition* table::create_partition_without_adding_to_table(
    const partition_column_identifier& pid, const partition_type::type& p_type,
    const storage_tier_type::type& s_type, bool do_pin,
    uint64_t                                      specified_version,
    std::shared_ptr<update_destination_interface> update_destination ) {
    partition_metadata p_metadata = create_partition_metadata(
        pid.table_id, pid.partition_start, pid.partition_end, pid.column_start,
        pid.column_end, metadata_.num_records_in_chain_,
        metadata_.num_records_in_snapshot_chain_, metadata_.site_location_ );

    auto part_version_holder =
        std::make_shared<partition_column_version_holder>(
            pid, specified_version, get_poll_epoch() );

    partition* part = create_partition_ptr( p_type );
    part->init( p_metadata, update_destination, part_version_holder,
                get_cell_data_types( pid ), this, specified_version );
    part->lock_partition();

    part->init_records();

    if( s_type == storage_tier_type::type::DISK ) {
        part->change_storage_tier( s_type );
    }

    if( do_pin ) {
        part->pin_partition();
    }
    return part;
}

std::vector<cell_data_type> table::get_cell_data_types(
    const partition_column_identifier& pid ) const {
    DCHECK_LT( pid.column_end, metadata_.num_columns_ );
    std::vector<cell_data_type> types;
    for( int32_t pos = pid.column_start; pos <= pid.column_end; pos++ ) {
        types.push_back( metadata_.col_types_.at( pos ) );
    }
    return types;
}
cell_data_type table::get_cell_data_type( const cell_identifier& cid ) const {
    auto found = metadata_.col_types_.at( cid.col_id_ );
    DVLOG( 40 ) << "Get cell data type:" << cid << ", col type:" << found;

    return found;
}

void table::remove_partition( const partition_column_identifier& pid ) {
    start_timer( REMOVE_PARTITION_TIMER_ID );

    DVLOG( 20 ) << "Remove partition:" << pid;
    // should free the pointer, if no one holds a reference
    // MTODO: should we return something here?
    auto part = get_partition( pid );
    if( !part->is_pinned() ) {

        snapshot_vector snapshot;
        set_snapshot_version( snapshot, pid, K_COMMITTED );

        enqueuers_->remove_subscription( pid, snapshot,
                                         k_remove_replica_cause_string );
        remove_pid_from_part_counters( pid );

        // erase after we do the state change
        partition_map_.erase( pid );
    } else {
        mark_partition_as_inactive_and_add_partition_to_gc( pid, part );
    }
    stop_timer( REMOVE_PARTITION_TIMER_ID );
}

void table::mark_partition_as_inactive_and_add_partition_to_gc(
    const partition_column_identifier& pid, std::shared_ptr<partition> part ) {
    DVLOG( 20 ) << "Mark partition as inactive and add to gc:" << pid;
    part->mark_as_inactive();
    gc_partition_map_.insert( pid, part );
}
void table::pin_and_mark_partition_as_active(
    const partition_column_identifier& pid, std::shared_ptr<partition> part ) {
    DVLOG( 20 ) << "Pin and mark partition as active:" << pid;
    part->pin_partition();
    mark_partition_as_active( pid, part );
}
void table::mark_partition_as_active( const partition_column_identifier& pid,
                                      std::shared_ptr<partition> part ) {
    DVLOG( 20 ) << "Mark partition as active:" << pid;
    part->mark_as_active();
    gc_partition_map_.erase( pid );
}

void table::gc_inactive_partitions() {
    std::vector<std::shared_ptr<partition>> partitions_safe_to_gc;
    for( auto& entry : gc_partition_map_ ) {
        auto part = entry.second;
        if( part->is_safe_to_remove() ) {
            partitions_safe_to_gc.push_back( part );
        }
    }
    for( auto& part : partitions_safe_to_gc ) {
        const partition_column_identifier pid =
            part->get_metadata().partition_id_;
        if( part->is_safe_to_remove() ) {
            partition_map_.erase( pid );
            gc_partition_map_.erase( pid );

            snapshot_vector snapshot;
            set_snapshot_version( snapshot, pid, K_COMMITTED );

            enqueuers_->remove_subscription( pid, snapshot,
                                             k_remove_replica_cause_string );
            remove_pid_from_part_counters( pid );
        }
    }
}

bool table::insert_partition( const partition_column_identifier& pid,
                              std::shared_ptr<partition>         part ) {
    partition_column_identifier pid_copy = pid;
    DVLOG( 20 ) << "Insert partition:" << pid;
    auto insertion = partition_map_.insert( std::move( pid_copy ), part );
    bool inserted = insertion.second;
    ;
    return inserted;
}
bool table::set_partition( const partition_column_identifier& pid,
                           std::shared_ptr<partition>         part ) {
    partition_column_identifier pid_copy = pid;
    DVLOG( 20 ) << "Insert partition:" << pid;
    auto assignment = partition_map_.assign( std::move( pid_copy ), part );
    bool assigned = assignment.hasValue();
    return assigned;

}

std::shared_ptr<partition> table::get_or_create_partition(
    const partition_column_identifier& pid ) {
    return get_or_create_partition_and_pin_helper( pid, false /*pin*/ );
}

std::shared_ptr<partition> table::get_or_create_partition_and_pin(
    const partition_column_identifier& pid ) {
    return get_or_create_partition_and_pin_helper( pid, true /*pin*/ );
}

std::shared_ptr<partition> table::get_or_create_partition_and_pin_helper(
    const partition_column_identifier& pid, bool do_pin ) {
    std::shared_ptr<partition> part = std::move( get_partition( pid ) );
    if( part == nullptr ) {
        // MTODO-STRATEGY do we need to change this type
        part = std::move( add_partition( pid, metadata_.default_partition_type_,
                                         metadata_.default_storage_type_,
                                         do_pin, false /* lock*/ ) );
        // part->unlock_partition();
    } else if( do_pin ) {
        pin_and_mark_partition_as_active( pid, part );
    }
    return std::move( part );
}

split_partition_result table::split_partition(
    const snapshot_vector& snapshot, const partition_column_identifier& old_pid,
    uint64_t split_row_point, uint32_t split_col_point,
    const partition_type::type& low_type, const partition_type::type& high_type,
    const storage_tier_type::type&                low_storage_type,
    const storage_tier_type::type&                high_storage_type,
    std::shared_ptr<update_destination_interface> low_update_destination,
    std::shared_ptr<update_destination_interface> high_update_destination ) {
    DVLOG( 10 ) << "Split partition:" << old_pid
                << " at row:" << split_row_point << ", col:" << split_col_point;

    if( split_row_point != k_unassigned_key ) {
        DCHECK_EQ( split_col_point, k_unassigned_col );
    }
    if( split_col_point != k_unassigned_col ) {
        DCHECK_EQ( split_row_point, k_unassigned_key );
    }

    std::tuple<partition_column_identifier, partition_column_identifier>
        split_pids = construct_split_partition_column_identifiers(
            old_pid, split_row_point, split_col_point );

    partition_column_identifier low_pid = std::get<0>( split_pids );
    partition_column_identifier high_pid = std::get<1>( split_pids );

    split_partition_result result;
    result.partition_cover_ = std::move( get_partition( old_pid ) );

    if( result.partition_cover_ == nullptr ) {
        result.okay_ = false;
        DLOG( WARNING ) << "Unable to split partition:" << old_pid
                        << ", as it does not exist";
        return result;
    }

    DCHECK( result.partition_cover_ );
    result.okay_ = true;

    // remove the partition from the map, this will not free the partition,
    // because we have a reference
    remove_partition( old_pid );

    result.partition_cover_->lock_partition();

    snapshot_vector ori_snap;
    result.partition_cover_->get_most_recent_snapshot_vector( ori_snap );

    uint64_t ori_v = get_snapshot_version( ori_snap, old_pid );

    uint64_t low_version =
        std::max( get_snapshot_version( ori_snap, low_pid ), ori_v );

    uint64_t high_version =
        std::max( get_snapshot_version( ori_snap, high_pid ), ori_v );

    // add the partitions to the map (going to own the lock)
    result.partition_low_ = std::move(
        add_partition( low_pid, low_type, storage_tier_type::MEMORY, false /*pin*/,
                       low_version, true /*lock*/, low_update_destination ) );
    result.partition_high_ = std::move( add_partition(
        high_pid, high_type, storage_tier_type::MEMORY, false /* pin*/,
        high_version, true /*lock*/, high_update_destination ) );

    std::vector<storage_tier_type::type> storage_tiers = {low_storage_type,
                                                          high_storage_type};

    transaction_partition_holder holder;
    snapshot_vector              commit_vv = holder.execute_split_merge(
        snapshot, result, split_col_point, split_row_point, K_SPLIT_OP,
        storage_tiers, get_poll_epoch() );
    result.snapshot_ = std::move( commit_vv );

    // free the partitions implicitly

    DVLOG( 10 ) << "Split partition:" << old_pid
                << " at row:" << split_row_point << ", col:" << split_col_point
                << "= [(" << low_pid << "," << result.partition_low_.get()
                << "), (" << high_pid << "," << result.partition_high_.get()
                << ")]";
    return result;
}

split_partition_result table::merge_partition(
    const snapshot_vector& snapshot, const partition_column_identifier& low_pid,
    const partition_column_identifier&            high_pid,
    const partition_type::type&                   merge_type,
    const storage_tier_type::type&                merge_storage_type,
    std::shared_ptr<update_destination_interface> update_destination ) {
    DVLOG( 10 ) << "Merge partitions:" << low_pid << ", " << high_pid;

    split_partition_result result;
    result.partition_low_ = std::move( get_partition( low_pid ) );
    result.partition_high_ = std::move( get_partition( high_pid ) );

    if( ( result.partition_low_ == nullptr ) or
        ( result.partition_high_ == nullptr ) ) {
        result.okay_ = false;
        DLOG( WARNING ) << "Unable to merge partitions:" << low_pid
                        << ", and:" << high_pid << ", as they do not exist";
        return result;
    }

    result.okay_ = true;

    DCHECK( result.partition_low_ );
    DCHECK( result.partition_high_ );

    DCHECK_EQ( low_pid.table_id, high_pid.table_id );

    uint64_t row_merge_point = k_unassigned_key;
    uint32_t col_merge_point = k_unassigned_col;

    if( low_pid.partition_end != high_pid.partition_end ) {
        DCHECK_EQ( low_pid.partition_end, high_pid.partition_start - 1 );
        DCHECK_EQ( low_pid.column_start, high_pid.column_start );
        DCHECK_EQ( low_pid.column_end, high_pid.column_end );
        row_merge_point = high_pid.partition_start;
    }
    if( low_pid.column_end != high_pid.column_end ) {
        DCHECK_EQ( low_pid.column_end, high_pid.column_start - 1 );
        DCHECK_EQ( low_pid.partition_start, high_pid.partition_start );
        DCHECK_EQ( low_pid.partition_end, high_pid.partition_end );

        col_merge_point = high_pid.column_start;
    }

    partition_column_identifier merged_pid = low_pid;
    merged_pid.partition_end = high_pid.partition_end;
    merged_pid.column_end = high_pid.column_end;

    // LOCK
    result.partition_low_->lock_partition();
    result.partition_high_->lock_partition();

    // remove the partitions from the map, this will not free the partition,
    // because we have a reference
    remove_partition( low_pid );
    remove_partition( high_pid );

    snapshot_vector low_ori_snap;
    result.partition_low_->get_most_recent_snapshot_vector( low_ori_snap );

    snapshot_vector high_ori_snap;
    result.partition_high_->get_most_recent_snapshot_vector( high_ori_snap );

    uint64_t merged_version = std::max(
        get_snapshot_version( low_ori_snap, merged_pid ),
        std::max( get_snapshot_version( high_ori_snap, merged_pid ),
                  std::max( get_snapshot_version( high_ori_snap, high_pid ),
                            get_snapshot_version( low_ori_snap, low_pid ) ) ) );

    // add the partition to the map
    // we have the lock of this partition
    result.partition_cover_ = std::move( add_partition(
        merged_pid, merge_type, storage_tier_type::type::MEMORY, false /*pin*/,
        merged_version, true /*lock*/, update_destination ) );

    std::vector<storage_tier_type::type> storage_tiers = {merge_storage_type};

    // do the actual merge
    transaction_partition_holder holder;
    snapshot_vector              commit_vv = holder.execute_split_merge(
        snapshot, result, col_merge_point, row_merge_point, K_MERGE_OP,
        storage_tiers, get_poll_epoch() );

    result.snapshot_ = std::move( commit_vv );
    // high and low partitions will no longer be owned (so they will be freed)

    DVLOG( 10 ) << "Merge partitions:" << low_pid << ", " << high_pid << "= ("
                << merged_pid << ", " << result.partition_cover_.get() << ")";

    return result;
}

std::shared_ptr<partition> table::get_partition_if_active(
    const partition_column_identifier& id ) const {
    auto part = get_partition( id );
    if( part and !part->is_active() ) {
        DVLOG( 30 ) << "Found Partition:" << id << "=" << part
                    << ", but it is inactive, returning nullptr";
        return nullptr;
    }
    return part;
}

std::shared_ptr<partition> table::get_partition(
    const partition_column_identifier& id ) const {
    DVLOG( 30 ) << "Get Partition:" << id;
    DCHECK_EQ( id.table_id, metadata_.table_id_ );
    std::shared_ptr<partition> part = nullptr;
    auto                       found = partition_map_.find( id );
    if( found != partition_map_.cend() ) {
        part = found->second;
    }
    DVLOG( 30 ) << "Found Partition:" << id << "=" << part;
    return std::move( part );
}

const table_metadata& table::get_metadata() const { return metadata_; }

void table::stop_subscribing(
    const std::vector<update_propagation_information>& subscriptions,
    const snapshot_vector& snapshot, const std::string& cause ) {
    for( const auto& info : subscriptions ) {
        enqueuers_->remove_source( info.propagation_config_, info.identifier_,
                                   snapshot, cause );
    }
}
void table::start_subscribing(
    const std::vector<update_propagation_information>& subscriptions,
    const snapshot_vector& snapshot, const std::string& cause ) {
    // group together to get the min
    enqueuers_->add_subscriptions_together_with_grouping( subscriptions,
                                                          snapshot, cause );
}
void table::mark_subscription_end(
    const std::vector<update_propagation_information>& subscriptions,
    const snapshot_vector&                             snapshot ) {
    for( const auto& info : subscriptions ) {
        propagation_configuration config = info.propagation_config_;
        config.offset = get_snapshot_version( snapshot, info.identifier_ );
        enqueuers_->mark_upper_bound_of_source( config, info.identifier_ );
    }
}

void table::switch_subscription(
    const std::vector<update_propagation_information>& subscription_switch,
    const std::string&                                 cause ) {
    DCHECK_EQ( 0, subscription_switch.size() % 2 );

    for( uint32_t pos = 0; pos < subscription_switch.size() / 2; pos++ ) {
        const auto& old_info = subscription_switch.at( pos * 2 );
        const auto& new_info = subscription_switch.at( ( pos * 2 ) + 1 );

        DCHECK_EQ( old_info.identifier_, new_info.identifier_ );
        enqueuers_->change_sources( old_info.identifier_,
                                    old_info.propagation_config_,
                                    new_info.propagation_config_, cause );
    }
}

void table::persist_table( data_site_table_persister* persister ) {
    persister->persist_table_info( metadata_ );

    for( auto& entry : partition_map_ ) {
        auto part = entry.second;
        bool locked = part->try_lock_partition();
        if( !locked ) {
            LOG( ERROR ) << "Persistence: unable to lock partition:"
                         << part->get_metadata().partition_id_;
        }

        persister->persist_partition( part );

        if( locked ) {
            part->unlock_partition();
        }
    }
}

void table::get_update_partition_states(
    std::vector<polled_partition_column_version_information>&
        polled_versions ) {

    uint64_t prev_poll_epoch = poll_epoch_.fetch_add( 1 );

    DVLOG( 40 ) << "Get update partition states:" << metadata_.table_id_
                << ", prev poll epoch:" << prev_poll_epoch;

    uint64_t get_version;
    uint64_t get_epoch;
    bool     increment = true;

    polled_partition_column_version_information polled_version;
    for( auto it = part_counters_.cbegin(); it != part_counters_.cend();
         /* intentional no increment */ ) {
        const auto& pid = it->first;
        auto        part_holder = it->second;

        part_holder->get_version_and_epoch( pid, &get_version, &get_epoch );
        if( get_epoch >= prev_poll_epoch ) {
            polled_version.pcid = pid;
            polled_version.version = get_version;

            if( get_version == K_ABORTED ) {
                polled_version.version = -1;
                it = part_counters_.erase( it );
                increment = false;
            }

            polled_versions.push_back( polled_version );
        }

        if( increment ) {
            ++it;
        }
        increment = true;
    }
}

void table::remove_pid_from_part_counters(
    const partition_column_identifier& pid ) {
    auto part_holder = std::make_shared<partition_column_version_holder>(
        pid, K_ABORTED, get_poll_epoch() );
    part_counters_.insert_or_assign( pid, part_holder );
}

uint64_t table::get_poll_epoch() const {
    uint64_t p = poll_epoch_;
    DVLOG( 40 ) << "Table:" << metadata_.table_id_ << ", poll epoch:" << p;
    return p;
}

uint64_t table::get_approx_total_number_of_partitions() const {
    return partition_map_.size();
}

table_stats* table::get_stats() const { return stats_.get(); }

