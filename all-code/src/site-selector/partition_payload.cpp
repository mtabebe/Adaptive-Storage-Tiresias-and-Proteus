#include "partition_payload.h"

#include <glog/logging.h>
#ifndef GLOG_STL_LOGGING_FOR_UNORDERED
#define GLOG_STL_LOGGING_FOR_UNORDERED
#endif
#include <glog/stl_logging.h>

#include "../data-site/db/mvcc_chain.h"
#include "../data-site/db/partition.h"

partition_location_information::partition_location_information()
    : master_location_( k_unassigned_master ),
      replica_locations_(),
      partition_types_(),
      storage_types_(),
      update_destination_slot_( 0 ),
      version_( 0 ),
      session_req_( 0 ),
      in_flight_( false ),
      repartition_op_( K_NO_OP ),
      repartitioned_one_(),
      repartitioned_two_() {}

void partition_location_information::
    set_from_existing_location_information_and_increment_version(
        const std::shared_ptr<partition_location_information> &existing ) {
    master_location_ = existing->master_location_;
    replica_locations_ = existing->replica_locations_;
    partition_types_ = existing->partition_types_;
    storage_types_ = existing->storage_types_;
    update_destination_slot_ = existing->update_destination_slot_;
    version_ = existing->version_ + 1;
    session_req_ = existing->session_req_;
    in_flight_ = existing->in_flight_;

    repartition_op_ = existing->repartition_op_;
    repartitioned_one_ = existing->repartitioned_one_;
    repartitioned_two_ = existing->repartitioned_two_;
}

std::tuple<bool, partition_type::type>
    partition_location_information::get_partition_type( uint32_t site ) const {
    auto found = partition_types_.find( site );
    if( found != partition_types_.end() ) {
        return std::make_tuple<>( true, found->second );
    }
    return std::make_tuple<>( false, partition_type::type::ROW );
}
std::tuple<bool, storage_tier_type::type>
    partition_location_information::get_storage_type( uint32_t site ) const {
    auto found = storage_types_.find( site );
    if( found != storage_types_.end() ) {
        return std::make_tuple<>( true, found->second );
    }
    return std::make_tuple<>( false, storage_tier_type::type::MEMORY );
}

bool can_merge_partition_location_informations( const partition_location_information &pr1,
                                  const partition_location_information &pr2 ) {
    DCHECK_NE( k_unassigned_master, pr1.master_location_ );
    DCHECK_NE( k_unassigned_master, pr2.master_location_ );

    return (
        ( pr1.master_location_ == pr2.master_location_ ) and
        ( pr1.replica_locations_.size() == pr2.replica_locations_.size() ) and
        ( pr1.replica_locations_ == pr2.replica_locations_ ) );
}

std::ostream &operator<<( std::ostream &                        out,
                          const partition_location_information &val ) {
    out << "[Partition location_information, master_location_ "
        << val.master_location_
        << ", replica_locations_:" << val.replica_locations_
        << ", partition_types_:" << val.partition_types_
        << ", storage_types_:" << val.storage_types_
        << ", update_destination_slot_:" << val.update_destination_slot_
        << ", version_:" << val.version_
        << ", session_req_:" << val.session_req_ << ", in_flight_"
        << val.in_flight_ << ", repartition_op_:" << val.repartition_op_;
    if( val.repartition_op_ != K_NO_OP ) {
        out << ", repartitioned_one_:" << val.repartitioned_one_;
        if( val.repartition_op_ == K_SPLIT_OP ) {
            out << ", repartitioned_two_:" << val.repartitioned_two_;
        }
    }
    out << "]";
    return out;
}

partition_payload::partition_payload()
    : sample_based_read_accesses_( 0 ),
      sample_based_write_accesses_( 0 ),
      read_accesses_( 0 ),
      write_accesses_( 0 ),
      multi_query_entry_( nullptr ),
      identifier_(),
      lock_(),
      spin_(),
      location_information_( nullptr ) {
    multi_query_entry_ = std::make_shared<multi_query_partition_entry>();
}

partition_payload::partition_payload( const partition_column_identifier &id )
    : sample_based_read_accesses_( 0 ),
      sample_based_write_accesses_( 0 ),
      read_accesses_( 0 ),
      write_accesses_( 0 ),
      multi_query_entry_( nullptr ),
      identifier_( id ),
      lock_(),
      spin_(),
      location_information_( nullptr ) {
    multi_query_entry_ = std::make_shared<multi_query_partition_entry>( id );
}

std::shared_ptr<partition_location_information>
    partition_payload::get_location_information(
        const partition_lock_mode &lock_mode ) const {
    std::shared_ptr<partition_location_information> ret = nullptr;
    switch( lock_mode ) {
        case partition_lock_mode::no_lock:
            ret = get_location_information();
            break;
        case partition_lock_mode::try_lock:
            ret = atomic_get_location_information();
            break;
        case partition_lock_mode::lock:
            ret = atomic_get_location_information();
            break;
    }

    return ret;
}

std::shared_ptr<partition_location_information> partition_payload::get_location_information() const {
    return location_information_;
}

std::shared_ptr<partition_location_information> partition_payload::atomic_get_location_information()
    const {
    spin_.lock();
    std::shared_ptr<partition_location_information> rec = location_information_;
    spin_.unlock();

    return rec;
}

bool do_insert_versions_match(
    std::shared_ptr<partition_location_information> new_location_information,
    std::shared_ptr<partition_location_information> old_location_information ) {
    if( old_location_information ) {
        DCHECK_EQ( old_location_information->version_ + 1,
                   new_location_information->version_ );
    } else {
        DCHECK_EQ( 1, new_location_information->version_ );
    }
    return true;
}

// can be called without a lock
bool partition_payload::compare_and_swap_location_information(
    std::shared_ptr<partition_location_information> new_location_information,
    std::shared_ptr<partition_location_information> expected_location_information ) {
    DCHECK( do_insert_versions_match( new_location_information, expected_location_information ) );
    bool ret = false;
    spin_.lock();
    if( expected_location_information == location_information_) {
        location_information_ = new_location_information;
        ret = true;
    }
    spin_.unlock();

    if( ret ) {
        DVLOG( 40 ) << "Compare and swap location_information in:" << new_location_information
                    << ", for partition:" << identifier_ << ", " << this;

    } else {
        DVLOG( 40 ) << "Unable to compare and swap location_information in:" << new_location_information
                    << ", expected:" << expected_location_information
                    << ", for partition:" << identifier_ << ", " << this;
    }
    return ret;
}

// held with lock
void partition_payload::set_location_information(
    std::shared_ptr<partition_location_information> new_location_information ) {
    // not safe but w/e, this is more of a sanity check
    spin_.lock();

    DCHECK( do_insert_versions_match( new_location_information, location_information_ ) );
    location_information_ = new_location_information;
    spin_.unlock();

    DVLOG( 40 ) << "Set location_information :" << *location_information_
                << ", for partition:" << identifier_ << ", " << this;
}

bool partition_payload::lock( const partition_lock_mode &mode ) {
    bool ret  = true;
    switch( mode ) {
        case partition_lock_mode::no_lock:
            break;
        case partition_lock_mode::try_lock:
            ret = try_lock();
            break;
        case partition_lock_mode::lock:
            lock();
            break;
    }

    return ret;
}

void partition_payload::unlock( const partition_lock_mode& mode ) {
    switch( mode ) {
        case partition_lock_mode::no_lock:
            break;
        case partition_lock_mode::try_lock:
            unlock();
            break;
        case partition_lock_mode::lock:
            unlock();
            break;
    }
}

void partition_payload::lock() {
    DVLOG( 40 ) << "Locking:" << identifier_ << ", " << this;

    std::chrono::high_resolution_clock::time_point start_point =
        std::chrono::high_resolution_clock::now();

    std::chrono::high_resolution_clock::time_point end_point = start_point;
    std::chrono::duration<double, std::nano> elapsed = end_point - start_point;

    if( k_timer_log_level_on ) {
        for( ;; ) {
            bool ret = lock_.try_lock_for( std::chrono::nanoseconds(
                (uint64_t) k_slow_timer_log_time_threshold ) );
            end_point = std::chrono::high_resolution_clock::now();
            elapsed = end_point - start_point;

            if( ret ) {
                break;
            }
            VLOG( k_timer_log_level ) << "Trying to lock:" << identifier_
                                      << ", have waited:" << elapsed.count()
                                      << " , so far, this:" << this;
        }
    } else {
        lock_.lock();
        end_point = std::chrono::high_resolution_clock::now();
        elapsed = end_point - start_point;
    }
    if( elapsed.count() > k_slow_timer_log_time_threshold ) {
        VLOG( k_slow_timer_error_log_level )
            << "Locked: " << identifier_
            << ", took long time:" << elapsed.count() << ", this:" << this;
    }

    DVLOG( 40 ) << "Locked:" << identifier_ << ", " << this;
}
bool partition_payload::try_lock() {
    DVLOG( 40 ) << "Try locking:" << identifier_ << ", " << this;
    bool ret = lock_.try_lock();
    if ( ret ) {
        DVLOG( 40 ) << "Locked:" << identifier_ << ", " << this;
    } else {
        DVLOG( 40 ) << "Unable to lock:" << identifier_ << ", " << this;
    }
    return ret;
}

void partition_payload::unlock() {
    DVLOG( 40 ) << "Unlocked:" << identifier_ << ", " << this;
    lock_.unlock();
}

double partition_payload::get_contention() const {
    return (double) write_accesses_.load();
}

double partition_payload::get_read_load() const {
    return (double) read_accesses_.load();
}

uint32_t partition_payload::get_partition_size() const {
    return ( 1 + identifier_.partition_end - identifier_.partition_start );
}
uint32_t partition_payload::get_num_rows() const {
    return get_number_of_rows( identifier_ );
}
uint32_t partition_payload::get_num_columns() const {
    return get_number_of_columns( identifier_ );
}
uint32_t partition_payload::get_num_cells() const {
    return get_number_of_cells( identifier_ );
}

std::tuple<bool, partition_type::type> partition_payload::get_partition_type(
    uint32_t site ) const {
    auto loc_info = get_location_information();
    if( loc_info ) {
        return loc_info->get_partition_type( site );
    }
    return std::make_tuple<>( false, partition_type::type::ROW );
}
std::tuple<bool, storage_tier_type::type> partition_payload::get_storage_type(
    uint32_t site ) const {
    auto loc_info = get_location_information();
    if( loc_info ) {
        return loc_info->get_storage_type( site );
    }
    return std::make_tuple<>( false, storage_tier_type::type::MEMORY );
}

void copy_over_transitions_before_split2(
    const partition_column_identifier &splitting_id,
    const partition_column_identifier &low_id,
    const partition_column_identifier &high_id, transition_statistics &dst_stat,
    transition_statistics &src_stat ) {
    std::lock( src_stat.mutex_, dst_stat.mutex_ );

    std::lock_guard<std::mutex> lk1( src_stat.mutex_, std::adopt_lock );
    std::lock_guard<std::mutex> lk2( dst_stat.mutex_, std::adopt_lock );

    const partition_column_identifier_map_t<int64_t> &src =
        src_stat.transitions_;
    partition_column_identifier_map_t<int64_t> &dst = dst_stat.transitions_;

    for( const auto &entry : src ) {
        if( entry.first != splitting_id ) {
            auto insert_entry = entry;
            dst.emplace( std::move( insert_entry ) );
            // Edge case, we have a self-transition and we are splitting
            // Make transitions to the things we are splitting into
            // instead
        } else {
            partition_column_identifier low_id_copy = low_id;
            DVLOG( 20 ) << "Inserting entry for: " << low_id_copy;
            int entry_count_copy1 = entry.second;
            dst.emplace( std::make_pair<partition_column_identifier, int64_t>(
                std::move( low_id_copy ), std::move( entry_count_copy1 ) ) );
            partition_column_identifier high_id_copy = high_id;
            DVLOG( 20 ) << "Inserting entry for: " << high_id_copy;
            int entry_count_copy2 = entry.second;
            dst.emplace( std::make_pair<partition_column_identifier, int64_t>(
                std::move( high_id_copy ), std::move( entry_count_copy2 ) ) );
        }
    }
}

void cut_transition_counts_in_half2(
    transition_statistics &statistics_to_modify, bool should_round_down ) {
    std::lock_guard<std::mutex> lk( statistics_to_modify.mutex_ );
    for( auto &transition : statistics_to_modify.transitions_ ) {
        if( should_round_down || transition.second % 2 == 0 ) {
            transition.second = transition.second / 2;
        } else {
            transition.second = ( transition.second / 2 ) + 1;
        }
    }
}

#define split_low_tracking_data( track_type, within_across )           \
    copy_over_transitions_before_split2(                               \
        payload->identifier_, low_p->identifier_, high_p->identifier_, \
        low_p->within_across##_##track_type##_txn_statistics_,         \
        payload->within_across##_##track_type##_txn_statistics_ );     \
    cut_transition_counts_in_half2(                                    \
        low_p->within_across##_##track_type##_txn_statistics_, true )

#define split_high_tracking_data( track_type, within_across )          \
    copy_over_transitions_before_split2(                               \
        payload->identifier_, low_p->identifier_, high_p->identifier_, \
        high_p->within_across##_##track_type##_txn_statistics_,        \
        payload->within_across##_##track_type##_txn_statistics_ );     \
    cut_transition_counts_in_half2(                                    \
        high_p->within_across##_##track_type##_txn_statistics_, false )

void split_partition_tracking_data(
    std::shared_ptr<partition_payload> &payload,
    std::shared_ptr<partition_payload> &low_p,
    std::shared_ptr<partition_payload> &high_p ) {

    /* Split transition counts in half across the new payloads. If the old
       payload
       does not divide evenly by 2, the higher half gets the remainder */

    split_low_tracking_data( rr, within );
    split_low_tracking_data( rw, within );
    split_low_tracking_data( wr, within );
    split_low_tracking_data( ww, within );

    split_low_tracking_data( rr, across );
    split_low_tracking_data( rw, across );
    split_low_tracking_data( wr, across );
    split_low_tracking_data( ww, across );

    // Lower half access counts
    low_p->write_accesses_ = payload->write_accesses_ / 2;
    low_p->read_accesses_ = payload->read_accesses_ / 2;

    low_p->sample_based_write_accesses_ =
        payload->sample_based_write_accesses_ / 2;
    low_p->sample_based_read_accesses_ =
        payload->sample_based_read_accesses_ / 2;

    split_high_tracking_data( rr, within );
    split_high_tracking_data( rw, within );
    split_high_tracking_data( wr, within );
    split_high_tracking_data( ww, within );

    split_high_tracking_data( rr, across );
    split_high_tracking_data( rw, across );
    split_high_tracking_data( wr, across );
    split_high_tracking_data( ww, across );

    high_p->write_accesses_ = ( payload->write_accesses_ / 2 );
    high_p->read_accesses_ = ( payload->read_accesses_ / 2 );
    if( payload->write_accesses_ % 2 != 0 ) {
        high_p->write_accesses_ += 1;
    }
    if( payload->read_accesses_ % 2 != 0 ) {
        high_p->read_accesses_ += 1;
    }

    high_p->sample_based_write_accesses_ =
        ( payload->sample_based_write_accesses_ / 2 );
    high_p->sample_based_read_accesses_ = ( payload->sample_based_read_accesses_ / 2 );
    if( payload->sample_based_write_accesses_ % 2 != 0 ) {
        high_p->sample_based_write_accesses_ += 1;
    }
    if( payload->sample_based_read_accesses_ % 2 != 0 ) {
        high_p->sample_based_read_accesses_ += 1;
    }
}

void copy_over_transitions_before_merge2( const partition_column_identifier &merged_id,
                                          const partition_column_identifier &low_id,
                                          const partition_column_identifier &high_id,
                                          transition_statistics &     dst_stat,
                                          transition_statistics &src_stat ) {
    std::lock( src_stat.mutex_, dst_stat.mutex_ );

    std::lock_guard<std::mutex> lk1( src_stat.mutex_, std::adopt_lock );
    std::lock_guard<std::mutex> lk2( dst_stat.mutex_, std::adopt_lock );

    const partition_column_identifier_map_t<int64_t> &src = src_stat.transitions_;
    partition_column_identifier_map_t<int64_t> &      dst = dst_stat.transitions_;


    for( const auto &entry : src ) {
        if( entry.first != low_id && entry.first != high_id ) {
            auto insert_entry = entry;
            dst.emplace( std::move( entry ) );
        } else {
            // Transitions to the low or high id are now transitions to the
            // merged_id
            auto search = dst.find( merged_id );
            if( search == dst.end() ) {
                partition_column_identifier merged_pid_copy = merged_id;
                int64_t              count = entry.second;
                dst.emplace( std::make_pair<partition_column_identifier, int64_t>(
                    std::move( merged_pid_copy ), std::move( count ) ) );
            } else {
                search->second += entry.second;
            }
        }
    }
}

void merge_in_transitions2( const partition_column_identifier &merged_id,
                            const partition_column_identifier &low_id,
                            const partition_column_identifier &high_id,
                            transition_statistics &     stat_to_merge_into,
                            transition_statistics &     src_stat ) {
    std::lock( src_stat.mutex_, stat_to_merge_into.mutex_ );

    std::lock_guard<std::mutex> lk1( src_stat.mutex_, std::adopt_lock );
    std::lock_guard<std::mutex> lk2( stat_to_merge_into.mutex_,
                                     std::adopt_lock );

    const partition_column_identifier_map_t<int64_t> &src = src_stat.transitions_;
    partition_column_identifier_map_t<int64_t> &      map_to_merge_into =
        stat_to_merge_into.transitions_;

    for( const auto &transition : src ) {
        if( transition.first != low_id && transition.first != high_id ) {
            auto search = map_to_merge_into.find( transition.first );
            if( search == map_to_merge_into.end() ) {
                partition_column_identifier copy_pid = transition.first;
                int64_t              access_count = transition.second;
                map_to_merge_into.insert(
                    std::make_pair<partition_column_identifier, int64_t>(
                        std::move( copy_pid ), std::move( access_count ) ) );
            } else {
                search->second += transition.second;
            }
            // If we had transitions to the low or high ids, they are now
            // transitions to the merged_id
        } else {
            auto search = map_to_merge_into.find( merged_id );
            if( search == map_to_merge_into.end() ) {
                partition_column_identifier copy_pid = merged_id;
                int64_t              access_count = transition.second;
                map_to_merge_into.insert(
                    std::make_pair<partition_column_identifier, int64_t>(
                        std::move( copy_pid ), std::move( access_count ) ) );
            } else {
                search->second += transition.second;
            }
        }
    }
}

#define merge_tracking_data( track_type, within_across )             \
    copy_over_transitions_before_merge2(                             \
        new_p->identifier_, low_p->identifier_, high_p->identifier_, \
        new_p->within_across##_##track_type##_txn_statistics_,       \
        low_p->within_across##_##track_type##_txn_statistics_ );     \
    merge_in_transitions2(                                           \
        new_p->identifier_, low_p->identifier_, high_p->identifier_, \
        new_p->within_across##_##track_type##_txn_statistics_,       \
        high_p->within_across##_##track_type##_txn_statistics_ )

void merge_partition_tracking_data(
    std::shared_ptr<partition_payload> &new_p,
    std::shared_ptr<partition_payload> &low_p,
    std::shared_ptr<partition_payload> &high_p ) {

    merge_tracking_data( rr, within );
    merge_tracking_data( rw, within );
    merge_tracking_data( wr, within );
    merge_tracking_data( ww, within );

    merge_tracking_data( rr, across );
    merge_tracking_data( rw, across );
    merge_tracking_data( wr, across );
    merge_tracking_data( ww, across );

    new_p->write_accesses_ = low_p->write_accesses_ + high_p->write_accesses_;
    new_p->read_accesses_ = low_p->read_accesses_ + high_p->read_accesses_;

    new_p->sample_based_write_accesses_ = low_p->sample_based_write_accesses_ +
                                          high_p->sample_based_write_accesses_;
    new_p->sample_based_read_accesses_ = low_p->sample_based_read_accesses_ +
                                         high_p->sample_based_read_accesses_;
}

std::tuple<std::shared_ptr<partition_payload>,
           std::shared_ptr<partition_payload>>
    split_partition_payload( std::shared_ptr<partition_payload> payload,
                             uint64_t row_split_point, uint32_t col_split_point,
                             const partition_type::type &   low_type,
                             const partition_type::type &   high_type,
                             const storage_tier_type::type &low_storage_type,
                             const storage_tier_type::type &high_storage_type,
                             uint32_t low_update_destination_slot,
                             uint32_t high_update_destination_slot,
                             const partition_lock_mode &lock_mode ) {
    DVLOG( 40 ) << "Split partition payloads:" << payload->identifier_
                << ", row_split_point:" << row_split_point
                << ", col_split_point:" << col_split_point;

    auto split_identifiers = construct_split_partition_column_identifiers(
        payload->identifier_, row_split_point, col_split_point );

    std::shared_ptr<partition_payload> low_p =
        std::make_shared<partition_payload>(
            std::get<0>( split_identifiers ) );
    std::shared_ptr<partition_payload> high_p =
        std::make_shared<partition_payload>(
            std::get<1>( split_identifiers ) );

    low_p->lock( lock_mode );
    high_p->lock( lock_mode );

    split_partition_tracking_data( payload, low_p, high_p );

    auto payload_rec = payload->atomic_get_location_information();

    if( !payload_rec ) {
        return std::make_tuple<std::shared_ptr<partition_payload>,
                               std::shared_ptr<partition_payload>>(
            std::move( low_p ), std::move( high_p ) );
    }

    DCHECK( payload_rec );

    auto low_p_rec = std::make_shared<partition_location_information>( );
    auto high_p_rec = std::make_shared<partition_location_information>();
    low_p_rec->set_from_existing_location_information_and_increment_version(
        payload_rec );
    high_p_rec->set_from_existing_location_information_and_increment_version(
        payload_rec );
    low_p_rec->update_destination_slot_ = low_update_destination_slot;
    high_p_rec->update_destination_slot_ = high_update_destination_slot;
    low_p_rec->version_ = 1;
    high_p_rec->version_ = 1;

    low_p_rec->partition_types_[low_p_rec->master_location_] = low_type;
    high_p_rec->partition_types_[high_p_rec->master_location_] = high_type;

    low_p_rec->storage_types_[low_p_rec->master_location_] = low_storage_type;
    high_p_rec->storage_types_[high_p_rec->master_location_] =
        high_storage_type;

    low_p->set_location_information( low_p_rec );
    high_p->set_location_information( high_p_rec );


    return std::make_tuple<std::shared_ptr<partition_payload>,
                           std::shared_ptr<partition_payload>>(
        std::move( low_p ), std::move( high_p ) );
}

std::shared_ptr<partition_payload> merge_partition_payloads(
    std::shared_ptr<partition_payload> low_p,
    std::shared_ptr<partition_payload> high_p,
    const partition_type::type &       merge_type,
    const storage_tier_type::type &    merge_storage_type,
    uint32_t update_destination_slot, const partition_lock_mode &lock_mode ) {
    DVLOG( 40 ) << "Merge partition payloads: [" << low_p->identifier_ << ", "
                << high_p->identifier_ << "]";

    partition_column_identifier new_pid = create_partition_column_identifier(
        low_p->identifier_.table_id, low_p->identifier_.partition_start,
        high_p->identifier_.partition_end, low_p->identifier_.column_start,
        high_p->identifier_.column_end );

    std::shared_ptr<partition_payload> new_p =
        std::make_shared<partition_payload>( new_pid );
    new_p->lock( lock_mode );

    merge_partition_tracking_data( new_p, low_p, high_p );

    auto low_p_rec = low_p->atomic_get_location_information();
    auto high_p_rec = low_p->atomic_get_location_information();

    if( !low_p_rec ) {
        DCHECK( !high_p_rec );
        return new_p;
    }

    DCHECK( low_p_rec );
    DCHECK( high_p_rec );

    auto new_p_rec = std::make_shared<partition_location_information>();
    new_p_rec->set_from_existing_location_information_and_increment_version(
        low_p_rec );
    new_p_rec->update_destination_slot_ = update_destination_slot;
    new_p_rec->version_ = 1;

    new_p_rec->partition_types_[new_p_rec->master_location_] = merge_type;
    new_p_rec->storage_types_[new_p_rec->master_location_] = merge_storage_type;

    if( new_p_rec->master_location_ != high_p_rec->master_location_ ) {
        DLOG( WARNING ) << "Mis matched master location for high :"
                        << *high_p_rec << ", low:" << *low_p_rec;
    }
    if( ( new_p_rec->replica_locations_.size() !=
          high_p_rec->replica_locations_.size() ) or
        ( new_p_rec->replica_locations_ != high_p_rec->replica_locations_ ) ) {
        DLOG( WARNING ) << "Mis matched replica locations for high:"
                        << *high_p_rec << ", low:" << *low_p_rec;
    }

    new_p->set_location_information( new_p_rec );


    return new_p;
}

void unlock_payloads(
    std::vector<std::shared_ptr<partition_payload>> &payloads,
    const partition_lock_mode &                      lock_mode ) {
    if( lock_mode == partition_lock_mode::no_lock ) {
        return;
    }
    for( auto &payload : payloads ) {
        payload->unlock( lock_mode );
    }
}

std::vector<partition_column_identifier> payloads_to_identifiers(
    const std::vector<std::shared_ptr<partition_payload>> &parts ) {
    std::vector<partition_column_identifier> pids;
    pids.reserve( parts.size() );
    for( auto payload : parts ) {
        pids.push_back( payload->identifier_ );
    }
    return pids;
}

std::ostream &operator<<( std::ostream &out, const partition_payload &val ) {
    auto location_information = val.atomic_get_location_information();

    out << "[Partition payload2, identifier_:" << val.identifier_
        << ", location_information_:";
    if( location_information ) {
        out << *location_information;
    } else {
        out << "nullptr";
    }
    out << "]";
    return out;
}

void add_split_to_partition_payload(
    std::shared_ptr<partition_payload> &partition, uint64_t split_row_id,
    uint32_t split_col_id ) {
    auto part_location_info = partition->get_location_information();
    // it must exist at this point
    DCHECK( part_location_info );

    auto split_location_information =
        std::make_shared<partition_location_information>();
    split_location_information
        ->set_from_existing_location_information_and_increment_version(
            part_location_info );
    auto split_identifiers = construct_split_partition_column_identifiers(
        partition->identifier_, split_row_id, split_col_id );
    split_location_information->repartition_op_ = K_SPLIT_OP;
    split_location_information->repartitioned_one_ =
        std::get<0>( split_identifiers );
    split_location_information->repartitioned_two_ =
        std::get<1>( split_identifiers );

    partition->set_location_information( split_location_information );
}

void add_merge_to_partition_payloads(
    std::shared_ptr<partition_payload> &left_partition,
    std::shared_ptr<partition_payload> &right_partition ) {
    DCHECK( left_partition );
    DCHECK( right_partition );

    partition_column_identifier merged_pid = create_partition_column_identifier(
        left_partition->identifier_.table_id,
        left_partition->identifier_.partition_start,
        right_partition->identifier_.partition_end,
        left_partition->identifier_.column_start,
        right_partition->identifier_.column_end );

    auto left_part_location_info = left_partition->get_location_information();
    // it must exist at this point
    DCHECK( left_part_location_info );

    auto right_part_location_info = right_partition->get_location_information();
    // it must exist at this point
    DCHECK( right_part_location_info );

    auto left_merge_location_information =
        std::make_shared<partition_location_information>();
    left_merge_location_information
        ->set_from_existing_location_information_and_increment_version(
            left_part_location_info );

    auto right_merge_location_information =
        std::make_shared<partition_location_information>();
    right_merge_location_information
        ->set_from_existing_location_information_and_increment_version(
            right_part_location_info );

    left_merge_location_information->repartition_op_ = K_MERGE_OP;
    left_merge_location_information->repartitioned_one_ = merged_pid;

    right_merge_location_information->repartition_op_ = K_MERGE_OP;
    right_merge_location_information->repartitioned_one_ = merged_pid;

    left_partition->set_location_information( left_merge_location_information );
    right_partition->set_location_information(
        right_merge_location_information );
}

void remove_repartition_op_from_partition_payload(
    std::shared_ptr<partition_payload> &partition ) {
    auto part_location_info = partition->get_location_information();
    // it must exist at this point
    DCHECK( part_location_info );

    auto no_repart_location_information =
        std::make_shared<partition_location_information>();
    no_repart_location_information
        ->set_from_existing_location_information_and_increment_version(
            part_location_info );
    no_repart_location_information->repartition_op_ = K_NO_OP;

    partition->set_location_information( no_repart_location_information );
}

partition_access_entry::partition_access_entry(
    double access_counts, const partition_column_identifier &pid, int site,
    std::shared_ptr<partition_payload> payload )
    : access_counts_( access_counts ),
      pid_( pid ),
      site_( site ),
      payload_( payload ) {}

partition_access_entry build_partition_access_entry(
    std::shared_ptr<partition_payload> &part ) {
    auto part_loc = part->get_location_information();
    if( part_loc ) {
        if( part_loc->master_location_ != k_unassigned_master ) {
            double access_count = part->read_accesses_ + part->write_accesses_;
            partition_access_entry part_entry( access_count, part->identifier_,
                                               part_loc->master_location_,
                                               part );
            return part_entry;
        }
    }

    partition_access_entry part_entry( 0, part->identifier_, -1, nullptr );

    return part_entry;
}

partition_payload_row_holder::partition_payload_row_holder( uint64_t row )
    : row_( row ), lock_(), column_payloads_() {}

std::shared_ptr<partition_payload> partition_payload_row_holder::get_payload(
    const cell_key &ck ) const {
    DVLOG( 40 ) << "Get payload:" << ck;
    DCHECK_EQ( row_, (uint64_t) ck.row_id );
    std::shared_ptr<partition_payload> payload = nullptr;

    lock_.lock_shared();

    for( auto &part : column_payloads_ ) {
        const auto pid = part->identifier_;
        if( ( pid.column_start <= ck.col_id ) and
            ( pid.column_end >= ck.col_id ) ) {
            DCHECK( is_cid_within_pcid( ck, pid ) );
            payload = part;
            break;
        } else if( pid.column_start > ck.col_id ) {
            break;
        }
    }

    lock_.unlock_shared();

    DVLOG( 40 ) << "Get payload:" << ck << ", found payload:" << payload;
    return payload;
}
std::shared_ptr<partition_payload>
    partition_payload_row_holder::get_or_create_payload(
        const cell_key &ck, std::shared_ptr<partition_payload> &payload ) {

    DVLOG( 40 ) << "Get or create payload:" << ck;

    DCHECK_EQ( row_, (uint64_t) ck.row_id );
    DCHECK( is_cid_within_pcid( ck, payload->identifier_ ) );

    auto found_payload = get_payload( ck );
    if( found_payload ) {
        DVLOG( 40 ) << "Get or create payload:" << ck << ", found existing";
        return found_payload;
    }

    lock_.lock();

    for( uint32_t pos = 0; pos < column_payloads_.size(); pos++ ) {
        auto       part = column_payloads_.at( pos );
        const auto pid = part->identifier_;
        if( ( pid.column_start <= ck.col_id ) and
            ( pid.column_end >= ck.col_id ) ) {
            DCHECK( is_cid_within_pcid( ck, pid ) );
            found_payload = part;
            break;
        } else if( pid.column_start > ck.col_id ) {
            // does the current payload fit in here
            if( pos > 0 ) {
                if( column_payloads_.at( pos - 1 )->identifier_.column_end >=
                    payload->identifier_.column_start ) {
                    break;
                }
            }
            if( column_payloads_.at( pos )->identifier_.column_start <=
                payload->identifier_.column_end ) {
                break;
            }
            DVLOG( 40 ) << "Inserting payload ";
            found_payload = payload;
            column_payloads_.insert( column_payloads_.begin() + pos,
                                     found_payload );
        }
    }

    if( !found_payload ) {
        if( column_payloads_.empty() or
            ( column_payloads_.at( column_payloads_.size() - 1 )
                  ->identifier_.column_end <
              payload->identifier_.column_start ) ) {
            DVLOG( 40 ) << "Inserting payload ";
            column_payloads_.emplace_back( payload );
            found_payload = payload;
        }
    }
    DVLOG( 40 ) << "Get or create payload:" << ck
                << ", found payload:" << found_payload;

    lock_.unlock();

    return found_payload;
}
std::shared_ptr<partition_payload>
    partition_payload_row_holder::get_or_create_payload_and_expand(
        const cell_key &ck, std::shared_ptr<partition_payload> &payload ) {
    DVLOG( 40 ) << "Get or create payload and expand:" << ck
                << ", current_pid:" << payload->identifier_;

    DCHECK_EQ( row_, (uint64_t) ck.row_id );
    DCHECK( is_cid_within_pcid( ck, payload->identifier_ ) );

    auto found_payload = get_payload( ck );
    if( found_payload ) {
        DVLOG( 40 ) << "Get or create payload and expand:" << ck
                    << ", found existing payload:" << found_payload;
        return found_payload;
    }

    lock_.lock();

    for( uint32_t pos = 0; pos < column_payloads_.size(); pos++ ) {
        auto       part = column_payloads_.at( pos );
        const auto pid = part->identifier_;
        if( ( pid.column_start <= ck.col_id ) and
            ( pid.column_end >= ck.col_id ) ) {
            DCHECK( is_cid_within_pcid( ck, pid ) );
            found_payload = part;
            break;
        } else if( pid.column_start > ck.col_id ) {
            // does the current payload fit in here
            DVLOG(40) << "Inserting payload";
            if( pos > 0 ) {
                if( column_payloads_.at( pos - 1 )->identifier_.column_end + 1 >
                    payload->identifier_.column_start ) {
                    payload->identifier_.column_start =
                        column_payloads_.at( pos - 1 )->identifier_.column_end +
                        1;
                    DVLOG( 40 ) << "Reshaping payload:" << payload->identifier_;
                }
            }
            if( column_payloads_.at( pos )->identifier_.column_start <=
                payload->identifier_.column_end ) {
                payload->identifier_.column_end =
                    column_payloads_.at( pos )->identifier_.column_start - 1;
                DVLOG( 40 ) << "Reshaping payload:" << payload->identifier_;
            }
            found_payload = payload;
            column_payloads_.insert( column_payloads_.begin() + pos,
                                     found_payload );
        }
    }

    if( !found_payload ) {
        DVLOG( 40 ) << "Inserting payload ";
        column_payloads_.emplace_back( payload );
        found_payload = payload;
        uint32_t size = column_payloads_.size();
        if( size > 1 ) {
            if( column_payloads_.at( size - 2 )->identifier_.column_end >=
                payload->identifier_.column_start ) {
                payload->identifier_.column_start =
                    column_payloads_.at( size - 2 )->identifier_.column_end + 1;
                DVLOG( 40 ) << "Reshaping payload:" << payload->identifier_;
            }
        }
    }

    lock_.unlock();

    DVLOG( 40 ) << "Get or create payload and expand:" << ck
                << ", found payload:" << found_payload;
    return found_payload;
}

bool partition_payload_row_holder::remove_payload(
    std::shared_ptr<partition_payload> &part ) {
    DVLOG( 40 ) << "Remove payload from row:" << row_;

    bool ret = false;
    lock_.lock();
    for( uint32_t pos = 0; pos < column_payloads_.size(); pos++ ) {
        if( part->identifier_ == column_payloads_.at( pos )->identifier_ ) {
            column_payloads_.erase( column_payloads_.begin() + pos );
            ret = true;
            break;
        } else if( column_payloads_.at( pos )->identifier_.column_end >
                   part->identifier_.column_start ) {
            break;
        }
    }
    lock_.unlock();
    DVLOG( 40 ) << "Remove payload from row:" << row_ << ", ret:" << ret;
    return ret;
}

bool partition_payload_row_holder::split_payload(
    std::shared_ptr<partition_payload> &ori,
    std::shared_ptr<partition_payload> &low,
    std::shared_ptr<partition_payload> &high ) {
    DVLOG( 40 ) << "Split payload from row:" << row_;
    DCHECK_EQ( ori->identifier_.table_id, low->identifier_.table_id );
    DCHECK_EQ( ori->identifier_.table_id, high->identifier_.table_id );

    DCHECK_GE( row_, ori->identifier_.partition_start );
    DCHECK_LE( row_, ori->identifier_.partition_end );

    bool ret = false;

    if( ( ori->identifier_.partition_start ==
          low->identifier_.partition_start ) and
        ( ori->identifier_.partition_end == low->identifier_.partition_end ) ) {
        DCHECK_EQ( ori->identifier_.partition_start,
                   high->identifier_.partition_start );
        DCHECK_EQ( ori->identifier_.partition_end,
                   high->identifier_.partition_end );

        DCHECK_EQ( ori->identifier_.column_start,
                   low->identifier_.column_start );
        DCHECK_EQ( ori->identifier_.column_end, high->identifier_.column_end );


        ret = split_payload_vertically( ori, low, high );

    } else {
        DCHECK_EQ( ori->identifier_.partition_start,
                   low->identifier_.partition_start );
        DCHECK_EQ( ori->identifier_.partition_end,
                   high->identifier_.partition_end );

        DCHECK_EQ( ori->identifier_.column_start,
                   high->identifier_.column_start );
        DCHECK_EQ( ori->identifier_.column_end,
                   high->identifier_.column_end );

        DCHECK_EQ( ori->identifier_.column_start,
                   low->identifier_.column_start );
        DCHECK_EQ( ori->identifier_.column_end,
                   low->identifier_.column_end );

        DCHECK_GE( row_, ori->identifier_.partition_start );
        DCHECK_LE( row_, ori->identifier_.partition_end );

        if( (int64_t) row_ <= low->identifier_.partition_end ) {
            ret = replace_payload_horizontally( ori, low );
        } else if( (int64_t) row_ >= high->identifier_.partition_start ) {
            ret = replace_payload_horizontally( ori, high );
        }
    }
    DVLOG( 40 ) << "Split payload from row:" << row_ << ", ret:" << ret;
    return ret;
}
bool partition_payload_row_holder::merge_payload(
    std::shared_ptr<partition_payload> &low,
    std::shared_ptr<partition_payload> &high,
    std::shared_ptr<partition_payload> &merged ) {
    DVLOG( 40 ) << "Merge payload from row:" << row_;
    DCHECK_EQ( merged->identifier_.table_id, low->identifier_.table_id );
    DCHECK_EQ( merged->identifier_.table_id, high->identifier_.table_id );

    DCHECK_GE( row_, merged->identifier_.partition_start );
    DCHECK_LE( row_, merged->identifier_.partition_end );

    bool ret = false;

    if( ( merged->identifier_.partition_start ==
          low->identifier_.partition_start ) and
        ( merged->identifier_.partition_end ==
          low->identifier_.partition_end ) ) {
        DCHECK_EQ( merged->identifier_.partition_start,
                   high->identifier_.partition_start );
        DCHECK_EQ( merged->identifier_.partition_end,
                   high->identifier_.partition_end );

        DCHECK_EQ( merged->identifier_.column_start,
                   low->identifier_.column_start );
        DCHECK_EQ( merged->identifier_.column_end,
                   high->identifier_.column_end );

        ret = merge_payload_vertically( low, high, merged );

    } else {
        DCHECK_EQ( merged->identifier_.partition_start,
                   low->identifier_.partition_start );
        DCHECK_EQ( merged->identifier_.partition_end,
                   high->identifier_.partition_end );

        DCHECK_EQ( merged->identifier_.column_start,
                   high->identifier_.column_start );
        DCHECK_EQ( merged->identifier_.column_end,
                   high->identifier_.column_end );

        DCHECK_EQ( merged->identifier_.column_start,
                   low->identifier_.column_start );
        DCHECK_EQ( merged->identifier_.column_end,
                   low->identifier_.column_end );

        DCHECK_GE( row_, merged->identifier_.partition_start );
        DCHECK_LE( row_, merged->identifier_.partition_end );

        if( (int64_t) row_ <= low->identifier_.partition_end ) {
            ret = replace_payload_horizontally( low, merged );
        } else if( (int64_t) row_ >= high->identifier_.partition_start ) {
            ret = replace_payload_horizontally( high, merged );
        }
    }
    DVLOG( 40 ) << "Merge payload from row:" << row_ << ", ret:" << ret;
    return ret;
}

bool partition_payload_row_holder::replace_payload_horizontally(
    std::shared_ptr<partition_payload> &ori,
    std::shared_ptr<partition_payload> &new_part ) {

    bool ret = false;
    lock_.lock();

    for( uint32_t pos = 0; pos < column_payloads_.size(); pos++ ) {
        if( ori->identifier_ == column_payloads_.at( pos )->identifier_ ) {
            column_payloads_.at( pos ) = new_part;
            ret = true;
            break;
        } else if( column_payloads_.at( pos )->identifier_.column_end >
                   ori->identifier_.column_start ) {
            break;
        }
    }

    lock_.unlock();

    return ret;
}

bool partition_payload_row_holder::merge_payload_vertically(
    std::shared_ptr<partition_payload> &low,
    std::shared_ptr<partition_payload> &high,
    std::shared_ptr<partition_payload> &merged ) {

    bool ret = false;
    lock_.lock();

    // iterate up to -1 because that's all we can look for in low
    for( uint32_t pos = 0; pos < column_payloads_.size() - 1; pos++ ) {
        if( low->identifier_ == column_payloads_.at( pos )->identifier_ ) {
            if( high->identifier_ ==
                column_payloads_.at( pos + 1 )->identifier_ ) {
                column_payloads_.at( pos ) = merged;
                column_payloads_.erase( column_payloads_.begin() +
                                        ( pos + 1 ) );
                ret = true;
                break;
            }
        } else if( column_payloads_.at( pos )->identifier_.column_end >
                   low->identifier_.column_start ) {
            break;
        }
    }

    lock_.unlock();

    return ret;
}
bool partition_payload_row_holder::split_payload_vertically(
    std::shared_ptr<partition_payload> &ori,
    std::shared_ptr<partition_payload> &low,
    std::shared_ptr<partition_payload> &high ) {

    bool ret = false;
    lock_.lock();

    for( uint32_t pos = 0; pos < column_payloads_.size(); pos++ ) {
        if( ori->identifier_ == column_payloads_.at( pos )->identifier_ ) {
            column_payloads_.at( pos ) = low;
            column_payloads_.insert( column_payloads_.begin() + ( pos + 1 ),
                                     high );
            ret = true;
            break;
        } else if( column_payloads_.at( pos )->identifier_.column_end >
                   ori->identifier_.column_start ) {
            break;
        }
    }

    lock_.unlock();

    return ret;
}


