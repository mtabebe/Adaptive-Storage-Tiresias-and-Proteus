#include "transaction_partition_holder.h"

#include <glog/logging.h>
#ifndef GLOG_STL_LOGGING_FOR_UNORDERED
#define GLOG_STL_LOGGING_FOR_UNORDERED
#endif
#include <glog/stl_logging.h>

#include "../../common/perf_tracking.h"
#include "../../common/string_conversion.h"
#include "dependency_map.h"

bool transaction_partition_holder::begin_transaction(
    const snapshot_vector& snapshot ) {
    snapshot_ = snapshot;
    // lock these in order. Thankfully the set gives an order
    partition_column_identifier undo_point;
    bool                        begin_ok = true;

    // two passes over snapshot, first pass gets the most recent id
    // and the second pass does the blocking

    start_timer( TRANSACTION_PARTITION_HOLDER_WAIT_FOR_SESSION_TIMER_ID );

    for( const partition_column_identifier& pid : read_partition_ids_ ) {
        std::shared_ptr<partition> part = get_partition( pid );
        part->begin_read_wait_and_build_snapshot(
            snapshot_, do_not_apply_last_update( pid ) );
    }
    for( const partition_column_identifier& pid : read_partition_ids_ ) {
        std::shared_ptr<partition> part = get_partition( pid );
        part->begin_read_wait_for_version( snapshot_,
                                           do_not_apply_last_update( pid ) );
    }

    partition_column_identifier_set read_write_set_partition_ids;

    // make sure you are up to date with the write partitions
    for( const partition_column_identifier& pid : write_partition_ids_ ) {
        std::shared_ptr<partition> part = get_partition( pid );
        part->begin_read_wait_for_version( snapshot_,
                                           do_not_apply_last_update( pid ) );
        if( read_partition_ids_.count( pid ) == 1 ) {
            DVLOG( 40 ) << "PID:" << pid << ", is read and write";
            read_write_set_partition_ids.insert( pid );
        }
    }

    stop_timer( TRANSACTION_PARTITION_HOLDER_WAIT_FOR_SESSION_TIMER_ID );

    // acquire locks
    for( const partition_column_identifier& pid : write_partition_ids_ ) {
        std::shared_ptr<partition> part = get_partition( pid );
        begin_ok = part->begin_write( write_snapshot_, true /*acquire locks*/ );
        if( !begin_ok ) {
            abort_writes( undo_point );
            return false;
        }
        undo_point = pid;
    }

    for( const partition_column_identifier& pid :
         read_write_set_partition_ids ) {
        set_snapshot_version( snapshot_, pid,
                              get_snapshot_version( write_snapshot_, pid ) );
        std::shared_ptr<partition> part = get_partition( pid );
        DVLOG( 40 ) << "Begin read for read/write PID:" << pid
                    << ", version:" << get_snapshot_version( snapshot_, pid );
        part->begin_read_wait_for_version( snapshot_,
                                           do_not_apply_last_update( pid ) );
    }

    DVLOG( 40 ) << "Begin, snapshot:" << snapshot_;

    return true;
}

bool transaction_partition_holder::begin_with_lock_already_acquired() {
    // lock these in order. Thankfully the set gives an order
    partition_column_identifier undo_point;
    bool                        begin_ok = true;
    for( const partition_column_identifier& pid : write_partition_ids_ ) {
        std::shared_ptr<partition> part = get_partition( pid );
        begin_ok = part->begin_write( write_snapshot_,
                                      false /* do not acquire lock*/ );
        if( !begin_ok ) {
            abort_writes( undo_point );
            return false;
        }
        undo_point = pid;
    }
    return true;
}

snapshot_vector transaction_partition_holder::commit_transaction() {
    return commit_transaction_with_unlock( true );
}
snapshot_vector transaction_partition_holder::commit_transaction_with_unlock(
    bool do_unlock ) {
    // unlock these in any order
    for( const partition_column_identifier& pid : write_partition_ids_ ) {
        std::shared_ptr<partition> part = get_partition( pid );
        part->build_commit_snapshot( write_snapshot_ );
    }
    for( const partition_column_identifier& pid : write_partition_ids_ ) {
        std::shared_ptr<partition> part = get_partition( pid );
        part->commit_write_transaction(
            write_snapshot_, table_to_poll_epoch_[pid.table_id], do_unlock );
    }

// no delta
#if 0  // HDB-MERGE_SNAPSHOTS
    merge_snapshots( snapshot_, write_snapshot_, {} );
#endif
    merge_snapshots( snapshot_, write_snapshot_ );

    return snapshot_;
}
void transaction_partition_holder::unlock_write_partitions() {
    for( const partition_column_identifier& pid : write_partition_ids_ ) {
        std::shared_ptr<partition> part = get_partition( pid );
        part->unlock_partition();
    }
}

void transaction_partition_holder::abort_writes(
    const partition_column_identifier& undo_point ) {
    for( const partition_column_identifier& pid : write_partition_ids_ ) {
        if( pid == undo_point ) {
            break;
        }
        std::shared_ptr<partition> part = get_partition( pid );
        part->abort_write_transaction();
    }
}

snapshot_vector transaction_partition_holder::abort_transaction() {
    DVLOG( 40 ) << "Aborting transaction";
    // there is no end point here
    partition_column_identifier end_point;
    end_point.table_id = -1;
    end_point.partition_start = -1;
    end_point.partition_end = -1;

    abort_writes( end_point );

    return snapshot_;
}

snapshot_vector transaction_partition_holder::execute_split_merge(
    const snapshot_vector& snapshot, split_partition_result split,
    uint32_t col_split_point, uint64_t row_split_point, uint32_t op,
    const std::vector<storage_tier_type::type>& storage_tiers,
    uint64_t                                    poll_epoch ) {
    snapshot_ = snapshot;

    auto low_p = split.partition_low_;
    auto high_p = split.partition_high_;
    auto cover_p = split.partition_cover_;

    add_write_partition( cover_p->get_metadata().partition_id_, cover_p );
    if( op == K_SPLIT_OP ) {
        add_read_partition( cover_p->get_metadata().partition_id_, cover_p );
    }
    add_write_partition( low_p->get_metadata().partition_id_, low_p );
    add_write_partition( high_p->get_metadata().partition_id_, high_p );
    if( op == K_MERGE_OP ) {
        add_read_partition( low_p->get_metadata().partition_id_, low_p );
        add_read_partition( high_p->get_metadata().partition_id_, high_p );
    }

    table_to_poll_epoch_[cover_p->get_metadata().partition_id_.table_id] =
        poll_epoch;

    bool ok = begin_with_lock_already_acquired();
    DCHECK( ok );

    auto low_p_update_prop_info = low_p->get_update_propagation_information();
    auto high_p_update_prop_info = high_p->get_update_propagation_information();
    auto cover_p_update_prop_info =
        cover_p->get_update_propagation_information();

    if( op == K_SPLIT_OP ) {
        cover_p->split_records( low_p.get(), high_p.get(), col_split_point,
                                row_split_point );

        cover_p->add_repartition_op( cover_p->get_metadata(), col_split_point,
                                     row_split_point, K_SPLIT_OP );

        cover_p_update_prop_info.do_seek_ = false;
        low_p_update_prop_info.do_seek_ = should_seek_topic(
            low_p_update_prop_info, cover_p_update_prop_info );
        high_p_update_prop_info.do_seek_ = should_seek_topic(
            high_p_update_prop_info, cover_p_update_prop_info );

        // remove the current subscription, add the new ones
        cover_p->add_subscription_state_to_write_buffer(
            cover_p_update_prop_info, false /*start subscribing*/ );
        cover_p->add_subscription_state_to_write_buffer(
            low_p_update_prop_info, true /*start subscribing*/ );
        cover_p->add_subscription_state_to_write_buffer(
            high_p_update_prop_info, true /* start subscribing*/ );

        high_p->add_repartition_op( cover_p->get_metadata(), col_split_point,
                                    row_split_point, K_NO_OP );
        low_p->add_repartition_op( cover_p->get_metadata(), col_split_point,
                                   row_split_point, K_NO_OP );

    } else if( op == K_MERGE_OP ) {
        cover_p->merge_records( low_p.get(), high_p.get(), col_split_point,
                                row_split_point );

        high_p->add_repartition_op( cover_p->get_metadata(), col_split_point,
                                    row_split_point, K_MERGE_OP );
        low_p->add_repartition_op( cover_p->get_metadata(), col_split_point,
                                   row_split_point, K_MERGE_OP );

        cover_p_update_prop_info.do_seek_ =
            should_seek_topic( cover_p_update_prop_info,
                               high_p_update_prop_info ) or
            should_seek_topic( cover_p_update_prop_info,
                               low_p_update_prop_info );
        high_p_update_prop_info.do_seek_ = false;
        low_p_update_prop_info.do_seek_ = false;

        // remove the current subscription, add the new covering one
        high_p->add_subscription_state_to_write_buffer(
            cover_p_update_prop_info, true /*start subscribing*/ );
        high_p->add_subscription_state_to_write_buffer(
            high_p_update_prop_info, false /*start subscribing*/ );

        low_p->add_subscription_state_to_write_buffer(
            cover_p_update_prop_info, true /*start subscribing*/ );
        low_p->add_subscription_state_to_write_buffer(
            low_p_update_prop_info, false /*start subscribing*/ );

        cover_p->add_repartition_op( cover_p->get_metadata(), col_split_point,
                                     row_split_point, K_INSERT_OP );
    }

    auto ret = commit_transaction_with_unlock( false /* don't unlock */ );

    std::vector<std::shared_ptr<partition>> parts;

    if( op == K_SPLIT_OP ) {
        parts.emplace_back( low_p );
        parts.emplace_back( high_p );
    } else if( op == K_MERGE_OP ) {
        parts.emplace_back( cover_p );
    }

    DCHECK_EQ( parts.size(), storage_tiers.size() );
    for( uint32_t pos = 0; pos < parts.size(); pos++ ) {
        const auto& s_type = storage_tiers.at( pos );
        if( s_type == storage_tier_type::type::DISK ) {
            parts.at( pos )->change_storage_tier( s_type );
        }
    }
    unlock_write_partitions();
    return ret;
}

// remaster a partition, and denote the subscription switch
std::tuple<snapshot_vector, std::vector<std::shared_ptr<partition>>>
    transaction_partition_holder::remaster_partitions(
        const std::vector<partition_column_identifier>& pids, bool is_release,
        uint32_t                                      new_master,
        const std::vector<propagation_configuration>& new_prop_configs ) {
    DCHECK_EQ( pids.size(), write_partition_ids_.size() );

    snapshot_vector snapshot;

    bool is_new_props_empty = new_prop_configs.empty();
    if( !is_new_props_empty ) {
        DCHECK_EQ( new_prop_configs.size(), write_partition_ids_.size() );
    }

    std::vector<std::shared_ptr<partition>> parts;

    for( uint32_t pos = 0; pos < pids.size(); pos++ ) {
        const partition_column_identifier& pid = pids.at( pos );
        std::shared_ptr<partition>         part = get_partition( pid );
        parts.push_back( part );
        DVLOG( 10 ) << "Remaster:" << pid << ", to:" << new_master;
        part->remaster( new_master );
        if( !is_new_props_empty ) {
            update_propagation_information up_info;
            up_info.propagation_config_ = new_prop_configs.at( pos );
            up_info.identifier_ = pid;

            auto old_prop_info = part->get_update_propagation_information();
            old_prop_info.do_seek_ = false;
            up_info.do_seek_ = should_seek_topic( up_info, old_prop_info );

            part->add_switch_subscription_state_to_write_buffer( old_prop_info,
                                                                 up_info );
        }
        snapshot_vector local_snapshot;
        part->get_most_recent_snapshot_vector( local_snapshot );
        set_snapshot_version( snapshot, pid,
                              get_snapshot_version( local_snapshot, pid ) );
    }
    return std::make_tuple<>( snapshot, parts );
}

snapshot_vector transaction_partition_holder::remaster_partitions(
    uint32_t new_master ) {
    snapshot_vector snapshot;
    for( const auto& pid : write_partition_ids_ ) {
        std::shared_ptr<partition> part = get_partition( pid );
        part->remaster( new_master );

        snapshot_vector local_snapshot;
        part->get_most_recent_snapshot_vector( local_snapshot );
        set_snapshot_version( snapshot, pid,
                              get_snapshot_version( local_snapshot, pid ) );
    }
    return snapshot;
}

std::shared_ptr<partition> transaction_partition_holder::internal_get_partition(
    const cell_identifier& cid ) const {
    start_extra_timer( HOLDER_GET_PARTITION_TIMER_ID );

    DVLOG( 40 ) << "Get partition for cid:" << cid;

    if( prev_part_ and ( is_cid_within_pcid(
                           prev_part_->get_metadata().partition_id_, cid ) ) ) {
        stop_extra_timer( HOLDER_GET_PARTITION_TIMER_ID );
        return prev_part_;
    }

    auto found = records_to_partitions_.find( cid );
    if( found != records_to_partitions_.end() ) {
        prev_part_ = found->second;

        stop_extra_timer( HOLDER_GET_PARTITION_TIMER_ID );
        return found->second;
    }
    std::shared_ptr<partition> found_part = nullptr;
    for( const auto& pid_to_partition : partitions_ ) {
        const auto& pid = pid_to_partition.first;
        if( is_cid_within_pcid( pid, cid ) ) {
            found_part = pid_to_partition.second;
            break;
        }
    }
    stop_extra_timer( HOLDER_GET_PARTITION_TIMER_ID );

    return found_part;
}

std::shared_ptr<partition> transaction_partition_holder::get_partition(
    const cell_identifier& cid ) const {
    start_extra_timer( HOLDER_GET_AND_SET_PARTITION_TIMER_ID );
    auto part = internal_get_partition( cid );
    records_to_partitions_[cid] = part;
    if( part ) {
        prev_part_ = part;
    }
    stop_extra_timer( HOLDER_GET_AND_SET_PARTITION_TIMER_ID );
    return part;
}

bool transaction_partition_holder::does_site_master_write_set( uint32_t site ) {
    for( const partition_column_identifier& pid : write_partition_ids_ ) {
        std::shared_ptr<partition> part = get_partition( pid );
        uint32_t                   master = part->get_master_location();
        if( master != site ) {
            DLOG( WARNING ) << "Partition:" << pid
                            << ", mastered at site:" << master
                            << ", but expect site:" << site;
            return false;
        }
    }
    return true;
}

// actually snapshot the partition, do this by getting a snapshot, and offset,
// and then actually do the read
snapshot_partition_column_state
    transaction_partition_holder::snapshot_partition(
        const partition_column_identifier& pid ) {
    return snapshot_partition_with_optional_commit( pid, true /* do commit*/ );
}

snapshot_partition_column_state
    transaction_partition_holder::snapshot_partition_with_optional_commit(
        const partition_column_identifier& pid, bool do_commit ) {
    DVLOG( 40 ) << "Snapshot partition:" << pid;

    DCHECK_GT( write_partition_ids_.size(), 0 );
    DCHECK_EQ( 1, write_partition_ids_.count( pid ) );

    std::shared_ptr<partition> part = get_partition( pid );

    // get the most recent offset and snapshot
    snapshot_partition_column_state snap_state;

    snap_state.pcid = pid;
    snap_state.prop_config =
        part->get_update_propagation_information().propagation_config_;
    snap_state.master_location = part->get_master_location();

    if( do_commit ) {
        // unlock
        snap_state.session_version_vector = commit_transaction();
    } else {
        part->get_most_recent_snapshot_vector(
            snap_state.session_version_vector );
    }

    DVLOG( 40 ) << "Snapshot partition:" << pid << ", session_version_vector:"
                << snap_state.session_version_vector;
    DVLOG( 40 ) << "Snapshot partition:" << pid
                << ", prop_config:" << snap_state.prop_config;

    part->snapshot_partition_data( snap_state );

    return snap_state;
}

void transaction_partition_holder::change_partition_output_destination(
    std::shared_ptr<update_destination_interface> new_update_destination ) {
    for( const partition_column_identifier& pid : write_partition_ids_ ) {
        std::shared_ptr<partition> part = get_partition( pid );
        part->switch_update_destination( new_update_destination );
    }
}

void transaction_partition_holder::set_partition_update_destinations(
    const std::vector<partition_column_identifier>& pids,
    const std::vector<propagation_configuration>&   new_prop_configs,
    std::shared_ptr<update_destination_generator>   update_gen ) {
    DCHECK_EQ( pids.size(), new_prop_configs.size() );

    for( uint32_t pos = 0; pos < pids.size(); pos++ ) {
        const partition_column_identifier& pid = pids.at( pos );
        DVLOG( 40 ) << "Setting partition: " << pid
                    << ", to update destination:" << new_prop_configs.at( pos );
        std::shared_ptr<partition> part = get_partition( pid );
        part->set_update_destination(
            update_gen->get_update_destination_by_propagation_configuration(
                new_prop_configs.at( pos ) ) );
    }
}

bool transaction_partition_holder::do_not_apply_last_update(
    const partition_column_identifier& pid ) const {
    return ( do_not_apply_last_partition_ids_.count( pid ) == 1 );
}

std::vector<uint32_t> transaction_partition_holder::get_tables() const {
    std::vector<uint32_t> table_ids;
    for( const auto& entry : table_to_poll_epoch_ ) {
        table_ids.push_back( entry.first );
    }
    return table_ids;
}
void transaction_partition_holder::add_table_poll_epoch( uint32_t table_id,
                                                         uint64_t poll_epoch ) {
    DVLOG( 40 ) << "add_table_poll_epoch table_id:" << table_id
                << ", poll_epoch:" << poll_epoch;
    table_to_poll_epoch_[table_id] = poll_epoch;
}

bool transaction_partition_holder::remove_data(
    const cell_identifier& ci ) const {
    auto p = get_partition( ci );
    if( !p ) {
        return false;
    }
    DVLOG( 40 ) << "Remove cell:" << ci << ", snapshot:" << snapshot_;
    assert_writeable_partition( p->get_metadata().partition_id_ );
    auto rec = p->remove_data( ci, snapshot_ );
    return rec;
}
#define TEMPL_UPDATE_DATA( _method, _ci, _data )                           \
    auto p = get_partition( ci );                                          \
    if( !p ) {                                                             \
        return false;                                                      \
    }                                                                      \
    DVLOG( 40 ) << "Update data cell:" << ci << ", snapshot:" << snapshot_ \
                << ", data:" << _data;                                     \
    assert_writeable_partition( p->get_metadata().partition_id_ );         \
    auto rec = p->_method( ci, _data, snapshot_ );                         \
    return rec;

#define TEMPL_READ_DATA( _method, _type, _def_ret, _ci )                  \
    auto p = get_partition( ci );                                         \
    if( !p ) {                                                            \
        return std::make_tuple<>( false, _def_ret );                      \
    }                                                                     \
    DVLOG( 40 ) << "Read data cell:" << ci << ", snapshot:" << snapshot_; \
    assert_readable_partition( p->get_metadata().partition_id_ );         \
    auto rec = p->_method( ci, snapshot_ );                               \
    return rec;

#define TEMPL_READ_LATEST_DATA( _method, _type, _def_ret, _ci )   \
    auto p = get_partition( ci );                                 \
    if( !p ) {                                                    \
        return std::make_tuple<>( false, _def_ret );              \
    }                                                             \
    DVLOG( 40 ) << "Read latest data cell:" << ci                 \
                << ", snapshot:" << snapshot_;                    \
    assert_readable_partition( p->get_metadata().partition_id_ ); \
    auto rec = p->_method( ci );                                  \
    return rec;

// point inserts
bool transaction_partition_holder::insert_uint64_data(
    const cell_identifier& ci, uint64_t data ) const {
    TEMPL_UPDATE_DATA( insert_uint64_data, ci, data );
}
bool transaction_partition_holder::insert_int64_data( const cell_identifier& ci,
                                                      int64_t data ) const {
    TEMPL_UPDATE_DATA( insert_int64_data, ci, data );
}
bool transaction_partition_holder::insert_string_data(
    const cell_identifier& ci, const std::string& data ) const {
    TEMPL_UPDATE_DATA( insert_string_data, ci, data );
}
bool transaction_partition_holder::insert_double_data(
    const cell_identifier& ci, double data ) const {
    TEMPL_UPDATE_DATA( insert_double_data, ci, data );
}

// point updates
bool transaction_partition_holder::update_uint64_data(
    const cell_identifier& ci, uint64_t data ) const {
    TEMPL_UPDATE_DATA( update_uint64_data, ci, data );
}
bool transaction_partition_holder::update_int64_data( const cell_identifier& ci,
                                                      int64_t data ) const {
    TEMPL_UPDATE_DATA( update_int64_data, ci, data );
}
bool transaction_partition_holder::update_string_data(
    const cell_identifier& ci, const std::string& data ) const {
    TEMPL_UPDATE_DATA( update_string_data, ci, data );
}
bool transaction_partition_holder::update_double_data(
    const cell_identifier& ci, double data ) const {
    TEMPL_UPDATE_DATA( update_double_data, ci, data );
}

// point lookups
std::tuple<bool, uint64_t> transaction_partition_holder::get_uint64_data(
    const cell_identifier& ci ) const {
    TEMPL_READ_DATA( get_uint64_data, uint64_t, 0, ci );
}
std::tuple<bool, int64_t> transaction_partition_holder::get_int64_data(
    const cell_identifier& ci ) const {
    TEMPL_READ_DATA( get_int64_data, int64_t, 0, ci );
}
std::tuple<bool, double> transaction_partition_holder::get_double_data(
    const cell_identifier& ci ) const {
    TEMPL_READ_DATA( get_double_data, double, 0, ci );
}
std::tuple<bool, std::string> transaction_partition_holder::get_string_data(
    const cell_identifier& ci ) const {
    TEMPL_READ_DATA( get_string_data, std::string, "", ci );
}

std::tuple<bool, uint64_t> transaction_partition_holder::get_latest_uint64_data(
    const cell_identifier& ci ) const {
    TEMPL_READ_LATEST_DATA( get_latest_uint64_data, uint64_t, 0, ci );
}
std::tuple<bool, int64_t> transaction_partition_holder::get_latest_int64_data(
    const cell_identifier& ci ) const {
    TEMPL_READ_LATEST_DATA( get_latest_int64_data, int64_t, 0, ci );
}
std::tuple<bool, double> transaction_partition_holder::get_latest_double_data(
    const cell_identifier& ci ) const {
    TEMPL_READ_LATEST_DATA( get_latest_double_data, double, 0, ci );
}
std::tuple<bool, std::string>
    transaction_partition_holder::get_latest_string_data(
        const cell_identifier& ci ) const {
    TEMPL_READ_LATEST_DATA( get_latest_string_data, std::string, "", ci );
}

std::vector<result_tuple> transaction_partition_holder::scan(
    uint32_t table_id, uint64_t low_key, uint64_t high_key,
    const std::vector<uint32_t>& project_cols,
    const predicate_chain&       predicate ) const {
    std::vector<result_tuple> results;
    scan( table_id, low_key, high_key, project_cols, predicate, results );
    return results;
}

std::tuple<const partition_column_identifier_set::iterator,
           const partition_column_identifier_set::iterator>
    transaction_partition_holder::get_scan_bounds( uint32_t table_id ) const {
    partition_column_identifier lb = create_partition_column_identifier(
        table_id, 0 /*p_start*/, 0 /*p_end*/, 0 /*c_start*/, 0 /*c_end*/ );
    partition_column_identifier ub = create_partition_column_identifier(
        table_id + 1, 0 /*p_start*/, 0 /*p_end*/, 0 /*c_start*/, 0 /*c_end*/ );

    // both return end if not found, so we are good
    const auto& lower_bound = read_partition_ids_.lower_bound( lb );
    const auto& upper_bound = read_partition_ids_.upper_bound( ub );

    return std::make_tuple<>( lower_bound, upper_bound );
}
partition_column_identifier transaction_partition_holder::get_scan_pcid(
    uint32_t table_id, uint64_t low_key, uint64_t high_key,
    const std::vector<uint32_t>& project_cols ) const {

    DCHECK( std::is_sorted( std::begin( project_cols ),
                            std::end( project_cols ) ) );

    partition_column_identifier pred_pcid;
    pred_pcid.table_id = table_id;
    pred_pcid.column_start = project_cols.at( 0 );
    pred_pcid.column_end = project_cols.at( project_cols.size() - 1 );
    pred_pcid.partition_start = low_key;
    pred_pcid.partition_end = high_key;

    return pred_pcid;
}

void transaction_partition_holder::scan(
    uint32_t table_id, uint64_t low_key, uint64_t high_key,
    const std::vector<uint32_t>& project_cols, const predicate_chain& predicate,
    std::vector<result_tuple>& results ) const {

    auto        scan_bounds = get_scan_bounds( table_id );
    const auto& lower_bound = std::get<0>( scan_bounds );
    const auto& upper_bound = std::get<1>( scan_bounds );

    auto pred_pcid = get_scan_pcid( table_id, low_key, high_key, project_cols );

    for( auto iter = lower_bound; iter != upper_bound; iter++ ) {
        auto pid = *iter;
        DVLOG( 40 ) << "Scan PID:" << pid;
        if( do_pcids_intersect( pid, pred_pcid ) ) {
            auto part = get_partition( pid );
            DCHECK( part );
            DVLOG( 40 ) << "Scan PID:" << pid << ", within range";
            part->scan( low_key, high_key, predicate, project_cols, snapshot_,
                        results );
        }
    }
}

#define TEMPL_GET( _cid, _res_cell, _get_method, _conv_method, _type ) \
    std::tuple<bool, _type> _found = _get_method( _cid );              \
    _res_cell.present = std::get<0>( _found );                         \
    if( _res_cell.present ) {                                          \
        _res_cell.data = _conv_method( std::get<1>( _found ) );        \
    }

result_tuple transaction_partition_holder::get_data(
    const cell_identifier& cid, const cell_data_type& c_type ) const {
    result_tuple ret_tuple;
    ret_tuple.table_id = cid.table_id_;
    ret_tuple.row_id = cid.key_;

    result_cell res_cell;
    res_cell.col_id = cid.col_id_;
    res_cell.type = cell_data_type_to_data_type( c_type );
    switch( c_type ) {
        case cell_data_type::UINT64: {
            TEMPL_GET( cid, res_cell, get_uint64_data, uint64_to_string,
                       uint64_t );
            break;
        }
        case cell_data_type::INT64: {
            TEMPL_GET( cid, res_cell, get_int64_data, int64_to_string,
                       int64_t );
            break;
        }
        case cell_data_type::DOUBLE: {
            TEMPL_GET( cid, res_cell, get_double_data, double_to_string,
                       double );
            break;
        }
        case cell_data_type::STRING: {
            TEMPL_GET( cid, res_cell, get_string_data, string_to_string,
                       std::string );
            break;
        }
        case cell_data_type::MULTI_COLUMN: {
            DLOG( FATAL ) << "Unable to get data, as multi column";
            res_cell.present = false;
        }
    }

    ret_tuple.cells.push_back( res_cell );

    return ret_tuple;
}

#define TEMPL_UPDATE( _cid, _data, _set_method, _conv_method, _type ) \
    _type _conv = _conv_method( _data );                              \
    return _set_method( _cid, _conv );

bool transaction_partition_holder::insert_data(
    const cell_identifier& cid, const std::string& data,
    const cell_data_type& c_type ) const {
    switch( c_type ) {
        case cell_data_type::UINT64: {
            TEMPL_UPDATE( cid, data, insert_uint64_data, string_to_uint64,
                          uint64_t );
            break;
        }
        case cell_data_type::INT64: {
            TEMPL_UPDATE( cid, data, insert_int64_data, string_to_int64,
                          int64_t );
            break;
        }
        case cell_data_type::DOUBLE: {
            TEMPL_UPDATE( cid, data, insert_double_data, string_to_double,
                          double );
            break;
        }
        case cell_data_type::STRING: {
            TEMPL_UPDATE( cid, data, insert_string_data, string_to_string,
                          std::string );
            break;
        }
        case cell_data_type::MULTI_COLUMN: {
            DLOG( FATAL ) << "Unable to insert data, as multi column";
        }
    }

    return false;
}

bool transaction_partition_holder::update_data(
    const cell_identifier& cid, const std::string& data,
    const cell_data_type& c_type ) const {
    switch( c_type ) {
        case cell_data_type::UINT64: {
            TEMPL_UPDATE( cid, data, update_uint64_data, string_to_uint64,
                          uint64_t );
            break;
        }
        case cell_data_type::INT64: {
            TEMPL_UPDATE( cid, data, update_int64_data, string_to_int64,
                          int64_t );
            break;
        }
        case cell_data_type::DOUBLE: {
            TEMPL_UPDATE( cid, data, update_double_data, string_to_double,
                          double );
            break;
        }
        case cell_data_type::STRING: {
            TEMPL_UPDATE( cid, data, update_string_data, string_to_string,
                          std::string );
            break;
        }
        case cell_data_type::MULTI_COLUMN: {
            DLOG( FATAL ) << "Unable to update data, as multi column";
        }
    }

    return false;
}
