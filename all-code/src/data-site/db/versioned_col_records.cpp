#include "versioned_col_records.h"

versioned_col_records::versioned_col_records(
    const partition_column_identifier& pcid, bool is_column_sorted,
    const std::vector<cell_data_type>& col_types )
    : pcid_( pcid ),
      is_column_sorted_( is_column_sorted ),
      col_types_( col_types ),
      snapshot_records_(),
      current_records_( nullptr ),
      snapshot_versions_(),
      updated_version_( 0 ),
      snapshot_lock_(),
      updated_lock_(),
      do_stats_maintenance_( false ) {
}

void versioned_col_records::init_records() {
    cell_data_type col_type = get_col_type();

    DVLOG(40) << "Init records";
    current_records_ = std::make_shared<packed_column_records>(
        is_column_sorted_, col_type, pcid_.partition_start,
        pcid_.partition_end );
    DVLOG( 40 ) << "Init records: current records_:" << current_records_;
    auto empty_snap = std::make_shared<packed_column_records>(
        is_column_sorted_, col_type, pcid_.partition_start,
        pcid_.partition_end );
    insert_into_snapshot_map( empty_snap, 0 );
}

uint64_t versioned_col_records::merge_updates() {
    updated_lock_.lock();
    snapshot_lock_.lock();

    uint64_t snapshot_version = get_snapshot_version();

    DVLOG( 40 ) << "Versioned Column Records, snapshot version:"
                << snapshot_version << ", updated version:" << updated_version_;

    cell_data_type col_type = get_col_type();

    if( snapshot_version != updated_version_ ) {

        // swap it
        auto new_records = std::make_shared<packed_column_records>(
            is_column_sorted_, col_type, pcid_.partition_start,
            pcid_.partition_end );

        DVLOG( 40 ) << "Current_records_:" << current_records_
                    << ", data:" << *current_records_;
        current_records_->update_statistics();

        // deep copy
        new_records->deep_copy( *current_records_ );
        insert_into_snapshot_map( current_records_, updated_version_ );
        snapshot_version = updated_version_;

        current_records_ = new_records;
    }

    snapshot_lock_.unlock();
    updated_lock_.unlock();

    return snapshot_version;
}

void versioned_col_records::lock_snapshot_shared() const {
    snapshot_lock_.lock_shared();
}
void versioned_col_records::unlock_snapshot_shared() const {
    snapshot_lock_.unlock_shared();
}

std::shared_ptr<packed_column_records>
    versioned_col_records::get_snapshot_columns( uint64_t version ) const {

    std::shared_ptr<packed_column_records> records = nullptr;
    auto found = snapshot_records_.find( version );
    if( found != snapshot_records_.cend() ) {
        records = found->second;
    }
    DVLOG( 40 ) << "Get snapshot columns:" << version << ", found:" << records;
    return records;
}
std::shared_ptr<packed_column_records>
    versioned_col_records::get_current_columns() const {

    updated_lock_.lock_shared();

    std::shared_ptr<packed_column_records> cur = current_records_;

    updated_lock_.unlock_shared();

    return cur;
}

std::tuple<std::shared_ptr<packed_column_records>, uint64_t>
    versioned_col_records::get_current_columns_and_version() const {

    updated_lock_.lock_shared();

    std::shared_ptr<packed_column_records> cur = current_records_;
    uint64_t                               version = updated_version_;

    updated_lock_.unlock_shared();

    return std::make_tuple<>( cur, version );
}

uint64_t versioned_col_records::get_snapshot_version() const {

    DCHECK_GT( snapshot_versions_.size(), 0 );
    uint64_t version = snapshot_versions_.at( snapshot_versions_.size() - 1 );

    return version;
}
uint64_t versioned_col_records::get_current_version() const {
    return updated_version_;
}

void versioned_col_records::set_updated_version( uint64_t version ) {
    updated_version_ = version;
}
void versioned_col_records::set_current_records(
    std::shared_ptr<packed_column_records> cur_columns, uint64_t version ) {
    updated_lock_.lock();

    DVLOG( 40 ) << "Set current records:" << version;

    current_records_ = cur_columns;
    updated_version_ = version;

    updated_lock_.unlock();
}

cell_data_type versioned_col_records::get_col_type() const {
    cell_data_type col_type = cell_data_type::MULTI_COLUMN;
    if( col_types_.size() == 1 ) {
        col_type = col_types_.at( 0 );
    }
    return col_type;
}

packed_cell_data* versioned_col_records::update_uint64_data(
    const cell_identifier& ci, uint64_t data, const snapshot_vector& snapshot ) {
    packed_cell_data* pcd = get_or_create_packed_cell_data( ci );
    DCHECK( pcd );
    pcd->set_uint64_data( data );
    return pcd;
}
packed_cell_data* versioned_col_records::update_int64_data(
    const cell_identifier& ci, int64_t data, const snapshot_vector& snapshot ) {
    packed_cell_data* pcd = get_or_create_packed_cell_data( ci );
    DCHECK( pcd );
    pcd->set_int64_data( data );
    return pcd;
}
packed_cell_data* versioned_col_records::update_string_data(
    const cell_identifier& ci, const std::string& data,
    const snapshot_vector& snapshot ) {
    packed_cell_data* pcd = get_or_create_packed_cell_data( ci );
    DCHECK( pcd );
    pcd->set_string_data( data );
    return pcd;
}
packed_cell_data* versioned_col_records::update_double_data(
    const cell_identifier& ci, double data, const snapshot_vector& snapshot ) {
    packed_cell_data* pcd = get_or_create_packed_cell_data( ci );
    DCHECK( pcd );
    pcd->set_double_data( data );
    return pcd;
}

void versioned_col_records::commit_latest_updates( uint64_t version ) {
    updated_lock_.lock();

    if( updated_version_ >= version ) {
        DCHECK( ongoing_writes_.empty() );
        updated_lock_.unlock();
        return;
    }
    DVLOG(40) <<"Commit latest updates:" << version;

    if( col_types_.size() > 1 ) {
        commit_latest_multi_column_updates();
    } else {
        for( const auto& entry : ongoing_writes_ ) {
            update_latest_records( entry.first, entry.second,
                                   col_types_.at( 0 ) );
        }
    }
    DVLOG( 40 ) << "Commit latest updates:" << version
                << ", records:" << current_records_
                << ", data:" << *current_records_;

    DCHECK_GT( version, updated_version_ );
    updated_version_ = version;

    updated_lock_.unlock();
}
void versioned_col_records::commit_latest_multi_column_updates() {
    std::unordered_map<uint64_t, multi_column_data*> multi_columns;

    std::vector<multi_column_data_type> multi_types;
    for( const auto& cell_type : col_types_ ) {
        uint32_t               size = sizeof( uint64_t );
        if( cell_type == cell_data_type::STRING) {
            size = 0;
        }
        multi_column_data_type mcdt = multi_column_data_type( cell_type, size );
        multi_types.push_back( mcdt );
    }
    for( auto& entry : ongoing_writes_ ) {
        cell_identifier   cid = entry.first;
        packed_cell_data* pcd = entry.second;

        auto              found_mc = multi_columns.find( cid.key_ );
        multi_column_data* mc = nullptr;
        if( found_mc != multi_columns.end() ) {
            mc = found_mc->second;
        } else {
            auto old_mc = current_records_->get_multi_column_data( cid.key_ );
            if( std::get<0>( old_mc ) ) {
                mc = new multi_column_data( std::get<1>( old_mc ) );
            } else {
                mc = new multi_column_data( multi_types );
            }
            multi_columns[cid.key_] = mc;
        }
        update_multi_column_record(
            mc, pcd, col_types_.at( normalize_column_id( cid.col_id_, pcid_ ) ),
            normalize_column_id( cid.col_id_, pcid_ ) );
    }

    for( auto& entry : multi_columns ) {
        uint64_t           key = entry.first;
        multi_column_data* data = entry.second;
        if( ( data == nullptr ) or data->empty() ) {
            current_records_->remove_data( key, do_stats_maintenance_ );
        } else {
            current_records_->update_data( key, *data, do_stats_maintenance_ );
            delete data;
        }
    }

    multi_columns.clear();
}

void versioned_col_records::update_multi_column_record(
    multi_column_data* mc, packed_cell_data* pcd,
    const cell_data_type& cell_type, uint32_t col_pos ) {
    if( !pcd->is_present() ) {
        mc->remove_data( col_pos );
        return;
    }
    switch( cell_type ) {
        case cell_data_type::UINT64:
            mc->set_uint64_data( col_pos, pcd->get_uint64_data() );
            break;
        case cell_data_type::INT64:
            mc->set_int64_data( col_pos, pcd->get_int64_data() );
            break;
        case cell_data_type::DOUBLE:
            mc->set_double_data( col_pos, pcd->get_double_data() );
            break;
        case cell_data_type::STRING:
            mc->set_string_data( col_pos, pcd->get_string_data() );
            break;
        case cell_data_type::MULTI_COLUMN:
            LOG( WARNING ) << "Should not be in a multi column situation";
    }
}

void versioned_col_records::update_latest_records(
    const cell_identifier& ci, packed_cell_data* pcd,
    const cell_data_type& cell_type ) {
    if( !pcd->is_present() ) {
        current_records_->remove_data( ci.key_, do_stats_maintenance_ );
        return;
    }
    switch( cell_type ) {
        case cell_data_type::UINT64:
            current_records_->update_data( ci.key_, pcd->get_uint64_data(),
                                           do_stats_maintenance_ );
            break;
        case cell_data_type::INT64:
            current_records_->update_data( ci.key_, pcd->get_int64_data(),
                                           do_stats_maintenance_ );
            break;
        case cell_data_type::DOUBLE:
            current_records_->update_data( ci.key_, pcd->get_double_data(),
                                           do_stats_maintenance_ );
            break;

        case cell_data_type::STRING:
            current_records_->update_data( ci.key_, pcd->get_string_data(),
                                           do_stats_maintenance_ );
            break;

        case cell_data_type::MULTI_COLUMN:
            LOG( WARNING ) << "Should not be in a multi column situation";
    }
}

void versioned_col_records::clear_latest_updates() {
    for( auto& entry : ongoing_writes_ ) {
        if( entry.second ) {
            delete entry.second;
        }
    }
    ongoing_writes_.clear();
}

packed_cell_data* versioned_col_records::get_or_create_packed_cell_data(
    const cell_identifier& ci ) {
    auto found = ongoing_writes_.find( ci );
    if ( found != ongoing_writes_.end()) {
        return found->second;
    }
    packed_cell_data* pcd = new packed_cell_data();
    ongoing_writes_[ ci ] = pcd;
    return pcd;
}

void versioned_col_records::insert_into_snapshot_map(
    std::shared_ptr<packed_column_records> record, uint64_t version ) {
    DVLOG( 40 ) << "Insert:" << version << ", into snapshot records:" << record
                << ", data:" << *record;
    snapshot_records_.insert_or_assign( version, record );
    snapshot_versions_.emplace_back( version );
}

// returns K_NOT_COMMITTED if not found;
uint64_t versioned_col_records::get_snapshot_version_that_satisfies_requirement(
    uint64_t req_version ) const {
    lock_snapshot_shared();

    DVLOG( 40 ) << "Get snapshot version that satisfies requirement:"
                << req_version;
    uint64_t sat_version =
        search_for_version( req_version, 0, snapshot_versions_.size() - 1 );
    DVLOG( 40 ) << "Get snapshot version that satisfies requirement:"
                << req_version << ", got version:" << sat_version;

    unlock_snapshot_shared();
    return sat_version;
}

uint64_t versioned_col_records::search_for_version( uint64_t req_version,
                                                    uint32_t low_pos,
                                                    uint32_t high_pos ) const {

    DVLOG( 40 ) << "Search for version:" << req_version
                << ", low pos:" << low_pos << ", high pos:" << high_pos;

    if( snapshot_versions_.at( high_pos ) < req_version ) {
        DVLOG( 40 ) << "High pos version:" << snapshot_versions_.at( high_pos )
                    << ", too small!";
        return K_NOT_COMMITTED;
    }
    uint32_t mid_pos = std::min( low_pos, high_pos );
    uint64_t mid_pos_value = snapshot_versions_.at( mid_pos );

    if( low_pos >= high_pos ) {
        DVLOG( 40 ) << "Low pos >= high pos";

        if( mid_pos_value >= req_version ) {
            DVLOG( 40 ) << "Version satisfies:" << mid_pos_value
                        << ", required version:" << req_version;
            return mid_pos_value;
        } else {
            DVLOG( 40 ) << "Version:" << mid_pos_value
                        << ", does not satisfiy required version:"
                        << req_version;

            return K_NOT_COMMITTED;
        }
    }
    mid_pos = ( low_pos + high_pos ) / 2;
    mid_pos_value = snapshot_versions_.at( mid_pos );

    DVLOG( 40 ) << "Mid pos:" << mid_pos << ", value:" << mid_pos_value;

    if( mid_pos_value == req_version ) {
        DVLOG( 40 ) << "Mid pos:" << mid_pos << ", value:" << mid_pos_value
                    << ", matches required version:" << req_version;
        return mid_pos_value;
    } else if( mid_pos_value < req_version ) {
        DVLOG( 40 ) << "Mid pos:" << mid_pos << ", value:" << mid_pos_value
                    << ", to small for required version:" << req_version
                    << ", restrict search to upper half";
        return search_for_version( req_version, mid_pos + 1, high_pos );
    } else {
        DVLOG( 40 ) << "Mid pos:" << mid_pos << ", value:" << mid_pos_value
                    << ", larger than required version:" << req_version
                    << ", restrict search to lower half";

        return search_for_version( req_version, low_pos, mid_pos );
    }

    return K_NOT_COMMITTED;
}
