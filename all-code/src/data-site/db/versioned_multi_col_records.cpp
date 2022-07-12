#include "versioned_multi_col_records.h"

versioned_multi_col_records::versioned_multi_col_records(
    const partition_column_identifier& pcid, bool is_column_sorted,
    const std::vector<cell_data_type>& col_types )
    : pcid_( pcid ),
      num_columns_( 1 + ( pcid_.column_end - pcid_.column_start ) ),
      is_column_sorted_( is_column_sorted ),
      col_types_( col_types ),
      snapshot_records_(),
      current_records_( num_columns_, nullptr ),
      dirtied_records_( num_columns_, false ),
      snapshot_versions_(),
      updated_version_( 0 ),
      snapshot_lock_(),
      updated_lock_(),
      do_stats_maintenance_( false ) {}

void versioned_multi_col_records::init_records() {
    DVLOG( 40 ) << "Init records";

    std::vector<std::shared_ptr<packed_column_records>> empty_snap;

    DCHECK_EQ( num_columns_, col_types_.size() );

    for( int32_t col = pcid_.column_start; col <= pcid_.column_end; col++ ) {
        int32_t        normal_col = normalize_column_id( col, pcid_ );
        cell_data_type col_type = get_col_type( col );

        current_records_.at( normal_col ) =
            std::make_shared<packed_column_records>(
                is_column_sorted_, col_type, pcid_.partition_start,
                pcid_.partition_end );

        auto empty_snap_recs = std::make_shared<packed_column_records>(
            is_column_sorted_, col_type, pcid_.partition_start,
            pcid_.partition_end );
        empty_snap.push_back( empty_snap_recs );
    }

    DVLOG( 40 ) << "Init records: current records_:" << current_records_;
    insert_into_snapshot_map( empty_snap, 0 );
}

uint64_t versioned_multi_col_records::merge_updates() {
    updated_lock_.lock();
    snapshot_lock_.lock();

    uint64_t snapshot_version = get_snapshot_version();

    DVLOG( 40 ) << "Versioned Column Records, snapshot version:"
                << snapshot_version << ", updated version:" << updated_version_;

    if( snapshot_version != updated_version_ ) {
        std::vector<std::shared_ptr<packed_column_records>> new_records_vec;

        for( int32_t col = pcid_.column_start; col <= pcid_.column_end;
             col++ ) {
            cell_data_type col_type = get_col_type( col );

            uint32_t normal_col = normalize_column_id( col, pcid_ );

            // deep copy if dirtied
            if( dirtied_records_.at( normal_col ) ) {
                current_records_.at( normal_col )->update_statistics();
                // swap it
                auto new_records = std::make_shared<packed_column_records>(
                    is_column_sorted_, col_type, pcid_.partition_start,
                    pcid_.partition_end );
                new_records->deep_copy(
                    *( current_records_.at( normal_col ) ) );

                new_records_vec.emplace_back( new_records );

                DVLOG( 40 )
                    << "Current_records_:" << current_records_.at( normal_col )
                    << ", data:" << *( current_records_.at( normal_col ) );

            } else {
                new_records_vec.emplace_back(
                    current_records_.at( normal_col ) );
            }
        }

        insert_into_snapshot_map( current_records_, updated_version_ );
        snapshot_version = updated_version_;

        for( uint32_t pos = 0; pos < new_records_vec.size(); pos++ ) {
            current_records_.at( pos ) = new_records_vec.at( pos );
            dirtied_records_.at( pos ) = false;
        }
    }

    snapshot_lock_.unlock();
    updated_lock_.unlock();

    return snapshot_version;
}

void versioned_multi_col_records::lock_snapshot_shared() const {
    snapshot_lock_.lock_shared();
}
void versioned_multi_col_records::unlock_snapshot_shared() const {
    snapshot_lock_.unlock_shared();
}

std::vector<std::shared_ptr<packed_column_records>>
    versioned_multi_col_records::get_snapshot_columns(
        uint64_t version ) const {

    std::vector<std::shared_ptr<packed_column_records>> records;
    auto found = snapshot_records_.find( version );
    if( found != snapshot_records_.cend() ) {
        for( uint32_t pos = 0; pos < found->second.size(); pos++ ) {
            records.emplace_back( found->second.at( pos ) );
        }
    }
    DVLOG( 40 ) << "Get snapshot columns:" << version << ", found:" << records;
    return records;
}
std::vector<std::shared_ptr<packed_column_records>>
    versioned_multi_col_records::get_current_columns() const {

    updated_lock_.lock_shared();

    std::vector<std::shared_ptr<packed_column_records>> cur;

    for( uint32_t col = 0; col < current_records_.size(); col++ ) {
        cur.emplace_back( current_records_.at( col ) );
    }

    updated_lock_.unlock_shared();

    return cur;
}

std::tuple<std::vector<std::shared_ptr<packed_column_records>>, uint64_t>
    versioned_multi_col_records::get_current_columns_and_version() const {

    updated_lock_.lock_shared();

    std::vector<std::shared_ptr<packed_column_records>> cur;

    for( uint32_t col = 0; col < current_records_.size(); col++ ) {
        cur.emplace_back( current_records_.at( col ) );
    }
    uint64_t version = updated_version_;

    updated_lock_.unlock_shared();

    return std::make_tuple<>( cur, version );
}

std::shared_ptr<packed_column_records>
    versioned_multi_col_records::get_snapshot_columns( uint64_t version,
                                                       uint32_t col ) const {
    uint32_t normal_col = normalize_column_id( col, pcid_ );
    auto     ret = get_snapshot_columns( version );
    if( normal_col < ret.size() ) {
        return ret.at(normal_col);
    }
    return nullptr;
}
std::shared_ptr<packed_column_records>
    versioned_multi_col_records::get_current_columns( uint32_t col ) const {
    uint32_t normal_col = normalize_column_id( col, pcid_ );
    auto     ret = get_current_columns();
    if( normal_col < ret.size() ) {
        return ret.at(normal_col);
    }
    return nullptr;
}
std::tuple<std::shared_ptr<packed_column_records>, uint64_t>
    versioned_multi_col_records::get_current_columns_and_version(
        uint32_t col ) const {
    uint32_t normal_col = normalize_column_id( col, pcid_ );
    auto     ret = get_current_columns_and_version();
    auto     ret_vec = std::get<0>( ret );
    if ( normal_col < ret_vec.size()) {
        return std::make_tuple<>( ret_vec.at( normal_col ),
                                  std::get<1>( ret ) );
    }

    return std::make_tuple<>( nullptr, std::get<1>( ret ) );
}

uint64_t versioned_multi_col_records::get_snapshot_version() const {

    DCHECK_GT( snapshot_versions_.size(), 0 );
    uint64_t version = snapshot_versions_.at( snapshot_versions_.size() - 1 );

    return version;
}
uint64_t versioned_multi_col_records::get_current_version() const {
    return updated_version_;
}

void versioned_multi_col_records::set_updated_version( uint64_t version ) {
    updated_version_ = version;
}
void versioned_multi_col_records::set_current_records(
    const std::vector<std::shared_ptr<packed_column_records>>& cur_columns,
    uint64_t                                                   version ) {
    updated_lock_.lock();

    DCHECK_EQ( cur_columns.size(), current_records_.size() );

    DVLOG( 40 ) << "Set current records:" << version;

    for( uint32_t col = 0; col < cur_columns.size(); col++ ) {
        current_records_.at( col ) = cur_columns.at( col );
    }
    updated_version_ = version;

    updated_lock_.unlock();
}

cell_data_type versioned_multi_col_records::get_col_type( uint32_t col ) const {
    uint32_t normal_col = normalize_column_id( col, pcid_ );
    DCHECK_LT( normal_col, col_types_.size() );

    return col_types_.at( normal_col );
}

packed_cell_data* versioned_multi_col_records::update_uint64_data(
    const cell_identifier& ci, uint64_t data,
    const snapshot_vector& snapshot ) {
    packed_cell_data* pcd = get_or_create_packed_cell_data( ci );
    DCHECK( pcd );
    pcd->set_uint64_data( data );
    return pcd;
}
packed_cell_data* versioned_multi_col_records::update_int64_data(
    const cell_identifier& ci, int64_t data, const snapshot_vector& snapshot ) {
    packed_cell_data* pcd = get_or_create_packed_cell_data( ci );
    DCHECK( pcd );
    pcd->set_int64_data( data );
    return pcd;
}
packed_cell_data* versioned_multi_col_records::update_string_data(
    const cell_identifier& ci, const std::string& data,
    const snapshot_vector& snapshot ) {
    packed_cell_data* pcd = get_or_create_packed_cell_data( ci );
    DCHECK( pcd );
    pcd->set_string_data( data );
    return pcd;
}
packed_cell_data* versioned_multi_col_records::update_double_data(
    const cell_identifier& ci, double data, const snapshot_vector& snapshot ) {
    packed_cell_data* pcd = get_or_create_packed_cell_data( ci );
    DCHECK( pcd );
    pcd->set_double_data( data );
    return pcd;
}

void versioned_multi_col_records::commit_latest_updates( uint64_t version ) {
    updated_lock_.lock();

    if( updated_version_ >= version ) {
        DCHECK( ongoing_writes_.empty() );
        updated_lock_.unlock();
        return;
    }
    DVLOG( 40 ) << "Commit latest updates:" << version;

    for( const auto& entry : ongoing_writes_ ) {
        update_latest_records( entry.first, entry.second );
    }
    DVLOG( 40 ) << "Commit latest updates:" << version
                << ", records:" << current_records_;

    DCHECK_GT( version, updated_version_ );
    updated_version_ = version;

    updated_lock_.unlock();
}

void versioned_multi_col_records::update_latest_records(
    const cell_identifier& ci, packed_cell_data* pcd ) {
    cell_data_type cell_type = get_col_type( ci.col_id_ );

    uint32_t normal_col = normalize_column_id( ci.col_id_, pcid_ );
    dirtied_records_.at( normal_col ) = true;

    if( !pcd->is_present() ) {
        current_records_.at( normal_col )
            ->remove_data( ci.key_, do_stats_maintenance_ );
        return;
    }
    switch( cell_type ) {
        case cell_data_type::UINT64:
            current_records_.at( normal_col )
                ->update_data( ci.key_, pcd->get_uint64_data(),
                               do_stats_maintenance_ );
            break;
        case cell_data_type::INT64:
            current_records_.at( normal_col )
                ->update_data( ci.key_, pcd->get_int64_data(),
                               do_stats_maintenance_ );
            break;
        case cell_data_type::DOUBLE:
            current_records_.at( normal_col )
                ->update_data( ci.key_, pcd->get_double_data(),
                               do_stats_maintenance_ );
            break;

        case cell_data_type::STRING:
            current_records_.at( normal_col )
                ->update_data( ci.key_, pcd->get_string_data(),
                               do_stats_maintenance_ );
            break;

        case cell_data_type::MULTI_COLUMN:
            LOG( WARNING ) << "Should not be in a multi column situation";
    }
}

void versioned_multi_col_records::clear_latest_updates() {
    for( auto& entry : ongoing_writes_ ) {
        if( entry.second ) {
            delete entry.second;
        }
    }
    ongoing_writes_.clear();
}

packed_cell_data* versioned_multi_col_records::get_or_create_packed_cell_data(
    const cell_identifier& ci ) {
    auto found = ongoing_writes_.find( ci );
    if( found != ongoing_writes_.end() ) {
        return found->second;
    }
    packed_cell_data* pcd = new packed_cell_data();
    ongoing_writes_[ci] = pcd;
    return pcd;
}

void versioned_multi_col_records::insert_into_snapshot_map(
    const std::vector<std::shared_ptr<packed_column_records>>& record,
    uint64_t                                                   version ) {
    DVLOG( 40 ) << "Insert:" << version << ", into snapshot records:" << record;
    snapshot_records_.insert_or_assign( version, record );
    snapshot_versions_.emplace_back( version );
}

// returns K_NOT_COMMITTED if not found;
uint64_t versioned_multi_col_records::
    get_snapshot_version_that_satisfies_requirement(
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

uint64_t versioned_multi_col_records::search_for_version(
    uint64_t req_version, uint32_t low_pos, uint32_t high_pos ) const {

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
