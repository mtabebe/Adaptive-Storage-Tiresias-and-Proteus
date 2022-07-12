#include "multi_col_partition.h"

#include "../../common/perf_tracking.h"
#include "../../common/string_conversion.h"

#include "predicate.h"

sorted_multi_col_partition::sorted_multi_col_partition()
    : multi_col_partition( true ) {}

partition_type::type sorted_multi_col_partition::get_partition_type() const {
    return partition_type::type::SORTED_MULTI_COLUMN;
}

multi_col_partition::multi_col_partition()
    : internal_partition(),
      records_( nullptr ),
      is_sorted_( false ),
      do_stats_maintenance_( false ) {}
multi_col_partition::multi_col_partition( bool is_sorted )
    : internal_partition(),
      records_( nullptr ),
      is_sorted_( is_sorted ),
      do_stats_maintenance_( false ) {}

void multi_col_partition::init(
    const partition_metadata&          metadata,
    const std::vector<cell_data_type>& col_types, uint64_t version,
    std::shared_ptr<shared_partition_state>& shared_partition,
    void*                                    parent_partition ) {

    internal_partition::init( metadata, col_types, version, shared_partition,
                              parent_partition );
    init_records();
}
partition_type::type multi_col_partition::get_partition_type() const {
    return partition_type::type::MULTI_COLUMN;
}
void multi_col_partition::init_records() {
    std::shared_ptr<versioned_multi_col_records> records =
        std::make_shared<versioned_multi_col_records>(
            metadata_.partition_id_, is_sorted_,
            shared_partition_->col_types_ );
    records->init_records();
    set_records( records );
}

void multi_col_partition::set_commit_version( uint64_t version ) {
    records_->set_updated_version( version );
    internal_partition::set_commit_version( version );
}

void multi_col_partition::abort_write_transaction() {
    records_->clear_latest_updates();
    internal_partition::abort_write_transaction();
}
void multi_col_partition::finalize_commit(
    const snapshot_vector& dependency_snapshot, uint64_t version,
    uint64_t table_epoch, bool no_prop_commit ) {

    DVLOG( 40 ) << "Column partition, finalize commit:"
                << metadata_.partition_id_ << ", version:" << version;

    records_->commit_latest_updates( version );
    internal_partition::finalize_commit( dependency_snapshot, version,
                                         table_epoch, no_prop_commit );

    DVLOG( 40 ) << "Column partition, finalize commit, okay!";
}

bool multi_col_partition::begin_read_wait_and_build_snapshot(
    snapshot_vector& snapshot, bool no_apply_last ) {
    bool ret = false;
    if( is_sorted_ ) {
        start_timer( SORTED_MULTI_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID );
        ret = internal_partition::begin_read_wait_and_build_snapshot(
            snapshot, no_apply_last );
        stop_timer( SORTED_MULTI_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID );
    } else {
        start_timer( MULTI_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID );
        ret = internal_partition::begin_read_wait_and_build_snapshot(
            snapshot, no_apply_last );
        stop_timer( MULTI_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID );
    }
    return ret;
}

bool multi_col_partition::begin_read_wait_for_version(
    snapshot_vector& snapshot, bool no_apply_last ) {
    bool ret = false;
    if( is_sorted_ ) {
        start_timer( SORTED_MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID );
        ret = internal_partition::begin_read_wait_for_version( snapshot,
                                                               no_apply_last );
        stop_timer( SORTED_MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID );
    } else {
        start_timer( MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID );
        ret = internal_partition::begin_read_wait_for_version( snapshot,
                                                               no_apply_last );
        stop_timer( MULTI_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID );
    }
    return ret;
}
bool multi_col_partition::begin_write( snapshot_vector& snapshot,
                                       bool             acquire_lock ) {
    bool ret = false;
    if( is_sorted_ ) {
        start_timer( SORTED_MULTI_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID );
        ret = internal_partition::begin_write( snapshot, acquire_lock );
        stop_timer( SORTED_MULTI_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID );
    } else {
        start_timer( MULTI_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID );
        ret = internal_partition::begin_write( snapshot, acquire_lock );
        stop_timer( MULTI_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID );
    }

    return ret;
}

void multi_col_partition::done_with_updates() {
    records_->clear_latest_updates();
    internal_partition::done_with_updates();
}

void multi_col_partition::set_records(
    std::shared_ptr<versioned_multi_col_records> records ) {
    records_ = records;
}

uint64_t multi_col_partition::wait_until_version_or_apply_updates(
    uint64_t req_version ) {

    DVLOG( 40 )
        << "Column partition, wait until version or apply updates, req_version:"
        << req_version;

    if( shared_partition_->get_storage_tier() != storage_tier_type::MEMORY ) {
        uint64_t storage_version = shared_partition_->storage_version_;
        if( storage_version >= req_version ) {
            return storage_version;
        }
    }
    pull_data_if_not_in_memory();



    uint64_t snap_version =
        records_->get_snapshot_version_that_satisfies_requirement(
            req_version );

    if( snap_version != K_NOT_COMMITTED ) {
        if( snap_version >= req_version ) {
            DVLOG( 40 )
                << "Column partition, wait until version or apply updates, "
                   "req_version:"
                << req_version << ", returning:" << snap_version;

            return snap_version;
        }
    }
    // wait for updates, then merge

    uint64_t current_version = records_->get_current_version();
    if( current_version < req_version ) {
        current_version =
            internal_partition::wait_until_version_or_apply_updates(
                req_version );
    }

    DVLOG( 40 ) << "Column partition, wait until version or apply updates, "
                   "req_version:"
                << req_version << ", merging updates in";

    current_version = records_->merge_updates();

    DVLOG( 40 ) << "Column partition, wait until version or apply updates, "
                   "req_version:"
                << req_version << ", returning:" << current_version;
    return current_version;
}

// point deletion
bool multi_col_partition::remove_data( const cell_identifier& ci,
                                       const snapshot_vector& snapshot ) {
    return internal_remove_data( ci, snapshot, true, true /*update stats*/ );
}
bool multi_col_partition::internal_remove_data( const cell_identifier& ci,
                                                const snapshot_vector& snapshot,
                                                bool                   do_prop,
                                                bool do_update_stats ) {
    packed_cell_data* pcd = records_->get_or_create_packed_cell_data( ci );
    if( !pcd ) {
        return false;
    }
    pcd->set_as_deleted();
    if( do_update_stats ) {
        remove_table_stats_data( ci, pcd->get_data_size() );
    }
    if( do_prop ) {
        add_to_write_buffer( ci, pcd, K_DELETE_OP );
    }

    return true;
}

#define UPDATE_TABLE_STATS( _ci, _prev_size, _new_size ) \
    update_table_stats_data( _ci, _prev_size, _new_size );

#define GENERIC_WRITE_DATA( _method, _op_code, _c_type, _ci, _data, _snapshot, \
                            _name, _do_prop, _do_update_stats, _prev_size,     \
                            _new_size, _b_ret )                                \
    DVLOG( 40 ) << "Col " << _name << " record:" << _ci;                       \
    packed_cell_data* _pcd = records_->_method( _ci, _data, _snapshot );       \
    _b_ret = false;                                                            \
    if( _pcd ) {                                                               \
        _b_ret = true;                                                         \
        if( _do_update_stats ) {                                               \
            UPDATE_TABLE_STATS( _ci, _prev_size, _new_size );                  \
        }                                                                      \
        if( _do_prop ) {                                                       \
            add_to_write_buffer( _ci, _pcd, _op_code );                        \
        }                                                                      \
    }                                                                          \
    DVLOG( 40 ) << "Col " << _name << " record:" << _ci << ", ret:" << _b_ret;

#define INSERT_DATA( _method, _c_type, _ci, _data, _snapshot, _prev_size, \
                     _new_size )                                          \
    bool _b_ret = false;                                                  \
    if( is_sorted_ ) {                                                    \
        start_timer( SORTED_MULTI_COLUMN_INSERT_RECORD_TIMER_ID );              \
        GENERIC_WRITE_DATA( _method, K_INSERT_OP, _c_type, _ci, _data,    \
                            _snapshot, "insert", true, true, _prev_size,  \
                            _new_size, _b_ret );                          \
        stop_timer( SORTED_MULTI_COLUMN_INSERT_RECORD_TIMER_ID );               \
    } else {                                                              \
        start_timer( MULTI_COLUMN_INSERT_RECORD_TIMER_ID );                     \
        GENERIC_WRITE_DATA( _method, K_INSERT_OP, _c_type, _ci, _data,    \
                            _snapshot, "insert", true, true, _prev_size,  \
                            _new_size, _b_ret );                          \
        stop_timer( MULTI_COLUMN_INSERT_RECORD_TIMER_ID );                      \
    }                                                                     \
    return _b_ret;

// point inserts
bool multi_col_partition::insert_uint64_data(
    const cell_identifier& ci, uint64_t data,
    const snapshot_vector& snapshot ) {
    INSERT_DATA( update_uint64_data, cell_data_type::UINT64, ci, data, snapshot,
                 -1, sizeof( uint64_t ) );
    return false;
}
bool multi_col_partition::insert_int64_data( const cell_identifier& ci,
                                             int64_t                data,
                                             const snapshot_vector& snapshot ) {
    INSERT_DATA( update_int64_data, cell_data_type::INT64, ci, data, snapshot,
                 -1, sizeof( int64_t ) );
    return false;
}
bool multi_col_partition::insert_string_data(
    const cell_identifier& ci, const std::string& data,
    const snapshot_vector& snapshot ) {
    INSERT_DATA( update_string_data, cell_data_type::STRING, ci, data, snapshot,
                 -1, data.size() );
    return false;
}
bool multi_col_partition::insert_double_data(
    const cell_identifier& ci, double data, const snapshot_vector& snapshot ) {
    INSERT_DATA( update_double_data, cell_data_type::DOUBLE, ci, data, snapshot,
                 -1, sizeof( double ) );
    return false;
}

#define UPDATE_DATA( _method, _c_type, _ci, _data, _snapshot, _prev_size, \
                     _new_size )                                          \
    bool _b_ret = false;                                                  \
    if( is_sorted_ ) {                                                    \
        start_timer( SORTED_MULTI_COLUMN_WRITE_RECORD_TIMER_ID );               \
        GENERIC_WRITE_DATA( _method, K_WRITE_OP, _c_type, _ci, _data,     \
                            _snapshot, "write", true, true, _prev_size,   \
                            _new_size, _b_ret );                          \
        stop_timer( SORTED_MULTI_COLUMN_WRITE_RECORD_TIMER_ID );                \
    } else {                                                              \
        start_timer( MULTI_COLUMN_WRITE_RECORD_TIMER_ID );                      \
        GENERIC_WRITE_DATA( _method, K_WRITE_OP, _c_type, _ci, _data,     \
                            _snapshot, "write", true, true, _prev_size,   \
                            _new_size, _b_ret );                          \
        stop_timer( MULTI_COLUMN_WRITE_RECORD_TIMER_ID );                       \
    }                                                                     \
    return _b_ret;

// point updates
bool multi_col_partition::update_uint64_data(
    const cell_identifier& ci, uint64_t data,
    const snapshot_vector& snapshot ) {
    UPDATE_DATA( update_uint64_data, cell_data_type::UINT64, ci, data, snapshot,
                 sizeof( uint64_t ), sizeof( uint64_t ) );
    return false;
}
bool multi_col_partition::update_int64_data( const cell_identifier& ci,
                                             int64_t                data,
                                             const snapshot_vector& snapshot ) {
    UPDATE_DATA( update_int64_data, cell_data_type::INT64, ci, data, snapshot,
                 sizeof( int64_t ), sizeof( int64_t ) );
    return false;
}
bool multi_col_partition::update_string_data(
    const cell_identifier& ci, const std::string& data,
    const snapshot_vector& snapshot ) {
    int32_t prev_size = -1;
    auto    found_prev = get_latest_string_data( ci );
    if( std::get<0>( found_prev ) ) {
        prev_size = std::get<1>( found_prev ).size();
    }

    UPDATE_DATA( update_string_data, cell_data_type::STRING, ci, data, snapshot,
                 prev_size, data.size() );
    return false;
}
bool multi_col_partition::update_double_data(
    const cell_identifier& ci, double data, const snapshot_vector& snapshot ) {
    UPDATE_DATA( update_double_data, cell_data_type::DOUBLE, ci, data, snapshot,
                 sizeof( double ), sizeof( double ) );
    return false;
}

int32_t multi_col_partition::get_index_position_from_file(
    data_reader* reader, const cell_identifier& ci ) const {

    DVLOG( 40 ) << "get_index_position_from_file:" << ci;

    int32_t on_disk_pos = k_unassigned_col;

    uint32_t col_position =
        ( normalize_column_id( ci.col_id_, metadata_.partition_id_ ) *
          sizeof( uint32_t ) ) +
        reader->get_position();

    DVLOG( 40 ) << "get_index_position_from_file:" << ci
                << ", col_position:" << col_position;

    reader->seek_to_position( col_position );

    int32_t col_location;
    reader->read_i32( (uint32_t*) &col_location );

    DVLOG( 40 ) << "get_index_position_from_file:" << ci
                << ", col_location:" << col_location;
    if( col_location == k_unassigned_col ) {
        return on_disk_pos;
    }

    reader->seek_to_position( col_location );

    uint32_t read_num_keys;
    reader->read_i32( (uint32_t*) &read_num_keys );
    uint32_t num_keys = ( metadata_.partition_id_.partition_end -
                          metadata_.partition_id_.partition_start ) +
                        1;
    DCHECK_EQ( num_keys, read_num_keys );

    uint32_t per_key_index_size =
        sizeof( uint64_t ) /*key*/ +
        ( 2 * sizeof( uint32_t ) /* relative pos, pos on disk*/ );

    uint32_t key_pos =
        ( per_key_index_size *
          ( ci.key_ - metadata_.partition_id_.partition_start ) ) +
        reader->get_position();

    DVLOG( 40 ) << "get_index_position_from_file:" << ci
                << ", key_pos:" << key_pos;

    reader->seek_to_position( key_pos );

    int64_t read_key;
    int32_t in_column_index_pos;

    reader->read_i64( (uint64_t*) &read_key );
    DCHECK_EQ( read_key, ci.key_ );

    reader->read_i32( (uint32_t*) &in_column_index_pos );
    reader->read_i32( (uint32_t*) &on_disk_pos );
    if( in_column_index_pos == -1 ) {
        DCHECK_EQ( on_disk_pos, -1 );
        on_disk_pos = k_unassigned_col;
    }

    DVLOG( 40 ) << "get_index_position_from_file:" << ci
                << ", on_disk_pos:" << on_disk_pos;

    return on_disk_pos;
}

#define TEMPL_READ_CELL_FROM_DISK( _ci, _snapshot, _read_method, _type,        \
                                   _read_type, _default )                      \
    internal_partition::check_cell_identifier_within_partition( _ci );         \
    bool  _b_ret = false;                                                      \
    _type _t_ret = _default;                                                   \
    shared_partition_->lock_storage();                                         \
    if( shared_partition_->get_storage_tier() ==                               \
        storage_tier_type::type::DISK ) {                                      \
        auto file_name = shared_partition_->get_disk_file_name();              \
        if( std::get<0>( file_name ) ) {                                       \
            data_reader _reader( std::get<1>( file_name ) );                   \
            _reader.open();                                                    \
            chomp_metadata( &_reader );                                        \
            int32_t _data_pos = get_index_position_from_file( &_reader, _ci ); \
            if( _data_pos != k_unassigned_col ) {                              \
                _reader.seek_to_position( _data_pos );                         \
                _b_ret = true;                                                 \
                _reader._read_method( (_read_type*) &_t_ret );                 \
            }                                                                  \
            _reader.close();                                                   \
        }                                                                      \
    }                                                                          \
    shared_partition_->unlock_storage();                                       \
    return std::make_tuple<>( _b_ret, _t_ret );

template <>
std::tuple<bool, uint64_t> multi_col_partition::read_cell_from_disk(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    TEMPL_READ_CELL_FROM_DISK( ci, snapshot, read_i64, uint64_t, uint64_t, 0 );
}
template <>
std::tuple<bool, int64_t> multi_col_partition::read_cell_from_disk(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    TEMPL_READ_CELL_FROM_DISK( ci, snapshot, read_i64, int64_t, uint64_t, 0 );
}
template <>
std::tuple<bool, double> multi_col_partition::read_cell_from_disk(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    TEMPL_READ_CELL_FROM_DISK( ci, snapshot, read_dbl, double, double, 0 );
}
template <>
std::tuple<bool, std::string> multi_col_partition::read_cell_from_disk(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    TEMPL_READ_CELL_FROM_DISK( ci, snapshot, read_str, std::string, std::string,
                               "" );
}

#define GENERIC_READ_DATA( _method, _read_method, _type, _c_type, _ci, \
                           _default, _snapshot )                       \
    DVLOG( 40 ) << "Col read record:" << _ci;                          \
    _t_ret = _default;                                                 \
    _b_ret = false;                                                    \
    if( shared_partition_->get_storage_tier() ==                       \
        storage_tier_type::type::DISK ) {                              \
        std::tuple<bool, _type> _read_disk =                           \
            read_cell_from_disk<_type>( _ci, _snapshot );              \
        _b_ret = std::get<0>( _read_disk );                            \
        _t_ret = std::get<1>( _read_disk );                            \
    } else {                                                           \
        std::shared_ptr<packed_column_records> _pcr =                  \
            _read_method( _ci, _snapshot );                            \
        if( _pcr ) {                                                   \
            auto _a_ret = _pcr->_method( _ci.key_ );                   \
            _b_ret = std::get<0>( _a_ret );                            \
            if( _b_ret ) {                                             \
                _t_ret = std::get<1>( _a_ret );                        \
            }                                                          \
        }                                                              \
    }                                                                  \
    DVLOG( 40 ) << "Col read record:" << _ci << ", ret:" << _b_ret;

#define READ_DATA( _method, _type, _c_type, _ci, _default, _snapshot )       \
    _type _t_ret = _default;                                                 \
    bool  _b_ret = false;                                                    \
    bool  is_on_disk = shared_partition_->get_storage_tier() ==              \
                      storage_tier_type::type::DISK;                         \
    if( is_sorted_ ) {                                                       \
        if( is_on_disk ) {                                                   \
            start_timer( READ_SORTED_MULTI_COL_FROM_DISK_TIMER_ID );         \
            GENERIC_READ_DATA( _method, read_cell_data, _type, _c_type, _ci, \
                               _default, _snapshot );                        \
            stop_timer( READ_SORTED_MULTI_COL_FROM_DISK_TIMER_ID );          \
        } else {                                                             \
            start_timer( SORTED_MULTI_COL_READ_RECORD_TIMER_ID );            \
            GENERIC_READ_DATA( _method, read_cell_data, _type, _c_type, _ci, \
                               _default, _snapshot );                        \
            stop_timer( SORTED_MULTI_COL_READ_RECORD_TIMER_ID );             \
        }                                                                    \
    } else {                                                                 \
        if( is_on_disk ) {                                                   \
            start_timer( READ_MULTI_COL_FROM_DISK_TIMER_ID );                \
            GENERIC_READ_DATA( _method, read_cell_data, _type, _c_type, _ci, \
                               _default, _snapshot );                        \
            stop_timer( READ_MULTI_COL_FROM_DISK_TIMER_ID );                 \
        } else {                                                             \
            start_timer( MULTI_COL_READ_RECORD_TIMER_ID );                   \
            GENERIC_READ_DATA( _method, read_cell_data, _type, _c_type, _ci, \
                               _default, _snapshot );                        \
            stop_timer( MULTI_COL_READ_RECORD_TIMER_ID );                    \
        }                                                                    \
    }                                                                        \
    return std::make_tuple<bool, _type>( std::forward<bool>( _b_ret ),       \
                                         std::forward<_type>( _t_ret ) );

#define READ_LATEST_DATA( _method, _type, _c_type, _ci, _default )             \
    snapshot_vector _snapshot;                                                 \
    _type           _t_ret = _default;                                         \
    bool            _b_ret = false;                                            \
    bool            is_on_disk = shared_partition_->get_storage_tier() ==      \
                      storage_tier_type::type::DISK;                           \
    if( is_sorted_ ) {                                                         \
        if( is_on_disk ) {                                                     \
            start_timer( READ_SORTED_MULTI_COL_FROM_DISK_TIMER_ID );           \
            GENERIC_READ_DATA( _method, read_latest_cell_data, _type, _c_type, \
                               _ci, _default, _snapshot );                     \
            stop_timer( READ_SORTED_MULTI_COL_FROM_DISK_TIMER_ID );            \
        } else {                                                               \
            start_timer( SORTED_MULTI_COL_READ_LATEST_RECORD_TIMER_ID );       \
            GENERIC_READ_DATA( _method, read_latest_cell_data, _type, _c_type, \
                               _ci, _default, _snapshot );                     \
            stop_timer( SORTED_MULTI_COL_READ_LATEST_RECORD_TIMER_ID );        \
        }                                                                      \
    } else {                                                                   \
        if( is_on_disk ) {                                                     \
            start_timer( READ_MULTI_COL_FROM_DISK_TIMER_ID );                  \
            GENERIC_READ_DATA( _method, read_latest_cell_data, _type, _c_type, \
                               _ci, _default, _snapshot );                     \
            stop_timer( READ_MULTI_COL_FROM_DISK_TIMER_ID );                   \
        } else {                                                               \
            start_timer( MULTI_COL_READ_LATEST_RECORD_TIMER_ID );              \
            GENERIC_READ_DATA( _method, read_latest_cell_data, _type, _c_type, \
                               _ci, _default, _snapshot );                     \
            stop_timer( MULTI_COL_READ_LATEST_RECORD_TIMER_ID );               \
        }                                                                      \
    }                                                                          \
    return std::make_tuple<bool, _type>( std::forward<bool>( _b_ret ),         \
                                         std::forward<_type>( _t_ret ) );

// point lookups
std::tuple<bool, uint64_t> multi_col_partition::get_uint64_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    READ_DATA( get_uint64_data, uint64_t, cell_data_type::UINT64, ci, 0,
               snapshot );
}
std::tuple<bool, int64_t> multi_col_partition::get_int64_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    READ_DATA( get_int64_data, int64_t, cell_data_type::INT64, ci, 0,
               snapshot );
}
std::tuple<bool, double> multi_col_partition::get_double_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    READ_DATA( get_double_data, double, cell_data_type::DOUBLE, ci, 0,
               snapshot );
}
std::tuple<bool, std::string> multi_col_partition::get_string_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    READ_DATA( get_string_data, std::string, cell_data_type::STRING, ci, "",
               snapshot );
}

std::tuple<bool, uint64_t> multi_col_partition::get_latest_uint64_data(
    const cell_identifier& ci ) const {
    READ_LATEST_DATA( get_uint64_data, uint64_t, cell_data_type::UINT64, ci,
                      0 );
}
std::tuple<bool, int64_t> multi_col_partition::get_latest_int64_data(
    const cell_identifier& ci ) const {
    READ_LATEST_DATA( get_int64_data, int64_t, cell_data_type::INT64, ci, 0 );
}
std::tuple<bool, double> multi_col_partition::get_latest_double_data(
    const cell_identifier& ci ) const {
    READ_LATEST_DATA( get_double_data, double, cell_data_type::DOUBLE, ci, 0 );
}
std::tuple<bool, std::string> multi_col_partition::get_latest_string_data(
    const cell_identifier& ci ) const {
    READ_LATEST_DATA( get_string_data, std::string, cell_data_type::STRING, ci,
                      "" );
}

void multi_col_partition::scan(
    uint64_t low_key, uint64_t high_key, const predicate_chain& predicate,
    const std::vector<uint32_t>& project_cols, const snapshot_vector& snapshot,
    std::vector<result_tuple>& result_tuples ) const {
    if( is_sorted_ ) {
        start_timer( SORTED_MULTI_COL_SCAN_RECORDS_TIMER_ID );
        internal_scan( low_key, high_key, predicate, project_cols, snapshot,
                       result_tuples );
        stop_timer( SORTED_MULTI_COL_SCAN_RECORDS_TIMER_ID );
    } else {
        start_timer( MULTI_COL_SCAN_RECORDS_TIMER_ID );
        internal_scan( low_key, high_key, predicate, project_cols, snapshot,
                       result_tuples );
        stop_timer( MULTI_COL_SCAN_RECORDS_TIMER_ID );
    }
}

void multi_col_partition::internal_scan(
    uint64_t low_key, uint64_t high_key, const predicate_chain& predicate,
    const std::vector<uint32_t>& project_cols, const snapshot_vector& snapshot,
    std::vector<result_tuple>& result_tuples ) const {
    std::vector<uint32_t> translated_cols_to_project =
        internal_partition::get_col_ids_to_scan( low_key, high_key,
                                                 project_cols );
    if( translated_cols_to_project.empty() ) {
        return;
    }

    predicate_chain translated_predicate =
        translate_predicate_to_partition( predicate );

    auto cols = records_->get_snapshot_columns(
        get_snapshot_version( snapshot, metadata_.partition_id_ ) );
    if( cols.empty() ) {
        return;
    }

    uint64_t min_key =
        std::max( low_key, (uint64_t) metadata_.partition_id_.partition_start );
    uint64_t max_key =
        std::min( high_key, (uint64_t) metadata_.partition_id_.partition_end );

    uint64_t num_keys = ( max_key - min_key ) + 1;

    (void) num_keys;

    partition_column_identifier loc_pid = metadata_.partition_id_;

    std::vector<result_tuple> global_results;
    std::unordered_map<uint64_t, uint32_t> map_to_pos;

    std::unordered_set<uint32_t> no_pred_cols;

    for( uint32_t pos = 0; pos < translated_cols_to_project.size(); pos++ ) {
        uint32_t trans_col = translated_cols_to_project.at( pos );
        DCHECK_LT( trans_col, cols.size() );

        uint32_t normal_col = trans_col + metadata_.partition_id_.column_start;

        loc_pid.column_start = normal_col;
        loc_pid.column_end = normal_col;

        std::vector<cell_data_type> loc_col_types = {
            shared_partition_->col_types_.at( trans_col )};
        predicate_chain loc_pred = translate_predicate_to_partition_id(
            predicate, loc_pid, loc_col_types );

        if( loc_pred.and_predicates.empty() and
            loc_pred.or_predicates.empty() ) {
            no_pred_cols.insert( trans_col );
        }

        std::vector<result_tuple> loc_res_tuple;
        std::vector<uint32_t>     loc_cols_to_project = {0};

        cols.at( trans_col )
            ->scan( loc_pid, min_key, max_key, loc_pred, loc_cols_to_project,
                    loc_res_tuple );

        for( auto& res_tuple : loc_res_tuple ) {
            DVLOG( 40 ) << "Col scan: row:" << res_tuple.row_id
                        << ", col:" << normal_col;

            DCHECK_EQ( 1, res_tuple.cells.size() );
            res_tuple.cells.at( 0 ).col_id = normal_col;
            auto found = map_to_pos.find( res_tuple.row_id );
            if( found == map_to_pos.end() ) {
                map_to_pos.emplace( res_tuple.row_id, global_results.size() );
                global_results.push_back( res_tuple );
            } else {
                auto& existing_res_tuple = global_results.at( found->second );
                existing_res_tuple.cells.push_back( res_tuple.cells.at( 0 ) );
            }
        }
    }

    // now we need to combine them
    //
    // if it's an or, we simply need 1 column that satisfies the predicate
    // if it's an and, we need every column to satisfy the predicate
    uint32_t num_cols_needed_to_satisfy_predicate = no_pred_cols.size();
    if( translated_predicate.is_and_join_predicates ) {
        num_cols_needed_to_satisfy_predicate =
            translated_cols_to_project.size();
    } else if( translated_predicate.or_predicates.empty() ) {
        num_cols_needed_to_satisfy_predicate =
            translated_cols_to_project.size();
    } else {
        // we need at least one of the or's
        num_cols_needed_to_satisfy_predicate += 1;
    }

    DVLOG( 40 ) << "Num cols needed to satisfy predicate:"
                << num_cols_needed_to_satisfy_predicate
                << ", translated_cols_to_project:" << translated_cols_to_project
                << ", project_cols:" << project_cols;

    for( auto& res_tuple : global_results ) {
        DVLOG( 40 ) << "Considering global res tuple:" << res_tuple.row_id
                    << ", num cells:" << res_tuple.cells.size();

        if( res_tuple.cells.size() < num_cols_needed_to_satisfy_predicate ) {
            DVLOG( 40 ) << "Skipping, as size below threshold";
            continue;
        }
        // consider it as an option as it has it least the right number of
        // columns
        if( res_tuple.cells.size() < translated_cols_to_project.size() ) {
            DVLOG( 40 ) << "Adding additional cells";
            // lets fill in what's missing
            uint32_t trans_cols_to_project_pos = 0;
            uint32_t res_pos = 0;
            while( res_pos < res_tuple.cells.size() ) {
                uint32_t cell_col = res_tuple.cells.at( res_pos ).col_id -
                                    metadata_.partition_id_.column_start;
                uint32_t trans_col =
                    translated_cols_to_project.at( trans_cols_to_project_pos );
                if( cell_col == trans_col ) {
                    res_pos += 1;
                    trans_cols_to_project_pos += 1;
                } else if( cell_col > trans_col ) {
                    DVLOG( 40 )
                        << "Adding additional cell, row:" << res_tuple.row_id
                        << ", col:"
                        << trans_col + metadata_.partition_id_.column_start;
                    res_tuple.cells.insert(
                        res_tuple.cells.begin() + res_pos,
                        get_result_cell( res_tuple.row_id, trans_col, cols ) );
                } else {
                    DLOG( FATAL ) << "cell_col(" << cell_col << "), trans_col("
                                  << trans_col << ")";
                }
            }
            while( trans_cols_to_project_pos <
                   translated_cols_to_project.size() ) {
                DVLOG( 40 )
                    << "Adding additional cell, row:" << res_tuple.row_id
                    << ", col:"
                    << translated_cols_to_project.at(
                           trans_cols_to_project_pos ) +
                           metadata_.partition_id_.column_start;

                res_tuple.cells.emplace_back( get_result_cell(
                    res_tuple.row_id,
                    translated_cols_to_project.at( trans_cols_to_project_pos ),
                    cols ) );

                trans_cols_to_project_pos += 1;
            }
        }

        DCHECK_EQ( res_tuple.cells.size(), translated_cols_to_project.size() );

        result_tuples.emplace_back( res_tuple );
    }
}

#define TEMPL_GET_RESULT_CELL( _res, _row, _col, _trans_col, _cols,           \
                               _data_type, _get_method, _conv_method, _type ) \
    DCHECK_LT( _col, _cols.size() );                                          \
    auto _col_data = _cols.at( _col );                                        \
    std::tuple<bool, _type> _read = _col_data->_get_method( _row );           \
    _res.col_id = _trans_col;                                                 \
    _res.present = std::get<0>( _read );                                      \
    _res.type = _data_type;                                                   \
    if( _res.present ) {                                                      \
        _res.data = _conv_method( std::get<1>( _read ) );                     \
    }

result_cell multi_col_partition::get_result_cell(
    uint64_t row, uint32_t logical_col,
    const std::vector<std::shared_ptr<packed_column_records>>& cols ) const {
    DCHECK_LT( logical_col, cols.size() );
    DCHECK_LT( logical_col, shared_partition_->col_types_.size() );

    auto col_type = shared_partition_->col_types_.at( logical_col );

    uint32_t trans_col = logical_col + metadata_.partition_id_.column_start;

    result_cell res;

    switch( ( col_type ) ) {
        case cell_data_type::UINT64: {
            TEMPL_GET_RESULT_CELL( res, row, logical_col, trans_col, cols,
                                   data_type::type::UINT64, get_uint64_data,
                                   uint64_to_string, uint64_t );
            break;
        }
        case cell_data_type::INT64: {
            TEMPL_GET_RESULT_CELL( res, row, logical_col, trans_col, cols,
                                   data_type::type::INT64, get_int64_data,
                                   int64_to_string, int64_t );
            break;
        }
        case cell_data_type::DOUBLE: {
            TEMPL_GET_RESULT_CELL( res, row, logical_col, trans_col, cols,
                                   data_type::type::DOUBLE, get_double_data,
                                   double_to_string, double );
            break;
        }
        case cell_data_type::STRING: {
            TEMPL_GET_RESULT_CELL( res, row, logical_col, trans_col, cols,
                                   data_type::type::STRING, get_string_data,
                                   string_to_string, std::string );

            break;
        }
        case cell_data_type::MULTI_COLUMN: {
            break;
        }
    }

    return res;
}

std::shared_ptr<packed_column_records> multi_col_partition::read_cell_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    internal_partition::check_cell_identifier_within_partition( ci );
    return records_->get_snapshot_columns(
        get_snapshot_version( snapshot, metadata_.partition_id_ ), ci.col_id_ );
}

std::shared_ptr<packed_column_records>
    multi_col_partition::read_latest_cell_data(
        const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    internal_partition::check_cell_identifier_within_partition( ci );
    return records_->get_current_columns( ci.col_id_ );
}

void multi_col_partition::add_to_write_buffer( const cell_identifier& ci,
                                               packed_cell_data*      pcd,
                                               uint32_t op_code ) {
    shared_partition_->txn_execution_state_.add_to_write_buffer( ci, pcd,
                                                                 op_code );
}

void copy_in_col_data(
    std::vector<std::shared_ptr<packed_column_records>>&       dest_rec,
    const std::vector<std::shared_ptr<packed_column_records>>& src_rec,
    const partition_column_identifier&                         dest_pcid,
    const partition_column_identifier&                         src_pcid,
    const std::vector<cell_data_type>&                         src_types ) {
    for( int32_t col = dest_pcid.column_start; col <= dest_pcid.column_end;
         col++ ) {
        int32_t normal_col = normalize_column_id( col, src_pcid );
        if( normal_col < (int32_t) src_rec.size() ) {
            std::shared_ptr<packed_column_records> covering_data =
                src_rec.at( normal_col );
            bool is_sorted = covering_data->is_sorted();
            std::shared_ptr<packed_column_records> dest_p =
                std::make_shared<packed_column_records>(
                    is_sorted, src_types.at( normal_col ),
                    dest_pcid.partition_start, dest_pcid.partition_end );
            dest_p->deep_copy( *( covering_data ) );
            dest_rec.emplace_back( dest_p );
        }
    }
}

void split_packed_multi_column_records_vertically(
    std::vector<std::shared_ptr<packed_column_records>>& low_p_recs,
    std::vector<std::shared_ptr<packed_column_records>>& high_p_recs,
    const std::vector<std::shared_ptr<packed_column_records>>&
                                       covering_data_recs,
    const partition_column_identifier& covering_pcid,
    const std::vector<cell_data_type>& col_types,
    const partition_column_identifier& low_pcid,
    const partition_column_identifier& high_pcid ) {
    copy_in_col_data( low_p_recs, covering_data_recs, low_pcid, covering_pcid,
                      col_types );
    copy_in_col_data( high_p_recs, covering_data_recs, high_pcid, covering_pcid,
                      col_types );
}

std::tuple<std::vector<std::shared_ptr<packed_column_records>>,
           std::vector<std::shared_ptr<packed_column_records>>>
    split_packed_multi_column_records(
        uint32_t col_split_point, uint64_t row_split_point,
        const std::vector<std::shared_ptr<packed_column_records>>&
                                           covering_data_recs,
        const partition_column_identifier& covering_pcid,
        const std::vector<cell_data_type>& col_types,
        const partition_column_identifier& low_pcid,
        const std::vector<cell_data_type>& low_types,
        const partition_column_identifier& high_pcid,
        const std::vector<cell_data_type>& high_types, bool is_sorted ) {

    std::vector<std::shared_ptr<packed_column_records>> low_p_recs;
    std::vector<std::shared_ptr<packed_column_records>> high_p_recs;

    if( row_split_point != k_unassigned_key ) {
        // split horizontally
        for( uint32_t normal_col = 0; normal_col < covering_data_recs.size();
             normal_col++ ) {
            auto covering_data = covering_data_recs.at( normal_col );

            std::shared_ptr<packed_column_records> low_p =
                std::make_shared<packed_column_records>(
                    covering_data->is_sorted(), col_types.at( normal_col ),
                    low_pcid.partition_start, low_pcid.partition_end );
            std::shared_ptr<packed_column_records> high_p =
                std::make_shared<packed_column_records>(
                    covering_data->is_sorted(), col_types.at( normal_col ),
                    high_pcid.partition_start, high_pcid.partition_end );

            covering_data->split_col_records_horizontally(
                low_p.get(), high_p.get(), row_split_point );

            low_p_recs.emplace_back( low_p );
            high_p_recs.emplace_back( high_p );
        }
    } else if( col_split_point != k_unassigned_col ) {
        // split vertically
        if( is_sorted ) {
            start_timer( SPLIT_MULTI_COL_RECORDS_VERTICALLY_TIMER_ID );
            split_packed_multi_column_records_vertically(
                low_p_recs, high_p_recs, covering_data_recs, covering_pcid,
                col_types, low_pcid, high_pcid );
            stop_timer( SPLIT_MULTI_COL_RECORDS_VERTICALLY_TIMER_ID );
        } else {
            start_timer( SPLIT_MULTI_SORTED_COL_RECORDS_VERTICALLY_TIMER_ID );
            split_packed_multi_column_records_vertically(
                low_p_recs, high_p_recs, covering_data_recs, covering_pcid,
                col_types, low_pcid, high_pcid );
            stop_timer( SPLIT_MULTI_SORTED_COL_RECORDS_VERTICALLY_TIMER_ID );
        }
    }

    return std::make_tuple<>( low_p_recs, high_p_recs );
}

void merge_packed_multi_column_records_vertically(
    std::vector<std::shared_ptr<packed_column_records>>&       merged_p_recs,
    const std::vector<std::shared_ptr<packed_column_records>>& low_data_recs,
    const partition_column_identifier&                         low_pcid,
    const std::vector<cell_data_type>&                         low_types,
    const std::vector<std::shared_ptr<packed_column_records>>& high_data_recs,
    const partition_column_identifier&                         high_pcid,
    const std::vector<cell_data_type>&                         high_types ) {
    copy_in_col_data( merged_p_recs, low_data_recs, low_pcid, low_pcid,
                      low_types );
    copy_in_col_data( merged_p_recs, high_data_recs, high_pcid, high_pcid,
                      high_types );
}

std::vector<std::shared_ptr<packed_column_records>>
    merge_packed_multi_column_records(
        uint32_t col_merge_point, uint64_t row_merge_point,
        const partition_column_identifier& cover_pcid,
        const std::vector<cell_data_type>& cover_types,
        const std::vector<std::shared_ptr<packed_column_records>>&
                                           low_data_recs,
        const partition_column_identifier& low_pcid,
        const std::vector<cell_data_type>& low_types,
        const std::vector<std::shared_ptr<packed_column_records>>&
                                           high_data_recs,
        const partition_column_identifier& high_pcid,
        const std::vector<cell_data_type>& high_types, bool is_sorted ) {

    std::vector<std::shared_ptr<packed_column_records>> merged_p_recs;

    if( row_merge_point != k_unassigned_key ) {
        DVLOG( 40 ) << "Merge packed multi column records horizontally";
        DCHECK_EQ( low_data_recs.size(), high_data_recs.size() );

        for( uint32_t col = 0; col < low_data_recs.size(); col++ ) {
            DCHECK_LT( col, cover_types.size() );

            auto low_data = low_data_recs.at( col );
            auto high_data = high_data_recs.at( col );

            bool is_sorted = low_data->is_sorted() and high_data->is_sorted();

            std::shared_ptr<packed_column_records> merged_p =
                std::make_shared<packed_column_records>(
                    is_sorted, cover_types.at( col ),
                    cover_pcid.partition_start, cover_pcid.partition_end );

            merged_p->merge_col_records_horizontally( low_data.get(),
                                                      high_data.get() );

            merged_p_recs.emplace_back( merged_p );
        }

    } else if( col_merge_point != k_unassigned_col ) {
        DVLOG( 40 ) << "Merge packed multi column records vertically";

        if( is_sorted ) {
            start_timer( MERGE_MULTI_COL_RECORDS_VERTICALLY_TIMER_ID );
            merge_packed_multi_column_records_vertically(
                merged_p_recs, low_data_recs, low_pcid, low_types,
                high_data_recs, high_pcid, high_types );
            stop_timer( MERGE_MULTI_COL_RECORDS_VERTICALLY_TIMER_ID );
        } else {
            start_timer( MERGE_MULTI_SORTED_COL_RECORDS_VERTICALLY_TIMER_ID );
            merge_packed_multi_column_records_vertically(
                merged_p_recs, low_data_recs, low_pcid, low_types,
                high_data_recs, high_pcid, high_types );
            stop_timer( MERGE_MULTI_SORTED_COL_RECORDS_VERTICALLY_TIMER_ID );
        }
    }

    return merged_p_recs;
}

void multi_col_partition::merge_multi_col_records( multi_col_partition* low_p,
                                                   multi_col_partition* high_p,
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

    auto merged_records = merge_packed_multi_column_records(
        col_merge_point, row_merge_point, metadata_.partition_id_,
        shared_partition_->col_types_, low_p->records_->get_current_columns(),
        low_p->metadata_.partition_id_, low_p->shared_partition_->col_types_,
        high_p->records_->get_current_columns(),
        high_p->metadata_.partition_id_, high_p->shared_partition_->col_types_,
        is_sorted_ );

    set_master_location( low_p->shared_partition_->get_master_location() );

    records_->set_current_records( merged_records, merge_version );
}

void multi_col_partition::split_multi_col_records( multi_col_partition* low_p,
                                                   multi_col_partition* high_p,
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

    auto split_records = split_packed_multi_column_records(
        col_split_point, row_split_point, records_->get_current_columns(),
        metadata_.partition_id_, shared_partition_->col_types_,
        low_p->metadata_.partition_id_, low_p->shared_partition_->col_types_,
        high_p->metadata_.partition_id_, high_p->shared_partition_->col_types_,
        is_sorted_ );

    low_p->set_master_location( shared_partition_->get_master_location() );
    high_p->set_master_location( shared_partition_->get_master_location() );

    low_p->records_->set_current_records( std::get<0>( split_records ),
                                          low_version );
    high_p->records_->set_current_records( std::get<1>( split_records ),
                                           high_version );
}

#define REPARTITION( _ci, _get_method, _set_method, _type, _src )  \
    snapshot_vector _snapshot;                                     \
    std::tuple<bool, _type> _get_ret = _src->_get_method( _ci );   \
    if( std::get<0>( _get_ret ) ) {                                \
        /* no propagation  */                                      \
        packed_cell_data* _pcd = nullptr;                          \
        if( is_sorted_ ) {                                         \
            start_timer( SORTED_MULTI_COL_REPARTITION_INSERT_TIMER_ID ); \
            _pcd = records_->_set_method(                          \
                _ci, (_type) std::get<1>( _get_ret ), _snapshot ); \
            stop_timer( SORTED_MULTI_COL_REPARTITION_INSERT_TIMER_ID );  \
        } else {                                                   \
            start_timer( MULTI_COL_REPARTITION_INSERT_TIMER_ID );        \
            _pcd = records_->_set_method(                          \
                _ci, (_type) std::get<1>( _get_ret ), _snapshot ); \
            stop_timer( MULTI_COL_REPARTITION_INSERT_TIMER_ID );         \
        }                                                          \
        DCHECK( _pcd );                                            \
    }

void multi_col_partition::repartition_cell_into_partition(
    const cell_identifier& ci, internal_partition* src ) {
    internal_partition::check_cell_identifier_within_partition( ci );
    // read and write into it
    switch( get_cell_type( ci ) ) {
        case cell_data_type::UINT64: {
            REPARTITION( ci, get_latest_uint64_data, update_uint64_data,
                         uint64_t, src );
            break;
        }
        case cell_data_type::INT64: {
            REPARTITION( ci, get_latest_int64_data, update_int64_data, int64_t,
                         src );
            break;
        }
        case cell_data_type::DOUBLE: {
            REPARTITION( ci, get_latest_double_data, update_double_data, double,
                         src );
            break;
        }
        case cell_data_type::STRING: {
            REPARTITION( ci, get_latest_string_data, update_string_data,
                         std::string, src );
            break;
        }
        case cell_data_type::MULTI_COLUMN: {
            break;
        }
    }
}

void multi_col_partition::finalize_repartitions( uint64_t version ) {
    //    records_->commit_latest_updates( version );
}

#define APPLY_PROP_UPDATE( _set_method, _type, _convert_method, _du_cell,   \
                           _snapshot, _name, _op_code )                     \
    _type _data = _convert_method( _du_cell.get_update_buffer(),            \
                                   _du_cell.get_update_buffer_length() );   \
    bool _b_ret = false;                                                    \
    GENERIC_WRITE_DATA( _set_method, _op_code, _type, _du_cell.cid_, _data, \
                        _snapshot, _name, false /* no prop*/,               \
                        false /* no stats*/, 0, 0, _b_ret );                \
    return _b_ret;

#define APPLY_CELL_PROP_UPDATE( _name, _du_cell, _snapshot, _op_code )        \
    switch( get_cell_type( _du_cell.cid_ ) ) {                                \
        case cell_data_type::UINT64: {                                        \
            APPLY_PROP_UPDATE( update_uint64_data, uint64_t, chars_to_uint64, \
                               _du_cell, _snapshot, _name, _op_code );        \
            break;                                                            \
        }                                                                     \
        case cell_data_type::INT64: {                                         \
            APPLY_PROP_UPDATE( update_int64_data, int64_t, chars_to_int64,    \
                               _du_cell, _snapshot, _name, _op_code );        \
            break;                                                            \
        }                                                                     \
        case cell_data_type::DOUBLE: {                                        \
            APPLY_PROP_UPDATE( update_double_data, double, chars_to_double,   \
                               _du_cell, _snapshot, _name, _op_code );        \
            break;                                                            \
        }                                                                     \
        case cell_data_type::STRING: {                                        \
            APPLY_PROP_UPDATE( update_string_data, std::string,               \
                               chars_to_string, _du_cell, _snapshot, _name,   \
                               _op_code );                                    \
            break;                                                            \
        }                                                                     \
        case cell_data_type::MULTI_COLUMN: {                                  \
            break;                                                            \
        }                                                                     \
    }

bool multi_col_partition::apply_propagated_cell_update(
    const deserialized_cell_op& du_cell, const snapshot_vector& snapshot,
    bool do_insert, bool do_store, uint64_t version ) {
    if( !do_store ) {
        return internal_remove_data( du_cell.cid_, snapshot, false /* no prop*/,
                                     false /* no update stats*/ );
    } else if( do_insert ) {
        APPLY_CELL_PROP_UPDATE( "propagated_insert", du_cell, snapshot,
                                K_INSERT_OP );
    } else {
        APPLY_CELL_PROP_UPDATE( "propagated_update", du_cell, snapshot,
                                K_WRITE_OP );
    }
    return false;
}

void multi_col_partition::snapshot_partition_data(
    snapshot_partition_column_state& snapshot ) {
    snapshot_partition_metadata( snapshot );
    DVLOG( 40 ) << "Snapshot partition data:" << metadata_.partition_id_
                << ", snapshot:" << snapshot.session_version_vector;
    auto col_records = records_->get_snapshot_columns( get_snapshot_version(
        snapshot.session_version_vector, metadata_.partition_id_ ) );
    if( col_records.empty() ) {
        return;
    }
    DCHECK_EQ( col_records.size(), snapshot.columns.size() );
    for( uint32_t col = 0; col < col_records.size(); col++ ) {
        auto records = col_records.at( col );
        if( !records ) {
            continue;
        }
        snapshot_partition_column_state loc_snapshot;
        loc_snapshot.columns.push_back( snapshot.columns.at( col ) );

        std::vector<cell_data_type> loc_col_types = {
            shared_partition_->col_types_.at( col )};

        records->snapshot_state( loc_snapshot, loc_col_types );

        snapshot.columns.at( col ) = std::move( loc_snapshot.columns.at( 0 ) );
    }
}

#define TEMPL_INSTALL_SNAPSHOT( _keys, _data, _snapshot, _cid, _update_method, \
                                _type, _conv_method )                          \
    _type _conv_data = _conv_method( _data );                                  \
    for( uint64_t key : _keys ) {                                              \
        _cid.key_ = key;                                                       \
        bool _b_ret = false;                                                   \
        GENERIC_WRITE_DATA( _update_method, K_INSERT_OP, _type, _cid,          \
                            _conv_data, _snapshot, "install_snapshot",         \
                            false /* no prop*/, false /* no stats*/, 0, 0,     \
                            _b_ret );                                          \
        DCHECK( _b_ret );                                                      \
    }                                                                          \
    return true;

void multi_col_partition::install_snapshotted_column(
    const snapshot_column_state& column, const snapshot_vector& snapshot ) {
    DCHECK_EQ( column.keys.size(), column.data.size() );

    for( uint32_t pos = 0; pos < column.keys.size(); pos++ ) {
        bool insert_ok = install_snapshotted_column_pos(
            column.keys.at( pos ), column.data.at( pos ), column.col_id,
            column.type, snapshot );
        DCHECK( insert_ok );
    }
}
bool multi_col_partition::install_snapshotted_column_pos(
    const std::vector<int64_t>& keys, const std::string& data,
    const int32_t& col_id, const data_type::type& d_type,
    const snapshot_vector& snapshot ) {
    cell_identifier cid;
    cid.table_id_ = metadata_.partition_id_.table_id;
    cid.col_id_ = col_id;
    cid.key_ = 0;

    switch( d_type ) {
        case data_type::type::UINT64: {
            TEMPL_INSTALL_SNAPSHOT( keys, data, snapshot, cid,
                                    update_uint64_data, uint64_t,
                                    string_to_uint64 );
            break;
        }
        case data_type::type::INT64: {
            TEMPL_INSTALL_SNAPSHOT( keys, data, snapshot, cid,
                                    update_int64_data, int64_t,
                                    string_to_int64 );
            break;
        }
        case data_type::type::DOUBLE: {
            TEMPL_INSTALL_SNAPSHOT( keys, data, snapshot, cid,
                                    update_double_data, double,
                                    string_to_double );
            break;
        }
        case data_type::type::STRING: {
            TEMPL_INSTALL_SNAPSHOT( keys, data, snapshot, cid,
                                    update_string_data, std::string,
                                    string_to_string );
            break;
        }
    }
    return false;
}

void multi_col_partition::change_type_from_sorted_column(
    const multi_col_partition* sorted_col, uint64_t version ) {

    start_timer( CHANGE_TYPE_FROM_SORTED_MULTI_COLUMN_TIMER_ID );

    DCHECK( !is_sorted_ );
    DCHECK( sorted_col->is_sorted_ );

    std::vector<std::shared_ptr<packed_column_records>> new_p_recs;
    for( int32_t col = metadata_.partition_id_.column_start;
         col <= metadata_.partition_id_.column_end; col++ ) {
        uint32_t normal_col =
            normalize_column_id( col, metadata_.partition_id_ );

        auto type = shared_partition_->col_types_.at( normal_col );
        std::shared_ptr<packed_column_records> new_p =
            std::make_shared<packed_column_records>(
                is_sorted_, type, metadata_.partition_id_.partition_start,
                metadata_.partition_id_.partition_end );

        if( sorted_col->records_ ) {
            auto other = sorted_col->records_->get_current_columns( col );
            if( other ) {
                new_p->install_from_sorted_data( other.get() );
            }
        }

        new_p_recs.emplace_back( new_p );
    }

    records_->set_current_records( new_p_recs, version );

    stop_timer( CHANGE_TYPE_FROM_SORTED_MULTI_COLUMN_TIMER_ID );
}

void multi_col_partition::finalize_change_type( uint64_t version ) {
    finalize_repartitions( version );
    records_->commit_latest_updates( version );
}

void multi_col_partition::persist_data( data_persister* part_persister ) const {
    if( records_ ) {
        auto    cols_and_version = records_->get_current_columns_and_version();
        auto    cols = std::get<0>( cols_and_version );
        int32_t num_cols = ( metadata_.partition_id_.column_end -
                             metadata_.partition_id_.column_start ) +
                           1;
        uint64_t version = std::get<1>( cols_and_version );

        if( cols.size() == (uint32_t) num_cols ) {
            persist_col_data( part_persister, cols, version );
        }
    }
}

#define TEMPL_PERSIST_CELL( _rec_id, _col, _part_persister, _get_method, \
                            _conv_method, _type )                        \
    std::tuple<bool, _type> _read_data = _col->_get_method( _rec_id );   \
    if( std::get<0>( _read_data ) ) {                                    \
        _part_persister->write_i8( 1 );                                  \
        _part_persister->write_str(                                      \
            _conv_method( std::get<1>( _read_data ) ) );                 \
    } else {                                                             \
        _part_persister->write_i8( 0 );                                  \
    }

void multi_col_partition::persist_col_data(
    data_persister*                                            part_persister,
    const std::vector<std::shared_ptr<packed_column_records>>& cols,
    uint64_t                                                   version ) const {
    uint64_t pid_hash = metadata_.partition_id_hash_;

    for( int64_t record = metadata_.partition_id_.partition_start;
         record <= metadata_.partition_id_.partition_end; record++ ) {

        part_persister->write_i64( record );
        part_persister->write_i64( pid_hash );
        part_persister->write_i64( version );
        part_persister->write_i64( pid_hash );

        for( uint32_t col = 0; col < cols.size(); col++ ) {
            cell_data_type col_type = shared_partition_->col_types_.at( col );
            switch( col_type ) {
                case cell_data_type::UINT64: {
                    TEMPL_PERSIST_CELL( record, cols.at( col ), part_persister,
                                        get_uint64_data, uint64_to_string,
                                        uint64_t );
                    break;
                }
                case cell_data_type::INT64: {
                    TEMPL_PERSIST_CELL( record, cols.at( col ), part_persister,
                                        get_int64_data, int64_to_string,
                                        int64_t );
                    break;
                }
                case cell_data_type::DOUBLE: {
                    TEMPL_PERSIST_CELL( record, cols.at( col ), part_persister,
                                        get_double_data, double_to_string,
                                        double );
                    break;
                }
                case cell_data_type::STRING: {
                    TEMPL_PERSIST_CELL( record, cols.at( col ), part_persister,
                                        get_string_data, string_to_string,
                                        std::string );
                    break;
                }
                case cell_data_type::MULTI_COLUMN: {
                    break;
                }
            }
        }
    }
}

column_stats<multi_column_data> multi_col_partition::persist_to_disk(
    data_persister* part_persister ) const {
    auto mcd_types =
        construct_multi_column_data_types( shared_partition_->col_types_ );
    multi_column_data default_stats( mcd_types );

    column_stats<multi_column_data> stats;
    stats.count_ = 0;
    stats.min_.deep_copy( default_stats );
    stats.max_.deep_copy( default_stats );
    stats.average_.deep_copy( default_stats );
    stats.sum_.deep_copy( default_stats );

    uint32_t col_position_pos = part_persister->get_position();

    DVLOG(40) << "Write index";
    // write in the column positions initially as k_unassigned_col
    for( int32_t col = metadata_.partition_id_.column_start;
         col <= metadata_.partition_id_.column_end; col++ ) {
        part_persister->write_i32( k_unassigned_col );
    }

    if( records_ == nullptr ) {
        return stats;
    }
    auto records_vec = records_->get_current_columns();
    if( records_vec.size() != shared_partition_->col_types_.size() ) {
        return stats;
    }


    DVLOG(40) << "Write data";
    for( uint32_t normal_col = 0; normal_col < records_vec.size();
         normal_col++ ) {
        DVLOG( 40 ) << "Write col:" << normal_col;
        uint32_t begin_position = part_persister->get_position();
        uint32_t index_pos =
            col_position_pos + ( normal_col * sizeof( uint32_t ) );

        // store the index position
        part_persister->seek_to_position( index_pos );
        part_persister->write_i32( begin_position );
        part_persister->seek_to_position( begin_position );

        auto records = records_vec.at( normal_col );

        DVLOG( 40 ) << "Write col data:" << normal_col;
        // now write the col
        auto loc_stat = records->persist_to_disk( part_persister );

        if ( loc_stat.count_ > 0) {
            stats.count_ = loc_stat.count_;
            stats.min_.copy_in_data( normal_col, loc_stat.min_, 0, 1 );
            stats.max_.copy_in_data( normal_col, loc_stat.max_, 0, 1 );
            stats.average_.copy_in_data( normal_col, loc_stat.average_, 0, 1 );
        }
    }

    return stats;
}

void multi_col_partition::restore_from_disk( data_reader* reader,
                                             uint64_t     version ) {
    std::vector<int32_t> col_pos;
    DVLOG( 40 ) << "Read index";
    for( int32_t col = metadata_.partition_id_.column_start;
         col <= metadata_.partition_id_.column_end; col++ ) {
        int32_t read_pos;
        reader->read_i32( (uint32_t*) &read_pos );
        col_pos.emplace_back( read_pos );
    }

    DCHECK_EQ( col_pos.size(), shared_partition_->col_types_.size() );

    DVLOG( 40 ) << "Read col data";
    std::vector<std::shared_ptr<packed_column_records>> recs;
    for( int32_t normal_col = 0; normal_col < (int32_t) col_pos.size();
         normal_col++ ) {
        DVLOG( 40 ) << "Read col:" << normal_col;
        auto rec = std::make_shared<packed_column_records>(
            is_sorted_, shared_partition_->col_types_.at( normal_col ),
            metadata_.partition_id_.partition_start,
            metadata_.partition_id_.partition_end );

        if( col_pos.at( normal_col ) != k_unassigned_col ) {
            DCHECK_EQ( col_pos.at( normal_col ), reader->get_position() );
            DVLOG( 40 ) << "Read col data:" << normal_col;
            rec->restore_from_disk( reader );
        }

        recs.emplace_back( rec );
    }

    records_->set_current_records( recs, version );
    records_->merge_updates();
}
