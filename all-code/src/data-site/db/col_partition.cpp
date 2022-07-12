#include "col_partition.h"

#include "../../common/perf_tracking.h"
#include "../../common/string_conversion.h"

sorted_col_partition::sorted_col_partition() : col_partition( true ) {}

partition_type::type sorted_col_partition::get_partition_type() const {
    return partition_type::type::SORTED_COLUMN;
}

col_partition::col_partition()
    : internal_partition(),
      records_( nullptr ),
      is_sorted_( false ),
      do_stats_maintenance_( false ) {}
col_partition::col_partition( bool is_sorted )
    : internal_partition(),
      records_( nullptr ),
      is_sorted_( is_sorted ),
      do_stats_maintenance_( false ) {}

void col_partition::init(
    const partition_metadata&          metadata,
    const std::vector<cell_data_type>& col_types, uint64_t version,
    std::shared_ptr<shared_partition_state>& shared_partition,
    void*                                    parent_partition ) {

    internal_partition::init( metadata, col_types, version, shared_partition,
                              parent_partition );
    init_records();
}
partition_type::type col_partition::get_partition_type() const {
    return partition_type::type::COLUMN;
}
void col_partition::init_records() {
    std::shared_ptr<versioned_col_records> records =
        std::make_shared<versioned_col_records>(
            metadata_.partition_id_, is_sorted_,
            shared_partition_->col_types_ );
    records->init_records();
    set_records( records );
}

void col_partition::set_commit_version( uint64_t version ) {
    records_->set_updated_version( version );
    internal_partition::set_commit_version( version );
}

void col_partition::abort_write_transaction() {
    records_->clear_latest_updates();
    internal_partition::abort_write_transaction();
}
void col_partition::finalize_commit( const snapshot_vector& dependency_snapshot,
                                     uint64_t version, uint64_t table_epoch,
                                     bool no_prop_commit ) {

    DVLOG( 40 ) << "Column partition, finalize commit:"
                << metadata_.partition_id_ << ", version:" << version;

    records_->commit_latest_updates( version );
    internal_partition::finalize_commit( dependency_snapshot, version,
                                         table_epoch, no_prop_commit );

    DVLOG( 40 ) << "Column partition, finalize commit, okay!";
}

bool col_partition::begin_read_wait_and_build_snapshot(
    snapshot_vector& snapshot, bool no_apply_last ) {
    bool ret = false;
    if( is_sorted_ ) {
        start_timer( SORTED_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID );
        ret = internal_partition::begin_read_wait_and_build_snapshot(
            snapshot, no_apply_last );
        stop_timer( SORTED_COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID );
    } else {
        start_timer( COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID );
        ret = internal_partition::begin_read_wait_and_build_snapshot(
            snapshot, no_apply_last );
        stop_timer( COL_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID );
    }
    return ret;
}

bool col_partition::begin_read_wait_for_version( snapshot_vector& snapshot,
                                                 bool no_apply_last ) {
    bool ret = false;
    if( is_sorted_ ) {
        start_timer( SORTED_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID );
        ret = internal_partition::begin_read_wait_for_version( snapshot,
                                                               no_apply_last );
        stop_timer( SORTED_COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID );
    } else {
        start_timer( COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID );
        ret = internal_partition::begin_read_wait_for_version( snapshot,
                                                               no_apply_last );
        stop_timer( COL_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID );
    }
    return ret;
}
bool col_partition::begin_write( snapshot_vector& snapshot,
                                 bool             acquire_lock ) {
    bool ret = false;
    if( is_sorted_ ) {
        start_timer( SORTED_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID );
        ret = internal_partition::begin_write( snapshot, acquire_lock );
        stop_timer( SORTED_COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID );
    } else {
        start_timer( COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID );
        ret = internal_partition::begin_write( snapshot, acquire_lock );
        stop_timer( COL_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID );
    }

    return ret;
}

void col_partition::done_with_updates() {
    records_->clear_latest_updates();
    internal_partition::done_with_updates();
}

void col_partition::set_records(
    std::shared_ptr<versioned_col_records> records ) {
    records_ = records;
}

uint64_t col_partition::wait_until_version_or_apply_updates(
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
bool col_partition::remove_data( const cell_identifier& ci,
                                 const snapshot_vector& snapshot ) {
    return internal_remove_data( ci, snapshot, true, true /*update stats*/ );
}
bool col_partition::internal_remove_data( const cell_identifier& ci,
                                          const snapshot_vector& snapshot,
                                          bool do_prop, bool do_update_stats ) {
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
        start_timer( SORTED_COLUMN_INSERT_RECORD_TIMER_ID );              \
        GENERIC_WRITE_DATA( _method, K_INSERT_OP, _c_type, _ci, _data,    \
                            _snapshot, "insert", true, true, _prev_size,  \
                            _new_size, _b_ret );                          \
        stop_timer( SORTED_COLUMN_INSERT_RECORD_TIMER_ID );               \
    } else {                                                              \
        start_timer( COLUMN_INSERT_RECORD_TIMER_ID );                     \
        GENERIC_WRITE_DATA( _method, K_INSERT_OP, _c_type, _ci, _data,    \
                            _snapshot, "insert", true, true, _prev_size,  \
                            _new_size, _b_ret );                          \
        stop_timer( COLUMN_INSERT_RECORD_TIMER_ID );                      \
    }                                                                     \
    return _b_ret;

// point inserts
bool col_partition::insert_uint64_data( const cell_identifier& ci,
                                        uint64_t               data,
                                        const snapshot_vector& snapshot ) {
    INSERT_DATA( update_uint64_data, cell_data_type::UINT64, ci, data, snapshot,
                 -1, sizeof( uint64_t ) );
    return false;
}
bool col_partition::insert_int64_data( const cell_identifier& ci, int64_t data,
                                       const snapshot_vector& snapshot ) {
    INSERT_DATA( update_int64_data, cell_data_type::INT64, ci, data, snapshot,
                 -1, sizeof( int64_t ) );
    return false;
}
bool col_partition::insert_string_data( const cell_identifier& ci,
                                        const std::string&     data,
                                        const snapshot_vector& snapshot ) {
    INSERT_DATA( update_string_data, cell_data_type::STRING, ci, data, snapshot,
                 -1, data.size() );
    return false;
}
bool col_partition::insert_double_data( const cell_identifier& ci, double data,
                                        const snapshot_vector& snapshot ) {
    INSERT_DATA( update_double_data, cell_data_type::DOUBLE, ci, data, snapshot,
                 -1, sizeof( double ) );
    return false;
}

#define UPDATE_DATA( _method, _c_type, _ci, _data, _snapshot, _prev_size, \
                     _new_size )                                          \
    bool _b_ret = false;                                                  \
    if( is_sorted_ ) {                                                    \
        start_timer( SORTED_COLUMN_WRITE_RECORD_TIMER_ID );               \
        GENERIC_WRITE_DATA( _method, K_WRITE_OP, _c_type, _ci, _data,     \
                            _snapshot, "write", true, true, _prev_size,   \
                            _new_size, _b_ret );                          \
        stop_timer( SORTED_COLUMN_WRITE_RECORD_TIMER_ID );                \
    } else {                                                              \
        start_timer( COLUMN_WRITE_RECORD_TIMER_ID );                      \
        GENERIC_WRITE_DATA( _method, K_WRITE_OP, _c_type, _ci, _data,     \
                            _snapshot, "write", true, true, _prev_size,   \
                            _new_size, _b_ret );                          \
        stop_timer( COLUMN_WRITE_RECORD_TIMER_ID );                       \
    }                                                                     \
    return _b_ret;

// point updates
bool col_partition::update_uint64_data( const cell_identifier& ci,
                                        uint64_t               data,
                                        const snapshot_vector& snapshot ) {
    UPDATE_DATA( update_uint64_data, cell_data_type::UINT64, ci, data, snapshot,
                 sizeof( uint64_t ), sizeof( uint64_t ) );
    return false;
}
bool col_partition::update_int64_data( const cell_identifier& ci, int64_t data,
                                       const snapshot_vector& snapshot ) {
    UPDATE_DATA( update_int64_data, cell_data_type::INT64, ci, data, snapshot,
                 sizeof( int64_t ), sizeof( int64_t ) );
    return false;
}
bool col_partition::update_string_data( const cell_identifier& ci,
                                        const std::string&     data,
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
bool col_partition::update_double_data( const cell_identifier& ci, double data,
                                        const snapshot_vector& snapshot ) {
    UPDATE_DATA( update_double_data, cell_data_type::DOUBLE, ci, data, snapshot,
                 sizeof( double ), sizeof( double ) );
    return false;
}

int32_t col_partition::get_index_position_from_file(
    data_reader* reader, const cell_identifier& ci ) const {

    DVLOG( 40 ) << "get_index_position_from_file:" << ci;

    int32_t on_disk_pos = k_unassigned_col;

    int32_t col_position;
    reader->read_i32( (uint32_t*) &col_position );
    DVLOG( 40 ) << "get_index_position_from_file:" << ci
                << ", read col_position:" << col_position;
    if( col_position == k_unassigned_col ) {
        return on_disk_pos;
    }

    // could seek, but it should be the next position

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

#define TEMPL_READ_CELL_FROM_DISK( _ci, _snapshot, _read_method,               \
                                   _mcd_read_method, _type, _read_type,        \
                                   _default )                                  \
    bool  _b_ret = false;                                                      \
    _type _t_ret = _default;                                                   \
    internal_partition::check_cell_identifier_within_partition( _ci );         \
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
                if( shared_partition_->col_types_.size() > 1 ) {               \
                    /* read in a mcd*/                                         \
                    multi_column_data _mcd;                                    \
                    _mcd.restore_from_disk( &_reader );                        \
                    std::tuple<bool, _type> _read_data =                       \
                        _mcd._mcd_read_method(                                 \
                            internal_partition::translate_column(              \
                                ci.col_id_ ) );                                \
                    _b_ret = std::get<0>( _read_data );                        \
                    _t_ret = std::get<1>( _read_data );                        \
                } else {                                                       \
                    _b_ret = true;                                             \
                    _reader._read_method( (_read_type*) &_t_ret );             \
                }                                                              \
            }                                                                  \
            _reader.close();                                                   \
        }                                                                      \
    }                                                                          \
    shared_partition_->unlock_storage();                                       \
    return std::make_tuple<>( _b_ret, _t_ret );

template <>
std::tuple<bool, uint64_t> col_partition::read_cell_from_disk(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    TEMPL_READ_CELL_FROM_DISK( ci, snapshot, read_i64, get_uint64_data,
                               uint64_t, uint64_t, 0 );
}
template <>
std::tuple<bool, int64_t> col_partition::read_cell_from_disk(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    TEMPL_READ_CELL_FROM_DISK( ci, snapshot, read_i64, get_int64_data,
int64_t, uint64_t, 0 );
}
template <>
std::tuple<bool, double> col_partition::read_cell_from_disk(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    TEMPL_READ_CELL_FROM_DISK( ci, snapshot, read_dbl, get_double_data, double,
                               double, 0 );
}
template <>
std::tuple<bool, std::string> col_partition::read_cell_from_disk(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    TEMPL_READ_CELL_FROM_DISK( ci, snapshot, read_str, get_string_data,
                               std::string, std::string, "" );
}

#define GENERIC_READ_DATA( _method, _read_method, _type, _c_type, _ci,       \
                           _default, _snapshot )                             \
    DVLOG( 40 ) << "Col read record:" << _ci;                                \
    _t_ret = _default;                                                       \
    _b_ret = false;                                                          \
    if( shared_partition_->get_storage_tier() ==                             \
        storage_tier_type::type::DISK ) {                                    \
        std::tuple<bool, _type> _read_disk =                                 \
            read_cell_from_disk<_type>( _ci, _snapshot );                    \
        _b_ret = std::get<0>( _read_disk );                                  \
        _t_ret = std::get<1>( _read_disk );                                  \
    } else {                                                                 \
        std::shared_ptr<packed_column_records> _pcr =                        \
            _read_method( _ci, _snapshot );                                  \
        if( _pcr ) {                                                         \
            if( records_->get_col_type() == cell_data_type::MULTI_COLUMN ) { \
                auto _mcr_ret = _pcr->get_multi_column_data( ci.key_ );      \
                _b_ret = std::get<0>( _mcr_ret );                            \
                if( _b_ret ) {                                               \
                    auto _a_ret =                                            \
                        std::get<1>( _mcr_ret )                              \
                            ._method( internal_partition::translate_column(  \
                                ci.col_id_ ) );                              \
                    _b_ret = std::get<0>( _a_ret );                          \
                    if( _b_ret ) {                                           \
                        _t_ret = std::get<1>( _a_ret );                      \
                    }                                                        \
                }                                                            \
            } else {                                                         \
                auto _a_ret = _pcr->_method( _ci.key_ );                     \
                _b_ret = std::get<0>( _a_ret );                              \
                if( _b_ret ) {                                               \
                    _t_ret = std::get<1>( _a_ret );                          \
                }                                                            \
            }                                                                \
        }                                                                    \
    }                                                                        \
    DVLOG( 40 ) << "Col read record:" << _ci << ", ret:" << _b_ret;

#define READ_DATA( _method, _type, _c_type, _ci, _default, _snapshot )       \
    _type _t_ret = _default;                                                 \
    bool  _b_ret = false;                                                    \
    bool  is_on_disk = shared_partition_->get_storage_tier() ==              \
                      storage_tier_type::type::DISK;                         \
    if( is_sorted_ ) {                                                       \
        if( is_on_disk ) {                                                   \
            start_timer( READ_SORTED_COL_FROM_DISK_TIMER_ID );               \
            GENERIC_READ_DATA( _method, read_cell_data, _type, _c_type, _ci, \
                               _default, _snapshot );                        \
            stop_timer( READ_SORTED_COL_FROM_DISK_TIMER_ID );                \
        } else {                                                             \
            start_timer( SORTED_COL_READ_RECORD_TIMER_ID );                  \
            GENERIC_READ_DATA( _method, read_cell_data, _type, _c_type, _ci, \
                               _default, _snapshot );                        \
            stop_timer( SORTED_COL_READ_RECORD_TIMER_ID );                   \
        }                                                                    \
    } else {                                                                 \
        if( is_on_disk ) {                                                   \
            start_timer( READ_COL_FROM_DISK_TIMER_ID );                      \
            GENERIC_READ_DATA( _method, read_cell_data, _type, _c_type, _ci, \
                               _default, _snapshot );                        \
            stop_timer( READ_COL_FROM_DISK_TIMER_ID );                       \
        } else {                                                             \
            start_timer( COL_READ_RECORD_TIMER_ID );                         \
            GENERIC_READ_DATA( _method, read_cell_data, _type, _c_type, _ci, \
                               _default, _snapshot );                        \
            stop_timer( COL_READ_RECORD_TIMER_ID );                          \
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
            start_timer( READ_SORTED_COL_FROM_DISK_TIMER_ID );                 \
            GENERIC_READ_DATA( _method, read_latest_cell_data, _type, _c_type, \
                               _ci, _default, _snapshot );                     \
            stop_timer( READ_SORTED_COL_FROM_DISK_TIMER_ID );                  \
        } else {                                                               \
            start_timer( SORTED_COL_READ_LATEST_RECORD_TIMER_ID );             \
            GENERIC_READ_DATA( _method, read_latest_cell_data, _type, _c_type, \
                               _ci, _default, _snapshot );                     \
            stop_timer( SORTED_COL_READ_LATEST_RECORD_TIMER_ID );              \
        }                                                                      \
    } else {                                                                   \
        if( is_on_disk ) {                                                     \
            start_timer( READ_COL_FROM_DISK_TIMER_ID );                        \
            GENERIC_READ_DATA( _method, read_latest_cell_data, _type, _c_type, \
                               _ci, _default, _snapshot );                     \
            stop_timer( READ_COL_FROM_DISK_TIMER_ID );                         \
        } else {                                                               \
            start_timer( COL_READ_LATEST_RECORD_TIMER_ID );                    \
            GENERIC_READ_DATA( _method, read_latest_cell_data, _type, _c_type, \
                               _ci, _default, _snapshot );                     \
            stop_timer( COL_READ_LATEST_RECORD_TIMER_ID );                     \
        }                                                                      \
    }                                                                          \
    return std::make_tuple<bool, _type>( std::forward<bool>( _b_ret ),         \
                                         std::forward<_type>( _t_ret ) );

// point lookups
std::tuple<bool, uint64_t> col_partition::get_uint64_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    READ_DATA( get_uint64_data, uint64_t, cell_data_type::UINT64, ci, 0,
               snapshot );
}
std::tuple<bool, int64_t> col_partition::get_int64_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    READ_DATA( get_int64_data, int64_t, cell_data_type::INT64, ci, 0,
               snapshot );
}
std::tuple<bool, double> col_partition::get_double_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    READ_DATA( get_double_data, double, cell_data_type::DOUBLE, ci, 0,
               snapshot );
}
std::tuple<bool, std::string> col_partition::get_string_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    READ_DATA( get_string_data, std::string, cell_data_type::STRING, ci, "",
               snapshot );
}

std::tuple<bool, uint64_t> col_partition::get_latest_uint64_data(
    const cell_identifier& ci ) const {
    READ_LATEST_DATA( get_uint64_data, uint64_t, cell_data_type::UINT64, ci,
                      0 );
}
std::tuple<bool, int64_t> col_partition::get_latest_int64_data(
    const cell_identifier& ci ) const {
    READ_LATEST_DATA( get_int64_data, int64_t, cell_data_type::INT64, ci, 0 );
}
std::tuple<bool, double> col_partition::get_latest_double_data(
    const cell_identifier& ci ) const {
    READ_LATEST_DATA( get_double_data, double, cell_data_type::DOUBLE, ci, 0 );
}
std::tuple<bool, std::string> col_partition::get_latest_string_data(
    const cell_identifier& ci ) const {
    READ_LATEST_DATA( get_string_data, std::string, cell_data_type::STRING, ci,
                      "" );
}

void col_partition::scan( uint64_t low_key, uint64_t high_key,
                          const predicate_chain&       predicate,
                          const std::vector<uint32_t>& project_cols,
                          const snapshot_vector&       snapshot,
                          std::vector<result_tuple>&   result_tuples ) const {
    if( is_sorted_ ) {
        start_timer( SORTED_COL_SCAN_RECORDS_TIMER_ID );
        internal_scan( low_key, high_key, predicate, project_cols, snapshot,
                       result_tuples );
        stop_timer( SORTED_COL_SCAN_RECORDS_TIMER_ID );
    } else {
        start_timer( COL_SCAN_RECORDS_TIMER_ID );
        internal_scan( low_key, high_key, predicate, project_cols, snapshot,
                       result_tuples );
        stop_timer( COL_SCAN_RECORDS_TIMER_ID );
    }
}

void col_partition::internal_scan(
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

    auto col = records_->get_snapshot_columns(
        get_snapshot_version( snapshot, metadata_.partition_id_ ) );

    uint64_t min_key =
        std::max( low_key, (uint64_t) metadata_.partition_id_.partition_start );
    uint64_t max_key =
        std::min( high_key, (uint64_t) metadata_.partition_id_.partition_end );

    col->scan( metadata_.partition_id_, min_key, max_key, translated_predicate,
               translated_cols_to_project, result_tuples );
}

std::shared_ptr<packed_column_records> col_partition::read_cell_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    internal_partition::check_cell_identifier_within_partition( ci );
    return records_->get_snapshot_columns(
        get_snapshot_version( snapshot, metadata_.partition_id_ ) );
}

std::shared_ptr<packed_column_records> col_partition::read_latest_cell_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    internal_partition::check_cell_identifier_within_partition( ci );
    return records_->get_current_columns();
}

void col_partition::add_to_write_buffer( const cell_identifier& ci,
                                         packed_cell_data*      pcd,
                                         uint32_t               op_code ) {
    shared_partition_->txn_execution_state_.add_to_write_buffer( ci, pcd,
                                                                 op_code );
}

std::tuple<std::shared_ptr<packed_column_records>,
           std::shared_ptr<packed_column_records>>
    split_packed_column_records(
        uint32_t col_split_point, uint64_t row_split_point,
        std::shared_ptr<packed_column_records> covering_data,
        const partition_column_identifier&     covering_pcid,
        const std::vector<cell_data_type>&     col_types,
        const partition_column_identifier&     low_pcid,
        const std::vector<cell_data_type>&     low_types,
        const partition_column_identifier&     high_pcid,
        const std::vector<cell_data_type>&     high_types ) {

    cell_data_type low_type = cell_data_type::MULTI_COLUMN;
    cell_data_type high_type = cell_data_type::MULTI_COLUMN;
    bool           is_sorted = covering_data->is_sorted();
    if( low_types.size() == 1 ) {
        low_type = low_types.at( 0 );
    }
    if( high_types.size() == 1 ) {
        high_type = high_types.at( 0 );
    }

    std::shared_ptr<packed_column_records> low_p =
        std::make_shared<packed_column_records>( is_sorted, low_type,
                                                 low_pcid.partition_start,
                                                 low_pcid.partition_end );
    std::shared_ptr<packed_column_records> high_p =
        std::make_shared<packed_column_records>( is_sorted, high_type,
                                                 high_pcid.partition_start,
                                                 high_pcid.partition_end );

    if( row_split_point != k_unassigned_key ) {
        covering_data->split_col_records_horizontally(
            low_p.get(), high_p.get(), row_split_point );
    } else if( col_split_point != k_unassigned_col ) {

        uint32_t translated_col =
            normalize_column_id( col_split_point, covering_pcid );
        covering_data->split_col_records_vertically(
            translated_col, col_types, low_p.get(),
            construct_multi_column_data_types( low_types ), high_p.get(),
            construct_multi_column_data_types( high_types ) );
    }

    return std::make_tuple<>( low_p, high_p );
}

std::shared_ptr<packed_column_records> merge_packed_column_records(
    uint32_t col_merge_point, uint64_t row_merge_point,
    const partition_column_identifier&     cover_pcid,
    const std::vector<cell_data_type>&     cover_types,
    std::shared_ptr<packed_column_records> low_data,
    const partition_column_identifier&     low_pcid,
    const std::vector<cell_data_type>&     low_types,
    std::shared_ptr<packed_column_records> high_data,
    const partition_column_identifier&     high_pcid,
    const std::vector<cell_data_type>&     high_types ) {

    cell_data_type type = cell_data_type::MULTI_COLUMN;
    if( cover_types.size() == 1 ) {
        type = cover_types.at( 0 );
    }

    bool is_sorted = low_data->is_sorted() and high_data->is_sorted();

    std::shared_ptr<packed_column_records> merged_p =
        std::make_shared<packed_column_records>( is_sorted, type,
                                                 cover_pcid.partition_start,
                                                 cover_pcid.partition_end );
    if( row_merge_point != k_unassigned_key ) {
        DVLOG( 40 ) << "Merge packed column records horizontally";
        merged_p->merge_col_records_horizontally( low_data.get(),
                                                  high_data.get() );
    } else if( col_merge_point != k_unassigned_col ) {
        DVLOG( 40 ) << "Merge packed column records vertically";
        merged_p->merge_col_records_vertically(
            construct_multi_column_data_types( cover_types ), low_data.get(),
            low_types, high_data.get(), high_types );
    }

    return merged_p;
}

void col_partition::merge_col_records( col_partition* low_p,
                                       col_partition* high_p,
                                       uint32_t       col_merge_point,
                                       uint64_t       row_merge_point,
                                       uint64_t       merge_version ) {
    DCHECK(
        ( low_p->get_partition_type() == partition_type::type::COLUMN ) or
        ( low_p->get_partition_type() ==
          partition_type::type::SORTED_COLUMN ) );
    DCHECK( ( high_p->get_partition_type() == partition_type::type::COLUMN ) or
            ( high_p->get_partition_type() ==
              partition_type::type::SORTED_COLUMN ) );

    auto merged_records = merge_packed_column_records(
        col_merge_point, row_merge_point, metadata_.partition_id_,
        shared_partition_->col_types_, low_p->records_->get_current_columns(),
        low_p->metadata_.partition_id_, low_p->shared_partition_->col_types_,
        high_p->records_->get_current_columns(),
        high_p->metadata_.partition_id_,
        high_p->shared_partition_->col_types_ );

    set_master_location( low_p->shared_partition_->get_master_location() );

    records_->set_current_records( merged_records, merge_version );
}

void col_partition::split_col_records(
    col_partition* low_p, col_partition* high_p, uint32_t col_split_point,
    uint64_t row_split_point, uint64_t low_version, uint64_t high_version ) {
    DCHECK(
        ( low_p->get_partition_type() == partition_type::type::COLUMN ) or
        ( low_p->get_partition_type() ==
          partition_type::type::SORTED_COLUMN ) );
    DCHECK( ( high_p->get_partition_type() == partition_type::type::COLUMN ) or
            ( high_p->get_partition_type() ==
              partition_type::type::SORTED_COLUMN ) );

    auto split_records = split_packed_column_records(
        col_split_point, row_split_point, records_->get_current_columns(),
        metadata_.partition_id_, shared_partition_->col_types_,
        low_p->metadata_.partition_id_, low_p->shared_partition_->col_types_,
        high_p->metadata_.partition_id_,
        high_p->shared_partition_->col_types_ );

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
            start_timer( SORTED_COL_REPARTITION_INSERT_TIMER_ID ); \
            _pcd = records_->_set_method(                          \
                _ci, (_type) std::get<1>( _get_ret ), _snapshot ); \
            stop_timer( SORTED_COL_REPARTITION_INSERT_TIMER_ID );  \
        } else {                                                   \
            start_timer( COL_REPARTITION_INSERT_TIMER_ID ); \
            _pcd = records_->_set_method(                          \
                _ci, (_type) std::get<1>( _get_ret ), _snapshot ); \
            stop_timer( COL_REPARTITION_INSERT_TIMER_ID ); \
        }                                                          \
        DCHECK( _pcd );                                            \
    }

void col_partition::repartition_cell_into_partition( const cell_identifier& ci,
                                                     internal_partition* src ) {
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

void col_partition::finalize_repartitions( uint64_t version ) {
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

bool col_partition::apply_propagated_cell_update(
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

void col_partition::snapshot_partition_data(
    snapshot_partition_column_state& snapshot ) {
    snapshot_partition_metadata( snapshot );
    DVLOG( 40 ) << "Snapshot partition data:" << metadata_.partition_id_
                << ", snapshot:" << snapshot.session_version_vector;
    auto records = records_->get_snapshot_columns( get_snapshot_version(
        snapshot.session_version_vector, metadata_.partition_id_ ) );
    if( records ) {
        DVLOG( 40 ) << "Snapshot data";
        records->snapshot_state( snapshot, shared_partition_->col_types_ );
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

void col_partition::install_snapshotted_column(
    const snapshot_column_state& column, const snapshot_vector& snapshot ) {
    DCHECK_EQ( column.keys.size(), column.data.size() );

    for( uint32_t pos = 0; pos < column.keys.size(); pos++ ) {
        bool insert_ok = install_snapshotted_column_pos(
            column.keys.at( pos ), column.data.at( pos ), column.col_id,
            column.type, snapshot );
        DCHECK( insert_ok );
    }
}
bool col_partition::install_snapshotted_column_pos(
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

void col_partition::change_type_from_sorted_column(
    const col_partition* sorted_col, uint64_t version ) {

    start_timer( CHANGE_TYPE_FROM_SORTED_COLUMN_TIMER_ID );

    cell_data_type type = cell_data_type::MULTI_COLUMN;
    if( shared_partition_->col_types_.size() == 1 ) {
        type = shared_partition_->col_types_.at( 0 );
    }

    DCHECK( !is_sorted_ );
    DCHECK( sorted_col->is_sorted_ );

    std::shared_ptr<packed_column_records> new_p =
        std::make_shared<packed_column_records>(
            is_sorted_, type, metadata_.partition_id_.partition_start,
            metadata_.partition_id_.partition_end );

    if( sorted_col->records_ ) {
        auto other = sorted_col->records_->get_current_columns();
        if( other ) {
            new_p->install_from_sorted_data( other.get() );
        }
    }

    records_->set_current_records( new_p, version );

    stop_timer( CHANGE_TYPE_FROM_SORTED_COLUMN_TIMER_ID );
}

void col_partition::finalize_change_type( uint64_t version) {
    finalize_repartitions( version );
    records_->commit_latest_updates( version );
}

void col_partition::persist_data( data_persister* part_persister ) const {
    if( records_ ) {
        auto cols_and_version = records_->get_current_columns_and_version();
        auto cols = std::get<0>( cols_and_version );
        if( cols ) {
            int32_t num_cols = ( metadata_.partition_id_.column_end -
                                 metadata_.partition_id_.column_start ) +
                               1;
            cols->persist_data( part_persister, metadata_.partition_id_hash_,
                                std::get<1>( cols_and_version ),
                                metadata_.partition_id_hash_, num_cols );
        }
    }
}

column_stats<multi_column_data> col_partition::persist_to_disk(
    data_persister* part_persister ) const {
    column_stats<multi_column_data> stats;

    int32_t col_position_pos = k_unassigned_col;

    std::shared_ptr<packed_column_records> rec = nullptr;

    if( records_ != nullptr ) {
        rec = records_->get_current_columns();
        if( rec != nullptr ) {
            col_position_pos =
                part_persister->get_position() + sizeof( uint32_t );
        }
    }

    part_persister->write_i32( col_position_pos );
    if( rec != nullptr ) {
        uint32_t begin_position = part_persister->get_position();
        DCHECK_EQ( begin_position, (uint32_t) col_position_pos );

        stats = rec->persist_to_disk( part_persister );
    }

    return stats;
}

void col_partition::restore_from_disk( data_reader* reader, uint64_t version ) {
    int32_t col_pos = k_unassigned_col;
    reader->read_i32( (uint32_t*) &col_pos );


    cell_data_type type = cell_data_type::MULTI_COLUMN;
    if( shared_partition_->col_types_.size() == 1 ) {
        type = shared_partition_->col_types_.at( 0 );
    }

    std::shared_ptr<packed_column_records> rec =
        std::make_shared<packed_column_records>(
            is_sorted_, type, metadata_.partition_id_.partition_start,
            metadata_.partition_id_.partition_end );

    if( col_pos != k_unassigned_col ) {
        DCHECK_EQ( col_pos, reader->get_position() );
        rec->restore_from_disk( reader );
    }

    records_->set_current_records( rec, version );
    records_->merge_updates();
}
