#include "row_partition.h"

#include "../../common/perf_tracking.h"
#include "../../common/string_conversion.h"
#include "predicate.h"

row_partition::row_partition()
    : internal_partition(), records_( nullptr ), inflight_updates_() {}

void row_partition::init(
    const partition_metadata&          metadata,
    const std::vector<cell_data_type>& col_types, uint64_t version,
    std::shared_ptr<shared_partition_state>& shared_partition,
    void*                                    parent_partition ) {
    internal_partition::init( metadata, col_types, version, shared_partition,
                              parent_partition );
    init_records();
}

partition_type::type row_partition::get_partition_type() const {
    return partition_type::type::ROW;
}

void row_partition::init_records() {
    std::shared_ptr<versioned_row_records> records =
        std::make_shared<versioned_row_records>( metadata_ );
    records->init_records();
    set_row_records( records );
}

void row_partition::set_row_records(
    std::shared_ptr<versioned_row_records> records ) {
    records_ = records;
}

std::shared_ptr<versioned_row_records> row_partition::get_row_records() const {
    auto records = records_;
    return records;
}

void row_partition::abort_write_transaction() {
    inflight_updates_.clear();
    internal_partition::abort_write_transaction();
}

void row_partition::finalize_commit( const snapshot_vector& dependency_snapshot,
                                     uint64_t version, uint64_t table_epoch,
                                     bool no_prop_commit ) {

    inflight_updates_.clear();
    internal_partition::finalize_commit( dependency_snapshot, version,
                                         table_epoch, no_prop_commit );
}

bool row_partition::begin_read_wait_and_build_snapshot(
    snapshot_vector& snapshot, bool no_apply_last ) {
    start_timer( ROW_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID );
    bool ret = internal_partition::begin_read_wait_and_build_snapshot(
        snapshot, no_apply_last );
    stop_timer( ROW_BEGIN_READ_WAIT_AND_BUILD_SNAPSHOT_TIMER_ID );
    return ret;
}

bool row_partition::begin_read_wait_for_version( snapshot_vector& snapshot,
                                                 bool no_apply_last ) {
    start_timer( ROW_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID );
    bool ret = internal_partition::begin_read_wait_for_version( snapshot,
                                                                no_apply_last );
    stop_timer( ROW_BEGIN_READ_WAIT_FOR_VERSION_TIMER_ID );
    return ret;
}
bool row_partition::begin_write( snapshot_vector& snapshot,
                                 bool             acquire_lock ) {
    start_timer( ROW_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID );
    bool ret = internal_partition::begin_write( snapshot, acquire_lock );
    stop_timer( ROW_PARTITION_BEGIN_WRITE_ACQUIRE_LOCKS_TIMER_ID );

    return ret;
}


// point deletion
bool row_partition::remove_data( const cell_identifier& ci,
                                 const snapshot_vector& snapshot ) {
    return internal_remove_data( ci, snapshot, true, false, 0 );
}
bool row_partition::internal_remove_data( const cell_identifier& ci,
                                          const snapshot_vector& snapshot,
                                          bool do_prop, bool set_version,
                                          uint64_t version ) {
    DVLOG( 40 ) << "Row remove record:" << ci;
    start_timer( ROW_WRITE_RECORD_TIMER_ID );
    bool              b_ret = false;
    auto found_pcd = write_cell_data( ci, snapshot, K_DELETE_OP, do_prop,
                                      set_version, version );
    packed_cell_data* pcd = std::get<1>( found_pcd );
    if( pcd ) {
        b_ret = true;
        pcd->set_as_deleted();
        remove_table_stats_data( ci, pcd->get_data_size() );
    }
    stop_timer( ROW_WRITE_RECORD_TIMER_ID );
    DVLOG( 40 ) << "Row remove record:" << ci << ", ret:" << b_ret;
    return b_ret;
}

#define UPDATE_TABLE_STATS( _ci, _prev_size, _new_size ) \
    update_table_stats_data( _ci, _prev_size, _new_size );

#define GENERIC_UPSERT_DATA( _method, _upsert_method, _type, _ci, _data,       \
                             _snapshot, _update_type, _do_prop,                \
                             _do_set_version, _version, _update_stats,         \
                             _new_size, _b_ret )                               \
    DVLOG( 40 ) << "Row " << _update_type << " record:" << _ci;                \
    _b_ret = false;                                                            \
    auto _up_ret =                                                             \
        _upsert_method( _ci, _snapshot, _do_prop, _do_set_version, _version ); \
    int32_t           _prev_size = std::get<0>( _up_ret );                     \
    packed_cell_data* _pcd = std::get<1>( _up_ret );                           \
    if( _pcd ) {                                                               \
        _b_ret = true;                                                         \
        _pcd->_method( _data );                                                \
        if( _update_stats ) {                                                  \
            UPDATE_TABLE_STATS( _ci, _prev_size, _new_size );                  \
        }                                                                      \
    }                                                                          \
    DVLOG( 40 ) << "Row " << _update_type << " record:" << _ci                 \
                << ", ret:" << _b_ret;

#define INSERT_DATA( _method, _type, _ci, _data, _data_size, _snapshot )       \
    start_timer( ROW_INSERT_RECORD_TIMER_ID );                                 \
    bool _b_ret = false;                                                       \
    GENERIC_UPSERT_DATA( _method, insert_cell_data, _type, _ci, _data,         \
                         _snapshot, "insert", true /* prop*/,                  \
                         false /*set version*/, 0 /*Version*/,                 \
                         true /* add stats*/, _data_size /* size */, _b_ret ); \
    stop_timer( ROW_INSERT_RECORD_TIMER_ID );                                  \
    return _b_ret;

#define UPDATE_DATA( _method, _type, _ci, _data, _data_size, _snapshot ) \
    start_timer( ROW_WRITE_RECORD_TIMER_ID );                            \
    bool _b_ret = false;                                                 \
    GENERIC_UPSERT_DATA( _method, update_cell_data, _type, _ci, _data,   \
                         _snapshot, "update", true /* prop*/,            \
                         false /*set version*/, 0 /*Version*/,           \
                         true /* update stats */, _data_size, _b_ret );  \
    stop_timer( ROW_WRITE_RECORD_TIMER_ID );                             \
    return _b_ret;

// point inserts
bool row_partition::insert_uint64_data( const cell_identifier& ci,
                                        uint64_t               data,
                                        const snapshot_vector& snapshot ) {
    INSERT_DATA( set_uint64_data, ( uint64_t ), ci, data, sizeof( uint64_t ),
                 snapshot );
    return false;
}
bool row_partition::insert_int64_data( const cell_identifier& ci, int64_t data,
                                       const snapshot_vector& snapshot ) {
    INSERT_DATA( set_int64_data, ( int64_t ), ci, data, sizeof( int64_t ),
                 snapshot );
    return false;
}
bool row_partition::insert_string_data( const cell_identifier& ci,
                                        const std::string&     data,
                                        const snapshot_vector& snapshot ) {
    INSERT_DATA( set_string_data, ( std::string ), ci, data, data.size(),
                 snapshot );
    return false;
}
bool row_partition::insert_double_data( const cell_identifier& ci, double data,
                                        const snapshot_vector& snapshot ) {
    INSERT_DATA( set_double_data, (double), ci, data, sizeof( double ),
                 snapshot );
    return false;
}

// point updates
bool row_partition::update_uint64_data( const cell_identifier& ci,
                                        uint64_t               data,
                                        const snapshot_vector& snapshot ) {
    UPDATE_DATA( set_uint64_data, ( uint64_t ), ci, data, sizeof( uint64_t ),
                 snapshot );
    return false;
}
bool row_partition::update_int64_data( const cell_identifier& ci, int64_t data,
                                       const snapshot_vector& snapshot ) {
    UPDATE_DATA( set_int64_data, ( int64_t ), ci, data, sizeof( int64_t ),
                 snapshot );
    return false;
}
bool row_partition::update_string_data( const cell_identifier& ci,
                                        const std::string&     data,
                                        const snapshot_vector& snapshot ) {
    UPDATE_DATA( set_string_data, ( std::string ), ci, data, data.size(),
                 snapshot );
    return false;
}
bool row_partition::update_double_data( const cell_identifier& ci, double data,
                                        const snapshot_vector& snapshot ) {
    UPDATE_DATA( set_double_data, (double), ci, data, sizeof( double ),
                 snapshot );
    return false;
}



#define GENERIC_READ_DATA( _method, _read_method, _type, _ci, _default, \
                           _snapshot, _b_ret, _t_ret )                  \
    DVLOG( 40 ) << "Row read record:" << _ci;                           \
    packed_cell_data* _pcd = nullptr;                                   \
    bool              _delete_pcd = false;                              \
    if( shared_partition_->get_storage_tier() ==                        \
        storage_tier_type::type::DISK ) {                               \
        _pcd = read_row_from_disk( _ci, _snapshot );                    \
        _delete_pcd = true;                                             \
    } else {                                                            \
        _pcd = _read_method( _ci, _snapshot );                          \
    }                                                                   \
    _b_ret = false;                                                     \
    _t_ret = _default;                                                  \
    if( ( _pcd ) and ( _pcd->is_present() ) ) {                         \
        _b_ret = true;                                                  \
        _t_ret = _pcd->_method();                                       \
    }                                                                   \
    if( _pcd and _delete_pcd ) {                                        \
        delete _pcd;                                                    \
    }                                                                   \
    DVLOG( 40 ) << "Row read record:" << _ci << ", ret:" << _b_ret      \
                << ", read:" << _t_ret;

#define READ_DATA( _method, _type, _ci, _default, _snapshot )             \
    bool  _b_ret = false;                                                 \
    _type _t_ret = _default;                                              \
    bool  is_on_disk = shared_partition_->get_storage_tier() ==           \
                      storage_tier_type::type::DISK;                      \
    if( is_on_disk ) {                                                    \
        start_timer( READ_ROW_FROM_DISK_TIMER_ID );                       \
        GENERIC_READ_DATA( _method, read_cell_data, _type, _ci, _default, \
                           _snapshot, _b_ret, _t_ret );                   \
        stop_timer( READ_ROW_FROM_DISK_TIMER_ID );                        \
    } else {                                                              \
        start_timer( ROW_READ_RECORD_TIMER_ID );                          \
        GENERIC_READ_DATA( _method, read_cell_data, _type, _ci, _default, \
                           _snapshot, _b_ret, _t_ret );                   \
        stop_timer( ROW_READ_RECORD_TIMER_ID );                           \
    }                                                                     \
    return std::make_tuple<bool, _type>( std::forward<bool>( _b_ret ),    \
                                         std::forward<_type>( _t_ret ) );

#define READ_LATEST_DATA( _method, _type, _ci, _default )                 \
    snapshot_vector _snapshot;                                            \
    bool            _b_ret = false;                                       \
    _type           _t_ret = _default;                                    \
    bool            is_on_disk = shared_partition_->get_storage_tier() == \
                      storage_tier_type::type::DISK;                      \
    if( is_on_disk ) {                                                    \
        start_timer( READ_ROW_FROM_DISK_TIMER_ID );                       \
        GENERIC_READ_DATA( _method, read_latest_cell_data, _type, _ci,    \
                           _default, _snapshot, _b_ret, _t_ret );         \
        stop_timer( READ_ROW_FROM_DISK_TIMER_ID );                        \
    } else {                                                              \
        start_timer( ROW_READ_LATEST_RECORD_TIMER_ID );                   \
        GENERIC_READ_DATA( _method, read_latest_cell_data, _type, _ci,    \
                           _default, _snapshot, _b_ret, _t_ret );         \
        stop_timer( ROW_READ_LATEST_RECORD_TIMER_ID );                    \
    }                                                                     \
    return std::make_tuple<bool, _type>( std::forward<bool>( _b_ret ),    \
                                         std::forward<_type>( _t_ret ) );

// point lookups
std::tuple<bool, uint64_t> row_partition::get_uint64_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    READ_DATA( get_uint64_data, uint64_t, ci, 0, snapshot );
}
std::tuple<bool, int64_t> row_partition::get_int64_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    READ_DATA( get_int64_data, int64_t, ci, 0, snapshot );
}
std::tuple<bool, double> row_partition::get_double_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    READ_DATA( get_double_data, double, ci, 0, snapshot );
}
std::tuple<bool, std::string> row_partition::get_string_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    READ_DATA( get_string_data, std::string, ci, "", snapshot );
}

std::tuple<bool, uint64_t> row_partition::get_latest_uint64_data(
    const cell_identifier& ci ) const {
    READ_LATEST_DATA( get_uint64_data, uint64_t, ci, 0 );
}
std::tuple<bool, int64_t> row_partition::get_latest_int64_data(
    const cell_identifier& ci ) const {
    READ_LATEST_DATA( get_int64_data, int64_t, ci, 0 );
}
std::tuple<bool, double> row_partition::get_latest_double_data(
    const cell_identifier& ci ) const {
    READ_LATEST_DATA( get_double_data, double, ci, 0 );
}
std::tuple<bool, std::string> row_partition::get_latest_string_data(
    const cell_identifier& ci ) const {
    READ_LATEST_DATA( get_string_data, std::string, ci, "" );
}

void row_partition::scan( uint64_t low_key, uint64_t high_key,
                          const predicate_chain&       predicate,
                          const std::vector<uint32_t>& project_cols,
                          const snapshot_vector&       snapshot,
                          std::vector<result_tuple>&   result_tuples ) const {

    start_timer( ROW_SCAN_RECORDS_TIMER_ID );

    std::vector<uint32_t> translated_cols_to_project =
        internal_partition::get_col_ids_to_scan( low_key, high_key,
                                                 project_cols );
    if( translated_cols_to_project.empty() ) {
        stop_timer( ROW_SCAN_RECORDS_TIMER_ID );
        return;
    }

    predicate_chain translated_predicate =
        translate_predicate_to_partition( predicate );

    uint64_t min_key =
        std::max( low_key, (uint64_t) metadata_.partition_id_.partition_start );
    uint64_t max_key =
        std::min( high_key, (uint64_t) metadata_.partition_id_.partition_end );

    for( uint64_t key = min_key; key <= max_key; key++ ) {
        const row_record* rec = get_row_record( key, snapshot );
        if( ( rec == nullptr ) or ( !rec->is_present() ) ) {
            continue;
        }

        DVLOG( 40 ) << "Evaluate predicate on:" << key;
        bool pred_eval =
            evaluate_predicate_on_row_record( rec, translated_predicate );
        DVLOG( 40 ) << "Evaluate predicate on:" << key << ", ret:" << pred_eval;
        if( pred_eval ) {
            result_tuples.emplace_back(
                generate_result_tuple( rec, translated_cols_to_project, key ) );
        }
    }
    stop_timer( ROW_SCAN_RECORDS_TIMER_ID );
}

result_tuple row_partition::generate_result_tuple(
    const row_record* row_rec, const std::vector<uint32_t>& cols_to_project,
    uint64_t row ) const {
    DCHECK( row_rec );
    DCHECK( row_rec->is_present() );

    DVLOG( 40 ) << "Adding key:" << row << ", to result tuple";

    result_tuple res;

    res.table_id = metadata_.partition_id_.table_id;
    res.row_id = row;

    packed_cell_data* pcd = row_rec->get_row_data();

    for( uint32_t col : cols_to_project ) {
        result_cell cell;
        cell.col_id = col + metadata_.partition_id_.column_start;
        cell.type = cell_data_type_to_data_type(
            shared_partition_->col_types_.at( col ) );

        cell.present = pcd[col].is_present();
        if( cell.present ) {
            cell.data = pcd[col].get_string_data();
        }

        res.cells.emplace_back( cell );
    }

    return res;
}

const row_record* row_partition::get_row_record(
    uint64_t key, const snapshot_vector& snapshot ) const {
    auto rec = records_->get_record( key );

    if( !rec ) {
        DVLOG( 40 ) << "Read row record:" << key << ", ret: nullptr";
        return nullptr;
    }
    DCHECK( rec );
    const row_record* row_rec = rec->read_record( key, snapshot );
    if( ( row_rec == nullptr ) or ( !row_rec->is_present() ) ) {
        return nullptr;
    }
    return row_rec;
}

std::tuple<int32_t, packed_cell_data*> row_partition::update_cell_data(
    const cell_identifier& ci, const snapshot_vector& snapshot, bool do_prop,
    bool set_version, uint64_t version ) {
    return write_cell_data( ci, snapshot, K_WRITE_OP, do_prop, set_version,
                            version );
}

std::tuple<int32_t, packed_cell_data*> row_partition::write_cell_data(
    const cell_identifier& ci, const snapshot_vector& snapshot_vector,
    uint32_t op_code, bool do_prop, bool set_version, uint64_t version ) {
    packed_cell_data* ret = nullptr;
    internal_partition::check_cell_identifier_within_partition( ci );

    row_record* row_rec = nullptr;
    int32_t prev_size = -1;

    uint32_t translated_column =
        internal_partition::translate_column( ci.col_id_ );

    auto        found = inflight_updates_.find( ci.key_ );
    if( found == inflight_updates_.end() ) {

        auto rec = records_->get_record( ci.key_ );
        if( !rec ) {
            return std::make_tuple<>( prev_size, ret );
        }
        DCHECK( rec );

        row_record* prev_rec = rec->read_latest_record( ci.key_ );
        row_rec = rec->write_record(
            ci.key_,
            shared_partition_->txn_execution_state_.get_transaction_state(),
            metadata_.num_records_in_chain_,
            shared_partition_->txn_execution_state_.get_low_watermark() );
        if( !row_rec ) {
            return std::make_tuple<>( prev_size, ret );
        }

        init_record( row_rec );
        if( prev_rec ) {
            if( prev_rec->is_present() and
                ( translated_column < prev_rec->get_num_columns() ) ) {
                auto prev_cell =
                    &( prev_rec->get_row_data()[translated_column] );
                prev_size = prev_cell->get_data_size();

                DVLOG( 40 ) << "Prev size is:" << prev_size
                            << ", data:" << *prev_cell;
            }
            row_rec->deep_copy( *prev_rec );
        }
        inflight_updates_[ci.key_] = row_rec;
        if( set_version ) {
            row_rec->set_version( version );
        }

    } else {
        row_rec = found->second;
    }

    DCHECK( row_rec );

    DCHECK_LT( translated_column, row_rec->get_num_columns() );
    ret = &( row_rec->get_row_data()[translated_column] );

    if( ret && do_prop ) {
        add_to_write_buffer( ci, row_rec, op_code );
    }

    return std::make_tuple<>( prev_size, ret );
}
std::tuple<int32_t, packed_cell_data*> row_partition::insert_cell_data(
    const cell_identifier& ci, const snapshot_vector& snapshot, bool do_prop,
    bool set_version, uint64_t version ) {
    packed_cell_data* ret = nullptr;
    internal_partition::check_cell_identifier_within_partition( ci );
    uint32_t prev_size = -1;

    row_record* row_rec = nullptr;
    auto        found = inflight_updates_.find( ci.key_ );
    if( found == inflight_updates_.end() ) {

        auto rec = records_->get_record( ci.key_ );
        if( !rec ) {
            return std::make_tuple<>( prev_size, ret );
        }
        DCHECK( rec );
        row_rec = rec->insert_record(
            ci.key_,
            shared_partition_->txn_execution_state_.get_transaction_state(),
            metadata_.partition_id_hash_, metadata_.num_records_in_chain_ );
        if( !row_rec ) {
            return std::make_tuple<>( prev_size, ret );
        }

        init_record( row_rec );
        inflight_updates_[ci.key_] = row_rec;

        if( set_version ) {
            row_rec->set_version( version );
        }

    } else {
        row_rec = found->second;
    }

    DCHECK( row_rec );
    uint32_t translated_column =
        internal_partition::translate_column( ci.col_id_ );

    DCHECK_LT( translated_column, row_rec->get_num_columns() );
    ret = &( row_rec->get_row_data()[translated_column] );

    if( ret && do_prop ) {
        add_to_write_buffer( ci, row_rec, K_INSERT_OP );
    }

    return std::make_tuple<>( prev_size, ret );
}

packed_cell_data* row_partition::read_cell_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {

    DVLOG( 40 ) << "Read cell data:" << ci;

    packed_cell_data* pcd = nullptr;
    internal_partition::check_cell_identifier_within_partition( ci );
    auto rec = records_->get_record( ci.key_ );
    if( !rec ) {
        DVLOG( 40 ) << "Read cell data:" << ci << ", ret:" << pcd;
        return pcd;
    }
    DCHECK( rec );
    const row_record* row_rec = rec->read_record( ci.key_, snapshot );
    return get_cell_data_from_row_record( ci, row_rec );
}
packed_cell_data* row_partition::read_row_from_disk(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {
    internal_partition::check_cell_identifier_within_partition( ci );

    shared_partition_->lock_storage();
    packed_cell_data* pcd = nullptr;

    if( shared_partition_->get_storage_tier() ==
        storage_tier_type::type::DISK ) {
        auto file_name = shared_partition_->get_disk_file_name();
        if( std::get<0>( file_name ) ) {
            data_reader reader( std::get<1>( file_name ) );
            reader.open();

            chomp_metadata( &reader );
            pcd = read_row_from_disk_file( &reader, ci );

            reader.close();
        }
    }

    shared_partition_->unlock_storage();

    return pcd;
}

packed_cell_data* row_partition::read_row_from_disk_file(
    data_reader* reader, const cell_identifier& ci ) const {

    DVLOG( 40 ) << "read_row_from_disk_file:" << ci;

    packed_cell_data* pcd = nullptr;

    uint64_t num_rows = ( metadata_.partition_id_.partition_end -
                          metadata_.partition_id_.partition_start ) +
                        1;

    uint64_t read_num_rows;
    reader->read_i64( &read_num_rows );
    DCHECK_EQ( read_num_rows, num_rows );

    uint32_t index_pos = ci.key_ - metadata_.partition_id_.partition_start;
    uint32_t index_seek_pos =
        ( index_pos * sizeof( uint32_t ) ) + reader->get_position();

    DVLOG( 40 ) << "read_row_from_disk_file:" << ci
                << ", index_seek_pos:" << index_seek_pos;

    reader->seek_to_position( index_seek_pos );

    int32_t read_index_pos;
    reader->read_i32( (uint32_t*) &read_index_pos );

    DVLOG( 40 ) << "read_row_from_disk_file:" << ci
                << ", read_index_pos:" << read_index_pos;

    if( read_index_pos == k_unassigned_col ) {
        return pcd;
    }
    reader->seek_to_position( read_index_pos );

    int64_t read_row_id;
    reader->read_i64( (uint64_t*) &read_row_id );
    DCHECK_EQ( read_row_id, ci.key_ );

    DVLOG( 40 ) << "read_row_from_disk_file:" << ci << ", read row record";

    row_record row_rec;
    row_rec.restore_from_disk( reader, shared_partition_->col_types_ );

    uint32_t translated_column =
        internal_partition::translate_column( ci.col_id_ );

    if( row_rec.is_present() and
        ( translated_column < row_rec.get_num_columns() ) ) {
        auto pcd_copy = &( row_rec.get_row_data()[translated_column] );
        pcd = new packed_cell_data();
        pcd->deep_copy( *pcd_copy );
    }

    return pcd;
}

packed_cell_data* row_partition::read_latest_cell_data(
    const cell_identifier& ci, const snapshot_vector& snapshot ) const {

    DVLOG( 40 ) << "Read cell data:" << ci;

    packed_cell_data* pcd = nullptr;
    internal_partition::check_cell_identifier_within_partition( ci );
    auto rec = records_->get_record( ci.key_ );
    if( !rec ) {
        DVLOG( 40 ) << "Read cell data:" << ci << ", ret:" << pcd;
        return pcd;
    }
    DCHECK( rec );
    const row_record* row_rec = rec->read_latest_record( ci.key_ );
    return get_cell_data_from_row_record( ci, row_rec );
}
packed_cell_data* row_partition::get_cell_data_from_row_record(
    const cell_identifier& ci, const row_record* row_rec ) const {
    packed_cell_data* pcd = nullptr;
    if( !row_rec ) {
        DVLOG( 40 ) << "Read cell data:" << ci << ", ret:" << pcd;
        return pcd;
    }

    uint32_t translated_column =
        internal_partition::translate_column( ci.col_id_ );

    DVLOG( 40 ) << "translated column:" << translated_column;
    DVLOG( 40 ) << "row rec is present:" << row_rec->is_present();
    DVLOG( 40 ) << "row rec num columns:" << row_rec->get_num_columns();

    if( ( !row_rec->is_present() ) or
        ( translated_column >= row_rec->get_num_columns() ) ) {
        DVLOG( 40 ) << "Read cell data:" << ci << ", ret:" << pcd;
        return pcd;
    }
    pcd = &( row_rec->get_row_data()[translated_column] );

    DVLOG( 40 ) << "Read cell data:" << ci << ", ret:" << pcd;

    return pcd;
}

void row_partition::init_record( row_record* rec ) {
    DCHECK( rec );
    uint32_t num_cols = ( metadata_.partition_id_.column_end -
                          metadata_.partition_id_.column_start ) +
                        1;
    rec->init_num_columns( num_cols, true /* do copy */ );
}

void row_partition::add_to_write_buffer( const cell_identifier& ci,
                                         row_record*            row_rec,
                                         uint32_t               op_code ) {
    shared_partition_->txn_execution_state_.add_to_write_buffer( ci, row_rec,
                                                                 op_code );
}

void row_partition::transfer_row_records_horizontally(
    row_partition* src, std::shared_ptr<versioned_row_records>& dest_records,
    const partition_metadata& metadata ) {
    // Must have locks, and src partition must no longer be in map
    // The reason is that, you are copying the versioned records over.
    // And there can only be a single writer to record at once. Hence if you
    // have both locks, you can gurantee nobody else is writing the record.
    // The src partition must not be in the map, because once it is
    // unlocked, if
    // it is in the map, then someone else could grab a lock, which defeats
    // this whole purpose.
    DVLOG( 30 ) << "Transfer records:" << metadata
                << ", from source:" << src->shared_partition_->get_metadata()
                << ", to dest:" << shared_partition_->get_metadata();
    uint64_t dest_part_id =
        shared_partition_->get_metadata().partition_id_hash_;

    auto src_records = src->get_row_records();

    for( int64_t key = metadata.partition_id_.partition_start;
         key <= metadata.partition_id_.partition_end; key++ ) {
        std::shared_ptr<versioned_row_record> record =
            src_records->get_record( (uint64_t) key );
        src->repartition_row_record( (uint64_t) key, dest_part_id );
        dest_records->set_record( (uint64_t) key, record );
    }
}

void row_partition::repartition_row_record( uint64_t key,
                                            uint64_t new_partition_id ) {
    DVLOG( 40 ) << "Repartition record:" << key;

    start_timer( REPARTITION_RECORD_TIMER_ID );

    auto rec = records_->get_record( key );
    DCHECK( rec );
    rec->repartition(
        shared_partition_->txn_execution_state_.get_repartitioned_records(),
        key, new_partition_id,
        shared_partition_->txn_execution_state_.get_transaction_state(),
        metadata_.num_records_in_chain_,
        shared_partition_->txn_execution_state_.get_low_watermark() );

    stop_timer( REPARTITION_RECORD_TIMER_ID );
}

void row_partition::merge_row_records_vertically(
    std::shared_ptr<versioned_row_records>& cover, uint64_t cover_v,
    uint32_t merge_num_columns, uint32_t merge_point,
    std::shared_ptr<versioned_row_records>& low,
    std::shared_ptr<versioned_row_records>& high ) {

    start_timer( MERGE_ROW_RECORDS_VERTICALLY_TIMER_ID );

    uint32_t translated_merge_point =
        internal_partition::translate_column( merge_point );
    for( int64_t key = metadata_.partition_id_.partition_start;
         key <= metadata_.partition_id_.partition_end; key++ ) {

        auto vr = cover->get_record( key );
        DCHECK( vr );

        auto low_vr = low->get_record( key );
        auto right_vr = high->get_record( key );
        DCHECK( low_vr );
        DCHECK( right_vr );

        auto low_r = low_vr->read_latest_record( key );
        auto right_r = right_vr->read_latest_record( key );

        row_record* r = vr->insert_record(
            key,
            shared_partition_->txn_execution_state_.get_transaction_state(),
            metadata_.partition_id_hash_, metadata_.num_records_in_chain_ );

        r->merge_vertically( low_r, right_r, merge_num_columns,
                             translated_merge_point );

        r->set_version( cover_v );
    }

    stop_timer( MERGE_ROW_RECORDS_VERTICALLY_TIMER_ID );
}
void row_partition::split_row_records_vertically(
    uint32_t split_col_point, std::shared_ptr<versioned_row_records>& cover,
    std::shared_ptr<versioned_row_records>& low, row_partition* low_p,
    uint64_t low_v, std::shared_ptr<versioned_row_records>& high,
    row_partition* high_p, uint64_t high_v ) {

    start_timer( SPLIT_ROW_RECORDS_VERTICALLY_TIMER_ID );

    for( int64_t key = metadata_.partition_id_.partition_start;
         key <= metadata_.partition_id_.partition_end; key++ ) {

        auto vr = cover->get_record( key );
        DCHECK( vr );

        auto low_vr = low->get_record( key );
        auto high_vr = high->get_record( key );

        DCHECK( low_vr );
        DCHECK( high_vr );

        auto rec = vr->read_latest_record( key );
        if( rec ) {
            row_record* low_r = low_vr->insert_record(
                key, low_p->shared_partition_->txn_execution_state_
                         .get_transaction_state(),
                low_p->metadata_.partition_id_hash_,
                low_p->metadata_.num_records_in_chain_ );
            row_record* high_r = high_vr->insert_record(
                key, high_p->shared_partition_->txn_execution_state_
                         .get_transaction_state(),
                high_p->metadata_.partition_id_hash_,
                high_p->metadata_.num_records_in_chain_ );

            rec->split_vertically( split_col_point, low_r, high_r );

            low_r->set_version( low_v );
            high_r->set_version( high_v );
        }
    }

    stop_timer( SPLIT_ROW_RECORDS_VERTICALLY_TIMER_ID );
}

void row_partition::merge_row_records( row_partition* low_p,
                                       row_partition* high_p,
                                       uint32_t       col_merge_point,
                                       uint64_t       row_merge_point,
                                       uint64_t       merge_version ) {
    DCHECK_EQ( partition_type::type::ROW, low_p->get_partition_type() );
    DCHECK_EQ( partition_type::type::ROW, high_p->get_partition_type() );

    auto cover_p_recs = std::make_shared<versioned_row_records>(
        shared_partition_->get_metadata() );
    cover_p_recs->init_records();

    if( col_merge_point != k_unassigned_col ) {
        DCHECK_EQ( row_merge_point, k_unassigned_key );
        merge_row_records_vertically( cover_p_recs, merge_version,
                                      shared_partition_->col_types_.size(), col_merge_point,
                                      low_p->records_, high_p->records_ );
    } else if( row_merge_point != k_unassigned_key ) {
        start_timer( MERGE_ROW_RECORDS_HORIZONTALLY_TIMER_ID );

        DCHECK_EQ( col_merge_point, k_unassigned_col );
        transfer_row_records_horizontally(
            low_p, cover_p_recs, low_p->shared_partition_->get_metadata() );
        transfer_row_records_horizontally(
            high_p, cover_p_recs, high_p->shared_partition_->get_metadata() );

        stop_timer( MERGE_ROW_RECORDS_HORIZONTALLY_TIMER_ID );
    }

    set_master_location( low_p->shared_partition_->get_master_location() );
    set_row_records( cover_p_recs );
}

void row_partition::split_row_records(
    row_partition* low_p, row_partition* high_p, uint32_t col_split_point,
    uint64_t row_split_point, uint64_t low_version, uint64_t high_version ) {
    DCHECK_EQ( partition_type::type::ROW, low_p->get_partition_type() );
    DCHECK_EQ( partition_type::type::ROW, high_p->get_partition_type() );

    auto low_p_recs = std::make_shared<versioned_row_records>(
        low_p->shared_partition_->get_metadata() );
    auto high_p_recs = std::make_shared<versioned_row_records>(
        high_p->shared_partition_->get_metadata() );

    low_p_recs->init_records();
    high_p_recs->init_records();

    if( col_split_point != k_unassigned_col ) {
        DCHECK_EQ( row_split_point, k_unassigned_key );

        split_row_records_vertically(
            internal_partition::translate_column( col_split_point ), records_,
            low_p_recs, low_p, low_version, high_p_recs, high_p, high_version );

    } else if( row_split_point != k_unassigned_key ) {
        start_timer( SPLIT_ROW_RECORDS_HORIZONTALLY_TIMER_ID );

        DCHECK_EQ( col_split_point, k_unassigned_col );

        low_p->transfer_row_records_horizontally(
            this, low_p_recs, low_p->shared_partition_->get_metadata() );
        high_p->transfer_row_records_horizontally(
            this, high_p_recs, high_p->shared_partition_->get_metadata() );

        stop_timer( SPLIT_ROW_RECORDS_HORIZONTALLY_TIMER_ID );
    }

    low_p->set_master_location(
        shared_partition_->get_master_location() );
    high_p->set_master_location( shared_partition_->get_master_location() );

    low_p->set_row_records( low_p_recs );
    high_p->set_row_records( high_p_recs );
}

#define REPARTITION( _ci, _get_method, _set_method, _type, _src )          \
    snapshot_vector _snapshot;                                             \
    std::tuple<bool, _type> _get_ret = _src->_get_method( _ci );           \
    if( std::get<0>( _get_ret ) ) {                                        \
        /* no propagation  */                                              \
        bool _b_ret = false;                                               \
        GENERIC_UPSERT_DATA( _set_method, insert_cell_data, _type, _ci,    \
                             std::get<1>( _get_ret ), _snapshot, "insert", \
                             false /* no prop */, false /*set version*/,   \
                             0 /*Version*/, false /* no stats */,          \
                             -1 /* don't care about size */, _b_ret );      \
        DCHECK( _b_ret );                                                  \
    }                                                                      \
    return true;

bool row_partition::internal_repartition_cell_into_partition(
    const cell_identifier& ci, internal_partition* src ) {
    // read and write into it
    switch( get_cell_type( ci ) ) {
        case cell_data_type::UINT64: {
            REPARTITION( ci, get_latest_uint64_data, set_uint64_data, uint64_t,
                         src );
            break;
        }
        case cell_data_type::INT64: {
            REPARTITION( ci, get_latest_int64_data, set_int64_data, int64_t,
                         src );
            break;
        }
        case cell_data_type::DOUBLE: {
            REPARTITION( ci, get_latest_double_data, set_double_data, double,
                         src );
            break;
        }
        case cell_data_type::STRING: {
            REPARTITION( ci, get_latest_string_data, set_string_data,
                         std::string, src );
            break;
        }
        case cell_data_type::MULTI_COLUMN: {
            break;
        }
    }
    return false;
}

void row_partition::repartition_cell_into_partition( const cell_identifier& ci,
                                                     internal_partition* src ) {
    internal_partition::check_cell_identifier_within_partition( ci );
    bool ret = internal_repartition_cell_into_partition( ci, src );
    DCHECK( ret );
}
void row_partition::finalize_repartitions( uint64_t version ) {
    for( auto& entry : inflight_updates_ ) {
        row_record* r = entry.second;
        r->set_version( version );
    }

    inflight_updates_.clear();
}

#define APPLY_PROP_UPDATE( _set_method, _update_method, _type,                 \
                           _convert_method, _du_cell, _snapshot, _name,        \
                           _version )                                          \
    _type _data = _convert_method( _du_cell.get_update_buffer(),               \
                                   _du_cell.get_update_buffer_length() );      \
    bool _b_ret = false;                                                       \
    GENERIC_UPSERT_DATA( _set_method, _update_method, _type, _du_cell.cid_,    \
                         _data, _snapshot, _name, false /* no prop*/,          \
                         true /*set version*/, _version, false /* no stats */, \
                         -1 /* don't care about stats */, _b_ret );            \
    return _b_ret;

#define APPLY_CELL_PROP_UPDATE( _update_method, _name, _du_cell, _snapshot,  \
                                _version )                                   \
    switch( get_cell_type( _du_cell.cid_ ) ) {                               \
        case cell_data_type::UINT64: {                                       \
            APPLY_PROP_UPDATE( set_uint64_data, _update_method, uint64_t,    \
                               chars_to_uint64, _du_cell, _snapshot, _name,  \
                               _version );                                   \
            break;                                                           \
        }                                                                    \
        case cell_data_type::INT64: {                                        \
            APPLY_PROP_UPDATE( set_int64_data, _update_method, int64_t,      \
                               chars_to_int64, _du_cell, _snapshot, _name,   \
                               _version );                                   \
            break;                                                           \
        }                                                                    \
        case cell_data_type::DOUBLE: {                                       \
            APPLY_PROP_UPDATE( set_double_data, _update_method, double,      \
                               chars_to_double, _du_cell, _snapshot, _name,  \
                               _version );                                   \
            break;                                                           \
        }                                                                    \
        case cell_data_type::STRING: {                                       \
            APPLY_PROP_UPDATE( set_string_data, _update_method, std::string, \
                               chars_to_string, _du_cell, _snapshot, _name,  \
                               _version );                                   \
            break;                                                           \
        }                                                                    \
        case cell_data_type::MULTI_COLUMN: {                                 \
            break;                                                           \
        }                                                                    \
    }

bool row_partition::apply_propagated_cell_update(
    const deserialized_cell_op& du_cell, const snapshot_vector& snapshot,
    bool do_insert, bool do_store, uint64_t version ) {

    DVLOG( 40 ) << "apply_propagated_cell_update: " << du_cell.cid_
                << ", do_insert:" << do_insert << ", do_store:" << do_store
                << ", version:" << version;

    if( !do_store ) {
        return internal_remove_data( du_cell.cid_, snapshot, false /* no prop*/,
                                     true /*set version*/, version );
    } else if( do_insert ) {
        APPLY_CELL_PROP_UPDATE( insert_cell_data, "propagated_insert", du_cell,
                                snapshot, version );
    } else {
        APPLY_CELL_PROP_UPDATE( update_cell_data, "propagated_update", du_cell,
                                snapshot, version );
    }
    return false;
}

void row_partition::snapshot_partition_data(
    snapshot_partition_column_state& snapshot ) {
    snapshot_partition_metadata( snapshot );

    for( int64_t row_id = metadata_.partition_id_.partition_start;
         row_id <= metadata_.partition_id_.partition_end; row_id++ ) {
        auto rec = records_->get_record( row_id );
        if( rec ) {
            const row_record* row_rec =
                rec->read_record( row_id, snapshot.session_version_vector );
            if( ( row_rec != nullptr ) and ( row_rec->is_present() ) ) {
                uint32_t num_cols = row_rec->get_num_columns();
                DCHECK_LE( num_cols, snapshot.columns.size() );
                packed_cell_data* pcd = row_rec->get_row_data();
                for( uint32_t col = 0; col < num_cols; col++ ) {
                    const packed_cell_data& p = pcd[col];
                    if( p.is_present() ) {
                        snapshot.columns.at( col ).data.push_back(
                            p.get_string_data() );
                        snapshot.columns.at( col ).keys.push_back( {row_id} );
                    }
                }
            }
        }
    }
}

#define TEMPL_INSTALL_SNAPSHOT( _keys, _data, _snapshot, _version, _cid,    \
                                _update_method, _type, _conv_method )       \
    _type _conv_data = _conv_method( _data );                               \
    for( uint64_t key : _keys ) {                                           \
        _cid.key_ = key;                                                    \
        bool _b_ret = false;                                                \
        GENERIC_UPSERT_DATA( _update_method, insert_cell_data, _type, _cid, \
                             _conv_data, _snapshot, "install_snapshot",     \
                             false /* no prop*/, true /*set version*/,      \
                             _version, false /* change stats*/,             \
                             -1 /* don't care about stats */, _b_ret );      \
        DCHECK( _b_ret );                                                   \
    }                                                                       \
    return true;

void row_partition::install_snapshotted_column(
    const snapshot_column_state& column, const snapshot_vector& snapshot ) {
    DCHECK_EQ( column.keys.size(), column.data.size() );

    uint64_t version =
        get_snapshot_version( snapshot, metadata_.partition_id_ );

    for( uint32_t pos = 0; pos < column.keys.size(); pos++ ) {
        bool insert_ok = install_snapshotted_column_pos(
            column.keys.at( pos ), column.data.at( pos ), column.col_id,
            column.type, snapshot, version );
        DCHECK( insert_ok );
    }
}
bool row_partition::install_snapshotted_column_pos(
    const std::vector<int64_t>& keys, const std::string& data,
    const int32_t& col_id, const data_type::type& d_type,
    const snapshot_vector& snapshot, uint64_t version ) {

    cell_identifier cid;
    cid.table_id_ = metadata_.partition_id_.table_id;
    cid.col_id_ = col_id;
    cid.key_ = 0;

    DVLOG( 40 ) << "Install_snapshotted column keys:" << keys
                << ", col_id:" << col_id
                << ", metadata_:" << metadata_.partition_id_;

    switch( d_type ) {
        case data_type::type::UINT64: {
            TEMPL_INSTALL_SNAPSHOT( keys, data, snapshot, version, cid,
                                    set_uint64_data, uint64_t,
                                    string_to_uint64 );
            break;
        }
        case data_type::type::INT64: {
            TEMPL_INSTALL_SNAPSHOT( keys, data, snapshot, version, cid,
                                    set_int64_data, int64_t, string_to_int64 );
            break;
        }
        case data_type::type::DOUBLE: {
            TEMPL_INSTALL_SNAPSHOT( keys, data, snapshot, version, cid,
                                    set_double_data, double, string_to_double );
            break;
        }
        case data_type::type::STRING: {
            TEMPL_INSTALL_SNAPSHOT( keys, data, snapshot, version, cid,
                                    set_string_data, std::string,
                                    string_to_string );
            break;
        }
    }
    return false;
}
void row_partition::finalize_change_type( uint64_t version ) {
    finalize_repartitions( version );
}

void row_partition::persist_data( data_persister* part_persister ) const {
    auto pid = metadata_.partition_id_;
    uint64_t pid_hash = metadata_.partition_id_hash_;

    auto part_recs = get_row_records();

    for( int64_t record_id = pid.partition_start;
         record_id <= pid.partition_end; record_id++ ) {
        DVLOG( 40 ) << "Persisting record:" << record_id;
        auto v_record = part_recs->get_record( (uint64_t) record_id );
        if( v_record == nullptr ) {
            continue;
        }
        auto record_tuple =
            v_record->read_latest_record_and_partition_column_information(
                record_id );
        row_record*  rec = std::get<0>( record_tuple );
        uint64_t record_pid = std::get<1>( record_tuple );

        if( rec == nullptr ) {
            continue;
        }

        part_persister->write_i64( record_id );
        part_persister->write_i64( record_pid );
        part_persister->write_i64( rec->get_version() );
        part_persister->write_i64( pid_hash );

        packed_cell_data* pcds = rec->get_row_data();
        for( int32_t i = pid.column_start; i <= pid.column_end; i++ ) {
            int32_t col = i - pid.column_start;

            bool has_data = false;

            if( pcds ) {
                auto& p_record = pcds[col];

                if( p_record.is_present() ) {
                    part_persister->write_i8( 1 );
                    part_persister->write_chars( p_record.get_buffer_data(),
                                                 p_record.get_length() );

                    has_data = true;
                }
            }
            if( !has_data ) {
                part_persister->write_i8( 0 );
            }
        }

        DVLOG( 40 ) << "Persisting record:" << record_id << ", okay!";
    }
}

column_stats<multi_column_data> row_partition::persist_to_disk(
    data_persister* part_persister ) const {

    int32_t  pos = k_unassigned_col;
    uint64_t num_rows = ( metadata_.partition_id_.partition_end -
                          metadata_.partition_id_.partition_start ) +
                        1;

    DVLOG( 40 ) << "Persisting num rows and indices:" << num_rows;
    part_persister->write_i64( num_rows );


    // first store the row indices
    uint32_t index_pos = part_persister->get_position();
    for( uint64_t row = 0; row < num_rows; row++ ) {
        part_persister->write_i32( pos );
    }

    column_stats<multi_column_data> stats;

    auto recs = get_row_records();
    if( recs == nullptr ) {
        return stats;
    }

    std::vector<int32_t> index_positions;


    auto mcd_types =
        construct_multi_column_data_types( shared_partition_->col_types_ );
    multi_column_data default_stats( mcd_types );

    stats.count_ = 0;
    stats.min_.deep_copy( default_stats );
    stats.max_.deep_copy( default_stats );
    stats.average_.deep_copy( default_stats );
    stats.sum_.deep_copy( default_stats );

    DVLOG( 40 ) << "Persisting row data";
    // then store the actual data
    for( int64_t row = metadata_.partition_id_.partition_start;
         row <= metadata_.partition_id_.partition_end; row++ ) {
        row_record* row_rec = nullptr;
        int32_t     rec_pos = k_unassigned_col;
        auto        rec = recs->get_record( row );
        if( rec ) {
            DVLOG( 40 ) << "Read latest record:" << row;
            row_rec = rec->read_latest_record( row );
            if( row_rec ) {
                rec_pos = (int32_t) part_persister->get_position();
            }
        }
        index_positions.emplace_back( rec_pos );

        if( row_rec ) {
            DVLOG( 40 ) << "Write i64:" << row;
            part_persister->write_i64( row );
            row_rec->persist_to_disk( part_persister,
                                      shared_partition_->col_types_ );
            update_stats_for_persistence( row_rec, stats );
        }
    }

    part_persister->seek_to_position( index_pos );

    DCHECK_EQ( index_positions.size(), num_rows );

    DVLOG( 40 ) << "Updating index data";
    for( int32_t index : index_positions ) {
        part_persister->write_i32( index );
    }

    return stats;
}

#define TEMPL_BUILD_MCD_AND_UPDATE_STATS( _col, _pcd, _mcd, _stats,         \
                                          _get_method, _set_method, _type ) \
    auto& _pcd_data = _pcd[col];                                            \
    if( _pcd_data.is_present() ) {                                          \
        _type _data = _pcd_data._get_method();                              \
        if( _stats.count_ == 0 ) {                                           \
            _stats.min_._set_method( _col, _data );                         \
            _stats.max_._set_method( _col, _data );                         \
        } else {                                                            \
            std::tuple<bool, _type> _min_data =                             \
                _stats.min_._get_method( _col );                            \
            std::tuple<bool, _type> _max_data =                             \
                _stats.max_._get_method( _col );                            \
            if( !std::get<0>( _min_data ) ) {                               \
                _stats.min_._set_method( _col, _data );                     \
            } else if( std::get<1>( _min_data ) > _data ) {                 \
                _stats.min_._set_method( _col, _data );                     \
            }                                                               \
            if( !std::get<0>( _max_data ) ) {                               \
                _stats.max_._set_method( _col, _data );                     \
            } else if( std::get<1>( _max_data ) < _data ) {                 \
                _stats.max_._set_method( _col, _data );                     \
            }                                                               \
        }                                                                   \
    }

void row_partition::update_stats_for_persistence(
    row_record* rec, column_stats<multi_column_data>& stats ) const {
    if( !rec ) {
        return;
    }
    if( !rec->is_present() ) {
        return;
    }
    uint32_t num_columns = rec->get_num_columns();
    DCHECK_LE( num_columns, shared_partition_->col_types_.size() );

    multi_column_data mcd(
        construct_multi_column_data_types( shared_partition_->col_types_ ) );

    packed_cell_data* pcd = rec->get_row_data();
    for( uint32_t col = 0; col < num_columns; col++ ) {
        switch( shared_partition_->col_types_.at( col ) ) {
            case cell_data_type::UINT64: {
                TEMPL_BUILD_MCD_AND_UPDATE_STATS( col, pcd, mcd, stats,
                                                  get_uint64_data,
                                                  set_uint64_data, uint64_t );
                break;
            }
            case cell_data_type::INT64: {
                TEMPL_BUILD_MCD_AND_UPDATE_STATS( col, pcd, mcd, stats,
                                                  get_int64_data,
                                                  set_int64_data, int64_t );
                break;
            }
            case cell_data_type::DOUBLE: {
                TEMPL_BUILD_MCD_AND_UPDATE_STATS( col, pcd, mcd, stats,
                                                  get_double_data,
                                                  set_double_data, double );
                break;
            }
            case cell_data_type::STRING: {
                TEMPL_BUILD_MCD_AND_UPDATE_STATS(
                    col, pcd, mcd, stats, get_string_data, set_string_data,
                    std::string );
                break;
            }
            case cell_data_type::MULTI_COLUMN: {
                break;
            }
        }
    }

    if( stats.count_ == 0 ) {
        stats.count_ = 1;
        stats.average_.deep_copy( mcd );
        stats.sum_.deep_copy( mcd );
    } else {
        auto updated_stats = update_average_and_total<multi_column_data>(
            stats.sum_, stats.count_, stats.count_ + 1, mcd,
            1 /* multiplier*/ );
        stats.average_ = std::get<0>( updated_stats );
        stats.sum_ = std::get<1>( updated_stats );
        stats.count_ = stats.count_ + 1;
    }
}

void row_partition::restore_from_disk( data_reader* reader, uint64_t version ) {
    uint64_t num_rows = ( metadata_.partition_id_.partition_end -
                          metadata_.partition_id_.partition_start ) +
                        1;

    uint64_t read_num_rows;
    reader->read_i64( &read_num_rows );
    DCHECK_EQ( read_num_rows, num_rows );

    // first store the row indices
    std::vector<int32_t> index_pos;
    for( uint64_t row = 0; row < num_rows; row++ ) {
        int32_t read_index_pos;
        reader->read_i32( (uint32_t*) &read_index_pos );
        index_pos.emplace_back( read_index_pos );
    }

    DCHECK_EQ( index_pos.size(), read_num_rows );

    auto row_recs = std::make_shared<versioned_row_records>( metadata_ );
    row_recs->init_records();

    for( int64_t row = 0; row < (int64_t) index_pos.size(); row++ ) {
        int64_t real_row = metadata_.partition_id_.partition_start + row;
        auto    versioned_rec = row_recs->get_record( real_row );
        if( index_pos.at( row ) != k_unassigned_col ) {
            auto row_rec = versioned_rec->insert_record(
                real_row,
                shared_partition_->txn_execution_state_.get_transaction_state(),
                metadata_.partition_id_hash_, metadata_.num_records_in_chain_ );
            DCHECK( row_rec );
            row_rec->set_version( version );


            int64_t read_row;
            reader->read_i64( (uint64_t*) &read_row );
            DCHECK_EQ( read_row, real_row );

            row_rec->restore_from_disk( reader, shared_partition_->col_types_ );
        }
    }

    set_row_records( row_recs );
}
