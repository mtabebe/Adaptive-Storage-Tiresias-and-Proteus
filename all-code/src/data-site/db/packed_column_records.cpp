#include "packed_column_records.h"

#include <glog/logging.h>

#include "../../common/perf_tracking.h"
#include "../../common/string_conversion.h"

#define CALL_INTERNAL_TEMPLATE( _method, _args... )                        \
    switch( col_type_ ) {                                                  \
        case cell_data_type::UINT64:                                       \
            return _method<uint64_t>(                                      \
                _args, *(packed_column_data<uint64_t>*) packed_data_ );    \
        case cell_data_type::INT64:                                        \
            return _method<int64_t>(                                       \
                _args, *(packed_column_data<int64_t>*) packed_data_ );     \
        case cell_data_type::DOUBLE:                                        \
            return _method<double>(                                        \
                _args, *(packed_column_data<double>*) packed_data_ );      \
        case cell_data_type::STRING:                                       \
            return _method<std::string>(                                   \
                _args, *(packed_column_data<std::string>*) packed_data_ ); \
        case cell_data_type::MULTI_COLUMN:                                 \
            return _method<multi_column_data>(                             \
                _args,                                                     \
                *(packed_column_data<multi_column_data>*) packed_data_ );  \
    }

#define CALL_INTERNAL_TEMPLATE_NO_ARGS( _method )                         \
    switch( col_type_ ) {                                                 \
        case cell_data_type::UINT64:                                      \
            return _method<uint64_t>(                                     \
                *(packed_column_data<uint64_t>*) packed_data_ );          \
        case cell_data_type::INT64:                                       \
            return _method<int64_t>(                                      \
                *(packed_column_data<int64_t>*) packed_data_ );           \
        case cell_data_type::DOUBLE:                                       \
            return _method<double>(                                       \
                *(packed_column_data<double>*) packed_data_ );            \
        case cell_data_type::STRING:                                      \
            return _method<std::string>(                                  \
                *(packed_column_data<std::string>*) packed_data_ );       \
        case cell_data_type::MULTI_COLUMN:                                \
            return _method<multi_column_data>(                            \
                *(packed_column_data<multi_column_data>*) packed_data_ ); \
    }

packed_column_records::packed_column_records( bool is_column_sorted,
                                              const cell_data_type& col_type,
                                              uint64_t              key_start,
                                              uint64_t              key_end )
    : is_column_sorted_( is_column_sorted ),
      keys_(),
      index_positions_(),
      key_start_( key_start ),
      key_end_( key_end ),
      data_counts_(),
      col_type_( col_type ),
      packed_data_( nullptr ) {
    DCHECK_GE( key_end, key_start );
    index_positions_.assign( ( key_end - key_start ) + 1, -1 );

    switch( col_type_ ) {
        case cell_data_type::UINT64:
            packed_data_ = (void*) new packed_column_data<uint64_t>();
            break;
        case cell_data_type::INT64:
            packed_data_ = (void*) new packed_column_data<int64_t>();
            break;
        case cell_data_type::DOUBLE:
            packed_data_ = (void*) new packed_column_data<double>();
            break;
        case cell_data_type::STRING:
            packed_data_ = (void*) new packed_column_data<std::string>();
            break;
        case cell_data_type::MULTI_COLUMN:
            packed_data_ = (void*) new packed_column_data<multi_column_data>();
            break;
    }
}

packed_column_records::~packed_column_records() {
    switch( col_type_ ) {
        case cell_data_type::UINT64:
            delete( (packed_column_data<uint64_t>*) packed_data_ );
            break;
        case cell_data_type::INT64:
            delete( (packed_column_data<int64_t>*) packed_data_ );
            break;
        case cell_data_type::DOUBLE:
            delete( (packed_column_data<double>*) packed_data_ );
            break;
        case cell_data_type::STRING:
            delete( (packed_column_data<std::string>*) packed_data_ );
            break;
        case cell_data_type::MULTI_COLUMN:
            delete( (packed_column_data<multi_column_data>*) packed_data_ );
            break;
    }
}

void packed_column_records::deep_copy( const packed_column_records& other ) {
    is_column_sorted_ = other.is_column_sorted_;
    index_positions_ = other.index_positions_;
    keys_ = other.keys_;
    key_start_ = other.key_start_;
    key_end_ = other.key_end_;

    data_counts_ = other.data_counts_;

    col_type_ = other.col_type_;

    switch( col_type_ ) {
        case cell_data_type::UINT64:
            packed_data_ = (void*) new packed_column_data<uint64_t>();
            templ_deep_copy(
                *(packed_column_data<uint64_t>*) packed_data_,
                *(packed_column_data<uint64_t>*) other.packed_data_ );
            break;
        case cell_data_type::INT64:
            packed_data_ = (void*) new packed_column_data<int64_t>();
            templ_deep_copy(
                *(packed_column_data<int64_t>*) packed_data_,
                *(packed_column_data<int64_t>*) other.packed_data_ );
            break;
        case cell_data_type::DOUBLE:
            packed_data_ = (void*) new packed_column_data<double>();
            templ_deep_copy(
                *(packed_column_data<double>*) packed_data_,
                *(packed_column_data<double>*) other.packed_data_ );
            break;
        case cell_data_type::STRING:
            packed_data_ = (void*) new packed_column_data<std::string>();
            templ_deep_copy(
                *(packed_column_data<std::string>*) packed_data_,
                *(packed_column_data<std::string>*) other.packed_data_ );
            break;
        case cell_data_type::MULTI_COLUMN:
            packed_data_ = (void*) new packed_column_data<multi_column_data>();
            templ_deep_copy(
                *(packed_column_data<multi_column_data>*) packed_data_,
                *(packed_column_data<multi_column_data>*) other.packed_data_ );
            break;
    }
}

int32_t packed_column_records::get_index_position( uint64_t key ) const {
    DCHECK_GE( key, key_start_ );
    DCHECK_LE( key, key_end_ );

    int pos = key - key_start_;
    return pos;
}

void packed_column_records::update_statistics() const {
    CALL_INTERNAL_TEMPLATE_NO_ARGS( templ_update_statistics );
}

bool packed_column_records::remove_data( uint64_t key,
                                         bool     do_statistics_maintenance ) {
    CALL_INTERNAL_TEMPLATE( templ_remove_data, key, do_statistics_maintenance );
    return false;
}

bool packed_column_records::insert_data( uint64_t key, uint64_t data,
                                         bool do_statistics_maintenance ) {
    DCHECK_EQ( col_type_, cell_data_type::UINT64 );
    return templ_insert_data<uint64_t>(
        key, data, do_statistics_maintenance,
        *(packed_column_data<uint64_t>*) packed_data_ );
}
bool packed_column_records::insert_data( uint64_t key, int64_t data,
                                         bool do_statistics_maintenance ) {
    DCHECK_EQ( col_type_, cell_data_type::INT64 );
    return templ_insert_data<int64_t>(
        key, data, do_statistics_maintenance,
        *(packed_column_data<int64_t>*) packed_data_ );
}

bool packed_column_records::insert_data( uint64_t key, const std::string& data,
                                         bool do_statistics_maintenance ) {
    DCHECK_EQ( col_type_, cell_data_type::STRING );
    return templ_insert_data<std::string>(
        key, data, do_statistics_maintenance,
        *(packed_column_data<std::string>*) packed_data_ );
}
bool packed_column_records::insert_data( uint64_t key, double data,
                                         bool do_statistics_maintenance ) {
    DCHECK_EQ( col_type_, cell_data_type::DOUBLE );
    return templ_insert_data<double>(
        key, data, do_statistics_maintenance,
        *(packed_column_data<double>*) packed_data_ );
}
bool packed_column_records::insert_data( uint64_t                 key,
                                         const multi_column_data& data,
                                         bool do_statistics_maintenance ) {
    DCHECK_EQ( col_type_, cell_data_type::MULTI_COLUMN );
    return templ_insert_data<multi_column_data>(
        key, data, do_statistics_maintenance,
        *(packed_column_data<multi_column_data>*) packed_data_ );
}


bool packed_column_records::update_data( uint64_t key, uint64_t data,
                                         bool do_statistics_maintenance ) {
    DCHECK_EQ( col_type_, cell_data_type::UINT64 );
    return templ_update_data<uint64_t>(
        key, data, do_statistics_maintenance,
        *(packed_column_data<uint64_t>*) packed_data_ );
}
bool packed_column_records::update_data( uint64_t key, int64_t data,
                                         bool do_statistics_maintenance ) {
    DCHECK_EQ( col_type_, cell_data_type::INT64 );
    return templ_update_data<int64_t>(
        key, data, do_statistics_maintenance,
        *(packed_column_data<int64_t>*) packed_data_ );
}

bool packed_column_records::update_data( uint64_t key, const std::string& data,
                                         bool do_statistics_maintenance ) {
    DCHECK_EQ( col_type_, cell_data_type::STRING );
    return templ_update_data<std::string>(
        key, data, do_statistics_maintenance,
        *(packed_column_data<std::string>*) packed_data_ );
}
bool packed_column_records::update_data( uint64_t key, double data,
                                         bool do_statistics_maintenance ) {
    DCHECK_EQ( col_type_, cell_data_type::DOUBLE );
    return templ_update_data<double>(
        key, data, do_statistics_maintenance,
        *(packed_column_data<double>*) packed_data_ );
}
bool packed_column_records::update_data( uint64_t key, const multi_column_data& data,
                                         bool do_statistics_maintenance ) {
    DCHECK_EQ( col_type_, cell_data_type::MULTI_COLUMN );
    return templ_update_data<multi_column_data>(
        key, data, do_statistics_maintenance,
        *(packed_column_data<multi_column_data>*) packed_data_ );
}


std::tuple<bool, uint64_t> packed_column_records::get_uint64_data(
    uint64_t key ) const {
    DCHECK_EQ( col_type_, cell_data_type::UINT64 );
    return templ_get_data<uint64_t>(
        key, *(packed_column_data<uint64_t>*) packed_data_ );
}
std::tuple<bool, int64_t> packed_column_records::get_int64_data(
    uint64_t key ) const {
    DCHECK_EQ( col_type_, cell_data_type::INT64 );
    return templ_get_data<int64_t>(
        key, *(packed_column_data<int64_t>*) packed_data_ );
}
std::tuple<bool, double> packed_column_records::get_double_data(
    uint64_t key ) const {
    DCHECK_EQ( col_type_, cell_data_type::DOUBLE );
    return templ_get_data<double>(
        key, *(packed_column_data<double>*) packed_data_ );
}
std::tuple<bool, std::string> packed_column_records::get_string_data(
    uint64_t key ) const {
    DCHECK_EQ( col_type_, cell_data_type::STRING );
    return templ_get_data<std::string>(
        key, *(packed_column_data<std::string>*) packed_data_ );
}
std::tuple<bool, multi_column_data>
    packed_column_records::get_multi_column_data( uint64_t key ) const {
    DCHECK_EQ( col_type_, cell_data_type::MULTI_COLUMN );
    return templ_get_data<multi_column_data>(
        key, *(packed_column_data<multi_column_data>*) packed_data_ );
}

void packed_column_records::scan(
    const partition_column_identifier& pid, uint64_t low_key, uint64_t high_key,
    const predicate_chain& predicate, const std::vector<uint32_t>& project_cols,
    std::vector<result_tuple>& result_tuples ) const {
    CALL_INTERNAL_TEMPLATE( templ_scan, pid, low_key, high_key, predicate,
                            project_cols, result_tuples );
}

bool packed_column_records::is_sorted() const { return is_column_sorted_; }

void packed_column_records::persist_data( data_persister* part_persister,
                                          uint64_t record_pid, uint64_t version,
                                          uint64_t pid_hash,
                                          uint32_t num_cols ) const {

    for( uint32_t pos = 0; pos < keys_.size(); pos++ ) {
        for( uint64_t key : keys_.at( pos ) ) {
            part_persister->write_i64( key );
            part_persister->write_i64( record_pid );
            part_persister->write_i64( version );
            part_persister->write_i64( pid_hash );

            persist_cell_data( part_persister, pos, num_cols );
        }
    }
}

#define TEMPL_PERSIST_CELL_DATA( _part_persister, _pos, _num_cols, \
                                 _conv_method, _type )             \
    DCHECK_NE( col_type_, cell_data_type::MULTI_COLUMN );          \
    DCHECK_EQ( _num_cols, 1 );                                     \
    packed_column_data<_type>* _pc_data =                          \
        (packed_column_data<_type>*) packed_data_;                 \
    DCHECK_LT( _pos, _pc_data->data_.size() );                     \
    _part_persister->write_i8( 1 );                                \
    _part_persister->write_str( _conv_method( _pc_data->data_.at( _pos ) ) );

#define TEMPL_PERSIST_MCD_CELL_DATA( _part_persister, _col, _mcd, _get_method, \
                                     _conv_method, _type )                     \
    std::tuple<bool, _type> _read_data = _mcd._get_method( _col );             \
    if( std::get<0>( _read_data ) ) {                                          \
        _part_persister->write_i8( 1 );                                        \
        _part_persister->write_str(                                            \
            _conv_method( std::get<1>( _read_data ) ) );                       \
    } else {                                                                   \
        _part_persister->write_i8( 0 );                                        \
    }

void persist_multi_column_cell_data(
    data_persister*                              part_persister,
    const packed_column_data<multi_column_data>* packed_data, uint32_t pos,
    uint32_t num_cols ) {
    DCHECK_LT( pos, packed_data->data_.size() );
    const multi_column_data& mcd = packed_data->data_.at( pos );
    uint32_t                 mcd_cols = mcd.get_num_columns();

    for( uint32_t col = 0; col < num_cols; col++ ) {
        if( col < mcd_cols ) {
            auto mcd_type = mcd.get_column_type( col );
            switch( mcd_type.type_ ) {
                case cell_data_type::UINT64: {
                    TEMPL_PERSIST_MCD_CELL_DATA( part_persister, col, mcd,
                                                 get_uint64_data,
                                                 uint64_to_string, uint64_t );
                    break;
                }
                case cell_data_type::INT64: {
                    TEMPL_PERSIST_MCD_CELL_DATA( part_persister, col, mcd,
                                                 get_int64_data,
                                                 int64_to_string, int64_t );
                    break;
                }
                case cell_data_type::DOUBLE: {
                    TEMPL_PERSIST_MCD_CELL_DATA( part_persister, col, mcd,
                                                 get_double_data,
                                                 double_to_string, double );
                    break;
                }
                case cell_data_type::STRING: {
                    TEMPL_PERSIST_MCD_CELL_DATA(
                        part_persister, col, mcd, get_string_data,
                        string_to_string, std::string );
                    break;
                }
                case cell_data_type::MULTI_COLUMN: {
                    break;
                }
            }
        } else {
            part_persister->write_i8( 0 );
        }
    }
}

void packed_column_records::persist_cell_data( data_persister* part_persister,
                                               uint32_t        pos,
                                               uint32_t num_cols ) const {
    switch( col_type_ ) {
        case cell_data_type::UINT64: {
            TEMPL_PERSIST_CELL_DATA( part_persister, pos, num_cols,
                                     uint64_to_string, uint64_t );
            break;
        }
        case cell_data_type::INT64: {
            TEMPL_PERSIST_CELL_DATA( part_persister, pos, num_cols,
                                     int64_to_string, int64_t );
            break;
        }
        case cell_data_type::DOUBLE: {
            TEMPL_PERSIST_CELL_DATA( part_persister, pos, num_cols,
                                     double_to_string, double );
            break;
        }
        case cell_data_type::STRING: {
            TEMPL_PERSIST_CELL_DATA( part_persister, pos, num_cols,
                                     string_to_string, std::string );
            break;
        }
        case cell_data_type::MULTI_COLUMN: {
            persist_multi_column_cell_data(
                part_persister,
                (packed_column_data<multi_column_data>*) packed_data_, pos,
                num_cols );
            break;
        }
    }
}

column_stats<multi_column_data> packed_column_records::persist_to_disk(
    data_persister* part_persister ) const {
    // first write the index: position of every data item
    // then write the actual data
    // then write the reverse index:
    // (for each data item the set of keys // with that data item)

    // first update stats
    update_statistics();

    // this means we first need to know the position of each data item
    DVLOG( 40 ) << "Get data positions";
    std::vector<uint32_t> data_positions = get_data_positions();
    DCHECK_EQ( data_counts_.size(), data_positions.size() );

    uint32_t num_keys = index_positions_.size();

    DVLOG( 40 ) << "Persist num keys";
    part_persister->write_i32( num_keys );

    uint32_t per_key_index_size =
        sizeof( uint64_t ) /*key*/ +
        ( 2 * sizeof( uint32_t ) /* relative pos, pos on disk*/ );

    uint32_t cur_pos = part_persister->get_position();
    uint32_t data_start_pos = cur_pos + ( num_keys * per_key_index_size ) +
                              /* num data items */ sizeof( uint32_t );

    DVLOG( 40 ) << "Persist keys index";
    for( uint32_t pos = 0; pos < num_keys; pos++ ) {
        uint64_t key = ( (uint64_t) pos ) + key_start_;
        int32_t  index_pos = index_positions_.at( pos );

        int32_t data_pos = index_pos;
        if( index_pos >= 0 ) {
            DCHECK_LT( index_pos, data_positions.size() );
            data_pos = data_positions.at( index_pos ) + data_start_pos;
        }
        part_persister->write_i64( key );
        part_persister->write_i32( index_pos );
        part_persister->write_i32( data_pos );
    }

    uint32_t num_data_items = keys_.size();

    part_persister->write_i32( num_data_items );
    // now write the data

    column_stats<multi_column_data> ret;

    DVLOG( 40 ) << "Persist column data";
    persist_data_to_disk( part_persister, data_positions, data_start_pos );

    DVLOG( 40 ) << "Persist per position keys";
    // now write the keys that match to that data
    part_persister->write_i32( num_data_items );
    for( uint32_t pos = 0; pos < keys_.size(); pos++ ) {
        part_persister->write_i32( keys_.at( pos ).size() );
        DCHECK_EQ( keys_.at( pos ).size(), data_counts_.at( pos ) );
        for( const uint64_t key : keys_.at( pos ) ) {
            part_persister->write_i64( key );
        }
        DCHECK_LT( pos, data_positions.size() );
        int32_t data_pos = data_positions.at( pos ) + data_start_pos;
        part_persister->write_i32( pos );
        part_persister->write_i32( data_pos );
    }

    DVLOG( 40 ) << "Persist stats";
    persist_stats_to_disk( part_persister, ret );

    return ret;
}

void packed_column_records::restore_from_disk( data_reader* part_reader ) {
    uint32_t num_keys;
    part_reader->read_i32( &num_keys );

    DVLOG(40) << "Restore read num keys";
    DCHECK_EQ( num_keys, index_positions_.size() );


    uint64_t key;
    int32_t  index_pos;
    int32_t  data_pos;

    DVLOG(40) << "Restore index positions";
    for( uint32_t pos = 0; pos < num_keys; pos++ ) {
        part_reader->read_i64( &key );
        part_reader->read_i32( (uint32_t*) &index_pos );
        part_reader->read_i32( (uint32_t*) &data_pos );

        DCHECK_EQ( key, pos + key_start_ );
        index_positions_.at( pos ) = index_pos;
    }

    uint32_t num_data_items;

    DVLOG(40) << "Restore data";
    part_reader->read_i32( &num_data_items );
    // now write the data
    restore_data_from_disk( part_reader, num_data_items );

    uint32_t read_num_data_items;
    part_reader->read_i32( &read_num_data_items );
    DCHECK_EQ( num_data_items, read_num_data_items );

    DVLOG(40) << "Restore key index";
    uint32_t num_keyset;
    for( uint32_t pos = 0; pos < num_data_items; pos++ ) {
        part_reader->read_i32( &num_keyset );
        data_counts_.emplace_back( num_keyset );

        uint64_t                     key;
        std::unordered_set<uint64_t> keys;
        for( uint32_t key_pos = 0; key_pos < num_keyset; key_pos++ ) {
            part_reader->read_i64( &key );
            keys.emplace( key );
        }
        part_reader->read_i32( (uint32_t*) &index_pos );
        part_reader->read_i32( (uint32_t*) &data_pos );
        DCHECK_EQ( index_pos, keys_.size() );
        keys_.emplace_back( keys );
    }

    DVLOG( 40 ) << "Restore stats";
    restore_stats_from_disk( part_reader );
}

void packed_column_records::restore_data_from_disk( data_reader* part_reader,
                                                    uint32_t num_data_items ) {
    CALL_INTERNAL_TEMPLATE( templ_restore_data_from_disk, part_reader,
                            num_data_items );
}

#define TEMPL_RESTORE_DATA_FROM_DISK( _reader, _num_data_items, _read_method, \
                                      _type, _cast_type )                     \
    data_.reserve( _num_data_items );                                         \
    _type _read;                                                              \
    for( uint32_t _pos = 0; _pos < _num_data_items; _pos++ ) {                \
        _reader->_read_method( (_cast_type*) &_read );                        \
        DCHECK_EQ( _pos, data_.size() );                                      \
        data_.emplace_back( _read );                                          \
    }

template <>
void packed_column_data<uint64_t>::restore_data_from_disk(
    data_reader* reader, uint32_t num_data_items ) {
    TEMPL_RESTORE_DATA_FROM_DISK( reader, num_data_items, read_i64, uint64_t,
                                  uint64_t );
}
template <>
void packed_column_data<int64_t>::restore_data_from_disk(
    data_reader* reader, uint32_t num_data_items ) {
    TEMPL_RESTORE_DATA_FROM_DISK( reader, num_data_items, read_i64, int64_t,
                                  uint64_t );
}
template <>
void packed_column_data<double>::restore_data_from_disk(
    data_reader* reader, uint32_t num_data_items ) {
    TEMPL_RESTORE_DATA_FROM_DISK( reader, num_data_items, read_dbl, double,
                                  double );
}
template <>
void packed_column_data<std::string>::restore_data_from_disk(
    data_reader* reader, uint32_t num_data_items ) {
    TEMPL_RESTORE_DATA_FROM_DISK( reader, num_data_items, read_str, std::string,
                                  std::string );
}

template <>
void packed_column_data<multi_column_data>::restore_data_from_disk(
    data_reader* reader, uint32_t num_data_items ) {
    for( uint32_t pos = 0; pos < num_data_items; pos++ ) {
        DVLOG( 40 ) << "Restore data pos:" << pos;
        DCHECK_EQ( pos, data_.size() );
        multi_column_data mcd;
        mcd.restore_from_disk( reader );
        data_.emplace_back( mcd );
    }
}


std::vector<uint32_t> packed_column_records::get_data_positions() const {
    CALL_INTERNAL_TEMPLATE_NO_ARGS( templ_get_data_positions );
    std::vector<uint32_t> data_positions;
    return data_positions;
}

void packed_column_records::persist_stats_to_disk(
    data_persister* persister, column_stats<multi_column_data>& stats ) const {
    CALL_INTERNAL_TEMPLATE( templ_persist_stats_to_disk, persister, stats );
}
void packed_column_records::restore_stats_from_disk( data_reader* reader ) {
    CALL_INTERNAL_TEMPLATE( templ_restore_stats_from_disk, reader );
}

#define TEMPL_RESTORE_STATS_FROM_DISK( _reader, _read_method, _type ) \
    _reader->read_i32( &( stats_.count_ ) );                          \
    if( stats_.count_ > 0 ) {                                         \
        _reader->_read_method( (_type*) &( stats_.min_ ) );           \
        _reader->_read_method( (_type*) &( stats_.max_ ) );           \
        _reader->_read_method( (_type*) &( stats_.average_ ) );       \
    }

template <>
void packed_column_data<uint64_t>::restore_stats_from_disk(
    data_reader* reader ) {
    TEMPL_RESTORE_STATS_FROM_DISK( reader, read_i64, uint64_t );
    }
template <>
void packed_column_data<int64_t>::restore_stats_from_disk(
    data_reader* reader ) {
    TEMPL_RESTORE_STATS_FROM_DISK( reader, read_i64, uint64_t );
}
template <>
void packed_column_data<double>::restore_stats_from_disk(
    data_reader* reader ) {
    TEMPL_RESTORE_STATS_FROM_DISK( reader, read_dbl, double );
}
template <>
void packed_column_data<std::string>::restore_stats_from_disk(
    data_reader* reader ) {
    TEMPL_RESTORE_STATS_FROM_DISK( reader, read_str, std::string );
}
template <>
void packed_column_data<multi_column_data>::restore_stats_from_disk(
    data_reader* reader ) {
    reader->read_i32( &( stats_.count_ ) );
    if( stats_.count_ > 0 ) {
        stats_.min_.restore_from_disk( reader );
        stats_.max_.restore_from_disk( reader );
        stats_.average_.restore_from_disk( reader );
    }
}

#define TEMPL_PERSIST_STATS_TO_DISK( _persister, _mcd_stats,           \
                                     _mcd_write_method, _write_method, \
                                     _cell_type, _type )               \
    _persister->write_i32( stats_.count_ );                            \
    _mcd_stats.count_ = stats_.count_;                                 \
    if( stats_.count_ > 0 ) {                                          \
        std::vector<cell_data_type>         _col_types = {_cell_type}; \
        std::vector<multi_column_data_type> _mcd_types =               \
            construct_multi_column_data_types( _col_types );           \
        multi_column_data _entry_mcd( _mcd_types );                    \
        _entry_mcd._mcd_write_method( 0, stats_.min_ );                \
        _mcd_stats.min_.deep_copy( _entry_mcd );                       \
        _entry_mcd._mcd_write_method( 0, stats_.max_ );                \
        _mcd_stats.max_.deep_copy( _entry_mcd );                       \
        _entry_mcd._mcd_write_method( 0, stats_.average_ );            \
        _mcd_stats.average_.deep_copy( _entry_mcd );                   \
                                                                       \
        _persister->_write_method( (_type) stats_.min_ );              \
        _persister->_write_method( (_type) stats_.max_ );              \
        _persister->_write_method( (_type) stats_.average_ );          \
    }                                                                  \

template <>
void packed_column_data<uint64_t>::persist_stats_to_disk(
    data_persister* persister, column_stats<multi_column_data>& stats ) const {
    TEMPL_PERSIST_STATS_TO_DISK( persister, stats, set_uint64_data, write_i64,
                                 cell_data_type::UINT64, uint64_t );
}
template <>
void packed_column_data<int64_t>::persist_stats_to_disk(
    data_persister* persister, column_stats<multi_column_data>& stats ) const {
    TEMPL_PERSIST_STATS_TO_DISK( persister, stats, set_int64_data, write_i64,
                                 cell_data_type::INT64, uint64_t );
}
template <>
void packed_column_data<double>::persist_stats_to_disk(
    data_persister* persister, column_stats<multi_column_data>& stats ) const {
    TEMPL_PERSIST_STATS_TO_DISK( persister, stats, set_double_data, write_dbl,
                                 cell_data_type::DOUBLE, double );
}
template <>
void packed_column_data<std::string>::persist_stats_to_disk(
    data_persister* persister, column_stats<multi_column_data>& stats ) const {
    TEMPL_PERSIST_STATS_TO_DISK( persister, stats, set_string_data, write_str,
                                 cell_data_type::STRING, std::string );
}
template <>
void packed_column_data<multi_column_data>::persist_stats_to_disk(
    data_persister* persister, column_stats<multi_column_data>& stats ) const {
    stats.count_ = stats_.count_;
    persister->write_i32( stats_.count_ );
    if( stats_.count_ > 0 ) {
        stats_.min_.persist_to_disk( persister );
        stats_.max_.persist_to_disk( persister );
        stats_.average_.persist_to_disk( persister );

        stats.min_.deep_copy( stats_.min_ );
        stats.max_.deep_copy( stats_.max_ );
        stats.average_.deep_copy( stats_.average_ );
        stats.sum_.deep_copy( stats_.sum_ );
    }
}

void packed_column_records::persist_data_to_disk(
    data_persister* persister, const std::vector<uint32_t>& data_positions,
    uint32_t data_start_pos ) const {
    CALL_INTERNAL_TEMPLATE( templ_persist_data_to_disk, persister,
                            data_positions, data_start_pos );
}

#define TEMPL_PERSIST_DATA_TO_DISK( _persister, _data_positions,            \
                                    _data_start_pos, _write_method, _type ) \
    DCHECK_EQ( data_.size(), _data_positions.size() );                      \
    for( uint32_t pos = 0; pos < data_.size(); pos++ ) {                    \
        DCHECK_EQ( _data_positions.at( pos ) + _data_start_pos,             \
                   _persister->get_position() );                            \
        _persister->_write_method( data_.at( pos ) );                       \
    }

template <>
void packed_column_data<uint64_t>::persist_data_to_disk(
    data_persister* persister, const std::vector<uint32_t>& data_positions,
    uint32_t data_start_pos ) const {
    TEMPL_PERSIST_DATA_TO_DISK( persister, data_positions, data_start_pos,
                                write_i64, uint64_t );
}
template <>
void packed_column_data<int64_t>::persist_data_to_disk(
    data_persister* persister, const std::vector<uint32_t>& data_positions,
    uint32_t data_start_pos ) const {
    TEMPL_PERSIST_DATA_TO_DISK( persister, data_positions, data_start_pos,
                                write_i64, uint64_t );
}
template <>
void packed_column_data<double>::persist_data_to_disk(
    data_persister* persister, const std::vector<uint32_t>& data_positions,
    uint32_t data_start_pos ) const {
    TEMPL_PERSIST_DATA_TO_DISK( persister, data_positions, data_start_pos,
                                write_dbl, double );
}
template <>
void packed_column_data<std::string>::persist_data_to_disk(
    data_persister* persister, const std::vector<uint32_t>& data_positions,
    uint32_t data_start_pos ) const {
    TEMPL_PERSIST_DATA_TO_DISK( persister, data_positions, data_start_pos,
                                write_str, std::string );
}
template <>
void packed_column_data<multi_column_data>::persist_data_to_disk(
    data_persister* persister, const std::vector<uint32_t>& data_positions,
    uint32_t data_start_pos ) const {
    DCHECK_EQ( data_.size(), data_positions.size() );
    for( uint32_t pos = 0; pos < data_.size(); pos++ ) {
        DCHECK_EQ( data_positions.at( pos ) + data_start_pos,
                   persister->get_position() );
        data_.at( pos ).persist_to_disk( persister );
    }
}

#define TEMPL_GET_DATA_POSITIONS( _type )                   \
    std::vector<uint32_t> _positions;                       \
    uint32_t              _cur_pos = 0;                     \
    for( uint32_t _pos = 0; _pos < data_.size(); _pos++ ) { \
        _positions.emplace_back( _cur_pos );                \
        _cur_pos += sizeof( _type );                        \
    }                                                       \
    return _positions;

template <>
std::vector<uint32_t> packed_column_data<uint64_t>::get_persistence_positions()
    const {
    TEMPL_GET_DATA_POSITIONS( uint64_t );
}
template <>
std::vector<uint32_t> packed_column_data<int64_t>::get_persistence_positions()
    const {
    TEMPL_GET_DATA_POSITIONS( int64_t );
}
template <>
std::vector<uint32_t> packed_column_data<double>::get_persistence_positions()
    const {
    TEMPL_GET_DATA_POSITIONS( double );
}
template <>
std::vector<uint32_t>
    packed_column_data<std::string>::get_persistence_positions() const {
    std::vector<uint32_t> positions;
    uint32_t              cur_pos = 0;
    for( uint32_t pos = 0; pos < data_.size(); pos++ ) {
        positions.emplace_back( cur_pos );
        uint32_t cur_size = sizeof( uint32_t ) + data_.at( pos ).size();
        cur_pos += cur_size;
    }
    return positions;
}
template <>
std::vector<uint32_t>
    packed_column_data<multi_column_data>::get_persistence_positions() const {

    std::vector<uint32_t> positions;
    uint32_t              cur_pos = 0;
    for( uint32_t pos = 0; pos < data_.size(); pos++ ) {
        positions.emplace_back( cur_pos );
        uint32_t cur_size = data_.at( pos ).compute_persistence_size();
        cur_pos += cur_size;
    }
    return positions;
}


uint32_t packed_column_records::get_count() const {
    CALL_INTERNAL_TEMPLATE_NO_ARGS( templ_get_count );
    return 0;
}

uint32_t packed_column_records::get_value_count( uint64_t data ) const {
    DCHECK_EQ( col_type_, cell_data_type::UINT64 );
    return templ_get_value_count<uint64_t>(
        data, *(packed_column_data<uint64_t>*) packed_data_ );
}
uint32_t packed_column_records::get_value_count( int64_t data ) const {
    DCHECK_EQ( col_type_, cell_data_type::INT64 );
    return templ_get_value_count<int64_t>(
        data, *(packed_column_data<int64_t>*) packed_data_ );
}
uint32_t packed_column_records::get_value_count( double data ) const {
    DCHECK_EQ( col_type_, cell_data_type::DOUBLE );
    return templ_get_value_count<double>( data,
        *(packed_column_data<double>*) packed_data_ );
}
uint32_t packed_column_records::get_value_count(
    const std::string& data ) const {
    DCHECK_EQ( col_type_, cell_data_type::STRING );
    return templ_get_value_count<std::string>(
        data, *(packed_column_data<std::string>*) packed_data_ );
}
uint32_t packed_column_records::get_value_count(
    const multi_column_data& data ) const {
    DCHECK_EQ( col_type_, cell_data_type::MULTI_COLUMN );
    return templ_get_value_count<multi_column_data>(
        data, *(packed_column_data<multi_column_data>*) packed_data_ );
}


column_stats<uint64_t> packed_column_records::get_uint64_column_stats() const {
    DCHECK_EQ( col_type_, cell_data_type::UINT64 );
    return templ_get_column_stats<uint64_t>(
        *(packed_column_data<uint64_t>*) packed_data_ );
}
column_stats<int64_t>     packed_column_records::get_int64_column_stats() const {
    DCHECK_EQ( col_type_, cell_data_type::INT64 );
    return templ_get_column_stats<int64_t>(
        *(packed_column_data<int64_t>*) packed_data_ );
}
column_stats<double> packed_column_records::get_double_column_stats() const {
    DCHECK_EQ( col_type_, cell_data_type::DOUBLE );
    return templ_get_column_stats<double>(
        *(packed_column_data<double>*) packed_data_ );
}
column_stats<std::string> packed_column_records::get_string_column_stats()
    const {
    DCHECK_EQ( col_type_, cell_data_type::STRING );
    return templ_get_column_stats<std::string>(
        *(packed_column_data<std::string>*) packed_data_ );
}
column_stats<multi_column_data> packed_column_records::get_multi_column_stats()
    const {
    DCHECK_EQ( col_type_, cell_data_type::MULTI_COLUMN );
    return templ_get_column_stats<multi_column_data>(
        *(packed_column_data<multi_column_data>*) packed_data_ );
}

void packed_column_records::add_to_data_counts_and_update_index_positions(
    int32_t new_data_pos ) {
    add_to_data_counts_and_update_index_positions( new_data_pos, 1 );
}


void packed_column_records::add_to_data_counts_and_update_index_positions(
    int32_t new_data_pos, uint32_t add ) {
    std::unordered_set<uint64_t> empty_set;
    // add to counts
    if( (uint32_t) new_data_pos == data_counts_.size() ) {
        DCHECK_EQ( keys_.size(), data_counts_.size() );
        // nothing to do
        data_counts_.emplace_back( add );
        keys_.emplace_back( empty_set );
    } else {
        // update the index positions for things greater than
        data_counts_.insert( data_counts_.begin() + new_data_pos, add );
        keys_.insert( keys_.begin() + new_data_pos, empty_set );
        for( uint32_t i_pos = 0; i_pos < index_positions_.size(); i_pos++ ) {
            if( index_positions_.at( i_pos ) >= new_data_pos ) {
                index_positions_.at( i_pos ) = index_positions_.at( i_pos ) + 1;
            }
        }
    }
}

void packed_column_records::remove_from_data_counts_and_update_index_positions(
    int32_t old_data_pos ) {
    data_counts_.erase( data_counts_.begin() + old_data_pos );
    keys_.erase( keys_.begin() + old_data_pos );
    // add to counts
    if( (uint32_t) old_data_pos == data_counts_.size() ) {
        return;
    }
    // update the index positions for things greater than
    for( uint32_t i_pos = 0; i_pos < index_positions_.size(); i_pos++ ) {
        if( index_positions_.at( i_pos ) >= old_data_pos ) {
            index_positions_.at( i_pos ) = index_positions_.at( i_pos ) - 1;
        }
    }
}

void packed_column_records::update_data_counts_and_update_index_positions(
    int32_t old_data_pos, int32_t new_data_pos ) {
    DVLOG( 40 ) << "Update data counts and index positions old_data_pos:"
                << old_data_pos << ", new_data_pos:" << new_data_pos
                << ", data_counts size:" << data_counts_.size();
    DVLOG(40) << "Update data counts: this:" << *this;
    DCHECK_EQ( keys_.size(), data_counts_.size() );
    std::unordered_set<uint64_t> empty_set;
    if( (uint32_t) new_data_pos == data_counts_.size() ) {
        data_counts_.emplace_back( 1 );
        keys_.emplace_back( empty_set );
    } else {
        data_counts_.insert( data_counts_.begin() + new_data_pos, 1 );
        keys_.insert( keys_.begin() + new_data_pos, empty_set );
    }

    int32_t min_pos = std::min( new_data_pos, old_data_pos );
    int32_t max_pos = std::max( new_data_pos, old_data_pos );
    if( min_pos == max_pos ) {
        DVLOG( 40 ) << "Nothing to do";
        return;
    }
    bool is_insert_smaller = false;
    if( min_pos == new_data_pos ) {
        is_insert_smaller = true;
    }

    DVLOG( 40 ) << "Is insert smaller:" << is_insert_smaller;

    for( uint32_t i_pos = 0; i_pos < index_positions_.size(); i_pos++ ) {
        int32_t ind = index_positions_.at( i_pos );
        if( ( ind >= min_pos ) and ( ind <= max_pos ) ) {
            if( is_insert_smaller ) {
                // increase
                index_positions_.at( i_pos ) = ind + 1;
            } else {
                // incresae
                index_positions_.at( i_pos ) = ind - 1;
            }
        }
    }
    DVLOG(40) << "Update data counts resulted in this:" << *this;
}

void packed_column_records::split_col_records_horizontally(
    packed_column_records* low, packed_column_records* high,
    uint64_t row_split_point ) {
    DCHECK_LT( key_start_, row_split_point );
    DCHECK_LE( row_split_point, key_end_ );

    DCHECK_EQ( col_type_, low->col_type_ );
    DCHECK_EQ( col_type_, high->col_type_ );

    if( is_column_sorted_) {
        start_timer( SPLIT_SORTED_COL_RECORDS_HORIZONTALLY_TIMER_ID );
        internal_split_col_records_horizontally( low, high, row_split_point );
        stop_timer( SPLIT_SORTED_COL_RECORDS_HORIZONTALLY_TIMER_ID );
    } else {
        start_timer( SPLIT_COL_RECORDS_HORIZONTALLY_TIMER_ID );
        internal_split_col_records_horizontally( low, high, row_split_point );
        stop_timer( SPLIT_COL_RECORDS_HORIZONTALLY_TIMER_ID );
    }
}
void packed_column_records::internal_split_col_records_horizontally(
    packed_column_records* low, packed_column_records* high,
    uint64_t row_split_point ) {

    switch( col_type_ ) {
        case cell_data_type::UINT64:
            templ_split_col_records_horizontally(
                (packed_column_data<uint64_t>*) packed_data_, low,
                (packed_column_data<uint64_t>*) low->packed_data_, high,
                (packed_column_data<uint64_t>*) high->packed_data_,
                row_split_point );
            break;
        case cell_data_type::INT64:
            templ_split_col_records_horizontally(
                (packed_column_data<uint64_t>*) packed_data_, low,
                (packed_column_data<uint64_t>*) low->packed_data_, high,
                (packed_column_data<uint64_t>*) high->packed_data_,
                row_split_point );

            break;
        case cell_data_type::DOUBLE:
            templ_split_col_records_horizontally(
                (packed_column_data<double>*) packed_data_, low,
                (packed_column_data<double>*) low->packed_data_, high,
                (packed_column_data<double>*) high->packed_data_,
                row_split_point );
            break;
        case cell_data_type::STRING:
            templ_split_col_records_horizontally(
                (packed_column_data<std::string>*) packed_data_, low,
                (packed_column_data<std::string>*) low->packed_data_, high,
                (packed_column_data<std::string>*) high->packed_data_,
                row_split_point );
            break;
        case cell_data_type::MULTI_COLUMN:
            templ_split_col_records_horizontally(
                (packed_column_data<multi_column_data>*) packed_data_, low,
                (packed_column_data<multi_column_data>*) low->packed_data_,
                high,
                (packed_column_data<multi_column_data>*) high->packed_data_,
                row_split_point );
            break;
    }
}

void packed_column_records::split_col_records_vertically(
    uint32_t col_split_point, const std::vector<cell_data_type>& col_types,
    packed_column_records*                     left,
    const std::vector<multi_column_data_type>& left_types,
    packed_column_records*                     right,
    const std::vector<multi_column_data_type>& right_types ) {
    DCHECK_EQ( col_type_, cell_data_type::MULTI_COLUMN );
    DCHECK_EQ( col_types.size(), left_types.size() + right_types.size() );
    DCHECK_LT( col_split_point, col_types.size() );
    DCHECK_GT( col_split_point, 0 );

    DCHECK_EQ( key_start_, left->key_start_ );
    DCHECK_EQ( key_start_, right->key_start_ );
    DCHECK_EQ( key_end_, left->key_end_ );
    DCHECK_EQ( key_end_, right->key_end_ );

    switch( left->col_type_ ) {
        case cell_data_type::UINT64:
            templ_split_col_records_vertically_left(
                col_split_point,
                (packed_column_data<multi_column_data>*) packed_data_,
                col_types, left,
                (packed_column_data<uint64_t>*) left->packed_data_, left_types,
                right, right_types );
            break;
        case cell_data_type::INT64:
            templ_split_col_records_vertically_left(
                col_split_point,
                (packed_column_data<multi_column_data>*) packed_data_,
                col_types, left,
                (packed_column_data<int64_t>*) left->packed_data_, left_types,
                right, right_types );
            break;
        case cell_data_type::DOUBLE:
            templ_split_col_records_vertically_left(
                col_split_point,
                (packed_column_data<multi_column_data>*) packed_data_,
                col_types, left,
                (packed_column_data<double>*) left->packed_data_, left_types,
                right, right_types );
            break;
        case cell_data_type::STRING:
            templ_split_col_records_vertically_left(
                col_split_point,
                (packed_column_data<multi_column_data>*) packed_data_,
                col_types, left,
                (packed_column_data<std::string>*) left->packed_data_,
                left_types, right, right_types );
            break;
        case cell_data_type::MULTI_COLUMN:
            templ_split_col_records_vertically_left(
                col_split_point,
                (packed_column_data<multi_column_data>*) packed_data_,
                col_types, left,
                (packed_column_data<multi_column_data>*) left->packed_data_,
                left_types, right, right_types );
            break;
    }

}

void packed_column_records::merge_col_records_horizontally(
    packed_column_records* low, packed_column_records* high ) {
    DCHECK_EQ( col_type_, low->col_type_ );
    DCHECK_EQ( col_type_, high->col_type_ );

    low->update_statistics();
    high->update_statistics();

    switch( col_type_ ) {
        case cell_data_type::UINT64:
            templ_merge_col_records_horizontally(
                (packed_column_data<uint64_t>*) packed_data_, low,
                (packed_column_data<uint64_t>*) low->packed_data_, high,
                (packed_column_data<uint64_t>*) high->packed_data_ );
            break;
        case cell_data_type::INT64:
            templ_merge_col_records_horizontally(
                (packed_column_data<uint64_t>*) packed_data_, low,
                (packed_column_data<uint64_t>*) low->packed_data_, high,
                (packed_column_data<uint64_t>*) high->packed_data_ );
            break;
        case cell_data_type::DOUBLE:
            templ_merge_col_records_horizontally(
                (packed_column_data<double>*) packed_data_, low,
                (packed_column_data<double>*) low->packed_data_, high,
                (packed_column_data<double>*) high->packed_data_ );
            break;
        case cell_data_type::STRING:
            templ_merge_col_records_horizontally(
                (packed_column_data<std::string>*) packed_data_, low,
                (packed_column_data<std::string>*) low->packed_data_, high,
                (packed_column_data<std::string>*) high->packed_data_ );
            break;
        case cell_data_type::MULTI_COLUMN:
            templ_merge_col_records_horizontally(
                (packed_column_data<multi_column_data>*) packed_data_, low,
                (packed_column_data<multi_column_data>*) low->packed_data_,
                high,
                (packed_column_data<multi_column_data>*) high->packed_data_ );
            break;
    }
}

void packed_column_records::merge_col_records_vertically(
    const std::vector<multi_column_data_type>& col_types,
    packed_column_records* left, const std::vector<cell_data_type>& left_types,
    packed_column_records*             right,
    const std::vector<cell_data_type>& right_types ) {
    DCHECK_EQ( col_type_, cell_data_type::MULTI_COLUMN );
    DCHECK_EQ( col_types.size(), left_types.size() + right_types.size() );

    DCHECK_EQ( key_start_, left->key_start_ );
    DCHECK_EQ( key_start_, right->key_start_ );
    DCHECK_EQ( key_end_, left->key_end_ );
    DCHECK_EQ( key_end_, right->key_end_ );

    switch( left->col_type_ ) {
        case cell_data_type::UINT64:
            templ_merge_col_records_vertically_left(
                (packed_column_data<multi_column_data>*) packed_data_,
                col_types, left,
                (packed_column_data<uint64_t>*) left->packed_data_, left_types,
                right, right_types );
            break;
        case cell_data_type::INT64:
            templ_merge_col_records_vertically_left(
                (packed_column_data<multi_column_data>*) packed_data_,
                col_types, left,
                (packed_column_data<int64_t>*) left->packed_data_, left_types,
                right, right_types );
            break;
        case cell_data_type::DOUBLE:
            templ_merge_col_records_vertically_left(
                (packed_column_data<multi_column_data>*) packed_data_,
                col_types, left,
                (packed_column_data<double>*) left->packed_data_, left_types,
                right, right_types );
            break;
        case cell_data_type::STRING:
            templ_merge_col_records_vertically_left(
                (packed_column_data<multi_column_data>*) packed_data_,
                col_types, left,
                (packed_column_data<std::string>*) left->packed_data_,
                left_types, right, right_types );
            break;
        case cell_data_type::MULTI_COLUMN:
            templ_merge_col_records_vertically_left(
                (packed_column_data<multi_column_data>*) packed_data_,
                col_types, left,
                (packed_column_data<multi_column_data>*) left->packed_data_,
                left_types, right, right_types );
            break;
    }
}

void packed_column_records::remove_key_from_keys( uint32_t old_data_position,
                                                  uint64_t key ) {
    DCHECK_LT( old_data_position, keys_.size() );
    DCHECK_EQ( 1, keys_.at( old_data_position ).count( key ) );
    DCHECK_GT( keys_.at( old_data_position ).size(), 1 );
    keys_.at( old_data_position ).erase( key );
}
void packed_column_records::insert_key_to_keys( uint32_t new_data_pos,
                                                uint64_t key ) {

    DCHECK_LT( new_data_pos, keys_.size() );
    keys_.at( new_data_pos ).insert( key );
}

void packed_column_records::insert_keys_to_keys(
    uint32_t new_data_pos, const std::unordered_set<uint64_t>& keys ) {
    DCHECK_LT( new_data_pos, keys_.size() );
    keys_.at( new_data_pos ).insert( keys.begin(), keys.end() );
}

void packed_column_records::check_key_only_key_in_keys( uint32_t data_position,
                                                        uint64_t key ) {
    DCHECK_LT( data_position, keys_.size() );
    DCHECK_EQ( 1, keys_.at( data_position ).count( key ) );
    DCHECK_EQ( 1, keys_.at( data_position ).size() );
}


std::ostream& operator<<( std::ostream&                os,
                          const packed_column_records& packed ) {
    os << "[ key_start_:" << packed.key_start_
       << ", key_end_:" << packed.key_end_
       << ", is_column_sorted_:" << packed.is_column_sorted_
       << ", keys_: [ ";
    for( const std::unordered_set<uint64_t>& keys : packed.keys_ ) {
        os << "( ";
        for( uint64_t key : keys ) {
            os << key << ", ";
        }
        os << " )";
    }
    os << " ], index_positions_:" << packed.index_positions_
       << ", data_counts:" << packed.data_counts_
       << ", col_type_:" << cell_data_type_to_string( packed.col_type_ );

    switch( packed.col_type_ ) {
        case cell_data_type::UINT64:
            os << ", u64_data_:"
               << *(packed_column_data<uint64_t>*) packed.packed_data_;
            break;
        case cell_data_type::INT64:
            os << ", i64_data_:"
               << *(packed_column_data<int64_t>*) packed.packed_data_;
            break;
        case cell_data_type::DOUBLE:
            os << ", double_data_:"
               << *(packed_column_data<double>*) packed.packed_data_;
            break;
        case cell_data_type::STRING:
            os << ", string_data_:"
               << *(packed_column_data<std::string>*) packed.packed_data_;
            break;
        case cell_data_type::MULTI_COLUMN:
            os << ", multi_column_data_:"
               << *(packed_column_data<multi_column_data>*) packed.packed_data_;
            break;
    }

    os << " ]";

    return os;
}

template <>
void write_in_mcd_data( multi_column_data* mcd, uint32_t start_col,
                        const multi_column_data&           data,
                        const std::vector<cell_data_type>& types ) {
    mcd->copy_in_data( start_col, data, 0, types.size() );
}

#define WRITE_IN_MCD_DATA( _mcd, _col, _data, _types, _expect_type, _method ) \
    DCHECK_EQ( _types.size(), 1 );                                            \
    DCHECK_EQ( cell_data_type::_expect_type, _types.at( 0 ) );                \
    bool _ret = _mcd->_method( _col, _data );                                 \
    DCHECK( _ret );

template <>
void write_in_mcd_data( multi_column_data* mcd, uint32_t start_col,
                        const uint64_t&           data,
                        const std::vector<cell_data_type>& types ) {
    WRITE_IN_MCD_DATA( mcd, start_col, data, types, UINT64, set_uint64_data );
}
template <>
void write_in_mcd_data( multi_column_data* mcd, uint32_t start_col,
                        const int64_t&                     data,
                        const std::vector<cell_data_type>& types ) {
    WRITE_IN_MCD_DATA( mcd, start_col, data, types, INT64, set_int64_data );
}
template <>
void write_in_mcd_data( multi_column_data* mcd, uint32_t start_col,
                        const double&                      data,
                        const std::vector<cell_data_type>& types ) {
    WRITE_IN_MCD_DATA( mcd, start_col, data, types, DOUBLE, set_double_data );
}
template <>
void write_in_mcd_data( multi_column_data* mcd, uint32_t start_col,
                        const std::string&                 data,
                        const std::vector<cell_data_type>& types ) {
    WRITE_IN_MCD_DATA( mcd, start_col, data, types, STRING, set_string_data );
}

#define SPLIT_COL_RECORD( _mcd, _col_start, _col_end, _overall_type,         \
                          _new_type, _expected_type, _get_method, _default ) \
    DCHECK_EQ( _col_start, _col_end );                                       \
    DCHECK_LT( _col_start, _overall_type.size() );                           \
    DCHECK_EQ( cell_data_type::_expected_type,                               \
               _overall_type.at( _col_start ) );                             \
    DCHECK_EQ( 1, _new_type.size() );                                        \
    DCHECK_EQ( cell_data_type::_expected_type, _new_type.at( 0 ).type_ );    \
    return _mcd._get_method( _col_start );

template <>
std::tuple<bool, uint64_t> packed_column_records::split_col_record_vertically(
    const multi_column_data& mcd, uint32_t col_start, uint32_t col_end,
    const std::vector<cell_data_type>&         overall_type,
    const std::vector<multi_column_data_type>& new_type,
    packed_column_data<uint64_t>*              col_data ) {
    SPLIT_COL_RECORD( mcd, col_start, col_end, overall_type, new_type, UINT64,
                      get_uint64_data, 0 );
}
template <>
std::tuple<bool, int64_t> packed_column_records::split_col_record_vertically(
    const multi_column_data& mcd, uint32_t col_start, uint32_t col_end,
    const std::vector<cell_data_type>&         overall_type,
    const std::vector<multi_column_data_type>& new_type,
    packed_column_data<int64_t>*               col_data ) {
    SPLIT_COL_RECORD( mcd, col_start, col_end, overall_type, new_type, INT64,
                      get_int64_data, 0 );
}
template <>
std::tuple<bool, double> packed_column_records::split_col_record_vertically(
    const multi_column_data& mcd, uint32_t col_start, uint32_t col_end,
    const std::vector<cell_data_type>&         overall_type,
    const std::vector<multi_column_data_type>& new_type,
    packed_column_data<double>*                col_data ) {
    SPLIT_COL_RECORD( mcd, col_start, col_end, overall_type, new_type, DOUBLE,
                      get_double_data, 0 );
}
template <>
std::tuple<bool, std::string>
    packed_column_records::split_col_record_vertically(
        const multi_column_data& mcd, uint32_t col_start, uint32_t col_end,
        const std::vector<cell_data_type>&         overall_type,
        const std::vector<multi_column_data_type>& new_type,
        packed_column_data<std::string>*           col_data ) {

    SPLIT_COL_RECORD( mcd, col_start, col_end, overall_type, new_type, STRING,
                      get_string_data, "" );
}

template <>
std::tuple<bool, multi_column_data>
    packed_column_records::split_col_record_vertically(
        const multi_column_data& mcd, uint32_t col_start, uint32_t col_end,
        const std::vector<cell_data_type>&         overall_type,
        const std::vector<multi_column_data_type>& new_type,
        packed_column_data<multi_column_data>*     col_data ) {
    multi_column_data mcd_ret( new_type );
    if( mcd.get_num_columns() < col_end ) {
        return std::make_tuple<>( false, mcd_ret );
    }
    mcd_ret.copy_in_data( 0, mcd, col_start, ( col_end - col_start ) + 1 );
    return std::make_tuple<>( true, mcd_ret );
}


template <>
std::tuple<std::string, std::string> update_average_and_total<std::string>(
    const std::string& old_total, uint32_t old_count, uint32_t new_count,
    const std::string& val, int multiplier ) {
    return std::make_tuple<>( "", "" );
}

template <>
std::string get_zero() {
    return "";
}

template <>
std::tuple<multi_column_data, multi_column_data>
    update_average_and_total<multi_column_data>(
        const multi_column_data& old_total, uint32_t old_count,
        uint32_t new_count, const multi_column_data& val, int multiplier ) {

    DVLOG( 40 ) << "update average and total:, old_total:" << old_total
                << ", old_count:" << old_count << ", new_count:" << new_count
                << ", val:" << val << ", multiplier:" << multiplier;
    if( new_count == 0 ) {
        return std::make_tuple<>( multi_column_data(), multi_column_data() );
    }

    multi_column_data new_total = old_total;
    new_total.add_value_to_data( val, multiplier );
    multi_column_data new_avg = new_total;
    new_avg.divide_by_constant( (double) new_count );

    return std::make_tuple<>( new_avg, new_total );
}

template <>
multi_column_data get_zero() {
    return multi_column_data();
}

void packed_column_records::snapshot_state(
    snapshot_partition_column_state& snapshot,
    std::vector<cell_data_type>&     col_types ) const {
    DCHECK_EQ( snapshot.columns.size(), col_types.size() );

    CALL_INTERNAL_TEMPLATE( templ_snapshot_state, snapshot, col_types );
}

#define TEMPL_INSTALL_FROM_SORTED( _packed, _other_records, _other_packed, \
                                   _type )                                 \
    packed_column_data<_type>* _packed_data =                              \
        (packed_column_data<_type>*) _packed;                              \
    packed_column_data<_type>* _other_packed_data =                        \
        (packed_column_data<_type>*) _other_packed;                        \
    templ_install_packed_data_from_sorted( _packed_data, _other_records,   \
                                           _other_packed_data );

void packed_column_records::install_from_sorted_data(
    const packed_column_records* other ) {
    if( !other->packed_data_ ) {
        return;
    }
    DCHECK( packed_data_ );
    DCHECK( other->packed_data_ );

    DCHECK_EQ( col_type_, other->col_type_ );
    DCHECK_EQ( key_start_, other->key_start_ );
    DCHECK_EQ( key_end_, other->key_end_ );

    DCHECK_EQ( index_positions_.size(), other->index_positions_.size() );
    DCHECK( !is_column_sorted_ );
    DCHECK( other->is_column_sorted_ );

    switch( col_type_ ) {
        case cell_data_type::UINT64: {
            TEMPL_INSTALL_FROM_SORTED( packed_data_, other, other->packed_data_,
                                       uint64_t );
            break;
        }
        case cell_data_type::INT64: {
            TEMPL_INSTALL_FROM_SORTED( packed_data_, other, other->packed_data_,
                                       int64_t );
            break;
        }
        case cell_data_type::DOUBLE: {
            TEMPL_INSTALL_FROM_SORTED( packed_data_, other, other->packed_data_,
                                       double );
            break;
        }
        case cell_data_type::STRING: {
            TEMPL_INSTALL_FROM_SORTED( packed_data_, other, other->packed_data_,
                                       std::string );
            break;
        }
        case cell_data_type::MULTI_COLUMN: {
            TEMPL_INSTALL_FROM_SORTED( packed_data_, other, other->packed_data_,
                                       multi_column_data );
            break;
        }
    }
}

#define ADD_TO_SNAPSHOT( _snapshot, _col_types, _data, _keys, _conv_method, \
                         _expected_type )                                   \
    DCHECK_EQ( 1, _col_types.size() );                                      \
    DCHECK_EQ( _col_types.at( 0 ), _expected_type );                        \
    std::vector<int64_t> _loc_keys( _keys.begin(), _keys.end() );           \
    _snapshot.columns.at( 0 ).keys.push_back( _loc_keys );                  \
    _snapshot.columns.at( 0 ).data.push_back( _conv_method( _data ) );

template <>
void add_to_snapshot( snapshot_partition_column_state&    snapshot,
                      std::vector<cell_data_type>&        col_types,
                      const uint64_t&                     data,
                      const std::unordered_set<uint64_t>& keys ) {
    ADD_TO_SNAPSHOT( snapshot, col_types, data, keys, uint64_to_string,
                     cell_data_type::UINT64 );
}

template <>
void add_to_snapshot( snapshot_partition_column_state&    snapshot,
                      std::vector<cell_data_type>&        col_types,
                      const int64_t&                      data,
                      const std::unordered_set<uint64_t>& keys ) {
    ADD_TO_SNAPSHOT( snapshot, col_types, data, keys, int64_to_string,
                     cell_data_type::INT64 );
}

template <>
void add_to_snapshot( snapshot_partition_column_state&    snapshot,
                      std::vector<cell_data_type>&        col_types,
                      const double&                       data,
                      const std::unordered_set<uint64_t>& keys ) {
    ADD_TO_SNAPSHOT( snapshot, col_types, data, keys, double_to_string,
                     cell_data_type::DOUBLE );
}

template <>
void add_to_snapshot( snapshot_partition_column_state&    snapshot,
                      std::vector<cell_data_type>&        col_types,
                      const std::string&                  data,
                      const std::unordered_set<uint64_t>& keys ) {
    ADD_TO_SNAPSHOT( snapshot, col_types, data, keys, string_to_string,
                     cell_data_type::STRING );
}

#define TEMPL_GENERATE_RESULT( _data, _pid, _project_cols, _type, \
                               _conv_method )                     \
    std::vector<result_cell> _res;                                \
    for( uint32_t _col : _project_cols ) {                        \
        if( _col == 0 ) {                                         \
            result_cell _cell;                                    \
            _cell.col_id = _col + _pid.column_start;              \
            _cell.type = _type;                                   \
            _cell.present = true;                                 \
            _cell.data = _conv_method( _data );                   \
            _res.emplace_back( _cell );                           \
        }                                                         \
    }                                                             \
    return _res;

template <>
std::vector<result_cell> templ_generate_result_cells(
    const uint64_t& data, const partition_column_identifier& pid,
    const std::vector<uint32_t>& project_cols ) {
    TEMPL_GENERATE_RESULT( data, pid, project_cols, data_type::type::UINT64,
                           uint64_to_string );
}
template <>
std::vector<result_cell> templ_generate_result_cells(
    const int64_t& data, const partition_column_identifier& pid,
    const std::vector<uint32_t>& project_cols ) {
    TEMPL_GENERATE_RESULT( data, pid, project_cols, data_type::type::INT64,
                           int64_to_string );
}
template <>
std::vector<result_cell> templ_generate_result_cells(
    const double& data, const partition_column_identifier& pid,
    const std::vector<uint32_t>& project_cols ) {
    TEMPL_GENERATE_RESULT( data, pid, project_cols, data_type::type::DOUBLE,
                           double_to_string );
}
template <>
std::vector<result_cell> templ_generate_result_cells(
    const std::string& data, const partition_column_identifier& pid,
    const std::vector<uint32_t>& project_cols ) {
    TEMPL_GENERATE_RESULT( data, pid, project_cols, data_type::type::STRING,
                           string_to_string );
}

#define TEMPL_GENERATE_MULTI_COL_RESULT( _data, _cell, _col, _get_method, \
                                         _conv_method )                   \
    auto _got_data = _data._get_method( _col );                           \
    _cell.present = std::get<0>( _got_data );                             \
    if( _cell.present ) {                                                 \
        cell.data = _conv_method( std::get<1>( _got_data ) );             \
    }

template <>
std::vector<result_cell> templ_generate_result_cells(
    const multi_column_data& data, const partition_column_identifier& pid,
    const std::vector<uint32_t>& project_cols ) {
    std::vector<result_cell> res;

    for( uint32_t col : project_cols ) {
        result_cell cell;
        cell.col_id = col + pid.column_start;
        DCHECK_LT( col, data.get_num_columns() );
        cell.type =
            cell_data_type_to_data_type( data.get_column_type( col ).type_ );

        switch( cell.type ) {
            case data_type::type::UINT64: {
                TEMPL_GENERATE_MULTI_COL_RESULT(
                    data, cell, col, get_uint64_data, uint64_to_string );
                break;
            }
            case data_type::type::INT64: {
                TEMPL_GENERATE_MULTI_COL_RESULT(
                    data, cell, col, get_int64_data, int64_to_string );
                break;
            }
            case data_type::type::DOUBLE: {
                TEMPL_GENERATE_MULTI_COL_RESULT(
                    data, cell, col, get_double_data, double_to_string );
                break;
            }
            case data_type::type::STRING: {
                TEMPL_GENERATE_MULTI_COL_RESULT(
                    data, cell, col, get_string_data, string_to_string );
                break;
            }
        }

        res.emplace_back( cell );
    }

    return res;
}

#define MCD_ADD_TO_SNAPSHOT( _snapshot, _mcd, _keys, _col_id, _get_method, \
                             _type, _conv_method )                         \
    std::tuple<bool, _type> _got_data = _mcd._get_method( _col_id );       \
    if( std::get<0>( _got_data ) ) {                                       \
        _snapshot.columns.at( _col_id ).keys.push_back( _keys );           \
        _snapshot.columns.at( _col_id ).data.push_back(                    \
            _conv_method( std::get<1>( _got_data ) ) );                    \
    }

template <>
void add_to_snapshot( snapshot_partition_column_state&    snapshot,
                      std::vector<cell_data_type>&        col_types,
                      const multi_column_data&            data,
                      const std::unordered_set<uint64_t>& keys ) {
    DCHECK_LE( data.get_num_columns(), snapshot.columns.size() );
    DCHECK_LE( data.get_num_columns(), col_types.size() );

    std::vector<int64_t> loc_keys( keys.begin(), keys.end() );

    for( uint32_t col_id = 0; col_id < data.get_num_columns(); col_id++ ) {
        switch( col_types.at( col_id ) ) {
            case cell_data_type::UINT64: {
                MCD_ADD_TO_SNAPSHOT( snapshot, data, loc_keys, col_id,
                                     get_uint64_data, uint64_t,
                                     uint64_to_string );
                break;
            }
            case cell_data_type::INT64: {
                MCD_ADD_TO_SNAPSHOT( snapshot, data, loc_keys, col_id,
                                     get_int64_data, int64_t, int64_to_string );
                break;
            }
            case cell_data_type::DOUBLE: {
                MCD_ADD_TO_SNAPSHOT( snapshot, data, loc_keys, col_id,
                                     get_double_data, double,
                                     double_to_string );
                break;
            }
            case cell_data_type::STRING: {
                MCD_ADD_TO_SNAPSHOT( snapshot, data, loc_keys, col_id,
                                     get_string_data, std::string,
                                     string_to_string );
                break;
            }
            case cell_data_type::MULTI_COLUMN: {
                DLOG( WARNING ) << "Can't have multi column type for column";
                break;
            }
        }
    }
}

#define TEMPL_GET_POSITION_FROM_CELL_PREDICATE( _c_pred, _type, _conv_method, \
                                                _expected_type )              \
    DCHECK_GT( data_.size(), 0 );                                             \
    DCHECK_NE( _c_pred.predicate, predicate_type::type::INEQUALITY );         \
    DCHECK_EQ( _c_pred.type, _expected_type );                                \
    _type    _pred_data = _conv_method( _c_pred.data );                       \
    auto     found = sorted_find_data_position( _pred_data );                 \
    uint32_t _true_data_size = data_.size() - 1;                              \
    DVLOG( 40 ) << "Get position from cell predicate:" << _c_pred.predicate   \
                << ", data:" << _pred_data                                    \
                << ", found info:" << std::get<0>( found ) << ", "            \
                << std::get<1>( found );                                      \
    switch( _c_pred.predicate ) {                                             \
        case predicate_type::type::EQUALITY: {                                \
            return std::make_tuple<>( std::get<0>( found ),                   \
                                      std::get<1>( found ),                   \
                                      std::get<1>( found ) );                 \
            break;                                                            \
        }                                                                     \
        case predicate_type::type::INEQUALITY: {                              \
            DLOG( FATAL ) << "Shouldn't fall here";                           \
            break;                                                            \
        }                                                                     \
        case predicate_type::type::LESS_THAN: {                               \
            bool _ret = true;                                                 \
            if( !std::get<0>( found ) and std::get<1>( found ) == 0 ) {       \
                /* it isn't in here at all*/                                  \
                _ret = false;                                                 \
            }                                                                 \
            /* search from start to just before the found*/                   \
            return std::make_tuple<>(                                         \
                _ret, 0,                                                      \
                std::min( (uint32_t) std::max( 0, std::get<1>( found ) - 1 ), \
                          _true_data_size ) );                                \
            break;                                                            \
        }                                                                     \
        case predicate_type::type::GREATER_THAN: {                            \
            bool _ret = true;                                                 \
            if( !std::get<0>( found ) and                                     \
                std::get<1>( found ) == (int32_t) data_.size() ) {            \
                /* it isn't in here at all*/                                  \
                _ret = false;                                                 \
            }                                                                 \
            return std::make_tuple<>( /* search from just after the found     \
                                         place to the end */                  \
                                      _ret, std::get<1>( found ) + 1,         \
                                      _true_data_size );                      \
            break;                                                            \
        }                                                                     \
        case predicate_type::type::LESS_THAN_OR_EQUAL: {                      \
            return std::make_tuple<>( /* search from just after the found     \
                                         place to the end */                  \
                                      true, 0, std::get<1>( found ) );        \
            break;                                                            \
        }                                                                     \
        case predicate_type::type::GREATER_THAN_OR_EQUAL: {                   \
            return std::make_tuple<>( /* search from just after the found     \
                                         place to the end */                  \
                                      true, std::get<1>( found ),             \
                                      _true_data_size );                      \
            break;                                                            \
        }                                                                     \
    }                                                                         \
    return std::make_tuple<>( false, 0, _true_data_size );

template <>
std::tuple<bool, uint32_t, uint32_t>
    packed_column_data<uint64_t>::get_bounded_position_from_cell_predicate(
        const cell_predicate& c_pred ) const {
    TEMPL_GET_POSITION_FROM_CELL_PREDICATE( c_pred, uint64_t, string_to_uint64,
                                            data_type::type::UINT64 );
}

template <>
std::tuple<bool, uint32_t, uint32_t>
    packed_column_data<int64_t>::get_bounded_position_from_cell_predicate(
        const cell_predicate& c_pred ) const {
    TEMPL_GET_POSITION_FROM_CELL_PREDICATE( c_pred, int64_t, string_to_int64,
                                            data_type::type::INT64 );
}

template <>
std::tuple<bool, uint32_t, uint32_t>
    packed_column_data<double>::get_bounded_position_from_cell_predicate(
        const cell_predicate&             c_pred ) const {
    TEMPL_GET_POSITION_FROM_CELL_PREDICATE( c_pred, double, string_to_double,
                                            data_type::type::DOUBLE );
}
template<>
std::tuple<bool, uint32_t, uint32_t>
    packed_column_data<std::string>::get_bounded_position_from_cell_predicate(
        const cell_predicate& c_pred ) const {
    TEMPL_GET_POSITION_FROM_CELL_PREDICATE(
        c_pred, std::string, string_to_string, data_type::type::STRING );
}

#define TEMPL_GET_POSITION_FROM_MULTI_CELL_PREDICATE(                         \
    _c_pred, _type, _conv_method, _set_method, _expected_type )               \
    DCHECK_GT( data_.size(), 0 );                                             \
    DCHECK_GT( stats_.count_, 0 );                                            \
    DCHECK_NE( _c_pred.predicate, predicate_type::type::INEQUALITY );         \
    DCHECK_EQ( _c_pred.type, _expected_type );                                \
    _type _pred_data = _conv_method( _c_pred.data );                          \
    auto  _low_data = stats_.min_;                                            \
    auto  _high_data = stats_.max_;                                           \
    DCHECK_LT( _c_pred.col_id, _low_data.get_num_columns() );                 \
    DCHECK_LT( _c_pred.col_id, _high_data.get_num_columns() );                \
    bool _set_ok = _low_data._set_method( _c_pred.col_id, _pred_data );       \
    DCHECK( _set_ok );                                                        \
    _set_ok = _high_data._set_method( _c_pred.col_id, _pred_data );           \
    DCHECK( _set_ok );                                                        \
    auto     _low_found = sorted_find_data_position( _low_data );             \
    auto     _high_found = sorted_find_data_position( _high_data );           \
    uint32_t _true_data_size = data_.size() - 1;                              \
    DVLOG( 40 ) << "Get position from cell predicate:" << _c_pred.predicate   \
                << ", data:" << _pred_data                                    \
                << ", low_found info:" << std::get<0>( _low_found ) << ", "   \
                << std::get<1>( _low_found )                                  \
                << ", high_found info:" << std::get<0>( _high_found ) << ", " \
                << std::get<1>( _high_found );                                \
    switch( _c_pred.predicate ) {                                             \
        case predicate_type::type::EQUALITY: {                                \
            return std::make_tuple<>(                                         \
                true, std::min( _true_data_size,                              \
                                (uint32_t) std::get<1>( _low_found ) ),       \
                std::min( _true_data_size,                                    \
                          (uint32_t) std::get<1>( _high_found ) ) );          \
            break;                                                            \
        }                                                                     \
        case predicate_type::type::INEQUALITY: {                              \
            DLOG( FATAL ) << "Shouldn't fall here";                           \
            break;                                                            \
        }                                                                     \
        case predicate_type::type::LESS_THAN: {                               \
            bool _ret = true;                                                 \
            if( !std::get<0>( _high_found ) and                               \
                std::get<1>( _high_found ) == 0 ) {                           \
                /* it isn't in here at all*/                                  \
                _ret = false;                                                 \
            }                                                                 \
            /* search from start to just before the found*/                   \
            return std::make_tuple<>(                                         \
                _ret, 0, std::min( (uint32_t) std::max(                       \
                                       0, std::get<1>( _high_found ) - 1 ),   \
                                   _true_data_size ) );                       \
            break;                                                            \
        }                                                                     \
        case predicate_type::type::GREATER_THAN: {                            \
            bool _ret = true;                                                 \
            if( !std::get<0>( _low_found ) and                                \
                std::get<1>( _low_found ) == (int32_t) data_.size() ) {       \
                /* it isn't in here at all*/                                  \
                _ret = false;                                                 \
            }                                                                 \
            return std::make_tuple<>( /* search from just after the found     \
                                         place to the end */                  \
                                      _ret, std::get<1>( _low_found ) + 1,    \
                                      _true_data_size );                      \
            break;                                                            \
        }                                                                     \
        case predicate_type::type::LESS_THAN_OR_EQUAL: {                      \
            return std::make_tuple<>( /* search from just after the found     \
                                         place to the end */                  \
                                      true, 0, std::get<1>( _high_found ) );  \
            break;                                                            \
        }                                                                     \
        case predicate_type::type::GREATER_THAN_OR_EQUAL: {                   \
            return std::make_tuple<>( /* search from just after the found     \
                                         place to the end */                  \
                                      true, std::get<1>( _low_found ),        \
                                      _true_data_size );                      \
            break;                                                            \
        }                                                                     \
    }                                                                         \
    return std::make_tuple<>( false, 0, _true_data_size );

template <>
std::tuple<bool, uint32_t, uint32_t> packed_column_data<multi_column_data>::
    get_bounded_position_from_cell_predicate(
        const cell_predicate& c_pred ) const {
    DCHECK_GT( data_.size(), 0 );
    DCHECK_NE( c_pred.predicate, predicate_type::type::INEQUALITY );

    switch( c_pred.type ) {
        case data_type::type::UINT64: {
            TEMPL_GET_POSITION_FROM_MULTI_CELL_PREDICATE(
                c_pred, uint64_t, string_to_uint64, set_uint64_data,
                c_pred.type );
            break;
        }
        case data_type::type::INT64: {
            TEMPL_GET_POSITION_FROM_MULTI_CELL_PREDICATE(
                c_pred, int64_t, string_to_int64, set_int64_data, c_pred.type );
            break;
        }
        case data_type::type::DOUBLE: {
            TEMPL_GET_POSITION_FROM_MULTI_CELL_PREDICATE(
                c_pred, double, string_to_double, set_double_data,
                c_pred.type );
            break;
        }
        case data_type::type::STRING: {
            TEMPL_GET_POSITION_FROM_MULTI_CELL_PREDICATE(
                c_pred, std::string, string_to_string, set_string_data,
                c_pred.type );

            break;
        }
    }

    return std::make_tuple<>( true, 0, data_.size() - 1 );
}
