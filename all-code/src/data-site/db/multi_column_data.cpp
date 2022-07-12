#include "multi_column_data.h"

#include <string.h>

#include <glog/logging.h>

#define T_OS( _type, _get_method, _data, _col, _os ) \
    auto _found = _data._get_method( _col );         \
    DCHECK( std::get<0>( _found ) );                 \
    _type _d = std::get<1>( _found );                \
    _os << _d;

#define T_CMP( _type, _get_method, _lhs, _rhs, _col )          \
    auto _l_found = _lhs._get_method( _col );                  \
    auto _r_found = _rhs._get_method( _col );                  \
    if( std::get<0>( _l_found ) != std::get<0>( _r_found ) ) { \
        int ret = 1;                                           \
        if( !std::get<0>( _l_found ) ) {                       \
            ret = -1;                                          \
        }                                                      \
        return ret;                                            \
    }                                                          \
    if( !std::get<0>( _l_found ) ) {                           \
        return 0;                                              \
    }                                                          \
    DCHECK( std::get<0>( _l_found ) );                         \
    DCHECK( std::get<0>( _r_found ) );                         \
    _type _l = std::get<1>( _l_found );                        \
    _type _r = std::get<1>( _r_found );                        \
    if( _l < _r ) {                                            \
        return -1;                                             \
    } else if( _l > _r ) {                                     \
        return 1;                                              \
    }                                                          \
    return 0;

#define T_ADD_VALUE_TO_DATA( _type, _get_method, _set_method, _to_add, \
                             _multiplier, _col )                       \
    auto  _found_to_add_t = _to_add._get_method( _col );               \
    _type _to_add_t = 0;                                               \
    if( std::get<0>( _found_to_add_t ) ) {                             \
        _to_add_t = std::get<1>( _found_to_add_t );                    \
    }                                                                  \
    auto  _found_cur_t = _get_method( _col );                          \
    _type _cur_t = 0;                                                  \
    if( std::get<0>( _found_cur_t ) ) {                                \
        _cur_t = std::get<1>( _found_cur_t );                          \
    }                                                                  \
    _cur_t = _cur_t + ( _type )( _multiplier * _to_add_t );            \
    _set_method( _col, _cur_t );

#define T_DIVIDE_VALUE( _type, _get_method, _set_method, _col, _divisor ) \
    auto _found_cur_t = _get_method( _col );                              \
    DCHECK( std::get<0>( _found_cur_t ) );                                \
    _type _cur_t = std::get<1>( _found_cur_t );                           \
    _cur_t = _cur_t / (_type) _divisor;                                   \
    _set_method( _col, _cur_t );

multi_column_data_type::multi_column_data_type( const cell_data_type& c,
                                                uint32_t data_size )
    : type_( c ), size_( data_size ) {
    DCHECK_NE( type_, cell_data_type::MULTI_COLUMN );
}

std::ostream& operator<<( std::ostream&                 os,
                          const multi_column_data_type& data ) {
    os << "type_:" << cell_data_type_to_string( data.type_ )
       << ", size_:" << data.size_;
    return os;
}
bool operator<( const multi_column_data_type& lhs,
                const multi_column_data_type& rhs ) {
    if( lhs.type_ != rhs.type_ ) {
        return lhs.type_ < rhs.type_;
    }
    return ( lhs.size_ < rhs.size_ );
}

bool operator==( const multi_column_data_type& lhs,
                 const multi_column_data_type& rhs ) {
    if( lhs.type_ != rhs.type_ ) {
        return false;
    }
    return ( lhs.size_ == rhs.size_ );
}

multi_column_data::multi_column_data()
    : types_(), offsets_(), present_(), data_() {}
multi_column_data::multi_column_data(
    const std::vector<multi_column_data_type>& types )
    : types_( types ), offsets_(), present_(), data_() {
    uint32_t offset = 0;
    for( const auto& t : types_ ) {
        offsets_.emplace_back( offset );
        offset += t.size_;
        present_.emplace_back( false );
    }
    data_.assign( offset, 0 );
}

bool multi_column_data::remove_data( uint32_t col ) {
    DCHECK_LT( col, types_.size() );
    // dummy buf
    void* buf = (void*) &col;
    bool ret = set_data( col, buf, types_.at( col ).type_, 0 );
    if ( ret ) {
        present_.at( col ) = false;
    }
    return ret;
}

bool multi_column_data::set_data( uint32_t col, void* buf_ptr,
                                  const cell_data_type& type, uint32_t size ) {
    DVLOG( 40 ) << "set_data col:" << col
                << ", type:" << cell_data_type_to_string( type )
                << ", size:" << size;

    if( col >= types_.size() ) {
        DVLOG( 40 ) << "set_data col:" << col
                    << ", larger than types_:" << types_.size();
        return false;
    }

    DCHECK_EQ( types_.size(), offsets_.size() );
    DCHECK_LT( col, types_.size() );

    adjust_column_size( col, size );
    DCHECK_EQ( size, types_.at( col ).size_ );

    // just overwrite
    memcpy( (void*) ( data_.data() + offsets_.at( col ) ), buf_ptr, size );
    types_.at( col ).type_ = type;
    present_.at( col ) = true;

    DVLOG( 40 ) << "set_data col:" << col
                << ", type:" << cell_data_type_to_string( type )
                << ", size:" << size << ", okay!";

    return true;
}

void multi_column_data::adjust_column_size( uint32_t col, uint32_t size ) {
    uint32_t cur_offset = offsets_.at( col );

    // lets update the sizes
    if( size < types_.at( col ).size_ ) {
        uint32_t delta = types_.at( col ).size_ - size;

        DVLOG( 40 ) << "Size:" << size
                    << ", smaller than types size:" << types_.at( col ).size_
                    << ", erasing " << delta
                    << ", bytes, starting at:" << cur_offset;

        data_.erase( data_.begin() + cur_offset,
                     data_.begin() + cur_offset + delta );

        for( uint32_t next_col = col + 1; next_col < offsets_.size();
             next_col++ ) {
            DCHECK_LE( delta, offsets_.at( next_col ) );
            offsets_.at( next_col ) = offsets_.at( next_col ) - delta;
        }
        types_.at( col ).size_ = size;
    } else if( size > types_.at( col ).size_ ) {
        uint32_t delta = size - types_.at( col ).size_;

        DVLOG( 40 ) << "Size:" << size
                    << ", larger than types size:" << types_.at( col ).size_
                    << ", adding " << delta
                    << ", bytes, starting at:" << cur_offset;

        data_.insert( data_.begin() + cur_offset, delta, 0 /*char*/ );

        for( uint32_t next_col = col + 1; next_col < offsets_.size();
             next_col++ ) {
            offsets_.at( next_col ) = offsets_.at( next_col ) + delta;
        }

        types_.at( col ).size_ = size;
    }

    DCHECK_EQ( size, types_.at( col ).size_ );
}

std::tuple<bool, const char*> multi_column_data::get_data_pointer(
    uint32_t col, const cell_data_type& type, uint32_t size ) const {
    DVLOG( 40 ) << "get_data_pointer col:" << col
                << ", type:" << cell_data_type_to_string( type )
                << ", size:" << size;

    if( col >= types_.size() ) {
        DVLOG( 40 ) << "get_data_pointer col:" << col
                    << ", larger than types size:" << types_.size();
        return std::make_tuple<>( false, nullptr );
    }
    DCHECK_LT( col, types_.size() );
    DCHECK_EQ( types_.size(), offsets_.size() );
    DCHECK_EQ( present_.size(), offsets_.size() );

    if( !present_.at( col ) ) {
        DVLOG( 40 ) << "get_data_pointer type:"
                    << cell_data_type_to_string( type ) << ", is not present";
        return std::make_tuple<>( false, nullptr );
    }

    if( type != types_.at( col ).type_ ) {
        DVLOG( 40 ) << "get_data_pointer type:"
                    << cell_data_type_to_string( type )
                    << ", does not match types"
                    << cell_data_type_to_string( types_.at( col ).type_ );
        return std::make_tuple<>( false, nullptr );
    }
    if( size > types_.at( col ).size_ ) {
        DVLOG( 40 ) << "get_data_pointer size:" << size
                    << ", does not match size" << types_.at( col ).size_;
        return std::make_tuple<>( false, nullptr );
    }
    if( offsets_.at( col ) + size > data_.size() ) {
        DVLOG( 40 ) << "get_data_pointer offset:" << offsets_.at( col )
                    << " + size:" << size
                    << ", larger than data size:" << data_.size();

        return std::make_tuple<>( false, nullptr );
    }

    const char* p = data_.data() + offsets_.at( col );
    DVLOG( 40 ) << "get_data_pointer col:" << col
                << ", type:" << cell_data_type_to_string( type )
                << ", size:" << size << ", found pointer!";

    return std::make_tuple<>( true, p );
}

void multi_column_data::deep_copy( const multi_column_data& other ) {
    types_ = other.types_;
    offsets_ = other.offsets_;
    present_ = other.present_;
    data_ = other.data_;
}

void multi_column_data::copy_in_data( uint32_t                 start_col,
                                      const multi_column_data& other,
                                      uint32_t                 other_start_col,
                                      uint32_t                 num_cols ) {
    DCHECK_LT( start_col, types_.size() );
    DCHECK_LE( start_col + num_cols, types_.size() );

    DCHECK_LT( other_start_col, other.types_.size() );
    DCHECK_LE( other_start_col + num_cols, other.types_.size() );

    DCHECK_EQ( types_.size(), offsets_.size() );
    DCHECK_EQ( types_.size(), present_.size() );

    DCHECK_EQ( other.types_.size(), other.offsets_.size() );
    DCHECK_EQ( other.types_.size(), other.present_.size() );

    // adjust the bytes
    uint32_t total_bytes = 0;
    for( uint32_t col = 0; col < num_cols; col++ ) {
        uint32_t cur_col = start_col + col;
        uint32_t other_col = other_start_col + col;

        DCHECK_EQ( types_.at( cur_col ).type_,
                   other.types_.at( other_col ).type_ );
        adjust_column_size( cur_col, other.types_.at( other_col ).size_ );
        DCHECK_EQ( types_.at( cur_col ).size_,
                   other.types_.at( other_col ).size_ );
        present_.at( cur_col ) = other.present_.at( other_col );

        total_bytes += other.types_.at( other_col ).size_;
    }

    // copy the data
    memcpy( (void*) ( data_.data() + offsets_.at( start_col ) ),
            other.data_.data() + offsets_.at( other_start_col ), total_bytes );
}

uint32_t multi_column_data::compute_persistence_size() const {
    uint32_t size = 0;
    size += sizeof( uint32_t );  //  num columns;

    DCHECK_EQ( types_.size(), present_.size() );
    DCHECK_EQ( types_.size(), offsets_.size() );
    // for each type write the type, the size, the offset, the present
    uint32_t per_type_size = sizeof( uint32_t ) + sizeof( uint32_t ) +
                             sizeof( uint32_t ) + sizeof( uint8_t );
    size += ( per_type_size * types_.size() );
    // the write the length of the data
    size += sizeof( uint32_t );
    size += data_.size();

    return size;
}

void multi_column_data::persist_to_disk( data_persister* persister ) const {
    DCHECK_EQ( types_.size(), present_.size() );
    DCHECK_EQ( types_.size(), offsets_.size() );

    persister->write_i32( types_.size() );

    for( uint32_t pos = 0; pos < types_.size(); pos++ ) {
        const auto& mcd = types_.at( pos );
        persister->write_i32( (uint32_t) mcd.type_ );
        persister->write_i32( mcd.size_ );
        persister->write_i8( (uint8_t) present_.at( pos ) );
        persister->write_i32( offsets_.at( pos ) );
    }
    persister->write_chars( data_.data(), data_.size() );
}

void multi_column_data::restore_from_disk( data_reader* reader ) {
    uint32_t num_types;
    reader->read_i32( &num_types );

    types_.clear();
    offsets_.clear();
    present_.clear();

    uint32_t mcd_type;
    uint32_t mcd_size;
    uint8_t  present;
    uint32_t offset;

    for( uint32_t pos = 0; pos < num_types; pos++ ) {

        reader->read_i32( &mcd_type );
        reader->read_i32( &mcd_size );
        reader->read_i8( &present );
        reader->read_i32( &offset );

        types_.emplace_back( (cell_data_type) mcd_type, mcd_size );
        present_.emplace_back( (bool) present );
        offsets_.emplace_back( offset );
    }

    char*    data_buf = nullptr;
    uint32_t data_len = 0;

    reader->read_chars( &data_buf, &data_len );

    data_.assign( data_buf, data_buf + data_len );
}

bool multi_column_data::empty() const {
    for( const bool b : present_ ) {
        if( b ) {
            return false;
        }
    }
    return true;
}

bool multi_column_data::set_uint64_data( uint32_t col, uint64_t data ) {
    return set_data( col, (void*) &data, cell_data_type::UINT64,
                     sizeof( uint64_t ) );
}
bool multi_column_data::set_int64_data( uint32_t col, int64_t data ) {
    return set_data( col, (void*) &data, cell_data_type::INT64,
                     sizeof( int64_t ) );
}
bool multi_column_data::set_double_data( uint32_t col, double data ) {
    return set_data( col, (void*) &data, cell_data_type::DOUBLE,
                     sizeof( double ) );
}

bool multi_column_data::set_string_data( uint32_t           col,
                                         const std::string& data ) {
    return set_data( col, (void*) data.c_str(), cell_data_type::STRING,
                     data.size() );
}

std::tuple<bool, uint64_t> multi_column_data::get_uint64_data(
    uint32_t col ) const {
    uint64_t d = 0;
    bool     found = false;
    auto     found_entry =
        get_data_pointer( col, cell_data_type::UINT64, sizeof( uint64_t ) );
    if( std::get<0>( found_entry ) ) {
        d = *(const uint64_t*) std::get<1>( found_entry );
        found = true;
    }
    return std::make_tuple<>( found, d );
}
std::tuple<bool, int64_t> multi_column_data::get_int64_data(
    uint32_t col ) const {
    int64_t d = 0;
    bool    found = false;
    auto    found_entry =
        get_data_pointer( col, cell_data_type::INT64, sizeof( int64_t ) );
    if( std::get<0>( found_entry ) ) {
        d = *(const int64_t*) std::get<1>( found_entry );
        found = true;
    }
    return std::make_tuple<>( found, d );
}
std::tuple<bool, double> multi_column_data::get_double_data(
    uint32_t col ) const {
    double d = 0;
    bool   found = false;
    auto   found_entry =
        get_data_pointer( col, cell_data_type::DOUBLE, sizeof( double ) );
    if( std::get<0>( found_entry ) ) {
        d = *(const double*) std::get<1>( found_entry );
        found = true;
    }
    return std::make_tuple<>( found, d );
}
std::tuple<bool, std::string> multi_column_data::get_string_data(
    uint32_t col ) const {
    std::string d;
    bool        found = false;
    if( col >= types_.size() ) {
        return std::make_tuple<>( found, d );
    }
    DCHECK_LT( col, types_.size() );
    DCHECK_EQ( types_.size(), offsets_.size() );

    uint32_t size = types_.at( col ).size_;

    auto found_entry = get_data_pointer( col, cell_data_type::STRING, size );
    if( std::get<0>( found_entry ) ) {
        const char* ptr_to_data = std::get<1>( found_entry );
        d.assign( ptr_to_data, size );
        found = true;
    }

    return std::make_tuple<>( found, d );
}

uint32_t multi_column_data::get_num_columns() const { return types_.size(); }
multi_column_data_type multi_column_data::get_column_type(
    uint32_t col ) const {
    DCHECK_LT( col, types_.size() );
    return types_.at( col );
}

void multi_column_data::add_value_to_data( const multi_column_data& to_add,
                                           int32_t multiplier ) {
    DVLOG( 40 ) << "add_value_to_data, to_add:" << to_add
                << ", multiplier:" << multiplier;
    DCHECK_EQ( types_.size(), offsets_.size() );
    if( types_.empty() ) {
        DCHECK( data_.empty() );
        if( multiplier > 0 ) {
            DVLOG( 40 ) << "add_value_to_data, empty, copying data over";
            data_.reserve( to_add.data_.size() );

            present_ = to_add.present_;

            for( uint32_t col = 0; col < to_add.types_.size(); col++ ) {
                offsets_.emplace_back( data_.size() );
                multi_column_data_type new_t = to_add.types_.at( col );

                DVLOG( 40 ) << "add_value_to_data col:" << col
                            << ", type:" << new_t;

                if( new_t.type_ == cell_data_type::STRING ) {
                    new_t.size_ = 0;
                    DVLOG( 40 ) << "add_value_to_data col:" << col
                                << ", is string skipping data";

                } else {
                    DVLOG( 40 ) << "add_value_to_data col:" << col
                                << ", copying data";
                    for( uint32_t pos = 0; pos < new_t.size_; pos++ ) {
                        data_.emplace_back(
                            multiplier *
                            to_add.data_.at( to_add.offsets_.at( col ) +
                                             pos ) );
                    }
                }
                types_.emplace_back( new_t );
            }
        }
        // otherwise subtraction doesn't make sense
        DVLOG( 40 ) << "add_value_to_data, okay!";
        return;
    }
    DCHECK_EQ( types_.size(), to_add.types_.size() );
    DCHECK_EQ( to_add.types_.size(), to_add.offsets_.size() );

    DVLOG( 40 ) << "add_value_to_data, adding to columns";

    for( uint32_t col = 0; col < types_.size(); col++ ) {
        const auto& to_add_t = to_add.types_.at( col );
        const auto& t = types_.at( col );
        DCHECK_EQ( t.type_, to_add_t.type_ );

        switch( t.type_ ) {
            case cell_data_type::UINT64: {
                T_ADD_VALUE_TO_DATA( uint64_t, get_uint64_data, set_uint64_data,
                                     to_add, multiplier, col );
                break;
            }
            case cell_data_type::INT64: {
                T_ADD_VALUE_TO_DATA( int64_t, get_int64_data, set_int64_data,
                                     to_add, multiplier, col );
                break;
            }
            case cell_data_type::DOUBLE: {
                T_ADD_VALUE_TO_DATA( double, get_double_data, set_double_data,
                                     to_add, multiplier, col );
                break;
            }
            case cell_data_type::STRING: {
                DCHECK_EQ( t.size_, 0 );
                break;
            }
            case cell_data_type::MULTI_COLUMN: {
                break;
            }
        }
    }
}
void multi_column_data::divide_by_constant( double divisor ) {
    DCHECK_EQ( types_.size(), offsets_.size() );

    DVLOG( 40 ) << "divide_by_constant, divisor:" << divisor;

    for( uint32_t col = 0; col < types_.size(); col++ ) {
        const auto& t = types_.at( col );

        if( present_.at( col ) ) {
            switch( t.type_ ) {
                case cell_data_type::UINT64: {
                    T_DIVIDE_VALUE( uint64_t, get_uint64_data, set_uint64_data,
                                    col, divisor );
                    break;
                }
                case cell_data_type::INT64: {
                    T_DIVIDE_VALUE( uint64_t, get_int64_data, set_int64_data,
                                    col, divisor );
                    break;
                }
                case cell_data_type::DOUBLE: {
                    T_DIVIDE_VALUE( double, get_double_data, set_double_data,
                                    col, divisor );
                    break;
                }
                case cell_data_type::STRING: {
                    DCHECK_EQ( t.size_, 0 );
                    break;
                }
                case cell_data_type::MULTI_COLUMN: {
                    break;
                }
            }
        }
    }

    DVLOG( 40 ) << "divide_by_constant, divisor:" << divisor << ", okay!";
}

std::ostream& operator<<( std::ostream& os, const multi_column_data& data ) {
    os << "[ Num Columns:" << data.types_.size();
    DCHECK_EQ( data.types_.size(), data.offsets_.size() );

    for( uint32_t pos = 0; pos < data.types_.size(); pos++ ) {
        os << " { column:" << pos << ", type:" << data.types_.at( pos )
           << ", offset:" << data.offsets_.at( pos ) << ", data:";
        if( !data.present_.at( pos ) ) {
            os << " [EMPTY]";
        } else {
            switch( data.types_.at( pos ).type_ ) {
                case cell_data_type::UINT64: {
                    T_OS( uint64_t, get_uint64_data, data, pos, os );
                    break;
                }
                case cell_data_type::INT64: {
                    T_OS( int64_t, get_int64_data, data, pos, os );
                    break;
                }
                case cell_data_type::DOUBLE: {
                    T_OS( double, get_double_data, data, pos, os );
                    break;
                }
                case cell_data_type::STRING: {
                    T_OS( std::string, get_string_data, data, pos, os );
                    break;
                }
                case cell_data_type::MULTI_COLUMN:
                    break;
            }
        }

        os << " }, ";
    }

    os << " ]";

    return os;
}

int compare_columns( uint32_t col, const cell_data_type& type,
                     const multi_column_data& lhs,
                     const multi_column_data& rhs ) {
    switch( type ) {
        case cell_data_type::UINT64: {
            T_CMP( uint64_t, get_uint64_data, lhs, rhs, col );
            break;
        }
        case cell_data_type::INT64: {
            T_CMP( int64_t, get_int64_data, lhs, rhs, col );
            break;
        }
        case cell_data_type::DOUBLE: {
            T_CMP( double, get_double_data, lhs, rhs, col );
            break;
        }
        case cell_data_type::STRING: {
            auto lhs_found = lhs.get_string_data( col );
            auto rhs_found = rhs.get_string_data( col );

            if( std::get<0>( lhs_found ) != std::get<0>( rhs_found ) ) {
                int ret = 1;
                if( !std::get<0>( lhs_found ) ) {
                    ret = -1;
                }

                return ret;
            }
            if( !std::get<0>( lhs_found ) ) {
                return 0;
            }

            DCHECK( std::get<0>( lhs_found ) );
            DCHECK( std::get<0>( rhs_found ) );

            std::string lhs_str = std::get<1>( lhs_found );
            std::string rhs_str = std::get<1>( rhs_found );

            return lhs_str.compare( rhs_str );
            break;
        }
        case cell_data_type::MULTI_COLUMN: {
            return 0;
        }
    }

    return 0;
}

bool operator<( const multi_column_data& lhs, const multi_column_data& rhs ) {
    uint32_t min_size = std::min( lhs.types_.size(), rhs.types_.size() );

    DCHECK_EQ( lhs.types_.size(), lhs.offsets_.size() );
    DCHECK_EQ( rhs.types_.size(), rhs.offsets_.size() );

    for( uint32_t pos = 0; pos < min_size; pos++ ) {
        const auto& left_type = lhs.types_.at( pos );
        const auto& right_type = rhs.types_.at( pos );

        // different types;
        if( left_type.type_ != right_type.type_ ) {
            return ( left_type.type_ < right_type.type_ );
        }

        // same type
        DCHECK_EQ( left_type.type_, right_type.type_ );

        int comp = compare_columns( pos, left_type.type_, lhs, rhs );

        if( comp < 0 ) {
            return true;
        } else if( comp > 0 ) {
            return false;
        }

        // they are the same
        DCHECK_EQ( comp, 0 );
    }
    // if lhs is out of size but rhs isn't then lhs < rhs
    // if lhs has size size but rhs doesn't then lhs > rhs
    // otherwise both same size, so lhs == rhs

    if( lhs.types_.size() == rhs.types_.size() ) {
        return false;
    }
    // if lhs is the min size then lhs < rhs, otherwise, rhs is the min size
    // so lhs > rhs
    return ( lhs.types_.size() == min_size );
}

bool operator==( const multi_column_data& lhs, const multi_column_data& rhs ) {
    if( lhs.types_.size() != rhs.types_.size() ) {
        return false;
    }
    DCHECK_EQ( lhs.types_.size(), lhs.offsets_.size() );
    DCHECK_EQ( rhs.types_.size(), rhs.offsets_.size() );

    uint32_t left_total_bytes = 0;
    uint32_t right_total_bytes = 0;
    for( uint32_t pos = 0; pos < lhs.types_.size(); pos++ ) {
        if( lhs.types_.at( pos ) != rhs.types_.at( pos ) ) {
            return false;
        }
        left_total_bytes += lhs.types_.at( pos ).size_;
        right_total_bytes += rhs.types_.at( pos ).size_;
        if ( lhs.present_.at(pos) != rhs.present_.at(pos)) {
            return false;
        }
    }
    if ( left_total_bytes != right_total_bytes) {
        return false;
    }

    DCHECK_EQ( left_total_bytes, lhs.data_.size() );
    DCHECK_EQ( right_total_bytes, rhs.data_.size() );

    // compare if bytes match
    int bytes_match = memcmp( (void*) lhs.data_.data(),
                              (void*) rhs.data_.data(), left_total_bytes );
    return ( bytes_match == 0 );
}

std::vector<multi_column_data_type> construct_multi_column_data_types(
    const std::vector<cell_data_type>& col_types ) {
    std::vector<multi_column_data_type> multi_types;
    for( const auto& cell_type : col_types ) {
        uint32_t size = sizeof( uint64_t );
        if( cell_type == cell_data_type::STRING ) {
            size = 0;
        }
        multi_column_data_type mcdt = multi_column_data_type( cell_type, size );
        multi_types.push_back( mcdt );
    }
    return multi_types;
}
