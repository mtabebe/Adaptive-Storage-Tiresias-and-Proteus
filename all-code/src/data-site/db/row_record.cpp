#include "row_record.h"

#include <glog/logging.h>

#include "../../common/hw.h"

static_assert( sizeof( row_record ) == 32, "row_record should be 32 bytes" );

row_record::row_record()
    : mvcc_ptr_( packed_pointer_ops::set_packed_pointer_int( K_NOT_COMMITTED,
                                                             K_IS_INT ) ),
      is_tombstone_( K_IS_REGULAR_RECORD ),
      is_deleted_( 1 ),
      flags_( 0 ),
      num_columns_( 0 ),
      data_( nullptr ) {}

row_record::~row_record() {
    set_as_deleted();
}

void row_record::set_as_deleted() {
    is_deleted_ = 1;

    is_tombstone_ = 0;
    num_columns_ = 0;
    if( data_ != nullptr ) {
        delete[] data_;
    }
    data_ = nullptr;
}

void row_record::deep_copy( const row_record& other ) {
    is_tombstone_ = other.is_tombstone_;
    is_deleted_ = other.is_deleted_;
    flags_ = other.flags_;

    data_ = nullptr;
    if( ( other.num_columns_ > 0 ) and ( is_deleted_ == 0 ) ) {
        data_ = new packed_cell_data[other.num_columns_];
        for( uint32_t pos = 0; pos < other.num_columns_; pos++ ) {
            data_[pos].deep_copy( other.data_[pos] );
        }
    }

    num_columns_ = other.num_columns_;
}

void row_record::split_vertically( uint32_t col_split, row_record* left,
                                   row_record* right ) const {

    left->is_tombstone_ = is_tombstone_;
    left->is_deleted_ = is_deleted_;
    left->flags_ = flags_;

    right->is_tombstone_ = is_tombstone_;
    right->is_deleted_ = is_deleted_;
    right->flags_ = flags_;

    if( num_columns_ == 0 ) {
        return;
    }
    DCHECK_LT( col_split, num_columns_ );
    DCHECK_GT( col_split, 0 );

    left->num_columns_ = col_split;
    right->num_columns_ = num_columns_ - col_split;

    left->data_ = new packed_cell_data[left->num_columns_];
    right->data_ = new packed_cell_data[right->num_columns_];

    if( !is_deleted_ ) {
        for( uint32_t col_id = 0; col_id < col_split; col_id++ ) {
            left->data_[col_id].deep_copy( data_[col_id] );
        }
        for( uint32_t col_id = col_split; col_id < num_columns_; col_id++ ) {
            right->data_[col_id - col_split].deep_copy( data_[col_id] );
        }
    }
    DVLOG( 40 ) << "Finished splitting, left:" << *left << ", right:" << *right;
}
void row_record::merge_vertically( const row_record* left,
                                   const row_record* right,
                                   uint32_t          num_columns,
                                   uint32_t          merge_point ) {
    DCHECK_LT( merge_point, num_columns );
    DCHECK_GT( merge_point, 0 );

    is_tombstone_ = 0;
    is_deleted_ = 1;
    flags_ = 0;

    num_columns_ = num_columns;

    if( ( left and !left->is_deleted_ ) or ( right and !right->is_deleted_ ) ) {
        is_deleted_ = 0;
    }

    if( is_deleted_ == 1 ) {
        return;
    }
    num_columns_ = num_columns;
    data_ = new packed_cell_data[num_columns_];
    if( left and !left->is_deleted_ ) {
        DCHECK_EQ( left->num_columns_, merge_point );
        for( uint32_t col = 0; col < left->num_columns_; col++ ) {
            data_[col].deep_copy( left->data_[col] );
        }
    }
    if( right and !right->is_deleted_ ) {
        DCHECK_EQ( right->num_columns_, num_columns_ - merge_point );
        for( uint32_t right_col = 0; right_col < right->num_columns_;
             right_col++ ) {
            data_[right_col + merge_point].deep_copy( right->data_[right_col] );
        }
    }
    DVLOG( 40 ) << "finished merging:" << *this;
}

uint64_t row_record::get_version() const {
    // Get snapshot of pointer
    packed_pointer data = mvcc_ptr_.load( std::memory_order_relaxed );
    uint16_t       state = packed_pointer_ops::get_flag_bits( data );
    uint64_t       version;
    if( state == K_IS_INT ) {
        version = packed_pointer_ops::get_int( data );
    } else {
        // look up in pointer
        const transaction_state* txn_state =
            (transaction_state*) packed_pointer_ops::get_pointer( data );
        version = txn_state->get_version();
    }
    return version;
}

uint64_t row_record::spin_for_committed_version() const {
    uint64_t version = get_version();
    while( version >= K_COMMITTED ) {
        version = get_version();
    }
    return version;
}

uint64_t row_record::spin_until_not_in_commit_version() const {
    uint64_t version = get_version();
    while( version >= K_COMMITTED ) {
        version = get_version();
        if( version == K_NOT_COMMITTED ) {
            break;
        }
    }
    return version;
}

packed_cell_data* row_record::get_row_data() const { return data_; }
bool row_record::get_cell_data( uint32_t          col_pos,
                                packed_cell_data& cell_data ) const {
    DVLOG( 40 ) << "Get cell data:" << col_pos;
    if( col_pos < num_columns_ ) {
        cell_data = data_[col_pos];
        DVLOG( 40 ) << "Get cell data:" << col_pos << ", okay!";
        return true;
    }
    DVLOG( 40 ) << "Get cell data:" << col_pos << ", fail!";
    return false;
}

bool row_record::update_if_tombstone( uint64_t& partition_id ) const {
    bool is_tombstone = ( is_tombstone_ == K_IS_TOMBSTONE );
    if( !is_tombstone ) {
        return false;
    }

    DCHECK_EQ( num_columns_, 1 );
    partition_id = data_[0].get_uint64_data();

    return true;
}

void row_record::set_tombstone( uint64_t partition_id ) {
    init_num_columns( 1, false /* no need to copy*/ );
    DCHECK_EQ( num_columns_, 1 );
    data_[0].set_uint64_data( partition_id );
    is_tombstone_ = K_IS_TOMBSTONE;
    is_deleted_ = 0;
}

bool row_record::is_present() const { return ( is_deleted_ == 0 ); }
uint32_t row_record::get_num_columns() const { return num_columns_; }

void row_record::set_snapshot_vector( snapshot_vector* snap ) {
    init_num_columns( 1, false /* no need to copy*/ );
    DCHECK_EQ( num_columns_, 1 );
    data_[0].set_pointer_data( (void*) snap, K_SNAPSHOT_VECTOR_RECORD_TYPE );
    is_deleted_ = 0;
}

void row_record::init_num_columns( uint32_t num_columns, bool do_data_copy ) {
    is_deleted_ = 0;

    if( num_columns_ == 0 ) {
        if( num_columns == 0 ) {
            data_ = nullptr;
        } else {
            data_ = new packed_cell_data[num_columns];
        }
    } else if( num_columns_ < num_columns ) {
        packed_cell_data* old_data = data_;
        // copy it in and delete
        if( num_columns == 0 ) {
            data_ = nullptr;
        } else {
            data_ = new packed_cell_data[num_columns];
        }

        if( do_data_copy ) {
            for( uint32_t col_id = 0; col_id < num_columns_; col_id++ ) {
                data_[col_id].deep_copy( old_data[col_id] );
            }
        }

        if ( num_columns_ > 0) {
            delete[] old_data;
        }
    } else {
        for( uint32_t col_id = 0; col_id < num_columns_; col_id++ ) {
            if( ( col_id < num_columns ) and do_data_copy ) {
                continue;
            }
            data_[col_id].set_as_deleted();
        }
    }

    num_columns_ = num_columns;
}

void set_version( transaction_state*              txn_state,
                  const std::vector<row_record*>& row_records ) {
    uint64_t v = txn_state->get_version();
    for( row_record* r : row_records ) {
        r->set_version( v );
    }
}

std::ostream& operator<<( std::ostream& os, const row_record& r ) {
    os << "Record[ mvcc_ptr_: " << r.mvcc_ptr_
       << ", is_tombstone_:" << r.is_tombstone_
       << ", is_deleted_:" << r.is_deleted_ << ", flags_:" << r.flags_
       << ", num_columns_:" << r.num_columns_ << ", data:";

    if (!r.is_deleted_) {
        for( uint32_t col_id = 0; col_id < r.num_columns_; col_id++ ) {
            os << " COL:" << col_id << ", " << r.data_[col_id];
        }
    }

    os << "]";

    return os;
}

#define RESTORE_FROM_DISK( _col, _reader, _set_method, _read_method, \
                           _set_type, _read_type )                   \
    _set_type _data;                                                 \
    _reader->_read_method( (_read_type*) &_data );                   \
    data_[_col]._set_method( _data );

void row_record::restore_from_disk(
    data_reader* reader, const std::vector<cell_data_type>& col_types ) {

    reader->read_i32( (uint32_t*) &num_columns_ );
    DCHECK_LE( num_columns_, col_types.size() );

    uint32_t read_tombstone;
    uint32_t read_deleted;

    reader->read_i32( (uint32_t*) &read_tombstone );
    reader->read_i32( (uint32_t*) &read_deleted );
    is_tombstone_ = (uint16_t) read_tombstone;
    is_deleted_ = (uint16_t) read_deleted;

    reader->read_i32( (uint32_t*) &flags_ );

    if( num_columns_ == 0 ) {
        data_ = nullptr;
        return;
    }

      uint8_t read_is_present;

    data_ = new packed_cell_data[num_columns_];
    for( uint32_t col = 0; col < num_columns_; col++ ) {
        reader->read_i8( &read_is_present );
        if( !read_is_present ) {
            data_[col].set_as_deleted();
        } else {
            switch( col_types.at( col ) ) {
                case cell_data_type::UINT64: {
                    RESTORE_FROM_DISK( col, reader, set_uint64_data, read_i64,
                                       uint64_t, uint64_t );
                    break;
                }
                case cell_data_type::INT64: {
                    RESTORE_FROM_DISK( col, reader, set_int64_data, read_i64,
                                       int64_t, uint64_t );
                    break;
                }
                case cell_data_type::DOUBLE: {
                    RESTORE_FROM_DISK( col, reader, set_double_data, read_dbl,
                                       double, double );
                    break;
                }
                case cell_data_type::STRING: {
                    RESTORE_FROM_DISK( col, reader, set_string_data, read_str,
                                       std::string, std::string );
                    break;
                }
                case cell_data_type::MULTI_COLUMN: {
                    break;
                }
            }
        }
    }
}

#define PERSIST_TO_DISK( _col, _persister, _get_method, _write_method, \
                         _get_type, _write_type )                      \
    DCHECK( data_[_col].is_present() );                                \
    _get_type _data = data_[_col]._get_method();                       \
    _persister->_write_method( (_write_type) _data );

void row_record::persist_to_disk(
    data_persister*                    persister,
    const std::vector<cell_data_type>& col_types ) const {

    DVLOG( 40 ) << "Write num columns:" << num_columns_;

    persister->write_i32( num_columns_ );
    DCHECK_LE( num_columns_, col_types.size() );

    persister->write_i32( (uint32_t) is_tombstone_ );
    persister->write_i32( (uint32_t) is_deleted_ );
    persister->write_i32( (uint32_t) flags_ );

    if( data_ == nullptr ) {
        return;
    }

    for( uint32_t col = 0; col < num_columns_; col++ ) {
        uint8_t is_present = data_[col].is_present();
        persister->write_i8( is_present );
        if( is_present ) {
            switch( col_types.at( col ) ) {
                case cell_data_type::UINT64: {
                    PERSIST_TO_DISK( col, persister, get_uint64_data, write_i64,
                                     uint64_t, uint64_t );
                    break;
                }
                case cell_data_type::INT64: {
                    PERSIST_TO_DISK( col, persister, get_int64_data, write_i64,
                                     int64_t, uint64_t );
                    break;
                }
                case cell_data_type::DOUBLE: {
                    PERSIST_TO_DISK( col, persister, get_double_data, write_dbl,
                                     double, double );
                    break;
                }
                case cell_data_type::STRING: {
                    PERSIST_TO_DISK( col, persister, get_string_data, write_str,
                                     std::string, std::string );
                    break;
                }
                case cell_data_type::MULTI_COLUMN: {
                    break;
                }
            }
        }
    }
}

