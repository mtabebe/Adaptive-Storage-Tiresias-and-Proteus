#pragma once

#include <glog/stl_logging.h>

#define versioned_data_identifier_types template <typename T>
#define versioned_data_identifier_T versioned_data_identifier<T>

static_assert( sizeof( versioned_row_record_identifier ) == 32,
               "versioned_row_record_identifier should be 32 bytes" );
static_assert( sizeof( versioned_cell_data_identifier ) == 32,
               "versioned_cell_data_identifier should be 32 bytes" );

versioned_data_identifier_types
    versioned_data_identifier_T::versioned_data_identifier()
    : identifier_(), op_code_( K_NO_OP ), data_( nullptr ) {}

versioned_data_identifier_types
    versioned_data_identifier_T::~versioned_data_identifier() {}

versioned_data_identifier_types void versioned_data_identifier_T::add_db_op(
    const cell_identifier& cid, T* r, uint32_t op_code ) {
    data_ = r;
    op_code_ = op_code;
    identifier_ = cid;
}

versioned_data_identifier_types inline uint32_t
    versioned_data_identifier_T::get_op() const {
    return op_code_;
}

versioned_data_identifier_types inline bool
    versioned_data_identifier_T::is_delete_op() const {
    // must be explicitly a db operation and
    // either: a delete op, or a db op that left a record empty, which is an
    // implicit delete
    uint32_t op = op_code_;
    bool     is_delete = ( op == K_DELETE_OP ) or ( !get_data()->is_present() );
    DVLOG( 40 ) << "is_delete_op:" << is_delete << ", op:" << op
                << ", is present:" << get_data()->is_present();
    return is_delete;
}

versioned_data_identifier_types inline T* versioned_data_identifier_T::get_data()
    const {
    return data_;
}

