#pragma once

inline void write_buffer::add_to_record_buffer(
    versioned_row_record_identifier&& modified_record ) {
    DVLOG( 40 ) << "Adding to record buffer:" << modified_record.identifier_
                << ", opcode:" << modified_record.op_code_;
    do_not_propagate_ = 0;
    record_buffer_.emplace_back( std::move( modified_record ) );
}
inline void write_buffer::add_to_cell_buffer(
    versioned_cell_data_identifier&& modified_cell ) {
    DVLOG( 40 ) << "Adding to cell buffer:" << modified_cell.identifier_
                << ", opcode:" << modified_cell.op_code_
                << ", is_delete?:" << modified_cell.is_delete_op();
    do_not_propagate_ = 0;
    cell_buffer_.emplace_back( std::move( modified_cell ) );
}

inline void write_buffer::add_to_partition_buffer(
    partition_column_operation_identifier&& poi ) {

    if( poi.op_code_ == K_INSERT_OP ) {
        DVLOG( 40 ) << "Insert:" << poi.identifier_ << ", is_new_partition_";
        is_new_partition_ = 1;
    }
    if( poi.op_code_ != K_NO_OP ) {
        do_not_propagate_ = 0;
    }
    if ( poi.op_code_ == K_SPLIT_OP ) {
        DVLOG( 10 ) << "Split op added to write buffer:" << poi.identifier_;
    }
    partition_buffer_.emplace_back( std::move( poi ) );
}

inline bool write_buffer::empty() const {
    return ( record_buffer_.empty() and partition_buffer_.empty() and
             cell_buffer_.empty() );
}

inline void write_buffer::add_to_subscription_buffer(
    const update_propagation_information& update_prop_info,
    bool                                  do_start_subscribing ) {
    if( do_start_subscribing ) {
        start_subscribing_.push_back( update_prop_info );
    } else {
        stop_subscribing_.push_back( update_prop_info );
    }
}

inline void write_buffer::add_to_subscription_switch_buffer(
    const update_propagation_information& old_sub,
    const update_propagation_information& new_sub ) {
    switch_subscription_.push_back( old_sub );
    switch_subscription_.push_back( new_sub );
}
