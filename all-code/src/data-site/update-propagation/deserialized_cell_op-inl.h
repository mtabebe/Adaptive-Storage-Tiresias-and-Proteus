#pragma once

inline uint64_t deserialized_cell_op::get_op() const { return op_code_; }

inline void deserialized_cell_op::set_cell_identifier(
    uint32_t table_id, uint32_t col_id, uint64_t key ) {
    cid_.table_id_ = table_id;
    cid_.col_id_ = col_id;
    cid_.key_ = key;
}

inline char* deserialized_cell_op::get_update_buffer() const {
    return payload_;
}
inline uint32_t deserialized_cell_op::get_update_buffer_length() const {
    return (uint32_t) payload_length_;
}
