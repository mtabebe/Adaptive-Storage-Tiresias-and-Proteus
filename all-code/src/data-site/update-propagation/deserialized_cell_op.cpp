#include "deserialized_cell_op.h"

deserialized_cell_op::deserialized_cell_op()
    : cid_(), op_code_( K_NO_OP ), payload_( nullptr ), payload_length_() {}
deserialized_cell_op::~deserialized_cell_op() {}

void deserialized_cell_op::add_update( uint64_t db_op, char* update_buffer,
                                       uint32_t update_length ) {
    op_code_ = db_op;
    payload_ = update_buffer;
    payload_length_ = update_length;
}
void deserialized_cell_op::add_delete() { op_code_ = K_DELETE_OP; }

