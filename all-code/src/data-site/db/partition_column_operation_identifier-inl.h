#pragma once

inline partition_column_operation_identifier::
    partition_column_operation_identifier()
    : identifier_(), data_64_( 0 ), data_32_( 0 ), op_code_( K_NO_OP ) {}
inline partition_column_operation_identifier::~partition_column_operation_identifier() {}

inline void partition_column_operation_identifier::add_partition_op(
    const partition_column_identifier& id, uint64_t data_64, uint32_t data_32, uint32_t op ) {
    identifier_ = id;
    data_64_ = data_64;
    data_32_ = data_32;
    op_code_ = op;
}

