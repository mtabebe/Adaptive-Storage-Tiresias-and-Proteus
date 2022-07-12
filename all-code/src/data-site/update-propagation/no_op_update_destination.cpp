#include "no_op_update_destination.h"

no_op_update_destination::no_op_update_destination( int partition )
    : partition_( partition ) {}
no_op_update_destination::~no_op_update_destination() {}

void no_op_update_destination::send_update(
    serialized_update&& update, const partition_column_identifier& pcid ) {
    if( update.buffer_ ) {
        free( update.buffer_ );
        update.buffer_ = nullptr;
    }
}

propagation_configuration
    no_op_update_destination::get_propagation_configuration() {
    propagation_configuration config;
    config.type = propagation_type::NO_OP;
    config.partition = partition_;
    return config;
}

uint64_t no_op_update_destination::get_num_updates() { return 0; }

