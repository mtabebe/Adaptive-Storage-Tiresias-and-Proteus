#pragma once

#include "../../gen-cpp/gen-cpp/proto_types.h"
#include "write_buffer_serialization.h"

class update_destination_interface {
   public:
    virtual ~update_destination_interface(){};

    virtual void send_update( serialized_update&&                update,
                              const partition_column_identifier& pid ) = 0;

    virtual propagation_configuration get_propagation_configuration() = 0;

    virtual uint64_t get_num_updates() = 0;
};

