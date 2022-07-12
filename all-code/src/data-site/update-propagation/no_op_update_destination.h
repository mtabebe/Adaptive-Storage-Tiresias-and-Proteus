#pragma once

#include "update_destination_interface.h"

class no_op_update_destination : public update_destination_interface {
   public:
    no_op_update_destination( int partition );
    ~no_op_update_destination();

    void send_update( serialized_update&&                update,
                      const partition_column_identifier& pcid );
    propagation_configuration get_propagation_configuration();
    uint64_t                  get_num_updates();

   private:
    int partition_;
};

