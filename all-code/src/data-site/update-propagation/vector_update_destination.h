#pragma once

#include "update_destination_interface.h"

#include "../../concurrency/semaphore.h"

class vector_update_destination : public update_destination_interface {
  public:
   vector_update_destination( int32_t id );
   ~vector_update_destination();

   void send_update( serialized_update&&                update,
                     const partition_column_identifier& pid );
   propagation_configuration get_propagation_configuration();
   uint64_t                  get_num_updates();

   bool do_stored_and_expected_match(
       const std::vector<serialized_update>& expected );

  private:
   int32_t                        id_;
   std::vector<serialized_update> stored_updates_;
   semaphore                      lock_;
};

