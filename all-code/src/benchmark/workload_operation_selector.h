#pragma once

#include <vector>

#include "../common/hw.h"
#include "workload_operation_enum.h"

class workload_operation_selector {
   public:
    workload_operation_selector();
    ~workload_operation_selector();

    void init( const std::vector<workload_operation_enum>& workload_ops,
               const std::vector<uint32_t>&                likelihoods );
    ALWAYS_INLINE workload_operation_enum get_operation( uint32_t prob ) const;

   private:
    std::vector<workload_operation_enum> workload_probabilities_;
};

#include "workload_operation_selector-inl.h"
