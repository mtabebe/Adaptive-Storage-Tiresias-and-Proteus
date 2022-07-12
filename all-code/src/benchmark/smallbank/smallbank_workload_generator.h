#pragma once

#include "../../common/hw.h"
#include "../../distributions/distributions.h"
#include "../../distributions/non_uniform_distribution.h"
#include "../workload_operation_selector.h"
#include "smallbank_configs.h"

class smallbank_workload_generator {
   public:
    smallbank_workload_generator( zipf_distribution_cdf*             z_cdf,
                             const workload_operation_selector& op_selector,
                             int32_t client_id, const smallbank_configs& configs );

    smallbank_workload_generator( zipf_distribution_cdf* z_cdf,
                                  int32_t                  client_id,
                                  const smallbank_configs& configs );

    ~smallbank_workload_generator();

    ALWAYS_INLINE workload_operation_enum get_operation();
    ALWAYS_INLINE uint64_t get_num_ops_before_timer_check() const;

    ALWAYS_INLINE void generate_customer_ids( bool need_two_accounts );

    distributions            dist_;

    smallbank_configs configs_;

    std::array<uint64_t, 2> customer_ids_;

   private:
    ALWAYS_INLINE uint64_t generate_second_customer_id( uint64_t ori_id );

    workload_operation_selector op_selector_;
    int32_t                     client_id_;

};

#include "smallbank_workload_generator-inl.h"
