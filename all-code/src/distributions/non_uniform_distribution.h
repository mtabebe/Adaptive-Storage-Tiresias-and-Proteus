#pragma once

#include "../common/hw.h"
#include "distributions.h"

// follow the non uniform distribution as specified by the TPCC spec
class non_uniform_distribution {
   public:
    ALWAYS_INLINE non_uniform_distribution();
    ALWAYS_INLINE non_uniform_distribution( uint32_t c_last, uint32_t c_id,
                                            uint32_t ol_i_id );

    ALWAYS_INLINE uint32_t nu_rand_c_last( uint32_t x, uint32_t y );
    ALWAYS_INLINE uint32_t nu_rand_c_id( uint32_t x, uint32_t y );
    ALWAYS_INLINE uint32_t nu_rand_ol_i_id( uint32_t x, uint32_t y );

   private:
    ALWAYS_INLINE uint32_t nu_rand( uint32_t A, uint32_t C, uint32_t,
                                    uint32_t y );

    distributions dist_;
    uint32_t      c_last_;
    uint32_t      c_id_;
    uint32_t      ol_i_id_;
};

#include "non_uniform_distribution-inl.h"
