#pragma once
#include "../common/hw.h"
#include <atomic>
#include <memory>

uint64_t spin_or_yield_until( const uint64_t &             desired,
                              const std::atomic<uint64_t> &value );
uint64_t spin_or_yield_until( const uint64_t &desired, uint64_t *value );

ALWAYS_INLINE bool should_yield( const uint64_t &num_updates_required );
ALWAYS_INLINE uint64_t get_num_loops_before_backoff();

#include "update_spinner-inl.h"
