#pragma once

enum partition_lock_mode { no_lock, try_lock, lock };

ALWAYS_INLINE std::string partition_lock_mode_string(
    const partition_lock_mode& lock_mode );
ALWAYS_INLINE partition_lock_mode
    string_to_partition_lock_mode( const std::string& s );

#include "partition_lock_mode-inl.h"

