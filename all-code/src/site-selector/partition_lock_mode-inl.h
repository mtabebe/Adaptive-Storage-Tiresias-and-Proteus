#pragma once
#include <algorithm>

inline std::string partition_lock_mode_string(
    const partition_lock_mode& lock_mode ) {
    switch( lock_mode ) {
      case partition_lock_mode::no_lock:
          return "no_lock";
      case partition_lock_mode::try_lock:
          return "try_lock";
      case partition_lock_mode::lock:
          return "lock";
    }
    return "lock";
}
inline partition_lock_mode string_to_partition_lock_mode(
    const std::string& s ) {
    std::string lower = s;
    std::transform( lower.begin(), lower.end(), lower.begin(), ::tolower );
    if( lower.compare( "no_lock" ) == 0 ) {
        return partition_lock_mode::no_lock;
    } else if( lower.compare( "try_lock" ) == 0 ) {
        return partition_lock_mode::try_lock;
    } else if( lower.compare( "lock" ) == 0 ) {
        return partition_lock_mode::lock;
    }

    return partition_lock_mode::lock;
}

