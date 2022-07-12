#pragma once

inline std::string lock_type_string( const lock_type& l_type ) {
    switch( l_type ) {
        case SPINLOCK:
            return "SPINLOCK";
        case MUTEX_LOCK:
            return "MUTEX_LOCK";
        case SEMAPHORE:
            return "SEMAPHORE";
        case UNKNOWN_LOCK:
            return "UNKNOWN_LOCK";
    }
    return "UNKNOWN_LOCK";
}
inline lock_type string_to_lock_type( const std::string& s ) {
    std::string lower = s;
    std::transform( lower.begin(), lower.end(), lower.begin(), ::tolower );
    if( lower.compare( "spinlock" ) == 0 ) {
        return lock_type::SPINLOCK;
    } else if( lower.compare( "mutex_lock" ) == 0 ) {
        return lock_type::MUTEX_LOCK;
    } else if( lower.compare( "semaphore" ) == 0 ) {
        return lock_type::SEMAPHORE;
    }
    return lock_type::UNKNOWN_LOCK;
}
