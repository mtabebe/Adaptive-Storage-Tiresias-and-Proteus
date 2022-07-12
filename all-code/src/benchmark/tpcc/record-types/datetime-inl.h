#pragma once

inline datetime get_current_time() {
    datetime d;
    d.c_since = (uint64_t) std::time( nullptr );
    return d;
}

inline int32_t get_year_from_c_since( uint64_t d ) {
    uint64_t seconds_in_year =
        60 /* seconds */ * 60 /* minutes */ * 24 /* hours */ * 365 /* days */;
    uint64_t ret = ( d / seconds_in_year );
    return ret;
}
inline int32_t get_year_from_datetime( const datetime& d ) {
    return get_year_from_c_since( d.c_since );
}

