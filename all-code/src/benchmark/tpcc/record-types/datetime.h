#pragma once

#include <ctime>

#include "../../../common/hw.h"

struct datetime {
   public:
    static constexpr uint64_t EMPTY_DATE = 0;
    // YYYY-MM-DD HH:MM:SS This is supposed to be a date/time field from Jan 1st
    // 1900 - Dec 31st 2100 with a resolution of 1 second. See TPC-C 1.3.1.
    // But only the man uses a string to store time. Systems programmers use
    // uint64_t's because 1) we expect this code to be robust and live longer
    // than 2038 and 2) we fight the system
    uint64_t c_since;
};

struct datetime_cols {
   public:
    static const uint32_t c_since = 0;
};

ALWAYS_INLINE datetime get_current_time();

ALWAYS_INLINE int32_t get_year_from_datetime( const datetime& d );
ALWAYS_INLINE int32_t get_year_from_c_since( uint64_t d );

#include "datetime-inl.h"
