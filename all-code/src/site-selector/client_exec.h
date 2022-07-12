#pragma once

#include <glog/logging.h>

#define make_rpc_and_get_service_timer( _method, _client, _res, _args... )   \
    std::chrono::high_resolution_clock::time_point start##_method =          \
        std::chrono::high_resolution_clock::now();                           \
    DVLOG( 10 ) << "Calling:" << #_method;                                   \
    _client->_method( _res, _args );                                         \
    std::chrono::high_resolution_clock::time_point end##_method =            \
        std::chrono::high_resolution_clock::now();                           \
    std::chrono::duration<double, std::micro> elapsed##_method =             \
        end##_method - start##_method;                                       \
    DVLOG( 10 ) << "Called:" << #_method                                     \
                << ", elapsed:" << elapsed##_method.count();                 \
    if( _res.timers.size() > 0 ) {                                           \
        uint32_t last_pos = _res.timers.size() - 1;                          \
        DCHECK_EQ( _res.timers.at( last_pos ).counter_id,                    \
                   RPC_AT_DATA_SITE_SERVICE_TIME_TIMER_ID );                 \
        double timer_total = _res.timers.at( last_pos ).counter_seen_count * \
                             _res.timers.at( last_pos ).timer_us_average;    \
        double diff = elapsed##_method.count() - timer_total;                \
        _res.timers.at( last_pos ).counter_seen_count = 1;                   \
        _res.timers.at( last_pos ).timer_us_average = diff;                  \
    }                                                                        \
    DVLOG( 40 ) << "Timers:" << _res.timers;

