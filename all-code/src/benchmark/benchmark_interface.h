#pragma once

#include "../common/constants.h"
#include "benchmark_statistics.h"
// this is just an interface, so that things conform, use it with templates

class benchmark_interface {
   public:
    virtual ~benchmark_interface(){};

    virtual void init() = 0;

    virtual void                 create_database() = 0;
    virtual void                 load_database() = 0;
    virtual void                 run_workload() = 0;
    virtual benchmark_statistics get_statistics() = 0;
};

void sleep_until_end_of_benchmark(
    std::chrono::high_resolution_clock::time_point end_time );

#define RETURN_IF_SS_DB( ret_code )                    \
    if( db_operators_.abstraction_configs_.db_type_ == \
        db_abstraction_type::SS_DB ) {                 \
        return ret_code;                               \
    }
#define RETURN_AND_COMMIT_IF_SS_DB( ret_code )         \
    if( db_operators_.abstraction_configs_.db_type_ == \
        db_abstraction_type::SS_DB ) {                 \
        db_operators_.commit_transaction();            \
        return ret_code;                               \
    }

#define RETURN_IF_PLAIN_DB( ret_code )                 \
    if( db_operators_.abstraction_configs_.db_type_ == \
        db_abstraction_type::PLAIN_DB ) {              \
        return ret_code;                               \
    }

#define RETURN_VOID_IF_SS_DB()                         \
    if( db_operators_.abstraction_configs_.db_type_ == \
        db_abstraction_type::SS_DB ) {                 \
        return;                                        \
    }

#define RETURN_VOID_AND_COMMIT_IF_SS_DB()              \
    if( db_operators_.abstraction_configs_.db_type_ == \
        db_abstraction_type::SS_DB ) {                 \
        db_operators_.commit_transaction();            \
        return;                                        \
    }

