#pragma once

#include <glog/logging.h>
#include <gtest/gtest.h>

#if !defined( DISABLE_GCHECK )

#define GDCHECK_COMP( _val1, _val2, _op )                                    \
    if( !( (_val1) _op( _val2 ) ) ) {                                        \
        DLOG( ERROR ) << "Check failed: " << #_val1 << " " << #_op << " "    \
                      << #_val2 << " (" << _val1 << " vs. " << _val2 << ")"; \
    }

#define GDCHECK( _val )                                                        \
    if( !( _val ) ) {                                                          \
        DLOG( ERROR ) << "Check failed: " << #_val << " (" << ( _val ) << ")"; \
    }

#define GDCHECK_EQ( _val1, _val2 ) GDCHECK_COMP( _val1, _val2, == )
#define GDCHECK_NE( _val1, _val2 ) GDCHECK_COMP( _val1, _val2, != )
#define GDCHECK_LE( _val1, _val2 ) GDCHECK_COMP( _val1, _val2, <= )
#define GDCHECK_LT( _val1, _val2 ) GDCHECK_COMP( _val1, _val2, < )
#define GDCHECK_GE( _val1, _val2 ) GDCHECK_COMP( _val1, _val2, >= )
#define GDCHECK_GT( _val1, _val2 ) GDCHECK_COMP( _val1, _val2, > )

#else

#define GDCHECK_EQ( _val1, _val2 ) DCHECK_EQ( _val1, _val2 )
#define GDCHECK_NE( _val1, _val2 ) DCHECK_NE( _val1, _val2 )
#define GDCHECK_LE( _val1, _val2 ) DCHECK_LE( _val1, _val2 )
#define GDCHECK_LT( _val1, _val2 ) DCHECK_LT( _val1, _val2 )
#define GDCHECK_GE( _val1, _val2 ) DCHECK_GE( _val1, _val2 )
#define GDCHECK_GT( _val1, _val2 ) DCHECK_GT( _val1, _val2 )

#endif

#define GASSERT_COMP( _val1, _val2, _expect_op, _check_op ) \
    _expect_op( _val1, _val2 );                             \
    _check_op( _val1, _val2 );

#define GASSERT_EQ( _val1, _val2 ) \
    GASSERT_COMP( _val1, _val2, EXPECT_EQ, CHECK_EQ )
#define GASSERT_NE( _val1, _val2 ) \
    GASSERT_COMP( _val1, _val2, EXPECT_NE, CHECK_NE )
#define GASSERT_LE( _val1, _val2 ) \
    GASSERT_COMP( _val1, _val2, EXPECT_LE, CHECK_LE )
#define GASSERT_LT( _val1, _val2 ) \
    GASSERT_COMP( _val1, _val2, EXPECT_LT, CHECK_LT )
#define GASSERT_GE( _val1, _val2 ) \
    GASSERT_COMP( _val1, _val2, EXPECT_GE, CHECK_GE )
#define GASSERT_GT( _val1, _val2 ) \
    GASSERT_COMP( _val1, _val2, EXPECT_GT, CHECK_GT )

#define GASSERT_TRUE( _val1 ) \
    EXPECT_TRUE( _val1 );     \
    CHECK( _val1 );

#define GASSERT_FALSE( _val1 ) \
    EXPECT_FALSE( _val1 );     \
    CHECK( !_val1 );
