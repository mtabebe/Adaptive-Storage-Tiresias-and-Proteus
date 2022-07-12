#include "smallbank_benchmark_worker.h"

bool add_balance_valid( int a, int b ) {
    DVLOG( 50 ) << "add_balance_valid: a:" << a << ", b:" << b;
    if( INT_MAX - a < b ) {
        DVLOG( 50 ) << "add_balance_valid: a:" << a << ", b:" << b
                    << " = false";
        return false;
    } else if( INT_MAX - b < a ) {
        DVLOG( 50 ) << "add_balance_valid: a:" << a << ", b:" << b
                    << " = false";
        return false;
    }

    DCHECK_LT( a + b, INT_MAX );
    DVLOG( 50 ) << "add_balance_valid: a:" << a << ", b:" << b << " = true";
    return true;
}
bool sub_balance_valid( int a, int b ) {
    DVLOG( 50 ) << "sub_balance_valid: a:" << a << ", b:" << b;
    if( a < b ) {
        DVLOG( 50 ) << "sub_balance_valid: a:" << a << ", b:" << b
                    << " = false";
        return false;
    }

    DCHECK_GE( a - b, 0 );
    DVLOG( 50 ) << "sub_balance_valid: a:" << a << ", b:" << b << " = true";

    return true;
}


