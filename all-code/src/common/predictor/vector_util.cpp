#include "vector_util.h"

d_var_vector dvector_from_vector( const std::vector<double>& vec ) {
    d_var_vector ret( vec.size(), 1 );

    for( uint32_t pos = 0; pos < vec.size(); pos++ ) {
        ret( pos ) = vec.at( pos );
    }

    return ret;
}

std::ostream& operator<<( std::ostream& os, const d_var_vector& v ) {
    os << "{ ";
    for (const double& d: v) {
        os << d << " ";
    }
    os << "}";
    return os;
}
