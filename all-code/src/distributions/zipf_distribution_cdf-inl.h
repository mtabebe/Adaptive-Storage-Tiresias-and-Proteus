#pragma once

#include <cmath>
#include <glog/logging.h>

inline std::vector<int64_t> zipf_distribution_cdf::get_range() const {
    return std::vector<int64_t>( {min_, max_} );
}

inline double zipf_distribution_cdf::get_alpha() const { return alpha_; }

inline int64_t zipf_distribution_cdf::get_value( double p ) const {
    int64_t pos = binary_search( p );
    int64_t v = pos + translation_;

    DCHECK_GE( v, min_ );
    DCHECK_LE( v, max_ );
    return v;
}

inline double zipf_distribution_cdf::invert_prob( int64_t pos ) const {
    // don't divide by 0
    // note that if alpha_ = 0, then this is a uniform distribution. The higher
    // the alpha_ the greater the skew
    double p = pow( (double) ( pos + 1 ), alpha_ );
    double inv_p = ( 1.0 / p );
    return inv_p;
}
