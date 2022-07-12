#include "zipf_distribution_cdf.h"

#include <math.h>

#include <glog/logging.h>

zipf_distribution_cdf::zipf_distribution_cdf( int64_t min, int64_t max,
                                              double alpha )
    : min_( min ),
      max_( max ),
      alpha_( alpha ),
      translation_( min ),
      num_elements_( 0 ),
      cummulative_dist_() {
    DVLOG( 5 ) << "zipf_distribution_cdf range: [" << min << ", " << max << "]";
    DVLOG( 5 ) << "zipf_distribution_cdf alpha: " << alpha;
    DCHECK_LE( min_, max_ );

    num_elements_ = ( max_ - min_ ) + 1;
    cummulative_dist_.reserve( num_elements_ );
}

// it's expensive to compute the distribution, so calculate all the probs
// and then later binary search
void zipf_distribution_cdf::init() {
    DVLOG( 20 ) << "init zipf: number of elements:" << num_elements_
                << ", alpha:" << alpha_;
    double running_sum = 0.0;

    for( int64_t pos = 0; pos < num_elements_; pos++ ) {
        double prob = invert_prob( pos );
        running_sum += prob;
        DVLOG( 20 ) << "prob:" << pos << " = " << prob
                    << ", cdf:" << running_sum;
        cummulative_dist_.push_back( running_sum );
    }

    for( int64_t pos = 0; pos < num_elements_; pos++ ) {
        double cum_prob = cummulative_dist_.at( pos );
        double prob = cum_prob / running_sum;
        cummulative_dist_.at( pos ) = prob;
        DVLOG( 20 ) << "prob:" << pos << " = " << prob;
    }
}

// binary search isn't quite right since we need to find it within a range
int64_t zipf_distribution_cdf::binary_search( double needle ) const {
    int64_t min_pos = 0;
    int64_t max_pos = num_elements_ - 1;
    int64_t med_pos = 0 /*(min_pos + max_pos) / 2*/;
    double  elem = 0;

    while( min_pos <= max_pos ) {
        med_pos = ( min_pos + max_pos ) / 2;
        DCHECK_LT( med_pos, num_elements_ );
        DCHECK_GE( med_pos, 0 );

        elem = cummulative_dist_.at( med_pos );
        DVLOG( 40 ) << "Binary Search for:" << needle << " in range ["
                    << min_pos << ", " << max_pos << "] med:" << med_pos
                    << " = " << elem;
        // found
        if( elem == needle ) {
            // breakout
            DVLOG( 40 ) << "Binary Search break equality!";
            break;
        } else if( elem < needle ) {
            min_pos = med_pos + 1;
        } else {  // >
            if( min_pos == max_pos ) {
                // breakout
                DVLOG( 40 ) << "Binary Search break greater than but min_pos "
                               "== max_pos!";
                break;
            }
            max_pos = med_pos - 1;
        }
        DVLOG( 40 ) << "Binary Search for:" << needle << " in range ["
                    << min_pos << ", " << max_pos << "]";
    }
    if( elem < needle ) {
        int64_t old_pos = med_pos;
        med_pos = std::min( old_pos + 1, num_elements_ - 1 );
        DVLOG( 40 ) << "Binary Search:" << needle
                    << " updating med_pos from:" << old_pos
                    << " to: " << med_pos;
    }
    DVLOG( 40 ) << "Binary Search:" << needle << " found in pos:" << med_pos;
    return med_pos;
}
