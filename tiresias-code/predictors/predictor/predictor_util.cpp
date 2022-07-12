#include "predictor_util.h"

#include <cmath>
#include <glog/logging.h>


double scale_to_bound( double pred, double bound ) {
    DVLOG( 30 ) << "Scaling to bound, pred:" << pred << ", bound:" << bound;
    double true_bound = bound;
    if ( true_bound == 0) {
      true_bound = 0.00000001;
    }
    DCHECK_NE( 0, true_bound );
    double sf = ( pred / true_bound );
    if( sf < 1 ) {
        sf = 1;
    }
    DCHECK_GE( sf, 1 );
    sf = std::min( log10( sf ), 10.0 );
    double ret = ( true_bound * sf );

    DVLOG( 30 ) << "Scaling to bound, pred:" << pred << ", bound:" << bound
                << ", true_bound:" << true_bound << ", rescaled:" << ret;

    return ret;
}

double scale_prediction_to_range( double pred, double min_bound,
                                  double max_bound ) {
    if( pred < min_bound ) {
        double scaled_pred = scale_to_bound( pred, min_bound );
        if( scaled_pred < min_bound ) {
            return min_bound;
        }
    } else if( pred > max_bound ) {
        double scaled_pred = scale_to_bound( pred, max_bound );
        if( scaled_pred > max_bound ) {
            return max_bound;
        }
    }
    return pred;
}
