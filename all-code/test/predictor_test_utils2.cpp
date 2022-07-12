#include "predictor_test_utils.h"

#include <gtest/gtest.h>

double sigmoid( const std::vector<double>& x ) {
    DCHECK_EQ( 1, x.size() );
    return 0.9 / ( 1 + exp( -x[0] ) ) + 0.05;
}

double mean_squared( const std::vector<double>& x ) {
    DCHECK_EQ( 2, x.size() );
    return sqrt( ( x[0] * x[0] + x[1] * x[1] ) / 2 ) + 0.05;
}

double mean_abs_sinc( const std::vector<double>& x ) {
    double ret = 0;
    for( uint32_t i = 0; i < x.size(); ++i ) {
        if( x[i] == 0 ) {
            ret += 1;
        } else {
            ret += abs( sin( x[i] ) ) / x[i];
        }
    }
    return ret / x.size() + 0.05;
}

double abs_weight( const std::vector<double>& x ) {
    DCHECK_EQ( 4, x.size() );
    return abs( 0.05 * x[0] - 0.15 * x[1] + 0.25 * x[2] - 0.35 * x[3] ) + 0.05;
}

std::vector<double> get_multi_x( const std::vector<double>& x_min,
                                 const std::vector<double>& x_max,
                                 distributions&             dist ) {
    DCHECK_EQ( x_min.size(), x_max.size() );
    std::vector<double> x( x_min.size(), 0 );
    for( uint32_t pos = 0; pos < x.size(); pos++ ) {
        x[pos] = dist.get_uniform_double_in_range( x_min[pos], x_max[pos] );
    }
    return x;
}

double make_actual_multi_y( double ( *func )( const std::vector<double>& ),
                            const std::vector<double>& x, double noise_range,
                            distributions& dist ) {
    double noise =
        dist.get_uniform_double_in_range( 0, noise_range * 2 ) - noise_range;
    return func( x ) + noise;
}

double test_dlib2_model( predictor<std::vector<double>, double>& p,
                         const std::vector<double>& x_min,
                         const std::vector<double>& x_max,
                         double ( *func )( const std::vector<double>& ),
                         double noise, int num_iters, int num_samples,
                         distributions dist ) {

    uint64_t model_v;

    for( int iter = 0; iter < num_iters; iter++ ) {
        model_v = p.model_version();

        for( int sample = 0; sample < num_samples; sample++ ) {
            std::vector<double> x = get_multi_x( x_min, x_max, dist );
            double     y = make_actual_multi_y( func, x, noise, dist );
            p.add_observation( x, y );
        }

        p.update_model();
        DVLOG( 40 ) << "training iteration " << iter << " finished";

        if( p.is_static() == false ) {
            // model version should be incremented
            EXPECT_EQ( model_v + 1, p.model_version() );
            model_v += 1;
        }
    }

    double sse = 0;
    double sae = 0;
    for( int sample = 0; sample < num_samples; sample++ ) {
        std::vector<double> x = get_multi_x( x_min, x_max, dist );
        double     y = make_actual_multi_y( func, x, noise, dist );

        double pred = p.make_prediction( x );
        double err = pred - y;

        sse += ( err * err );
        sae += abs( err );

        // model version shouldn't change
        EXPECT_EQ( model_v, p.model_version() );

        DVLOG( 40 ) << "x: " << x << ", y: " << y << ", pred: " << pred
                    << ", err: " << err;
    }

    double rmse = sqrt( sse / num_samples );
    double mae = sae / num_samples;

    DVLOG( 20 ) << "RMSE:" << rmse << ", MAE:" << mae << ", Noise:" << noise;

    return rmse;
}
