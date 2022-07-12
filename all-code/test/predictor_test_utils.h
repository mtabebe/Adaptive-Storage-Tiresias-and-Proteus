#pragma once

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "../src/common/predictor/predictor.h"
#include "../src/distributions/distributions.h"

////// SAMPLE FUNCTIONS //////

// sigmoid function: for a single number. normalized by 0.95 to allow for noise.
ALWAYS_INLINE double sigmoid( const dvector<1>& x );

// mean_squared: mean of squares for two numbers
ALWAYS_INLINE double mean_squared( const dvector<2>& x );

// mean_abs_sinc function: mean of |sinc| for three numbers
ALWAYS_INLINE double mean_abs_sinc( const dvector<3>& x );

// abs_weight function: three values with linear weights
ALWAYS_INLINE double abs_weight( const dvector<4>& x );

////// DISTRIBUTIONS //////

template <long N>
dvector<N> get_multi_x( const dvector<N>& x_min, const dvector<N>& x_max,
                        distributions& dist );

template <long N>
double         make_actual_multi_y( double ( *func )( const dvector<N>& ),
                            const dvector<N>& x, double noise_range,
                            distributions& dist );

////// TESTING OF DLIB MODELS //////

template <long N>
double test_dlib_model( predictor<dvector<N>, double>& p,
                        const dvector<N>& x_min, const dvector<N>&    x_max,
                        double ( *func )( const dvector<N>& ), double noise,
                        int num_iters, int num_samples,
                        distributions dist = distributions( nullptr ) );

#include "predictor_test_utils.tcc"
