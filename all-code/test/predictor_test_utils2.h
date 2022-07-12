#pragma once

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "../src/common/predictor/predictor.h"
#include "../src/distributions/distributions.h"

////// SAMPLE FUNCTIONS //////

// sigmoid function: for a single number. normalized by 0.95 to allow for noise.
// 2
double sigmoid( const std::vector<double>& x );

// mean_squared: mean of squares for two numbers
double mean_squared( const std::vector<double>& x );

// mean_abs_sinc function: mean of |sinc| for three numbers
// 3
double mean_abs_sinc( const std::vector<double>& x );

// abs_weight function: three values with linear weights
// 4
double abs_weight( const std::vector<double>& x );

////// DISTRIBUTIONS //////

std::vector<double> get_multi_x( const std::vector<double>& x_min,
                                 const std::vector<double>& x_max,
                                 distributions&             dist );

double make_actual_multi_y( double ( *func )( const std::vector<double>& ),
                            const std::vector<double>& x, double noise_range,
                            distributions& dist );

////// TESTING OF DLIB MODELS //////

double test_dlib2_model( predictor<std::vector<double>, double>& p,
                         const std::vector<double>& x_min,
                         const std::vector<double>& x_max,
                         double ( *func )( const std::vector<double>& ),
                         double noise, int num_iters, int num_samples,
                         distributions dist = distributions( nullptr ) );

