#pragma once

#include <cinttypes>
#include <vector>

#include "../common/hw.h"

// this is based off of http://www.csee.usf.edu/~kchriste/tools/genzipf.c
// this class caches the entire distribution, so instantiate it only once,
// that is why everything here is a constant method.
class zipf_distribution_cdf {
   public:
    zipf_distribution_cdf( int64_t min, int64_t max, double alpha );
    void          init();
    ALWAYS_INLINE std::vector<int64_t> get_range() const;
    ALWAYS_INLINE double               get_alpha() const;

    ALWAYS_INLINE int64_t get_value( double p ) const;

   private:
    ALWAYS_INLINE double invert_prob( int64_t pos ) const;

    int64_t binary_search( double needle ) const;

    int64_t min_;
    int64_t max_;
    double  alpha_;
    int64_t translation_;
    int64_t num_elements_;

    std::vector<double> cummulative_dist_;  // this fills everything in
};

#include "zipf_distribution_cdf-inl.h"
