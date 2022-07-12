#pragma once

#include <ctime>

static const std::string k_all_chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    // ABCDE FGHIJ KLMNO PQRST UVWXY Zabcd efghi jklmn opqrs tuvwx yz";
static const std::string k_all_nums = "01234567899";

inline int64_t distributions::get_zipf_value() {
    double  p = get_uniform_double();
    int64_t d = dist_cdf_->get_value( p );
    return d;
}

inline int64_t distributions::get_scrambled_zipf_value() {
  // 16777619 is FNV prime 32
  return get_zipf_value() * 16777619;
}

inline double distributions::get_uniform_double() {
    return real_distribution_( generator_ );
}

inline double distributions::get_uniform_double_in_range( double min,
                                                          double max ) {
    DCHECK_LE( min, max );
    double range = max - min;
    double r = get_uniform_double();
    return ( ( range * r ) + min );
}

inline int32_t distributions::get_uniform_int( int32_t min, int32_t max ) {
    DCHECK_LE( min, max );
    int32_t r = int_distribution_( generator_ );
    int32_t range = std::max( max - min, 1 );
    return ( ( r % range ) + min );
}

inline char distributions::get_uniform_char() {
    int32_t pos = get_uniform_int( 0, k_all_chars.size() - 1 );
    return k_all_chars.at( pos );
}

inline char distributions::get_uniform_nchar() {
    int32_t pos = get_uniform_int( 0, k_all_nums.size() - 1 );
    return k_all_nums.at( pos );
}

inline float distributions::fixed_point( int digits, float lower,
                                         float upper ) {
    int32_t multiplier = 1;

    for( int i = 0; i < digits; i++ ) {
        multiplier = multiplier * 10;
    }

    int32_t int_lower =
        static_cast<int32_t>( lower * static_cast<double>( multiplier ) + 0.5 );
    int32_t int_upper =
        static_cast<int32_t>( upper * static_cast<double>( multiplier ) + 0.5 );

    int32_t numerator = get_uniform_int( int_lower, int_upper );

    return ( (float) numerator ) / ( (float) multiplier );
}
inline int64_t distributions::get_current_time() const {
    return (int64_t) std::time( nullptr );
}
