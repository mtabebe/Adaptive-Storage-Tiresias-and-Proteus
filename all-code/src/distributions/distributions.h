#pragma once

#include <random>

#include "../common/hw.h"
#include "zipf_distribution_cdf.h"

class distributions {
   public:
    distributions( zipf_distribution_cdf* dist_cdf );
    // range defined by distCDF
    ALWAYS_INLINE int64_t get_zipf_value();
    ALWAYS_INLINE int64_t get_scrambled_zipf_value();
    // 0 to 1
    ALWAYS_INLINE double get_uniform_double();
    ALWAYS_INLINE double get_uniform_double_in_range( double min, double max );
    // in range
    ALWAYS_INLINE int32_t get_uniform_int( int32_t min = 0,
                                           int32_t max = INT32_MAX );

    ALWAYS_INLINE char get_uniform_char();
    ALWAYS_INLINE char get_uniform_nchar();

    ALWAYS_INLINE float fixed_point( int digits, float lower, float upper );

    std::string write_uniform_str( uint32_t lower_len, uint32_t upper_len );
    std::string write_uniform_nstr( uint32_t lower_len, uint32_t upper_len );
    std::string write_last_name( uint32_t id );

    void write_uniform_str( uint32_t* len, char* dest, uint32_t lower_len,
                            uint32_t upper_len );
    void write_uniform_nstr( uint32_t* len, char* dest, uint32_t lower_len,
                             uint32_t upper_len );
    void write_last_name( uint32_t* len, char* dest, uint32_t id );


    std::string generate_string_for_selectivity( double             selectivity,
                                                 uint32_t           max_len,
                                                 const std::string& str ) const;

    ALWAYS_INLINE int64_t get_current_time() const;

   private:
    zipf_distribution_cdf* dist_cdf_;

    std::mt19937                     generator_;
    std::uniform_real_distribution<> real_distribution_;
    std::uniform_int_distribution<>  int_distribution_;
};

#include "distributions-inl.h"
