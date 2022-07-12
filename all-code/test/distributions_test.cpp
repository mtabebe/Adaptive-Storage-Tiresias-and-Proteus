#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/distributions/distributions.h"

class distribution_test : public ::testing::Test {};

void check_distributions( const zipf_distribution_cdf& z_cdf,
                          const std::vector<double>&   probs,
                          const std::vector<int64_t>& expected, int64_t min,
                          int64_t max, double alpha ) {
    EXPECT_EQ( probs.size(), expected.size() );

    std::vector<int64_t> range = z_cdf.get_range();
    EXPECT_EQ( 2, range.size() );
    EXPECT_EQ( min, range.at( 0 ) );
    EXPECT_EQ( max, range.at( 1 ) );

    EXPECT_DOUBLE_EQ( alpha, z_cdf.get_alpha() );

    for( unsigned int iter = 0; iter < probs.size(); iter++ ) {
        double  prob = probs.at( iter );
        int64_t expected_get = expected.at( iter );

        int64_t dist_get = z_cdf.get_value( prob );

        EXPECT_EQ( expected_get, dist_get );
    }
}

TEST_F( distribution_test, zipf_distribution_cdf_test ) {
    int64_t min = 0;
    int64_t max = 2;
    int64_t alpha = 1.0;
    // 0 ==> 1, 1 ==> 1/2, 2 ==> 1/3
    // 0 ==> 6/11, 1 ==> 3/11,  2 ==> 2/11
    zipf_distribution_cdf z_cdf( min, max, alpha );
    z_cdf.init();

    std::vector<double> probs = {
        0.0,
        1 / (double) 3,
        1 / (double) 2,
        6 / (double) 11, /* breaking point for 0*/
        7 / (double) 11,
        3 / (double) 4,
        8 / (double) 11,
        9 / (double) 11, /*breaking point for 1*/
        10 / (double) 11,
        19 / (double) 20,
        11 / (double) 11,
        1.0,
    };
    std::vector<int64_t> expected = {0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2};

    check_distributions( z_cdf, probs, expected, min, max, alpha );

    int64_t              shift = 5;
    std::vector<int64_t> expected_shifted;
    for( int64_t e : expected ) {
        expected_shifted.push_back( e + shift );
    }
    int64_t               min_shift = min + shift;
    int64_t               max_shift = max + shift;
    zipf_distribution_cdf z_cdf_shift( min_shift, max_shift, alpha );
    z_cdf_shift.init();

    check_distributions( z_cdf_shift, probs, expected_shifted, min_shift,
                         max_shift, alpha );
}

TEST_F( distribution_test, zipf_distribution_test ) {
    int64_t min = 5;
    // i've tested up to 10 million
    int64_t max = 100000;
    int64_t alpha = 1.0;

    int64_t               numTests = 10000;
    zipf_distribution_cdf z_cdf( min, max, alpha );
    z_cdf.init();

    distributions dist( &z_cdf );

    for( int64_t i = 0; i < numTests; i++ ) {
        int64_t zd = dist.get_zipf_value();

        EXPECT_GE( zd, min );
        EXPECT_LE( zd, max );

        int64_t ud = dist.get_uniform_int( min, max );

        EXPECT_GE( ud, min );
        EXPECT_LE( ud, max );
    }
}

TEST_F( distribution_test, string_selectivity_test) {
    distributions dist( nullptr );

    auto found_string =
        dist.generate_string_for_selectivity( 0.0, 2, k_all_chars );
    EXPECT_TRUE( found_string.empty() );

    found_string = dist.generate_string_for_selectivity( 1.0, 2, k_all_chars );
    EXPECT_EQ( "z", found_string );

    found_string = dist.generate_string_for_selectivity( 0.35, 2, k_all_chars );
    // 0th index
    // 18th char, 49 char (19/52 is ~/0.36)
    EXPECT_EQ( "Sx", found_string );
}
