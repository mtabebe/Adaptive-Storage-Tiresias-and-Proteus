#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <sstream>
#include <stdlib.h>

#include "../src/common/predictor/ensemble/gaussian_scheduled_distribution.h"

using ::testing::_;
using ::testing::Return;

class gaussian_scheduled_distribution_test : public ::testing::Test {};

TEST_F( gaussian_scheduled_distribution_test, basic_tests_initializer ) {
    gaussian_scheduled_distribution gsd(
        100 /* base_time */, 200 /* period_time */, 50 /* time_window */,
        100 /* y_max */, 0 /* y_min */, 0 /* mean */, 1 /* std_deviation */
        );

    // The results can be programatically verified
    EXPECT_EQ( gsd.get_estimated_query_count( 100 ), 100 );
    EXPECT_EQ( gsd.get_estimated_query_count( 101 ), 60 );
    EXPECT_EQ( gsd.get_estimated_query_count( 102 ), 13 );
    EXPECT_EQ( gsd.get_estimated_query_count( 103 ), 1 );
    EXPECT_EQ( gsd.get_estimated_query_count( 104 ), 0 );

    gsd = gaussian_scheduled_distribution(
        100 /* base_time */, 200 /* period_time */, 50 /* time_window */,
        100 /* y_max */, 0 /* y_min */, 0 /* mean */, 10 /* std_deviation */
        );

    // The results can be programatically verified
    EXPECT_EQ( gsd.get_estimated_query_count( 100 ), 100 );
    EXPECT_EQ( gsd.get_estimated_query_count( 101 ), 99 );
    EXPECT_EQ( gsd.get_estimated_query_count( 102 ), 98 );
    EXPECT_EQ( gsd.get_estimated_query_count( 103 ), 95 );
    EXPECT_EQ( gsd.get_estimated_query_count( 110 ), 60 );

    // We are at the boundary for the time window
    EXPECT_EQ( gsd.get_estimated_query_count( 125 ), 4 );
    // Exceeds time window boundary since it's 100 + (50 / 2)
    // Therefore we simply default to zero
    EXPECT_EQ( gsd.get_estimated_query_count( 126 ), 0 );

    // Since the distribution is symmetrical, this is ok
    EXPECT_EQ( gsd.get_estimated_query_count( 299 ), 99 );
    EXPECT_EQ( gsd.get_estimated_query_count( 275 ), 4 );
    // Exceeds time window boundary, but in the other direction
    EXPECT_EQ( gsd.get_estimated_query_count( 274 ), 0 );

    // Consider time period shifts as well
    EXPECT_EQ( gsd.get_estimated_query_count( 300 ), 100 );
    EXPECT_EQ( gsd.get_estimated_query_count( 500 ), 100 );
}

TEST_F( gaussian_scheduled_distribution_test, basic_tests_too_early ) {
    gaussian_scheduled_distribution gsd(
        100 /* base_time */, 200 /* period_time */, 50 /* time_window */,
        100 /* y_max */, 0 /* y_min */, 0 /* mean */, 1 /* std_deviation */
        );

    // The results can be programatically verified
    EXPECT_EQ( gsd.get_estimated_query_count( 99 ), 0 );
}

TEST_F( gaussian_scheduled_distribution_test, min_value_test ) {
    gaussian_scheduled_distribution gsd(
        100 /* base_time */, 0 /* period_time */, 50 /* time_window */,
        200 /* y_max */, 100 /* y_min */, 0 /* mean */, 1 /* std_deviation */
        );

    EXPECT_EQ( gsd.get_estimated_query_count( 100 ), 200 );
    EXPECT_EQ( gsd.get_estimated_query_count( 99 ), 0 );
    EXPECT_EQ( gsd.get_estimated_query_count( 900 ), 0 );
}

TEST_F( gaussian_scheduled_distribution_test, io_tests ) {
    std::stringstream ss;
    ss << "gaussian" << std::endl;
    ss << "100 200 50" << std::endl;
    ss << "100 0 0 1" << std::endl;

    std::istringstream              ssi( ss.str() );
    gaussian_scheduled_distribution gsd( ssi );

    // The results can be programatically verified
    EXPECT_EQ( gsd.get_estimated_query_count( 100 ), 100 );
    EXPECT_EQ( gsd.get_estimated_query_count( 101 ), 60 );
}

TEST_F( gaussian_scheduled_distribution_test, no_period ) {
    std::stringstream ss;
    ss << "gaussian" << std::endl;
    ss << "100 0 50" << std::endl;
    ss << "100 0 0 1" << std::endl;

    std::istringstream              ssi( ss.str() );
    gaussian_scheduled_distribution gsd( ssi );

    EXPECT_EQ( gsd.get_estimated_query_count( 100 ), 100 );
    EXPECT_EQ( gsd.get_estimated_query_count( 101 ), 60 );

    // We do not consider periods if the periodic value is zero
    EXPECT_EQ( gsd.get_estimated_query_count( 201 ), 0 );
}

TEST_F( gaussian_scheduled_distribution_test, invalid_std_deviation ) {
    std::stringstream ss;
    ss << "gaussian" << std::endl;
    ss << "100 0 50" << std::endl;
    ss << "100 0 0 0" << std::endl;

    std::istringstream ssi( ss.str() );

    EXPECT_DEATH( gaussian_scheduled_distribution gsd( ssi ), "" );
}
