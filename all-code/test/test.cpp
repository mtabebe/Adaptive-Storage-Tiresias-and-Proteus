#define GTEST_HAS_TR1_TUPLE 0

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/common/constants.h"
#include "../src/common/perf_tracking.h"

int main( int argc, char **argv ) {
    google::InitGoogleLogging( argv[0] );
    ::testing::InitGoogleTest( &argc, argv );

    google::ParseCommandLineFlags( &argc, &argv, true );
    google::InstallFailureSignalHandler();

    init_global_state();

    return RUN_ALL_TESTS();
}
